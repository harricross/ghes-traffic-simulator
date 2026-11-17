package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"syscall"
	"time"

	"github.com/actions/scaleset"
)

const (
	modeFleet = "fleet"
	// runnerGroupLimit is the documented, non-adjustable maximum number of
	// runners in one runner group.
	runnerGroupLimit   = 10000
	fleetMaxGroups     = 20
	fleetScaleSetMax   = 40
	sessionBytesPer    = 256 << 10 // measured ~0.21 GiB per 1,000 sessions
	fileDescriptorBase = 1024
	fleetMargin        = 10 * time.Minute
)

var fleetNamePart = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,15}$`)

type fleetGroupSpec struct {
	Name    string `json:"name"`
	Real    int    `json:"real"`
	Online  int    `json:"online"`
	Offline int    `json:"offline"`
}

func (g fleetGroupSpec) total() int { return g.Real + g.Online + g.Offline }

// fleetSpec is the non-secret description of a fleet run: one entry per
// runner group with its mix of real, fake-online and fake-offline runners.
type fleetSpec struct {
	GroupPrefix string           `json:"group_prefix"`
	Groups      []fleetGroupSpec `json:"groups"`
}

func loadFleetSpec(path string) (fleetSpec, error) {
	var spec fleetSpec
	file, err := os.Open(path)
	if err != nil {
		return spec, fmt.Errorf("open fleet spec: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return spec, fmt.Errorf("decode fleet spec: %w", err)
	}
	return spec, nil
}

// groupName is the runner group's name on GHES.
func (s fleetSpec) groupName(g fleetGroupSpec) string {
	if s.GroupPrefix == "" {
		return g.Name
	}
	return s.GroupPrefix + "-" + g.Name
}

func fleetScaleSetName(base, group, kind string) string {
	return base + "-" + group + "-" + kind
}

func (s fleetSpec) validate(scaleSetName string) error {
	if s.GroupPrefix != "" && !fleetNamePart.MatchString(s.GroupPrefix) {
		return errors.New("fleet group_prefix must be 1-16 lowercase letters, digits or hyphens")
	}
	if len(s.Groups) == 0 || len(s.Groups) > fleetMaxGroups {
		return fmt.Errorf("fleet spec must have between 1 and %d groups", fleetMaxGroups)
	}
	seen := map[string]bool{}
	for _, g := range s.Groups {
		if !fleetNamePart.MatchString(g.Name) {
			return fmt.Errorf("fleet group name %q must be 1-16 lowercase letters, digits or hyphens", g.Name)
		}
		full := s.groupName(g)
		if seen[full] {
			return fmt.Errorf("fleet group %q is listed more than once", full)
		}
		seen[full] = true
		if g.Real < 0 || g.Online < 0 || g.Offline < 0 {
			return fmt.Errorf("fleet group %s: runner counts cannot be negative", full)
		}
		if g.total() == 0 {
			return fmt.Errorf("fleet group %s must have at least one runner", full)
		}
		if g.total() > runnerGroupLimit {
			return fmt.Errorf("fleet group %s asks for %d runners; a runner group holds at most %d", full, g.total(), runnerGroupLimit)
		}
		if name := fleetScaleSetName(scaleSetName, full, scaleSetReal); len(name) > fleetScaleSetMax {
			return fmt.Errorf("generated scale set name %q is longer than %d characters; shorten scale_set_name, group_prefix or the group name", name, fleetScaleSetMax)
		}
	}
	return nil
}

// checkCapacity fails when a group cannot take its runners on top of the
// runners it already holds; existing is keyed by full group name.
func (s fleetSpec) checkCapacity(existing map[string]int) error {
	var failures []error
	for _, g := range s.Groups {
		full := s.groupName(g)
		if have := existing[full]; g.total()+have > runnerGroupLimit {
			failures = append(failures, fmt.Errorf("runner group %s already has %d runners; adding %d would exceed the %d limit by %d",
				full, have, g.total(), runnerGroupLimit, g.total()+have-runnerGroupLimit))
		}
	}
	return errors.Join(failures...)
}

type fleetBudget struct {
	Real, Online, Offline, Fake int
	SessionBytes                uint64
	FileDescriptors             uint64
	Fill, Ramp, Dispatch        time.Duration
	Removal                     time.Duration
	MinDuration                 time.Duration
}

// budget estimates host resources and the minimum --duration: fake fill at
// rate, the slowest group's real ramp, the longer of dwell and the dispatch
// window, fake removal at rate, and a margin for the last workflows.
func (s fleetSpec) budget(rate, rampRate, dispatchRate float64, dispatches int, dwell time.Duration) fleetBudget {
	var b fleetBudget
	maxReal := 0
	for _, g := range s.Groups {
		b.Real += g.Real
		b.Online += g.Online
		b.Offline += g.Offline
		maxReal = max(maxReal, g.Real)
	}
	b.Fake = b.Online + b.Offline
	b.SessionBytes = uint64(b.Online) * sessionBytesPer
	b.FileDescriptors = uint64(b.Online) + fileDescriptorBase
	b.Fill = secondsAt(b.Fake, rate)
	b.Ramp = secondsAt(maxReal, rampRate)
	if b.Real > 0 {
		b.Dispatch = secondsAt(dispatches, dispatchRate)
	}
	b.Removal = b.Fill
	b.MinDuration = b.Fill + b.Ramp + max(dwell, b.Dispatch) + b.Removal + fleetMargin
	return b
}

func secondsAt(count int, perSecond float64) time.Duration {
	if count <= 0 || perSecond <= 0 {
		return 0
	}
	return time.Duration(float64(count) / perSecond * float64(time.Second)).Round(time.Second)
}

// fleetCleanupTimeout scales the cleanup deadline with the runners left to
// remove at the bulk-removal rate.
func fleetCleanupTimeout(runners int) time.Duration {
	return max(10*time.Minute, time.Duration(1.5*float64(secondsAt(runners, holdCleanupRate)))+5*time.Minute)
}

func checkFileLimit(need uint64) error {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("read open-file limit: %w", err)
	}
	return fileLimitError(limit.Cur, need)
}

func fileLimitError(current, need uint64) error {
	if current < need {
		return fmt.Errorf("open-file limit %d is below the %d needed for the fake-online sessions; run `ulimit -n %d` first", current, need, need)
	}
	return nil
}

// fleetSetupAPI is the SDK surface fleet setup needs on top of teardown's.
type fleetSetupAPI interface {
	scaleSetAPI
	GetRunnerGroupByName(ctx context.Context, name string) (*scaleset.RunnerGroup, error)
	CreateRunnerScaleSet(ctx context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
}

// fleetGroup is one runner group of a fleet run and its scale sets on GHES.
type fleetGroup struct {
	Spec     fleetGroupSpec
	Name     string // full runner-group name on GHES
	ID       int64
	Reused   bool
	Existing int // runners already in the group before this run
	// Real and Fake are zero when the group has no runners of that kind.
	Real scaleSetRecord
	Fake scaleSetRecord
}

func (g fleetGroup) scaleSetKinds() []string {
	var kinds []string
	if g.Spec.Real > 0 {
		kinds = append(kinds, scaleSetReal)
	}
	if g.Spec.Online+g.Spec.Offline > 0 {
		kinds = append(kinds, scaleSetFake)
	}
	return kinds
}

// preflightFleet reads every named group without changing anything: it
// refuses default, inherited or inaccessible groups and leftover scale sets,
// and checks each group has room for its runners.
func preflightFleet(ctx context.Context, groups groupAPI, sets fleetSetupAPI, org string, repoID int64, spec fleetSpec, base string) ([]fleetGroup, error) {
	plan := make([]fleetGroup, 0, len(spec.Groups))
	existing := map[string]int{}
	for _, g := range spec.Groups {
		fg := fleetGroup{Spec: g, Name: spec.groupName(g)}
		live, err := groups.findRunnerGroup(ctx, org, fg.Name)
		if err != nil {
			return nil, err
		}
		if live != nil {
			if err := checkReusedGroup(ctx, groups, sets, org, repoID, base, &fg, *live); err != nil {
				return nil, err
			}
			existing[fg.Name] = fg.Existing
		}
		plan = append(plan, fg)
	}
	if err := spec.checkCapacity(existing); err != nil {
		return nil, err
	}
	return plan, nil
}

func checkReusedGroup(ctx context.Context, groups groupAPI, sets fleetSetupAPI, org string, repoID int64, base string, fg *fleetGroup, live apiRunnerGroup) error {
	switch {
	case live.Name != fg.Name:
		return fmt.Errorf("runner group %s already exists as %q; rename it or choose another group name", fg.Name, live.Name)
	case live.Default || live.Inherited:
		return fmt.Errorf("runner group %s is a default or enterprise-inherited group; choose another group name", fg.Name)
	}
	if fg.Spec.Real > 0 {
		allowed, err := groups.runnerGroupAllowsRepository(ctx, org, live, repoID)
		if err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("runner group %s does not allow the stress repository, so its real runners would never receive jobs", fg.Name)
		}
	}
	if err := checkServiceGroupID(ctx, sets, fg.Name, live.ID); err != nil {
		return err
	}
	count, err := groups.runnerGroupRunnerCount(ctx, org, live.ID)
	if err != nil {
		return err
	}
	fg.ID, fg.Reused, fg.Existing = live.ID, true, count
	for _, kind := range fg.scaleSetKinds() {
		name := fleetScaleSetName(base, fg.Name, kind)
		found, err := sets.GetRunnerScaleSet(ctx, int(live.ID), name)
		if err != nil {
			return fmt.Errorf("look up scale set %s: %w", name, err)
		}
		if found != nil {
			return fmt.Errorf("scale set %s (ID %d) already exists in runner group %s; clean up the run that created it first", name, found.ID, fg.Name)
		}
	}
	return nil
}

// checkServiceGroupID confirms the Actions service addresses the group by
// the same ID as the REST API, which scale-set calls rely on.
func checkServiceGroupID(ctx context.Context, sets fleetSetupAPI, name string, restID int64) error {
	group, err := sets.GetRunnerGroupByName(ctx, name)
	if err != nil {
		return fmt.Errorf("resolve runner group %s in the Actions service: %w", name, err)
	}
	if group == nil || int64(group.ID) != restID {
		got := 0
		if group != nil {
			got = group.ID
		}
		return fmt.Errorf("runner group %s has REST ID %d but Actions service ID %d", name, restID, got)
	}
	return nil
}

// createFleet creates missing groups and every scale set, journaling each
// before returning so cleanup can remove a partial setup. It returns the
// groups set up so far, even on error.
func createFleet(ctx context.Context, groups groupAPI, sets fleetSetupAPI, org string, repoID int64, base string, plan []fleetGroup, state *runState) ([]fleetGroup, error) {
	done := make([]fleetGroup, 0, len(plan))
	for _, fg := range plan {
		if err := ensureFleetGroup(ctx, groups, sets, org, repoID, &fg, state); err != nil {
			return done, err
		}
		for _, kind := range fg.scaleSetKinds() {
			record, err := createFleetScaleSet(ctx, sets, state, fg, base, kind)
			if err != nil {
				return append(done, fg), err
			}
			if kind == scaleSetReal {
				fg.Real = record
			} else {
				fg.Fake = record
			}
		}
		done = append(done, fg)
	}
	return done, nil
}

func ensureFleetGroup(ctx context.Context, groups groupAPI, sets fleetSetupAPI, org string, repoID int64, fg *fleetGroup, state *runState) error {
	if !fg.Reused {
		created, err := groups.createRunnerGroup(ctx, org, fg.Name, repoID)
		if err != nil {
			// The group did not exist at preflight, so one that exists now
			// is ours from an ambiguous create response.
			found, findErr := groups.findRunnerGroup(ctx, org, fg.Name)
			if findErr != nil || found == nil {
				return err
			}
			created = found
		}
		fg.ID = created.ID
	}
	record := runnerGroupRecord{ID: fg.ID, Name: fg.Name, CreatedByRun: !fg.Reused}
	if err := state.update(func(data *runData) { data.RunnerGroups = append(data.RunnerGroups, record) }); err != nil {
		if !fg.Reused {
			err = errors.Join(err, groups.deleteRunnerGroup(context.WithoutCancel(ctx), org, fg.ID))
		}
		return err
	}
	event := "runner_group_reused"
	if !fg.Reused {
		event = "runner_group_created"
	}
	if err := state.record(event, map[string]any{"runner_group": fg.Name, "runner_group_id": fg.ID, "existing_runners": fg.Existing}); err != nil {
		return err
	}
	if fg.Reused {
		return nil
	}
	return checkServiceGroupID(ctx, sets, fg.Name, fg.ID)
}

// createFleetScaleSet journals the scale set before creating it so a crash
// mid-create leaves a record cleanup can resolve by name.
func createFleetScaleSet(ctx context.Context, sets fleetSetupAPI, state *runState, fg fleetGroup, base, kind string) (scaleSetRecord, error) {
	record := scaleSetRecord{Name: fleetScaleSetName(base, fg.Name, kind), Group: fg.Name, GroupID: fg.ID, Kind: kind}
	if err := state.update(func(data *runData) { data.ScaleSets = append(data.ScaleSets, record) }); err != nil {
		return record, err
	}
	created, err := sets.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          record.Name,
		RunnerGroupID: int(fg.ID),
		Labels:        []scaleset.Label{{Name: record.Name, Type: "System"}},
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: false},
	})
	if err != nil {
		return record, fmt.Errorf("create scale set %s: %w", record.Name, err)
	}
	if created == nil || created.ID <= 0 || created.Name != record.Name {
		return record, fmt.Errorf("create scale set %s: response has no matching scale set", record.Name)
	}
	record.ID = created.ID
	if err := state.update(func(data *runData) {
		for i := range data.ScaleSets {
			if data.ScaleSets[i].ID == 0 && data.ScaleSets[i].Name == record.Name && data.ScaleSets[i].Group == record.Group {
				data.ScaleSets[i].ID = record.ID
			}
		}
	}); err != nil {
		return record, err
	}
	return record, state.record("scale_set_created", map[string]any{
		"scale_set_id": record.ID, "scale_set_name": record.Name, "runner_group": fg.Name, "kind": kind,
	})
}
