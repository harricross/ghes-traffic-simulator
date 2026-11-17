package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"
	"time"

	"github.com/actions/scaleset"
)

// fleetStatusInterval is how often a fleet run samples every group.
const fleetStatusInterval = time.Minute

// fleetSample is one group's status at one instant. A nil statistic or a
// negative GroupRunners means that read failed; Err holds the first failure.
type fleetSample struct {
	Real, Fake   *scaleset.RunnerScaleSetStatistic
	GroupRunners int
	Sessions     sessionGroupStats
	Err          error
}

func newFleetResults(groups []fleetGroup) []fleetGroupResult {
	results := make([]fleetGroupResult, len(groups))
	for i, g := range groups {
		results[i] = fleetGroupResult{
			Name: g.Name, ID: g.ID, Created: !g.Reused, ExistingRunners: g.Existing,
			Real: g.Spec.Real, Online: g.Spec.Online, Offline: g.Spec.Offline,
		}
	}
	return results
}

func sampleFleetGroup(ctx context.Context, sets scaleSetAPI, groups groupAPI, org string, g fleetGroup, sessions sessionGroupStats) fleetSample {
	sample := fleetSample{GroupRunners: -1, Sessions: sessions}
	note := func(err error) {
		if sample.Err == nil {
			sample.Err = err
		}
	}
	read := func(set scaleSetRecord) *scaleset.RunnerScaleSetStatistic {
		if set.ID == 0 {
			return nil
		}
		got, err := sets.GetRunnerScaleSetByID(ctx, set.ID)
		switch {
		case err != nil:
			note(fmt.Errorf("scale set %s statistics: %w", set.Name, err))
		case got == nil || got.Statistics == nil:
			note(fmt.Errorf("scale set %s returned no statistics", set.Name))
		default:
			return got.Statistics
		}
		return nil
	}
	sample.Real = read(g.Real)
	sample.Fake = read(g.Fake)
	if count, err := groups.runnerGroupRunnerCount(ctx, org, g.ID); err != nil {
		note(fmt.Errorf("runner group %s runner count: %w", g.Name, err))
	} else {
		sample.GroupRunners = count
	}
	return sample
}

// note folds a sample into the group's result: peaks for counts GHES
// reports, and the session manager's cumulative counters as of now.
func (r *fleetGroupResult) note(s fleetSample) {
	if s.Real != nil {
		r.PeakRealRegistered = max(r.PeakRealRegistered, s.Real.TotalRegisteredRunners)
		r.PeakRealBusy = max(r.PeakRealBusy, s.Real.TotalBusyRunners)
	}
	if s.Fake != nil {
		r.PeakFakeRegistered = max(r.PeakFakeRegistered, s.Fake.TotalRegisteredRunners)
		r.PeakFakeIdle = max(r.PeakFakeIdle, s.Fake.TotalIdleRunners)
	}
	r.PeakGroupRunners = max(r.PeakGroupRunners, s.GroupRunners)
	r.PeakSessionsOpen = max(r.PeakSessionsOpen, s.Sessions.Open)
	r.SessionOpenFailed = s.Sessions.OpenFailed
	r.Polls = s.Sessions.Polls
	r.PollErrors = s.Sessions.PollErrors
	r.UnexpectedMessages = s.Sessions.Unexpected
	if len(s.Sessions.UnexpectedTypes) > 0 {
		r.UnexpectedTypes = maps.Clone(s.Sessions.UnexpectedTypes)
	}
	if s.Sessions.LastError != "" {
		r.LastError = s.Sessions.LastError
	}
	if s.Err != nil {
		r.LastError = safeError(s.Err)
	}
}

func formatFleetSample(g fleetGroup, s fleetSample) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s:", g.Name)
	stat := func(kind string, set scaleSetRecord, st *scaleset.RunnerScaleSetStatistic) {
		switch {
		case set.ID == 0:
		case st == nil:
			fmt.Fprintf(&b, " %s unavailable;", kind)
		default:
			fmt.Fprintf(&b, " %s registered %d idle %d busy %d;", kind, st.TotalRegisteredRunners, st.TotalIdleRunners, st.TotalBusyRunners)
		}
	}
	stat("real", g.Real, s.Real)
	stat("fake", g.Fake, s.Fake)
	if s.GroupRunners >= 0 {
		fmt.Fprintf(&b, " group runners %d;", s.GroupRunners)
	} else {
		b.WriteString(" group runners unavailable;")
	}
	if g.Spec.Online > 0 {
		fmt.Fprintf(&b, " sessions open %d/%d (failed %d), polls %d, poll errors %d, unexpected messages %d",
			s.Sessions.Open, g.Spec.Online, s.Sessions.OpenFailed, s.Sessions.Polls, s.Sessions.PollErrors, s.Sessions.Unexpected)
	}
	return strings.TrimSuffix(b.String(), ";")
}

// recordFleetStatus samples every group once, prints one line per group,
// journals each sample and updates the per-group results.
func recordFleetStatus(ctx context.Context, sets scaleSetAPI, groups groupAPI, org string, fleet []fleetGroup,
	sessions map[string]sessionGroupStats, state *runState, phase string, out io.Writer) error {
	for _, g := range fleet {
		s := sampleFleetGroup(ctx, sets, groups, org, g, sessions[g.Name])
		fmt.Fprintf(out, "fleet[%s] %s\n", phase, formatFleetSample(g, s))
		fields := map[string]any{
			"phase": phase, "runner_group": g.Name, "group_runners": s.GroupRunners,
			"sessions_open": s.Sessions.Open, "polls": s.Sessions.Polls,
			"poll_errors": s.Sessions.PollErrors, "unexpected_messages": s.Sessions.Unexpected,
		}
		for kind, st := range map[string]*scaleset.RunnerScaleSetStatistic{scaleSetReal: s.Real, scaleSetFake: s.Fake} {
			if st != nil {
				fields[kind+"_registered"] = st.TotalRegisteredRunners
				fields[kind+"_idle"] = st.TotalIdleRunners
				fields[kind+"_busy"] = st.TotalBusyRunners
			}
		}
		if s.Err != nil {
			fields["error"] = safeError(s.Err)
		}
		if err := state.record("fleet_group_status", fields); err != nil {
			return err
		}
		if err := state.update(func(data *runData) {
			if r := fleetResult(data, g.Name); r != nil {
				r.note(s)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func fleetResult(data *runData, name string) *fleetGroupResult {
	for i := range data.FleetGroups {
		if data.FleetGroups[i].Name == name {
			return &data.FleetGroups[i]
		}
	}
	return nil
}

// tallyFleetWorkflows recomputes each group's workflow counts from the
// journaled workflow records.
func tallyFleetWorkflows(data *runData) {
	for i := range data.FleetGroups {
		r := &data.FleetGroups[i]
		r.Dispatched, r.Passed, r.IntentionalFails, r.Failed, r.Unresolved = 0, 0, 0, 0, 0
	}
	for _, w := range data.Workflows {
		r := fleetResult(data, w.Group)
		if r == nil {
			continue
		}
		r.Dispatched++
		switch w.Outcome {
		case workflowPassed:
			r.Passed++
		case workflowIntentionalFailure:
			r.IntentionalFails++
		case workflowFailed:
			r.Failed++
		default:
			r.Unresolved++
		}
	}
}

func printFleetSummary(w io.Writer, results []fleetGroupResult) {
	for _, r := range results {
		origin := "reused"
		if r.Created {
			origin = "created"
		}
		fmt.Fprintf(w, "%s (id %d, %s): real %d/%d, busy peak %d; online %d/%d idle (sessions peak %d); fake registered %d/%d",
			r.Name, r.ID, origin, r.PeakRealRegistered, r.Real, r.PeakRealBusy,
			r.PeakFakeIdle, r.Online, r.PeakSessionsOpen, r.FakeRegistered, r.Online+r.Offline)
		if r.Dispatched > 0 {
			fmt.Fprintf(w, "; jobs %d dispatched, %d passed, %d intentional failures, %d failed, %d unresolved",
				r.Dispatched, r.Passed, r.IntentionalFails, r.Failed, r.Unresolved)
		}
		fmt.Fprintf(w, "; unexpected messages %d", r.UnexpectedMessages)
		if r.CeilingReason != "" {
			fmt.Fprintf(w, "; stopped growing: %s", r.CeilingReason)
		}
		fmt.Fprintln(w)
	}
}
