package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

func TestFleetSpecValidation(t *testing.T) {
	ok := fleetSpec{GroupPrefix: "stress", Groups: []fleetGroupSpec{
		{Name: "g01", Real: 2, Online: 100, Offline: 1000},
		{Name: "g02", Offline: 10000},
	}}
	if err := ok.validate("load"); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	cases := map[string]struct {
		spec fleetSpec
		want string
	}{
		"no groups":     {fleetSpec{}, "between 1 and 20"},
		"bad prefix":    {fleetSpec{GroupPrefix: "Stress", Groups: ok.Groups}, "group_prefix"},
		"bad name":      {fleetSpec{Groups: []fleetGroupSpec{{Name: "g 1", Real: 1}}}, "lowercase"},
		"duplicate":     {fleetSpec{Groups: []fleetGroupSpec{{Name: "a", Real: 1}, {Name: "a", Online: 1}}}, "more than once"},
		"negative":      {fleetSpec{Groups: []fleetGroupSpec{{Name: "a", Real: 2, Online: -1}}}, "negative"},
		"empty group":   {fleetSpec{Groups: []fleetGroupSpec{{Name: "a"}}}, "at least one runner"},
		"over limit":    {fleetSpec{Groups: []fleetGroupSpec{{Name: "a", Online: 5000, Offline: 5001}}}, "at most 10000"},
		"long set name": {fleetSpec{GroupPrefix: "abcdefghijklmnop", Groups: []fleetGroupSpec{{Name: "abcdefghijklmnop", Real: 1}}}, "longer than 40"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.spec.validate("load")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadFleetSpecRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.json")
	if err := os.WriteFile(path, []byte(`{"groups":[{"name":"a","real":1,"onlin":5}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFleetSpec(path); err == nil || !strings.Contains(err.Error(), "onlin") {
		t.Fatalf("loadFleetSpec() = %v, want unknown field error", err)
	}
}

func TestFleetCapacityCountsExistingRunners(t *testing.T) {
	spec := fleetSpec{GroupPrefix: "s", Groups: []fleetGroupSpec{{Name: "a", Offline: 9000}, {Name: "b", Offline: 100}}}
	if err := spec.checkCapacity(map[string]int{"s-a": 1000, "s-b": 50}); err != nil {
		t.Fatalf("exactly-full group rejected: %v", err)
	}
	err := spec.checkCapacity(map[string]int{"s-a": 1001})
	if err == nil || !strings.Contains(err.Error(), "s-a already has 1001") || !strings.Contains(err.Error(), "by 1") {
		t.Fatalf("checkCapacity() = %v, want s-a over by 1", err)
	}
}

func TestFleetBudget(t *testing.T) {
	spec := fleetSpec{Groups: []fleetGroupSpec{
		{Name: "a", Real: 10, Online: 1000, Offline: 4000},
		{Name: "b", Real: 4, Offline: 5000},
	}}
	b := spec.budget(25, 5, 1, 60, 30*time.Minute)
	if b.Real != 14 || b.Online != 1000 || b.Fake != 10000 {
		t.Fatalf("counts = %+v", b)
	}
	if b.FileDescriptors != 2024 || b.SessionBytes != 1000*sessionBytesPer {
		t.Fatalf("host budget = %d fds, %d bytes", b.FileDescriptors, b.SessionBytes)
	}
	// 10,000 fake at 25/s = 400s; slowest group ramps 10 at 5/s = 2s;
	// dwell 30m outlasts 60 dispatches at 1/s; removal mirrors the fill.
	if want := 400*time.Second + 2*time.Second + 30*time.Minute + 400*time.Second + fleetMargin; b.MinDuration != want {
		t.Fatalf("MinDuration = %s, want %s", b.MinDuration, want)
	}
	if fake := (fleetSpec{Groups: []fleetGroupSpec{{Name: "a", Offline: 1}}}).budget(25, 5, 1, 60, 0); fake.Dispatch != 0 {
		t.Fatalf("fake-only fleet budgets %s for dispatches", fake.Dispatch)
	}
}

func TestFleetCleanupTimeoutAndFileLimit(t *testing.T) {
	if got := fleetCleanupTimeout(100); got != 10*time.Minute {
		t.Fatalf("small cleanup timeout = %s", got)
	}
	// 200,000 runners at 100/s = 2000s; x1.5 + 5m = 55m.
	if got := fleetCleanupTimeout(200000); got != 55*time.Minute {
		t.Fatalf("large cleanup timeout = %s", got)
	}
	if err := fileLimitError(4096, 2024); err != nil {
		t.Fatal(err)
	}
	if err := fileLimitError(1024, 2024); err == nil || !strings.Contains(err.Error(), "ulimit -n 2024") {
		t.Fatalf("fileLimitError() = %v", err)
	}
}

func TestFleetBudgetCheck(t *testing.T) {
	cfg := config{RunnerImage: "ghcr.io/actions/actions-runner@sha256:abc"}
	o := runOptions{total: 10, maxRunners: 20, duration: time.Hour, dwell: 10 * time.Minute}
	ok := fleetBudget{Real: 20, MinDuration: time.Hour}
	if err := ok.check(o, cfg); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		b    fleetBudget
		o    runOptions
		cfg  config
		want string
	}{
		"no image":      {ok, o, config{}, "runner_image"},
		"too many runs": {ok, runOptions{total: 101, maxRunners: 20, duration: time.Hour}, cfg, "100 workflow dispatches"},
		"max runners":   {fleetBudget{Real: 21}, o, cfg, "--max-runners is 20"},
		"short":         {fleetBudget{Fake: 1, MinDuration: time.Hour + time.Second}, o, cfg, "needs at least 1h0m1s"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := tc.b.check(tc.o, tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("check() = %v, want error containing %q", err, tc.want)
			}
		})
	}
	fakeOnly := fleetBudget{Fake: 100, MinDuration: time.Minute}
	if err := fakeOnly.check(runOptions{total: 500, maxRunners: 1, duration: time.Hour}, config{}); err != nil {
		t.Fatalf("fake-only fleet needs no image or dispatch limit: %v", err)
	}
}

// fakeFleetSDK adds the Actions-service group lookup and scale-set create to
// fakeScaleSets, resolving groups from the REST fake.
type fakeFleetSDK struct {
	*fakeScaleSets
	groups     *fakeGroups
	idSkew     int
	failCreate string
	nextSet    int
}

func (f *fakeFleetSDK) GetRunnerGroupByName(ctx context.Context, name string) (*scaleset.RunnerGroup, error) {
	group, _ := f.groups.findRunnerGroup(ctx, "", name)
	if group == nil {
		return nil, fmt.Errorf("no runner group found with name %q", name)
	}
	return &scaleset.RunnerGroup{ID: int(group.ID) + f.idSkew, Name: group.Name}, nil
}

func (f *fakeFleetSDK) CreateRunnerScaleSet(_ context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error) {
	if set.Name == f.failCreate {
		return nil, errors.New("unexpected status code: 500")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSet++
	created := *set
	created.ID = 500 + f.nextSet
	f.sets[created.ID] = created
	return &created, nil
}

func newFleetSetupFakes(t *testing.T) (*runState, *fakeFleetSDK, *fakeGroups) {
	t.Helper()
	groups := &fakeGroups{groups: map[int64]apiRunnerGroup{}, runners: map[int64]int{}, repos: map[int64][]int64{}, nextID: 20}
	sdk := &fakeFleetSDK{fakeScaleSets: &fakeScaleSets{sets: map[int]scaleset.RunnerScaleSet{}}, groups: groups}
	state := newTestState(t)
	state.data.SchemaVersion = stateSchemaVersion
	return state, sdk, groups
}

func TestFleetSetupThenTeardownRemovesOnlyRunObjects(t *testing.T) {
	ctx := context.Background()
	state, sdk, groups := newFleetSetupFakes(t)
	groups.groups[7] = apiRunnerGroup{ID: 7, Name: "s-b", Visibility: "all"}
	groups.runners[7] = 100
	spec := fleetSpec{GroupPrefix: "s", Groups: []fleetGroupSpec{
		{Name: "a", Real: 2, Online: 1, Offline: 1},
		{Name: "b", Offline: 5},
	}}
	plan, err := preflightFleet(ctx, groups, sdk, "org", 42, spec, "load")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups.created) != 0 || len(sdk.sets) != 0 {
		t.Fatal("preflight changed GHES")
	}
	groups.onCreate = func(name string) {
		// The intent must be journaled before the group exists on GHES.
		if got := state.snapshot().RunnerGroups; len(got) != 1 || got[0].Name != name || got[0].ID != 0 || !got[0].CreatedByRun {
			t.Errorf("journaled groups before creating %s = %+v", name, got)
		}
	}
	fleet, err := createFleet(ctx, groups, sdk, "org", 42, "load", plan, state)
	if err != nil {
		t.Fatal(err)
	}
	a, b := fleet[0], fleet[1]
	if a.Reused || a.ID != 21 || a.Real.Name != "load-s-a-real" || a.Fake.Name != "load-s-a-fake" || a.Real.ID == 0 || a.Fake.ID == 0 {
		t.Fatalf("group a = %+v", a)
	}
	if !b.Reused || b.Existing != 100 || b.Real.ID != 0 || b.Fake.Name != "load-s-b-fake" {
		t.Fatalf("group b = %+v", b)
	}
	if !slices.Equal(groups.repos[21], []int64{42}) {
		t.Fatalf("created group repositories = %v", groups.repos[21])
	}
	snapshot := state.snapshot()
	if len(snapshot.RunnerGroups) != 2 || !snapshot.RunnerGroups[0].CreatedByRun || snapshot.RunnerGroups[1].CreatedByRun {
		t.Fatalf("journaled groups = %+v", snapshot.RunnerGroups)
	}
	for _, set := range snapshot.ScaleSets {
		if set.ID == 0 {
			t.Fatalf("scale set %s journaled without ID", set.Name)
		}
	}
	if got := journalEvents(t, state); !slices.Equal(got, []string{
		"runner_group_created", "scale_set_created", "scale_set_created", "runner_group_reused", "scale_set_created",
	}) {
		t.Fatalf("journal = %v", got)
	}
	if err := teardownFleet(ctx, sdk, groups, "org", state); err != nil {
		t.Fatal(err)
	}
	if len(sdk.sets) != 0 || !slices.Equal(groups.deleted, []int64{21}) {
		t.Fatalf("teardown left sets %v, deleted groups %v", sdk.sets, groups.deleted)
	}
}

func TestFleetPreflightRefusals(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		live     apiRunnerGroup
		repos    []int64
		count    int
		leftover bool
		skew     int
		group    fleetGroupSpec
		want     string
	}{
		"default":        {live: apiRunnerGroup{Name: "s-a", Default: true}, group: fleetGroupSpec{Name: "a", Offline: 1}, want: "default"},
		"inherited":      {live: apiRunnerGroup{Name: "s-a", Inherited: true}, group: fleetGroupSpec{Name: "a", Offline: 1}, want: "inherited"},
		"case mismatch":  {live: apiRunnerGroup{Name: "S-A"}, group: fleetGroupSpec{Name: "a", Offline: 1}, want: `exists as "S-A"`},
		"no repo access": {live: apiRunnerGroup{Name: "s-a", Visibility: "selected"}, repos: []int64{9}, group: fleetGroupSpec{Name: "a", Real: 1}, want: "does not allow"},
		"id mismatch":    {live: apiRunnerGroup{Name: "s-a", Visibility: "all"}, skew: 1, group: fleetGroupSpec{Name: "a", Real: 1}, want: "Actions service ID"},
		"full":           {live: apiRunnerGroup{Name: "s-a", Visibility: "all"}, count: 9999, group: fleetGroupSpec{Name: "a", Online: 2}, want: "exceed the 10000 limit by 1"},
		"leftover set":   {live: apiRunnerGroup{Name: "s-a", Visibility: "all"}, leftover: true, group: fleetGroupSpec{Name: "a", Offline: 1}, want: "already exists"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, sdk, groups := newFleetSetupFakes(t)
			tc.live.ID = 3
			groups.groups[3] = tc.live
			groups.repos[3] = tc.repos
			groups.runners[3] = tc.count
			sdk.idSkew = tc.skew
			if tc.leftover {
				sdk.sets[9] = scaleset.RunnerScaleSet{ID: 9, Name: "load-s-a-fake", RunnerGroupID: 3}
			}
			spec := fleetSpec{GroupPrefix: "s", Groups: []fleetGroupSpec{tc.group}}
			_, err := preflightFleet(ctx, groups, sdk, "org", 42, spec, "load")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("preflightFleet() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestFleetFailedScaleSetCreateIsRecoverable(t *testing.T) {
	ctx := context.Background()
	state, sdk, groups := newFleetSetupFakes(t)
	sdk.failCreate = "load-s-a-fake"
	spec := fleetSpec{GroupPrefix: "s", Groups: []fleetGroupSpec{{Name: "a", Real: 1, Offline: 1}}}
	plan, err := preflightFleet(ctx, groups, sdk, "org", 42, spec, "load")
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := createFleet(ctx, groups, sdk, "org", 42, "load", plan, state)
	if err == nil || len(fleet) != 1 || fleet[0].Real.ID == 0 {
		t.Fatalf("createFleet() = %+v, %v; want partial fleet and error", fleet, err)
	}
	sets := state.snapshot().ScaleSets
	if len(sets) != 2 || sets[1].Name != "load-s-a-fake" || sets[1].ID != 0 {
		t.Fatalf("journaled scale sets = %+v", sets)
	}
	if err := teardownFleet(ctx, sdk, groups, "org", state); err != nil {
		t.Fatal(err)
	}
	if len(sdk.sets) != 0 || len(groups.groups) != 0 {
		t.Fatalf("teardown left sets %v, groups %v", sdk.sets, groups.groups)
	}
}
