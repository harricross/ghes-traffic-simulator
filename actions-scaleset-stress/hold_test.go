package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

// fakeRunnerAPI is an in-memory runner registry for exercising hold logic.
type fakeRunnerAPI struct {
	mu          sync.Mutex
	nextID      int
	runners     map[string]int
	failJIT     func(call int) error
	jitCalls    int
	removeCalls int
	lookupErr   error
	dropOnGet   map[string]bool
	jitSets     []int         // scale set ID of each JIT call, in call order
	perSet      map[int]int   // runners created per scale set
	setLimit    map[int]int   // runner group limit per scale set
	failSet     map[int]error // every JIT call for the scale set fails
}

func newFakeRunnerAPI() *fakeRunnerAPI {
	return &fakeRunnerAPI{
		nextID: 1000, runners: map[string]int{}, dropOnGet: map[string]bool{},
		perSet: map[int]int{}, setLimit: map[int]int{}, failSet: map[int]error{},
	}
}

func (f *fakeRunnerAPI) GenerateJitRunnerConfig(_ context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jitCalls++
	f.jitSets = append(f.jitSets, scaleSetID)
	if err := f.failSet[scaleSetID]; err != nil {
		return nil, err
	}
	if limit, ok := f.setLimit[scaleSetID]; ok && f.perSet[scaleSetID] >= limit {
		// Shaped like the SDK's error for GHES's 400 response.
		return nil, fmt.Errorf("request POST https://ghes.example/_services/pipelines/SECRETtoken/_apis/runtime/runnerscalesets/%d/generatejitconfig failed: %w: RunnerGroupTooManyRunnersException: Runner group 1 has reached max limit of %d runners",
			scaleSetID, scaleset.BadRequestError, limit)
	}
	if f.failJIT != nil {
		if err := f.failJIT(f.jitCalls); err != nil {
			return nil, err
		}
	}
	f.nextID++
	f.perSet[scaleSetID]++
	f.runners[setting.Name] = f.nextID
	return &scaleset.RunnerScaleSetJitRunnerConfig{
		Runner:           &scaleset.RunnerReference{ID: f.nextID, Name: setting.Name},
		EncodedJITConfig: "jit-secret-" + setting.Name,
	}, nil
}

func (f *fakeRunnerAPI) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	if f.dropOnGet[name] {
		delete(f.runners, name)
	}
	if id, ok := f.runners[name]; ok {
		return &scaleset.RunnerReference{ID: id, Name: name}, nil
	}
	return nil, nil
}

func (f *fakeRunnerAPI) GetRunner(_ context.Context, id int) (*scaleset.RunnerReference, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, existing := range f.runners {
		if existing == id {
			return &scaleset.RunnerReference{ID: id, Name: name}, nil
		}
	}
	// The real client reports a missing ID as a 404 error, not a nil result.
	return nil, fmt.Errorf("unexpected status code: 404: %w", scaleset.NotFoundError)
}

func (f *fakeRunnerAPI) RemoveRunner(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeCalls++
	for name, existing := range f.runners {
		if int64(existing) == id {
			delete(f.runners, name)
			return nil
		}
	}
	return fmt.Errorf("unexpected status code: 404: %w", scaleset.RunnerNotFoundError)
}

func (f *fakeRunnerAPI) registered() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.runners)
}

func newTestState(t *testing.T) *runState {
	t.Helper()
	dir := t.TempDir()
	journal, err := os.OpenFile(filepath.Join(dir, "journal.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	state := &runState{dir: dir, log: journal, data: runData{SchemaVersion: 1, RunID: "hold-test", Mode: modeHold}}
	t.Cleanup(state.Close)
	return state
}

func newTestHolder(api runnerAPI, state *runState) *holder {
	h := newHolder(api, state, 7, 8, 5000, io.Discard)
	h.sampleEvery = 10 * time.Millisecond
	return h
}

func TestForEachPacedRunsAllWithinConcurrencyLimit(t *testing.T) {
	var running, peak, done atomic.Int64
	forEachPaced(context.Background(), 40, 4, 10000, func(int) {
		now := running.Add(1)
		for {
			old := peak.Load()
			if now <= old || peak.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		running.Add(-1)
		done.Add(1)
	})
	if done.Load() != 40 || peak.Load() > 4 {
		t.Fatalf("done=%d peak=%d, want 40 and <= 4", done.Load(), peak.Load())
	}
}

func TestForEachPacedStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	forEachPaced(ctx, 1000, 2, 10000, func(int) {
		if calls.Add(1) == 5 {
			cancel()
		}
	})
	if calls.Load() >= 1000 {
		t.Fatalf("calls=%d, expected an early stop", calls.Load())
	}
}

func TestHoldRegistersHoldsAndRemovesEverything(t *testing.T) {
	api := newFakeRunnerAPI()
	state := newTestState(t)
	if err := state.setLazySaves(time.Hour); err != nil {
		t.Fatal(err)
	}
	h := newTestHolder(api, state)

	reason, err := h.register(context.Background(), 60)
	if err != nil || reason != "" {
		t.Fatalf("register: reason=%q err=%v", reason, err)
	}
	if h.count() != 60 || api.registered() != 60 {
		t.Fatalf("tracked=%d registered=%d, want 60", h.count(), api.registered())
	}
	h.hold(context.Background(), 50*time.Millisecond)
	verified, missing, _ := h.checkSample(context.Background(), 20)
	if verified != 20 || missing != 0 {
		t.Fatalf("sample verified=%d missing=%d", verified, missing)
	}
	if err := h.removeAll(context.Background()); err != nil {
		t.Fatalf("removeAll: %v", err)
	}
	if api.registered() != 0 {
		t.Fatalf("%d runners left registered", api.registered())
	}
	if err := state.setLazySaves(0); err != nil {
		t.Fatal(err)
	}
	for _, runner := range state.snapshot().Runners {
		if !runner.Removed {
			t.Fatalf("runner %s not marked removed", runner.Name)
		}
	}
}

func TestHoldRecordsRunnersThatVanishDuringHold(t *testing.T) {
	api := newFakeRunnerAPI()
	state := newTestState(t)
	h := newTestHolder(api, state)
	if _, err := h.register(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	gone := runnerName("hold-test", 3)
	api.dropOnGet[gone] = true

	_, missing, _ := h.checkSample(context.Background(), 10)
	if missing != 1 {
		t.Fatalf("missing=%d, want 1", missing)
	}
	scale := state.snapshot().Scale
	if scale.HeldMissing != 1 || scale.FirstMissingAfterSeconds < 0 {
		t.Fatalf("scale=%+v", scale)
	}
	if err := h.removeAll(context.Background()); err != nil {
		t.Fatalf("removeAll: %v", err)
	}
	if api.removeCalls != 9 {
		t.Fatalf("removeCalls=%d, want 9 (the vanished runner is not removed again)", api.removeCalls)
	}
}

func TestHoldStopsGrowingAfterRepeatedFailures(t *testing.T) {
	api := newFakeRunnerAPI()
	api.failJIT = func(call int) error {
		if call > 5 {
			return errors.New("service unavailable")
		}
		return nil
	}
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.workers = 1

	reason, err := h.register(context.Background(), 500)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !strings.Contains(reason, "consecutive registration failures") {
		t.Fatalf("reason=%q", reason)
	}
	if h.count() != 5 {
		t.Fatalf("tracked=%d, want the 5 successful registrations", h.count())
	}
	if err := h.removeAll(context.Background()); err != nil || api.registered() != 0 {
		t.Fatalf("removeAll err=%v left=%d", err, api.registered())
	}
}

func TestHoldStopsAtRunnerGroupLimit(t *testing.T) {
	api := newFakeRunnerAPI()
	api.setLimit[7] = 5
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.workers = 1

	reason, err := h.register(context.Background(), 500)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !strings.Contains(reason, "runner group limit reached") || strings.Contains(reason, "SECRETtoken") {
		t.Fatalf("reason=%q, want a redacted runner group limit", reason)
	}
	if h.count() != 5 || api.jitCalls != 6 {
		t.Fatalf("tracked=%d calls=%d, want 5 runners and a stop at the first refusal", h.count(), api.jitCalls)
	}
}

func readJournalEvents(t *testing.T, state *runState, name string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(state.dir, "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var event struct {
			Event  string         `json:"event"`
			Fields map[string]any `json:"fields"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("journal line %q: %v", line, err)
		}
		if event.Event == name {
			events = append(events, event.Fields)
		}
	}
	return events
}

func TestRegisterTargetsInterleavesAndAttributesScaleSets(t *testing.T) {
	api := newFakeRunnerAPI()
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.workers = 1
	targets := []holdTarget{
		{Group: "g01", ScaleSetID: 11, Count: 3},
		{Group: "g02", ScaleSetID: 12, Count: 1},
		{Group: "g03", ScaleSetID: 13, Count: 2},
	}
	results, err := h.registerTargets(context.Background(), targets, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(api.jitSets), "[11 12 13 11 13 11]"; got != want {
		t.Fatalf("JIT calls by scale set %s, want round-robin %s", got, want)
	}
	want := map[string]int{}
	for _, target := range targets {
		if r := results[target.Group]; r.Registered != target.Count || r.Ceiling || r.Errors != 0 || r.Online != 0 {
			t.Fatalf("%s result %+v", target.Group, r)
		}
		for i := 1; i <= target.Count; i++ {
			want[fleetRunnerName("hold-test", target.Group, fleetKindFake, i)] = target.ScaleSetID
		}
	}
	got := map[string]int{}
	for _, runner := range state.snapshot().Runners {
		got[runner.Name] = runner.ScaleSetID
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("state runners %v, want %v", got, want)
	}
	events := readJournalEvents(t, state, "runner_registered")
	if len(events) != len(want) {
		t.Fatalf("%d runner_registered events, want %d", len(events), len(want))
	}
	for _, fields := range events {
		name, _ := fields["runner_name"].(string)
		group, _ := fields["runner_group"].(string)
		setID, _ := fields["scale_set_id"].(float64)
		if int(setID) != want[name] || !strings.Contains(name, "-"+group+"-f") {
			t.Fatalf("journal fields %v do not attribute the runner", fields)
		}
	}

	// As after a crash before the state was saved, the journal restores
	// each runner's scale set.
	if err := state.update(func(data *runData) { data.Runners = nil }); err != nil {
		t.Fatal(err)
	}
	if err := reconcileRunnersFromJournal(state); err != nil {
		t.Fatal(err)
	}
	got = map[string]int{}
	for _, runner := range state.snapshot().Runners {
		got[runner.Name] = runner.ScaleSetID
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reconciled runners %v, want %v", got, want)
	}
}

func TestRegisterTargetsHandsOnlyTheOnlineSubsetToCallback(t *testing.T) {
	api := newFakeRunnerAPI()
	state := newTestState(t)
	h := newTestHolder(api, state)
	targets := []holdTarget{
		{Group: "g01", ScaleSetID: 11, Count: 5, Online: 2},
		{Group: "g02", ScaleSetID: 12, Count: 3, Online: 3},
		{Group: "g03", ScaleSetID: 13, Count: 2},
	}
	var mu sync.Mutex
	handed := map[string]int{}
	results, err := h.registerTargets(context.Background(), targets, func(target holdTarget, id int, name, jit string) {
		mu.Lock()
		defer mu.Unlock()
		if id <= 0 || jit != "jit-secret-"+name || !strings.Contains(name, "-"+target.Group+"-f") {
			t.Errorf("callback got id=%d name=%s for %s", id, name, target.Group)
		}
		handed[target.Group]++
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(handed); got != "map[g01:2 g02:3]" {
		t.Fatalf("JIT configs handed out %s", got)
	}
	for _, target := range targets {
		if r := results[target.Group]; r.Registered != target.Count || r.Online != target.Online {
			t.Fatalf("%s result %+v", target.Group, r)
		}
	}
	for _, file := range []string{"state.json", "journal.jsonl"} {
		data, err := os.ReadFile(filepath.Join(state.dir, file))
		if err != nil || bytes.Contains(data, []byte("jit-secret")) {
			t.Fatalf("%s must not hold JIT configs (err=%v)", file, err)
		}
	}
}

func TestRegisterTargetsStopsOnlyTheFailingTarget(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(*fakeRunnerAPI)
		registered int
		calls      int
		groupLimit bool
		reason     string
	}{
		{"runner group limit", func(api *fakeRunnerAPI) { api.setLimit[11] = 2 }, 2, 3, true, "runner group limit reached"},
		{"persistent failures", func(api *fakeRunnerAPI) { api.failSet[11] = errors.New("service unavailable") }, 0, holdMaxConsecutiveFailures, false, "consecutive registration failures"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeRunnerAPI()
			tc.setup(api)
			state := newTestState(t)
			h := newTestHolder(api, state)
			h.workers = 1
			results, err := h.registerTargets(context.Background(), []holdTarget{
				{Group: "g01", ScaleSetID: 11, Count: 30},
				{Group: "g02", ScaleSetID: 12, Count: 30},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			stopped := results["g01"]
			if stopped.Registered != tc.registered || !stopped.Ceiling || stopped.GroupLimit != tc.groupLimit ||
				!strings.Contains(stopped.CeilingReason, tc.reason) || strings.Contains(stopped.CeilingReason, "SECRETtoken") {
				t.Fatalf("g01 result %+v", stopped)
			}
			calls := 0
			for _, id := range api.jitSets {
				if id == 11 {
					calls++
				}
			}
			if calls != tc.calls {
				t.Fatalf("%d calls to the stopped scale set, want %d", calls, tc.calls)
			}
			if other := results["g02"]; other.Registered != 30 || other.Ceiling || api.perSet[12] != 30 {
				t.Fatalf("g02 result %+v, registered %d: the other target must complete", other, api.perSet[12])
			}
			if events := readJournalEvents(t, state, "hold_target_stopped"); len(events) != 1 || events[0]["runner_group"] != "g01" {
				t.Fatalf("hold_target_stopped events %v", events)
			}
			if err := h.removeAll(context.Background()); err != nil || api.registered() != 0 {
				t.Fatalf("removeAll err=%v left=%d", err, api.registered())
			}
		})
	}
}

func TestSafeErrorRedactsPipelinesToken(t *testing.T) {
	err := errors.New(`request POST https://ghes.example/_services/pipelines/SECRETtoken123/_apis/runtime/x failed(status="400")`)
	got := safeError(err)
	if strings.Contains(got, "SECRETtoken123") || !strings.Contains(got, "/_services/pipelines/<redacted>/_apis/runtime/x") {
		t.Fatalf("safeError=%q", got)
	}
}

func TestHoldTracksRunnerCreatedByFailedRegistration(t *testing.T) {
	api := newFakeRunnerAPI()
	api.failJIT = func(call int) error {
		if call == 1 {
			api.nextID++
			api.runners[runnerName("hold-test", 1)] = api.nextID
			return errors.New("timeout after create")
		}
		return nil
	}
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.workers = 1
	if _, err := h.register(context.Background(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	if h.count() != 1 {
		t.Fatalf("tracked=%d, want the runner GHES created despite the error", h.count())
	}
	if err := h.removeAll(context.Background()); err != nil || api.registered() != 0 {
		t.Fatalf("removeAll err=%v left=%d", err, api.registered())
	}
}

func TestHoldAmbiguousRegistrationIsTrackedByNameAndRemoved(t *testing.T) {
	api := newFakeRunnerAPI()
	api.failJIT = func(call int) error { return errors.New("boom") }
	api.lookupErr = errors.New("lookup failed")
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.workers = 1
	h.register(context.Background(), 1)
	if h.count() != 1 || state.snapshot().Unresolved != 1 {
		t.Fatalf("tracked=%d unresolved=%d, want an unresolved tracked runner", h.count(), state.snapshot().Unresolved)
	}
	api.lookupErr = nil
	api.failJIT = nil
	if err := h.removeAll(context.Background()); err != nil {
		t.Fatalf("removeAll: %v", err)
	}
}

func TestRemoveRefusesRunnersNotOwnedByRun(t *testing.T) {
	api := newFakeRunnerAPI()
	api.runners["someone-elses-runner"] = 5
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.runners = []heldRunner{{id: 5, name: "someone-elses-runner"}}
	if err := h.removeAll(context.Background()); err == nil {
		t.Fatal("expected removal of a foreign runner to be refused")
	}
	if api.registered() != 1 {
		t.Fatal("foreign runner was removed")
	}
}

func TestCleanupVerifiesJournaledIDMatchesName(t *testing.T) {
	api := newFakeRunnerAPI()
	name := runnerName("hold-test", 1)
	api.runners["another-name"] = 77
	state := newTestState(t)
	h := newTestHolder(api, state)
	h.verifyOwnership = true
	h.runners = []heldRunner{{id: 77, name: name}}
	if err := h.removeAll(context.Background()); err == nil {
		t.Fatal("expected a mismatched ID/name to be refused")
	}
	if api.registered() != 1 {
		t.Fatal("runner with a mismatched name was removed")
	}
}

func TestCleanupTreatsAlreadyRemovedRunnersAsDone(t *testing.T) {
	api := newFakeRunnerAPI()
	name := runnerName("hold-test", 1)
	state := newTestState(t)
	if err := state.update(func(data *runData) { data.Runners = append(data.Runners, runnerRecord{ID: 77, Name: name}) }); err != nil {
		t.Fatal(err)
	}
	h := newTestHolder(api, state)
	h.verifyOwnership = true
	h.runners = []heldRunner{{id: 77, name: name, pos: 0}}
	if err := h.removeAll(context.Background()); err != nil {
		t.Fatalf("a runner that is already gone must count as removed: %v", err)
	}
	if !state.snapshot().Runners[0].Removed {
		t.Fatal("already-gone runner was not marked removed")
	}
}

func TestReconcileRunnersFromJournalRecoversUnsavedRegistrations(t *testing.T) {
	state := newTestState(t)
	if err := state.setLazySaves(time.Hour); err != nil {
		t.Fatal(err)
	}
	// "saved" is in state; the others only reached the journal, as after a
	// crash between lazy state saves.
	saved, withID, intentOnly := runnerName("hold-test", 1), runnerName("hold-test", 2), runnerName("hold-test", 3)
	_ = state.update(func(data *runData) { data.Runners = append(data.Runners, runnerRecord{ID: 11, Name: saved}) })
	_ = state.record("runner_registered", map[string]any{"runner_id": 11, "runner_name": saved})
	_ = state.record("runner_registration_intent", map[string]any{"runner_name": withID})
	_ = state.record("runner_registered", map[string]any{"runner_id": 12, "runner_name": withID})
	_ = state.record("runner_registration_intent", map[string]any{"runner_name": intentOnly})

	if err := reconcileRunnersFromJournal(state); err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, runner := range state.snapshot().Runners {
		got[runner.Name] = runner.ID
	}
	want := map[string]int{saved: 11, withID: 12, intentOnly: 0}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("runners=%v, want %v", got, want)
	}
	if err := reconcileRunnersFromJournal(state); err != nil || len(state.snapshot().Runners) != 3 {
		t.Fatalf("second reconcile must be idempotent: err=%v runners=%d", err, len(state.snapshot().Runners))
	}
}

func TestLazySavesBatchStateWritesButFlushOnDemand(t *testing.T) {
	state := newTestState(t)
	path := filepath.Join(state.dir, "state.json")
	if err := state.setLazySaves(time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := state.update(func(data *runData) { data.Attempts = 1 }); err != nil {
		t.Fatal(err)
	}
	// The first write is immediate (lastSave is zero); later ones are deferred.
	if err := state.update(func(data *runData) { data.Attempts = 2 }); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if !bytes.Contains(before, []byte(`"attempts":1`)) {
		t.Fatalf("expected the first save on disk, got %s", before)
	}
	if err := state.flush(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Contains(after, []byte(`"attempts":2`)) {
		t.Fatalf("flush did not persist the deferred update: %s", after)
	}
	if err := state.update(func(data *runData) { data.Attempts = 3 }); err != nil {
		t.Fatal(err)
	}
	if err := state.setLazySaves(0); err != nil {
		t.Fatal(err)
	}
	final, _ := os.ReadFile(path)
	if !bytes.Contains(final, []byte(`"attempts": 3`)) {
		t.Fatalf("disabling lazy saves must flush: %s", final)
	}
}

func TestHoldModeValidation(t *testing.T) {
	base := runOptions{
		mode: modeHold, duration: 3 * time.Hour, dwell: 2 * time.Hour, total: 50000, rate: 100,
		maxInflight: 32, maxRunners: 4, rampRate: 5, rampTimeout: time.Minute, jobsPerRun: 1,
		jobDuration: time.Second, maxLogBytes: 1024, maxOutstandingJobs: 20, runnerMemoryMiB: 2048,
		runnerCPUs: 1, drainTimeout: time.Second, workload: workloadSynthetic, runID: "ok",
	}
	if err := base.validate(); err != nil {
		t.Fatalf("valid hold options rejected: %v", err)
	}
	short := base
	short.duration = 2 * time.Hour
	if err := short.validate(); err == nil {
		t.Fatal("--duration equal to --dwell must be rejected")
	}
	other := base
	other.mode = "registrations"
	if err := other.validate(); err == nil {
		t.Fatal("a 2h dwell outside hold mode must be rejected")
	}
	for name, mutate := range map[string]func(*runOptions){
		"fake percent":  func(o *runOptions) { o.fakeJobPercent = 101 },
		"fake duration": func(o *runOptions) { o.fakeJobDuration = 11 * time.Minute },
	} {
		bad := base
		mutate(&bad)
		if err := bad.validate(); err == nil || !strings.Contains(err.Error(), "--fake-job") {
			t.Errorf("%s: validate() = %v, want a --fake-job flag error", name, err)
		}
	}
}

func TestRegisterReplacementRetriesWithFreshNameAndConsumedRunnersAreNotSampled(t *testing.T) {
	api := newFakeRunnerAPI()
	api.failJIT = func(call int) error {
		if call == 1 {
			return errors.New("runner group briefly full")
		}
		return nil
	}
	h := newTestHolder(api, newTestState(t))
	n := 0
	next := func() string { n++; return fmt.Sprintf("fake-%d", n) }
	id, name, jit, err := h.registerReplacement(context.Background(), holdTarget{Group: "g", ScaleSetID: 7, Count: 1}, next)
	if err != nil || id == 0 || name != "fake-2" || jit != "jit-secret-fake-2" {
		t.Fatalf("registerReplacement() = %d, %q, %q, %v; want fake-2 after one retry", id, name, jit, err)
	}
	if h.count() != 1 || len(h.pick(5, true)) != 1 {
		t.Fatalf("replacement not tracked: count %d", h.count())
	}
	h.consume("fake-2")
	if picks := h.pick(5, true); len(picks) != 0 {
		t.Fatalf("a consumed runner must not be sampled, got %v", picks)
	}
	if picks := h.pick(5, false); len(picks) != 1 {
		t.Fatalf("a consumed runner is still removed at teardown, got %v", picks)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := h.registerReplacement(cancelled, holdTarget{Group: "g", ScaleSetID: 7, Count: 1}, next); !errors.Is(err, context.Canceled) {
		t.Fatalf("registerReplacement after cancel = %v", err)
	}
}
