package main

import (
	"bytes"
	"context"
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
}

func newFakeRunnerAPI() *fakeRunnerAPI {
	return &fakeRunnerAPI{nextID: 1000, runners: map[string]int{}, dropOnGet: map[string]bool{}}
}

func (f *fakeRunnerAPI) GenerateJitRunnerConfig(_ context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jitCalls++
	if f.failJIT != nil {
		if err := f.failJIT(f.jitCalls); err != nil {
			return nil, err
		}
	}
	f.nextID++
	f.runners[setting.Name] = f.nextID
	return &scaleset.RunnerScaleSetJitRunnerConfig{Runner: &scaleset.RunnerReference{ID: f.nextID, Name: setting.Name}}, nil
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
}
