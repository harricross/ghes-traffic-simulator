package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
)

// modeHold registers many JIT runners, holds them without starting any
// runner process, samples that they remain registered, then removes them all.
// The runners are registered-only: they take up rows in GHES but never go
// online.
const modeHold = "hold"

const (
	holdSaveInterval           = 2 * time.Second
	holdSampleInterval         = time.Minute
	holdSampleSize             = 50
	holdFinalSampleSize        = 500
	holdVerifyRemovedSize      = 200
	holdProgressInterval       = 30 * time.Second
	holdMaxConsecutiveFailures = 20
	holdMissingJournalLimit    = 50
	holdRemoveAttempts         = 3
	holdReplaceAttempts        = 3
	holdCallTimeout            = 30 * time.Second
	holdSampleWorkers          = 8
	holdCleanupWorkers         = 16
	holdCleanupRate            = 100.0
)

// runnerAPI is the subset of the scale-set client used to hold runners.
type runnerAPI interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(ctx context.Context, name string) (*scaleset.RunnerReference, error)
	GetRunner(ctx context.Context, runnerID int) (*scaleset.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

type heldRunner struct {
	id      int
	name    string
	pos     int // index into runData.Runners
	created time.Time
	gone    bool
	// consumed marks a fake runner that ran a job; GHES deletes such
	// ephemeral runners itself.
	consumed bool
}

// fatalHoldError marks failures of the run's own bookkeeping, which must stop
// registration rather than count as an API failure.
type fatalHoldError struct{ err error }

func (e fatalHoldError) Error() string { return e.err.Error() }
func (e fatalHoldError) Unwrap() error { return e.err }

type holder struct {
	api         runnerAPI
	state       *runState
	scaleSetID  int
	namePrefix  string
	workers     int
	rate        float64
	sampleEvery time.Duration
	out         io.Writer
	// verifyOwnership confirms a journaled runner ID still has the journaled
	// name before removing it; set when IDs come from a state file rather
	// than from this process's own registrations.
	verifyOwnership bool

	mu      sync.Mutex
	runners []heldRunner
	started time.Time
	missing int

	// replaceMu paces replacement registrations at rate.
	replaceMu   sync.Mutex
	nextReplace time.Time
}

func newHolder(api runnerAPI, state *runState, scaleSetID, workers int, rate float64, out io.Writer) *holder {
	return &holder{
		api:         api,
		state:       state,
		scaleSetID:  scaleSetID,
		namePrefix:  "load-" + shortHash(state.data.RunID) + "-",
		workers:     workers,
		rate:        rate,
		sampleEvery: holdSampleInterval,
		out:         out,
		started:     time.Now(),
	}
}

func runRegistrationHold(ctx context.Context, t *target, state *runState, options runOptions) (err error) {
	scaleSet, err := createScaleSet(ctx, t, state)
	if err != nil {
		return err
	}
	if err := state.setLazySaves(holdSaveInterval); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, state.setLazySaves(0)) }()

	h := newHolder(t.scaleSet, state, scaleSet.ID, options.maxInflight, options.rate, os.Stdout)
	stopReason, regErr := h.register(ctx, options.total)
	held := h.count()
	updateErr := state.update(func(data *runData) {
		data.Scale.PeakHeld = held
		if stopReason != "" {
			data.Scale.CeilingRunners = held
			data.Scale.CeilingReason = stopReason
		}
	})
	_ = state.record("hold_registration_finished", map[string]any{"held": held, "stop_reason": stopReason})

	var removeErr error
	if regErr == nil && updateErr == nil {
		if held == 0 && stopReason != "" {
			regErr = errors.New(stopReason)
		} else {
			h.hold(ctx, options.dwell)
			h.checkSample(ctx, holdFinalSampleSize)
		}
	}
	removeErr = h.removeAll(ctx)
	return errors.Join(regErr, updateErr, removeErr)
}

// forEachPaced runs fn for 0..n-1 with at most workers calls in flight and
// at most rate starts per second, stopping early when ctx ends.
func forEachPaced(ctx context.Context, n, workers int, rate float64, fn func(int)) {
	interval := time.Duration(float64(time.Second) / rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	work := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				fn(i)
			}
		}()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
feed:
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			break feed
		case <-ticker.C:
		}
		select {
		case work <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
}

func (h *holder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.runners)
}

// register creates total runners in the holder's scale set. It stops
// growing, without failing, at the runner group limit or after repeated API
// errors and reports why; only bookkeeping failures are errors.
func (h *holder) register(ctx context.Context, total int) (string, error) {
	results, err := h.registerTargets(ctx, []holdTarget{{ScaleSetID: h.scaleSetID, Count: total}}, nil)
	return results[""].CeilingReason, err
}

// holdTarget is one scale set to fill with registered runners.
type holdTarget struct {
	Group      string // "" means legacy single-scale-set hold naming
	ScaleSetID int
	Count      int // runners to register into this scale set
	Online     int // the first Online successful registrations are handed to onJIT
}

// holdTargetResult reports one target. Ceiling is set when the target
// stopped early; GroupLimit when GHES reported its runner group full.
type holdTargetResult struct {
	Registered, Online, Errors int
	Ceiling                    bool
	CeilingReason              string
	GroupLimit                 bool
}

type holdTargetRun struct {
	target holdTarget
	ctx    context.Context
	cancel context.CancelFunc
	next   int // last index handed out; only the feeder uses it

	registered, online, failed, consecutive atomic.Int64

	mu         sync.Mutex
	reason     string
	groupLimit bool
}

// stop ends registration for this target only, unless the whole run is
// already ending. It reports whether this call stopped the target.
func (r *holdTargetRun) stop(parent context.Context, reason string, groupLimit bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reason != "" || parent.Err() != nil {
		return false
	}
	r.reason, r.groupLimit = reason, groupLimit
	r.cancel()
	return true
}

func (r *holdTargetRun) claimOnline() bool {
	for {
		n := r.online.Load()
		if n >= int64(r.target.Online) {
			return false
		}
		if r.online.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (r *holdTargetRun) result() holdTargetResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return holdTargetResult{
		Registered: int(r.registered.Load()), Online: int(r.online.Load()), Errors: int(r.failed.Load()),
		Ceiling: r.reason != "", CeilingReason: r.reason, GroupLimit: r.groupLimit,
	}
}

// isRunnerGroupFull reports GHES refusing a registration because the runner
// group is at its runner limit. The SDK does not export the exception type.
func isRunnerGroupFull(err error) bool {
	return err != nil && strings.Contains(err.Error(), "RunnerGroupTooManyRunnersException")
}

func validateHoldTargets(targets []holdTarget) error {
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if _, dup := seen[target.Group]; dup {
			return fmt.Errorf("duplicate hold target %q", target.Group)
		}
		seen[target.Group] = struct{}{}
		if target.ScaleSetID <= 0 || target.Count < 0 || target.Online < 0 {
			return fmt.Errorf("invalid hold target %q: scale set %d, count %d, online %d", target.Group, target.ScaleSetID, target.Count, target.Online)
		}
	}
	return nil
}

// registerTargets registers each target's Count runners into its scale set,
// interleaving targets round-robin under the holder's global rate and worker
// limits so they fill evenly. A runner group limit or repeated failures stop
// only that target. The first Online registrations per target are passed to
// onJIT; every other JIT config is discarded at once and none is stored.
// Results are keyed by target Group.
func (h *holder) registerTargets(ctx context.Context, targets []holdTarget,
	onJIT func(target holdTarget, runnerID int, runnerName string, encodedJIT string)) (map[string]holdTargetResult, error) {
	if err := validateHoldTargets(targets); err != nil {
		return nil, err
	}
	regCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runs := make([]*holdTargetRun, len(targets))
	total := 0
	for i, target := range targets {
		runCtx, runCancel := context.WithCancel(regCtx)
		defer runCancel()
		runs[i] = &holdTargetRun{target: target, ctx: runCtx, cancel: runCancel}
		total += target.Count
	}

	var fatalMu sync.Mutex
	var fatal error
	stopAll := func(err error) {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		if fatal == nil {
			fatal = err
			cancel()
		}
	}
	var failures atomic.Int64
	progressCtx, stopProgress := context.WithCancel(ctx)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		h.reportProgress(progressCtx, "registering", total, &failures)
	}()

	runID := h.state.data.RunID
	forEachTargetPaced(regCtx, runs, h.workers, h.rate, func(run *holdTargetRun, index int) {
		if run.ctx.Err() != nil {
			return
		}
		target := run.target
		name := runnerName(runID, index)
		if target.Group != "" {
			name = fleetRunnerName(runID, target.Group, fleetKindFake, index)
		}
		id, encodedJIT, err := h.registerOne(run.ctx, target, name)
		if id > 0 {
			run.registered.Add(1)
		}
		var fatalErr fatalHoldError
		switch {
		case err == nil:
			run.consecutive.Store(0)
			if onJIT != nil && run.claimOnline() {
				onJIT(target, id, name, encodedJIT)
			}
		case errors.As(err, &fatalErr):
			stopAll(fatalErr.err)
		case ctx.Err() != nil:
		case run.ctx.Err() != nil:
			// The target stopped while this call was in flight.
		default:
			failures.Add(1)
			run.failed.Add(1)
			var reason string
			groupLimit := isRunnerGroupFull(err)
			switch {
			case groupLimit:
				reason = "runner group limit reached: " + safeError(err)
			case run.consecutive.Add(1) >= holdMaxConsecutiveFailures:
				reason = fmt.Sprintf("%d consecutive registration failures; last: %s", holdMaxConsecutiveFailures, safeError(err))
			default:
				return
			}
			if run.stop(ctx, reason, groupLimit) && target.Group != "" {
				_ = h.state.record("hold_target_stopped", map[string]any{
					"runner_group": target.Group, "scale_set_id": target.ScaleSetID,
					"registered": run.registered.Load(), "group_limit": groupLimit, "reason": reason,
				})
			}
		}
	})
	stopProgress()
	<-progressDone

	results := make(map[string]holdTargetResult, len(runs))
	for _, run := range runs {
		results[run.target.Group] = run.result()
	}
	fatalMu.Lock()
	defer fatalMu.Unlock()
	return results, fatal
}

// forEachTargetPaced hands out 1-based per-target indexes round-robin across
// targets that are neither exhausted nor stopped, with at most workers calls
// in flight and at most rate starts per second, until ctx ends.
func forEachTargetPaced(ctx context.Context, runs []*holdTargetRun, workers int, rate float64, fn func(*holdTargetRun, int)) {
	type slot struct {
		run   *holdTargetRun
		index int
	}
	cursor := 0
	next := func() (slot, bool) {
		for range runs {
			run := runs[cursor]
			cursor = (cursor + 1) % len(runs)
			if run.next < run.target.Count && run.ctx.Err() == nil {
				run.next++
				return slot{run, run.next}, true
			}
		}
		return slot{}, false
	}
	interval := time.Duration(float64(time.Second) / rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	work := make(chan slot)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range work {
				fn(s.run, s.index)
			}
		}()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
feed:
	for {
		s, ok := next()
		if !ok {
			break
		}
		select {
		case <-ctx.Done():
			break feed
		case <-ticker.C:
		}
		select {
		case work <- s:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
}

// holdRunnerFields are the journal fields that attribute a runner to its
// scale set and, in fleet mode, its runner group.
func holdRunnerFields(target holdTarget, name string, extra map[string]any) map[string]any {
	fields := map[string]any{"runner_name": name, "scale_set_id": target.ScaleSetID}
	if target.Group != "" {
		fields["runner_group"] = target.Group
	}
	for k, v := range extra {
		fields[k] = v
	}
	return fields
}

// registerOne registers name into the target's scale set and returns the
// runner ID (also when a failed call is reconciled to an existing runner)
// and, on success only, the encoded JIT config.
func (h *holder) registerOne(ctx context.Context, target holdTarget, name string) (int, string, error) {
	if err := h.state.record("runner_registration_intent", holdRunnerFields(target, name, nil)); err != nil {
		return 0, "", fatalHoldError{err}
	}
	callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
	defer cancel()
	jit, err := h.api.GenerateJitRunnerConfig(callCtx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       name,
		WorkFolder: "_work",
	}, target.ScaleSetID)
	if err != nil {
		return h.reconcileRegistration(ctx, target, name, err)
	}
	if jit == nil || jit.Runner == nil || jit.Runner.ID <= 0 {
		return h.reconcileRegistration(ctx, target, name, errors.New("registration returned no runner identity"))
	}
	id := jit.Runner.ID
	if err := h.track(id, name, target.ScaleSetID); err != nil {
		return id, "", fatalHoldError{err}
	}
	if err := h.state.record("runner_registered", holdRunnerFields(target, name, map[string]any{"runner_id": id})); err != nil {
		return id, "", fatalHoldError{err}
	}
	return id, jit.EncodedJITConfig, nil
}

// reconcileRegistration resolves a failed registration by name so a runner
// GHES created is still tracked and removed. It returns the runner ID when
// one was found.
func (h *holder) reconcileRegistration(ctx context.Context, target holdTarget, name string, cause error) (int, string, error) {
	lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), holdCallTimeout)
	defer cancel()
	observed, lookupErr := h.api.GetRunnerByName(lookupCtx, name)
	switch {
	case lookupErr == nil && observed != nil:
		if err := h.track(observed.ID, name, target.ScaleSetID); err != nil {
			return observed.ID, "", fatalHoldError{err}
		}
		if err := h.state.record("runner_registered", holdRunnerFields(target, name, map[string]any{"runner_id": observed.ID, "reconciled": true})); err != nil {
			return observed.ID, "", fatalHoldError{err}
		}
		return observed.ID, "", fmt.Errorf("register %s: %w (runner exists and is tracked)", name, cause)
	case lookupErr == nil:
		return 0, "", fmt.Errorf("register %s: %w", name, cause)
	default:
		if err := h.track(0, name, target.ScaleSetID); err != nil {
			return 0, "", fatalHoldError{err}
		}
		if err := h.state.update(func(data *runData) { data.Unresolved++ }); err != nil {
			return 0, "", fatalHoldError{err}
		}
		if err := h.state.record("runner_registration_unresolved", holdRunnerFields(target, name, nil)); err != nil {
			return 0, "", fatalHoldError{err}
		}
		return 0, "", errors.Join(fmt.Errorf("register %s: %w", name, cause), lookupErr)
	}
}

// consume marks the named runner as having run a fake job, so sampling no
// longer expects it. Removal still tries it and tolerates its absence.
func (h *holder) consume(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.runners) - 1; i >= 0; i-- {
		if h.runners[i].name == name {
			h.runners[i].consumed = true
			return
		}
	}
}

// waitReplaceSlot spaces replacement registrations 1/rate apart.
func (h *holder) waitReplaceSlot(ctx context.Context) error {
	interval := time.Duration(float64(time.Second) / h.rate)
	h.replaceMu.Lock()
	slot := h.nextReplace
	if now := time.Now(); slot.Before(now) {
		slot = now
	}
	h.nextReplace = slot.Add(interval)
	h.replaceMu.Unlock()
	if wait := time.Until(slot); wait > 0 {
		sleepCtx(ctx, wait)
	}
	return ctx.Err()
}

// registerReplacement registers one runner into target, taking a fresh name
// from nextName for each of up to holdReplaceAttempts attempts, and returns
// its ID, name and encoded JIT config.
func (h *holder) registerReplacement(ctx context.Context, target holdTarget, nextName func() string) (int, string, string, error) {
	var last error
	for attempt := 0; attempt < holdReplaceAttempts; attempt++ {
		if attempt > 0 {
			// GHES may not have deleted the finished runner yet, leaving
			// the group briefly full.
			sleepCtx(ctx, time.Duration(attempt)*2*time.Second)
		}
		if err := h.waitReplaceSlot(ctx); err != nil {
			return 0, "", "", err
		}
		name := nextName()
		id, encodedJIT, err := h.registerOne(ctx, target, name)
		var fatalErr fatalHoldError
		switch {
		case err == nil:
			return id, name, encodedJIT, nil
		case errors.As(err, &fatalErr):
			return 0, "", "", err
		case ctx.Err() != nil:
			return 0, "", "", ctx.Err()
		}
		last = err
	}
	return 0, "", "", fmt.Errorf("%d replacement registrations failed; last: %s", holdReplaceAttempts, safeError(last))
}

func (h *holder) track(id int, name string, scaleSetID int) error {
	now := time.Now().UTC()
	pos := -1
	err := h.state.update(func(data *runData) {
		pos = len(data.Runners)
		data.Runners = append(data.Runners, runnerRecord{ID: id, Name: name, ScaleSetID: scaleSetID, CreatedAt: now})
	})
	h.mu.Lock()
	h.runners = append(h.runners, heldRunner{id: id, name: name, pos: pos, created: now})
	h.mu.Unlock()
	return err
}

// hold waits for the dwell, sampling that registrations are still present.
func (h *holder) hold(ctx context.Context, dwell time.Duration) {
	if dwell <= 0 {
		return
	}
	deadline := time.NewTimer(dwell)
	defer deadline.Stop()
	sample := time.NewTicker(h.sampleEvery)
	defer sample.Stop()
	progress := time.NewTicker(holdProgressInterval)
	defer progress.Stop()
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-sample.C:
			h.checkSample(ctx, holdSampleSize)
		case <-progress.C:
			h.flushState()
			elapsed := time.Since(start).Round(time.Second)
			fmt.Fprintf(h.out, "%s holding=%d elapsed=%s remaining=%s missing_so_far=%d\n",
				time.Now().Format("15:04:05"), h.count(), elapsed, (dwell - elapsed).Round(time.Second), h.missingCount())
		}
	}
}

// flushState persists batched state so a long dwell does not leave
// state.json stale; the journal remains the source of truth for recovery.
func (h *holder) flushState() {
	if err := h.state.flush(); err != nil {
		fmt.Fprintf(h.out, "%s state flush failed: %v\n", time.Now().Format("15:04:05"), err)
	}
}

func (h *holder) missingCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.missing
}

// pick returns up to n distinct random runners that are not known gone.
// With onlyPresent it also skips consumed runners, which GHES deletes.
func (h *holder) pick(n int, onlyPresent bool) []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	total := len(h.runners)
	if total == 0 {
		return nil
	}
	present := func(i int) bool { return !onlyPresent || (!h.runners[i].gone && !h.runners[i].consumed) }
	chosen := make(map[int]struct{}, n)
	if total <= n {
		for i := range h.runners {
			if present(i) {
				chosen[i] = struct{}{}
			}
		}
	} else {
		for attempts := 0; len(chosen) < n && attempts < n*4; attempts++ {
			i := rand.IntN(total)
			if present(i) {
				chosen[i] = struct{}{}
			}
		}
	}
	picks := make([]int, 0, len(chosen))
	for i := range chosen {
		picks = append(picks, i)
	}
	return picks
}

// checkSample verifies a random sample is still registered, marking vanished
// runners so they are not removed again. It returns verified, missing and
// inconclusive counts.
func (h *holder) checkSample(ctx context.Context, n int) (verified, missing, inconclusive int) {
	picks := h.pick(n, true)
	var v, m, e atomic.Int64
	forEachPaced(ctx, len(picks), holdSampleWorkers, 1000, func(i int) {
		index := picks[i]
		h.mu.Lock()
		runner := h.runners[index]
		h.mu.Unlock()
		callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
		defer cancel()
		observed, err := h.api.GetRunnerByName(callCtx, runner.name)
		switch {
		case err != nil:
			e.Add(1)
		case observed != nil:
			v.Add(1)
		default:
			m.Add(1)
			h.markGone(index, runner)
		}
	})
	verified, missing, inconclusive = int(v.Load()), int(m.Load()), int(e.Load())
	_ = h.state.update(func(data *runData) {
		data.Scale.HeldVerified += verified
		data.Scale.HeldMissing += missing
	})
	return verified, missing, inconclusive
}

func (h *holder) markGone(index int, runner heldRunner) {
	age := time.Since(runner.created).Round(time.Second)
	h.mu.Lock()
	h.runners[index].gone = true
	h.missing++
	journal := h.missing <= holdMissingJournalLimit
	h.mu.Unlock()
	_ = h.state.update(func(data *runData) {
		if runner.pos >= 0 && runner.pos < len(data.Runners) {
			data.Runners[runner.pos].Removed = true
		}
		if data.Scale.FirstMissingAfterSeconds == 0 {
			data.Scale.FirstMissingAfterSeconds = int(age.Seconds())
		}
	})
	if journal {
		_ = h.state.record("runner_missing_during_hold", map[string]any{"runner_name": runner.name, "runner_id": runner.id, "age_seconds": int(age.Seconds())})
	}
}

// removeAll removes every tracked runner that is not known gone, even when
// ctx was cancelled, then spot-checks that removals took effect.
func (h *holder) removeAll(ctx context.Context) error {
	h.mu.Lock()
	var pending []int
	for i, runner := range h.runners {
		if !runner.gone {
			pending = append(pending, i)
		}
	}
	h.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	budget := time.Duration(float64(len(pending))/h.rate*float64(time.Second))*2 + 5*time.Minute
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()

	var removed, failed atomic.Int64
	var firstErr atomic.Value
	progressCtx, stopProgress := context.WithCancel(rctx)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		h.reportProgress(progressCtx, "removing", len(pending), &failed)
	}()
	forEachPaced(rctx, len(pending), h.workers, h.rate, func(i int) {
		if err := h.removeOne(rctx, pending[i]); err != nil {
			failed.Add(1)
			firstErr.CompareAndSwap(nil, safeError(err))
		} else {
			removed.Add(1)
		}
	})
	stopProgress()
	<-progressDone

	var errs []error
	if f := failed.Load(); f > 0 {
		errs = append(errs, fmt.Errorf("%d of %d held runners could not be removed; first: %v", f, len(pending), firstErr.Load()))
	}
	if notAttempted := int64(len(pending)) - removed.Load() - failed.Load(); notAttempted > 0 {
		errs = append(errs, fmt.Errorf("%d held runners were not attempted before the removal deadline", notAttempted))
	}
	if present := h.verifyRemoved(rctx, holdVerifyRemovedSize); present > 0 {
		errs = append(errs, fmt.Errorf("%d of %d sampled removed runners are still registered", present, holdVerifyRemovedSize))
	}
	return errors.Join(errs...)
}

func isRunnerNotFound(err error) bool {
	return errors.Is(err, scaleset.NotFoundError) || errors.Is(err, scaleset.RunnerNotFoundError)
}

// inspectRunnerByID returns nil when the runner no longer exists. The SDK
// reports a missing ID as an error rather than a nil result.
func inspectRunnerByID(ctx context.Context, api runnerAPI, id int) (*scaleset.RunnerReference, error) {
	observed, err := api.GetRunner(ctx, id)
	if err != nil && isRunnerNotFound(err) {
		return nil, nil
	}
	return observed, err
}

func (h *holder) removeOne(ctx context.Context, index int) error {
	h.mu.Lock()
	runner := h.runners[index]
	h.mu.Unlock()
	if !strings.HasPrefix(runner.name, h.namePrefix) {
		return fmt.Errorf("refusing to remove runner %q: not owned by this run", runner.name)
	}
	id := runner.id
	if id > 0 && h.verifyOwnership {
		callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
		observed, err := inspectRunnerByID(callCtx, h.api, id)
		cancel()
		if err != nil {
			return fmt.Errorf("inspect journaled runner %s: %w", runner.name, err)
		}
		if observed == nil {
			return h.markRemoved(index, runner)
		}
		if observed.Name != runner.name {
			return fmt.Errorf("refusing to remove runner ID %d: name is %q, journal expects %q", id, observed.Name, runner.name)
		}
	}
	if id <= 0 {
		callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
		observed, err := h.api.GetRunnerByName(callCtx, runner.name)
		cancel()
		if err != nil {
			return fmt.Errorf("look up runner %s: %w", runner.name, err)
		}
		if observed == nil {
			return h.markRemoved(index, runner)
		}
		id = observed.ID
	}
	var err error
	for attempt := 1; attempt <= holdRemoveAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
		err = h.api.RemoveRunner(callCtx, int64(id))
		cancel()
		if err == nil || isRunnerNotFound(err) {
			return h.markRemoved(index, runner)
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
	defer cancel()
	if observed, lookupErr := h.api.GetRunnerByName(callCtx, runner.name); lookupErr == nil && observed == nil {
		return h.markRemoved(index, runner)
	}
	return fmt.Errorf("remove runner %s: %w", runner.name, err)
}

func (h *holder) markRemoved(index int, runner heldRunner) error {
	h.mu.Lock()
	h.runners[index].gone = true
	h.mu.Unlock()
	return h.state.update(func(data *runData) {
		if runner.pos >= 0 && runner.pos < len(data.Runners) {
			data.Runners[runner.pos].Removed = true
		}
	})
}

// verifyRemoved spot-checks removed runners and re-opens any still present
// so the sequential cleanup retries them.
func (h *holder) verifyRemoved(ctx context.Context, n int) int {
	h.mu.Lock()
	var removed []int
	for i, runner := range h.runners {
		if runner.gone {
			removed = append(removed, i)
		}
	}
	h.mu.Unlock()
	if len(removed) > n {
		rand.Shuffle(len(removed), func(i, j int) { removed[i], removed[j] = removed[j], removed[i] })
		removed = removed[:n]
	}
	var present atomic.Int64
	forEachPaced(ctx, len(removed), holdSampleWorkers, 1000, func(i int) {
		index := removed[i]
		h.mu.Lock()
		runner := h.runners[index]
		h.mu.Unlock()
		callCtx, cancel := context.WithTimeout(ctx, holdCallTimeout)
		defer cancel()
		observed, err := h.api.GetRunnerByName(callCtx, runner.name)
		if err == nil && observed != nil {
			present.Add(1)
			h.mu.Lock()
			h.runners[index].gone = false
			h.mu.Unlock()
			_ = h.state.update(func(data *runData) {
				if runner.pos >= 0 && runner.pos < len(data.Runners) {
					data.Runners[runner.pos].Removed = false
				}
			})
		}
	})
	return int(present.Load())
}

func (h *holder) reportProgress(ctx context.Context, phase string, total int, failures *atomic.Int64) {
	ticker := time.NewTicker(holdProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.flushState()
			h.mu.Lock()
			registered, gone := len(h.runners), 0
			for _, runner := range h.runners {
				if runner.gone {
					gone++
				}
			}
			h.mu.Unlock()
			fmt.Fprintf(h.out, "%s %s total=%d tracked=%d removed_or_gone=%d failures=%d\n",
				time.Now().Format("15:04:05"), phase, total, registered, gone, failures.Load())
		}
	}
}

// cleanupHeldRunners removes leftover runners of a hold run in bulk,
// recovering any registrations the lazily saved state missed from the journal.
func cleanupHeldRunners(ctx context.Context, t *target, state *runState) error {
	if err := reconcileRunnersFromJournal(state); err != nil {
		return err
	}
	snapshot := state.snapshot()
	if err := state.setLazySaves(holdSaveInterval); err != nil {
		return err
	}
	h := newHolder(t.scaleSet, state, snapshot.ScaleSetID, holdCleanupWorkers, holdCleanupRate, os.Stdout)
	h.verifyOwnership = true
	for pos, runner := range snapshot.Runners {
		if !runner.Removed {
			h.runners = append(h.runners, heldRunner{id: runner.ID, name: runner.Name, pos: pos, created: runner.CreatedAt})
		}
	}
	err := h.removeAll(ctx)
	return errors.Join(err, state.setLazySaves(0))
}

// reconcileRunnersFromJournal adds runners the journal recorded but the
// batched state snapshot did not, for example after an abrupt stop.
func reconcileRunnersFromJournal(state *runState) error {
	file, err := os.Open(filepath.Join(state.dir, "journal.jsonl"))
	if err != nil {
		return fmt.Errorf("read run journal: %w", err)
	}
	defer file.Close()
	known := make(map[string]struct{})
	for _, runner := range state.snapshot().Runners {
		known[runner.Name] = struct{}{}
	}
	ids := make(map[string]int)
	sets := make(map[string]int)
	var order []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		var event struct {
			Event  string `json:"event"`
			Fields struct {
				RunnerID   int    `json:"runner_id"`
				RunnerName string `json:"runner_name"`
				ScaleSetID int    `json:"scale_set_id"`
			} `json:"fields"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) != nil || event.Fields.RunnerName == "" {
			continue
		}
		name := event.Fields.RunnerName
		if _, ok := known[name]; ok {
			continue
		}
		switch event.Event {
		case "runner_registration_intent":
			if _, seen := ids[name]; !seen {
				ids[name] = 0
				order = append(order, name)
			}
		case "runner_registered":
			if _, seen := ids[name]; !seen {
				order = append(order, name)
			}
			ids[name] = event.Fields.RunnerID
		default:
			continue
		}
		if event.Fields.ScaleSetID > 0 {
			sets[name] = event.Fields.ScaleSetID
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan run journal: %w", err)
	}
	if len(order) == 0 {
		return nil
	}
	now := time.Now().UTC()
	return state.update(func(data *runData) {
		for _, name := range order {
			data.Runners = append(data.Runners, runnerRecord{ID: ids[name], Name: name, ScaleSetID: sets[name], CreatedAt: now})
		}
	})
}
