package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	dockerclient "github.com/docker/docker/client"
)

// fleetRunnerVersion is the runner version fake-online sessions report.
const fleetRunnerVersion = "2.338.0"

// runFleet sets up the fleet's runner groups and scale sets, ramps the real
// pools, fills and holds the fake runners, dispatches workflows to the real
// runners, and removes everything it registered. cleanupRun then deletes
// the scale sets and the groups this run created.
func runFleet(ctx context.Context, t *target, state *runState, options runOptions, budget fleetBudget) (err error) {
	out := io.Writer(os.Stdout)
	org := t.cfg.Owner
	repoID, err := t.api.repositoryID(ctx, org, t.cfg.Repo)
	if err != nil {
		return err
	}
	plan, err := preflightFleet(ctx, t.api, t.scaleSet, org, repoID, options.fleet, t.cfg.ScaleSetName)
	if err != nil {
		return err
	}
	if err := state.update(func(data *runData) { data.FleetGroups = newFleetResults(plan) }); err != nil {
		return err
	}
	groups, err := createFleet(ctx, t.api, t.scaleSet, org, repoID, t.cfg.ScaleSetName, plan, state)
	if err != nil {
		return err
	}
	if err := state.update(func(data *runData) {
		for _, g := range groups {
			if r := fleetResult(data, g.Name); r != nil {
				r.ID, r.Created, r.ExistingRunners = g.ID, !g.Reused, g.Existing
			}
		}
	}); err != nil {
		return err
	}
	t.scaleSet.SetSystemInfo(scaleset.SystemInfo{
		System: "ghes-traffic-simulator", Version: "dev", ScaleSetID: firstFleetScaleSet(groups), Subsystem: "actions-load",
	})
	printFleetPlan(out, groups, budget)

	if err := state.setLazySaves(holdSaveInterval); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, state.setLazySaves(0)) }()

	sessions := newSessionManager(sessionHTTPClient(t, budget.Online), options.openConcurrency, fleetRunnerVersion, out)
	holder := newHolder(t.scaleSet, state, 0, options.maxInflight, options.rate, out)
	status := startFleetStatus(t, state, groups, sessions, out)

	var pools fleetPools
	var listeners []*fakeJobListener
	workErr := func() error {
		var err error
		status.setPhase("fill")
		if budget.Real > 0 {
			if pools, err = startFleetPools(ctx, t, state, groups, options); err != nil {
				return err
			}
		}
		if listeners, err = startFakeJobs(ctx, t, state, groups, sessions, holder, options); err != nil {
			return err
		}
		if err := fillFleet(ctx, holder, sessions, state, groups, out); err != nil {
			return err
		}
		status.setPhase("hold")
		status.sampleNow(ctx)
		return holdFleet(ctx, holder, options.dwell, func(ctx context.Context) error {
			if !budget.dispatches() {
				fmt.Fprintln(out, "No real or fake_jobs runners in this fleet; no workflows are dispatched.")
				return nil
			}
			return dispatchFleet(ctx, t, state, options, pools, groups, sessions, listeners)
		}, out)
	}()

	status.setPhase("teardown")
	status.sampleNow(context.WithoutCancel(ctx))
	status.stop()
	teardownErr := teardownFleetRun(ctx, t, state, sessions, holder, pools, listeners, options, workErr != nil, budget, out)
	tallyErr := state.update(tallyFleetWorkflows)
	printFleetSummary(out, state.snapshot().FleetGroups)
	return errors.Join(workErr, teardownErr, status.err(), tallyErr)
}

// fleetPools is every group's real runner pool and their shared Docker client.
type fleetPools struct {
	docker *dockerclient.Client
	pools  []*realPool
}

// startFleetPools starts one listener and runner pool per group with real
// runners. Each pool ramps to, and is capped at, the group's real count.
func startFleetPools(ctx context.Context, t *target, state *runState, groups []fleetGroup, options runOptions) (fleetPools, error) {
	docker, err := newDockerClient(ctx, t.cfg.RunnerImage)
	if err != nil {
		return fleetPools{}, err
	}
	fp := fleetPools{docker: docker}
	set := newPoolSet()
	for _, g := range groups {
		if g.Spec.Real == 0 || g.Real.ID == 0 {
			continue
		}
		pool, err := startRealPool(ctx, t, state, docker, set, g.Real.ID, g.Name, g.Real.Name, g.Spec.Real, g.Spec.Real, options)
		if err != nil {
			return fp, err
		}
		fp.pools = append(fp.pools, pool)
	}
	return fp, nil
}

func stopFleetPools(ctx context.Context, fp fleetPools) error {
	if fp.docker == nil {
		return nil
	}
	err := stopRealPools(ctx, fp.pools)
	return errors.Join(err, fp.docker.Close())
}

// dispatchFleet sends --total workflows across the real pools and fake_jobs
// groups at --dispatch-rate once the pools have ramped, and waits for every
// run to verify. A fake-job failure or an exited fake-job listener ends it.
func dispatchFleet(ctx context.Context, t *target, state *runState, options runOptions, fp fleetPools,
	groups []fleetGroup, sessions *sessionManager, listeners []*fakeJobListener) error {
	interval := time.Duration(float64(time.Second) / options.dispatchRate)
	targets := append(poolTargets(fp.pools), fakeJobTargets(groups)...)
	dispatchCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go watchFakeJobs(dispatchCtx, cancel, sessions, listeners)
	err := dispatchAndObserve(dispatchCtx, t, state, options, fp.pools, targets, interval)
	if cause := context.Cause(dispatchCtx); ctx.Err() == nil && cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

func fakeJobTargets(groups []fleetGroup) []dispatchTarget {
	var targets []dispatchTarget
	for _, g := range groups {
		if g.Spec.FakeJobs && g.Fake.ID > 0 {
			targets = append(targets, dispatchTarget{Group: g.Name, Label: g.Fake.Name, FakeJobs: true})
		}
	}
	return targets
}

// watchFakeJobs cancels ctx with the first fake-job failure or when a
// fake-job listener exits before it is stopped.
func watchFakeJobs(ctx context.Context, cancel context.CancelCauseFunc, sessions *sessionManager, listeners []*fakeJobListener) {
	exited := make(chan string, len(listeners))
	for _, l := range listeners {
		go func() {
			select {
			case <-l.exited:
				exited <- l.Group
			case <-ctx.Done():
			}
		}()
	}
	select {
	case err := <-sessions.fatalErrors():
		cancel(err)
	case group := <-exited:
		cancel(groupError(group, errors.New("fake scale-set listener exited unexpectedly")))
	case <-ctx.Done():
	}
}

// startFakeJobs starts a job listener for each fake_jobs group's fake scale
// set and makes the group's fake-online runners run the jobs they are
// assigned, each replaced by a fresh registration once its job completes.
// Replacement names continue after the fill's indexes.
func startFakeJobs(ctx context.Context, t *target, state *runState, groups []fleetGroup, sessions *sessionManager,
	holder *holder, options runOptions) ([]*fakeJobListener, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("get listener owner name: %w", err)
	}
	runID := state.data.RunID
	var listeners []*fakeJobListener
	for _, g := range groups {
		if !g.Spec.FakeJobs || g.Fake.ID == 0 {
			continue
		}
		target := holdTarget{Group: g.Name, ScaleSetID: g.Fake.ID, Count: 1}
		next := new(atomic.Int64)
		next.Store(int64(g.Spec.Online + g.Spec.Offline))
		nextName := func() string {
			return fleetRunnerName(runID, target.Group, fleetKindFake, int(next.Add(1)-1))
		}
		sessions.setFakeJobs(g.Name, &fakeJobConfig{
			duration: options.fakeJobDuration,
			replace: func(jobCtx context.Context, finished string) error {
				holder.consume(finished)
				if ctx.Err() != nil {
					return errReplacementSkipped
				}
				replaceCtx, cancel := context.WithCancel(jobCtx)
				defer cancel()
				defer context.AfterFunc(ctx, cancel)()
				id, name, jit, err := holder.registerReplacement(replaceCtx, target, nextName)
				if err != nil {
					if ctx.Err() != nil {
						return errReplacementSkipped
					}
					return err
				}
				sessions.add(jobCtx, target.Group, id, name, jit)
				return nil
			},
		})
		l, err := startFakeJobListener(ctx, t.scaleSet, g.Fake.ID, g.Spec.Online, hostname+"-"+runID+"-"+g.Name+"-fake", g.Name)
		if err != nil {
			return listeners, err
		}
		listeners = append(listeners, l)
	}
	return listeners, nil
}

func firstFleetScaleSet(groups []fleetGroup) int {
	for _, g := range groups {
		for _, set := range []scaleSetRecord{g.Real, g.Fake} {
			if set.ID > 0 {
				return set.ID
			}
		}
	}
	return 0
}

func printFleetPlan(w io.Writer, groups []fleetGroup, b fleetBudget) {
	fmt.Fprintf(w, "Fleet: %d groups, %d real, %d fake-online, %d fake-offline runners; estimated fill %s, ramp %s, dispatch %s, removal %s\n",
		len(groups), b.Real, b.Online, b.Offline, b.Fill, b.Ramp, b.Dispatch, b.Removal)
	for _, g := range groups {
		origin := "created"
		if g.Reused {
			origin = fmt.Sprintf("reused, %d existing runners", g.Existing)
		}
		fakeJobs := ""
		if g.Spec.FakeJobs {
			fakeJobs = ", fake_jobs"
		}
		fmt.Fprintf(w, "  %s (id %d, %s): real %d, online %d, offline %d%s\n", g.Name, g.ID, origin, g.Spec.Real, g.Spec.Online, g.Spec.Offline, fakeJobs)
	}
}

// sessionHTTPClient gives fake-online sessions their own connection pool,
// sized so thousands of concurrent long polls reuse connections.
func sessionHTTPClient(t *target, sessions int) *http.Client {
	transport := t.transport
	if base, ok := t.transport.(*http.Transport); ok {
		clone := base.Clone()
		clone.MaxIdleConns = 0
		clone.MaxIdleConnsPerHost = sessions + 64
		transport = clone
	}
	return &http.Client{Transport: transport, Timeout: 90 * time.Second}
}

// fleetHoldTargets is one registration target per group with fake runners.
func fleetHoldTargets(groups []fleetGroup) []holdTarget {
	var targets []holdTarget
	for _, g := range groups {
		if g.Fake.ID > 0 {
			targets = append(targets, holdTarget{Group: g.Name, ScaleSetID: g.Fake.ID, Count: g.Spec.Online + g.Spec.Offline, Online: g.Spec.Online})
		}
	}
	return targets
}

// fillFleet registers every group's fake runners round-robin and hands the
// first Online of each group to the session manager. A group that stops
// early is reported, not fatal; registering nothing at all is.
func fillFleet(ctx context.Context, holder *holder, sessions *sessionManager, state *runState, groups []fleetGroup, out io.Writer) error {
	targets := fleetHoldTargets(groups)
	if len(targets) == 0 {
		return nil
	}
	started := time.Now()
	results, err := holder.registerTargets(ctx, targets, func(target holdTarget, runnerID int, runnerName, encodedJIT string) {
		sessions.add(ctx, target.Group, runnerID, runnerName, encodedJIT)
	})
	held := holder.count()
	updateErr := state.update(func(data *runData) { applyFillResults(data, results, held) })
	recordErr := state.record("fleet_fill_finished", map[string]any{"held": held, "seconds": int(time.Since(started).Seconds())})
	fmt.Fprintf(out, "Fake fill finished in %s: %d registered\n", time.Since(started).Round(time.Second), held)
	if err := errors.Join(err, updateErr, recordErr); err != nil {
		return err
	}
	if held == 0 {
		for _, r := range results {
			if r.CeilingReason != "" {
				return fmt.Errorf("no fake runners could be registered: %s", r.CeilingReason)
			}
		}
		return errors.New("no fake runners could be registered")
	}
	return ctx.Err()
}

func applyFillResults(data *runData, results map[string]holdTargetResult, held int) {
	data.Scale.PeakHeld = max(data.Scale.PeakHeld, held)
	for name, r := range results {
		if res := fleetResult(data, name); res != nil {
			res.FakeRegistered, res.FakeErrors, res.CeilingReason = r.Registered, r.Errors, r.CeilingReason
		}
	}
}

// holdFleet runs work (workflow dispatch) and keeps the fleet in place
// until both the dwell has passed and work has finished, sampling that held
// registrations still exist. A work error ends the hold at once.
func holdFleet(ctx context.Context, holder *holder, dwell time.Duration, work func(context.Context) error, out io.Writer) error {
	workDone := make(chan error, 1)
	go func() { workDone <- work(ctx) }()
	dwellOver := time.After(dwell)
	sample := time.NewTicker(holder.sampleEvery)
	defer sample.Stop()
	started := time.Now()
	fmt.Fprintf(out, "Holding the fleet for %s\n", dwell)
	var workErr error
	working, dwelling := true, dwell > 0
	for working || dwelling {
		select {
		case <-ctx.Done():
			if working {
				workErr = <-workDone
			}
			return errors.Join(workErr, ctx.Err())
		case workErr = <-workDone:
			working = false
			if workErr != nil {
				return workErr
			}
		case <-dwellOver:
			dwelling = false
		case <-sample.C:
			holder.checkSample(ctx, holdSampleSize)
			holder.flushState()
		}
	}
	if holder.count() > 0 {
		holder.checkSample(ctx, holdFinalSampleSize)
	}
	fmt.Fprintf(out, "Hold finished after %s\n", time.Since(started).Round(time.Second))
	return nil
}

// teardownFleetRun stops workflows, fake-job listeners, sessions, fake
// registrations and real pools in that order. It ignores ctx cancellation
// (the run deadline). Each phase has its own deadline, so a long fake
// removal at --rate cannot exhaust the time left to stop the real pools.
func teardownFleetRun(ctx context.Context, t *target, state *runState, sessions *sessionManager, holder *holder,
	pools fleetPools, listeners []*fakeJobListener, options runOptions, failed bool, budget fleetBudget, out io.Writer) error {
	detached := context.WithoutCancel(ctx)
	var errs []error
	if failed && !allWorkflowsTerminal(state.snapshot().Workflows) {
		drainCtx, drainCancel := context.WithTimeout(detached, options.drainTimeout)
		errs = append(errs, cancelActiveWorkflows(drainCtx, t, state, options.drainTimeout))
		drainCancel()
	}
	for _, l := range listeners {
		errs = append(errs, l.stop())
	}
	closeCtx, closeCancel := context.WithTimeout(detached, 5*time.Minute+secondsAt(budget.Online, float64(max(options.openConcurrency, 1))*5))
	errs = append(errs, sessions.closeAll(closeCtx))
	closeCancel()
	fmt.Fprintf(out, "Removing %d fake runners at %.0f/s\n", holder.count(), holder.rate)
	errs = append(errs, holder.removeAll(detached)) // bounded by its own rate-based deadline
	poolCtx, poolCancel := context.WithTimeout(detached, fleetCleanupTimeout(budget.Real))
	errs = append(errs, stopFleetPools(poolCtx, pools))
	poolCancel()
	return errors.Join(errs...)
}

// fleetStatus samples every group each fleetStatusInterval until stopped.
type fleetStatus struct {
	t        *target
	state    *runState
	groups   []fleetGroup
	sessions *sessionManager
	out      io.Writer

	phase   atomic.Value
	mu      sync.Mutex // serializes samples
	cancel  context.CancelFunc
	done    chan struct{}
	errOnce sync.Once
	first   error
}

func startFleetStatus(t *target, state *runState, groups []fleetGroup, sessions *sessionManager, out io.Writer) *fleetStatus {
	ctx, cancel := context.WithCancel(context.Background())
	s := &fleetStatus{t: t, state: state, groups: groups, sessions: sessions, out: out, cancel: cancel, done: make(chan struct{})}
	s.phase.Store("setup")
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(fleetStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sampleNow(ctx)
			}
		}
	}()
	return s
}

func (s *fleetStatus) setPhase(phase string) { s.phase.Store(phase) }

func (s *fleetStatus) sampleNow(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := recordFleetStatus(callCtx, s.t.scaleSet, s.t.api, s.t.cfg.Owner, s.groups, s.sessions.stats(), s.state, s.phase.Load().(string), s.out); err != nil {
		s.errOnce.Do(func() { s.first = fmt.Errorf("record fleet status: %w", err) })
	}
}

func (s *fleetStatus) stop() {
	s.cancel()
	<-s.done
}

func (s *fleetStatus) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first
}
