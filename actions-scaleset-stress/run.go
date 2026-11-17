package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset/listener"
)

func runWorkload(ctx context.Context, t *target, state *runState, options runOptions) error {
	switch options.mode {
	case "registrations":
		return runRegistrationChurn(ctx, t, state, options)
	case modeHold:
		return runRegistrationHold(ctx, t, state, options)
	}
	return runWorkflowLoad(ctx, t, state, options)
}

func runWorkflowLoad(ctx context.Context, t *target, state *runState, options runOptions) error {
	if options.maxOutstandingJobs < jobsPerDispatch(options.workload, options.jobsPerRun) {
		return errors.New("--max-outstanding-jobs must fit at least one full workflow dispatch")
	}
	docker, err := newDockerClient(ctx, t.cfg.RunnerImage)
	if err != nil {
		return err
	}
	defer docker.Close()
	scaleSet, err := createScaleSet(ctx, t, state)
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get listener owner name: %w", err)
	}
	session, err := t.scaleSet.MessageSessionClient(ctx, scaleSet.ID, hostname+"-"+state.data.RunID)
	if err != nil {
		return fmt.Errorf("create scale-set message session: %w", err)
	}
	listenerCtx, stopListener := context.WithCancel(context.Background())
	pool := newRunnerPool(t, state, docker, scaleSet.ID, options)
	pool.session = session
	scaleListener, err := listener.New(session, listener.Config{
		ScaleSetID: scaleSet.ID,
		MaxRunners: options.maxRunners,
	})
	if err != nil {
		stopListener()
		return errors.Join(fmt.Errorf("create scale-set listener: %w", err), session.Close(context.Background()))
	}
	listenerDone := make(chan error, 1)
	listenerExited := make(chan struct{})
	go func() {
		listenerDone <- scaleListener.Run(listenerCtx, pool)
		close(listenerExited)
	}()
	if err := state.update(func(data *runData) {
		data.Scale.StandingTarget = options.standingRunners
		data.Scale.RampStartedAt = time.Now().UTC()
	}); err != nil {
		stopListener()
		return errors.Join(err, session.Close(context.Background()))
	}
	poolCtx, stopPool := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		defer close(poolDone)
		var group sync.WaitGroup
		group.Add(2)
		go func() { defer group.Done(); pool.provision(poolCtx) }()
		go func() { defer group.Done(); pool.monitor(poolCtx) }()
		group.Wait()
	}()

	workErr := dispatchAndObserve(ctx, t, state, options, pool, listenerExited)
	if workErr != nil {
		drainCtx, cancel := context.WithTimeout(context.Background(), options.drainTimeout)
		if err := cancelActiveWorkflows(drainCtx, t, state, options.drainTimeout); err != nil {
			workErr = errors.Join(workErr, err)
		}
		cancel()
	}
	pool.setStanding(0)
	stopPool()
	<-poolDone
	stopListener()
	select {
	case listenerErr := <-listenerDone:
		if !errors.Is(listenerErr, context.Canceled) && listenerErr != nil && workErr == nil {
			workErr = fmt.Errorf("scale-set listener stopped: %w", listenerErr)
		}
	case <-time.After(10 * time.Second):
		workErr = errors.Join(workErr, errors.New("scale-set listener did not stop within 10 seconds"))
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := pool.shutdown(shutdownCtx); err != nil {
		workErr = errors.Join(workErr, err)
	}
	cancel()
	if err := session.Close(context.Background()); err != nil {
		workErr = errors.Join(workErr, fmt.Errorf("close scale-set session: %w", err))
	}
	return workErr
}

func dispatchAndObserve(ctx context.Context, t *target, state *runState, options runOptions, pool *runnerPool, listenerExited <-chan struct{}) error {
	if options.standingRunners > 0 {
		fmt.Printf("Ramping up to %d standing runners (rate %.1f/s, timeout %s)\n", options.standingRunners, options.rampRate, options.rampTimeout)
		if err := pool.waitRamped(ctx, options.rampTimeout); err != nil {
			return fmt.Errorf("ramp standing runners: %w", err)
		}
		snapshot := state.snapshot()
		fmt.Printf("Ramp finished after %s with %d runners (ceiling %d %s)\n",
			snapshot.Scale.RampFinishedAt.Sub(snapshot.Scale.RampStartedAt).Round(time.Second), pool.count(),
			snapshot.Scale.CeilingRunners, snapshot.Scale.CeilingReason)
	}
	period := time.Duration(float64(time.Second) / options.rate)
	dispatchTicker := time.NewTicker(period)
	defer dispatchTicker.Stop()
	pollTicker := time.NewTicker(3 * time.Second)
	defer pollTicker.Stop()

	dispatched := 0
	outstandingJobs := 0
	jobs := jobsPerDispatch(options.workload, options.jobsPerRun)
	workflowFile := workflowFileFor(t.cfg, options.workload)
	nextDispatch := time.Now()
	startedAt := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if dispatched < options.total && outstandingJobs+jobs <= options.maxOutstandingJobs && !time.Now().Before(nextDispatch) {
			marker := fmt.Sprintf("%s-%04d", state.data.RunID, dispatched+1)
			failures := []int{}
			if runFails(dispatched, options.failPercent) {
				failures = []int{0}
			}
			if err := state.record("workflow_dispatch_intent", map[string]any{
				"marker": marker, "workload": options.workload, "jobs": jobs, "expected_failures": len(failures),
			}); err != nil {
				return err
			}
			record := workflowRecord{
				Marker: marker, CreatedAt: time.Now().UTC(), Status: "dispatching",
				Workload: options.workload, Shards: options.jobsPerRun, Jobs: jobs, ExpectedFailures: len(failures),
			}
			if err := state.update(func(data *runData) {
				data.Attempts++
				data.Workflows = append(data.Workflows, record)
			}); err != nil {
				return err
			}
			id, err := t.api.dispatch(ctx, t.cfg, workflowFile, marker, shardNumbers(options.jobsPerRun), failures, options.jobDuration, options.maxLogBytes)
			dispatched++
			nextDispatch = time.Now().Add(period)
			if err != nil {
				pendingError := fmt.Errorf("dispatch %s has an ambiguous result and will not be retried: %w", marker, err)
				return errors.Join(pendingError, state.record("workflow_dispatch_ambiguous", map[string]any{"marker": marker, "error": err.Error()}))
			}
			outstandingJobs += jobs
			if id > 0 {
				if err := updateWorkflow(state, marker, id, "queued", ""); err != nil {
					return err
				}
				if err := state.update(func(data *runData) { data.Accepted++ }); err != nil {
					return err
				}
				if err := state.record("workflow_dispatch_accepted", map[string]any{"marker": marker, "run_id": id}); err != nil {
					return err
				}
			} else {
				if err := updateWorkflow(state, marker, 0, "awaiting_discovery", ""); err != nil {
					return err
				}
			}
		}

		select {
		case <-listenerExited:
			if ctx.Err() == nil {
				return errors.New("scale-set listener stopped unexpectedly")
			}
		case err := <-pool.fatal:
			return fmt.Errorf("runner provisioning: %w", err)
		case <-pollTicker.C:
			var pollErr error
			outstandingJobs, pollErr = pollWorkflowRuns(ctx, t, state, startedAt, outstandingJobs)
			if pollErr != nil {
				return pollErr
			}
		case <-dispatchTicker.C:
		case <-ctx.Done():
		}

		snapshot := state.snapshot()
		if dispatched >= options.total && allWorkflowsTerminal(snapshot.Workflows) && pool.activeJobs() == 0 {
			return nil
		}
	}
}

// completedJobListGrace bounds how long a completed run's job list may stay
// shorter than expected before it is verified as-is.
const completedJobListGrace = 30 * time.Second

func pollWorkflowRuns(ctx context.Context, t *target, state *runState, since time.Time, outstanding int) (int, error) {
	snapshot := state.snapshot()
	runs, err := workflowRunsForState(ctx, t, snapshot, since)
	if err != nil {
		return outstanding, fmt.Errorf("poll workflow runs: %w", err)
	}
	for _, run := range runs {
		marker := matchingMarker(run, snapshot.Workflows)
		if marker == "" {
			continue
		}
		var current workflowRecord
		found := false
		for _, record := range snapshot.Workflows {
			if record.Marker == marker {
				current, found = record, true
				break
			}
		}
		if !found || current.Status == "completed" {
			continue
		}
		if current.ID == 0 {
			if err := updateWorkflow(state, marker, run.ID, run.Status, run.Conclusion); err != nil {
				return outstanding, err
			}
			if err := state.update(func(data *runData) { data.Accepted++ }); err != nil {
				return outstanding, err
			}
			if err := state.record("workflow_run_observed", map[string]any{"marker": marker, "run_id": run.ID}); err != nil {
				return outstanding, err
			}
			current.ID = run.ID
		}
		if run.Status != "completed" {
			if err := updateWorkflow(state, marker, run.ID, run.Status, run.Conclusion); err != nil {
				return outstanding, err
			}
			continue
		}
		jobs, err := t.api.workflowJobs(ctx, t.cfg, run.ID)
		if err != nil {
			return outstanding, fmt.Errorf("read jobs for workflow run %d: %w", run.ID, err)
		}
		if len(jobs) < minObservedJobs(current) {
			if current.CompletedSeenAt.IsZero() {
				if err := markCompletedSeen(state, marker); err != nil {
					return outstanding, err
				}
				continue
			}
			if time.Since(current.CompletedSeenAt) < completedJobListGrace {
				continue
			}
		}
		valid, reason := verifyWorkflowRun(current, jobs)
		expectedConclusion := "success"
		if current.ExpectedFailures > 0 {
			expectedConclusion = "failure"
		}
		if valid && run.Conclusion != expectedConclusion {
			valid = false
			reason = fmt.Sprintf("workflow conclusion %q, expected %q", run.Conclusion, expectedConclusion)
		}
		if err := updateWorkflow(state, marker, run.ID, "completed", run.Conclusion); err != nil {
			return outstanding, err
		}
		if err := state.update(func(data *runData) {
			data.Completed++
			switch {
			case !valid:
				data.Failed++
			case current.ExpectedFailures > 0:
				data.IntentionalFailures++
			default:
				data.Passed++
			}
		}); err != nil {
			return outstanding, err
		}
		outstanding = max(0, outstanding-max(1, current.Jobs))
		if err := state.record("workflow_run_completed", map[string]any{
			"marker": marker, "run_id": run.ID, "workload": current.workload(), "conclusion": run.Conclusion,
			"expected_failures": current.ExpectedFailures, "verified": valid, "verification_error": reason,
		}); err != nil {
			return outstanding, err
		}
	}
	return outstanding, nil
}

func workflowRunsForState(ctx context.Context, t *target, state runData, since time.Time) ([]apiWorkflowRun, error) {
	byFile := make(map[string][]workflowRecord)
	var files []string
	for _, record := range state.Workflows {
		file := workflowFileFor(t.cfg, record.workload())
		if _, ok := byFile[file]; !ok {
			files = append(files, file)
		}
		byFile[file] = append(byFile[file], record)
	}
	var runs []apiWorkflowRun
	for _, file := range files {
		records := byFile[file]
		found := make(map[string]bool, len(records))
		for page := 1; page <= 10; page++ {
			pageRuns, err := t.api.workflowRuns(ctx, t.cfg, file, since, page)
			if err != nil {
				return nil, err
			}
			runs = append(runs, pageRuns...)
			for _, run := range pageRuns {
				if marker := matchingMarker(run, records); marker != "" {
					found[marker] = true
				}
			}
			if len(found) == len(records) || len(pageRuns) < 100 {
				break
			}
		}
	}
	return runs, nil
}

func matchingMarker(run apiWorkflowRun, records []workflowRecord) string {
	for _, record := range records {
		if strings.Contains(run.DisplayTitle, record.Marker) || strings.Contains(run.Name, record.Marker) {
			return record.Marker
		}
	}
	return ""
}

func updateWorkflow(state *runState, marker string, id int64, status, conclusion string) error {
	return state.update(func(data *runData) {
		for i := range data.Workflows {
			if data.Workflows[i].Marker == marker {
				if id > 0 {
					data.Workflows[i].ID = id
				}
				data.Workflows[i].Status = status
				data.Workflows[i].Conclusion = conclusion
				return
			}
		}
	})
}

func markCompletedSeen(state *runState, marker string) error {
	return state.update(func(data *runData) {
		for i := range data.Workflows {
			if data.Workflows[i].Marker == marker && data.Workflows[i].CompletedSeenAt.IsZero() {
				data.Workflows[i].CompletedSeenAt = time.Now().UTC()
				return
			}
		}
	})
}

func allWorkflowsTerminal(workflows []workflowRecord) bool {
	for _, workflow := range workflows {
		if workflow.Status != "completed" {
			return false
		}
	}
	return true
}

func shardNumbers(count int) []int {
	shards := make([]int, count)
	for i := range shards {
		shards[i] = i
	}
	return shards
}

// minObservedJobs is the smallest complete job list: a failing CI run may omit its skipped Build job.
func minObservedJobs(record workflowRecord) int {
	if record.workload() == workloadCI && record.ExpectedFailures > 0 {
		return record.shards() + 1
	}
	return jobsPerDispatch(record.workload(), record.shards())
}

func verifyWorkflowRun(record workflowRecord, jobs []apiJob) (bool, string) {
	if record.workload() == workloadCI {
		return verifyCIJobs(jobs, record.shards(), record.ExpectedFailures)
	}
	return verifySyntheticJobs(jobs, record.shards(), record.ExpectedFailures)
}

func stepConclusions(job apiJob) map[string]string {
	steps := make(map[string]string, len(job.Steps))
	for _, step := range job.Steps {
		steps[step.Name] = step.Conclusion
	}
	return steps
}

func stepsSucceeded(steps map[string]string, names ...string) bool {
	for _, name := range names {
		if steps[name] != "success" {
			return false
		}
	}
	return true
}

func verifySyntheticJobs(jobs []apiJob, shards, expectedFailures int) (bool, string) {
	if len(jobs) != shards {
		return false, fmt.Sprintf("observed %d jobs, expected %d", len(jobs), shards)
	}
	failures := 0
	seen := make(map[int]struct{}, len(jobs))
	for _, job := range jobs {
		shard, err := jobShard(job.Name, "synthetic")
		if err != nil {
			return false, fmt.Sprintf("cannot identify shard for job %q", job.Name)
		}
		if shard < 0 || shard >= shards {
			return false, fmt.Sprintf("job %q has an out-of-range shard", job.Name)
		}
		if _, ok := seen[shard]; ok {
			return false, fmt.Sprintf("duplicate shard %d", shard)
		}
		seen[shard] = struct{}{}
		steps := stepConclusions(job)
		if !stepsSucceeded(steps, "Synthetic work and output", "Verify synthetic output") {
			return false, fmt.Sprintf("job %q did not produce and verify its output", job.Name)
		}
		intentionalFailure := steps["Intentional synthetic failure"] == "failure"
		if shard < expectedFailures {
			if job.Conclusion != "failure" || !intentionalFailure {
				return false, fmt.Sprintf("job %q did not fail at the intentional failure step", job.Name)
			}
			failures++
		} else if job.Conclusion != "success" || intentionalFailure {
			return false, fmt.Sprintf("job %q had an unexpected failure", job.Name)
		}
	}
	if failures != expectedFailures {
		return false, fmt.Sprintf("observed %d failed jobs, expected %d", failures, expectedFailures)
	}
	return true, ""
}

func verifyCIJobs(jobs []apiJob, shards, expectedFailures int) (bool, string) {
	lint, build := false, false
	tests := make(map[int]struct{}, shards)
	failures := 0
	for _, job := range jobs {
		steps := stepConclusions(job)
		switch {
		case job.Name == "Lint":
			if lint {
				return false, "duplicate Lint job"
			}
			lint = true
			if job.Conclusion != "success" || !stepsSucceeded(steps, "Check out repository", "Lint sources") {
				return false, fmt.Sprintf("Lint concluded %q", job.Conclusion)
			}
		case job.Name == "Build":
			if build {
				return false, "duplicate Build job"
			}
			build = true
			if expectedFailures > 0 {
				if job.Conclusion != "skipped" {
					return false, fmt.Sprintf("Build concluded %q after a failing test shard, expected skipped", job.Conclusion)
				}
			} else if job.Conclusion != "success" || !stepsSucceeded(steps, "Check out repository", "Build package", "Verify package") {
				return false, fmt.Sprintf("Build concluded %q", job.Conclusion)
			}
		case strings.HasPrefix(job.Name, "Test shard "):
			shard, err := strconv.Atoi(strings.TrimPrefix(job.Name, "Test shard "))
			if err != nil || shard < 0 || shard >= shards {
				return false, fmt.Sprintf("job %q has an invalid shard", job.Name)
			}
			if _, ok := tests[shard]; ok {
				return false, fmt.Sprintf("duplicate test shard %d", shard)
			}
			tests[shard] = struct{}{}
			if !stepsSucceeded(steps, "Check out repository", "Run unit tests", "Verify test report") {
				return false, fmt.Sprintf("job %q did not pass its unit tests", job.Name)
			}
			intentionalFailure := steps["Intentional check failure"] == "failure"
			if shard < expectedFailures {
				if job.Conclusion != "failure" || !intentionalFailure {
					return false, fmt.Sprintf("job %q did not fail at the intentional check failure step", job.Name)
				}
				failures++
			} else if job.Conclusion != "success" || intentionalFailure {
				return false, fmt.Sprintf("job %q had an unexpected failure", job.Name)
			}
		default:
			return false, fmt.Sprintf("unexpected job %q", job.Name)
		}
	}
	switch {
	case !lint:
		return false, "Lint job missing"
	case len(tests) != shards:
		return false, fmt.Sprintf("observed %d test shards, expected %d", len(tests), shards)
	case expectedFailures == 0 && !build:
		return false, "Build job missing"
	case failures != expectedFailures:
		return false, fmt.Sprintf("observed %d failed test shards, expected %d", failures, expectedFailures)
	}
	return true, ""
}

// jobShard parses GHES matrix job names such as "synthetic (0)" or "synthetic (shard: 0)".
func jobShard(name, job string) (int, error) {
	value, ok := strings.CutPrefix(name, job+" (")
	if !ok {
		return 0, errors.New("unexpected job name")
	}
	value, ok = strings.CutSuffix(value, ")")
	if !ok {
		return 0, errors.New("matrix value missing")
	}
	value = strings.TrimSpace(strings.TrimPrefix(value, "shard:"))
	return strconv.Atoi(value)
}
