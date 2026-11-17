package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/actions/scaleset/listener"
)

func runWorkload(ctx context.Context, t *target, state *runState, options runOptions) error {
	if options.mode == "registrations" {
		return runRegistrationChurn(ctx, t, state, options)
	}
	return runWorkflowLoad(ctx, t, state, options)
}

func runWorkflowLoad(ctx context.Context, t *target, state *runState, options runOptions) error {
	if options.maxOutstandingJobs < options.jobsPerRun {
		return errors.New("--max-outstanding-jobs must fit at least one full workflow matrix")
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

	workErr := dispatchAndObserve(ctx, t, state, options, pool, listenerExited)
	if workErr != nil {
		drainCtx, cancel := context.WithTimeout(context.Background(), options.drainTimeout)
		if err := cancelActiveWorkflows(drainCtx, t, state, options.drainTimeout); err != nil {
			workErr = errors.Join(workErr, err)
		}
		cancel()
	}
	stopListener()
	select {
	case listenerErr := <-listenerDone:
		if !errors.Is(listenerErr, context.Canceled) && listenerErr != nil && workErr == nil {
			workErr = fmt.Errorf("scale-set listener stopped: %w", listenerErr)
		}
	case <-time.After(10 * time.Second):
		workErr = errors.Join(workErr, errors.New("scale-set listener did not stop within 10 seconds"))
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	period := time.Duration(float64(time.Second) / options.rate)
	dispatchTicker := time.NewTicker(period)
	defer dispatchTicker.Stop()
	pollTicker := time.NewTicker(3 * time.Second)
	defer pollTicker.Stop()

	dispatched := 0
	outstandingJobs := 0
	nextDispatch := time.Now()
	startedAt := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if dispatched < options.total && outstandingJobs+options.jobsPerRun <= options.maxOutstandingJobs && !time.Now().Before(nextDispatch) {
			marker := fmt.Sprintf("%s-%04d", state.data.RunID, dispatched+1)
			failures := failureShards(options.jobsPerRun, options.failPercent)
			if err := state.record("workflow_dispatch_intent", map[string]any{
				"marker": marker, "jobs": options.jobsPerRun, "expected_failures": len(failures),
			}); err != nil {
				return err
			}
			record := workflowRecord{
				Marker: marker, CreatedAt: time.Now().UTC(), Status: "dispatching",
				Jobs: options.jobsPerRun, ExpectedFailures: len(failures),
			}
			if err := state.update(func(data *runData) {
				data.Attempts++
				data.Workflows = append(data.Workflows, record)
			}); err != nil {
				return err
			}
			id, err := t.api.dispatch(ctx, t.cfg, marker, shardNumbers(options.jobsPerRun), failures, options.jobDuration, options.maxLogBytes)
			dispatched++
			nextDispatch = time.Now().Add(period)
			if err != nil {
				pendingError := fmt.Errorf("dispatch %s has an ambiguous result and will not be retried: %w", marker, err)
				return errors.Join(pendingError, state.record("workflow_dispatch_ambiguous", map[string]any{"marker": marker, "error": err.Error()}))
			}
			outstandingJobs += options.jobsPerRun
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
		case <-pollTicker.C:
			var pollErr error
			outstandingJobs, pollErr = pollWorkflowRuns(ctx, t, state, options, startedAt, outstandingJobs)
			if pollErr != nil {
				return pollErr
			}
		case <-dispatchTicker.C:
		case <-ctx.Done():
		}

		snapshot := state.snapshot()
		if dispatched >= options.total && allWorkflowsTerminal(snapshot.Workflows) && pool.count() == 0 {
			return nil
		}
	}
}

func pollWorkflowRuns(ctx context.Context, t *target, state *runState, options runOptions, since time.Time, outstanding int) (int, error) {
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
		expectedJobs := current.Jobs
		if expectedJobs == 0 {
			expectedJobs = options.jobsPerRun
		}
		if len(jobs) < expectedJobs {
			continue
		}
		valid, reason := verifyWorkflowJobs(jobs, expectedJobs, current.ExpectedFailures)
		expectedConclusion := "success"
		if current.ExpectedFailures > 0 {
			expectedConclusion = "failure"
		}
		if run.Conclusion != expectedConclusion {
			valid = false
			reason = fmt.Sprintf("workflow conclusion %q, expected %q", run.Conclusion, expectedConclusion)
		}
		if err := updateWorkflow(state, marker, run.ID, "completed", run.Conclusion); err != nil {
			return outstanding, err
		}
		if err := state.update(func(data *runData) {
			data.Completed++
			if !valid {
				data.Failed++
			}
		}); err != nil {
			return outstanding, err
		}
		outstanding = max(0, outstanding-options.jobsPerRun)
		if err := state.record("workflow_run_completed", map[string]any{
			"marker": marker, "run_id": run.ID, "conclusion": run.Conclusion,
			"expected_failures": current.ExpectedFailures, "verified": valid, "verification_error": reason,
		}); err != nil {
			return outstanding, err
		}
	}
	return outstanding, nil
}

func workflowRunsForState(ctx context.Context, t *target, state runData, since time.Time) ([]apiWorkflowRun, error) {
	if len(state.Workflows) == 0 {
		return nil, nil
	}
	found := make(map[string]bool, len(state.Workflows))
	runs := make([]apiWorkflowRun, 0, len(state.Workflows))
	for page := 1; page <= 10; page++ {
		pageRuns, err := t.api.workflowRuns(ctx, t.cfg, since, page)
		if err != nil {
			return nil, err
		}
		runs = append(runs, pageRuns...)
		for _, run := range pageRuns {
			if marker := matchingMarker(run, state.Workflows); marker != "" {
				found[marker] = true
			}
		}
		if len(found) == len(state.Workflows) || len(pageRuns) < 100 {
			return runs, nil
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

func failureShards(count, percent int) []int {
	failures := int(math.Round(float64(count*percent) / 100))
	return shardNumbers(failures)
}

func verifyWorkflowJobs(jobs []apiJob, expectedJobs, expectedFailures int) (bool, string) {
	if len(jobs) != expectedJobs {
		return false, fmt.Sprintf("observed %d jobs, expected %d", len(jobs), expectedJobs)
	}
	failures := 0
	seen := make(map[int]struct{}, len(jobs))
	for _, job := range jobs {
		shard, err := jobShard(job.Name)
		if err != nil {
			return false, fmt.Sprintf("cannot identify shard for job %q", job.Name)
		}
		if shard < 0 || shard >= expectedJobs {
			return false, fmt.Sprintf("job %q has an out-of-range shard", job.Name)
		}
		if _, ok := seen[shard]; ok {
			return false, fmt.Sprintf("duplicate shard %d", shard)
		}
		seen[shard] = struct{}{}
		shouldFail := shard < expectedFailures
		workOK, outputOK, intentionalFailure := false, false, false
		for _, step := range job.Steps {
			switch step.Name {
			case "Synthetic work and output":
				workOK = step.Conclusion == "success"
			case "Verify synthetic output":
				outputOK = step.Conclusion == "success"
			case "Intentional synthetic failure":
				intentionalFailure = step.Conclusion == "failure"
			}
		}
		if !workOK || !outputOK {
			return false, fmt.Sprintf("job %q did not produce and verify its output", job.Name)
		}
		if shouldFail {
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

func jobShard(name string) (int, error) {
	const marker = "shard: "
	start := strings.LastIndex(name, marker)
	if start < 0 {
		return 0, errors.New("shard marker missing")
	}
	value := name[start+len(marker):]
	if end := strings.IndexByte(value, ')'); end >= 0 {
		value = value[:end]
	}
	return strconv.Atoi(strings.TrimSpace(value))
}
