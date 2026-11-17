package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/actions/scaleset"
	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

func cleanupRun(ctx context.Context, t *target, state *runState) error {
	if snapshot := state.snapshot(); len(snapshot.ScaleSets) > 0 || len(snapshot.RunnerGroups) > 0 {
		return cleanupFleetRun(ctx, t, state)
	}
	var failures []error
	if state.snapshot().Mode == "workflows" {
		drainCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := cancelActiveWorkflows(drainCtx, t, state, 10*time.Second); err != nil {
			failures = append(failures, err)
		}
		cancel()
		if err := removeRunContainers(ctx, t, state); err != nil {
			failures = append(failures, err)
		}
	}

	if state.snapshot().Mode == modeHold {
		if err := cleanupHeldRunners(ctx, t, state); err != nil {
			failures = append(failures, err)
		}
	}

	for _, runner := range state.snapshot().Runners {
		if runner.Removed {
			continue
		}
		var observed *scaleset.RunnerReference
		var err error
		if runner.ID > 0 {
			observed, err = inspectRunnerByID(ctx, t.scaleSet, runner.ID)
		} else {
			observed, err = t.scaleSet.GetRunnerByName(ctx, runner.Name)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("inspect journaled runner %s: %w", runner.Name, err))
			continue
		}
		if observed == nil {
			if err := markRunnerRemoved(state, runner.ID, runner.Name); err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if observed.Name != runner.Name {
			failures = append(failures, fmt.Errorf("refusing to remove runner ID %d: name is %q, journal expects %q", runner.ID, observed.Name, runner.Name))
			continue
		}
		if err := removeRunner(ctx, t, state, runner.ID, runner.Name); err != nil {
			failures = append(failures, err)
		}
	}

	snapshot := state.snapshot()
	activeRunners := false
	for _, runner := range state.snapshot().Runners {
		if !runner.Removed {
			activeRunners = true
			break
		}
	}
	if snapshot.ScaleSetID > 0 && !snapshot.ScaleSetRemoved && activeRunners {
		failures = append(failures, errors.New("runner scale set retained because one or more run-owned runners remain"))
	}
	activeWorkflows := false
	for _, workflow := range snapshot.Workflows {
		if workflow.Status != "completed" {
			activeWorkflows = true
			break
		}
	}
	if snapshot.ScaleSetID > 0 && !snapshot.ScaleSetRemoved && activeWorkflows {
		failures = append(failures, errors.New("runner scale set retained because one or more workflow runs remain unresolved"))
	}
	if snapshot.ScaleSetID > 0 && !snapshot.ScaleSetRemoved && !activeRunners && !activeWorkflows {
		scaleSet, err := t.scaleSet.GetRunnerScaleSetByID(ctx, snapshot.ScaleSetID)
		if err != nil {
			failures = append(failures, fmt.Errorf("inspect journaled runner scale set: %w", err))
		} else if scaleSet == nil || scaleSet.Name != t.cfg.ScaleSetName {
			failures = append(failures, fmt.Errorf("refusing to delete scale set ID %d: it does not match configured name %q", snapshot.ScaleSetID, t.cfg.ScaleSetName))
		} else if err := t.scaleSet.DeleteRunnerScaleSet(ctx, snapshot.ScaleSetID); err != nil {
			failures = append(failures, fmt.Errorf("delete run-owned runner scale set: %w", err))
		} else if err := state.update(func(data *runData) { data.ScaleSetRemoved = true }); err != nil {
			failures = append(failures, err)
		} else if err := state.record("scale_set_removed", map[string]any{"scale_set_id": snapshot.ScaleSetID}); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// cleanupFleetRun tears down a schema-v2 run: workflows and containers first,
// then every run-owned runner in bulk, then scale sets and run-created groups.
func cleanupFleetRun(ctx context.Context, t *target, state *runState) error {
	var failures []error
	if !allWorkflowsTerminal(state.snapshot().Workflows) {
		drainCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := cancelActiveWorkflows(drainCtx, t, state, 10*time.Second); err != nil {
			failures = append(failures, err)
		}
		cancel()
	}
	if err := removeRunContainers(ctx, t, state); err != nil {
		failures = append(failures, err)
	}
	if err := cleanupHeldRunners(ctx, t, state); err != nil {
		failures = append(failures, err)
	}
	if err := teardownFleet(ctx, t.scaleSet, t.api, t.cfg.Owner, state); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func cancelActiveWorkflows(ctx context.Context, t *target, state *runState, drain time.Duration) error {
	snapshot := state.snapshot()
	var failures []error
	records := make(map[int64]workflowRecord, len(snapshot.Workflows))
	for _, workflow := range snapshot.Workflows {
		if workflow.ID > 0 {
			records[workflow.ID] = workflow
		}
	}
	runs, err := workflowRunsForState(ctx, t, snapshot, snapshot.CreatedAt)
	if err != nil {
		failures = append(failures, fmt.Errorf("discover workflow runs before cleanup: %w", err))
	} else {
		for _, run := range runs {
			marker := matchingMarker(run, snapshot.Workflows)
			if marker == "" {
				continue
			}
			var current workflowRecord
			for _, record := range snapshot.Workflows {
				if record.Marker == marker {
					current = record
					break
				}
			}
			if err := updateWorkflow(state, marker, run.ID, run.Status, run.Conclusion); err != nil {
				failures = append(failures, err)
				continue
			}
			current.ID, current.Status, current.Conclusion = run.ID, run.Status, run.Conclusion
			records[run.ID] = current
		}
	}
	for id, workflow := range records {
		if workflow.Status == "completed" {
			continue
		}
		if err := t.api.cancelRun(ctx, t.cfg, id); err != nil {
			failures = append(failures, fmt.Errorf("cancel workflow run %d: %w", id, err))
		} else if err := state.record("workflow_run_cancel_requested", map[string]any{"run_id": id, "marker": workflow.Marker}); err != nil {
			failures = append(failures, err)
		}
	}
	deadline := time.Now().Add(drain)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		_, pollErr := pollWorkflowRuns(pollCtx, t, state, snapshot.CreatedAt, 0)
		cancel()
		if pollErr == nil && allWorkflowsTerminal(state.snapshot().Workflows) {
			return errors.Join(failures...)
		}
		if pollErr != nil && !errors.Is(pollErr, context.DeadlineExceeded) {
			failures = append(failures, pollErr)
		}
		select {
		case <-ctx.Done():
			return errors.Join(append(failures, ctx.Err())...)
		case <-time.After(time.Second):
		}
	}
	for _, workflow := range state.snapshot().Workflows {
		if workflow.Status != "completed" {
			if err := state.update(func(data *runData) { data.Unresolved++ }); err != nil {
				failures = append(failures, err)
			}
			failures = append(failures, fmt.Errorf("workflow %s did not reach a terminal state before cleanup deadline", workflow.Marker))
		}
	}
	return errors.Join(failures...)
}

func markRunnerRemoved(state *runState, id int, name string) error {
	return state.update(func(data *runData) {
		for i := range data.Runners {
			if (id > 0 && data.Runners[i].ID == id) || (id == 0 && data.Runners[i].Name == name) {
				data.Runners[i].Removed = true
			}
		}
	})
}

func removeRunContainers(ctx context.Context, t *target, state *runState) error {
	snapshot := state.snapshot()
	if len(snapshot.ContainerIDs) == 0 {
		return nil
	}
	docker, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("create Docker client for cleanup: %w", err)
	}
	defer docker.Close()
	if _, err := docker.Ping(ctx); err != nil {
		return fmt.Errorf("connect to Docker for cleanup: %w", err)
	}
	var failures []error
	for _, id := range snapshot.ContainerIDs {
		inspected, err := docker.ContainerInspect(ctx, id)
		if err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			failures = append(failures, fmt.Errorf("inspect journaled runner container %s: %w", id, err))
			continue
		}
		if inspected.Config == nil || inspected.Config.Labels[runContainerLabel] != snapshot.RunID {
			failures = append(failures, fmt.Errorf("refusing to remove container %s: run ownership label does not match", id))
			continue
		}
		if inspected.State != nil && inspected.State.Running {
			if err := docker.ContainerStop(ctx, id, container.StopOptions{Timeout: new(int)}); err != nil && !errdefs.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("stop runner container %s: %w", id, err))
				continue
			}
		}
		if err := docker.ContainerRemove(ctx, id, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, fmt.Errorf("remove runner container %s: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

// pipelinesTokenPath matches the per-tenant token that GHES embeds in the
// pipelines service URL path; SDK errors quote the full request URL.
var pipelinesTokenPath = regexp.MustCompile(`(/_services/pipelines/)[^/\s"]+`)

func redactErrorText(text string) string {
	return pipelinesTokenPath.ReplaceAllString(text, "${1}<redacted>")
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	return redactErrorText(err.Error())
}
