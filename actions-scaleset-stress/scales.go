package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
)

func createScaleSet(ctx context.Context, t *target, state *runState) (*scaleset.RunnerScaleSet, error) {
	groupID := 1
	if !strings.EqualFold(t.cfg.RunnerGroup, scaleset.DefaultRunnerGroup) {
		group, err := t.scaleSet.GetRunnerGroupByName(ctx, t.cfg.RunnerGroup)
		if err != nil {
			return nil, fmt.Errorf("resolve runner group: %w", err)
		}
		if group == nil {
			return nil, fmt.Errorf("runner group %q was not found", t.cfg.RunnerGroup)
		}
		groupID = group.ID
	}
	created, err := t.scaleSet.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          t.cfg.ScaleSetName,
		RunnerGroupID: groupID,
		Labels:        []scaleset.Label{{Name: t.cfg.ScaleSetName, Type: "System"}},
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: false},
	})
	if err != nil {
		return nil, fmt.Errorf("create run-owned runner scale set: %w", err)
	}
	if created == nil || created.ID <= 0 {
		return nil, errors.New("scale-set creation returned no valid ID")
	}
	t.scaleSet.SetSystemInfo(scaleset.SystemInfo{
		System:     "ghes-traffic-simulator",
		Version:    "dev",
		ScaleSetID: created.ID,
		Subsystem:  "actions-load",
	})
	if err := state.update(func(data *runData) { data.ScaleSetID = created.ID }); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		deleteErr := t.scaleSet.DeleteRunnerScaleSet(cleanupCtx, created.ID)
		if deleteErr != nil {
			return nil, errors.Join(err, fmt.Errorf("delete scale set after state persistence failed: %w", deleteErr))
		}
		return nil, err
	}
	if err := state.record("scale_set_created", map[string]any{"scale_set_id": created.ID}); err != nil {
		return nil, err
	}
	return created, nil
}

func runnerName(runID string, index int) string {
	digest := shortHash(runID)
	return fmt.Sprintf("load-%s-%06d", digest, index)
}

func (t *target) registerOne(ctx context.Context, state *runState, index int, dwell time.Duration, held *atomic.Int64) error {
	name := runnerName(state.data.RunID, index)
	if err := state.record("runner_registration_intent", map[string]any{"runner_name": name}); err != nil {
		return err
	}
	jit, err := t.scaleSet.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       name,
		WorkFolder: "_work",
	}, state.snapshot().ScaleSetID)
	if err != nil {
		observed, lookupErr := t.scaleSet.GetRunnerByName(context.WithoutCancel(ctx), name)
		if lookupErr == nil && observed != nil {
			if stateErr := state.update(func(data *runData) {
				data.Runners = append(data.Runners, runnerRecord{ID: observed.ID, Name: name, CreatedAt: time.Now().UTC()})
			}); stateErr != nil {
				return errors.Join(fmt.Errorf("generate runner config: %w", err), stateErr)
			}
			if removeErr := removeRunner(context.Background(), t, state, observed.ID, name); removeErr != nil {
				_ = state.update(func(data *runData) { data.Unresolved++ })
				return errors.Join(fmt.Errorf("generate runner config: %w", err), removeErr)
			}
			return fmt.Errorf("generate runner config: %w", err)
		} else {
			stateErr := state.update(func(data *runData) {
				data.Runners = append(data.Runners, runnerRecord{Name: name, CreatedAt: time.Now().UTC()})
				data.Unresolved++
			})
			recordErr := state.record("runner_registration_unresolved", map[string]any{"runner_name": name})
			if lookupErr != nil {
				return errors.Join(fmt.Errorf("generate runner configuration and reconcile %s: %w", name, err), lookupErr, stateErr, recordErr)
			}
			return errors.Join(fmt.Errorf("generate runner configuration for %s: %w", name, err), stateErr, recordErr)
		}
	}
	if jit == nil || jit.Runner == nil || jit.Runner.ID <= 0 {
		_ = state.record("runner_registration_unresolved", map[string]any{"runner_name": name})
		return fmt.Errorf("generate runner configuration for %s returned no runner identity", name)
	}
	runner := runnerRecord{ID: jit.Runner.ID, Name: name, CreatedAt: time.Now().UTC()}
	if err := state.update(func(data *runData) { data.Runners = append(data.Runners, runner) }); err != nil {
		return errors.Join(err, removeRunner(context.Background(), t, state, runner.ID, name))
	}
	if err := state.record("runner_registered", map[string]any{"runner_id": runner.ID, "runner_name": name}); err != nil {
		return errors.Join(err, removeRunner(context.Background(), t, state, runner.ID, name))
	}
	holding := int(held.Add(1))
	defer held.Add(-1)
	if err := state.update(func(data *runData) { data.Scale.PeakHeld = max(data.Scale.PeakHeld, holding) }); err != nil {
		return errors.Join(err, removeRunner(context.Background(), t, state, runner.ID, name))
	}
	if dwell > 0 {
		timer := time.NewTimer(dwell)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
		if ctx.Err() == nil {
			observed, lookupErr := t.scaleSet.GetRunnerByName(ctx, name)
			switch {
			case lookupErr != nil:
				_ = state.record("runner_hold_check_failed", map[string]any{"runner_name": name, "error": lookupErr.Error()})
			case observed != nil:
				if err := state.update(func(data *runData) { data.Scale.HeldVerified++ }); err != nil {
					return errors.Join(err, removeRunner(context.Background(), t, state, runner.ID, name))
				}
			default:
				if err := state.update(func(data *runData) { data.Scale.HeldMissing++ }); err != nil {
					return errors.Join(err, removeRunner(context.Background(), t, state, runner.ID, name))
				}
				_ = state.record("runner_missing_before_removal", map[string]any{"runner_name": name, "runner_id": runner.ID})
			}
		}
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := removeRunner(cleanupCtx, t, state, runner.ID, name); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func removeRunner(ctx context.Context, t *target, state *runState, id int, name string) error {
	err := t.scaleSet.RemoveRunner(ctx, int64(id))
	observed, verifyErr := t.scaleSet.GetRunnerByName(ctx, name)
	if verifyErr != nil {
		_ = state.record("runner_removal_unresolved", map[string]any{"runner_id": id, "runner_name": name})
		if err != nil {
			return fmt.Errorf("remove runner %s: %v; verify absence: %w", name, err, verifyErr)
		}
		return fmt.Errorf("verify runner %s removal: %w", name, verifyErr)
	}
	if observed != nil {
		_ = state.record("runner_removal_failed", map[string]any{"runner_id": id, "runner_name": name})
		if err != nil {
			return fmt.Errorf("remove runner %s: %w", name, err)
		}
		return fmt.Errorf("runner %s remains registered after removal", name)
	}
	if err := state.update(func(data *runData) {
		for i := range data.Runners {
			if data.Runners[i].ID == id {
				data.Runners[i].Removed = true
			}
		}
	}); err != nil {
		return err
	}
	return state.record("runner_removed", map[string]any{"runner_id": id, "runner_name": name, "remove_error": errString(err)})
}

func runRegistrationChurn(ctx context.Context, t *target, state *runState, options runOptions) error {
	if _, err := createScaleSet(ctx, t, state); err != nil {
		return err
	}
	semaphore := make(chan struct{}, options.maxInflight)
	errorsFound := make(chan error, options.total)
	var workers sync.WaitGroup
	interval := time.Duration(float64(time.Second) / options.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var held atomic.Int64
	started := 0
	for index := 1; index <= options.total; index++ {
		if index > 1 {
			select {
			case <-ctx.Done():
				break
			case <-ticker.C:
			}
		}
		if ctx.Err() != nil {
			break
		}
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
		started++
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			defer func() { <-semaphore }()
			if err := t.registerOne(ctx, state, i, options.dwell, &held); err != nil {
				errorsFound <- err
			}
		}(index)
	}
	workers.Wait()
	close(errorsFound)
	var failures []error
	for err := range errorsFound {
		failures = append(failures, err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("registration run stopped after %d of %d attempts: %w", started, options.total, ctx.Err())
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return nil
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:4])
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
