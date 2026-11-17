package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

type managedRunner struct {
	name        string
	runnerID    int
	containerID string
}

type runnerPool struct {
	mu          sync.Mutex
	sequence    atomic.Int64
	target      *target
	state       *runState
	docker      *dockerclient.Client
	session     listener.Client
	scaleSetID  int
	maxRunners  int
	memoryBytes int64
	nanoCPUs    int64
	entries     map[string]managedRunner
}

func newDockerClient(ctx context.Context, imageRef string) (*dockerclient.Client, error) {
	client, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create Docker client: %w", err)
	}
	if _, err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to Docker daemon: %w", err)
	}
	if _, _, err := client.ImageInspectWithRaw(ctx, imageRef); err != nil {
		if !errdefs.IsNotFound(err) {
			_ = client.Close()
			return nil, fmt.Errorf("inspect pinned runner image: %w", err)
		}
		pull, err := client.ImagePull(ctx, imageRef, image.PullOptions{})
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("pull pinned runner image: %w", err)
		}
		if _, err := io.Copy(io.Discard, pull); err != nil {
			_ = pull.Close()
			_ = client.Close()
			return nil, fmt.Errorf("read runner image pull result: %w", err)
		}
		if err := pull.Close(); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("close runner image pull: %w", err)
		}
	}
	return client, nil
}

func newRunnerPool(t *target, state *runState, docker *dockerclient.Client, scaleSetID int, options runOptions) *runnerPool {
	return &runnerPool{
		target:      t,
		state:       state,
		docker:      docker,
		scaleSetID:  scaleSetID,
		maxRunners:  options.maxRunners,
		memoryBytes: options.runnerMemoryMiB * 1024 * 1024,
		nanoCPUs:    int64(math.Round(options.runnerCPUs * 1e9)),
		entries:     make(map[string]managedRunner),
	}
}

func (p *runnerPool) Scale(ctx context.Context, message *scaleset.RunnerScaleSetMessage) error {
	if message == nil {
		return nil
	}
	if len(message.JobAvailableMessages) > 0 {
		requestIDs := make([]int64, 0, len(message.JobAvailableMessages))
		for _, job := range message.JobAvailableMessages {
			requestIDs = append(requestIDs, job.RunnerRequestID)
		}
		if p.session == nil {
			return errors.New("scale-set message session is unavailable")
		}
		if _, err := p.session.AcquireJobs(ctx, requestIDs); err != nil {
			return fmt.Errorf("acquire available jobs: %w", err)
		}
	}
	for _, job := range message.JobStartedMessages {
		if err := p.state.record("job_started", map[string]any{
			"runner_name":     job.RunnerName,
			"workflow_run_id": job.WorkflowRunID,
		}); err != nil {
			return err
		}
	}
	for _, job := range message.JobCompletedMessages {
		if err := p.completeRunner(ctx, job.RunnerName); err != nil {
			return err
		}
		if err := p.state.record("job_completed", map[string]any{
			"runner_name":     job.RunnerName,
			"workflow_run_id": job.WorkflowRunID,
			"result":          job.Result,
		}); err != nil {
			return err
		}
	}
	if message.Statistics == nil {
		return nil
	}
	desired := min(message.Statistics.TotalAssignedJobs, p.maxRunners)
	for p.count() < desired {
		if err := p.startRunner(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *runnerPool) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

func (p *runnerPool) startRunner(ctx context.Context) error {
	index := int(p.sequence.Add(1))
	name := runnerName(p.state.data.RunID, 100000+index)
	if err := p.state.record("workflow_runner_intent", map[string]any{"runner_name": name}); err != nil {
		return err
	}
	jit, err := p.target.scaleSet.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       name,
		WorkFolder: "_work",
	}, p.scaleSetID)
	if err != nil {
		observed, lookupErr := p.target.scaleSet.GetRunnerByName(context.WithoutCancel(ctx), name)
		if lookupErr == nil && observed != nil {
			if stateErr := p.state.update(func(data *runData) {
				data.Runners = append(data.Runners, runnerRecord{ID: observed.ID, Name: name, CreatedAt: time.Now().UTC()})
				data.Unresolved++
			}); stateErr != nil {
				return errors.Join(fmt.Errorf("generate runner config: %w", err), stateErr)
			}
			if cleanupErr := removeRunner(context.Background(), p.target, p.state, observed.ID, name); cleanupErr != nil {
				return errors.Join(fmt.Errorf("generate runner config: %w", err), cleanupErr)
			}
			return fmt.Errorf("generate runner config: %w", err)
		}
		if lookupErr != nil {
			stateErr := p.state.update(func(data *runData) {
				data.Runners = append(data.Runners, runnerRecord{Name: name, CreatedAt: time.Now().UTC()})
				data.Unresolved++
			})
			return errors.Join(fmt.Errorf("generate runner config failed and reconciliation is unresolved: %w", err), stateErr)
		}
		return fmt.Errorf("generate runner config: %w", err)
	}
	if jit == nil || jit.Runner == nil || jit.Runner.ID <= 0 || jit.EncodedJITConfig == "" {
		return fmt.Errorf("runner config for %s did not include a runner identity and JIT config", name)
	}
	if err := p.state.update(func(data *runData) {
		data.Runners = append(data.Runners, runnerRecord{ID: jit.Runner.ID, Name: name, CreatedAt: time.Now().UTC()})
	}); err != nil {
		_ = removeRunner(context.Background(), p.target, p.state, jit.Runner.ID, name)
		return err
	}
	containerName := "ghes-load-" + shortHash(p.state.data.RunID) + "-" + fmt.Sprintf("%06d", index)
	runLabel := "com.github.ghes-traffic-simulator.run"
	config := &container.Config{
		Image: p.target.cfg.RunnerImage,
		User:  "runner",
		Cmd:   []string{"/home/runner/run.sh"},
		Env:   []string{"ACTIONS_RUNNER_INPUT_JITCONFIG=" + jit.EncodedJITConfig},
		Labels: map[string]string{
			runLabel: p.state.data.RunID,
			"com.github.ghes-traffic-simulator.runner": name,
		},
	}
	hostConfig := &container.HostConfig{
		Memory:        p.memoryBytes,
		NanoCPUs:      p.nanoCPUs,
		RestartPolicy: container.RestartPolicy{Name: "no"},
	}
	created, err := p.docker.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
	if err != nil {
		_ = removeRunner(context.Background(), p.target, p.state, jit.Runner.ID, name)
		return fmt.Errorf("create runner container: %w", err)
	}
	if err := p.docker.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = p.docker.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
		_ = removeRunner(context.Background(), p.target, p.state, jit.Runner.ID, name)
		return fmt.Errorf("start runner container: %w", err)
	}
	entry := managedRunner{name: name, runnerID: jit.Runner.ID, containerID: created.ID}
	p.mu.Lock()
	p.entries[name] = entry
	p.mu.Unlock()
	if err := p.state.update(func(data *runData) {
		data.ContainerIDs = append(data.ContainerIDs, created.ID)
	}); err != nil {
		_ = p.docker.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
		return err
	}
	return p.state.record("workflow_runner_started", map[string]any{
		"runner_id":    jit.Runner.ID,
		"runner_name":  name,
		"container_id": created.ID,
	})
}

func (p *runnerPool) completeRunner(ctx context.Context, name string) error {
	p.mu.Lock()
	entry, ok := p.entries[name]
	if ok {
		delete(p.entries, name)
	}
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("job completed for untracked runner %q", name)
	}
	err := p.docker.ContainerRemove(ctx, entry.containerID, container.RemoveOptions{Force: true})
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove completed runner container: %w", err)
	}
	observed, err := p.target.scaleSet.GetRunnerByName(ctx, name)
	if err != nil {
		return fmt.Errorf("verify ephemeral runner removal: %w", err)
	}
	if observed != nil {
		if err := removeRunner(ctx, p.target, p.state, observed.ID, name); err != nil {
			return err
		}
	} else {
		if err := p.state.update(func(data *runData) {
			for i := range data.Runners {
				if data.Runners[i].Name == name {
					data.Runners[i].Removed = true
				}
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *runnerPool) shutdown(ctx context.Context) error {
	p.mu.Lock()
	entries := make([]managedRunner, 0, len(p.entries))
	for _, entry := range p.entries {
		entries = append(entries, entry)
	}
	p.entries = make(map[string]managedRunner)
	p.mu.Unlock()
	var failures []error
	for _, entry := range entries {
		if err := p.docker.ContainerStop(ctx, entry.containerID, container.StopOptions{Timeout: new(int)}); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, fmt.Errorf("stop runner %s: %w", entry.name, err))
			continue
		}
		if err := p.docker.ContainerRemove(ctx, entry.containerID, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, fmt.Errorf("remove runner %s: %w", entry.name, err))
		}
	}
	return errors.Join(failures...)
}

var _ listener.Scaler = (*runnerPool)(nil)
