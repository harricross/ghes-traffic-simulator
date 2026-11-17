package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	dockerclient "github.com/docker/docker/client"
)

const (
	guardPollInterval = 2 * time.Second
	guardStaleAfter   = 15 * time.Second
	runContainerLabel = "com.github.ghes-traffic-simulator.run"
)

type guardProcess struct {
	command *exec.Cmd
	done    chan error
	dir     string
}

func runGuard(dir string) error {
	stateBytes, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return fmt.Errorf("read guard state: %w", err)
	}
	var data runData
	if err := json.Unmarshal(stateBytes, &data); err != nil || !safeRunID.MatchString(data.RunID) {
		return errors.New("guard state is invalid")
	}
	docker, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("create guard Docker client: %w", err)
	}
	defer docker.Close()
	ctx := context.Background()
	if _, err := docker.Ping(ctx); err != nil {
		return fmt.Errorf("guard cannot reach Docker: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "guard-ready"), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
		return fmt.Errorf("write guard readiness: %w", err)
	}
	ticker := time.NewTicker(guardPollInterval)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(dir, "guard-stop")); err == nil {
			return nil
		}
		info, err := os.Stat(filepath.Join(dir, "heartbeat.json"))
		if err != nil || time.Since(info.ModTime()) > guardStaleAfter {
			return guardStopContainers(ctx, docker, dir, data.RunID)
		}
		<-ticker.C
	}
}

func guardStopContainers(ctx context.Context, docker *dockerclient.Client, dir, runID string) error {
	filtersArg := filters.NewArgs(filters.Arg("label", runContainerLabel+"="+runID))
	items, err := docker.ContainerList(ctx, container.ListOptions{All: true, Filters: filtersArg})
	if err != nil {
		return fmt.Errorf("guard list owned runner containers: %w", err)
	}
	var failures []error
	for _, item := range items {
		stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := docker.ContainerStop(stopCtx, item.ID, container.StopOptions{Timeout: new(int)})
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("stop container %s: %w", item.ID, err))
			continue
		}
		removeCtx, removeCancel := context.WithTimeout(ctx, 10*time.Second)
		err = docker.ContainerRemove(removeCtx, item.ID, container.RemoveOptions{Force: true})
		removeCancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("remove container %s: %w", item.ID, err))
		}
	}
	result := struct {
		RunID     string    `json:"run_id"`
		StoppedAt time.Time `json:"stopped_at"`
		Count     int       `json:"container_count"`
		Errors    []string  `json:"errors,omitempty"`
	}{RunID: runID, StoppedAt: time.Now().UTC(), Count: len(items)}
	for _, failure := range failures {
		result.Errors = append(result.Errors, failure.Error())
	}
	payload, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		return marshalErr
	}
	if writeErr := writeAtomic(filepath.Join(dir, "guard-fired.json"), payload, 0600); writeErr != nil {
		failures = append(failures, writeErr)
	}
	return errors.Join(failures...)
}

func (g *guardProcess) stop() error {
	if err := os.WriteFile(filepath.Join(g.dir, "guard-stop"), []byte("stop\n"), 0600); err != nil {
		return fmt.Errorf("signal guard to stop: %w", err)
	}
	select {
	case err := <-g.done:
		if err != nil {
			return fmt.Errorf("container guard exited with error: %w", err)
		}
		return nil
	case <-time.After(5 * time.Second):
		if err := g.command.Process.Kill(); err != nil {
			return fmt.Errorf("stop container guard after timeout: %w", err)
		}
		<-g.done
		return errors.New("container guard did not stop within 5 seconds")
	}
}
