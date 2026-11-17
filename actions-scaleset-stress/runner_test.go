package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

func newFleetTestState(t *testing.T) *runState {
	t.Helper()
	state := &runState{dir: t.TempDir()}
	state.data.RunID = "test-run"
	return state
}

func TestRunnerPoolNamesIncludeGroup(t *testing.T) {
	state := newFleetTestState(t)
	options := runOptions{maxRunners: 10, rampRate: 1}
	legacy := newRunnerPool(nil, state, nil, nil, 7, "", 0, 0, options)
	if got, want := legacy.runnerNameFor(3), runnerName("test-run", 100003); got != want {
		t.Errorf("legacy runner name = %q, want %q", got, want)
	}
	if got, want := legacy.containerNameFor(3), "ghes-load-"+shortHash("test-run")+"-000003"; got != want {
		t.Errorf("legacy container name = %q, want %q", got, want)
	}

	grouped := newRunnerPool(nil, state, nil, nil, 8, "stress-g01", 0, 0, options)
	name := grouped.runnerNameFor(3)
	if want := fleetRunnerName("test-run", "stress-g01", fleetKindReal, 3); name != want {
		t.Errorf("group runner name = %q, want %q", name, want)
	}
	if ok, reason := verifyJobPlacement([]apiJob{{Name: "job", Conclusion: "success", RunnerName: name}}, "test-run", "stress-g01", fleetKindReal); !ok {
		t.Errorf("group runner name is not recognised as the group's: %s", reason)
	}
	if got, want := grouped.containerNameFor(3), "ghes-load-"+shortHash("test-run")+"-stress-g01-000003"; got != want {
		t.Errorf("group container name = %q, want %q", got, want)
	}
}

func TestRunnerPoolMaxRunnersCapsListenerDemand(t *testing.T) {
	state := newFleetTestState(t)
	options := runOptions{maxRunners: 100, rampRate: 1}
	if pool := newRunnerPool(nil, state, nil, nil, 7, "", 0, 0, options); pool.maxRunners != 100 {
		t.Fatalf("zero max runners = %d, want --max-runners 100", pool.maxRunners)
	}
	pool := newRunnerPool(nil, state, nil, newPoolSet(), 7, "stress-g01", 1, 3, options)
	err := pool.Scale(context.Background(), &scaleset.RunnerScaleSetMessage{
		Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := pool.desiredLocked(); got != 3 {
		t.Fatalf("desired = %d with 10 assigned jobs, want the pool cap 3", got)
	}
	for i := range 3 {
		if !pool.reserve() {
			t.Fatalf("reserve %d refused below the cap", i+1)
		}
	}
	if pool.reserve() {
		t.Fatal("reserve exceeded the pool's max runners")
	}
}

func TestPoolSetCountsEveryPool(t *testing.T) {
	state := newFleetTestState(t)
	options := runOptions{maxRunners: 10, rampRate: 1}
	set := newPoolSet()
	a := newRunnerPool(nil, state, nil, set, 1, "g1", 2, 0, options)
	b := newRunnerPool(nil, state, nil, set, 2, "g2", 3, 0, options)
	a.entries["a1"] = managedRunner{name: "a1"}
	a.entries["a2"] = managedRunner{name: "a2"}
	b.entries["b1"] = managedRunner{name: "b1"}
	b.entries["b2"] = managedRunner{name: "b2"}
	b.entries["b3"] = managedRunner{name: "b3"}
	b.starting = 1

	if live, starting := a.hostRunners(nil); live != 5 || starting != 1 {
		t.Fatalf("host runners = %d live %d starting, want 5 and 1", live, starting)
	}
	if err := b.notePeaks(); err != nil {
		t.Fatal(err)
	}
	if got := state.snapshot().Scale.PeakContainers; got != 5 {
		t.Fatalf("peak containers = %d, want the sum across pools 5", got)
	}

	a.lowerCeiling("memory low")
	scale := state.snapshot().Scale
	if scale.CeilingRunners != 6 || scale.CeilingReason != "group g1: memory low" {
		t.Fatalf("host ceiling = %d %q, want 6 %q", scale.CeilingRunners, scale.CeilingReason, "group g1: memory low")
	}
	summary := a.summary()
	if summary.Group != "g1" || summary.ScaleSetID != 1 || summary.CeilingRunners != 2 || summary.CeilingReason != "memory low" || summary.Live != 2 || summary.StandingTarget != 2 {
		t.Fatalf("summary = %+v", summary)
	}

	b.fail(errors.New("boom"))
	select {
	case err := <-a.fatal:
		if !strings.Contains(err.Error(), "group g2: boom") {
			t.Fatalf("shared fatal error = %v", err)
		}
	default:
		t.Fatal("pools in a set must share one fatal channel")
	}
}

func TestWaitPoolsRampedTimesOutUnfinishedPools(t *testing.T) {
	state := newFleetTestState(t)
	options := runOptions{maxRunners: 10, rampRate: 1}
	set := newPoolSet()
	idle := &realPool{Group: "g1", pool: newRunnerPool(nil, state, nil, set, 1, "g1", 0, 0, options)}
	stuck := &realPool{Group: "g2", pool: newRunnerPool(nil, state, nil, set, 2, "g2", 2, 0, options)}
	if err := waitPoolsRamped(context.Background(), state, []*realPool{idle, stuck}, 20*time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	if idle.summary().RampFinishedAt.IsZero() || stuck.summary().RampFinishedAt.IsZero() {
		t.Fatal("every pool's ramp must be marked finished")
	}
	if state.snapshot().Scale.RampFinishedAt.IsZero() {
		t.Fatal("the host ramp must be marked finished")
	}
}
