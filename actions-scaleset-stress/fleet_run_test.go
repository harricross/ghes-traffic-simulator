package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFillFleetRegistersEachGroupAndHandsOnlineRunnersToSessions(t *testing.T) {
	api := newFakeRunnerAPI()
	api.setLimit[12] = 2
	state := newTestState(t)
	groups := []fleetGroup{
		{Name: "lab-g01", Spec: fleetGroupSpec{Name: "g01", Online: 2, Offline: 3}, Fake: scaleSetRecord{ID: 11}},
		{Name: "lab-g02", Spec: fleetGroupSpec{Name: "g02", Offline: 4}, Fake: scaleSetRecord{ID: 12}},
		{Name: "lab-g03", Spec: fleetGroupSpec{Name: "g03", Real: 2}, Real: scaleSetRecord{ID: 13}},
	}
	if err := state.update(func(data *runData) { data.FleetGroups = newFleetResults(groups) }); err != nil {
		t.Fatal(err)
	}
	h := newTestHolder(api, state)
	sessions := newSessionManager(http.DefaultClient, 2, fleetRunnerVersion, &syncBuffer{})
	if err := fillFleet(context.Background(), h, sessions, state, groups, &syncBuffer{}); err != nil {
		t.Fatal(err)
	}
	if err := sessions.closeAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if api.perSet[13] != 0 {
		t.Fatal("the real scale set must not get fake registrations")
	}
	data := state.snapshot()
	g01, g02, g03 := fleetResult(&data, "lab-g01"), fleetResult(&data, "lab-g02"), fleetResult(&data, "lab-g03")
	if g01.FakeRegistered != 5 || g01.CeilingReason != "" {
		t.Fatalf("g01 %+v", g01)
	}
	if g02.FakeRegistered != 2 || !strings.Contains(g02.CeilingReason, "runner group limit") {
		t.Fatalf("g02 %+v", g02)
	}
	if g03.FakeRegistered != 0 || data.Scale.PeakHeld != 7 {
		t.Fatalf("g03 %+v, peak held %d", g03, data.Scale.PeakHeld)
	}
	stats := sessions.stats()
	if stats["lab-g01"].Requested != 2 || stats["lab-g02"].Requested != 0 {
		t.Fatalf("session requests %+v", stats)
	}
	if err := h.removeAll(context.Background()); err != nil || api.registered() != 0 {
		t.Fatalf("removeAll err=%v left=%d", err, api.registered())
	}
}

func TestFillFleetFailsWhenNothingRegisters(t *testing.T) {
	api := newFakeRunnerAPI()
	api.setLimit[11] = 0
	state := newTestState(t)
	groups := []fleetGroup{{Name: "lab-g01", Spec: fleetGroupSpec{Name: "g01", Offline: 3}, Fake: scaleSetRecord{ID: 11}}}
	sessions := newSessionManager(http.DefaultClient, 1, fleetRunnerVersion, &syncBuffer{})
	err := fillFleet(context.Background(), newTestHolder(api, state), sessions, state, groups, &syncBuffer{})
	if err == nil || !strings.Contains(err.Error(), "runner group limit") {
		t.Fatalf("err = %v", err)
	}
}

func TestHoldFleetWaitsForDwellAndWork(t *testing.T) {
	quick := func(context.Context) error { return nil }
	slow := func(ctx context.Context) error {
		select {
		case <-time.After(150 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	cases := []struct {
		name    string
		dwell   time.Duration
		work    func(context.Context) error
		atLeast time.Duration
	}{
		{"dwell outlasts work", 150 * time.Millisecond, quick, 150 * time.Millisecond},
		{"work outlasts dwell", 10 * time.Millisecond, slow, 150 * time.Millisecond},
		{"no dwell", 0, quick, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHolder(newFakeRunnerAPI(), newTestState(t))
			started := time.Now()
			if err := holdFleet(context.Background(), h, tc.dwell, tc.work, &syncBuffer{}); err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(started); elapsed < tc.atLeast {
				t.Fatalf("returned after %s, want at least %s", elapsed, tc.atLeast)
			}
		})
	}
}

func TestHoldFleetEndsEarlyOnWorkErrorOrCancellation(t *testing.T) {
	h := newTestHolder(newFakeRunnerAPI(), newTestState(t))
	boom := errors.New("listener stopped")
	started := time.Now()
	err := holdFleet(context.Background(), h, time.Hour, func(context.Context) error { return boom }, &syncBuffer{})
	if !errors.Is(err, boom) || time.Since(started) > 5*time.Second {
		t.Fatalf("err = %v after %s", err, time.Since(started))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	workStopped := false
	err = holdFleet(ctx, h, time.Hour, func(ctx context.Context) error {
		<-ctx.Done()
		workStopped = true
		return ctx.Err()
	}, &syncBuffer{})
	if !errors.Is(err, context.DeadlineExceeded) || !workStopped {
		t.Fatalf("err = %v, work stopped %v", err, workStopped)
	}
}

func TestWatchFakeJobsCancelsOnFatalErrorOrListenerExit(t *testing.T) {
	sessions := newSessionManager(http.DefaultClient, 1, "2.338.0", &syncBuffer{})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go watchFakeJobs(ctx, cancel, sessions, nil)
	sessions.reportFatal(errors.New("job rejected"))
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a fake-job failure must cancel dispatch")
	}
	if cause := context.Cause(ctx); cause == nil || cause.Error() != "job rejected" {
		t.Fatalf("cause = %v", cause)
	}

	listener := &fakeJobListener{Group: "stress-g01", exited: make(chan struct{})}
	ctx, cancel = context.WithCancelCause(context.Background())
	defer cancel(nil)
	go watchFakeJobs(ctx, cancel, newSessionManager(http.DefaultClient, 1, "2.338.0", &syncBuffer{}), []*fakeJobListener{listener})
	close(listener.exited)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a listener exit must cancel dispatch")
	}
	if cause := context.Cause(ctx); cause == nil || !strings.Contains(cause.Error(), "stress-g01") || !strings.Contains(cause.Error(), "exited") {
		t.Fatalf("cause = %v", cause)
	}
}
