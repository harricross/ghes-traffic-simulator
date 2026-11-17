package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/actions/scaleset"
)

func TestRecordFleetStatusTracksPeaksAndFailedReads(t *testing.T) {
	state := newTestState(t)
	sets := &fakeScaleSets{sets: map[int]scaleset.RunnerScaleSet{
		1: {ID: 1, Statistics: &scaleset.RunnerScaleSetStatistic{TotalRegisteredRunners: 10, TotalBusyRunners: 4}},
		2: {ID: 2, Statistics: &scaleset.RunnerScaleSetStatistic{TotalRegisteredRunners: 900, TotalIdleRunners: 300}},
	}}
	groups := &fakeGroups{runners: map[int64]int{7: 910}}
	fleet := []fleetGroup{
		{Spec: fleetGroupSpec{Name: "g01", Real: 10, Online: 300, Offline: 600}, Name: "stress-g01", ID: 7,
			Real: scaleSetRecord{ID: 1, Name: "s-stress-g01-real"}, Fake: scaleSetRecord{ID: 2, Name: "s-stress-g01-fake"}},
		{Spec: fleetGroupSpec{Name: "g02", Offline: 5}, Name: "stress-g02", ID: 8, Reused: true,
			Fake: scaleSetRecord{ID: 99, Name: "s-stress-g02-fake"}},
	}
	state.data.FleetGroups = newFleetResults(fleet)
	sessions := map[string]sessionGroupStats{"stress-g01": {Open: 300, Polls: 1200, Unexpected: 1, UnexpectedTypes: map[string]int64{"JobAvailable": 1}}}

	var out bytes.Buffer
	if err := recordFleetStatus(context.Background(), sets, groups, "lab", fleet, sessions, state, "hold", &out); err != nil {
		t.Fatal(err)
	}
	// A later, lower sample must not lower the peaks.
	sets.sets[1].Statistics.TotalBusyRunners = 1
	sessions["stress-g01"] = sessionGroupStats{Open: 250, Polls: 1500, Unexpected: 1}
	if err := recordFleetStatus(context.Background(), sets, groups, "lab", fleet, sessions, state, "hold", &out); err != nil {
		t.Fatal(err)
	}

	g1, g2 := state.data.FleetGroups[0], state.data.FleetGroups[1]
	if g1.PeakRealRegistered != 10 || g1.PeakRealBusy != 4 || g1.PeakFakeIdle != 300 || g1.PeakGroupRunners != 910 ||
		g1.PeakSessionsOpen != 300 || g1.Polls != 1500 || g1.UnexpectedMessages != 1 || g1.UnexpectedTypes["JobAvailable"] != 1 {
		t.Fatalf("group 1 result = %+v", g1)
	}
	if g2.Created || !strings.Contains(g2.LastError, "s-stress-g02-fake") {
		t.Fatalf("group 2 should record the failed statistics read: %+v", g2)
	}
	text := out.String()
	for _, want := range []string{
		"fleet[hold] stress-g01: real registered 10 idle 0 busy 4; fake registered 900 idle 300 busy 0; group runners 910; sessions open 300/300",
		"fleet[hold] stress-g02: fake unavailable; group runners 0\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output missing %q:\n%s", want, text)
		}
	}
	count := 0
	for _, event := range journalEvents(t, state) {
		if event == "fleet_group_status" {
			count++
		}
	}
	if count != 4 {
		t.Fatalf("journaled %d fleet_group_status events, want 4", count)
	}
}

func TestTallyFleetWorkflowsAndSummary(t *testing.T) {
	data := runData{
		FleetGroups: []fleetGroupResult{{Name: "a", ID: 1, Created: true, Real: 2, Online: 3, Offline: 4, PeakRealRegistered: 2, FakeRegistered: 7}, {Name: "b", ID: 2}},
		Workflows: []workflowRecord{
			{Group: "a", Outcome: workflowPassed}, {Group: "a", Outcome: workflowIntentionalFailure},
			{Group: "a", Outcome: workflowFailed}, {Group: "a"}, {Group: "a", Outcome: workflowPassed},
			{Group: "other", Outcome: workflowPassed},
		},
	}
	tallyFleetWorkflows(&data)
	tallyFleetWorkflows(&data) // idempotent
	a := data.FleetGroups[0]
	if a.Dispatched != 5 || a.Passed != 2 || a.IntentionalFails != 1 || a.Failed != 1 || a.Unresolved != 1 || data.FleetGroups[1].Dispatched != 0 {
		t.Fatalf("tally = %+v", data.FleetGroups)
	}
	var out bytes.Buffer
	printFleetSummary(&out, data.FleetGroups)
	want := "a (id 1, created): real 2/2, busy peak 0; online 0/3 idle (sessions peak 0); fake registered 7/7; jobs 5 dispatched, 2 passed, 1 intentional failures, 1 failed, 1 unresolved; unexpected messages 0\n"
	if !strings.HasPrefix(out.String(), want) || !strings.Contains(out.String(), "b (id 2, reused)") {
		t.Fatalf("summary =\n%s", out.String())
	}
}
