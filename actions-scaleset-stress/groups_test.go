package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/actions/scaleset"
)

type fakeScaleSets struct {
	mu      sync.Mutex
	sets    map[int]scaleset.RunnerScaleSet
	deleted []int
}

func (f *fakeScaleSets) GetRunnerScaleSet(_ context.Context, groupID int, name string) (*scaleset.RunnerScaleSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, set := range f.sets {
		if set.RunnerGroupID == groupID && set.Name == name {
			return &set, nil
		}
	}
	return nil, nil
}

func (f *fakeScaleSets) GetRunnerScaleSetByID(_ context.Context, id int) (*scaleset.RunnerScaleSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if set, ok := f.sets[id]; ok {
		return &set, nil
	}
	return nil, fmt.Errorf("unexpected status code: 404: %w", scaleset.NotFoundError)
}

func (f *fakeScaleSets) ListRunnerScaleSets(_ context.Context, groupID int) ([]scaleset.RunnerScaleSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found []scaleset.RunnerScaleSet
	for _, set := range f.sets {
		if set.RunnerGroupID == groupID {
			found = append(found, set)
		}
	}
	return found, nil
}

func (f *fakeScaleSets) DeleteRunnerScaleSet(_ context.Context, id int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sets[id]; !ok {
		return fmt.Errorf("unexpected status code: 404: %w", scaleset.NotFoundError)
	}
	delete(f.sets, id)
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeGroups struct {
	groups  map[int64]apiRunnerGroup
	runners map[int64]int
	repos   map[int64][]int64
	nextID  int64
	created []string
	deleted []int64
}

func (f *fakeGroups) findRunnerGroup(_ context.Context, _, name string) (*apiRunnerGroup, error) {
	for _, group := range f.groups {
		if strings.EqualFold(group.Name, name) {
			return &group, nil
		}
	}
	return nil, nil
}

func (f *fakeGroups) createRunnerGroup(_ context.Context, _, name string, repoID int64) (*apiRunnerGroup, error) {
	f.nextID++
	group := apiRunnerGroup{ID: f.nextID, Name: name, Visibility: "selected"}
	f.groups[group.ID] = group
	f.repos[group.ID] = []int64{repoID}
	f.created = append(f.created, name)
	return &group, nil
}

func (f *fakeGroups) runnerGroupAllowsRepository(_ context.Context, _ string, group apiRunnerGroup, repoID int64) (bool, error) {
	if group.Visibility != "selected" {
		return true, nil
	}
	return slices.Contains(f.repos[group.ID], repoID), nil
}

func (f *fakeGroups) getRunnerGroup(_ context.Context, _ string, id int64) (*apiRunnerGroup, error) {
	if group, ok := f.groups[id]; ok {
		return &group, nil
	}
	return nil, nil
}

func (f *fakeGroups) runnerGroupRunnerCount(_ context.Context, _ string, id int64) (int, error) {
	return f.runners[id], nil
}

func (f *fakeGroups) deleteRunnerGroup(_ context.Context, _ string, id int64) error {
	delete(f.groups, id)
	f.deleted = append(f.deleted, id)
	return nil
}

// newFleetFixture has a run-created group "lab-a" (ID 11) holding scale sets
// 101 and 102 and a reused group "shared" (ID 12) holding scale set 201.
func newFleetFixture(t *testing.T) (*runState, *fakeScaleSets, *fakeGroups) {
	t.Helper()
	state := newTestState(t)
	state.data.SchemaVersion = stateSchemaVersion
	state.data.RunnerGroups = []runnerGroupRecord{
		{ID: 11, Name: "lab-a", CreatedByRun: true},
		{ID: 12, Name: "shared"},
	}
	state.data.ScaleSets = []scaleSetRecord{
		{ID: 101, Name: "lab-a-real", Group: "lab-a", GroupID: 11, Kind: scaleSetReal},
		{ID: 102, Name: "lab-a-fake", Group: "lab-a", GroupID: 11, Kind: scaleSetFake},
		{ID: 201, Name: "shared-fake", Group: "shared", GroupID: 12, Kind: scaleSetFake},
	}
	sets := &fakeScaleSets{sets: map[int]scaleset.RunnerScaleSet{
		101: {ID: 101, Name: "lab-a-real", RunnerGroupID: 11},
		102: {ID: 102, Name: "lab-a-fake", RunnerGroupID: 11},
		201: {ID: 201, Name: "shared-fake", RunnerGroupID: 12},
	}}
	groups := &fakeGroups{
		groups:  map[int64]apiRunnerGroup{11: {ID: 11, Name: "lab-a"}, 12: {ID: 12, Name: "shared"}},
		runners: map[int64]int{},
		repos:   map[int64][]int64{},
		nextID:  100,
	}
	return state, sets, groups
}

func journalEvents(t *testing.T, state *runState) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(state.dir, "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var entry struct {
			Event string `json:"event"`
		}
		if line != "" && json.Unmarshal([]byte(line), &entry) == nil {
			events = append(events, entry.Event)
		}
	}
	return events
}

func TestTeardownFleetDeletesScaleSetsThenOnlyRunCreatedGroups(t *testing.T) {
	state, sets, groups := newFleetFixture(t)
	if err := teardownFleet(context.Background(), sets, groups, "org", state); err != nil {
		t.Fatal(err)
	}
	if len(sets.sets) != 0 || len(sets.deleted) != 3 {
		t.Fatalf("scale sets left %v, deleted %v", sets.sets, sets.deleted)
	}
	if len(groups.deleted) != 1 || groups.deleted[0] != 11 {
		t.Fatalf("deleted groups %v, want only the run-created group 11", groups.deleted)
	}
	snapshot := state.snapshot()
	if !snapshot.RunnerGroups[0].Removed || snapshot.RunnerGroups[1].Removed {
		t.Fatalf("group records %+v", snapshot.RunnerGroups)
	}
	events := strings.Join(journalEvents(t, state), ",")
	if events != "scale_set_removed,scale_set_removed,scale_set_removed,runner_group_removed" {
		t.Fatalf("journal events %s", events)
	}

	// A repeated cleanup finds nothing left to do.
	if err := teardownFleet(context.Background(), sets, groups, "org", state); err != nil {
		t.Fatalf("repeated teardown: %v", err)
	}
	if len(groups.deleted) != 1 || len(sets.deleted) != 3 {
		t.Fatal("repeated teardown deleted again")
	}
}

func TestTeardownFleetRetainsScaleSetWithActiveRunnersAndItsGroup(t *testing.T) {
	state, sets, groups := newFleetFixture(t)
	state.data.Runners = []runnerRecord{{ID: 5, Name: "load-x-000001", ScaleSetID: 101}}
	err := teardownFleet(context.Background(), sets, groups, "org", state)
	if err == nil || !strings.Contains(err.Error(), "lab-a-real retained because 1 run-owned runners remain") {
		t.Fatalf("expected active runner refusal, got %v", err)
	}
	if _, ok := sets.sets[101]; !ok {
		t.Fatal("scale set with an active runner was deleted")
	}
	if _, ok := sets.sets[102]; ok {
		t.Fatal("idle scale set in the same group was not deleted")
	}
	if len(groups.deleted) != 0 {
		t.Fatal("group deleted while it still has a run-owned scale set")
	}
}

func TestTeardownFleetRefusesMismatchedNamesAndOccupiedGroups(t *testing.T) {
	state, sets, groups := newFleetFixture(t)
	sets.sets[102] = scaleset.RunnerScaleSet{ID: 102, Name: "someone-elses-set", RunnerGroupID: 11}
	err := teardownFleet(context.Background(), sets, groups, "org", state)
	if err == nil || !strings.Contains(err.Error(), `refusing to delete scale set ID 102`) {
		t.Fatalf("expected name mismatch refusal, got %v", err)
	}
	if _, ok := sets.sets[102]; !ok || len(groups.deleted) != 0 {
		t.Fatal("mismatched scale set or its group was deleted")
	}

	state, sets, groups = newFleetFixture(t)
	sets.sets[999] = scaleset.RunnerScaleSet{ID: 999, Name: "unjournaled", RunnerGroupID: 11}
	err = teardownFleet(context.Background(), sets, groups, "org", state)
	if err == nil || !strings.Contains(err.Error(), "lab-a retained because it still contains 1 scale sets") {
		t.Fatalf("expected unknown scale set refusal, got %v", err)
	}

	state, sets, groups = newFleetFixture(t)
	groups.runners[11] = 3
	err = teardownFleet(context.Background(), sets, groups, "org", state)
	if err == nil || !strings.Contains(err.Error(), "lab-a retained because it still contains 3 runners") {
		t.Fatalf("expected occupied group refusal, got %v", err)
	}

	state, sets, groups = newFleetFixture(t)
	groups.groups[11] = apiRunnerGroup{ID: 11, Name: "renamed"}
	if err := teardownFleet(context.Background(), sets, groups, "org", state); err == nil || len(groups.deleted) != 0 {
		t.Fatalf("expected renamed group refusal, got %v deleted %v", err, groups.deleted)
	}
}

func TestTeardownFleetTreatsMissingObjectsAsRemoved(t *testing.T) {
	state, sets, groups := newFleetFixture(t)
	delete(sets.sets, 101)
	delete(groups.groups, 11)
	state.data.ScaleSets = append(state.data.ScaleSets, scaleSetRecord{Name: "lab-a-unrecorded-id", Group: "lab-a", GroupID: 11, Kind: scaleSetFake})
	if err := teardownFleet(context.Background(), sets, groups, "org", state); err != nil {
		t.Fatal(err)
	}
	for _, set := range state.snapshot().ScaleSets {
		if !set.Removed {
			t.Fatalf("scale set %s not marked removed", set.Name)
		}
	}
	if !state.snapshot().RunnerGroups[0].Removed || len(groups.deleted) != 0 {
		t.Fatal("missing group was not marked removed without a delete call")
	}
}

func TestOpenRunStateAcceptsSchemaOneAndTwoOnly(t *testing.T) {
	root := t.TempDir()
	target := &target{api: newGHESAPI("https://ghes.example.test/api/v3", "", nil), cfg: config{Owner: "load-lab", Repo: "actions-stress"}}
	for version, wantOK := range map[int]bool{1: true, 2: true, 3: false} {
		id := fmt.Sprintf("run-v%d", version)
		dir := filepath.Join(root, id)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(runData{SchemaVersion: version, RunID: id, APIHost: target.api.base, Owner: "load-lab", Repo: "actions-stress"})
		if err := os.WriteFile(filepath.Join(dir, "state.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		state, err := openRunState(root, id, target)
		if (err == nil) != wantOK {
			t.Fatalf("schema %d: open error %v", version, err)
		}
		if state != nil {
			state.Close()
		}
	}
}

func TestRunnerGroupAPIPagesCreatesAndTreatsMissingAsGone(t *testing.T) {
	var created map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/orgs/lab/actions/runner-groups":
			if r.URL.Query().Get("page") == "1" {
				groups := make([]apiRunnerGroup, runnerGroupPageSize)
				for i := range groups {
					groups[i] = apiRunnerGroup{ID: int64(i + 1), Name: fmt.Sprintf("g%d", i)}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 101, "runner_groups": groups})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 101, "runner_groups": []apiRunnerGroup{{ID: 500, Name: "Lab-A"}}})
		case r.Method == http.MethodPost && r.URL.Path == "/orgs/lab/actions/runner-groups":
			_ = json.NewDecoder(r.Body).Decode(&created)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(apiRunnerGroup{ID: 501, Name: "lab-b", Visibility: "selected"})
		case r.Method == http.MethodGet && r.URL.Path == "/orgs/lab/actions/runner-groups/404":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodDelete && r.URL.Path == "/orgs/lab/actions/runner-groups/404":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodGet && r.URL.Path == "/orgs/lab/actions/runner-groups/501/repositories":
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 1, "repositories": []apiRepo{{ID: 42}}})
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
		}
	}))
	defer server.Close()
	api := newGHESAPI(server.URL, "test-token", server.Client())
	ctx := context.Background()

	found, err := api.findRunnerGroup(ctx, "lab", "lab-a")
	if err != nil || found == nil || found.ID != 500 {
		t.Fatalf("find on second page: %+v %v", found, err)
	}
	group, err := api.createRunnerGroup(ctx, "lab", "lab-b", 42)
	if err != nil || group.ID != 501 {
		t.Fatalf("create: %+v %v", group, err)
	}
	if created["visibility"] != "selected" || fmt.Sprint(created["selected_repository_ids"]) != "[42]" || created["allows_public_repositories"] != false {
		t.Fatalf("create body %v", created)
	}
	if missing, err := api.getRunnerGroup(ctx, "lab", 404); err != nil || missing != nil {
		t.Fatalf("missing group: %+v %v", missing, err)
	}
	if err := api.deleteRunnerGroup(ctx, "lab", 404); err != nil {
		t.Fatalf("delete missing group: %v", err)
	}
	for repoID, want := range map[int64]bool{42: true, 43: false} {
		if allowed, err := api.runnerGroupAllowsRepository(ctx, "lab", *group, repoID); err != nil || allowed != want {
			t.Fatalf("repository %d allowed = %v, %v; want %v", repoID, allowed, err, want)
		}
	}
	if allowed, err := api.runnerGroupAllowsRepository(ctx, "lab", apiRunnerGroup{ID: 1, Visibility: "all"}, 43); err != nil || !allowed {
		t.Fatalf("visibility all: %v %v", allowed, err)
	}
}
