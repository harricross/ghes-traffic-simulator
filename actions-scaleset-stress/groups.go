package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/actions/scaleset"
)

// apiRunnerGroup is an organization self-hosted runner group from the REST API.
type apiRunnerGroup struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Visibility string `json:"visibility"`
	Default    bool   `json:"default"`
	Inherited  bool   `json:"inherited"`
}

// groupAPI is the organization runner-group surface fleet runs and cleanup use.
// getRunnerGroup returns nil, nil when the group no longer exists and
// deleteRunnerGroup treats an already-deleted group as success.
type groupAPI interface {
	findRunnerGroup(ctx context.Context, org, name string) (*apiRunnerGroup, error)
	createRunnerGroup(ctx context.Context, org, name string, repoID int64) (*apiRunnerGroup, error)
	getRunnerGroup(ctx context.Context, org string, id int64) (*apiRunnerGroup, error)
	runnerGroupRunnerCount(ctx context.Context, org string, id int64) (int, error)
	runnerGroupAllowsRepository(ctx context.Context, org string, group apiRunnerGroup, repoID int64) (bool, error)
	deleteRunnerGroup(ctx context.Context, org string, id int64) error
}

// scaleSetAPI is the scale-set subset of the SDK client fleet cleanup needs.
type scaleSetAPI interface {
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, name string) (*scaleset.RunnerScaleSet, error)
	GetRunnerScaleSetByID(ctx context.Context, id int) (*scaleset.RunnerScaleSet, error)
	ListRunnerScaleSets(ctx context.Context, runnerGroupID int) ([]scaleset.RunnerScaleSet, error)
	DeleteRunnerScaleSet(ctx context.Context, id int) error
}

const runnerGroupPageSize = 100

func runnerGroupsPath(org string) string {
	return "/orgs/" + url.PathEscape(org) + "/actions/runner-groups"
}

func (a *ghesAPI) findRunnerGroup(ctx context.Context, org, name string) (*apiRunnerGroup, error) {
	for page := 1; page <= 100; page++ {
		var listing struct {
			TotalCount   int              `json:"total_count"`
			RunnerGroups []apiRunnerGroup `json:"runner_groups"`
		}
		route := fmt.Sprintf("%s?per_page=%d&page=%d", runnerGroupsPath(org), runnerGroupPageSize, page)
		if _, err := a.request(ctx, http.MethodGet, route, nil, &listing, http.StatusOK); err != nil {
			return nil, fmt.Errorf("list runner groups: %w", err)
		}
		for _, group := range listing.RunnerGroups {
			if strings.EqualFold(group.Name, name) {
				found := group
				return &found, nil
			}
		}
		if len(listing.RunnerGroups) < runnerGroupPageSize || page*runnerGroupPageSize >= listing.TotalCount {
			return nil, nil
		}
	}
	return nil, errors.New("list runner groups: more than 100 pages")
}

// createRunnerGroup creates a group visible only to the selected repository.
func (a *ghesAPI) createRunnerGroup(ctx context.Context, org, name string, repoID int64) (*apiRunnerGroup, error) {
	body := map[string]any{
		"name":                       name,
		"visibility":                 "selected",
		"selected_repository_ids":    []int64{repoID},
		"allows_public_repositories": false,
	}
	var group apiRunnerGroup
	if _, err := a.request(ctx, http.MethodPost, runnerGroupsPath(org), body, &group, http.StatusCreated); err != nil {
		return nil, fmt.Errorf("create runner group %q: %w", name, err)
	}
	if group.ID <= 0 || group.Name != name {
		return nil, fmt.Errorf("create runner group %q: unexpected response for group %d %q", name, group.ID, group.Name)
	}
	return &group, nil
}

func (a *ghesAPI) getRunnerGroup(ctx context.Context, org string, id int64) (*apiRunnerGroup, error) {
	var group apiRunnerGroup
	status, err := a.request(ctx, http.MethodGet, fmt.Sprintf("%s/%d", runnerGroupsPath(org), id), nil, &group, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return nil, fmt.Errorf("get runner group %d: %w", id, err)
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	return &group, nil
}

func (a *ghesAPI) runnerGroupRunnerCount(ctx context.Context, org string, id int64) (int, error) {
	var listing struct {
		TotalCount int `json:"total_count"`
	}
	if _, err := a.request(ctx, http.MethodGet, fmt.Sprintf("%s/%d/runners?per_page=1", runnerGroupsPath(org), id), nil, &listing, http.StatusOK); err != nil {
		return 0, fmt.Errorf("count runners in group %d: %w", id, err)
	}
	return listing.TotalCount, nil
}

func (a *ghesAPI) deleteRunnerGroup(ctx context.Context, org string, id int64) error {
	if _, err := a.request(ctx, http.MethodDelete, fmt.Sprintf("%s/%d", runnerGroupsPath(org), id), nil, nil, http.StatusNoContent, http.StatusNotFound); err != nil {
		return fmt.Errorf("delete runner group %d: %w", id, err)
	}
	return nil
}

// runnerGroupAllowsRepository reports whether workflows in the private
// repository can use the group's runners.
func (a *ghesAPI) runnerGroupAllowsRepository(ctx context.Context, org string, group apiRunnerGroup, repoID int64) (bool, error) {
	if group.Visibility != "selected" {
		return true, nil
	}
	for page := 1; page <= 100; page++ {
		var listing struct {
			TotalCount   int       `json:"total_count"`
			Repositories []apiRepo `json:"repositories"`
		}
		route := fmt.Sprintf("%s/%d/repositories?per_page=%d&page=%d", runnerGroupsPath(org), group.ID, runnerGroupPageSize, page)
		if _, err := a.request(ctx, http.MethodGet, route, nil, &listing, http.StatusOK); err != nil {
			return false, fmt.Errorf("list repositories of runner group %d: %w", group.ID, err)
		}
		for _, repo := range listing.Repositories {
			if repo.ID == repoID {
				return true, nil
			}
		}
		if len(listing.Repositories) < runnerGroupPageSize || page*runnerGroupPageSize >= listing.TotalCount {
			return false, nil
		}
	}
	return false, errors.New("list runner group repositories: more than 100 pages")
}

func (a *ghesAPI) repositoryID(ctx context.Context, owner, repo string) (int64, error) {
	var found apiRepo
	if _, err := a.request(ctx, http.MethodGet, repositoryPath(owner, repo), nil, &found, http.StatusOK); err != nil {
		return 0, fmt.Errorf("read repository: %w", err)
	}
	if found.ID <= 0 {
		return 0, errors.New("read repository: response has no repository ID")
	}
	return found.ID, nil
}

func isScaleSetNotFound(err error) bool {
	return errors.Is(err, scaleset.NotFoundError)
}

// teardownFleet deletes run-owned scale sets, then run-created runner groups.
// Callers remove runners first. Each deletion re-reads the live object and
// refuses on any name mismatch, remaining runner or unknown scale set, so a
// repeated cleanup is safe and pre-existing groups are never deleted.
func teardownFleet(ctx context.Context, sets scaleSetAPI, groups groupAPI, org string, state *runState) error {
	snapshot := state.snapshot()
	activeWorkflows := !allWorkflowsTerminal(snapshot.Workflows)
	activeBySet := map[int]int{}
	unattributed := 0
	for _, runner := range snapshot.Runners {
		switch {
		case runner.Removed:
		case runner.ScaleSetID == 0:
			unattributed++
		default:
			activeBySet[runner.ScaleSetID]++
		}
	}
	var failures []error
	for _, set := range snapshot.ScaleSets {
		if set.Removed {
			continue
		}
		active := activeBySet[set.ID] + unattributed
		if err := deleteFleetScaleSet(ctx, sets, state, set, active, activeWorkflows); err != nil {
			failures = append(failures, err)
		}
	}
	snapshot = state.snapshot()
	for _, group := range snapshot.RunnerGroups {
		if group.Removed || !group.CreatedByRun {
			continue
		}
		if err := deleteFleetGroup(ctx, sets, groups, org, state, group, snapshot.ScaleSets); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func deleteFleetScaleSet(ctx context.Context, sets scaleSetAPI, state *runState, set scaleSetRecord, activeRunners int, activeWorkflows bool) error {
	if activeRunners > 0 {
		return fmt.Errorf("scale set %s retained because %d run-owned runners remain", set.Name, activeRunners)
	}
	if activeWorkflows {
		return fmt.Errorf("scale set %s retained because one or more workflow runs remain unresolved", set.Name)
	}
	var live *scaleset.RunnerScaleSet
	var err error
	if set.ID > 0 {
		live, err = sets.GetRunnerScaleSetByID(ctx, set.ID)
		if isScaleSetNotFound(err) {
			live, err = nil, nil
		}
	} else {
		// Creation was journaled but its ID was not; resolve by group and name.
		live, err = sets.GetRunnerScaleSet(ctx, int(set.GroupID), set.Name)
	}
	if err != nil {
		return fmt.Errorf("inspect scale set %s: %w", set.Name, err)
	}
	if live == nil {
		return markScaleSetRemoved(state, set, set.ID)
	}
	if live.Name != set.Name || (set.ID > 0 && live.ID != set.ID) {
		return fmt.Errorf("refusing to delete scale set ID %d: it is named %q, journal expects %q", live.ID, live.Name, set.Name)
	}
	if err := sets.DeleteRunnerScaleSet(ctx, live.ID); err != nil && !isScaleSetNotFound(err) {
		return fmt.Errorf("delete scale set %s: %w", set.Name, err)
	}
	return markScaleSetRemoved(state, set, live.ID)
}

func markScaleSetRemoved(state *runState, set scaleSetRecord, liveID int) error {
	if err := state.update(func(data *runData) {
		for i := range data.ScaleSets {
			if data.ScaleSets[i].Name == set.Name && data.ScaleSets[i].Group == set.Group {
				data.ScaleSets[i].Removed = true
				if data.ScaleSets[i].ID == 0 {
					data.ScaleSets[i].ID = liveID
				}
			}
		}
	}); err != nil {
		return err
	}
	return state.record("scale_set_removed", map[string]any{"scale_set_id": liveID, "scale_set_name": set.Name, "runner_group": set.Group})
}

func deleteFleetGroup(ctx context.Context, sets scaleSetAPI, groups groupAPI, org string, state *runState, group runnerGroupRecord, journaled []scaleSetRecord) error {
	for _, set := range journaled {
		if group.ID > 0 && set.GroupID == group.ID && !set.Removed {
			return fmt.Errorf("runner group %s retained because scale set %s remains", group.Name, set.Name)
		}
	}
	var live *apiRunnerGroup
	var err error
	if group.ID > 0 {
		live, err = groups.getRunnerGroup(ctx, org, group.ID)
	} else {
		// Creation was journaled but its ID was not; resolve by name.
		live, err = groups.findRunnerGroup(ctx, org, group.Name)
	}
	if err != nil {
		return err
	}
	if live == nil {
		return markRunnerGroupRemoved(state, group, group.ID)
	}
	if live.Name != group.Name || (group.ID > 0 && live.ID != group.ID) || live.Default || live.Inherited {
		return fmt.Errorf("refusing to delete runner group ID %d: it is %q (default %t, inherited %t), journal expects %q", live.ID, live.Name, live.Default, live.Inherited, group.Name)
	}
	remaining, err := sets.ListRunnerScaleSets(ctx, int(live.ID))
	if err != nil {
		return fmt.Errorf("list scale sets in runner group %s: %w", group.Name, err)
	}
	if len(remaining) > 0 {
		return fmt.Errorf("runner group %s retained because it still contains %d scale sets", group.Name, len(remaining))
	}
	count, err := groups.runnerGroupRunnerCount(ctx, org, live.ID)
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("runner group %s retained because it still contains %d runners", group.Name, count)
	}
	if err := groups.deleteRunnerGroup(ctx, org, live.ID); err != nil {
		return err
	}
	return markRunnerGroupRemoved(state, group, live.ID)
}

// markRunnerGroupRemoved marks group's record removed, filling in liveID for a
// record journaled before its creation returned an ID.
func markRunnerGroupRemoved(state *runState, group runnerGroupRecord, liveID int64) error {
	if err := state.update(func(data *runData) {
		for i := range data.RunnerGroups {
			if data.RunnerGroups[i].ID == group.ID && data.RunnerGroups[i].Name == group.Name {
				data.RunnerGroups[i].Removed = true
				if data.RunnerGroups[i].ID == 0 {
					data.RunnerGroups[i].ID = liveID
				}
			}
		}
	}); err != nil {
		return err
	}
	return state.record("runner_group_removed", map[string]any{"runner_group_id": liveID, "runner_group": group.Name})
}
