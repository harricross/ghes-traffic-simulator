package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const apiVersion = "2022-11-28"

type ghesAPI struct {
	base  string
	token string
	http  *http.Client
}

func newGHESAPI(base, token string, httpClient *http.Client) *ghesAPI {
	return &ghesAPI{base: strings.TrimRight(base, "/"), token: token, http: httpClient}
}

func (a *ghesAPI) request(ctx context.Context, method, route string, body any, out any, statuses ...int) (int, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("encode %s request: %w", route, err)
		}
		reader = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+route, reader)
	if err != nil {
		return 0, fmt.Errorf("build %s request: %w", route, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("Authorization", "Bearer "+a.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", method, route, err)
	}
	defer resp.Body.Close()
	if !statusExpected(resp.StatusCode, statuses) {
		return resp.StatusCode, fmt.Errorf("%s %s returned HTTP %d", method, route, resp.StatusCode)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, nil
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 4<<20))
	if err := decoder.Decode(out); err != nil {
		return resp.StatusCode, fmt.Errorf("decode %s response: %w", route, err)
	}
	return resp.StatusCode, nil
}

func statusExpected(got int, expected []int) bool {
	for _, status := range expected {
		if got == status {
			return true
		}
	}
	return false
}

type apiUser struct {
	Login string `json:"login"`
}

type apiRepo struct {
	ID            int64  `json:"id"`
	FullName      string `json:"full_name"`
	Private       bool   `json:"private"`
	DefaultBranch string `json:"default_branch"`
}

type apiWorkflowRun struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	DisplayTitle string    `json:"display_title"`
	Status       string    `json:"status"`
	Conclusion   string    `json:"conclusion"`
	CreatedAt    time.Time `json:"created_at"`
}

type apiJob struct {
	Name            string `json:"name"`
	Conclusion      string `json:"conclusion"`
	RunnerName      string `json:"runner_name"`
	RunnerGroupName string `json:"runner_group_name"`
	Steps           []struct {
		Name       string `json:"name"`
		Conclusion string `json:"conclusion"`
	} `json:"steps"`
}

func (a *ghesAPI) check(ctx context.Context) (string, string, error) {
	var meta struct {
		InstalledVersion string `json:"installed_version"`
	}
	if _, err := a.request(ctx, http.MethodGet, "/meta", nil, &meta, http.StatusOK); err != nil {
		return "", "", fmt.Errorf("GHES version check: %w", err)
	}
	var user apiUser
	if _, err := a.request(ctx, http.MethodGet, "/user", nil, &user, http.StatusOK); err != nil {
		return "", "", fmt.Errorf("token identity check: %w", err)
	}
	if user.Login == "" {
		return "", "", errors.New("GHES /user response did not include a login")
	}
	return meta.InstalledVersion, user.Login, nil
}

func (t *target) checkAuthenticated(ctx context.Context) error {
	version, user, err := t.api.check(ctx)
	if err != nil {
		return err
	}
	t.identity = user
	t.ready = false
	t.repoState = fmt.Sprintf("ready for GHES %s", version)
	return nil
}

// verifyRepository checks the configured repository is the expected private,
// Actions-enabled target on its default branch. found is false on HTTP 404.
func (t *target) verifyRepository(ctx context.Context, repo *apiRepo) (bool, error) {
	status, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo), nil, repo, http.StatusOK, http.StatusNotFound)
	if err != nil || status == http.StatusNotFound {
		return false, err
	}
	if !repo.Private || !strings.EqualFold(repo.FullName, t.cfg.Owner+"/"+t.cfg.Repo) {
		return true, errors.New("configured repository is not the expected private target")
	}
	if repo.DefaultBranch != t.cfg.Ref {
		return true, fmt.Errorf("configured ref %q must match repository default branch %q", t.cfg.Ref, repo.DefaultBranch)
	}
	var permissions struct {
		Enabled bool `json:"enabled"`
	}
	if _, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo)+"/actions/permissions", nil, &permissions, http.StatusOK); err != nil {
		return true, fmt.Errorf("read repository Actions permissions: %w", err)
	}
	if !permissions.Enabled {
		return true, errors.New("GitHub Actions is disabled for the target repository; enable it through the approved repository settings")
	}
	return true, nil
}

func (t *target) check(ctx context.Context) error {
	if err := t.checkAuthenticated(ctx); err != nil {
		return err
	}
	var repo apiRepo
	found, err := t.verifyRepository(ctx, &repo)
	if err != nil {
		return err
	}
	if !found {
		t.repoState = "not found (run prepare to create the private target)"
		return nil
	}
	snapshot, err := t.managedSnapshot(ctx)
	if err != nil {
		return err
	}
	if len(snapshot.missing) > 0 {
		t.repoState = fmt.Sprintf("repository exists; %d of %d managed files are missing (run prepare)", len(snapshot.missing), snapshot.total)
		return nil
	}
	if len(snapshot.outdated) > 0 {
		t.repoState = fmt.Sprintf("repository exists; %d managed workflow files are outdated (run prepare)", len(snapshot.outdated))
		return nil
	}
	for _, file := range []string{t.cfg.WorkflowFile, ciWorkflowFile} {
		var workflow struct {
			State string `json:"state"`
		}
		workflowPath := repositoryPath(t.cfg.Owner, t.cfg.Repo) + "/actions/workflows/" + url.PathEscape(file)
		status, err := t.api.request(ctx, http.MethodGet, workflowPath, nil, &workflow, http.StatusOK, http.StatusNotFound)
		if err != nil {
			return fmt.Errorf("verify workflow %s is registered: %w", file, err)
		}
		if status == http.StatusNotFound {
			t.repoState = fmt.Sprintf("workflow %s is committed but not registered yet", file)
			return nil
		}
		if workflow.State != "active" {
			return fmt.Errorf("workflow %s state is %q, not active", file, workflow.State)
		}
	}
	t.ready = true
	t.repoState = fmt.Sprintf("private repository, Actions enabled, %d managed files and both workflows verified", snapshot.total)
	return nil
}

func (t *target) prepare(ctx context.Context) error {
	if err := t.checkAuthenticated(ctx); err != nil {
		return err
	}
	var repo apiRepo
	found, err := t.verifyRepository(ctx, &repo)
	if found && err != nil {
		return fmt.Errorf("refusing to prepare repository: %w", err)
	}
	if err != nil {
		return err
	}
	if !found {
		var owner struct {
			Type string `json:"type"`
		}
		ownerPath := "/users/" + url.PathEscape(t.cfg.Owner)
		if _, err := t.api.request(ctx, http.MethodGet, ownerPath, nil, &owner, http.StatusOK); err != nil {
			return fmt.Errorf("resolve repository owner: %w", err)
		}
		createPath := "/user/repos"
		if strings.EqualFold(owner.Type, "Organization") {
			createPath = "/orgs/" + url.PathEscape(t.cfg.Owner) + "/repos"
		}
		if _, err := t.api.request(ctx, http.MethodPost, createPath, map[string]any{
			"name":        t.cfg.Repo,
			"private":     true,
			"auto_init":   true,
			"description": "Disposable private Actions load-test target",
		}, &repo, http.StatusCreated); err != nil {
			return fmt.Errorf("create private test repository: %w", err)
		}
		if found, err = t.verifyRepository(ctx, &repo); err != nil || !found {
			return errors.Join(errors.New("verify created test repository"), err)
		}
	}
	if err := t.installManagedFiles(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := t.check(ctx); err != nil {
			return fmt.Errorf("verify prepared repository: %w", err)
		}
		if t.ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("prepared repository is not ready: %s", t.repoState)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

var gitObjectID = regexp.MustCompile(`^[0-9a-f]{40}$`)

type repoSnapshot struct {
	commit  string
	tree    string
	missing []repoFile
	// outdated lists workflow files this tool generated earlier whose content
	// is stale; prepare replaces them.
	outdated []repoFile
	total    int
}

// managedSnapshot compares the managed files with the default branch's tree.
// A managed workflow file whose header still matches the generated workflow is
// outdated; any other difference is a conflict that prepare never overwrites.
func (t *target) managedSnapshot(ctx context.Context) (repoSnapshot, error) {
	base := repositoryPath(t.cfg.Owner, t.cfg.Repo)
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	status, err := t.api.request(ctx, http.MethodGet, base+"/git/ref/heads/"+escapeAPIPath(t.cfg.Ref), nil, &ref, http.StatusOK, http.StatusNotFound, http.StatusConflict)
	if err != nil {
		return repoSnapshot{}, fmt.Errorf("read branch %s: %w", t.cfg.Ref, err)
	}
	if status != http.StatusOK || !gitObjectID.MatchString(ref.Object.SHA) {
		return repoSnapshot{}, fmt.Errorf("branch %s has no commits; initialize the repository first", t.cfg.Ref)
	}
	var commit struct {
		Tree struct {
			SHA string `json:"sha"`
		} `json:"tree"`
	}
	if _, err := t.api.request(ctx, http.MethodGet, base+"/git/commits/"+ref.Object.SHA, nil, &commit, http.StatusOK); err != nil {
		return repoSnapshot{}, fmt.Errorf("read branch head commit: %w", err)
	}
	if !gitObjectID.MatchString(commit.Tree.SHA) {
		return repoSnapshot{}, errors.New("branch head commit has an invalid tree")
	}
	var tree struct {
		Truncated bool `json:"truncated"`
		Tree      []struct {
			Path string `json:"path"`
			Type string `json:"type"`
			SHA  string `json:"sha"`
		} `json:"tree"`
	}
	if _, err := t.api.request(ctx, http.MethodGet, base+"/git/trees/"+commit.Tree.SHA+"?recursive=1", nil, &tree, http.StatusOK); err != nil {
		return repoSnapshot{}, fmt.Errorf("read repository tree: %w", err)
	}
	if tree.Truncated {
		return repoSnapshot{}, errors.New("repository tree is too large to verify; use a dedicated test repository")
	}
	existing := make(map[string]string, len(tree.Tree))
	for _, entry := range tree.Tree {
		existing[entry.Path] = entry.Type + " " + entry.SHA
	}
	files, err := managedFiles(t.cfg)
	if err != nil {
		return repoSnapshot{}, err
	}
	snapshot := repoSnapshot{commit: ref.Object.SHA, tree: commit.Tree.SHA, total: len(files)}
	for _, file := range files {
		entry, ok := existing[file.Path]
		if !ok {
			snapshot.missing = append(snapshot.missing, file)
			continue
		}
		if entry != "blob "+gitBlobSHA(file.Content) {
			generated, err := t.generatedWorkflow(ctx, file, entry)
			if err != nil {
				return repoSnapshot{}, err
			}
			if !generated {
				return repoSnapshot{}, fmt.Errorf("%s differs from the generated load-test content; refusing to run or overwrite it", file.Path)
			}
			snapshot.outdated = append(snapshot.outdated, file)
		}
	}
	return snapshot, nil
}

// generatedWorkflow reports whether the existing tree entry for a managed
// workflow file starts with the generated name and run-name lines, so an
// earlier version of this tool wrote it.
func (t *target) generatedWorkflow(ctx context.Context, file repoFile, entry string) (bool, error) {
	if file.Path != t.cfg.WorkflowFile && file.Path != ciWorkflowFile {
		return false, nil
	}
	blobSHA, ok := strings.CutPrefix(entry, "blob ")
	if !ok || !gitObjectID.MatchString(blobSHA) {
		return false, nil
	}
	var blob struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if _, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo)+"/git/blobs/"+blobSHA, nil, &blob, http.StatusOK); err != nil {
		return false, fmt.Errorf("read existing %s: %w", file.Path, err)
	}
	if blob.Encoding != "base64" {
		return false, nil
	}
	data, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(blob.Content, "\n", ""))
	if err != nil {
		return false, nil
	}
	return workflowHeader(string(data)) == workflowHeader(file.Content), nil
}

func workflowHeader(content string) string {
	lines := strings.SplitN(content, "\n", 3)
	if len(lines) < 3 {
		return ""
	}
	return lines[0] + "\n" + lines[1]
}

// installManagedFiles adds every missing managed file and replaces outdated
// generated workflows in one fast-forward commit.
func (t *target) installManagedFiles(ctx context.Context) error {
	snapshot, err := t.managedSnapshot(ctx)
	if err != nil {
		return err
	}
	if len(snapshot.missing) == 0 && len(snapshot.outdated) == 0 {
		return nil
	}
	base := repositoryPath(t.cfg.Owner, t.cfg.Repo)
	entries := make([]map[string]string, 0, len(snapshot.missing)+len(snapshot.outdated))
	for _, file := range append(snapshot.missing, snapshot.outdated...) {
		entries = append(entries, map[string]string{"path": file.Path, "mode": "100644", "type": "blob", "content": file.Content})
	}
	var created struct {
		SHA string `json:"sha"`
	}
	if _, err := t.api.request(ctx, http.MethodPost, base+"/git/trees", map[string]any{"base_tree": snapshot.tree, "tree": entries}, &created, http.StatusCreated); err != nil {
		return fmt.Errorf("create tree with load-test files: %w", err)
	}
	if !gitObjectID.MatchString(created.SHA) {
		return errors.New("created tree has an invalid object ID")
	}
	treeSHA := created.SHA
	created.SHA = ""
	if _, err := t.api.request(ctx, http.MethodPost, base+"/git/commits", map[string]any{
		"message": fmt.Sprintf("Install Actions load-test workflows and CI project (%d added, %d updated)", len(snapshot.missing), len(snapshot.outdated)),
		"tree":    treeSHA,
		"parents": []string{snapshot.commit},
	}, &created, http.StatusCreated); err != nil {
		return fmt.Errorf("create load-test commit: %w", err)
	}
	if !gitObjectID.MatchString(created.SHA) {
		return errors.New("created commit has an invalid object ID")
	}
	if _, err := t.api.request(ctx, http.MethodPatch, base+"/git/refs/heads/"+escapeAPIPath(t.cfg.Ref), map[string]any{"sha": created.SHA, "force": false}, nil, http.StatusOK); err != nil {
		return fmt.Errorf("fast-forward %s with load-test files (re-run prepare if the branch moved): %w", t.cfg.Ref, err)
	}
	return nil
}

func repositoryPath(owner, repo string) string {
	return "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo)
}

func escapeAPIPath(value string) string {
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func (a *ghesAPI) dispatch(ctx context.Context, cfg config, workflowFile, marker, runnerLabel string, shards, failures []int, duration time.Duration, logBytes int) (int64, error) {
	shardJSON, _ := json.Marshal(shards)
	failJSON, _ := json.Marshal(failures)
	inputs := map[string]string{
		"run_id":           marker,
		"shards":           string(shardJSON),
		"failure_shards":   string(failJSON),
		"duration_seconds": strconv.Itoa(int(duration.Seconds())),
		"log_bytes":        strconv.Itoa(logBytes),
	}
	if runnerLabel != "" {
		inputs["runner_label"] = runnerLabel
	}
	var out struct {
		WorkflowRunID int64 `json:"workflow_run_id"`
	}
	path := repositoryPath(cfg.Owner, cfg.Repo) + "/actions/workflows/" + url.PathEscape(workflowFile) + "/dispatches"
	status, err := a.request(ctx, http.MethodPost, path, map[string]any{"ref": cfg.Ref, "inputs": inputs}, &out, http.StatusOK, http.StatusNoContent)
	if err != nil {
		return 0, err
	}
	if status == http.StatusOK {
		return out.WorkflowRunID, nil
	}
	return 0, nil
}

func workflowYAML(scaleSetName string) string {
	return strings.ReplaceAll(`name: Synthetic Actions load
run-name: Actions load ${{ inputs.run_id }}
on:
  workflow_dispatch:
    inputs:
      run_id:
        description: Correlation identifier
        required: true
        type: string
      shards:
        description: JSON array of bounded shard numbers
        required: true
        type: string
      failure_shards:
        description: JSON array of shards that intentionally fail
        required: true
        type: string
      duration_seconds:
        description: Bounded synthetic work duration
        required: true
        type: string
      log_bytes:
        description: Bounded synthetic log output
        required: true
        type: string
      runner_label:
        description: Runner scale set label that runs every job
        required: false
        type: string
        default: '__SCALE_SET_NAME__'
jobs:
  synthetic:
    strategy:
      fail-fast: false
      matrix:
        shard: ${{ fromJSON(inputs.shards) }}
    runs-on: ${{ inputs.runner_label }}
    steps:
      - name: Synthetic work and output
        id: work
        shell: bash
        env:
          RUN_ID: ${{ inputs.run_id }}
          SHARD: ${{ matrix.shard }}
          DURATION: ${{ inputs.duration_seconds }}
          LOG_BYTES: ${{ inputs.log_bytes }}
        run: |
          set -euo pipefail
          [[ "$RUN_ID" =~ ^[A-Za-z0-9._-]{1,64}$ ]]
          [[ "$SHARD" =~ ^[0-9]{1,3}$ ]]
          [[ "$DURATION" =~ ^[0-9]{1,3}$ ]] && (( DURATION <= 600 ))
          [[ "$LOG_BYTES" =~ ^[0-9]{1,7}$ ]] && (( LOG_BYTES <= 1048576 ))
          printf 'run=%s shard=%s\n' "$RUN_ID" "$SHARD"
          printf 'marker=%s:%s\n' "$RUN_ID" "$SHARD" >> "$GITHUB_OUTPUT"
          printf 'Synthetic output marker: %s:%s\n' "$RUN_ID" "$SHARD"
          if (( LOG_BYTES > 0 )); then
            head -c "$LOG_BYTES" /dev/zero | tr '\0' 'x'
            printf '\n'
          fi
          sleep "$DURATION"
      - name: Verify synthetic output
        shell: bash
        env:
          RUN_ID: ${{ inputs.run_id }}
          SHARD: ${{ matrix.shard }}
          OUTPUT_MARKER: ${{ steps.work.outputs.marker }}
        run: |
          set -euo pipefail
          [[ "$OUTPUT_MARKER" == "$RUN_ID:$SHARD" ]]
          printf 'Verified output marker: %s\n' "$OUTPUT_MARKER" >> "$GITHUB_STEP_SUMMARY"
      - name: Intentional synthetic failure
        if: ${{ contains(fromJSON(inputs.failure_shards), matrix.shard) }}
        shell: bash
        env:
          SHARD: ${{ matrix.shard }}
        run: |
          echo "Intentional synthetic failure for shard $SHARD"
          exit 1
`, "__SCALE_SET_NAME__", scaleSetName)
}

func (a *ghesAPI) workflowRuns(ctx context.Context, cfg config, workflowFile string, since time.Time, page int) ([]apiWorkflowRun, error) {
	path := repositoryPath(cfg.Owner, cfg.Repo) + "/actions/workflows/" + url.PathEscape(workflowFile) + "/runs"
	query := url.Values{}
	query.Set("per_page", "100")
	query.Set("page", strconv.Itoa(page))
	query.Set("created", ">="+since.UTC().Format("2006-01-02"))
	var out struct {
		WorkflowRuns []apiWorkflowRun `json:"workflow_runs"`
	}
	if _, err := a.request(ctx, http.MethodGet, path+"?"+query.Encode(), nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out.WorkflowRuns, nil
}

func (a *ghesAPI) workflowJobs(ctx context.Context, cfg config, runID int64) ([]apiJob, error) {
	path := repositoryPath(cfg.Owner, cfg.Repo) + fmt.Sprintf("/actions/runs/%d/jobs?per_page=100", runID)
	var out struct {
		Jobs []apiJob `json:"jobs"`
	}
	if _, err := a.request(ctx, http.MethodGet, path, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func (a *ghesAPI) cancelRun(ctx context.Context, cfg config, runID int64) error {
	path := repositoryPath(cfg.Owner, cfg.Repo) + fmt.Sprintf("/actions/runs/%d/cancel", runID)
	_, err := a.request(ctx, http.MethodPost, path, nil, nil, http.StatusAccepted, http.StatusConflict)
	return err
}
