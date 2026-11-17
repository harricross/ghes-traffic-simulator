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
	FullName      string `json:"full_name"`
	Private       bool   `json:"private"`
	DefaultBranch string `json:"default_branch"`
}

type apiContent struct {
	SHA      string `json:"sha"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
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
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
	Steps      []struct {
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
	t.repoState = "not yet checked"
	t.repoState = fmt.Sprintf("ready for GHES %s", version)
	return nil
}

func (t *target) check(ctx context.Context) error {
	if err := t.checkAuthenticated(ctx); err != nil {
		return err
	}
	var repo apiRepo
	status, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo), nil, &repo, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		t.repoState = "not found (run prepare to create the private target)"
		return nil
	}
	if !repo.Private || !strings.EqualFold(repo.FullName, t.cfg.Owner+"/"+t.cfg.Repo) {
		return errors.New("configured repository is not the expected private target")
	}
	if repo.DefaultBranch != t.cfg.Ref {
		return fmt.Errorf("configured ref %q does not match repository default branch %q", t.cfg.Ref, repo.DefaultBranch)
	}
	var permissions struct {
		Enabled bool `json:"enabled"`
	}
	if _, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo)+"/actions/permissions", nil, &permissions, http.StatusOK); err != nil {
		return fmt.Errorf("read repository Actions permissions: %w", err)
	}
	if !permissions.Enabled {
		return errors.New("GitHub Actions is disabled for the target repository")
	}
	contentURL := repositoryPath(t.cfg.Owner, t.cfg.Repo) + "/contents/" + escapeAPIPath(t.cfg.WorkflowFile) + "?ref=" + url.QueryEscape(t.cfg.Ref)
	var content apiContent
	status, err = t.api.request(ctx, http.MethodGet, contentURL, nil, &content, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		t.repoState = "repository exists; workflow is missing (run prepare)"
		return nil
	}
	if content.Encoding != "base64" {
		return errors.New("workflow file has an unsupported content encoding")
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(content.Content), ""))
	if err != nil {
		return fmt.Errorf("decode workflow file: %w", err)
	}
	if string(decoded) != workflowYAML(t.cfg.ScaleSetName) {
		return errors.New("workflow file differs from the generated Actions load template; refusing to run")
	}
	var workflow struct {
		State string `json:"state"`
	}
	workflowPath := repositoryPath(t.cfg.Owner, t.cfg.Repo) + "/actions/workflows/" + url.PathEscape(t.cfg.WorkflowFile)
	if _, err := t.api.request(ctx, http.MethodGet, workflowPath, nil, &workflow, http.StatusOK); err != nil {
		return fmt.Errorf("verify workflow is registered: %w", err)
	}
	if workflow.State != "active" {
		return fmt.Errorf("workflow state is %q, not active", workflow.State)
	}
	t.repoState = "private repository, Actions enabled, workflow verified"
	return nil
}

func (t *target) prepare(ctx context.Context) error {
	if err := t.checkAuthenticated(ctx); err != nil {
		return err
	}
	var repo apiRepo
	status, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo), nil, &repo, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
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
	}
	if !repo.Private || !strings.EqualFold(repo.FullName, t.cfg.Owner+"/"+t.cfg.Repo) {
		return errors.New("refusing to prepare a repository that is not the expected private target")
	}
	if repo.DefaultBranch == "" {
		return errors.New("repository has no default branch")
	}
	if repo.DefaultBranch != t.cfg.Ref {
		return fmt.Errorf("configured ref %q must match repository default branch %q for the prepared workflow", t.cfg.Ref, repo.DefaultBranch)
	}
	var permissions struct {
		Enabled bool `json:"enabled"`
	}
	if _, err := t.api.request(ctx, http.MethodGet, repositoryPath(t.cfg.Owner, t.cfg.Repo)+"/actions/permissions", nil, &permissions, http.StatusOK); err != nil {
		return fmt.Errorf("read repository Actions permissions: %w", err)
	}
	if !permissions.Enabled {
		return errors.New("GitHub Actions is disabled for the target repository; enable it through the approved repository settings")
	}

	workflow := workflowYAML(t.cfg.ScaleSetName)
	contentPath := t.cfg.WorkflowFile
	contentURL := repositoryPath(t.cfg.Owner, t.cfg.Repo) + "/contents/" + escapeAPIPath(contentPath)
	var existing apiContent
	status, err = t.api.request(ctx, http.MethodGet, contentURL+"?ref="+url.QueryEscape(repo.DefaultBranch), nil, &existing, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		if existing.Encoding != "base64" {
			return errors.New("existing workflow content has an unsupported encoding")
		}
		decoded, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(existing.Content), ""))
		if err != nil {
			return fmt.Errorf("decode existing workflow content: %w", err)
		}
		if string(decoded) != workflow {
			return errors.New("refusing to overwrite an existing workflow that differs from the generated test workflow")
		}
	} else {
		payload := map[string]any{
			"message": "Add synthetic Actions load workflow",
			"content": base64.StdEncoding.EncodeToString([]byte(workflow)),
			"branch":  repo.DefaultBranch,
		}
		if _, err := t.api.request(ctx, http.MethodPut, contentURL, payload, nil, http.StatusCreated); err != nil {
			return fmt.Errorf("install synthetic workflow: %w", err)
		}
	}
	if err := t.check(ctx); err != nil {
		return fmt.Errorf("verify prepared repository and workflow: %w", err)
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

func (a *ghesAPI) dispatch(ctx context.Context, cfg config, marker string, shards, failures []int, duration time.Duration, logBytes int) (int64, error) {
	shardJSON, _ := json.Marshal(shards)
	failJSON, _ := json.Marshal(failures)
	inputs := map[string]string{
		"run_id":           marker,
		"shards":           string(shardJSON),
		"failure_shards":   string(failJSON),
		"duration_seconds": strconv.Itoa(int(duration.Seconds())),
		"log_bytes":        strconv.Itoa(logBytes),
	}
	var out struct {
		WorkflowRunID int64 `json:"workflow_run_id"`
	}
	path := repositoryPath(cfg.Owner, cfg.Repo) + "/actions/workflows/" + url.PathEscape(cfg.WorkflowFile) + "/dispatches"
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
jobs:
  synthetic:
    strategy:
      fail-fast: false
      matrix:
        shard: ${{ fromJSON(inputs.shards) }}
    runs-on: __SCALE_SET_NAME__
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

func (a *ghesAPI) workflowRuns(ctx context.Context, cfg config, since time.Time, page int) ([]apiWorkflowRun, error) {
	path := repositoryPath(cfg.Owner, cfg.Repo) + "/actions/workflows/" + url.PathEscape(cfg.WorkflowFile) + "/runs"
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
