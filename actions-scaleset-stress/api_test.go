package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDispatchSendsRunnerLabelOnlyWhenSet(t *testing.T) {
	var mu sync.Mutex
	var inputs []map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Inputs map[string]string `json:"inputs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		inputs = append(inputs, body.Inputs)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	api := newGHESAPI(server.URL, "test-token", server.Client())
	cfg := config{Owner: "lab", Repo: "load", Ref: "main"}
	for _, label := range []string{"", "load-scale-g1-real"} {
		if _, err := api.dispatch(context.Background(), cfg, "synthetic-load.yml", "marker", label, []int{0}, nil, time.Second, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := inputs[0]["runner_label"]; ok {
		t.Errorf("an empty label must omit runner_label, got inputs %v", inputs[0])
	}
	if got := inputs[1]["runner_label"]; got != "load-scale-g1-real" {
		t.Errorf("runner_label = %q, want the pool label", got)
	}
}

// fakeRepo serves the git data endpoints prepare uses. Every managed file is
// current except the synthetic workflow, whose committed content is old.
type fakeRepo struct {
	mu     sync.Mutex
	files  []repoFile
	old    string
	posted []map[string]any
}

const (
	fakeCommitSHA  = "1111111111111111111111111111111111111111"
	fakeTreeSHA    = "2222222222222222222222222222222222222222"
	fakeNewTreeSHA = "3333333333333333333333333333333333333333"
	fakeNewCommit  = "4444444444444444444444444444444444444444"
)

func (f *fakeRepo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	const base = "/repos/lab/load"
	oldSHA := gitBlobSHA(f.old)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == base+"/git/ref/heads/main":
		_ = json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"sha": fakeCommitSHA}})
	case r.Method == http.MethodGet && r.URL.Path == base+"/git/commits/"+fakeCommitSHA:
		_ = json.NewEncoder(w).Encode(map[string]any{"tree": map[string]string{"sha": fakeTreeSHA}})
	case r.Method == http.MethodGet && r.URL.Path == base+"/git/trees/"+fakeTreeSHA:
		var entries []map[string]string
		for _, file := range f.files {
			sha := gitBlobSHA(file.Content)
			if file.Path == ".github/workflows/synthetic-load.yml" {
				sha = oldSHA
			}
			entries = append(entries, map[string]string{"path": file.Path, "type": "blob", "sha": sha})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tree": entries})
	case r.Method == http.MethodGet && r.URL.Path == base+"/git/blobs/"+oldSHA:
		_ = json.NewEncoder(w).Encode(map[string]string{"content": base64.StdEncoding.EncodeToString([]byte(f.old)), "encoding": "base64"})
	case r.Method == http.MethodPost && r.URL.Path == base+"/git/trees":
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.posted = append(f.posted, body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"sha": fakeNewTreeSHA})
	case r.Method == http.MethodPost && r.URL.Path == base+"/git/commits":
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"sha": fakeNewCommit})
	case r.Method == http.MethodPatch && r.URL.Path == base+"/git/refs/heads/main":
		_ = json.NewEncoder(w).Encode(map[string]string{})
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
	}
}

func TestInstallManagedFilesUpdatesOutdatedWorkflow(t *testing.T) {
	cfg := config{Owner: "lab", Repo: "load", Ref: "main", WorkflowFile: ".github/workflows/synthetic-load.yml", ScaleSetName: "load-scale"}
	files, err := managedFiles(cfg)
	if err != nil {
		t.Fatal(err)
	}
	current := workflowYAML(cfg.ScaleSetName)
	for name, tc := range map[string]struct {
		old     string
		updates bool
	}{
		"earlier generated version": {strings.ReplaceAll(current, "runs-on: ${{ inputs.runner_label }}", "runs-on: load-scale"), true},
		"foreign workflow":          {"name: Somebody else's workflow\non: push\njobs: {}\n", false},
	} {
		t.Run(name, func(t *testing.T) {
			repo := &fakeRepo{files: files, old: tc.old}
			server := httptest.NewServer(repo)
			defer server.Close()
			tgt := &target{api: newGHESAPI(server.URL, "test-token", server.Client()), cfg: cfg}
			err := tgt.installManagedFiles(context.Background())
			if !tc.updates {
				if err == nil || !strings.Contains(err.Error(), "refusing to run or overwrite it") || len(repo.posted) != 0 {
					t.Fatalf("foreign workflow: error %v, %d trees posted", err, len(repo.posted))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(repo.posted) != 1 {
				t.Fatalf("posted %d trees, want 1", len(repo.posted))
			}
			entries, _ := repo.posted[0]["tree"].([]any)
			if len(entries) != 1 {
				t.Fatalf("tree has %d entries, want only the outdated workflow", len(entries))
			}
			entry, _ := entries[0].(map[string]any)
			if entry["path"] != cfg.WorkflowFile || entry["content"] != current || repo.posted[0]["base_tree"] != fakeTreeSHA {
				t.Fatalf("tree entry %v based on %v", entry["path"], repo.posted[0]["base_tree"])
			}
		})
	}
}
