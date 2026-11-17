package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkflowYAMLPreservesShellFormatsAndVerifiesOutput(t *testing.T) {
	workflow := workflowYAML("load-scale")
	for _, expected := range []string{
		"runs-on: ${{ inputs.runner_label }}",
		"printf 'run=%s shard=%s\\n'",
		"printf 'marker=%s:%s\\n'",
		"- name: Verify synthetic output",
		"[[ \"$OUTPUT_MARKER\" == \"$RUN_ID:$SHARD\" ]]",
		"- name: Intentional synthetic failure",
	} {
		if !strings.Contains(workflow, expected) {
			t.Errorf("generated workflow is missing %q", expected)
		}
	}
	if strings.Contains(workflow, "%!s(") {
		t.Fatal("generated workflow contains fmt formatting corruption")
	}
}

type testStep = struct {
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

func testJob(name, conclusion string, steps ...string) apiJob {
	job := apiJob{Name: name, Conclusion: conclusion}
	for i := 0; i+1 < len(steps); i += 2 {
		job.Steps = append(job.Steps, testStep{Name: steps[i], Conclusion: steps[i+1]})
	}
	return job
}

func TestVerifySyntheticJobs(t *testing.T) {
	job := func(name, conclusion, work, verify, intentional string) apiJob {
		return testJob(name, conclusion,
			"Synthetic work and output", work,
			"Verify synthetic output", verify,
			"Intentional synthetic failure", intentional)
	}

	t.Run("success with GHES matrix names", func(t *testing.T) {
		jobs := []apiJob{
			job("synthetic (0)", "success", "success", "success", "skipped"),
			job("synthetic (1)", "success", "success", "success", "skipped"),
		}
		if ok, reason := verifySyntheticJobs(jobs, 2, 0); !ok {
			t.Fatalf("expected valid successful workflow, got %q", reason)
		}
	})

	t.Run("intentional failure after output verification", func(t *testing.T) {
		jobs := []apiJob{
			job("synthetic (shard: 0)", "failure", "success", "success", "failure"),
			job("synthetic (shard: 1)", "success", "success", "success", "skipped"),
		}
		if ok, reason := verifySyntheticJobs(jobs, 2, 1); !ok {
			t.Fatalf("expected valid intentional failure, got %q", reason)
		}
	})

	t.Run("fails if output was not verified", func(t *testing.T) {
		jobs := []apiJob{job("synthetic (0)", "failure", "success", "failure", "failure")}
		if ok, _ := verifySyntheticJobs(jobs, 1, 1); ok {
			t.Fatal("expected output verification failure")
		}
	})

	t.Run("rejects duplicate shards", func(t *testing.T) {
		jobs := []apiJob{
			job("synthetic (0)", "success", "success", "success", "skipped"),
			job("synthetic (shard: 0)", "success", "success", "success", "skipped"),
		}
		if ok, _ := verifySyntheticJobs(jobs, 2, 0); ok {
			t.Fatal("expected duplicate shard to be rejected")
		}
	})
}

func TestJobShard(t *testing.T) {
	for name, want := range map[string]int{"synthetic (0)": 0, "synthetic (shard: 12)": 12} {
		if got, err := jobShard(name, "synthetic"); err != nil || got != want {
			t.Errorf("jobShard(%q) = %d, %v; want %d", name, got, err, want)
		}
	}
	for _, name := range []string{"synthetic", "other (0)", "synthetic (x)"} {
		if _, err := jobShard(name, "synthetic"); err == nil {
			t.Errorf("jobShard(%q) unexpectedly succeeded", name)
		}
	}
}

func TestRunFailsSpreadsFailuresEvenly(t *testing.T) {
	for _, tc := range []struct {
		percent int
		want    string
	}{
		{0, "........"},
		{25, "...x...x"},
		{50, ".x.x.x.x"},
		{100, "xxxxxxxx"},
	} {
		var got strings.Builder
		for i := 0; i < 8; i++ {
			if runFails(i, tc.percent) {
				got.WriteByte('x')
			} else {
				got.WriteByte('.')
			}
		}
		if got.String() != tc.want {
			t.Errorf("percent %d: got %s, want %s", tc.percent, got.String(), tc.want)
		}
	}
}

func TestVerifyCIJobs(t *testing.T) {
	lint := testJob("Lint", "success", "Check out repository", "success", "Lint sources", "success")
	test := func(shard int, conclusion, intentional string) apiJob {
		return testJob("Test shard "+strconv.Itoa(shard), conclusion,
			"Check out repository", "success",
			"Run unit tests", "success",
			"Verify test report", "success",
			"Intentional check failure", intentional)
	}
	build := testJob("Build", "success", "Check out repository", "success", "Build package", "success", "Verify package", "success")

	t.Run("all checks pass", func(t *testing.T) {
		jobs := []apiJob{lint, test(0, "success", "skipped"), test(1, "success", "skipped"), build}
		if ok, reason := verifyCIJobs(jobs, 2, 0); !ok {
			t.Fatalf("expected passing CI run, got %q", reason)
		}
	})

	t.Run("intentional failure skips or omits Build", func(t *testing.T) {
		failing := []apiJob{lint, test(0, "failure", "failure"), test(1, "success", "skipped")}
		if ok, reason := verifyCIJobs(failing, 2, 1); !ok {
			t.Fatalf("expected intentional failure without Build, got %q", reason)
		}
		skipped := append(failing, testJob("Build", "skipped"))
		if ok, reason := verifyCIJobs(skipped, 2, 1); !ok {
			t.Fatalf("expected intentional failure with skipped Build, got %q", reason)
		}
	})

	t.Run("rejects unexpected failures and missing jobs", func(t *testing.T) {
		for name, jobs := range map[string][]apiJob{
			"failed test":   {lint, test(0, "failure", "skipped"), build},
			"missing build": {lint, test(0, "success", "skipped")},
			"failed lint":   {testJob("Lint", "failure", "Lint sources", "failure"), testJob("Test shard ${{ matrix.shard }}", "skipped")},
			"unknown job":   {lint, test(0, "success", "skipped"), build, testJob("Deploy", "success")},
		} {
			if ok, _ := verifyCIJobs(jobs, 1, 0); ok {
				t.Errorf("%s: expected verification failure", name)
			}
		}
	})
}

func TestCIWorkflowMatchesVerifier(t *testing.T) {
	workflow := ciWorkflowYAML("load-scale")
	for _, expected := range []string{
		"runs-on: ${{ inputs.runner_label }}",
		"name: Lint",
		"name: Test shard ${{ matrix.shard }}",
		"name: Build",
		"- name: Check out repository",
		"- name: Lint sources",
		"- name: Run unit tests",
		"- name: Verify test report",
		"- name: Intentional check failure",
		"- name: Build package",
		"- name: Verify package",
		"working-directory: " + ciProjectDir,
	} {
		if !strings.Contains(workflow, expected) {
			t.Errorf("CI workflow is missing %q", expected)
		}
	}
}

func TestGitBlobSHA(t *testing.T) {
	if got := gitBlobSHA("hello\n"); got != "ce013625030ba8dba906f756967f9e9ca394464a" {
		t.Fatalf("gitBlobSHA = %s", got)
	}
}

func TestManagedFilesIncludeCIProject(t *testing.T) {
	files, err := managedFiles(config{WorkflowFile: ".github/workflows/synthetic-load.yml", ScaleSetName: "load-scale"})
	if err != nil {
		t.Fatal(err)
	}
	paths := make(map[string]bool, len(files))
	for _, file := range files {
		if strings.Contains(file.Path, "__pycache__") || strings.HasSuffix(file.Path, ".pyc") {
			t.Errorf("managed files include build output %s", file.Path)
		}
		paths[file.Path] = true
	}
	for _, want := range []string{
		".github/workflows/synthetic-load.yml", ciWorkflowFile,
		"ci/scripts/lint.sh", "ci/scripts/build.sh", "ci/tests/run_shard.py", "ci/src/loadlab/__init__.py",
	} {
		if !paths[want] {
			t.Errorf("managed files missing %s", want)
		}
	}
}

func TestCIProjectShardsAllRunTests(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	files, err := ciProjectFiles()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, file := range files {
		name := filepath.Join(root, filepath.FromSlash(file.Path))
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(file.Content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	project := filepath.Join(root, ciProjectDir)
	const total = 25
	sum := 0
	for shard := 0; shard < total; shard++ {
		report := filepath.Join(t.TempDir(), "report.json")
		cmd := exec.Command(python, "tests/run_shard.py", "--shard", strconv.Itoa(shard), "--total", strconv.Itoa(total), "--report", report)
		cmd.Dir = project
		cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("shard %d failed: %v\n%s", shard, err, out)
		}
		var result struct {
			TestsRun int `json:"tests_run"`
			Failures int `json:"failures"`
			Errors   int `json:"errors"`
		}
		data, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.TestsRun == 0 || result.Failures != 0 || result.Errors != 0 {
			t.Fatalf("shard %d report %+v", shard, result)
		}
		sum += result.TestsRun
	}
	if sum < total {
		t.Fatalf("shards ran %d tests in total", sum)
	}
}

func TestWorkflowsDefaultRunnerLabelToScaleSet(t *testing.T) {
	input := "      runner_label:\n" +
		"        description: Runner scale set label that runs every job\n" +
		"        required: false\n" +
		"        type: string\n" +
		"        default: 'load-scale'\n"
	for name, workflow := range map[string]string{"synthetic": workflowYAML("load-scale"), "ci": ciWorkflowYAML("load-scale")} {
		if !strings.Contains(workflow, input) {
			t.Errorf("%s workflow lacks the optional runner_label input defaulting to the scale set", name)
		}
		if strings.Contains(workflow, "__SCALE_SET_NAME__") {
			t.Errorf("%s workflow has an unreplaced scale set placeholder", name)
		}
		if all, labelled := strings.Count(workflow, "runs-on:"), strings.Count(workflow, "runs-on: ${{ inputs.runner_label }}"); all == 0 || all != labelled {
			t.Errorf("%s workflow: %d of %d jobs use the runner_label input", name, labelled, all)
		}
	}
}

func TestVerifyWorkflowRunChecksGroupPlacement(t *testing.T) {
	job := func(runner, group string) apiJob {
		job := testJob("synthetic (0)", "success", "Synthetic work and output", "success", "Verify synthetic output", "success")
		job.RunnerName, job.RunnerGroupName = runner, group
		return job
	}
	own := fleetRunnerName("run", "stress-g01", fleetKindReal, 1)
	other := fleetRunnerName("run", "stress-g02", fleetKindReal, 1)
	suffixed := fleetRunnerName("run", "x-stress-g01", fleetKindReal, 1)
	otherRun := fleetRunnerName("other-run", "stress-g01", fleetKindReal, 1)
	record := workflowRecord{Workload: workloadSynthetic, Shards: 1, Group: "stress-g01"}
	for name, tc := range map[string]struct {
		job  apiJob
		want bool
	}{
		"group name matches":             {job(own, "stress-g01"), true},
		"runner name only":               {job(own, ""), true},
		"group name differs":             {job(own, "stress-g02"), false},
		"runner from other group":        {job(other, ""), false},
		"runner from suffix-named group": {job(suffixed, ""), false},
		"runner from other run":          {job(otherRun, ""), false},
		"fake runner of the group":       {job(fleetRunnerName("run", "stress-g01", fleetKindFake, 1), ""), false},
		"ran with no runner names":       {job("", ""), false},
	} {
		ok, reason := verifyWorkflowRun(record, "run", []apiJob{tc.job})
		if ok != tc.want {
			t.Errorf("%s: verified %v (%s), want %v", name, ok, reason, tc.want)
		}
		if !ok && !strings.Contains(reason, "stress-g01") {
			t.Errorf("%s: reason %q does not name the expected group", name, reason)
		}
	}
	legacy := record
	legacy.Group = ""
	if ok, reason := verifyWorkflowRun(legacy, "run", []apiJob{job(other, "Default")}); !ok {
		t.Errorf("records without a group must not check placement: %s", reason)
	}
	skipped := testJob("Build", "skipped")
	if ok, reason := verifyJobPlacement([]apiJob{skipped}, "run", "stress-g01"); !ok {
		t.Errorf("a skipped job has no runner and must pass placement: %s", reason)
	}
}

// fakeActions serves the dispatch, run listing and job endpoints that
// dispatchAndObserve uses. Every dispatched run completes at once with one
// synthetic job on a runner of the group its runner_label names.
type fakeActions struct {
	mu     sync.Mutex
	groups map[string]string
	labels []string
	runs   []apiWorkflowRun
	jobs   map[int64]apiJob
}

func (f *fakeActions) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/dispatches"):
		var body struct {
			Inputs map[string]string `json:"inputs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		label := body.Inputs["runner_label"]
		f.labels = append(f.labels, label)
		id := int64(1000 + len(f.runs))
		f.runs = append(f.runs, apiWorkflowRun{ID: id, DisplayTitle: "Actions load " + body.Inputs["run_id"], Status: "completed", Conclusion: "success"})
		job := testJob("synthetic (0)", "success", "Synthetic work and output", "success", "Verify synthetic output", "success")
		job.RunnerGroupName = f.groups[label]
		f.jobs[id] = job
		_ = json.NewEncoder(w).Encode(map[string]int64{"workflow_run_id": id})
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/runs"):
		_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": f.runs})
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/jobs"):
		id, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/repos/lab/load/actions/runs/"), "/jobs"), 10, 64)
		_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []apiJob{f.jobs[id]}})
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
	}
}

func newPoolTestTarget(t *testing.T, handler http.Handler) (*target, *runState) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	tgt := &target{
		api: newGHESAPI(server.URL, "test-token", server.Client()),
		cfg: config{Owner: "lab", Repo: "load", Ref: "main", WorkflowFile: ".github/workflows/synthetic-load.yml"},
	}
	state, err := createRunState(filepath.Join(t.TempDir(), "runs"), "pool-test", "workflows", tgt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	return tgt, state
}

func newTestRealPools(state *runState, groups ...string) []*realPool {
	set := newPoolSet()
	options := runOptions{maxRunners: 10, rampRate: 1}
	pools := make([]*realPool, 0, len(groups))
	for i, group := range groups {
		pools = append(pools, &realPool{
			Group: group, Label: "load-scale-" + group + "-real", ScaleSetID: i + 1,
			pool:   newRunnerPool(nil, state, nil, set, i+1, group, 0, 0, options),
			exited: make(chan struct{}),
		})
	}
	return pools
}

func TestDispatchAndObserveRoundRobinsAcrossPools(t *testing.T) {
	fake := &fakeActions{groups: map[string]string{
		"load-scale-g1-real": "g1",
		"load-scale-g2-real": "g2",
	}, jobs: map[int64]apiJob{}}
	tgt, state := newPoolTestTarget(t, fake)
	pools := newTestRealPools(state, "g1", "g2")
	options := runOptions{
		total: 4, rate: 1000, maxOutstandingJobs: 100, workload: workloadSynthetic, jobsPerRun: 1,
		jobDuration: time.Second, rampRate: 1, rampTimeout: time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dispatchAndObserve(ctx, tgt, state, options, pools, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	want := []string{"load-scale-g1-real", "load-scale-g2-real", "load-scale-g1-real", "load-scale-g2-real"}
	if fmt.Sprint(fake.labels) != fmt.Sprint(want) {
		t.Fatalf("dispatched labels %v, want %v", fake.labels, want)
	}
	snapshot := state.snapshot()
	for i, record := range snapshot.Workflows {
		if pool := pools[i%2]; record.Group != pool.Group || record.RunnerLabel != pool.Label || record.Outcome != workflowPassed {
			t.Errorf("workflow %d targets %q/%q with outcome %q, want %q/%q passed", i, record.Group, record.RunnerLabel, record.Outcome, pool.Group, pool.Label)
		}
	}
	if snapshot.Passed != 4 || snapshot.Failed != 0 {
		t.Fatalf("passed %d failed %d, want 4 verified runs", snapshot.Passed, snapshot.Failed)
	}
}

func TestDispatchAndObserveFailsWhenAnyListenerExits(t *testing.T) {
	tgt, state := newPoolTestTarget(t, http.NotFoundHandler())
	pools := newTestRealPools(state, "g1", "g2")
	close(pools[1].exited)
	options := runOptions{total: 1, rate: 1000, workload: workloadSynthetic, jobsPerRun: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := dispatchAndObserve(ctx, tgt, state, options, pools, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "listener for group g2 stopped unexpectedly") {
		t.Fatalf("error = %v, want the g2 listener exit", err)
	}
}
