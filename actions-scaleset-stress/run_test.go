package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestWorkflowYAMLPreservesShellFormatsAndVerifiesOutput(t *testing.T) {
	workflow := workflowYAML("load-scale")
	for _, expected := range []string{
		"runs-on: load-scale",
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
		"runs-on: load-scale",
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
