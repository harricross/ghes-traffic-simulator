package main

import (
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

func TestVerifyWorkflowJobs(t *testing.T) {
	job := func(name, conclusion, work, verify, intentional string) apiJob {
		return apiJob{
			Name:       name,
			Conclusion: conclusion,
			Steps: []struct {
				Name       string `json:"name"`
				Conclusion string `json:"conclusion"`
			}{
				{Name: "Synthetic work and output", Conclusion: work},
				{Name: "Verify synthetic output", Conclusion: verify},
				{Name: "Intentional synthetic failure", Conclusion: intentional},
			},
		}
	}

	t.Run("success", func(t *testing.T) {
		jobs := []apiJob{
			job("synthetic (shard: 0)", "success", "success", "success", "skipped"),
			job("synthetic (shard: 1)", "success", "success", "success", "skipped"),
		}
		if ok, reason := verifyWorkflowJobs(jobs, 2, 0); !ok {
			t.Fatalf("expected valid successful workflow, got %q", reason)
		}
	})

	t.Run("intentional failure after output verification", func(t *testing.T) {
		jobs := []apiJob{
			job("synthetic (shard: 0)", "failure", "success", "success", "failure"),
			job("synthetic (shard: 1)", "success", "success", "success", "skipped"),
		}
		if ok, reason := verifyWorkflowJobs(jobs, 2, 1); !ok {
			t.Fatalf("expected valid intentional failure, got %q", reason)
		}
	})

	t.Run("fails if output was not verified", func(t *testing.T) {
		jobs := []apiJob{job("synthetic (shard: 0)", "failure", "success", "failure", "failure")}
		if ok, _ := verifyWorkflowJobs(jobs, 1, 1); ok {
			t.Fatal("expected output verification failure")
		}
	})

	t.Run("rejects duplicate shards", func(t *testing.T) {
		jobs := []apiJob{
			job("synthetic (shard: 0)", "success", "success", "success", "skipped"),
			job("synthetic (shard: 0)", "success", "success", "success", "skipped"),
		}
		if ok, _ := verifyWorkflowJobs(jobs, 2, 0); ok {
			t.Fatal("expected duplicate shard to be rejected")
		}
	})
}
