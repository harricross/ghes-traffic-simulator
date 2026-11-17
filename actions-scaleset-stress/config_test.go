package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigRejectsTrailingData(t *testing.T) {
	t.Setenv("GHES_TEST_TOKEN", "test-token")
	config := `{
  "api_base_url": "https://ghes.example.test/api/v3",
  "config_url": "https://ghes.example.test/load-lab/actions-stress",
  "owner": "load-lab",
  "repo": "actions-stress",
  "workflow_file": ".github/workflows/synthetic-load.yml",
  "ref": "main",
  "scale_set_name": "actions-load-test",
  "runner_group": "default",
  "token_env": "GHES_TEST_TOKEN"
}`

	for _, suffix := range []string{" trailing", ` {"another":"object"}`} {
		t.Run(suffix, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(config+suffix), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := loadConfig(path); err == nil {
				t.Fatal("expected trailing data to be rejected")
			}
		})
	}
}

func TestConfigRequiresGHESHostAndRepositoryScopedURL(t *testing.T) {
	cfg := config{
		APIBaseURL:   "https://ghes.example.test/api/v3",
		ConfigURL:    "https://ghes.example.test/load-lab/actions-stress",
		Owner:        "load-lab",
		Repo:         "actions-stress",
		WorkflowFile: ".github/workflows/synthetic-load.yml",
		Ref:          "main",
		ScaleSetName: "actions-load-test",
		RunnerGroup:  "default",
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("valid GHES config rejected: %v", err)
	}

	cfg.APIBaseURL = "https://api.github.com/api/v3"
	cfg.ConfigURL = "https://api.github.com/load-lab/actions-stress"
	if err := cfg.validate(); err == nil {
		t.Fatal("expected hosted GitHub to be rejected")
	}
}
