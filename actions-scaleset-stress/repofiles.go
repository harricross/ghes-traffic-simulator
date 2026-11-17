package main

import (
	"crypto/sha1"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

const (
	workloadSynthetic = "synthetic"
	workloadCI        = "ci"
	ciWorkflowFile    = ".github/workflows/ci-checks.yml"
	ciProjectDir      = "ci"
	ciEmbedRoot       = "ciproject"
)

//go:embed all:ciproject
var ciProjectFS embed.FS

type repoFile struct {
	Path    string
	Content string
}

// managedFiles lists every file prepare installs and check verifies byte-for-byte.
func managedFiles(cfg config) ([]repoFile, error) {
	files := []repoFile{
		{Path: cfg.WorkflowFile, Content: workflowYAML(cfg.ScaleSetName)},
		{Path: ciWorkflowFile, Content: ciWorkflowYAML(cfg.ScaleSetName)},
	}
	project, err := ciProjectFiles()
	if err != nil {
		return nil, err
	}
	files = append(files, project...)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func ciProjectFiles() ([]repoFile, error) {
	var files []repoFile
	err := fs.WalkDir(ciProjectFS, ciEmbedRoot, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		base := entry.Name()
		if name != ciEmbedRoot && (strings.HasPrefix(base, ".") || base == "__pycache__") {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() || strings.HasSuffix(base, ".pyc") {
			return nil
		}
		data, err := ciProjectFS.ReadFile(name)
		if err != nil {
			return err
		}
		files = append(files, repoFile{
			Path:    path.Join(ciProjectDir, strings.TrimPrefix(name, ciEmbedRoot+"/")),
			Content: string(data),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read embedded CI project: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("embedded CI project is empty")
	}
	return files, nil
}

// gitBlobSHA returns the object ID Git assigns to a blob with this content.
func gitBlobSHA(content string) string {
	sum := sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", len(content), content)))
	return hex.EncodeToString(sum[:])
}

func workflowFileFor(cfg config, workload string) string {
	if workload == workloadCI {
		return ciWorkflowFile
	}
	return cfg.WorkflowFile
}

// jobsPerDispatch counts every job a dispatch creates: CI adds Lint and Build to the test shards.
func jobsPerDispatch(workload string, shards int) int {
	if workload == workloadCI {
		return shards + 2
	}
	return shards
}

// runFails spreads failing dispatches evenly: floor(n*percent/100) of the first n runs fail.
func runFails(index, percent int) bool {
	return (index+1)*percent/100 > index*percent/100
}

func ciWorkflowYAML(scaleSetName string) string {
	return strings.ReplaceAll(`name: CI checks
run-name: CI checks ${{ inputs.run_id }}
on:
  workflow_dispatch:
    inputs:
      run_id:
        description: Correlation identifier
        required: true
        type: string
      shards:
        description: JSON array of bounded test shard numbers
        required: true
        type: string
      failure_shards:
        description: JSON array of test shards that intentionally fail
        required: true
        type: string
      duration_seconds:
        description: Bounded extra test duration
        required: true
        type: string
      log_bytes:
        description: Bounded extra test log output
        required: true
        type: string
permissions:
  contents: read
defaults:
  run:
    shell: bash
    working-directory: ci
jobs:
  lint:
    name: Lint
    runs-on: __SCALE_SET_NAME__
    steps:
      - name: Check out repository
        uses: actions/checkout@v4
        with:
          persist-credentials: false
      - name: Lint sources
        run: bash scripts/lint.sh
  test:
    name: Test shard ${{ matrix.shard }}
    needs: lint
    runs-on: __SCALE_SET_NAME__
    strategy:
      fail-fast: false
      matrix:
        shard: ${{ fromJSON(inputs.shards) }}
    steps:
      - name: Check out repository
        uses: actions/checkout@v4
        with:
          persist-credentials: false
      - name: Run unit tests
        env:
          RUN_ID: ${{ inputs.run_id }}
          SHARD: ${{ matrix.shard }}
          TOTAL: ${{ strategy.job-total }}
          DURATION: ${{ inputs.duration_seconds }}
          LOG_BYTES: ${{ inputs.log_bytes }}
        run: |
          set -euo pipefail
          [[ "$RUN_ID" =~ ^[A-Za-z0-9._-]{1,64}$ ]]
          [[ "$SHARD" =~ ^[0-9]{1,3}$ ]]
          [[ "$TOTAL" =~ ^[0-9]{1,3}$ ]]
          [[ "$DURATION" =~ ^[0-9]{1,3}$ ]] && (( DURATION <= 600 ))
          [[ "$LOG_BYTES" =~ ^[0-9]{1,7}$ ]] && (( LOG_BYTES <= 1048576 ))
          python3 tests/run_shard.py --shard "$SHARD" --total "$TOTAL" --report test-report.json
          if (( LOG_BYTES > 0 )); then
            head -c "$LOG_BYTES" /dev/zero | tr '\0' 'x'
            printf '\n'
          fi
          sleep "$DURATION"
      - name: Verify test report
        env:
          SHARD: ${{ matrix.shard }}
        run: |
          set -euo pipefail
          jq -e --argjson shard "$SHARD" '.shard == $shard and .tests_run > 0 and .failures == 0 and .errors == 0' test-report.json
          printf 'Test shard %s: %s tests passed\n' "$SHARD" "$(jq -r .tests_run test-report.json)" >> "$GITHUB_STEP_SUMMARY"
      - name: Intentional check failure
        if: ${{ contains(fromJSON(inputs.failure_shards), matrix.shard) }}
        env:
          SHARD: ${{ matrix.shard }}
        run: |
          echo "::error title=Intentional failure::Load test requested a failing check for test shard $SHARD"
          exit 1
  build:
    name: Build
    needs: test
    runs-on: __SCALE_SET_NAME__
    steps:
      - name: Check out repository
        uses: actions/checkout@v4
        with:
          persist-credentials: false
      - name: Build package
        env:
          RUN_ID: ${{ inputs.run_id }}
        run: bash scripts/build.sh "$RUN_ID"
      - name: Verify package
        run: |
          set -euo pipefail
          cd dist
          sha256sum -c ./*.sha256
          printf 'Package checksum verified\n' >> "$GITHUB_STEP_SUMMARY"
`, "__SCALE_SET_NAME__", scaleSetName)
}
