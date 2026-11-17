# actions-scaleset-stress

`actions-scaleset-stress` generates bounded GitHub Actions load against a
**real GHES appliance**, using GitHub's official `actions/scaleset` client and
disposable runner scale sets. It has two profiles:

- `registrations` repeatedly creates just-in-time runner registrations and
  removes them after a configurable dwell.
- `workflows` dispatches a synthetic matrix workflow and runs it on
  digest-pinned, operator-supplied Actions runner containers. The workflow
  produces and verifies a small output, can emit bounded logs and duration,
  and can intentionally fail a selected percentage of jobs.

There is no fake GHES server or mock runner protocol. The tool does not contact
GHES until `check`, `prepare`, `run`, or `cleanup` is explicitly invoked.

## Requirements and safety

- Go 1.27.3 or later to build (or set `ACTIONS_SCALESET_STRESS_BIN` to a
  prebuilt executable).
- A disposable GHES repository and a token authorized for the repository's
  Actions runner scale-set APIs. `prepare` also needs permission to create a
  private repository and workflow when they do not already exist.
- Workflow mode additionally needs Docker and a trusted Actions runner image
  pinned by SHA-256 digest. Use an image built from an official runner release;
  the JIT configuration is passed to the container as an environment variable.

Only test on a GHES appliance and repository you control, with an approved
load window and resource budget. The defaults are bounded, not authorization
to run load. The tool refuses GitHub.com and hosted GitHub domains, uses TLS
verification by default, keeps tokens out of run artifacts, and journals
resource IDs for cleanup. `--insecure` disables TLS verification and should
only be used for a specifically approved lab.

Each run uses one uniquely named scale set in the repository's configured
runner group. Existing scale sets are not adopted. Use a disposable repository
with Actions enabled, and do not point this tool at a repository with real
workloads or runners that might match the scale-set label.

## Configure

Create a non-secret JSON file, for example `actions-scaleset-stress.json`:

```json
{
  "api_base_url": "https://ghes.example.test/api/v3",
  "config_url": "https://ghes.example.test/load-lab/actions-stress",
  "owner": "load-lab",
  "repo": "actions-stress",
  "workflow_file": ".github/workflows/synthetic-load.yml",
  "ref": "main",
  "scale_set_name": "actions-load-test",
  "runner_group": "default",
  "runner_image": "ghcr.io/example/actions-runner@sha256:<64-hex-digest>",
  "token_env": "GHES_ACTIONS_TOKEN"
}
```

`config_url` is the repository's HTTPS URL, not its API URL. Set the token in
the named environment variable; do not put it in the JSON file or command
line. The image reference is required only for workflow mode and must use an
immutable SHA-256 digest.

## Commands

Run the wrapper from the repository root; it builds the Go program under
`tmp/bin/` when needed:

```bash
./actions-scaleset-stress.sh help
./actions-scaleset-stress.sh check --config actions-scaleset-stress.json
./actions-scaleset-stress.sh prepare --config actions-scaleset-stress.json
```

`prepare` creates the configured private repository if absent and installs
the generated workflow only if that path does not already contain different
content. It verifies the repository and active workflow before succeeding.
An existing different workflow is never overwritten. Run `check` again if
Actions settings or the repository have changed.

Registration churn example:

```bash
./actions-scaleset-stress.sh run \
  --config actions-scaleset-stress.json \
  --mode registrations \
  --duration 2m --total 60 --rate 1 \
  --max-inflight 4 --dwell 5s
```

Workflow example:

```bash
./actions-scaleset-stress.sh run \
  --config actions-scaleset-stress.json \
  --mode workflows \
  --duration 5m --total 20 --rate 1 \
  --max-runners 4 --jobs-per-run 2 \
  --max-outstanding-jobs 20 \
  --job-duration 10s --max-log-bytes 1024
```

`--fail-percent` (0-100) selects a deterministic prefix of matrix shards to
fail after output verification. Intentional failures are counted as valid
synthetic outcomes; unexpected failures or missing outputs fail the run.
Workflow mode accepts at most 100 dispatches per run and 25 matrix jobs per
dispatch. The `--duration` deadline, operation rate, runner count, job count,
outstanding-job limit, container CPU/memory, work duration, log size, and
drain timeout are independently bounded.

The default run artifacts are written to
`tmp/actions-scaleset-stress/<run-id>/` with restrictive permissions. The
`state.json`, append-only journal, heartbeat, and final report contain
non-secret IDs and outcomes, not tokens or JIT configurations. Preserve the
run ID for cleanup and review.

Cleanup is preview-only unless `--apply` is passed. It acts only on
journaled workflow, runner, container, and scale-set IDs, verifies runner
names and container ownership labels, and refuses to delete a scale set that
does not match the configured name:

```bash
./actions-scaleset-stress.sh cleanup \
  --config actions-scaleset-stress.json \
  --run-id <run-id>

./actions-scaleset-stress.sh cleanup \
  --config actions-scaleset-stress.json \
  --run-id <run-id> --apply
```

If a run is interrupted, inspect its report and use cleanup with the same
configuration and run ID. Cleanup may report unresolved remote state; do not
repeat load until the scale set, runners, active workflow runs, and containers
have been reconciled.

## Validation

From `actions-scaleset-stress/`:

```bash
go test ./...
go test -race ./...
```
