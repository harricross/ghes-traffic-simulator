# actions-scaleset-stress

`actions-scaleset-stress` generates bounded GitHub Actions load against a
**real GHES appliance**, using GitHub's official `actions/scaleset` client and
disposable runner scale sets. It has two profiles:

- `registrations` repeatedly creates just-in-time runner registrations and
  removes them after a configurable dwell.
- `hold` bulk-registers tens of thousands of registered-only runners, holds
  them for hours, then removes them all; see [Holding registered-only
  runners at scale](#holding-registered-only-runners-at-scale).
- `workflows` dispatches workflows and runs them on digest-pinned,
  operator-supplied Actions runner containers. The `synthetic` workload is a
  matrix that produces and verifies bounded output; the `ci` workload runs a
  real check pipeline (lint, sharded unit tests, build) from a small project
  that `prepare` commits. A selected percentage of runs can fail
  intentionally; the rest are expected to pass and are verified.

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
all managed files in one commit on the configured ref:

- the synthetic workflow at `workflow_file`;
- the CI workflow at `.github/workflows/ci-checks.yml`;
- a small Python project under `ci/` (sources, settings, 38 unit tests, a
  shard runner, and `lint.sh`/`build.sh` check scripts).

A managed path that already contains different content is never
overwritten; `prepare` refuses instead. It verifies the repository and that
both workflows are active before succeeding. Because the workflows are
committed under `.github/workflows`, the token also needs the `workflow`
scope. Run `check` again if Actions settings or the repository have changed.

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

`--workload` selects what each dispatch runs:

- `synthetic` (default): `--jobs-per-run` matrix jobs that write and verify
  bounded output.
- `ci`: a check pipeline of `Lint`, then `--jobs-per-run` sharded
  `Test shard N` jobs running the real unit tests, then `Build`, which
  packages a deterministic tarball and verifies its checksum. Each dispatch
  uses `--jobs-per-run + 2` jobs, which must fit `--max-outstanding-jobs`.

```bash
./actions-scaleset-stress.sh run \
  --config actions-scaleset-stress.json \
  --mode workflows --workload ci \
  --duration 10m --total 20 --rate 1 \
  --max-runners 6 --jobs-per-run 4 \
  --max-outstanding-jobs 30 --fail-percent 10
```

`--fail-percent` (0-100) is the share of dispatched runs that fail
intentionally, spread evenly (for example, 25 fails every fourth run). A
failing run fails only shard 0, after its output or tests have been
verified; in the CI workload `Build` is then skipped. All other runs are
expected to pass completely. Each completed run is verified job by job
and step by step, and counted as `passed`, `intentional_failures`, or
`failed` (unexpected outcome) in `state.json`. The run exits non-zero if
any workflow run failed verification or was unresolved.
Workflow mode accepts at most 100 dispatches per run and 25 matrix jobs or
test shards per dispatch. The `--duration` deadline, operation rate, runner count, job count,
outstanding-job limit, container CPU/memory, work duration, log size, and
drain timeout are independently bounded.

### Scaling the runner count

Two different numbers matter, and the tool reports them separately:

- **Registered-only** runners: `--mode registrations` creates JIT runner
  registrations and holds them for `--dwell` without starting any runner
  process. `--max-inflight` (up to 1000) is the number held at once, and the
  summary reports the peak plus how many were still registered when removed
  (`held_verified`/`held_missing`). These are registrations, not online
  runners, and need no host memory beyond the tool itself.
- **Online** runners: in workflow mode every runner is a real container.
  `--standing-runners N` ramps up to N idle runners at `--ramp-rate` per
  second and keeps that many warm while dispatched jobs consume them
  (ephemeral runners are replaced after each job). `--max-runners` (up to
  1000) caps the total. Dispatching waits for the ramp up to `--ramp-timeout`.

```bash
./actions-scaleset-stress.sh run \
  --config actions-scaleset-stress.json \
  --mode workflows \
  --duration 40m --total 100 --rate 0.5 --jobs-per-run 5 \
  --standing-runners 150 --max-runners 200 --ramp-rate 2 \
  --max-outstanding-jobs 100 --job-duration 30s \
  --runner-memory-mib 768
```

Runners are limited by the Docker host, not by this tool. On Linux the host
guard (`--host-guard`, default on) samples `/proc` before each start and
stops growing the pool at a **ceiling** when available memory falls below
`--min-free-mem-mib` plus `--worker-reserve-mib` for each runner that can be
busy at once (`--max-outstanding-jobs`), or free disk under the artifacts
directory falls below `--min-free-disk-mib`. High load
(`--max-load-per-cpu`) only pauses growth. Three consecutive start failures
or an unexpected non-zero runner container exit also lower the ceiling. The
ceiling never rises during a run; it is recorded in `state.json`
(`scale.ceiling_runners`, `scale.ceiling_reason`) and in the summary, along
with the GHES-reported peak registered/idle/busy runners. A pool that cannot
start any runner fails the run. Status lines print every 30 seconds. The
guard protects the Docker host only; watch the appliance separately.

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

### Holding registered-only runners at scale

`--mode hold` is for filling the GHES database with runner rows rather than
running jobs. Each runner is one JIT registration (one API call, no container),
so a single small host can hold tens of thousands:

```bash
./actions-scaleset-stress.sh run \
  --config actions-scaleset-stress.json \
  --mode hold \
  --total 10000 --rate 50 --max-inflight 32 \
  --dwell 30m --duration 2h
```

- `--total` (up to 100000) registrations are created at up to `--rate` per
  second with `--max-inflight` concurrent API calls. After 20 consecutive
  registration failures the run stops growing, holds what it has, and records
  the stopping point in `scale.ceiling_runners`/`scale.ceiling_reason`.
- `--dwell` (up to 24h) is the hold time; `--duration` must exceed it. Every
  minute a random sample of 50 registrations is checked, and a final sample of
  500 runs before removal. Registrations GHES drops on its own are counted in
  `held_missing`, the first 50 are journaled as `runner_missing_during_hold`,
  and `first_missing_after_seconds` records how old the first one was. This
  tells you whether GHES garbage-collects runners that never connect.
- Removal is bulk and paced, retries each runner, never removes a runner whose
  name does not belong to the run, and spot-checks that removed runners are
  gone. `cleanup --apply` uses the same path and verifies each journaled
  runner ID still has its journaled name.
- To keep a 100000-runner run practical, `state.json` is written at most every
  two seconds (the journal is still synced per event). If the process is
  killed, `cleanup` rebuilds the runner list from the journal.

These runners are registrations only; they never appear online and cannot run
jobs. Watch the appliance separately, for example database size and load, while
they are held.

### Making runners appear online without containers (`probe-online`)

`probe-online` is an experiment, not a workload mode. It registers JIT runners
under a throwaway scale set (`probe-online-<MMDD-HHMMSS>`), then opens a
runner session for each one using only the credentials inside its JIT config
and long-polls for messages, so GHES reports the runners as `idle` instead of
`registered-only`. No runner binary or container runs.

```bash
./actions-scaleset-stress.sh probe-online --config actions-scaleset-stress.json \
  --runners 1000 --hold 10m --open-concurrency 10
```

- `--runners` (1–10000), `--hold` (10s–30m), `--open-concurrency` (1–50) and
  `--runner-version` (default 2.338.0, the version reported in the session).
  Raise `ulimit -n` above the runner count first (each session is one
  connection).
- It prints only non-secret metadata: the JIT file names, flow flags, redacted
  URLs, scale-set statistics (`registered`/`idle`/`busy`) every 30s, and an
  aggregate of session-open latency and poll results. Cleanup (always run,
  including on SIGTERM) closes every session, removes every runner and deletes
  the scale set.
- It follows the protocol the JIT config selects. GHES 3.20.10 returned the
  legacy (non-broker) flow: OAuth at `<server_url>_apis/oauth2/token`, then
  `POST .../_apis/distributedtask/pools/<pool>/sessions` and a long poll on
  `.../messages`. If `use_v2_flow` is true the session is created at the V2
  server URL with `brokerSession`; this path is untested.
- These runners cannot run jobs: the probe never accepts a message. Use
  `--mode workflows` with real containers when jobs must execute.
- The protocol is internal to the runner and can change between GHES versions.
  Validate on the exact version under test. Token refresh on long holds (a 401
  response triggers a new token) is implemented but has not been observed.

Measured on GHES 3.20.10 (8 CPU, 61 GB) from a 4-core/16 GB Codespace:

| Runners online | Idle as seen by GHES | Poll errors | Codespace memory | Appliance load1 |
|---|---|---|---|---|
| 100 | 100 | 0 | <1 GiB | ~3 |
| 1000 | 1000 | 0 | ~1.8 GiB | ~4–5 |
| 3000 | 3000 | 0 | ~1.7 GiB | ~4–7.5 while opening |

For comparison, a real runner container used about 768 MiB each. The Actions
data lives in MSSQL (`/data/user/mssql`), not MySQL; the MySQL size did not
change measurably with 1000 runners.

## Validation

From `actions-scaleset-stress/`:

```bash
go test ./...
go test -race ./...
```
