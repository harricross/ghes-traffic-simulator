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
- `fleet` creates several organization runner groups, each holding a mix of
  real container runners, fake runners that appear online, and registered-only
  runners, then runs workflows on the real ones; see [Runner groups mixing
  real and fake runners](#runner-groups-mixing-real-and-fake-runners---mode-fleet).

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

`config_url` is the repository's HTTPS URL, not its API URL. Fleet mode instead
needs the owning organization's URL (`https://ghes.example.test/load-lab`) so
its runner groups and scale sets belong to the organization; other modes
accept either. Set the token in
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
  --total 10000 --rate 25 --max-inflight 16 \
  --dwell 30m --duration 2h
```

- `--total` (up to 100000) registrations are created at up to `--rate` per
  second with `--max-inflight` concurrent API calls. After 20 consecutive
  registration failures the run stops growing, holds what it has, and records
  the stopping point in `scale.ceiling_runners`/`scale.ceiling_reason`.
- GHES allows at most **10,000 runners per runner group** (online or not);
  the 10,001st registration fails with `RunnerGroupTooManyRunnersException`
  and the run stops there. Use a different `runner_group` in the configuration
  to hold more than 10,000 at once.
- Registration is CPU-heavy on the appliance. On the 8-CPU test appliance,
  about 65 registrations/s raised load1 to ~25 within a minute (the guard
  stopped the run at 8,298), while 25/s kept load1 at 3-5. Start at
  `--rate 25` and raise it only while watching appliance load.
- `--dwell` (up to 24h) is the hold time; `--duration` must exceed it. Every
  minute a random sample of 50 registrations is checked, and a final sample of
  500 runs before removal. Registrations GHES drops on its own are counted in
  `held_missing`, the first 50 are journaled as `runner_missing_during_hold`,
  and `first_missing_after_seconds` records how old the first one was. This
  tells you whether GHES garbage-collects runners that never connect.
- Removal is bulk and paced at the same `--rate`, retries each runner, never
  removes a runner whose name does not belong to the run, and spot-checks that
  removed runners are gone. `cleanup --apply` uses the same path and verifies
  each journaled runner ID still has its journaled name.
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
| 6000 | 6000 | 0 | ~1.9 GiB | peak ~12.7 while opening, ~4 held |
| 10000 (group limit) | 10000 | 0 | ~2.1 GiB | peak ~8 while opening, ~3–5 held |

Open sessions gradually (`--open-concurrency 6-10`); the opening burst, not the
steady state, is what loads the appliance. The 10,000 runner-group limit
applies here too.

For comparison, a real runner container used about 768 MiB each. The Actions
data lives in MSSQL (`/data/user/mssql`), not MySQL; the MySQL size did not
change measurably with 1000 runners.

### Runner groups mixing real and fake runners (`--mode fleet`)

`--mode fleet` fills several organization runner groups at once. Each group
holds any mix of:

- **real** runners: digest-pinned runner containers that run dispatched
  workflows, as in `--mode workflows`;
- **online** fake runners: JIT registrations with an open session and long
  poll (the `probe-online` technique), so GHES lists them as idle;
- **offline** fake runners: registrations only (the `hold` technique).

Each group gets two scale sets, `<scale_set_name>-<group>-real` and
`<scale_set_name>-<group>-fake`, so fake runners never match a workflow
label. Both count toward the group's 10,000-runner limit.

Fleet mode needs an organization-scope configuration (see
[Configure](#configure)) and a private repository in that organization
prepared with `prepare` (re-run `prepare` on repositories prepared before
fleet mode existed so the workflows accept a `runner_label` input). The token
needs `admin:org` to create runner groups. Describe the groups in a non-secret
spec file:

```json
{
  "group_prefix": "stress",
  "groups": [
    {"name": "g01", "real": 10, "online": 3000, "offline": 6990},
    {"name": "g02", "real": 10, "online": 3000, "offline": 6990},
    {"name": "g03", "online": 0, "offline": 10000}
  ]
}
```

```bash
ulimit -n 10000
./actions-scaleset-stress.sh run --config fleet-config.json --mode fleet --fleet fleet.json \
  --rate 25 --max-inflight 16 --open-concurrency 6 --max-runners 20 \
  --workload ci --total 60 --jobs-per-run 5 --dispatch-rate 0.5 \
  --dwell 15m --duration 2h
```

- Group names are `<group_prefix>-<name>` (lowercase letters, digits and
  hyphens; up to 20 groups). A missing group is created with access limited to
  the stress repository; an existing group is reused and left in place. The
  run refuses default or enterprise-inherited groups, a reused group that
  cannot run the repository's workflows, leftover scale sets from an earlier
  run, and any group whose existing runners plus the requested ones would
  exceed 10,000.
- Before creating anything the run also checks that the real runners fit
  `--max-runners`, that `ulimit -n` covers one connection per online runner
  plus 1,024, that memory covers about 0.25 GiB per 1,000 online sessions
  above `--min-free-mem-mib`, and that `--duration` covers fake fill and
  removal at `--rate`, the real ramp, `--dwell` (or the dispatch window if
  longer) and a 10-minute margin. It prints the minimum duration if not.
- Fake runners are registered round-robin across groups at `--rate`; the
  first `online` in each group get sessions opened at `--open-concurrency`.
  Hitting the 10,000 limit stops only that group. Real runners ramp to each
  group's `real` count while the fake fill runs. Once both finish, `--total`
  workflows are dispatched round-robin over groups with real runners at
  `--dispatch-rate`, each pinned to that group's `-real` label (or a
  `fake_jobs` group's `-fake` label, see below), and every job
  is checked to have run in its group.
- The fleet is then held until `--dwell` has passed and every workflow has
  finished, whichever is later. Per-group status is printed and journaled
  every minute: registered, idle and busy counts, the group's REST runner
  count, sessions open, poll errors and unexpected messages (anything other
  than an empty poll; this should stay 0). Held registrations are
  spot-checked as in hold mode.
- Teardown closes sessions, removes fake runners at `--rate`, stops real
  runners, deletes the scale sets, then deletes groups this run created once
  GHES reports them empty. `cleanup --run-id <id> --apply` repeats the same
  steps from the journal; the cleanup deadline grows with the runner count.
- The console summary and `report.json` (`fleet_groups`) give one line per
  group: peaks for real, busy, fake idle and group runners; sessions opened
  and failed; fake runners registered against requested with any ceiling
  reason; and workflows passed, intentionally failed, failed and unresolved.

#### Fake runners that run jobs (`fake_jobs`)

Set `"fake_jobs": true` on a group to make its fake-online runners accept
real workflow jobs without a container. GHES schedules, assigns and records
the jobs as usual; each fake runner reports a canned result instead of
executing steps.

```json
{"group_prefix": "stress", "groups": [
  {"name": "mixed", "real": 1, "online": 300, "fake_jobs": true}
]}
```

```bash
./actions-scaleset-stress.sh run --config fleet-config.json --mode fleet --fleet fleet.json \
  --workload ci --total 100 --jobs-per-run 2 --fail-percent 10 \
  --fake-job-percent 95 --fake-job-duration 5s --rate 10 --dispatch-rate 1 \
  --max-outstanding-jobs 200 --max-runners 1 --duration 40m
```

- A `fake_jobs` group needs `online` > 0. The run starts a scale-set
  listener on the group's `-fake` scale set, which acquires every job offered
  to it, and dispatches workflows pinned to that label as well as to the
  `-real` labels.
- `--fake-job-percent` (0–100, default 100) is the share of dispatches sent
  to `fake_jobs` groups when the fleet also has real runners; it must be 100
  when there are none. `--fake-job-duration` (0–10m, default 5s) is how long
  each fake runner holds a job, renewing its lock, before completing it.
- A fake job follows the runner's own sequence: it marks its timeline
  record in progress, holds the job, closes its session, marks the record
  completed with the result and sends `JobCompleted`. The result is
  `failed` when the job's `matrix.shard` is in the workflow's
  `failure_shards` input (as with `--fail-percent`), `canceled` if GHES
  cancels it, and `succeeded` otherwise. Jobs therefore have conclusions
  but no steps or logs, and `needs` gating behaves as with real runners.
- Fake runners are ephemeral like real ones: GHES deletes each runner when
  its job completes, and the run registers and opens a replacement (named
  after the fill's indexes) unless the run is ending. Every run is still
  verified to have run its jobs in its group with the expected conclusion.
- The first fake-job error (an undecodable message, a rejected completion
  or a failed replacement) stops dispatching and fails the run.
- Only the legacy (non-broker) message protocol that GHES 3.20 selects is
  supported. The session key is decrypted with RSA-OAEP SHA-256 when GHES
  sets `useFipsEncryption`, otherwise SHA-1, as the runner does.

#### Fleet measurements

Measured on GHES 3.20.10 (8 CPU, 61 GB) from a 4-core/16 GB Codespace:

- Registration ran at 15–25 runners/s (`--rate 25`) with no rate limiting.
- About 3,000 concurrent fake-online sessions was comfortable for the
  appliance; open them at `--open-concurrency 6`–10.
- MSSQL (`/data/user/mssql`) grew by about 67–78 KB per registered runner.
- `--max-runners` must be at least the total of the groups' `real` counts.
- Workflow dispatch, not idle sessions, drives appliance load. With 600
  fake-online `fake_jobs` runners holding sessions (load1 about 2–3), the CI
  workload at `--dispatch-rate 1` pushed load1 to about 24 within 80 seconds.
  At `--dispatch-rate 0.2` with `--max-outstanding-jobs 80`, 100 runs (370
  fake jobs, 0 errors) peaked at load1 11 and averaged 6–9. Start at 0.2 or
  lower and watch the appliance load.
- That 100-run job workload grew MSSQL by about 400 MB, mostly the Pipelines
  transaction log, whose space is reused only after a log backup.

## Validation

From `actions-scaleset-stress/`:

```bash
go test ./...
go test -race ./...
```
