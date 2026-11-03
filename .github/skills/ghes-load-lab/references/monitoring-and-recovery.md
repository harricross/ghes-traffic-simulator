# Monitoring, scheduled check-ins, and recovery

This reference is the operational runbook for a GHES load lab coordinated by
the Copilot App. Read it before starting any writer. It describes requirements
and example artifacts, **not an implemented monitoring service**. This repository
does not provide the STOP adapter, watchdog, capacity collector, run journal, or
Copilot scheduling integration described below. Implement and test those
run-specific controls, or mark the run `blocked`. A five-minute chat check-in is
not a substitute for a working safety control.

Use only a user-authorized lab, explicitly named nodes, repositories, identities,
and workload limits. Do not change GHES services, replication, authentication,
rate limits, or storage policy to make a workload succeed.

Two-node examples illustrate HA. For a standalone appliance, collect and verify
its one declared node; for a larger topology, use every node in the approved
measurement scope. "Both nodes" below never means inventing a replica.
Only enforce data targets actually requested: a traffic-only run needs capacity
and traffic acceptance checks, not an unsolicited 100 GB storage load.

## 1. Separate coordination from safety

| Component | Location | Cadence | Responsibility |
| --- | --- | --- | --- |
| Coordinator | Original Copilot App conversation | Phase changes and scheduled check-ins | Plan, interpret instructions, report status, request decisions, retain evidence |
| Appliance collector | User's local machine, using its existing SSH key | Every 60 seconds | Read-only capacity and physical-use samples from every authorized node; publish nonsecret telemetry and heartbeat |
| Workload supervisor and guard | Codespace, independently of Copilot conversation execution | Every 5 seconds | Check workspace/runtime capacity, admission budget, local collector freshness, owned processes, and STOP requests |
| Guard liveness enforcement | Independently supervised execution boundary in the Codespace | At most 5-second checks | Stop writers when the guard itself fails or stops heartbeating |
| Status artifact writer | Codespace supervisor | Every 60 seconds and every transition | Atomic status snapshots and append-only journal |
| Scheduled check-in | Same Copilot App session | Every 5 minutes when execution is available | Read snapshots, provide progress/ETA, investigate problems; never act as the only watchdog |

Local SSH access remains local. Do not copy private keys to a Codespace or enable
SSH agent forwarding by default. Do not expose an unauthenticated local collector
endpoint. The local collector may publish sanitized samples using an authenticated,
user-approved Codespace connection. Carry its original sample timestamps and
sequence numbers unchanged; the Codespace must not manufacture fresh local
heartbeats when the local machine stops reporting.

The Codespace connects to GHES for authorized Git/API traffic through the approved
network path and TLS trust configuration, not by pretending it possesses the
local SSH identity. Keep administrative SSH collection separate from workload
credentials. Verify SSH host keys against trusted user-provided information; do
not disable host-key checking or TLS verification.

Copilot App closure, machine sleep, a lost SSH connection, a Codespace disconnect,
or a delayed automation must not leave unbounded writers running. The execution
boundary must be able to stop owned writers without calling the model, receiving
a chat message, or making another SSH request.

### Required preflight demonstration

Before meaningful writes, run a bounded, disposable canary and demonstrate:

1. An exclusive run lock prevents a second supervisor for the same run ID.
2. Both appliance nodes publish baseline samples and verified identities.
3. The guard measures the filesystems backing both `/workspaces` and `/tmp`.
4. A STOP request stops admission and all owned worker descendants.
5. A deliberately stale collector heartbeat stops the canary.
6. Loss of guard heartbeat stops the canary through independent enforcement.
7. A simulated low-capacity sample stops admission without filling a real disk.
8. Logs and counters survive termination and can be read after disconnect/reconnect.

Record the evidence and stop the canary before raising concurrency. An untested
control, unidentified process, inaccessible node, or unsupported measurement is a
blocker, not a reason to proceed with a warning.

## 2. Durable run artifacts and ownership

In the Codespace, keep nonsecret run state below the repository's ignored
`tmp/<run-id>/` directory. This is a repository-relative `tmp` directory, not the
system `/tmp` directory. Verify ignore rules with `git check-ignore` before
writing state. Existing simulator runtime files may nevertheless reside on the
system `/tmp` filesystem; monitor that filesystem too. Do not assume setting
`TMPDIR` relocates the simulator's hard-coded runtime allocation.

Recommended layout:

```text
tmp/<run-id>/
  run-spec.json
  status.json
  manifest.jsonl
  appliance-samples.jsonl
  collector-heartbeat.json
  guard-heartbeat.json
  stop-request.json
  processes.json
  evidence/
    baseline.json
    final-physical-verification.json
    counter-snapshots.jsonl
    final-report.txt
    sanitized-workload.log
```

These are templates to produce, not files already supplied by this skill.
Use restrictive permissions, atomic replacement of snapshot JSON, an exclusive
writer for JSONL, and a bounded retention/rotation policy. Partial JSON or a
truncated final JSONL line must be detectable. Preserve the last valid snapshot
and label its freshness; never treat parse failure as an empty successful run.
Reserve space for evidence even when stopping for low disk.

### Nonsecret `run-spec.json` template

Replace placeholders, normalize user units to bytes, and validate the complete
spec before launching. This example models a two-node storage target plus a Git
storage range; other instructions need their own explicit acceptance criteria.

```json
{
  "schema_version": 1,
  "run_id": "<unique-run-id>",
  "created_at_utc": "<RFC3339-UTC>",
  "codespace_name": "<approved-codespace-name>",
  "repository_checkout": "<absolute-codespace-checkout>",
  "source_revision": "<git-commit-sha>",
  "approved_scope": {
    "appliance_node_ids": ["<node-a>", "<node-b>"],
    "repository_prefix": "<disposable-run-prefix>",
    "allowed_operations": ["<approved-operation>"],
    "maximum_workers": 1,
    "maximum_duration_seconds": 3600
  },
  "targets": {
    "user_storage": {
      "path": "/data/user/storage",
      "metric": "physical_allocated_bytes",
      "semantics": "absolute_total_per_node",
      "minimum_bytes_per_node": "<normalized-integer-bytes>"
    },
    "git_storage": {
      "path": "/data/user/repositories",
      "metric": "physical_allocated_bytes",
      "semantics": "absolute_total_per_node",
      "minimum_bytes_per_node": "<normalized-integer-bytes>",
      "maximum_bytes_per_node": "<normalized-integer-bytes>"
    }
  },
  "safety": {
    "guard_interval_seconds": 5,
    "collector_interval_seconds": 60,
    "collector_stale_after_seconds": 180,
    "guard_stale_after_seconds": 15,
    "appliance_free_floor_gib": 25,
    "appliance_free_floor_capacity_fraction": 0.10,
    "codespace_free_floor_gib": 10,
    "codespace_free_floor_capacity_fraction": 0.15,
    "drain_grace_seconds": 60,
    "physical_verification_deadline_seconds": "<approved-integer-seconds>"
  },
  "admission_budget": {
    "maximum_inflight_bytes_per_node": "<conservative-integer-bytes>",
    "estimated_peak_workspace_bytes": "<integer-bytes>",
    "estimated_peak_runtime_bytes": "<integer-bytes>",
    "reserved_evidence_bytes": "<integer-bytes>",
    "estimation_basis": "<canary-observations-and-headroom>"
  },
  "coordination": {
    "session_id": "<original-copilot-session-id>",
    "checkin_interval_minutes": 5,
    "automation_owned_by_run": false,
    "automation_id": null
  }
}
```

String-valued integer placeholders above must become actual JSON integers.
This expanded example and the main skill's intake example use different
illustrative field names. Select one versioned canonical schema before
implementing adapters, normalize intake into it, and make the supervisor,
collector, guard, and scheduled prompt agree on that schema. Do not alternate
between two incompatible `run-spec.json` formats during a run.
If a user requests a **delta** instead of an absolute total, store baseline,
requested delta, and derived absolute threshold separately for each node.
Never reinterpret decimal GB as GiB without documenting the conversion.
If Git must be in range only on a designated primary, record that scope explicitly;
do not silently apply or remove a per-node requirement.

Exclude private keys, PATs, cookies, passwords, authorization headers, session
secrets, signed upload URLs, and raw upload policies. Secret-bearing files such
as `tmp/sim-users.json` are **not** the nonsecret run manifest. Do not copy them
into coordinator artifacts or status messages. Existence of a Git ignore rule
does not make a secret safe to disclose.

### `status.json` shape

Include:

- Run ID, monotonic event sequence, schema version, UTC write timestamp, state,
  phase, reason, and last successful checkpoint.
- Source timestamp and age of every collector/guard sample; appliance identity
  and filesystem identity, capacity, available bytes, and physical directory use.
- Owned job IDs, sanitized operation names, PID/PGID/session identifiers and
  process start identities, current admission limit, inflight budget, and exits.
- Attempted/succeeded/failed operation counts, committed uploads, uploaded
  logical bytes, physical-use observations, and missing/uncertain measurements.
- Remaining work, recent throughput window, ETA range/confidence, dependency
  blockers, replication/verification status, and next action.
- Last STOP request, acknowledgment, outstanding descendants, drain deadline,
  and evidence persistence result.

Do not place credentials in command arguments, process dumps, or status output.
Even environment variables can leak through process inspection; retain only an
allowlisted process identity, not a full command line or environment dump. Treat
raw tool errors and logs as potentially secret-bearing and sanitize before
publishing. Avoid verbose HTTP traces and shell `set -x` around authentication.

### Append-only `manifest.jsonl`

Record intent **before** each externally visible mutation and its result after.
Use stable operation IDs, a run marker, content checksum where meaningful,
remote object IDs/refs, phase, UTC timestamp, and sequence number.

```json
{"sequence":1,"run_id":"<run-id>","at_utc":"<UTC>","event":"intent","operation_id":"<stable-id>","kind":"create-repository","repository":"<approved-run-repository>"}
{"sequence":2,"run_id":"<run-id>","at_utc":"<UTC>","event":"confirmed","operation_id":"<stable-id>","remote_repository_id":"<id>","ownership_marker":"<run-id>"}
{"sequence":3,"run_id":"<run-id>","at_utc":"<UTC>","event":"checkpoint","phase":"<phase>","committed_units":1}
```

The journal is necessary but not sufficient for exactly-once execution: an API
response can be lost after a mutation succeeds. Reconcile with remote objects
before retrying an intent without a confirmation.

### Coordinator artifact contract

Keep a sanitized artifact in the original session's durable storage, outside any
disposable Codespace checkout, containing:

- Run ID, approved scope and limits, source revision, acceptance criteria, and
  the Codespace name/connection identifier.
- Nonsecret run-spec copy or digest and exact remote artifact directory.
- Schedule ownership record, effective interval, STOP contract, and last observed
  state/checkpoint.
- Last fresh physical measurements, remaining work and ETA uncertainty.
- Final evidence location/digests and unresolved decisions.

Do not create a new conversation merely to retain this state. If the run is
linked to an issue, use the App's issue-artifact tools for preserved analysis
and update an existing artifact rather than duplicating it. A local artifact
is required when no issue is linked. This skill does not imply that an issue
or artifact has already been created.

## 3. Capacity guard and admission policy

Use **available** filesystem bytes for free-space headroom and distinguish them
from allocated directory bytes. Normalize `df`/`du` output units and platform
behavior. Identify the filesystem backing each relevant path; `/workspaces`
and `/tmp` may share a filesystem. Check both paths, but do not double-count
capacity or concurrent allocations on the same filesystem.

For every appliance node and every filesystem involved in its writes:

```text
appliance_floor = max(25 * 1024^3, 0.10 * filesystem_capacity_bytes)
codespace_floor = max(10 * 1024^3, 0.15 * filesystem_capacity_bytes)
```

Apply the Codespace floor independently to the backing filesystems of
`/workspaces` and `/tmp`. Apply appliance floors per node, not to a combined
cluster total. User-approved stricter floors win. A target that cannot coexist
with the floor is infeasible: report `blocked`, not "almost complete."

Before each batch, estimate both:

1. **Projected budget:** remaining payload, Git objects/packing and clones,
   replication, temporary upload staging, indexes, retries/orphan allowances,
   logs, existing inflight work, and safety margin.
2. **Actual budget:** current free bytes and observed physical growth on each
   node and Codespace filesystem. Logical request bytes are not a substitute.

Calibrate estimates with the canary. Do not assume compression ratios,
deduplication, replication amplification, or reclaim latency. Recalculate after
each material change in payload or concurrency.

Admission requires, for each affected filesystem:

```text
available_bytes
  - reserved_evidence_bytes
  - outstanding_allocations_not_yet_visible_in_the_sample
  - conservative_next_batch_peak_bytes
  > applicable_floor
```

Account for shared filesystems once. Never subtract already-observed allocations
twice. Stop admitting work if the projected budget exceeds actual headroom.

### Five-second enforcement requirements

At every guard tick:

1. Validate its exclusive run identity and owned supervisor/process identities.
2. Read current Codespace `df` measurements for both required paths.
3. Validate newest local collector samples for **both** appliance nodes.
4. Reject replayed sequence numbers, future/skewed timestamps, malformed data,
   missing paths, changed node identity, and stale samples.
5. Track freshness using monotonic receipt time plus original source age.
   A reconnect must not make an old sample fresh. Record UTC for humans and
   monotonic elapsed time for deadlines.
6. Re-evaluate floors, projected growth, inflight bytes, and maximum duration.
7. Check the run's STOP request and guard-liveness enforcement.
8. Disable admission and trigger owned-process STOP on any unsafe condition.

Collector freshness must expire in **no more than 180 seconds**. Missing
heartbeat starts unsafe; there is no startup "assume healthy" interval.
The independent guard-liveness enforcement should use a shorter timeout, such
as 15 seconds. A healthy guard cannot renew a missing appliance heartbeat.

The 60-second appliance cadence creates an observation gap. Bound in-flight
and newly admitted bytes so the worst-case growth during the entire stale
window plus termination grace cannot consume the remaining floor. If a
conservative bound is unavailable, reduce workload/admission or block the run.
Five-second guard checks alone cannot guarantee safety from stale appliance
capacity data.

On the local machine, the collector must also fail closed on unsafe capacity
and publish an authenticated stop signal when possible. The Codespace timeout
is the fallback when publication or SSH collection fails. Do not start a
load-producing monitor on GHES or install an undocumented appliance daemon.
Read-only commands must be approved and appropriate for that GHES version and
topology; arrange sufficiently bounded measurements that the collector can
maintain its cadence without concurrent expensive recursive scans.

## 4. State transitions and STOP contract

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `pending` | Spec recorded, no workload running | `running`, `blocked`, `stopped` |
| `running` | Admission enabled under healthy guards | `draining`, `failed` |
| `draining` | Admission disabled; owned inflight work stopping or finishing | `complete`, `failed`, `stopped`, `blocked` |
| `complete` | Writers exited, evidence retained, acceptance verified | Terminal |
| `failed` | Execution or safety failure; admission disabled | Terminal until explicit reconciliation |
| `stopped` | Requested stop completed; targets not claimed complete | Terminal until explicit reconciliation |
| `blocked` | Cannot safely proceed or verify | Terminal until blocker resolved and journaled |

Record process-liveness separately: `failed` never means surviving descendants
can be ignored. Remain in recovery and escalate until owned writers are gone.
Re-entry after a terminal state requires a journaled resume decision, not an
unrecorded transition back to `running`. Natural workload exit enters
`draining` for evidence and physical verification, not immediately `complete`.

### STOP is a required adapter, not a repository command

There is no documented `./stop-run.sh` or universal STOP API in this repository.
The supervisor adapter must provide a tested equivalent of:

```text
request_stop(run_id, reason, requested_at_utc)
acknowledge_stop(run_id, admission_disabled_at_utc)
drain_owned_work(run_id, deadline)
verify_no_owned_writers(run_id)
persist_final_evidence(run_id)
```

An atomic `stop-request.json` is one possible transport. It is inert unless a
tested supervisor consumes it. Scope requests to an exact run, deduplicate them,
and require an acknowledgment. Do not advertise a stop as successful merely
because a request file was written.

Launch jobs in isolated owned sessions/process groups on the Linux Codespace,
and record UID, PID, PGID, session ID, start time, and run ownership at launch.
Account for descendants and prevent untracked daemonization. A PID alone can
be reused. Verify identity immediately before signaling; never signal an
unverified group or an entire terminal/Codespace session.

Use a supported supervisor or a tested run-specific launcher rather than
assuming that `tmux`, a background shell, or parent PID termination manages
descendants. `tmux kill-session` is not proof that all workloads stopped.
Do not use `pkill`, `killall`, or pattern-based process termination.

### Ordered shutdown

1. Journal the trigger and disable new admission immediately.
2. Persist the latest sanitized logs and counters before cleanup can remove them.
3. Request cooperative shutdown of **owned** worker jobs and descendants,
   using each worker's tested signal behavior. Preserve the reporting/collector
   path long enough to retain evidence.
4. Apply a bounded grace period, for example the approved 60 seconds. Continue
   five-second capacity enforcement; low headroom may require shorter grace.
5. Re-verify process ownership and start identities. Escalate to hard termination
   only for remaining owned processes after the deadline, and document why.
6. Reap exited workers, verify no owned writers remain, flush evidence, and
   perform bounded physical measurements.
7. Set the final state and remove only this run's scheduled check-in. Stop only
   owned guard/collector processes once they no longer protect running writers.

For `simulate-day-of-traffic.sh`, the cleanup trap calls `wait` before emitting
its final report and deletes `COUNTER_DIR` afterward. Sending TERM to the parent
alone can therefore wait on live worker children rather than stop them promptly.
The adapter must manage the owned worker tree and reporter explicitly. Preserve
counter snapshots while the run is active; after cleanup they may be unavailable.
The script's built-in report fires every 60 seconds and its detailed final
latency statistics are not necessarily present in each periodic report.

For `seed-issue-attachments.py`, interruption cancels queued futures but waits
for in-flight uploads through executor shutdown. Do not promise instant
termination or interpret an interrupt as proof that no upload completed.
Reserve capacity for in-flight completion and reconcile results afterward.

Never delete runtime files, clones, repositories, or assets while their owned
writers are active. Preserve final logs/counters before deleting even disposable
run files. An emergency stop is authorized by the agreed safety contract;
Codespace stop/deletion and credential revocation are separate user decisions.

## 5. Copilot App five-minute check-ins

Prefer a **same-session automation**: it wakes the original conversation with
its retained context. App-wide `save_workflow` automations start new sessions
and are not the default for this run. Do not invent a cron syntax or assume
an App automation is a precise execution scheduler.

### Ordinary current-session setup

The following are tool-call templates, not shell commands:

1. Read `get_session_automation({})` before creating or editing anything.
2. If no automation exists, create the check-in with:

   ```json
   {
     "interval": "minutes",
     "every_minutes": 5,
     "prompt": "<fully substituted standalone prompt below>"
   }
   ```

   Call `save_session_automation` with that object.

3. If the ordinary session already has **this run's** check-in, update it with
   the same current-session call. Preserve the original ownership evidence.
4. If it has an unrelated automation, **do not overwrite it silently**. Present
   the conflict and obtain a decision about replacing it or using an explicitly
   coordinated session. Until resolved, label scheduled chat updates unavailable;
   independent guards remain mandatory.
5. Read `get_session_automation({})` again, verify the effective schedule/prompt,
   and persist ownership in the coordinator artifact and nonsecret run spec.

For ordinary current-session calls, omit `automation_id`, `session_id`,
`execution`, `inline`, and `automation_session_id`. The current session is the
target. Do not borrow persistent Project-only fields.

For a **persistent Project**, a different owned automation, or a coordinated
child target, first inspect the tools' current full schemas and current ownership
with discovery/read tools. Project coordinators can own multiple schedules;
ordinary singleton addressing is not necessarily valid there. Select exactly
the owned automation/child according to the discovered schema. Do not guess
identifiers or create additional children merely to bypass a schedule conflict.

Scheduling may require user permission or be unavailable on the execution
host. The App's effective schedule and idle/admission semantics determine when
a check-in runs; a busy conversation, sleep, shutdown, permission prompt, or
disconnect can delay or prevent delivery. Report these limits, and do not
promise precise five-minute messages. Do not repeatedly retry a permission
denial. The run must remain safe without any chat delivery.

### Standalone scheduled prompt template

Substitute **all** placeholders before saving. No real hostnames, usernames,
tokens, key paths, or private telemetry are embedded in this template.

```text
Check in on the existing authorized GHES load-lab run <RUN_ID>.
This is monitoring/recovery only; do not launch a duplicate run, provision another
Codespace, increase approved limits, or restart a terminal job automatically.

Original coordinator: <SESSION_ID>.
Codespace: <CODESPACE_NAME>.
Approved remote checkout: <CHECKOUT_PATH>.
Nonsecret artifacts: <CHECKOUT_PATH>/tmp/<RUN_ID>/.
Coordinator artifact: <DURABLE_ARTIFACT_REFERENCE>.
Approved maximum duration/concurrency: <LIMITS>.
Acceptance criteria, normalized bytes and node scope: <TARGETS>.
STOP adapter and acknowledgment location: <TESTED_STOP_CONTRACT>.
This run's automation ownership record: <OWNERSHIP_REFERENCE>.

1. Read run-spec.json, status.json, newest complete manifest.jsonl events,
   collector and guard heartbeats, and any STOP request. Check run ID, source
   revision, timestamp/sequence freshness and ownership. Use read-only access
   until recovery needs an authorized stop. Never read or publish secrets.
2. Confirm both appliance nodes' latest sample ages are below the configured
   stale threshold, never greater than 180 seconds, and that independent
   five-second guard/liveness enforcement is healthy. Verify /workspaces and
   /tmp backing-filesystem headroom and the per-node appliance floors. Missing
   telemetry is unsafe, not success.
3. If unsafe or stopped by a guard, use only the tested owned STOP contract,
   verify acknowledgment and remaining descendants, and explain the trigger.
   If the remote path is inaccessible, do not fabricate status or freshness:
   report last-known data, reconnect through the approved route, and rely on
   the independent watchdog to fail closed. Never disable guardrails.
4. Reconcile committed work with journal and remote refs/IDs when needed. Do
   not repeat unconfirmed mutations before checking whether they succeeded.
5. Calculate progress and ETA from 3–5 fresh comparable samples and remaining
   dependencies. Keep progress below 100% until final physical verification.
   Do not invent p95/p99, throughput, replication completion, or an ETA.
6. Post one concise, substantive status update: UTC sample times, state/phase,
   each target's measured physical progress, committed work, recent throughput,
   ETA range/confidence, free-space floors, errors, next action, and decisions
   needed. Separate logical uploaded bytes from actual physical consumption.
7. If all owned jobs exited, retain logs/counters, perform final bounded physical
   verification on both nodes, and save sanitized evidence. Mark complete only
   when every approved acceptance criterion passes.
8. On a final complete/failed/stopped/blocked outcome, read the current automation
   and ownership record before removing only this run's check-in. For an ordinary
   current-session singleton that is still this run's automation, call
   save_session_automation({"clear":true}). Do not clear an unrelated/replaced
   schedule. Persistent Project targeting requires the discovered exact schema.
   Stop only owned guards after confirming no writers survive. Preserve all
   required evidence. Ask before stopping/deleting the Codespace or revoking
   user-owned credentials; follow only explicitly approved expiration/revocation
   for run-minted short-lived PATs.
```

### Regular and event-driven updates

Post after authorization/preflight, canary completion, each phase transition,
every available scheduled five-minute check-in, a meaningful error or change
in ETA, safety stop, resume decision, and final verification. Avoid empty
"still working" messages. An update should distinguish observation time from
message time:

```text
Run <run-id> — <running/draining/...>, sampled <UTC>, reported <UTC>.
Phase: <phase>; committed <done>/<planned> units; progress <capped-percent>.
Physical storage: node A <bytes>/<target>, node B <bytes>/<target>.
Git storage: <per-node actual bytes and approved range>.
Capacity: <each appliance filesystem and Codespace filesystem available/floor>.
Telemetry: collector ages <A/B>; guard age <age>; admission <enabled/disabled>.
Throughput: <range, units, 3–5-sample window>; ETA <range or unavailable>, <confidence>.
Errors: <counts and sanitized cause>; dependencies/replication: <status>.
Next: <bounded action>; user decision: <none or exact decision>.
```

## 6. Honest progress and ETA

Use three to five **fresh, comparable** samples with UTC timestamps, elapsed
monotonic intervals, and a consistent counter/measurement definition.
Compute rates from differences, not a script's configured concurrency or
payload size. Reset the window after a restart, counter reset, payload change,
or phase change. A decreasing physical measurement needs explanation, not
an absolute-value growth rate.

For a growth target:

```text
remaining_bytes = max(0, target_bytes - actual_physical_bytes)
interval_growth_rate = (new_physical_bytes - old_physical_bytes) / elapsed_seconds
ETA range = remaining_bytes / recent positive rate range
```

Use low/high positive observed rates as an illustrative range, not a statistical
confidence interval. If fewer than three comparable samples exist, growth is
zero/negative, data is stale, or replication is catching up, report "ETA
unavailable" and explain why. Include drain/replication/verification time
separately; do not assume it is zero.

For duration-bound traffic, show elapsed versus approved duration as traffic
phase progress, not proof of storage acceptance. For independent parallel jobs,
use the longest remaining dependent path; for serial dependencies, add their
remaining durations. Do not sum independent parallel ETAs or multiply progress
by worker count. Report the bottleneck node when both nodes must reach target.

Cap displayed overall progress at **99%** until all physical acceptance checks
pass and writers have exited. Logical units can be 100% while the run is still
`draining`. Git range acceptance is a pass/fail/range check, not "more is better":
overshooting the maximum fails that criterion.

The simulator supplies counts and final latency count/sum/min/max and averages/
trends. Without a retained histogram or raw sample set, do not invent median,
p95, or p99. Operation counters are not automatically bytes stored, distinct
repositories, or successfully replicated objects.

## 7. Failure handling and bounded recovery

| Symptom | Immediate response | Safe recovery |
| --- | --- | --- |
| Appliance or Codespace floor threatened | Disable admission; stop owned work within budget; preserve evidence | Recalculate budget; obtain approval for a feasible changed target/capacity plan |
| Local collector silent, node unavailable, SSH identity mismatch | Fail closed; never refresh missing heartbeat | Restore approved connectivity/identity; collect fresh samples on both nodes |
| Guard process gone or stale | Independent enforcement stops writers | Verify no survivors; repair/test enforcement before any resume |
| App check-in delayed or denied | Report scheduling limitation when possible | Maintain independent guards; resolve permission/conflict without duplicate schedules |
| API 401/403 or unauthorized web login | Stop the affected phase; sanitize diagnostics | Verify intended identity, permissions, expiry, auth mode; obtain authorized credentials |
| API throttling or secondary limits | Reduce admission, honor retry guidance and approved bounded backoff | Resume at lower authorized rate; never rotate identities to evade policy |
| Upload 500 or ambiguous timeout | Stop/reduce the failing batch; retain operation intent | Canary a smaller payload, reconcile assets, bound retries; no blanket endless retries |
| Git push rejected or unexpected remote refs | Stop that job before force-pushing | Verify journaled ownership and refs; never overwrite unrecorded repositories |
| Codespace stopped/rebuilt or process status unknown | Treat last snapshot as stale; no immediate restart | Reconnect, inventory exact ownership, reconcile remote mutations, obtain resume decision |
| Measurement parse error, counter reset, skew, or changed mount | Stop unsafe admission; invalidate ETA | Repair collector and baseline; verify consistent units and filesystem identity |
| Logical targets met but physical replication lags | Drain writers and continue bounded read-only verification | Report lag honestly; deadline expiry is `blocked`, not complete |

### Attachment upload compatibility

The issue attachment utility uses the authenticated web upload flow, not simply
a REST issue upload endpoint. On GHES 3.21, an asset policy can omit
`asset.href`; successful upload metadata may supply the `href` instead.
`seed-issue-attachments.py` contains the policy-to-upload-response fallback.
Inspect the deployed version and sanitized metadata shape rather than assuming
every installation returns an identical policy.

A reported GHES 3.21 observation was: an exact 10 MiB image upload failed with
HTTP 500, while an 8 MiB image worked. This is **not a universal GHES size
limit**, evidence that every 500 means "too large," or authorization to change
an appliance limit. Use bounded canaries and recorded payload sizes on the
actual environment; keep attachment shape/size within approved tested bounds.

Do not bypass SSO/MFA or web session requirements. A PAT permitting REST
operations does not necessarily authorize this web flow. Login failure can
reflect auth mode, identity, cookie expiry, permissions, or server behavior;
retain only sanitized errors, not cookies, CSRF tokens, or upload authorization
material.

An upload may finish before issue creation fails, leaving an orphan asset.
Logical failure counters therefore cannot imply zero storage growth. Journal
asset IDs/unsigned references when available and measure actual consumption.
Do not delete orphan assets through an undocumented appliance operation.

## 8. Resume without duplication

1. Load the original nonsecret spec and journal; verify run ID, Codespace,
   repository prefix, approved scope, source revision, and acceptance criteria.
2. Acquire the exclusive run lock. If an existing supervisor is alive, reconnect
   to it rather than starting a second run.
3. Verify every recorded process start identity and owned session/group.
   Unknown processes are a blocker; never terminate them by name.
4. Reconcile remote repository IDs, branch/tag refs, commit IDs, issue markers,
   and uploaded asset records with pending intents.
5. Skip operations already confirmed remotely. For ambiguity that cannot be
   reconciled, record uncertainty and seek a decision before repeating it.
6. Never force-push, recreate, or delete a repository merely because its name
   resembles the run prefix. Require the journaled ID and ownership evidence.
7. Inventory current physical usage, possible orphans, retained clones/runtime
   files, and remaining budgets. Validate fresh two-node collector data.
8. Test STOP/liveness controls again if the Codespace, launcher, or supervisor
   changed. Reset ETA samples, not committed-work accounting.
9. Record the explicit approved resume boundary and new process identities.
   Resume only uncompleted reconciled operations within the original limits,
   or obtain approval for changed limits.

A graceful stop can complete additional inflight work after the last checkpoint.
Treat the journal plus live remote state as authoritative, not a saved local
counter alone. Hard termination can leave unknown commits/uploads; reconcile
before cleanup or restart.

## 9. Final physical verification and lifecycle

Completion requires **actual physical measurements**, not generated file bytes,
upload success totals, wall-clock duration, or an API response saying "created."
After writers are confirmed stopped:

1. Preserve final report/logs and the last counter snapshots before simulator
   cleanup deletes runtime counters. Record whether the final report is missing
   because hard stop was necessary; do not reconstruct invented totals.
2. Collect fresh, timestamped, read-only measurements from **both** approved
   nodes. Verify node identity and normalize physical allocated-byte semantics.
3. If an Alambic target was requested, confirm the verified storage path
   (for example `/data/user/storage`) on each scoped node is at least its
   normalized target, using absolute or baseline-plus-delta semantics exactly
   as approved.
4. If a Git storage target was requested, confirm the verified Git path
   (for example `/data/user/repositories`) falls within the desired normalized
   range on each node in the approved Git scope. For traffic, separately
   verify the approved duration, mix, actual errors, and latency criteria.
5. Confirm each relevant appliance filesystem and the Codespace `/workspaces`
   and `/tmp` filesystems still meet their respective free-space floors.
6. Wait for the approved replication/settling window and collect another fresh
   measurement if required. Use documented health evidence appropriate for
   the environment; directory size equality alone does not prove replication
   health. Bound retries by the verification deadline.
7. Persist values, units, times, baseline/deltas, target predicates, measurement
   method, source identities, health evidence, process-exit evidence, and any
   uncertainty to `evidence/final-physical-verification.json`.
8. Copy sanitized final evidence and summary to coordinator-owned durable
   storage before any Codespace or artifact deletion.

Sparse files, compression, packfiles, object-storage implementation, orphan
uploads, and replication can make logical and physical measurements differ.
Report both with labels. If the relevant physical path cannot be measured
through authorized supported access, mark verification `blocked`; do not
replace it silently with a logical proxy.

Set `complete` only when every acceptance predicate passes. Otherwise select
`failed`, `stopped`, or `blocked` with concrete reasons and retained evidence.
Do not "top up" a target with further writes after shutdown without re-entering
the approved admission/resume process.

### Remove only owned scheduled work

Read the live schedule immediately before clearing it and compare run ownership,
prompt, and identifier with the durable record. If an ordinary current-session
singleton is still this run's check-in, call:

```json
{"clear": true}
```

Use `save_session_automation`; omit other fields for that ordinary own-session
call. If the schedule has been replaced, leave it alone. For a persistent
Project, discover and use the exact owned-automation schema instead of assuming
singleton semantics. Re-read to verify removal and record any failure; never
claim it was removed solely because the run ended.

Stop owned monitors only after no owned writers survive and final evidence is
retained. Do not stop other users' collectors or processes. Ask before:

- Stopping or deleting the Codespace, which affects retained workspace and cost.
- Deleting GHES repositories, users, issues, assets, or generated content.
- Revoking user-owned PATs, SSH keys, or web sessions.

Distinguish those credentials from **run-minted short-lived PATs**: record their
nonsecret identity/expiry and follow the explicitly approved expiration or
revocation policy. Never revoke a credential whose ownership is ambiguous.
Remove run-local secret files only after their approved use ends and evidence
is sanitized; keep secret values out of the final summary.

The final update should state what was verified on each node, what remains in
GHES and the Codespace, which processes/schedule stopped, evidence location,
unresolved risks, and the precise cleanup/cost/credential decisions still needed.
