# Workload recipes and implementation contracts

Use these recipes with the main [skill](../SKILL.md). They describe existing
utilities and the required behavior of any run-specific adapter. An adapter
is not already installed by this skill. Keep such run-specific files in the
session artifact directory or ignored remote `tmp/<run-id>/`, never in the
source tree with credentials.

## 1. Tool inventory

| Utility | Purpose | Important constraints |
|---|---|---|
| `prepare-appliance-for-traffic-sim.sh` | Synthetic users, org/repo permissions, impersonation tokens | Admin authorization; 1-50 users; warnings can leave partial results |
| `seed-issue-attachments.py` | Issues with unique PNG user assets | API token plus web session; bounded upload/generation pools |
| `generate-image.py` | Exact-size uncompressed PNG generation | 1-10240 KiB accepted locally; real server can reject boundary sizes |
| `seed-messy-repo.sh` | Expensive randomized Git history, branches, tags, optional text | Builds new history and force-pushes; disposable targets only |
| `generate-searchable-text.py` | Synthetic searchable records and book samples | 1-1024 KiB/file; source bytes are not indexed/physical bytes |
| `seed-repo-tags.sh` | Permanent refs without rebuilding history | Existing repo and push-capable token |
| `import-github-repos-into-org.sh` | Real GitHub repository branches/tags | Can overwrite refs; no guarantee of aggregate size |
| `simulate-day-of-traffic.sh` | Concurrent Git/API traffic with counters and timings | 1-16 workers; default mix mutates data and can dispatch workflows |

Initialize `test-data` only when needed:

```bash
git submodule update --init --recursive
```

The corpus submodule is private. Missing access must be reported; do not copy
the operator's github.com credentials into a remote Git URL to work around it.

## 2. Image attachments and Alambic targets

### Sizing

Use unique image seeds/names for each run/issue/image. Reusing identical
assets may defeat the requested physical growth. The generator uses random
RGB noise and uncompressed storage; check physical growth with the canary.

For fixed image size:

```text
logical_bytes_per_issue = images_per_issue * image_bytes
remaining_upload_bytes = max(0, normalized_target - verified_baseline)
estimated_images = ceil(remaining_upload_bytes / image_bytes)
estimated_issues = ceil(estimated_images / images_per_issue)
```

Subtract baseline only for a final-total target. Incremental targets have
their own additional-byte goal. Physical Alambic usage includes filesystem
rounding/metadata and may differ from logical payload totals. Run a bounded
final batch based on physical measurements rather than overshooting a tight
target with a whole large batch.

Example proposal:

- 100 decimal GB, 8 MiB/image: approximately 11,921 images.
- Eight images/issue: approximately 1,491 issues.
- A batch of 16 issues is 128 images, or 1 GiB of payload.
- Start with four upload workers and two generators; increase only after
  canary measurements show adequate CPU/RAM/API/storage headroom.

```bash
python3 seed-issue-attachments.py \
  -r <disposable-issue-repo> -n 1 -i 1 -s 1024 -p 1

python3 seed-issue-attachments.py \
  -r <disposable-issue-repo> -n 4 -i 8 -s 8192 \
  -p 4 --generation-workers 2
```

Do not add `--save-dir` to bulk runs. In-memory generation is bounded by
workers but still includes multiple copies for PNG generation, process
serialization, and multipart request construction. Measure peak RAM; do not
multiply the image size by one and call that the memory budget.

Current script limits:

- `-n`: 1-100000 issues per invocation.
- `-i`: 1-100 images per issue.
- `-s`: exact or `MIN-MAX` KiB, 1-10240.
- `-p`: 1-32 upload workers.
- `--generation-workers`: 1-32, no greater than upload workers.
- `--dry-run`: no GHES contact; cannot verify authentication or Alambic.
- `-k`: skips TLS verification; not the default.

### Upload protocol and compatibility

The script uses the authenticated browser upload policy, storage upload,
optional finalization, then REST issue creation. GHES does not provide the
same token attachment flow as github.com `gh issue create --attach`.

An asset URL may arrive in `policy.asset.href` or in the completed upload
response's `href`. The uploader supports both, retaining finalization.
If maintaining a different toolkit revision, verify this behavior before
launching a long run. Do not guess an asset URL from an ID.

One reference GHES 3.21.7 appliance rejected an exact 10 MiB PNG with HTTP
500 but accepted 8 MiB. This is evidence to run a size canary, not proof that
all GHES releases have an 8 MiB limit or that all HTTP 500s are size failures.
Do not change the image generator's maximum solely from that observation.

When upload-policy rate limiting is enabled, the documented toolkit warning
is 100 policies/user/hour. Observe actual configured behavior and headers.
For 100 GB this can dominate ETA. Do not disable rate limits or create extra
identities to evade policy without explicit authorized test configuration.

The current retry helper handles 429, 502, 503, 504, qualifying rate-limit
403s, and URL errors, with bounded retries. It does not retry every HTTP 500.
Do not blanket-retry asset finalization or issue creation after ambiguous
responses: reconcile whether the asset/issue already exists.

### Persistent attachment adapter

Before chaining batches, implement this contract:

1. Validate the run specification and exact target host/API pair.
2. Check STOP, disk guard status, recent appliance heartbeat, and deadline.
3. Reconcile manifests for this run only. Do not count unrelated dry-run
   records or another environment's `tmp/issue-attachments-*.jsonl`.
4. Choose the next bounded batch using remaining target and safe headroom.
5. Run the existing seeder and capture its exit status and output to a
   protected log. Each invocation generates its own run ID: record an
   explicit mapping from coordinator run ID to seeder run ID/manifest.
6. Count completed issue records separately from uploaded images that never
   became an issue. Failed issue creation can leave orphan assets.
7. Atomically update logical progress and retain timestamped history.
8. Ask the local observer for physical progress on every required HA node.
9. Stop admitting work when physical target is reached or safety fails.
10. Drain in-flight work; reconcile, verify links, then mark complete.

Do not blindly resume a crashed issue batch by restarting its entire count.
The script does not offer an idempotent `--resume` flag.

## 3. Predictable synthetic Git storage

Use a collection of bounded disposable repositories when the request is
aggregate disk size, not complex history. A useful proposal is 25 repos
with 1 GiB of unique random blobs each: 26.84 decimal GB before small metadata
overhead. The final physical target remains measured, not assumed.

### Generator design

Build one bare repository at a time using `git fast-import`:

1. Allocate a unique repo name in this run's namespace.
2. Create a private empty remote repository through the API.
3. Initialize a local bare repository in the ignored run directory.
4. Stream random blobs in small chunks instead of buffering the entire repo.
   Example: 32 blobs of 32 MiB, streamed as 1 MiB chunks, yields 1 GiB.
   Each file stays below the commonly applicable 100 MiB Git file limit;
   verify actual policies before ingestion.
5. Create a commit/tree on `refs/heads/main`, with unique blob contents.
6. Wait for `fast-import` to exit successfully before pushing.
7. Push without forced ref replacement; check the remote HEAD.
8. Journal remote repo/refs and byte counts, then delete only that verified
   local disposable repository.

The fast-import `data N` field is a **byte count**, not string length.
Compute `len(message.encode("utf-8"))` rather than hand-counting it.
Keep binary data out of shell variables. Close stdin and check the importer
exit code; terminate only the owned importer if generation fails.

### Avoid wasted pack CPU

Random blobs do not delta-compress well. Apply these settings per command,
including both importer and push; do not change global Git configuration:

```bash
git -c core.compression=0 \
    -c pack.compression=0 \
    -c pack.window=0 \
    -c pack.depth=0 \
    -c pack.threads=2 \
    -C <owned-bare-repo> push origin refs/heads/main
```

Use the same configuration prefix for fast-import. `pack.window=0` prevents
expensive delta comparisons; compression zero avoids futile deflation.
Two packing threads are a bounded starting point, not a promise of two-core
usage or a throughput guarantee. Existing packs may still be reused.
These client settings do not alter GHES's server-side pack/replication policy.

In a reference run, disabling delta searches reduced client pack CPU from
about one full core to roughly 13% of a core. Treat that as an observation,
not an acceptance threshold. Measure current processes/throughput safely.
A Git process already running does not inherit later configuration changes.

If restarting a CPU-heavy push:

- Verify the exact owned PID and process tree without printing credential
  arguments. Stop only that push/packer, preserving the generated bare repo.
- Reconcile remote refs in case the server accepted the interrupted push.
- Mark status `recovering` or `running` with a new attempt ID; retain the
  old failure event but do not display it as a current failure indefinitely.
- Retry the verified generated repository with explicit per-command settings.
- Validate the remote ref and resume the next repository, never regenerate
  or overwrite a remote whose state you have not reconciled.

### Credentials and disk footprint

Prefer process-local Git credential plumbing. An environment-provided
`http.https://<validated-host>/.extraheader` through `GIT_CONFIG_COUNT`,
`GIT_CONFIG_KEY_0`, and `GIT_CONFIG_VALUE_0` avoids a token in a remote URL.
Treat that environment as secret: never log it, inspect it via process
environment dumps, or write it into `.git/config`.

Existing repository scripts embed tokens in Git URLs. Do not use verbose
process output or `git remote -v` in those remote work directories. A future
adapter should use safer credential plumbing rather than copying this
pattern. Protect logs even when normal output appears non-secret.

Budget temporary local packed objects, push buffers, and any worktree,
not only the nominal source bytes. Before each repo require:

```text
free_bytes >= disk_reserve + estimated_peak_next_repo + logs_and_guard_margin
```

Monitor the actual mount receiving data. A bare streaming import avoids a
second full worktree copy. Sequential generation makes temporary space
bounded; the entire 26.84 GB corpus need not fit locally at once.

Do not remove an unverified local repo after a failed push; retain it for
recovery if headroom permits. If it must be removed for safety, journal
exactly what is missing and require deliberate regeneration.

## 4. Realistic messy Git and searchable source

For expensive history, use existing flags:

```bash
./seed-messy-repo.sh -r <new-disposable-repo> \
  -c 200 -b 25 -k 256 -t 2000
```

`-c` controls linear history, `-b` divergent branches, `-k` base random
binary KiB, and `-t` tags. Actual blob sizes and operation mix vary, so this
is not an exact byte-target generator. Run a smaller canary and size the
remaining workload from observed packed growth.

For bulk searchable source:

```bash
./seed-messy-repo.sh -r <new-search-repo> \
  -c 0 -b 0 -k 1 -t 0 -T 1024 -N 8000 -P 4 -B 250
```

- `-T`: 0-1024 KiB per file.
- `-N`: up to 20000 files; requires nonzero `-T`.
- `-P`: 1-16 generators.
- `-B`: 1-1000 files per commit.
- 8000 x 1 MiB is **7.8125 GiB / 8.39 GB of source**, not 8 GB physical Git
  storage or Elasticsearch.

Text compresses and Git deltas can shrink it. Index expansion, indexing
delay, and search exclusions also matter. Do not disable compression
globally to turn a search experiment into a different workload.
Verify representative content through approved search/API reads after
ingestion; never write directly to Elasticsearch.

The script force-pushes generated history. Reserve it for new/disposable
repositories, and never rerun it just to append tags.

## 5. Real repositories and tags

Only mirror sources the user selected/approved and can lawfully access:

```bash
./import-github-repos-into-org.sh \
  -o <approved-org> -p <unique-prefix> \
  cli/cli hashicorp/terraform
```

The source list above is illustrative, not permission to import it.
Read repository size metadata for planning, then measure actual bare packed
size and remote storage. GitHub API size is not a guaranteed transfer/physical
size. Stop between repositories once the approved range is reached.

The script imports branch/tag refs, not issues, releases, LFS payloads,
packages, or every object a shallow/file listing might imply. It pushes
replacement refs with pruning; do not target valuable existing history.

For permanent tags on an existing disposable target:

```bash
./seed-repo-tags.sh -r <existing-disposable-repo> -n 2000 -p <run-prefix>
```

For churn, use `api_tag_ref` traffic. It creates 3-10 refs per operation
and removes about half, so persistent ref count still grows. Account for
that growth and avoid namespace collisions.

## 6. Prepare traffic identities

```bash
./prepare-appliance-for-traffic-sim.sh \
  -n 16 -p <unique-user-prefix> -r <traffic-target-repo>
```

This creates synthetic users, membership, a repository/seed content, push
permissions, impersonation tokens, and `tmp/sim-users.json`.
The default existing admin login assumption may not match the user's account:
set/verify `admin_user` as required by the script.

Preparation mutates GHES even if the later traffic mix is read-only.
Get permission for preparation separately; reuse existing approved identities
where appropriate. Protect the token manifest with mode `0600` and the parent
directory with `0700`.

Verify every required user's effective access and a sample authenticated
read/write before traffic. Changing the target repo with `-r` does not
automatically provision access across all 25 generated repositories.
Run preparation/permission assignment for each target as approved, or use a
single designated traffic repo. Do not launch one simulator per repository
without a combined concurrency budget.

## 7. Traffic mix selection and ramp

Parameters:

- `-u`: simulator manifest; omission uses the config's admin token.
- `-r`: one target repo per invocation.
- `-p`: 1-16 workers.
- `-d`: seconds; validate positive bounded input before calling.
- `-j`: maximum random jitter in seconds; validate nonnegative input.
- `-w`: comma-separated integer operation percentages totaling 100.
- `-x`: disabled operations; weights rebalance as implemented.
- `-v`: verbose operations; review token exposure before enabling.

Allowed operations:

| Operation | Effect |
|---|---|
| `git_clone`, `git_fetch` | Client clones/fetches; CPU/network/local-disk load |
| `git_push`, `git_branch` | Commits, files, and branches |
| `api_read`, `api_refs_read` | Repository/issues/search/refs reads |
| `api_tag_ref` | Persistent tag-ref growth and churn |
| `api_commit` | Contents API commits |
| `api_issue`, `api_issue_comment`, `api_issue_close` | Issue creation/comment/closure |
| `api_pr`, `api_merge` | PR creation/activity and merges |
| `api_fork_pr_merge` | Fork writes, upstream PRs, merges |
| `workflow_dispatch` | Workflow execution; requires approved runner/cost scope |

Do not call the default distribution read-only. If mutation is not approved,
use an explicit read-only mix and no preparation unless separately approved:

```bash
./simulate-day-of-traffic.sh \
  -r <approved-repo> -u <protected-users-manifest> \
  -p 2 -d 120 -j 5 \
  -w 'git_clone=35,git_fetch=25,api_read=20,api_refs_read=20'
```

After a successful canary, a proposed full mixed run might be:

```bash
./simulate-day-of-traffic.sh \
  -r <disposable-traffic-repo> -u <protected-users-manifest> \
  -p 8 -d 3600 -j 10 \
  -w 'git_clone=25,git_fetch=20,api_read=20,api_refs_read=15,git_push=5,api_commit=5,api_issue=5,api_tag_ref=5'
```

Ramp concurrency rather than restarting overlapping full runs. Measure a
low-worker interval first, then stop/drain it and start the next approved
stage. The script's concurrency is not requests/second: operation duration
and jitter change achieved throughput. If the user gives an RPS goal, explain
this limitation and calibrate or implement a reviewed rate controller.

For clone-heavy workloads, budget at least all persistent worker clones,
active temporary clones, fork/worktrees, Git objects, and logs. Observe both
`/tmp` and `/workspaces`. A disk-safe seeding generator does not make a
16-worker cloning workload disk-safe.

## 8. Traffic reports and acceptance

The simulator prints progress about every 60 seconds. App conversation
updates are a separate scheduled task. It reports operation success/errors,
per-operation timing count/mean/min/max, early/late means and trends, and
per-user results. It does not provide percentile latency: never invent p95.

The runtime directory is printed at startup and lives under
`/tmp/ghes-traffic-sim-<suffix>`. Preserve the protected full log and final
stdout report in the run directory. Capture counters before cleanup if needed;
the script's cleanup removes its counter directory. Do not publish raw logs
without secret review.

Worker errors can be counted while the script returns success. Acceptance
must use actual counters and the approved error/latency thresholds, not only
exit status. Distinguish sampled/no-op operations from confirmed effects
using the final report's actual counters, such as actual merges.

Read the installed stop/cleanup behavior before supervising it. The current
cleanup waits for children, so sending TERM to the parent alone may not
produce a quick stop. The run adapter and guard must own the worker process
tree and implement bounded draining and escalation without name-based kills.
