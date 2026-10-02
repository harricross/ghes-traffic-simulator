#!/usr/bin/env bash
# simulate-day-of-traffic.sh
#
# Simulates a day of GHES traffic by running git clone/fetch operations and
# REST API calls concurrently, targeting 5-16 parallel clone-level operations
# to exercise spokes, babeld, and the API layer under realistic load.
#
# When given a users manifest (produced by prepare-appliance-for-traffic-sim.sh)
# each worker randomly impersonates a different sim user per operation, so the
# traffic appears in babeld/audit logs with varied identities rather than all
# coming from a single admin token.
#
# Pair with seed-messy-repo.sh to clone a repo with deep history, binary blobs,
# and many diverged branches -- maximising packfile work on every clone.
#
# Usage:
#   ./simulate-day-of-traffic.sh [-u <users_manifest>] [-r <repo>] \
#                                [-p <parallelism>] [-d <duration>] \
#                                [-j <jitter>] [-w <traffic_mix>] \
#                                [-x <disabled_ops>] [-v]
#
#   -u FILE  JSON manifest from prepare-appliance-for-traffic-sim.sh
#            (default: admin token from .gh-api-examples.conf used for all ops)
#   -r NAME  Override the repo name from .gh-api-examples.conf
#            (use the name given to seed-messy-repo.sh, e.g. "messy-repo")
#   -p N     Number of parallel workers (default: 5, max: 16)
#   -d N     Duration in seconds (default: 3600)
#   -j N     Max random jitter in seconds between worker ops (default: 10)
#   -w LIST  Comma-separated operation percentages, for example
#            "git_clone=50,git_fetch=25,api_read=25"
#            When supplied, omitted operations are set to 0%; values must total 100%.
#   -x LIST  Comma-separated operations to disable (for example,
#            "git_clone,git_fetch,git_push,git_branch")
#   -v       Verbose: show every operation, not just summaries
#
# Operation mix (approximate realistic day distribution):
#   git_clone        18% - most expensive GHES op; exercises spokes + babeld
#   git_fetch        12% - continuous CI/CD-style fetches on a persistent clone
#   git_push          7% - publish a small commit from a simulator worktree
#   git_branch        6% - create, commit to, and push a new branch
#   api_read         11% - list repos, list issues, search code
#   api_refs_read    12% - list/paginate/match refs via git/refs and git/ref
#   api_tag_ref       8% - create a batch of tag refs, then delete about half
#   api_commit        6% - create a file commit via the Contents API
#   api_issue         5% - open a new issue with an optional PR link and assignee
#   api_issue_comment 4% - add activity entries to an open simulator issue
#   api_issue_close   2% - close a randomly selected simulator issue
#   api_pr            4% - open a PR and add follow-up commits from mixed users
#   api_merge         2% - merge at most one open PR every 60 seconds
#   api_fork_pr_merge 2% - push to a user fork, PR to upstream, then merge into main
#   workflow_dispatch 1% - trigger an Actions workflow dispatch event

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
USERS_MANIFEST=""
REPO_OVERRIDE=""
PARALLELISM=5
DURATION=3600
JITTER=10
TRAFFIC_WEIGHTS=""
DISABLED_OPS=""
VERBOSE=false

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while getopts "u:r:p:d:j:w:x:v" opt; do
  case $opt in
    u) USERS_MANIFEST=$OPTARG ;;
    r) REPO_OVERRIDE=$OPTARG ;;
    p) PARALLELISM=$OPTARG ;;
    d) DURATION=$OPTARG ;;
    j) JITTER=$OPTARG ;;
    w) TRAFFIC_WEIGHTS=$OPTARG ;;
    x) DISABLED_OPS=$OPTARG ;;
    v) VERBOSE=true ;;
    *)
      echo "Usage: $0 [-u users_manifest] [-r repo] [-p parallelism] [-d duration_s] [-j jitter_s] [-w traffic_mix] [-x disabled_ops] [-v]" >&2
      exit 1
      ;;
  esac
done

if (( PARALLELISM < 1 || PARALLELISM > 16 )); then
  echo "ERROR: -p must be between 1 and 16." >&2; exit 1
fi

OP_NAMES=(git_clone git_fetch git_push git_branch api_read api_refs_read api_tag_ref api_commit api_issue api_issue_comment api_issue_close api_pr api_merge api_fork_pr_merge workflow_dispatch)
WEIGHT_NAMES=()
WEIGHT_VALUES=()
TRAFFIC_WEIGHT_TOTAL=0
if [[ -n "${TRAFFIC_WEIGHTS}" ]]; then
  TRAFFIC_WEIGHTS="${TRAFFIC_WEIGHTS// /}"
  IFS=',' read -r -a TRAFFIC_WEIGHT_LIST <<< "${TRAFFIC_WEIGHTS}"
  for weight_entry in "${TRAFFIC_WEIGHT_LIST[@]}"; do
    if [[ "${weight_entry}" != *=* ]]; then
      echo "ERROR: traffic weight '${weight_entry}' must use operation=percentage syntax." >&2
      exit 1
    fi
    weight_name="${weight_entry%%=*}"
    weight_value="${weight_entry#*=}"
    valid=false
    for known_op in "${OP_NAMES[@]}"; do
      [[ "${weight_name}" == "${known_op}" ]] && valid=true
    done
    if [[ "${valid}" != true ]]; then
      echo "ERROR: unknown operation '${weight_name}' in -w. Valid operations: ${OP_NAMES[*]}" >&2
      exit 1
    fi
    if [[ ! "${weight_value}" =~ ^[0-9]+$ ]]; then
      echo "ERROR: traffic weight for '${weight_name}' must be an integer from 0 to 100." >&2
      exit 1
    fi
    weight_value=$((10#${weight_value}))
    if (( weight_value > 100 )); then
      echo "ERROR: traffic weight for '${weight_name}' must be an integer from 0 to 100." >&2
      exit 1
    fi
    if (( ${#WEIGHT_NAMES[@]} > 0 )); then
      for known_weight_name in "${WEIGHT_NAMES[@]}"; do
        if [[ "${known_weight_name}" == "${weight_name}" ]]; then
          echo "ERROR: duplicate traffic weight for '${weight_name}' in -w." >&2
          exit 1
        fi
      done
    fi
    WEIGHT_NAMES+=("${weight_name}")
    WEIGHT_VALUES+=("${weight_value}")
    TRAFFIC_WEIGHT_TOTAL=$(( TRAFFIC_WEIGHT_TOTAL + weight_value ))
  done
  if (( TRAFFIC_WEIGHT_TOTAL != 100 )); then
    echo "ERROR: traffic percentages in -w must total 100 (got ${TRAFFIC_WEIGHT_TOTAL})." >&2
    exit 1
  fi
fi
DISABLED_OP_LIST=()
if [[ -n "${DISABLED_OPS}" ]]; then
  DISABLED_OPS="${DISABLED_OPS// /}"
  IFS=',' read -r -a DISABLED_OP_LIST <<< "${DISABLED_OPS}"
  for disabled_op in "${DISABLED_OP_LIST[@]}"; do
    valid=false
    for known_op in "${OP_NAMES[@]}"; do
      [[ "${disabled_op}" == "${known_op}" ]] && valid=true
    done
    if [[ "${valid}" != true ]]; then
      echo "ERROR: unknown operation '${disabled_op}' in -x. Valid operations: ${OP_NAMES[*]}" >&2
      exit 1
    fi
  done
fi

# ---------------------------------------------------------------------------
# Load config (same pattern as every other script in this repo)
# ---------------------------------------------------------------------------
CONF="./.gh-api-examples.conf"
if [[ ! -f "$CONF" ]]; then
  echo "ERROR: $CONF not found. Populate it before running this script." >&2; exit 1
fi
# shellcheck source=/dev/null
. "$CONF"

: "${hostname:?hostname not set in $CONF}"
: "${org:?org not set in $CONF}"
: "${repo:?repo not set in $CONF}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN not set in $CONF}"
: "${GITHUB_API_BASE_URL:?GITHUB_API_BASE_URL not set in $CONF}"

# -r flag overrides the repo from config
[[ -n "${REPO_OVERRIDE}" ]] && repo="${REPO_OVERRIDE}"

GIT_HOSTNAME="${hostname}"
[[ "${GIT_HOSTNAME}" == "api.github.com" ]] && GIT_HOSTNAME="github.com"

# Preserve the generated curl failure mode without combining -f and
# --fail-with-body, which curl rejects.
if [[ "${VERBOSE}" == true ]]; then
  CURL_FLAGS="${curl_custom_flags:-} -v"
else
  CURL_FLAGS="${curl_custom_flags:-} --silent --show-error"
fi

timestamp_ms() {
  python3 -c 'import time; print(time.time_ns() // 1000000)'
}

# ---------------------------------------------------------------------------
# Load sim users from manifest (if provided)
#
# SIM_USERNAMES and SIM_TOKENS are parallel arrays indexed 0..N-1.
# pick_user <worker_id> sets CURRENT_USER and CURRENT_TOKEN for this op.
# Falls back to the admin token when no manifest is supplied.
# ---------------------------------------------------------------------------
SIM_USERNAMES=()
SIM_TOKENS=()

if [[ -n "${USERS_MANIFEST}" ]]; then
  if [[ ! -f "${USERS_MANIFEST}" ]]; then
    echo "ERROR: users manifest '${USERS_MANIFEST}' not found." >&2; exit 1
  fi
  if ! command -v jq &>/dev/null; then
    echo "ERROR: jq is required to parse the users manifest." >&2; exit 1
  fi

  count=$(jq 'length' "${USERS_MANIFEST}")
  for (( i=0; i<count; i++ )); do
    SIM_USERNAMES+=("$(jq -r ".[${i}].username" "${USERS_MANIFEST}")")
    SIM_TOKENS+=("$(jq -r ".[${i}].token" "${USERS_MANIFEST}")")
  done
fi

SIM_USER_COUNT="${#SIM_USERNAMES[@]}"

# Sets CURRENT_USER and CURRENT_TOKEN to a randomly chosen sim user,
# or to "admin" + the admin token when no manifest was loaded.
pick_user() {
  if (( SIM_USER_COUNT > 0 )); then
    local idx=$(( RANDOM % SIM_USER_COUNT ))
    CURRENT_USER="${SIM_USERNAMES[$idx]}"
    CURRENT_TOKEN="${SIM_TOKENS[$idx]}"
  else
    CURRENT_USER="admin"
    CURRENT_TOKEN="${GITHUB_TOKEN}"
  fi
}

# Build a git clone URL for the given token and repository coordinates
git_clone_url_for_repo() {
  local token="$1" owner="$2" repo_name="$3"
  local frst3="${token:0:3}"
  case "${frst3}" in
    ghs) echo "https://x-access-token:${token}@${GIT_HOSTNAME}/${owner}/${repo_name}.git" ;;
    *)   echo "https://${token}:x-oauth-basic@${GIT_HOSTNAME}/${owner}/${repo_name}.git" ;;
  esac
}

git_clone_url() {
  local token="$1"
  git_clone_url_for_repo "${token}" "${org}" "${repo}"
}

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
TMPDIR_ROOT="$(mktemp -d "/tmp/ghes-traffic-sim-XXXXXX")"
LOGFILE="${TMPDIR_ROOT}/simulator.log"
START_TS=$(date +%s)
END_TS=$(( START_TS + DURATION ))
PERF_MIDPOINT_TS=$(( START_TS + (DURATION / 2) ))

COUNTER_DIR="${TMPDIR_ROOT}/counters"
mkdir -p "${COUNTER_DIR}"
BRANCH_DIR="${TMPDIR_ROOT}/branches"
mkdir -p "${BRANCH_DIR}"
PR_DIR="${TMPDIR_ROOT}/prs"
mkdir -p "${PR_DIR}"
ISSUE_DIR="${TMPDIR_ROOT}/issues"
mkdir -p "${ISSUE_DIR}"
FORK_DIR="${TMPDIR_ROOT}/forks"
mkdir -p "${FORK_DIR}"
MERGE_INTERVAL=60
MERGE_STATE_FILE="${TMPDIR_ROOT}/last-merge.ts"

# Weighted operation table: one entry per percentage point
WEIGHTED_OPS=()
is_disabled() {
  local candidate="$1"
  local disabled_op
  if (( ${#DISABLED_OP_LIST[@]} == 0 )); then
    return 1
  fi
  for disabled_op in "${DISABLED_OP_LIST[@]}"; do
    [[ "${disabled_op}" == "${candidate}" ]] && return 0
  done
  return 1
}

add_weighted_op() {
  local op="$1" weight="$2"
  if [[ -n "${TRAFFIC_WEIGHTS}" ]]; then
    weight=0
  fi
  for (( n=0; n<${#WEIGHT_NAMES[@]}; n++ )); do
    if [[ "${WEIGHT_NAMES[$n]}" == "${op}" ]]; then
      weight="${WEIGHT_VALUES[$n]}"
      break
    fi
  done
  if is_disabled "${op}"; then
    return
  fi
  for (( n=0; n<weight; n++ )); do
    WEIGHTED_OPS+=("${op}")
  done
}

add_weighted_op git_clone 18
add_weighted_op git_fetch 12
add_weighted_op git_push 7
add_weighted_op git_branch 6
add_weighted_op api_read 11
add_weighted_op api_refs_read 12
add_weighted_op api_tag_ref 8
add_weighted_op api_commit 6
add_weighted_op api_issue 5
add_weighted_op api_issue_comment 4
add_weighted_op api_issue_close 2
add_weighted_op api_pr 4
add_weighted_op api_merge 2
add_weighted_op api_fork_pr_merge 2
add_weighted_op workflow_dispatch 1
WEIGHTED_OPS_LEN="${#WEIGHTED_OPS[@]}"
if (( WEIGHTED_OPS_LEN == 0 )); then
  echo "ERROR: -x disabled every operation; enable at least one operation." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Logging / counters
# ---------------------------------------------------------------------------
log() {
  local level="$1"; shift
  local msg="[$(date '+%H:%M:%S')] $*"
  echo "${msg}" >> "${LOGFILE}"
  if [[ "${VERBOSE}" == true ]] || [[ "${level}" != "DEBUG" ]]; then
    echo "${msg}"
  fi
}

bump_counter() {
  bump_counter_by "$1" 1
}

bump_counter_by() {
  local op="$1" amount="$2"
  local f="${COUNTER_DIR}/${op}"
  local lock_dir="${f}.lockdir"
  (
    while ! mkdir "${lock_dir}" 2>/dev/null; do
      sleep 0.01
    done
    trap 'rmdir "${lock_dir}" 2>/dev/null || true' EXIT
    local n=0; [[ -f "$f" ]] && n=$(cat "$f")
    echo $(( n + amount )) > "$f"
  )
}

read_counter() {
  local f="${COUNTER_DIR}/${1}"
  [[ -f "$f" ]] && cat "$f" || echo 0
}

perf_record_operation() {
  local op="$1" elapsed_ms="$2"
  local perf_prefix="${COUNTER_DIR}/_perf_${op}"
  local lock_dir="${perf_prefix}.lockdir"
  local phase="early"
  local now_s; now_s=$(date +%s)
  if (( now_s >= PERF_MIDPOINT_TS )); then
    phase="late"
  fi

  (
    while ! mkdir "${lock_dir}" 2>/dev/null; do
      sleep 0.01
    done
    trap 'rmdir "${lock_dir}" 2>/dev/null || true' EXIT

    local count_file="${perf_prefix}_count"
    local sum_file="${perf_prefix}_sum_ms"
    local min_file="${perf_prefix}_min_ms"
    local max_file="${perf_prefix}_max_ms"
    local phase_count_file="${perf_prefix}_${phase}_count"
    local phase_sum_file="${perf_prefix}_${phase}_sum_ms"

    local count=0 sum_ms=0 min_ms=0 max_ms=0 phase_count=0 phase_sum_ms=0
    [[ -f "${count_file}" ]] && count=$(cat "${count_file}")
    [[ -f "${sum_file}" ]] && sum_ms=$(cat "${sum_file}")
    [[ -f "${min_file}" ]] && min_ms=$(cat "${min_file}")
    [[ -f "${max_file}" ]] && max_ms=$(cat "${max_file}")
    [[ -f "${phase_count_file}" ]] && phase_count=$(cat "${phase_count_file}")
    [[ -f "${phase_sum_file}" ]] && phase_sum_ms=$(cat "${phase_sum_file}")

    count=$(( count + 1 ))
    sum_ms=$(( sum_ms + elapsed_ms ))
    if (( min_ms == 0 || elapsed_ms < min_ms )); then
      min_ms="${elapsed_ms}"
    fi
    if (( elapsed_ms > max_ms )); then
      max_ms="${elapsed_ms}"
    fi

    phase_count=$(( phase_count + 1 ))
    phase_sum_ms=$(( phase_sum_ms + elapsed_ms ))

    echo "${count}" > "${count_file}"
    echo "${sum_ms}" > "${sum_file}"
    echo "${min_ms}" > "${min_file}"
    echo "${max_ms}" > "${max_file}"
    echo "${phase_count}" > "${phase_count_file}"
    echo "${phase_sum_ms}" > "${phase_sum_file}"
  )
}

random_file_batch_size() {
  echo $(( 3 + RANDOM % 6 ))
}

build_diff_heavy_content() {
  local kind="$1" username="$2" ts="$3" file_index="$4" context="$5"
  local line_count=$(( 10 + RANDOM % 15 ))
  local line
  {
    printf "kind=%s\n" "${kind}"
    printf "user=%s\n" "${username}"
    printf "timestamp_ms=%s\n" "${ts}"
    printf "context=%s\n" "${context}"
    printf "file_index=%s\n" "${file_index}"
    printf "entropy_seed=%s%s%s\n" "${RANDOM}" "${RANDOM}" "${RANDOM}"
    for (( line=1; line<=line_count; line++ )); do
      printf "line_%02d token=%s%s%s mix=%d\n" \
        "${line}" "${RANDOM}" "${RANDOM}" "${RANDOM}" "$(( (line * 37 + RANDOM) % 997 ))"
    done
  }
}

api_status_code() {
  local token="$1" url="$2"
  curl --silent --show-error -o /dev/null -w "%{http_code}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${url}" || echo "000"
}

ensure_user_fork_ready() {
  local username="$1" token="$2"
  local safe_user="${username//[^a-zA-Z0-9_-]/-}"
  local marker="${FORK_DIR}/${safe_user}.ready"
  local fork_api_url="${GITHUB_API_BASE_URL}/repos/${username}/${repo}"
  local fork_create_status=""
  if [[ -f "${marker}" ]]; then
    return 0
  fi

  local status; status=$(api_status_code "${token}" "${fork_api_url}")
  if [[ "${status}" != "200" ]]; then
    fork_create_status=$(curl ${CURL_FLAGS} -X POST \
      -H "Accept: application/vnd.github+json" \
      -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
      -H "Authorization: token ${token}" \
      "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/forks" \
      -d '{}' -o /dev/null -w "%{http_code}") || true
  fi

  local attempt
  for (( attempt=1; attempt<=10; attempt++ )); do
    status=$(api_status_code "${token}" "${fork_api_url}")
    if [[ "${status}" == "200" ]]; then
      : > "${marker}"
      return 0
    fi
    sleep 2
  done
  log INFO "Fork ${username}/${repo} is unavailable after creation attempt (HTTP ${fork_create_status:-$status}); check fork policy and user access"
  return 1
}

api_put_file_commit() {
  local username="$1" token="$2" path="$3" message="$4" content="$5" branch="$6"
  local encoded; encoded=$(printf "%s" "${content}" | python3 base64encode.py)
  local payload
  if [[ -n "${branch}" ]]; then
    payload=$(jq -cn \
      --arg msg "${message}" \
      --arg content "${encoded}" \
      --arg branch "${branch}" \
      '{"message":$msg,"content":$content,"branch":$branch}')
  else
    payload=$(jq -cn \
      --arg msg "${message}" \
      --arg content "${encoded}" \
      '{"message":$msg,"content":$content}')
  fi
  curl ${CURL_FLAGS} -X PUT \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/contents/${path}" \
    -d "${payload}" > /dev/null
}

pick_issue_file() {
  local candidates=()
  local issue_file
  for issue_file in "${ISSUE_DIR}"/*.issue; do
    [[ -f "${issue_file}" ]] || continue
    [[ -f "${issue_file}.closed" ]] && continue
    candidates+=("${issue_file}")
  done
  (( ${#candidates[@]} > 0 )) || return 1
  printf "%s\n" "${candidates[$(( RANDOM % ${#candidates[@]} ))]}"
}

pick_user_pr_url() {
  local username="$1"
  local pr_file pr_user pr_url
  for pr_file in "${PR_DIR}"/*.pr; do
    [[ -f "${pr_file}" ]] || continue
    IFS='|' read -r pr_user pr_url _ < "${pr_file}"
    if [[ "${pr_user}" == "${username}" ]]; then
      printf "%s\n" "${pr_url}"
      return 0
    fi
  done
  return 1
}

# ---------------------------------------------------------------------------
# Operation implementations
# Each function receives (worker_dir, username, token) so every git or API
# call is authenticated as the chosen sim user rather than the admin.
# ---------------------------------------------------------------------------

op_git_clone() {
  local worker_dir="$1" username="$2" token="$3"
  local clone_dir="${worker_dir}/clone-$$"
  local url; url=$(git_clone_url "${token}")
  rm -rf "${clone_dir}"
  GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
    git clone --quiet "${url}" "${clone_dir}" 2>&1
  rm -rf "${clone_dir}"
}

op_git_fetch() {
  local worker_dir="$1" username="$2" token="$3"
  local fetch_dir="${worker_dir}/fetch-repo"
  local url; url=$(git_clone_url "${token}")

  # Clone once into the worker's persistent fetch dir; re-clone if the
  # remote credential changed (different user picked on a prior cycle).
  if [[ ! -d "${fetch_dir}/.git" ]]; then
    GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
      git clone --quiet "${url}" "${fetch_dir}" 2>&1
  else
    # Update the remote URL to match whichever user was chosen this cycle
    git -C "${fetch_dir}" remote set-url origin "${url}" 2>/dev/null || true
    GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
      git -C "${fetch_dir}" fetch --quiet --all 2>&1
  fi
}

op_git_push() {
  # Push a batch of files in one commit on the feature branch.
  # Runs as the chosen sim user so the commit author varies across the run.
  local worker_dir="$1" username="$2" token="$3"
  local push_dir="${worker_dir}/push-repo"
  local url; url=$(git_clone_url "${token}")
  local safe_user="${username//[^a-zA-Z0-9_-]/-}"

  if [[ ! -d "${push_dir}/.git" ]]; then
    GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
      git clone --quiet "${url}" "${push_dir}" 2>&1 || return 1
    git -C "${push_dir}" config user.email "${username}@example.com"
    git -C "${push_dir}" config user.name  "${username}"
  fi

  local branch="${new_branch:-feature-branch}"
  git -C "${push_dir}" checkout --quiet "${branch}" 2>/dev/null \
    || git -C "${push_dir}" checkout --quiet -b "${branch}" 2>/dev/null \
    || return 1

  local ts; ts=$(timestamp_ms)
  echo "${username} ${ts}" >> "${push_dir}/sim-push-log.txt"
  local file_count; file_count=$(random_file_batch_size)
  local file_index
  local file_dir="${push_dir}/sim-push/${safe_user}"
  local file_path
  mkdir -p "${file_dir}"
  git -C "${push_dir}" add sim-push-log.txt
  for (( file_index=1; file_index<=file_count; file_index++ )); do
    file_path="${file_dir}/push-${ts}-${RANDOM}-${file_index}.txt"
    build_diff_heavy_content "git_push" "${username}" "${ts}" "${file_index}" "${branch}" > "${file_path}"
    git -C "${push_dir}" add "${file_path}"
  done
  git -C "${push_dir}" commit --quiet -m "sim push by ${username} at ${ts}"
  GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
    git -C "${push_dir}" push --quiet origin "${branch}" 2>&1 || return 1
  bump_counter_by git_push_files "$file_count"
}

op_git_branch() {
  local worker_dir="$1" username="$2" token="$3"
  local branch_dir="${worker_dir}/branch-repo"
  local url; url=$(git_clone_url "${token}")
  local ts; ts=$(timestamp_ms)
  local worker_name="${worker_dir##*/}"
  local safe_user="${username//[^a-zA-Z0-9_-]/-}"
  local branch="sim/${safe_user}/${worker_name}/branch-${ts}-${RANDOM}"

  if [[ ! -d "${branch_dir}/.git" ]]; then
    GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
      git clone --quiet "${url}" "${branch_dir}" 2>&1 || return 1
  else
    git -C "${branch_dir}" remote set-url origin "${url}" 2>/dev/null || true
    GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
      git -C "${branch_dir}" fetch --quiet origin main 2>&1 || return 1
  fi

  git -C "${branch_dir}" config user.email "${username}@example.com"
  git -C "${branch_dir}" config user.name "${username}"
  git -C "${branch_dir}" checkout --quiet -B "${branch}" origin/main

  local file_count; file_count=$(random_file_batch_size)
  local file_index
  local file_dir="sim-branches/${safe_user}"
  mkdir -p "${branch_dir}/${file_dir}"
  for (( file_index=1; file_index<=file_count; file_index++ )); do
    local path="${file_dir}/branch-${ts}-${RANDOM}-${file_index}.txt"
    build_diff_heavy_content "git_branch" "${username}" "${ts}" "${file_index}" "${branch}" > "${branch_dir}/${path}"
    git -C "${branch_dir}" add "${path}"
  done
  git -C "${branch_dir}" commit --quiet -m "sim: create ${branch}"
  GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
    git -C "${branch_dir}" push --quiet origin "${branch}:${branch}" 2>&1 || return 1
  bump_counter_by git_branch_files "$file_count"

  printf "%s\n" "${branch}" > "${BRANCH_DIR}/${safe_user}-${worker_name}-${ts}-${RANDOM}.branch"
}

pick_pr_branch() {
  local candidates=()
  local branch_file
  for branch_file in "${BRANCH_DIR}"/*.branch; do
    [[ -f "${branch_file}" ]] || continue
    [[ -f "${branch_file}.pr" ]] && continue
    candidates+=("$(cat "${branch_file}")")
  done

  if (( ${#candidates[@]} > 0 )); then
    printf "%s\n" "${candidates[$(( RANDOM % ${#candidates[@]} ))]}"
  else
    printf "%s\n" "${new_branch:-feature-branch}"
  fi
}

op_api_read() {
  local _worker_dir="$1" username="$2" token="$3"
  local auth="Authorization: token ${token}"
  local accept="Accept: application/vnd.github+json"
  local apiver="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"
  local variant=$(( RANDOM % 4 ))
  case $variant in
    0)
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${GITHUB_API_BASE_URL}/orgs/${org}/repos?per_page=30" > /dev/null
      ;;
    1)
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/issues?per_page=30" > /dev/null
      ;;
    2)
      curl ${CURL_FLAGS} -H "${accept}" -H "${auth}" \
        "${GITHUB_API_BASE_URL}/search/repositories?q=org:${org}" > /dev/null
      ;;
    3)
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/commits?per_page=30" > /dev/null
      ;;
  esac
}

op_api_commit() {
  local _worker_dir="$1" username="$2" token="$3"
  local ts; ts=$(timestamp_ms)
  local safe_user="${username//[^a-zA-Z0-9_-]/-}"
  local file_count; file_count=$(random_file_batch_size)
  local file_index
  for (( file_index=1; file_index<=file_count; file_index++ )); do
    local path="sim-commits/${safe_user}/${ts}-${RANDOM}-${file_index}.txt"
    local content
    content=$(build_diff_heavy_content "api_commit" "${username}" "${ts}" "${file_index}" "${base_branch:-main}")
    api_put_file_commit "${username}" "${token}" "${path}" \
      "sim: ${username} @ ${ts} file ${file_index}" "${content}" "${base_branch:-main}" \
      || return 1
    bump_counter_by api_commit_files 1
  done
}

op_api_issue() {
  local _worker_dir="$1" username="$2" token="$3"
  local ts; ts=$(date +%s)
  local related_pr_url=""
  if related_pr_url=$(pick_user_pr_url "${username}"); then
    :
  else
    related_pr_url=""
  fi

  local entries=""
  local entry_count=$(( 2 + RANDOM % 4 ))
  local entry_index
  for (( entry_index=1; entry_index<=entry_count; entry_index++ )); do
    entries+=$'\n'"Entry ${entry_index}: simulator activity ${RANDOM}${RANDOM}."
  done
  local body="Automated issue created by the traffic simulator as user ${username}.${entries}"
  if [[ -n "${related_pr_url}" ]]; then
    body+=$'\n'"Related PR from the same user: ${related_pr_url}"
  fi

  local assignee_json='[]'
  if (( SIM_USER_COUNT > 0 )) && (( RANDOM % 100 < 75 )); then
    local assignee_idx=$(( RANDOM % SIM_USER_COUNT ))
    assignee_json=$(jq -cn --arg assignee "${SIM_USERNAMES[$assignee_idx]}" '[$assignee]')
  fi
  local payload; payload=$(jq -cn \
    --arg title "Sim issue by ${username} (${ts})" \
    --arg body "${body}" \
    --argjson assignees "${assignee_json}" \
    '{"title":$title,"body":$body,"assignees":$assignees}')
  local response
  response=$(curl ${CURL_FLAGS} -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/issues" \
    -d "${payload}") || return 1
  local issue_number; issue_number=$(printf "%s" "${response}" | jq -r '.number // empty')
  local issue_url; issue_url=$(printf "%s" "${response}" | jq -r '.html_url // empty')
  [[ -n "${issue_number}" ]] || return 1
  printf "%s|%s|%s|%s\n" "${issue_number}" "${username}" "${issue_url}" "${related_pr_url}" \
    > "${ISSUE_DIR}/${issue_number}.issue"
}

op_api_issue_comment() {
  local _worker_dir="$1" username="$2" token="$3"
  local issue_file; issue_file=$(pick_issue_file) || return 0
  local issue_number issue_user issue_url related_pr_url
  IFS='|' read -r issue_number issue_user issue_url related_pr_url < "${issue_file}"
  local comment="Entry from ${username}: activity ${RANDOM}${RANDOM}."
  if [[ -n "${related_pr_url}" ]]; then
    comment+=" Related PR: ${related_pr_url}"
  fi
  local payload; payload=$(jq -cn --arg body "${comment}" '{"body":$body}')
  curl ${CURL_FLAGS} -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/issues/${issue_number}/comments" \
    -d "${payload}" > /dev/null || return 1
  bump_counter api_issue_comments
}

op_api_issue_close() {
  local _worker_dir="$1" username="$2" token="$3"
  local issue_file; issue_file=$(pick_issue_file) || return 0
  local issue_number issue_user issue_url related_pr_url
  IFS='|' read -r issue_number issue_user issue_url related_pr_url < "${issue_file}"
  local payload='{"state":"closed"}'
  local response
  response=$(curl ${CURL_FLAGS} -X PATCH \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/issues/${issue_number}" \
    -d "${payload}") || return 1
  if [[ "$(printf "%s" "${response}" | jq -r '.state // empty')" != "closed" ]]; then
    return 1
  fi
  : > "${issue_file}.closed"
  bump_counter api_issue_close_actual
}

op_api_pr() {
  local _worker_dir="$1" username="$2" token="$3"
  local ts; ts=$(date +%s)
  local head; head=$(pick_pr_branch)
  local base="${base_branch:-main}"
  local payload; payload=$(jq -cn \
    --arg title "Sim PR by ${username} (${ts})" \
    --arg body "Automated PR created by the traffic simulator as user ${username}." \
    --arg head "${head}" \
    --arg base "${base}" \
    '{"title":$title,"body":$body,"head":$head,"base":$base}')
  local response
  response=$(curl ${CURL_FLAGS} -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/pulls" \
    -d "${payload}") || return 1

  local pr_number; pr_number=$(printf "%s" "${response}" | jq -r '.number // empty')
  [[ -n "${pr_number}" ]] || return 1
  local pr_url; pr_url=$(printf "%s" "${response}" | jq -r '.html_url // empty')
  printf "%s|%s|%s|%s\n" "${username}" "${pr_url}" "${pr_number}" "${head}" \
    > "${PR_DIR}/${pr_number}.pr"

  for branch_file in "${BRANCH_DIR}"/*.branch; do
    [[ -f "${branch_file}" ]] || continue
    if [[ "$(cat "${branch_file}")" == "${head}" ]]; then
      : > "${branch_file}.pr"
      break
    fi
  done

  local activity_count=$(( 2 + RANDOM % 4 ))
  local activity_index
  for (( activity_index=1; activity_index<=activity_count; activity_index++ )); do
    local contrib_username="${username}"
    local contrib_token="${token}"
    if (( SIM_USER_COUNT > 1 )); then
      local contrib_idx=$(( RANDOM % SIM_USER_COUNT ))
      contrib_username="${SIM_USERNAMES[$contrib_idx]}"
      contrib_token="${SIM_TOKENS[$contrib_idx]}"
    fi
    local safe_contrib="${contrib_username//[^a-zA-Z0-9_-]/-}"
    local activity_path="sim-pr-activity/${safe_contrib}/${pr_number}-${ts}-${RANDOM}-${activity_index}.txt"
    local activity_content
    activity_content=$(build_diff_heavy_content "pr_activity" "${contrib_username}" "${ts}" "${activity_index}" "pr-${pr_number}")
    api_put_file_commit "${contrib_username}" "${contrib_token}" "${activity_path}" \
      "sim: PR ${pr_number} follow-up ${activity_index} by ${contrib_username}" \
      "${activity_content}" "${head}" || return 1
    bump_counter pr_activity_commits
    if [[ "${contrib_username}" != "${username}" ]]; then
      bump_counter pr_cross_user_commits
    fi
  done
}

claim_merge_slot() {
  local now="$1"
  local lock_dir="${MERGE_STATE_FILE}.lockdir"
  (
    while ! mkdir "${lock_dir}" 2>/dev/null; do
      sleep 0.01
    done
    trap 'rmdir "${lock_dir}" 2>/dev/null || true' EXIT
    local last=0
    [[ -f "${MERGE_STATE_FILE}" ]] && last=$(cat "${MERGE_STATE_FILE}")
    if (( now - last < MERGE_INTERVAL )); then
      exit 1
    fi
    echo "${now}" > "${MERGE_STATE_FILE}"
  )
}

op_api_merge() {
  local _worker_dir="$1" username="$2" token="$3"
  local response
  response=$(curl ${CURL_FLAGS} \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/pulls?state=open&per_page=100") || return 1

  local numbers=()
  while IFS= read -r number; do
    [[ -n "${number}" ]] && numbers+=("${number}")
  done < <(printf "%s" "${response}" | jq -r '.[].number // empty')

  if (( ${#numbers[@]} == 0 )); then
    return 0
  fi
  claim_merge_slot "$(date +%s)" || return 0

  local selected="${numbers[$(( RANDOM % ${#numbers[@]} ))]}"
  local merge_response
  merge_response=$(curl ${CURL_FLAGS} -X PUT \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/pulls/${selected}/merge" \
    -d '{"merge_method":"merge"}') || return 1
  if [[ "$(printf "%s" "${merge_response}" | jq -r '.merged // false')" == "true" ]]; then
    bump_counter api_merge_actual
    return 0
  fi
  return 1
}

op_api_fork_pr_merge() {
  local worker_dir="$1" username="$2" token="$3"
  local safe_user="${username//[^a-zA-Z0-9_-]/-}"
  local worker_name="${worker_dir##*/}"
  local base="${base_branch:-main}"
  local ts; ts=$(timestamp_ms)

  ensure_user_fork_ready "${username}" "${token}" || return 1

  local fork_dir="${worker_dir}/fork-repo-${safe_user}"
  local fork_url; fork_url=$(git_clone_url_for_repo "${token}" "${username}" "${repo}")
  if [[ ! -d "${fork_dir}/.git" ]]; then
    local attempt cloned=false
    for (( attempt=1; attempt<=5; attempt++ )); do
      if GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
        git clone --quiet "${fork_url}" "${fork_dir}" 2>&1; then
        cloned=true
        break
      fi
      rm -rf "${fork_dir}"
      sleep 2
    done
    [[ "${cloned}" == true ]] || return 1
  else
    git -C "${fork_dir}" remote set-url origin "${fork_url}" 2>/dev/null || true
    GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
      git -C "${fork_dir}" fetch --quiet origin "${base}" 2>&1 || return 1
  fi

  git -C "${fork_dir}" config user.email "${username}@example.com"
  git -C "${fork_dir}" config user.name "${username}"

  local branch="fork/${safe_user}/${worker_name}/${ts}-${RANDOM}"
  git -C "${fork_dir}" checkout --quiet -B "${branch}" "origin/${base}" || return 1

  local file_count; file_count=$(random_file_batch_size)
  local file_index
  local file_dir="${fork_dir}/sim-fork/${safe_user}"
  mkdir -p "${file_dir}"
  for (( file_index=1; file_index<=file_count; file_index++ )); do
    local file_path="${file_dir}/fork-${ts}-${RANDOM}-${file_index}.txt"
    build_diff_heavy_content "fork_push" "${username}" "${ts}" "${file_index}" "${branch}" > "${file_path}"
    git -C "${fork_dir}" add "${file_path}"
  done
  git -C "${fork_dir}" commit --quiet -m "sim fork push by ${username} at ${ts}" || return 1
  GIT_HTTP_USER_AGENT="the-power-simulator/${username}" \
    git -C "${fork_dir}" push --quiet origin "${branch}:${branch}" 2>&1 || return 1
  bump_counter_by fork_push_files "${file_count}"

  local pr_payload; pr_payload=$(jq -cn \
    --arg title "Sim fork PR by ${username} (${ts})" \
    --arg body "Automated PR from ${username}'s fork branch ${branch}." \
    --arg head "${username}:${branch}" \
    --arg base "${base}" \
    '{"title":$title,"body":$body,"head":$head,"base":$base}')
  local pr_response
  pr_response=$(curl ${CURL_FLAGS} -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/pulls" \
    -d "${pr_payload}") || return 1

  local pr_number; pr_number=$(printf "%s" "${pr_response}" | jq -r '.number // empty')
  [[ -n "${pr_number}" ]] || return 1
  local pr_url; pr_url=$(printf "%s" "${pr_response}" | jq -r '.html_url // empty')
  printf "%s|%s|%s|%s\n" "${username}" "${pr_url}" "${pr_number}" "${branch}" > "${PR_DIR}/${pr_number}.pr"
  bump_counter fork_pr_created_actual

  local merge_response
  merge_response=$(curl ${CURL_FLAGS} -X PUT \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/pulls/${pr_number}/merge" \
    -d '{"merge_method":"merge"}') || return 1
  if [[ "$(printf "%s" "${merge_response}" | jq -r '.merged // false')" == "true" ]]; then
    bump_counter fork_pr_merged_actual
    return 0
  fi
  return 1
}

op_workflow_dispatch() {
  local _worker_dir="$1" username="$2" token="$3"
  # Discover the latest workflow id and dispatch it
  local wf_id; wf_id=$(curl ${CURL_FLAGS} \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/actions/workflows?per_page=5" \
    | jq '[.workflows[].id] | max // empty' 2>/dev/null || echo "")
  [[ -z "${wf_id}" || "${wf_id}" == "null" ]] && return 0  # no workflows, skip silently

  local ref="${new_branch:-feature-branch}"
  local payload; payload=$(jq -cn --arg ref "${ref}" '{"ref":$ref}')
  curl ${CURL_FLAGS} -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${token}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/actions/workflows/${wf_id}/dispatches" \
    -d "${payload}" > /dev/null
}

op_api_refs_read() {
  local _worker_dir="$1" username="$2" token="$3"
  local auth="Authorization: token ${token}"
  local accept="Accept: application/vnd.github+json"
  local apiver="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"
  local refs_base="${GITHUB_API_BASE_URL}/repos/${org}/${repo}/git"

  # Every variant lands on /repositories/:repository_id/git/refs/* internally.
  # Reading a heavily tagged repo makes the ref store and pagination work.
  local variant=$(( RANDOM % 6 ))
  case $variant in
    0)
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${refs_base}/matching-refs/tags/?per_page=100" > /dev/null
      ;;
    1)
      # Deep pagination across the tag namespace
      local page=$(( 1 + RANDOM % 20 ))
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${refs_base}/matching-refs/tags/?per_page=100&page=${page}" > /dev/null
      ;;
    2)
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${refs_base}/matching-refs/heads/?per_page=100" > /dev/null
      ;;
    3)
      # Prefix match against a nested tag namespace
      local prefixes=(release/2026 build/ci- sim/nightly v1.)
      local prefix="${prefixes[$(( RANDOM % ${#prefixes[@]} ))]}"
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${refs_base}/matching-refs/tags/${prefix}?per_page=100" > /dev/null
      ;;
    4)
      # Single ref lookup for a tag that may or may not exist
      local status
      status=$(curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${refs_base}/ref/tags/build/ci-$(printf '%06d' $(( 1 + RANDOM % 2000 )))" \
        -o /dev/null -w "%{http_code}") || true
      [[ "${status}" == "200" || "${status}" == "404" ]]
      ;;
    5)
      curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/tags?per_page=100" > /dev/null
      ;;
  esac
}

op_api_tag_ref() {
  local _worker_dir="$1" username="$2" token="$3"
  local auth="Authorization: token ${token}"
  local accept="Accept: application/vnd.github+json"
  local apiver="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"
  local refs_base="${GITHUB_API_BASE_URL}/repos/${org}/${repo}/git"
  local safe_user="${username//[^a-zA-Z0-9_-]/-}"
  local ts; ts=$(timestamp_ms)

  # Target a random commit so tags do not all point at the same object.
  local sha
  sha=$(curl ${CURL_FLAGS} -H "${accept}" -H "${apiver}" -H "${auth}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/commits?per_page=30" \
    | jq -r '.[].sha' 2>/dev/null \
    | awk -v seed="${RANDOM}" 'BEGIN{srand(seed)} {a[NR]=$0} END{if (NR>0) print a[int(rand()*NR)+1]}')
  [[ -z "${sha}" || "${sha}" == "null" ]] && return 1

  local created=0
  local tag_count=$(( 3 + RANDOM % 8 ))
  local created_tags=()
  local i
  for (( i=1; i<=tag_count; i++ )); do
    local tag_name="sim/${safe_user}/${ts}-${RANDOM}-${i}"
    local payload; payload=$(jq -cn --arg ref "refs/tags/${tag_name}" --arg sha "${sha}" \
      '{"ref":$ref,"sha":$sha}')
    if curl ${CURL_FLAGS} -X POST -H "${accept}" -H "${apiver}" -H "${auth}" \
      "${refs_base}/refs" -d "${payload}" > /dev/null; then
      created=$(( created + 1 ))
      created_tags+=("${tag_name}")
    fi
  done
  bump_counter_by api_tags_created "${created}"

  # Delete roughly half of them so the ref store churns rather than only grows.
  local tag
  if (( created > 0 )); then
    for tag in "${created_tags[@]}"; do
      (( RANDOM % 2 == 0 )) || continue
      curl ${CURL_FLAGS} -X DELETE -H "${accept}" -H "${apiver}" -H "${auth}" \
        "${refs_base}/refs/tags/${tag}" > /dev/null || true
      bump_counter api_tags_deleted
    done
  else
    return 1
  fi
}

# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------
worker() {
  local worker_id="$1"
  local worker_dir="${TMPDIR_ROOT}/worker-${worker_id}"
  mkdir -p "${worker_dir}"

  log DEBUG "Worker ${worker_id} started (PID $$)"

  while (( $(date +%s) < END_TS )); do
    # Pick operation
    local idx=$(( RANDOM % WEIGHTED_OPS_LEN ))
    local op="${WEIGHTED_OPS[$idx]}"

    # Pick a random sim user (or admin fallback)
    pick_user
    local username="${CURRENT_USER}"
    local token="${CURRENT_TOKEN}"

    log DEBUG "Worker ${worker_id} [${username}]: ${op}"

    local t_start; t_start=$(timestamp_ms)
    local rc=0

    case "${op}" in
      git_clone)         op_git_clone        "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      git_fetch)         op_git_fetch        "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      git_push)          op_git_push         "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      git_branch)        op_git_branch       "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_read)          op_api_read         "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_refs_read)     op_api_refs_read    "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_tag_ref)       op_api_tag_ref      "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_commit)        op_api_commit       "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_issue)         op_api_issue        "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_issue_comment) op_api_issue_comment "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_issue_close)   op_api_issue_close   "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_pr)            op_api_pr           "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_merge)         op_api_merge        "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      api_fork_pr_merge) op_api_fork_pr_merge "${worker_dir}" "${username}" "${token}" || rc=$? ;;
      workflow_dispatch) op_workflow_dispatch "${worker_dir}" "${username}" "${token}" || rc=$? ;;
    esac

    local t_end; t_end=$(timestamp_ms)
    local elapsed=$(( t_end - t_start ))
    perf_record_operation "${op}" "${elapsed}"

    if (( rc == 0 )); then
      bump_counter "${op}"
      bump_counter "_total_ok"
      user_key="${username//[^a-zA-Z0-9_-]/-}"
      bump_counter "_user_${user_key}_ok"
      log DEBUG "Worker ${worker_id} [${username}]: ${op} OK (${elapsed}ms)"
    else
      bump_counter "_total_err"
      bump_counter "_err_${op}"
      user_key="${username//[^a-zA-Z0-9_-]/-}"
      bump_counter "_user_${user_key}_err"
      log INFO "Worker ${worker_id} [${username}]: ${op} FAILED rc=${rc} (${elapsed}ms)"
    fi

    # Random jitter to avoid thundering herd
    local sleep_for=$(( RANDOM % (JITTER + 1) ))
    sleep "${sleep_for}"
  done

  log DEBUG "Worker ${worker_id} done"
  rm -rf "${worker_dir}"
}

# ---------------------------------------------------------------------------
# Progress reporter (background, fires every 60 s)
# ---------------------------------------------------------------------------
reporter() {
  while (( $(date +%s) < END_TS )); do
    sleep 60
    (( $(date +%s) >= END_TS )) && break

    local elapsed=$(( $(date +%s) - START_TS ))
    local remaining=$(( END_TS - $(date +%s) ))
    local total_ok; total_ok=$(read_counter _total_ok)
    local total_err; total_err=$(read_counter _total_err)
    local ops_str=""
    for op in "${OP_NAMES[@]}"; do
      local n; n=$(read_counter "${op}")
      (( n > 0 )) && ops_str+="${op}:${n} "
    done

    echo ""
    echo "=== [$(date '+%H:%M:%S')] ${elapsed}s elapsed, ~${remaining}s remaining ==="
    echo "    OK=${total_ok}  ERR=${total_err}  ${ops_str:-no ops yet}"
    echo ""
  done
}

# ---------------------------------------------------------------------------
# Cleanup / final report (EXIT trap)
# ---------------------------------------------------------------------------
cleanup() {
  echo ""
  echo "=== Simulator stopping, waiting for workers... ==="
  wait 2>/dev/null || true

  local total_ok; total_ok=$(read_counter _total_ok)
  local total_err; total_err=$(read_counter _total_err)
  local actual_duration=$(( $(date +%s) - START_TS ))

  echo ""
  echo "=========================================="
  echo "  GHES Traffic Simulator - Final Report"
  echo "=========================================="
  printf "  Duration:       %ds\n"  "${actual_duration}"
  printf "  Workers:        %d\n"   "${PARALLELISM}"
  printf "  Sim users:      %d\n"   "${SIM_USER_COUNT}"
  printf "  Operations OK:  %d\n"   "${total_ok}"
  printf "  Operations ERR: %d\n"   "${total_err}"
  echo "  Breakdown:"
  for op in "${OP_NAMES[@]}"; do
    local n; n=$(read_counter "${op}")
    printf "    %-22s %d\n" "${op}" "${n}"
  done
  echo "  Errors by operation:"
  for op in "${OP_NAMES[@]}"; do
    local n; n=$(read_counter "_err_${op}")
    (( n > 0 )) && printf "    %-22s %d\n" "${op}" "${n}"
  done
  echo "  Dynamic state changes:"
  printf "    %-22s %d\n" "branches_created" "$(read_counter git_branch)"
  printf "    %-22s %d\n" "git_push_commits" "$(read_counter git_push)"
  printf "    %-22s %d\n" "git_push_files" "$(read_counter git_push_files)"
  printf "    %-22s %d\n" "git_branch_files" "$(read_counter git_branch_files)"
  printf "    %-22s %d\n" "api_commits" "$(read_counter api_commit)"
  printf "    %-22s %d\n" "tag_refs_created" "$(read_counter api_tags_created)"
  printf "    %-22s %d\n" "tag_refs_deleted" "$(read_counter api_tags_deleted)"
  printf "    %-22s %d\n" "api_commit_files" "$(read_counter api_commit_files)"
  printf "    %-22s %d\n" "pr_activity_commits" "$(read_counter pr_activity_commits)"
  printf "    %-22s %d\n" "pr_cross_user_commits" "$(read_counter pr_cross_user_commits)"
  printf "    %-22s %d\n" "pull_requests_created" "$(read_counter api_pr)"
  printf "    %-22s %d\n" "pull_requests_merged" "$(read_counter api_merge_actual)"
  printf "    %-22s %d\n" "fork_push_files" "$(read_counter fork_push_files)"
  printf "    %-22s %d\n" "fork_prs_created" "$(read_counter fork_pr_created_actual)"
  printf "    %-22s %d\n" "fork_prs_merged" "$(read_counter fork_pr_merged_actual)"
  printf "    %-22s %d\n" "issue_comments" "$(read_counter api_issue_comments)"
  printf "    %-22s %d\n" "issues_closed" "$(read_counter api_issue_close_actual)"
  echo "  Performance by operation (ms):"
  for op in "${OP_NAMES[@]}"; do
    local perf_count; perf_count=$(read_counter "_perf_${op}_count")
    (( perf_count > 0 )) || continue
    local perf_sum; perf_sum=$(read_counter "_perf_${op}_sum_ms")
    local perf_min; perf_min=$(read_counter "_perf_${op}_min_ms")
    local perf_max; perf_max=$(read_counter "_perf_${op}_max_ms")
    local perf_avg=$(( perf_sum / perf_count ))

    local early_count; early_count=$(read_counter "_perf_${op}_early_count")
    local early_sum; early_sum=$(read_counter "_perf_${op}_early_sum_ms")
    local late_count; late_count=$(read_counter "_perf_${op}_late_count")
    local late_sum; late_sum=$(read_counter "_perf_${op}_late_sum_ms")

    local early_avg_num=0 late_avg_num=0
    local early_avg_label="-" late_avg_label="-"
    if (( early_count > 0 )); then
      early_avg_num=$(( early_sum / early_count ))
      early_avg_label="${early_avg_num}"
    fi
    if (( late_count > 0 )); then
      late_avg_num=$(( late_sum / late_count ))
      late_avg_label="${late_avg_num}"
    fi

    local trend="n/a"
    if (( early_count > 0 && late_count > 0 && early_avg_num > 0 )); then
      local delta_pct=$(( ((late_avg_num - early_avg_num) * 100) / early_avg_num ))
      if (( delta_pct > 0 )); then
        trend="+${delta_pct}%"
      else
        trend="${delta_pct}%"
      fi
    fi

    printf "    %-18s n=%-5d avg=%-6d min=%-6d max=%-6d early=%-6s late=%-6s trend=%s\n" \
      "${op}" "${perf_count}" "${perf_avg}" "${perf_min}" "${perf_max}" \
      "${early_avg_label}" "${late_avg_label}" "${trend}"
  done
  if (( SIM_USER_COUNT > 0 )); then
    echo "  Per-user results:"
    for username in "${SIM_USERNAMES[@]}"; do
      local user_key="${username//[^a-zA-Z0-9_-]/-}"
      local user_ok; user_ok=$(read_counter "_user_${user_key}_ok")
      local user_err; user_err=$(read_counter "_user_${user_key}_err")
      printf "    %-22s OK=%d ERR=%d\n" "${username}" "${user_ok}" "${user_err}"
    done
  fi
  echo ""
  echo "  Full log: ${LOGFILE}"
  echo "=========================================="
  rm -rf "${COUNTER_DIR}"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=========================================="
echo "  GHES Traffic Simulator"
echo "=========================================="
echo "  Host:        ${hostname}"
echo "  Org/Repo:    ${org}/${repo}"
echo "  Sim users:   ${SIM_USER_COUNT} ($([ $SIM_USER_COUNT -gt 0 ] && echo "rotating identities" || echo "admin token only"))"
echo "  Duration:    ${DURATION}s"
echo "  Max jitter:  ${JITTER}s"
echo "  Traffic mix: ${TRAFFIC_WEIGHTS:-default}"
echo "  Disabled:    ${DISABLED_OPS:-none}"
echo "  Verbose:     ${VERBOSE}"
echo "  Log:         ${LOGFILE}"
echo "=========================================="
echo ""
echo "Starting ${PARALLELISM} workers..."
echo ""

WORKER_PIDS=()
for (( i=1; i<=PARALLELISM; i++ )); do
  worker "${i}" &
  WORKER_PIDS+=($!)
  # Stagger starts to avoid a thundering herd of clones at second 0
  (( i < PARALLELISM )) && sleep 2
done

reporter &
REPORTER_PID=$!

for pid in "${WORKER_PIDS[@]}"; do
  wait "${pid}" 2>/dev/null || true
done

kill "${REPORTER_PID}" 2>/dev/null || true
wait "${REPORTER_PID}" 2>/dev/null || true
# EXIT trap runs cleanup
