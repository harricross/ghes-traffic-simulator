#!/usr/bin/env bash
# prepare-appliance-for-traffic-sim.sh
#
# Provisions a blank GHES appliance so simulate-day-of-traffic.sh can run
# multi-user, randomised load.  Safe to run more than once; each step is
# guarded so existing resources are skipped rather than duplicated.
#
# What it does:
#   1. Creates N simulator users (default 16) via the GHES admin API
#   2. Creates the org if it does not already exist
#   3. Creates the repo and seeds it with a README + a feature branch,
#      giving all the git clone/fetch/push ops something meaningful to hit
#   4. Adds every sim user to the org as a member
#   5. Adds every sim user as a push collaborator on the repo
#   6. Mints an impersonation OAuth token for each sim user
#   7. Writes tmp/sim-users.json  <-- consumed by simulate-day-of-traffic.sh
#
# Usage:
#   ./prepare-appliance-for-traffic-sim.sh [-n <num_users>] [-p <user_prefix>]
#                                          [-r <repo_name>] [-v]
#
#   -n N   Number of simulator users to create (default: 16, max: 50)
#   -p S   Username prefix (default: "sim-user")
#   -r S   Target repository (default: repo from .gh-api-examples.conf)
#   -v     Verbose curl output (default: silent)
#
# After this script completes, run:
#   ./simulate-day-of-traffic.sh -u tmp/sim-users.json -p 12 -d 3600

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
NUM_USERS=16
USER_PREFIX="sim-user"
REPO_OVERRIDE=""
VERBOSE=false
CONF="./.gh-api-examples.conf"
MANIFEST="tmp/sim-users.json"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while getopts "n:p:r:v" opt; do
  case $opt in
    n) NUM_USERS=$OPTARG ;;
    p) USER_PREFIX=$OPTARG ;;
    r) REPO_OVERRIDE=$OPTARG ;;
    v) VERBOSE=true ;;
    *) echo "Usage: $0 [-n num_users] [-p user_prefix] [-r repo_name] [-v]" >&2; exit 1 ;;
  esac
done

if (( NUM_USERS < 1 || NUM_USERS > 50 )); then
  echo "ERROR: -n must be between 1 and 50" >&2; exit 1
fi

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------
if [[ ! -f "$CONF" ]]; then
  echo "ERROR: $CONF not found.  Populate it before running this script." >&2
  exit 1
fi
# shellcheck source=/dev/null
. "$CONF"

: "${hostname:?hostname not set in $CONF}"
: "${org:?org not set in $CONF}"
: "${repo:?repo not set in $CONF}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN not set in $CONF}"
: "${GITHUB_API_BASE_URL:?GITHUB_API_BASE_URL not set in $CONF}"

if [[ -n "${REPO_OVERRIDE}" ]]; then
  repo="${REPO_OVERRIDE}"
fi

mkdir -p tmp

# Determine git hostname (strip /api/v3 path context - hostname var is host only)
GIT_HOSTNAME="${hostname}"
[[ "${GIT_HOSTNAME}" == "api.github.com" ]] && GIT_HOSTNAME="github.com"

# curl flags: preserve the config's failure behaviour, then add output flags.
# Do not combine -f with --fail-with-body; curl rejects that combination.
CURL_FLAGS="${curl_custom_flags:-}"
if [[ "${VERBOSE}" == true ]]; then
  CURL_FLAGS="${CURL_FLAGS} -v"
else
  CURL_FLAGS="${CURL_FLAGS} --silent --show-error"
fi

# Admin auth header
ADMIN_AUTH="Authorization: token ${GITHUB_TOKEN}"
ACCEPT="Accept: application/vnd.github+json"
API_VER="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"

timestamp_ms() {
  python3 -c 'import time; print(time.time_ns() // 1000000)'
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
step() { echo ""; echo ">>> $*"; }
ok()   { echo "    OK: $*"; }
skip() { echo "    SKIP: $*"; }
warn() { echo "    WARN: $*"; }

http_status() {
  curl ${CURL_FLAGS} -o /dev/null -w "%{http_code}" \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" "$1" || echo "000"
}

# ---------------------------------------------------------------------------
# Step 1: Create simulator users
# ---------------------------------------------------------------------------
step "Creating ${NUM_USERS} simulator users (prefix: ${USER_PREFIX})"

CREATED_USERS=()
for (( i=1; i<=NUM_USERS; i++ )); do
  username=$(printf "%s-%03d" "${USER_PREFIX}" "${i}")
  email="${username}@example.com"

  # Check if user already exists
  status=$(http_status "${GITHUB_API_BASE_URL}/users/${username}")
  if [[ "${status}" == "200" ]]; then
    skip "${username} already exists"
  else
    payload=$(jq -cn --arg l "${username}" --arg e "${email}" \
      '{"login":$l,"email":$e}')
    result=$(curl ${CURL_FLAGS} -X POST \
      -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
      -H "Content-Type: application/json" \
      "${GITHUB_API_BASE_URL}/admin/users" \
      -d "${payload}" 2>&1) || true
    if echo "${result}" | jq -e '.login' > /dev/null 2>&1; then
      ok "Created ${username}"
    else
      warn "Failed to create ${username}: ${result}"
    fi
  fi
  CREATED_USERS+=("${username}")
done

# ---------------------------------------------------------------------------
# Step 2: Ensure the organisation exists
# ---------------------------------------------------------------------------
step "Ensuring organisation '${org}' exists"

org_status=$(http_status "${GITHUB_API_BASE_URL}/orgs/${org}")
if [[ "${org_status}" == "200" ]]; then
  skip "Org '${org}' already exists"
else
  admin_user="${admin_user:-ghe-admin}"
  payload=$(jq -cn \
    --arg nm "${org}" \
    --arg pn "${org}: Traffic Simulator Org" \
    --arg ad "${admin_user}" \
    '{"login":$nm,"profile_name":$pn,"admin":$ad}')
  result=$(curl ${CURL_FLAGS} -X POST \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
    "${GITHUB_API_BASE_URL}/admin/organizations" \
    -d "${payload}" 2>&1) || true
  if echo "${result}" | jq -e '.url' > /dev/null 2>&1; then
    ok "Created org '${org}'"
  else
    warn "Failed to create org (may already exist): ${result}"
  fi
fi

# ---------------------------------------------------------------------------
# Step 3: Ensure the repo exists and has content
# ---------------------------------------------------------------------------
step "Ensuring repo '${org}/${repo}' exists and is seeded"

repo_status=$(http_status "${GITHUB_API_BASE_URL}/repos/${org}/${repo}")
if [[ "${repo_status}" == "200" ]]; then
  skip "Repo '${org}/${repo}' already exists"
else
  payload=$(jq -cn \
    --arg nm "${repo}" \
    --arg desc "Traffic simulator target repo" \
    '{"name":$nm,"description":$desc,"private":false,"auto_init":true}')
  result=$(curl ${CURL_FLAGS} -X POST \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
    "${GITHUB_API_BASE_URL}/orgs/${org}/repos" \
    -d "${payload}" 2>&1) || true
  if echo "${result}" | jq -e '.html_url' > /dev/null 2>&1; then
    ok "Created repo '${org}/${repo}'"
    sleep 2  # give the auto-init commit a moment to land
  else
    warn "Failed to create repo: ${result}"
  fi
fi

# Seed a feature branch so fetch/clone ops have something to traverse
step "Seeding feature branch '${new_branch:-feature-branch}' in repo"
FEATURE_BRANCH="${new_branch:-feature-branch}"
BASE_BRANCH="${base_branch:-main}"

branch_status=$(http_status \
  "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/branches/${FEATURE_BRANCH}")
if [[ "${branch_status}" == "200" ]]; then
  skip "Branch '${FEATURE_BRANCH}' already exists"
else
  # Get the SHA of the default branch HEAD
  base_sha=$(curl ${CURL_FLAGS} \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/git/refs/heads/${BASE_BRANCH}" \
    | jq -r '.object.sha' 2>/dev/null || echo "")

  if [[ -z "${base_sha}" || "${base_sha}" == "null" ]]; then
    warn "Could not get SHA for '${BASE_BRANCH}'; skipping branch creation"
  else
    payload=$(jq -cn \
      --arg ref "refs/heads/${FEATURE_BRANCH}" \
      --arg sha "${base_sha}" \
      '{"ref":$ref,"sha":$sha}')
    result=$(curl ${CURL_FLAGS} -X POST \
      -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
      "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/git/refs" \
      -d "${payload}" 2>&1) || true
    if echo "${result}" | jq -e '.ref' > /dev/null 2>&1; then
      ok "Created branch '${FEATURE_BRANCH}'"
    else
      warn "Branch creation may have failed: ${result}"
    fi
  fi
fi

# Add a few commits to make the repo non-trivial to clone
step "Adding seed commits so the repo has real objects to clone"
SEED_COUNT=5
for (( s=1; s<=SEED_COUNT; s++ )); do
  ts=$(timestamp_ms)
  content=$(echo "seed content ${s} timestamp ${ts}" | python3 base64encode.py)
  # Get current SHA of the file if it exists
  existing_sha=$(curl ${CURL_FLAGS} \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/contents/seed/file-${s}.txt" \
    | jq -r '.sha // empty' 2>/dev/null || echo "")

  if [[ -n "${existing_sha}" ]]; then
    payload=$(jq -cn \
      --arg msg "Seed commit ${s}" \
      --arg content "${content}" \
      --arg sha "${existing_sha}" \
      '{"message":$msg,"content":$content,"sha":$sha}')
  else
    payload=$(jq -cn \
      --arg msg "Seed commit ${s}" \
      --arg content "${content}" \
      '{"message":$msg,"content":$content}')
  fi

  curl ${CURL_FLAGS} -X PUT \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/contents/seed/file-${s}.txt" \
    -d "${payload}" > /dev/null 2>&1 || warn "Seed commit ${s} failed (may already exist)"
done
ok "Seed commits done"

# ---------------------------------------------------------------------------
# Step 4: Add sim users to the org
# ---------------------------------------------------------------------------
step "Adding sim users to org '${org}'"

for username in "${CREATED_USERS[@]}"; do
  result=$(curl ${CURL_FLAGS} -X PUT \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
    "${GITHUB_API_BASE_URL}/orgs/${org}/memberships/${username}" \
    -d '{"role":"member"}' 2>&1) || true
  state=$(echo "${result}" | jq -r '.state // empty' 2>/dev/null || echo "")
  if [[ -n "${state}" ]]; then
    ok "${username} -> org member (state: ${state})"
  else
    warn "${username} org membership may have failed"
  fi
done

# ---------------------------------------------------------------------------
# Step 5: Add sim users as push collaborators on the repo
# ---------------------------------------------------------------------------
step "Adding sim users as push collaborators on '${org}/${repo}'"

COLLAB_FAILURE_COUNT=0
for username in "${CREATED_USERS[@]}"; do
  status=$(curl ${CURL_FLAGS} -X PUT \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
    "${GITHUB_API_BASE_URL}/repos/${org}/${repo}/collaborators/${username}" \
    -d '{"permission":"push"}' -o /dev/null -w "%{http_code}") || true
  if [[ "${status}" == "201" || "${status}" == "204" ]]; then
    ok "${username} -> push collaborator"
  else
    warn "Failed to add ${username} as push collaborator (HTTP ${status})"
    COLLAB_FAILURE_COUNT=$(( COLLAB_FAILURE_COUNT + 1 ))
  fi
done

# ---------------------------------------------------------------------------
# Step 6: Mint impersonation OAuth tokens for each sim user
# ---------------------------------------------------------------------------
step "Minting impersonation tokens for ${NUM_USERS} sim users"

# Build JSON array as we go
JSON_ENTRIES=()
FAILED_USERS=()

for username in "${CREATED_USERS[@]}"; do
  token_result=$(curl ${CURL_FLAGS} -X POST \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
    "${GITHUB_API_BASE_URL}/admin/users/${username}/authorizations" \
    -d '{"scopes":["repo","read:org"]}' 2>&1) || true

  token=$(echo "${token_result}" | jq -r '.token // empty' 2>/dev/null || echo "")

  if [[ -n "${token}" ]]; then
    entry=$(jq -cn \
      --arg u "${username}" \
      --arg t "${token}" \
      '{"username":$u,"token":$t}')
    JSON_ENTRIES+=("${entry}")
    ok "Token minted for ${username} (${token:0:4}...${token: -6})"
  else
    FAILED_USERS+=("${username}")
    warn "Failed to mint token for ${username}: ${token_result}"
  fi
done

# ---------------------------------------------------------------------------
# Step 7: Write the users manifest
# ---------------------------------------------------------------------------
step "Writing user manifest to ${MANIFEST}"

# Join array entries with commas and wrap in []
if (( ${#JSON_ENTRIES[@]} == 0 )); then
  printf '[]\n' > "${MANIFEST}"
else
  printf '%s\n' "${JSON_ENTRIES[@]}" | jq -s '.' > "${MANIFEST}"
fi

manifest_count=$(jq 'length' "${MANIFEST}")
ok "Wrote ${manifest_count} users to ${MANIFEST}"

if (( ${#FAILED_USERS[@]} > 0 )); then
  warn "Token minting failed for: ${FAILED_USERS[*]}"
fi
if (( COLLAB_FAILURE_COUNT > 0 )); then
  warn "Failed to grant push access to ${COLLAB_FAILURE_COUNT} simulator user(s) on ${org}/${repo}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  Appliance Prep Complete"
echo "=========================================="
printf "  Host:          %s\n" "${hostname}"
printf "  Org:           %s\n" "${org}"
printf "  Repo:          %s/%s\n" "${org}" "${repo}"
printf "  Sim users:     %d (prefix: %s)\n" "${manifest_count}" "${USER_PREFIX}"
printf "  Manifest:      %s\n" "${MANIFEST}"
echo ""
echo "  Run the traffic simulator:"
echo ""
echo "    ./simulate-day-of-traffic.sh -u ${MANIFEST} -p 12 -d 3600"
echo ""
echo "  Flags:"
echo "    -p  parallel workers (8-16 recommended)"
echo "    -d  duration in seconds (3600 = 1 hour)"
echo "    -j  max jitter seconds between ops (default 10)"
echo "=========================================="

if (( COLLAB_FAILURE_COUNT > 0 )); then
  exit 1
fi
