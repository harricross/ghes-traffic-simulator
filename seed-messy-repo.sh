#!/usr/bin/env bash
# seed-messy-repo.sh
#
# Builds a locally-assembled git repository with randomized history that
# forces Git processes to work hard, then pushes it to the GHES target.
#
# What makes it messy:
#   - Deep history: commit content is randomised per commit from a weighted
#     operation set (text-only, binary-only, both, wide-tree batch, large
#     blob, rename/delete, new-file burst) so no two runs produce the same
#     packfile structure
#   - Non-deltifiable binary blobs: pseudo-random bytes with randomly varied
#     sizes (0.5x-2x BINARY_KB), forcing the packfile to store near-full copies
#   - Mutating large text file: a growing war-and-peace excerpt that delta-
#     compresses well -- intentional contrast against the binary blobs
#   - Wide diverged branches (default 25): fork points chosen randomly across
#     the full history, not evenly spaced, creating an irregular DAG
#   - Some branches merged back into main: produces merge commits and a non-
#     linear graph that is expensive for upload-pack to traverse
#   - Commit authors rotate through a pool of synthetic names so the author
#     graph varies (audit log realism, author attribution overhead)
#   - File renames, deletions, and re-adds: similarity detection on log/diff
#   - Pushes all branches so GHES spokes must replicate the full graph
#
# Usage:
#   ./seed-messy-repo.sh [-r <messy_repo_name>] [-c <commits>] [-b <branches>]
#                        [-k <binary_kb>] [-v]
#
#   -r NAME  Name of the repo to create on GHES (default: "messy-repo")
#   -c N     Commits on main branch (default: 200)
#   -b N     Diverged branches to create (default: 25)
#   -k N     Base binary blob size in KB -- actual sizes vary 0.5x-2x (default: 256)
#   -v       Verbose git output
#
# Requires: git, openssl
#
# After completion run the traffic simulator against this repo:
#   ./simulate-day-of-traffic.sh -r messy-repo -u tmp/sim-users.json -p 8 -d 3600

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
MESSY_REPO="messy-repo"
LINEAR_COMMITS=200
BRANCHES=25
COMMITS_PER_BRANCH=20
BINARY_KB=256
VERBOSE=false
CONF="./.gh-api-examples.conf"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while getopts "r:c:b:k:v" opt; do
  case $opt in
    r) MESSY_REPO=$OPTARG ;;
    c) LINEAR_COMMITS=$OPTARG ;;
    b) BRANCHES=$OPTARG ;;
    k) BINARY_KB=$OPTARG ;;
    v) VERBOSE=true ;;
    *)
      echo "Usage: $0 [-r repo_name] [-c linear_commits] [-b branches] [-k binary_kb] [-v]" >&2
      exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------
if [[ ! -f "$CONF" ]]; then
  echo "ERROR: $CONF not found." >&2; exit 1
fi
# shellcheck source=/dev/null
. "$CONF"

: "${hostname:?hostname not set in $CONF}"
: "${org:?org not set in $CONF}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN not set in $CONF}"
: "${GITHUB_API_BASE_URL:?GITHUB_API_BASE_URL not set in $CONF}"

GIT_HOSTNAME="${hostname}"
[[ "${GIT_HOSTNAME}" == "api.github.com" ]] && GIT_HOSTNAME="github.com"

TOKEN_FRST3="${GITHUB_TOKEN:0:3}"
case "${TOKEN_FRST3}" in
  ghs) REMOTE_URL="https://x-access-token:${GITHUB_TOKEN}@${GIT_HOSTNAME}/${org}/${MESSY_REPO}.git" ;;
  *)   REMOTE_URL="https://${GITHUB_TOKEN}:x-oauth-basic@${GIT_HOSTNAME}/${org}/${MESSY_REPO}.git" ;;
esac

if [[ "${VERBOSE}" == true ]]; then
  CURL_FLAGS="${curl_custom_flags:-} -v"
else
  CURL_FLAGS="${curl_custom_flags:-} --silent --show-error"
fi
ADMIN_AUTH="Authorization: token ${GITHUB_TOKEN}"
ACCEPT="Accept: application/vnd.github+json"
API_VER="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"

GIT_FLAGS="--quiet"
[[ "${VERBOSE}" == true ]] && GIT_FLAGS=""

export GIT_TERMINAL_PROMPT=0
export GIT_ADVICE=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
step() { echo ""; echo ">>> $*"; }
ok()   { echo "    OK: $*"; }
warn() { echo "    WARN: $*"; }

# Generate exactly N kilobytes of pseudo-random data.
# openssl rand is fast; xor with a counter byte makes distinct output across
# calls while keeping the data non-deltifiable.
gen_binary() {
  local kb="$1" counter="${2:-0}"
  openssl rand $(( kb * 1024 )) | python3 -c "
import sys
data = sys.stdin.buffer.read()
ctr = ${counter} & 0xFF
sys.stdout.buffer.write(bytes(b ^ ctr for b in data))
"
}

# Return a random integer in [0, N)
rand_int() { echo $(( RANDOM % $1 )); }

# Return a random integer in [lo, hi] inclusive
rand_range() { echo $(( $1 + RANDOM % ($2 - $1 + 1) )); }

timestamp_ms() {
  python3 -c 'import time; print(time.time_ns() // 1000000)'
}

# Random binary size: 50%-200% of BINARY_KB, rounded to nearest 32KB
rand_blob_kb() {
  local base=$(( BINARY_KB / 2 + RANDOM % BINARY_KB + RANDOM % BINARY_KB / 2 ))
  echo $(( (base / 32 + 1) * 32 ))
}

# Append a random-sized chunk from war-and-peace (or fallback text).
LOREM_FILE="test-data/war-and-peace.txt"
LOREM_LINES=0
LOREM_POS=1
init_lorem() {
  if [[ -f "${LOREM_FILE}" ]]; then
    LOREM_LINES=$(wc -l < "${LOREM_FILE}")
  fi
}
append_text() {
  local target="$1"
  local chunk=$(rand_range 10 60)
  if (( LOREM_LINES > 0 )); then
    local end=$(( LOREM_POS + chunk ))
    sed -n "${LOREM_POS},${end}p" "${LOREM_FILE}" >> "${target}"
    LOREM_POS=$(( end + 1 ))
    (( LOREM_POS >= LOREM_LINES )) && LOREM_POS=1
  else
    for _ in $(seq 1 "${chunk}"); do
      echo "ts=$(timestamp_ms) r=${RANDOM} filler line"
    done >> "${target}"
  fi
}

# Pool of synthetic commit authors (name + email pairs, parallel arrays)
AUTHOR_NAMES=(
  "Alice Thornton"   "Bob Ramirez"    "Carol Singh"     "David Park"
  "Eve Kowalski"     "Frank Osei"     "Grace Liu"       "Hank Petrov"
  "Iris Nakamura"    "Jack O'Brien"   "Karen Diallo"    "Leo Ferreira"
  "Mia Chen"         "Noah Johansson" "Olivia Mensah"   "Pedro Santos"
  "Quinn Adams"      "Rosa Takahashi" "Sam Okafor"      "Tara Flynn"
)
AUTHOR_EMAILS=(
  "alice@example.com"   "bob@example.com"     "carol@example.com"   "david@example.com"
  "eve@example.com"     "frank@example.com"   "grace@example.com"   "hank@example.com"
  "iris@example.com"    "jack@example.com"    "karen@example.com"   "leo@example.com"
  "mia@example.com"     "noah@example.com"    "olivia@example.com"  "pedro@example.com"
  "quinn@example.com"   "rosa@example.com"    "sam@example.com"     "tara@example.com"
)
AUTHOR_COUNT="${#AUTHOR_NAMES[@]}"

set_random_author() {
  local idx=$(rand_int "${AUTHOR_COUNT}")
  git config user.name  "${AUTHOR_NAMES[$idx]}"
  git config user.email "${AUTHOR_EMAILS[$idx]}"
}

# ---------------------------------------------------------------------------
# Weighted commit operation table.
# Each entry is a label; the count of entries determines the probability.
# Weights (100 total):
#   text_binary     35%  standard churn: text + mutating binary blob
#   text_only       20%  lighter commit, good for delta
#   binary_only     15%  heavy, no delta gain
#   churn_burst     10%  add 5-15 tiny files to inflate loose-object count
#   wide_tree_batch  8%  add a batch of files to a new subdir (tree objects)
#   large_blob       6%  add/replace a large binary (4x-8x base size)
#   rename_move      4%  rename or move an existing file
#   delete_recreate  2%  delete a tracked file and create a replacement
# ---------------------------------------------------------------------------
OP_TABLE=()
for _ in {1..35}; do OP_TABLE+=(text_binary);     done
for _ in {1..20}; do OP_TABLE+=(text_only);       done
for _ in {1..15}; do OP_TABLE+=(binary_only);     done
for _ in {1..10}; do OP_TABLE+=(churn_burst);     done
for _ in {1..8};  do OP_TABLE+=(wide_tree_batch); done
for _ in {1..6};  do OP_TABLE+=(large_blob);      done
for _ in {1..4};  do OP_TABLE+=(rename_move);     done
for _ in {1..2};  do OP_TABLE+=(delete_recreate); done
OP_TABLE_LEN="${#OP_TABLE[@]}"

# ---------------------------------------------------------------------------
# Step 1: Create the repo on GHES
# ---------------------------------------------------------------------------
step "Creating repo '${org}/${MESSY_REPO}' on GHES"

existing=$(curl ${CURL_FLAGS} \
  -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
  "${GITHUB_API_BASE_URL}/repos/${org}/${MESSY_REPO}" \
  -o /dev/null -w "%{http_code}" || echo "000")

if [[ "${existing}" == "200" ]]; then
  warn "Repo already exists -- pushing on top of existing history"
else
  payload=$(jq -cn \
    --arg nm "${MESSY_REPO}" \
    --arg desc "Intentionally messy repo for traffic simulation load testing" \
    '{"name":$nm,"description":$desc,"private":false,"auto_init":false}')
  result=$(curl ${CURL_FLAGS} -X POST \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
    "${GITHUB_API_BASE_URL}/orgs/${org}/repos" \
    -d "${payload}" 2>&1) || true
  if echo "${result}" | jq -e '.html_url' > /dev/null 2>&1; then
    ok "Created '${org}/${MESSY_REPO}'"
  else
    warn "Repo creation response: ${result}"
  fi
fi

# ---------------------------------------------------------------------------
# Step 2: Initialise local repository
# ---------------------------------------------------------------------------
WORKDIR="$(mktemp -d "/tmp/messy-repo-build-XXXXXX")"
trap 'echo "Cleaning up ${WORKDIR}..."; rm -rf "${WORKDIR}"' EXIT

step "Initialising local repo in ${WORKDIR}"
cd "${WORKDIR}"
git init ${GIT_FLAGS} --initial-branch=main .
init_lorem

# ---------------------------------------------------------------------------
# Step 3: Deep main branch history with randomised commit content
# ---------------------------------------------------------------------------
step "Building ${LINEAR_COMMITS} randomised commits on main"

# Persistent state for rename/delete ops -- keep a list of tracked paths
mkdir -p binary text churn blobs wide tree moved

TRACKED_CHURN=()
BINARY_COUNTER=0
BLOB_VERSION=0
TREE_BATCH=0
WIDE_BATCH_FILES=()

# Seed files so rename/delete ops always have something to work with
gen_binary "${BINARY_KB}" 0 > binary/payload.bin
touch text/history.txt
echo "initial" > churn/file-00000.txt
TRACKED_CHURN+=("churn/file-00000.txt")

set_random_author
git add .
git commit ${GIT_FLAGS} -m "init: initial commit" > /dev/null

for (( i=1; i<=LINEAR_COMMITS; i++ )); do
  # Rotate author on a randomly-sized window (every 3-8 commits)
  if (( i % (3 + RANDOM % 6) == 0 )); then
    set_random_author
  fi

  # Pick a random operation from the weighted table
  op="${OP_TABLE[$(rand_int ${OP_TABLE_LEN})]}"

  # Safety: fall back to text_binary if there's nothing to rename/delete
  if [[ "${op}" == "rename_move" || "${op}" == "delete_recreate" ]]; then
    (( ${#TRACKED_CHURN[@]} < 2 )) && op="text_binary"
  fi

  committed=false

  case "${op}" in

    text_binary)
      BINARY_COUNTER=$(( BINARY_COUNTER + 1 ))
      local_kb=$(rand_blob_kb)
      gen_binary "${local_kb}" "${BINARY_COUNTER}" > binary/payload.bin
      append_text text/history.txt
      git add binary/ text/
      git commit ${GIT_FLAGS} -m "churn(${i}): update binary (${local_kb}KB) and text" > /dev/null
      committed=true
      ;;

    text_only)
      append_text text/history.txt
      git add text/history.txt
      git commit ${GIT_FLAGS} -m "docs(${i}): update history text" > /dev/null
      committed=true
      ;;

    binary_only)
      BINARY_COUNTER=$(( BINARY_COUNTER + 1 ))
      local_kb=$(rand_blob_kb)
      gen_binary "${local_kb}" "${BINARY_COUNTER}" > binary/payload.bin
      git add binary/
      git commit ${GIT_FLAGS} -m "data(${i}): replace binary payload (${local_kb}KB)" > /dev/null
      committed=true
      ;;

    churn_burst)
      count=$(rand_range 5 15)
      ts=$(timestamp_ms)
      for j in $(seq 1 "${count}"); do
        fname="churn/burst-${i}-${j}.txt"
        printf "burst i=%d j=%d ts=%s r=%d\n" "${i}" "${j}" "${ts}" "${RANDOM}" > "${fname}"
        TRACKED_CHURN+=("${fname}")
      done
      git add churn/
      git commit ${GIT_FLAGS} -m "test(${i}): burst add ${count} churn files" > /dev/null
      committed=true
      ;;

    wide_tree_batch)
      TREE_BATCH=$(( TREE_BATCH + 1 ))
      dir="tree/batch-$(printf '%03d' ${TREE_BATCH})"
      mkdir -p "${dir}"
      file_count=$(rand_range 8 20)
      for f in $(seq 1 "${file_count}"); do
        printf "batch=%d file=%d r=%d\n" "${TREE_BATCH}" "${f}" "${RANDOM}" > "${dir}/f${f}.dat"
        WIDE_BATCH_FILES+=("${dir}/f${f}.dat")
      done
      git add tree/
      git commit ${GIT_FLAGS} -m "structure(${i}): wide tree batch ${TREE_BATCH} (${file_count} files)" > /dev/null
      committed=true
      ;;

    large_blob)
      BLOB_VERSION=$(( BLOB_VERSION + 1 ))
      large_kb=$(rand_range $(( BINARY_KB * 4 )) $(( BINARY_KB * 8 )))
      gen_binary "${large_kb}" "${BLOB_VERSION}" > "blobs/archive-v${BLOB_VERSION}.bin"
      git add blobs/
      git commit ${GIT_FLAGS} -m "asset(${i}): add large blob v${BLOB_VERSION} (${large_kb}KB)" > /dev/null
      committed=true
      ;;

    rename_move)
      # Pick a random tracked churn file and rename it
      src_idx=$(rand_int "${#TRACKED_CHURN[@]}")
      src="${TRACKED_CHURN[$src_idx]}"
      if [[ -f "${src}" ]]; then
        dest="moved/$(basename "${src}")-moved-${i}"
        git mv "${src}" "${dest}"
        # Replace in tracked list
        TRACKED_CHURN[$src_idx]="${dest}"
        git commit ${GIT_FLAGS} -m "refactor(${i}): rename $(basename ${src})" > /dev/null
        committed=true
      fi
      ;;

    delete_recreate)
      # Delete a random churn file, then add a new one in its place
      src_idx=$(rand_int "${#TRACKED_CHURN[@]}")
      src="${TRACKED_CHURN[$src_idx]}"
      if [[ -f "${src}" ]]; then
        git rm ${GIT_FLAGS} "${src}"
        new_name="churn/recreated-${i}.txt"
        printf "recreated from %s at %s r=%d\n" "${src}" "$(timestamp_ms)" "${RANDOM}" > "${new_name}"
        TRACKED_CHURN[$src_idx]="${new_name}"
        git add "${new_name}"
        git commit ${GIT_FLAGS} -m "cleanup(${i}): delete and recreate $(basename ${src})" > /dev/null
        committed=true
      fi
      ;;
  esac

  # Fallback: if somehow nothing committed (e.g. file already gone), do text
  if [[ "${committed}" != true ]]; then
    append_text text/history.txt
    git add text/history.txt
    git commit ${GIT_FLAGS} -m "fallback(${i}): text update" > /dev/null
  fi
done

ok "Main history done: $(git rev-list --count HEAD) commits, $(git log --format='%ae' | sort -u | wc -l | tr -d ' ') distinct authors"

# ---------------------------------------------------------------------------
# Step 4: Diverged branches with random fork points and varied content
# ---------------------------------------------------------------------------
step "Creating ${BRANCHES} diverged branches (${COMMITS_PER_BRANCH} commits each)"

MAIN_SHAS=()
while IFS= read -r sha; do
  MAIN_SHAS+=("${sha}")
done < <(git log --format='%H' main)
MAIN_LEN="${#MAIN_SHAS[@]}"

MERGE_BRANCH_SHAS=()  # branches we will later merge back into main

for (( b=1; b<=BRANCHES; b++ )); do
  branch_name="feature/sim-branch-$(printf '%03d' ${b})"

  # Truly random fork point -- anywhere in the full history
  fork_idx=$(rand_int "${MAIN_LEN}")
  fork_sha="${MAIN_SHAS[$fork_idx]}"

  git checkout ${GIT_FLAGS} -b "${branch_name}" "${fork_sha}"
  set_random_author

  bdir="branch-work/$(printf '%03d' ${b})"
  mkdir -p "${bdir}"

  for (( c=1; c<=COMMITS_PER_BRANCH; c++ )); do
    # Rotate author randomly on branch too
    (( RANDOM % 4 == 0 )) && set_random_author

    # Random operation: roughly 50% text, 30% binary, 20% both
    local_op=$(( RANDOM % 10 ))
    case $local_op in
      0|1|2|3|4)
        printf "branch %03d commit %03d ts=%s r=%d\n" "${b}" "${c}" "$(timestamp_ms)" "${RANDOM}" \
          > "${bdir}/commit-$(printf '%03d' ${c}).txt"
        git add "${bdir}/commit-$(printf '%03d' ${c}).txt"
        git commit ${GIT_FLAGS} -m "feat(branch-${b}): text commit ${c}" > /dev/null
        ;;
      5|6|7)
        local_kb=$(rand_range $(( BINARY_KB / 4 )) $(( BINARY_KB )))
        gen_binary "${local_kb}" $(( b * c + RANDOM % 256 )) > "${bdir}/data-${c}.bin"
        git add "${bdir}/data-${c}.bin"
        git commit ${GIT_FLAGS} -m "feat(branch-${b}): binary blob ${c} (${local_kb}KB)" > /dev/null
        ;;
      8|9)
        printf "mixed %03d/%03d\n" "${b}" "${c}" > "${bdir}/mixed-${c}.txt"
        local_kb=$(rand_range 32 $(( BINARY_KB / 2 )))
        gen_binary "${local_kb}" $(( b + c )) > "${bdir}/mixed-${c}.bin"
        git add "${bdir}/mixed-${c}.txt" "${bdir}/mixed-${c}.bin"
        git commit ${GIT_FLAGS} -m "feat(branch-${b}): mixed commit ${c}" > /dev/null
        ;;
    esac
  done

  # Randomly decide whether this branch will be merged back into main (~30%)
  if (( RANDOM % 10 < 3 )); then
    MERGE_BRANCH_SHAS+=("${branch_name}")
  fi
done

ok "Created ${BRANCHES} branches with random fork points"

# ---------------------------------------------------------------------------
# Step 5: Merge a random subset of branches back into main
# Creates merge commits and a non-linear DAG that is more expensive for
# upload-pack to traverse on clone.
# ---------------------------------------------------------------------------
if (( ${#MERGE_BRANCH_SHAS[@]} > 0 )); then
  step "Merging ${#MERGE_BRANCH_SHAS[@]} branches back into main (non-linear DAG)"

  git checkout ${GIT_FLAGS} main
  set_random_author

  for branch in "${MERGE_BRANCH_SHAS[@]}"; do
    git merge ${GIT_FLAGS} --no-ff "${branch}" \
      -m "Merge ${branch} into main (sim)" 2>/dev/null || {
      # On conflict just abort and skip
      git merge --abort 2>/dev/null || true
      warn "Merge conflict on ${branch} -- skipped"
    }
    ok "Merged ${branch}"
  done
else
  git checkout ${GIT_FLAGS} main
fi

# Keep the simulator's shared feature branch related to main so pull-request
# operations are valid after replacing the appliance's primer history.
git checkout ${GIT_FLAGS} -b feature-branch main
printf "seed feature branch ts=%s r=%d\n" "$(timestamp_ms)" "${RANDOM}" > sim-feature-seed.txt
git add sim-feature-seed.txt
git commit ${GIT_FLAGS} -m "seed: create simulator feature branch" > /dev/null
git checkout ${GIT_FLAGS} main

# ---------------------------------------------------------------------------
# Step 6: Push everything to GHES
# ---------------------------------------------------------------------------
step "Adding remote and pushing all refs"

git remote add origin "${REMOTE_URL}"

echo ""
echo "  Pushing main..."
git push ${GIT_FLAGS} --force origin main

echo "  Pushing all branches..."
git push ${GIT_FLAGS} --force origin --all

# Summary statistics
TOTAL_COMMITS=$(git rev-list --count HEAD)
TOTAL_BRANCHES=$(git branch | wc -l | tr -d ' ')
DISTINCT_AUTHORS=$(git log --format='%ae' --all | sort -u | wc -l | tr -d ' ')
TOTAL_OBJECTS=$(git count-objects -v | awk '/^count/ {print $2}')
PACK_SIZE_KB=$(git count-objects -v | awk '/^size-pack/ {print $2}')

ok "Push complete"
echo ""
echo "=========================================="
echo "  Messy Repo Seeding Complete"
echo "=========================================="
printf "  Repo:              %s/%s\n" "${org}" "${MESSY_REPO}"
printf "  Main commits:      %s\n"    "${TOTAL_COMMITS}"
printf "  Branches:          %s\n"    "${TOTAL_BRANCHES}"
printf "  Merged back:       %d\n"    "${#MERGE_BRANCH_SHAS[@]}"
printf "  Distinct authors:  %s\n"    "${DISTINCT_AUTHORS}"
printf "  Total objects:     %s\n"    "${TOTAL_OBJECTS}"
printf "  Pack size (KB):    %s\n"    "${PACK_SIZE_KB}"
echo ""
echo "  Run the traffic simulator:"
echo ""
echo "    ./simulate-day-of-traffic.sh -r ${MESSY_REPO} -u tmp/sim-users.json -p 8 -d 3600"
echo "=========================================="

# The randomized implementation above is the complete seeder.  The legacy
# implementation below is retained in the file history but must not execute.
exit 0

#
# Builds a locally-assembled git repository with characteristics that force
# Git processes (upload-pack, receive-pack, git-gc, delta compression) to
# work hard, then pushes it to the GHES target so every clone and fetch
# in simulate-day-of-traffic.sh has to serve real load.
#
# What makes it messy:
#   - Deep linear history: many commits with both text and binary churn
#   - Non-deltifiable binary blobs: pseudo-random bytes appended each commit,
#     forcing the packfile to store near-full copies of every version
#   - Mutating large text file: a growing war-and-peace excerpt that delta-
#     compresses across versions (intentionally good/bad contrast in one repo)
#   - Wide diverged branches (default 25): complex DAG that upload-pack has
#     to traverse on every clone
#   - Wide directory tree commit: 300 small files across 30 subdirs (many
#     tree objects, expensive to traverse)
#   - File renames and moves: triggers similarity detection on fetch/log ops
#   - Pushes all branches so GHES spokes must replicate the full graph
#
# Usage:
#   ./seed-messy-repo.sh [-r <messy_repo_name>] [-c <commits>] [-b <branches>]
#                        [-k <binary_kb>] [-v]
#
#   -r NAME  Name of the repo to create on GHES (default: "messy-repo")
#   -c N     Linear commits on main branch (default: 200)
#   -b N     Diverged branches to create (default: 25)
#   -k N     Size of the binary blob in KB per commit (default: 256)
#   -v       Verbose git output
#
# Requires: git, python3 (for base64encode.py), openssl
#
# After completion, update .gh-api-examples.conf repo= to "messy-repo" (or
# the -r value you chose) before running simulate-day-of-traffic.sh, or
# pass -r messy-repo to that script's -u manifest.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
MESSY_REPO="messy-repo"
LINEAR_COMMITS=200
BRANCHES=25
COMMITS_PER_BRANCH=20
BINARY_KB=256
VERBOSE=false
CONF="./.gh-api-examples.conf"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while getopts "r:c:b:k:v" opt; do
  case $opt in
    r) MESSY_REPO=$OPTARG ;;
    c) LINEAR_COMMITS=$OPTARG ;;
    b) BRANCHES=$OPTARG ;;
    k) BINARY_KB=$OPTARG ;;
    v) VERBOSE=true ;;
    *)
      echo "Usage: $0 [-r repo_name] [-c linear_commits] [-b branches] [-k binary_kb] [-v]" >&2
      exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------
if [[ ! -f "$CONF" ]]; then
  echo "ERROR: $CONF not found." >&2; exit 1
fi
# shellcheck source=/dev/null
. "$CONF"

: "${hostname:?hostname not set in $CONF}"
: "${org:?org not set in $CONF}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN not set in $CONF}"
: "${GITHUB_API_BASE_URL:?GITHUB_API_BASE_URL not set in $CONF}"

GIT_HOSTNAME="${hostname}"
[[ "${GIT_HOSTNAME}" == "api.github.com" ]] && GIT_HOSTNAME="github.com"

TOKEN_FRST3="${GITHUB_TOKEN:0:3}"
case "${TOKEN_FRST3}" in
  ghs) REMOTE_URL="https://x-access-token:${GITHUB_TOKEN}@${GIT_HOSTNAME}/${org}/${MESSY_REPO}.git" ;;
  *)   REMOTE_URL="https://${GITHUB_TOKEN}:x-oauth-basic@${GIT_HOSTNAME}/${org}/${MESSY_REPO}.git" ;;
esac

CURL_FLAGS="${curl_custom_flags:-} -s"
ADMIN_AUTH="Authorization: token ${GITHUB_TOKEN}"
ACCEPT="Accept: application/vnd.github+json"
API_VER="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"

GIT_FLAGS="--quiet"
[[ "${VERBOSE}" == true ]] && GIT_FLAGS=""

# Silence git hints
export GIT_TERMINAL_PROMPT=0
export GIT_ADVICE=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
step() { echo ""; echo ">>> $*"; }
ok()   { echo "    OK: $*"; }
warn() { echo "    WARN: $*"; }

# Generate N kilobytes of pseudo-random-ish data.
# Uses openssl which is much faster than /dev/urandom for large blocks.
# The XOR of the counter byte ensures each call produces distinct content
# (so git can't delta them to zero).
gen_binary() {
  local kb="$1" counter="${2:-0}"
  # openssl rand produces cryptographically random bytes; xor via python
  # ensures each generation is distinct without being fully re-random each time
  openssl rand $(( kb * 1024 )) | python3 -c "
import sys, struct
data = sys.stdin.buffer.read()
ctr = ${counter} & 0xFF
out = bytes(b ^ ctr for b in data)
sys.stdout.buffer.write(out)
"
}

# Append a paragraph to the growing text file.
# Uses war-and-peace.txt if available, otherwise generates text.
LOREM_FILE="test-data/war-and-peace.txt"
LOREM_LINE=1
append_text() {
  local target="$1"
  if [[ -f "${LOREM_FILE}" ]]; then
    local chunk_end=$(( LOREM_LINE + 40 ))
    sed -n "${LOREM_LINE},${chunk_end}p" "${LOREM_FILE}" >> "${target}"
    LOREM_LINE=$(( chunk_end + 1 ))
    # Wrap around
    local total; total=$(wc -l < "${LOREM_FILE}")
    (( LOREM_LINE >= total )) && LOREM_LINE=1
  else
    echo "line ${RANDOM} timestamp $(timestamp_ms) filler text for delta compression testing" >> "${target}"
  fi
}

# ---------------------------------------------------------------------------
# Step 1: Create the repo on GHES
# ---------------------------------------------------------------------------
step "Creating repo '${org}/${MESSY_REPO}' on GHES"

existing=$(curl ${CURL_FLAGS} \
  -H "${ADMIN_AUTH}" -H "${ACCEPT}" \
  "${GITHUB_API_BASE_URL}/repos/${org}/${MESSY_REPO}" \
  -o /dev/null -w "%{http_code}" || echo "000")

if [[ "${existing}" == "200" ]]; then
  warn "Repo already exists -- pushing on top of existing history"
else
  payload=$(jq -cn \
    --arg nm "${MESSY_REPO}" \
    --arg desc "Intentionally messy repo for traffic simulation load testing" \
    '{"name":$nm,"description":$desc,"private":false,"auto_init":false}')
  result=$(curl ${CURL_FLAGS} -X POST \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
    "${GITHUB_API_BASE_URL}/orgs/${org}/repos" \
    -d "${payload}" 2>&1) || true
  if echo "${result}" | jq -e '.html_url' > /dev/null 2>&1; then
    ok "Created '${org}/${MESSY_REPO}'"
  else
    warn "Repo creation response: ${result}"
  fi
fi

# ---------------------------------------------------------------------------
# Step 2: Build the local repository
# ---------------------------------------------------------------------------
WORKDIR="$(mktemp -d "/tmp/messy-repo-build-XXXXXX")"
trap 'echo "Cleaning up ${WORKDIR}..."; rm -rf "${WORKDIR}"' EXIT

step "Initialising local repo in ${WORKDIR}"

cd "${WORKDIR}"
git init ${GIT_FLAGS} --initial-branch=main .
git config user.email "simulator@example.com"
git config user.name  "Traffic Simulator"

# ---------------------------------------------------------------------------
# Step 3: Deep linear history on main
#
# Each commit contains:
#   a) A mutating binary blob  -- defeats delta compression
#   b) Appended text file      -- compresses well across versions (contrast)
#   c) A tiny per-commit file  -- inflates loose object / pack object count
# ---------------------------------------------------------------------------
step "Building ${LINEAR_COMMITS} linear commits on main (binary=${BINARY_KB}KB per commit)"

mkdir -p binary text churn

# Seed the initial binary file
gen_binary "${BINARY_KB}" 0 > binary/payload.bin
# Seed the text file
touch text/history.txt

for (( i=1; i<=LINEAR_COMMITS; i++ )); do
  # Mutate the binary: re-generate with a new counter so delta breaks down
  gen_binary "${BINARY_KB}" "${i}" > binary/payload.bin

  # Grow the text file
  append_text text/history.txt

  # Add a tiny churn file unique to this commit
  printf "commit %05d ts=%s random=%05d\n" \
    "${i}" "$(timestamp_ms)" "$(( RANDOM ))" > "churn/file-$(printf '%05d' ${i}).txt"

  # Every 20 commits also add a small directory tree to inflate tree objects
  if (( i % 20 == 0 )); then
    dir="wide/batch-$(printf '%03d' $(( i / 20 )))"
    mkdir -p "${dir}"
    for j in {1..10}; do
      echo "batch $i file $j" > "${dir}/f${j}.txt"
    done
    git add wide/ ${GIT_FLAGS}
  fi

  git add binary/ text/ churn/ ${GIT_FLAGS}
  git commit ${GIT_FLAGS} -m "churn: commit ${i} of ${LINEAR_COMMITS}" > /dev/null
done

ok "Linear history done ($(git rev-list --count HEAD) commits)"

# ---------------------------------------------------------------------------
# Step 4: File renames and moves (triggers similarity detection)
# ---------------------------------------------------------------------------
step "Adding rename/move operations"

mkdir -p moved
# Move several churn files to a different path
for f in churn/file-00001.txt churn/file-00010.txt churn/file-00020.txt; do
  [[ -f "$f" ]] || continue
  dest="moved/$(basename "${f}")"
  git mv "${f}" "${dest}"
done
git commit ${GIT_FLAGS} -m "refactor: move files to moved/ directory" > /dev/null

# Rename binary payload
git mv binary/payload.bin binary/data-archive.bin
gen_binary "${BINARY_KB}" 255 > binary/data-archive.bin
git add binary/
git commit ${GIT_FLAGS} -m "refactor: rename payload.bin -> data-archive.bin" > /dev/null

ok "Renames committed"

# ---------------------------------------------------------------------------
# Step 5: Wide directory tree commit
# 300 small files across 30 subdirectories forces a large tree-object graph.
# upload-pack must traverse every tree on clone.
# ---------------------------------------------------------------------------
step "Adding wide directory tree (300 files, 30 dirs)"

for d in $(seq -w 1 30); do
  dir="tree/dir-${d}"
  mkdir -p "${dir}"
  for f in $(seq -w 1 10); do
    printf "dir=%s file=%s random=%d\n" "${d}" "${f}" "${RANDOM}" > "${dir}/file-${f}.dat"
  done
done
git add tree/
git commit ${GIT_FLAGS} -m "structure: add wide tree (30 dirs x 10 files)" > /dev/null
ok "Wide tree committed ($(git ls-files tree/ | wc -l | tr -d ' ') files)"

# ---------------------------------------------------------------------------
# Step 6: Large binary blobs committed and then mutated
# Each version is incompressible and distinct from the last, so pack stores
# multiple near-full copies -- maximises packfile work on serve.
# ---------------------------------------------------------------------------
step "Adding large binary blob history (5 versions)"

mkdir -p blobs
for v in {1..5}; do
  gen_binary $(( BINARY_KB * 4 )) "${v}" > blobs/archive-v${v}.bin
  git add blobs/
  git commit ${GIT_FLAGS} -m "blob: add binary archive version ${v}" > /dev/null
  ok "Binary blob v${v} committed ($(( BINARY_KB * 4 ))KB)"
done

# Also mutate a blob in-place (same path, different content) to create chains
for v in {6..10}; do
  gen_binary $(( BINARY_KB * 4 )) $(( v * 7 )) > blobs/archive-v1.bin
  git add blobs/archive-v1.bin
  git commit ${GIT_FLAGS} -m "blob: mutate archive-v1 iteration ${v}" > /dev/null
done
ok "Mutating blob history done"

# ---------------------------------------------------------------------------
# Step 7: Diverged branches
# Each branch forks from a random commit in the linear history so upload-pack
# must walk a wide, irregular DAG.  Each branch then adds its own churn.
# ---------------------------------------------------------------------------
step "Creating ${BRANCHES} diverged branches (${COMMITS_PER_BRANCH} commits each)"

# Collect commit SHAs from main so we can fork from varied points
MAIN_SHAS=()
while IFS= read -r sha; do
  MAIN_SHAS+=("${sha}")
done < <(git log --format='%H' main)
MAIN_LEN="${#MAIN_SHAS[@]}"

for (( b=1; b<=BRANCHES; b++ )); do
  branch_name="feature/sim-branch-$(printf '%03d' ${b})"

  # Pick a fork point: vary across the first 80% of history
  fork_idx=$(( (b * (MAIN_LEN * 8 / 10)) / BRANCHES ))
  fork_sha="${MAIN_SHAS[$fork_idx]}"

  git checkout ${GIT_FLAGS} -b "${branch_name}" "${fork_sha}"

  mkdir -p "branch-work/$(printf '%03d' ${b})"
  for (( c=1; c<=COMMITS_PER_BRANCH; c++ )); do
    # Mix of text churn and small binary on each branch
    printf "branch %03d commit %03d ts=%s\n" "${b}" "${c}" "$(timestamp_ms)" \
      > "branch-work/$(printf '%03d' ${b})/commit-$(printf '%03d' ${c}).txt"

    if (( c % 5 == 0 )); then
      # Every 5th branch commit includes a small binary mutation
      gen_binary 64 $(( b * c )) > "branch-work/$(printf '%03d' ${b})/data.bin"
      git add "branch-work/$(printf '%03d' ${b})/data.bin"
    fi

    git add "branch-work/$(printf '%03d' ${b})/commit-$(printf '%03d' ${c}).txt"
    git commit ${GIT_FLAGS} -m "branch ${b}: commit ${c}" > /dev/null
  done
done

# Return to main
git checkout ${GIT_FLAGS} main
ok "Created ${BRANCHES} branches ($(git branch | wc -l | tr -d ' ') total)"

# ---------------------------------------------------------------------------
# Step 8: Push everything to GHES
# ---------------------------------------------------------------------------
step "Adding remote and pushing all branches"

git remote add origin "${REMOTE_URL}"

echo ""
echo "Pushing main..."
git push ${GIT_FLAGS} origin main

echo "Pushing all branches (this may take a moment)..."
git push ${GIT_FLAGS} origin --all

TOTAL_OBJECTS=$(git count-objects -v | awk '/^count/ {print $2}')
TOTAL_SIZE=$(git count-objects -v | awk '/^size-pack/ {print $2}')

ok "Push complete"
echo ""
echo "=========================================="
echo "  Messy Repo Seeding Complete"
echo "=========================================="
printf "  Repo:           %s/%s\n" "${org}" "${MESSY_REPO}"
printf "  Commits (main): %d\n"    "$(git rev-list --count main)"
printf "  Branches:       %d\n"    "$(git branch | wc -l | tr -d ' ')"
printf "  Total objects:  %s\n"    "${TOTAL_OBJECTS}"
printf "  Pack size (KB): %s\n"    "${TOTAL_SIZE}"
echo ""
echo "  To use this repo in the simulator:"
echo ""
echo "    # Either: edit .gh-api-examples.conf and set repo=${MESSY_REPO}"
echo ""
echo "    # Or: pass an explicit repo override to the simulator once"
echo "    # that flag is added (repo is read from config today)"
echo ""
echo "  Run the traffic simulator against this repo:"
echo "    repo=${MESSY_REPO} ./simulate-day-of-traffic.sh -u tmp/sim-users.json -p 8 -d 3600"
echo "=========================================="
