#!/usr/bin/env bash
# Adds lightweight tags to an existing GHES repository without cloning it or
# rewriting its branches and commit history.
#
# Usage:
#   ./seed-repo-tags.sh [-r <repo_name>] [-n <tag_count>] [-p <tag_prefix>] [-v]
#
#   -r NAME  Target repository (default: repo from .gh-api-examples.conf)
#   -n N     Number of tags to create (default: 1000, max: 4000)
#   -p NAME  Tag namespace prefix (default: "load-test")
#   -v       Verbose curl output

set -euo pipefail

TARGET_REPO=""
TAG_COUNT=1000
TAG_PREFIX="load-test"
VERBOSE=false
CONF="./.gh-api-examples.conf"

usage() {
  cat <<'EOF'
Usage:
  ./seed-repo-tags.sh [-r repo_name] [-n tag_count] [-p tag_prefix] [-v]

Adds lightweight tags to an existing repository. Each tag points to a randomly
selected recent commit; branches and commit history are not changed.
EOF
}

while getopts "r:n:p:vh" opt; do
  case "${opt}" in
    r) TARGET_REPO="${OPTARG}" ;;
    n) TAG_COUNT="${OPTARG}" ;;
    p) TAG_PREFIX="${OPTARG}" ;;
    v) VERBOSE=true ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 1 ;;
  esac
done

if [[ ! "${TAG_COUNT}" =~ ^[0-9]{1,5}$ ]]; then
  echo "ERROR: -n must be an integer between 1 and 4000." >&2
  exit 1
fi
TAG_COUNT=$((10#${TAG_COUNT}))
if (( TAG_COUNT < 1 || TAG_COUNT > 4000 )); then
  echo "ERROR: -n must be an integer between 1 and 4000." >&2
  exit 1
fi

if [[ ! -f "${CONF}" ]]; then
  echo "ERROR: ${CONF} not found. Configure this repo first." >&2
  exit 1
fi
# shellcheck source=/dev/null
. "${CONF}"

: "${org:?org not set in ${CONF}}"
: "${repo:?repo not set in ${CONF}}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN not set in ${CONF}}"
: "${GITHUB_API_BASE_URL:?GITHUB_API_BASE_URL not set in ${CONF}}"
[[ -n "${TARGET_REPO}" ]] || TARGET_REPO="${repo}"

if ! git check-ref-format "refs/tags/${TAG_PREFIX}/probe"; then
  echo "ERROR: invalid tag prefix '${TAG_PREFIX}'." >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required." >&2
  exit 1
fi

if [[ "${VERBOSE}" == true ]]; then
  CURL_FLAGS="${curl_custom_flags:-} -v"
else
  CURL_FLAGS="${curl_custom_flags:-} --silent --show-error"
fi
ADMIN_AUTH="Authorization: token ${GITHUB_TOKEN}"
ACCEPT="Accept: application/vnd.github+json"
API_VER="X-GitHub-Api-Version: ${github_api_version:-2022-11-28}"
REPO_API="${GITHUB_API_BASE_URL}/repos/${org}/${TARGET_REPO}"

echo ">>> Loading recent commit targets from ${org}/${TARGET_REPO}"
commit_result=$(curl ${CURL_FLAGS} \
  -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
  "${REPO_API}/commits?per_page=100" \
  -w $'\n%{http_code}') || true
http_status="${commit_result##*$'\n'}"
commit_body="${commit_result%$'\n'*}"
if [[ "${http_status}" != "200" ]]; then
  message=$(printf "%s" "${commit_body}" | jq -r '.message // empty' 2>/dev/null || true)
  echo "ERROR: could not list commit targets (HTTP ${http_status}${message:+: ${message}})." >&2
  exit 1
fi

COMMIT_SHAS=()
COMMIT_COUNT=0
while IFS= read -r sha; do
  if [[ -n "${sha}" ]]; then
    COMMIT_SHAS+=("${sha}")
    COMMIT_COUNT=$(( COMMIT_COUNT + 1 ))
  fi
done < <(printf "%s" "${commit_body}" | jq -r '.[].sha')

if (( COMMIT_COUNT == 0 )); then
  echo "ERROR: ${org}/${TARGET_REPO} has no commits to tag." >&2
  exit 1
fi

RUN_ID=$(python3 -c 'import time; print(time.time_ns())')
CREATED=0
FAILED=0

echo ">>> Creating ${TAG_COUNT} permanent lightweight tags"
for (( i=1; i<=TAG_COUNT; i++ )); do
  tag_name="${TAG_PREFIX}/${RUN_ID}/$(printf '%06d' "${i}")"
  target_sha="${COMMIT_SHAS[$(( RANDOM % COMMIT_COUNT ))]}"
  payload=$(jq -cn \
    --arg ref "refs/tags/${tag_name}" \
    --arg sha "${target_sha}" \
    '{"ref":$ref,"sha":$sha}')

  result=$(curl ${CURL_FLAGS} -X POST \
    -H "${ADMIN_AUTH}" -H "${ACCEPT}" -H "${API_VER}" \
    -H "Content-Type: application/json" \
    "${REPO_API}/git/refs" \
    -d "${payload}" \
    -w $'\n%{http_code}') || true
  http_status="${result##*$'\n'}"
  response_body="${result%$'\n'*}"

  if [[ "${http_status}" == "201" ]]; then
    CREATED=$(( CREATED + 1 ))
  else
    message=$(printf "%s" "${response_body}" | jq -r '.message // empty' 2>/dev/null || true)
    echo "WARN: failed to create ${tag_name} (HTTP ${http_status}${message:+: ${message}})" >&2
    FAILED=$(( FAILED + 1 ))
  fi

  if (( i % 100 == 0 )); then
    printf "    Progress: %d/%d requested, %d created\n" "${i}" "${TAG_COUNT}" "${CREATED}"
  fi
done

echo ""
printf "Tags created: %d\n" "${CREATED}"
printf "Tags failed:  %d\n" "${FAILED}"
printf "Tag prefix:   %s/%s\n" "${TAG_PREFIX}" "${RUN_ID}"

if (( FAILED > 0 )); then
  exit 1
fi
