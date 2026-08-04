#!/usr/bin/env bash
# import-github-repos-into-org.sh
#
# Clones repositories from github.com and imports them into a target GHES org.
# Useful for quickly populating a traffic simulation org with real-world history.
#
# Usage:
#   ./import-github-repos-into-org.sh [-o <target_org>] [-f <repo_list_file>] \
#                                     [-p <target_name_prefix>] [-v] \
#                                     <owner/repo|https://github.com/owner/repo> [...]
#
#   -o ORG   Target GHES organization (default: org from .gh-api-examples.conf)
#   -f FILE  File containing source repositories, one per line
#            (blank lines and lines starting with # are ignored)
#   -p STR   Prefix applied to imported target repo names (default: none)
#   -v       Verbose output
#
# Environment:
#   GITHUB_DOTCOM_TOKEN (optional) - token used for cloning private github.com repos.
#
# Example:
#   ./import-github-repos-into-org.sh -o traffic-sim -p src- \
#     cli/cli hashicorp/terraform

set -euo pipefail

TARGET_ORG=""
REPO_LIST_FILE=""
TARGET_PREFIX=""
VERBOSE=false
CONF="./.gh-api-examples.conf"

usage() {
  cat <<'EOF'
Usage:
  ./import-github-repos-into-org.sh [-o target_org] [-f repo_list_file] [-p target_name_prefix] [-v] <repo...>

Input repo formats:
  owner/repo
  https://github.com/owner/repo
  https://github.com/owner/repo.git
  git@github.com:owner/repo.git
EOF
}

while getopts "o:f:p:vh" opt; do
  case "$opt" in
    o) TARGET_ORG="$OPTARG" ;;
    f) REPO_LIST_FILE="$OPTARG" ;;
    p) TARGET_PREFIX="$OPTARG" ;;
    v) VERBOSE=true ;;
    h) usage; exit 0 ;;
    *) usage; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

if [[ ! -f "${CONF}" ]]; then
  echo "ERROR: ${CONF} not found. Configure this repo first." >&2
  exit 1
fi
# shellcheck source=/dev/null
. "${CONF}"

: "${hostname:?hostname not set in ${CONF}}"
: "${org:?org not set in ${CONF}}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN not set in ${CONF}}"
: "${GITHUB_API_BASE_URL:?GITHUB_API_BASE_URL not set in ${CONF}}"

[[ -z "${TARGET_ORG}" ]] && TARGET_ORG="${org}"

if [[ "${VERBOSE}" == true ]]; then
  CURL_FLAGS="${curl_custom_flags:-} -v"
  GIT_QUIET_FLAGS=""
else
  CURL_FLAGS="${curl_custom_flags:-} --silent --show-error"
  GIT_QUIET_FLAGS="--quiet"
fi

step() { echo ""; echo ">>> $*"; }
ok()   { echo "    OK: $*"; }
warn() { echo "    WARN: $*"; }

normalize_repo_spec() {
  local spec="$1"
  spec="${spec#https://github.com/}"
  spec="${spec#http://github.com/}"
  spec="${spec#git@github.com:}"
  spec="${spec#ssh://git@github.com/}"
  spec="${spec%.git}"
  spec="${spec%/}"

  if [[ "${spec}" != */* ]]; then
    return 1
  fi

  local owner="${spec%%/*}"
  local repo_name="${spec#*/}"
  if [[ -z "${owner}" || -z "${repo_name}" || "${repo_name}" == */* ]]; then
    return 1
  fi

  printf "%s|%s\n" "${owner}" "${repo_name}"
}

dotcom_clone_url() {
  local owner="$1" repo_name="$2"
  if [[ -n "${GITHUB_DOTCOM_TOKEN:-}" ]]; then
    echo "https://x-access-token:${GITHUB_DOTCOM_TOKEN}@github.com/${owner}/${repo_name}.git"
  else
    echo "https://github.com/${owner}/${repo_name}.git"
  fi
}

ghes_push_url() {
  local owner="$1" repo_name="$2"
  local token_prefix="${GITHUB_TOKEN:0:3}"
  local git_hostname="${hostname}"
  [[ "${git_hostname}" == "api.github.com" ]] && git_hostname="github.com"
  case "${token_prefix}" in
    ghs) echo "https://x-access-token:${GITHUB_TOKEN}@${git_hostname}/${owner}/${repo_name}.git" ;;
    *)   echo "https://${GITHUB_TOKEN}:x-oauth-basic@${git_hostname}/${owner}/${repo_name}.git" ;;
  esac
}

read_specs=()
for arg in "$@"; do
  read_specs+=("${arg}")
done

if [[ -n "${REPO_LIST_FILE}" ]]; then
  if [[ ! -f "${REPO_LIST_FILE}" ]]; then
    echo "ERROR: repo list file not found: ${REPO_LIST_FILE}" >&2
    exit 1
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" || "${line:0:1}" == "#" ]] && continue
    read_specs+=("${line}")
  done < "${REPO_LIST_FILE}"
fi

if (( ${#read_specs[@]} == 0 )); then
  echo "ERROR: no source repositories provided." >&2
  usage
  exit 1
fi

normalized_specs=()
for spec in "${read_specs[@]}"; do
  if ! norm="$(normalize_repo_spec "${spec}")"; then
    echo "ERROR: invalid repository spec '${spec}'." >&2
    exit 1
  fi
  normalized_specs+=("${norm}")
done

WORKDIR="$(mktemp -d "/tmp/import-github-repos-XXXXXX")"
trap 'rm -rf "${WORKDIR}"' EXIT

step "Importing ${#normalized_specs[@]} repositories into ${TARGET_ORG}"

success_count=0
fail_count=0

for entry in "${normalized_specs[@]}"; do
  src_owner="${entry%%|*}"
  src_repo="${entry#*|}"
  target_repo="${TARGET_PREFIX}${src_repo}"

  step "Importing github.com/${src_owner}/${src_repo} -> ${TARGET_ORG}/${target_repo}"

  repo_status=$(curl ${CURL_FLAGS} \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
    -H "Authorization: token ${GITHUB_TOKEN}" \
    "${GITHUB_API_BASE_URL}/repos/${TARGET_ORG}/${target_repo}" \
    -o /dev/null -w "%{http_code}" || echo "000")

  if [[ "${repo_status}" == "200" ]]; then
    ok "Target repo already exists"
  else
    create_payload=$(jq -cn \
      --arg name "${target_repo}" \
      --arg desc "Imported from github.com/${src_owner}/${src_repo}" \
      '{"name":$name,"description":$desc,"private":false,"auto_init":false}')
    create_response=$(curl ${CURL_FLAGS} -X POST \
      -H "Accept: application/vnd.github+json" \
      -H "X-GitHub-Api-Version: ${github_api_version:-2022-11-28}" \
      -H "Authorization: token ${GITHUB_TOKEN}" \
      "${GITHUB_API_BASE_URL}/orgs/${TARGET_ORG}/repos" \
      -d "${create_payload}" 2>&1) || true
    if echo "${create_response}" | jq -e '.html_url // empty' > /dev/null 2>&1; then
      ok "Created target repo"
    else
      warn "Failed to create target repo (${TARGET_ORG}/${target_repo})"
      fail_count=$(( fail_count + 1 ))
      continue
    fi
  fi

  source_url="$(dotcom_clone_url "${src_owner}" "${src_repo}")"
  target_url="$(ghes_push_url "${TARGET_ORG}" "${target_repo}")"
  local_dir="${WORKDIR}/${src_owner}-${src_repo}.git"
  rm -rf "${local_dir}"

  if ! git clone ${GIT_QUIET_FLAGS} --bare "${source_url}" "${local_dir}" 2>&1; then
    warn "Failed to clone source repository"
    fail_count=$(( fail_count + 1 ))
    continue
  fi

  git -C "${local_dir}" remote add target "${target_url}"
  if git -C "${local_dir}" push ${GIT_QUIET_FLAGS} --prune target \
    '+refs/heads/*:refs/heads/*' \
    '+refs/tags/*:refs/tags/*' 2>&1; then
    ok "Imported branches and tags"
    success_count=$(( success_count + 1 ))
  else
    warn "Failed to push to target repo"
    fail_count=$(( fail_count + 1 ))
  fi
done

echo ""
echo "=========================================="
echo "  GitHub.com Repository Import Summary"
echo "=========================================="
printf "  Target org:   %s\n" "${TARGET_ORG}"
printf "  Imported OK:  %d\n" "${success_count}"
printf "  Failed:       %d\n" "${fail_count}"
echo "=========================================="

if (( fail_count > 0 )); then
  exit 1
fi
