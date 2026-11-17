#!/usr/bin/env bash
# Runs actions-scaleset-stress, a Go tool that stresses the GitHub Actions
# control plane on a real GHES appliance through the official scale-set SDK.
# It supports runner-registration churn and synthetic workflows on
# digest-pinned runner containers. Builds the binary into tmp/bin/ when it is
# missing or older than its sources. See actions-scaleset-stress/README.md.
#
# Usage:
#   ./actions-scaleset-stress.sh <subcommand> [flags]
#
#   check        Verify GHES identity and target repository readiness
#   prepare      Create the private test repository and synthetic workflow
#   run          Execute bounded registrations or workflows load
#   cleanup      Preview or remove this run's journaled resources
#   version      Print version information
#   help         Show subcommands and flags
#
# Environment:
#   ACTIONS_SCALESET_STRESS_BIN  Run this prebuilt binary instead of building
#                                tmp/bin/actions-scaleset-stress
#
# Relative --config and --artifacts-dir paths resolve against the current
# directory, so run from the repository root to use .gh-api-examples.conf and
# the git-ignored tmp/ directory.

set -euo pipefail

# Follow symlinks to the real script without readlink -f, which older macOS
# releases lack.
SCRIPT_PATH="${BASH_SOURCE[0]}"
while [[ -L "${SCRIPT_PATH}" ]]; do
  LINK_DIR="$(cd -P "$(dirname "${SCRIPT_PATH}")" && pwd)"
  SCRIPT_PATH="$(readlink "${SCRIPT_PATH}")"
  [[ "${SCRIPT_PATH}" == /* ]] || SCRIPT_PATH="${LINK_DIR}/${SCRIPT_PATH}"
done
ROOT_DIR="$(cd -P "$(dirname "${SCRIPT_PATH}")" && pwd)"
TOOL_DIR="${ROOT_DIR}/actions-scaleset-stress"
DEFAULT_BIN="${ROOT_DIR}/tmp/bin/actions-scaleset-stress"
BIN="${ACTIONS_SCALESET_STRESS_BIN:-${DEFAULT_BIN}}"

# needs_build succeeds when the default binary is missing or older than any
# build input: a non-test Go file or a module file.
needs_build() {
  local stale=""
  [[ -x "${DEFAULT_BIN}" ]] || return 0
  stale="$(find "${TOOL_DIR}" -maxdepth 1 -type f \
    \( -name '*.go' -o -name 'go.mod' -o -name 'go.sum' \) ! -name '*_test.go' \
    -newer "${DEFAULT_BIN}" -print)"
  if [[ -z "${stale}" && -d "${TOOL_DIR}/internal" ]]; then
    stale="$(find "${TOOL_DIR}/internal" -type f -name '*.go' ! -name '*_test.go' \
      -newer "${DEFAULT_BIN}" -print)"
  fi
  [[ -n "${stale}" ]]
}

build() {
  local tmp_bin="${DEFAULT_BIN}.build.$$"
  if ! command -v go >/dev/null 2>&1; then
    echo "ERROR: go is not installed or not on PATH. Install Go 1.27.3 or later to build" >&2
    echo "       actions-scaleset-stress, or set ACTIONS_SCALESET_STRESS_BIN to a prebuilt binary." >&2
    exit 2
  fi
  mkdir -p "${ROOT_DIR}/tmp/bin"
  echo "Building tmp/bin/actions-scaleset-stress ..." >&2
  # Build under a temporary name and rename it into place so a concurrent
  # invocation never executes a partially written binary.
  if ! (cd "${TOOL_DIR}" && go build -trimpath -o "${tmp_bin}" .) >&2; then
    rm -f "${tmp_bin}"
    echo "ERROR: go build failed in actions-scaleset-stress/; see the messages above." >&2
    exit 2
  fi
  mv -f "${tmp_bin}" "${DEFAULT_BIN}"
}

if [[ -n "${ACTIONS_SCALESET_STRESS_BIN:-}" ]]; then
  if [[ ! -f "${BIN}" || ! -x "${BIN}" ]]; then
    echo "ERROR: ACTIONS_SCALESET_STRESS_BIN does not name an executable file." >&2
    exit 2
  fi
  # A bare file name would make exec search PATH instead of the current directory.
  case "${BIN}" in
    */*) ;;
    *) BIN="./${BIN}" ;;
  esac
else
  if [[ ! -f "${TOOL_DIR}/go.mod" ]]; then
    echo "ERROR: actions-scaleset-stress/go.mod not found next to this script; use a full checkout." >&2
    exit 2
  fi
  if needs_build; then
    build
  fi
fi

exec "${BIN}" "$@"
