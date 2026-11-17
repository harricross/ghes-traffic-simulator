#!/usr/bin/env bash
# Build a reproducible source package and its SHA-256 checksum.
set -euo pipefail
cd "$(dirname "$0")/.."

version="${1:?usage: build.sh <version>}"
if [[ ! "$version" =~ ^[A-Za-z0-9._-]{1,64}$ ]]; then
  echo "invalid version" >&2
  exit 2
fi

rm -rf dist
mkdir dist
name="loadlab-${version}.tar.gz"
tar --sort=name --mtime='2000-01-01 00:00:00Z' --owner=0 --group=0 --numeric-owner \
  --exclude='__pycache__' -cf - src config | gzip -n > "dist/$name"
(cd dist && sha256sum "$name" > "$name.sha256")
printf 'Built dist/%s (%s bytes)\n' "$name" "$(stat -c %s "dist/$name")"
