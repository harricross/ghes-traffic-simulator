#!/usr/bin/env bash
# Repository checks for the synthetic CI workflow. Problems are reported as
# workflow annotations so they appear on the job's check run.
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
problem() {
  printf '::error file=%s,title=Lint::%s\n' "$1" "$2"
  status=1
}

mapfile -t python_files < <(find src tests -name '*.py' | sort)
mapfile -t shell_files < <(find scripts -name '*.sh' | sort)
mapfile -t json_files < <(find config -name '*.json' | sort)

python3 -c 'import ast, sys
for name in sys.argv[1:]:
    with open(name, encoding="utf-8") as source:
        ast.parse(source.read(), name)' "${python_files[@]}"

for file in "${shell_files[@]}"; do
  bash -n "$file" || problem "$file" "shell syntax error"
done
for file in "${json_files[@]}"; do
  jq empty "$file" || problem "$file" "invalid JSON"
done
for file in "${python_files[@]}" "${shell_files[@]}" "${json_files[@]}"; do
  if grep -q '[[:space:]]$' "$file"; then
    problem "$file" "trailing whitespace"
  fi
done
for file in "${python_files[@]}"; do
  if grep -q $'\t' "$file"; then
    problem "$file" "tab indentation"
  fi
done

checked=$(( ${#python_files[@]} + ${#shell_files[@]} + ${#json_files[@]} ))
printf '::notice title=Lint summary::%d files checked\n' "$checked"
exit "$status"
