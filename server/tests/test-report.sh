#!/usr/bin/env bash
set -euo pipefail

if (( $# != 0 )); then
    printf 'Usage: %s\n' "$0" >&2
    exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
report_dir=$(mktemp -d)
trap 'rm -rf -- "$report_dir"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

printf 'Running regular tests…\n' >&2
regular_status=0
"$script_dir/test.sh" >"$report_dir/regular.log" 2>&1 || regular_status=$?

printf 'Running performance tests…\n' >&2
performance_status=0
"$script_dir/test.sh" --performance >"$report_dir/performance.log" 2>&1 || performance_status=$?

if ! python3 -m venv --without-pip "$report_dir/venv"; then
    cat -- "$report_dir/regular.log" "$report_dir/performance.log" >&2
    exit 1
fi

"$report_dir/venv/bin/python" "$script_dir/test-report.py" \
    "$report_dir/regular.log" "$regular_status" \
    "$report_dir/performance.log" "$performance_status"
