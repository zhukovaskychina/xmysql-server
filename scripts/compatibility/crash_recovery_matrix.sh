#!/usr/bin/env bash
set -euo pipefail

report_dir="${1:-reports/compatibility/crash-recovery}"
repeat="${2:-3}"
mkdir -p "$report_dir"
status=PASS
for run in $(seq 1 "$repeat"); do
  if ! go test ./server/innodb/manager -run 'Test(Crash|FullCrash|FaultInjector|Recovery)' -count=1 >"$report_dir/run-${run}.log" 2>&1; then
    status=FAIL
    break
  fi
done
revision=$(git rev-parse HEAD)
cat >"$report_dir/crash-recovery.json" <<EOF
{"generated_at":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","git_revision":"$revision","repeat":$repeat,"status":"$status"}
EOF
echo "$status"
[[ "$status" == PASS ]]
