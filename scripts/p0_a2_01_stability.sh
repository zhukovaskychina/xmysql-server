#!/usr/bin/env bash
# P0 A2-01 稳定性复验脚本：高风险测试 10 轮运行（可配置）

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

REPORT_ROOT="${A2_01_REPORT_DIR:-$ROOT_DIR/reports}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$REPORT_ROOT/p0_a2_01_stability_${TIMESTAMP}"
SUMMARY_FILE="$RUN_DIR/summary.log"

ROUNDS="${A2_01_ROUNDS:-10}"
TIMEOUT="${A2_01_TIMEOUT:-300s}"
STOP_ON_FAIL="${A2_01_STOP_ON_FAIL:-1}"
TEST_FILTER="${A2_01_TEST_FILTER:-TestBTREE|TestConcurrentCheckAndExpand|TestPrefetch}"
if [[ -n "${A2_01_PACKAGES:-}" ]]; then
  IFS=' ' read -r -a PACKAGES <<<"$A2_01_PACKAGES"
else
  PACKAGES=("./server/innodb/manager" "./server/innodb/buffer_pool")
fi

mkdir -p "$RUN_DIR"

echo "P0 A2-01 flaky 测试稳定性复验" >"$SUMMARY_FILE"
echo "repo: $ROOT_DIR" >>"$SUMMARY_FILE"
echo "timestamp: $TIMESTAMP" >>"$SUMMARY_FILE"
echo "rounds: $ROUNDS" >>"$SUMMARY_FILE"
echo "test_filter: $TEST_FILTER" >>"$SUMMARY_FILE"
echo "timeout: $TIMEOUT" >>"$SUMMARY_FILE"
echo "packages: ${PACKAGES[*]}" >>"$SUMMARY_FILE"
echo "run_dir: $RUN_DIR" >>"$SUMMARY_FILE"
echo "" >>"$SUMMARY_FILE"

pass_rounds=0
fail_rounds=0
rounds_with_failure=()

run_round() {
  local round="$1"
  local package="$2"
  local package_label
  package_label="$(echo "$package" | sed 's#^.*/##')"
  local round_log="$RUN_DIR/round_${round}_${package_label}.log"
  local cmd="$GO_BIN test $package -count=1 -timeout=$TIMEOUT -run '$TEST_FILTER'"

  {
    echo "=== round=${round} package=${package} ==="
    echo "cmd: $cmd"
  } >"$round_log"

  if "$GO_BIN" test "$package" -count=1 -timeout="$TIMEOUT" -run "$TEST_FILTER" >>"$round_log" 2>&1; then
    echo "[PASS] round=$round package=$package" >>"$SUMMARY_FILE"
    return 0
  fi

  echo "[FAIL] round=$round package=$package" >>"$SUMMARY_FILE"
  return 1
}

for ((i=1; i<=ROUNDS; i++)); do
  round_fail=0

  for package in "${PACKAGES[@]}"; do
    if ! run_round "$i" "$package"; then
      round_fail=1
      if [[ "$STOP_ON_FAIL" == "1" ]]; then
        break
      fi
    fi
  done

  if [[ "$round_fail" == "1" ]]; then
    fail_rounds=$((fail_rounds + 1))
    rounds_with_failure+=("$i")
    if [[ "$STOP_ON_FAIL" == "1" ]]; then
      break
    fi
  else
    pass_rounds=$((pass_rounds + 1))
  fi
done

{
  echo ""
  echo "=== final ==="
  echo "passed_rounds=$pass_rounds"
  echo "failed_rounds=$fail_rounds"
  if ((${#rounds_with_failure[@]} > 0)); then
    echo "failed_round_indexes=${rounds_with_failure[*]}"
  fi
  if [[ "$fail_rounds" -eq 0 ]]; then
    echo "RESULT: PASS"
  else
    echo "RESULT: FAIL"
  fi
} >>"$SUMMARY_FILE"

exit "$fail_rounds"
