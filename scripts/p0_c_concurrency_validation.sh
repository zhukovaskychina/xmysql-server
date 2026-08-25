#!/usr/bin/env bash
# P0-C 并发正确性验证脚本（最小闭环版）
#
# 功能：
# - 以 round 维度重复执行并发相关测试子集
# - 覆盖冲突写、范围读、长事务、死锁/锁等待场景
# - 输出每轮的压测日志、通过率和一致性检查结果
#
# 运行示例：
#   P0_C_REPORT_ROOT=./reports/p0_c_validation \
#   P0_C_ROUNDS=5 \
#   ./scripts/p0_c_concurrency_validation.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

REPORT_ROOT="${P0_C_REPORT_ROOT:-$ROOT_DIR/reports/p0_c_validation}"
RUN_ID="${P0_C_RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
RUN_DIR="$REPORT_ROOT/p0_concurrency_validation_$RUN_ID"
SUMMARY_FILE="$RUN_DIR/concurrency_validation_report.md"

ROUNDS="${P0_C_ROUNDS:-3}"
TIMEOUT="${P0_C_TIMEOUT:-300s}"
STOP_ON_FAIL="${P0_C_STOP_ON_FAIL:-0}"
CONSISTENCY_PATTERN="${P0_C_CONSISTENCY_PATTERN:-TestP0C_.*Consistency}"
CONSISTENCY_PACKAGE="${P0_C_CONSISTENCY_PACKAGE:-./server/innodb/manager}"

SCENARIO_NAMES=(
  "conflict_write"
  "range_read"
  "long_tx"
  "deadlock_wait"
)

SCENARIO_PACKAGES=(
  "./server/innodb/manager"
  "./server/innodb/storage/store/mvcc ./server/innodb/manager ./server/innodb/engine"
  "./server/innodb/manager ./server/innodb/engine"
  "./server/innodb/storage/store/mvcc ./server/innodb/manager ./server/innodb/engine"
)

SCENARIO_PATTERNS=(
  "TestConcurrentInsert|TestConcurrentRollback|TestConcurrentCheckAndExpand|TestMVCC_ConcurrentTransactions"
  "TestGapLock|TestDeadlock|TestRange|TestSimpleDeadlock|TestComplexDeadlock|TestMVCC_ConcurrentTransactions"
  "TestConcurrentLongTransactionDetection|TestLongTransaction"
  "TestDeadlock|TestLockManager_DeadlockDetection|TestDeadlockScenarios|TestDeadlockDetectionPerformance"
)

SCENARIO_RESULTS=("" "" "" "")
CONSISTENCY_RESULTS=("" "" "" "")
CONSISTENCY_LINES=()

mkdir -p "$RUN_DIR"

run_scenario() {
  local round="$1"
  local index="$2"
  local name="${SCENARIO_NAMES[$index]}"
  local package_list="${SCENARIO_PACKAGES[$index]}"
  local pattern="${SCENARIO_PATTERNS[$index]}"
  local log_dir="$RUN_DIR/round_${round}"
  local status="PASS"

  mkdir -p "$log_dir"

  local scenario_log="$log_dir/${name}.log"
  local scenario_start
  local scenario_end
  local duration
  local test_runs=0
  local fail_tests=0
  local deadlock_hits=0
  local lock_wait_hits=0

  : >"$scenario_log"
  {
    echo "=== P0_C concurrency scenario ==="
    echo "round: $round"
    echo "scenario: $name"
    echo "packages: $package_list"
    echo "pattern: $pattern"
    echo "timeout: $TIMEOUT"
    echo "start: $(date -Iseconds 2>/dev/null || date)"
    echo
  } >>"$scenario_log"

  scenario_start="$(date +%s)"
  for pkg in $package_list; do
    local cmd="$GO_BIN test -count=1 -timeout=$TIMEOUT -run '$pattern' \"$pkg\" -v"
    {
      echo ">>> $cmd"
    } >>"$scenario_log"
    if ! "$GO_BIN" test -count=1 -timeout="$TIMEOUT" -run "$pattern" "$pkg" -v >>"$scenario_log" 2>&1; then
      status="FAIL"
    fi
  done
  scenario_end="$(date +%s)"
  duration=$((scenario_end - scenario_start))

  if [[ -f "$scenario_log" ]]; then
    test_runs="$(grep -c '^=== RUN' "$scenario_log" || true)"
    fail_tests="$(grep -c '^--- FAIL:' "$scenario_log" || true)"
    deadlock_hits="$(grep -ic 'deadlock' "$scenario_log" || true)"
    lock_wait_hits="$(grep -ic 'lock wait' "$scenario_log" || true)"
    if (( fail_tests > 0 )); then
      status="FAIL"
    fi
  fi

  {
    echo "| ${round} | ${name} | ${status} | ${test_runs} | ${fail_tests} | ${duration} | ${deadlock_hits} | ${lock_wait_hits} | ${scenario_log} |"
  } >>"$SUMMARY_FILE"

  if [[ -z "${SCENARIO_RESULTS[$index]}" ]]; then
    SCENARIO_RESULTS[$index]="$status"
  else
    SCENARIO_RESULTS[$index]+=" ${status}"
  fi

  if [[ "$status" == "FAIL" && "$STOP_ON_FAIL" == "1" ]]; then
    return 1
  fi

  return 0
}

run_consistency_suite() {
  local round="$1"
  local log_dir="$RUN_DIR/round_${round}"
  local consistency_log="$log_dir/consistency.log"
  local overall_consistency_status="PASS"
  local marker_status
  local marker

  mkdir -p "$log_dir"

  : >"$consistency_log"
  echo "=== P0_C consistency suite ===" >>"$consistency_log"
  echo "round: $round" >>"$consistency_log"
  echo "pattern: $CONSISTENCY_PATTERN" >>"$consistency_log"
  echo "package: $CONSISTENCY_PACKAGE" >>"$consistency_log"
  echo "start: $(date -Iseconds 2>/dev/null || date)" >>"$consistency_log"
  echo >>"$consistency_log"

  if ! "$GO_BIN" test -count=1 -timeout="$TIMEOUT" -run "$CONSISTENCY_PATTERN" "$CONSISTENCY_PACKAGE" -v >>"$consistency_log" 2>&1; then
    overall_consistency_status="FAIL"
  fi

  for i in "${!SCENARIO_NAMES[@]}"; do
    local name="${SCENARIO_NAMES[$i]}"
    marker="$(grep -aF "CONSISTENCY|${name}|" "$consistency_log" | head -n 1 || true)"

    if [[ -z "$marker" ]]; then
      marker_status="MISSING"
      overall_consistency_status="FAIL"
    elif [[ "$marker" != *"|status=PASS"* ]]; then
      marker_status="FAIL"
      overall_consistency_status="FAIL"
    else
      marker_status="PASS"
    fi

    if [[ -z "${CONSISTENCY_RESULTS[$i]}" ]]; then
      CONSISTENCY_RESULTS[$i]="$marker_status"
    else
      CONSISTENCY_RESULTS[$i]+=" ${marker_status}"
    fi

    CONSISTENCY_LINES+=("| ${round} | ${name} | ${marker_status} | ${marker} |")
  done

  {
    echo "consistency suite result: ${overall_consistency_status}"
    echo
    if [[ "$overall_consistency_status" != "PASS" ]]; then
      echo "missing markers or fail status detected"
    fi
  } >>"$consistency_log"

  if [[ "$overall_consistency_status" == "FAIL" ]]; then
    return 1
  fi
  return 0
}

{
  echo "# XMySQL P0-C 并发验证报告"
  echo "time: $(date +%Y%m%d_%H%M%S)"
  echo "repo: $ROOT_DIR"
  echo "report_root: $REPORT_ROOT"
  echo "rounds: $ROUNDS"
  echo "timeout: $TIMEOUT"
  echo "stop_on_fail: $STOP_ON_FAIL"
  echo
  echo "## 1. 测试矩阵"
  echo "- conflict_write: 插入/回滚/索引分裂并发"
  echo "- range_read: 范围读与 gap lock 相关路径"
  echo "- long_tx: 长事务检测与并发链路"
  echo "- deadlock_wait: 死锁检测与等待路径"
  echo
  echo "## 2. 明细（按轮次）"
  echo "| round | scenario | status | test_runs | fail_tests | duration_s | deadlock_hit | lock_wait_hit | log |"
  echo "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
} >"$SUMMARY_FILE"

overall_fail=0

for ((round = 1; round <= ROUNDS; round++)); do
  for ((i = 0; i < ${#SCENARIO_NAMES[@]}; i++)); do
    if ! run_scenario "$round" "$i"; then
      overall_fail=1
      break
    fi
  done

  if ! run_consistency_suite "$round"; then
    overall_fail=1
    break
  fi

  if ((overall_fail == 1 && STOP_ON_FAIL == 1)); then
    break
  fi
done

{
  echo
  echo "## 3. 场景一致性标记（P0C 标记）"
  echo "| round | scenario | marker_status | marker |"
  echo "| --- | --- | --- | --- |"
} >>"$SUMMARY_FILE"

for line in "${CONSISTENCY_LINES[@]}"; do
  echo "$line" >>"$SUMMARY_FILE"
done

{
  echo
  echo "## 4. 一致性检查"
  echo "- 场景级测试通过率基于 go test 结果; 一致性基于 CONSISTENCY|<scenario> 标记"
} >>"$SUMMARY_FILE"

stable_ok="yes"
scenario_check_lines=()
consistency_check_lines=()

for ((i = 0; i < ${#SCENARIO_NAMES[@]}; i++)); do
  name="${SCENARIO_NAMES[$i]}"
  IFS=' ' read -r -a status_list <<< "${SCENARIO_RESULTS[$i]}"
  base_status="${status_list[0]:-MISSING}"
  scenario_consistent="yes"
  IFS=' ' read -r -a consistency_list <<< "${CONSISTENCY_RESULTS[$i]}"
  consistency_base="${consistency_list[0]:-MISSING}"
  consistency_consistent="yes"

  for s in "${status_list[@]}"; do
    if [[ "$s" != "$base_status" ]]; then
      scenario_consistent="no"
      stable_ok="no"
      break
    fi
  done

  for s in "${consistency_list[@]}"; do
    if [[ "$s" != "$consistency_base" ]]; then
      consistency_consistent="no"
      stable_ok="no"
      break
    fi
  done

  line="- ${name}: baseline=${base_status}，rounds=${SCENARIO_RESULTS[$i]};一致性=${scenario_consistent}"
  scenario_check_lines+=("$line")

  line="  - ${name}: markers_baseline=${consistency_base}，rounds=${CONSISTENCY_RESULTS[$i]}；marker一致性=${consistency_consistent}"
  consistency_check_lines+=("$line")
done

for line in "${scenario_check_lines[@]}"; do
  echo "$line" >>"$SUMMARY_FILE"
done
for line in "${consistency_check_lines[@]}"; do
  echo "$line" >>"$SUMMARY_FILE"
done

if [[ "$stable_ok" == "yes" ]]; then
  echo "- 一致性结论：PASS（同一场景每轮结果一致）" >>"$SUMMARY_FILE"
else
  echo "- 一致性结论：FAIL（存在跨轮结果分歧）" >>"$SUMMARY_FILE"
  overall_fail=1
fi

{
  echo
  echo "## 5. 闭环建议"
  echo "- 若 FAIL：保留失败轮次日志，缩小测试范围后按场景单独复现。"
  echo "- 若 PASS：将本报告路径加入 P0_C 脚本审计闭环。"
  echo "## 6. 审批路径"
  if (( overall_fail == 0 )); then
    echo "- [ ] C-01 冲突写场景通过"
    echo "- [ ] C-01 范围读场景通过"
    echo "- [ ] C-01 长事务场景通过"
    echo "- [ ] C-01 死锁/锁等待可控"
    echo "- [ ] C-01 无脏读/丢写/重复写的连续一致性证据"
    echo "RESULT: PASS"
  else
    echo "- [ ] C-01 需补充修复（先重跑失败场景）"
    echo "RESULT: FAIL"
  fi
  echo "report: $SUMMARY_FILE"
} >>"$SUMMARY_FILE"

exit "$overall_fail"
