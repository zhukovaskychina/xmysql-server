#!/usr/bin/env bash
# 崩溃恢复演练（GAP-02 第一阶段增强版）：
# - 分组覆盖 redo / undo / 半提交事务（含故障注入）三类场景
# - 每组单独日志 + 总结报告，便于审计归档
# 用法:
#   ./scripts/crash_recovery_drill.sh
# 环境变量:
#   CR_REPORT_DIR=/path/to/reports
#   CR_PKG=./server/innodb/manager/
#   CR_TIMEOUT=300s

set -u -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${CR_REPORT_DIR:-$ROOT/reports}"
PKG="${CR_PKG:-./server/innodb/manager/}"
TIMEOUT="${CR_TIMEOUT:-300s}"
TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$OUT_DIR/crash_recovery_drill_${TS}"
SUMMARY="$RUN_DIR/summary.log"

mkdir -p "$RUN_DIR"

run_group() {
  local name="$1"
  local pattern="$2"
  local logfile="$RUN_DIR/${name}.log"

  {
    echo "=== [$name] $(date -Iseconds 2>/dev/null || date) ==="
    echo "pattern: $pattern"
    echo "command: go test -count=1 -timeout=${TIMEOUT} ${PKG} -run '${pattern}' -v"
    echo
    go test -count=1 -timeout="${TIMEOUT}" "${PKG}" -run "${pattern}" -v
  } >"$logfile" 2>&1
  local ec=$?

  if [[ $ec -eq 0 ]]; then
    echo "[PASS] $name -> $logfile" | tee -a "$SUMMARY"
  else
    echo "[FAIL] $name -> $logfile" | tee -a "$SUMMARY"
    echo "        (exit_code=$ec)" | tee -a "$SUMMARY"
  fi
  return $ec
}

{
  echo "=== crash recovery drill $(date -Iseconds 2>/dev/null || date) ==="
  echo "repo: $ROOT"
  echo "go: $(go version 2>/dev/null || true)"
  echo "pkg: $PKG"
  echo "timeout: $TIMEOUT"
  echo "run_dir: $RUN_DIR"
  echo
} >"$SUMMARY"

overall=0

# 1) Redo 场景：重放、幂等、LSN/统计
run_group "redo" \
  '^(TestTXN001_|TestRedoReplay|TestRedoLogManager|TestCrashRecoveryRedoPhase|TestRedoLogReplay)$' || overall=1

# 2) Undo 场景：倒序回滚、CLR、版本链、保存点
run_group "undo" \
  '^(TestTXN002_|TestUndoRollbackWithCLR|TestPartialRollback|TestSavepoint_|TestUndoLogManager|TestCrashRecoveryUndoPhase)$' || overall=1

# 3) 半提交/故障注入：写入中断、提交中断、恢复后校验
run_group "half_commit" \
  '^(TestFaultInjection_CrashDuringCommit|TestFaultInjection_CrashDuringWrite|TestFaultInjection_CrashDuringRedo|TestCrashRecoveryThreePhases|TestCrashRecoveryFullCycle|TestFullCrashRecovery)$' || overall=1

{
  echo
  echo "=== aggregate ==="
  grep -hE '^(--- FAIL:|FAIL\t|PASS\t|ok\t)' "$RUN_DIR"/*.log 2>/dev/null || true
  echo
  if [[ $overall -eq 0 ]]; then
    echo "RESULT: PASS"
  else
    echo "RESULT: FAIL"
  fi
} >>"$SUMMARY"

cat "$SUMMARY"
echo
echo "Artifacts:"
echo "  summary: $SUMMARY"
echo "  details: $RUN_DIR/{redo.log,undo.log,half_commit.log}"

exit $overall
