#!/usr/bin/env bash
# GAP-06 安全姿态审计脚本
# 输出：
# - 配置文件中 dev_bypass_password_auth 当前值
# - server/conf 单测最小回归（不依赖全量测试）

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

REPORT_ROOT="${GAP06_REPORT_ROOT:-$ROOT_DIR/reports/p0_gap06}"
RUN_ID="${GAP06_RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
RUN_DIR="$REPORT_ROOT/p0_gap06_${RUN_ID}"
mkdir -p "$RUN_DIR"
SUMMARY="$RUN_DIR/gap06_audit_report.md"

{
  echo "# GAP-06 安全姿态审计"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- repo: ${ROOT_DIR}"
  echo
  echo "## 1. 配置文件快照"
  echo
  for f in conf/default.ini conf/my.ini conf/jdbc_local.ini; do
    echo "### ${f}"
    if [[ -f "$f" ]]; then
      if grep -n "dev_bypass_password_auth" "$f" >/dev/null 2>&1; then
        grep -n "dev_bypass_password_auth" "$f"
      else
        echo "- [MISSING] dev_bypass_password_auth not found"
      fi
      if grep -n "^bind-address" "$f" >/dev/null 2>&1; then
        grep -n "^bind-address" "$f"
      fi
    else
      echo "- [MISSING] ${f}"
    fi
    echo
  done
  echo
  echo "## 2. 代码审计回归"
} > "$SUMMARY"

GO_TEST_CMD="$GO_BIN test ./server/conf -run 'TestShouldRejectDevBypass|TestNormalizeBindAddress_Localhost|TestIsLocalBindAddress' -count=1"
{
  echo "command: ${GO_TEST_CMD}"
} >> "$SUMMARY"

go_test_rc=0
if ${GO_TEST_CMD} >> "$SUMMARY" 2>&1; then
  echo "[PASS] conf package regression tests" | tee -a "$SUMMARY"
else
  echo "[FAIL] conf package regression tests" | tee -a "$SUMMARY"
  go_test_rc=1
fi

{
  echo
  echo "## 3. 审计结论"
  echo "- 关注点：非本地监听下不允许开启 dev_bypass_password_auth"
  echo "- 结论请基于上面 PASS/FAIL 结果与配置快照确认"
  echo "- 如果 command 报错，请先修复 conf 包单测后再复验"
} >> "$SUMMARY"

echo "report: $SUMMARY"
exit "$go_test_rc"
