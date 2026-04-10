#!/usr/bin/env bash
# 崩溃恢复演练：聚合 manager 包内与 crash recovery 相关的测试，输出可归档日志。
# 用法: ./scripts/crash_recovery_drill.sh
# 环境变量: CR_REPORT_DIR 指定报告目录（默认仓库根目录下 reports）

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
OUT_DIR="${CR_REPORT_DIR:-$ROOT/reports}"
mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/crash_recovery_drill_$(date +%Y%m%d_%H%M%S).log"

{
  echo "=== crash recovery drill $(date -Iseconds 2>/dev/null || date) ==="
  echo "repo: $ROOT"
  echo "go: $(go version 2>/dev/null || true)"
  echo
  echo "--- go test ./server/innodb/manager/ -run CrashRecovery -count=1 -timeout=180s -v ---"
  go test -count=1 -timeout=180s ./server/innodb/manager/ -run 'CrashRecovery' -v
} 2>&1 | tee "$REPORT"

echo "Report written: $REPORT"
