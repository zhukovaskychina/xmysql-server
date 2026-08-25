#!/usr/bin/env bash
# XMySQL Server P0-A Stage1 基线检查脚本
# 目标：统一首次入场环境和最小测试指令，减少“我这边能过/你这边过不了”的环境歧义

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

REPORT_ROOT="${STAGE1_REPORT_DIR:-$ROOT_DIR/reports}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$REPORT_ROOT/stage1_baseline_${TIMESTAMP}"
SUMMARY_FILE="$RUN_DIR/summary.log"
export GO_MIN_MINOR="${GO_MIN_MINOR:-24}"

mkdir -p "$RUN_DIR"

require_go() {
  local go_bin
  if ! go_bin=$(command -v "$GO_BIN"); then
    echo "[ERROR] go 未安装或不在 PATH。" | tee -a "$SUMMARY_FILE"
    return 1
  fi

  local version_line
  version_line="$($GO_BIN version 2>/dev/null || true)"
  local ver_num
  if [[ ! "$version_line" =~ go([0-9]+)\.([0-9]+) ]]; then
    echo "[ERROR] 无法解析 go version 输出：$version_line" | tee -a "$SUMMARY_FILE"
    return 1
  fi

  local major="${BASH_REMATCH[1]}"
  local minor="${BASH_REMATCH[2]}"

  {
    echo "go_binary: $GO_BIN"
    echo "go_version_line: $version_line"
    echo "required_min_version: 1.$GO_MIN_MINOR"
  } >>"$SUMMARY_FILE"

  if (( major < 1 || minor < GO_MIN_MINOR )); then
    echo "[ERROR] Go 版本过低：$version_line，当前最低要求 1.$GO_MIN_MINOR+" | tee -a "$SUMMARY_FILE"
    return 1
  fi

  echo "[OK] Go 版本检查通过: go$major.$minor" | tee -a "$SUMMARY_FILE"
}

run_cmd() {
  local cmd="$1"
  local label="$2"
  local ec=0

  echo ""
  echo "=== $label ===" | tee -a "$SUMMARY_FILE"
  echo "command: $cmd" | tee -a "$SUMMARY_FILE"

  if [[ "${STAGE1_DRY_RUN:-0}" == "1" ]]; then
    echo "[SKIP] DRY-RUN enabled" | tee -a "$SUMMARY_FILE"
    return 0
  fi

  local out_file="$RUN_DIR/${label// /_}.log"
  if eval "$cmd" >"$out_file" 2>&1; then
    echo "[PASS] $label" | tee -a "$SUMMARY_FILE"
    echo "log: $out_file" | tee -a "$SUMMARY_FILE"
  else
    ec=$?
    echo "[FAIL] $label (exit=$ec)" | tee -a "$SUMMARY_FILE"
    echo "log: $out_file" | tee -a "$SUMMARY_FILE"
    return $ec
  fi

  return 0
}

{
  echo "# P0 Stage1 Baseline Report"
  echo "repo: $ROOT_DIR"
  echo "timestamp: $TIMESTAMP"
  echo "report_dir: $RUN_DIR"
  echo "dry_run: ${STAGE1_DRY_RUN:-0}"
  echo "go_min_minor: $GO_MIN_MINOR"
} >"$SUMMARY_FILE"

overall=0
if ! require_go; then
  overall=1
fi

run_cmd "$GO_BIN version" "go_version" || overall=1
run_cmd "$GO_BIN test ./server/innodb/engine -run 'TestIndexScanOperator|TestIndexReading|Test.*Duplicate.*'" "engine_focused_baseline" || overall=1
run_cmd "$GO_BIN test ./server/dispatcher" "dispatcher_package" || overall=1
if [[ "${STAGE1_SKIP_FULL_ENGINE:-0}" != "1" ]]; then
  run_cmd "$GO_BIN test ./server/innodb/engine" "engine_package_full" || overall=1
fi

{
  echo ""
  echo "=== final ==="
  if [[ $overall -eq 0 ]]; then
    echo "RESULT: PASS"
  else
    echo "RESULT: FAIL"
  fi
  echo "summary_file=$SUMMARY_FILE"
  echo "run_dir=$RUN_DIR"
} | tee -a "$SUMMARY_FILE"

echo ""
echo "Stage1 report: $SUMMARY_FILE"
exit $overall
