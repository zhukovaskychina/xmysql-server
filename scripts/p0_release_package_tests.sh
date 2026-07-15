#!/usr/bin/env bash
# XMySQL Server P0 发布包测试集合

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

GO_BIN="${GO_BIN:=/Users/zhukovasky/sdk/go1.24.3/bin/go}"
REPORT_DIR="${P0_RELEASE_REPORT_DIR:-$ROOT_DIR/reports/p0_release_tests}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$REPORT_DIR/$TIMESTAMP"
SUMMARY_FILE="$RUN_DIR/summary.log"

PACKAGES=(
  ./server/conf
  ./server/dispatcher
  ./server/net
  ./server/protocol
  ./server/innodb/basic
  ./server/innodb/record
  ./server/innodb/manager
  ./server/innodb/engine
)

# 发布测试策略：
# - packages: 只跑固定发布包（默认，适用于当前已知全量门禁未全绿的阶段）
# - full: 尝试先跑 go test ./...，失败时回退到固定包集合并在报告里落失败摘要
TEST_SCOPE="${P0_RELEASE_TEST_SCOPE:-packages}"
# 是否要求 ./... 必须通过。1=必须通过，0=仅回退记录
FULL_TEST_ENFORCED="${P0_RELEASE_ENFORCE_FULL_TEST:-0}"
RETRY_TIMES="${P0_RELEASE_RETRY_TIMES:-0}"

mkdir -p "$RUN_DIR"
{
  echo "# P0 Release Package Test Report"
  echo "generated_at_utc=$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo "go_binary=$GO_BIN"
  echo "go_version=$($GO_BIN version)"
  echo "repo=$ROOT_DIR"
  echo "run_dir=$RUN_DIR"
  echo "timestamp=$TIMESTAMP"
  echo "test_scope=$TEST_SCOPE"
  echo "enforce_full_test=$FULL_TEST_ENFORCED"
  echo "packages=${PACKAGES[*]}"
  echo "retry_times=$RETRY_TIMES"
  echo ""
} >"$SUMMARY_FILE"

if [[ ! -x "$GO_BIN" ]]; then
  echo "[ERROR] 指定 go 工具不存在或不可执行: $GO_BIN" | tee -a "$SUMMARY_FILE"
  exit 1
fi

START_TS=$(date +%s)
TOTAL_PACKAGES=${#PACKAGES[@]}
PASS_PACKAGES=0
FAILED_PACKAGES=()
FAILED_DETAILS=""

run_package() {
  local pkg="$1"
  local safe_pkg out_file
  local attempt=0
  local max_attempts=$((RETRY_TIMES + 1))
  local last_ec=0

  safe_pkg="${pkg//\//_}"
  safe_pkg="${safe_pkg//./}"
  out_file="$RUN_DIR/${safe_pkg}_test.log"

  {
    echo "=== package=$pkg ==="
    echo "command=$GO_BIN test $pkg -count=1"
  } >>"$SUMMARY_FILE"

  while (( attempt < max_attempts )); do
    attempt=$((attempt + 1))
    local start_ts=$(date +%s)
    local duration_ms=0

    if "$GO_BIN" test "$pkg" -count=1 >"$out_file" 2>&1; then
      local end_ts=$(date +%s)
      duration_ms=$(( (end_ts - start_ts) * 1000 ))
      echo "attempt=$attempt" >>"$SUMMARY_FILE"
      echo "duration_ms=$duration_ms" >>"$SUMMARY_FILE"
      echo "result=PASS" >>"$SUMMARY_FILE"
      PASS_PACKAGES=$((PASS_PACKAGES + 1))
      echo "RESULT: PASS" | tee -a "$SUMMARY_FILE"
      return 0
    fi

    local ec=$?
    last_ec=$ec
    local end_ts=$(date +%s)
    duration_ms=$(( (end_ts - start_ts) * 1000 ))

    echo "attempt=$attempt" >>"$SUMMARY_FILE"
    echo "duration_ms=$duration_ms" >>"$SUMMARY_FILE"
    echo "result=FAIL exit=$ec" >>"$SUMMARY_FILE"

    if (( attempt < max_attempts )); then
      echo "[WARN] attempt=$attempt result=FAIL retry_after=1s" | tee -a "$SUMMARY_FILE"
      sleep 1
    fi
  done

  FAILED_PACKAGES+=("$pkg")
  local tail_lines
  tail_lines="$(tail -n 40 "$out_file")"
  FAILED_DETAILS+="$pkg: ${RETRY_TIMES} retry(ies) exhausted, final exit=$last_ec. output=$out_file
"
  {
    echo "FAILURE_DETAILS_BEGIN"
    echo "$tail_lines"
    echo "FAILURE_DETAILS_END"
  } >>"$SUMMARY_FILE"
  echo "RESULT: FAIL exit=$last_ec" | tee -a "$SUMMARY_FILE"
  return "$last_ec"
}

run_package_suite() {
  local overall=0
  for pkg in "${PACKAGES[@]}"; do
    echo "" >>"$SUMMARY_FILE"
    if ! run_package "$pkg"; then
      overall=1
    fi
  done
  return "$overall"
}

run_full_suite() {
  local full_log="$RUN_DIR/full_test.log"
  local start_ts=$(date +%s)
  {
    echo "=== full_scope=go test ./... ==="
    echo "command=$GO_BIN test ./... -count=1"
    echo "start=$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  } >>"$SUMMARY_FILE"

  if "$GO_BIN" test ./... -count=1 >"$full_log" 2>&1; then
    local end_ts=$(date +%s)
    local duration_ms=$(( (end_ts - start_ts) * 1000 ))
    echo "result=PASS" >>"$SUMMARY_FILE"
    echo "duration_ms=$duration_ms" >>"$SUMMARY_FILE"
    echo "RESULT: PASS full=go test ./..." | tee -a "$SUMMARY_FILE"
    return 0
  fi

  local ec=$?
  local end_ts=$(date +%s)
  local duration_ms=$(( (end_ts - start_ts) * 1000 ))
  local fail_tail
  fail_tail="$(tail -n 60 "$full_log")"

  echo "result=FAIL exit=$ec" >>"$SUMMARY_FILE"
  echo "duration_ms=$duration_ms" >>"$SUMMARY_FILE"
  echo "RESULT: FAIL full=go test ./... exit=$ec" | tee -a "$SUMMARY_FILE"
  {
    echo "FAILURE_DETAILS_BEGIN"
    echo "$fail_tail"
    echo "FAILURE_DETAILS_END"
  } >>"$SUMMARY_FILE"
  return "$ec"
}

OVERALL_EXIT=0
if [[ "$TEST_SCOPE" == "full" ]]; then
  echo "scope=full (attempt full suite first)" | tee -a "$SUMMARY_FILE"
  if ! run_full_suite; then
    if [[ "$FULL_TEST_ENFORCED" == "1" ]]; then
      OVERALL_EXIT=1
      echo "[ERROR] go test ./... is mandatory and failed. package scope is skipped by policy." | tee -a "$SUMMARY_FILE"
      echo "full_scope_enforced_failed=1" >>"$SUMMARY_FILE"
    else
      echo "[WARN] full scope failed; continue package scope for release evidence" | tee -a "$SUMMARY_FILE"
      echo "full_scope_fallback=package_set" >>"$SUMMARY_FILE"
      run_package_suite || OVERALL_EXIT=1
    fi
  fi
else
  echo "scope=packages (fixed package set)" | tee -a "$SUMMARY_FILE"
  run_package_suite || OVERALL_EXIT=1
fi

END_TS=$(date +%s)
TOTAL_DURATION_SEC=$((END_TS - START_TS))
{
  echo ""
  echo "=== final ==="
  echo "packages_total=$TOTAL_PACKAGES"
  echo "packages_passed=$PASS_PACKAGES"
  echo "packages_failed=${#FAILED_PACKAGES[@]}"
  if (( ${#FAILED_PACKAGES[@]} > 0 )); then
    echo "failed_packages=${FAILED_PACKAGES[*]}"
  else
    echo "failed_packages="
  fi
  echo "failure_summary=$FAILED_DETAILS"
  echo "total_duration_sec=$TOTAL_DURATION_SEC"
  if [[ "$OVERALL_EXIT" -eq 0 ]]; then
    echo "overall=PASS"
  else
    echo "overall=FAIL"
  fi
} | tee -a "$SUMMARY_FILE"

echo ""
echo "Summary: $SUMMARY_FILE"
exit "$OVERALL_EXIT"
