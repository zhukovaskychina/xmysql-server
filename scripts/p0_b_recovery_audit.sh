#!/usr/bin/env bash
# B-02 恢复演练审计脚本
# 按最近演练目录扫描，输出每轮场景结果，便于门禁核对

set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

REPORT_ROOT="${B_AUDIT_REPORT_DIR:-$ROOT_DIR/reports/p0_b_audit}"
CR_PROC_REPORT_DIR="${CR_PROC_REPORT_DIR:-${B_AUD_PROC_REPORT_DIR:-$ROOT_DIR/reports}}"
RUN_DIRS_ARG="${B_AUDIT_RUN_DIRS:-}"
MAX_DIRS="${B_AUDIT_MAX_DIRS:-${B_AUD_MAX_DIRS:-5}}"
EXPECTED_ROUNDS="${B_AUD_EXPECT_ROUNDS:-3}"

mkdir -p "$REPORT_ROOT"

if [[ -z "$RUN_DIRS_ARG" ]]; then
  SELECTED_DIRS=()
  while IFS= read -r d; do
    SELECTED_DIRS+=("$d")
  done < <(ls -dt "$CR_PROC_REPORT_DIR"/crash_recovery_process_drill_* 2>/dev/null | head -n "$MAX_DIRS")
else
  IFS=',' read -r -a SELECTED_DIRS <<<"$RUN_DIRS_ARG"
fi

if (( ${#SELECTED_DIRS[@]} == 0 )); then
  echo "No crash recovery process run directory found under reports/"
  exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="$REPORT_ROOT/p0_recovery_audit_${TS}.md"

scenario_seq() {
  local summary_file="$1"
  local scenario="$2"
  local rounds="${3:-$EXPECTED_ROUNDS}"
  local values=()

  local i
  for ((i = 1; i <= rounds; i++)); do
    if grep -q "^\\[ROUND ${i}\\] \\[SCENARIO\\] ${scenario} result=PASS" "$summary_file"; then
      values+=("r${i}=PASS")
    elif grep -q "^\\[ROUND ${i}\\] \\[SCENARIO\\] ${scenario} result=FAIL" "$summary_file"; then
      values+=("r${i}=FAIL")
    else
      values+=("r${i}=MISSING")
    fi
  done

  printf "%s" "${values[*]}"
}

required_markers() {
  local client_file="$1"
  local scenario="$2"
  local markers
  local i
  local missing=()

  case "$scenario" in
    redo)
      markers=(REDO_SETUP_OK REDO_VERIFY_OK SHOW_TABLES_WHERE_OK SNAPSHOT_ROWS_OK)
      ;;
    undo)
      markers=(SNAPSHOT_ROWS_OK UNDO_VERIFY_OK)
      ;;
    half_commit)
      markers=(SNAPSHOT_ROWS_OK HALF_COMMIT_FINAL_COUNT)
      ;;
    *)
      markers=(SNAPSHOT_ROWS_OK)
      ;;
  esac

  local i
  for marker in "${markers[@]}"; do
    if ! grep -q "$marker" "$client_file" 2>/dev/null; then
      missing+=("$marker")
    fi
  done

  if (( ${#missing[@]} == 0 )); then
    echo "PASS"
  else
    printf "%s" "missing=${missing[*]}"
  fi
}

{
  echo "# XMySQL Recovery Audit"
  echo "time: $TS"
  echo "root: $ROOT_DIR"
  echo
  echo "## 1. Inputs"
  echo "- report_root: $REPORT_ROOT"
  echo "- max_dirs: $MAX_DIRS"
  echo "- expected_rounds: $EXPECTED_ROUNDS"
  echo "- run_dirs:"
  for d in "${SELECTED_DIRS[@]}"; do
    echo "  - $d"
  done
  echo
  echo "## 2. Result Summary"
} >"$OUT_FILE"

echo "| run_dir | result | rounds | redo | undo | half_commit | redo_markers | undo_markers | half_markers |" >>"$OUT_FILE"
echo "| --- | --- | --- | --- | --- | --- | --- | --- | --- |" >>"$OUT_FILE"

for run_dir in "${SELECTED_DIRS[@]}"; do
  run_dir="${run_dir//$'\r'/}"
  summary_file="$run_dir/summary.log"
  client_file="$run_dir/client.log"

  if [[ ! -f "$summary_file" ]]; then
    echo "| ${run_dir} | MISSING | - | - | - | - | summary_missing |" >>"$OUT_FILE"
    continue
  fi

  result="UNKNOWN"
  if grep -q "RESULT: PASS" "$summary_file"; then
    result="PASS"
  elif grep -q "RESULT: FAIL" "$summary_file"; then
    result="FAIL"
  elif grep -q "\[ROUND .*\] \[SCENARIO\].*result=FAIL" "$summary_file"; then
    result="PARTIAL_FAIL"
  else
    result="UNKNOWN"
  fi

  redo_status="$(scenario_seq "$summary_file" redo "$EXPECTED_ROUNDS")"
  undo_status="$(scenario_seq "$summary_file" undo "$EXPECTED_ROUNDS")"
  half_status="$(scenario_seq "$summary_file" half_commit "$EXPECTED_ROUNDS")"

  redo_marker_status="$(required_markers "$client_file" redo)"
  undo_marker_status="$(required_markers "$client_file" undo)"
  half_marker_status="$(required_markers "$client_file" half_commit)"

  round_count="${EXPECTED_ROUNDS}"
  if [[ -s "$summary_file" ]]; then
    max_round=$(awk 'match($0,/^\\[ROUND ([0-9]+)\\] \\[SCENARIO\\]/,a){if(a[1]>m)m=a[1]}END{if(m=="")m=0; print m}' "$summary_file")
    if [[ -n "$max_round" && "$max_round" -gt 0 ]]; then
      round_count="$max_round"
    fi
  fi

  echo "| ${run_dir} | ${result} | ${round_count} | ${redo_status} | ${undo_status} | ${half_status} | ${redo_marker_status} | ${undo_marker_status} | ${half_marker_status} |" >>"$OUT_FILE"
done

{
  echo
  echo "## 3. Compliance checks"
  echo "- For each of redo/undo/half_commit, every expected round should be PASS."
  echo "- Required markers must appear in client.log:"
  echo "  - redo: REDO_SETUP_OK, REDO_VERIFY_OK, SHOW_TABLES_WHERE_OK, SNAPSHOT_ROWS_OK"
  echo "  - undo: SNAPSHOT_ROWS_OK, UNDO_VERIFY_OK"
  echo "  - half_commit: SNAPSHOT_ROWS_OK, HALF_COMMIT_FINAL_COUNT"
  echo "- summary.log and client.log must be present per run."
  echo
  echo "## 4. Review gates"
  echo '- [ ] 连续 N 轮 `redo/undo/half_commit` 均 PASS'
  echo '- [ ] redo/undo/half 的每轮关键 marker 均出现'
  echo '- [ ] 复盘日志保存，`summary.log` 与 `client.log` 可复放'
} >>"$OUT_FILE"

echo "Audit report: $OUT_FILE"
