#!/usr/bin/env bash
# Crash recovery drill for P0-B evidence collection.
# Usage: ./scripts/crash_recovery_drill.sh
# Environment: CR_REPORT_DIR overrides the default reports directory.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${CR_REPORT_DIR:-$ROOT/reports}"
RUN_PATTERN="${CR_RUN_PATTERN:-TestTXN001|TestCrashRecovery|TestRedo|TestUndoRollback|TestSavepoint}"
TIMEOUT_SECONDS="${CR_TIMEOUT_SECONDS:-180}"
VERBOSE_FLAG="${CR_VERBOSE_FLAG:-}"
RUNS="${CR_RUNS:-1}"
STATE_EVIDENCE="${CR_STATE_EVIDENCE:-0}"

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
  echo "CR_RUNS must be an integer greater than or equal to 1." >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
STARTED_AT="$(date -Iseconds 2>/dev/null || date)"
LOG_PATH="$OUT_DIR/crash_recovery_drill_$TIMESTAMP.log"
MD_PATH="$OUT_DIR/crash_recovery_drill_$TIMESTAMP.md"
STATE_PATH="$OUT_DIR/crash_recovery_drill_$TIMESTAMP.state.json"
PACKAGE="./server/innodb/manager"

GO_VERSION="$(go version 2>/dev/null || true)"
CMD=(go test "$PACKAGE" -run "$RUN_PATTERN" -count=1 -timeout="${TIMEOUT_SECONDS}s")
if [[ -n "$VERBOSE_FLAG" ]]; then
  CMD+=(-v)
fi

{
  echo "=== crash recovery drill $STARTED_AT ==="
  echo "repo: $ROOT"
  echo "go: $GO_VERSION"
  echo "package: $PACKAGE"
  echo "pattern: $RUN_PATTERN"
  echo "timeout: ${TIMEOUT_SECONDS}s"
  echo "runs: $RUNS"
  echo
  echo "--- ${CMD[*]} ---"
} > "$LOG_PATH"

EXIT_CODE=0
RUN_RESULTS=""
STATE_RUNS_JSON=""

for RUN in $(seq 1 "$RUNS"); do
  RUN_STARTED_AT="$(date -Iseconds 2>/dev/null || date)"
  {
    echo
    echo "=== replay run $RUN/$RUNS started at $RUN_STARTED_AT ==="
  } >> "$LOG_PATH"

  set +e
  "${CMD[@]}" 2>&1 | tee -a "$LOG_PATH"
  RUN_EXIT_CODE=${PIPESTATUS[0]}
  set -e

  RUN_FINISHED_AT="$(date -Iseconds 2>/dev/null || date)"
  if [[ "$RUN_EXIT_CODE" -eq 0 ]]; then
    RUN_STATUS="PASS"
  else
    RUN_STATUS="FAIL"
  fi

  echo "=== replay run $RUN/$RUNS finished at $RUN_FINISHED_AT: $RUN_STATUS (exit code $RUN_EXIT_CODE) ===" >> "$LOG_PATH"
  RUN_RESULTS="${RUN_RESULTS}- Run $RUN: $RUN_STATUS (exit code $RUN_EXIT_CODE, started $RUN_STARTED_AT, finished $RUN_FINISHED_AT)
"
  if [[ -n "$STATE_RUNS_JSON" ]]; then
    STATE_RUNS_JSON="${STATE_RUNS_JSON},
"
  fi
  STATE_RUNS_JSON="${STATE_RUNS_JSON}    {
      \"run\": $RUN,
      \"status\": \"$RUN_STATUS\",
      \"exit_code\": $RUN_EXIT_CODE,
      \"started_at\": \"$RUN_STARTED_AT\",
      \"finished_at\": \"$RUN_FINISHED_AT\",
      \"checks\": [
        {
          \"name\": \"transaction_lifecycle_baseline\",
          \"expected\": \"focused transaction lifecycle tests pass during replay\",
          \"actual\": \"go test exit code $RUN_EXIT_CODE\",
          \"status\": \"$RUN_STATUS\"
        },
        {
          \"name\": \"crash_recovery_command_replay\",
          \"expected\": \"focused crash recovery tests pass during replay\",
          \"actual\": \"go test exit code $RUN_EXIT_CODE\",
          \"status\": \"$RUN_STATUS\"
        },
        {
          \"name\": \"redo_undo_savepoint_command_replay\",
          \"expected\": \"focused redo, undo, and savepoint tests pass during replay\",
          \"actual\": \"go test exit code $RUN_EXIT_CODE\",
          \"status\": \"$RUN_STATUS\"
        }
      ]
    }"

  if [[ "$RUN_EXIT_CODE" -ne 0 && "$EXIT_CODE" -eq 0 ]]; then
    EXIT_CODE="$RUN_EXIT_CODE"
  fi
done

FINISHED_AT="$(date -Iseconds 2>/dev/null || date)"
if [[ "$EXIT_CODE" -eq 0 ]]; then
  STATUS="PASS"
else
  STATUS="FAIL"
fi

if [[ "$STATE_EVIDENCE" == "1" || "$STATE_EVIDENCE" == "true" || "$STATE_EVIDENCE" == "TRUE" ]]; then
  STATE_EVIDENCE_LINE="- State evidence: \`$STATE_PATH\`"
  cat > "$STATE_PATH" <<EOF
{
  "generated_at": "$FINISHED_AT",
  "run_id": "crash_recovery_drill_$TIMESTAMP",
  "status": "$STATUS",
  "evidence_type": "command_replay",
  "package": "$PACKAGE",
  "test_pattern": "$RUN_PATTERN",
  "timeout_seconds": $TIMEOUT_SECONDS,
  "raw_log": "$LOG_PATH",
  "markdown_report": "$MD_PATH",
  "limitations": [
    "This file records command-level replay evidence.",
    "It does not yet prove page-level, row-level, or WAL-level snapshot/state diff consistency.",
    "Full P0-B acceptance still requires storage-state snapshot or state-diff evidence."
  ],
  "runs": [
$STATE_RUNS_JSON
  ]
}
EOF
else
  STATE_EVIDENCE_LINE="- State evidence: not requested"
fi

cat > "$MD_PATH" <<EOF
# Crash Recovery Drill Report

## Summary

- Status: $STATUS
- Started at: $STARTED_AT
- Finished at: $FINISHED_AT
- Repository: $ROOT
- Go version: $GO_VERSION
- Package: $PACKAGE
- Test pattern: \`$RUN_PATTERN\`
- Timeout: ${TIMEOUT_SECONDS}s
- Runs: $RUNS
- Raw log: \`$LOG_PATH\`
$STATE_EVIDENCE_LINE

## Command

\`\`\`bash
${CMD[*]}
\`\`\`

## Replay results

$RUN_RESULTS

## Scenario coverage represented by this drill

- Transaction lifecycle baseline: \`TestTXN001\`
- Crash recovery tests: \`TestCrashRecovery*\`
- Redo recovery tests: \`TestRedo*\`
- Undo rollback tests: \`TestUndoRollback*\`
- Savepoint recovery behavior: \`TestSavepoint*\`

## Acceptance note

This report is P0-B crash-recovery evidence, but it is not the full production acceptance package by itself.

Remaining evidence required before P0-B can be marked fully accepted:

- snapshot or state-diff evidence,
- scenario matrix for redo, undo, half-commit, and consistency checks,
- final regression gate using \`go test ./...\` after recovery-related code changes.

## Result

- Exit code: $EXIT_CODE
- Result: $STATUS
EOF

echo "Crash recovery drill status: $STATUS"
echo "Raw log written: $LOG_PATH"
echo "Markdown report written: $MD_PATH"

exit "$EXIT_CODE"
