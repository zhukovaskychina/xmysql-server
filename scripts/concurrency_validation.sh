#!/usr/bin/env bash
# Command-level P0-C concurrency validation runner.
# Usage: CONCURRENCY_RUNS=3 ./scripts/concurrency_validation.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${CONCURRENCY_REPORT_DIR:-$ROOT/reports}"
RUNS="${CONCURRENCY_RUNS:-1}"
TIMEOUT_SECONDS="${CONCURRENCY_TIMEOUT_SECONDS:-180}"
VERBOSE_FLAG="${CONCURRENCY_VERBOSE_FLAG:-}"

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
  echo "CONCURRENCY_RUNS must be an integer greater than or equal to 1." >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_ID="concurrency_validation_$TIMESTAMP"
STARTED_AT="$(date -Iseconds 2>/dev/null || date)"
LOG_PATH="$OUT_DIR/$RUN_ID.log"
MD_PATH="$OUT_DIR/$RUN_ID.md"
JSON_PATH="$OUT_DIR/$RUN_ID.json"
GO_VERSION="$(go version 2>/dev/null || true)"

SCENARIO_NAMES=(
  "lock_behavior"
  "mvcc_isolation"
  "long_transactions"
  "conflict_and_recovery_concurrency"
  "wrapper_concurrency"
  "storage_mvcc_deadlock"
)

SCENARIO_PACKAGES=(
  "./server/innodb/manager"
  "./server/innodb/manager"
  "./server/innodb/manager"
  "./server/innodb/manager"
  "./server/innodb/storage/wrapper/..."
  "./server/innodb/storage/store/mvcc"
)

SCENARIO_PATTERNS=(
  "TestLockManager_.*|TestGapLock.*|TestNextKeyLock.*|TestLockCompatibilityMatrix|TestExplainLockConflict"
  "TestMVCC_.*|TestVersionChainConcurrency"
  "TestLongTransaction.*|TestConcurrentLongTransactionDetection"
  "TestConcurrentRollback|TestConcurrentInsert|TestCrashRecoveryConcurrentTransactions"
  "Test.*Concurrent.*|Test.*Concurrency.*"
  "TestDeadlockScenarios|TestReadView_MultipleTransactions|TestReadView_TransactionCanSeeOwnChanges"
)

SCENARIO_EXPECTED=(
  "lock compatibility, lock release, gap lock, next-key lock, and deadlock behavior tests pass"
  "MVCC visibility and concurrent transaction tests pass"
  "long transaction detection and concurrent long transaction tests pass"
  "concurrent rollback, insert, and recovery tests pass"
  "wrapper-level concurrency tests pass"
  "storage MVCC deadlock and read-view tests pass"
)

{
  echo "=== concurrency validation $STARTED_AT ==="
  echo "repo: $ROOT"
  echo "go: $GO_VERSION"
  echo "runs: $RUNS"
  echo "timeout: ${TIMEOUT_SECONDS}s"
  echo "evidence_type: concurrency_validation"
  echo
} > "$LOG_PATH"

OVERALL_EXIT_CODE=0
SCENARIOS_JSON=""
SCENARIO_LINES=""

for INDEX in "${!SCENARIO_NAMES[@]}"; do
  NAME="${SCENARIO_NAMES[$INDEX]}"
  PACKAGE="${SCENARIO_PACKAGES[$INDEX]}"
  PATTERN="${SCENARIO_PATTERNS[$INDEX]}"
  EXPECTED="${SCENARIO_EXPECTED[$INDEX]}"
  RUNS_JSON=""
  FAILED_RUNS=0

  for RUN in $(seq 1 "$RUNS"); do
    RUN_STARTED_AT="$(date -Iseconds 2>/dev/null || date)"
    CMD=(go test "$PACKAGE" -run "$PATTERN" -count=1 -timeout="${TIMEOUT_SECONDS}s")
    if [[ -n "$VERBOSE_FLAG" ]]; then
      CMD+=(-v)
    fi

    {
      echo
      echo "=== scenario $NAME run $RUN/$RUNS started at $RUN_STARTED_AT ==="
      echo "--- ${CMD[*]} ---"
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
      FAILED_RUNS=$((FAILED_RUNS + 1))
    fi

    echo "=== scenario $NAME run $RUN/$RUNS finished at $RUN_FINISHED_AT: $RUN_STATUS (exit code $RUN_EXIT_CODE) ===" >> "$LOG_PATH"

    if [[ -n "$RUNS_JSON" ]]; then
      RUNS_JSON="${RUNS_JSON},
"
    fi
    RUNS_JSON="${RUNS_JSON}        {
          \"run\": $RUN,
          \"status\": \"$RUN_STATUS\",
          \"exit_code\": $RUN_EXIT_CODE,
          \"started_at\": \"$RUN_STARTED_AT\",
          \"finished_at\": \"$RUN_FINISHED_AT\"
        }"

    if [[ "$RUN_EXIT_CODE" -ne 0 && "$OVERALL_EXIT_CODE" -eq 0 ]]; then
      OVERALL_EXIT_CODE="$RUN_EXIT_CODE"
    fi
  done

  if [[ "$FAILED_RUNS" -eq 0 ]]; then
    SCENARIO_STATUS="PASS"
  else
    SCENARIO_STATUS="FAIL"
  fi

  SCENARIO_LINES="${SCENARIO_LINES}- $NAME: $SCENARIO_STATUS ($FAILED_RUNS/$RUNS failed runs)
"

  if [[ -n "$SCENARIOS_JSON" ]]; then
    SCENARIOS_JSON="${SCENARIOS_JSON},
"
  fi
  SCENARIOS_JSON="${SCENARIOS_JSON}    {
      \"name\": \"$NAME\",
      \"status\": \"$SCENARIO_STATUS\",
      \"package\": \"$PACKAGE\",
      \"pattern\": \"$PATTERN\",
      \"expected\": \"$EXPECTED\",
      \"actual\": \"go test scenario completed with status $SCENARIO_STATUS\",
      \"metrics\": {
        \"runs\": $RUNS,
        \"failed_runs\": $FAILED_RUNS
      },
      \"runs\": [
$RUNS_JSON
      ]
    }"
done

FINISHED_AT="$(date -Iseconds 2>/dev/null || date)"
if [[ "$OVERALL_EXIT_CODE" -eq 0 ]]; then
  STATUS="PASS"
else
  STATUS="FAIL"
fi

cat > "$JSON_PATH" <<EOF
{
  "generated_at": "$FINISHED_AT",
  "run_id": "$RUN_ID",
  "status": "$STATUS",
  "evidence_type": "concurrency_validation",
  "profile": {
    "runs": $RUNS,
    "timeout_seconds": $TIMEOUT_SECONDS,
    "scenario_count": ${#SCENARIO_NAMES[@]}
  },
  "repository": "$ROOT",
  "go_version": "$GO_VERSION",
  "raw_log": "$LOG_PATH",
  "markdown_report": "$MD_PATH",
  "limitations": [
    "This report records command-level concurrency validation evidence.",
    "It does not yet prove row-level serializability, full isolation semantics, or storage-state diff consistency.",
    "Full P0-C acceptance still requires explicit consistency checks and final regression validation."
  ],
  "scenarios": [
$SCENARIOS_JSON
  ]
}
EOF

cat > "$MD_PATH" <<EOF
# Concurrency Validation Report

## Summary

- Status: $STATUS
- Started at: $STARTED_AT
- Finished at: $FINISHED_AT
- Repository: $ROOT
- Go version: $GO_VERSION
- Runs: $RUNS
- Timeout: ${TIMEOUT_SECONDS}s
- Raw log: \`$LOG_PATH\`
- JSON report: \`$JSON_PATH\`

## Scenario results

$SCENARIO_LINES

## Acceptance note

This is command-level P0-C evidence. It does not replace explicit consistency checks, row-level anomaly detection, or storage-state diff evidence.

## Result

- Exit code: $OVERALL_EXIT_CODE
- Result: $STATUS
EOF

echo "Concurrency validation status: $STATUS"
echo "Raw log written: $LOG_PATH"
echo "Markdown report written: $MD_PATH"
echo "JSON report written: $JSON_PATH"

exit "$OVERALL_EXIT_CODE"
