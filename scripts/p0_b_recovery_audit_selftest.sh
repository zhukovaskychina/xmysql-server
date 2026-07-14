#!/usr/bin/env bash
# P0-06 audit self-test: audit must fail when per-scenario evidence files are missing.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

RUN_DIR="$TMP_DIR/crash_recovery_process_drill_fake"
mkdir -p "$RUN_DIR"

cat >"$RUN_DIR/summary.log" <<'SUMMARY_EOF'
[ROUND 1] [SCENARIO] redo result=PASS
[ROUND 1] [SCENARIO] undo result=PASS
[ROUND 1] [SCENARIO] half_commit result=PASS
RESULT: PASS
SUMMARY_EOF

cat >"$RUN_DIR/client.log" <<'CLIENT_EOF'
REDO_SETUP_OK
REDO_VERIFY_OK
SHOW_TABLES_WHERE_OK
SNAPSHOT_ROWS_OK
UNDO_VERIFY_OK
HALF_COMMIT_FINAL_COUNT
CLIENT_EOF

if B_AUDIT_RUN_DIRS="$RUN_DIR" B_AUDIT_EXPECT_ROUNDS=1 B_AUDIT_REPORT_DIR="$TMP_DIR/audit" "$ROOT_DIR/scripts/p0_b_recovery_audit.sh" >/tmp/p0_b_audit_selftest.out 2>/tmp/p0_b_audit_selftest.err; then
  echo "expected audit to fail when evidence files are missing"
  cat /tmp/p0_b_audit_selftest.out
  cat /tmp/p0_b_audit_selftest.err
  exit 1
fi

echo "PASS: audit rejects runs without evidence files"
