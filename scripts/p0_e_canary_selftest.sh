#!/usr/bin/env bash
# P0-07 canary script self-test: verify read/write/restore stage log contract without a live server.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

out="$(
  P0E_MODE=selftest \
  P0E_BACKUP_ROOT="$TMP_DIR/backups" \
  P0E_DATA_DIR="$TMP_DIR/data" \
  P0E_CANARY_DB="p0e_selftest" \
  P0E_CANARY_TABLE="t1" \
  bash "$ROOT_DIR/scripts/p0_e_backup_snapshot.sh"
)"

summary_file="$(printf '%s\n' "$out" | awk -F': ' '/canary_summary:/ {print $2}' | tail -n1)"
if [[ -z "$summary_file" || ! -f "$summary_file" ]]; then
  echo "expected canary_summary file, got output:"
  printf '%s\n' "$out"
  exit 1
fi

stage_log="$(awk -F': ' '/stage_events:/ {print $2}' "$summary_file" | tail -n1)"
if [[ -z "$stage_log" || ! -f "$stage_log" ]]; then
  echo "expected stage_events file in summary"
  cat "$summary_file"
  exit 1
fi

head -n1 "$stage_log" | grep -qx 'stage,schema,table,error_code,log_path,duration_ms'
grep -q '^read,p0e_selftest,t1,OK,' "$stage_log"
grep -q '^write,p0e_selftest,t1,OK,' "$stage_log"
grep -q '^restore,p0e_selftest,t1,OK,' "$stage_log"
grep -q '^result: PASS$' "$summary_file"

echo "PASS: canary selftest stage log contract is valid"
