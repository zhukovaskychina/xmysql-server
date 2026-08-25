#!/usr/bin/env bash
# P0-E-02 数据恢复与回滚辅助脚本
# 目标：为全链路演练提供可复用的备份点与恢复点管理能力

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
GO_BIN="${P0E_GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

MODE="${P0E_MODE:-backup}"
DATA_DIR_DEFAULT="${P0E_DATA_DIR:-$ROOT_DIR/data}"
BACKUP_ROOT="${P0E_BACKUP_ROOT:-$ROOT_DIR/reports/p0_e_backups}"
TARGET_DIR="${P0E_RESTORE_DIR:-$ROOT_DIR/data_restored}"
SNAPSHOT_TAG="${P0E_SNAPSHOT_TAG:-manual}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
CANARY_DB="${P0E_CANARY_DB:-p0e_canary_${TIMESTAMP}}"
CANARY_TABLE="${P0E_CANARY_TABLE:-t1}"
CANARY_SCHEMA="${P0E_CANARY_SCHEMA:-$CANARY_DB}"
CANARY_DSN="${P0E_DSN:-root:root%401234@tcp(127.0.0.1:3309)/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true}"
CANARY_WORK_DIR="$BACKUP_ROOT/canary_${TIMESTAMP}"
CANARY_STAGE_LOG=""
CANARY_BACKUP_FILE=""
CANARY_CLIENT_BIN=""

usage() {
  cat <<'USAGE'
Usage:
  MODE=backup  P0E_MODE=backup [P0E_DATA_DIR=./data] [P0E_BACKUP_ROOT=./reports/p0_e_backups] bash scripts/p0_e_backup_snapshot.sh
  MODE=restore P0E_MODE=restore P0E_BACKUP_FILE=<backup.tar.gz> [P0E_RESTORE_DIR=./data_restored]
  MODE=list    bash scripts/p0_e_backup_snapshot.sh
  MODE=canary  P0E_MODE=canary [P0E_DSN=root:...@tcp(127.0.0.1:3309)/mysql] bash scripts/p0_e_backup_snapshot.sh
  MODE=selftest P0E_MODE=selftest bash scripts/p0_e_backup_snapshot.sh

Environment:
  P0E_MODE: backup | restore | list | canary | selftest
  P0E_DATA_DIR: source data directory for backup
  P0E_BACKUP_ROOT: directory to store backups
  P0E_RESTORE_DIR: restore output dir for restore mode
  P0E_SNAPSHOT_TAG: snapshot tag used in backup file name
  P0E_BACKUP_FILE: full path to backup file used in restore mode
  P0E_DSN: DSN used by canary read/write stage
  P0E_CANARY_DB: database name for canary write stage
  P0E_CANARY_TABLE: table name for canary write stage
  P0E_CANARY_SCHEMA: schema name used in stage logs
  P0E_GO_BIN: Go binary path for canary stage client build
USAGE
}

mkdir -p "$BACKUP_ROOT"

log_stage_event() {
  local stage="$1"
  local schema="$2"
  local table="$3"
  local error_code="$4"
  local log_path="$5"
  local duration_ms="$6"

  printf '%s,%s,%s,%s,%s,%s\n' \
    "$stage" "$schema" "$table" "$error_code" "$log_path" "$duration_ms" \
    >>"$CANARY_STAGE_LOG"
}

run_stage() {
  local stage="$1"
  local schema="$2"
  local table="$3"
  local stage_log="$CANARY_WORK_DIR/stage_${stage}.log"
  shift 3

  local start_ns
  local end_ns
  local duration_ms
  local rc=0
  local error_code="OK"

  start_ns="$(date +%s%N)"
  if "$@" >"$stage_log" 2>&1; then
    rc=0
  else
    rc=$?
    error_code="ERR_${rc}"
  fi
  end_ns="$(date +%s%N)"
  duration_ms=$(( (end_ns - start_ns) / 1000000 ))
  log_stage_event "$stage" "$schema" "$table" "$error_code" "$stage_log" "$duration_ms"

  if [[ "$rc" -ne 0 ]]; then
    return "$rc"
  fi
}

build_canary_client() {
  CANARY_CLIENT_BIN="$CANARY_WORK_DIR/recovery_drill_client"
  "$GO_BIN" build -o "$CANARY_CLIENT_BIN" ./cmd/recovery_drill_client
}

run_canary_read_stage() {
  "$CANARY_CLIENT_BIN" -dsn "$CANARY_DSN" -mode ping
}

run_canary_write_stage() {
  local mode_common=("-dsn" "$CANARY_DSN" "-db" "$CANARY_DB" "-table" "$CANARY_TABLE")

  "$CANARY_CLIENT_BIN" "${mode_common[@]}" -mode setup_redo
  "$CANARY_CLIENT_BIN" "${mode_common[@]}" -mode snapshot
  "$CANARY_CLIENT_BIN" "${mode_common[@]}" -mode verify_redo
}

do_backup() {
  local snapshot_tag="${1:-$SNAPSHOT_TAG}"
  local backup_ts="$(date +%Y%m%d_%H%M%S)"

  if [[ ! -d "$DATA_DIR_DEFAULT" ]]; then
    echo "ERROR: data dir not found: $DATA_DIR_DEFAULT"
    return 1
  fi

  local commit_hash
  commit_hash="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  local snapshot_file="$BACKUP_ROOT/xmysql-backup-${snapshot_tag}-${backup_ts}.tar.gz"
  local manifest_file="$BACKUP_ROOT/xmysql-backup-${snapshot_tag}-${backup_ts}.manifest.json"
  CANARY_BACKUP_FILE="$snapshot_file"

  tar -czf "$snapshot_file" -C "$ROOT_DIR" "${DATA_DIR_DEFAULT#$ROOT_DIR/}"

  {
    echo '{'
    echo "  \"snapshot_tag\": \"${snapshot_tag}\"," 
    echo "  \"timestamp\": \"${backup_ts}\"," 
    echo "  \"git_commit\": \"${commit_hash}\"," 
    echo "  \"root_dir\": \"${ROOT_DIR}\"," 
    echo "  \"data_dir\": \"${DATA_DIR_DEFAULT}\"," 
    echo "  \"backup_file\": \"${snapshot_file}\""
    echo '}'
  } > "$manifest_file"

  echo "backup_file: $snapshot_file"
  echo "manifest: $manifest_file"
}

do_restore() {
  local backup_file="${1:-${P0E_BACKUP_FILE:-}}"
  local restore_dir="${2:-$TARGET_DIR}"

  if [[ -z "$backup_file" ]]; then
    echo "ERROR: restore mode requires P0E_BACKUP_FILE"
    usage
    return 1
  fi
  if [[ ! -f "$backup_file" ]]; then
    echo "ERROR: backup file not found: $backup_file"
    return 1
  fi

  mkdir -p "$restore_dir"
  rm -rf "${restore_dir:?}/"
  mkdir -p "$restore_dir"

  tar -xzf "$backup_file" -C "$restore_dir"
  echo "restored_to: $restore_dir"
}

run_canary() {
  mkdir -p "$CANARY_WORK_DIR"
  local summary_file="$CANARY_WORK_DIR/summary.md"
  CANARY_STAGE_LOG="$CANARY_WORK_DIR/stage_events.log"
  : >"$CANARY_STAGE_LOG"

  echo "# P0-E Canary rehearsal" > "$summary_file"
  echo "work_dir: $CANARY_WORK_DIR" >> "$summary_file"

  local overall=0
  echo "stage,schema,table,error_code,log_path,duration_ms" >"$CANARY_STAGE_LOG"
  {
    build_canary_client
  } || {
    echo "- build failure" >> "$summary_file"
    return 1
  }

  if ! do_backup "canary"; then
    echo "- backup failure" >> "$summary_file"
    return 1
  fi

  CANARY_RESTORE_DIR="${P0E_RESTORE_DIR:-$CANARY_WORK_DIR/restore}"

  if ! run_stage "read" "$CANARY_SCHEMA" "$CANARY_TABLE" run_canary_read_stage; then
    overall=1
    echo "- read stage failed" >> "$summary_file"
  else
    echo "- read stage passed" >> "$summary_file"
  fi

  if ! run_stage "write" "$CANARY_SCHEMA" "$CANARY_TABLE" run_canary_write_stage; then
    overall=1
    echo "- write stage failed" >> "$summary_file"
  else
    echo "- write stage passed" >> "$summary_file"
  fi

  if ! run_stage "restore" "$CANARY_SCHEMA" "$CANARY_TABLE" do_restore "$CANARY_BACKUP_FILE" "$CANARY_RESTORE_DIR"; then
    overall=1
    echo "- restore stage failed" >> "$summary_file"
  else
    echo "- restore stage passed" >> "$summary_file"
  fi

  echo "canary_db: $CANARY_DB" >> "$summary_file"
  echo "canary_table: $CANARY_TABLE" >> "$summary_file"
  echo "schema: $CANARY_SCHEMA" >> "$summary_file"
  echo "stage_events: $CANARY_STAGE_LOG" >> "$summary_file"
  echo "backup_file: $CANARY_BACKUP_FILE" >> "$summary_file"
  echo "restore_dir: $CANARY_RESTORE_DIR" >> "$summary_file"

  if [[ "$overall" -eq 0 ]]; then
    echo "result: PASS" >> "$summary_file"
  else
    echo "result: FAIL" >> "$summary_file"
  fi

  echo "canary_summary: $summary_file"

  return "$overall"
}

fake_canary_stage() {
  local label="$1"
  echo "${label}_OK"
}

run_canary_selftest() {
  mkdir -p "$CANARY_WORK_DIR"
  local summary_file="$CANARY_WORK_DIR/summary.md"
  CANARY_STAGE_LOG="$CANARY_WORK_DIR/stage_events.log"
  echo "stage,schema,table,error_code,log_path,duration_ms" >"$CANARY_STAGE_LOG"

  echo "# P0-E Canary rehearsal selftest" > "$summary_file"
  echo "work_dir: $CANARY_WORK_DIR" >> "$summary_file"

  local overall=0
  if ! run_stage "read" "$CANARY_SCHEMA" "$CANARY_TABLE" fake_canary_stage "READ"; then
    overall=1
    echo "- read stage failed" >> "$summary_file"
  else
    echo "- read stage passed" >> "$summary_file"
  fi

  if ! run_stage "write" "$CANARY_SCHEMA" "$CANARY_TABLE" fake_canary_stage "WRITE"; then
    overall=1
    echo "- write stage failed" >> "$summary_file"
  else
    echo "- write stage passed" >> "$summary_file"
  fi

  if ! run_stage "restore" "$CANARY_SCHEMA" "$CANARY_TABLE" fake_canary_stage "RESTORE"; then
    overall=1
    echo "- restore stage failed" >> "$summary_file"
  else
    echo "- restore stage passed" >> "$summary_file"
  fi

  echo "canary_db: $CANARY_DB" >> "$summary_file"
  echo "canary_table: $CANARY_TABLE" >> "$summary_file"
  echo "schema: $CANARY_SCHEMA" >> "$summary_file"
  echo "stage_events: $CANARY_STAGE_LOG" >> "$summary_file"

  if [[ "$overall" -eq 0 ]]; then
    echo "result: PASS" >> "$summary_file"
  else
    echo "result: FAIL" >> "$summary_file"
  fi

  echo "canary_summary: $summary_file"
  return "$overall"
}

case "$MODE" in
  backup)
    do_backup "$SNAPSHOT_TAG"
    ;;

  restore)
    do_restore
    ;;

  list)
    echo "# P0-E backup snapshots"
    ls -1t "$BACKUP_ROOT"/xmysql-backup-*.tar.gz 2>/dev/null || true
    ;;

  canary)
    run_canary
    ;;

  selftest)
    run_canary_selftest
    ;;

  *)
    usage
    exit 1
    ;;
esac
