#!/usr/bin/env bash
# 真实进程级崩溃恢复演练（GAP-02 第二阶段）
# 每轮覆盖 redo / undo / 半提交，按 start -> crash -> restart -> verify 执行。

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"
PYTHON_BIN="${PYTHON_BIN:-python3.12}"

OUT_BASE="${CR_PROC_REPORT_DIR:-$ROOT/reports}"
PORT="${CR_PROC_PORT:-3310}"
HOST="${CR_PROC_DSN_HOST:-127.0.0.1}"
DSN_USER="${CR_PROC_DSN_USER:-root}"
DSN_PASS_RAW="${CR_PROC_DSN_PASS:-root@1234}"
SERVER_USER="${CR_PROC_SERVER_USER:-$DSN_USER}"
DEV_BYPASS_AUTH="${CR_PROC_BYPASS_AUTH:-false}"
ROUNDS="${CR_PROC_ROUNDS:-3}"
UNDO_HOLD_SECONDS="${CR_PROC_UNDO_HOLD_SECONDS:-3}"
HALF_SLEEP_SECONDS="${CR_PROC_HALF_SLEEP_SECONDS:-1}"

TS="$(date +%Y%m%d_%H%M%S)"
DB_NAME_BASE="drill_recovery_db_${TS}"
TABLE_NAME_BASE="drill_txn_${TS}"
RUN_DIR="$OUT_BASE/crash_recovery_process_drill_${TS}"
WORK_DIR="$RUN_DIR/workdir"
DATA_DIR="$WORK_DIR/data"
LOG_DIR="$WORK_DIR/logs"
CONF_FILE="$WORK_DIR/drill.ini"
SERVER_LOG="$RUN_DIR/server.log"
SUMMARY="$RUN_DIR/summary.log"
CLIENT_LOG="$RUN_DIR/client.log"
CLIENT_BIN="$RUN_DIR/recovery_drill_client"
EVIDENCE_DIR="$RUN_DIR/evidence"

mkdir -p "$RUN_DIR" "$DATA_DIR" "$LOG_DIR" "$EVIDENCE_DIR"

url_encode() {
  local value="$1"
  value="${value//%/%25}"
  value="${value//@/%40}"
  value="${value//:/%3A}"
  value="${value//\//%2F}"
  value="${value//\?/%3F}"
  value="${value//&/%26}"
  value="${value//=/%3D}"
  value="${value//\+/%2B}"
  echo "$value"
}

DSN_PASS_ENCODED="$(url_encode "$DSN_PASS_RAW")"
if [[ -n "$DSN_PASS_ENCODED" ]]; then
  DSN="${DSN_USER}:${DSN_PASS_ENCODED}@tcp(${HOST}:${PORT})/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true"
else
  DSN="${DSN_USER}@tcp(${HOST}:${PORT})/mysql?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true"
fi

make_conf() {
  cat >"$CONF_FILE" <<CONF_EOF
[mysqld]
user = ${SERVER_USER}
bind-address = 127.0.0.1
port = ${PORT}
basedir = ${ROOT}
datadir = ${DATA_DIR}
tmpdir = /tmp
lc-messages-dir = /usr/share/mysql
max_session_number = 1000
session_timeout = 60s
fail_fast_timeout = 5s
dev_bypass_password_auth = ${DEV_BYPASS_AUTH}

[innodb]
data_dir = ${DATA_DIR}
data_file_path = ibdata1:100M:autoextend
buffer_pool_size = 134217728
page_size = 16384
redo_log_dir = redo
undo_log_dir = undo
log_file_size = 50331648
log_buffer_size = 16777216
flush_log_at_trx_commit = 1
file_format = Barracuda
default_row_format = DYNAMIC
doublewrite = 1
adaptive_hash_index = 1

[session]
compress_encoding = false
tcp_no_delay = true
tcp_keep_alive = true
keep_alive_period = 180s
tcp_r_buf_size = 262144
tcp_w_buf_size = 65536
pkg_rq_size = 1024
pkg_wq_size = 1024
tcp_read_timeout = 1s
tcp_write_timeout = 5s
wait_timeout = 7s
max_msg_len = 16777216
session_name = xmysql-server

[logs]
log_error = ${LOG_DIR}/error.log
log_infos = ${LOG_DIR}/mysql.log
log_level = info
CONF_EOF
}

start_server() {
  kill_port_listener
  "$GO_BIN" run . -configPath="$CONF_FILE" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  echo "[SERVER] start pid=${SERVER_PID}" >>"$SUMMARY"
}

wait_port() {
  local n=0
  while [[ $n -lt 60 ]]; do
    if (echo >"/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1; then
      return 0
    fi
    n=$((n + 1))
    sleep 1
  done
  return 1
}

wait_port_free() {
  local n=0
  while [[ $n -lt 30 ]]; do
    if ! (echo >"/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1; then
      return 0
    fi
    n=$((n + 1))
    sleep 1
  done
  return 1
}

wait_client_ready() {
  local max_attempts="${CR_PROC_CLIENT_READY_RETRIES:-20}"
  local attempt=0
  local rc

  while [[ $attempt -lt "$max_attempts" ]]; do
    "$CLIENT_BIN" -dsn "$DSN" -db mysql -mode ping >/dev/null 2>&1
    rc=$?
    if [[ $rc -eq 0 ]]; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  return 1
}

kill_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill -9 "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  wait_port_free || true
}

kill_port_listener() {
  local pids
  pids="$(lsof -tiTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$pids" ]]; then
    for pid in $pids; do
      kill -9 "$pid" >/dev/null 2>&1 || true
    done
    wait_port_free || true
  fi
}

run_client() {
  local db_name="$1"
  local table_name="$2"
  local mode="$3"
  shift 3
  local rc=0

  local capture_file="${RUN_CLIENT_CAPTURE_FILE:-}"
  local tmp_out
  tmp_out="$(mktemp "$RUN_DIR/client_${mode}_XXXXXX.log")"

  echo "[CLIENT] mode=${mode} db=${db_name} table=${table_name} args=$*" >>"$CLIENT_LOG"
  "$CLIENT_BIN" -dsn "$DSN" -db "$db_name" -table "$table_name" -mode "$mode" "$@" >"$tmp_out" 2>&1
  rc=$?
  cat "$tmp_out" >>"$CLIENT_LOG"
  if [[ -n "$capture_file" ]]; then
    mkdir -p "$(dirname "$capture_file")"
    cp "$tmp_out" "$capture_file"
  fi
  rm -f "$tmp_out"
  return $rc
}

write_evidence_json() {
  local raw_file="$1"
  local json_file="$2"
  local round="$3"
  local scenario="$4"
  local phase="$5"
  local db_name="$6"
  local table_name="$7"

  mkdir -p "$(dirname "$json_file")"
  "$PYTHON_BIN" - "$raw_file" "$json_file" "$round" "$scenario" "$phase" "$db_name" "$table_name" "$SUMMARY" "$CLIENT_LOG" <<'PY_EOF'
import json
import pathlib
import sys
from datetime import datetime, timezone

raw_file, json_file, round_id, scenario, phase, db_name, table_name, summary, client_log = sys.argv[1:]
raw = pathlib.Path(raw_file).read_text(errors="replace") if pathlib.Path(raw_file).exists() else ""
payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "round": round_id,
    "scenario": scenario,
    "phase": phase,
    "database": db_name,
    "table": table_name,
    "summary_log": summary,
    "client_log": client_log,
    "raw_output": raw,
}
pathlib.Path(json_file).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
PY_EOF
}

write_diff_json() {
  local round="$1"
  local scenario="$2"
  local dir="$EVIDENCE_DIR/round_${round}/${scenario}"
  local before="$dir/before.json"
  local after="$dir/after.json"
  local diff="$dir/diff.json"

  "$PYTHON_BIN" - "$before" "$after" "$diff" "$round" "$scenario" <<'PY_EOF'
import json
import pathlib
import sys
from datetime import datetime, timezone

before_file, after_file, diff_file, round_id, scenario = sys.argv[1:]
before = json.loads(pathlib.Path(before_file).read_text()) if pathlib.Path(before_file).exists() else {}
after = json.loads(pathlib.Path(after_file).read_text()) if pathlib.Path(after_file).exists() else {}
before_raw = before.get("raw_output", "")
after_raw = after.get("raw_output", "")
payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "round": round_id,
    "scenario": scenario,
    "before_file": before_file,
    "after_file": after_file,
    "raw_equal": before_raw == after_raw,
    "before_bytes": len(before_raw.encode()),
    "after_bytes": len(after_raw.encode()),
}
pathlib.Path(diff_file).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
PY_EOF
}

capture_client() {
  local round="$1"
  local scenario="$2"
  local phase="$3"
  local db_name="$4"
  local table_name="$5"
  local mode="$6"
  shift 6

  local dir="$EVIDENCE_DIR/round_${round}/${scenario}"
  local raw_file="$dir/${phase}.raw.log"
  local json_file="$dir/${phase}.json"
  local rc=0

  RUN_CLIENT_CAPTURE_FILE="$raw_file" run_client "$db_name" "$table_name" "$mode" "$@"
  rc=$?
  write_evidence_json "$raw_file" "$json_file" "$round" "$scenario" "$phase" "$db_name" "$table_name"
  return $rc
}

run_client_bg() {
  local db_name="$1"
  local table_name="$2"
  local mode="$3"
  shift 3

  "$CLIENT_BIN" -dsn "$DSN" -db "$db_name" -table "$table_name" -mode "$mode" "$@" >>"$CLIENT_LOG" 2>&1 &
  echo "$!"
}

wait_client() {
  local pid="$1"
  local timeout_seconds="${2:-120}"
  local elapsed=0

  while [[ $elapsed -lt "$timeout_seconds" ]]; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      wait "$pid"
      return $?
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  kill -9 "$pid" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  return 1
}

step() {
  local round="$1"
  local scenario="$2"
  local label="$3"
  shift 3

  local start_ts end_ts duration_ms rc
  start_ts="$(date +%s)"
  echo "[ROUND ${round}] [STEP] scenario=${scenario} label=${label}" >>"$SUMMARY"
  "$@"
  rc=$?
  end_ts="$(date +%s)"
  duration_ms=$(( (end_ts - start_ts) * 1000 ))
  if [[ $rc -eq 0 ]]; then
    echo "[ROUND ${round}] [PASS] scenario=${scenario} label=${label} duration_ms=${duration_ms}" >>"$SUMMARY"
  else
    echo "[ROUND ${round}] [FAIL] scenario=${scenario} label=${label} duration_ms=${duration_ms}" >>"$SUMMARY"
    overall=1
  fi
  return $rc
}

scenario_round_result() {
  local round="$1"
  local scenario="$2"
  local result="$3"
  echo "[ROUND ${round}] [SCENARIO] ${scenario} result=${result}" >>"$SUMMARY"
}

run_redo_round() {
  local round="$1"
  local db_name="$2"
  local table_name="$3"
  local round_ok=0

  step "$round" "redo" "start server" start_server || round_ok=1
  step "$round" "redo" "wait server" wait_port || round_ok=1
  step "$round" "redo" "wait client ready" wait_client_ready || round_ok=1
  step "$round" "redo" "setup redo" run_client "$db_name" "$table_name" setup_redo || round_ok=1
  step "$round" "redo" "snapshot before crash" capture_client "$round" "redo" "before" "$db_name" "$table_name" snapshot || round_ok=1
  step "$round" "redo" "kill server after setup" kill_server || round_ok=1
  step "$round" "redo" "restart for redo verify" start_server || round_ok=1
  step "$round" "redo" "wait server after restart" wait_port || round_ok=1
  step "$round" "redo" "snapshot after restart" capture_client "$round" "redo" "after" "$db_name" "$table_name" snapshot || round_ok=1
  step "$round" "redo" "write diff evidence" write_diff_json "$round" "redo" || round_ok=1
  step "$round" "redo" "verify redo" run_client "$db_name" "$table_name" verify_redo || round_ok=1
  step "$round" "redo" "verify show tables where" run_client "$db_name" "$table_name" verify_show_tables_where || round_ok=1

  if [[ $round_ok -eq 0 ]]; then
    scenario_round_result "$round" "redo" "PASS"
  else
    scenario_round_result "$round" "redo" "FAIL"
  fi
  return $round_ok
}

run_undo_round() {
  local round="$1"
  local db_name="$2"
  local table_name="$3"
  local round_ok=0

  step "$round" "undo" "snapshot before undo" capture_client "$round" "undo" "before" "$db_name" "$table_name" snapshot || round_ok=1
  scenario_round_result "$round" "undo" "START"

  local client_pid
  client_pid="$(run_client_bg "$db_name" "$table_name" hold_undo -hold-seconds "$UNDO_HOLD_SECONDS")"
  sleep 1
  step "$round" "undo" "kill server while tx open" kill_server || round_ok=1

  local undo_client_rc=0
  if wait_client "$client_pid" "${CR_PROC_CLIENT_TIMEOUT:-120}"; then
    undo_client_rc=0
  else
    undo_client_rc=$?
  fi
  echo "[ROUND ${round}] [CLIENT] undo pid=${client_pid} exit=${undo_client_rc}" >>"$SUMMARY"

  step "$round" "undo" "restart for undo verify" start_server || round_ok=1
  step "$round" "undo" "wait server after restart" wait_port || round_ok=1
  step "$round" "undo" "snapshot after restart" capture_client "$round" "undo" "after" "$db_name" "$table_name" snapshot || round_ok=1
  step "$round" "undo" "write diff evidence" write_diff_json "$round" "undo" || round_ok=1
  step "$round" "undo" "verify undo" run_client "$db_name" "$table_name" verify_undo || round_ok=1

  if [[ $round_ok -eq 0 ]]; then
    scenario_round_result "$round" "undo" "PASS"
  else
    scenario_round_result "$round" "undo" "FAIL"
  fi
  return $round_ok
}

run_half_round() {
  local round="$1"
  local db_name="$2"
  local table_name="$3"
  local round_ok=0

  step "$round" "half_commit" "snapshot before half-commit" capture_client "$round" "half_commit" "before" "$db_name" "$table_name" snapshot || round_ok=1
  scenario_round_result "$round" "half_commit" "START"

  local client_pid
  client_pid="$(run_client_bg "$db_name" "$table_name" race_commit)"
  sleep "$HALF_SLEEP_SECONDS"
  step "$round" "half_commit" "kill server during commit" kill_server || round_ok=1

  local half_client_rc=0
  if wait_client "$client_pid" "${CR_PROC_CLIENT_TIMEOUT:-120}"; then
    half_client_rc=0
  else
    half_client_rc=$?
  fi
  echo "[ROUND ${round}] [CLIENT] half_commit pid=${client_pid} exit=${half_client_rc}" >>"$SUMMARY"

  step "$round" "half_commit" "restart for half-commit verify" start_server || round_ok=1
  step "$round" "half_commit" "wait server after restart" wait_port || round_ok=1
  step "$round" "half_commit" "snapshot after restart" capture_client "$round" "half_commit" "after" "$db_name" "$table_name" snapshot || round_ok=1
  step "$round" "half_commit" "write diff evidence" write_diff_json "$round" "half_commit" || round_ok=1
  step "$round" "half_commit" "verify half-commit" run_client "$db_name" "$table_name" verify_half_commit || round_ok=1

  if [[ $round_ok -eq 0 ]]; then
    scenario_round_result "$round" "half_commit" "PASS"
  else
    scenario_round_result "$round" "half_commit" "FAIL"
  fi
  return $round_ok
}

run_round() {
  local round="$1"
  local db_name="$2"
  local table_name="$3"

  echo "[ROUND ${round}] [START] db=${db_name} table=${table_name}" >>"$SUMMARY"

  run_redo_round "$round" "$db_name" "$table_name" || return 1
  run_undo_round "$round" "$db_name" "$table_name" || return 1
  run_half_round "$round" "$db_name" "$table_name" || return 1

  echo "[ROUND ${round}] [ROUND_RESULT] PASS" >>"$SUMMARY"
  return 0
}

trap 'kill_server; kill_port_listener' EXIT

overall=0
{
  echo "=== crash recovery process drill $(date -Iseconds 2>/dev/null || date) ==="
  echo "repo: $ROOT"
  echo "go: $($GO_BIN version 2>/dev/null || true)"
  echo "run_dir: $RUN_DIR"
  echo "rounds: $ROUNDS"
  echo "host: $HOST"
  echo "port: $PORT"
  echo "dsn: $DSN"
  echo "db_base: $DB_NAME_BASE"
  echo "table_base: $TABLE_NAME_BASE"
  echo "undo_hold_seconds: $UNDO_HOLD_SECONDS"
  echo "half_sleep_seconds: $HALF_SLEEP_SECONDS"
  echo "client_bin: $CLIENT_BIN"
  echo
} >"$SUMMARY"

make_conf
if ! "$GO_BIN" build -o "$CLIENT_BIN" ./cmd/recovery_drill_client; then
  echo "build client failed" | tee -a "$SUMMARY" >/dev/stderr
  exit 1
fi

step "global" "global" "cleanup stale listener" kill_port_listener || true

round=1
while [[ $round -le "$ROUNDS" ]]; do
  db_name="${DB_NAME_BASE}_r${round}"
  table_name="${TABLE_NAME_BASE}_r${round}"
  if ! run_round "$round" "$db_name" "$table_name"; then
    echo "[ROUND ${round}] [ROUND_RESULT] FAIL" >>"$SUMMARY"
    overall=1
  fi
  round=$((round + 1))
done

{
  echo
  if [[ $overall -eq 0 ]]; then
    echo "RESULT: PASS"
  else
    echo "RESULT: FAIL"
  fi
  echo "summary=$SUMMARY"
  echo "server_log=$SERVER_LOG"
  echo "client_log=$CLIENT_LOG"
} | tee -a "$SUMMARY"

exit $overall
