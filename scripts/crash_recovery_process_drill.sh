#!/usr/bin/env bash
# 真实进程级崩溃恢复演练（GAP-02 第二阶段）：
# - 启动独立实例 -> 构造 redo 场景 -> kill -9 -> 重启 -> 校验
# - undo / 半提交场景使用 manager 包故障注入与回滚测试补齐
# - 产出可审计日志目录
#
# 用法:
#   ./scripts/crash_recovery_process_drill.sh
# 环境变量:
#   CR_PROC_REPORT_DIR=/path/to/reports
#   CR_PROC_PORT=3310
#   CR_PROC_DSN_USER=root
#   CR_PROC_DSN_PASS=root@1234  # 支持包含特殊字符，脚本会进行 URL 编码
#   CR_PROC_SERVER_USER=root      # 写入 my.ini 的用户名，默认与 CR_PROC_DSN_USER 一致
#   CR_PROC_BYPASS_AUTH=false  （如需临时联调可设 true）
#   CR_PROC_DSN_HOST=127.0.0.1

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_BASE="${CR_PROC_REPORT_DIR:-$ROOT/reports}"
PORT="${CR_PROC_PORT:-3310}"
HOST="${CR_PROC_DSN_HOST:-127.0.0.1}"
DSN_USER="${CR_PROC_DSN_USER:-root}"
DSN_PASS_RAW="${CR_PROC_DSN_PASS:-root@1234}"
SERVER_USER="${CR_PROC_SERVER_USER:-$DSN_USER}"
DEV_BYPASS_AUTH="${CR_PROC_BYPASS_AUTH:-false}"
TS="$(date +%Y%m%d_%H%M%S)"
DB_NAME="drill_recovery_db_${TS}"
TABLE_NAME="drill_txn_${TS}"
RUN_DIR="$OUT_BASE/crash_recovery_process_drill_${TS}"
WORK_DIR="$RUN_DIR/workdir"
DATA_DIR="$WORK_DIR/data"
LOG_DIR="$WORK_DIR/logs"
CONF_FILE="$WORK_DIR/drill.ini"
SERVER_LOG="$RUN_DIR/server.log"
SUMMARY="$RUN_DIR/summary.log"
CLIENT_LOG="$RUN_DIR/client.log"
TEST_LOG="$RUN_DIR/manager_tests.log"

mkdir -p "$RUN_DIR" "$DATA_DIR" "$LOG_DIR"

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
  cat >"$CONF_FILE" <<EOF
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
EOF
}

start_server() {
  kill_port_listener
  go run . -configPath="$CONF_FILE" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  echo "server_pid=${SERVER_PID}" >>"$SUMMARY"
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
  local mode="$1"
  shift || true
  echo "[CLIENT] DSN=${DSN} db=${DB_NAME} table=${TABLE_NAME} mode=${mode}" >>"$CLIENT_LOG"
  go run ./cmd/recovery_drill_client -dsn "$DSN" -db "$DB_NAME" -table "$TABLE_NAME" -mode "$mode" "$@" >>"$CLIENT_LOG" 2>&1
}

run_manager_group() {
  local name="$1"
  local pattern="$2"
  {
    echo "=== [${name}] $(date -Iseconds 2>/dev/null || date) ==="
    echo "go test -count=1 -timeout=300s ./server/innodb/manager/ -run '${pattern}' -v"
    go test -count=1 -timeout=300s ./server/innodb/manager/ -run "${pattern}" -v
    echo
  } >>"$TEST_LOG" 2>&1
}

trap 'kill_server; kill_port_listener' EXIT

overall=0

step() {
  local label="$1"
  shift
  echo "[STEP] ${label}" | tee -a "$SUMMARY"
  if "$@"; then
    echo "[OK]   ${label}" | tee -a "$SUMMARY"
  else
    echo "[FAIL] ${label}" | tee -a "$SUMMARY"
    overall=1
  fi
}

{
  echo "=== crash recovery process drill $(date -Iseconds 2>/dev/null || date) ==="
  echo "repo: $ROOT"
  echo "go: $(go version 2>/dev/null || true)"
  echo "run_dir: $RUN_DIR"
  echo "host: $HOST"
  echo "port: $PORT"
  echo "dsn: $DSN"
  echo "db: $DB_NAME"
  echo "table: $TABLE_NAME"
  echo
} >"$SUMMARY"

make_conf
step "cleanup stale listener" kill_port_listener
step "start server" start_server
step "wait server port" wait_port

step "setup redo" run_client setup_redo
step "crash after redo commit" kill_server

step "cleanup stale listener before redo verify restart" kill_port_listener
step "restart server for redo verify" start_server
step "wait server port (redo verify)" wait_port
step "verify redo" run_client verify_redo
step "verify show tables where" run_client verify_show_tables_where

step "undo scenario via manager rollback/fault tests" run_manager_group "undo" '^(TestTXN002_|TestUndoRollbackWithCLR|TestPartialRollback|TestSavepoint_)$'

step "half-commit scenario via fault injection tests" run_manager_group "half_commit" '^(TestFaultInjection_CrashDuringCommit|TestCrashRecoveryThreePhases|TestFullCrashRecovery)$'

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
  echo "manager_test_log=$TEST_LOG"
} | tee -a "$SUMMARY"

exit $overall
