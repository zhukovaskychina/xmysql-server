import json
import os
import secrets
import ssl
import sys

try:
    import pymysql
    from pymysql.constants import CLIENT
except ImportError:
    print("PyMySQL is not installed", file=sys.stderr)
    sys.exit(125)

dsn = os.environ["XMYSQL_CLIENT_DSN"]
host, port = dsn.split(":", 1)
ca_file = os.environ.get("XMYSQL_CLIENT_TLS_CA")
def make_connection_args(user, password, database=None, multi_statements=False):
    args = dict(host=host, port=int(port), user=user, password=password, autocommit=True, charset="utf8mb4", connect_timeout=10, read_timeout=10, write_timeout=10)
    if database is not None:
        args["database"] = database
    if multi_statements:
        args["client_flag"] = CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS
    if ca_file:
        tls_context = ssl.create_default_context(cafile=ca_file)
        tls_context.check_hostname = False
        args["ssl"] = tls_context
    return args

connection_args = make_connection_args(os.environ["XMYSQL_CLIENT_USER"], os.environ["XMYSQL_CLIENT_PASSWORD"], database="mysql", multi_statements=True)
connection = pymysql.connect(**connection_args)
COM_RESET_CONNECTION = 31
cases = {}
def passed(name):
    cases[name] = "PASS"

def run_auth_plugin_case():
    password = os.environ.get("XMYSQL_CLIENT_AUTH_PLUGIN_PASSWORD") or secrets.token_hex(24)
    accounts = [
        ("xmysql_cache_client", "caching_sha2_password"),
        ("xmysql_sha_client", "sha256_password"),
    ]
    with connection.cursor() as cursor:
        for user, _ in accounts:
            cursor.execute(f"DROP USER IF EXISTS '{user}'@'%'")
        for user, plugin in accounts:
            cursor.execute(f"CREATE USER '{user}'@'%' IDENTIFIED WITH {plugin} BY '{password}'")
            cursor.execute(f"GRANT SELECT ON *.* TO '{user}'@'%'")
    try:
        for user, _ in accounts:
            plugin_connection = pymysql.connect(**make_connection_args(user, password))
            try:
                with plugin_connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    assert cursor.fetchone()[0] == 1
            finally:
                plugin_connection.close()
        passed("auth-plugins")
    finally:
        with connection.cursor() as cursor:
            for user, _ in accounts:
                cursor.execute(f"DROP USER IF EXISTS '{user}'@'%'")

try:
    if os.environ.get("XMYSQL_CLIENT_AUTH_PLUGINS") == "1":
        run_auth_plugin_case()
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
        passed("connection-auth")
        cursor.execute("CREATE DATABASE IF NOT EXISTS client_matrix")
        cursor.execute("CREATE TABLE IF NOT EXISTS client_matrix.matrix_rows(id INT PRIMARY KEY, label VARCHAR(32))")
        cursor.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (1, %s) ON DUPLICATE KEY UPDATE label=VALUES(label)", ("one",))
        passed("database-ddl-dml")
        cursor.execute("SELECT %s + 1", (41,))
        assert cursor.fetchone()[0] == 42
        passed("prepared-statements")
        connection.begin()
        cursor.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (2, %s)", ("tx",))
        connection.rollback()
        passed("transactions")
        cursor.execute("SELECT NULL, CAST(42 AS SIGNED), _utf8mb4'兼容'")
        row = cursor.fetchone()
        assert row == (None, 42, "兼容"), row
        passed("null-and-types")
        cursor.execute("SELECT _utf8mb4'兼容'")
        assert cursor.fetchone()[0] == "兼容"
        passed("charset")
        cursor.execute("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'")
        assert cursor.fetchone()[0] > 0
        passed("metadata")
        cursor.execute("SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' ORDER BY TABLE_NAME, ORDINAL_POSITION")
        assert [column[0].lower() for column in cursor.description] == ["table_schema", "table_name", "column_name", "ordinal_position"]
        row = cursor.fetchone()
        assert row and row[0] == "client_matrix" and row[1] and row[2] and row[3] >= 1, row
        passed("metadata-shape")
        cursor.execute("CREATE TABLE IF NOT EXISTS client_matrix.auto_rows(id INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(32))")
        cursor.execute("INSERT INTO client_matrix.auto_rows(label) VALUES (%s)", ("python-client",))
        assert cursor.lastrowid > 0 and cursor.rowcount == 1, (cursor.lastrowid, cursor.rowcount)
        passed("auto-increment-and-result-metadata")
        connection.begin()
        cursor.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300001, 'savepoint-before')")
        cursor.execute("SAVEPOINT client_matrix_sp")
        cursor.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300002, 'savepoint-after')")
        cursor.execute("ROLLBACK TO SAVEPOINT client_matrix_sp")
        cursor.execute("RELEASE SAVEPOINT client_matrix_sp")
        connection.commit()
        cursor.execute("SELECT COUNT(*) FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)")
        assert cursor.fetchone()[0] == 1
        cursor.execute("DELETE FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)")
        passed("savepoints")
        cursor.execute("SET @client_matrix_value = 41")
        cursor.execute("SELECT @client_matrix_value + 1")
        assert cursor.fetchone()[0] == 42
        passed("session-state")
        connection.ping(reconnect=False)
        passed("protocol-ping")
        connection.select_db("client_matrix")
        cursor.execute("SELECT DATABASE()")
        assert cursor.fetchone()[0] == "client_matrix"
        passed("protocol-init-db")
        cursor.execute("SET @client_matrix_reset = 41")
        connection._execute_command(COM_RESET_CONNECTION, b"")
        connection._read_ok_packet()
        cursor.execute("SELECT @client_matrix_reset")
        assert cursor.fetchone()[0] is None
        passed("session-reset")
        cursor.execute("SELECT 1 AS first_col; SELECT 2 AS second_col")
        assert cursor.fetchone()[0] == 1
        assert cursor.nextset()
        assert cursor.fetchone()[0] == 2
        passed("multi-result-and-error")
        try:
            cursor.execute("SELECT * FROM table_that_does_not_exist")
            raise AssertionError("missing table query unexpectedly succeeded")
        except pymysql.MySQLError as error:
            assert "table" in str(error).lower(), error
        try:
            cursor.execute("SELECT * FROM client_matrix_missing_table")
            raise AssertionError("missing table query unexpectedly succeeded")
        except pymysql.MySQLError as error:
            assert error.args and error.args[0] == 1146, error.args
        passed("negative-error-code")
        cursor.execute("CREATE TABLE IF NOT EXISTS client_matrix.type_rows(id BIGINT PRIMARY KEY, decimal_value DECIMAL(10,2), double_value DOUBLE, text_value TEXT, blob_value BLOB, created_at TIMESTAMP)")
        cursor.execute("SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' AND TABLE_NAME='type_rows' ORDER BY ORDINAL_POSITION")
        assert [row[0].lower() for row in cursor.fetchall()] == ["bigint", "decimal", "double", "text", "blob", "timestamp"]
        passed("extended-types-metadata")
        cursor.execute("CREATE TABLE IF NOT EXISTS client_matrix.wire_rows(id INT PRIMARY KEY, decimal_value DECIMAL(10,2), double_value DOUBLE, binary_value BLOB, date_value DATE)")
        cursor.execute("DELETE FROM client_matrix.wire_rows WHERE id = 1")
        cursor.execute("INSERT INTO client_matrix.wire_rows(id, decimal_value, double_value, binary_value, date_value) VALUES (1, 12.34, 1.5, _binary'xy', '2026-09-28')")
        cursor.execute("SELECT decimal_value, double_value, binary_value, date_value FROM client_matrix.wire_rows WHERE id = 1")
        row = cursor.fetchone()
        binary_value = row[2].encode() if isinstance(row[2], str) else bytes(row[2])
        assert str(row[0]) == "12.34" and float(row[1]) == 1.5 and binary_value == b"xy" and str(row[3]) == "2026-09-28", row
        passed("wire-value-types")
    pool_first = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], database="mysql", autocommit=True, charset="utf8mb4")
    pool_second = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], database="mysql", autocommit=True, charset="utf8mb4")
    try:
        with pool_first.cursor() as pool_cursor:
            pool_cursor.execute("SELECT 1")
            assert pool_cursor.fetchone()[0] == 1
        with pool_second.cursor() as pool_cursor:
            pool_cursor.execute("SELECT 2")
            assert pool_cursor.fetchone()[0] == 2
    finally:
        pool_first.close()
        pool_second.close()
    passed("multi-session-pool")
    connection.close()
    connection = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], database="mysql", autocommit=True, charset="utf8mb4", client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
    passed("reconnect")
    print(json.dumps({"client": "pymysql", "cases": cases}, ensure_ascii=False))
finally:
    connection.close()
