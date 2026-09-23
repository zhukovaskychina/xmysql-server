import json
import os
import sys

try:
    import pymysql
    from pymysql.constants import CLIENT
except ImportError:
    print("PyMySQL is not installed", file=sys.stderr)
    sys.exit(125)

dsn = os.environ["XMYSQL_CLIENT_DSN"]
host, port = dsn.split(":", 1)
connection = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], autocommit=True, charset="utf8mb4", client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS)
cases = {}
def passed(name):
    cases[name] = "PASS"
try:
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
        cursor.execute("SELECT 1 AS first_value; SELECT 2 AS second_value")
        assert cursor.fetchone()[0] == 1
        assert cursor.nextset()
        assert cursor.fetchone()[0] == 2
        passed("multi-result-and-error")
        try:
            cursor.execute("SELECT * FROM table_that_does_not_exist")
            raise AssertionError("missing table query unexpectedly succeeded")
        except pymysql.MySQLError as error:
            assert "table" in str(error).lower(), error
    connection.close()
    connection = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], autocommit=True, charset="utf8mb4", client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
    passed("reconnect")
    print(json.dumps({"client": "pymysql", "cases": cases}, ensure_ascii=False))
finally:
    connection.close()
