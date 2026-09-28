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
connection = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], database="mysql", autocommit=True, charset="utf8mb4", client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS)
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
    connection.close()
    connection = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], database="mysql", autocommit=True, charset="utf8mb4", client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
    passed("reconnect")
    print(json.dumps({"client": "pymysql", "cases": cases}, ensure_ascii=False))
finally:
    connection.close()
