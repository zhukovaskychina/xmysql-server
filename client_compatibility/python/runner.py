import json
import os
import sys

try:
    import pymysql
except ImportError:
    print("PyMySQL is not installed", file=sys.stderr)
    sys.exit(125)

dsn = os.environ["XMYSQL_CLIENT_DSN"]
host, port = dsn.split(":", 1)
connection = pymysql.connect(host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"], password=os.environ["XMYSQL_CLIENT_PASSWORD"], autocommit=True, charset="utf8mb4")
try:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
        cursor.execute("CREATE DATABASE IF NOT EXISTS client_matrix")
        cursor.execute("CREATE TABLE IF NOT EXISTS client_matrix.matrix_rows(id INT PRIMARY KEY, label VARCHAR(32))")
        cursor.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (1, %s) ON DUPLICATE KEY UPDATE label=VALUES(label)", ("one",))
        cursor.execute("SELECT %s + 1", (41,))
        assert cursor.fetchone()[0] == 42
        connection.begin()
        cursor.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (2, %s)", ("tx",))
        connection.rollback()
        cursor.execute("SELECT NULL, CAST(42 AS SIGNED), _utf8mb4'兼容'")
        row = cursor.fetchone()
        assert row == (None, 42, "兼容"), row
        cursor.execute("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'")
        assert cursor.fetchone()[0] > 0
    print(json.dumps({"client": "pymysql", "status": "PASS"}, ensure_ascii=False))
finally:
    connection.close()
