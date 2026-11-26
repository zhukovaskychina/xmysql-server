"""CONTENT acceptance over ordinary PyMySQL text-protocol parameters."""
import hashlib
import importlib.metadata
import json
import os

import pymysql
from pymysql.constants import FIELD_TYPE


def main():
    host, port = os.environ["XMYSQL_CLIENT_DSN"].rsplit(":", 1)
    connection = pymysql.connect(
        host=host, port=int(port), user=os.environ["XMYSQL_CLIENT_USER"],
        password=os.environ["XMYSQL_CLIENT_PASSWORD"], database="wire_compat",
        autocommit=True, charset="utf8mb4", binary_prefix=True,
        connect_timeout=10, read_timeout=15, write_timeout=15,
    )
    payload = bytes(range(256)) * 512 + bytes(range(37))
    replacement = payload[::-1]
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE external_python_content (id INT PRIMARY KEY, body CONTENT, marker INT)")
            cursor.execute("INSERT INTO external_python_content VALUES (%s, CONTENT_FROM_BYTES(%s, 'application/octet-stream'), %s)", (1, payload, 31415))
            cursor.execute("SELECT body, marker, CONTENT_REVISION(body) FROM external_python_content WHERE id=%s", (1,))
            body, marker, revision = cursor.fetchone()
            assert cursor.description[0][1] == FIELD_TYPE.LONG_BLOB, cursor.description
            assert body == payload, "binary CONTENT bytes changed during parameter binding"
            assert marker == 31415
            reconstructed = bytearray()
            for offset in range(0, len(payload), 4096):
                cursor.execute("SELECT CONTENT_SLICE(body, %s, %s) FROM external_python_content WHERE id=%s AND CONTENT_REVISION(body)=%s", (offset, min(4096, len(payload) - offset), 1, revision))
                row = cursor.fetchone()
                assert row is not None, "source changed during sliced read"
                reconstructed.extend(row[0])
            assert hashlib.sha256(reconstructed).digest() == hashlib.sha256(payload).digest()
            cursor.execute("UPDATE external_python_content SET body=CONTENT_FROM_BYTES(%s, 'application/octet-stream') WHERE id=%s", (replacement, 1))
            cursor.execute("SELECT body, CONTENT_REVISION(body) FROM external_python_content WHERE id=1")
            body, updated_revision = cursor.fetchone()
            assert body == replacement
            assert updated_revision != revision
            connection.begin()
            cursor.execute("UPDATE external_python_content SET body=CONTENT_FROM_BYTES(%s, 'application/octet-stream') WHERE id=%s", (payload, 1))
            connection.rollback()
            cursor.execute("SELECT body, CONTENT_REVISION(body) FROM external_python_content WHERE id=1")
            assert cursor.fetchone() == (replacement, updated_revision), "rollback changed committed CONTENT"
            cursor.execute("DELETE FROM external_python_content WHERE id=%s", (1,))
            cursor.execute("SELECT COUNT(*) FROM external_python_content")
            assert cursor.fetchone()[0] == 0
        print(json.dumps({"client": "PyMySQL", "version": importlib.metadata.version("PyMySQL"), "status": "PASS", "bytes": len(payload), "protocol": "text", "cases": ["binary-crud", "longblob-metadata", "revision-slices", "rollback", "delete"]}))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
