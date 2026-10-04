import json
import os
import sys
import time

try:
    import pymysql
except ImportError:
    print("PyMySQL is not installed", file=sys.stderr)
    sys.exit(125)

host, port = os.environ["XMYSQL_RECONNECT_ENDPOINT"].split(":", 1)
user = os.environ["XMYSQL_CLIENT_USER"]
# An empty password is valid for the local dev-bypass fixture. On Windows,
# PowerShell removes an environment variable assigned an empty string, so use
# an empty default instead of treating that fixture as a missing configuration.
password = os.environ.get("XMYSQL_CLIENT_PASSWORD", "")
ready_path = os.environ["XMYSQL_RECONNECT_READY_FILE"]
server_ready_path = os.environ["XMYSQL_RECONNECT_SERVER_READY_FILE"]
timeout = float(os.environ.get("XMYSQL_RECONNECT_TIMEOUT_SECONDS", "30"))

connection = None
try:
    connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database="mysql", autocommit=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
    with open(ready_path, "w", encoding="utf-8") as marker:
        marker.write("ready\n")
    deadline = time.time() + timeout
    while not os.path.exists(server_ready_path):
        if time.time() >= deadline:
            raise RuntimeError("server ready marker did not appear")
        time.sleep(0.1)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        raise RuntimeError("old physical connection unexpectedly succeeded after server restart")
    except Exception as error:
        if isinstance(error, RuntimeError) and "unexpectedly succeeded" in str(error):
            raise
    connection.close()
    connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database="mysql", autocommit=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
    recovery_case = "reconnect-after-network-fault" if os.environ.get("XMYSQL_RECONNECT_FAULT_MODE") == "network" else "reconnect-after-server-restart"
    print(json.dumps({"client": "pymysql", "cases": {"old-connection-failure": "PASS", recovery_case: "PASS"}}))
finally:
    if connection is not None:
        connection.close()
