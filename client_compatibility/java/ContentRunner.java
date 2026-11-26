import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.Arrays;

// Run with java -cp <connector-j.jar> ContentRunner.java. The Go wire fixture
// supplies a temporary database and test account through environment variables.
class ContentRunner {
    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static byte[] payload(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) bytes[i] = (byte) (i % 256);
        return bytes;
    }

    static long verify(Connection connection, String table, byte[] expected) throws Exception {
        long revision;
        boolean boundedOnly = expected.length > 1048576;
        try (Statement query = connection.createStatement();
             ResultSet rows = query.executeQuery("SELECT " + (boundedOnly ? "CONTENT_LENGTH(body)" : "body") + ", CONTENT_REVISION(body) FROM " + table + " WHERE id=1")) {
            check(rows.next(), "missing source row");
            if (boundedOnly) {
                check(rows.getLong(1) == expected.length, "streamed CONTENT length mismatch");
            } else {
                check(rows.getMetaData().getColumnType(1) == Types.LONGVARBINARY, "CONTENT must use binary LONGBLOB metadata");
                check(Arrays.equals(expected, rows.getBytes(1)), "binary CONTENT bytes changed");
            }
            revision = rows.getLong(2);
        }
        try (PreparedStatement read = connection.prepareStatement("SELECT CONTENT_SLICE(body, ?, ?) FROM " + table + " WHERE id=? AND CONTENT_REVISION(body)=?")) {
            for (int offset = 0; offset < expected.length; offset += 65536) {
                int length = Math.min(65536, expected.length - offset);
                read.setInt(1, offset); read.setInt(2, length); read.setInt(3, 1); read.setLong(4, revision);
                try (ResultSet rows = read.executeQuery()) {
                    check(rows.next(), "source changed during slice read");
                    check(Arrays.equals(Arrays.copyOfRange(expected, offset, offset + length), rows.getBytes(1)), "CONTENT_SLICE mismatch");
                }
            }
        }
        return revision;
    }

    public static void main(String[] args) throws Exception {
        String driverVersion = "";
        for (boolean serverPrepared : new boolean[]{false, true}) {
            String table = "external_java_" + (serverPrepared ? "server" : "emulated");
            String url = "jdbc:mysql://" + System.getenv("XMYSQL_CLIENT_DSN")
                + "/wire_compat?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"
                + "&connectionTimeZone=UTC&connectTimeout=10000&socketTimeout=15000"
                + "&useServerPrepStmts=" + serverPrepared + "&cachePrepStmts=false&maxAllowedPacket=1048576&blobSendChunkSize=262144";
            try (Connection connection = DriverManager.getConnection(url, System.getenv("XMYSQL_CLIENT_USER"), System.getenv("XMYSQL_CLIENT_PASSWORD"))) {
                driverVersion = connection.getMetaData().getDriverVersion();
                try (Statement ddl = connection.createStatement()) {
                    ddl.executeUpdate("CREATE TABLE " + table + " (id INT PRIMARY KEY, body CONTENT, marker INT)");
                }
                byte[] original = payload(131109);
                byte[] updated = original.clone();
                for (int i = 0; i < updated.length; i++) updated[i] ^= 0x55;
                try (PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table + " VALUES (?, CONTENT_FROM_BYTES(?, 'application/octet-stream'), ?)")) {
                    if (serverPrepared) check(insert.getClass().getSimpleName().equals("ServerPreparedStatement"), "driver silently fell back to client emulation");
                    insert.setInt(1, 1); insert.setBytes(2, original); insert.setInt(3, 31415);
                    insert.executeUpdate();
                }
                long revision = verify(connection, table, original);
                try (PreparedStatement update = connection.prepareStatement("UPDATE " + table + " SET body=CONTENT_FROM_BYTES(?, 'application/octet-stream') WHERE id=?")) {
                    update.setBytes(1, updated); update.setInt(2, 1); update.executeUpdate();
                    long updatedRevision = verify(connection, table, updated);
                    check(updatedRevision != revision, "replacement did not advance revision");
                    connection.setAutoCommit(false);
                    update.setBytes(1, original); update.executeUpdate();
                    connection.rollback(); connection.setAutoCommit(true);
                    check(verify(connection, table, updated) == updatedRevision, "rollback changed committed revision");
                }
                if (serverPrepared) {
                    byte[] streamed = payload(2 * 1024 * 1024 + 37);
                    try (PreparedStatement update = connection.prepareStatement("UPDATE " + table + " SET body=CONTENT_FROM_BYTES(?, 'application/octet-stream'), marker=? WHERE id=1")) {
                        update.setBinaryStream(1, new ByteArrayInputStream(streamed), streamed.length);
                        update.setInt(2, 27182); update.executeUpdate();
                    }
                    verify(connection, table, streamed);
                    try (Statement query = connection.createStatement(); ResultSet rows = query.executeQuery("SELECT marker FROM " + table + " WHERE id=1")) {
                        check(rows.next() && rows.getInt(1) == 27182, "parameter after long data was not decoded");
                    }
                }
                try (PreparedStatement delete = connection.prepareStatement("DELETE FROM " + table + " WHERE id=?")) {
                    delete.setInt(1, 1); delete.executeUpdate();
                }
                try (Statement query = connection.createStatement(); ResultSet rows = query.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    check(rows.next() && rows.getInt(1) == 0, "delete did not remove CONTENT");
                }
            }
        }
        System.out.println("{\"client\":\"Connector/J\",\"version\":\"" + driverVersion + "\",\"status\":\"PASS\",\"protocols\":[\"emulated\",\"server-prepared\"],\"stream_bytes\":2097189}");
    }
}
