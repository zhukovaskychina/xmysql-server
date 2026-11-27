package com.xmysql.server.test;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Live acceptance matrix for the sqlparser upgrade. It is disabled in the
 * default Maven build because it requires a separately provisioned test
 * service and an isolated database; the commands are intentionally kept in
 * source so the authorized service run is reproducible.
 */
@Disabled("requires an isolated XMySQL test service")
public class SQLParserUpgradeTest extends BaseIntegrationTest {
    @Test
    void showDdlAndPreparedMatrix() throws Exception {
        executeUpdate("CREATE TABLE sqlparser_upgrade_probe (id INT PRIMARY KEY, value VARCHAR(32))");
        executeUpdate("ALTER TABLE sqlparser_upgrade_probe ADD COLUMN marker INT");
        try (var result = executeQuery("SHOW COLUMNS FROM sqlparser_upgrade_probe")) {
            if (!result.next()) throw new AssertionError("SHOW COLUMNS returned no rows");
        }
        try (var prepared = connection.prepareStatement("SELECT id FROM sqlparser_upgrade_probe WHERE id = ?")) {
            prepared.setInt(1, 1);
            prepared.executeQuery().close();
        }
        executeUpdate("DROP TABLE sqlparser_upgrade_probe");
    }
}
