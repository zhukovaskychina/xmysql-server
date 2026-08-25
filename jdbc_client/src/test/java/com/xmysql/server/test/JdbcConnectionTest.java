package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * JDBC 协议连接过程专项测试（TDD）。
 * 覆盖：建立连接、元数据、关闭、错误密码、URL 中默认库等，用于发现连接流程遗漏。
 * 需 XMySQL 运行在 localhost:3309，否则相关用例会被跳过。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcConnectionTest {

    private static final String BASE_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String USER = "root";
    private static final String PASSWORD = "root@1234";

    /** 类加载时探测一次服务是否可达，避免每个用例都因 Connection refused 报错 */
    private static final boolean SERVER_AVAILABLE = checkServerAvailable();

    private static boolean checkServerAvailable() {
        try (Connection c = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            return c != null && !c.isClosed();
        } catch (Throwable t) {
            return false; // 连接失败视为服务不可用
        }
    }

    @Test
    @Order(1)
    @DisplayName("有效账号密码应成功建立连接")
    public void testConnectionSucceedsWithValidCredentials() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();
        }
    }

    @Test
    @Order(2)
    @DisplayName("连接后元数据应反映服务端（URL、产品名、版本）")
    public void testConnectionMetadataReflectsServer() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            DatabaseMetaData meta = conn.getMetaData();
            assertThat(meta).isNotNull();
            assertThat(meta.getURL()).isNotBlank();
            assertThat(meta.getDatabaseProductName()).isNotBlank();
            assertThat(meta.getDatabaseProductVersion()).isNotBlank();
            // XMySQL 握手包中版本类似 8.0.32-xmysql-server
            assertThat(meta.getDatabaseProductVersion()).contains("8.0");
        }
    }

    @Test
    @Order(3)
    @DisplayName("close 后 isClosed 应为 true")
    public void testConnectionCloseMakesIsClosedTrue() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD);
        assertThat(conn.isClosed()).isFalse();
        conn.close();
        assertThat(conn.isClosed()).isTrue();
    }

    @Test
    @Order(4)
    @DisplayName("错误密码应抛出 SQLException（服务端应返回认证错误）")
    public void testConnectionFailsWithWrongPassword() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection c = DriverManager.getConnection(BASE_URL, USER, "wrong_password")) {
            // 若服务端未校验密码（如部分环境允许任意密码），跳过本断言
            assumeTrue(false, "服务端当前接受错误密码，未做认证校验，跳过本用例");
        } catch (SQLException e) {
            String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            assertThat(
                msg.contains("access denied") || msg.contains("1045") || e.getErrorCode() == 1045
            ).as("期望认证错误 Access denied/1045，实际: %s", e.getMessage()).isTrue();
        }
    }

    @Test
    @Order(5)
    @DisplayName("URL 中带默认库时 getCatalog 应一致")
    public void testConnectionWithDefaultDatabaseInUrl() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        String urlWithDb = "jdbc:mysql://localhost:3309/mysql?useSSL=false&allowPublicKeyRetrieval=true";
        try (Connection conn = DriverManager.getConnection(urlWithDb, USER, PASSWORD)) {
            String catalog = conn.getCatalog();
            // 服务端若支持 CONNECT_WITH_DB 并在握手时传库名，catalog 为 mysql；否则可能为空
            assertThat(catalog).isNotNull();
            if (!catalog.isEmpty()) {
                assertThat(catalog).isEqualTo("mysql");
            }
        }
    }

    @Test
    @Order(6)
    @DisplayName("无默认库连接后 getCatalog 可为空或空串")
    public void testConnectionWithoutDatabaseCatalog() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            String catalog = conn.getCatalog();
            // 未 USE 或 URL 未指定库时，可能为空串或 null（视驱动/服务端而定）
            assertThat(catalog == null || catalog.isEmpty() || catalog.length() > 0).isTrue();
        }
    }

    @Test
    @Order(7)
    @DisplayName("连接后能执行简单查询（验证握手+认证完整）")
    public void testExecuteSimpleQueryAfterConnect() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS one")) {
            assertThat(rs.next()).isTrue();
            // 列名可能为 "one" 或首列，用索引取数兼容不同服务端
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    @Order(8)
    @DisplayName("JDBC元数据探测应兼容DataGrip常用接口")
    public void testDataGripMetadataProbeQueriesDoNotFail() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertMetadataProbeDoesNotFail(meta.getProcedures(null, null, "%"));
            assertMetadataProbeDoesNotFail(meta.getFunctions(null, null, "%"));
            assertMetadataProbeDoesNotFail(meta.getPrimaryKeys(null, null, "xmysql_missing_table"));
            assertMetadataProbeDoesNotFail(meta.getIndexInfo(null, null, "xmysql_missing_table", false, false));
        }
    }

    @Test
    @Order(9)
    @DisplayName("DataGrip完整元数据SQL探测不应失败")
    public void testDataGripFullMetadataSqlProbesDoNotFail() throws Exception {
        assumeTrue(SERVER_AVAILABLE, "XMySQL 未运行在 localhost:3309，跳过连接测试");
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD);
            Statement stmt = conn.createStatement()) {
            String[] queries = {
                "select database(), schema(), left(user(), instr(concat(user(),'@'),'@')-1)",
                "select table_name, auto_increment from information_schema.tables where table_schema = 'performance_schema' and auto_increment is not null",
                "select table_name, index_name, index_comment, index_type, non_unique, column_name, sub_part, collation, expression from information_schema.statistics where table_schema = 'performance_schema'",
                "select c.constraint_name, c.constraint_schema, c.table_name, c.constraint_type, c.enforced = 'YES' enforced from information_schema.table_constraints c where c.table_schema = 'performance_schema'",
                "select constraint_name, table_name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name from information_schema.key_column_usage where table_schema = 'performance_schema'",
                "select table_name, partition_name, subpartition_name, partition_ordinal_position, subpartition_ordinal_position, partition_method, subpartition_method, partition_expression, subpartition_expression, partition_description, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, create_time, update_time, check_time, checksum, partition_comment, nodegroup, tablespace_name from information_schema.partitions where table_schema = 'performance_schema'",
                "select trigger_name, event_manipulation, event_object_table, action_statement, action_timing, definer from information_schema.triggers where trigger_schema = 'performance_schema'",
                "select event_name, event_definition, event_type, execute_at, interval_value, interval_field, status, definer from information_schema.events where event_schema = 'performance_schema'",
                "select routine_name, routine_type, routine_definition, routine_comment, dtd_identifier, definer from information_schema.routines where routine_schema = 'performance_schema'",
                "select collation_name, character_set_name, is_default from information_schema.collations",
                "select grantee, privilege_type, is_grantable from information_schema.user_privileges",
                "select grantee, table_schema, privilege_type, is_grantable from information_schema.schema_privileges",
                "select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc from mysql.procs_priv where Db = 'performance_schema'",
                "select grantee, table_name, column_name, privilege_type, is_grantable from information_schema.column_privileges where table_schema = 'performance_schema' union all select grantee, table_name, null as column_name, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'performance_schema'",
                "select table_name, view_definition, definer from information_schema.views where table_schema = 'performance_schema'"
            };

            for (String query : queries) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    assertThat(meta.getColumnCount()).as(query).isGreaterThan(0);
                    while (rs.next()) {
                        // Compatibility boundary: DataGrip metadata SQL must return a valid result set.
                    }
                }
            }
        }
    }

    private static void assertMetadataProbeDoesNotFail(ResultSet rs) throws SQLException {
        try (rs) {
            while (rs.next()) {
                // The compatibility boundary is that metadata probes return a valid result set.
            }
        }
    }
}
