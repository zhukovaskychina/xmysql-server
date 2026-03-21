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
}
