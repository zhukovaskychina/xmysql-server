package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.*;

/**
 * 集成测试基类
 * 提供数据库连接管理和通用测试工具方法
 */
public abstract class BaseIntegrationTest {
    
    // 数据库连接配置
    protected static final String BASE_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
    protected static final String USER = "root";
    protected static final String PASSWORD = "root@1234";
    
    protected static Connection connection;
    
    /**
     * 在所有测试之前执行一次
     */
    @BeforeAll
    public static void setUpClass() throws Exception {
        // 加载 JDBC 驱动（Connector/J 8.x 使用 com.mysql.cj.jdbc.Driver；JDBC 4+ 也可省略，会自动加载）
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 建立数据库连接
        connection = DriverManager.getConnection(BASE_URL, USER, PASSWORD);
        System.out.println("✅ 数据库连接已建立");
    }
    
    /**
     * 在所有测试之后执行一次
     */
    @AfterAll
    public static void tearDownClass() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            System.out.println("✅ 数据库连接已关闭");
        }
    }
    
    /**
     * 在每个测试之前执行
     */
    @BeforeEach
    public void setUp() throws Exception {
        // 子类可以重写此方法进行特定的初始化
    }
    
    /**
     * 在每个测试之后执行
     */
    @AfterEach
    public void tearDown() throws Exception {
        // 子类可以重写此方法进行特定的清理
    }
    
    /**
     * 执行SQL更新语句（INSERT, UPDATE, DELETE, DDL等）
     */
    protected int executeUpdate(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }
    
    /**
     * 执行SQL查询语句
     */
    protected ResultSet executeQuery(String sql) throws SQLException {
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(sql);
    }
    
    /**
     * 检查表是否存在
     */
    protected boolean tableExists(String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }
    
    /**
     * 检查数据库是否存在
     */
    protected boolean databaseExists(String databaseName) throws SQLException {
        try (ResultSet rs = executeQuery("SHOW DATABASES LIKE '" + databaseName + "'")) {
            return rs.next();
        }
    }
    
    /**
     * 获取表中的行数
     */
    protected int getTableRowCount(String tableName) throws SQLException {
        try (ResultSet rs = executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
    
    /**
     * 清空表数据
     */
    protected void truncateTable(String tableName) throws SQLException {
        executeUpdate("TRUNCATE TABLE " + tableName);
    }
    
    /**
     * 删除表（如果存在）
     */
    protected void dropTableIfExists(String tableName) throws SQLException {
        executeUpdate("DROP TABLE IF EXISTS " + tableName);
    }
    
    /**
     * 删除数据库（如果存在）
     */
    protected void dropDatabaseIfExists(String databaseName) throws SQLException {
        executeUpdate("DROP DATABASE IF EXISTS " + databaseName);
    }
    
    /**
     * 创建测试数据库
     */
    protected void createTestDatabase(String databaseName) throws SQLException {
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + databaseName);
    }
    
    /**
     * 使用指定数据库
     */
    protected void useDatabase(String databaseName) throws SQLException {
        executeUpdate("USE " + databaseName);
    }
    
    /**
     * 打印测试信息
     */
    protected void printTestInfo(String message) {
        System.out.println("ℹ️  " + message);
    }
    
    /**
     * 打印成功信息
     */
    protected void printSuccess(String message) {
        System.out.println("✅ " + message);
    }
    
    /**
     * 打印错误信息
     */
    protected void printError(String message) {
        System.err.println("❌ " + message);
    }
}

