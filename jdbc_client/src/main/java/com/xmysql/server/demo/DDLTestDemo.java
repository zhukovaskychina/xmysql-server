package com.xmysql.server.demo;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * XMySQL DDL操作测试类
 * 测试创建数据库、删除数据库、创建表、删除表等DDL操作
 */
public class DDLTestDemo {
    // 数据库连接信息
    private static final String BASE_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String USER = "root";
    private static final String PASSWORD = "root@1234";
    
    // 测试数据库名称
    private static final String[] TEST_DATABASES = {
        "test_db_1", 
        "test_db_2", 
        "test_db_utf8mb4"
    };
    
    // 测试表名称
    private static final String[] TEST_TABLES = {
        "users", 
        "products", 
        "orders"
    };

    public static void main(String[] args) {
        System.out.println("🚀 XMySQL DDL操作测试开始");
        System.out.println("=" .repeat(60));
        
        try {
            // 注册MySQL驱动
            Class.forName("com.mysql.jdbc.Driver");
            
            // 1. 测试数据库操作
            testDatabaseOperations();
            
            // 2. 测试表操作
            testTableOperations();
            
            // 3. 清理测试数据
           // cleanupTestData();
            
            System.out.println("\n🎉 所有DDL测试完成！");
            
        } catch (Exception e) {
            System.err.println(" 测试过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 测试数据库操作
     */
    private static void testDatabaseOperations() throws SQLException {
        System.out.println("\n 1. 测试数据库操作");
        System.out.println("-".repeat(40));
        
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            // 1.1 创建数据库测试
            testCreateDatabases(conn);
            
            // 1.2 测试IF NOT EXISTS
            testCreateDatabaseIfNotExists(conn);
            
            // 1.3 测试字符集指定
            testCreateDatabaseWithCharset(conn);
        }
    }
    
    /**
     * 测试创建数据库
     */
    private static void testCreateDatabases(Connection conn) throws SQLException {
        System.out.println("\n 1.1 创建数据库测试:");
        
        for (String dbName : TEST_DATABASES) {
            try {
                String sql = "CREATE DATABASE " + dbName;
                executeUpdate(conn, sql, "创建数据库 " + dbName);
            } catch (SQLException e) {
                System.out.println("    数据库 " + dbName + " 可能已存在: " + e.getMessage());
            }
        }
    }
    
    /**
     * 测试IF NOT EXISTS
     */
    private static void testCreateDatabaseIfNotExists(Connection conn) throws SQLException {
        System.out.println("\n🔄 1.2 测试 IF NOT EXISTS:");
        
        String dbName = TEST_DATABASES[0];
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
        executeUpdate(conn, sql, "创建数据库 " + dbName + " (IF NOT EXISTS)");
    }
    
    /**
     * 测试指定字符集创建数据库
     */
    private static void testCreateDatabaseWithCharset(Connection conn) throws SQLException {
        System.out.println("\n🌐 1.3 测试指定字符集:");
        
        String dbName = "test_charset_db";
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName + 
                    " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
        executeUpdate(conn, sql, "创建数据库 " + dbName + " (指定字符集)");
        
        // 删除测试数据库
        sql = "DROP DATABASE IF EXISTS " + dbName;
        executeUpdate(conn, sql, "删除测试数据库 " + dbName);
    }
    
    /**
     * 测试表操作
     */
    private static void testTableOperations() throws SQLException {
        System.out.println("\n 2. 测试表操作");
        System.out.println("-".repeat(40));
        
        String testDb = TEST_DATABASES[0];
        
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            // 切换到测试数据库
            String useDbSql = "USE " + testDb;
            executeUpdate(conn, useDbSql, "切换到数据库 " + testDb);
            
            // 2.1 创建表测试
            testCreateTables(conn);
            
            // 2.2 删除表测试
            testDropTables(conn);
        }
    }
    
    /**
     * 测试创建表
     */
    private static void testCreateTables(Connection conn) throws SQLException {
        System.out.println("\n 2.1 创建表测试:");
        
        // 创建用户表
        String createUsersTable = """
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                age INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        executeUpdate(conn, createUsersTable, "创建用户表");
        
        // 创建产品表
        String createProductsTable = """
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category VARCHAR(50),
                stock INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        executeUpdate(conn, createProductsTable, "创建产品表");
        
        // 创建订单表
        String createOrdersTable = """
            CREATE TABLE IF NOT EXISTS orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_id INT,
                product_id INT,
                quantity INT NOT NULL,
                total_price DECIMAL(10,2) NOT NULL,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'pending'
            )
            """;
        executeUpdate(conn, createOrdersTable, "创建订单表");
    }
    
    /**
     * 测试删除表
     */
    private static void testDropTables(Connection conn) throws SQLException {
        System.out.println("\n🗑️ 2.2 删除表测试:");
        
        for (String tableName : TEST_TABLES) {
            String sql = "DROP TABLE IF EXISTS " + tableName;
            executeUpdate(conn, sql, "删除表 " + tableName);
        }
    }
    
    /**
     * 清理测试数据
     */
    private static void cleanupTestData() throws SQLException {
        System.out.println("\n🧹 3. 清理测试数据");
        System.out.println("-".repeat(40));
        
        try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
            for (String dbName : TEST_DATABASES) {
                String sql = "DROP DATABASE IF EXISTS " + dbName;
                executeUpdate(conn, sql, "删除测试数据库 " + dbName);
            }
        }
    }
    
    /**
     * 执行更新SQL并打印结果
     */
    private static void executeUpdate(Connection conn, String sql, String description) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("    " + description + " - 成功");
        } catch (SQLException e) {
            System.out.println("    " + description + " - 失败: " + e.getMessage());
            throw e;
        }
    }
    
    /**
     * 执行查询SQL并返回结果
     */
    private static List<String> executeQuery(Connection conn, String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                results.add(rs.getString(1));
            }
        }
        return results;
    }
} 