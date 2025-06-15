package com.xmysql.server.demo;

import java.sql.*;

/**
 * SQL解析功能测试程序
 * 验证使用sqlparser重构后的SQL处理是否正确识别各种语句类型
 */
public class SQLParserTestDemo {
    private static final String BASE_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String USER = "root";
    private static final String PASSWORD = "root@1234";

    public static void main(String[] args) {
        System.out.println(" SQL解析功能测试开始");
        System.out.println("=" .repeat(50));
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            
            try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
                // 1. 测试各种SELECT语句
                testSelectStatements(conn);
                
                // 2. 测试DDL语句
                testDDLStatements(conn);
                
                // 3. 测试DML语句
                testDMLStatements(conn);
                
                // 4. 测试事务语句
                testTransactionStatements(conn);
                
                // 5. 测试SHOW语句
                testShowStatements(conn);
                
                System.out.println("\n 所有SQL解析测试完成！");
                
            }
        } catch (Exception e) {
            System.err.println(" 测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testSelectStatements(Connection conn) {
        System.out.println("\n 1. 测试SELECT语句解析");
        System.out.println("-".repeat(30));
        
        String[] selectQueries = {
            "SELECT 1",
            "SELECT * FROM users",
            "SELECT id, name FROM users WHERE id = 1",
            "SELECT COUNT(*) FROM products",
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id"
        };
        
        for (String query : selectQueries) {
            executeQuery(conn, query, "SELECT语句");
        }
    }
    
    private static void testDDLStatements(Connection conn) {
        System.out.println("\n 2. 测试DDL语句解析");
        System.out.println("-".repeat(30));
        
        String[] ddlQueries = {
            "CREATE DATABASE test_parser_db",
            "USE test_parser_db",
            "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))",
            "ALTER TABLE test_table ADD COLUMN email VARCHAR(100)",
            "DROP TABLE test_table",
            "DROP DATABASE test_parser_db"
        };
        
        for (String query : ddlQueries) {
            executeUpdate(conn, query, "DDL语句");
        }
    }
    
    private static void testDMLStatements(Connection conn) {
        System.out.println("\n 3. 测试DML语句解析");
        System.out.println("-".repeat(30));
        
        // 先创建测试环境
        executeUpdate(conn, "CREATE DATABASE IF NOT EXISTS test_dml_db", "创建测试数据库");
        executeUpdate(conn, "USE test_dml_db", "切换数据库");
        executeUpdate(conn, "CREATE TABLE test_users (id INT PRIMARY KEY, name VARCHAR(50))", "创建测试表");
        
        String[] dmlQueries = {
            "INSERT INTO test_users (id, name) VALUES (1, 'Alice')",
            "UPDATE test_users SET name = 'Bob' WHERE id = 1",
            "DELETE FROM test_users WHERE id = 1"
        };
        
        for (String query : dmlQueries) {
            executeUpdate(conn, query, "DML语句");
        }
        
        // 清理测试环境
        executeUpdate(conn, "DROP DATABASE test_dml_db", "清理测试数据库");
    }
    
    private static void testTransactionStatements(Connection conn) {
        System.out.println("\n🔄 4. 测试事务语句解析");
        System.out.println("-".repeat(30));
        
        String[] transactionQueries = {
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK"
        };
        
        for (String query : transactionQueries) {
            executeUpdate(conn, query, "事务语句");
        }
    }
    
    private static void testShowStatements(Connection conn) {
        System.out.println("\n 5. 测试SHOW语句解析");
        System.out.println("-".repeat(30));
        
        String[] showQueries = {
            "SHOW DATABASES",
            "SHOW TABLES"
        };
        
        for (String query : showQueries) {
            executeQuery(conn, query, "SHOW语句");
        }
    }
    
    private static void executeQuery(Connection conn, String sql, String type) {
        try {
            System.out.printf("    执行%s: %s", type, sql);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                int columnCount = rs.getMetaData().getColumnCount();
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                System.out.printf(" - 成功 (%d列, %d行)\n", columnCount, rowCount);
            }
        } catch (SQLException e) {
            System.out.printf(" - 失败: %s\n", e.getMessage());
        }
    }
    
    private static void executeUpdate(Connection conn, String sql, String type) {
        try {
            System.out.printf("    执行%s: %s", type, sql);
            try (Statement stmt = conn.createStatement()) {
                int result = stmt.executeUpdate(sql);
                System.out.printf(" - 成功 (影响%d行)\n", result);
            }
        } catch (SQLException e) {
            System.out.printf(" - 失败: %s\n", e.getMessage());
        }
    }
} 