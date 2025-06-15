package com.xmysql.server.demo;

import java.sql.*;

public class InsertDataTestDemo {
    private static final String URL = "jdbc:mysql://localhost:3309/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    public static void main(String[] args) {
        System.out.println(" XMySQL 插入数据功能测试");
        System.out.println("============================================================");

        // 加载JDBC驱动
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println(" JDBC驱动加载成功");
        } catch (ClassNotFoundException e) {
            System.err.println(" JDBC驱动加载失败: " + e.getMessage());
            return;
        }

        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            System.out.println(" 数据库连接成功");
            
            // 1. 创建测试数据库
            createTestDatabase(connection);
            
            // 2. 使用测试数据库
            useTestDatabase(connection);
            
            // 3. 创建测试表
            createTestTables(connection);
            
            // 4. 插入测试数据
            insertTestData(connection);
            
            // 5. 查询验证数据
            queryTestData(connection);
            
            System.out.println("\n 所有插入数据测试完成");
            
        } catch (SQLException e) {
            System.err.println(" 测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void createTestDatabase(Connection connection) throws SQLException {
        System.out.println("\n 1. 创建测试数据库");
        System.out.println("----------------------------------------");

        String[] createDbQueries = {
            "CREATE DATABASE IF NOT EXISTS insert_test_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
        };

        for (String query : createDbQueries) {
            System.out.printf(" 执行: %s\n", query);
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(query);
                System.out.println("    成功");
            } catch (SQLException e) {
                System.out.printf("    错误: %s\n", e.getMessage());
                // 如果数据库已存在，继续执行
                if (e.getMessage().contains("already exists")) {
                    System.out.println("   ℹ️ 数据库已存在，继续执行");
                } else {
                    throw e;
                }
            }
        }
    }

    private static void useTestDatabase(Connection connection) throws SQLException {
        System.out.println("\n 2. 使用测试数据库");
        System.out.println("----------------------------------------");

        String useQuery = "USE insert_test_db";
        System.out.printf(" 执行: %s\n", useQuery);
        
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(useQuery);
            System.out.println("    成功切换到 insert_test_db");
        } catch (SQLException e) {
            System.out.printf("    错误: %s\n", e.getMessage());
            throw e;
        }
    }

    private static void createTestTables(Connection connection) throws SQLException {
        System.out.println("\n 3. 创建测试表");
        System.out.println("----------------------------------------");

        String[] createTableQueries = {
            // 简单用户表
            "CREATE TABLE IF NOT EXISTS users (" +
            "    id INT AUTO_INCREMENT PRIMARY KEY," +
            "    username VARCHAR(50) NOT NULL," +
            "    email VARCHAR(100) NOT NULL," +
            "    age INT DEFAULT 18" +
            ")",
            
            // 产品表
            "CREATE TABLE IF NOT EXISTS products (" +
            "    id INT AUTO_INCREMENT PRIMARY KEY," +
            "    name VARCHAR(100) NOT NULL," +
            "    price DECIMAL(10,2) NOT NULL," +
            "    category VARCHAR(50) DEFAULT 'general'" +
            ")"
        };

        for (String query : createTableQueries) {
            System.out.printf(" 执行: %s\n", query);
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(query);
                System.out.println("    表创建成功");
            } catch (SQLException e) {
                System.out.printf("    错误: %s\n", e.getMessage());
                throw e;
            }
        }
    }

    private static void insertTestData(Connection connection) throws SQLException {
        System.out.println("\n 4. 插入测试数据");
        System.out.println("----------------------------------------");

        // 测试简单INSERT语句
        testSimpleInserts(connection);
        
        // 测试PreparedStatement插入
        testPreparedInserts(connection);
        
        // 测试批量插入
        testBatchInserts(connection);
    }

    private static void testSimpleInserts(Connection connection) throws SQLException {
        System.out.println("\n🔸 4.1 测试简单INSERT语句");
        
        String[] insertQueries = {
            "INSERT INTO users (username, email, age) VALUES ('alice', 'alice@example.com', 25)",
            "INSERT INTO users (username, email, age) VALUES ('bob', 'bob@example.com', 30)",
            "INSERT INTO products (name, price, category) VALUES ('Laptop', 999.99, 'Electronics')",
            "INSERT INTO products (name, price) VALUES ('Book', 29.99)"
        };

        for (String query : insertQueries) {
            System.out.printf(" 执行: %s\n", query);
            try (Statement stmt = connection.createStatement()) {
                int rowsAffected = stmt.executeUpdate(query);
                System.out.printf("    成功，影响行数: %d\n", rowsAffected);
            } catch (SQLException e) {
                System.out.printf("    错误: %s\n", e.getMessage());
            }
        }
    }

    private static void testPreparedInserts(Connection connection) throws SQLException {
        System.out.println("\n🔸 4.2 测试PreparedStatement插入");
        
        // 用户数据
        String userInsertSql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
        System.out.printf(" 准备语句: %s\n", userInsertSql);
        
        try (PreparedStatement pstmt = connection.prepareStatement(userInsertSql)) {
            // 插入多个用户
            String[][] userData = {
                {"charlie", "charlie@example.com", "28"},
                {"diana", "diana@example.com", "32"},
                {"eve", "eve@example.com", "24"}
            };
            
            for (String[] user : userData) {
                pstmt.setString(1, user[0]);
                pstmt.setString(2, user[1]);
                pstmt.setInt(3, Integer.parseInt(user[2]));
                
                int rowsAffected = pstmt.executeUpdate();
                System.out.printf("    插入用户 %s，影响行数: %d\n", user[0], rowsAffected);
            }
        } catch (SQLException e) {
            System.out.printf("    PreparedStatement错误: %s\n", e.getMessage());
        }
    }

    private static void testBatchInserts(Connection connection) throws SQLException {
        System.out.println("\n🔸 4.3 测试批量插入");
        
        String productInsertSql = "INSERT INTO products (name, price, category) VALUES (?, ?, ?)";
        System.out.printf(" 批量插入语句: %s\n", productInsertSql);
        
        try (PreparedStatement pstmt = connection.prepareStatement(productInsertSql)) {
            // 批量插入产品数据
            String[][] productData = {
                {"Mouse", "25.99", "Electronics"},
                {"Keyboard", "79.99", "Electronics"},
                {"Desk", "199.99", "Furniture"},
                {"Chair", "149.99", "Furniture"}
            };
            
            for (String[] product : productData) {
                pstmt.setString(1, product[0]);
                pstmt.setDouble(2, Double.parseDouble(product[1]));
                pstmt.setString(3, product[2]);
                pstmt.addBatch();
            }
            
            int[] results = pstmt.executeBatch();
            System.out.printf("    批量插入完成，总共插入 %d 条记录\n", results.length);
            
            for (int i = 0; i < results.length; i++) {
                System.out.printf("     - 记录 %d: 影响行数 %d\n", i + 1, results[i]);
            }
        } catch (SQLException e) {
            System.out.printf("    批量插入错误: %s\n", e.getMessage());
        }
    }

    private static void queryTestData(Connection connection) throws SQLException {
        System.out.println("\n 5. 查询验证数据");
        System.out.println("----------------------------------------");

        // 查询用户数据
        queryUsers(connection);
        
        // 查询产品数据
        queryProducts(connection);
    }

    private static void queryUsers(Connection connection) throws SQLException {
        System.out.println("\n🔸 5.1 查询用户数据");
        
        String query = "SELECT id, username, email, age FROM users ORDER BY id";
        System.out.printf(" 执行查询: %s\n", query);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("    用户数据:");
            System.out.println("   ID | Username | Email | Age");
            System.out.println("   ---|----------|-------|----");
            
            int count = 0;
            while (rs.next()) {
                count++;
                System.out.printf("   %d  | %-8s | %-20s | %d\n",
                    rs.getInt("id"),
                    rs.getString("username"),
                    rs.getString("email"),
                    rs.getInt("age"));
            }
            
            System.out.printf("    总共查询到 %d 条用户记录\n", count);
            
        } catch (SQLException e) {
            System.out.printf("    查询用户数据错误: %s\n", e.getMessage());
        }
    }

    private static void queryProducts(Connection connection) throws SQLException {
        System.out.println("\n🔸 5.2 查询产品数据");
        
        String query = "SELECT id, name, price, category FROM products ORDER BY id";
        System.out.printf(" 执行查询: %s\n", query);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            System.out.println("    产品数据:");
            System.out.println("   ID | Name     | Price  | Category");
            System.out.println("   ---|----------|--------|----------");
            
            int count = 0;
            while (rs.next()) {
                count++;
                System.out.printf("   %d  | %-8s | %6.2f | %s\n",
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getDouble("price"),
                    rs.getString("category"));
            }
            
            System.out.printf("    总共查询到 %d 条产品记录\n", count);
            
        } catch (SQLException e) {
            System.out.printf("    查询产品数据错误: %s\n", e.getMessage());
        }
    }
} 