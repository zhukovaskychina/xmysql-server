package com.xmysql.server.demo;

import java.sql.*;

/**
 * INSERT功能专项测试程序
 * 测试各种INSERT语句的执行情况
 */
public class InsertTestDemo {
    private static final String BASE_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String USER = "root";
    private static final String PASSWORD = "root@1234";

    public static void main(String[] args) {
        System.out.println("🚀 INSERT功能专项测试开始");
        System.out.println("=".repeat(50));
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            
            try (Connection conn = DriverManager.getConnection(BASE_URL, USER, PASSWORD)) {
                // 1. 准备测试环境
                setupTestEnvironment(conn);
                
                // 2. 测试基本INSERT
                testBasicInsert(conn);
                
                // 3. 测试批量INSERT
                testBatchInsert(conn);
                
                // 4. 测试带默认值的INSERT
                testInsertWithDefaults(conn);
                
                // 5. 测试INSERT...SELECT
                testInsertSelect(conn);
                
                // 6. 测试INSERT ON DUPLICATE KEY UPDATE
                testInsertOnDuplicateKey(conn);
                
                // 7. 清理测试环境
                cleanupTestEnvironment(conn);
                
                System.out.println("\n INSERT功能测试全部完成！");
                
            } catch (SQLException e) {
                System.err.println(" 数据库连接或操作失败: " + e.getMessage());
                e.printStackTrace();
            }
            
        } catch (ClassNotFoundException e) {
            System.err.println(" JDBC驱动未找到: " + e.getMessage());
        }
    }
    
    private static void setupTestEnvironment(Connection conn) throws SQLException {
        System.out.println("\n 1. 准备测试环境");
        System.out.println("-".repeat(30));
        
        // 创建测试数据库
        executeUpdate(conn, "CREATE DATABASE IF NOT EXISTS insert_test_db", "创建测试数据库");
        executeUpdate(conn, "USE insert_test_db", "切换到测试数据库");
        
        // 创建测试表
        String createUsersTable = """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL UNIQUE,
                email VARCHAR(100) NOT NULL,
                age INT DEFAULT 18,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status ENUM('active', 'inactive') DEFAULT 'active'
            )
            """;
        executeUpdate(conn, createUsersTable, "创建用户表");
        
        String createProductsTable = """
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category VARCHAR(50) DEFAULT 'general',
                in_stock BOOLEAN DEFAULT TRUE
            )
            """;
        executeUpdate(conn, createProductsTable, "创建产品表");
        
        String createOrdersTable = """
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                product_id INT,
                quantity INT DEFAULT 1,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        executeUpdate(conn, createOrdersTable, "创建订单表");
    }
    
    private static void testBasicInsert(Connection conn) throws SQLException {
        System.out.println("\n 2. 测试基本INSERT");
        System.out.println("-".repeat(30));
        
        // 测试完整字段INSERT
        String insertUser1 = """
            INSERT INTO users (username, email, age, status) 
            VALUES ('john_doe', 'john@example.com', 25, 'active')
            """;
        executeUpdate(conn, insertUser1, "插入完整用户信息");
        
        // 测试部分字段INSERT（使用默认值）
        String insertUser2 = """
            INSERT INTO users (username, email) 
            VALUES ('jane_smith', 'jane@example.com')
            """;
        executeUpdate(conn, insertUser2, "插入部分用户信息（使用默认值）");
        
        // 测试产品INSERT
        String insertProduct1 = """
            INSERT INTO products (name, price, category) 
            VALUES ('Laptop', 999.99, 'electronics')
            """;
        executeUpdate(conn, insertProduct1, "插入产品信息");
        
        String insertProduct2 = """
            INSERT INTO products (name, price) 
            VALUES ('Mouse', 29.99)
            """;
        executeUpdate(conn, insertProduct2, "插入产品信息（使用默认分类）");
    }
    
    private static void testBatchInsert(Connection conn) throws SQLException {
        System.out.println("\n 3. 测试批量INSERT");
        System.out.println("-".repeat(30));
        
        // 测试多行INSERT
        String batchInsert = """
            INSERT INTO users (username, email, age) VALUES 
            ('alice_wonder', 'alice@example.com', 22),
            ('bob_builder', 'bob@example.com', 30),
            ('charlie_brown', 'charlie@example.com', 28)
            """;
        executeUpdate(conn, batchInsert, "批量插入用户");
        
        // 测试产品批量插入
        String batchProductInsert = """
            INSERT INTO products (name, price, category) VALUES 
            ('Keyboard', 79.99, 'electronics'),
            ('Monitor', 299.99, 'electronics'),
            ('Desk Chair', 199.99, 'furniture')
            """;
        executeUpdate(conn, batchProductInsert, "批量插入产品");
    }
    
    private static void testInsertWithDefaults(Connection conn) throws SQLException {
        System.out.println("\n 4. 测试带默认值的INSERT");
        System.out.println("-".repeat(30));
        
        // 测试只插入必需字段，其他使用默认值
        String insertMinimal = """
            INSERT INTO users (username, email) 
            VALUES ('minimal_user', 'minimal@example.com')
            """;
        executeUpdate(conn, insertMinimal, "插入最少字段（测试默认值）");
        
        // 测试显式使用DEFAULT关键字
        String insertWithDefault = """
            INSERT INTO products (name, price, category, in_stock) 
            VALUES ('Test Product', 19.99, DEFAULT, DEFAULT)
            """;
        executeUpdate(conn, insertWithDefault, "使用DEFAULT关键字插入");
    }
    
    private static void testInsertSelect(Connection conn) throws SQLException {
        System.out.println("\n 5. 测试INSERT...SELECT");
        System.out.println("-".repeat(30));
        
        // 先查询现有数据，然后基于查询结果插入订单
        String insertSelect = """
            INSERT INTO orders (user_id, product_id, quantity) 
            SELECT u.id, p.id, 2 
            FROM users u, products p 
            WHERE u.username = 'john_doe' AND p.name = 'Laptop'
            LIMIT 1
            """;
        executeUpdate(conn, insertSelect, "基于SELECT结果插入订单");
    }
    
    private static void testInsertOnDuplicateKey(Connection conn) throws SQLException {
        System.out.println("\n 6. 测试INSERT ON DUPLICATE KEY UPDATE");
        System.out.println("-".repeat(30));
        
        // 注意：这个功能可能在当前实现中不支持，但我们可以测试
        try {
            String insertOrUpdate = """
                INSERT INTO users (username, email, age) 
                VALUES ('john_doe', 'john_updated@example.com', 26)
                ON DUPLICATE KEY UPDATE email = VALUES(email), age = VALUES(age)
                """;
            executeUpdate(conn, insertOrUpdate, "INSERT ON DUPLICATE KEY UPDATE");
        } catch (Exception e) {
            System.out.println("    ON DUPLICATE KEY UPDATE 功能暂不支持: " + e.getMessage());
        }
        
        // 测试REPLACE语句（如果支持）
        try {
            String replaceStatement = """
                REPLACE INTO users (username, email, age) 
                VALUES ('john_doe', 'john_replaced@example.com', 27)
                """;
            executeUpdate(conn, replaceStatement, "REPLACE语句测试");
        } catch (Exception e) {
            System.out.println("    REPLACE 功能暂不支持: " + e.getMessage());
        }
    }
    
    private static void cleanupTestEnvironment(Connection conn) throws SQLException {
        System.out.println("\n 7. 清理测试环境");
        System.out.println("-".repeat(30));
        
        // 查看插入的数据
        System.out.println("    查看插入的数据:");
        executeQuery(conn, "SELECT COUNT(*) as user_count FROM users", "用户总数");
        executeQuery(conn, "SELECT COUNT(*) as product_count FROM products", "产品总数");
        executeQuery(conn, "SELECT COUNT(*) as order_count FROM orders", "订单总数");
        
        // 清理表
        executeUpdate(conn, "DROP TABLE IF EXISTS orders", "删除订单表");
        executeUpdate(conn, "DROP TABLE IF EXISTS products", "删除产品表");
        executeUpdate(conn, "DROP TABLE IF EXISTS users", "删除用户表");
        
        // 删除测试数据库
        executeUpdate(conn, "DROP DATABASE IF EXISTS insert_test_db", "删除测试数据库");
    }
    
    private static void executeUpdate(Connection conn, String sql, String description) {
        try {
            long startTime = System.currentTimeMillis();
            int result = conn.createStatement().executeUpdate(sql);
            long endTime = System.currentTimeMillis();
            
            System.out.printf("    %s: 影响 %d 行 (耗时: %d ms)%n",
                description, result, (endTime - startTime));
        } catch (SQLException e) {
            System.err.printf("    %s 失败: %s%n", description, e.getMessage());
        }
    }
    
    private static void executeQuery(Connection conn, String sql, String description) {
        try {
            long startTime = System.currentTimeMillis();
            ResultSet rs = conn.createStatement().executeQuery(sql);
            long endTime = System.currentTimeMillis();
            
            if (rs.next()) {
                System.out.printf("    %s: %s (耗时: %d ms)%n",
                    description, rs.getString(1), (endTime - startTime));
            }
            rs.close();
        } catch (SQLException e) {
            System.err.printf("    %s 查询失败: %s%n", description, e.getMessage());
        }
    }
} 