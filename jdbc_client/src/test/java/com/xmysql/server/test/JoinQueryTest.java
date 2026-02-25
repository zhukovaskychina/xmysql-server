package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * JOIN查询测试类
 * 测试INNER JOIN, LEFT JOIN, RIGHT JOIN等连接查询
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JoinQueryTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_join_queries";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
        
        // 创建用户表
        connection.createStatement().executeUpdate("""
            CREATE TABLE users (
                user_id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100)
            )
            """);
        
        // 创建订单表
        connection.createStatement().executeUpdate("""
            CREATE TABLE orders (
                order_id INT PRIMARY KEY AUTO_INCREMENT,
                user_id INT,
                product_name VARCHAR(100),
                amount DECIMAL(10,2),
                order_date DATE
            )
            """);
        
        // 创建产品表
        connection.createStatement().executeUpdate("""
            CREATE TABLE products (
                product_id INT PRIMARY KEY AUTO_INCREMENT,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2)
            )
            """);
        
        // 插入用户数据
        connection.createStatement().executeUpdate("""
            INSERT INTO users (username, email) VALUES
            ('alice', 'alice@example.com'),
            ('bob', 'bob@example.com'),
            ('charlie', 'charlie@example.com'),
            ('diana', 'diana@example.com')
            """);
        
        // 插入订单数据（注意：diana没有订单）
        connection.createStatement().executeUpdate("""
            INSERT INTO orders (user_id, product_name, amount, order_date) VALUES
            (1, 'Laptop', 999.99, '2024-01-15'),
            (1, 'Mouse', 29.99, '2024-01-16'),
            (2, 'Keyboard', 79.99, '2024-01-17'),
            (3, 'Monitor', 299.99, '2024-01-18'),
            (3, 'Headphones', 149.99, '2024-01-19')
            """);
        
        // 插入产品数据
        connection.createStatement().executeUpdate("""
            INSERT INTO products (product_name, category, price) VALUES
            ('Laptop', 'Electronics', 999.99),
            ('Mouse', 'Electronics', 29.99),
            ('Keyboard', 'Electronics', 79.99),
            ('Monitor', 'Electronics', 299.99),
            ('Headphones', 'Electronics', 149.99),
            ('Desk', 'Furniture', 399.99)
            """);
    }
    
    @AfterAll
    public static void cleanUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    
    @Test
    @Order(1)
    @DisplayName("测试INNER JOIN - 基本连接")
    public void testInnerJoinBasic() throws SQLException {
        String sql = """
            SELECT u.username, o.product_name, o.amount
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertThat(rs.getString("username")).isNotNull();
                assertThat(rs.getString("product_name")).isNotNull();
            }
            // alice有2个订单，bob有1个，charlie有2个，diana没有订单
            assertThat(count).isEqualTo(5);
        }
        
        printSuccess("INNER JOIN基本连接测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试LEFT JOIN - 左连接")
    public void testLeftJoin() throws SQLException {
        String sql = """
            SELECT u.username, o.product_name
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            ORDER BY u.user_id
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            boolean foundDiana = false;
            
            while (rs.next()) {
                count++;
                String username = rs.getString("username");
                String productName = rs.getString("product_name");
                
                // diana应该出现，但product_name为NULL
                if ("diana".equals(username)) {
                    foundDiana = true;
                    assertThat(productName).isNull();
                }
            }
            
            // 应该包含所有用户，包括没有订单的diana
            assertThat(count).isEqualTo(6); // 5个订单 + 1个diana的NULL记录
            assertThat(foundDiana).isTrue();
        }
        
        printSuccess("LEFT JOIN测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试RIGHT JOIN - 右连接")
    public void testRightJoin() throws SQLException {
        String sql = """
            SELECT u.username, o.product_name
            FROM orders o
            RIGHT JOIN users u ON o.user_id = u.user_id
            ORDER BY u.user_id
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            // 应该包含所有用户
            assertThat(count).isEqualTo(6);
        }
        
        printSuccess("RIGHT JOIN测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试多表JOIN")
    public void testMultipleJoins() throws SQLException {
        String sql = """
            SELECT u.username, o.product_name, p.category, p.price
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            INNER JOIN products p ON o.product_name = p.product_name
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertThat(rs.getString("username")).isNotNull();
                assertThat(rs.getString("category")).isNotNull();
                assertThat(rs.getBigDecimal("price")).isNotNull();
            }
            assertThat(count).isEqualTo(5);
        }
        
        printSuccess("多表JOIN测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试JOIN with WHERE条件")
    public void testJoinWithWhere() throws SQLException {
        String sql = """
            SELECT u.username, o.product_name, o.amount
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            WHERE o.amount > 100
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                double amount = rs.getBigDecimal("amount").doubleValue();
                assertThat(amount).isGreaterThan(100);
            }
            assertThat(count).isGreaterThan(0);
        }
        
        printSuccess("JOIN with WHERE测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试JOIN with GROUP BY")
    public void testJoinWithGroupBy() throws SQLException {
        String sql = """
            SELECT u.username, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            GROUP BY u.user_id, u.username
            ORDER BY u.username
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            while (rs.next()) {
                String username = rs.getString("username");
                int orderCount = rs.getInt("order_count");
                
                if ("alice".equals(username)) {
                    assertThat(orderCount).isEqualTo(2);
                } else if ("bob".equals(username)) {
                    assertThat(orderCount).isEqualTo(1);
                } else if ("charlie".equals(username)) {
                    assertThat(orderCount).isEqualTo(2);
                } else if ("diana".equals(username)) {
                    assertThat(orderCount).isEqualTo(0);
                }
            }
        }
        
        printSuccess("JOIN with GROUP BY测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试自连接（Self Join）")
    public void testSelfJoin() throws SQLException {
        // 创建员工表用于自连接测试
        executeUpdate("""
            CREATE TABLE IF NOT EXISTS employees (
                emp_id INT PRIMARY KEY,
                emp_name VARCHAR(50),
                manager_id INT
            )
            """);
        
        executeUpdate("""
            INSERT INTO employees VALUES
            (1, 'Alice', NULL),
            (2, 'Bob', 1),
            (3, 'Charlie', 1),
            (4, 'Diana', 2)
            """);
        
        String sql = """
            SELECT e.emp_name as employee, m.emp_name as manager
            FROM employees e
            LEFT JOIN employees m ON e.manager_id = m.emp_id
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                String employee = rs.getString("employee");
                String manager = rs.getString("manager");
                
                if ("Alice".equals(employee)) {
                    assertThat(manager).isNull(); // Alice没有经理
                } else if ("Bob".equals(employee) || "Charlie".equals(employee)) {
                    assertThat(manager).isEqualTo("Alice");
                } else if ("Diana".equals(employee)) {
                    assertThat(manager).isEqualTo("Bob");
                }
            }
            assertThat(count).isEqualTo(4);
        }
        
        printSuccess("自连接测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试JOIN with ORDER BY")
    public void testJoinWithOrderBy() throws SQLException {
        String sql = """
            SELECT u.username, o.amount
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            ORDER BY o.amount DESC
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            double previousAmount = Double.MAX_VALUE;
            while (rs.next()) {
                double currentAmount = rs.getBigDecimal("amount").doubleValue();
                assertThat(currentAmount).isLessThanOrEqualTo(previousAmount);
                previousAmount = currentAmount;
            }
        }
        
        printSuccess("JOIN with ORDER BY测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试JOIN with LIMIT")
    public void testJoinWithLimit() throws SQLException {
        String sql = """
            SELECT u.username, o.product_name
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            LIMIT 3
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertThat(count).isEqualTo(3);
        }
        
        printSuccess("JOIN with LIMIT测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试JOIN with 聚合函数和HAVING")
    public void testJoinWithAggregateAndHaving() throws SQLException {
        String sql = """
            SELECT u.username, COUNT(o.order_id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            GROUP BY u.user_id, u.username
            HAVING COUNT(o.order_id) >= 2
            """;
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                int orderCount = rs.getInt("order_count");
                assertThat(orderCount).isGreaterThanOrEqualTo(2);
            }
            // alice和charlie各有2个订单
            assertThat(count).isEqualTo(2);
        }
        
        printSuccess("JOIN with 聚合函数和HAVING测试通过");
    }
}

