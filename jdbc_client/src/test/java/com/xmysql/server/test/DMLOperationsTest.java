package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * DML操作测试类
 * 测试INSERT, UPDATE, DELETE等数据操作语句
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DMLOperationsTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_dml_operations";
    private static final String USERS_TABLE = "users";
    private static final String PRODUCTS_TABLE = "products";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        // 创建测试数据库
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
        
        // 创建用户表
        connection.createStatement().executeUpdate("""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) NOT NULL UNIQUE,
                email VARCHAR(100) NOT NULL,
                age INT DEFAULT 18,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """);
        
        // 创建产品表
        connection.createStatement().executeUpdate("""
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INT DEFAULT 0,
                category VARCHAR(50)
            )
            """);
    }
    
    @AfterAll
    public static void cleanUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    
    @BeforeEach
    public void clearTables() throws SQLException {
        executeUpdate("TRUNCATE TABLE " + USERS_TABLE);
        executeUpdate("TRUNCATE TABLE " + PRODUCTS_TABLE);
    }
    
    @Test
    @Order(1)
    @DisplayName("测试INSERT - 单行插入")
    public void testInsertSingleRow() throws SQLException {
        String sql = "INSERT INTO users (username, email, age) VALUES ('alice', 'alice@example.com', 25)";
        int rowsAffected = executeUpdate(sql);
        
        assertThat(rowsAffected).isEqualTo(1);
        assertThat(getTableRowCount(USERS_TABLE)).isEqualTo(1);
        
        printSuccess("单行插入测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试INSERT - 批量插入")
    public void testInsertMultipleRows() throws SQLException {
        String sql = """
            INSERT INTO users (username, email, age) VALUES 
            ('bob', 'bob@example.com', 30),
            ('charlie', 'charlie@example.com', 28),
            ('diana', 'diana@example.com', 32)
            """;
        
        int rowsAffected = executeUpdate(sql);
        
        assertThat(rowsAffected).isEqualTo(3);
        assertThat(getTableRowCount(USERS_TABLE)).isEqualTo(3);
        
        printSuccess("批量插入测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试INSERT - PreparedStatement")
    public void testInsertWithPreparedStatement() throws SQLException {
        String sql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // 插入第一条
            pstmt.setString(1, "eve");
            pstmt.setString(2, "eve@example.com");
            pstmt.setInt(3, 24);
            int rows1 = pstmt.executeUpdate();
            
            // 插入第二条
            pstmt.setString(1, "frank");
            pstmt.setString(2, "frank@example.com");
            pstmt.setInt(3, 35);
            int rows2 = pstmt.executeUpdate();
            
            assertThat(rows1).isEqualTo(1);
            assertThat(rows2).isEqualTo(1);
            assertThat(getTableRowCount(USERS_TABLE)).isEqualTo(2);
        }
        
        printSuccess("PreparedStatement插入测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试INSERT - 默认值")
    public void testInsertWithDefaultValues() throws SQLException {
        String sql = "INSERT INTO users (username, email) VALUES ('grace', 'grace@example.com')";
        executeUpdate(sql);
        
        // 验证默认值
        try (ResultSet rs = executeQuery("SELECT age FROM users WHERE username = 'grace'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("age")).isEqualTo(18); // 默认值
        }
        
        printSuccess("默认值插入测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试UPDATE - 单行更新")
    public void testUpdateSingleRow() throws SQLException {
        // 先插入数据
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('henry', 'henry@example.com', 25)");
        
        // 更新数据
        String updateSql = "UPDATE users SET age = 26 WHERE username = 'henry'";
        int rowsAffected = executeUpdate(updateSql);
        
        assertThat(rowsAffected).isEqualTo(1);
        
        // 验证更新结果
        try (ResultSet rs = executeQuery("SELECT age FROM users WHERE username = 'henry'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("age")).isEqualTo(26);
        }
        
        printSuccess("单行更新测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试UPDATE - 批量更新")
    public void testUpdateMultipleRows() throws SQLException {
        // 插入测试数据
        executeUpdate("""
            INSERT INTO users (username, email, age) VALUES 
            ('user1', 'user1@example.com', 20),
            ('user2', 'user2@example.com', 21),
            ('user3', 'user3@example.com', 22)
            """);
        
        // 批量更新
        String updateSql = "UPDATE users SET age = age + 1 WHERE age < 25";
        int rowsAffected = executeUpdate(updateSql);
        
        assertThat(rowsAffected).isEqualTo(3);
        
        printSuccess("批量更新测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试UPDATE - PreparedStatement")
    public void testUpdateWithPreparedStatement() throws SQLException {
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('iris', 'iris@example.com', 25)");
        
        String sql = "UPDATE users SET email = ?, age = ? WHERE username = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, "newemail@example.com");
            pstmt.setInt(2, 30);
            pstmt.setString(3, "iris");
            
            int rowsAffected = pstmt.executeUpdate();
            assertThat(rowsAffected).isEqualTo(1);
        }
        
        printSuccess("PreparedStatement更新测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试DELETE - 单行删除")
    public void testDeleteSingleRow() throws SQLException {
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('jack', 'jack@example.com', 25)");
        
        String deleteSql = "DELETE FROM users WHERE username = 'jack'";
        int rowsAffected = executeUpdate(deleteSql);
        
        assertThat(rowsAffected).isEqualTo(1);
        assertThat(getTableRowCount(USERS_TABLE)).isEqualTo(0);
        
        printSuccess("单行删除测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试DELETE - 批量删除")
    public void testDeleteMultipleRows() throws SQLException {
        executeUpdate("""
            INSERT INTO users (username, email, age) VALUES 
            ('user1', 'user1@example.com', 20),
            ('user2', 'user2@example.com', 25),
            ('user3', 'user3@example.com', 30)
            """);
        
        String deleteSql = "DELETE FROM users WHERE age >= 25";
        int rowsAffected = executeUpdate(deleteSql);
        
        assertThat(rowsAffected).isEqualTo(2);
        assertThat(getTableRowCount(USERS_TABLE)).isEqualTo(1);
        
        printSuccess("批量删除测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试DELETE - 全部删除")
    public void testDeleteAll() throws SQLException {
        executeUpdate("""
            INSERT INTO users (username, email, age) VALUES 
            ('user1', 'user1@example.com', 20),
            ('user2', 'user2@example.com', 25)
            """);
        
        String deleteSql = "DELETE FROM users";
        executeUpdate(deleteSql);
        
        assertThat(getTableRowCount(USERS_TABLE)).isEqualTo(0);
        
        printSuccess("全部删除测试通过");
    }
    
    @Test
    @Order(11)
    @DisplayName("测试INSERT - 产品数据（DECIMAL类型）")
    public void testInsertProductsWithDecimal() throws SQLException {
        String sql = """
            INSERT INTO products (name, price, stock, category) VALUES 
            ('Laptop', 999.99, 10, 'Electronics'),
            ('Mouse', 29.99, 50, 'Electronics'),
            ('Desk', 299.50, 5, 'Furniture')
            """;
        
        int rowsAffected = executeUpdate(sql);
        assertThat(rowsAffected).isEqualTo(3);
        
        // 验证DECIMAL精度
        try (ResultSet rs = executeQuery("SELECT price FROM products WHERE name = 'Laptop'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("price").doubleValue()).isEqualTo(999.99);
        }
        
        printSuccess("DECIMAL类型插入测试通过");
    }
    
    @Test
    @Order(12)
    @DisplayName("测试UPDATE - 库存更新")
    public void testUpdateProductStock() throws SQLException {
        executeUpdate("INSERT INTO products (name, price, stock) VALUES ('Widget', 19.99, 100)");
        
        // 减少库存
        executeUpdate("UPDATE products SET stock = stock - 10 WHERE name = 'Widget'");
        
        try (ResultSet rs = executeQuery("SELECT stock FROM products WHERE name = 'Widget'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("stock")).isEqualTo(90);
        }
        
        printSuccess("库存更新测试通过");
    }
    
    @Test
    @Order(13)
    @DisplayName("测试INSERT - UNIQUE约束违反")
    public void testInsertUniqueConstraintViolation() throws SQLException {
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('unique_user', 'unique@example.com', 25)");
        
        // 尝试插入重复的username
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO users (username, email, age) VALUES ('unique_user', 'another@example.com', 30)")
        ).isInstanceOf(SQLException.class);
        
        printSuccess("UNIQUE约束测试通过");
    }
}

