package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;

import static org.assertj.core.api.Assertions.*;

/**
 * PreparedStatement测试类
 * 测试预编译语句的各种功能
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PreparedStatementTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_prepared_stmt";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
        
        // 创建测试表
        connection.createStatement().executeUpdate("""
            CREATE TABLE users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100),
                age INT,
                salary DECIMAL(10,2),
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """);
    }
    
    @AfterAll
    public static void cleanUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    
    @BeforeEach
    public void clearTable() throws SQLException {
        executeUpdate("TRUNCATE TABLE users");
    }
    
    @Test
    @Order(1)
    @DisplayName("测试PreparedStatement - 基本INSERT")
    public void testPreparedStatementBasicInsert() throws SQLException {
        String sql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, "alice");
            pstmt.setString(2, "alice@example.com");
            pstmt.setInt(3, 25);
            
            int rowsAffected = pstmt.executeUpdate();
            assertThat(rowsAffected).isEqualTo(1);
        }
        
        assertThat(getTableRowCount("users")).isEqualTo(1);
        printSuccess("PreparedStatement基本INSERT测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试PreparedStatement - 批量INSERT")
    public void testPreparedStatementBatchInsert() throws SQLException {
        String sql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // 第一条
            pstmt.setString(1, "user1");
            pstmt.setString(2, "user1@example.com");
            pstmt.setInt(3, 20);
            pstmt.addBatch();
            
            // 第二条
            pstmt.setString(1, "user2");
            pstmt.setString(2, "user2@example.com");
            pstmt.setInt(3, 25);
            pstmt.addBatch();
            
            // 第三条
            pstmt.setString(1, "user3");
            pstmt.setString(2, "user3@example.com");
            pstmt.setInt(3, 30);
            pstmt.addBatch();
            
            int[] results = pstmt.executeBatch();
            assertThat(results).hasSize(3);
            assertThat(results[0]).isGreaterThan(0);
        }
        
        assertThat(getTableRowCount("users")).isEqualTo(3);
        printSuccess("PreparedStatement批量INSERT测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试PreparedStatement - SELECT查询")
    public void testPreparedStatementSelect() throws SQLException {
        // 先插入数据
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('bob', 'bob@example.com', 30)");
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('charlie', 'charlie@example.com', 35)");
        
        String sql = "SELECT * FROM users WHERE age > ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, 28);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    assertThat(rs.getInt("age")).isGreaterThan(28);
                }
                assertThat(count).isEqualTo(2);
            }
        }
        
        printSuccess("PreparedStatement SELECT测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试PreparedStatement - UPDATE")
    public void testPreparedStatementUpdate() throws SQLException {
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('diana', 'diana@example.com', 25)");
        
        String sql = "UPDATE users SET age = ?, email = ? WHERE username = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, 26);
            pstmt.setString(2, "newemail@example.com");
            pstmt.setString(3, "diana");
            
            int rowsAffected = pstmt.executeUpdate();
            assertThat(rowsAffected).isEqualTo(1);
        }
        
        // 验证更新结果
        try (ResultSet rs = executeQuery("SELECT age, email FROM users WHERE username = 'diana'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("age")).isEqualTo(26);
            assertThat(rs.getString("email")).isEqualTo("newemail@example.com");
        }
        
        printSuccess("PreparedStatement UPDATE测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试PreparedStatement - DELETE")
    public void testPreparedStatementDelete() throws SQLException {
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('eve', 'eve@example.com', 28)");
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('frank', 'frank@example.com', 32)");
        
        String sql = "DELETE FROM users WHERE age < ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, 30);
            
            int rowsAffected = pstmt.executeUpdate();
            assertThat(rowsAffected).isEqualTo(1); // 只有eve被删除
        }
        
        assertThat(getTableRowCount("users")).isEqualTo(1);
        
        printSuccess("PreparedStatement DELETE测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试PreparedStatement - 多种数据类型")
    public void testPreparedStatementMultipleTypes() throws SQLException {
        String sql = "INSERT INTO users (username, email, age, salary, is_active) VALUES (?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, "grace");
            pstmt.setString(2, "grace@example.com");
            pstmt.setInt(3, 27);
            pstmt.setBigDecimal(4, new BigDecimal("75000.50"));
            pstmt.setBoolean(5, true);
            
            pstmt.executeUpdate();
        }
        
        // 验证数据
        try (ResultSet rs = executeQuery("SELECT * FROM users WHERE username = 'grace'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("username")).isEqualTo("grace");
            assertThat(rs.getInt("age")).isEqualTo(27);
            assertThat(rs.getBigDecimal("salary")).isEqualByComparingTo(new BigDecimal("75000.50"));
            assertThat(rs.getBoolean("is_active")).isTrue();
        }
        
        printSuccess("PreparedStatement多种数据类型测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试PreparedStatement - NULL值处理")
    public void testPreparedStatementNullValues() throws SQLException {
        String sql = "INSERT INTO users (username, email, age, salary) VALUES (?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, "henry");
            pstmt.setNull(2, Types.VARCHAR);
            pstmt.setNull(3, Types.INTEGER);
            pstmt.setNull(4, Types.DECIMAL);
            
            pstmt.executeUpdate();
        }
        
        // 验证NULL值
        try (ResultSet rs = executeQuery("SELECT * FROM users WHERE username = 'henry'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("email")).isNull();
            rs.getInt("age");
            assertThat(rs.wasNull()).isTrue();
            assertThat(rs.getBigDecimal("salary")).isNull();
        }
        
        printSuccess("PreparedStatement NULL值处理测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试PreparedStatement - 参数重用")
    public void testPreparedStatementParameterReuse() throws SQLException {
        String sql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // 第一次执行
            pstmt.setString(1, "user1");
            pstmt.setString(2, "user1@example.com");
            pstmt.setInt(3, 20);
            pstmt.executeUpdate();
            
            // 重用PreparedStatement，修改参数
            pstmt.setString(1, "user2");
            pstmt.setString(2, "user2@example.com");
            pstmt.setInt(3, 25);
            pstmt.executeUpdate();
            
            // 再次重用
            pstmt.setString(1, "user3");
            pstmt.setString(2, "user3@example.com");
            pstmt.setInt(3, 30);
            pstmt.executeUpdate();
        }
        
        assertThat(getTableRowCount("users")).isEqualTo(3);
        printSuccess("PreparedStatement参数重用测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试PreparedStatement - 获取生成的主键")
    public void testPreparedStatementGeneratedKeys() throws SQLException {
        String sql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, "iris");
            pstmt.setString(2, "iris@example.com");
            pstmt.setInt(3, 24);
            
            pstmt.executeUpdate();
            
            // 获取生成的主键
            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                assertThat(generatedKeys.next()).isTrue();
                int generatedId = generatedKeys.getInt(1);
                assertThat(generatedId).isGreaterThan(0);
                printTestInfo("生成的主键ID: " + generatedId);
            }
        }
        
        printSuccess("PreparedStatement获取生成主键测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试PreparedStatement - IN子句")
    public void testPreparedStatementInClause() throws SQLException {
        // 插入测试数据
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('user1', 'user1@example.com', 20)");
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('user2', 'user2@example.com', 25)");
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('user3', 'user3@example.com', 30)");
        executeUpdate("INSERT INTO users (username, email, age) VALUES ('user4', 'user4@example.com', 35)");
        
        // 使用IN子句
        String sql = "SELECT * FROM users WHERE age IN (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, 20);
            pstmt.setInt(2, 25);
            pstmt.setInt(3, 30);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    int age = rs.getInt("age");
                    assertThat(age).isIn(20, 25, 30);
                }
                assertThat(count).isEqualTo(3);
            }
        }
        
        printSuccess("PreparedStatement IN子句测试通过");
    }
    
    @Test
    @Order(11)
    @DisplayName("测试PreparedStatement - LIKE模糊查询")
    public void testPreparedStatementLike() throws SQLException {
        executeUpdate("INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com')");
        executeUpdate("INSERT INTO users (username, email) VALUES ('alicia', 'alicia@example.com')");
        executeUpdate("INSERT INTO users (username, email) VALUES ('bob', 'bob@example.com')");
        
        String sql = "SELECT * FROM users WHERE username LIKE ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, "ali%");
            
            try (ResultSet rs = pstmt.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    String username = rs.getString("username");
                    assertThat(username).startsWith("ali");
                }
                assertThat(count).isEqualTo(2);
            }
        }
        
        printSuccess("PreparedStatement LIKE模糊查询测试通过");
    }
    
    @Test
    @Order(12)
    @DisplayName("测试PreparedStatement - 事务中使用")
    public void testPreparedStatementInTransaction() throws SQLException {
        connection.setAutoCommit(false);
        
        try {
            String sql = "INSERT INTO users (username, email, age) VALUES (?, ?, ?)";
            
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, "jack");
                pstmt.setString(2, "jack@example.com");
                pstmt.setInt(3, 28);
                pstmt.executeUpdate();
                
                pstmt.setString(1, "jill");
                pstmt.setString(2, "jill@example.com");
                pstmt.setInt(3, 26);
                pstmt.executeUpdate();
            }
            
            connection.commit();
            
            assertThat(getTableRowCount("users")).isEqualTo(2);
            
            printSuccess("PreparedStatement事务中使用测试通过");
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }
}

