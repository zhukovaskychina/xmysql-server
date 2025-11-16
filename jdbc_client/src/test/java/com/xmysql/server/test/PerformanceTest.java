package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * 性能测试类
 * 测试大批量数据操作的性能
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_performance";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
        
        connection.createStatement().executeUpdate("""
            CREATE TABLE performance_test (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                value INT,
                description TEXT,
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
        executeUpdate("TRUNCATE TABLE performance_test");
    }
    
    @Test
    @Order(1)
    @DisplayName("性能测试 - 批量插入1000条记录")
    public void testBatchInsert1000Records() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        String sql = "INSERT INTO performance_test (name, value, description) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 1; i <= 1000; i++) {
                pstmt.setString(1, "Record " + i);
                pstmt.setInt(2, i);
                pstmt.setString(3, "Description for record " + i);
                pstmt.addBatch();
                
                // 每100条执行一次批处理
                if (i % 100 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch(); // 执行剩余的
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        assertThat(getTableRowCount("performance_test")).isEqualTo(1000);
        printTestInfo("插入1000条记录耗时: " + duration + "ms");
        printSuccess("批量插入1000条记录测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("性能测试 - 查询大量数据")
    public void testQueryLargeDataset() throws SQLException {
        // 先插入数据
        insertTestData(5000);
        
        long startTime = System.currentTimeMillis();
        
        try (ResultSet rs = executeQuery("SELECT * FROM performance_test")) {
            int count = 0;
            while (rs.next()) {
                count++;
                // 读取数据
                rs.getInt("id");
                rs.getString("name");
                rs.getInt("value");
            }
            assertThat(count).isEqualTo(5000);
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        printTestInfo("查询5000条记录耗时: " + duration + "ms");
        printSuccess("查询大量数据测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("性能测试 - 带WHERE条件的查询")
    public void testQueryWithWhereClause() throws SQLException {
        insertTestData(10000);
        
        long startTime = System.currentTimeMillis();
        
        try (ResultSet rs = executeQuery("SELECT * FROM performance_test WHERE value > 5000 AND value < 6000")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertThat(count).isEqualTo(999);
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        printTestInfo("带WHERE条件查询耗时: " + duration + "ms");
        printSuccess("带WHERE条件的查询测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("性能测试 - 批量更新")
    public void testBatchUpdate() throws SQLException {
        insertTestData(1000);
        
        long startTime = System.currentTimeMillis();
        
        String sql = "UPDATE performance_test SET value = value * 2 WHERE id = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 1; i <= 1000; i++) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
                
                if (i % 100 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        printTestInfo("批量更新1000条记录耗时: " + duration + "ms");
        printSuccess("批量更新测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("性能测试 - 批量删除")
    public void testBatchDelete() throws SQLException {
        insertTestData(2000);
        
        long startTime = System.currentTimeMillis();
        
        executeUpdate("DELETE FROM performance_test WHERE value > 1000");
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        assertThat(getTableRowCount("performance_test")).isEqualTo(1000);
        printTestInfo("批量删除1000条记录耗时: " + duration + "ms");
        printSuccess("批量删除测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("性能测试 - 事务中批量操作")
    public void testBatchOperationInTransaction() throws SQLException {
        connection.setAutoCommit(false);
        
        try {
            long startTime = System.currentTimeMillis();
            
            String sql = "INSERT INTO performance_test (name, value, description) VALUES (?, ?, ?)";
            
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                for (int i = 1; i <= 5000; i++) {
                    pstmt.setString(1, "TxRecord " + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "Transaction record " + i);
                    pstmt.addBatch();
                    
                    if (i % 500 == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
            }
            
            connection.commit();
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            assertThat(getTableRowCount("performance_test")).isEqualTo(5000);
            printTestInfo("事务中插入5000条记录耗时: " + duration + "ms");
            printSuccess("事务中批量操作测试通过");
            
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("性能测试 - 聚合查询")
    public void testAggregateQuery() throws SQLException {
        insertTestData(10000);
        
        long startTime = System.currentTimeMillis();
        
        try (ResultSet rs = executeQuery("SELECT COUNT(*), SUM(value), AVG(value), MAX(value), MIN(value) FROM performance_test")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(10000);
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        printTestInfo("聚合查询10000条记录耗时: " + duration + "ms");
        printSuccess("聚合查询测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("性能测试 - GROUP BY查询")
    public void testGroupByQuery() throws SQLException {
        // 插入分组数据
        String sql = "INSERT INTO performance_test (name, value, description) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 1; i <= 1000; i++) {
                int group = i % 10; // 10个分组
                pstmt.setString(1, "Group " + group);
                pstmt.setInt(2, i);
                pstmt.setString(3, "Record in group " + group);
                pstmt.addBatch();
                
                if (i % 100 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }
        
        long startTime = System.currentTimeMillis();
        
        try (ResultSet rs = executeQuery("SELECT name, COUNT(*), SUM(value) FROM performance_test GROUP BY name")) {
            int groupCount = 0;
            while (rs.next()) {
                groupCount++;
            }
            assertThat(groupCount).isEqualTo(10);
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        printTestInfo("GROUP BY查询耗时: " + duration + "ms");
        printSuccess("GROUP BY查询测试通过");
    }
    
    /**
     * 辅助方法：插入测试数据
     */
    private void insertTestData(int count) throws SQLException {
        String sql = "INSERT INTO performance_test (name, value, description) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 1; i <= count; i++) {
                pstmt.setString(1, "Record " + i);
                pstmt.setInt(2, i);
                pstmt.setString(3, "Description " + i);
                pstmt.addBatch();
                
                if (i % 500 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }
    }
}

