package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;

import static org.assertj.core.api.Assertions.*;

/**
 * 事务测试类
 * 测试事务的COMMIT、ROLLBACK、SAVEPOINT等功能
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransactionTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_transactions";
    private static final String ACCOUNTS_TABLE = "accounts";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
        
        // 创建账户表
        connection.createStatement().executeUpdate("""
            CREATE TABLE accounts (
                id INT PRIMARY KEY AUTO_INCREMENT,
                account_name VARCHAR(100) NOT NULL,
                balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
            )
            """);
    }
    
    @AfterAll
    public static void cleanUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    
    @BeforeEach
    public void clearTable() throws SQLException {
        executeUpdate("TRUNCATE TABLE " + ACCOUNTS_TABLE);
        // 确保自动提交是开启的
        connection.setAutoCommit(true);
    }
    
    @Test
    @Order(1)
    @DisplayName("测试事务COMMIT")
    public void testTransactionCommit() throws SQLException {
        // 关闭自动提交
        connection.setAutoCommit(false);
        
        try {
            // 插入数据
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Alice', 1000.00)");
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Bob', 2000.00)");
            
            // 提交事务
            connection.commit();
            
            // 验证数据已保存
            assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(2);
            
            printSuccess("事务COMMIT测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("测试事务ROLLBACK")
    public void testTransactionRollback() throws SQLException {
        connection.setAutoCommit(false);
        
        try {
            // 插入数据
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Charlie', 3000.00)");
            
            // 验证数据在事务中存在
            assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(1);
            
            // 回滚事务
            connection.rollback();
            
            // 验证数据已回滚
            assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(0);
            
            printSuccess("事务ROLLBACK测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("测试转账事务 - 成功场景")
    public void testTransferMoneySuccess() throws SQLException {
        // 初始化账户
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Alice', 1000.00)");
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Bob', 500.00)");
        
        connection.setAutoCommit(false);
        
        try {
            // 从Alice转账200给Bob
            executeUpdate("UPDATE accounts SET balance = balance - 200 WHERE account_name = 'Alice'");
            executeUpdate("UPDATE accounts SET balance = balance + 200 WHERE account_name = 'Bob'");
            
            // 提交事务
            connection.commit();
            
            // 验证余额
            try (ResultSet rs = executeQuery("SELECT balance FROM accounts WHERE account_name = 'Alice'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBigDecimal("balance").doubleValue()).isEqualTo(800.00);
            }
            
            try (ResultSet rs = executeQuery("SELECT balance FROM accounts WHERE account_name = 'Bob'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBigDecimal("balance").doubleValue()).isEqualTo(700.00);
            }
            
            printSuccess("转账事务成功场景测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("测试转账事务 - 失败回滚场景")
    public void testTransferMoneyFailureRollback() throws SQLException {
        // 初始化账户
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Alice', 100.00)");
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Bob', 500.00)");
        
        connection.setAutoCommit(false);
        
        try {
            // 尝试从Alice转账200给Bob（余额不足）
            executeUpdate("UPDATE accounts SET balance = balance - 200 WHERE account_name = 'Alice'");
            
            // 检查余额是否为负
            try (ResultSet rs = executeQuery("SELECT balance FROM accounts WHERE account_name = 'Alice'")) {
                rs.next();
                double balance = rs.getBigDecimal("balance").doubleValue();
                
                if (balance < 0) {
                    // 余额不足，回滚事务
                    connection.rollback();
                    printTestInfo("检测到余额不足，事务已回滚");
                } else {
                    executeUpdate("UPDATE accounts SET balance = balance + 200 WHERE account_name = 'Bob'");
                    connection.commit();
                }
            }
            
            // 验证Alice的余额未改变
            try (ResultSet rs = executeQuery("SELECT balance FROM accounts WHERE account_name = 'Alice'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBigDecimal("balance").doubleValue()).isEqualTo(100.00);
            }
            
            printSuccess("转账失败回滚场景测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("测试SAVEPOINT - 部分回滚")
    public void testSavepoint() throws SQLException {
        connection.setAutoCommit(false);
        
        try {
            // 插入第一条记录
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Alice', 1000.00)");
            
            // 创建保存点
            Savepoint savepoint1 = connection.setSavepoint("savepoint1");
            
            // 插入第二条记录
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Bob', 2000.00)");
            
            // 回滚到保存点
            connection.rollback(savepoint1);
            
            // 插入第三条记录
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Charlie', 3000.00)");
            
            // 提交事务
            connection.commit();
            
            // 验证结果：应该有Alice和Charlie，没有Bob
            assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(2);
            
            try (ResultSet rs = executeQuery("SELECT account_name FROM accounts ORDER BY account_name")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("account_name")).isEqualTo("Alice");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("account_name")).isEqualTo("Charlie");
            }
            
            printSuccess("SAVEPOINT部分回滚测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(6)
    @DisplayName("测试多个SAVEPOINT")
    public void testMultipleSavepoints() throws SQLException {
        connection.setAutoCommit(false);
        
        try {
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Account1', 100.00)");
            Savepoint sp1 = connection.setSavepoint("sp1");
            
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Account2', 200.00)");
            Savepoint sp2 = connection.setSavepoint("sp2");
            
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Account3', 300.00)");
            Savepoint sp3 = connection.setSavepoint("sp3");
            
            executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('Account4', 400.00)");
            
            // 回滚到sp2
            connection.rollback(sp2);
            
            // 提交
            connection.commit();
            
            // 应该只有Account1和Account2
            assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(2);
            
            printSuccess("多个SAVEPOINT测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("测试事务隔离 - READ COMMITTED")
    public void testTransactionIsolationReadCommitted() throws SQLException {
        // 设置隔离级别
        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
        
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('TestAccount', 1000.00)");
        
        connection.setAutoCommit(false);
        
        try {
            executeUpdate("UPDATE accounts SET balance = 1500.00 WHERE account_name = 'TestAccount'");
            
            // 在提交前，其他事务应该看不到这个更新
            // 这里我们只是验证当前事务可以看到更新
            try (ResultSet rs = executeQuery("SELECT balance FROM accounts WHERE account_name = 'TestAccount'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBigDecimal("balance").doubleValue()).isEqualTo(1500.00);
            }
            
            connection.commit();
            
            printSuccess("事务隔离级别测试通过");
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    @Test
    @Order(8)
    @DisplayName("测试BEGIN和START TRANSACTION")
    public void testBeginTransaction() throws SQLException {
        // 使用BEGIN开始事务
        executeUpdate("BEGIN");
        
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('BeginTest', 500.00)");
        
        // 回滚
        executeUpdate("ROLLBACK");
        
        // 验证数据已回滚
        assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(0);
        
        // 使用START TRANSACTION
        executeUpdate("START TRANSACTION");
        executeUpdate("INSERT INTO accounts (account_name, balance) VALUES ('StartTest', 600.00)");
        executeUpdate("COMMIT");
        
        // 验证数据已提交
        assertThat(getTableRowCount(ACCOUNTS_TABLE)).isEqualTo(1);
        
        printSuccess("BEGIN/START TRANSACTION测试通过");
    }
}

