package com.xmysql.server.test;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * DDL操作测试类
 * 测试数据库和表的创建、删除等DDL操作
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DDLOperationsTest extends BaseIntegrationTest {
    
    private static final String TEST_DB_PREFIX = "test_ddl_db_";
    private static final String TEST_TABLE_PREFIX = "test_table_";
    
    @AfterEach
    public void cleanUp() throws SQLException {
        // 清理测试数据库
        for (int i = 1; i <= 5; i++) {
            dropDatabaseIfExists(TEST_DB_PREFIX + i);
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("测试创建数据库")
    public void testCreateDatabase() throws SQLException {
        String dbName = TEST_DB_PREFIX + "1";
        
        // 创建数据库
        int result = executeUpdate("CREATE DATABASE " + dbName);
        
        // 验证数据库已创建
        assertThat(databaseExists(dbName)).isTrue();
        printSuccess("数据库 " + dbName + " 创建成功");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试创建数据库 - IF NOT EXISTS")
    public void testCreateDatabaseIfNotExists() throws SQLException {
        String dbName = TEST_DB_PREFIX + "2";
        
        // 第一次创建
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName);
        assertThat(databaseExists(dbName)).isTrue();
        
        // 第二次创建（不应该报错）
        assertThatCode(() -> executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName))
            .doesNotThrowAnyException();
        
        printSuccess("IF NOT EXISTS 测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试创建数据库 - 指定字符集")
    public void testCreateDatabaseWithCharset() throws SQLException {
        String dbName = TEST_DB_PREFIX + "3";
        
        // 创建带字符集的数据库
        executeUpdate("CREATE DATABASE " + dbName + " CHARACTER SET utf8mb4");
        
        assertThat(databaseExists(dbName)).isTrue();
        printSuccess("带字符集的数据库创建成功");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试删除数据库")
    public void testDropDatabase() throws SQLException {
        String dbName = TEST_DB_PREFIX + "4";
        
        // 先创建数据库
        executeUpdate("CREATE DATABASE " + dbName);
        assertThat(databaseExists(dbName)).isTrue();
        
        // 删除数据库
        executeUpdate("DROP DATABASE " + dbName);
        assertThat(databaseExists(dbName)).isFalse();
        
        printSuccess("数据库删除成功");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试删除数据库 - IF EXISTS")
    public void testDropDatabaseIfExists() throws SQLException {
        String dbName = TEST_DB_PREFIX + "5";
        
        // 删除不存在的数据库（不应该报错）
        assertThatCode(() -> executeUpdate("DROP DATABASE IF EXISTS " + dbName))
            .doesNotThrowAnyException();
        
        printSuccess("DROP IF EXISTS 测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试创建表 - 基本类型")
    public void testCreateTableBasicTypes() throws SQLException {
        String dbName = TEST_DB_PREFIX + "table_test";
        String tableName = TEST_TABLE_PREFIX + "basic";
        
        createTestDatabase(dbName);
        useDatabase(dbName);
        
        String createTableSql = """
            CREATE TABLE %s (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                age INT,
                salary DECIMAL(10,2),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """.formatted(tableName);
        
        executeUpdate(createTableSql);
        assertThat(tableExists(tableName)).isTrue();
        
        printSuccess("基本类型表创建成功");
        
        dropDatabaseIfExists(dbName);
    }
    
    @Test
    @Order(7)
    @DisplayName("测试创建表 - 带索引")
    public void testCreateTableWithIndex() throws SQLException {
        String dbName = TEST_DB_PREFIX + "index_test";
        String tableName = TEST_TABLE_PREFIX + "indexed";
        
        createTestDatabase(dbName);
        useDatabase(dbName);
        
        String createTableSql = """
            CREATE TABLE %s (
                id INT PRIMARY KEY AUTO_INCREMENT,
                email VARCHAR(100) UNIQUE NOT NULL,
                username VARCHAR(50) NOT NULL,
                INDEX idx_username (username)
            )
            """.formatted(tableName);
        
        executeUpdate(createTableSql);
        assertThat(tableExists(tableName)).isTrue();
        
        printSuccess("带索引的表创建成功");
        
        dropDatabaseIfExists(dbName);
    }
    
    @Test
    @Order(8)
    @DisplayName("测试创建表 - 外键约束")
    public void testCreateTableWithForeignKey() throws SQLException {
        String dbName = TEST_DB_PREFIX + "fk_test";
        
        createTestDatabase(dbName);
        useDatabase(dbName);
        
        // 创建父表
        executeUpdate("""
            CREATE TABLE users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL
            )
            """);
        
        // 创建子表（带外键）
        executeUpdate("""
            CREATE TABLE orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_id INT,
                amount DECIMAL(10,2),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            """);
        
        assertThat(tableExists("users")).isTrue();
        assertThat(tableExists("orders")).isTrue();
        
        printSuccess("带外键的表创建成功");
        
        dropDatabaseIfExists(dbName);
    }
    
    @Test
    @Order(9)
    @DisplayName("测试删除表")
    public void testDropTable() throws SQLException {
        String dbName = TEST_DB_PREFIX + "drop_test";
        String tableName = TEST_TABLE_PREFIX + "drop";
        
        createTestDatabase(dbName);
        useDatabase(dbName);
        
        // 创建表
        executeUpdate("CREATE TABLE " + tableName + " (id INT PRIMARY KEY)");
        assertThat(tableExists(tableName)).isTrue();
        
        // 删除表
        executeUpdate("DROP TABLE " + tableName);
        assertThat(tableExists(tableName)).isFalse();
        
        printSuccess("表删除成功");
        
        dropDatabaseIfExists(dbName);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"utf8", "utf8mb4", "latin1"})
    @DisplayName("测试不同字符集的数据库创建")
    public void testCreateDatabaseWithDifferentCharsets(String charset) throws SQLException {
        String dbName = TEST_DB_PREFIX + "charset_" + charset;
        
        executeUpdate("CREATE DATABASE " + dbName + " CHARACTER SET " + charset);
        assertThat(databaseExists(dbName)).isTrue();
        
        printSuccess("字符集 " + charset + " 的数据库创建成功");
        
        dropDatabaseIfExists(dbName);
    }
    
    @Test
    @Order(10)
    @DisplayName("测试ALTER TABLE - 添加列")
    public void testAlterTableAddColumn() throws SQLException {
        String dbName = TEST_DB_PREFIX + "alter_test";
        String tableName = TEST_TABLE_PREFIX + "alter";
        
        createTestDatabase(dbName);
        useDatabase(dbName);
        
        // 创建表
        executeUpdate("CREATE TABLE " + tableName + " (id INT PRIMARY KEY)");
        
        // 添加列
        executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR(100)");
        executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN age INT DEFAULT 0");
        
        printSuccess("ALTER TABLE ADD COLUMN 测试通过");
        
        dropDatabaseIfExists(dbName);
    }
    
    @Test
    @Order(11)
    @DisplayName("测试TRUNCATE TABLE")
    public void testTruncateTable() throws SQLException {
        String dbName = TEST_DB_PREFIX + "truncate_test";
        String tableName = TEST_TABLE_PREFIX + "truncate";
        
        createTestDatabase(dbName);
        useDatabase(dbName);
        
        // 创建表并插入数据
        executeUpdate("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, name VARCHAR(50))");
        executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'test1'), (2, 'test2')");
        
        assertThat(getTableRowCount(tableName)).isEqualTo(2);
        
        // 清空表
        executeUpdate("TRUNCATE TABLE " + tableName);
        assertThat(getTableRowCount(tableName)).isEqualTo(0);
        
        printSuccess("TRUNCATE TABLE 测试通过");
        
        dropDatabaseIfExists(dbName);
    }
}

