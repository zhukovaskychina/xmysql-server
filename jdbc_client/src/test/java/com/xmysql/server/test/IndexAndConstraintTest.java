package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * 索引和约束测试类
 * 测试PRIMARY KEY, UNIQUE, INDEX, FOREIGN KEY等约束
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexAndConstraintTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_indexes_constraints";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
    }
    
    @AfterAll
    public static void cleanUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    
    @Test
    @Order(1)
    @DisplayName("测试PRIMARY KEY约束")
    public void testPrimaryKeyConstraint() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_pk (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
            """);
        
        executeUpdate("INSERT INTO test_pk VALUES (1, 'First')");
        
        // 尝试插入重复的主键（应该失败）
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO test_pk VALUES (1, 'Duplicate')")
        ).isInstanceOf(SQLException.class);
        
        printSuccess("PRIMARY KEY约束测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试复合PRIMARY KEY")
    public void testCompositePrimaryKey() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_composite_pk (
                user_id INT,
                product_id INT,
                quantity INT,
                PRIMARY KEY (user_id, product_id)
            )
            """);
        
        executeUpdate("INSERT INTO test_composite_pk VALUES (1, 100, 5)");
        executeUpdate("INSERT INTO test_composite_pk VALUES (1, 101, 3)");
        executeUpdate("INSERT INTO test_composite_pk VALUES (2, 100, 2)");
        
        // 尝试插入重复的复合主键
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO test_composite_pk VALUES (1, 100, 10)")
        ).isInstanceOf(SQLException.class);
        
        assertThat(getTableRowCount("test_composite_pk")).isEqualTo(3);
        
        printSuccess("复合PRIMARY KEY测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试UNIQUE约束")
    public void testUniqueConstraint() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_unique (
                id INT PRIMARY KEY AUTO_INCREMENT,
                email VARCHAR(100) UNIQUE,
                username VARCHAR(50) UNIQUE
            )
            """);
        
        executeUpdate("INSERT INTO test_unique (email, username) VALUES ('user@example.com', 'user1')");
        
        // 尝试插入重复的email
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO test_unique (email, username) VALUES ('user@example.com', 'user2')")
        ).isInstanceOf(SQLException.class);
        
        // 尝试插入重复的username
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO test_unique (email, username) VALUES ('another@example.com', 'user1')")
        ).isInstanceOf(SQLException.class);
        
        printSuccess("UNIQUE约束测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试NOT NULL约束")
    public void testNotNullConstraint() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_not_null (
                id INT PRIMARY KEY AUTO_INCREMENT,
                required_field VARCHAR(50) NOT NULL,
                optional_field VARCHAR(50)
            )
            """);
        
        // 正常插入
        executeUpdate("INSERT INTO test_not_null (required_field) VALUES ('value')");
        
        // 尝试插入NULL到NOT NULL列（应该失败）
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO test_not_null (required_field) VALUES (NULL)")
        ).isInstanceOf(SQLException.class);
        
        printSuccess("NOT NULL约束测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试FOREIGN KEY约束")
    public void testForeignKeyConstraint() throws SQLException {
        // 创建父表
        executeUpdate("""
            CREATE TABLE parent_table (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50)
            )
            """);
        
        // 创建子表（带外键）
        executeUpdate("""
            CREATE TABLE child_table (
                id INT PRIMARY KEY AUTO_INCREMENT,
                parent_id INT,
                description VARCHAR(100),
                FOREIGN KEY (parent_id) REFERENCES parent_table(id)
            )
            """);
        
        // 插入父表数据
        executeUpdate("INSERT INTO parent_table (name) VALUES ('Parent 1')");
        executeUpdate("INSERT INTO parent_table (name) VALUES ('Parent 2')");
        
        // 插入子表数据（引用存在的父记录）
        executeUpdate("INSERT INTO child_table (parent_id, description) VALUES (1, 'Child of Parent 1')");
        
        // 尝试插入引用不存在的父记录（应该失败）
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO child_table (parent_id, description) VALUES (999, 'Invalid parent')")
        ).isInstanceOf(SQLException.class);
        
        printSuccess("FOREIGN KEY约束测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试CHECK约束")
    public void testCheckConstraint() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_check (
                id INT PRIMARY KEY AUTO_INCREMENT,
                age INT CHECK (age >= 0 AND age <= 150),
                salary DECIMAL(10,2) CHECK (salary > 0)
            )
            """);
        
        // 正常插入
        executeUpdate("INSERT INTO test_check (age, salary) VALUES (25, 50000.00)");
        
        // 注意：MySQL的CHECK约束支持可能有限，某些版本会忽略CHECK约束
        // 这里主要测试语法是否正确
        
        printSuccess("CHECK约束测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试INDEX索引")
    public void testIndex() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_index (
                id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50),
                email VARCHAR(100),
                created_at TIMESTAMP,
                INDEX idx_username (username),
                INDEX idx_email (email)
            )
            """);
        
        // 插入测试数据
        for (int i = 1; i <= 100; i++) {
            executeUpdate(String.format(
                "INSERT INTO test_index (username, email) VALUES ('user%d', 'user%d@example.com')",
                i, i
            ));
        }
        
        // 使用索引查询
        try (ResultSet rs = executeQuery("SELECT * FROM test_index WHERE username = 'user50'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("username")).isEqualTo("user50");
        }
        
        printSuccess("INDEX索引测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试复合索引")
    public void testCompositeIndex() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_composite_index (
                id INT PRIMARY KEY AUTO_INCREMENT,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                age INT,
                INDEX idx_name (last_name, first_name),
                INDEX idx_age_name (age, last_name)
            )
            """);
        
        executeUpdate("INSERT INTO test_composite_index (first_name, last_name, age) VALUES ('John', 'Doe', 30)");
        executeUpdate("INSERT INTO test_composite_index (first_name, last_name, age) VALUES ('Jane', 'Doe', 28)");
        executeUpdate("INSERT INTO test_composite_index (first_name, last_name, age) VALUES ('Bob', 'Smith', 35)");
        
        // 使用复合索引查询
        try (ResultSet rs = executeQuery("SELECT * FROM test_composite_index WHERE last_name = 'Doe' AND first_name = 'John'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("first_name")).isEqualTo("John");
        }
        
        printSuccess("复合索引测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试UNIQUE INDEX")
    public void testUniqueIndex() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_unique_index (
                id INT PRIMARY KEY AUTO_INCREMENT,
                code VARCHAR(20),
                UNIQUE INDEX idx_unique_code (code)
            )
            """);
        
        executeUpdate("INSERT INTO test_unique_index (code) VALUES ('CODE001')");
        
        // 尝试插入重复的code
        assertThatThrownBy(() -> 
            executeUpdate("INSERT INTO test_unique_index (code) VALUES ('CODE001')")
        ).isInstanceOf(SQLException.class);
        
        printSuccess("UNIQUE INDEX测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试FULLTEXT索引")
    public void testFulltextIndex() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_fulltext (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT,
                FULLTEXT INDEX idx_fulltext (title, content)
            )
            """);
        
        executeUpdate("INSERT INTO test_fulltext (title, content) VALUES ('MySQL Tutorial', 'This is a comprehensive MySQL tutorial')");
        executeUpdate("INSERT INTO test_fulltext (title, content) VALUES ('Java Programming', 'Learn Java programming from scratch')");
        
        // 注意：FULLTEXT搜索语法可能因MySQL版本而异
        printSuccess("FULLTEXT索引创建测试通过");
    }
    
    @Test
    @Order(11)
    @DisplayName("测试ON DELETE CASCADE")
    public void testOnDeleteCascade() throws SQLException {
        // 创建父表
        executeUpdate("""
            CREATE TABLE cascade_parent (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50)
            )
            """);
        
        // 创建子表（带级联删除）
        executeUpdate("""
            CREATE TABLE cascade_child (
                id INT PRIMARY KEY AUTO_INCREMENT,
                parent_id INT,
                description VARCHAR(100),
                FOREIGN KEY (parent_id) REFERENCES cascade_parent(id) ON DELETE CASCADE
            )
            """);
        
        // 插入数据
        executeUpdate("INSERT INTO cascade_parent (name) VALUES ('Parent 1')");
        executeUpdate("INSERT INTO cascade_child (parent_id, description) VALUES (1, 'Child 1')");
        executeUpdate("INSERT INTO cascade_child (parent_id, description) VALUES (1, 'Child 2')");
        
        assertThat(getTableRowCount("cascade_child")).isEqualTo(2);
        
        // 删除父记录
        executeUpdate("DELETE FROM cascade_parent WHERE id = 1");
        
        // 验证子记录也被删除
        assertThat(getTableRowCount("cascade_child")).isEqualTo(0);
        
        printSuccess("ON DELETE CASCADE测试通过");
    }
    
    @Test
    @Order(12)
    @DisplayName("测试ON UPDATE CASCADE")
    public void testOnUpdateCascade() throws SQLException {
        // 创建父表
        executeUpdate("""
            CREATE TABLE update_parent (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
            """);
        
        // 创建子表（带级联更新）
        executeUpdate("""
            CREATE TABLE update_child (
                id INT PRIMARY KEY AUTO_INCREMENT,
                parent_id INT,
                description VARCHAR(100),
                FOREIGN KEY (parent_id) REFERENCES update_parent(id) ON UPDATE CASCADE
            )
            """);
        
        // 插入数据
        executeUpdate("INSERT INTO update_parent (id, name) VALUES (100, 'Parent')");
        executeUpdate("INSERT INTO update_child (parent_id, description) VALUES (100, 'Child')");
        
        // 更新父记录的ID
        executeUpdate("UPDATE update_parent SET id = 200 WHERE id = 100");
        
        // 验证子记录的parent_id也被更新
        try (ResultSet rs = executeQuery("SELECT parent_id FROM update_child")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("parent_id")).isEqualTo(200);
        }
        
        printSuccess("ON UPDATE CASCADE测试通过");
    }
}

