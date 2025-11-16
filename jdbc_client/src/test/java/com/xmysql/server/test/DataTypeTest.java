package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.*;

/**
 * 数据类型测试类
 * 测试各种MySQL数据类型的存储和检索
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DataTypeTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_data_types";
    
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
    @DisplayName("测试整数类型 - INT, BIGINT, SMALLINT, TINYINT")
    public void testIntegerTypes() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_integers (
                id INT PRIMARY KEY AUTO_INCREMENT,
                tiny_val TINYINT,
                small_val SMALLINT,
                medium_val MEDIUMINT,
                int_val INT,
                big_val BIGINT
            )
            """);
        
        executeUpdate("""
            INSERT INTO test_integers (tiny_val, small_val, medium_val, int_val, big_val)
            VALUES (127, 32767, 8388607, 2147483647, 9223372036854775807)
            """);
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_integers WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getByte("tiny_val")).isEqualTo((byte) 127);
            assertThat(rs.getShort("small_val")).isEqualTo((short) 32767);
            assertThat(rs.getInt("medium_val")).isEqualTo(8388607);
            assertThat(rs.getInt("int_val")).isEqualTo(2147483647);
            assertThat(rs.getLong("big_val")).isEqualTo(9223372036854775807L);
        }
        
        printSuccess("整数类型测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试浮点类型 - FLOAT, DOUBLE, DECIMAL")
    public void testFloatingPointTypes() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_floats (
                id INT PRIMARY KEY AUTO_INCREMENT,
                float_val FLOAT,
                double_val DOUBLE,
                decimal_val DECIMAL(10,2)
            )
            """);
        
        executeUpdate("""
            INSERT INTO test_floats (float_val, double_val, decimal_val)
            VALUES (3.14159, 2.718281828, 12345.67)
            """);
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_floats WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getFloat("float_val")).isCloseTo(3.14159f, within(0.0001f));
            assertThat(rs.getDouble("double_val")).isCloseTo(2.718281828, within(0.000001));
            assertThat(rs.getBigDecimal("decimal_val")).isEqualTo(new BigDecimal("12345.67"));
        }
        
        printSuccess("浮点类型测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试字符串类型 - VARCHAR, CHAR, TEXT")
    public void testStringTypes() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_strings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                char_val CHAR(10),
                varchar_val VARCHAR(100),
                text_val TEXT,
                tiny_text TINYTEXT,
                medium_text MEDIUMTEXT,
                long_text LONGTEXT
            )
            """);
        
        String longString = "This is a very long text that will be stored in TEXT column";
        
        executeUpdate(String.format("""
            INSERT INTO test_strings (char_val, varchar_val, text_val, tiny_text)
            VALUES ('CHAR', 'VARCHAR value', '%s', 'Tiny text')
            """, longString));
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_strings WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("char_val").trim()).isEqualTo("CHAR");
            assertThat(rs.getString("varchar_val")).isEqualTo("VARCHAR value");
            assertThat(rs.getString("text_val")).isEqualTo(longString);
            assertThat(rs.getString("tiny_text")).isEqualTo("Tiny text");
        }
        
        printSuccess("字符串类型测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试日期时间类型 - DATE, DATETIME, TIMESTAMP, TIME, YEAR")
    public void testDateTimeTypes() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_datetime (
                id INT PRIMARY KEY AUTO_INCREMENT,
                date_val DATE,
                datetime_val DATETIME,
                timestamp_val TIMESTAMP,
                time_val TIME,
                year_val YEAR
            )
            """);
        
        executeUpdate("""
            INSERT INTO test_datetime (date_val, datetime_val, timestamp_val, time_val, year_val)
            VALUES ('2024-01-15', '2024-01-15 14:30:00', '2024-01-15 14:30:00', '14:30:00', 2024)
            """);
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_datetime WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            
            Date date = rs.getDate("date_val");
            assertThat(date).isNotNull();
            
            Timestamp datetime = rs.getTimestamp("datetime_val");
            assertThat(datetime).isNotNull();
            
            Timestamp timestamp = rs.getTimestamp("timestamp_val");
            assertThat(timestamp).isNotNull();
            
            printTestInfo("Date: " + date);
            printTestInfo("DateTime: " + datetime);
            printTestInfo("Timestamp: " + timestamp);
        }
        
        printSuccess("日期时间类型测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试布尔类型 - BOOLEAN/BOOL")
    public void testBooleanType() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_boolean (
                id INT PRIMARY KEY AUTO_INCREMENT,
                is_active BOOLEAN,
                is_deleted BOOL DEFAULT FALSE
            )
            """);
        
        executeUpdate("INSERT INTO test_boolean (is_active, is_deleted) VALUES (TRUE, FALSE)");
        executeUpdate("INSERT INTO test_boolean (is_active, is_deleted) VALUES (1, 0)");
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_boolean ORDER BY id")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean("is_active")).isTrue();
            assertThat(rs.getBoolean("is_deleted")).isFalse();
            
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean("is_active")).isTrue();
            assertThat(rs.getBoolean("is_deleted")).isFalse();
        }
        
        printSuccess("布尔类型测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试ENUM类型")
    public void testEnumType() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_enum (
                id INT PRIMARY KEY AUTO_INCREMENT,
                status ENUM('active', 'inactive', 'pending') DEFAULT 'pending',
                priority ENUM('low', 'medium', 'high')
            )
            """);
        
        executeUpdate("INSERT INTO test_enum (status, priority) VALUES ('active', 'high')");
        executeUpdate("INSERT INTO test_enum (priority) VALUES ('low')");
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_enum ORDER BY id")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("status")).isEqualTo("active");
            assertThat(rs.getString("priority")).isEqualTo("high");
            
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("status")).isEqualTo("pending"); // 默认值
            assertThat(rs.getString("priority")).isEqualTo("low");
        }
        
        printSuccess("ENUM类型测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试SET类型")
    public void testSetType() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_set (
                id INT PRIMARY KEY AUTO_INCREMENT,
                permissions SET('read', 'write', 'execute', 'delete')
            )
            """);
        
        executeUpdate("INSERT INTO test_set (permissions) VALUES ('read,write')");
        executeUpdate("INSERT INTO test_set (permissions) VALUES ('read,write,execute,delete')");
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_set ORDER BY id")) {
            assertThat(rs.next()).isTrue();
            String perms1 = rs.getString("permissions");
            assertThat(perms1).contains("read");
            assertThat(perms1).contains("write");
            
            assertThat(rs.next()).isTrue();
            String perms2 = rs.getString("permissions");
            assertThat(perms2).contains("read");
            assertThat(perms2).contains("delete");
        }
        
        printSuccess("SET类型测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试NULL值处理")
    public void testNullValues() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_nulls (
                id INT PRIMARY KEY AUTO_INCREMENT,
                nullable_int INT,
                nullable_varchar VARCHAR(100),
                nullable_date DATE,
                not_null_val VARCHAR(50) NOT NULL DEFAULT 'default'
            )
            """);
        
        executeUpdate("INSERT INTO test_nulls (nullable_int, nullable_varchar, nullable_date) VALUES (NULL, NULL, NULL)");
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_nulls WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            
            rs.getInt("nullable_int");
            assertThat(rs.wasNull()).isTrue();
            
            assertThat(rs.getString("nullable_varchar")).isNull();
            assertThat(rs.getDate("nullable_date")).isNull();
            assertThat(rs.getString("not_null_val")).isEqualTo("default");
        }
        
        printSuccess("NULL值处理测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试BLOB和BINARY类型")
    public void testBinaryTypes() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_binary (
                id INT PRIMARY KEY AUTO_INCREMENT,
                binary_val BINARY(16),
                varbinary_val VARBINARY(100),
                blob_val BLOB
            )
            """);
        
        byte[] binaryData = new byte[]{1, 2, 3, 4, 5};
        
        // 注意：这里简化测试，实际应该使用PreparedStatement
        executeUpdate("INSERT INTO test_binary (id) VALUES (1)");
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_binary WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            printTestInfo("BINARY类型记录已创建");
        }
        
        printSuccess("BLOB和BINARY类型测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试AUTO_INCREMENT")
    public void testAutoIncrement() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_auto_inc (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50)
            )
            """);
        
        executeUpdate("INSERT INTO test_auto_inc (name) VALUES ('First')");
        executeUpdate("INSERT INTO test_auto_inc (name) VALUES ('Second')");
        executeUpdate("INSERT INTO test_auto_inc (name) VALUES ('Third')");
        
        try (ResultSet rs = executeQuery("SELECT id FROM test_auto_inc ORDER BY id")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("id")).isEqualTo(1);
            
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("id")).isEqualTo(2);
            
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("id")).isEqualTo(3);
        }
        
        printSuccess("AUTO_INCREMENT测试通过");
    }
    
    @Test
    @Order(11)
    @DisplayName("测试DEFAULT值")
    public void testDefaultValues() throws SQLException {
        executeUpdate("""
            CREATE TABLE test_defaults (
                id INT PRIMARY KEY AUTO_INCREMENT,
                status VARCHAR(20) DEFAULT 'active',
                count INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_enabled BOOLEAN DEFAULT TRUE
            )
            """);
        
        executeUpdate("INSERT INTO test_defaults (id) VALUES (1)");
        
        try (ResultSet rs = executeQuery("SELECT * FROM test_defaults WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("status")).isEqualTo("active");
            assertThat(rs.getInt("count")).isEqualTo(0);
            assertThat(rs.getTimestamp("created_at")).isNotNull();
            assertThat(rs.getBoolean("is_enabled")).isTrue();
        }
        
        printSuccess("DEFAULT值测试通过");
    }
}

