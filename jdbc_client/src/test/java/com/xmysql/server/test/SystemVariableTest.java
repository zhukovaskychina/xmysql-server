package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * 系统变量测试类
 * 测试系统变量的查询和设置
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SystemVariableTest extends BaseIntegrationTest {
    
    @Test
    @Order(1)
    @DisplayName("测试查询系统变量 - @@version")
    public void testSelectVersion() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT @@version")) {
            assertThat(rs.next()).isTrue();
            String version = rs.getString(1);
            assertThat(version).isNotNull();
            printTestInfo("数据库版本: " + version);
        }
        
        printSuccess("@@version查询测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试查询系统变量 - @@character_set_client")
    public void testSelectCharacterSetClient() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT @@character_set_client")) {
            assertThat(rs.next()).isTrue();
            String charset = rs.getString(1);
            assertThat(charset).isNotNull();
            printTestInfo("客户端字符集: " + charset);
        }
        
        printSuccess("@@character_set_client查询测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试查询系统变量 - @@session.autocommit")
    public void testSelectAutocommit() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT @@session.autocommit")) {
            assertThat(rs.next()).isTrue();
            int autocommit = rs.getInt(1);
            assertThat(autocommit).isIn(0, 1);
            printTestInfo("自动提交状态: " + autocommit);
        }
        
        printSuccess("@@session.autocommit查询测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试查询系统变量 - @@global.port")
    public void testSelectGlobalPort() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT @@global.port")) {
            assertThat(rs.next()).isTrue();
            int port = rs.getInt(1);
            assertThat(port).isGreaterThan(0);
            printTestInfo("数据库端口: " + port);
        }
        
        printSuccess("@@global.port查询测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试查询多个系统变量")
    public void testSelectMultipleVariables() throws SQLException {
        String sql = "SELECT @@version, @@character_set_client, @@session.autocommit";
        
        try (ResultSet rs = executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isNotNull(); // version
            assertThat(rs.getString(2)).isNotNull(); // character_set_client
            assertThat(rs.getInt(3)).isIn(0, 1);     // autocommit
        }
        
        printSuccess("多个系统变量查询测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试SET系统变量 - autocommit")
    public void testSetAutocommit() throws SQLException {
        // 获取当前值
        int originalValue;
        try (ResultSet rs = executeQuery("SELECT @@session.autocommit")) {
            rs.next();
            originalValue = rs.getInt(1);
        }
        
        // 设置为相反的值
        int newValue = (originalValue == 1) ? 0 : 1;
        executeUpdate("SET @@session.autocommit = " + newValue);
        
        // 验证设置成功
        try (ResultSet rs = executeQuery("SELECT @@session.autocommit")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(newValue);
        }
        
        // 恢复原值
        executeUpdate("SET @@session.autocommit = " + originalValue);
        
        printSuccess("SET autocommit测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试SET系统变量 - character_set_results")
    public void testSetCharacterSetResults() throws SQLException {
        // 设置为NULL
        executeUpdate("SET character_set_results = NULL");
        
        try (ResultSet rs = executeQuery("SELECT @@character_set_results")) {
            rs.next();
            String value = rs.getString(1);
            // NULL值的处理
        }
        
        // 设置为utf8
        executeUpdate("SET character_set_results = 'utf8'");
        
        try (ResultSet rs = executeQuery("SELECT @@character_set_results")) {
            rs.next();
            String value = rs.getString(1);
            assertThat(value).isEqualToIgnoringCase("utf8");
        }
        
        printSuccess("SET character_set_results测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试SET系统变量 - time_zone")
    public void testSetTimeZone() throws SQLException {
        executeUpdate("SET @@session.time_zone = '+08:00'");
        
        try (ResultSet rs = executeQuery("SELECT @@session.time_zone")) {
            rs.next();
            String timeZone = rs.getString(1);
            assertThat(timeZone).isEqualTo("+08:00");
        }
        
        printSuccess("SET time_zone测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试SET系统变量 - sql_mode")
    public void testSetSqlMode() throws SQLException {
        // 获取当前sql_mode
        String originalMode;
        try (ResultSet rs = executeQuery("SELECT @@sql_mode")) {
            rs.next();
            originalMode = rs.getString(1);
            printTestInfo("原始SQL模式: " + originalMode);
        }
        
        // 设置新的sql_mode
        executeUpdate("SET sql_mode = 'STRICT_TRANS_TABLES'");
        
        try (ResultSet rs = executeQuery("SELECT @@sql_mode")) {
            rs.next();
            String newMode = rs.getString(1);
            assertThat(newMode).contains("STRICT_TRANS_TABLES");
        }
        
        printSuccess("SET sql_mode测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试SET NAMES")
    public void testSetNames() throws SQLException {
        executeUpdate("SET NAMES 'utf8mb4'");
        
        // 验证字符集已设置
        try (ResultSet rs = executeQuery("SELECT @@character_set_client, @@character_set_connection, @@character_set_results")) {
            rs.next();
            String client = rs.getString(1);
            String connection = rs.getString(2);
            String results = rs.getString(3);
            
            printTestInfo("Client: " + client + ", Connection: " + connection + ", Results: " + results);
        }
        
        printSuccess("SET NAMES测试通过");
    }
    
    @Test
    @Order(11)
    @DisplayName("测试SHOW VARIABLES")
    public void testShowVariables() throws SQLException {
        try (ResultSet rs = executeQuery("SHOW VARIABLES")) {
            int count = 0;
            while (rs.next() && count < 10) {
                String varName = rs.getString(1);
                String varValue = rs.getString(2);
                printTestInfo(varName + " = " + varValue);
                count++;
            }
            assertThat(count).isGreaterThan(0);
        }
        
        printSuccess("SHOW VARIABLES测试通过");
    }
    
    @Test
    @Order(12)
    @DisplayName("测试SHOW VARIABLES LIKE")
    public void testShowVariablesLike() throws SQLException {
        try (ResultSet rs = executeQuery("SHOW VARIABLES LIKE 'character_set%'")) {
            int count = 0;
            while (rs.next()) {
                String varName = rs.getString(1);
                assertThat(varName).startsWith("character_set");
                count++;
            }
            assertThat(count).isGreaterThan(0);
        }
        
        printSuccess("SHOW VARIABLES LIKE测试通过");
    }
    
    @Test
    @Order(13)
    @DisplayName("测试SHOW SESSION VARIABLES")
    public void testShowSessionVariables() throws SQLException {
        try (ResultSet rs = executeQuery("SHOW SESSION VARIABLES LIKE 'autocommit'")) {
            assertThat(rs.next()).isTrue();
            String varName = rs.getString(1);
            String varValue = rs.getString(2);
            
            assertThat(varName).isEqualToIgnoringCase("autocommit");
            assertThat(varValue).isIn("ON", "OFF", "0", "1");
        }
        
        printSuccess("SHOW SESSION VARIABLES测试通过");
    }
    
    @Test
    @Order(14)
    @DisplayName("测试SHOW GLOBAL VARIABLES")
    public void testShowGlobalVariables() throws SQLException {
        try (ResultSet rs = executeQuery("SHOW GLOBAL VARIABLES LIKE 'port'")) {
            assertThat(rs.next()).isTrue();
            String varName = rs.getString(1);
            assertThat(varName).isEqualToIgnoringCase("port");
        }
        
        printSuccess("SHOW GLOBAL VARIABLES测试通过");
    }
    
    @Test
    @Order(15)
    @DisplayName("测试SET多个变量")
    public void testSetMultipleVariables() throws SQLException {
        executeUpdate("SET autocommit = 1, sql_mode = 'TRADITIONAL'");
        
        try (ResultSet rs = executeQuery("SELECT @@autocommit, @@sql_mode")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString(2)).contains("TRADITIONAL");
        }
        
        printSuccess("SET多个变量测试通过");
    }
}

