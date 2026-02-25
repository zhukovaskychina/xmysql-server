package com.xmysql.server.test;

import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * SELECT查询测试类
 * 测试各种SELECT查询语句
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SelectQueryTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_select_queries";
    
    @BeforeAll
    public static void setUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
        connection.createStatement().executeUpdate("USE " + TEST_DB);
        
        // 创建员工表
        connection.createStatement().executeUpdate("""
            CREATE TABLE employees (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(50),
                salary DECIMAL(10,2),
                hire_date DATE,
                age INT
            )
            """);
        
        // 插入测试数据
        connection.createStatement().executeUpdate("""
            INSERT INTO employees (name, department, salary, hire_date, age) VALUES
            ('Alice', 'Engineering', 80000.00, '2020-01-15', 28),
            ('Bob', 'Sales', 60000.00, '2019-03-20', 32),
            ('Charlie', 'Engineering', 90000.00, '2018-06-10', 35),
            ('Diana', 'HR', 55000.00, '2021-02-01', 26),
            ('Eve', 'Sales', 65000.00, '2020-08-15', 29),
            ('Frank', 'Engineering', 95000.00, '2017-11-05', 40),
            ('Grace', 'HR', 58000.00, '2022-01-10', 24)
            """);
    }
    
    @AfterAll
    public static void cleanUpTestDatabase() throws SQLException {
        connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    
    @Test
    @Order(1)
    @DisplayName("测试SELECT - 查询所有列")
    public void testSelectAllColumns() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT * FROM employees")) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertThat(rs.getString("name")).isNotNull();
            }
            assertThat(count).isEqualTo(7);
        }
        
        printSuccess("查询所有列测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试SELECT - 查询指定列")
    public void testSelectSpecificColumns() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT name, department FROM employees")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isNotNull();
            assertThat(rs.getString("department")).isNotNull();
            
            // 验证只有这两列
            assertThat(rs.getMetaData().getColumnCount()).isEqualTo(2);
        }
        
        printSuccess("查询指定列测试通过");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试SELECT - WHERE条件")
    public void testSelectWithWhere() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT * FROM employees WHERE department = 'Engineering'")) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertThat(rs.getString("department")).isEqualTo("Engineering");
            }
            assertThat(count).isEqualTo(3);
        }
        
        printSuccess("WHERE条件测试通过");
    }
    
    @Test
    @Order(4)
    @DisplayName("测试SELECT - 多个WHERE条件（AND）")
    public void testSelectWithMultipleWhereAnd() throws SQLException {
        String sql = "SELECT * FROM employees WHERE department = 'Engineering' AND salary > 85000";
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertThat(rs.getString("department")).isEqualTo("Engineering");
                assertThat(rs.getBigDecimal("salary").doubleValue()).isGreaterThan(85000);
            }
            assertThat(count).isEqualTo(2); // Charlie and Frank
        }
        
        printSuccess("多个AND条件测试通过");
    }
    
    @Test
    @Order(5)
    @DisplayName("测试SELECT - 多个WHERE条件（OR）")
    public void testSelectWithMultipleWhereOr() throws SQLException {
        String sql = "SELECT * FROM employees WHERE department = 'HR' OR department = 'Sales'";
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                String dept = rs.getString("department");
                assertThat(dept).isIn("HR", "Sales");
            }
            assertThat(count).isEqualTo(4); // Bob, Diana, Eve, Grace
        }
        
        printSuccess("多个OR条件测试通过");
    }
    
    @Test
    @Order(6)
    @DisplayName("测试SELECT - ORDER BY升序")
    public void testSelectOrderByAsc() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT name, salary FROM employees ORDER BY salary ASC")) {
            double previousSalary = 0;
            while (rs.next()) {
                double currentSalary = rs.getBigDecimal("salary").doubleValue();
                assertThat(currentSalary).isGreaterThanOrEqualTo(previousSalary);
                previousSalary = currentSalary;
            }
        }
        
        printSuccess("ORDER BY ASC测试通过");
    }
    
    @Test
    @Order(7)
    @DisplayName("测试SELECT - ORDER BY降序")
    public void testSelectOrderByDesc() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT name, salary FROM employees ORDER BY salary DESC")) {
            double previousSalary = Double.MAX_VALUE;
            while (rs.next()) {
                double currentSalary = rs.getBigDecimal("salary").doubleValue();
                assertThat(currentSalary).isLessThanOrEqualTo(previousSalary);
                previousSalary = currentSalary;
            }
        }
        
        printSuccess("ORDER BY DESC测试通过");
    }
    
    @Test
    @Order(8)
    @DisplayName("测试SELECT - LIMIT")
    public void testSelectWithLimit() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT * FROM employees LIMIT 3")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertThat(count).isEqualTo(3);
        }
        
        printSuccess("LIMIT测试通过");
    }
    
    @Test
    @Order(9)
    @DisplayName("测试SELECT - LIMIT with OFFSET")
    public void testSelectWithLimitOffset() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT * FROM employees LIMIT 2 OFFSET 3")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertThat(count).isEqualTo(2);
        }
        
        printSuccess("LIMIT OFFSET测试通过");
    }
    
    @Test
    @Order(10)
    @DisplayName("测试SELECT - COUNT聚合函数")
    public void testSelectCount() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT COUNT(*) as total FROM employees")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("total")).isEqualTo(7);
        }
        
        printSuccess("COUNT聚合函数测试通过");
    }
    
    @Test
    @Order(11)
    @DisplayName("测试SELECT - SUM聚合函数")
    public void testSelectSum() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT SUM(salary) as total_salary FROM employees")) {
            assertThat(rs.next()).isTrue();
            double totalSalary = rs.getBigDecimal("total_salary").doubleValue();
            assertThat(totalSalary).isGreaterThan(0);
        }
        
        printSuccess("SUM聚合函数测试通过");
    }
    
    @Test
    @Order(12)
    @DisplayName("测试SELECT - AVG聚合函数")
    public void testSelectAvg() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT AVG(salary) as avg_salary FROM employees")) {
            assertThat(rs.next()).isTrue();
            double avgSalary = rs.getBigDecimal("avg_salary").doubleValue();
            assertThat(avgSalary).isBetween(60000.0, 80000.0);
        }
        
        printSuccess("AVG聚合函数测试通过");
    }
    
    @Test
    @Order(13)
    @DisplayName("测试SELECT - MAX和MIN聚合函数")
    public void testSelectMaxMin() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT MAX(salary) as max_sal, MIN(salary) as min_sal FROM employees")) {
            assertThat(rs.next()).isTrue();
            double maxSalary = rs.getBigDecimal("max_sal").doubleValue();
            double minSalary = rs.getBigDecimal("min_sal").doubleValue();
            
            assertThat(maxSalary).isEqualTo(95000.00);
            assertThat(minSalary).isEqualTo(55000.00);
        }
        
        printSuccess("MAX/MIN聚合函数测试通过");
    }
    
    @Test
    @Order(14)
    @DisplayName("测试SELECT - GROUP BY")
    public void testSelectGroupBy() throws SQLException {
        String sql = "SELECT department, COUNT(*) as emp_count FROM employees GROUP BY department";
        
        try (ResultSet rs = executeQuery(sql)) {
            int groupCount = 0;
            while (rs.next()) {
                groupCount++;
                String dept = rs.getString("department");
                int count = rs.getInt("emp_count");
                
                if ("Engineering".equals(dept)) {
                    assertThat(count).isEqualTo(3);
                } else if ("Sales".equals(dept)) {
                    assertThat(count).isEqualTo(2);
                } else if ("HR".equals(dept)) {
                    assertThat(count).isEqualTo(2);
                }
            }
            assertThat(groupCount).isEqualTo(3);
        }
        
        printSuccess("GROUP BY测试通过");
    }
    
    @Test
    @Order(15)
    @DisplayName("测试SELECT - GROUP BY with HAVING")
    public void testSelectGroupByHaving() throws SQLException {
        String sql = "SELECT department, AVG(salary) as avg_sal FROM employees GROUP BY department HAVING AVG(salary) > 60000";
        
        try (ResultSet rs = executeQuery(sql)) {
            while (rs.next()) {
                double avgSal = rs.getBigDecimal("avg_sal").doubleValue();
                assertThat(avgSal).isGreaterThan(60000);
            }
        }
        
        printSuccess("GROUP BY HAVING测试通过");
    }
    
    @Test
    @Order(16)
    @DisplayName("测试SELECT - DISTINCT")
    public void testSelectDistinct() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT DISTINCT department FROM employees")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertThat(count).isEqualTo(3); // Engineering, Sales, HR
        }
        
        printSuccess("DISTINCT测试通过");
    }
    
    @Test
    @Order(17)
    @DisplayName("测试SELECT - LIKE模糊查询")
    public void testSelectLike() throws SQLException {
        try (ResultSet rs = executeQuery("SELECT * FROM employees WHERE name LIKE 'A%'")) {
            while (rs.next()) {
                String name = rs.getString("name");
                assertThat(name).startsWith("A");
            }
        }
        
        printSuccess("LIKE模糊查询测试通过");
    }
    
    @Test
    @Order(18)
    @DisplayName("测试SELECT - IN条件")
    public void testSelectIn() throws SQLException {
        String sql = "SELECT * FROM employees WHERE department IN ('Engineering', 'HR')";
        
        try (ResultSet rs = executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                String dept = rs.getString("department");
                assertThat(dept).isIn("Engineering", "HR");
            }
            assertThat(count).isEqualTo(5);
        }
        
        printSuccess("IN条件测试通过");
    }
    
    @Test
    @Order(19)
    @DisplayName("测试SELECT - BETWEEN条件")
    public void testSelectBetween() throws SQLException {
        String sql = "SELECT * FROM employees WHERE salary BETWEEN 60000 AND 80000";
        
        try (ResultSet rs = executeQuery(sql)) {
            while (rs.next()) {
                double salary = rs.getBigDecimal("salary").doubleValue();
                assertThat(salary).isBetween(60000.0, 80000.0);
            }
        }
        
        printSuccess("BETWEEN条件测试通过");
    }
}

