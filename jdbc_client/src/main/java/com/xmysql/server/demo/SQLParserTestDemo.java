package com.xmysql.server.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 通过 JDBC 对常见 SQL 语句进行解析和执行验证。
 *
 * <p>此程序需要一个正在运行的 XMySQL 服务。测试数据只写入本次运行创建的临时数据库，
 * 结束时会删除该数据库。</p>
 */
public class SQLParserTestDemo {
    private static final String DEFAULT_URL =
            "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String TEST_DATABASE = "parser_demo_" + UUID.randomUUID().toString().replace("-", "");
    private static final List<String> failures = new ArrayList<>();
    private static int passed;

    private SQLParserTestDemo() {}

    public static void main(String[] args) throws Exception {
        String url = configured("xmysql.jdbc.url", "XMYSQL_JDBC_URL", DEFAULT_URL);
        String user = configured("xmysql.jdbc.user", "XMYSQL_JDBC_USER", "root");
        String password = configured("xmysql.jdbc.password", "XMYSQL_JDBC_PASSWORD", "root@1234");

        System.out.println("SQL 解析与执行用例开始");
        System.out.println("连接地址: " + url);
        Class.forName("com.mysql.cj.jdbc.Driver");

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            try {
                prepareDatabase(connection);
                testSelectStatements(connection);
                testInvalidStatements(connection);
                testDDLStatements(connection);
                testDMLStatements(connection);
                testTransactionStatements(connection);
                testShowStatements(connection);
            } finally {
                dropTestDatabase(connection);
            }
        }

        System.out.printf("\n用例结果：通过 %d，失败 %d%n", passed, failures.size());
        if (!failures.isEmpty()) {
            failures.forEach(failure -> System.err.println("  - " + failure));
            throw new IllegalStateException("SQL 解析与执行测试失败");
        }
        System.out.println("所有 SQL 解析与执行用例均已通过。");
    }

    private static void prepareDatabase(Connection connection) throws SQLException {
        executeSetup(connection, "CREATE DATABASE " + TEST_DATABASE);
        executeSetup(connection, "USE " + TEST_DATABASE);
        executeSetup(connection, """
                CREATE TABLE parser_employees (
                    id INT PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    department VARCHAR(30),
                    salary DECIMAL(10, 2),
                    manager_id INT
                )
                """);
        executeSetup(connection, """
                CREATE TABLE parser_departments (
                    id INT PRIMARY KEY,
                    name VARCHAR(30) NOT NULL
                )
                """);
        executeSetup(connection, """
                INSERT INTO parser_employees (id, name, department, salary, manager_id) VALUES
                (1, 'Alice', 'Engineering', 80000.00, NULL),
                (2, 'Bob', 'Sales', 60000.00, 1),
                (3, 'Charlie', 'Engineering', 90000.00, 1),
                (4, 'Diana', 'HR', 55000.00, NULL),
                (5, 'Eve', 'Sales', 65000.00, 2)
                """);
        executeSetup(connection, """
                INSERT INTO parser_departments (id, name) VALUES
                (1, 'Engineering'), (2, 'Sales'), (3, 'HR')
                """);
    }

    private static void testSelectStatements(Connection connection) {
        System.out.println("\n1. SELECT 查询");
        runQuery(connection, "常量与别名", "SELECT 1 AS value", 1, 1);
        runQuery(connection, "指定列与比较条件",
                "SELECT name FROM parser_employees WHERE id >= 2 AND id <= 3", 1, 2);
        runQuery(connection, "括号与 OR 条件",
                "SELECT id FROM parser_employees WHERE (department = 'HR' OR department = 'Sales') AND id > 2",
                1, 3);
        runQuery(connection, "LIKE 条件",
                "SELECT id FROM parser_employees WHERE name LIKE 'A%'", 1, 1);
        runQuery(connection, "IN 条件",
                "SELECT id FROM parser_employees WHERE department IN ('Engineering', 'HR')", 1, 3);
        runQuery(connection, "BETWEEN 条件",
                "SELECT id FROM parser_employees WHERE salary BETWEEN 60000 AND 80000", 1, 3);
        runQuery(connection, "IS NULL 条件",
                "SELECT id FROM parser_employees WHERE manager_id IS NULL", 1, 2);
        runQuery(connection, "INNER JOIN",
                "SELECT e.name, d.name AS department_name FROM parser_employees e " +
                        "JOIN parser_departments d ON e.department = d.name", 2, 5);
        runQuery(connection, "聚合与 GROUP BY",
                "SELECT department, COUNT(*) AS employee_count FROM parser_employees " +
                        "GROUP BY department HAVING COUNT(*) > 1", 2, 2);
        runQuery(connection, "DISTINCT、排序与 LIMIT/OFFSET",
                "SELECT DISTINCT department FROM parser_employees ORDER BY department LIMIT 2 OFFSET 1", 1, 2);
        runQuery(connection, "IN 子查询",
                "SELECT name FROM parser_employees WHERE department IN " +
                        "(SELECT name FROM parser_departments WHERE id <= 2)", 1, 4);
    }

    private static void testDDLStatements(Connection connection) {
        System.out.println("\n3. DDL 语句");
        runUpdate(connection, "CREATE TABLE", "CREATE TABLE parser_ddl (id INT PRIMARY KEY, note VARCHAR(30))");
        runUpdate(connection, "ALTER TABLE ADD COLUMN", "ALTER TABLE parser_ddl ADD COLUMN created_at DATE");
        runUpdate(connection, "CREATE INDEX", "CREATE INDEX idx_parser_ddl_note ON parser_ddl (note)");
        runUpdate(connection, "DROP INDEX", "DROP INDEX idx_parser_ddl_note ON parser_ddl");
        runUpdate(connection, "DROP TABLE", "DROP TABLE parser_ddl");
    }

    private static void testInvalidStatements(Connection connection) {
        System.out.println("\n2. 非法 SQL 拒绝检查");
        runRejected(connection, "错误的 SELECT 关键字", "SELEC 1");
        runRejected(connection, "缺少 FROM 表名", "SELECT * FROM");
        runRejected(connection, "WHERE 条件不完整", "SELECT id FROM parser_employees WHERE");
    }

    private static void testDMLStatements(Connection connection) {
        System.out.println("\n4. DML 语句");
        runUpdate(connection, "INSERT 单行", "INSERT INTO parser_employees (id, name, department, salary) " +
                "VALUES (6, 'Frank', 'Engineering', 95000.00)");
        runQuery(connection, "检查 INSERT 结果",
                "SELECT id FROM parser_employees WHERE id = 6", 1, 1);
        runUpdate(connection, "UPDATE", "UPDATE parser_employees SET salary = salary + 5000 WHERE id = 6");
        runQuery(connection, "检查 UPDATE 结果",
                "SELECT id FROM parser_employees WHERE id = 6 AND salary = 100000", 1, 1);
        runUpdate(connection, "DELETE", "DELETE FROM parser_employees WHERE id = 6");
        runQuery(connection, "检查 DELETE 结果",
                "SELECT id FROM parser_employees WHERE id = 6", 1, 0);
    }

    private static void testTransactionStatements(Connection connection) {
        System.out.println("\n5. 事务语句");
        runUpdate(connection, "START TRANSACTION", "START TRANSACTION");
        runUpdate(connection, "事务内 INSERT", "INSERT INTO parser_employees (id, name) VALUES (7, 'Rollback')");
        runUpdate(connection, "ROLLBACK", "ROLLBACK");
        runQuery(connection, "回滚后数据不存在",
                "SELECT id FROM parser_employees WHERE id = 7", 1, 0);

        runUpdate(connection, "START TRANSACTION", "START TRANSACTION");
        runUpdate(connection, "事务内 INSERT", "INSERT INTO parser_employees (id, name) VALUES (8, 'Commit')");
        runUpdate(connection, "COMMIT", "COMMIT");
        runQuery(connection, "提交后数据存在",
                "SELECT id FROM parser_employees WHERE id = 8", 1, 1);
    }

    private static void testShowStatements(Connection connection) {
        System.out.println("\n6. SHOW 语句");
        runQueryAtLeastRows(connection, "SHOW DATABASES", "SHOW DATABASES", 1, 1);
        runQuery(connection, "SHOW TABLES", "SHOW TABLES", 1, 2);
        runQuery(connection, "SHOW COLUMNS", "SHOW COLUMNS FROM parser_employees", 6, 5);
        runQuery(connection, "SHOW INDEX", "SHOW INDEX FROM parser_employees", 1, 1);
    }

    private static void runQuery(Connection connection, String name, String sql, int expectedColumns, int expectedRows) {
        runQuery(connection, name, sql, expectedColumns, expectedRows, false);
    }

    private static void runQueryAtLeastRows(Connection connection, String name, String sql,
                                            int expectedColumns, int minimumRows) {
        runQuery(connection, name, sql, expectedColumns, minimumRows, true);
    }

    private static void runQuery(Connection connection, String name, String sql, int expectedColumns,
                                 int expectedRows, boolean minimumRows) {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            int actualColumns = resultSet.getMetaData().getColumnCount();
            int actualRows = 0;
            while (resultSet.next()) {
                actualRows++;
            }
            boolean rowCountMatches = minimumRows ? actualRows >= expectedRows : actualRows == expectedRows;
            if (actualColumns != expectedColumns || !rowCountMatches) {
                String rowExpectation = minimumRows ? "至少 " + expectedRows : "" + expectedRows;
                fail(name, sql, "期望 " + expectedColumns + " 列、" + rowExpectation + " 行，实际 " +
                        actualColumns + " 列、" + actualRows + " 行");
                return;
            }
            pass(name, sql);
        } catch (SQLException exception) {
            fail(name, sql, exception.getMessage());
        }
    }

    private static void runUpdate(Connection connection, String name, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            pass(name, sql);
        } catch (SQLException exception) {
            fail(name, sql, exception.getMessage());
        }
    }

    private static void runRejected(Connection connection, String name, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            fail(name, sql, "语句未报错，非法 SQL 被接受");
        } catch (SQLException expected) {
            pass(name, sql);
        }
    }

    private static void executeSetup(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private static void dropTestDatabase(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP DATABASE IF EXISTS " + TEST_DATABASE);
        }
    }

    private static void pass(String name, String sql) {
        passed++;
        System.out.printf("  通过：%s | %s%n", name, sql);
    }

    private static void fail(String name, String sql, String reason) {
        failures.add(name + " | " + sql + " | " + reason);
        System.err.printf("  失败：%s | %s | %s%n", name, sql, reason);
    }

    private static String configured(String property, String environment, String fallback) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank()) {
            value = System.getenv(environment);
        }
        return value == null || value.isBlank() ? fallback : value;
    }
}
