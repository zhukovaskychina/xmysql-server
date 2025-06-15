package com.xmysql.server.demo;

import java.sql.*;

public class SystemVariableTestDemo {
    private static final String URL = "jdbc:mysql://localhost:3309/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";

    public static void main(String[] args) {
        System.out.println(" XMySQL 系统变量查询测试");
        System.out.println("============================================================");

        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            System.out.println(" 数据库连接成功");
            
            // 测试简单的系统变量查询
            testSimpleSystemVariables(connection);
            
            // 测试复杂的多系统变量查询（JDBC连接时的查询）
            testComplexSystemVariables(connection);
            
            // 测试SET系统变量
            testSetSystemVariables(connection);
            
            System.out.println("\n 所有系统变量测试完成");
            
        } catch (SQLException e) {
            System.err.println(" 数据库连接失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testSimpleSystemVariables(Connection connection) throws SQLException {
        System.out.println("\n 1. 测试简单系统变量查询");
        System.out.println("----------------------------------------");

        String[] queries = {
            "SELECT @@version",
            "SELECT @@character_set_client",
            "SELECT @@session.autocommit",
            "SELECT @@global.port"
        };

        for (String query : queries) {
            System.out.printf(" 执行查询: %s\n", query);
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                if (rs.next()) {
                    System.out.printf("    结果: %s\n", rs.getString(1));
                } else {
                    System.out.println("    无结果");
                }
            } catch (SQLException e) {
                System.out.printf("    错误: %s\n", e.getMessage());
            }
        }
    }

    private static void testComplexSystemVariables(Connection connection) throws SQLException {
        System.out.println("\n 2. 测试复杂多系统变量查询");
        System.out.println("----------------------------------------");

        // 模拟JDBC连接时的系统变量查询
        String complexQuery = "SELECT " +
            "@@session.auto_increment_increment AS auto_increment_increment, " +
            "@@character_set_client AS character_set_client, " +
            "@@character_set_connection AS character_set_connection, " +
            "@@character_set_results AS character_set_results, " +
            "@@character_set_server AS character_set_server, " +
            "@@collation_server AS collation_server, " +
            "@@sql_mode AS sql_mode, " +
            "@@time_zone AS time_zone";

        System.out.printf(" 执行复杂查询: %s\n", complexQuery);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(complexQuery)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            System.out.printf("    返回 %d 列:\n", columnCount);
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("     - %s\n", metaData.getColumnName(i));
            }
            
            if (rs.next()) {
                System.out.println("   📄 数据:");
                for (int i = 1; i <= columnCount; i++) {
                    System.out.printf("     %s: %s\n", 
                        metaData.getColumnName(i), rs.getString(i));
                }
                System.out.println("    复杂查询成功");
            } else {
                System.out.println("    无结果");
            }
        } catch (SQLException e) {
            System.out.printf("    错误: %s\n", e.getMessage());
        }
    }

    private static void testSetSystemVariables(Connection connection) throws SQLException {
        System.out.println("\n 3. 测试SET系统变量");
        System.out.println("----------------------------------------");

        String[] setQueries = {
            "SET @@session.autocommit = 1",
            "SET character_set_results = NULL",
            "SET @@session.time_zone = '+08:00'"
        };

        for (String query : setQueries) {
            System.out.printf(" 执行SET: %s\n", query);
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(query);
                System.out.println("    SET成功");
            } catch (SQLException e) {
                System.out.printf("    错误: %s\n", e.getMessage());
            }
        }
    }
} 