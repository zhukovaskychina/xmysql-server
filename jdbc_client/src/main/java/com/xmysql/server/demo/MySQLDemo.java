package com.xmysql.server.demo;

import java.sql.*;

/**
 * MySQL 连接与 CRUD 演示，可重复执行（幂等）。
 * 流程：连接 → 确保库存在并 USE → 确保表存在 → 无数据则插入示例 → 查询展示。
 */
public class MySQLDemo {
    private static final String HOST = "localhost";
    private static final int PORT = 33090;
    private static final String USER = "root";
    private static final String PASSWORD = "root@1234";
    private static final String DATABASE = "demo_db";

    /** 连接时先不指定库，避免「no database selected」依赖服务端 createDatabaseIfNotExist */
    private static final String URL = "jdbc:mysql://" + HOST + ":" + PORT + "?useSSL=false";

    public static void main(String[] args) {
        try {
            try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
                System.out.println("数据库连接成功！");
                ensureDatabaseAndUse(conn);
                createTableIfNotExists(conn);
                if (!hasData(conn)) {
                    insertSampleData(conn);
                }
                queryData(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** 幂等：若库不存在则创建，并 USE 到该库（兼容服务端对 CREATE DATABASE IF NOT EXISTS 的差异实现） */
    private static void ensureDatabaseAndUse(Connection conn) throws SQLException {
        String useDb = "USE `" + DATABASE + "`";
        try (Statement stmt = conn.createStatement()) {
            try {
                stmt.execute(useDb);
            } catch (SQLException e) {
                // 库不存在则创建（部分服务端在库已存在时会对 IF NOT EXISTS 报错，故先 USE 再按需 CREATE）
                String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                if (msg.contains("unknown database") || msg.contains("no database")) {
                    try {
                        stmt.execute("CREATE DATABASE `" + DATABASE + "`");
                    } catch (SQLException createEx) {
                        String createMsg = createEx.getMessage() != null ? createEx.getMessage().toLowerCase() : "";
                        if (!createMsg.contains("already exists")) {
                            throw createEx;
                        }
                    }
                    stmt.execute(useDb);
                } else {
                    throw e;
                }
            }
            System.out.println("已选择数据库: " + DATABASE);
        }
    }

    /** 幂等：表不存在则创建 */
    private static void createTableIfNotExists(Connection conn) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id INT PRIMARY KEY AUTO_INCREMENT, " +
                "name VARCHAR(50) NOT NULL, " +
                "email VARCHAR(100) NOT NULL, " +
                "age INT" +
                ")";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("表 users 已就绪（已存在或刚创建）。");
        }
    }

    private static boolean hasData(Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) FROM users";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) > 0;
        }
    }

    /** 幂等：仅在无数据时插入，多次执行不会重复插入 */
    private static void insertSampleData(Connection conn) throws SQLException {
        String sql = "INSERT INTO users (name, email, age) VALUES " +
                "('张三', 'zhangsan@example.com', 25), " +
                "('李四', 'lisi@example.com', 30), " +
                "('王五', 'wangwu@example.com', 28)";
        try (Statement stmt = conn.createStatement()) {
            int rows = stmt.executeUpdate(sql);
            System.out.println("成功插入 " + rows + " 条示例数据。");
        }
    }

    private static void queryData(Connection conn) throws SQLException {
        String sql = "SELECT * FROM users";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("\n当前 users 表数据：");
            System.out.println("ID\t姓名\t邮箱\t\t\t年龄");
            System.out.println("----------------------------------------");
            while (rs.next()) {
                System.out.printf("%d\t%s\t%s\t%d%n",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("email"),
                        rs.getInt("age"));
            }
        }
    }
}
