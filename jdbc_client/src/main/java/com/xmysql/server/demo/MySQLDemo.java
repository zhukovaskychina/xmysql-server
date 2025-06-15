package com.xmysql.server.demo;

import java.sql.*;

public class MySQLDemo {
    // 数据库连接信息
    private static final String URL = "jdbc:mysql://localhost:3309/demo_db?useSSL=false&createDatabaseIfNotExist=true&logger=com.mysql.jdbc.log.StandardLogger&profileSQL=true";
    private static final String USER = "root";
    private static final String PASSWORD = "root@1234";

    public static void main(String[] args) {
        try {
            // 注册MySQL驱动
            Class.forName("com.mysql.jdbc.Driver");
            
            // 建立连接
            try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
                System.out.println("数据库连接成功！");
                
                // 创建表（如果不存在）
                createTable(conn);
                
                // 检查是否有数据，如果没有则插入示例数据
                if (!hasData(conn)) {
                    insertSampleData(conn);
                }
                
                // 查询并显示数据
                queryData(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void createTable(Connection conn) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                    "id INT PRIMARY KEY AUTO_INCREMENT, " +
                    "name VARCHAR(50) NOT NULL, " +
                    "email VARCHAR(100) NOT NULL, " +
                    "age INT" +
                    ")";
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("表创建成功或已存在！");
        }
    }
    
    private static boolean hasData(Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) FROM users";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        }
        return false;
    }
    
    private static void insertSampleData(Connection conn) throws SQLException {
        String sql = "INSERT INTO users (name, email, age) VALUES " +
                    "('张三', 'zhangsan@example.com', 25), " +
                    "('李四', 'lisi@example.com', 30), " +
                    "('王五', 'wangwu@example.com', 28)";
        
        try (Statement stmt = conn.createStatement()) {
            int rows = stmt.executeUpdate(sql);
            System.out.println("成功插入 " + rows + " 条数据！");
        }
    }
    
    private static void queryData(Connection conn) throws SQLException {
        String sql = "SELECT * FROM users";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            System.out.println("\n当前用户表数据：");
            System.out.println("ID\t姓名\t邮箱\t\t\t年龄");
            System.out.println("----------------------------------------");
            
            while (rs.next()) {
                System.out.printf("%d\t%s\t%s\t%d%n",
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getString("email"),
                    rs.getInt("age")
                );
            }
        }
    }
}
