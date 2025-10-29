import java.sql.*;

/**
 * JDBC PreparedStatement 集成测试
 * 
 * 使用方法:
 * 1. 确保 XMySQL Server 正在运行
 * 2. 添加 MySQL JDBC 驱动到 classpath
 * 3. 编译: javac -cp mysql-connector-java-8.0.28.jar PreparedStatementTest.java
 * 4. 运行: java -cp .:mysql-connector-java-8.0.28.jar PreparedStatementTest
 */
public class PreparedStatementTest {
    
    private static final String URL = "jdbc:mysql://localhost:3306/test";
    private static final String USER = "root";
    private static final String PASSWORD = "";
    
    public static void main(String[] args) {
        PreparedStatementTest test = new PreparedStatementTest();
        
        try {
            test.testBasicPreparedQuery();
            test.testMultipleParameters();
            test.testNullParameter();
            test.testStatementReuse();
            test.testBatchExecution();
            
            System.out.println("\n✅ All tests passed!");
        } catch (Exception e) {
            System.err.println("\n❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 测试1: 基本预编译查询
     */
    public void testBasicPreparedQuery() throws SQLException {
        System.out.println("\n=== Test 1: Basic Prepared Query ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // 准备语句
            String sql = "SELECT * FROM users WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // 绑定参数
                pstmt.setInt(1, 123);
                
                // 执行查询
                try (ResultSet rs = pstmt.executeQuery()) {
                    System.out.println("Query executed successfully");
                    
                    // 打印结果
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    
                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(metaData.getColumnName(i) + ": " + rs.getString(i) + " ");
                        }
                        System.out.println();
                    }
                }
            }
        }
        
        System.out.println("✓ Test 1 passed");
    }
    
    /**
     * 测试2: 多参数绑定
     */
    public void testMultipleParameters() throws SQLException {
        System.out.println("\n=== Test 2: Multiple Parameters ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "SELECT * FROM users WHERE age > ? AND city = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 18);
                pstmt.setString(2, "Beijing");
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    System.out.println("Query with multiple parameters executed successfully");
                    
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    System.out.println("Found " + count + " rows");
                }
            }
        }
        
        System.out.println("✓ Test 2 passed");
    }
    
    /**
     * 测试3: NULL值处理
     */
    public void testNullParameter() throws SQLException {
        System.out.println("\n=== Test 3: NULL Parameter ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "INSERT INTO users (name, email) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, "Charlie");
                pstmt.setNull(2, Types.VARCHAR);
                
                int affectedRows = pstmt.executeUpdate();
                System.out.println("Inserted " + affectedRows + " row(s) with NULL value");
            }
        }
        
        System.out.println("✓ Test 3 passed");
    }
    
    /**
     * 测试4: 语句重用
     */
    public void testStatementReuse() throws SQLException {
        System.out.println("\n=== Test 4: Statement Reuse ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "SELECT * FROM users WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // 执行10次，每次使用不同的参数
                for (int i = 1; i <= 10; i++) {
                    pstmt.setInt(1, i);
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            System.out.println("Found user with id=" + i);
                        }
                    }
                }
            }
        }
        
        System.out.println("✓ Test 4 passed");
    }
    
    /**
     * 测试5: 批量操作
     */
    public void testBatchExecution() throws SQLException {
        System.out.println("\n=== Test 5: Batch Execution ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "INSERT INTO users (name, age) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // 添加多个批次
                pstmt.setString(1, "Alice");
                pstmt.setInt(2, 25);
                pstmt.addBatch();
                
                pstmt.setString(1, "Bob");
                pstmt.setInt(2, 30);
                pstmt.addBatch();
                
                pstmt.setString(1, "Carol");
                pstmt.setInt(2, 28);
                pstmt.addBatch();
                
                // 执行批量操作
                int[] results = pstmt.executeBatch();
                
                System.out.println("Batch execution completed:");
                for (int i = 0; i < results.length; i++) {
                    System.out.println("  Batch " + i + ": " + results[i] + " row(s) affected");
                }
            }
        }
        
        System.out.println("✓ Test 5 passed");
    }
    
    /**
     * 测试6: 不同数据类型
     */
    public void testDifferentDataTypes() throws SQLException {
        System.out.println("\n=== Test 6: Different Data Types ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "INSERT INTO test_types (int_col, varchar_col, double_col, date_col) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 42);
                pstmt.setString(2, "Test String");
                pstmt.setDouble(3, 3.14159);
                pstmt.setDate(4, new java.sql.Date(System.currentTimeMillis()));
                
                int affectedRows = pstmt.executeUpdate();
                System.out.println("Inserted " + affectedRows + " row(s) with different data types");
            }
        }
        
        System.out.println("✓ Test 6 passed");
    }
    
    /**
     * 测试7: 性能对比（预编译 vs 普通查询）
     */
    public void testPerformanceComparison() throws SQLException {
        System.out.println("\n=== Test 7: Performance Comparison ===");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            int iterations = 1000;
            
            // 测试预编译语句
            long preparedStart = System.currentTimeMillis();
            String sql = "SELECT * FROM users WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (int i = 1; i <= iterations; i++) {
                    pstmt.setInt(1, i % 100);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            // 处理结果
                        }
                    }
                }
            }
            long preparedTime = System.currentTimeMillis() - preparedStart;
            
            // 测试普通查询
            long normalStart = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                for (int i = 1; i <= iterations; i++) {
                    String query = "SELECT * FROM users WHERE id = " + (i % 100);
                    try (ResultSet rs = stmt.executeQuery(query)) {
                        while (rs.next()) {
                            // 处理结果
                        }
                    }
                }
            }
            long normalTime = System.currentTimeMillis() - normalStart;
            
            System.out.println("Prepared Statement: " + preparedTime + "ms");
            System.out.println("Normal Statement: " + normalTime + "ms");
            System.out.println("Performance improvement: " + 
                String.format("%.2f", (double)normalTime / preparedTime) + "x");
        }
        
        System.out.println("✓ Test 7 passed");
    }
}

