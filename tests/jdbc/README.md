# JDBC PreparedStatement 集成测试

本目录包含 XMySQL Server 预编译语句功能的 JDBC 集成测试。

## 前置条件

1. **启动 XMySQL Server**
   ```bash
   cd /Users/zhukovasky/GolandProjects/xmysql-server
   go run main.go
   ```

2. **下载 MySQL JDBC 驱动**
   ```bash
   wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar
   ```

3. **准备测试数据库**
   ```sql
   CREATE DATABASE IF NOT EXISTS test;
   USE test;
   
   CREATE TABLE users (
       id INT PRIMARY KEY AUTO_INCREMENT,
       name VARCHAR(100),
       age INT,
       city VARCHAR(50),
       email VARCHAR(100)
   );
   
   INSERT INTO users (name, age, city, email) VALUES
       ('Alice', 25, 'Beijing', 'alice@example.com'),
       ('Bob', 30, 'Shanghai', 'bob@example.com'),
       ('Charlie', 28, 'Guangzhou', 'charlie@example.com');
   
   CREATE TABLE test_types (
       id INT PRIMARY KEY AUTO_INCREMENT,
       int_col INT,
       varchar_col VARCHAR(255),
       double_col DOUBLE,
       date_col DATE
   );
   ```

## 运行测试

### 方法1: 使用命令行

```bash
# 编译
javac -cp mysql-connector-java-8.0.28.jar PreparedStatementTest.java

# 运行
java -cp .:mysql-connector-java-8.0.28.jar PreparedStatementTest
```

### 方法2: 使用 Maven

创建 `pom.xml`:

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.xmysql</groupId>
    <artifactId>jdbc-test</artifactId>
    <version>1.0</version>
    
    <dependencies>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.28</version>
        </dependency>
    </dependencies>
</project>
```

运行:
```bash
mvn compile exec:java -Dexec.mainClass="PreparedStatementTest"
```

## 测试用例说明

### Test 1: 基本预编译查询
- **目的**: 验证基本的 PreparedStatement 功能
- **SQL**: `SELECT * FROM users WHERE id = ?`
- **验证点**:
  - COM_STMT_PREPARE 协议正确处理
  - COM_STMT_EXECUTE 协议正确处理
  - 参数绑定正确
  - 结果集返回正确

### Test 2: 多参数绑定
- **目的**: 验证多个参数的绑定
- **SQL**: `SELECT * FROM users WHERE age > ? AND city = ?`
- **验证点**:
  - 多个参数正确解析
  - 参数顺序正确
  - 不同类型参数（INT, VARCHAR）正确处理

### Test 3: NULL值处理
- **目的**: 验证 NULL 值的处理
- **SQL**: `INSERT INTO users (name, email) VALUES (?, ?)`
- **验证点**:
  - NULL 位图正确解析
  - NULL 值正确绑定到 SQL

### Test 4: 语句重用
- **目的**: 验证预编译语句可以重复使用
- **验证点**:
  - 同一个 PreparedStatement 可以执行多次
  - 每次执行使用不同的参数
  - 语句缓存正常工作

### Test 5: 批量操作
- **目的**: 验证批量执行功能
- **验证点**:
  - addBatch() 正常工作
  - executeBatch() 正常工作
  - 批量操作性能优于单条执行

### Test 6: 不同数据类型
- **目的**: 验证各种 MySQL 数据类型的支持
- **验证点**:
  - INT 类型
  - VARCHAR 类型
  - DOUBLE 类型
  - DATE 类型

### Test 7: 性能对比
- **目的**: 验证预编译语句的性能优势
- **验证点**:
  - 预编译语句比普通查询快 2-3 倍
  - 语句缓存有效减少解析开销

## 预期输出

```
=== Test 1: Basic Prepared Query ===
Query executed successfully
id: 123 name: Alice age: 25 city: Beijing email: alice@example.com
✓ Test 1 passed

=== Test 2: Multiple Parameters ===
Query with multiple parameters executed successfully
Found 2 rows
✓ Test 2 passed

=== Test 3: NULL Parameter ===
Inserted 1 row(s) with NULL value
✓ Test 3 passed

=== Test 4: Statement Reuse ===
Found user with id=1
Found user with id=2
Found user with id=3
✓ Test 4 passed

=== Test 5: Batch Execution ===
Batch execution completed:
  Batch 0: 1 row(s) affected
  Batch 1: 1 row(s) affected
  Batch 2: 1 row(s) affected
✓ Test 5 passed

=== Test 6: Different Data Types ===
Inserted 1 row(s) with different data types
✓ Test 6 passed

=== Test 7: Performance Comparison ===
Prepared Statement: 1234ms
Normal Statement: 3456ms
Performance improvement: 2.80x
✓ Test 7 passed

✅ All tests passed!
```

## 使用 Wireshark 验证协议

### 1. 启动 Wireshark 抓包

```bash
sudo wireshark
```

### 2. 过滤 MySQL 流量

过滤器: `tcp.port == 3306`

### 3. 验证协议包

#### COM_STMT_PREPARE 请求
```
Packet Type: 0x16 (COM_STMT_PREPARE)
SQL: SELECT * FROM users WHERE id = ?
```

#### COM_STMT_PREPARE 响应
```
Status: 0x00 (OK)
Statement ID: 0x00000001
Column Count: 0x0005
Param Count: 0x0001
```

#### COM_STMT_EXECUTE 请求
```
Packet Type: 0x17 (COM_STMT_EXECUTE)
Statement ID: 0x00000001
Flags: 0x00
Iteration Count: 0x00000001
NULL Bitmap: 0x00
New Params Bound Flag: 0x01
Param Types: 0x03 0x00 (LONG, unsigned=false)
Param Values: 0x7B 0x00 0x00 0x00 (123)
```

#### COM_STMT_CLOSE 请求
```
Packet Type: 0x19 (COM_STMT_CLOSE)
Statement ID: 0x00000001
```

## 故障排查

### 问题1: 连接失败
```
java.sql.SQLException: Communications link failure
```

**解决方法**:
- 确认 XMySQL Server 正在运行
- 检查端口 3306 是否被占用
- 检查防火墙设置

### 问题2: 未知的预编译语句
```
Unknown prepared statement handler (1)
```

**解决方法**:
- 检查 PreparedStatementManager 是否正确初始化
- 检查语句ID是否正确传递
- 查看服务器日志

### 问题3: 参数绑定错误
```
Incorrect arguments to mysql_stmt_execute
```

**解决方法**:
- 检查参数数量是否匹配
- 检查参数类型是否正确
- 检查 NULL 位图是否正确

### 问题4: 性能未提升
```
Performance improvement: 1.05x
```

**解决方法**:
- 检查语句缓存是否生效
- 检查是否每次都重新解析 SQL
- 增加测试迭代次数以减少误差

## 参考资料

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/internals/en/prepared-statements.html)
- [JDBC PreparedStatement API](https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html)
- [XMySQL Server JDBC Protocol Analysis](../../docs/JDBC_PROTOCOL_ANALYSIS.md)

