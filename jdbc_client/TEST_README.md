# XMySQL Server 测试文档

## 📋 目录

- [测试概述](#测试概述)
- [测试环境配置](#测试环境配置)
- [运行测试](#运行测试)
- [测试类说明](#测试类说明)
- [测试覆盖范围](#测试覆盖范围)
- [编写新测试](#编写新测试)

## 🎯 测试概述

本项目使用 **JUnit 5** 作为测试框架，配合 **AssertJ** 进行断言，**Mockito** 进行模拟测试。测试代码覆盖了 XMySQL Server 的主要功能模块。

### 测试框架和工具

- **JUnit 5** (5.10.1) - 测试框架
- **AssertJ** (3.24.2) - 流式断言库
- **Mockito** (5.8.0) - 模拟框架
- **Maven Surefire** (3.2.3) - 测试运行插件

## ⚙️ 测试环境配置

### 前置条件

1. **Java 17** 或更高版本
2. **Maven 3.6+**
3. **XMySQL Server** 运行在 `localhost:3309`
4. 数据库用户名：`root`，密码：`root@1234`

### 配置数据库连接

测试使用的数据库连接配置在 `BaseIntegrationTest.java` 中：

```java
protected static final String BASE_URL = "jdbc:mysql://localhost:3309?useSSL=false&allowPublicKeyRetrieval=true";
protected static final String USER = "root";
protected static final String PASSWORD = "root@1234";
```

如需修改，请编辑 `src/test/java/com/xmysql/server/test/BaseIntegrationTest.java` 文件。

## 🚀 运行测试

### 运行所有测试

```bash
cd jdbc_client
mvn test
```

### 运行特定测试类

```bash
# 运行DDL操作测试
mvn test -Dtest=DDLOperationsTest

# 运行DML操作测试
mvn test -Dtest=DMLOperationsTest

# 运行SELECT查询测试
mvn test -Dtest=SelectQueryTest
```

### 运行测试套件

```bash
mvn test -Dtest=AllTestsSuite
```

### 运行单个测试方法

```bash
mvn test -Dtest=DDLOperationsTest#testCreateDatabase
```

### 生成测试报告

```bash
mvn surefire-report:report
```

报告将生成在 `target/surefire-reports/` 目录下。

## 📚 测试类说明

### 0. JdbcConnectionTest.java（JDBC 连接专项）
**连接过程专项测试**，仅覆盖 JDBC 协议连接生命周期，不依赖 BaseIntegrationTest。

**测试用例：** (7 个)
- ✅ 有效账号密码应成功建立连接
- ✅ 连接后元数据应反映服务端（URL、产品名、版本）
- ✅ close 后 isClosed 应为 true
- ✅ 错误密码应抛出 SQLException（服务端应返回 Access denied/1045）
- ✅ URL 中带默认库时 getCatalog 应一致
- ✅ 无默认库连接后 getCatalog 可为空或空串
- ✅ 连接后能执行简单查询（验证握手+认证完整）

**说明：** 需 XMySQL 运行在 `localhost:3309`，否则上述用例会被跳过（`assumeTrue(SERVER_AVAILABLE)`）。

### 1. BaseIntegrationTest.java
**基础测试类**，所有测试类的父类。

**功能：**
- 管理数据库连接
- 提供通用的测试工具方法
- 处理测试前后的初始化和清理

**主要方法：**
- `executeUpdate(String sql)` - 执行更新语句
- `executeQuery(String sql)` - 执行查询语句
- `tableExists(String tableName)` - 检查表是否存在
- `databaseExists(String databaseName)` - 检查数据库是否存在
- `getTableRowCount(String tableName)` - 获取表行数

### 2. DDLOperationsTest.java
**DDL操作测试类** - 测试数据定义语言操作

**测试用例：** (11个测试)
- ✅ 创建数据库
- ✅ 创建数据库 (IF NOT EXISTS)
- ✅ 创建数据库 (指定字符集)
- ✅ 删除数据库
- ✅ 删除数据库 (IF EXISTS)
- ✅ 创建表 (基本类型)
- ✅ 创建表 (带索引)
- ✅ 创建表 (外键约束)
- ✅ 删除表
- ✅ ALTER TABLE (添加列)
- ✅ TRUNCATE TABLE

### 3. DMLOperationsTest.java
**DML操作测试类** - 测试数据操作语言

**测试用例：** (13个测试)
- ✅ INSERT 单行插入
- ✅ INSERT 批量插入
- ✅ INSERT PreparedStatement
- ✅ INSERT 默认值
- ✅ UPDATE 单行更新
- ✅ UPDATE 批量更新
- ✅ UPDATE PreparedStatement
- ✅ DELETE 单行删除
- ✅ DELETE 批量删除
- ✅ DELETE 全部删除
- ✅ INSERT DECIMAL类型
- ✅ UPDATE 库存更新
- ✅ UNIQUE约束违反

### 4. SelectQueryTest.java
**SELECT查询测试类** - 测试各种查询语句

**测试用例：** (19个测试)
- ✅ SELECT * (所有列)
- ✅ SELECT 指定列
- ✅ WHERE 条件
- ✅ WHERE AND 多条件
- ✅ WHERE OR 多条件
- ✅ ORDER BY ASC
- ✅ ORDER BY DESC
- ✅ LIMIT
- ✅ LIMIT OFFSET
- ✅ COUNT 聚合函数
- ✅ SUM 聚合函数
- ✅ AVG 聚合函数
- ✅ MAX/MIN 聚合函数
- ✅ GROUP BY
- ✅ GROUP BY HAVING
- ✅ DISTINCT
- ✅ LIKE 模糊查询
- ✅ IN 条件
- ✅ BETWEEN 条件

### 5. JoinQueryTest.java
**JOIN查询测试类** - 测试表连接查询

**测试用例：** (10个测试)
- ✅ INNER JOIN 基本连接
- ✅ LEFT JOIN 左连接
- ✅ RIGHT JOIN 右连接
- ✅ 多表JOIN
- ✅ JOIN with WHERE
- ✅ JOIN with GROUP BY
- ✅ 自连接 (Self Join)
- ✅ JOIN with ORDER BY
- ✅ JOIN with LIMIT
- ✅ JOIN with 聚合函数和HAVING

### 6. TransactionTest.java
**事务测试类** - 测试事务处理

**测试用例：** (8个测试)
- ✅ COMMIT 提交事务
- ✅ ROLLBACK 回滚事务
- ✅ 转账事务 (成功场景)
- ✅ 转账事务 (失败回滚)
- ✅ SAVEPOINT 部分回滚
- ✅ 多个SAVEPOINT
- ✅ 事务隔离级别
- ✅ BEGIN/START TRANSACTION

### 7. SystemVariableTest.java
**系统变量测试类** - 测试系统变量查询和设置

**测试用例：** (15个测试)
- ✅ SELECT @@version
- ✅ SELECT @@character_set_client
- ✅ SELECT @@session.autocommit
- ✅ SELECT @@global.port
- ✅ SELECT 多个系统变量
- ✅ SET autocommit
- ✅ SET character_set_results
- ✅ SET time_zone
- ✅ SET sql_mode
- ✅ SET NAMES
- ✅ SHOW VARIABLES
- ✅ SHOW VARIABLES LIKE
- ✅ SHOW SESSION VARIABLES
- ✅ SHOW GLOBAL VARIABLES
- ✅ SET 多个变量

### 8. DataTypeTest.java
**数据类型测试类** - 测试各种MySQL数据类型

**测试用例：** (11个测试)
- ✅ 整数类型 (INT, BIGINT, SMALLINT, TINYINT)
- ✅ 浮点类型 (FLOAT, DOUBLE, DECIMAL)
- ✅ 字符串类型 (VARCHAR, CHAR, TEXT)
- ✅ 日期时间类型 (DATE, DATETIME, TIMESTAMP, TIME, YEAR)
- ✅ 布尔类型 (BOOLEAN/BOOL)
- ✅ ENUM类型
- ✅ SET类型
- ✅ NULL值处理
- ✅ BLOB和BINARY类型
- ✅ AUTO_INCREMENT
- ✅ DEFAULT值

### 9. IndexAndConstraintTest.java
**索引和约束测试类** - 测试索引和各种约束

**测试用例：** (12个测试)
- ✅ PRIMARY KEY约束
- ✅ 复合PRIMARY KEY
- ✅ UNIQUE约束
- ✅ NOT NULL约束
- ✅ FOREIGN KEY约束
- ✅ CHECK约束
- ✅ INDEX索引
- ✅ 复合索引
- ✅ UNIQUE INDEX
- ✅ FULLTEXT索引
- ✅ ON DELETE CASCADE
- ✅ ON UPDATE CASCADE

### 10. PreparedStatementTest.java
**PreparedStatement测试类** - 测试预编译语句

**测试用例：** (12个测试)
- ✅ 基本INSERT
- ✅ 批量INSERT
- ✅ SELECT查询
- ✅ UPDATE
- ✅ DELETE
- ✅ 多种数据类型
- ✅ NULL值处理
- ✅ 参数重用
- ✅ 获取生成的主键
- ✅ IN子句
- ✅ LIKE模糊查询
- ✅ 事务中使用

### 11. PerformanceTest.java
**性能测试类** - 测试大批量数据操作性能

**测试用例：** (8个测试)
- ✅ 批量插入1000条记录
- ✅ 查询大量数据 (5000条)
- ✅ 带WHERE条件的查询 (10000条)
- ✅ 批量更新 (1000条)
- ✅ 批量删除 (1000条)
- ✅ 事务中批量操作 (5000条)
- ✅ 聚合查询 (10000条)
- ✅ GROUP BY查询

## 📊 测试覆盖范围

### 功能覆盖

| 功能模块 | 测试类 | 测试用例数 | 覆盖率 |
|---------|--------|-----------|--------|
| JDBC 连接 | JdbcConnectionTest | 7 | ✅ 高 |
| DDL操作 | DDLOperationsTest | 11 | ✅ 高 |
| DML操作 | DMLOperationsTest | 13 | ✅ 高 |
| SELECT查询 | SelectQueryTest | 19 | ✅ 高 |
| JOIN查询 | JoinQueryTest | 10 | ✅ 高 |
| 事务处理 | TransactionTest | 8 | ✅ 高 |
| 系统变量 | SystemVariableTest | 15 | ✅ 高 |
| 数据类型 | DataTypeTest | 11 | ✅ 高 |
| 索引约束 | IndexAndConstraintTest | 12 | ✅ 高 |
| PreparedStatement | PreparedStatementTest | 12 | ✅ 高 |
| 性能测试 | PerformanceTest | 8 | ✅ 中 |
| **总计** | **11个测试类** | **126个测试** | **✅ 高** |

### SQL语句类型覆盖

- ✅ CREATE DATABASE/TABLE
- ✅ DROP DATABASE/TABLE
- ✅ ALTER TABLE
- ✅ TRUNCATE TABLE
- ✅ INSERT (单行、批量、PreparedStatement)
- ✅ UPDATE (单行、批量、PreparedStatement)
- ✅ DELETE (单行、批量、条件删除)
- ✅ SELECT (基本查询、聚合、分组、排序、限制)
- ✅ JOIN (INNER, LEFT, RIGHT, Self Join)
- ✅ 事务 (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
- ✅ 系统变量 (SELECT @@, SET, SHOW VARIABLES)

## ✍️ 编写新测试

### 1. 创建新的测试类

继承 `BaseIntegrationTest` 类：

```java
package com.xmysql.server.test;

import org.junit.jupiter.api.*;
import java.sql.SQLException;
import static org.assertj.core.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MyNewTest extends BaseIntegrationTest {
    
    private static final String TEST_DB = "test_my_feature";
    
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
    @DisplayName("测试我的新功能")
    public void testMyNewFeature() throws SQLException {
        // 测试代码
        executeUpdate("CREATE TABLE test_table (id INT PRIMARY KEY)");
        assertThat(tableExists("test_table")).isTrue();
        
        printSuccess("测试通过");
    }
}
```

### 2. 使用AssertJ断言

```java
// 基本断言
assertThat(value).isEqualTo(expected);
assertThat(value).isNotNull();
assertThat(value).isGreaterThan(10);

// 字符串断言
assertThat(str).startsWith("prefix");
assertThat(str).contains("substring");
assertThat(str).isEqualToIgnoringCase("VALUE");

// 集合断言
assertThat(list).hasSize(5);
assertThat(list).contains("item");
assertThat(value).isIn("a", "b", "c");

// 异常断言
assertThatThrownBy(() -> executeUpdate("INVALID SQL"))
    .isInstanceOf(SQLException.class);
```

### 3. 测试最佳实践

1. **每个测试独立** - 不依赖其他测试的执行顺序
2. **清理测试数据** - 使用 `@BeforeEach` 和 `@AfterEach` 清理
3. **有意义的测试名** - 使用 `@DisplayName` 提供清晰的描述
4. **断言充分** - 验证所有重要的结果
5. **异常测试** - 测试错误情况和边界条件

## 📈 持续改进

### 待添加的测试

- [x] JDBC 连接过程专项测试（见 JdbcConnectionTest）
- [ ] 存储过程测试
- [ ] 触发器测试
- [ ] 视图测试
- [ ] 用户权限测试
- [ ] 并发测试
- [ ] 压力测试

### 贡献指南

欢迎贡献新的测试用例！请遵循以下步骤：

1. Fork 项目
2. 创建新的测试类或在现有类中添加测试方法
3. 确保所有测试通过：`mvn test`
4. 提交 Pull Request

## 📞 联系方式

如有问题或建议，请提交 Issue 或联系项目维护者。

---

**最后更新：** 2026-03-17
**测试框架版本：** JUnit 5.10.1
**总测试用例数：** 126个（含 JdbcConnectionTest 7 个连接专项用例）

