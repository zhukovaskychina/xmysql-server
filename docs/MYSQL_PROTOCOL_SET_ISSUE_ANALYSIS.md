# MySQL 协议 SET 语句问题分析与修复方案

## 🔍 问题描述

**现象**: JDBC 客户端连接 xmysql-server 时，能正常完成握手，但在执行 SET 语句时客户端自动断开连接。

**影响范围**: 
- JDBC 连接初始化失败
- 所有依赖 SET 语句的操作无法正常工作
- 客户端认为服务器响应异常而断开连接

## 🐛 根本原因分析

### 1. **重复响应问题** (Critical Bug)

在 `server/innodb/engine/enginx.go` 第 260-268 行：

```go
case *sqlparser.Set:
    for _, expr := range stmt.Exprs {
        logger.Error(expr)
        results <- &Result{
            StatementID: ctx.statementId,
            ResultType:  common.RESULT_TYPE_SET,
        }
        session.SendOK()  // ❌ 问题：直接调用 SendOK()
    }
```

**问题分析**:
1. 对每个 SET 表达式都调用 `session.SendOK()`
2. 同时向 `results` channel 发送 Result
3. 导致**双重响应**：一次来自 `SendOK()`，一次来自 Result 处理
4. MySQL 协议要求每个命令只能有**一个响应**
5. 客户端收到多个响应包会认为协议错误，主动断开连接

**示例场景**:
```sql
-- JDBC 连接时常见的 SET 语句
SET autocommit=1;
SET NAMES utf8mb4;
SET character_set_results=utf8mb4;
```

如果一个 SET 语句包含 3 个表达式，会发送 3 个 OK 包 + 3 个 Result，总共 6 个响应！

### 2. **响应流程混乱**

当前的响应流程：

```
SQL 解析 (enginx.go)
    ↓
直接调用 session.SendOK()  ← 第一次响应
    ↓
发送 Result 到 channel
    ↓
Result 被处理后再次发送响应  ← 第二次响应
    ↓
客户端收到多个响应 → 断开连接
```

正确的流程应该是：

```
SQL 解析 (enginx.go)
    ↓
发送 Result 到 channel (不直接响应)
    ↓
Result 被统一处理
    ↓
发送一次响应给客户端
    ↓
客户端正常接收
```

### 3. **SET 语句处理不完整**

当前实现的问题：
- ✅ 能识别 SET 语句
- ✅ 能解析 SET 表达式
- ❌ 没有实际设置变量值
- ❌ 没有持久化会话变量
- ❌ 响应机制错误

## 📊 JDBC 连接时的 SET 语句序列

JDBC 驱动在连接时会发送以下 SET 语句：

```sql
-- 1. 设置自动提交
SET autocommit=1

-- 2. 设置字符集
SET NAMES utf8mb4

-- 3. 设置字符集相关变量
SET character_set_client=utf8mb4
SET character_set_connection=utf8mb4
SET character_set_results=utf8mb4

-- 4. 设置 SQL 模式
SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'

-- 5. 设置时区
SET time_zone='+08:00'

-- 6. 设置事务隔离级别
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED

-- 7. 其他会话变量
SET net_write_timeout=600
SET net_read_timeout=600
SET max_allowed_packet=67108864
```

**每个 SET 语句都必须返回一个 OK 包**，否则 JDBC 会认为连接失败。

## 🔧 修复方案

### 方案 1: 移除直接调用 SendOK() (推荐)

修改 `server/innodb/engine/enginx.go`:

```go
case *sqlparser.Set:
    // 处理 SET 语句
    logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理SET语句，包含 %d 个表达式", len(stmt.Exprs))
    e.QueryExecutor.executeSetStatement(ctx, stmt, session)
```

新增 `server/innodb/engine/executor.go` 中的实现：

```go
// executeSetStatement 执行 SET 语句
func (e *XMySQLExecutor) executeSetStatement(ctx *ExecutionContext, stmt *sqlparser.Set, session server.MySQLServerSession) {
    logger.Debugf(" [executeSetStatement] 执行SET语句，包含 %d 个表达式", len(stmt.Exprs))
    
    sessionID := session.GetSessionId()
    affectedVars := 0
    
    // 处理每个 SET 表达式
    for _, expr := range stmt.Exprs {
        varName := expr.Name.String()
        logger.Debugf(" [executeSetStatement] 处理变量: %s", varName)
        
        // 解析变量值
        value, err := e.evaluateSetValue(expr.Expr)
        if err != nil {
            ctx.Results <- &Result{
                Err:        fmt.Errorf("failed to evaluate SET value for %s: %v", varName, err),
                ResultType: "ERROR",
            }
            return
        }
        
        // 设置会话变量
        if err := e.setSessionVariable(session, varName, value); err != nil {
            logger.Warnf(" [executeSetStatement] 设置变量 %s 失败: %v", varName, err)
            // 继续处理其他变量，不中断
        }
        
        affectedVars++
    }
    
    // 返回成功结果 - 只发送一次响应
    ctx.Results <- &Result{
        ResultType: "SET",
        Message:    fmt.Sprintf("SET statement executed, %d variables processed", affectedVars),
    }
    
    logger.Debugf(" [executeSetStatement] SET语句执行完成，处理了 %d 个变量", affectedVars)
}

// evaluateSetValue 计算 SET 表达式的值
func (e *XMySQLExecutor) evaluateSetValue(expr sqlparser.Expr) (interface{}, error) {
    switch v := expr.(type) {
    case *sqlparser.SQLVal:
        switch v.Type {
        case sqlparser.IntVal:
            val, err := strconv.ParseInt(string(v.Val), 10, 64)
            if err != nil {
                return nil, err
            }
            return val, nil
        case sqlparser.StrVal:
            return string(v.Val), nil
        default:
            return string(v.Val), nil
        }
    case *sqlparser.ColName:
        // 处理变量引用，如 @@session.autocommit
        return v.Name.String(), nil
    case sqlparser.BoolVal:
        if v {
            return int64(1), nil
        }
        return int64(0), nil
    default:
        return nil, fmt.Errorf("unsupported expression type: %T", expr)
    }
}

// setSessionVariable 设置会话变量
func (e *XMySQLExecutor) setSessionVariable(session server.MySQLServerSession, name string, value interface{}) error {
    // 解析变量名，去除 @@ 前缀和 scope
    cleanName := strings.TrimPrefix(name, "@@")
    cleanName = strings.TrimPrefix(cleanName, "session.")
    cleanName = strings.TrimPrefix(cleanName, "global.")
    cleanName = strings.ToLower(cleanName)
    
    logger.Debugf(" [setSessionVariable] 设置变量: %s = %v", cleanName, value)
    
    // 使用会话的参数存储
    session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
    
    return nil
}
```

### 方案 2: 完善 SystemVariableEngine 集成

确保 `dispatcher/system_variable_engine.go` 正确处理 SET 语句：

```go
// executeSetStatement 在 SystemVariableEngine 中的实现已经存在
// 需要确保返回的 SQLResult 被正确转换为响应

func (e *SystemVariableEngine) executeSetStatement(session server.MySQLServerSession, query string) *SQLResult {
    // ... 现有实现 ...
    
    return &SQLResult{
        ResultType: "set",  // 注意：这里是小写
        Message:    fmt.Sprintf("SET statement executed successfully, %d variables set", affectedRows),
        Columns:    []string{},
        Rows:       [][]interface{}{},
    }
}
```

在 `net/decoupled_handler.go` 中确保正确处理 SET 结果：

```go
func (h *DecoupledMySQLMessageHandler) handleBusinessMessageSync(session Session, message protocol.Message) error {
    // ...
    if response != nil {
        switch resp := response.(type) {
        case *protocol.ResponseMessage:
            if resp.Result != nil {
                // 检查结果类型
                if len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0 {
                    // SET 语句返回 OK 包
                    logger.Debugf("[handleBusinessMessageSync] SET语句执行成功，发送OK包")
                    return h.sendMySQLOKPacket(session, 0, 0, 1)
                }
                return h.sendQueryResultSet(session, resp.Result, 1)
            }
            return h.sendMySQLOKPacket(session, 0, 0, 1)
        // ...
        }
    }
}
```

## 📝 完整实现步骤

### Step 1: 修复 enginx.go 中的重复响应

**文件**: `server/innodb/engine/enginx.go`

**修改位置**: 第 260-268 行

**修改前**:
```go
case *sqlparser.Set:
    for _, expr := range stmt.Exprs {
        logger.Error(expr)
        results <- &Result{
            StatementID: ctx.statementId,
            ResultType:  common.RESULT_TYPE_SET,
        }
        session.SendOK()  // ❌ 移除这行
    }
```

**修改后**:
```go
case *sqlparser.Set:
    // 处理 SET 语句
    logger.Debugf(" [XMySQLEngine.ExecuteQuery] 处理SET语句，包含 %d 个表达式", len(stmt.Exprs))
    e.QueryExecutor.executeSetStatement(ctx, stmt, session)
```

### Step 2: 实现 executeSetStatement 方法

**文件**: `server/innodb/engine/executor.go`

**添加位置**: 在文件末尾，与其他 execute 方法一起

**完整代码**:
```go
// executeSetStatement 执行 SET 语句
func (e *XMySQLExecutor) executeSetStatement(ctx *ExecutionContext, stmt *sqlparser.Set, session server.MySQLServerSession) {
    logger.Debugf(" [executeSetStatement] 执行SET语句，包含 %d 个表达式", len(stmt.Exprs))
    
    sessionID := session.GetSessionId()
    affectedVars := 0
    var errors []string
    
    // 处理每个 SET 表达式
    for _, expr := range stmt.Exprs {
        varName := expr.Name.String()
        logger.Debugf(" [executeSetStatement] 处理变量: %s", varName)
        
        // 解析变量值
        value, err := e.evaluateSetValue(expr.Expr)
        if err != nil {
            errMsg := fmt.Sprintf("failed to evaluate SET value for %s: %v", varName, err)
            logger.Errorf(" [executeSetStatement] %s", errMsg)
            errors = append(errors, errMsg)
            continue
        }
        
        // 设置会话变量
        if err := e.setSessionVariable(session, varName, value); err != nil {
            logger.Warnf(" [executeSetStatement] 设置变量 %s 失败: %v", varName, err)
            errors = append(errors, fmt.Sprintf("failed to set %s: %v", varName, err))
            continue
        }
        
        affectedVars++
        logger.Debugf(" [executeSetStatement] 成功设置变量: %s = %v", varName, value)
    }
    
    // 如果所有变量都设置失败，返回错误
    if affectedVars == 0 && len(errors) > 0 {
        ctx.Results <- &Result{
            Err:        fmt.Errorf("SET statement failed: %s", strings.Join(errors, "; ")),
            ResultType: "ERROR",
        }
        return
    }
    
    // 返回成功结果
    message := fmt.Sprintf("SET statement executed, %d variables processed", affectedVars)
    if len(errors) > 0 {
        message += fmt.Sprintf(" (%d errors: %s)", len(errors), strings.Join(errors, "; "))
    }
    
    ctx.Results <- &Result{
        ResultType: "SET",
        Message:    message,
    }
    
    logger.Debugf(" [executeSetStatement] SET语句执行完成: %s", message)
}

// evaluateSetValue 计算 SET 表达式的值
func (e *XMySQLExecutor) evaluateSetValue(expr sqlparser.Expr) (interface{}, error) {
    switch v := expr.(type) {
    case *sqlparser.SQLVal:
        switch v.Type {
        case sqlparser.IntVal:
            val, err := strconv.ParseInt(string(v.Val), 10, 64)
            if err != nil {
                return nil, fmt.Errorf("invalid integer value: %s", string(v.Val))
            }
            return val, nil
        case sqlparser.StrVal:
            return string(v.Val), nil
        case sqlparser.FloatVal:
            val, err := strconv.ParseFloat(string(v.Val), 64)
            if err != nil {
                return nil, fmt.Errorf("invalid float value: %s", string(v.Val))
            }
            return val, nil
        default:
            return string(v.Val), nil
        }
    case *sqlparser.ColName:
        // 处理变量引用，如 @@session.autocommit
        return v.Name.String(), nil
    case sqlparser.BoolVal:
        if v {
            return int64(1), nil
        }
        return int64(0), nil
    case *sqlparser.NullVal:
        return nil, nil
    default:
        return nil, fmt.Errorf("unsupported expression type: %T", expr)
    }
}

// setSessionVariable 设置会话变量
func (e *XMySQLExecutor) setSessionVariable(session server.MySQLServerSession, name string, value interface{}) error {
    // 解析变量名，去除 @@ 前缀和 scope
    cleanName := strings.TrimPrefix(name, "@@")
    cleanName = strings.TrimPrefix(cleanName, "session.")
    cleanName = strings.TrimPrefix(cleanName, "global.")
    cleanName = strings.TrimPrefix(cleanName, "`")
    cleanName = strings.TrimSuffix(cleanName, "`")
    cleanName = strings.ToLower(cleanName)
    
    logger.Debugf(" [setSessionVariable] 设置变量: %s = %v (type: %T)", cleanName, value, value)
    
    // 特殊处理某些变量
    switch cleanName {
    case "autocommit":
        // 转换为整数
        var intVal int64
        switch v := value.(type) {
        case int64:
            intVal = v
        case string:
            if v == "on" || v == "ON" || v == "true" || v == "TRUE" || v == "1" {
                intVal = 1
            } else {
                intVal = 0
            }
        default:
            intVal = 1
        }
        session.SetParamByName(cleanName, fmt.Sprintf("%d", intVal))
        logger.Debugf(" [setSessionVariable] autocommit 设置为: %d", intVal)
        
    case "names":
        // SET NAMES utf8mb4 等价于设置多个字符集变量
        charset := fmt.Sprintf("%v", value)
        session.SetParamByName("character_set_client", charset)
        session.SetParamByName("character_set_connection", charset)
        session.SetParamByName("character_set_results", charset)
        logger.Debugf(" [setSessionVariable] NAMES 设置为: %s", charset)
        
    case "character_set_client", "character_set_connection", "character_set_results",
         "character_set_database", "character_set_server":
        session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
        
    case "sql_mode":
        session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
        
    case "time_zone":
        session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
        
    case "transaction_isolation", "tx_isolation":
        session.SetParamByName("transaction_isolation", fmt.Sprintf("%v", value))
        
    default:
        // 其他变量直接存储
        session.SetParamByName(cleanName, fmt.Sprintf("%v", value))
        logger.Debugf(" [setSessionVariable] 通用变量 %s 设置为: %v", cleanName, value)
    }
    
    return nil
}
```

### Step 3: 添加必要的 import

在 `server/innodb/engine/executor.go` 文件顶部确保有：

```go
import (
    // ... 现有 imports ...
    "strconv"
    "strings"
)
```

### Step 4: 确保响应处理正确

在 `server/net/decoupled_handler.go` 中，确保 SET 结果被正确处理：

```go
// 在 handleBusinessMessageSync 方法中
case *protocol.ResponseMessage:
    if resp.Result != nil {
        // 检查是否为 SET 语句结果（无列无行）
        if len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0 {
            logger.Debugf("[handleBusinessMessageSync] 无数据结果，发送OK包: %s", resp.Result.Message)
            return h.sendMySQLOKPacket(session, 0, 0, 1)
        }
        return h.sendQueryResultSet(session, resp.Result, 1)
    }
    return h.sendMySQLOKPacket(session, 0, 0, 1)
```

## 🧪 测试验证

### 测试用例 1: 基本 SET 语句

```sql
SET autocommit=1;
SET NAMES utf8mb4;
SET character_set_results=utf8mb4;
```

**预期结果**: 每个语句返回一个 OK 包

### 测试用例 2: 多变量 SET

```sql
SET autocommit=1, sql_mode='STRICT_TRANS_TABLES';
```

**预期结果**: 返回一个 OK 包（不是两个）

### 测试用例 3: 作用域指定

```sql
SET @@session.autocommit=1;
SET @@global.max_connections=1000;
SET SESSION transaction_isolation='READ-COMMITTED';
```

**预期结果**: 每个语句返回一个 OK 包

### 测试用例 4: JDBC 连接测试

```java
import java.sql.*;

public class TestSetStatement {
    public static void main(String[] args) {
        String url = "jdbc:mysql://127.0.0.1:3309/test?useSSL=false";
        String user = "root";
        String password = "root";
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("✅ 连接成功！");
            
            // 测试各种 SET 语句
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET autocommit=1");
                System.out.println("✅ SET autocommit 成功");
                
                stmt.execute("SET NAMES utf8mb4");
                System.out.println("✅ SET NAMES 成功");
                
                stmt.execute("SET character_set_results=utf8mb4");
                System.out.println("✅ SET character_set_results 成功");
                
                stmt.execute("SET sql_mode='STRICT_TRANS_TABLES'");
                System.out.println("✅ SET sql_mode 成功");
            }
            
            System.out.println("\n✅ 所有 SET 语句测试通过！");
            
        } catch (SQLException e) {
            System.err.println("❌ 错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
```

### 测试用例 5: 验证变量值

```sql
-- 设置变量
SET @user_var = 'test_value';
SET autocommit=0;

-- 查询变量（如果支持）
SELECT @@autocommit;
SHOW VARIABLES LIKE 'autocommit';
```

## 📊 协议抓包分析

### 正常的 SET 语句协议流程

```
客户端 → 服务器: COM_QUERY (0x03)
    Payload: "SET autocommit=1"
    
服务器 → 客户端: OK Packet
    Header: [长度3字节][序号1字节]
    Payload: 
        [0x00]              # OK 标识
        [affected_rows]     # 受影响行数 (lenenc-int)
        [last_insert_id]    # 最后插入ID (lenenc-int)
        [status_flags]      # 状态标志 (2字节)
        [warnings]          # 警告数 (2字节)
```

### 错误的协议流程（当前问题）

```
客户端 → 服务器: COM_QUERY (0x03)
    Payload: "SET autocommit=1"
    
服务器 → 客户端: OK Packet #1  ← session.SendOK()
    [0x00][0x00][0x00][0x02][0x00]
    
服务器 → 客户端: OK Packet #2  ← Result 处理
    [0x00][0x00][0x00][0x02][0x00]
    
客户端: ❌ 协议错误，断开连接
```

## 🎯 修复后的预期行为

1. **单一响应**: 每个 SET 语句只返回一个 OK 包
2. **变量持久化**: 会话变量被正确存储在 session 中
3. **JDBC 兼容**: JDBC 驱动能正常连接和初始化
4. **错误处理**: 无效的 SET 语句返回错误包而不是 OK 包
5. **日志完整**: 所有 SET 操作都有详细日志记录

## 🔍 调试建议

### 1. 启用详细日志

在测试时，确保日志级别设置为 DEBUG：

```go
logger.SetLevel(logger.DEBUG)
```

### 2. 关键日志点

在以下位置添加日志：
- `enginx.go` 的 SET case 入口
- `executeSetStatement` 的开始和结束
- 每个变量设置的成功/失败
- 响应发送前

### 3. 使用 Wireshark 抓包

```bash
# 抓取 MySQL 协议包
sudo tcpdump -i lo0 -w mysql.pcap port 3309

# 使用 Wireshark 打开 mysql.pcap
# 过滤器: mysql
```

### 4. 使用 MySQL 客户端测试

```bash
# 使用官方 MySQL 客户端连接
mysql -h 127.0.0.1 -P 3309 -u root -p

# 执行 SET 语句
mysql> SET autocommit=1;
Query OK, 0 rows affected (0.00 sec)

mysql> SET NAMES utf8mb4;
Query OK, 0 rows affected (0.00 sec)
```

## ✅ 验收标准

- [ ] JDBC 能成功连接并初始化
- [ ] 单个 SET 语句只返回一个响应
- [ ] 多变量 SET 语句只返回一个响应
- [ ] 会话变量被正确存储
- [ ] 特殊变量（autocommit, NAMES）被正确处理
- [ ] 无效的 SET 语句返回错误而不是崩溃
- [ ] 日志显示完整的处理流程
- [ ] Wireshark 抓包显示正确的协议交互

## 📚 相关 MySQL 协议文档

- [MySQL Protocol - Text Protocol](https://dev.mysql.com/doc/internals/en/text-protocol.html)
- [MySQL Protocol - OK Packet](https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
- [MySQL Protocol - ERR Packet](https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html)
- [MySQL SET Statement](https://dev.mysql.com/doc/refman/8.0/en/set-variable.html)

## 🎉 总结

**核心问题**: SET 语句处理时重复发送响应导致协议错误

**解决方案**: 
1. 移除 `session.SendOK()` 直接调用
2. 统一通过 Result channel 发送响应
3. 实现完整的变量设置逻辑
4. 确保每个命令只有一个响应

**优先级**: 🔴 Critical - 阻塞 JDBC 连接

**预计工作量**: 2-3 小时

---

**文档创建时间**: 2024-11-15  
**问题严重程度**: Critical  
**状态**: 待修复  
**负责人**: Codex
