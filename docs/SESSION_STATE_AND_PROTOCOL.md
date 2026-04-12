# MySQL 协议会话状态与 handlePacket 逻辑说明

> **文档状态（2026-04）**：技术说明仍有效；**总索引**见 [protocol/PROTOCOL_DOCUMENTATION_INDEX.md](./protocol/PROTOCOL_DOCUMENTATION_INDEX.md)。与预处理、握手细节并列阅读 [development/NET-001-PREPARED-STATEMENT-SUMMARY.md](./development/NET-001-PREPARED-STATEMENT-SUMMARY.md)。

## 1. COM_INIT_DB (0x02) 与 COM_QUERY (0x03) 的差异


| 项目        | COM_INIT_DB (0x02)                        | COM_QUERY (0x03)                                       |
| --------- | ----------------------------------------- | ------------------------------------------------------ |
| **含义**    | 协议层“切换当前数据库”命令                            | 执行任意 SQL 文本（包括 `USE db`）                               |
| **包体**    | `[0x02][db_name]`（无 null 结尾）              | `[0x03][SQL 文本]`                                       |
| **典型来源**  | 部分客户端在连接后发 INIT_DB；或 UI 切换库时              | JDBC/CLI 执行 `USE demo_db` 时通常发 COM_QUERY               |
| **服务端必须** | 解析 db 名 → **更新 session.currentDB** → 回 OK | 解析 SQL → 执行（若为 USE 则引擎内 **更新 session.currentDB**）→ 回结果 |


要点：

- **COM_INIT_DB**：只做“选库”，不执行 SQL。服务端必须在协议层或业务层把 `db_name` 写入**当前连接的 session**（如 `session.SetParamByName("database", dbName)` 或等价存储）。
- **COM_QUERY "USE db"**：走 SQL 执行路径。必须用**同一连接对应的 session** 执行，这样引擎里对 USE 的处理（`SetParamByName("database", dbName)`）才会更新到“真实 session”；若每次查询都 new 一个临时 session，USE 只改临时对象，下一句 CREATE TABLE 仍拿不到 currentDB，就会报 “no database selected”。

因此：

- 若只实现 COM_QUERY 而不实现 COM_INIT_DB：JDBC 用 `USE db` 时仍可工作，只要 USE 和后续 SQL 共用同一 session。
- 若只实现 COM_INIT_DB 而不在 COM_QUERY 里用真实 session：USE 走 COM_QUERY 时不会更新真实 session，CREATE TABLE 仍会报错。

当前修复已做两件事：

1. **COM_INIT_DB**：在 `handlePacket` 里识别 `0x02`，解析 db 名，**更新当前连接的 session.currentDB**，再回 OK。
2. **COM_QUERY**：查询一律用**真实 session**（从 sessionMap 取出）执行，这样 USE 等语句会更新同一 session，后续 DDL/DML 能拿到 currentDB。

---

## 2. session.currentDB 的标准维护方式

- **唯一真相来源**：每个连接对应一个 session 对象（如 `MySQLServerSession` / 底层 net.Session），`currentDB` 应存在该 session 上（例如用 `SetParamByName("database", name)` / `GetParamByName("database")` 或等价属性）。
- **何时写入**  
  - 认证时若客户端指定了 database，在握手/认证完成后写入一次。  
  - 收到 **COM_INIT_DB**：解析出 db 名后，对**当前连接**的 session 调用 `SetParamByName("database", dbName)`（或等价）。  
  - 执行 **USE**（COM_QUERY）：在引擎执行 USE 时，对**传入的同一 session** 调用 `SetParamByName("database", dbName)`（当前实现里引擎已做，需保证传入的是“真实 session”）。
- **何时读取**  
  - 执行任意需要“当前库”的 SQL 时（CREATE TABLE、DROP TABLE、未带库名的表引用等），从**同一 session** 用 `GetParamByName("database")` 取 currentDB；若为空且 SQL 未带库名，再报 “no database selected”。

要点：**同一连接、同一 session、写入和读取必须是同一个对象**；不能为每次 COM_QUERY 创建新的临时 session，否则 USE 的更新对后续 SQL 不可见。

---

## 3. handlePacket 逻辑示例（与当前实现一致）

```go
func (h *DecoupledMySQLMessageHandler) handlePacket(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
    // 1) 未认证 → 走认证
    if session.GetAttribute("auth_status") == nil {
        return h.handleAuthentication(session, currentMysqlSession, recMySQLPkg)
    }
    if len(recMySQLPkg.Body) == 0 {
        return fmt.Errorf("empty packet body")
    }
    firstByte := recMySQLPkg.Body[0]

    // 2) COM_INIT_DB (0x02)：只更新 session.currentDB，再回 OK
    if len(recMySQLPkg.Body) >= 2 && firstByte == 0x02 {
        dbName := strings.TrimSpace(string(recMySQLPkg.Body[1:]))
        (*currentMysqlSession).SetParamByName("database", dbName)
        return session.WriteBytes(protocol.EncodeOK(nil, 0, 0, nil))
    }

    // 3) COM_QUERY (0x03)：用【当前连接的 session】执行，保证 USE 会更新 currentDB
    if len(recMySQLPkg.Body) >= 2 && firstByte == 0x03 {
        query := string(recMySQLPkg.Body[1:])
        queryMsg := &protocol.QueryMessage{...}
        return h.handleQueryMessageDirect(session, currentMysqlSession, queryMsg)
    }

    // 4) 其他命令走协议解析 + 业务分发
    message, _ := h.protocolParser.ParsePacket(recMySQLPkg.Body, session.Stat())
    return h.handleBusinessMessageSync(session, message)
}
```

`handleQueryMessageDirect` 必须收到 **currentMysqlSession**（即当前连接在 sessionMap 里的那个），并从其上取 `database` 传给引擎；执行时引擎若遇到 USE，会对同一 session 调用 `SetParamByName("database", dbName)`，这样后续 CREATE TABLE 才能拿到 currentDB。

---

## 4. 调试建议

1. **打日志确认 currentDB 是否被写入/读出**
  - 在 COM_INIT_DB 分支里：打完 `SetParamByName("database", dbName)` 后打一条日志，带上 `dbName` 和 `session.Stat()`（或连接 ID）。  
  - 在 `handleQueryMessageDirect` 里：从 `currentMysqlSession.GetParamByName("database")` 取出当前库，每条 COM_QUERY 打一条日志（query 前几字 + currentDB）。  
  - 在引擎执行 USE 时：打一条 “USE 已设置 session.database = xxx”。  
  - 在 CREATE TABLE 入口：打一条 “executeCreateTableStatement databaseName=xxx”。
2. **确认 JDBC 发的是 COM_QUERY 还是 COM_INIT_DB**
  - 抓包或服务端在 `handlePacket` 里对 `firstByte` 打日志；若看到 `0x03` + “USE demo_db”，说明是 COM_QUERY，必须用真实 session 执行 USE。
3. **确认是否走了“真实 session”路径**
  - 在 `HandleQueryWithRealSession` 入口打日志；在 `Dispatch(realSession, query, database)` 处打 `database` 和 `session` 的标识（如指针或 sessionID），确认和上一句 USE 是同一 session。
4. **单测**
  - 建一个连接，发 COM_INIT_DB "demo_db"，再发 COM_QUERY "CREATE TABLE t1(id int)"，断言成功且表在 demo_db。  
  - 同一连接先发 COM_QUERY "USE demo_db"，再发 COM_QUERY "CREATE TABLE t2(id int)"，同样断言成功。

---

## 5. 当前 xmysql-server 中的“上下文状态”实现情况


| 状态项                  | 是否实现    | 存放位置                                                                                     | 说明                                                                                           |
| -------------------- | ------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **currentDB**        | ✅ 已实现   | `session.SetParamByName("database", name)` / 底层 `Session.SetAttribute("database", name)` | 认证、COM_INIT_DB、USE 语句都会写入；CREATE TABLE 等从同一 session 读取。修复后 COM_QUERY 使用真实 session，USE 会正确更新。 |
| **user**             | ✅ 已实现   | `session.SetParamByName("user", username)`                                               | 认证时写入，权限/审计等处读取。                                                                             |
| **autocommit**       | ⚠️ 部分实现 | 引擎/系统变量层有解析与 SetParamByName(autocommit)；部分从 session 读                                    | 在 executor 的 SET 处理里有 `session.SetParamByName("autocommit", ...)`；是否所有路径都从 session 读需再确认。    |
| **txStatus**（事务是否开启） | ❓ 未显式见  | -                                                                                        | 未见统一的 “in_transaction” 会话状态；若要做事务语义，需要在 BEGIN/COMMIT/ROLLBACK 及 autocommit 路径维护。             |


结论：**currentDB** 和 **user** 已按“会话状态”维护；**autocommit** 有写入和部分使用；**txStatus** 若需要，建议在事务边界（BEGIN/COMMIT/ROLLBACK）和 autocommit 逻辑里显式维护一个 session 级标志并在需要处读取。