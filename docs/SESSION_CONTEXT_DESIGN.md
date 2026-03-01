# SessionContext 设计文档

## 1. 目标与背景

### 1.1 为什么要引入 SessionContext

当前会话状态分散在 **key-value 存储**（`net.Session.attrs` + `MySQLServerSession.GetParamByName/SetParamByName`）中，存在：

- **无类型**：键为 string，值为 `interface{}`，易拼错、难重构、无编译期约束。
- **无显式契约**：哪些键是“会话上下文”、哪些是协议内部标记，没有文档化类型。
- **多实现易不一致**：`MySQLServerSessionImpl` 与 `EnhancedMockMySQLServerSession` 各有一套存储，Mock 未绑定连接级存储时会导致 USE 等状态不生效。
- **扩展性差**：新增会话级变量（如 `txStatus`、`savepoint`）时只能继续塞 key-value，难以做统一校验与生命周期管理。

引入 **SessionContext** 的目标：

- 将会话级状态收敛为 **显式、可类型化的上下文对象**。
- 明确 **谁创建、谁更新、谁读取**，与现有 net 层、dispatcher、engine 的调用关系对齐。
- 为后续 **事务状态（txStatus）、字符集、时区** 等扩展留出清晰字段与扩展点，便于维护和测试。

### 1.2 不改变的前提

- **一连接一上下文**：每个 TCP 连接对应一个 SessionContext，生命周期与连接一致。
- **与现有协议/引擎流程兼容**：设计可渐进落地，先与现有 `GetParamByName/SetParamByName` 并存，再逐步迁移。

---

## 2. 现状简要梳理

### 2.1 会话状态当前存在哪里

| 层级 | 存储 | 说明 |
|------|------|------|
| 协议/连接层 | `net.session.attrs`（`*gxcontext.ValuesContext`） | 每个连接一个 `session`，key-value，接口为 `GetAttribute`/`SetAttribute`。 |
| 对引擎的抽象 | `server.MySQLServerSession` | 接口方法 `GetParamByName(name)/SetParamByName(name, value)`，引擎只依赖该接口。 |
| 真实连接实现 | `net.MySQLServerSessionImpl` | 持有 `session`，Get/SetParamByName 委托给 `session.GetAttribute/SetAttribute`，即 **params = attrs**。 |
| Mock 实现 | `dispatcher.EnhancedMockMySQLServerSession` | 自带 `params map[string]interface{}`，不绑定 net.Session，易导致“临时 session、状态不持久”的问题。 |

### 2.2 当前用到的“上下文”键（按用途分类）

**认证与协议**

- `auth_status`、`auth_challenge`、`client_capabilities`、`max_allowed_packet`

**业务/引擎可见（应纳入 SessionContext 的候选）**

- `database` — 当前库（USE / COM_INIT_DB）
- `user` — 当前用户
- `autocommit` — 是否自动提交
- `character_set_client`、`character_set_connection`、`character_set_results`、`character_set_database`、`character_set_server`
- `sql_mode`、`time_zone`、`transaction_isolation` / `tx_isolation`、`net_write_timeout`、`net_read_timeout`
- `__sysvar_synced__` — 系统变量是否已同步（可视为实现细节）

**内部标记（可保留在 attrs 或单独标记）**

- `__result_sent__`、`should_close`、`session_id`（若存在）

### 2.3 读写位置汇总

- **写入**：认证完成（handler/decoupled_handler）、COM_INIT_DB、USE 语句（executor）、SET 变量（executor.setSessionVariable）。
- **读取**：CREATE TABLE/DROP TABLE/SHOW TABLES 等（executor、enginx）、系统变量引擎（system_variable_engine）、权限/审计（enhanced_message_handler）。

---

## 3. SessionContext 概念定义

### 3.1 定义

**SessionContext** 表示 **单条 MySQL 连接上的会话级状态**，是该连接上所有“会话变量”与“会话级语义状态”的唯一、显式载体。

- **归属**：一条连接对应一个 SessionContext 实例；建议由 **持有该连接的协议层**（如 net 层或与 sessionMap 同级的模块）创建并持有，随连接建立而创建、随连接关闭而释放。
- **传递**：通过 `MySQLServerSession` 接口（或新接口）暴露给 dispatcher 与 engine，保证 USE、SET、DDL/DML 等读写的是 **同一实例**。
- **与 net.Session 的关系**：SessionContext 可视为“会话状态”的 **结构化视图**；底层仍可落库到 `session.attrs` 以实现持久化与兼容，或逐步改为 SessionContext 为主、attrs 仅存协议内部标记。

### 3.2 职责边界

| 职责 | 说明 |
|------|------|
| 承载当前库、用户、自动提交等会话变量 | 供引擎与系统变量引擎读取，供协议层/引擎写入。 |
| 提供类型安全的访问方式 | 字段或 getter/setter，避免 string key + interface{}。 |
| 可选：承载事务状态 | 如 InTransaction、SavepointStack，为后续事务语义扩展预留。 |
| **不**负责 | 协议细节（如 auth_challenge、packet 序列号）、连接级 IO、认证流程本身；这些仍由 net.Session 或现有逻辑负责。 |

---

## 4. 结构设计

### 4.1 核心字段（强类型）

与当前代码中实际使用的键对齐，先纳入最常用、最易出错的项：

```text
SessionContext（建议的 Go 结构，仅作设计说明）

// 连接/会话标识（只读，创建时设定）
ConnectionID   uint32   // 与握手包 ConnectionID 一致，便于排查与 @@connection_id
SessionID      string   // 与 session.Stat() 或现有 session_id 一致

// 当前会话语义状态（引擎与协议层会读写）
CurrentDB      string   // 当前库，对应现有 "database"
Username       string   // 当前用户，对应现有 "user"
Host           string   // 客户端地址，可选

// 会话变量（与 SET / 系统变量引擎 对齐）
Autocommit     bool     // 对应 "autocommit"，默认 true
CharacterSet   string   // 简化：client/connection/results 同源时一个即可，或拆成多个字段
SQLMode        string
TimeZone       string
TransactionIsolation string

// 事务状态（预留，当前可为零值）
InTransaction  bool     // 是否在显式事务中（BEGIN 后未 COMMIT/ROLLBACK）
```

### 4.2 扩展方式（可选）

- **方式 A**：在 SessionContext 中保留 `Extra map[string]interface{}`，用于尚未升格为字段的变量（如 `net_write_timeout`），仍可通过 key 访问，但“主契约”在字段上。
- **方式 B**：所有 SET 变量仍通过现有 SetParamByName 写回 attrs，SessionContext 只维护“核心字段”，并从 attrs 同步或反写，保证与现有系统变量引擎兼容。

### 4.3 接口形态（与现有 MySQLServerSession 的关系）

两种可选方案，文档阶段仅描述，不绑定实现：

- **方案 1：SessionContext 作为 MySQLServerSession 的组成部分**  
  - `MySQLServerSession` 增加方法：`SessionContext() *SessionContext`。  
  - 引擎与 dispatcher 在需要会话状态时，优先从 `SessionContext()` 读/写；原有 `GetParamByName("database")` 等可改为内部从 SessionContext 取，或逐步废弃。

- **方案 2：SessionContext 独立，由持有 session 的层注入**  
  - 协议层（如 DecoupledMySQLMessageHandler）在创建/获取 `MySQLServerSession` 时，同时创建或绑定一个 `SessionContext`，并在调用引擎时一并传入（例如 `Dispatch(session, ctx, query)`）。  
  - 引擎 API 显式接收 `SessionContext`，减少对 string key 的依赖。

推荐 **方案 1**：对现有 `Dispatch(session, query, database)` 的改动最小，且“一个 session 一个 context”的归属关系清晰（session 持有 context）。

---

## 5. 生命周期与集成点

### 5.1 创建

- **时机**：连接建立后、认证完成前或认证完成时。  
- **位置**：在创建或绑定 `MySQLServerSession` 的地方（如 `OnOpen` 中 NewMySQLServerSession 之后），同时创建 `SessionContext`，并使之与 `MySQLServerSessionImpl` 关联（例如 MySQLServerSessionImpl 内持有一个 `*SessionContext`）。

### 5.2 更新

| 事件 | 更新内容 | 当前代码位置（参考） |
|------|----------|----------------------|
| 认证成功 | Username, Host, CurrentDB（若客户端指定了 database） | handler.handleAuth / decoupled_handler.handleAuthentication |
| COM_INIT_DB | CurrentDB | decoupled_handler.handlePacket（0x02 分支） |
| USE 语句 | CurrentDB | executor.executeUse → SetParamByName("database", dbName) |
| SET 变量 | Autocommit, CharacterSet, SQLMode 等 | executor.setSessionVariable → SetParamByName(...) |

以上在引入 SessionContext 后，应改为（或同时）写 SessionContext 的对应字段；若保留 attrs 兼容，可再同步写 attrs。

### 5.3 读取

- **引擎**：CREATE TABLE、DROP TABLE、SHOW TABLES、未带库名的表引用等，从 SessionContext.CurrentDB 读取；权限/审计从 Username/Host 读取；系统变量引擎从 SessionContext 对应字段或 Extra 读取。  
- **协议层**：如需根据当前库/用户做路由或日志，从同一 SessionContext 读。

### 5.4 销毁

- **时机**：连接关闭（OnClose、COM_QUIT 等）。  
- **动作**：释放 SessionContext，不再被任何引用；若 SessionContext 是 MySQLServerSessionImpl 的一部分，随 session 从 sessionMap 移除一并回收。

---

## 6. 与现有代码的对应关系（便于落地）

### 6.1 现有调用到 SessionContext 的映射（建议）

| 现有写法 | 迁移后（建议） |
|----------|----------------|
| `GetParamByName("database")` | `session.SessionContext().CurrentDB` 或保留 GetParamByName 但内部从 SessionContext 读 |
| `SetParamByName("database", dbName)` | `session.SessionContext().CurrentDB = dbName`（或 setter） |
| `GetParamByName("user")` | `session.SessionContext().Username` |
| `SetParamByName("autocommit", ...)` | `session.SessionContext().Autocommit = ...` |
| 其他 SET 变量 | SessionContext 对应字段或 Extra，或仍经 SetParamByName 写 attrs 再由 Context 同步 |

### 6.2 Mock 实现（测试 / 非连接路径）

- **EnhancedMockMySQLServerSession**：应持有一个 `*SessionContext` 或内联的 SessionContext 字段，所有 GetParamByName("database"/"user"/...) 从该 Context 读，SetParamByName 写回该 Context。  
- 这样“USE 改的是哪个 context”一目了然；在测试或特殊路径下若传入 Mock，应传入 **与真实连接共享同一 SessionContext** 的 Mock，或明确文档“该路径下 SessionContext 为请求级、不跨请求”。

### 6.3 兼容与渐进迁移

- **阶段 1**：引入 `SessionContext` 结构体及 `MySQLServerSession.SessionContext()`，在 `MySQLServerSessionImpl` 内创建并持有；认证、COM_INIT_DB、USE、SET 在写 attrs 的同时写 SessionContext。  
- **阶段 2**：引擎与 system_variable 引擎中，对 `database`、`user`、`autocommit` 等改为优先从 SessionContext 读；GetParamByName 内部可改为从 SessionContext 反查，保证旧调用仍有效。  
- **阶段 3**：逐步将更多变量迁入 SessionContext 字段或 Extra，减少对 string key 的依赖；Mock 统一改为使用 SessionContext。

---

## 7. 小结

- **SessionContext** 是“每条连接一个”的 **显式、可类型化的会话状态对象**，用于收敛当前分散在 key-value 中的 database、user、autocommit 等，并为事务状态等扩展预留位置。  
- **归属**：建议由协议层在建立连接时创建，并由 `MySQLServerSession` 实现体持有；通过 `SessionContext()` 暴露给引擎与 dispatcher。  
- **与现有逻辑**：可先与 `GetParamByName/SetParamByName` 及 `session.attrs` 并存，由 SessionContext 与 attrs 双写/同步，再逐步将读写迁移到 SessionContext，减少对 string key 的依赖，并统一 Mock 与真实连接的语义。

本文档仅描述概念与设计，不包含具体代码修改；实现时可从“创建 SessionContext + 在认证/COM_INIT_DB/USE/SET 处双写”开始，再逐步迁移读取路径与 Mock。
