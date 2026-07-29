# DataGrip 元数据兼容开发计划

> 目标：DataGrip 使用 MySQL 驱动连接 `xmysql-server` 后，连接、库表刷新、元数据 introspection 不再出现当前日志里的 `invalid system variable query`、`unsupported statement type`、`auto_increment` 解析错误。

## 1. 当前问题

服务端已能启动并接受 DataGrip 连接，端口为 `127.0.0.1:3309`。当前失败点不在握手认证，而在 DataGrip 连接后的元数据扫描阶段。

已观察到的失败类型：

| 错误 | 触发 SQL 类型 | 说明 |
| --- | --- | --- |
| `invalid system variable query` | `information_schema.views` 等 | 元数据 SQL 被错误路由到系统变量引擎 |
| `unsupported statement type` | `column_privileges union all table_privileges` | SQL parser 能解析为 `Union`，但执行器顶层分发不支持 |
| `parse error ... near 'auto_increment'` | `information_schema.tables` 查询 `auto_increment is not null` | DataGrip 的投影和过滤形态未被元数据兼容层提前接住 |
| `invalid system variable query` | `select database(), schema(), left(user(), instr(concat(user(),'@'),'@')-1)` | DataGrip 连接初始化 SQL 中有嵌套系统函数，现有系统函数执行器只识别顶层简单函数 |

DataGrip 当前还会查询这些元数据对象：

- `information_schema.tables`
- `information_schema.statistics`
- `information_schema.table_constraints`
- `information_schema.key_column_usage`
- `information_schema.partitions`
- `information_schema.triggers`
- `information_schema.events`
- `information_schema.routines`
- `information_schema.views`
- `information_schema.column_privileges`
- `information_schema.table_privileges`
- `mysql.procs_priv`

## 2. 设计边界

本计划只保证 DataGrip 元数据扫描不报错，不在本阶段实现真实的视图、分区、触发器、事件、存储过程、权限系统。

兼容策略：

1. 对已知 DataGrip 元数据 SQL，优先走元数据兼容路径，不进入系统变量引擎。
2. 对未实现的 MySQL 元数据对象，返回列形态正确的空结果集。
3. 对 `.frm` 中能读取的库、表、列，继续返回真实基础元数据。
4. 对 DataGrip 特殊 SQL 形态，例如 `UNION ALL` 和 `auto_increment is not null`，增加专门 fast path。

## 3. 需要修改的文件

| 文件 | 修改内容 |
| --- | --- |
| `server/dispatcher/system_variable_engine.go` | 扩展元数据 SQL 路由白名单，避免误判为系统变量查询 |
| `server/dispatcher/system_variable_engine_test.go` | 增加 DataGrip 元数据路由和初始化系统函数测试 |
| `server/innodb/engine/executor.go` | 增加 `views/partitions/triggers/events/mysql.procs_priv/UNION ALL/auto_increment` 元数据执行分支 |
| `server/innodb/engine/executor_ddl_test.go` | 增加 Go 层元数据 SQL 回归测试 |
| `jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java` | 增加 JDBC 层 DataGrip SQL 探测回归 |

## 4. 开发任务

### 任务一：补齐元数据路由

目标：DataGrip 查询的元数据表不再进入 `SystemVariableEngine`。

实现点：

- 在 `server/dispatcher/system_variable_engine.go` 中扩展 `informationSchemaMetadataTableNames()`：
  - `views`
  - `partitions`
  - `triggers`
  - `events`
- 增加 `mysql.procs_priv` 识别逻辑。
- `CanHandle()` 遇到上述元数据 SQL 时返回 `false`，交给 InnoDB 执行器。
- `isSystemTable()` 中不要把 `mysql.procs_priv` 当作系统变量查询处理。

测试：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher -run 'TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes' -count=1 -timeout=60s
```

验收：

- `information_schema.views`
- `information_schema.partitions`
- `information_schema.triggers`
- `information_schema.events`
- `mysql.procs_priv`

这些查询路由结果应为 `innodb`。

### 任务二：补齐空元数据结果集

目标：DataGrip 查询未实现对象时，服务端返回合法空结果集，而不是报错。

实现点：

- 在 `server/innodb/engine/executor.go` 的 `executeInformationSchemaMetadataSelect()` 中增加分支：
  - `information_schema.views`
  - `information_schema.partitions`
  - `information_schema.triggers`
  - `information_schema.events`
  - `mysql.procs_priv`
- 每类返回 `newInformationSchemaSelectResult(...)`。
- 结果行可以为空，但列名必须匹配 DataGrip 查询期望。

建议列形态：

```text
views: TABLE_NAME, VIEW_DEFINITION, DEFINER
partitions: TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME, PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION, PARTITION_METHOD, SUBPARTITION_METHOD, PARTITION_EXPRESSION, SUBPARTITION_EXPRESSION, PARTITION_DESCRIPTION, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, CREATE_TIME, UPDATE_TIME, CHECK_TIME, CHECKSUM, PARTITION_COMMENT, NODEGROUP, TABLESPACE_NAME
triggers: TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT, ACTION_TIMING, DEFINER
events: EVENT_NAME, EVENT_DEFINITION, EVENT_TYPE, EXECUTE_AT, INTERVAL_VALUE, INTERVAL_FIELD, STATUS, DEFINER
mysql.procs_priv: HOST, USER, ROUTINE_NAME, PROC_PRIV, IS_PROC
```

测试：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaJDBCProbeTablesReturnEmptyMetadataResults' -count=1 -timeout=120s
```

验收：

- 上述元数据 SQL 返回 `ResultType=query`。
- `Err == nil`。
- `ResultSetMetaData.getColumnCount() > 0`。
- 无需返回真实数据行。

### 任务三：支持 DataGrip 的权限 `UNION ALL`

目标：以下 SQL 不再触发 `unsupported statement type`：

```sql
select grantee, table_name, column_name, privilege_type, is_grantable
from information_schema.column_privileges
where table_schema = 'performance_schema'
union all
select grantee, table_name, null as column_name, privilege_type, is_grantable
from information_schema.table_privileges
where table_schema = 'performance_schema'
```

实现点：

- 在执行器顶层分发中支持 `*sqlparser.Union`。
- 如果 Union 左右两侧都是 `information_schema.column_privileges/table_privileges`，直接返回空结果集。
- 列名固定为：

```text
GRANTEE, TABLE_NAME, COLUMN_NAME, PRIVILEGE_TYPE, IS_GRANTABLE
```

测试：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaPrivilegesUnionAllReturnsEmptyResult' -count=1 -timeout=120s
```

验收：

- 不再返回 `unsupported statement type`。
- DataGrip 权限扫描阶段不报错。

### 任务四：支持 `information_schema.tables.auto_increment`

目标：以下 SQL 不再触发解析错误：

```sql
select table_name, auto_increment
from information_schema.tables
where table_schema = 'performance_schema'
  and auto_increment is not null
```

实现点：

- 在 `executeInformationSchemaTablesSelect()` 中识别 `table_name, auto_increment` 投影。
- 当前不实现真实 auto increment 元数据，遇到 `auto_increment is not null` 时返回空结果集即可。
- 列名固定为：

```text
TABLE_NAME, AUTO_INCREMENT
```

测试：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchemaTablesAutoIncrementProjectionReturnsRequestedColumns' -count=1 -timeout=120s
```

验收：

- 不再出现 `parse error ... near 'auto_increment'`。
- 返回合法 ResultSet。

### 任务五：增加 JDBC 回归测试

目标：用 JDBC 直接执行 DataGrip 当前会发出的元数据 SQL，防止后续改动回归。

实现点：

- 在 `jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java` 增加测试：
  - `testDataGripFullMetadataSqlProbesDoNotFail`
- 测试中执行当前日志里的 DataGrip 元数据 SQL。
- 每条 SQL 都要求：
  - `executeQuery()` 不抛异常。
  - `ResultSetMetaData.getColumnCount() > 0`。
  - 遍历 ResultSet 不报错。

测试命令：

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn -q '-Dtest=JdbcConnectionTest#testDataGripFullMetadataSqlProbesDoNotFail' test
```

验收：

- 单个 DataGrip 元数据 JDBC 测试通过。
- 完整 `JdbcConnectionTest` 通过：

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn -q -Dtest=JdbcConnectionTest test
```

### 任务六：支持 DataGrip 初始化系统函数 SQL

目标：以下 SQL 不再触发 `invalid system variable query`：

```sql
select database(), schema(), left(user(), instr(concat(user(),'@'),'@')-1)
```

实现点：

- 在 `server/dispatcher/system_variable_engine.go` 的 `executeSystemFunctionQuery()` 解析前识别该 SQL。
- 返回 3 列：

```text
database(), schema(), user
```

- `database()` 和 `schema()` 返回当前会话库名。
- `user` 返回 `user()` 中 `@` 前的用户名。

测试：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher -run 'TestSystemVariableEngine_DataGripSessionInfoQueryDoesNotFail' -count=1 -timeout=60s
```

验收：

- DataGrip 连接初始化阶段不再因为该 SQL 记录 `invalid system variable query`。
- JDBC 回归测试列表包含该 SQL。

## 5. 最终验收流程

### 5.1 启动服务

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go run . --configPath=conf/jdbc_local.ini
```

确认监听：

```bash
lsof -nP -iTCP:3309 -sTCP:LISTEN
```

### 5.2 跑 Go 回归

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher -run 'TestSystemVariableEngine_DoesNotRouteJDBCInformationSchemaProbes' -count=1 -timeout=60s
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'TestInformationSchema|Test.*Metadata|Test.*PrivilegesUnion|Test.*AutoIncrement' -count=1 -timeout=120s
```

### 5.3 跑 JDBC 回归

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server/jdbc_client
mvn -q -Dtest=JdbcConnectionTest test
```

### 5.4 用 DataGrip 实连

连接参数：

```text
Host: 127.0.0.1
Port: 3309
User: root
Password: root@1234
Driver: MySQL Connector/J
```

操作：

1. 新建 DataGrip 数据源。
2. Test Connection。
3. 展开 schema。
4. 手动 Refresh。

验收标准：

- DataGrip 不弹连接错误。
- schema 刷新不失败。
- 当前时间之后的日志里不再出现：

```text
invalid system variable query
unsupported statement type
parse error: syntax error at position 74 near 'auto_increment'
```

检查日志：

```bash
tail -n 200 tmp/jdbc_logs/error.log
```

## 6. 提交范围

只提交这些文件：

```text
server/dispatcher/system_variable_engine.go
server/dispatcher/system_variable_engine_test.go
server/innodb/engine/executor.go
server/innodb/engine/executor_ddl_test.go
jdbc_client/src/test/java/com/xmysql/server/test/JdbcConnectionTest.java
docs/development/DATAGRIP_METADATA_COMPATIBILITY_PLAN_20260725.md
```

不要提交运行时产物：

```text
jdbc_client/target/
server/net/data/
tmp/
```

## 7. 风险和后续

| 风险 | 处理 |
| --- | --- |
| DataGrip 版本不同，SQL 形态略有差异 | 以日志为准继续补 metadata fast path |
| 返回空元数据会让部分高级功能不可见 | 本阶段只保证连接和刷新不报错，真实对象后续单独实现 |
| Parser 对部分 SQL 仍可能先失败 | 对已知 DataGrip metadata SQL 在解析失败前增加字符串级 fast path |

完成本计划后，DataGrip 连接 XMySQL 的最低可用标准是：能连接、能刷新、能展开 schema，元数据扫描阶段无服务端错误。
