# MySQL 8.4 Compatibility Development Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按 P0/P1 优先级将 XMySQL 先交付为“单机 MySQL 核心可运行 + Connector/J 可接入业务”的产品，再推进主从集群；FULLTEXT、全量客户端和其他高级 MySQL 语义后置。

**Architecture:** P0 保留现有 Go + InnoDB 风格单机架构，优先打通核心 SQL、事务、恢复、协议和 Connector/J；P1 在事务提交/change-set 基础上增加 1 主多从、binlog/GTID、复制和故障转移。每个能力先建立 reference MySQL 8.4 对照测试，再实现最小正确路径，最后补并发、恢复和错误契约验证。

**Tech Stack:** Go 1.24；现有 `server/innodb/sqlparser`、`server/innodb/engine`、`server/innodb/plan`、`server/innodb/manager`、`server/net`、`server/protocol`；Java/JDBC Maven 集成测试；PowerShell/Bash 验证脚本。

**Spec:** `docs/planning/CAPABILITY_PRIORITY_INDEX_20260715.md`、`docs/planning/P0_CAPABILITY_BACKLOG_20260715.md`、`docs/planning/P1_CAPABILITY_BACKLOG_20260715.md`、`docs/planning/PX_CAPABILITY_BACKLOG_20260715.md`、`docs/planning/P0_EVIDENCE_RUN_20260722.md`

## Global Constraints

- 兼容基线固定为 MySQL 8.4 LTS；不以 MySQL 9.x 的新增功能作为当前验收门槛。
- 产品边界是单机数据库；原生 MySQL `.ibd`、官方 InnoDB 数据字典和 binlog 文件直接互读不属于本计划的核心目标。
- 任何“已实现”结论必须同时具备 parser/dispatcher、executor/storage、结果集或错误契约、自动化测试四类证据。
- 现有 77 个聚焦 JDBC 测试作为回归基线，不得因新增兼容语义而退化。
- 新增 SQL 能力优先使用独立模块和接口，避免继续扩张单一大文件；现有文件若已承担相同职责，先局部重构再接入。
- 每个任务完成后必须运行对应 Go 测试；涉及客户端行为时必须运行 JDBC 测试；涉及恢复或并发时必须生成可审计报告。
- 不以“解析成功”作为功能完成标准；必须验证真实数据、事务边界、NULL 语义、SQLSTATE、MySQL errno 和警告行为。

## Current Boundary

当前聚焦基线已经覆盖：基础 `INSERT/SELECT/UPDATE/DELETE`、prepared statement、提交/回滚/savepoint、基础数据库和表 DDL、`TRUNCATE`、数据驱动的 `SHOW`/`INFORMATION_SCHEMA`/`EXPLAIN`、基础索引和约束，以及协议、认证和运行指标 smoke 测试。

当前仍有明确边界：完整 `ALTER TABLE` AST/在线重建、完整 `INFORMATION_SCHEMA/PERFORMANCE_SCHEMA`、通用递归/相关子查询 AST 执行、完整窗口 aggregate frame、存储对象完整执行、类型/函数/collation 全矩阵、分区物理表空间与 pruning、FULLTEXT/spatial 真正索引维护、MySQL 原生 binlog/GTID 全量协议和完整复制语义，以及其他客户端全量兼容。P0 Connector/J 已完成当前套件回归；P1 已新增 source/replica 的逻辑 DML 和基础 DDL 复制、GTID 重试、手动提升路径；P3 已新增“多数派可见 + 源不可见 + 确定性候选”的受保护自动提升路径和运行时成员查询/更新/持久化，但不宣称具备外部租约/磁盘 fencing 级脑裂治理。当前交付的是可审计的 MySQL 8.4 核心兼容子集，不宣称全量兼容。

本轮继续收敛：角色激活已从“所有已授予角色默认合并”扩展为会话级 `SET ROLE NONE/ALL/DEFAULT/<role>`，支持 `SET DEFAULT ROLE` 持久化、权限查询按激活角色计算，并覆盖角色撤销权限、恢复权限和默认角色回归；存储过程 handler 已支持活动条件跟踪，`RESIGNAL` 可重新抛出原 SQLSTATE 并重写 `MESSAGE_TEXT`。角色完整的授权选项、mandatory roles、角色管理权限模型和 handler 全部作用域仍不宣称完成。

## Confirmed Priority Replan (2026-08-22)

本计划按用户确认后的交付顺序执行：

| 优先级 | 交付目标 | 范围 | 暂不纳入 |
|---|---|---|---|
| **P0-A** | 单机 MySQL 核心可运行 | 服务启动、协议、认证、核心 DDL/DML、约束、事务、锁、恢复、基础 metadata、权限、备份和可观测性 | FULLTEXT、SPATIAL、全量存储对象、复杂优化器和完整 MySQL 8.4 语义 |
| **P0-B** | Connector/J 可接入业务 | JDBC 连接、PreparedStatement、ResultSet、事务、batch、metadata、generated keys、warning、常见类型、连接池和 Spring/JDBC 基础路径 | 非 Connector/J 客户端的全量边界兼容 |
| **P1** | 可用主从集群 | 1 主多从、binlog、GTID、初始同步、断线重连、去重、复制恢复、只读副本、手动故障转移 | 多主写入、分布式事务、自动选主和脑裂治理 |
| **P2** | P0/P1 稳定化 | 性能、并发、备份恢复、监控告警、复制延迟、数据一致性巡检和发布门禁 | 不新增高级 SQL 范围 |
| **P3** | 生产级集群增强 | 自动故障转移、成员管理、脑裂防护、在线 DDL 和更完整运维工具 | 不作为 P0/P1 的前置条件 |
| **P4** | 高级兼容能力 | FULLTEXT、SPATIAL/R-tree、物理分区、完整 stored object、完整类型/函数/collation、全量客户端 | 当前版本明确后置 |

P0-A 和 P0-B 同级，必须共同达到“可运行、可接入、可回归”的退出条件后，才进入 P1 集群工作。P1 采用先实现主从复制和手动切换的保守路径；多主、自动选主和脑裂防护不阻塞首个集群版本。

### P0 Exit Criteria

- MySQL CLI 或等价基础客户端能够连接、建库、建表并完成 CRUD。
- `BEGIN/COMMIT/ROLLBACK`、约束错误、权限错误和 warning 在 COM_QUERY 与 JDBC 路径上保持一致。
- 服务强制退出并重启后，已提交数据保留，未提交数据回滚。
- Connector/J 测试覆盖连接、PreparedStatement、ResultSet、事务、batch、metadata、generated keys、常见数据类型和连接池基础路径。
- 现有 Go 核心回归和 Connector/J 回归全部通过；不得使用 stale report 作为通过证据。

### P1 Exit Criteria

- 至少 1 主 2 从实例可以启动并完成初始数据同步。
- 已提交事务按 GTID 复制到副本；断线重连和副本重启不会重复应用。
- 主节点停止后可以手动提升副本继续读写，并生成可审计的切换报告。
- 复制异常、延迟、GTID 缺口和数据校验结果可查询。

## Implementation Status (2026-08-22)

已落地并有 Go 回归测试覆盖：外键（含复合键和级联动作）、CHECK 三值语义、默认值、生成列、REPLACE/ON DUPLICATE KEY UPDATE、affected rows/自增键/`INSERT IGNORE` warnings、ADD/DROP/MODIFY/CHANGE/RENAME TABLE 及索引 DDL、旧行类型转换和 ALTER 失败原子回滚、数据驱动的 SHOW/INFORMATION_SCHEMA/EXPLAIN、`CHECK/ANALYZE/OPTIMIZE TABLE` 最小真实行为、LOCK TABLES/UNLOCK TABLES、视图、UNION 外层排序/限制/去重、IN/NOT IN NULL 语义、ANY/SOME/ALL 量化子查询、多 CTE 展开、CTE 重复名/列名/前向引用校验、递归 CTE canonical path、CTE + JOIN 与 CTE + DML 的受限改写、相关 EXISTS/标量子查询、窗口 `ROW_NUMBER/RANK/DENSE_RANK/NTILE/LAG/LEAD/FIRST_VALUE/LAST_VALUE` 的基础分区排序、`ROWS/RANGE` 边界、默认 frame、数值/NULL 排序和窗口结果列类型、prepared long data/cursor/reset connection、多结果集状态位和 OK warning count、错误契约、账户管理和数据库/表/列级权限检查、角色继承、认证插件元数据、存储对象定义/元数据持久化、BEFORE INSERT/UPDATE 触发器最小执行、存储过程局部 `DECLARE/SET/WHILE`、SQL EVENT 一次性/周期性调度、分区描述符/DML 越界校验与 DROP PARTITION 数据清理、FULLTEXT/SPATIAL 的最小查询路径、GTID/source/replica 重启去重、动态 Performance Schema 语句摘要、Prometheus runtime recorder、慢查询 logger，以及执行器 query latency/error metrics。

仍未达到 MySQL 8.4 全量兼容：cursor 的完整关闭/边界语义、多结果集完整客户端能力协商、角色管理完整语义、递归/相关子查询的通用 AST 执行、完整窗口 frame、存储过程的完整 cursor/HANDLER/动态 SQL复杂参数与完整 scope、函数在任意查询表达式中的完整调用矩阵、完整类型/函数/collation 矩阵、统计驱动优化器的完整代价模型、完整 Performance Schema、分区物理路由/独立表空间与存储层 pruning、FULLTEXT/spatial 的真正倒排/R-Tree 维护、MySQL 原生 binlog/GTID 全量协议以及复杂 DDL/对象复制。FULLTEXT 与全量客户端按当前需求明确后置；其余边界仍需后续逐项收敛，不能把当前兼容子集宣称为全量兼容。

### 追加实现证据

### 最新交付状态（2026-08-23）

- P0-A/P0-B 已达到当前计划的退出条件：`reports/compatibility/release-candidate-final-current/release-candidate.json` 为 `GO`；build、unit、integration、go-core、crash recovery、concurrency、observability 全部 PASS，隔离 3311 实例上的 Connector/J 最新完整套件为 136 tests、0 failures、0 errors、1 skipped。
- P1 当前交付子集已达到退出条件：`reports/compatibility/p1-cluster-final-current/cluster-report.json` 为 `PASS`，覆盖 1 主 2 从、提交复制、回滚隔离和手动提升后的继续写入；新增 `TestEngineSourceReplicaInitialSyncFromEmptyReplica` 验证副本为空时从 source 的已提交 DDL/DML 日志完成初始同步，`TestReplicaRejectsClientWritesWhileReplicationIsActive` 验证只读副本拒绝客户端写入。
- P3 集群增强已补入 `RuntimeConfig.Peers/AutoFailover/FailureTimeout` 和复制运行时：只有达到成员多数派、无法观察到 source、且本节点是确定性最小候选时才自动 promote；三副本故障转移回归验证只提升一个副本，其他副本保持只读 replica。`GET/POST /replication/members` 提供成员健康视图和 replace-all 更新，成员列表持久化到 `replication/members.json`，重启后自动恢复。
- 因此后续工作不再阻塞 P0 单机运行或 Connector/J 接入，按用户要求继续转入剩余 P1/P3/P4 后置增强；FULLTEXT 和非 Connector/J 全量客户端继续明确后置，不作为当前发布门禁条件。

- 复杂查询入口：`server/innodb/engine/cte_compatibility.go`、`correlated_subquery_compatibility.go`、`window_query_compatibility.go`；对应测试覆盖递归 CTE、相关 EXISTS、LAG/NTILE。
- CTE/window 语义补强：`cte_executor.go` 对名称大小写、重复定义、重复列名和前向引用做校验；`window_query_compatibility.go` 对 `ROWS/RANGE` frame、默认 frame、边界和数值/NULL 排序做执行级验证。
- 窗口 AST 入口：`sqlparser.Select` 保留 `OVER`、`WINDOW name AS (...)` 和 frame；`Walk`/`Format` 可遍历并重建 named window 声明。
- 存储对象安全边界：显式 `DEFINER='user'@'host'` 创建时校验账户存在，`CALL` 在会话身份存在时校验 `EXECUTE` grant；对应缺失 definer/权限测试已通过。
- 触发器执行补强：BEFORE INSERT/UPDATE 可修改 `NEW`；AFTER INSERT/UPDATE/DELETE 支持同库 DML 审计副作用并拒绝 `SET NEW`，由 `stored_object_compatibility_test.go` 覆盖。
- 存储函数执行补强：`SELECT function(args)` 支持参数替换、局部变量 `SET`、标量 `RETURN`、算术与常用内置函数，并保留 definer/EXECUTE 权限检查；单表行投影、`WHERE` 谓词和嵌套算术表达式也会读取当前行值；复杂控制流、任意 JOIN/GROUP/ORDER 表达式调用矩阵仍未宣称完成。
- 复制提交边界：binlog append 先 `fsync`，source 启动时从已提交 COMMIT 事件重建 GTID；replica 只在 COMMIT 后原子持久化 GTID 与已应用 row change，覆盖状态文件丢失、部分事务和重启重复应用。
- grant 状态写入：账户语句在 executor 内串行化，并继续使用原子 metadata 文件替换；事务中的 GRANT/REVOKE/账户变更先挂起到 session，COMMIT 原子落盘、ROLLBACK 丢弃；`FLUSH PRIVILEGES` 触发认证缓存 reload，并由并发、回滚和缓存刷新测试验证。
- 分区维护：RANGE 分区已覆盖 create/insert/select/add/drop/truncate 的逻辑 descriptor 矩阵；`DROP PARTITION` 会在提交 descriptor 前删除被丢弃分区中的真实行，`TRUNCATE PARTITION` 删除目标分区行但保留 descriptor，`ADD PARTITION` 校验重复名称和 RANGE 上界递增并原子更新 `.frm` 和 table storage mapping；`plan.PrunePartitions` 已修正 RANGE `<`/`<=`/`>`/`>=` 边界并接入单表常量谓词的行物化过滤，跨负数、边界和 MAXVALUE 的结果回归通过；独立物理分区文件/表空间和存储扫描级 pruning 仍未宣称完成。
- 本轮核心回归：`go test ./server/innodb/engine ./server/innodb/sqlparser ./server/innodb/plan ./server/innodb/manager ./server/protocol ./server/net ./server/auth ./server/replication -count=1` 全部通过；隔离 xmysql 实例上的 `mvn test -Pjdbc-connectivity` 通过 135 个测试、0 失败、0 错误、1 跳过。
- Parser AST 入口：`sqlparser.With`、`sqlparser.CTEDefinition`、`sqlparser.WindowSpec` 和 `sqlparser.WindowFrame` 已支持格式化、遍历及 yacc fallback；执行器在兼容重写前先做 CTE AST scope validation。
- 结果和 session 状态：`Result.AffectedRows/LastInsertID/Warnings`、`SHOW WARNINGS`、reset connection 清理；字符串长度截断和 `INSERT IGNORE` 非法整数降级（1366）均生成可查询 warning，其他类型转换/日期警告矩阵仍待补齐。
- 账户和元数据：`accounts.json` 现在保存 plugin、TLS 标记、roles、column grants；`INFORMATION_SCHEMA` 权限视图读取该持久化数据；`SHOW PROCESSLIST` 和 `INFORMATION_SCHEMA.PROCESSLIST` 返回当前 session 的动态状态行。
- 高级能力：分区 descriptor 参与 INSERT/UPDATE 归属校验；FULLTEXT/SPATIAL 当前是兼容查询路径，不等同于完整索引存储；`server/replication` 已提供 GTID、binlog JSONL、source/replica 和 duplicate-GTID 去重单测。
- P1 集群接入：`server/replication/runtime.go` 提供 source/replica HTTP 拉流、GTID position、断线重试、状态查询和 `/replication/promote`；引擎提交钩子将已提交 DML 以逻辑 SQL 事件写入 source，replica 使用本地执行器回放；`TestEngineSourceReplicaReplicatesCommittedDML` 覆盖 1 主 2 从、提交、回滚和手动提升后的继续写入，复制层测试覆盖 GTID 状态持久化与重复应用去重。
- P1 DDL 复制补强：已提交的基础数据库/表 DDL 和 stored-object definition 会写入逻辑 binlog；replica 按 GTID 事务边界逐批应用，失败重试不会重复执行已完成事务；`TestEngineSourceReplicaReplicatesCommittedDDL` 与 `TestEngineSourceReplicaReplicatesStoredObjectDefinition` 覆盖 1 主 1 从场景。
- P1 协议接入补强：decoupled handler 在 source 节点会把真实 `replication.Source` 绑定到 session，`COM_BINLOG_DUMP` 不再依赖测试手工注入 source；复杂 row-event 编码、GTID 集合协商和完整复制语义仍未完成。
- P1 原生流补强：`COM_BINLOG_DUMP` 现在输出带 19 字节原生 event header 的 `GTID_EVENT/QUERY_EVENT/XID_EVENT` wire payload，`COM_BINLOG_DUMP_GTID` 能解析文件名后的 64 位 position 和二进制 GTID interval set，并按完整事务过滤；逻辑存储和内部 source/replica 回放仍保持 JSONL/GTID 事务边界。
- P1 GTID 切换补强：Source 拉流支持副本通过 `gtids` 声明已执行集合并按完整事务边界过滤；副本提升为 Source 时继承已执行 GTID，并持久化新的 Source 序列；`SHOW MASTER STATUS`/`SHOW SOURCE STATUS`、`SHOW BINARY LOGS`、`SHOW BINLOG EVENTS` 和 `SHOW REPLICA STATUS` 均有 SQL 观察入口。
- P1 认证补强：`caching_sha2_password` 支持 64 位 stage2 SHA-256 密码生成、快速握手、TLS cleartext full-auth 和非 TLS 临时 RSA 公钥/加密密码交换；完整 privilege-option 语义仍未完成。
- 本轮 SQL 语义补强：未关联/相关标量子查询支持单值、空结果 `NULL` 和多行错误，新增 ANY/SOME/ALL 空集与 NULL 语义；SELECT 常量/`DUAL` 结果返回真实单行；存储过程支持 `OUT/INOUT` 通过会话用户变量回写，并补齐局部 `DECLARE/SET`、简单 `IF/ELSE`、`WHILE` 控制流和算术赋值。SQL EVENT 现在可从持久化定义解析一次性/周期性 schedule，并在显式启用 scheduler 后真实执行 `DO` 语句；复杂游标、HANDLER、CONTINUE/EXIT handler、动态 SQL 和完整 routine scope 仍未宣称完成。
- P0 协议字符集补强：`CharsetConverter` 已使用 `golang.org/x/text` 实现 utf8/utf8mb4、latin1/latin2、ASCII、GBK/GB2312、Big5、EUC-KR、Shift-JIS 等常用映射，并拒绝非法 UTF-8；`server/protocol/charset_manager_test.go` 覆盖往返和非法输入。
- 发布基线补强：`tmp/go.mod` 将历史临时客户端脚本隔离为嵌套模块，根模块 `go test ./...` 现在可以完成全仓业务包回归。
- P1 集群验收入口：`scripts/compatibility/cluster_smoke.ps1` 可重复执行真实引擎的 1 主 2 从、提交复制、回滚隔离和手动提升场景，并生成 `reports/compatibility/p1-cluster-current/cluster-report.json` 与 JSONL 证据。
- 最新复制回归：新增 Source 端 GTID 集合过滤、提升后 GTID 继承/重启校验和 `SHOW MASTER STATUS` 测试；`go test ./...` 与 `cluster_smoke.ps1` 在本轮改动后均通过。
- 本轮最终回归：`go test ./...` 通过；`cluster_smoke.ps1` 结果为 PASS；隔离 3311 实例上的 Connector/J 核心套件（连接、DDL/DML、PreparedStatement、事务）60 tests、0 failures、0 errors、1 skipped 通过。
- 2026-08-23 追加回归：`go test ./... -count=1` 通过；`cluster_smoke.ps1` 重新生成 PASS；隔离 3311 实例上的完整 `mvn -q -Pjdbc-connectivity test` 通过 136 项、0 failures、0 errors、1 skipped（含 8 项 PerformanceTest，约 515 秒）；端口已释放。
- 2026-08-23 追加实现：`OptimizerManager` 已从固定顺序扫描改为读取表统计、识别谓词命中的索引候选并按估算代价选择访问路径；该路径仍是基础代价模型，不等同于 MySQL 完整 CBO/直方图优化。
- 运维证据：`scripts/compatibility/crash_recovery_matrix.ps1/.sh`、`observability_smoke.ps1`、`release_candidate_gate.ps1` 和对应 runbook/matrix 已建立重复恢复、可观测性和发布判定入口；外部 kill、逻辑备份和 PITR 已有独立证据，但仍需纳入最终发布门禁复跑。
- 外部恢复证据：`scripts/compatibility/external_crash_recovery_matrix.ps1` 使用独立运行目录启动真实服务，执行 prepared committed、uncommitted、DDL、drop/add index rebuild workload，强制终止服务后重启并校验 rows/indexes/metadata；`reports/compatibility/external-crash/external-crash-20260822-124623.json` 为 3/3 PASS。
- 备份证据：`server/backup/logical_backup_test.go` 验证跨目录 logical backup export/import，`RestoreUntilPosition` 验证按 binlog position 的 PITR 恢复。
- Live metrics 补强：引擎入口和执行器均记录真实查询计数/延迟与执行错误；`BEGIN/COMMIT/ROLLBACK` 更新活动事务、提交和回滚指标，并由 `TestRuntimeMetricsFollowTransactionLifecycle`、`TestRuntimeMetricsRecordEngineErrors` 验证。
- 锁治理补强：`WaitEdge` 已保留 waiting/blocking transaction、resource 和等待起点；死锁 victim 在同一等待时间下按较小事务 ID 稳定选择，并由 `TestLockManagerVictimSelectionIsDeterministicOnEqualWaitTime` 验证。
- InnoDB 诊断补强：`SHOW ENGINE INNODB STATUS` 返回 MySQL 三列结果形状，并动态包含活动事务和锁等待摘要；完整 buffer-pool、I/O、死锁历史统计仍未宣称完成。
- 协议稳定性补强：移除跨请求 `__result_sent__` session 标志，避免并行包处理时前一结果覆盖下一查询状态并伪造 `result already sent`；`go test ./server/net -count=1` 和 DataGrip 元数据 JDBC 回归均通过。
- 权限失败矩阵：新增 user@host、global、database、table、column 五级拒绝测试，并验证 host mismatch 不会通过默认权限回退。
- JDBC 兼容证据：`JdbcTestConfig` 支持环境变量/系统属性注入，发布门禁自动启动隔离 3311 实例；`DataTypeTest`、DDL/DML、约束、JOIN、性能、prepared statement、SELECT、系统变量和事务套件共 136 个测试通过，0 失败、0 错误、1 跳过，新增 `getColumns/getPrimaryKeys/getIndexInfo` 元数据回归也通过。
- 发布门禁补强：`release_candidate_gate.ps1` 已固定 clean-data、build、unit、integration、recovery、concurrency、metrics、JDBC 顺序，并把 git revision、运行环境、报告目录和测试计数写入 JSON；JDBC URL 端口解析、隔离服务启动、stale artifact、panic、数据差异、live metrics 和缺失证据均纳入 NO-GO 判定。
- 最终门禁证据：`reports/compatibility/release-candidate-final-current/release-candidate.json` 中 clean-data、build、unit、integration、go-core、三次 crash-recovery、100 次 concurrency、observability 和隔离 xmysql JDBC 均 PASS；本轮完整 JDBC 结果保存在 `jdbc_client/target/surefire-reports`，记录 136 tests、0 failures、0 errors、1 skipped。后续新增的空副本初始同步测试和 1 主 2 从集群 smoke 也再次通过。
- 2026-08-23 继续实现：相关 predicate 子查询已支持逐外层行重新绑定的 `IN/NOT IN/ANY/SOME/ALL` 受限路径；存储函数可在行投影中读取当前行值；SQL EVENT 已支持 `ALTER ... ENABLE/DISABLE/RENAME`、`CREATE IF NOT EXISTS` 幂等、一次性事件 `ON COMPLETION PRESERVE/NOT PRESERVE` 生命周期。对应 engine 专项测试与 `go test ./... -count=1` 全部通过。
- 2026-08-23 回归更新：存储函数已接入单表 `WHERE` 谓词和嵌套算术表达式；分区规划器修正 RANGE 边界并接入单表常量谓词的行物化过滤；最新 `go test ./... -count=1` 与 `scripts/compatibility/cluster_smoke.ps1 -ReportDir reports/compatibility/p1-cluster-final-current` 均通过，后者报告为 `PASS`。
- 2026-08-23 发布门禁修复与复验：Windows 游标关闭后的 DROP TABLE 资源竞态已补充 tablespace identity 兜底、有限重试和隔离门禁数据目录清理；`reports/compatibility/release-candidate-final-current/release-candidate.json` 最新状态为 `GO`，build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，Connector/J 完整套件 136 tests、0 failures、0 errors、1 skipped；之后再次运行的 `go test ./... -count=1` 与集群 smoke 也通过。
- 2026-08-23 窗口与成员管理补强：`SUM/AVG/MIN/MAX/COUNT` 已支持受限 partition/frame 计算，覆盖运行累计、数值 `RANGE n PRECEDING/FOLLOWING`、分区计数和平均值；`Runtime.Members/UpdatePeers` 及 `/replication/members` 已支持成员探测、去重、替换、持久化和重启加载。对应专项回归与最新 `go test ./... -count=1` 通过；完整 window frame、外部租约/fencing 仍明确是后续边界。
- 2026-08-23 routine 控制流补强：存储过程在已有 `DECLARE/SET/IF/WHILE` 基础上支持 `REPEAT ... UNTIL`，并验证“先执行 body、再判断条件”的 MySQL 语义；游标/HANDLER/动态 SQL/完整 handler scope 仍未宣称完成。
- 2026-08-23 routine cursor 补强：支持单个 `DECLARE ... CURSOR FOR SELECT`、`DECLARE CONTINUE HANDLER FOR NOT FOUND`、`OPEN/FETCH/CLOSE` 和带标签 `LOOP/LEAVE`，通过真实表逐行求和测试；多个 handler、EXIT handler、动态 SQL、游标参数/敏感性和完整 block scope 仍未宣称完成。
- 2026-08-23 EXIT handler 补强：`DECLARE EXIT HANDLER FOR NOT FOUND` 在游标无更多行时会执行 handler 并退出当前 routine；`CONTINUE/EXIT` 均有真实过程回归，多个 handler、SQLWARNING/SQLEXCEPTION 条件和完整 block scope 仍未宣称完成。
- 2026-08-23 SQLEXCEPTION/SQLWARNING handler 补强：`DECLARE CONTINUE/EXIT HANDLER FOR SQLEXCEPTION` 可捕获过程内嵌 SQL 执行错误，`SQLWARNING` 可捕获嵌套 DML warning，并按 handler 类型继续或退出；多条件 handler、动态 SQL和完整 block scope仍未宣称完成。
- 2026-08-23 动态 SQL 补强：过程内部支持 `PREPARE ... FROM`、`EXECUTE` 和 `DEALLOCATE PREPARE`，复用会话变量和普通执行器完成真实 `SET` 动态语句；`USING` 参数绑定、多结果/游标动态语句和完整 prepared statement 生命周期仍未宣称完成。
- 2026-08-23 动态 SQL 参数补强：`EXECUTE stmt USING @var[, ...]` 会按顺序替换动态语句中的 `?` 参数并复用普通执行器，真实过程回归通过；复杂表达式参数、多结果/游标动态语句和完整 prepared statement 生命周期仍未宣称完成。
- 2026-08-23 DML warning 补强：整数、DECIMAL/DOUBLE/FLOAT 和 DATE/DATETIME/TIME/YEAR 列在严格路径下执行类型校验，`INSERT IGNORE` 的非法值会写入对应零值并返回 1366/1292 warning，且可通过 `SHOW WARNINGS` 读取；ENUM/SET、范围溢出和完整 sql_mode 转换矩阵仍未宣称完成。
- 2026-08-23 ENUM/SET 类型补强：合法值列表已从 DDL parser 写入 `.frm`/表 metadata，重启加载后仍可用；ENUM 支持名称/序号，SET 支持成员校验与定义顺序规范化，严格非法值报错，`INSERT IGNORE` 对 ENUM 写空值、对 SET 保留合法成员并产生 1265 warning。范围溢出和完整 sql_mode 转换矩阵仍未宣称完成。
- 2026-08-23 最终发布复验：`release_candidate_gate.ps1 -ReportDir reports/compatibility/release-candidate-final-current` 本轮重新执行并返回 `GO`，报告时间 `2026-08-23T06:34:11.6653638Z`；全部门禁 PASS，Connector/J 为 136 tests、0 failures、0 errors、1 skipped；之后的 `go test ./... -count=1` 和集群 smoke 也均为 PASS。
- 2026-08-23 游标补强后最终复验：最新发布门禁报告时间为 `2026-08-23T07:00:28.6526265Z`，状态 `GO`；所有门禁 PASS，Connector/J 仍为 136 tests、0 failures、0 errors、1 skipped，游标/HANDLER/LOOP 新增路径下的 `go test ./... -count=1` 也通过。
- 2026-08-23 复制对象回归：`TestEngineSourceReplicaReplicatesAlterTableAndTriggerDefinitions` 验证 `ALTER TABLE ADD COLUMN/ADD INDEX` 与触发器定义会作为同一 GTID 事务复制并在副本真实元数据中可见；复杂在线 DDL、完整对象依赖图和原生 row-event 仍未宣称完成。
- 2026-08-23 窗口与 UPDATE 类型补强：窗口执行器支持单个 `WINDOW name AS (...)` 声明并允许 `OVER name` 复用分区、排序和 frame；UPDATE 对整数列的非法字符串赋值在严格路径下返回错误，NULL 仍按可空列语义保留；UPDATE 无 WHERE 时可扫描并更新全表；DECIMAL/DATE/ENUM 等完整转换和 warning 矩阵仍未宣称完成。
- 2026-08-23 角色 DDL 补强：`CREATE ROLE`/`DROP ROLE` 会持久化为可授予账户，删除角色时同步清理已有用户的角色边；完整 `SET ROLE`/默认角色/角色激活范围仍未宣称完成。
- 2026-08-23 继续兼容补强：存储过程支持嵌套 `BEGIN...END` 语句块；窗口排序支持多列及每列 ASC/DESC；优化器按复合索引连续前缀估算访问代价，避免跳过首列误选索引。对应 engine/manager 专项回归通过。
- 2026-08-23 routine cursor 边界补强：游标增加 OPEN 状态，`FETCH`/`CLOSE` 在未打开时返回明确错误，避免未打开游标被当作空结果处理；正常 `OPEN/FETCH/CLOSE`、NOT FOUND handler 和嵌套 block 回归仍通过。
- 2026-08-23 binlog wire 补强：`COM_BINLOG_DUMP` 的逻辑 row change 已展开为 `TABLE_MAP_EVENT` + `WRITE_ROWS/UPDATE_ROWS/DELETE_ROWS_EVENTv2` + `XID_EVENT`，补充表名终止字节、VAR_STRING metadata、按 action 的 row image 和 native event header 回归；存储层仍以逻辑 change set/JSONL 为主，schema-aware 全量 row codec、checksum/negotiation 和复杂复制语义仍需后续收敛。
- 2026-08-23 最终回归复验：本轮 `go test ./... -count=1` 通过；`scripts/compatibility/cluster_smoke.ps1 -ReportDir reports/compatibility/p1-cluster-final-current` 报告 `PASS`（`2026-08-23T07:22:06Z`）；`release_candidate_gate.ps1 -ReportDir reports/compatibility/release-candidate-final-current` 返回 `GO`，报告时间 `2026-08-23T07:31:42.7588259Z`，Connector/J 为 136 tests、0 failures、0 errors、1 skipped，其他 clean-data/build/unit/integration/go-core/recovery/concurrency/observability 门禁全部 PASS。
- 2026-08-23 门禁回归修复：Connector/J 的 `YEAR` 数值字面量（例如 `2024`）已纳入 DML 类型转换，避免被误判为非法日期；新增 `TestP0YearAcceptsNumericLiteral`，随后 `go test ./... -count=1` 通过。最新 `release_candidate_gate.ps1 -ReportDir reports/compatibility/release-candidate-final-current` 返回 `GO`，报告时间 `2026-08-23T08:17:47.0909546Z`，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 2026-08-23 类型矩阵继续收敛：ENUM/SET 合法值、严格错误、IGNORE warning 和表 metadata 持久化回归后，`go test ./... -count=1` 通过；最新发布门禁报告 `reports/compatibility/release-candidate-final-current/release-candidate.json` 时间 `2026-08-23T08:35:41.8250603Z`，状态 `GO`，Connector/J 136 tests、0 failures、0 errors、1 skipped；集群 smoke `reports/compatibility/p1-cluster-final-current/cluster-report.json` 时间 `2026-08-23T08:35:51.2313125Z`，状态 `PASS`。
- 2026-08-23 继续收敛：角色激活已支持会话级 `SET ROLE NONE/ALL/DEFAULT/<role>` 与 `SET DEFAULT ROLE` 持久化，权限查询会按当前激活角色计算；handler 活动条件已支持 `RESIGNAL` 原 SQLSTATE 重抛和 `MESSAGE_TEXT` 重写；DML `NO_ZERO_DATE/NO_ZERO_IN_DATE` 已接入 INSERT/UPDATE 的零日期转换。角色/handler 变更后的全仓 Go、集群 smoke 与发布门禁分别为 PASS、PASS、GO（发布报告 `2026-08-23T09:09:39.8469153Z`，Connector/J 136 tests、0 failures、0 errors、1 skipped）；零日期改动已有专项回归，需在最终交付前再次刷新全量门禁。
- 认证握手现在会把已配置的 default roles 写入真实连接的 `active_roles` 会话状态，后续权限检查和 `SET ROLE` 使用同一状态；没有 default role 时不再隐式激活全部角色。认证/网络专项回归通过。
- 角色管理再补齐 `REVOKE 'role'@'host' FROM 'user'@'host'`，同时清除该用户的 default role 绑定，并让权限缓存刷新识别 `SET DEFAULT ROLE`；角色激活专项回归通过。
- 本轮继续收敛角色会话可观测性：系统变量引擎新增 `CURRENT_ROLE()`，按连接 `active_roles` 返回逗号分隔的当前角色，无激活角色返回 `NONE`；`INFORMATION_SCHEMA.ENABLED_ROLES` 新增 `ROLE_NAME/ROLE_HOST` 投影，覆盖连接级角色元数据回归。mandatory roles、角色管理权限和完整角色授权矩阵仍保留边界。
- ALTER 兼容再补齐单列 `RENAME COLUMN old TO new` 的原始 SQL 入口、列元数据和索引引用更新，并通过真实查询与 `INFORMATION_SCHEMA.STATISTICS` 回归；在线重建、复杂多表 DDL 和完整算法/锁选项仍是边界。
- Performance Schema 再补齐动态 `setup_consumers` 与 `setup_instruments` 兼容视图，提供 Connector/J/运维工具常用的 instrument 配置列；当前又接入有界真实语句事件 history/current/history_long 视图，完整 P_S consumers/instruments 生命周期、wait/memory 全矩阵仍是边界。
- 2026-08-23 最终回归复验：`go test ./... -count=1` 通过；集群 smoke 报告 `2026-08-23T09:31:45.2809597Z` 为 `PASS`；发布门禁报告 `2026-08-23T09:40:43.2198105Z` 为 `GO`，所有 checks 通过，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 2026-08-23 继续实现：统一表达式路径新增 `GREATEST/LEAST`、`DATE_FORMAT/STR_TO_DATE`、`UNIX_TIMESTAMP/FROM_UNIXTIME`、`CAST/CONVERT`；聚合路径新增 `GROUP_CONCAT(DISTINCT ...)`、标准差/方差和位聚合；游标 `CLOSE` 保留声明以支持再次 `OPEN`；存储过程支持 `DECLARE ... CONDITION FOR SQLSTATE/errno` 及 handler 条件别名。
- 2026-08-23 相关查询继续收敛：相关 `EXISTS/NOT EXISTS` 支持外层谓词与 `ORDER BY`，相关 `IN/NOT IN/ANY/SOME/ALL` 支持与外层谓词组合并保持逐外层行绑定；新增 NULL、排序、量化谓词回归均通过。
- 2026-08-23 最新最终门禁：`go test ./... -count=1`、engine 全包均 PASS；集群报告 `reports/compatibility/p1-cluster-final-current/cluster-report.json` 于 `2026-08-23T10:00:46.5619313Z` 为 PASS；发布报告 `reports/compatibility/release-candidate-final-current/release-candidate.json` 于 `2026-08-23T10:09:59.0651906Z` 为 GO，build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 全部 PASS，Connector/J 为 136 tests、0 failures、0 errors、1 skipped。
- 2026-08-23 函数矩阵复验：新增数值截断/三角函数、`FORMAT`、`LPAD/RPAD/REPEAT/SPACE/ASCII/FIND_IN_SET/BIT_LENGTH/ELT/FIELD/CHAR/QUOTE` 后，`go test ./... -count=1` 仍 PASS；集群报告最新为 `2026-08-23T10:13:40.0086051Z`、PASS，发布门禁最新报告为 `2026-08-23T10:24:16.3481344Z`、GO，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 2026-08-23 最终复验（GROUP_CONCAT/字符集 AST 接入及其回归修复后）：`go test ./... -count=1`、`git diff --check` 均通过；集群报告 `2026-08-23T10:28:40.9113541Z` 为 PASS，发布门禁报告 `2026-08-23T10:38:17.7912785Z` 为 GO，全部 checks PASS，Connector/J 136 tests、0 failures、0 errors、1 skipped。

## Delivery Map

| 阶段 | 时间目标 | 主要结果 | 退出条件 |
|---|---:|---|---|
| P0-A | 先行 | 单机 MySQL 核心可运行 | 核心 CRUD、事务、恢复、权限和基础运维通过 |
| P0-B | 与 P0-A 并行 | Connector/J 业务接入 | JDBC 核心套件、连接池和常见框架路径通过 |
| P1 | P0 完成后 | 1 主多从集群和手动故障转移 | 复制、GTID、重连、重启和切换矩阵通过 |
| P2 | P1 后 | 稳定化和发布门禁 | 性能、并发、备份、监控和一致性证据完整 |
| P3 | 后续 | 自动故障转移和生产级集群增强 | 集群运维和脑裂防护矩阵通过 |
| P4 | 后续 | FULLTEXT、SPATIAL、物理分区和高级兼容 | 高级能力按独立矩阵发布 |

---

## Phase 0: Compatibility Baseline and Test Gate

### Task 0.1: 建立 MySQL 8.4 对照矩阵

**Files:**
- Create: `docs/compatibility/mysql84_compatibility_matrix.md`
- Create: `scripts/compatibility/run_mysql84_matrix.ps1`
- Create: `scripts/compatibility/run_mysql84_matrix.sh`
- Test: `jdbc_client/src/test/java/com/xmysql/server/test/CompatibilityMatrixTest.java`

**Interfaces:**
- Consumes: reference MySQL 8.4 connection、XMySQL JDBC connection、SQL case 文件。
- Produces: 每个 case 的 result rows、column metadata、errno、SQLSTATE、warnings 和执行耗时。

- [x] **Step 1: 定义测试分类和结果格式**

矩阵至少包含：核心 DML、约束、DDL、事务、复杂 SELECT、SHOW/metadata、协议、权限、数据类型、存储对象、运维语句。每个 case 使用固定 JSON 结构记录 `name`、`sql`、`expected_rows`、`expected_columns`、`expected_error`、`expected_warnings`。

- [x] **Step 2: 添加 reference/XMySQL 双连接执行器**

脚本接受明确的 reference 和 target 连接参数，按相同顺序执行 setup、query、cleanup，并将结果写入 `reports/compatibility/`。连接失败必须退出非零，不得生成“通过”报告。

- [x] **Step 3: 写入第一版基线用例**

至少加入以下 SQL：

```sql
CREATE TABLE compat_parent(id INT PRIMARY KEY, code VARCHAR(20) UNIQUE);
CREATE TABLE compat_child(id INT PRIMARY KEY, parent_id INT,
  CONSTRAINT fk_parent FOREIGN KEY(parent_id) REFERENCES compat_parent(id));
INSERT INTO compat_parent VALUES (1, 'p1');
INSERT INTO compat_child VALUES (1, 1);
SELECT id, parent_id FROM compat_child ORDER BY id;
SHOW FULL TABLES;
SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, ORDINAL_POSITION;
```

- [x] **Step 4: 建立回归门禁**

证据：`scripts/compatibility/release_candidate_gate.ps1` 已形成 clean-data/build/unit/integration/crash/concurrency/observability/JDBC 门禁，`reports/compatibility/release-candidate-final8/release-candidate.json` 为 GO。

运行：

```powershell
go test ./server/innodb/sqlparser ./server/innodb/engine ./server/dispatcher ./server/net -count=1
Set-Location jdbc_client
mvn test -Dtest=DMLOperationsTest,PreparedStatementTest,TransactionTest,DDLOperationsTest,SystemVariableTest,IndexAndConstraintTest
```

验收：reference 与 target 的已声明支持范围全部有结果；不支持语句返回结构化错误，不得返回成功或 panic。

### Task 0.2: 固化错误码、SQLSTATE 和警告契约

**Files:**
- Modify: `server/common/errorcode.go`
- Modify: `server/common/error_state.go`
- Modify: `server/protocol/error_helper.go`
- Modify: `server/net/decoupled_handler.go`
- Create: `server/protocol/error_contract_test.go`

**Interfaces:**
- Consumes: executor 返回的结构化错误阶段、MySQL errno 和 SQLSTATE。
- Produces: 统一的错误响应编码和 `SHOW WARNINGS` 可查询结果。

- [x] **Step 1: 为约束、语法、权限、协议和事务错误建立表驱动映射**
- [x] **Step 2: 为每个错误类别添加协议层断言**
- [x] **Step 3: 验证错误后 session 状态、事务状态和 warning 状态保持正确**
- [x] **Step 4: 运行 `go test ./server/common ./server/protocol ./server/net -count=1`**

验收：同一 SQL 在 COM_QUERY、prepared statement 和 JDBC 路径上返回一致的 errno/SQLSTATE 类别；错误不会被包装成泛化的 `HY000`，除非没有更具体的 MySQL 映射。

---

## Phase 1: Core SQL Correctness

### Task 1.1: 完整外键执行矩阵

**Files:**
- Modify: `server/innodb/engine/foreign_key_runtime.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/manager/info_schema_manager.go`
- Create: `server/innodb/engine/foreign_key_compatibility_test.go`

**Interfaces:**
- Consumes: `foreignKeyRuntimeMeta`、table metadata、transaction DML change recorder。
- Produces: 多列 FK 校验、父表更新/删除策略执行、INFORMATION_SCHEMA 约束元数据。

- [x] **Step 1: 先写失败测试覆盖单列和复合外键**

测试必须覆盖：缺失父键的 INSERT/UPDATE、父表删除、父键更新、复合键、事务回滚，以及 `RESTRICT`、`CASCADE`、`SET NULL`、`NO ACTION`。

- [x] **Step 2: 将外键元数据从单列结构扩展为列对列表**

使用 `Columns []string` 和 `RefColumns []string` 同长度校验；任何长度不一致在建表时返回结构化错误。

- [x] **Step 3: 实现父表变更策略**

删除和更新前先读取引用表；`RESTRICT/NO ACTION` 在当前事务中发现引用行时拒绝，`CASCADE` 产生同一事务的 DML change，`SET NULL` 仅允许 nullable 子列。

- [x] **Step 4: 更新约束元数据和错误消息**
- [x] **Step 5: 运行 `go test ./server/innodb/engine -run 'Test.*ForeignKey' -count=1` 及 JDBC IndexAndConstraintTest**

验收：父子表任何支持的变更都保持原子性；复合外键按完整 key 比较；失败后父表、子表和索引状态均不变化。

### Task 1.2: CHECK、默认值和生成列语义

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/expression_evaluator.go`
- Create: `server/innodb/engine/check_constraint_compatibility_test.go`

**Interfaces:**
- Consumes: column metadata、compiled expression、INSERT/UPDATE row values。
- Produces: pre-write constraint validation、default expression计算、generated column计算。

- [x] **Step 1: 写入 NULL、三值逻辑、默认值和 CHECK 失败测试**
- [x] **Step 2: 为 `TableMeta` 增加稳定的 CHECK 定义和 generated-column 元数据**
- [x] **Step 3: 在 row encode 前按列顺序补默认值并计算生成列**
- [x] **Step 4: 对 INSERT、UPDATE、REPLACE、ON DUPLICATE KEY UPDATE 统一执行 CHECK**
- [x] **Step 5: 运行 `go test ./server/innodb/engine -run 'Test.*Check|Test.*Default|Test.*Generated' -count=1`**

验收：CHECK 为 UNKNOWN 的 NULL 语义与 MySQL 8.4 对齐；生成列不可被普通写入覆盖；约束失败不会留下二级索引或 undo 残留。

### Task 1.3: 完整核心 DML 语义

**Files:**
- Modify: `server/innodb/engine/storage_integrated_dml_executor.go`
- Modify: `server/innodb/engine/dml_operators.go`
- Modify: `server/innodb/engine/unified_executor.go`
- Create: `server/innodb/engine/dml_compatibility_test.go`

**Interfaces:**
- Consumes: parsed `Insert`/`Update`/`Delete`、unique index state、transaction context。
- Produces: `REPLACE`、`ON DUPLICATE KEY UPDATE`、multi-row affected-row and generated-key semantics。

- [x] **Step 1: 添加冲突分类测试**

覆盖 primary key、single/composite unique key、多行 VALUES、更新主键、更新多个唯一键、触发级联、失败回滚和 `LAST_INSERT_ID`。

- [x] **Step 2: 统一冲突检测和变更计划**

先构造 row-level change plan，再一次性执行删除/更新/插入和索引变更；禁止在冲突处理中直接绕过事务 recorder。

- [x] **Step 3: 对齐 affected rows、warnings 和 generated keys**
- [x] **Step 4: 运行 `go test ./server/innodb/engine -run 'Test.*DML|Test.*Duplicate|Test.*Replace' -count=1`**
- [x] **Step 5: 运行 JDBC `DMLOperationsTest,PreparedStatementTest,IndexAndConstraintTest`**

验收：同一条 DML 在 COM_QUERY 和 prepared statement 下结果一致；冲突、重复写和回滚不会产生孤儿索引记录。

### Task 1.4: 扩展 ALTER TABLE 和索引 DDL

**Files:**
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/manager/schema_manager.go`
- Modify: `server/innodb/manager/index_manager.go`
- Modify: `server/innodb/engine/table_storage_mapping.go`
- Create: `server/innodb/engine/alter_table_compatibility_test.go`

**Interfaces:**
- Consumes: DDL AST、table metadata、index metadata、storage mapping。
- Produces: add/drop/modify/change/rename column、add/drop/rename index、table rename 的原子 DDL。

- [x] **Step 1: 为每种 ALTER 操作写 metadata、row decode、index rebuild 测试**
- [x] **Step 2: 将 ALTER 操作解析为独立的 `AlterOperation` 列表**
- [x] **Step 3: 先写新 metadata 临时文件和新 index state，再原子替换旧状态**
- [x] **Step 4: 对旧行执行默认值填充、类型转换和 nullable 检查**
- [x] **Step 5: 为失败路径增加恢复旧 metadata/storage mapping 的测试**
- [x] **Step 6: 运行 `go test ./server/innodb/engine ./server/innodb/manager -run 'Test.*Alter|Test.*Index' -count=1`**

验收：所有支持的 ALTER 操作都同时更新字典、存储、索引和 INFORMATION_SCHEMA；中途失败可继续访问旧表。

---

## Phase 2: Metadata, Administration, Protocol and Security

### Task 2.1: 真实 INFORMATION_SCHEMA、SHOW INDEX 和 EXPLAIN

**Files:**
- Modify: `server/innodb/manager/info_schema_manager.go`
- Modify: `server/innodb/manager/info_schema_generators.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/engine/show_executor.go`
- Modify: `server/innodb/engine/select_executor.go`
- Create: `server/innodb/engine/metadata_compatibility_test.go`

**Interfaces:**
- Consumes: schema manager、table/index metadata、live session and lock state。
- Produces: data-backed `TABLES`、`COLUMNS`、`STATISTICS`、`KEY_COLUMN_USAGE`、`TABLE_CONSTRAINTS`、`REFERENTIAL_CONSTRAINTS`、`VIEWS`、`TRIGGERS`、`ROUTINES`、`PARTITIONS`、`PROCESSLIST` and `SHOW INDEX`。

- [x] **Step 1: 为每张系统表定义列 schema、数据源和过滤规则**
- [x] **Step 2: 移除依赖硬编码表行的生产路径，保留 fixture 仅用于单元测试**
- [x] **Step 3: 支持 `SHOW INDEX FROM tbl` 的非唯一、唯一、主键、复合索引和 cardinality**
- [x] **Step 4: 将 `EXPLAIN SELECT` 接入实际 logical/physical plan，并返回 key、rows、type、Extra 等稳定列**
- [x] **Step 5: 增加 JDBC `DatabaseMetaData.getTables/getColumns/getPrimaryKeys/getIndexInfo` 测试**
- [x] **Step 6: 运行 metadata Go 测试和 `mvn test -Pjdbc-connectivity`**

验收：同一 schema 通过 SQL 查询、SHOW 和 JDBC metadata 得到一致定义；删除/ALTER/TRUNCATE 后元数据立即更新。

### Task 2.2: 管理和维护语句

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/sqlparser/sql.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/dispatcher/enhanced_message_handler.go`
- Create: `server/innodb/engine/admin_statement_compatibility_test.go`

**Interfaces:**
- Consumes: parsed administrative statements、privilege checker、table/storage managers。
- Produces: `DESCRIBE/DESC`、`EXPLAIN`、`ANALYZE TABLE`、`CHECK TABLE`、`OPTIMIZE TABLE`、`LOCK TABLES`、`UNLOCK TABLES`、`FLUSH`、`SHOW CREATE` and `SHOW GRANTS`。

- [x] **Step 1: 以 `OtherRead/OtherAdmin` 为边界建立明确 AST 类型**
- [x] **Step 2: 为每条语句补 dispatcher 路由、权限要求和结构化 unsupported 错误**
- [x] **Step 3: 实现 `CHECK TABLE`、`ANALYZE TABLE`、`OPTIMIZE TABLE` 的最小真实存储行为**
- [x] **Step 4: 实现 session 级 LOCK TABLES 状态并在 DML 前检查**
- [x] **Step 5: 对 `LOAD DATA` 和 outfile 能力先返回明确权限/功能错误，不允许静默成功**
- [x] **Step 6: 运行 `go test ./server/dispatcher ./server/innodb/engine ./server/innodb/sqlparser -count=1`**

验收：每条已声明支持的管理语句有权限、执行、结果集和错误测试；未支持语句不会进入错误的 DML 路径。

### Task 2.3: MySQL command protocol completion

**Files:**
- Modify: `server/net/handler.go`
- Modify: `server/net/decoupled_handler.go`
- Modify: `server/protocol/prepared_statement_manager.go`
- Modify: `server/protocol/stmt_execute.go`
- Modify: `server/protocol/mysql_protocol.go`
- Create: `server/net/command_compatibility_test.go`

**Interfaces:**
- Consumes: decoded command packets、session state、prepared statement cursor state。
- Produces: long data、server-side cursor fetch、reset connection、多结果集和 capability flags 的一致实现。

- [x] **Step 1: 为每个 command 建立 support table**

至少区分 `COM_QUERY`、`COM_INIT_DB`、`COM_PING`、`COM_STMT_PREPARE`、`COM_STMT_EXECUTE`、`COM_STMT_CLOSE`、`COM_STMT_RESET`、`COM_STMT_SEND_LONG_DATA`、`COM_STMT_FETCH`、`COM_RESET_CONNECTION`、`COM_BINLOG_DUMP` 和未知命令。

- [x] **Step 2: 实现 `COM_STMT_SEND_LONG_DATA` 的 per-parameter buffer 生命周期**
- [x] **Step 3: 实现 prepared cursor 的 execute/fetch/close 状态机**
- [x] **Step 4: 正确编码 multiple result sets、EOF/OK terminator 和 capability flags**
- [x] **Step 5: 为 reset connection 清理 transaction、prepared statements、user variables 和 session variables**
- [x] **Step 6: 运行 `go test ./server/net ./server/protocol -count=1` 及 JDBC prepared statement 回归**

验收：Connector/J、数据库客户端和简单协议客户端在连接、prepared、reset、cursor 关闭场景下不会死锁或复用旧 session 状态。

### Task 2.4: 账户管理、认证和权限模型

**Files:**
- Modify: `server/auth/auth_service.go`
- Modify: `server/auth/engine_access.go`
- Modify: `server/auth/password_validator.go`
- Modify: `server/dispatcher/enhanced_message_handler.go`
- Modify: `server/innodb/manager/mysql_user_data.go`
- Create: `server/auth/account_management_compatibility_test.go`

**Interfaces:**
- Consumes: `mysql.user`、grant tables、host pattern、authentication plugin、session identity。
- Produces: `CREATE/ALTER/DROP USER`、`GRANT/REVOKE`、roles、password/plugin changes and table/database/column privilege checks。

- [x] **Step 1: 为 user@host、database、table、column、global 五级权限写失败测试**
- [x] **Step 2: 为账户管理语句增加 parser、executor 和 privilege checks**
- [x] **Step 3: 实现 grant table 的事务性更新和 `FLUSH PRIVILEGES` reload**
- [x] **Step 4: 将认证插件能力明确分为 `mysql_native_password`、`caching_sha2_password`、TLS required 三类**
- [x] **Step 5: 增加错误密码、锁定账户、过期密码、host 不匹配和权限不足测试**
- [x] **Step 6: 运行 `go test ./server/auth ./server/dispatcher ./server/net -count=1`**

验收：没有 root/default fallback 绕过；权限判断以存储中的用户和 grant 数据为准；认证失败不泄漏密码或用户存在性细节。

---

## Phase 3: Query Language and Advanced SQL

### Task 3.1: 子查询、HAVING、DISTINCT 和 UNION 完整闭环

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/plan/logical_plan.go`
- Modify: `server/innodb/plan/subquery_optimizer.go`
- Modify: `server/innodb/engine/volcano_executor.go`
- Modify: `server/innodb/engine/select_executor.go`
- Create: `server/innodb/engine/complex_select_compatibility_test.go`

**Interfaces:**
- Consumes: SELECT AST、logical plan、outer-row bindings。
- Produces: scalar/IN/EXISTS/ANY/ALL/correlated subquery、HAVING、DISTINCT、UNION/UNION ALL 的正确执行。

- [x] **Step 1: 为每类查询写 reference differential tests**

证据：`complex_select_compatibility_test.go` 覆盖 DISTINCT、HAVING、IN/NOT IN、EXISTS、UNION/UNION ALL、CTE 和窗口查询的固定 reference result vectors。

- [x] **Step 2: 确保 outer-column binding 和 NULL 三值逻辑在 correlated subquery 中一致**

证据：`TestCorrelatedExistsBindsOuterRowPerEvaluation` 和 `TestCorrelatedExistsUsesUnknownForNullEqualityPerOuterRow` 验证每个 outer row 重新绑定，且 `NULL = NULL` 为 UNKNOWN，不会进入 EXISTS。
- [x] **Step 3: 为 UNION 的列类型合并、ORDER BY、LIMIT 和重复消除建立执行规则**
- [x] **Step 4: 将 HAVING 放在 aggregation 之后、projection 之前的正确阶段**
- [x] **Step 5: 运行 `go test ./server/innodb/plan ./server/innodb/engine -run 'Test.*Subquery|Test.*Union|Test.*Having' -count=1`**

验收：复杂查询结果、NULL 行为、列 metadata 和错误消息与 MySQL 8.4 对齐；相关子查询不会重复使用上一行的绑定值。

### Task 3.2: CTE 和递归 CTE SQL 入口

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/sqlparser/sql.go`
- Modify: `server/innodb/plan/logical_plan.go`
- Modify: `server/innodb/engine/cte_executor.go`
- Modify: `server/innodb/engine/executor.go`
- Create: `server/innodb/engine/cte_compatibility_test.go`

**Interfaces:**
- Consumes: `WITH`/`WITH RECURSIVE` AST、CTE definitions、main query。
- Produces: CTE scope resolution、materialization/inlining、递归终止和 cycle protection。

- [x] **Step 1: 增加 `With`、`CTEDefinition` 和 column list AST，并覆盖格式化/遍历**
- [x] **Step 2: 将 CTE 名称注册到查询 scope，禁止未定义引用和重复名称**
- [x] **Step 3: 接通现有 `CTEOperator`/`RecursiveCTEOperator` 到真实 plan builder**

证据：`BuildLogicalPlanStatement` 已生成 `LogicalCTEStatement/LogicalRecursiveCTE`，物理计划已接入 `VolcanoExecutor`；`cte_plan_execution_test.go` 覆盖非递归物化 `[7]` 和递归执行 `[1,2,3]`，包括 `CTEScanOperator` 的递归批次刷新。
- [x] **Step 4: 实现递归深度、循环检测和 UNION ALL 约束**
- [x] **Step 5: 运行非递归、递归、多 CTE、CTE + JOIN、CTE + DML 测试**

验收：CTE 结果可被主查询和后续 CTE 正确引用；递归查询超限返回明确错误而不阻塞 session。

### Task 3.3: 窗口函数

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/sqlparser/sql.go`
- Modify: `server/innodb/engine/window_function_executor.go`
- Modify: `server/innodb/engine/select_executor.go`
- Modify: `server/innodb/plan/physical_plan.go`
- Create: `server/innodb/engine/window_function_compatibility_test.go`

**Interfaces:**
- Consumes: window specification、partition/order/frame、input rows。
- Produces: `ROW_NUMBER`、`RANK`、`DENSE_RANK`、`NTILE`、`LAG`、`LEAD`、`FIRST_VALUE`、`LAST_VALUE` 及聚合窗口函数。

- [x] **Step 1: 增加 `OVER`、named window 和 frame AST**
- [x] **Step 2: 为 partition/order/frame 做类型和边界校验**
- [x] **Step 3: 将现有窗口执行器接入 projection pipeline**
- [x] **Step 4: 添加空分区、重复排序键、NULL、ROWS/RANGE frame 和 offset 边界测试**
- [x] **Step 5: 运行 `go test ./server/innodb/engine ./server/innodb/sqlparser -run 'Test.*Window' -count=1`**

验收：窗口函数不改变原始行数；窗口排序只影响窗口计算，不错误改变外层 SELECT 的最终顺序。

### Task 3.4: 视图、存储过程、函数、触发器和事件

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/sqlparser/sql.go`
- Modify: `server/innodb/manager/schema_types.go`
- Modify: `server/innodb/manager/info_schema_manager.go`
- Create: `server/innodb/engine/stored_objects.go`
- Create: `server/innodb/engine/stored_objects_compatibility_test.go`

**Interfaces:**
- Consumes: stored object definition、definer/invoker identity、transaction context。
- Produces: `CREATE/ALTER/DROP VIEW`、stored routine invocation、trigger timing/event、event scheduler metadata。

- [x] **Step 1: 先实现 view definition 的持久化、权限和 SELECT 展开**

证据：`TestCreateAndDropViewPersistsDefinition` 覆盖 definition 文件、SHOW CREATE、SELECT 展开和 DROP；`executeViewDDL`/`rewriteViewQuery` 使用持久化定义。
- [x] **Step 2: 为 routine 建立 definition、parameter、local variable 和 return value 数据结构**

证据：`persistedStoredObject` 已持久化 parameters、return type/expression 和 local variables；`TestRoutineMetadataPersistsReturnAndLocalVariableDefinitions` 验证 JSON 结构。
- [x] **Step 3: 增加 trigger execution hook：BEFORE/AFTER INSERT/UPDATE/DELETE**

证据：`trigger_runtime.go` 与 storage-integrated DML 已接入 BEFORE/AFTER INSERT/UPDATE/DELETE 的时序；AFTER 阶段禁止修改 NEW，`TestAfterTriggerRunsAtPostWriteBoundaryAndRejectsNewMutation` 验证失败会进入外层回滚。
- [x] **Step 4: 以显式 scheduler 生命周期实现 EVENT，默认关闭后台执行并提供启动配置**

证据：`EventScheduler` 默认 disabled，只有显式 `SetEnabled(true)` 后 `Start` 才运行，`Stop` 可回收生命周期；`TestEventSchedulerIsDisabledUntilExplicitlyEnabled` 覆盖门禁。
- [x] **Step 5: 运行对象创建、重启加载、权限、递归调用和事务回滚测试**

证据：`stored_object_compatibility_test.go` 覆盖过程参数替换、definer/EXECUTE 权限、递归 CALL、BEFORE trigger 回滚边界和持久化 metadata；递归 routine 的 OUT/INOUT 完整回传、复杂动态 SQL 和完整 AFTER EVENT 仍属于后续增强边界。

验收：对象定义、依赖关系、`INFORMATION_SCHEMA` 元数据和权限检查一致；触发器失败时原始 DML 整体回滚。

### Task 3.5: 数据类型、字符集、排序规则和函数矩阵

**Files:**
- Modify: `server/innodb/metadata/metadata.go`
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/engine/expression_evaluator.go`
- Modify: `server/innodb/engine/value_converter.go`
- Modify: `server/protocol/column_type_mapping.go`
- Create: `server/innodb/engine/type_function_compatibility_test.go`

**Interfaces:**
- Consumes: parsed type/function expressions、session charset/collation/time zone。
- Produces: consistent encode/decode、comparison、cast、result metadata and warning behavior。

- [x] **Step 1: 建立类型矩阵**

覆盖 `DECIMAL` precision/scale、日期时间 fractional seconds、`ENUM/SET`、`BLOB/TEXT`、binary string、unsigned numeric、JSON、spatial type 和 NULL。

- [x] **Step 2: 建立函数矩阵**

覆盖字符串、日期、数值、转换、NULL、聚合、JSON 路径和正则函数；每个函数记录 accepted input、return type、NULL result 和 warning。

- [x] **Step 3: 统一 expression evaluator、row codec、protocol type mapping 的类型转换**

证据：`dml_executor.go`/`record_codec.go` 使用 metadata.DataType 进行值校验和 encode/decode，`protocol/encoder.go` 使用同一类型映射；类型回归与 JDBC result metadata 已通过。
- [x] **Step 4: 对 `utf8mb4`、binary、case-insensitive collation 和排序稳定性添加 reference tests**

证据：`type_function_compatibility_test.go` 覆盖 DECIMAL/DATETIME/ENUM/SET/JSON/VARBINARY metadata 验证，以及 COALESCE、LOWER、ROUND、JSON_EXTRACT；既有表达式测试覆盖 NULL、聚合、字符串和排序行为。
- [x] **Step 5: 运行 parser、engine、protocol 和 JDBC 类型测试**

证据：现有 `executor_dbddl_and_column_type_test.go`、协议编码测试和 JDBC 全量 135 用例均通过；JSON/ENUM/SET/DECIMAL/时间类型已纳入 row codec 与 metadata 基线。

验收：同一列经过 INSERT、WHERE、ORDER BY、GROUP BY、prepared parameter 和 result set 编码后类型不漂移。

---

## Phase 4: Optimizer, Storage, Recovery and Operations

### Task 4.1: 优化器统计、索引选择和 Join 计划

**Files:**
- Modify: `server/innodb/plan/index_pushdown_optimizer.go`
- Modify: `server/innodb/plan/optimizer.go`
- Modify: `server/innodb/plan/subquery_optimizer.go`
- Modify: `server/innodb/manager/optimizer_manager.go`
- Modify: `server/innodb/manager/statistics_manager.go`
- Create: `server/innodb/plan/optimizer_compatibility_test.go`

**Interfaces:**
- Consumes: durable index metadata、row/page counts、column statistics、query predicates。
- Produces: safe predicate pushdown、column pruning、composite prefix matching、Index Merge、Join order and cost explainability。

- [x] **Step 1: 为每种索引访问路径建立 plan assertion**
- [x] **Step 2: 实现统计刷新、NDV、min/max、histogram 和 DDL/DML invalidation**
- [x] **Step 3: 为复合索引实现 equality-prefix、range-prefix 和不合法跳跃列测试**
- [x] **Step 4: 实现 OR 条件的 Index Merge，并验证去重和 NULL 语义**
- [x] **Step 5: 增加 hash join、sort-merge join、nested loop 的可解释选择规则**

证据：`optimizer_compatibility_test.go`、`index_merge_or_compatibility_test.go` 和既有 `index_pushdown_integration_test.go`/`cbo_integrated_test.go` 覆盖索引访问、统计直方图/NDV、复合索引前缀、OR 分支去重/NULL 三值逻辑以及三种 Join 成本选择。OR 计划对未索引分支保守回退，避免错误的部分扫描。
- [x] **Step 6: 运行 plan tests、EXPLAIN 对照矩阵和查询 benchmark**

证据：`go test ./server/innodb/plan -run 'TestOptimizerCompatibility|TestPartitionPruning|TestORIndexMerge|TestSQLThreeValuedLogic' -count=1`、`go test ./server/innodb/plan -run '^$' -bench 'Benchmark(SelectivityEstimation|HyperLogLog)' -benchtime=1x -count=1` 和 `TestDescribeAndExplainUsePersistedMetadata` 均通过。

验收：执行计划不因统计缓存产生错误结果；优化只改变性能，不改变 rows、columns、warnings 和 transaction semantics。

### Task 4.2: 事务、锁、死锁和长事务治理

**Files:**
- Modify: `server/innodb/manager/transaction_manager.go`
- Modify: `server/innodb/manager/lock_manager.go`
- Modify: `server/innodb/manager/gap_lock.go`
- Modify: `server/innodb/manager/mvcc_manager.go`
- Modify: `server/innodb/engine/executor.go`
- Create: `server/innodb/manager/transaction_anomaly_test.go`

**Interfaces:**
- Consumes: transaction/session lifecycle、read views、row/gap/next-key locks。
- Produces: RC/RR visibility、deadlock victim selection、wait graph、lock wait timeout and long transaction diagnostics。

- [x] **Step 1: 写入 dirty read、non-repeatable read、phantom、lost update 和 write skew 场景**
- [x] **Step 2: 为锁等待记录 transaction ID、resource、等待起点和 blocker**
- [x] **Step 3: 实现 wait-for graph 检测和 deterministic victim selection**
- [x] **Step 4: 将锁等待和长事务信息暴露到 PROCESSLIST/PERFORMANCE_SCHEMA 兼容查询**
- [x] **Step 5: 运行并发测试至少 100 次，保存汇总报告**

证据：`reports/compatibility/concurrency-final/concurrency_validation_20260822_111119.json` 为 6 个场景各 100/100 PASS，`status=PASS`；同时保留明确的 consistency gap 字段，未将测试选择器等同于完整串行化证明。

证据：`transaction_anomaly_test.go` 覆盖 RU/RC/RR 可见性、幻读边界、丢失更新和串行化写偏差；`lock_wait_observability_compatibility_test.go` 验证 `SHOW PROCESSLIST` 与 `performance_schema.threads` 的 `Waiting for lock` 实时状态。

验收：死锁不会永久等待；回滚只撤销 victim 事务；active read view 存在时 purge 不回收可见版本。

### Task 4.3: 外部崩溃恢复、备份和恢复

**Files:**
- Modify: `server/innodb/manager/crash_recovery.go`
- Modify: `server/innodb/manager/redo_log_manager.go`
- Modify: `server/innodb/manager/undo_rollback.go`
- Modify: `server/innodb/engine/storage_integrated_checkpoint.go`
- Create: `scripts/compatibility/crash_recovery_matrix.ps1`
- Create: `scripts/compatibility/crash_recovery_matrix.sh`
- Create: `docs/operations/mysql84-backup-restore-runbook.md`

**Interfaces:**
- Consumes: running server process、redo/undo/WAL、checkpoint、database directory snapshot。
- Produces: kill/restart/replay workflow、state diff artifact、logical backup restore and point-in-time recovery evidence。

- [x] **Step 1: 定义 committed、uncommitted、prepared、DDL、index rebuild 五类 crash case**
- [x] **Step 2: 在脚本中启动服务、执行 workload、强制终止、重启恢复并校验 rows/indexes/metadata**
- [x] **Step 3: 为恢复结果生成 JSON state diff 和 Markdown 报告**
- [x] **Step 4: 实现 logical backup export/import，并验证跨目录恢复**
- [x] **Step 5: 在具备 binlog 后增加 point-in-time restore case**
- [x] **Step 6: 运行至少 3 次重复 drill，任何一次数据差异都使任务失败**

证据：`scripts/crash_recovery_drill.ps1` 生成 JSON/Markdown/log/state artifact；`scripts/compatibility/external_crash_recovery_matrix.ps1` 生成真实进程 kill/restart 报告，覆盖 prepared committed、uncommitted、DDL 和 index rebuild 五类 case；`reports/compatibility/external-crash/external-crash-20260822-124623.json` 为 3/3 PASS；`server/backup/logical_backup_test.go` 验证跨目录 logical backup 和 binlog position PITR。

验收：恢复后已提交数据全部存在，未提交数据全部撤销，索引和 metadata 可重新查询；报告包含版本、配置、LSN、事务和校验摘要。

### Task 4.4: Performance Schema、慢查询和 live metrics

**Files:**
- Modify: `server/observability/metrics/runtime.go`
- Modify: `server/observability/metrics/defaults.go`
- Modify: `server/observability/metrics/http.go`
- Modify: `server/net/decoupled_handler.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/manager/operational_metrics.go`
- Create: `docs/observability/mysql84-observability-matrix.md`
- Create: `scripts/compatibility/observability_smoke.ps1`

**Interfaces:**
- Consumes: query/session/transaction/lock/storage events。
- Produces: live counters/histograms、slow query records、PROCESSLIST/PERFORMANCE_SCHEMA rows、Prometheus endpoint and evidence report。

- [x] **Step 1: 给 query parse、execute、error、lock wait、commit、rollback、page IO 添加 runtime instrumentation**
- [x] **Step 2: 将 live recorder 接入真实 handler/executor 路径**

证据：`RuntimeRecorder` 已提供 query/error/transaction/lock/recovery/checkpoint 指标，`executor.go` 和 `enginx.go` 已接入真实 query/transaction/error 路径；现有 Performance Schema 测试读取 live recorder，而非 fixture。
- [x] **Step 3: 实现慢查询阈值、采样字段、脱敏 SQL 和滚动日志**
- [x] **Step 4: 将关键指标映射到 PERFORMANCE_SCHEMA 兼容视图**

证据：`SlowQueryLogger` 实现阈值、结构化字段和追加式滚动日志；`executePerformanceSchemaStatementsSelect`、`executePerformanceSchemaThreadsSelect` 和 `information_schema.processlist` 使用 live runtime 数据，已有 `performance_schema_compatibility_test.go` 回归。
- [x] **Step 5: 运行 smoke workload 并验证 metrics 数值随流量变化**

验收：不能只返回静态 0 或固定样例；请求数、错误数、延迟、连接数、事务数和 lock wait 必须随真实 workload 改变。

### Task 4.5: Release candidate and rollback gate

**Files:**
- Modify: `docs/planning/P0_PRODUCTION_CHECKLIST.md`
- Modify: `docs/planning/P0_EVIDENCE_BUNDLE.md`
- Modify: `scripts/p0_a2_01_stability.sh`
- Modify: `scripts/p0_b_recovery_audit.ps1`
- Create: `scripts/compatibility/release_candidate_gate.ps1`
- Create: `docs/operations/mysql84-release-gate.md`

**Interfaces:**
- Consumes: Go/JDBC/compatibility/recovery/concurrency/metrics reports。
- Produces: one release candidate decision: `GO` or `NO-GO` with failed cases and owners。

- [x] **Step 1: 固定 clean-data、build、unit、integration、recovery、concurrency、metrics 的执行顺序**
- [x] **Step 2: 检查报告时间戳、git revision、配置摘要和测试数量**
- [x] **Step 3: 将 stale artifact、panic、数据 diff、live metrics 缺失和 rollback drill 缺失定义为 NO-GO**
- [x] **Step 4: 运行一次完整 candidate gate 并归档报告**

验收：没有人工编辑结果即可得出发布判断；任何必需证据缺失时默认 NO-GO。

---

## Phase 5: Advanced Features and Replication

### Task 5.1: Partitioned tables

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/sqlparser/sql.go`
- Modify: `server/innodb/manager/schema_manager.go`
- Modify: `server/innodb/manager/table_storage_mapping.go`
- Modify: `server/innodb/plan/optimizer.go`
- Create: `server/innodb/engine/partition_compatibility_test.go`

**Interfaces:**
- Consumes: partition definition、partition expression、row key、optimizer predicate。
- Produces: RANGE/LIST/HASH partition routing、partition pruning、partition metadata and maintenance operations。

- [x] **Step 1: 先实现 RANGE COLUMNS 的 create/insert/select/drop partition 矩阵**
- [x] **Step 2: 将 partition descriptor 持久化并纳入 table storage mapping**
- [x] **Step 3: 在 INSERT/UPDATE 时校验唯一分区归属和 out-of-range 行为**
- [x] **Step 4: 在 plan 中根据常量和范围 predicate 做 pruning**
- [x] **Step 5: 再扩展 LIST/HASH，并运行跨重启测试**

证据：`TestListAndHashPartitionRoutingIsDeterministic` 覆盖负数 HASH 和 LIST 值选择；分区描述持久化到 `.frm`/TableStorageInfo，重启恢复由 metadata reload 路径覆盖。

验收：分区裁剪只减少扫描范围，不改变结果；ALTER/TRUNCATE/DROP 后分区 metadata 和 storage mapping 一致。

### Task 5.2: FULLTEXT and spatial indexes

**Files:**
- Modify: `server/innodb/sqlparser/ast.go`
- Modify: `server/innodb/engine/executor.go`
- Modify: `server/innodb/manager/index_manager.go`
- Create: `server/innodb/engine/fulltext_executor.go`
- Create: `server/innodb/engine/spatial_executor.go`
- Create: `server/innodb/engine/fulltext_spatial_compatibility_test.go`

**Interfaces:**
- Consumes: tokenized text、MATCH AGAINST expression、geometry values、spatial predicates。
- Produces: FULLTEXT index build/update/query/ranking and minimum R-Tree spatial search。

- [x] **Step 1: 定义 tokenizer、stopword、token normalization 和 index segment 格式**
- [x] **Step 2: 实现 MATCH AGAINST natural-language 和 boolean mode 的最小矩阵**
- [x] **Step 3: 定义 geometry encoding、SRID 校验和 bounding-box 查询**

证据：`fulltext_spatial_compatibility.go` 已实现 MATCH AGAINST 两种 mode 的最小 tokenizer/过滤路径，以及 `ST_GEOMFROMTEXT` + `ST_WITHIN/MBRCONTAINS` 的 WKT bounding-box 处理；`fulltext_spatial_compatibility_test.go` 覆盖真实查询结果。
- [x] **Step 4: 实现 index rebuild、drop、restart recovery**
- [x] **Step 5: 对排名、NULL、空文本、无效 geometry 和重复索引建立 reference tests**

证据：`fulltext_spatial_compatibility.go` 持久化 tokenizer/stopword/segment metadata，`fulltext_spatial_compatibility_test.go` 覆盖 segment rebuild、drop、JSON reload、重复 token ranking、NULL/空文本、无效 geometry 和重复高级索引。

验收：全文和空间能力单独标记为高级特性，不得影响普通 B+Tree DML 和恢复路径。

### Task 5.3: Stored object execution hardening

**Files:**
- Modify: `server/innodb/engine/stored_objects.go`
- Modify: `server/innodb/manager/schema_types.go`
- Modify: `server/auth/auth_service.go`
- Modify: `server/innodb/manager/info_schema_generators.go`
- Create: `server/innodb/engine/stored_object_security_test.go`

**Interfaces:**
- Consumes: definer/invoker identity、routine/trigger/event dependency graph。
- Produces: security-context switching、dependency invalidation、drop/alter protection and metadata consistency。

- [x] **Step 1: 验证 definer 存在性和 invoker privilege**
- [x] **Step 2: 禁止删除仍被对象引用的表/列，或按 MySQL 规则返回明确错误**
- [x] **Step 3: 为对象 ALTER/DROP、重启加载和失败恢复增加测试**

证据：`findStoredObjectTableDependency` 接入 DROP TABLE，`TestDropTableProtectsStoredObjectDependencies` 验证引用视图返回明确错误；既有 stored object/v​iew drop 与 definer/权限测试覆盖持久化及失败路径。
- [x] **Step 4: 运行完整 stored object compatibility suite**

证据：`go test ./server/innodb/engine -run 'Test.*(Stored|View|Trigger|Routine|Event)' -count=1` 通过。

验收：对象执行不会绕过权限；对象失败会回滚同一事务中的数据变更。

### Task 5.4: Binary log, GTID and replication

**Files:**
- Create: `server/replication/binlog_writer.go`
- Create: `server/replication/binlog_events.go`
- Create: `server/replication/gtid.go`
- Create: `server/replication/source.go`
- Create: `server/replication/replica.go`
- Modify: `server/net/decoupled_handler.go`
- Modify: `server/innodb/manager/transaction_manager.go`
- Create: `server/replication/replication_compatibility_test.go`

**Interfaces:**
- Consumes: committed transaction change set、server UUID、binlog position、GTID set。
- Produces: transactional binlog、`COM_BINLOG_DUMP`/GTID dump、source/replica apply loop and replication metadata。

- [x] **Step 1: 定义 transaction commit 与 binlog append 的顺序和 crash recovery contract**
- [x] **Step 2: 实现 row-based event format、rotate、position 和 checksum**
- [x] **Step 3: 实现 GTID set parse/merge/containment and persistence**
- [x] **Step 4: 接入 COM_BINLOG_DUMP 和 replica apply transaction boundary**
- [x] **Step 5: 实现 replica restart、duplicate GTID、network retry 和 lag metadata**
- [x] **Step 6: 运行 source/replica 双实例测试和 crash/replay 测试**

证据：`server/replication/replication_compatibility_test.go` 覆盖 source/replica、partial transaction、restart、duplicate GTID、rotate/position/checksum、retry/lag；`server/net/binlog_compatibility_test.go` 覆盖 `COM_BINLOG_DUMP` packet stream。

验收：已提交事务按顺序且恰好一次应用；重复 GTID 不重复写入；复制状态可查询并能在重启后恢复。

---

## Cross-Phase Test Matrix

每个阶段都必须保留以下四层测试：

| 层级 | 内容 | 最低门禁 |
|---|---|---|
| Parser | AST、格式化、非法语法、关键字冲突 | `go test ./server/innodb/sqlparser -count=1` |
| Engine | executor/operator、NULL、事务、错误契约 | `go test ./server/innodb/engine -count=1` |
| Storage | page/record/index、重启、恢复、并发 | `go test ./server/innodb/manager ./server/innodb/storage/... -count=1` |
| Protocol/JDBC | packet、metadata、prepared、session lifecycle | `go test ./server/net ./server/protocol -count=1`；`cd jdbc_client && mvn test` |

新增 SQL 能力必须至少提供：

1. 成功路径；
2. 语法错误路径；
3. 权限错误路径；
4. NULL/空结果路径；
5. 事务回滚路径；
6. 重启后读取路径；
7. 与官方 MySQL 8.4 的结果或错误对照。

## Dependency Rules

```text
Phase 0 baseline
  -> Phase 1 constraints/DML/DDL
  -> Phase 2 metadata/protocol/security
  -> Phase 3 complex SQL and stored objects
  -> Phase 4 optimizer/recovery/operations
  -> Phase 5 partition/fulltext/spatial/replication
```

- 外键、CHECK 和复杂 DML 必须先于触发器和存储过程。
- metadata 和权限必须先于完整 stored object。
- 事务 commit/change-set 语义必须先于 binlog/replication。
- recovery 和 observability 必须先于生产 release gate。
- FULLTEXT、空间和分区不得修改普通 B+Tree 的默认访问路径，除非有独立 plan assertion。

## Release Criteria

### Core Compatibility Release

- P0-A/P0-B 全部完成；
- 现有 77 个 JDBC 聚焦测试保持通过；
- 核心约束、复杂 DML、核心 DDL、metadata、prepared protocol、Connector/J 和权限矩阵通过；
- `go test ./...` 或明确记录的包级例外全部有原因和 owner；
- 至少 3 次 crash/restart/recovery drill 通过；
- concurrency、metrics、rollback 和 clean-data candidate gate 通过。

### Advanced Compatibility Release

- Core Compatibility Release 和 P1 集群退出条件已通过；
- P3/P4 的复杂查询、stored object、物理分区、FULLTEXT、SPATIAL 和全量客户端能力按独立矩阵验收；
- 高级特性按独立能力发布，不把未完成能力宣称为 MySQL 兼容。

## Definition of Done

一个任务只有同时满足以下条件才可标记完成：

- parser/dispatcher 路径存在并能正确拒绝不支持变体；
- executor/storage 或 protocol 行为已接入真实运行路径；
- metadata、权限、事务和错误契约已定义；
- 单元、集成、JDBC 或对照测试通过；
- 重启、回滚或并发场景按任务性质完成；
- 计划中的文档、运行命令和 evidence report 已更新；
- 没有遗留 panic、静默成功、固定样例数据或 stale report 依赖。

### 2026-08-23 Continued Compatibility Work

- 统一表达式与存储层行表达式均接入 `DATE_ADD/ADDDATE/DATE_SUB/SUBDATE`、`DATEDIFF`、`TIMESTAMPDIFF`，覆盖 YEAR/QUARTER/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND/MICROSECOND 常用单位。
- `GROUP_CONCAT` 现在真实执行 `DISTINCT`、`ORDER BY` 和 `SEPARATOR`；Performance Schema 新增 `global_variables/session_variables`、`global_status/session_status` 与 `setup_actors` 常用观察视图。
- 存储过程嵌套 block 的 `EXIT HANDLER` 按声明作用域退出，不再错误结束整个 routine；真实过程回归和全仓 `go test ./... -count=1` 已通过。
- 本轮门禁复验：`reports/compatibility/p1-cluster-final-current/cluster-report.json` 于 `2026-08-23T10:49:38.6029453Z` 为 PASS；`reports/compatibility/release-candidate-final-current/release-candidate.json` 于 `2026-08-23T10:59:32.2474772Z` 为 GO，build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 和 Connector/J 全套 136 tests 均 PASS（0 failures、0 errors、1 skipped）。
- 继续扩展 ALTER 语义：原始 SQL 路径新增 `ALTER TABLE ... ADD/DROP PRIMARY KEY` 和 `ALTER COLUMN ... SET/DROP DEFAULT`，同步列/索引 metadata，并通过重复主键错误、`INFORMATION_SCHEMA.STATISTICS`、默认值插入和删除默认值后的 NULL 行回归。
- 同一路径新增 `ALTER TABLE ... ADD/DROP FOREIGN KEY`，持久化引用关系和隐式索引，并验证无效子行拒绝、`ON DELETE CASCADE` 以及删除约束后的放行行为；相关 engine/manager/plan/net/protocol/replication 回归全部通过。
- 约束 DDL 再补齐 `ALTER TABLE ... ADD/DROP CONSTRAINT ... CHECK`，持久化命名检查表达式并接入现有 INSERT 校验；非法值拒绝和删除约束后放行回归通过。
- `ANALYZE TABLE` 现在会对有数据的表执行真实扫描，生成列 distinct/null/min/max/平均长度与索引基数统计，持久化到 `.frm`，并供 `EXPLAIN`、`SHOW INDEX`、`INFORMATION_SCHEMA.STATISTICS` 使用；空表无需数据根页即可返回零统计，`TestAnalyzeTablePersistsOptimizerStatistics` 已覆盖。
- `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE (...)` 已接入持久化索引路径和唯一约束运行时校验，`TestAlterTableAddConstraintUniqueUpdatesIndexMetadata` 已覆盖元数据及重复值拒绝。
- 追加主键现在会扫描已有数据并拒绝 NULL 或重复键值，避免仅修改 `.frm` 后留下非法主键状态；`TestAlterTableAddPrimaryKeyRejectsExistingDuplicateOrNullValues` 已覆盖两类失败路径。
- 本轮最终门禁：`reports/compatibility/release-candidate-final-turn/release-candidate.json` 于 `2026-08-23T11:45:17.5837312Z` 为 `GO`，Connector/J 136 tests 为 0 failures、0 errors、1 skipped，build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 PASS；`reports/compatibility/p1-cluster-final-turn/cluster-report.json` 于 `2026-08-23T11:45:27.8326084Z` 为 PASS。
- 继续收敛 P1 优化器：表达式规范化新增安全吸收律 `x AND (x OR y)`/`x OR (x AND y)`，旧版 `OptimizerManager` 按 join graph 生成确定性连接顺序；引擎 `OptimizeLogicalPlan` 已实际委托统一优化流水线。对应 `TestAbsorptionLaw`、`TestOptimizerJoinOrderFollowsJoinGraph` 和 `TestEngineOptimizeLogicalPlanUsesPlanOptimizationPipeline` 通过。
- 继续收敛 P1 执行器：并行表扫描增加 workers/chunkSize 下限、context 取消、错误传播和按 chunk 顺序汇并；`PhysicalMergeJoin` 对等值条件接入真实排序合并算子，非等值条件保留 Nested Loop fallback。`TestParallelTableScanAssemblesChunksInOrder`、`TestParallelExecutorHonorsCancellationAndNormalizesConfig`、`TestSortMergeJoinHandlesUnsortedInputs` 和 LEFT OUTER 回归通过。
- 继续收敛 P1 统计生命周期：Enhanced statistics collector 在未接入存储访问器时安全降级并缓存，首次列/索引统计不再递归持锁死锁，新增表级 `Invalidate` 同时清理关联列/索引缓存；`TestEnhancedStatisticsCollectorFallsBackAndInvalidatesSafely` 通过。该路径仍不宣称具备真实列值全表扫描、完整直方图刷新和 undo 驱动 ModifyCount。
- 索引下推再补齐限定列引用：`table.column` 会按裸列名匹配单列/复合索引，并复用裸列统计的 NDV、直方图选择率；`TestQualifiedColumnUsesCompositeIndexPrefix` 覆盖索引前缀与选择率。
- 本轮最新门禁复验：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-final-next/cluster-report.json` 于 `2026-08-23T12:07:05.458Z` 为 `PASS`；`reports/compatibility/release-candidate-final-next/release-candidate.json` 于 `2026-08-23T12:18:51.885Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 PASS，隔离 Connector/J 为 136 tests、0 failures、0 errors、1 skipped。
- 本轮继续收敛 P1-OPT-003：INSERT/UPDATE/DELETE 成功后清理表级持久化统计，后续 `SHOW INDEX` 与 `INFORMATION_SCHEMA.STATISTICS` 重新计算当前基数；补充 `index_name = ...` 精确过滤和信息模式数字列类型推断，`TestDMLInvalidatesPersistedIndexCardinality` 通过。
- 本轮继续收敛 P1-OPT-004：复合二级索引支持连续前导等值谓词的范围扫描，例如 `(tenant_id,email)` 可使用 `tenant_id = ... AND email = ...` 的完整前缀，剩余条件仍回表过滤；新增 `SecondaryIndexPrefixEqualityRange`、`TestP1SelectUsesCompositeSecondaryIndexPrefix`，相关索引回归及 engine 全量测试通过。
- 本轮继续收敛 P1-OPT-005：计划层新增带最大项数保护的 DNF 转换器和 `ExpressionNormalizer.NormalizeToDNF`，覆盖 AND/OR 分配与超限保守回退，避免布尔表达式展开无界膨胀；新增 DNF 三组回归测试通过。
- 本轮继续收敛 P1-IDX-001：SelectExecutor 对简单等值 `OR` 支持多二级索引范围扫描、主键去重、回表和残余谓词过滤，复杂/范围 OR 保守退回表扫描；新增 `TestP1SelectMergesSimpleOrSecondaryIndexes`，engine/plan/manager 回归通过。
- 本轮继续收敛 P1-IDX-001：二级索引合并扩展到简单单列范围 `OR` 分支，仍通过主键去重和回表后的原始谓词过滤保证语义；由于当前持久化索引键不是类型可排序编码，范围分支先扫描该索引命名空间，复杂/多列 OR 仍保留边界；新增 `TestP1SelectMergesRangeOrSecondaryIndexesWithResidualFiltering`。
- 本轮继续收敛 P1-SQL-003/P1-IDX-002：ALTER 新增的二级索引在首次访问时使用当前 `.frm` 元数据重建已有行，删除后的索引从活动访问路径移除；新增 `RebuildIndexWithTableMetadata` 和 `TestP1AlterAddIndexBuildsEntriesForExistingRows`，覆盖加索引查存量数据及删索引回退表扫描。
- 本轮修正索引重建时机：DML 路径只注册并增量同步索引，读路径仅对 ALTER/重启后尚未注册的索引执行存量重建，避免重复二级索引项和破坏坏索引错误契约；相关 P0 回归、engine 全量和全仓回归均通过。
- 本轮继续收敛 P1-TXN-001：等待边新增锁类型/模式/等待时长，LockManager 保存最近一次死锁的 cycle、victim 和最大等待时长，`SHOW ENGINE INNODB STATUS` 输出结构化等待和最近死锁摘要；新增管理器诊断回归，现有 processlist/Performance Schema 锁等待回归通过。
- 本轮继续收敛 P1-OPT-006/P1-EXE-002：逻辑优化器改为七阶段固定流水线，新增规则顺序 trace、重复/空规则冲突拒绝；VolcanoExecutor 新增 `ExecuteBatches`，按 callback 批量输出并保持 Open/Next/Close 生命周期，新增边界回归。
- 本轮继续收敛 P1-SQL-002：修复混合 `UNION ALL`/`UNION DISTINCT` 的逐操作符去重语义，新增混合集合运算回归；原有外层 ORDER BY/LIMIT 与 UNION ALL 回归保持通过。
- 本轮继续收敛 P1-OPT-004/P1-SQL-002：复合二级索引支持“前导等值 + 后续单范围”前缀扫描并保留残余谓词过滤；统一执行入口新增 `INTERSECT`/`EXCEPT` 核心 DISTINCT/ALL 集合语义，按 MySQL 规则处理无括号混合集合运算的 `INTERSECT` 优先级及外层 ORDER BY/LIMIT，并可解析简单括号包裹的 SELECT 分支；嵌套括号 set-expression 仍保留边界。
- 本轮同步校准 P1-OPT-001/P1-OPT-002/P1-SQL-001 状态：谓词下推、列裁剪和标量/IN/量化/相关子查询的已有可验证矩阵已写入 backlog；未覆盖的通用递归/派生/复杂相关 AST 路径继续保持明确边界。
- 本轮继续收敛 P1-SQL-001：派生表路径扩展为单源嵌套执行，支持外层投影、WHERE、ORDER BY、DISTINCT、LIMIT/OFFSET，以及常见外层 `COUNT/SUM/AVG/MIN/MAX` 与单层 `GROUP BY/HAVING`；接入 `executeQuery` 与主引擎入口；多源派生表和通用相关 AST 仍保留边界。
- 本轮继续收敛 P1-TXN-002/P1-OPS-001/P1-OPS-002：长事务查询改为排序的不可变诊断快照并支持监控停止后重启，逻辑备份/PITR 证据已存在；新增稳定性能基准脚本并生成 JSON 报告。
- 本轮继续收敛 P1-SEC-001：持久化账号和系统表认证回退统一按精确 host、具体通配模式、`%` 的优先级选择，避免账号文件顺序导致宽泛账号抢占；新增 host specificity 回归。
- 本轮继续收敛 P1-OPT-003/P1-EXE-003：增强统计收集器记录失效次数并在刷新时形成 ModifyCount，扩展并行 hash join/aggregate/sort 的取消检查与分区序稳定归并，相关 plan 回归通过。
- 本轮继续收敛 P1-EXE-004：ProjectionOperator 的表达式热路径复用行上下文 map，并对缺失列显式清空，避免跨行残留值；新增回归覆盖 NULL/缺失列语义。
- 本轮继续收敛 P1-SQL-003：ALTER TABLE ADD/DROP/RENAME INDEX 成功后立即刷新元数据并重建新增/改名索引，避免“ALTER 后先 DML、后 SELECT”产生存量索引缺口；新增时序回归通过。
- 本轮继续收敛 P1-SQL-003：补齐 `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT/DROP DEFAULT` 的原始 SQL 入口和持久化 `.frm` 元数据更新，已有 ALTER 默认值回归通过；在线重建、复杂算法/锁选项仍保留边界。
- 本轮继续收敛 P1-SEC-001：认证边界执行账号 `REQUIRE SSL/TLS` 与 `REQUIRE X509` 客户端证书要求；`caching_sha2_password` 支持 TLS cleartext full-auth 和非 TLS 会话的临时 RSA 公钥请求/加密密码解密，新增 RSA 回归；完整 privilege-option 语义仍保留边界。
- 本轮继续收敛 P1-OPT-003：信息模式和表管理器统计缓存新增 DML `ModifyCount` 生命周期，失效后计数跨缓存边界传递到下一次刷新，`ANALYZE` 更新后清理旧计数；`TestInfoSchemaStatsModifyCountSurvivesInvalidationUntilRefresh` 通过；物理统计文件持久化和真实采样仍保留边界。
- 本轮继续收敛存储对象安全边界：普通 SELECT 与单源派生表的存储函数投影、WHERE 和嵌套算术统一复用 DEFINER/EXECUTE 校验，并严格按当前会话 active role 或账户 default role 继承 EXECUTE；完整 privilege-option、角色管理和动态权限矩阵仍保留边界。
- 本轮继续收敛元数据兼容：`INFORMATION_SCHEMA.PARAMETERS` 现在从持久化 procedure/function 定义返回参数模式、JDBC 类型和函数返回参数，`INFORMATION_SCHEMA.COLLATIONS` 提供常用 utf8mb4/latin1 基础行；其余 I_S/P_S 表的完整生命周期和全量字符集/collation 矩阵仍保留边界。
- 本轮继续收敛元数据兼容：`INFORMATION_SCHEMA.ROUTINES` 对原生 `ROUTINE_SCHEMA/ROUTINE_TYPE/DATA_TYPE` 查询返回持久化 routine 定义，同时保留 Connector/J 的 JDBC 别名投影路径；全量 I_S/P_S 生命周期仍未宣称完成。
- 本轮继续收敛存储对象元数据：`mysql.procs_priv` 现在从持久化 routine grants 返回 Host/User/Db/Routine/Proc_priv，覆盖 routine EXECUTE 授权的管理工具查询；完整 mysql 系统权限表和动态生命周期仍保留边界。
- 本轮继续收敛只读系统元数据：补充 `INFORMATION_SCHEMA.CHARACTER_SETS` 与 `INFORMATION_SCHEMA.ENGINES` 的常用基础行和列投影/字符集过滤；完整系统表生命周期与全量字符集矩阵仍保留边界。
- 本轮继续收敛约束与权限元数据：补充 `INFORMATION_SCHEMA.CHECK_CONSTRAINTS`，从持久化 `.frm` 读取命名检查约束、表达式、表和 `ENFORCED` 状态，并增加 `mysql.db`、`mysql.tables_priv`、`mysql.columns_priv` 对持久化数据库/表/列授权的查询投影；完整 MySQL 系统表生命周期和授权选项语义仍保留边界。
- 本轮继续收敛递归 CTE：主查询新增投影、过滤、排序、去重、LIMIT/OFFSET、常见聚合以及 `INSERT ... SELECT` 物化路径；多定义递归 CTE、通用递归 AST、递归 UPDATE/DELETE 和复杂集合递归仍保留边界。
- 本轮验证证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-final-continuation7/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-final-continuation7/release-candidate.json` 于 `2026-08-23T16:45:20.0788887Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 本轮继续收敛账号系统表：`mysql.user` 现在反映持久化账号的认证插件、认证串、锁定/过期状态和全局权限，并修正 `IDENTIFIED WITH ... BY` 密码持久化；完整 mysql.user 字段、动态授权选项和系统表生命周期仍保留边界。
- 本轮最终验证证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-final-continuation8/cluster-report.json` 于 `2026-08-23T16:48:34.0914527Z` 为 `PASS`；`reports/compatibility/release-candidate-final-continuation8/release-candidate.json` 于 `2026-08-23T16:58:36.1227988Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 最新验证证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-final-continuation6/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-final-continuation6/release-candidate.json` 为 `GO`，其中 Connector/J 136 项测试 0 失败、0 错误、1 跳过。工作仍继续推进，不能据此宣称 MySQL 8.4 全量兼容。
- 本轮最终门禁：`reports/compatibility/p1-cluster-final-continued/cluster-report.json` 于 `2026-08-23T13:38:44.8056702Z` 为 `PASS`；`reports/compatibility/release-candidate-final-continued/release-candidate.json` 于 `2026-08-23T13:48:35.5115481Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、1 skipped；其后 `go test ./... -count=1` 与 `git diff --check` 通过。
- 边界审计结论：当前范围内可验证的 P0 单机核心、P0 Connector/J、P1 基础集群和本轮 P1 统计/索引/表达式增强均有真实路径与回归证据；剩余不是“已完成但未记录”，而是完整 ALTER AST/在线重建、完整 I_S/P_S、通用递归/相关子查询 AST 执行、完整窗口 frame、存储对象完整执行、全类型/函数/collation 矩阵、统计驱动全成本模型、物理分区表空间/pruning、真正 FULLTEXT/SPATIAL 索引、MySQL 原生 binlog/GTID 全协议与复杂复制、以及非 Connector/J 全量客户端兼容。这些继续按计划后置，其中 FULLTEXT 和全量客户端遵循用户明确要求不纳入当前门禁。
- 本轮继续收敛 SQL/安全能力：普通多定义 CTE 支持多个 `FROM/JOIN` 来源、链式引用、声明列名和 `INSERT ... SELECT` 物化，并复用多源派生表 JOIN；派生表 JOIN 已覆盖 INNER、LEFT、RIGHT 路径及 `USING` 双侧列比较。账号授权补齐 `WITH GRANT OPTION`、`REVOKE GRANT OPTION FOR`、`REVOKE ALL PRIVILEGES`、多角色 GRANT/REVOKE、角色 `WITH ADMIN OPTION`、`ALTER USER ... PASSWORD EXPIRE/NEVER`，并同步 `SHOW GRANTS`、`mysql.tables_priv` 和 `mysql.user` 回归。通用递归 CTE、完整授权转授链和更复杂相关 AST 仍未宣称完成。
- 最新继续实现 P1 安全与窗口能力：普通会话执行 `CREATE/DROP USER`、`CREATE/DROP ROLE` 和修改他人账号前必须具备相应全局管理权限；账号可修改自己的密码；窗口执行器新增 `CUME_DIST`、`PERCENT_RANK`、`NTH_VALUE`，并覆盖 `ROWS/RANGE` frame 的默认行为。对应授权与窗口专项回归通过。
- Connector/J 批量执行审计结论：标准 MySQL wire protocol 的 `COM_STMT_EXECUTE` 没有批次边界或批量载荷，Connector/J 默认 `executeBatch()` 会发送多次执行包；因此不能在服务端无条件合并请求而改变事务/错误语义。当前批量 DML 的功能门禁通过，但吞吐优化应落在 prepared-DML 快速路径或 Connector/J 的 `rewriteBatchedStatements` 客户端策略，不能宣称已有 MySQL 原生 Batch Execute 协议。
- 本轮新增账号兼容：支持 `SET PASSWORD [FOR user] = '...'` 的自助/管理员权限边界，支持 `RENAME USER` 持久化改名并同步角色、默认角色和 ADMIN OPTION 引用；专项回归及随后 `go test ./... -count=1` 全仓回归通过。该变更之后需要重新执行完整 release candidate 才能生成最新 Connector/J 证据。
- 最新 continuation9 门禁：`go test ./... -count=1`、engine 全量、cluster smoke 均 PASS；`reports/compatibility/release-candidate-current-continuation9/release-candidate.json` 于 `2026-08-23T17:37:40.3914287Z` 为 `GO`，隔离 Connector/J 136 tests 为 0 failures、0 errors、1 skipped，3 次 crash-recovery、100 次并发和 observability 全部 PASS。Connector/J 性能批量 DML 耗时约 9 分钟，功能通过但吞吐仍是后续优化项。
- 本轮门禁证据：`reports/compatibility/p1-cluster-continued/cluster-report.json` 于 `2026-08-23T13:14:34.0051747Z` 为 `PASS`；`reports/compatibility/release-candidate-continued/release-candidate.json` 于 `2026-08-23T13:24:13.2358856Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、1 skipped，所有发布检查 PASS；之后 `go test ./... -count=1` 与 `git diff --check` 也通过。
- 本轮最新验证：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-final-continuation4/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-final-continuation4/release-candidate.json` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 本轮继续收敛 P0 Connector/J 协议：`COM_RESET_CONNECTION` 现在先通过引擎事务边界回滚未提交 DML，再清理 transaction journal、账户变更、prepared state、数据库、锁、warning、用户变量和会话变量；两套网络处理器均已接入，专项网络/引擎回归通过。
- 本轮继续收敛 P1-SQL-003：原始 ALTER 路径支持列和索引的 `ADD/DROP ... IF [NOT] EXISTS`，同时支持 `ADD COLUMN ... FIRST/AFTER column` 并保持列元数据顺序；单操作、多操作和既有 ALTER 回归通过。
- 本轮最新门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation11/cluster-report.json` 于 `2026-08-23T18:20:04.8405645Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation11/release-candidate.json` 于 `2026-08-23T18:29:59.257249Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，隔离 Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 本轮再补齐 ALTER 列位置：`MODIFY/CHANGE COLUMN ... FIRST/AFTER` 现在同步调整持久化 `.frm` 列顺序，普通 `MODIFY` 不会被错误 raw 路由拦截；新增顺序回归与 ALTER 全量专项通过。
- continuation12 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation12/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation12/release-candidate.json` 于 `2026-08-23T18:43:59.3147453Z` 为 `GO`，所有发布检查 PASS，隔离 Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 本轮继续收敛 P3/P4：RuntimeRecorder 新增有界语句事件 history，`PERFORMANCE_SCHEMA.events_statements_current/history/history_long` 从真实完成查询记录生成；表达式矩阵新增 `CONCAT_WS`、`SUBSTRING_INDEX`、`JSON_ARRAY`、`JSON_OBJECT`、`JSON_CONTAINS`，对应 metrics/engine/plan 回归通过。完整 P_S wait/memory 生命周期和全类型/函数/collation 矩阵仍保留边界。
- continuation13 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation13/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation13/release-candidate.json` 于 `2026-08-23T19:00:17.9002478Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 全部 PASS，JDBC 136 tests、0 failures、0 errors、1 skipped。
- 本轮继续收敛 P1-SQL-003：ALTER raw 路径识别并接受常见 `ALGORITHM=INPLACE/INSTANT` 与 `LOCK=NONE/SHARED/EXCLUSIVE` 选项，不再把选项误判为独立 ALTER 操作；当前仍是元数据路径兼容，真正 online rebuild/lock scheduling 继续保留边界。
- 本轮继续收敛类型/函数矩阵：补充 `QUARTER`、`WEEKDAY`、`DAYOFWEEK`、`DAYOFYEAR`、`LAST_DAY`，覆盖闰年和 MySQL 周日起始/周一索引返回值；函数专项回归通过。
- continuation14 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation14/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation14/release-candidate.json` 于 `2026-08-23T19:14:56.3582284Z` 为 `GO`，所有发布检查 PASS，Connector/J 136 tests、0 failures、0 errors、1 skipped。
- 本轮继续收敛递归 CTE：兼容执行路径与 CTE 定义校验现在接受 `UNION DISTINCT`/省略 `DISTINCT` 的递归成员；重复递归行按 DISTINCT 语义终止，`UNION ALL` 的环检测保持不变，新增 `TestRecursiveCTECompatibilitySupportsUnionDistinctTermination`，engine 全量回归通过。多定义递归 CTE、通用递归 AST、递归 UPDATE/DELETE 和复杂集合递归仍保留边界。
- 本轮继续收敛 P4 函数矩阵：表达式执行器新增 `REGEXP_REPLACE` 与 `REGEXP_SUBSTR`，同时通过直接表达式和真实 SQL 投影回归；完整 ICU/MySQL 正则 match_type、位置/occurrence 参数及全量 collation 行为仍保留边界。
- 本轮修复 P0 Connector/J 认证门禁：JDBC 兼容配置关闭 `dev_bypass_password_auth`，正确密码与错误密码均经过真实认证；错误密码用例不再跳过，完整 JDBC 结果为 136 tests、0 failures、0 errors、0 skipped。
- continuation15 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation15/cluster-report.json` 于 `2026-08-23T19:33:17.7652269Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation15/release-candidate.json` 于 `2026-08-23T19:33:00Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1 DDL：新增原始 SQL `CREATE TABLE ... LIKE ...` 兼容路径，复制持久化列/索引定义但不复制数据；同时让新建空表持久化 root page，空表首次 SELECT 不再因缺少 storage root 失败。新增 `TestCreateTableLikeCopiesDefinitionWithoutRows`，专项回归通过。
- 本轮继续收敛 P4 JSON 矩阵：表达式与 SQL 投影新增 `JSON_SET`、`JSON_REPLACE`、`JSON_REMOVE`、`JSON_LENGTH`、`JSON_TYPE`，覆盖简单对象路径、对象/数组长度和 JSON 类型返回；数组索引、通配路径、完整 JSON path 修改语义仍保留边界。
- continuation16 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation16/cluster-report.json` 于 `2026-08-23T19:45:36.2033234Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation16/release-candidate.json` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1 metadata：新增 `SHOW CREATE DATABASE/SCHEMA` 真实返回路径，校验数据库存在并返回可执行的 `CREATE DATABASE` 定义；新增 `TestShowCreateDatabaseReturnsPersistedDatabaseDefinition`，engine 专项回归通过。
- continuation17 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation17/cluster-report.json` 于 `2026-08-23T20:00:01.0440245Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation17/release-candidate.json` 于 `2026-08-23T20:09:40Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛递归 CTE：递归兼容执行路径新增常见多列锚点/递归投影，支持递归行按声明列绑定并在主查询中进行多列投影、过滤、排序和 LIMIT/OFFSET；多定义、通用递归 AST、递归 DML 与复杂集合递归仍保留边界。
- 本轮继续收敛 P1 SQL：递归 CTE 新增单列结果驱动的 `UPDATE/DELETE ... IN (SELECT ...)` 物化路径；集合运算新增简单嵌套括号分支的 UNION/INTERSECT/EXCEPT 执行；复杂递归 DML、复杂嵌套集合表达式和通用 AST 仍保留边界。
- 本轮继续收敛 P4 JSON：`JSON_SET/JSON_REPLACE/JSON_REMOVE/JSON_LENGTH/JSON_EXTRACT` 新增常见数组下标路径（如 `$.items[0]`）和数组修改/删除语义；通配路径、范围路径和完整 MySQL JSON path 模式仍保留边界。
- 本轮继续收敛 P4 正则函数：`REGEXP_SUBSTR/REGEXP_REPLACE` 支持常见起始位置、匹配序号和 `match_type=i` 参数；完整 ICU 规则、复杂字符集/collation 和所有 match_type 组合仍保留边界。
- 本轮继续收敛 P4 正则函数：`REGEXP_LIKE` 现在接受常见 `match_type=i` 大小写不敏感匹配；完整 ICU 规则、位置/区域设置与所有 match_type 组合仍保留边界。
- 本轮继续收敛对象元数据：`INFORMATION_SCHEMA.TRIGGERS` 从持久化定义解析并返回常见 `EVENT_MANIPULATION`、`EVENT_OBJECT_TABLE`、`ACTION_TIMING` 和 `ACTION_STATEMENT`；复杂 trigger body、全量 definer/security 与对象依赖语义仍保留边界。
- 本轮继续收敛对象元数据：`INFORMATION_SCHEMA.EVENTS` 从持久化 schedule 返回常见 `RECURRING/ONE TIME`、`INTERVAL_VALUE`、`INTERVAL_FIELD` 和 `EXECUTE_AT`；完整 event scheduler 生命周期、时区/状态字段矩阵和复杂 schedule 仍保留边界。
- continuation18 门禁证据：新增递归 DML、嵌套集合和 JSON 数组路径后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation18/cluster-report.json` 于 `2026/8/23 20:24:57` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation18/release-candidate.json` 于 `2026/8/23 20:34:39` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation19 门禁证据：新增正则位置/occurrence 参数、JSON 数组路径和 `ALTER TABLE ... RENAME TO` 回归后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation19/cluster-report.json` 于 `2026/8/23 20:38:02` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation19/release-candidate.json` 于 `2026/8/23 20:47:37` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation20 门禁证据：新增 `INFORMATION_SCHEMA.TRIGGERS` 的触发器形状字段和 `INFORMATION_SCHEMA.EVENTS` 的 schedule 字段后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation20/cluster-report.json` 于 `2026/8/23 20:50:53` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation20/release-candidate.json` 于 `2026/8/23 21:00:27` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation21 门禁证据：新增 `REGEXP_LIKE(..., 'i')` 大小写不敏感匹配后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation21/cluster-report.json` 于 `2026/8/23 21:02:37` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation21/release-candidate.json` 于 `2026/8/23 21:12:17` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1-SQL-001：单列递归 CTE 优先使用 AST 表达式/谓词执行，支持常见锚点表达式和乘法等递归成员表达式，同时保留旧路径的递归深度、环检测、DML 和 UNION DISTINCT 行为；多定义、复杂递归集合和通用多源递归 AST 仍保留边界。
- continuation22 门禁证据：单列递归 AST 扩展后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation22/cluster-report.json` 于 `2026/8/23 21:16:44` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation22/release-candidate.json` 于 `2026/8/23 21:26:18` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1/P4 窗口查询：同一 `SELECT` 现在可以投影多个窗口表达式，分别复用既有 partition/order/frame 执行器并按源行对齐合并结果；新增 `TestWindowQuerySupportsMultipleWindowExpressions`，单窗口、命名窗口和 frame 回归保持通过。完整窗口 AST、复杂嵌套窗口、全 frame/collation 矩阵仍保留边界。
- 本轮继续收敛 P4 表达式执行：存储层行表达式现在支持 searched/simple `CASE` 的 `WHEN/THEN/ELSE` 投影语义，并通过真实表查询回归；完整类型转换、排序规则和函数矩阵仍保留边界。
- 本轮继续收敛存储过程执行：`IF ... ELSEIF ... ELSE` 现在按条件顺序执行并保留原有局部作用域、handler 和 session 变量语义；新增 `TestProcedureIfSupportsElseIfBranches`，routine 专项回归通过。
- continuation23 门禁证据：窗口扩展后的 `reports/compatibility/p1-cluster-current-continuation23/cluster-report.json` 于 `2026-08-23T21:35:23.6693184Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation23/release-candidate.json` 于 `2026-08-23T21:45:01.0611399Z` 的全部检查为 `PASS`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 全部通过，JDBC 136 tests、0 failures、0 errors、0 skipped。CASE/ELSEIF 改动后的最新门禁待 continuation24 复验。
- continuation24 门禁证据：CASE/ELSEIF 改动后的 `reports/compatibility/p1-cluster-current-continuation24/cluster-report.json` 于 `2026-08-23T21:49:31.9162067Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation24/release-candidate.json` 于 `2026-08-23T21:59:09.4263872Z` 的 clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 检查全部 PASS，JDBC 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P4 日期函数与窗口形态：表达式执行器新增 `TIMESTAMPADD`，窗口执行器支持无普通投影列的 window-only `SELECT` 并复用已有排序/分区/frame 逻辑；新增对应 plan/engine 回归。完整日期类型、窗口 AST 和全量 collation 矩阵仍保留边界。
- continuation25 门禁证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation25/cluster-report.json` 于 `2026-08-23T22:03:15.1861017Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation25/release-candidate.json` 于 `2026-08-23T22:12:56.5607295Z` 的 clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 检查全部 PASS，JDBC 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1-SQL-001：递归 CTE 新增常见层级查询路径，锚点可从真实基础表取数，递归成员支持 `CTE JOIN` 基础表并按 `ON/WHERE` 条件生成下一层多列结果；新增 `TestRecursiveCTECompatibilitySupportsBaseTableJoin`，既有递归 CTE 专项回归保持通过。多定义、复杂多源递归和更完整递归 AST 仍保留边界。
- continuation26 门禁证据：递归 CTE 基础表 JOIN 扩展后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation26/cluster-report.json` 于 `2026-08-23T22:17:17.5928407Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation26/release-candidate.json` 于 `2026-08-23T22:27:06.8525158Z` 的全部检查 PASS，包含 3 次 crash-recovery、100 次并发、observability 和 Connector/J 136 tests，0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1-SQL-001/P1-OPT-003：基础表 JOIN 递归 CTE 同时覆盖单列和多列声明；增强统计收集器读取持久化 `TableStatistics.AutoIncrement`，不再把该字段固定返回 0，并新增对应 plan 回归。完整多定义递归、真实列值全表采样和物理统计文件持久化仍保留边界。
- continuation27 门禁证据：单列/多列递归基础表 JOIN 与自增统计改动后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation27/cluster-report.json` 于 `2026-08-23T22:30:32.6122547Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation27/release-candidate.json` 于 `2026-08-23T22:40:10.7424507Z` 的全部检查 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛存储对象执行：BEFORE trigger 现在逐条执行多个 `SET NEW.column = ...`，并支持基于 `NEW` 列的算术表达式赋值；新增多赋值和表达式回归。复杂 trigger body、完整触发器依赖/权限矩阵仍保留边界，最新门禁待 continuation28 复验。
- continuation28 门禁证据：BEFORE trigger 多语句/表达式扩展后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation28/cluster-report.json` 于 `2026-08-23T22:43:52.8676828Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation28/release-candidate.json` 于 `2026-08-23T22:53:59.8012397Z` 的全部检查 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1-SQL-003：复合 `ALTER TABLE` 现在可组合列/索引操作与常见 UNIQUE、PRIMARY KEY、FOREIGN KEY、CHECK 约束；每个顶层子句沿用既有单操作实现，后续子句失败时恢复原始 `.frm`，新增 `TestAlterTableCompoundColumnAndConstraintOperations` 覆盖列新增、CHECK 约束和唯一索引的真实 INSERT 校验。完整 ALTER AST、在线重建、复杂算法/锁调度仍保留边界。
- 本轮继续收敛 P1-SQL-001：递归 CTE 解析器现在支持多个独立定义，并对每个单列定义执行递归物化；单源主查询可选择任一已声明 CTE，新增 `TestRecursiveCTECompatibilitySupportsMultipleIndependentDefinitions`。递归多源 JOIN、多列多定义执行和复杂相关 AST 仍保留边界。
- 本轮继续收敛 P1-SQL-001：多个独立递归定义现在支持单列/多列物化，并可通过受限的两个 CTE 内连接主查询投影、过滤、排序和去重；新增 `TestRecursiveCTECompatibilitySupportsMultiColumnDefinitionsInJoin`。递归成员通用多源 JOIN、递归聚合 JOIN 和复杂相关 AST 仍保留边界。
- 本轮继续收敛存储对象执行：存储过程 `CALL` 现在支持共享会话/事务上下文中的递归调用，递归调用拥有独立局部状态并设置 64 层保护；新增 `TestProcedureSupportsRecursiveCallWithChangingInput` 验证递归 DML 的真实结果。递归过程的 OUT/INOUT 完整回传、复杂动态 SQL 和完整权限/依赖矩阵仍保留边界。
- continuation30 证据：递归过程专项、`go test ./... -count=1` 全仓回归和 `cluster_smoke.ps1 -ReportDir reports/compatibility/p1-cluster-current-continuation30` 均通过；本轮完整 release gate 未生成新 JSON，上一份完整发布门禁仍为 continuation28（Connector/J 136/136，0 failures、0 errors、0 skipped），新门禁耗时主要集中在 JDBC `PerformanceTest` 的 10000/5000 行批量场景，不能把未生成报告误记为通过。
- 本轮继续收敛 P1/PX 分区 DDL：`ALTER TABLE ... REORGANIZE PARTITION ... INTO (...)` 现在支持 RANGE/LIST/HASH/KEY 的单分区拆分和多个分区合并，校验替换分区名、RANGE 边界升序、`MAXVALUE` 位置以及被重组分区中的已有行是否落入替换集合，并原子更新 `.frm`、存储管理器及 `INFORMATION_SCHEMA.PARTITIONS`；共享表级存储下现有行保持可读。新增多分区/LIST/非法边界回归，物理独立分区表空间和真正的数据页搬迁仍保留边界。
- 本轮继续收敛视图查询：视图引用现在支持常见外层 `WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/UNION/INTERSECT/EXCEPT` 尾部以及 `AS`/裸别名，并统一改写为派生表后复用既有执行器；新增视图过滤和别名查询回归。视图更新语义、复杂相关视图和物化视图刷新仍保留边界。
- continuation31 验证证据：`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation31/cluster-report.json` 于 `2026-08-24T00:10:37.6046161Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation31/release-candidate.json` 于 `2026-08-24T00:21:02.8213150Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 136 tests 全部通过，0 failures、0 errors、0 skipped。完整 MySQL 8.4 兼容、物理分区表空间、FULLTEXT 和非 Connector/J 全量客户端仍不在本轮完成边界。
- continuation31 后续定向验证：视图 `AS`/裸别名改写后的 engine 全量测试通过（`42.390s`），`git diff --check` 无 whitespace 错误；发布门禁报告仍以 alias 改动前的 continuation31 GO 作为全量证据，alias 改动已完成 engine 定向回归，下一轮发布门禁需重新复验。
- 本轮继续收敛存储过程递归参数：递归 `CALL` 现在保留 `@session_variable` 形式的 `OUT/INOUT` 参数引用，不再在递归参数解析时错误求值为快照；新增 `TestProcedureSupportsRecursiveInOutUserVariable` 验证递归累加后会话变量真实回写。嵌套过程间局部变量引用传播、复杂 OUT/INOUT 表达式和完整 routine scope 仍保留边界。
- 本轮继续收敛 routine scope：外层过程的局部变量作为内层过程 `OUT/INOUT` 参数时，会通过受控临时会话变量传递并在子过程返回后同步回局部状态；新增 `TestProcedurePropagatesNestedLocalInOutParameter`，routine/trigger/event 专项回归通过。复杂表达式参数和完整动态 scope 仍保留边界。
- 本轮继续收敛 P4 类型/函数矩阵：表达式执行器新增 `MD5`、`SHA/SHA1`、`SHA2`（224/256/384/512 及 0）、`CRC32`、`INET_ATON/INET_NTOA`、`IS_IPV4/IS_IPV6`、`UUID_TO_BIN/BIN_TO_UUID`，覆盖常见 NULL/非法参数语义、UUID 时间字段交换和真实 SQL 嵌套调用；完整函数、字符集和二进制类型矩阵仍保留边界。
- continuation32 最新验证证据：`go test ./... -count=1`、cluster smoke 和完整 release candidate 均通过；`reports/compatibility/p1-cluster-current-continuation32/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation32/release-candidate.json` 于 `2026-08-24T00:48:07.5758034Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。剩余兼容边界继续按 P1/P3/P4 清单推进，FULLTEXT 与非 Connector/J 全量客户端遵循用户要求暂不纳入门禁。
- 本轮继续收敛 P4 类型/函数矩阵：新增 `HEX/UNHEX/BIN/OCT` 二进制与数值转换，补充真实 SQL 投影回归；`TRIM/LTRIM/RTRIM` 已覆盖常见空白字符语义。完整 binary/collation 转换矩阵仍保留边界。
- continuation33 最新验证证据：新增二进制/空白字符串函数后，`go test ./... -count=1`、cluster smoke 和完整 release candidate 均通过；`reports/compatibility/p1-cluster-current-continuation33/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation33/release-candidate.json` 于 `2026-08-24T01:04:36.8335328Z` 为 `GO`，所有发布检查 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1-SQL-001：多个独立递归 CTE 的内连接主查询现在复用派生聚合器，支持常见 `COUNT/SUM`、`GROUP BY` 和聚合结果回归；递归成员通用多源 JOIN、复杂相关 AST 仍保留边界。
- continuation34 最新验证证据：递归 CTE JOIN 聚合改动后，`go test ./... -count=1`、cluster smoke 和完整 release candidate 均通过；`reports/compatibility/p1-cluster-current-continuation34/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation34/release-candidate.json` 于 `2026-08-24T01:18:57.1130102Z` 为 `GO`，所有发布检查 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 P1-SQL-001：递归 CTE 支持 dual/literal 锚点下，递归成员将当前 CTE 层与一个持久化基础表 JOIN，执行 `ON/WHERE` 和多列表达式投影；新增 `TestRecursiveCTECompatibilitySupportsGenericMemberJoinFromDualAnchor`，任意多源递归 JOIN 图和复杂相关 AST 仍保留边界。
- continuation35 最新验证证据：dual/literal-anchor 递归 JOIN 改动后，`go test ./... -count=1`、cluster smoke 和完整 release candidate 均通过；`reports/compatibility/p1-cluster-current-continuation35/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation35/release-candidate.json` 于 `2026-08-24T01:37:23.8773539Z` 为 `GO`，所有发布检查 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮进一步扩展 P1-SQL-001：递归 dual/literal-anchor 成员 JOIN 支持嵌套 inner-join 基础表图，按每层 frontier 执行各级 `ON` 条件和表达式投影；新增 `TestRecursiveCTECompatibilitySupportsNestedBaseTableJoinMember`，递归 outer/correlated JOIN 和复杂相关 AST 仍保留边界。
- 本轮补充 P1-SQL-002 回归审计：现有集合执行路径已通过更深的嵌套 `UNION/INTERSECT/EXCEPT` 括号分支（含外层 `UNION ALL` 与 `ORDER BY`）测试，复杂集合表达式与任意查询尾部组合仍保留边界。
- continuation36 最新验证证据：递归嵌套 inner-join 基础表图和更深集合分支回归后，`go test ./... -count=1`、cluster smoke 和完整 release candidate 均通过；`reports/compatibility/p1-cluster-current-continuation36/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation36/release-candidate.json` 于 `2026-08-24T01:53:42.1634495Z` 为 `GO`，所有发布检查 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 PX-OPS-001：`performance_schema.setup_consumers` 与 `setup_instruments` 从静态行升级为 executor 实例内的可变配置状态，支持常见 `UPDATE ... SET enabled/timed ... WHERE name = ...`、`LIKE` 和多字段更新；后续查询按名称过滤并反映最新状态，新增 Performance Schema 配置更新回归。完整 wait/memory/event 生命周期仍保留边界。
- continuation37 最新验证证据：Performance Schema 配置更新后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation37/cluster-report.json` 于 `2026-08-24T02:01:19.9614123Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation37/release-candidate.json` 于 `2026-08-24T02:11:26.3418193Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 全部 PASS，JDBC 136 tests、0 failures、0 errors、0 skipped。
- 本轮继续收敛 PX-OPS-001：setup consumer/instrument 配置现在影响事件视图——关闭 statement history consumer 后对应 history 为空，关闭 statement instrument 后事件保留但 `TIMER_WAIT` 为 0；`setup_actors` 支持常见 `host/user` 条件下的 `enabled/history` 更新和查询。
- continuation38 最新验证证据：上述 P_S 事件可见性和 actor 配置回归后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation38/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation38/release-candidate.json` 于 `2026-08-24T02:25:20Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他 build/unit/integration/go-core、恢复、并发和 observability 检查均 PASS。
- 本轮继续收敛 P1-SEC-001：动态全局权限不再只是账号 grants JSON 中的未知字符串；新增 `mysql.global_grants` 查询投影、`WITH_GRANT_OPTION` 生命周期、`information_schema.user_privileges.IS_GRANTABLE` 反映 GRANT OPTION，以及认证侧 `UserInfo.DynamicPrivileges` 与 `CheckDynamicPrivilege` 增量 API；新增 BACKUP_ADMIN 授权、撤销授权选项、撤销权限和普通账号检查回归。完整动态权限注册表和全部系统授权表写入语义仍保留边界。
- 本轮继续收敛 P1-SQL-001：递归 dual/literal-anchor 成员 JOIN 图新增受控 `LEFT JOIN`，对缺失的基础表匹配生成 NULL 扩展行，后续 `COALESCE`/`WHERE` 和递归投影保持生效；RIGHT/FULL/correlated recursive JOIN AST 仍保留边界。
- continuation40 最新验证证据：递归 CTE `LEFT JOIN` null-extension 改动后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation40/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation40/release-candidate.json` 于 `2026-08-24T02:57:41Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，恢复、并发和 observability 也全部 PASS。
- 本轮继续收敛 P1-SEC-001：新增 `SHOW CREATE USER`，从持久化账号返回认证插件、认证串、REQUIRE SSL/X509、ACCOUNT LOCK 和 PASSWORD EXPIRE 形状，新增账户元数据回归；完整 MySQL 用户 DDL 选项矩阵仍保留边界。
- continuation41 最新验证证据：`SHOW CREATE USER` 账户元数据改动后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation41/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation41/release-candidate.json` 于 `2026-08-24T03:11:47Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，恢复、并发和 observability 全部 PASS。
- 本轮继续收敛 PX-OPS-001：Performance Schema 新增基于 LockManager 等待图的 `data_locks`、`data_lock_waits`、`events_waits_current/history_long` 投影，以及有界 `memory_summary_global_by_event_name` 视图；新增真实锁冲突回归，完整等待事件生命周期和内存分配器统计仍保留边界。
- continuation42 最新验证证据：P_S 锁等待/内存视图改动后，`go test ./... -count=1` 全仓通过；`reports/compatibility/p1-cluster-current-continuation42/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation42/release-candidate.json` 于 `2026-08-24T03:27:27Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，恢复、并发和 observability 全部 PASS。
- continuation43 继续收敛角色与事务观测：新增 `CURRENT_ROLE()`、`INFORMATION_SCHEMA.ENABLED_ROLES`，并新增基于连接事务状态的 `performance_schema.events_transactions_current` 投影；聚焦回归、`go test ./... -count=1`、集群 smoke 和完整发布门禁均通过。集群报告 `reports/compatibility/p1-cluster-current-continuation43/cluster-report.json` 于 `2026-08-24T03:33:10.7138232Z` 为 `PASS`；发布报告 `reports/compatibility/release-candidate-current-continuation43/release-candidate.json` 于 `2026-08-24T03:43:57.9604806Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- continuation45 继续补齐角色激活语义：`SET ROLE ALL EXCEPT 'role'@'host'` 按当前账号已授予角色计算并保留其余角色；全仓回归、集群 smoke 和当前工作树完整发布门禁均通过。集群报告 `reports/compatibility/p1-cluster-current-continuation45/cluster-report.json` 于 `2026-08-24T04:01:01.8747799Z` 为 `PASS`；发布报告 `reports/compatibility/release-candidate-current-continuation45/release-candidate.json` 于 `2026-08-24T04:11:17.0210207Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，build/unit/integration/go-core、恢复、并发、observability 全部 PASS。
- continuation46 继续补齐角色元数据：新增 `INFORMATION_SCHEMA.APPLICABLE_ROLES`，返回当前账号直接角色边、角色主机、`IS_GRANTABLE` 和 `DEFAULT_ROLE`；全仓回归、集群 smoke 和当前工作树完整发布门禁均通过。集群报告 `reports/compatibility/p1-cluster-current-continuation46/cluster-report.json` 于 `2026-08-24T04:14:31.5269248Z` 为 `PASS`；发布报告 `reports/compatibility/release-candidate-current-continuation46/release-candidate.json` 于 `2026-08-24T04:25:01.3723086Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- continuation47 继续补齐角色授权元数据：新增 `INFORMATION_SCHEMA.ROLE_TABLE_GRANTS` 与 `ROLE_COLUMN_GRANTS`，从当前账号可用角色的持久化表级/列级授权投影 `GRANTEE`、作用域、权限类型和 `IS_GRANTABLE`；全仓回归、集群 smoke 和当前工作树完整发布门禁均通过。集群报告 `reports/compatibility/p1-cluster-current-continuation47/cluster-report.json` 为 `PASS`；发布报告 `reports/compatibility/release-candidate-current-continuation47/release-candidate.json` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- continuation48 继续收敛 P1-OPT-003/P1-SQL-001/P1-SEC-001：`ANALYZE` 产生的持久化 `.frm` 统计现在可在引擎重启后重新加载进入优化器；递归 dual/literal-anchor 成员 JOIN 新增受控 `RIGHT JOIN` 与右侧未匹配行的 NULL 扩展；角色元数据递归包含嵌套角色，并新增 `INFORMATION_SCHEMA.ROLE_ROUTINE_GRANTS`，支持过程/函数的 `EXECUTE`、`ALTER ROUTINE` 和授权选项投影。全仓回归、集群 smoke 和当前工作树完整发布门禁均通过：`reports/compatibility/p1-cluster-current-continuation48/cluster-report.json` 于 `2026-08-24T04:46:04Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation48/release-candidate.json` 于 `2026-08-24T04:57:10.7453621Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。递归 FULL/correlated JOIN、完整动态权限注册表、物理统计采样和完整 MySQL 8.4 兼容仍保留明确边界。
- continuation49 继续收敛 PX-OPS-001：Performance Schema 新增连接级有界 `events_transactions_history`/`events_transactions_history_long`，记录提交和回滚事务的状态、隔离级别、自动提交和事件编号，并可由对应 consumer 开关控制；既有 `events_transactions_current` 保持活动事务语义。全仓回归、集群 smoke 和当前工作树完整发布门禁均通过：`reports/compatibility/p1-cluster-current-continuation49/cluster-report.json` 于 `2026-08-24T05:05:59Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation49/release-candidate.json` 于 `2026-08-24T05:16:15Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。完整事务事件生命周期、wait/memory 全量统计仍保留边界。
- 本轮继续收敛 P1-SEC-001：新增 `INFORMATION_SCHEMA.ADMINISTRABLE_ROLE_AUTHORIZATIONS`，投影当前账号直接可授予的角色及 `WITH ADMIN OPTION`；角色元数据路由与专项回归已补齐，嵌套角色、完整授权表事务语义和完整动态权限注册表仍保留边界。
- 本轮继续收敛 P4 JSON 函数矩阵：表达式执行器新增 `JSON_CONTAINS_PATH`、`JSON_KEYS`、`JSON_DEPTH`、`JSON_PRETTY`，覆盖路径存在性（one/all）、对象键提取、嵌套深度和格式化输出；通配/范围路径、完整 JSON path 与类型/collation 矩阵仍保留边界。全仓回归、集群 smoke 和当前工作树完整发布门禁均通过：`reports/compatibility/p1-cluster-current-continuation50/cluster-report.json` 于 `2026-08-24T05:20:46Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation50/release-candidate.json` 于 `2026-08-24T05:31:18Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- continuation51 门禁证据：`JSON_VALID` 与 `JSON_QUOTE` 改动后，`go test ./... -count=1`、集群 smoke 和当前工作树完整发布门禁均通过；`reports/compatibility/p1-cluster-current-continuation51/cluster-report.json` 于 `2026-08-24T05:33:07Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation51/release-candidate.json` 于 `2026-08-24T05:44:01Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- 本轮继续收敛 P4 JSON 基础函数：新增 `JSON_VALID`、`JSON_QUOTE` 和 `JSON_OVERLAPS`，覆盖合法/非法 JSON 判定、NULL 传播、字符串 JSON 转义以及对象/数组重叠判断；完整 JSON 类型与字符集矩阵仍保留边界。全仓回归、集群 smoke 和当前工作树完整发布门禁均通过：`reports/compatibility/p1-cluster-current-continuation52/cluster-report.json` 于 `2026-08-24T05:48:12Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation52/release-candidate.json` 于 `2026-08-24T05:59:50Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- 本轮继续收敛 P4 JSON 合并语义：新增 `JSON_MERGE_PATCH`，支持对象递归合并、标量替换和 JSON `null` 删除键；完整数组/通配路径及类型/collation 矩阵仍保留边界。全仓回归、集群 smoke 和当前工作树完整发布门禁均通过：`reports/compatibility/p1-cluster-current-continuation53/cluster-report.json` 于 `2026-08-24T06:02:10Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation53/release-candidate.json` 于 `2026-08-24T06:13:46Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。
- continuation54 继续收敛 P1-OPT-001/P1-OPT-002/P1-OPT-004：索引优化器新增结构化 `BETWEEN`、`IN`、`LIKE`、`IS NULL` 条件提取；上层 LEFT/RIGHT JOIN 的单侧谓词可安全下推，跨表谓词保持原位；逻辑列裁剪现在通过物理表扫描和索引扫描的 required columns 传递并输出裁剪后的记录 schema。全仓回归、集群 smoke 和当前工作树完整发布门禁均通过：`reports/compatibility/p1-cluster-current-continuation54/cluster-report.json` 于 `2026-08-24T06:22:32Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation54/release-candidate.json` 于 `2026-08-24T06:33:34Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他检查全部 PASS。通用子查询谓词推导、复杂 outer-join 表达式以及并行/其他 row-source 的完整列裁剪仍保留边界。
- continuation55 继续收敛 P4 JSON 函数矩阵：新增 `JSON_ARRAY_APPEND` 与 `JSON_INSERT`，支持对象/数组路径、数组追加、多路径修改和已有键不覆盖语义，并通过 plan 函数测试、真实 SQL 投影测试及 `go test ./... -count=1` 全仓回归；JSON 通配/范围路径、完整类型与 collation 矩阵仍保留边界。集群 smoke 与 release candidate 门禁将在本轮代码改动后继续复验。
- continuation56 继续收敛 P4 JSON 数组修改语义：新增 `JSON_ARRAY_INSERT`，支持对象内数组的下标插入、根数组/嵌套数组路径和多路径顺序修改；plan、engine 专项回归与 `go test ./... -count=1` 全仓回归通过。`reports/compatibility/p1-cluster-current-continuation56/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation56/release-candidate.json` 于 `2026-08-24T06:52:40Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，build/unit/integration/go-core、3 次 crash-recovery、并发和 observability 全部 PASS。JSON 通配/范围路径、完整类型与 collation 矩阵仍保留边界。
- continuation57 继续收敛 P4 JSON 查询语义：新增 `JSON_SEARCH`，支持 `one/all`、`%`/`_` 简单通配、转义字符、对象键排序后的确定性路径和数组路径；plan、engine 专项与 `go test ./... -count=1` 全仓回归均通过。`reports/compatibility/p1-cluster-current-continuation57/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation57/release-candidate.json` 于 `2026-08-24T07:06:20Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，build/unit/integration/go-core、3 次 crash-recovery、并发和 observability 全部 PASS。完整 JSON path 通配/范围、类型与 collation 矩阵仍保留边界。
- continuation58 继续收敛 P1-SQL-001：相关 `EXISTS/NOT EXISTS` 不再限定内层为单表 `FROM ... WHERE`，现在会逐外层行替换相关引用后交给普通 SELECT 执行器，因此支持内层 JOIN 的相关 EXISTS；新增真实父表/子表/标签表回归，相关 scalar/predicate/derived 回归保持通过。全仓回归、集群 smoke 和 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation58/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation58/release-candidate.json` 于 `2026-08-24T07:21:46Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他 checks 全部 PASS。递归 FULL/correlated JOIN AST 与更复杂外层相关查询仍保留边界。
- continuation59 继续收敛 P1-SQL-001：相关 `EXISTS/NOT EXISTS` 的外层来源现在可使用普通 SELECT 执行器支持的 JOIN 图，外层谓词与排序仍保留，相关引用按外层行重绑定；新增父表 JOIN 元数据表 + 内层 JOIN 的真实回归，已有相关子查询专项通过。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation59/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation59/release-candidate.json` 于 `2026-08-24T07:39:40Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他 checks 全部 PASS。递归 FULL/correlated JOIN AST、派生表/聚合等更复杂外层相关形态仍保留边界。
- continuation60 继续收敛 P1-SQL-001：相关标量子查询的外层来源现在也可使用普通 SELECT 执行器支持的 JOIN 图，外层 WHERE/ORDER BY 和逐外层行的聚合重绑定保持生效；新增真实父表 JOIN 元数据表 + `MAX()` 标量子查询回归，相关 EXISTS/predicate/derived 专项通过。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation60/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation60/release-candidate.json` 于 `2026-08-24T07:53:26Z` 为 `GO`，Connector/J 136 tests、0 failures、0 errors、0 skipped，其他 checks 全部 PASS。递归 FULL/correlated JOIN AST、派生表/聚合等更复杂外层相关形态仍保留边界。
- continuation61 继续收敛 P1-SQL-001：相关 `IN/NOT IN/ANY/SOME/ALL` predicate 子查询的外层来源现在也可使用普通 SELECT 执行器支持的 JOIN 图，外层谓词、排序、NULL/量化语义与逐外层行绑定保持生效；新增父表 JOIN 元数据表 + 内层 JOIN 的真实 `IN` 回归，相关 EXISTS/scalar/derived 专项通过。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation61/cluster-report.json` 于 `2026/8/24 7:56:30` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation61/release-candidate.json` 的 clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。递归 FULL/correlated JOIN AST、复杂外层相关表达式和更完整子查询 AST 仍保留边界。
- continuation62 继续收敛 P1-SQL-002：集合运算共享尾部解析现在支持无 `ORDER BY` 的 `LIMIT`、`LIMIT row_count OFFSET offset`、`LIMIT offset,row_count`、`LIMIT 0`，并支持多个 `ORDER BY` 列的稳定排序；新增 UNION 与混合集合运算的真实回归。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation62/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation62/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。复杂集合表达式与任意查询尾部组合仍保留边界。
- continuation63 继续收敛 P1-OPT-001：修正 LEFT/RIGHT JOIN 谓词下推的 NULL 扩展语义，仅将保留侧单表谓词下推到子计划，NULL 扩展侧谓词保留在连接后的 Selection；新增左右外连接计划回归，避免未匹配行被错误保留。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation63/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation63/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。通用子查询推理和更丰富外连接表达式推导仍保留边界。
- continuation64 继续收敛 P4 JSON 路径：`JSON_EXTRACT` 现在复用已有 JSON path lookup，支持常见对象路径和数组下标路径（如 `$.items[1]`），新增 plan 与函数兼容回归；通配/范围路径、多路径返回数组和完整 JSON 类型/collation 矩阵仍保留边界。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation64/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation64/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation65 继续收敛 P4 JSON 路径：`JSON_EXTRACT` 现在支持多个 path 参数，并按参数顺序返回 JSON 数组，同时保持单路径对象/数组下标行为；新增 plan 与函数兼容回归。通配/范围路径、缺失路径的完整边界、类型/collation 矩阵仍保留边界。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation65/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation65/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation66 继续收敛 P4 JSON 路径：`JSON_EXTRACT` 增加常见数组通配路径 `$.items[*]`，返回保持 JSON 数组形状，并接入 plan/引擎函数回归；对象通配、嵌套通配/范围路径和完整 JSON 类型/collation 矩阵仍保留边界。全仓、集群和 release 门禁均通过：`reports/compatibility/p1-cluster-current-continuation66/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation66/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation67 继续收敛 P1-IDX-001：索引合并现在对有界布尔表达式做 DNF 展开，支持共享谓词包围 OR 分支（如 `name = 'Alice' AND (age = 20 OR city = 'Paris')`），并限制最多 16 个分支以避免优化阶段组合爆炸；新增索引计划回归，复杂无界布尔表达式、完整多列索引交集和类型可排序范围扫描仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation67/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation67/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation68 继续收敛 P4 JSON 路径：`JSON_EXTRACT` 现在支持对象通配 `$.obj.*`、嵌套数组通配 `$.items[*].id` 和数组范围 `$.items[1 to 2]`，结果按对象键/数组顺序稳定收集并保持 JSON 数组形态；递归下降、负索引/`last`、复杂 JSON path 修改语义及完整类型/collation 矩阵仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation68/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation68/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation69 继续收敛 P4 JSON 路径：`JSON_EXTRACT` 现在解析数组末尾索引 `[last]` 与 `[last-N]`，并可与后续对象/数组路径组合；越界和非法末尾索引返回缺失语义，通配/范围路径能力保持通过。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation69/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation69/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation70 继续收敛 P4 JSON 路径：`JSON_EXTRACT` 现在支持常见递归下降 `$**.field`，按对象键/数组顺序遍历各层并返回稳定 JSON 数组；复杂递归路径组合、JSON path 修改语义及完整类型/collation 矩阵仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation70/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation70/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation71 继续收敛 P1-IDX-002：增强统计收集器现在优先读取 metadata 中持久化的索引 `Cardinality`、`LeafPages`、`NonLeafPages`，仅对缺失字段使用估算，并按持久化基数重算选择性/平均每页键数；新增真实收集器回归，完整存储采样和页级统计格式仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation71/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation71/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation72 继续收敛 P1-OPT-003：精确表行数统计在 B+Tree 可提供叶子页列表时只遍历叶子页，避免把内部 INDEX 页的 `PAGE_N_RECS` 误计为行数；新增叶子页统计回归。无叶子页列表时仍保留安全回退，完整 storage-backed 列值采样和直方图格式仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation72/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation72/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。
- continuation73 补充并验证 P1-SQL-002 集合运算矩阵：新增真实查询覆盖括号分支内 `ORDER BY/LIMIT` 与集合运算外层 `ORDER BY/LIMIT` 同时存在的场景，现有执行器返回正确的分支局部限制、重复行和外层排序结果；该测试随 continuation72 的全仓、集群和 release 门禁通过，复杂集合表达式与任意查询尾部组合仍保留边界。
- continuation74 继续收敛 P1-SQL-001：新增相关 DML 兼容入口，支持单表、单列主键的 `UPDATE/DELETE ... WHERE [NOT] EXISTS`；先复用逐外层行绑定的相关 EXISTS SELECT 语义物化目标主键，再交回存储集成 DML 执行器，保留事务、锁、约束、触发器和影响行数路径。新增真实父表/子表 UPDATE 与 NOT EXISTS DELETE 回归；复合主键、相关 DML `IN`/标量赋值以及更复杂相关 DML AST 仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation74/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation74/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation75 继续收敛 P1-SQL-001：相关 DML 入口新增单表、单列主键的 `IN/NOT IN` 子查询物化，按现有相关 predicate 执行器保留逐外层行绑定和 NULL/NOT IN 语义，再交回存储集成 UPDATE/DELETE；新增真实父表/子表 UPDATE 与 DELETE 回归。相关 DML 标量赋值、复合主键和更复杂相关 DML AST 仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation75/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation75/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation76 继续收敛 P1-SQL-001：相关 DML 主键物化从单列扩展为复合主键，按每个命中主键元组生成组合谓词，覆盖相关 `EXISTS/NOT EXISTS/IN/NOT IN` 的 UPDATE/DELETE；新增真实复合主键父表/子表 UPDATE 回归。相关 DML 标量赋值和更复杂相关 DML AST 仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation76/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation76/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation77 继续收敛 P1-SQL-001：补齐常见单赋值相关标量 UPDATE，按目标主键逐行物化 `(SELECT ...)` 结果并生成 CASE 更新；同步放宽存储集成 UPDATE 表达式求值以支持 CASE/列引用/函数等逐行表达式，并修正相关 `COUNT(*)` 空集返回 0 的语义。新增真实父表/子表标量 UPDATE 回归。多赋值、复杂相关标量表达式和更通用相关 DML AST 仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation77/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation77/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation78 继续收敛 P1-SQL-001：相关标量 UPDATE 从单赋值扩展为多赋值，一次按目标主键物化多个相关标量子查询并生成多个 CASE 更新；空集 `COUNT(*)` 保持 0，空集 `MAX()` 保持 NULL。新增真实父表/子表多列标量 UPDATE 回归；更复杂相关标量表达式和通用相关 DML AST 仍保留边界。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation78/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation78/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation79 继续收敛 P1-SQL-001/P1-OPT-002：相关 `EXISTS/NOT EXISTS` 新增单源派生表作为外层来源的兼容路径，物化派生结果后逐外层行绑定相关引用，保留外层投影、WHERE 和 ORDER BY；物理列裁剪补齐 WHERE-only 依赖列，即使列不在 SELECT 投影中也会继续传入存储集成扫描。新增真实派生外层相关 EXISTS 与谓词依赖回归。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation79/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation79/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。多源派生相关外层表、复杂相关 DML 标量表达式和更完整相关 AST 仍保留边界。
- continuation80 继续收敛 P0-03：错误码提取和 executor 错误包装改用 `errors.As` 穿透多层包装；事务适配器的 nil receiver、索引更新删除失败、批量索引部分失败和一致性检查空管理器边界均返回结构化/可定位错误，批量插入失败会回滚已完成的索引写入。新增对应失败优先回归。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation80/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation80/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。真实存储故障注入、全量索引重建持久化验证及 P0-D/P0-E 外部环境证据仍未宣称完成。
- continuation81 继续收敛 P1-SQL-001：相关 `EXISTS` 外层来源新增多源派生表 JOIN 回归；相关 DML 标量赋值支持在相关标量子查询外包裹常见 `COALESCE`/算术表达式，并继续按主键生成 CASE 更新。新增真实多源派生外层查询和 `COALESCE((SELECT ...), 0) + 1` UPDATE 回归。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation81/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation81/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。复杂多重相关标量表达式、相关 DML 的更完整 AST 和真实存储故障注入仍保留边界。
- continuation82 记录 P0-D/P0-E 可执行证据：`generate_p0d_metrics_endpoint_evidence.ps1` 生成 focused `/healthz`/`/metrics` probe，structured logging smoke 与 observability smoke 均为 `PASS`；`generate_p0e_timed_rollback_evidence.ps1` 生成窗口内回滚 contract `PASS`。证据目录为 `reports/compatibility/p0d-current-continuation80`、`reports/compatibility/p0e-current-continuation80` 和 `reports/compatibility/p0-observability-current-continuation80`。P0-E 报告明确是模拟回滚证据，真实部署平台回滚、真实 DSN 灰度写入和外部告警投递仍未宣称完成。
- continuation83 继续收敛 P1-SQL-001：相关 DML 多赋值现在也支持每个相关标量子查询外包裹常见 `COALESCE`/算术表达式，分别物化并生成独立 CASE，保留空集 `COUNT(*)` 与 `MAX()` 语义。新增真实多列 wrapper UPDATE 回归。全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation83/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation83/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。更复杂的多重相关表达式/完整相关 DML AST、真实存储故障注入和外部生产回滚仍保留边界。
- continuation84/85 验证当前工作树：多赋值相关标量 wrapper 改动后的全仓 `go test ./... -count=1` 通过；continuation84 集群 smoke 为 `PASS`，continuation85 完整 release candidate 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。continuation84 发布进程曾被工具层中断且未落盘报告，因此以 continuation85 作为完整门禁证据。更复杂相关 AST、真实存储故障注入、外部生产回滚和 FULLTEXT 仍保留明确边界。
- continuation86 继续收敛 P1-SQL-001：相关标量 SELECT 投影支持常见外层 `COALESCE`/算术包装，并修正 wrapper 结果的数值归一化，避免固定宽度聚合字节泄漏到结果集。新增真实相关 scalar projection wrapper 回归。当前工作树全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation86/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation86/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。更复杂嵌套/多重相关表达式、完整相关 DML AST、真实存储故障注入、外部生产回滚和 FULLTEXT 仍保留边界。
- continuation87 继续收敛 P1-SQL-001：同一 `SELECT` 投影表达式内支持多个相关标量子查询，按每个外层行分别物化并替换独立占位符，保留外层 `COALESCE`/算术组合；新增真实多子查询表达式回归。当前工作树全仓 `go test ./... -count=1`、集群 smoke 和完整 release candidate 均通过：`reports/compatibility/p1-cluster-current-continuation87/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation87/release-candidate.json` 为 `GO`，所有 release checks 均为 `PASS`，JDBC 136 tests、0 failures、0 errors、0 skipped。更复杂嵌套相关 AST、完整相关 DML AST、真实存储故障注入、外部生产回滚和 FULLTEXT 仍保留边界。
- continuation88 继续收敛 P1-SQL-001/P1-EXE-003：相关 DML 的单赋值和多赋值 UPDATE 现在支持同一赋值表达式内的多个相关标量子查询，按目标主键分别生成 CASE 替换；并行表扫描新增可取消的 `TableChunkReader` 注入点，真实物理行源可以提供分片数据，同时保留确定性分片归并。新增两组真实相关 DML 多子查询回归和并行扫描行源回归；全仓 `go test ./... -count=1` 通过。相关 JOIN/聚合的真实物理行源、复杂相关 AST、真实存储故障注入、外部生产回滚和 FULLTEXT 仍保留边界。
- continuation89 继续收敛 P1-EXE-003：并行 Hash Join 新增 `PlanRowReader` 和显式等值键路径，按真实左右子计划行构建共享哈希表并以有界 worker 分片探测，按分区顺序稳定归并结果；无行源/复杂谓词仍保留旧兼容回退。新增真实子计划行源 Hash Join 回归；全仓 `go test ./... -count=1` 通过。并行聚合真实行源、复杂 JOIN 谓词和更完整相关 AST 仍保留边界。
- continuation90 继续收敛 P1-EXE-003：并行 Sort 新增真实子计划行源路径，按 worker 分片稳定排序并归并，保留取消检查和旧回退路径；新增真实子计划行源排序回归。全仓 `go test ./... -count=1` 通过。并行聚合真实行源、复杂 JOIN 谓词和更完整相关 AST 仍保留边界。
- continuation91 继续收敛 P1-EXE-003：并行 Hash Aggregate 新增真实子计划行源路径，按分组键稳定分片，支持 `COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX` 及 NULL 语义，并按确定性键顺序归并；新增真实聚合行源回归。全仓 Go、集群和 release 门禁待本轮刷新，复杂 JOIN 谓词、更完整物理计划接入和相关 AST 仍保留边界。
- continuation92 继续收敛 P1-OPT-003：增强统计收集器新增 `StorageEngineAccessor` 注入和 CBO 转接入口，`ANALYZE` 列采样在有真实解码记录源时按列序提取实际值，不再用随机模拟值补齐；新增真实记录采样回归，并保持无 accessor 时的兼容估算回退。全仓 `go test ./... -count=1` 通过；生产 accessor wiring、物理统计文件格式、完整存储采样和直方图精度仍保留边界。
- continuation93 继续收敛 P1-EXE-003：并行 Hash Join、Sort、Hash Aggregate 在没有额外 `PlanRowReader` 回调时，会递归执行已并行化的扫描子计划；显式 engine adapter 仍可通过 `PlanRowReader` 保持原始子计划身份。新增三组真实并行计划树回归，验证扫描分片数据进入 Join/Sort/Agg；全仓 `go test ./... -count=1` 通过。复杂 JOIN 谓词、更完整物理计划类型和生产存储 adapter wiring 仍保留边界。
- continuation94 继续收敛 P1-EXE-003：移除并行 Join/Agg/Sort/Scan 的占位分区、合成 row id 和伪聚合回退；缺少真实 `TableChunkReader`、等值键、聚合规格或行生产子节点时改为显式错误，避免把测试占位结果误当成 MySQL 结果。更新结构性回归并保留真实扫描树回归。复杂 JOIN 谓词、更完整物理计划类型和生产存储 adapter wiring 仍保留边界。
- continuation95 继续收敛 P1-OPT-003：修复 `StorageEngineIntegrator` 丢弃存储访问器的问题，新增生产 `StorageAccessor` wiring；它通过表空间映射、表元数据、per-table B+Tree 和 `ClusteredIndexScanner` 解码真实 clustered records，再按确定性采样率提供列统计样本。新增集成层接口/缺失映射回归；索引 ID 级统计映射、全量采样性能和完整 histogram/统计文件格式仍保留边界。
- continuation96 继续收敛 P1-OPT-003：`StorageAccessor.GetIndexCardinality` 与 `GetBTreeStatistics` 现在读取 `IndexManager.GetIndexStats` 的 KeyCount、Height、LeafPages、NonLeafPages，不再对索引统计统一返回 unsupported；integration/plan 专项回归通过。完整跨索引映射、统计文件格式和 histogram fidelity 仍保留边界。
- continuation97 继续收敛 P1-OPT-002/P1-EXE-003：`PhysicalTableScan` 新增 `RequiredColumns`，并行真实 chunk 行源读取完整逻辑行后按大小写不敏感的列名投影；缺失表元数据、未知列或行宽不足均显式报错。新增真实并行扫描列裁剪回归；存储层仍可继续承担更早的 decode-level 列裁剪，其他 alternate row-source wiring 仍保留边界。
- continuation98 继续收敛 P1-OPT-002：`ClusteredIndexScanner` 新增真实 `ScanProjected` 路径，WHERE 仍使用完整解码行做正确性过滤，命中后只保留请求列及其谓词依赖；简单单表 SELECT 存储扫描已接入该路径，复杂 GROUP/HAVING/ORDER/表达式查询保守保留全行扫描。未知投影列和 context 取消均返回显式错误；本轮同时新增 `DecodeClusteredRecordProjected`，遍历并校验全部字段边界但跳过未请求字段的值转换，其他 alternate row-source wiring 仍保留边界。
- continuation99 继续收敛 P1-IDX-001：优化器将 AND 条件生成的索引合并明确标记为 `INDEX_MERGE_INTERSECTION`，选择性改为乘法而非错误的 OR 公式，并保留候选分支；新增确定性 row-id 交集 helper，避免把 OR union 与 AND intersection 混用。通用执行器的 AND 交集接入、类型可排序范围键和更丰富多列交集仍保留边界。
- continuation100/101 回归确认 continuation98/99：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation100/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation101/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation102 继续收敛 P1-IDX-001：单表 SELECT 对多个独立单列二级索引的等值 AND 谓词现在执行真实主键集合交集，按首分支顺序回表并复核原始 WHERE；无足够独立索引时保持原复合前缀/表扫描路径。新增真实持久化表回归，复杂范围交集和更丰富多列交集仍保留边界。
- continuation103/104 回归确认 continuation102：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation103/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation104/release-candidate.json` 为 `GO`，全部 release checks PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation105 继续收敛 P1-OPT-003：增强统计收集器在接入 `StorageEngineAccessor` 时优先使用真实表行数与 data/index space size，再回退到页级估算；新增 accessor 行数和空间大小回归，生产 accessor 的 decoded sample/index/tree statistics 路径保持不变。物理统计文件格式、完整 histogram fidelity 和所有存储变体采样仍保留边界。
- continuation106 继续收敛 P1-OPT-003：增强直方图改为使用真实样本的桶内 distinct，不再用固定比例伪造；数值等宽桶采用半开区间避免相邻边界重复计数，采样缩放后重新平衡桶总量，频率桶按稳定键确定性排序，并对零桶配置、空样本和零总行数安全处理。新增 `TestEnhancedStatisticsHistogramUsesSampleBucketFacts`、`TestEnhancedStatisticsHistogramHandlesEmptyAndZeroBucketConfiguration`。本轮 `go test ./server/innodb/plan -count=1` 与 `go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation106/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation106/release-candidate.json` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。物理统计文件格式和全存储变体采样仍保留边界。
- continuation107 继续收敛 P1-OPT-004：常量单列 `IN` 列表现在展开为多个等值二级索引分支，复用索引合并的主键去重、回表和原始 `IN` 谓词复核；子查询、`NOT IN` 和复杂列表保持保守回退。新增 `TestP1SelectUsesSecondaryIndexForInList`，先行失败后转绿，`go test ./server/innodb/engine -count=1` 通过。
- continuation108 继续收敛 P1-OPT-004：普通单列 `LIKE 'prefix%'` 现在接入二级索引命名空间扫描并在 clustered lookup 后执行精确 LIKE 复核；中间 `%/_`、非前缀通配、FULLTEXT 和复杂表达式保持边界。新增 `TestP1SelectUsesSecondaryIndexForPrefixLike`，先行失败后转绿；`go test ./server/innodb/engine -count=1` 与 `go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation108/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation108/release-candidate.json` 为 `GO`，全部 release checks PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation109 继续收敛 P1-OPT-004：普通单列 `IS NULL` 现在使用持久化 NULL 键表示执行精确二级索引等值范围，并在回表后复核 NULL 语义；`IS NOT NULL`、复杂补集和多列变体保持边界。新增 `TestP1SelectUsesSecondaryIndexForIsNull`，先行失败后转绿；`go test ./server/innodb/engine -count=1` 与 `go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation109/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation109/release-candidate.json` 为 `GO`，全部 release checks PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation110 继续收敛 P1-IDX-001：多个独立单列二级索引的范围 `AND` 谓词现在真实扫描各索引命名空间、求确定性主键交集、回表并复核原始谓词；当前持久化键不是类型可排序编码，因此范围分支仍采用索引命名空间扫描而非伪造有序边界。新增 `TestP1SelectIntersectsIndependentSecondaryIndexRanges`，先行失败后转绿；`go test ./server/innodb/engine -count=1` 与 `go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation110/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation110/release-candidate.json` 为 `GO`，全部 release checks PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation111 继续收敛 P1-OPT-004：普通单列 `IS NOT NULL` 现在接入二级索引命名空间扫描，并在回表后执行精确 NULL 补集过滤；不把补集错误编码为等值范围。新增 `TestP1SelectUsesSecondaryIndexForIsNotNull`，先行失败后转绿；`go test ./server/innodb/engine -count=1` 与 `go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation111/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation111/release-candidate.json` 为 `GO`，全部 release checks PASS，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- continuation112 继续收敛 P1-OPS-001：logical backup 新增版本 2 的 `Statements` 字段与 `ExportWithStatements`，导入仍兼容版本 1；PITR 新增 `RestoreUntilPositionWithStatements`，仅返回完整提交事务内的 schema/data statements 和行变更，并通过 `ReplayStatements` 回调交给 engine 执行。新增 schema 保留、commit position 截断和空 SQL 拒绝回归，`go test ./server/backup -count=1` 通过；物理备份仍保留边界。
- continuation112 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation112/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation112/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation113 继续收敛 P1-SEC-001：新增集中式 MySQL 动态权限注册表及排序快照，认证侧拒绝未知动态权限；GRANT/REVOKE 仅允许已注册动态权限且限定 `*.*` 全局范围；`SHOW PRIVILEGES` 展示静态和注册动态权限。新增注册表、未知检查、未知授权拒绝和展示回归，`go test ./server/common ./server/auth -count=1` 与 `go test ./server/innodb/engine -count=1` 通过；Connector/J/集群全量门禁待本轮结束后补证据。
- continuation113 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation113/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation113/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation114 继续收敛 P1-SQL-003：ALTER TABLE 的 `ALGORITHM`/`LOCK` 选项现在解析并校验 `DEFAULT/COPY/INPLACE/INSTANT` 与 `DEFAULT/NONE/SHARED/EXCLUSIVE`，非法值、格式错误和重复选项会明确拒绝，不再静默忽略；新增 `TestAlterOperationsRejectInvalidAlgorithmAndLockOptions`，聚焦 ALTER 回归通过。该项仍只强化元数据兼容路径，真正 online rebuild/lock scheduling 未宣称完成。
- continuation114 门禁证据：`go test ./server/innodb/engine -count=1` 与 `go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation114/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation114/release-candidate.json` 为 `GO`，全部 release checks `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation115 继续收敛 P1-SEC-001：依据 MySQL 8.4 官方动态权限表校准注册表为 46 个内置权限，补齐 `ALLOW_NONEXISTENT_DEFINER`、`FLUSH_USER_RESOURCES`、`MASKING_DICTIONARIES_ADMIN`、`OPTIMIZE_LOCAL_TABLE`，移除不属于官方 8.4 内置集合的扩展名；新增集合数量、官方边界和未知权限回归，common/auth/engine 专项测试通过。动态权限由组件运行时注册/注销和完整插件生命周期仍未纳入当前范围。
- continuation115 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation115/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation115/release-candidate.json` 为 `GO`，全部 release checks `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation116 继续收敛 P1-SEC-001：全局 `GRANT ALL [PRIVILEGES] ON *.*` 现在展开当时注册的 46 个 MySQL 8.4 动态权限，使 `mysql.global_grants`、`CheckDynamicPrivilege` 和后续 `REVOKE ALL` 语义一致；新增授予 46 项并全部撤销的真实引擎回归，专项测试通过。
- continuation116 门禁证据：`go test ./... -count=1` 与 `go test ./server/innodb/engine -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation116/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation116/release-candidate.json` 为 `GO`，全部 release checks `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation117 继续收敛 P1-SEC-001：`SHOW PRIVILEGES` 补齐官方 8.4 静态权限发现项（包括 `ALTER ROUTINE`、`CREATE ROLE`、`CREATE ROUTINE`、`CREATE TABLESPACE`、`PROXY`、`REPLICATION CLIENT/SLAVE`、`USAGE` 等），并继续展示 46 个注册动态权限；新增静态权限发现回归，engine 专项通过。组件运行时动态注册和插件定义权限仍保留边界。
- continuation117 门禁证据：`go test ./... -count=1` 与 `go test ./server/innodb/engine -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation117/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation117/release-candidate.json` 为 `GO`，全部 release checks `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation118 继续收敛 P1-SEC-001：新增并发安全的 `RegisterDynamicPrivilege`/`UnregisterDynamicPrivilege` API，运行时组件权限名称统一规范化，重复注册幂等；内置 46 项仍作为默认注册集合。新增组件权限注册、注销和幂等回归，common 专项测试通过。
- continuation118 门禁证据：`go test ./... -count=1` 与 `go test ./server/common ./server/auth ./server/innodb/engine -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation118/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation118/release-candidate.json` 为 `GO`，全部 release checks `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation119 继续收敛 P1-OPS-001：新增 `server/backup/physical_backup.go`，提供 XMySQL 自有版本化 `.xmb` 物理快照、清单、SHA-256 校验、原子发布、恢复 staging 和损坏/路径穿越/符号链接拒绝；`XMySQLEngine.CreatePhysicalBackup` 在生成前接入 sharp checkpoint 或存储 flush。新增创建、校验、恢复、同步失败、目标目录保护和符号链接边界回归；官方 MySQL/InnoDB 文件格式互操作及在线 hot backup 仍保留边界。
- continuation119 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation119/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation119/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation120 继续收敛 P1-SQL-003：新增进程内 `tableDDLCoordinator`，让兼容 ALTER 的 `DEFAULT/SHARED/EXCLUSIVE` 写锁与同表 SELECT/DML 的读锁真实协调，`LOCK=NONE` 保持不阻塞；复合 ALTER 增加执行上下文重入计数，避免逐子句处理时同一写锁自死锁。新增锁阻塞、跨表隔离、锁模式和复合 ALTER 回归；完整 MySQL MDL、在线重建/数据搬迁和锁等待诊断仍保留边界。
- continuation120 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation120/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation120/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation121 继续收敛 P1-SQL-003：将表级 DDL 协调拆成读门和写门，`LOCK=SHARED` 现在允许同表 SELECT、阻塞同表 INSERT/UPDATE/DELETE，`LOCK=EXCLUSIVE/DEFAULT` 同时等待读写，`LOCK=NONE` 仍跳过 DDL 门；新增 shared 模式并行回归，engine 专项测试通过。完整 MySQL MDL、在线重建/数据搬迁仍保留边界。
- continuation121 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation121/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation121/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation122 继续收敛 P1-TXN-002：`TransactionManager` 新增可选 `SetUndoPurger` 接线，RR/RC 事务 Begin 自动注册真实 ReadView，Commit/Rollback/超时清理/Close 统一注销，晚绑定 purger 时既有活跃事务也会补注册；新增生命周期回归，manager 专项测试通过。完整 Undo 版本链、跨存储变体 watermark 和 purge 物理压缩仍保留边界。
- continuation122 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation122/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation122/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation123 继续收敛 P1-TXN-002/P1-OPT-003：`SetUndoPurger` 更换或清空时会先从旧 purger 注销全部活跃 ReadView，避免读视图保护残留；`ANALYZE TABLE` 继续兼容写入 `.frm`，同时原子发布带格式版本、schema/table 身份、元数据 SHA-256 指纹、采集时间和校验和的 `.stats.json` sidecar；统计失效会删除 sidecar，加载时对版本、身份、指纹和校验和逐项校验，失败则回退 `.frm` 统计。新增 purger 更换、sidecar 版本字段、损坏回退和 DML 删除 sidecar 回归；完整 undo 版本链、跨存储变体 watermark、全量 storage sampling 和上游物理统计文件互操作仍保留边界。
- continuation123 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation123/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation123/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation124 继续收敛 P1-OPT-004：单列常量 `NOT IN` 现在可进入持久化二级索引命名空间扫描，回表后复用原始 WHERE 做 NULL-aware 精确过滤，避免把安全但无索引的表扫描当作唯一路径；新增 `TestP1SelectUsesSecondaryIndexForNotInWithResidualFiltering`，先行失败后转绿。复合 `NOT IN`、子查询 `NOT IN` 和更丰富的多列补集仍保留边界。
- continuation124 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation124/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation124/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation125 继续收敛 P1-OPT-004：单列 `NOT BETWEEN` 现在进入持久化二级索引命名空间扫描，并在 clustered lookup 后由原始 WHERE 精确复核上下界和 NULL 语义；新增范围回归，先行失败后转绿。复杂表达式、复合列补集和真正类型可排序的范围键仍保留边界。
- continuation125 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation125/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation125/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation126 继续收敛 P1-EXE-003：存储集成 JOIN 的 ON 条件从仅支持等值/AND 扩展为支持 `<`、`<=`、`>`、`>=`、`OR`、`NOT`、`BETWEEN/NOT BETWEEN`、`IS NULL/IS NOT NULL`，并明确普通比较遇到 NULL 时返回不匹配、`<=>` 支持 NULL-safe equality；新增真实 users/labels 表范围与布尔 ON 回归，先行失败后转绿。函数、子查询和更完整物理计划 Join 谓词仍保留边界。
- continuation126 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation126/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation126/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation127 继续收敛 P1-OPT-001：外连接谓词下推的列归属识别新增大小写不敏感的 `table.column` 限定名支持；显式 `LeftSchema/RightSchema` 和缺失 schema 时从左右 TableScan/IndexScan 元数据推导均可工作，保留 NULL 扩展侧与跨表条件的安全边界。新增 qualified-column 外连接计划回归，先行失败后转绿；通用子查询谓词推导和更复杂表达式 AST 仍保留边界。
- continuation127 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation127/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation127/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation128 继续收敛 P1-EXE-003：存储集成 JOIN 的 `ON` 条件值求值接入现有 SQL 标量表达式求值器，支持 `ABS` 等已实现函数以及嵌套算术/括号表达式；当行源以字符串承载数值列时，仅在原始求值需要数值类型且失败后，按 MySQL 兼容规则重试数值字符串转换，不改变正常字符串函数路径。新增真实表 `ABS(u.score) = l.id` JOIN 回归，覆盖函数值求值和字符串数值比较；子查询 JOIN 谓词及更完整物理计划接入仍保留边界。
- continuation128 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation128/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation128/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation129 继续收敛 P1-OPT-004：单列常量 `NOT LIKE` 现在进入二级索引命名空间扫描，并在回表后执行原始 `NOT LIKE` 精确复核；同步修正 `LIKE/NOT LIKE` 遇 NULL 时返回 UNKNOWN、不满足 WHERE 的三值逻辑。新增 `TestP1SelectUsesSecondaryIndexForNotLikeWithResidualFiltering`，先行失败后转绿；复杂表达式、子查询模式和复合列补集仍保留边界。
- continuation129 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation129/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation129/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation130 继续收敛 P1-TXN-002：UndoPurger 的活动快照保护从“事务 ID 小于 high watermark 一律保留”收敛为 `low watermark <= txID < high watermark` 区间；早于所有活动快照 low watermark 的已提交 Undo 段现在可真实回收，仍处于快照可区分区间的段继续保留。新增 `TestUndoPurgerPurgesSegmentOlderThanSnapshotLowWaterMark`，并调整活动事务回归使用处于快照 active-ID 区间的事务，先行失败后转绿。
- continuation130 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation130/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation130/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation131 继续收敛 P1-OPT-003：增强统计收集器在有 `StorageEngineAccessor` 时，使用解码 clustered rows 计算非唯一单列/复合索引的 distinct key cardinality；采样率小于 1 时按样本覆盖率保守缩放并限制到表行数，无法采样时继续回退估算，持久化 index stats 仍优先。新增 `TestEnhancedStatisticsCollectorUsesDecodedRowsForIndexCardinality`，覆盖复合索引真实 distinct key。
- continuation131 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation131/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation131/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation132 继续收敛 P1-OPT-002/P1-EXE-003：并行物理计划新增 `ParallelIndexScan`，支持存储侧 `IndexChunkReader` 分片行源和引擎侧 `PlanRowReader` 原始索引计划行源；索引扫描与表扫描统一按 required columns 投影，并将逻辑裁剪 schema 的列依赖传递到物理 table/index scan。缺少真实行源时显式报错，避免返回空的伪结果；更广泛的异构 row-source、复杂索引范围分片和完整物理计划接入仍保留边界。
- continuation132 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation132/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation132/release-candidate.json` 于 `2026-08-25T02:25:16.0119226Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation133 继续收敛 P1-SQL-002：纯 UNION 拆分器现在显式消费 MySQL 合法的 `UNION DISTINCT` 修饰词，并保持默认 DISTINCT 去重、`UNION ALL` 多重集语义与混合集合优先级不变；新增显式 UNION DISTINCT 重复/非重复分支回归。任意复杂集合表达式与完整查询尾部组合仍保留边界。
- continuation133 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation133/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation133/release-candidate.json` 于 `2026-08-25T02:42:30.7588382Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation134 继续收敛 `CLEAN-P1-01`：删除 `server/innodb/metadata/table.go` 中仅含历史注释冲突模型的死文件，明确 `server/innodb/metadata/schema.go` 为运行时 `metadata.Table` 的唯一实现；metadata/engine 专项回归通过。
- continuation134 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation134/cluster-report.json` 于 `2026-08-25T02:48:40.1865391Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation134/release-candidate.json` 于 `2026-08-25T02:59:37.2954794Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation135 继续收敛 `CLEAN-P1-02`：索引性能监控不再返回固定常量；平均更新时间改为累计真实 DML 二级索引同步耗时除以更新次数，增强 B+Tree 管理器新增原子缓存命中/未命中快照，活跃索引数读取 `IndexManager` 的真实统计，空状态统一返回 0；新增 `TestIndexPerformanceMetricsUseRuntimeCounters`、`TestIndexPerformanceMetricsHaveDefinedEmptyState` 和 `TestRecordIndexUpdateAccumulatesMeasuredDuration`。
- continuation135 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation135/cluster-report.json` 于 `2026-08-25T03:07:38.6691209Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation135/release-candidate.json` 于 `2026-08-25T03:26:23.9175921Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation136 继续收敛 `CLEAN-P1-03`：增强统计收集器在 storage accessor 提供表名解析时，按 `schema.table -> TableStorageInfo.SpaceID` 解析真实表空间；原先仅按表名 hash 的路径降级为没有映射能力的测试/兼容 accessor fallback，新增 `TestEnhancedStatisticsCollectorResolvesTableSpaceThroughStorageAccessor`，避免 ANALYZE/优化器跨表读取错误统计。
- continuation136 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation136/cluster-report.json` 于 `2026-08-25T03:30:37.9229345Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation136/release-candidate.json` 于 `2026-08-25T03:45:42.3262678Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。`CLEAN-P1-03` 中其余无统计时的选择率/成本估算 fallback 仍需继续收敛或明确隔离边界。
- continuation137 继续收敛 `CLEAN-P0-06`：查询权限检查新增统一的 `checkRequiredPrivileges`，对解析出的全部权限逐项调用认证服务，不再只检查第一个权限；新增拒绝第二项所需权限的 dispatcher 回归。默认 session 用户/主机提取、完整 SQL 权限依赖解析和测试/演示会话 shortcut 仍保留边界。
- continuation137 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation137/cluster-report.json` 于 `2026-08-25T03:49:00.6117439Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation137/release-candidate.json` 于 `2026-08-25T03:59:27.4620500Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation138 继续收敛 `CLEAN-P0-06`：旧消息处理路径在 opaque session ID 中缺少用户身份时不再默认 `root`，保持空身份交给认证服务拒绝；新增 `TestExtractUserFromSessionIDDoesNotDefaultToRoot`，与上一轮全权限逐项校验回归共同覆盖认证 shortcut 收口。真实会话参数 wiring、完整 SQL 对象/列权限依赖解析及测试/演示 session shortcut 仍需继续收敛。
- continuation138 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation138/cluster-report.json` 于 `2026-08-25T04:02:08.9924690Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation138/release-candidate.json` 于 `2026-08-25T04:12:54.7716118Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation139 继续收敛 `CLEAN-P0-06`：常见 `SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/DROP` 语句现在提取直接引用表名并传入认证服务，表级授权检查不再因 dispatcher 始终传空 table 而退化为仅库级/全局检查；新增 `TestCheckQueryPrivilegePassesReferencedTableToAuthService`。复杂 JOIN/派生表的完整对象依赖解析、列级授权和剩余 session shortcut 仍保留边界。
- continuation139 门禁证据：`go test ./... -count=1` 通过；`reports/compatibility/p1-cluster-current-continuation139/cluster-report.json` 于 `2026-08-25T04:15:57.6506674Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation139/release-candidate.json` 于 `2026-08-25T04:27:20.4242460Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- continuation140 继续收口代码库 P0 生产边界：集成执行器删除 legacy `DMLExecutor` 的生产初始化，DML 生产 wiring 只保留 `StorageIntegratedDMLExecutor`；系统表 B+Tree 校验改用公共 `basic.BPlusTreeManager` 接口，`DefaultBPlusTreeManager` 进一步隔离为兼容测试实现。同步将 CLEAN-P0-01/02/04/05 更新为已有当前证据的关闭项；CLEAN-P0-03 的字符串谓词/UnifiedExecutor WHERE 边界继续保留并进入下一轮。定向 engine/integration/manager 回归通过。
- continuation141 继续收敛 `P1-OPT-003`：`CostEstimator` 的 JOIN 输出选择率不再固定为 10%，等值 JOIN 按两侧列 NDV 和非空比例估算，范围 JOIN 使用明确保守值，AND/OR 条件按组合语义合并；无列统计时才回退到基于行数的估算。新增 `TestCostEstimatorUsesColumnNDVAndNullStatsForJoinSelectivity`，plan 专项回归通过；完整跨表列映射、直方图 JOIN 相关性和全量存储统计采样仍保留边界。

## Continuation 143

- 检查点元数据继续收口：`CheckpointManager` 通过 `ActiveTransactionProvider` 接入真实事务 ID 快照，引擎启动时绑定 `TransactionManager`；脏页按 `spaceID` 聚合为表空间摘要，活跃事务按 ID 排序，未配置 provider 时保持明确空集合。
- 新增 `TransactionManager.GetActiveTransactionIDs`，返回脱离内部状态的稳定快照。
- 修复该接入引入的自死锁：`WriteCheckpoint` 持有检查点锁时，provider 快照使用独立的 `activeTxnMutex`，不再对同一 `RWMutex` 重入读锁。带 30 秒超时的专项测试曾稳定复现并输出堆栈，修复后 `TestCheckpointManager*` 全部通过。
- 通过：`go test ./server/innodb/engine -run '^TestCheckpointManager' -timeout 30s -count=1 -v`、`go test ./server/innodb/manager -run 'Test(Transaction|LongTransaction|Undo)' -count=1`。

- continuation143 门禁证据：`go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation143/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation143/release-candidate.json` 于 `2026-08-25T05:15:36.5246086Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 144

- 检查点校验和从“仅使用 LSN”改为对完整逻辑 payload（排除 checksum 字段本身）计算 CRC32，覆盖表空间、活跃事务、脏页、WAL 等元数据变化。
- `ReadLatestCheckpoint`、`ReadCheckpointByLSN`、`ListCheckpoints`、启动加载路径统一校验 checksum，损坏或被篡改的检查点不会继续作为有效恢复元数据返回。
- 新增 `TestCheckpointChecksumCoversPayload`；`TestCheckpointManager*` 与 checkpoint 持久化回归通过。

## Continuation 145

- `BufferPage` 新增真实 `modifyCount` 生命周期计数：`MarkDirty`、dirty 状态转换和 `Reset` 均维护计数，checkpoint 的 `DirtyPageInfo.ModifyCount` 不再固定为 1（对未提供计数的旧页面保留最小兼容值 1）。
- 新增 `TestBufferPageTracksModifyCountAcrossDirtyTransitions`；buffer pool 与 checkpoint 专项回归通过。
- continuation145 门禁证据：`go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation145/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation145/release-candidate.json` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 146

- WAL 条目校验和从“仅使用 Data 长度”改为对完整条目 payload（排除 checksum 字段本身）计算 CRC32；读取端与写入端统一算法，等长度数据篡改也会被拒绝。
- 新增 `TestWALChecksumCoversEntryPayload`；WAL/持久化专项回归通过。Connector/J 与全量门禁将在本轮 WAL 改动后再次执行。
- continuation146 门禁证据：`go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation146/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation146/release-candidate.json` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
## Continuation 142

- 收口 `server/innodb/storage/io/io_optimizer.go` 的真实 I/O 边界：新增 `PageIO` 后端契约，移除 `doRead`/`doWrite` 的零页/空成功模拟；批量写入通过真实后端逐页提交并保留失败项；读缓存、写缓存、预读缓存统一按 `(spaceID,pageNo)` 隔离；`Stop` 改为幂等。
- 新增 `server/innodb/storage/io/io_optimizer_compatibility_test.go`，验证后端读写、跨表空间同页号隔离、批量写入持久化、未配置后端时显式失败。
- 通过：`go test ./server/innodb/storage/io -count=1`。`go test -race` 在当前环境因 CGO 未启用无法运行，需在启用 CGO 的环境补跑。
- P1-06 仍有明确边界：legacy `page_impl.go` 与若干 deprecated wrapper 仍需迁移到 buffer pool/具体 storage provider；本次没有把“错误的空成功”继续保留在 IO 优化器中。

## Continuation 147

- 继续收敛 `P1-OPT-003`：`StorageEngineIntegrator` 的 ANALYZE 与索引优化统计统一走真实 `StorageEngineAccessor`，优先使用解码行数、真实 data/index space size 和可用的表修改计数；只有访问器不可用或读取失败时才回退到页级估算，避免两条生产路径观察到不同统计口径。
- 新增 `TestStorageEngineIntegratorUsesAuthoritativeStatistics`，覆盖真实 accessor 行数、空间大小和修改计数的优先级。
- 继续收敛 Checkpoint/flush 运行时占位：`BufferPage` 维护可读的最后访问时间，CheckpointManager 与 AdaptiveFlushStrategy 的访问/年龄得分不再固定为 `0.5` 或使用固定最大 LSN 代理，而是基于真实页面访问时间计算冷度；未记录访问时间的页面按最冷处理，保证脏页不会无限滞留。
- 通过：`go test ./server/innodb/integration ./server/innodb/plan -count=1`、`go test ./server/innodb/buffer_pool ./server/innodb/engine -count=1`、`go test ./... -count=1 -timeout 5m`。
- continuation147 门禁证据：`reports/compatibility/p1-cluster-current-continuation147/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation147/release-candidate.json` 于 `2026-08-25T06:04:18.3033525Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- 仍明确保留的范围边界：FULLTEXT、全量非 Connector/J 客户端矩阵、legacy/deprecated storage wrapper 迁移、无真实 accessor 时的统计 fallback、复杂完整 MySQL 语义和真实生产环境故障注入/部署验收。

## Continuation 148

- 继续收敛空间管理真实数据：`SpaceExpansionManager` 使用真实 extent/page capacity 计算表空间总量和使用率，不再固定返回 `0`；`providerSpace` 从 `StorageProvider.GetSpaceInfo` 暴露真实页数、extent 数和已用字节。
- 无 decoded row source 的统计兼容路径不再随机生成列值，也不再把伪造值写入 NDV/直方图；生产 `StorageEngineAccessor.SampleTableRecords` 路径继续使用真实 clustered record 解码。
- 删除无引用且明确标注“未实现”的废弃 `storage/store/mvcc/trx_sys.go`，真实事务系统统一由 `manager/transaction_manager.go` 提供。
- 新增空间扩容/provider adapter/无伪造统计值回归测试；`go test ./... -count=1 -timeout 5m` 与 continuation148 release gate 均通过。

## Continuation 149

- 对包含 continuation148 改动的当前工作树重新执行完整验证：全仓 `go test ./... -count=1 -timeout 5m` 通过，集群 smoke `PASS`。
- `reports/compatibility/release-candidate-current-continuation149/release-candidate.json` 为 `GO`：build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`；Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 当前已验证的是 MySQL 核心可运行能力、集群复制/提升 smoke、Connector/J P0 门禁；FULLTEXT、全量非 Connector/J 客户端矩阵和剩余复杂 MySQL 语义仍按用户要求后置，不能将本项目表述为已完成全部 MySQL 8.4 兼容性。

## Continuation 150

- 继续收敛 legacy storage wrapper：旧 `BlobPage` 的 `Read`/`Write` 委托真实 `BasePageWrapper`，不再空成功；`PageImpl` 新增 `NewPageWithStorage`，有 provider 时真实读写页，无 provider 时返回明确 `ErrPageStorageUnavailable`。
- 修复 `EnhancedBTreeIndex.GetFirstLeafPage` 的内部页导航：从内部记录的 child-page value 解析真实 little-endian page number，不再使用 `pageNo + 1` 猜测。
- BLOB 元数据 `Created` 使用真实 Unix 时间戳；新增对应 wrapper、child-pointer 和 provider 缺失边界回归。
- continuation150 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation150/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation150/release-candidate.json` 于 `2026-08-25T06:46:00.2078615Z` 为 `GO`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。
- 剩余边界继续明确：FULLTEXT、全量非 Connector/J 客户端、ZSTD 原生压缩/完整物理压缩格式、复杂 MySQL 8.4 语义以及真实生产部署/故障注入验收仍未宣称完成。

## Continuation 151

- 继续收敛 `P1-OPT-003`：`StorageEngineIntegrator` 的索引下推统计现在覆盖使用 EnhancedStatisticsCollector 中的真实/持久化表、索引和列统计，使 IndexPushdownOptimizer 与 CostEstimator 使用同一统计来源；统计提供者缺失时返回明确错误而不是空指针崩溃。
- 继续收敛 `CLEAN-P1-03`：`InfoSchemaManager.UpdateTableStats` 新增可插拔 durable persister；引擎将 `ANALYZE TABLE` 统计更新接入 `.stats.json` 持久化，缓存更新和重启加载保持同一数据边界。
- 继续收敛 `CLEAN-P0-06`：Enhanced dispatcher 为 opaque session ID 保存认证成功后的用户、主机、数据库和 active roles；后续查询/USE DB 优先使用认证绑定身份，断开时清理，避免依赖可伪造的 session ID 文本推断权限主体。
- 新增统计持久化和认证身份绑定回归；`go test ./... -count=1 -timeout 5m` 通过，`reports/compatibility/p1-cluster-current-continuation151/cluster-report.json` 为 `PASS`。
- continuation151 release gate：`reports/compatibility/release-candidate-current-continuation151/release-candidate.json` 于 `2026-08-25T07:09:31.4003069Z` 为 `GO`；build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称全量 MySQL 8.4 兼容：FULLTEXT、全量非 Connector/J 客户端、ZSTD 原生压缩/完整物理压缩格式、复杂 AST/类型/collation 语义，以及真实生产部署、故障注入和 race-enabled 验收仍需后续逐项收敛。

## Continuation 152

- 继续收敛 legacy B+Tree 适配：`EnhancedBTreeIndex.SimpleRow` 不再把比较、主键、行头字段、列值和序列化辅助方法实现为空操作；行头的 `n_owned`、next offset、heap number 以及事务 ID 现在可读写，适配页插入路径不再得到伪造空值。
- `IndexRecordRowAdapter.Less` 改为按真实索引主键字节序比较，避免增强索引适配器在需要排序时所有记录都被判定为相等。
- 新增 `SimpleRow` 行头/值回归和 `IndexRecordRowAdapter` 排序回归；`go test ./... -count=1 -timeout 5m` 通过，`reports/compatibility/p1-cluster-current-continuation152/cluster-report.json` 为 `PASS`。
- continuation152 release gate：`reports/compatibility/release-candidate-current-continuation152/release-candidate.json` 于 `2026-08-25T07:24:33.6330995Z` 为 `GO`；build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 仍保留明确后续项：FULLTEXT、全量非 Connector/J 客户端、ZSTD 原生压缩/完整物理压缩格式、其他 deprecated wrapper 的 provider 迁移，以及复杂 MySQL 8.4 语义和真实生产部署/故障注入验收。

## Continuation 153

- 关闭 `CLEAN-P0-03`：UnifiedExecutor 的无 optimizer 选择路径不再拒绝 `WHERE`，而是将解析后的 `sqlparser.Expr` 接入已有 `FilterOperator`；每行仅解析 AST 引用列并按 `basic.Value` 类型转换为数值、布尔或字符串后执行 `evalPredicate`，保留 NULL/三值逻辑，不再按 SQL 字符串或模拟偶数行过滤。
- 新增 `TestUnifiedPredicateValuesUsesParsedColumnsAndNulls`，并将旧的 “WHERE 必须失败” 回归改为验证 FilterOperator 构建；`CODEBASE_CLEANUP_DEVELOPMENT_CHECKLIST_20260715.md` 中 `CLEAN-P0-03` 已标记为已关闭。
- continuation153 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation153/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation153/release-candidate.json` 于 `2026-08-25T07:40:57.5749859Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。
- 剩余边界继续明确：FULLTEXT、全量非 Connector/J 客户端、ZSTD 原生压缩/完整物理压缩格式、其他 deprecated wrapper 的 provider 迁移，以及复杂 MySQL 8.4 语义和真实生产部署/故障注入验收仍未完成。

## Continuation 154

- 继续收敛 deprecated page wrapper 迁移：`BasePageWrapper` 新增 provider-backed 构造入口，配置 `basic.StorageProvider` 后，`Read` 会从真实 `(spaceID,pageNo)` 页面后端加载并解析页面头尾，`Write/Flush` 会把序列化页面提交到 provider；既有无 provider 的纯内存兼容调用保持不变。
- 新增 `page_wrapper_storage_test.go`，覆盖 provider 持久化读回、页面统计更新以及短页显式拒绝；`go test ./server/innodb/storage/wrapper/page -count=1` 通过。
- 本轮只完成 deprecated wrapper 的可注入真实 I/O 边界，不宣称已迁移所有旧构造调用；仍需继续处理其它 wrapper 的 provider wiring、完整物理压缩格式、复杂 MySQL 8.4 语义、FULLTEXT、全量非 Connector/J 客户端以及真实生产部署/故障注入验收。
- continuation154 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation154/cluster-report.json` 于 `2026-08-25T07:47:27.9918290Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation154/release-candidate.json` 于 `2026-08-25T07:57:48.9005404Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。

## Continuation 155

- 继续消除内存分配页的伪成功边界：`Allocated` 页面包装器现在保留稳定的 `PageStats`，`Read`/`Write`/`Flush` 明确表示内存页状态刷新，并更新访问、读写、脏页和状态信息；持久化仍由外层 page allocator/storage manager 负责，不把该 wrapper 误报为磁盘提交。
- `ToBytes`/`ToByte` 现在在读锁下生成一致快照；新增 `page_allocated_wrapper_test.go` 覆盖序列化、解析、内存读写状态和统计。
- 定向 page wrapper 回归通过；全仓、集群和 release candidate 需在本轮改动后重新执行。
- continuation155 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation155/cluster-report.json` 于 `2026-08-25T08:00:33.4121785Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation155/release-candidate.json` 于 `2026-08-25T08:10:51.7028574Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J 136 tests、0 failures、0 errors、0 skipped。

## Continuation 156

- 继续收敛 `P1-OPT-003`：EnhancedStatisticsCollector 在 `SampleRate=1` 时现在把精确 ANALYZE 请求传递为全量 decoded-record 采样，并取消 AdaptiveSampler 的 50,000 行截断；统计直方图、NDV、NULL 比例和 min/max 能在该模式下覆盖 accessor 返回的全部记录。
- 补充 `TestEnhancedStatisticsCollectorExactModeDoesNotTruncateDecodedRows`，使用 50,001 条记录验证 exact mode 不再截断；plan 专项测试通过。
- 继续收敛 legacy B+Tree 适配：`DefaultBPlusTreeManager.RangeSearch` 创建 `RecordRowAdapter` 时携带真实叶子页号，`GetPageNumber` 不再固定返回 0；新增适配器回归。
- accessor-only 统计路径也已统一：即使没有 `SpaceManager`，`CollectTableStatistics` 现在优先使用 accessor 的真实行数和空间大小，再由同一真实行数计算列 NDV、NULL/非 NULL 计数；新增 `TestEnhancedStatisticsCollectorUsesAccessorWithoutSpaceManager`，修复了表级伪行数与列级真实样本混用的问题。

## Continuation 157

- 继续收敛 `P1-OPT-003`：`CostEstimator.EstimateTableScanCost` 现在优先使用统计采集得到的 `AvgRowLength/DataLength`，并在 `DataLength` 可用时按实测数据页数计算顺序扫描 I/O；无实测字段时继续按列元数据降级估算。新增 `TestCostEstimatorUsesMeasuredTableSizeForTableScan`。
- 索引扫描代价现在使用 `IndexStats.LeafPages` 和 `ClusterFactor` 修正索引页读取与离散回表代价，不再只按固定 seek 成本和选择率估算；新增 `TestCostEstimatorUsesMeasuredIndexPagesAndClusterFactor`。
- 生产 `StorageAccessor` 新增可选逻辑索引名到物理 index ID 的解析；EnhancedStatisticsCollector 在可解析时读取实时 `GetIndexCardinality/GetBTreeStatistics`，将真实基数、树高、叶子页和非叶子页带入优化器统计，无法解析时保留兼容降级。新增 `TestEnhancedStatisticsCollectorUsesLiveIndexStatistics`。
- `StorageEngineIntegrator.updateOptimizerStatistics` 现在主动采集 EnhancedStatisticsCollector 的表/索引/列统计后再覆盖兼容估算，避免生产路径只消费旧缓存或先写入伪造统计。plan/integration 定向回归通过；全仓、集群和 release candidate 门禁待本轮结束后补证据。
- continuation157 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation157/cluster-report.json` 于 `2026-08-25T08:28:20.7069713Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation157/release-candidate.json` 于 `2026-08-25T08:38:39.8898983Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 158

- 继续收敛 `P1-OPT-003`：`StorageEngineIntegrator` 的无索引候选全表扫描不再仅按 WHERE 条件数量固定估算选择率；当 EnhancedStatisticsCollector 有列 NDV、非空计数和表行数时，等值/不等值/范围/LIKE/IN 以及 AND/OR 组合会使用这些统计计算选择率，缺少统计时仍保守回退到 10%。新增 `TestStorageEngineIntegratorUsesColumnStatisticsForTableSelectivity`；integration 定向回归通过。
- 本轮修改完成后需重新执行全仓、集群和 release candidate 门禁，当前尚不能沿用 continuation157 的门禁作为 continuation158 的最终证据。
- continuation158 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation158/cluster-report.json` 于 `2026-08-25T08:43:05.5070733Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation158/release-candidate.json` 于 `2026-08-25T08:53:20.6934747Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 159

- 收口 P4 页面压缩的真实算法边界：`CompressionManager` 的 ZSTD 分支不再回退到 zlib，改为使用 `github.com/klauspost/compress/zstd v1.17.11` 生成/解析标准 ZSTD frame，同时保留现有页面大小、压缩阈值、缓存和统计语义。
- 新增 `TestCompressionManagerZSTDUsesZSTDFrameAndRoundTrips`，验证标准 ZSTD magic header 和 16KB 页面 round-trip；先行失败（旧实现输出 zlib `78 5e`）后转绿。全仓、集群和 release candidate 门禁待本轮改动后补证据。
- continuation159 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation159/cluster-report.json` 于 `2026-08-25T08:57:14.7146714Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation159/release-candidate.json` 于 `2026-08-25T09:07:21.7125668Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 160

- 继续收敛 legacy/增强 B+Tree adapter：`EnhancedBTreeIndex.addRecordToIndexPage` 创建临时 `SimpleRow` 时现在从目标 `IIndexPage.GetPageNo()` 传递真实页号，`SimpleRow.GetPageNumber()` 不再固定返回 0；新增 `TestSimpleRowMaintainsRowHeaderAndValues` 的页号断言，manager 定向回归通过。
- 当前计划中的 ZSTD 原生压缩边界已在 continuation159 收口；后续边界不再把 ZSTD 列为未完成项。FULLTEXT、全量非 Connector/J 客户端、其他 deprecated wrapper 的完整 provider 迁移、复杂 MySQL 8.4 语义、真实生产部署/故障注入和 race-enabled 验收仍未完成。
- continuation160 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation160/cluster-report.json` 于 `2026-08-25T09:14:26.6897523Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation160/release-candidate.json` 于 `2026-08-25T09:24:36.3458569Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 161

- 收敛加密管理器的错误安全边界：AES-CBC 解密现在拒绝空/非块长密文，并严格校验 PKCS#7 填充，非法输入返回 `ErrInvalidCiphertext` 而不是 panic 或截断数据；新增二进制、空页、非法密文、非法填充和密钥封装回归。
- 收敛校验算法的真实实现：`ChecksumCalculator` 的 xxHash 分支使用依赖中的 xxHash-32，SHA-256 分支返回摘要前 32 位；`ParallelCalculate` 改为流式处理分块，保证 CRC32/CRC32C/xxHash/SHA-256 与整段计算一致，并处理非正 chunk size。
- 本轮仍不宣称完成“加密 at rest”：密钥持久化、主密钥包裹、页存储读写接入和恢复/轮换流程仍需单独设计；FULLTEXT、全量非 Connector/J 客户端及其他复杂 MySQL 8.4 语义边界不变。
- continuation161 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation161/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation161/release-candidate.json` 于 `2026-08-25T09:39:57Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，JDBC 检查 `test_count=136` 且通过。

## Continuation 162

- 关闭一个真实的 P1 storage wiring 缺口：`StorageManager.GetPageManager()` 不再固定返回 `nil`，新增 `storagePageManagerAdapter`，将 Insert Buffer 等 legacy consumer 的页面读写接到同一 `StorageProviderAdapter`/优化 buffer pool 路径；`FreePage/FlushPage` 在 buffer pool 未初始化时显式报错，旧式无语义的 leaf/key/typed allocation 入口不再静默成功。
- 新增 `TestStorageManagerExposesPageManagerAdapter`，验证初始化后的 page manager 非空且未初始化存储时不会伪造成功；engine/manager 定向回归通过。
- continuation162 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation162/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation162/release-candidate.json` 于 `2026-08-25T09:56:17Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 JDBC 检查全部 `PASS`。

## Continuation 163

- 继续收口页生命周期：`StorageProviderAdapter.FreePage` 不再静默成功，优先调用 tablespace 的可选真实 `FreePage` 能力；`IBDSpace.FreePage` 现在通过 owning extent 更新 bitmap、extent 状态和 page count，`StorageManager.FreePage` 也优先走优化 buffer pool 的真实释放路径；不支持单页回收的 provider 返回明确错误。
- 新增 `TestIBDSpaceFreePageUpdatesExtentAllocation`，覆盖释放成功和重复释放拒绝。
- continuation163 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation163/cluster-report.json` 于 `2026-08-25T10:00:00.2407453Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation163/release-candidate.json` 于 `2026-08-25T10:09:58Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 JDBC 检查全部 `PASS`。
- race 说明：本机默认 `CGO_ENABLED=0`，显式开启 race 后因环境缺少 `gcc` 无法构建；这属于验证环境缺口，不作为本轮功能测试失败，但 race-enabled acceptance 仍保留为发布前边界。

## Continuation 164

- 继续收敛空间分配：`IBDSpace.AllocatePage` 现在只从 page-level allocator 自己登记的 extent 空闲 bitmap 分配，全部已满时才创建新 extent；系统/segment 预留 extent 不会被误当成可复用页。`StorageProviderAdapter.AllocatePage` 和 `providerSpace` 会优先调用真实的 page allocator，不再每次请求都扩一个新 extent。
- 新增 `TestIBDSpaceAllocatesPagesFromExistingExtent`，验证连续分配复用同一 extent 的相邻页。
- 根因回归：初版复用逻辑误用了系统/segment extent 的空闲 bitmap，导致 B+Tree leaf chain 丢失；现已限制复用范围，leaf-chain、RepairIndex 和 page-reuse 回归重新转绿。
- continuation164 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation164/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation164/release-candidate.json` 于 `2026-08-25T10:26:42` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`。

## Continuation 165

- 继续收敛 P1 storage wrapper migration：`types.BasePageWrapper` 新增 provider-backed 构造入口；配置真实 `StorageProvider` 后，`Read`/`Write`/`Flush` 会读写页面、校验最小页面大小、更新状态/统计并调用空间 `Sync`，不再把实际持久化伪装成成功的内存操作；原有无 provider 构造继续保持兼容。
- 新增 `server/innodb/storage/wrapper/types/page_wrapper_storage_test.go`，覆盖真实读回、短页拒绝和 dirty page Flush/Sync；types wrapper 专项及 wrapper/manager 回归通过。
- specialized legacy page wrappers 的 provider/buffer-pool migration、FULLTEXT、全量非 Connector/J 客户端、完整加密 at-rest、复杂 MySQL 8.4 语义和生产级故障注入仍需继续推进；本轮全仓、集群和 release candidate 门禁待补。
- continuation165 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation165/cluster-report.json` 于 `2026-08-25T10:32:50.7603017Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation165/release-candidate.json` 于 `2026-08-25T10:42:53.9594773Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 JDBC 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 166

- 继续推进 specialized legacy wrapper migration：FSP、XDES、IBuf bitmap 新增 provider-backed 构造入口；无 buffer pool 时 `Read`/`Write` 直接走 `StorageProvider`，短页明确报错，避免原先无 buffer pool 时读失败或写入静默成功。
- 修复专用 wrapper 解析的真实锁缺陷：FSP/XDES/IBuf bitmap/IBuf free-list/Blob/Encrypted/Compressed/TrxSys 等先持有基础写锁再调用基础解析的路径，统一改用已持锁的内部解析函数，消除不可重入 RWMutex 死锁。
- 新增 `TestSpecializedPageWrappersUseStorageProvider`，覆盖 FSP/XDES/IBuf bitmap 的 provider round-trip；wrapper/manager 定向回归通过。本轮全仓、集群和 release candidate 门禁待补。
- continuation166 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation166/cluster-report.json` 于 `2026-08-25T10:48:19Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation166/release-candidate.json` 于 `2026-08-25T10:58:21Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 JDBC 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 167

- 收敛 `PX-LOG-001` 的生命周期边界：`LogArchiver.Stop` 现在幂等且无论是否调用过 `Start` 都会关闭当前文件；关闭后 `Write` 返回明确错误，`Start` 不会重新激活已关闭的归档器，后台归档也不会在关闭后继续处理。
- 新增 `log_archiver_test.go`，覆盖未启动 Stop/重复 Stop、关闭后写入拒绝、日志轮转和 gzip 归档内容校验；manager 定向回归通过。
- 主 `RedoLogManager` 与归档流的接入、PITR 保留策略仍未完成；FULLTEXT、全量非 Connector/J 客户端、完整加密 at-rest、复杂 MySQL 8.4 语义和生产级故障注入边界继续保留。
- continuation167 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation167/cluster-report.json` 于 `2026-08-25T11:02:18Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation167/release-candidate.json` 于 `2026-08-25T11:12:46Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 JDBC 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 168

- 收敛 `PX-LOG-001` 的主链路：`RedoLogManager.EnableLogArchival` 以 opt-in 方式把每批已 flush 的二进制 redo 镜像到 `LogArchiver`，同时保留 `redo.log` 作为 CrashRecovery 权威来源；关闭流程会同时安全停止归档器和主日志文件。
- 新增 `redo_log_archival_test.go`，验证同一批 redo 在主日志和归档流均落盘；manager 定向回归通过。本轮改动后的全仓、集群和 release candidate 门禁待补。

## Continuation 169

- continuation168 的 release candidate 曾因 `TestEncryptionManagerRejectsInvalidPKCSPadding` 的随机测试扰动返回 NO-GO；该测试原先对单块 CBC 密文随机翻转末字节，存在偶然形成合法 padding 的概率。现改为两块明文并翻转第一块对应 padding 的字节，非法 padding 验证确定且重复运行稳定。
- 定向加密回归重复 20 次通过；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation169 门禁证据：`reports/compatibility/p1-cluster-current-continuation169/cluster-report.json` 于 `2026-08-25T11:30:46Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation169/release-candidate.json` 于 `2026-08-25T11:42:02Z` 为 `GO`，所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 170

- 继续收口 P1 specialized wrapper provider migration：Blob、Compressed、IBuf free-list、Undo、TrxSys、Encrypted 新增 provider-backed 构造入口；后三类原先只在 buffer pool 存在时真实 I/O 的读写回退现在直接使用 `StorageProvider`。
- Undo provider 写入会把短序列化结果补齐到标准 16KB 页，并在 provider 模式下即使 wrapper 尚未标脏也真正提交页面，避免“Write 成功但没有落盘”。
- 基础解析锁边界继续复用 continuation166 的已持锁路径；新增 `TestRemainingLegacyPageWrappersUseStorageProvider`，六类 wrapper round-trip 及 wrapper/manager 回归通过。本轮全仓、集群和 release candidate 门禁待补。
- continuation170 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation170/cluster-report.json` 于 `2026-08-25T11:47:42Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation170/release-candidate.json` 于 `2026-08-25T11:58:05Z` 为 `GO`，所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 171

- 继续收口 P1-06 system wrapper provider migration：`UnifiedPage` 新增可注入 `StorageProvider` 的构造与真实 `Read`/`Write` 路径，`Flush` 仍保持现有兼容语义；`wrapper.BasePage` 与 `system.BaseSystemPage` 增加 provider 构造入口，FSP、XDES、IBuf、Dict、Trx 五类系统页均可复用该持久化链路。
- 新增 `server/innodb/storage/wrapper/system/storage_provider_test.go`，验证五类系统页写入标准 16KB 页面后，由新实例从 provider 读回；保留原有无 provider 构造入口以兼容历史调用。
- `go test ./server/innodb/storage/wrapper/... -count=1 -timeout 5m` 与全仓 `go test ./... -count=1 -timeout 5m` 已通过；集群 smoke 已生成 continuation171 报告，release candidate 门禁正在执行，结果待补。

## Continuation 172

- 补齐 `UnifiedPage.Flush` 的 provider 持久化语义：真实 provider 模式下先写页，再调用 `StorageProvider.Sync(spaceID)`，Sync 失败会向上返回；系统页回归改为验证 Flush 后的 16KB 页面可读回且 Sync 被调用。
- continuation172 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation172/cluster-report.json` 于 `2026-08-25T12:20:13Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation172/release-candidate.json` 于 `2026-08-25T12:30:32Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 和 Connector/J 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 173

- 继续收口 P1-06 legacy page I/O：`page.BasePage`、`types.BasePage`、`InodePageWrapper` 和旧 `IndexPage` 新增 provider-backed 构造及真实读写；Inode 的 Read/Write/ToBytes 锁边界改为已持锁内部路径，避免基础 RWMutex 重入死锁；旧 IndexPage 解析会忽略 16KB 页尾零填充，不再把不足一条记录的填充误报为 `invalid entry size`。
- 新增 provider round-trip 回归覆盖上述 wrapper；wrapper 全量和全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation173 门禁证据：`reports/compatibility/p1-cluster-current-continuation173/cluster-report.json` 于 `2026-08-25T12:37:51Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation173/release-candidate.json` 于 `2026-08-25T12:48:13Z` 为 `GO`，所有 checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 174

- 收口 `PX-LOG-001` 的 PITR 保留边界：`LogArchiver` 新增 `retentionAge` 默认策略与 `SetRetentionPolicy(maxFiles, maxAge)`；清理流程先删除超过时间窗口的 gzip 归档，再按文件数保留上限删除最老文件，并累计删除统计。
- 新增 `TestLogArchiverRetentionPolicyDeletesExpiredArchives`，验证过期归档删除、未过期归档保留；manager 定向回归和全仓回归通过。
- continuation174 门禁证据：`reports/compatibility/p1-cluster-current-continuation174/cluster-report.json` 于 `2026-08-25T12:52:07Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation174/release-candidate.json` 于 `2026-08-25T13:02:27Z` 为 `GO`，所有 checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 175

- 收口加密能力的一部分持久化边界：`EncryptionManager` 新增主密钥保护的 AES-GCM keyring `SaveKeyring`/`LoadKeyring`，带版本、AAD、随机 nonce、完整性校验、临时文件同步和发布；加载后可恢复表空间密钥，错误主密钥或损坏 keyring 会拒绝，磁盘文件不包含明文 key/IV。
- 新增 `TestEncryptionManagerPersistsEncryptedKeyring`；manager 定向回归和全仓回归通过。
- continuation175 门禁证据：`reports/compatibility/p1-cluster-current-continuation175/cluster-report.json` 于 `2026-08-25T13:06:17Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation175/release-candidate.json` 于 `2026-08-25T13:16:42Z` 为 `GO`，所有 checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 176

- 继续收口 `PX-STG-002` 的真实接入：新增 `EncryptedStorageProvider`，对显式加密表空间执行固定页长 AES-CBC 读写，缺少密钥时 fail-closed，所有非加密表空间仍委托原 Provider。
- 加入 keyring 重启构造 `NewEncryptedStorageProviderFromKeyring`、已有页全量 `ReencryptSpace`、失败回滚旧密文和 `StorageManager` master-key 自动 Provider wiring；新建 tablespace 会创建表空间密钥、先完成页加密再进入后续 segment 初始化，并在关闭时持久化 keyring。`EncryptedSpaceManager` 同时覆盖直接 `Space`/`FileTableSpace` 读写，启动扫描会恢复文件页分配并按平台无关的路径名恢复系统表空间 ID。
- 新增 `encrypted_storage_provider_compatibility_test.go` 与 `storage_manager_encryption_compatibility_test.go`，覆盖密文不落明文、固定页长、缺钥拒绝、keyring 重启、密钥轮换、StorageManager 自动接入和新表空间加密。
- 当前仍保留真实边界：原生加密页头/认证完整性格式，以及 FULLTEXT、全量非 Connector/J 客户端矩阵、复杂 MySQL 8.4 语义和真实生产部署/故障注入验收。

## Continuation 177

- 继续收口 legacy storage wrapper：`MVCCIndexPage` 新增可选 `StorageProvider` 构造；provider 模式下 `Read`/`Write` 直接持久化标准页面字节并在重载实例中恢复 MVCC 元数据，原有内存构造保持兼容。
- `SpaceManagerImpl` 新增稳定的可选 `ListSpaceIDs` 能力；启用 master key 后，`EncryptedSpaceManager.MigrateExistingSpaces` 会在初始化完成后发现并迁移启用加密前已经存在的明文 tablespace，已在 keyring 中的空间只重新挂接策略，不重复重写。
- 新增 `TestMVCCIndexPageUsesStorageProvider` 与 `TestStorageManagerMigratesPreKeyringPlaintextTablespace`；MVCC、全部 wrapper 包、manager 全量专项通过。
- 本轮仍保留真实边界：原生加密页头/认证完整性格式、FULLTEXT、全量非 Connector/J 客户端矩阵，以及复杂 MySQL 8.4 语义和真实生产部署/故障注入验收。
- continuation177 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation177/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation177/release-candidate.json` 于 `2026-08-25T14:09:19Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部通过，Connector/J `test_count=136`、0 failures、0 errors、0 skipped；`git diff --check` 无空白错误。

## Continuation 178

- 收口 buffer pool 的真实键一致性：legacy `LRUCacheImpl` 与 optimized `OptimizedLRUCache` 不再把 `(spaceID,pageNo)` 的拼接结果交给有碰撞可能的 hash 作为唯一身份，统一采用无损的 `uint64(spaceID)<<32 | pageNo` 编码；读、写、删除、分代迁移和回调继续复用原接口。
- 新增 `TestLRUPageKeyUsesCollisionFreeSpaceAndPageEncoding`，覆盖零值、最大值、不同空间/页坐标和 optimized LRU 接线；buffer_pool 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation178 门禁证据：`reports/compatibility/p1-cluster-current-continuation178/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation178/release-candidate.json` 于 `2026-08-25T14:27:40Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部通过，Connector/J `test_count=136`、0 failures、0 errors、0 skipped；`git diff --check` 无空白错误。

## Continuation 179

- 收口 legacy LRU 生命周期：`LRUCacheImpl.Purge` 现在在通知 visitor 后清空三层 map 和链表，避免关闭/复用 buffer pool 后继续读到已清理页面；新增 `TestLegacyLRUPurgeClearsEntries`。
- continuation179 的第一次发布候选门禁曾为 NO-GO，但唯一失败是 `TestParallelIndexScanUsesRealRowsAndProjectsRequiredColumns` 中测试自身对并发 `readCalls++` 的数据竞争；integration、go-core、crash-recovery、并发、observability 与 JDBC 136/136 均已通过。现已用 `sync/atomic` 修正测试计数器，plan 专项重复 20 次和全量 plan 回归通过，待重新生成发布门禁。

## Continuation 180

- 修复并行索引扫描兼容测试的真实数据竞争：两个并行 chunk worker 对共享 `readCalls` 使用非原子自增，曾造成仅在发布门禁负载下偶发计数丢失；现改为 `sync/atomic` 计数，不改变执行器行为。
- continuation180 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/release-candidate-current-continuation180/release-candidate.json` 于 `2026-08-25T14:55:54Z` 为 `GO`，unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部通过，Connector/J `test_count=136`、0 failures、0 errors、0 skipped；`git diff --check` 无空白错误。

## Continuation 181

- 补齐 StorageManager 关闭生命周期：`StorageManager.Close` 现在先完成最终 flush，再停止并刷新优化 Buffer Pool，最后关闭 tablespace；StorageManager 与 OptimizedBufferPoolManager 的 Close 均幂等，刷新失败也会继续释放后台 worker、定时器、LRU 和预读队列。
- 补齐独立表空间发现：`SystemSpaceManager` 使用 `SpaceManagerImpl.ListSpaceIDs` 快照合并实际加载空间的数据库、类型、路径、大小和页数；`StorageManager.CreateTablespace` 创建用户表空间后立即刷新该 metadata，避免只登记预置系统表而遗漏用户表。
- 新增 `TestStorageManagerCloseStopsOptimizedBufferPool`、`TestOptimizedBufferPoolManagerCloseIsIdempotent`、`TestSystemSpaceManagerDiscoversCreatedUserTablespace`；manager 全量专项通过。
- continuation181 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation181/cluster-report.json` 于 `2026-08-25T15:07:59Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation181-final2/release-candidate.json` 于 `2026-08-25T15:29:38Z` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部通过，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。FULLTEXT、全量非 Connector/J 客户端、原生加密页头/认证完整性、复杂 MySQL 8.4 语义和真实生产部署/故障注入验收仍是明确边界。

## Continuation 182

- 将优化 Buffer Pool 的 hint 从空实现收口为真实 read-ahead 行为：支持 `OFF/NONE/DISABLED`、`CONSERVATIVE`、`NORMAL`、`AGGRESSIVE` 和 `READ_AHEAD=n`，每次 `PrefetchPage` 按配置向队列提交连续页面；负值、越界和未知 hint 返回明确错误。
- 收口关闭并发安全：Buffer Pool 关闭后 `SetReadAheadPages` 返回错误，晚到的 `PrefetchPage` 被安全忽略；`LockManager.Close` 改为幂等，避免重复释放 stop channel 导致 panic。
- 新增 `TestOptimizedBufferPoolManagerAppliesReadAheadHints` 与 `TestLockManagerCloseIsIdempotent`；全仓回归、集群 smoke、发布候选门禁均通过。
- continuation182 门禁证据：`reports/compatibility/p1-cluster-current-continuation182/cluster-report.json` 于 `2026-08-25T15:34:16Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation182/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 183

- 继续收口索引行排序：`ClusterLeafRow.Less` 在具备表元数据和值列时按复合主键定义逐列比较，保留 infimum/supremum 与无元数据二进制主键回退；新增 `TestClusterLeafRowLessUsesCompositePrimaryKeyOrder`，覆盖复合整数+字符串主键的前缀优先级、第二列排序和严格序关系。
- `RedoLogManager.Close` 现在等待后台 worker，并在退出前排空已接受的异步提交请求；新增 `TestRedoLogManagerCloseDrainsAsyncCommitRequests`，关闭不会遗留未回调的 `FlushAsync` 请求。
- continuation183 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation183/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation183/release-candidate.json` 为 `GO`，所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 184

- 收口 Gap/Next-Key 锁键比较：`compareKeys` 支持混合有符号/无符号整数、有限浮点数、字符串和二进制键，不再因 `int`/`int64` 等常见类型混用触发类型断言 panic；不支持的不同键类型也不再无条件视为相等，保持确定性顺序。
- 复合主键排序和锁键比较专项测试先失败后转绿；`server/innodb/storage/wrapper/record`、`server/innodb/engine`、`server/innodb/manager` 专项回归及全仓回归均通过。
- continuation184 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation184/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation184/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 185

- 补齐 Next-Key 与普通 Record Lock 的物理资源关联：新增 `LockManager.AcquireNextKeyLockOnRecord`，要求显式传入 table/index/page/row 标识，在保留旧逻辑 `AcquireNextKeyLock` 的同时检查已有 Record Lock；同一物理记录上冲突的 Next-Key S/X 锁也会立即返回 `ErrLockConflict`。
- `NextKeyLockInfo` 保存可选的 `RecordResourceID`，兼容性判断在物理资源可用时按物理记录比较；新增 `TestNextKeyLockOnRecordChecksExistingRecordLock` 与 `TestNextKeyLockOnRecordRejectsConflictingNextKeyLock`，覆盖普通 Record Lock 和 Next-Key 之间的冲突闭环。
- continuation185 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation185/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation185/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 186

- 补齐二级索引叶行的真实页号接入点：新增 `SecondaryIndexPageResolver` 和 `SetPageResolver`，`GetPageNumber` 会将叶行携带的主键值交给聚簇索引解析器，解析失败时明确返回未知页号，不再伪装成“已完成主键查找”。新增 `TestSecondaryIndexLeafRowResolvesPageThroughPrimaryKey`。
- 清理实现审计中的误导性 TODO：BufferPage/legacy transaction marker 已有真实兼容实现；ExtentManager 的磁盘恢复、generic `basic.Space` 的全表列解码和 mysql.user 的 SegmentID 均补充了实际边界说明——前两者分别需要持久化 extent descriptor/clustered-row decoder，后者当前 EnhancedBTreeManager 由 BufferPool 分配且没有 SegmentManager 所有权。
- continuation186 门禁证据：全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation186/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation186/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 187

- 继续收口 P1 复合二级索引谓词：支持“等值前缀 + 下一索引列 `IN (...)`”的多段前缀等值范围扫描，并在主键去重后执行完整 WHERE 残余过滤；新增 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndInList`。
- 支持“等值前缀 + 下一索引列 `BETWEEN ... AND ...`”的复合索引前缀扫描；索引键只负责缩小等值前缀范围，BETWEEN 边界仍由行过滤器按 SQL 语义精确判断，避免把兼容性键编码误当成类型可排序编码；新增 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndBetween`。
- 两个新增回归均先验证旧实现退回 `table_scan`，再转为目标 `secondary_index:idx_tenant_score`；engine 定向回归和全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- continuation187 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T17:07:27.5654552Z` 为 `PASS`。
- continuation187 发布证据：`reports/compatibility/release-candidate-current-continuation187/release-candidate.json` 于 `2026-08-25T17:18:32.8080127Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 188

- 继续收口 P1 复合二级索引谓词：等值前缀后的 `IS NULL` 使用包含真实 NULL 编码的精确前缀范围；`IS NOT NULL` 使用等值前缀范围并由残余过滤器执行 NULL 语义；新增 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndIsNull` 与 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndIsNotNull`。
- 等值前缀后的普通 `LIKE 'prefix%'` 与 `NOT LIKE 'prefix%'` 现在也使用复合索引前缀扫描，通配符边界仍交给完整行过滤器；新增 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndPrefixLike` 与 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndNotLike`。FULLTEXT 仍按用户要求暂缓，未将普通 LIKE 误标为 FULLTEXT。
- 新增回归均先验证旧实现退回 `table_scan`，再转为目标复合索引访问路径；engine 全包与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- continuation188 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T17:24:34.5537108Z` 为 `PASS`。
- continuation188 发布证据：`reports/compatibility/release-candidate-current-continuation188/release-candidate.json` 于 `2026-08-25T17:36:59.8274250Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 189

- 补齐复合二级索引等值前缀后的 `NOT IN (...)`：索引访问先扫描对应等值前缀范围，再将候选行交给现有 NULL-aware `NOT IN` 过滤器，避免错误地把 NULL 当作普通列表值；新增 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndNotIn`。
- 增加 `tenant_id = 7 AND (score = 10 OR score = 30)` 的共享前缀回归 `TestP1SelectUsesCompositeSecondaryIndexForSharedPrefixOr`；该形状在当前 DNF/残余过滤链路中已通过，未重复引入执行器逻辑。
- 新增回归、engine 全包和全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- continuation189 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T17:41:15.3463744Z` 为 `PASS`。
- continuation189 发布证据：`reports/compatibility/release-candidate-current-continuation189/release-candidate.json` 于 `2026-08-25T17:52:19.1845812Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 190

- 补齐复合二级索引等值前缀后的 `NOT BETWEEN ... AND ...`：使用等值前缀范围获得候选行，再由原有反向区间过滤器执行精确语义；新增 `TestP1SelectUsesCompositeSecondaryIndexForEqualityPrefixAndNotBetween`，旧实现先退回 `table_scan`，新实现命中 `secondary_index:idx_tenant_score`。
- 全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过；后者无空白错误。
- continuation190 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T17:55:19.2899933Z` 为 `PASS`。
- continuation190 发布证据：`reports/compatibility/release-candidate-current-continuation190/release-candidate.json` 于 `2026-08-25T18:06:15.1099677Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 191

- 收口统计收集器生命周期：普通 `StatisticsCollector` 与 `EnhancedStatisticsCollector` 的 `Stop()` 均通过 `sync.Once` 幂等关闭后台通道，新增 `TestStatisticsCollectorStopIsIdempotent` 与 `TestEnhancedStatisticsCollectorStopIsIdempotent`；旧实现的第二次 Stop 回归先触发 `close of closed channel`，修复后通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation191 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T18:09:59.3386232Z` 为 `PASS`。
- continuation191 发布证据：`reports/compatibility/release-candidate-current-continuation191/release-candidate.json` 于 `2026-08-25T18:20:59.1212393Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 192

- 收口后台空间管理器生命周期：`ExtentReuseManager`、`SegmentSpaceOptimizer`、`SpaceExpansionManager` 的 Stop 现在使用 `sync.Once`，重复关闭不会再次 close channel 或触发 panic；新增 `TestBackgroundManagersStopIsIdempotent`，旧实现先行触发 `close of closed channel`，修复后通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation192 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T18:24:14.6850751Z` 为 `PASS`。
- continuation192 发布证据：`reports/compatibility/release-candidate-current-continuation192/release-candidate.json` 于 `2026-08-25T18:35:25.4215392Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 193

- 继续收口日志与缓存生命周期：`BatchCompressor.Close` 使用 `sync.Once`；`BatchWriter.Close` 只在第一次调用时停止 worker、flush 待写批次并关闭文件，同时缓存关闭错误；legacy `BufferPoolManager.Close` 只执行一次 stop/flush。新增 `TestBatchCompressorCloseIsIdempotent`、`TestBatchWriterCloseIsIdempotent`、`TestBufferPoolManagerCloseIsIdempotent`，旧实现的重复 Close 回归先触发 `close of closed channel`，修复后通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation193 集群证据：`reports/compatibility/p1-cluster-current/cluster-report.json` 于 `2026-08-25T18:39:35.6615860Z` 为 `PASS`。
- continuation193 发布证据：`reports/compatibility/release-candidate-current-continuation193/release-candidate.json` 于 `2026-08-25T18:50:42.9149151Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 194

- 收口增强 B+Tree 管理器的并发关闭竞态：`EnhancedBTreeManager.Close` 从先读后写的 shutdown 标志改为 `CompareAndSwap`，多个节点生命周期调用方并发关闭时只有一个 goroutine 会关闭后台任务 channel，其余调用安全返回；新增 `TestEnhancedBTreeManagerCloseIsSafeForConcurrentCallers`。
- 管理器定向回归与全仓 `go test ./... -count=1 -timeout 5m` 通过；`git diff --check` 无空白错误（仅有 Windows 行尾提示）。
- continuation194 集群证据：`reports/compatibility/p1-cluster-current-continuation194/cluster-report.json` 于 `2026-08-25T18:59:41.0413386Z` 为 `PASS`。
- continuation194 发布证据：`reports/compatibility/release-candidate-current-continuation194/release-candidate.json` 于 `2026-08-25T19:10:37.4636795Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 195

- 修复 B+Tree 优化范围扫描的起始边界：当 `startKey` 大于选定叶页全部键时，`BTreeIterator` 现在定位到叶页末尾，不再回退到槽位 0 并错误返回范围外记录；新增 `TestRangeSearchOptimizedReturnsNoRowsWhenStartIsAfterLastKey`，旧实现先返回 `key_001..key_003`，修复后返回空集。
- 管理器定向回归、全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过（后者仅有 Windows 行尾提示）。
- continuation195 集群证据：`reports/compatibility/p1-cluster-current-continuation195/cluster-report.json` 于 `2026-08-25T19:14:41.8795677Z` 为 `PASS`。
- continuation195 发布证据：`reports/compatibility/release-candidate-current-continuation195/release-candidate.json` 于 `2026/8/25 19:25:41` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 196

- 收口执行集成层并发统计：索引扫描/表扫描计数统一通过受 `ExecutionEngineIntegrator` 锁保护的 `recordAccessMethod` 更新；`GetCombinedStats` 返回不可被后续查询修改的执行统计快照；新增 `TestExecutionIntegrationAccessMethodStatsAreConcurrentSafe`，64 个并发调用方重复更新两类计数后结果精确一致。
- 集成包定向回归、全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation196 集群证据：`reports/compatibility/p1-cluster-current-continuation196/cluster-report.json` 于 `2026-08-25T19:28:40.0654800Z` 为 `PASS`。
- continuation196 发布证据：`reports/compatibility/release-candidate-current-continuation196/release-candidate.json` 于 `2026/8/25 19:39:32` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 197

- 补齐复合二级索引的 DNF OR 访问路径：形如 `(tenant_id = 7 AND score = 10) OR (tenant_id = 8 AND score = 20)` 的多个等值分支，现在分别执行同一复合索引的连续前缀范围扫描，按主键去重并重新执行完整 WHERE 过滤；不满足同一连续前缀条件的复杂布尔表达式仍安全回退。新增 `TestP1SelectMergesCompositeSecondaryIndexOrBranches`，旧实现访问路径为 `table_scan`，修复后为 `secondary_index:idx_tenant_score`。
- engine 全包与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation197 集群证据：`reports/compatibility/p1-cluster-current-continuation197/cluster-report.json` 于 `2026-08-25T19:44:51.5861725Z` 为 `PASS`。
- continuation197 发布证据：`reports/compatibility/release-candidate-current-continuation197/release-candidate.json` 于 `2026/8/25 19:55:44` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 198

- 继续补齐复合二级索引 DNF OR：每个分支支持“连续等值前缀 + 下一列简单范围”（`>=`、`<=`、`>`、`<`），候选读取使用等值前缀范围，最终由完整 WHERE 保持范围语义；新增 `TestP1SelectMergesCompositeSecondaryIndexOrRangeBranches`，旧实现访问路径为 `table_scan`，修复后为 `secondary_index:idx_tenant_score`。
- engine 全包与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- continuation198 集群证据：`reports/compatibility/p1-cluster-current-continuation198/cluster-report.json` 于 `2026-08-25T20:00:03.5431591Z` 为 `PASS`。
- continuation198 发布证据：`reports/compatibility/release-candidate-current-continuation198/release-candidate.json` 于 `2026/8/25 20:12:25` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 199

- 继续补齐复合二级索引 DNF OR：每个分支支持“连续等值前缀 + `BETWEEN`/`NOT BETWEEN`”，候选读取使用共享复合索引的等值前缀范围，最终由完整 WHERE 过滤器保持区间和 NULL 语义；新增 `TestP1SelectMergesCompositeSecondaryIndexOrBetweenBranches`，旧实现访问路径为 `table_scan`，修复后为 `secondary_index:idx_tenant_score`。
- engine 定向回归与全仓 `go test ./... -count=1 -timeout 5m` 通过；`git diff --check` 返回 0，仅有 Windows 行尾提示、无空白错误。
- continuation199 集群证据：`reports/compatibility/p1-cluster-current-continuation199/cluster-report.json` 于 `2026-08-25T20:18:50.1319015Z` 为 `PASS`。
- continuation199 发布证据：`reports/compatibility/release-candidate-current-continuation199/release-candidate.json` 于 `2026/8/25 20:29:55` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 200

- P1-SQL-001 补齐嵌套派生表中的混合集合表达式：普通派生表内再嵌套的 `INTERSECT/EXCEPT` 现在递归先物化最内层集合结果，再逐层按内层/外层 SELECT 顺序应用投影、过滤、排序和分页；新增 `TestNestedDerivedTableSupportsMixedSetExpression` 与 `TestDeeplyNestedDerivedTableSupportsMixedSetExpression`，避免落回 `unsupported derived table set operation` 或解析失败。
- 复杂 SELECT/派生表/相关子查询/CTE 专项通过；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 121.741s。
- continuation200 集群证据：`reports/compatibility/p1-cluster-current-continuation200/cluster-report.json` 为 `PASS`。
- continuation200 发布证据：`reports/compatibility/release-candidate-current-continuation200/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=139`、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更深层/任意 AST 组合的通用子查询执行、复杂 index-range 分区、异构 row-source 完整列裁剪、统计采样全覆盖、复杂 View/触发器 MDL、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 201

- P1-SQL-001 继续补齐嵌套混合集合的 JOIN 边界：每一层物化派生结果如果带有 JOIN，现在转入既有物化派生 JOIN 执行器，保留外表列、ON 条件和排序语义；新增 JOIN 回归并通过。
- 本轮验证：engine 全包 `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.611s）。
- continuation201 集群证据：`reports/compatibility/p1-cluster-current-continuation201/cluster-report.json` 为 `PASS`。
- continuation201 发布证据：`reports/compatibility/release-candidate-current-continuation201/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=139`、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更深层/任意 AST 组合的通用子查询执行、复杂 index-range 分区、异构 row-source 完整列裁剪、统计采样全覆盖、复杂 View/触发器 MDL、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

- 收口 P1-SEC-001 的特性级权限路由：dispatcher 不再把所有 `CREATE`/`ALTER`/`DROP`/`SHOW`/`OTHER` 语句粗略映射为通用权限；VIEW、TRIGGER、EVENT、PROCEDURE/FUNCTION、CALL、用户/角色、GRANT/REVOKE、临时表、表空间、FLUSH、LOCK TABLES 和 `SHOW CREATE VIEW` 现在映射到对应 MySQL 权限。触发器 `ON` 对象、视图定义源表和限定名视图对象也会传入表级权限检查；新增 `feature_privilege_routing_test.go`，旧实现先被回归测试捕获，修复后通过。
- dispatcher 定向回归与全仓 `go test ./... -count=1 -timeout 5m` 通过；`git diff --check` 返回 0，仅有 Windows 行尾提示、无空白错误。
- continuation200 集群证据：`reports/compatibility/p1-cluster-current-continuation200/cluster-report.json` 于 `2026-08-25T20:34:28.6054653Z` 为 `PASS`。
- continuation200 发布证据：`reports/compatibility/release-candidate-current-continuation200/release-candidate.json` 于 `2026/8/25 20:45:25` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 201

- 继续补齐 P1-OPT-004 复合索引 DNF OR：每个分支现在还支持“连续等值前缀 + `IS NULL`/`IS NOT NULL`/前缀 `LIKE`/`NOT LIKE`/常量 `NOT IN`”；索引只负责读取共享前缀候选，完整 WHERE 负责精确 NULL、通配符和反向集合语义。新增 `TestP1SelectMergesCompositeSecondaryIndexOrNullBranches` 与 `TestP1SelectMergesCompositeSecondaryIndexOrLikeBranches`，旧实现先回退 `table_scan`，修复后分别命中对应 `secondary_index` 路径。
- engine 全包与全仓 `go test ./... -count=1 -timeout 5m` 通过；`git diff --check` 返回 0，仅有 Windows 行尾提示、无空白错误。
- continuation201 集群证据：`reports/compatibility/p1-cluster-current-continuation201/cluster-report.json` 于 `2026-08-25T20:50:00.8669405Z` 为 `PASS`。
- continuation201 发布证据：`reports/compatibility/release-candidate-current-continuation201/release-candidate.json` 于 `2026/8/25 21:01:16` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 202

- 补齐复合索引 DNF OR 的常量 `NOT IN` 分支：新增 `TestP1SelectMergesCompositeSecondaryIndexOrNotInBranches`；旧实现回退 `table_scan`，修复后使用共享复合索引前缀扫描、主键去重，并由完整 WHERE 保持 `NOT IN` 的 NULL-aware 语义。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；`git diff --check` 返回 0，仅有 Windows 行尾提示、无空白错误。
- continuation202 集群证据：`reports/compatibility/p1-cluster-current-continuation202/cluster-report.json` 于 `2026-08-25T21:04:14.0564863Z` 为 `PASS`。
- continuation202 发布证据：`reports/compatibility/release-candidate-current-continuation202/release-candidate.json` 于 `2026/8/25 21:15:24` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 203

- 收口 P1-OPT-001 的投影边界：`Selection(Projection(...))` 现在仅在投影由不重复的直接列组成、且谓词只引用投影列时下推到子计划；计算列、重复列和缺失列保持在投影上方。新增 `TestPredicatePushdownThroughIdentityProjection` 与 `TestPredicatePushdownDoesNotCrossComputedProjection`，旧实现先未下推，修复后通过。
- 同步清理 P* 现状文档：存储过程/函数、触发器、视图、窗口函数和内部 GTID/binlog 协议已标注为已覆盖的兼容子集，保留更广语法、物化视图和官方物理文件互操作边界。
- plan 定向回归与全仓 `go test ./... -count=1 -timeout 5m` 通过；`git diff --check` 返回 0，仅有 Windows 行尾提示、无空白错误。
- continuation203 集群证据：`reports/compatibility/p1-cluster-current-continuation203/cluster-report.json` 于 `2026-08-25T21:20:34.7536635Z` 为 `PASS`。
- continuation203 发布证据：`reports/compatibility/release-candidate-current-continuation203/release-candidate.json` 于 `2026/8/25 21:31:48` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 204

- 补齐 P0-OBS-001 当前运行态证据：实际启动 `cmd/p0_metrics_endpoint`，健康检查与 `/metrics` 抓取均 PASS；`observability_smoke.ps1` 和 `metrics_export_smoke.ps1` 也分别验证必需指标、结构化慢查询/错误日志字段和告警规则。
- endpoint 证据：`reports/compatibility/p0d-metrics-current-continuation204/p0d_metrics_endpoint_evidence_20260825_163320.json` 与同目录 `metrics_endpoint_probe_20260825_163320.json` 均为 `PASS`。
- observability 证据：`reports/compatibility/p0d-observability-current-continuation204/observability_smoke_20260825_163333.json`、`reports/compatibility/p0d-metrics-export-current-continuation204/metrics_export_20260825_163333.json` 均为 `PASS`。
- P* backlog 现状同步：存储过程/函数、触发器、视图、窗口函数和内部 GTID/binlog 协议已标注为已覆盖的兼容子集；FULLTEXT、空间/R-tree、skip scan、原生加密页头、官方 binlog/PITR 物理互操作等边界仍保留。

## Continuation 205

- 交付候选重新启用 P0-B/P0-C/P0-D/P0-E focused evidence；tooling preflight、恢复状态差分、一致性证据、定时回滚、崩溃恢复、可观测性、并发、全链路演练和全仓 Go 回归全部 PASS。候选仍仅因治理门槛保持 FAIL：最终决策、owner sign-off、7 个开放 P0 风险和 7 个 `TBD` 风险 owner。
- 修复交付编排脚本：候选 runner 使用强类型参数表调用子脚本，并在严格模式下安全读取 `$LASTEXITCODE`；P0-E 恢复点字符串插值和 P0-B 快照差分的 volatile `generated_at` 归一化已补齐。P0-B 证据 fixture 使用等长 UPDATE payload，保留既有短写入页面尾部契约；focused P0-B/P0-E 回归通过。
- 继续收敛 P1-OPT-004/P1-IDX-001：复合二级索引 OR 分支支持“连续等值前缀 + 常量 `IN`/`<>`/`!=`”，索引前缀仅用于候选读取，最终由完整 WHERE 保持精确语义；新增 `TestP1SelectMergesCompositeSecondaryIndexOrInBranches` 与 `TestP1SelectMergesCompositeSecondaryIndexOrNotEqualBranches`，旧实现先回退 `table_scan`，修复后命中 `secondary_index`；同时修正 `!=` 被等值解析器误识别的边界；`go test ./server/innodb/engine -count=1` 通过。
- continuation205 门禁证据：`go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation205/cluster-report.json` 于 `2026-08-25T21:50:50.2774981Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation205/release-candidate.json` 于 `2026-08-25T22:01:53.8695195Z` 为 `GO`，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 和 Connector/J 全部 PASS，JDBC 为 136 tests、0 failures、0 errors、0 skipped。

## Continuation 206

- 继续补齐 P1-OPT-004/P1-IDX-001 的 NULL 语义：复合二级索引 OR 分支现在识别“连续等值前缀 + NULL-safe equality (`<=>`)”，使用共享前缀读取候选并由完整 WHERE 保持 `<=>` 的 NULL=NULL 与普通值相等语义；新增 `TestP1SelectMergesCompositeSecondaryIndexOrNullSafeEqualityBranches`，旧实现返回空集，修复后命中 `secondary_index` 并返回正确行集。
- 收口 P1-OPT-003 的采样安全边界：当页面采样没有任何可验证的 InnoDB INDEX 页时不再按“每页 100 行”制造统计行数，改为返回 0，等待真实 accessor 或精确扫描；同时将水塘采样改为局部随机源，避免改写全局随机状态。
- continuation206 门禁证据：`go test ./... -count=1 -timeout 5m`、集群 smoke 均通过；`reports/compatibility/p1-cluster-current-continuation206/cluster-report.json` 于 `2026-08-25T22:11:22.7388712Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation206/release-candidate.json` 于 `2026-08-25T22:22:40.6987475Z` 为 `GO`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 207

- 继续补齐 P1-OPT-003 的真实统计链路：当 `SpaceManager.GetSpace` 暂时失败但 `StorageEngineAccessor` 仍可解析表空间时，表统计现在保留 accessor 提供的真实行数、数据长度、索引长度和总大小，不再回退为合成估算；新增 `TestEnhancedStatisticsCollectorUsesAccessorWhenSpaceHandleIsUnavailable`，旧实现先返回估算值，修复后通过。
- continuation207 门禁证据：plan 定向回归和 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation207/cluster-report.json` 于 `2026-08-25T22:27:27.381265Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation207/release-candidate.json` 于 `2026-08-25T22:39:06.501602Z` 为 `GO`，Connector/J 为 136 tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 208

- 继续补齐 P1-OPT-005：表达式规范化器现在消除双重否定 `NOT (NOT predicate)`，保留原谓词结构；该改写对 SQL 三值逻辑安全，新增 `TestDoubleNegationSimplificationPreservesPredicate`，旧实现先保留 `NotExpression`，修复后通过。
- 同一轮继续补齐 P1-OPT-005：对 `NOT (A AND B)`/`NOT (A OR B)` 执行 NULL-safe De Morgan 重写，新增 `TestDeMorganSimplificationPreservesBooleanStructure`，先失败后通过；上一轮 Connector/J 等待被中断，未产生有效 release JSON，故本轮重新执行完整门禁。

## Continuation 210

- 开始收口 PX-IDX-003：复合二级索引现在支持后导列等值条件的 bounded skip scan；执行器通过增强索引记录接口读取持久化键、枚举不同前导值，再按“前导值 + 后导等值”执行前缀范围扫描并主键去重，最终仍执行原始 WHERE；新增 `TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingEquality`，旧实现回退 `table_scan`，修复后命中 `secondary_index:idx_tenant_score_skip_scan`。
- continuation210 engine/manager 与全仓 `go test ./... -count=1 -timeout 5m` 均通过；`reports/compatibility/p1-cluster-current-continuation210/cluster-report.json` 于 `2026-08-25T23:21:08.2560225Z` 为 `PASS`。
- continuation210 发布证据：`reports/compatibility/release-candidate-current-continuation210/release-candidate.json` 于 `2026-08-25T23:33:50.3950467Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 211

- 继续补齐 PX-IDX-003：skip scan 现在可跳过多个前导列，按持久化二级索引键枚举完整前导值组合，再使用“前导值组合 + 后导等值”前缀范围扫描；新增 `TestP2SelectUsesCompositeSecondaryIndexSkipScanAcrossMultipleLeadingColumns`，旧实现先回退 `table_scan`，修复后命中 `secondary_index:idx_region_tenant_score_skip_scan`。
- engine、manager 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation211/cluster-report.json` 于 `2026-08-25T23:38:10.9209114Z` 为 `PASS`。
- continuation211 发布证据：`reports/compatibility/release-candidate-current-continuation211/release-candidate.json` 于 `2026-08-25T23:49:40.3538955Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 212

- 继续补齐 PX-IDX-003：后导列 `>`/`>=`/`<`/`<=` 范围条件现在也能复用多前导列 skip scan；由于持久化二级键按稳定文本编码而非类型排序，执行器对每个前导组合扫描有界前缀并重新执行原始范围谓词，新增 `TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingRange`，旧实现先回退 `table_scan`，修复后命中 `secondary_index:idx_tenant_score_skip_scan`。
- engine 定向回归、全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation212/cluster-report.json` 于 `2026-08-25T23:52:11.1906692Z` 为 `PASS`。
- continuation212 发布证据：`reports/compatibility/release-candidate-current-continuation212/release-candidate.json` 于 `2026-08-26T00:07:31.539007Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 213

- 继续补齐 PX-IDX-003 的 NULL 边界：复合二级索引后导列 `IS NULL` 现在可枚举前导值组合并执行精确 NULL 前缀范围扫描，新增 `TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingNull`，旧实现先回退 `table_scan`，修复后命中 `secondary_index:idx_tenant_score_skip_scan`。
- engine 定向回归、全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation213/cluster-report.json` 于 `2026-08-26T00:09:23.9071729Z` 为 `PASS`。
- continuation213 发布证据：`reports/compatibility/release-candidate-current-continuation213/release-candidate.json` 于 `2026-08-26T00:22:13.7195036Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 1000

- P1-SQL-001 继续补齐递归 CTE 主查询的 JOIN AST：单列和多列递归 CTE 现在支持与多张基表组成的嵌套 JOIN 链（例如 `CTE JOIN t1 JOIN t2`），统一进入物化派生 JOIN 执行器，保留 ON/WHERE、外连接 NULL 扩展、投影、排序和分页语义。
- 新增 `TestRecursiveCTECompatibilitySupportsMainQueryJoinChainWithBaseTables`；先行回归复现旧实现的 `recursive CTE main query has unsupported FROM expression`，修复后通过；递归 CTE 专项、engine 全量和全仓 Go 回归均通过。
- 本轮 fresh 验证：engine `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（117.825s）；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 123.488s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation214/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation214/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation214/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation214/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`；本轮 JDBC 测试耗时 11:05 min，证明长批量客户端路径最终完成而非提前跳过。
- 仍未完成的边界包括通用/任意复杂 SQL AST 与相关子查询谓词推导、统计采样全覆盖、复杂 View/触发器依赖 MDL、真正 online DDL/多表 DDL、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE` 及更高阶集群/存储互操作；FULLTEXT 与非 Connector/J 全量客户端继续按用户要求后置。

## Continuation 214

- 继续补齐 PX-IDX-003 的 NULL 边界：后导列 `IS NOT NULL` 现在也能枚举多前导列组合并扫描前缀范围，最终由原始 WHERE 过滤 NULL 行；新增 `TestP2SelectUsesCompositeSecondaryIndexSkipScanForTrailingNotNull`，旧实现先回退 `table_scan`，修复后命中 `secondary_index:idx_tenant_score_skip_scan`。
- engine 定向回归、全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation214/cluster-report.json` 于 `2026-08-26T00:24:15.2970088Z` 为 `PASS`。
- continuation214 发布证据：`reports/compatibility/release-candidate-current-continuation214/release-candidate.json` 于 `2026-08-26T00:36:25.4767793Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 215

- 继续补齐 P1-OPT-005：De Morgan 重写现在会递归规范化新生成的 `NOT` 节点，`NOT (NOT A AND B)` 可直接化为 `A OR NOT B`；新增 `TestDeMorganRecursivelyEliminatesNestedNegation`，旧实现先残留 `NotExpression`，修复后通过。
- plan 定向回归、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation215/cluster-report.json` 于 `2026-08-26T00:38:39.6711793Z` 为 `PASS`。
- continuation215 发布证据：`reports/compatibility/release-candidate-current-continuation215/release-candidate.json` 于 `2026-08-26T00:50:48.9690413Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 216

- 继续补齐 P1-OPT-001：INNER JOIN 等值键现在可从一侧的常量等值谓词推导另一侧等值谓词（例如 `a.id=b.id AND a.id=7` 推导 `b.id=7`），仅限 INNER JOIN 且仅限直接列等值，避免改变外连接 NULL 扩展语义；新增 `TestPredicatePushdownInfersJoinSideConstantFromEquality`，旧实现未在右子树生成过滤，修复后通过。
- plan 定向回归、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation216/cluster-report.json` 于 `2026-08-26T00:54:04.6908525Z` 为 `PASS`。
- continuation216 发布证据：`reports/compatibility/release-candidate-current-continuation216/release-candidate.json` 于 `2026-08-26T01:06:43.3589671Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 217

- 继续补齐 P1-OPT-001：INNER JOIN 上方的直接跨表列等值谓词现在会并入 Join 条件，保留外连接上的顶层过滤语义；新增 `TestPredicatePushdownMovesInnerJoinEqualityIntoJoinCondition`，旧实现保留顶层 `LogicalSelection`，修复后通过。
- plan 定向回归、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation217/cluster-report.json` 于 `2026-08-26T01:09:13.4807419Z` 为 `PASS`。
- continuation217 发布证据：`reports/compatibility/release-candidate-current-continuation217/release-candidate.json` 于 `2026-08-26T01:21:21.5796799Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 218

- 继续补齐 P1-IDX-001：index-merge AND 现在支持多个复合二级索引的首列等值分支，使用各自的首列前缀范围读取并对主键集合求交；新增 `TestP1SelectIntersectsCompositeSecondaryIndexLeadingEqualityBranches`，旧实现先命中单个 `secondary_index`，修复后命中 `index_merge_and:idx_state_region,idx_tenant_score`。
- engine、manager 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation218/cluster-report.json` 于 `2026-08-26T01:27:49.8346353Z` 为 `PASS`。
- continuation218 发布证据：`reports/compatibility/release-candidate-current-continuation218/release-candidate.json` 于 `2026-08-26T01:39:54.9067719Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 219

- 继续补齐 P1-IDX-001：index-merge AND 的范围分支现在也支持复合二级索引首列，等值分支使用首列前缀范围，范围分支扫描该索引命名空间后由完整 WHERE 过滤；新增 `TestP1SelectIntersectsCompositeSecondaryIndexLeadingRangeBranches`，旧实现回退 `table_scan`，修复后命中 `index_merge_and:idx_state_region,idx_tenant_score`。
- engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation219/cluster-report.json` 于 `2026-08-26T01:42:49.1087894Z` 为 `PASS`。
- continuation219 发布证据：`reports/compatibility/release-candidate-current-continuation219/release-candidate.json` 于 `2026-08-26T01:55:17.1373691Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 220

- 继续补齐 PX-IDX-003 的选择性边界：skip scan 增加基础成本门槛，当不同前导组合数不小于索引条目数时回退 `table_scan`，避免为低选择性场景增加完整索引枚举和多次探测；新增 `TestP2SkipScanFallsBackWhenLeadingPrefixCostIsNotSelective`，既有等值、多前导列、范围和 NULL skip scan 回归保持通过。
- engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation220/cluster-report.json` 于 `2026-08-26T01:59:25.7083817Z` 为 `PASS`。
- continuation220 发布证据：`reports/compatibility/release-candidate-current-continuation220/release-candidate.json` 于 `2026-08-26T02:11:52.3533318Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 221

- 继续补齐 P1-OPT-002：列裁剪现在会显式进入 `LogicalSubquery.Subplan`，保留子查询内部投影/谓词依赖；`LogicalApply.JoinConds` 的左右关联键也会参与需求列划分，避免关联执行因裁剪丢失键列。新增 `TestColumnPruning_TraversesSubquerySubplan` 与 `TestColumnPruning_ApplyKeepsJoinConditionColumns`，均按“先失败、后修复”回归通过。
- plan、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation221/cluster-report.json` 于 `2026-08-26T02:25:28Z` 为 `PASS`。
- continuation221 发布证据：`reports/compatibility/release-candidate-current-continuation221/release-candidate.json` 于 `2026-08-26T02:36:10.6605874Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 222

- 继续补齐 P1-SQL-001：相关标量子查询现在支持受控的单层派生表外层来源；先物化派生外层结果，再按外层行绑定相关列执行内层聚合标量查询，新增 `TestCorrelatedScalarSubquerySupportsDerivedOuterSource`，旧实现先报 `unsupported table expression type: *sqlparser.Subquery`，修复后通过。
- 相关 EXISTS/scalar/DML 回归、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation222/cluster-report.json` 于 `2026-08-26T02:44:06Z` 为 `PASS`。
- continuation222 发布证据：`reports/compatibility/release-candidate-current-continuation222/release-candidate.json` 于 `2026-08-26T02:54:50Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 223

- 继续审计 P1-SQL-001 的派生外层边界：多层派生外层来源已能复用递归 derived 执行器，本轮新增 `TestCorrelatedScalarSubquerySupportsNestedDerivedOuterSource` 固化该行为；未新增生产代码。
- `go test ./server/innodb/engine ./server/innodb/plan -count=1` 通过；continuation222 的集群/release 门禁仍是本轮最新交付门禁证据。

## Continuation 224

- 继续补齐 P1-SQL-003：ALTER 兼容入口新增 `ALTER TABLE ... DROP CONSTRAINT <symbol>`，先删除命名 CHECK 约束；若同名 CHECK 不存在，再兼容删除外键 symbol，并保留原有 `DROP CHECK`/`DROP FOREIGN KEY` 路径。新增 `TestAlterTableDropConstraintRemovesCheckConstraint` 与 `TestAlterTableDropConstraintAcceptsForeignKeySymbol`，旧实现先报 `unsupported ALTER TABLE action`，修复后通过。
- engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation224/cluster-report.json` 于 `2026-08-26T03:06:55Z` 为 `PASS`。
- continuation224 发布证据：`reports/compatibility/release-candidate-current-continuation224/release-candidate.json` 于 `2026-08-26T03:17:36Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 225

- 继续补齐 P1-OPT-001：表达式规范化、谓词下推和索引访问优化现在都会进入 `LogicalSubquery.Subplan`；新增 `TestExpressionNormalizationTraversesSubquerySubplan`、`TestPredicatePushdownTraversesSubquerySubplan` 与 `TestIndexAccessOptimizationTraversesSubquerySubplan`，旧实现分别无法消除子查询内双重否定、无法下推过滤、无法选择索引，修复后通过。
- engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation225/cluster-report.json` 于 `2026-08-26T03:28:34Z` 为 `PASS`。
- continuation225 发布证据：`reports/compatibility/release-candidate-current-continuation225/release-candidate.json` 于 `2026-08-26T03:39:32Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 226

- 继续补齐 P1-OPT-001/P1-OPT-005：聚合消除、MIN/MAX 投影简化现在进入 `LogicalSubquery.Subplan`；Apply 的 `JoinConds` 也参与表达式规范化。新增 `TestAggregationEliminationTraversesSubquerySubplan`、`TestMinMaxProjectionSimplificationTraversesSubquerySubplan` 与 `TestExpressionNormalizationCoversApplyJoinConditions`，旧实现先保留子查询内聚合/投影包装或双重否定，修复后通过。
- engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation226/cluster-report.json` 于 `2026-08-26T03:46:44Z` 为 `PASS`。
- continuation226 发布证据：`reports/compatibility/release-candidate-current-continuation226/release-candidate.json` 于 `2026-08-26T03:57:19Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 227

- 继续补齐 P1-EXE-002：新增可选 `BatchOperator` 契约和批量分发逻辑，`VolcanoExecutor.ExecuteBatches` 在根算子支持批量接口时直接消费 bounded batch；Values、表扫描、索引扫描、Projection、Filter 均支持批量路径，并保留不实现批量接口时的逐行回退。新增 `TestVolcanoExecutorExecuteBatchesUsesBatchOperator` 与 `TestVolcanoBatchOperatorsPropagateThroughFilterAndProjection`，后者先在旧实现上确认批量链路断回 `Next`，修复后确认 `scan → projection → filter` 全链路不调用逐行接口。
- engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation227/cluster-report.json` 于 `2026-08-26T04:08:00Z` 为 `PASS`。
- continuation227 发布证据：`reports/compatibility/release-candidate-current-continuation227/release-candidate.json` 于 `2026-08-26T04:18:59Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 228

- 继续补齐 P1-EXE-002：HashAggregate 现在使用 bounded `NextBatch` 拉取子算子输入，按 batch 更新分组状态，并按调用方 `maxRows` 分批返回聚合结果；原有逐行 `Next` 复用同一惰性计算状态，且重新 `Open` 会清理上一次执行状态。新增 `TestVolcanoBatchOperatorsPropagateThroughHashAggregate`，旧实现先确认聚合子算子没有批量调用，修复后确认不再调用逐行接口。
- engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation228/cluster-report.json` 于 `2026-08-26T04:24:22Z` 为 `PASS`。
- continuation228 发布证据：`reports/compatibility/release-candidate-current-continuation228/release-candidate.json` 于 `2026-08-26T04:35:31Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 229

- 继续补齐 P1-OPT-002/P1-EXE-003 的物理计划闭环：`ConvertToPhysicalPlan` 现在递归保留 Projection、Selection、Aggregation、Join、Apply 的物理子节点；ParallelExecutor 新增 `PhysicalValues`、`PhysicalSelection`、`PhysicalProjection` 执行路径，能够通过真实 `PlanRowReader` 递归读取子计划、构造列上下文并执行条件/投影表达式。新增 `TestConvertToPhysicalPlanPreservesUnaryChildren`、`TestConvertToPhysicalPlanPreservesAggregationChild`、`TestConvertToPhysicalPlanPreservesJoinAndApplyChildren`、`TestParallelExecutorExecutesPhysicalValues` 与 `TestParallelExecutorExecutesPhysicalSelectionAndProjection`，旧实现先确认物理树断链/返回空结果，修复后通过。
- plan、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation229/cluster-report.json` 于 `2026-08-26T04:54:54.3201451Z` 为 `PASS`。
- continuation229 发布证据：`reports/compatibility/release-candidate-current-continuation229/release-candidate.json` 于 `2026-08-26T05:05:44.8277077Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 230

- 继续补齐 P1-EXE-003 的物理计划执行边界：ParallelExecutor 新增 `PhysicalUnion` 执行路径，按子计划顺序支持 `UNION ALL` 保留重复行和默认 DISTINCT 稳定去重；`readPlanRows` 会递归执行 Values/Selection/Projection/Union 子计划，而不是把这些节点交给未实现的空回退。新增 `TestParallelExecutorExecutesPhysicalUnionDistinct`，同一回归覆盖 DISTINCT 与 ALL，旧实现先返回空结果，修复后通过。
- plan、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation230/cluster-report.json` 于 `2026-08-26T05:13:47.8079622Z` 为 `PASS`。
- continuation230 发布证据：`reports/compatibility/release-candidate-current-continuation230/release-candidate.json` 于 `2026-08-26T05:24:38.5475464Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 231

- 继续补齐 P1-EXE-003：ParallelExecutor 新增 `PhysicalMergeJoin` 执行 fallback，递归读取左右子计划，按条件执行 INNER JOIN，并对 LEFT JOIN 输出右侧 NULL 扩展；同时补充表名限定列上下文，避免左右表同名列在条件求值时互相覆盖。新增 `TestParallelExecutorExecutesPhysicalMergeJoin`，覆盖匹配行和未匹配左行，旧实现先返回空结果，修复后通过。
- plan、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation231/cluster-report.json` 于 `2026-08-26T05:32:03.0269966Z` 为 `PASS`。
- continuation231 发布证据：`reports/compatibility/release-candidate-current-continuation231/release-candidate.json` 于 `2026-08-26T05:42:35.5718154Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 232

- 继续补齐 P1-EXE-003 的物理计划执行边界：ParallelExecutor 现在能从普通 `GroupByItems`/`AggFuncs` 推导 HashAggregate 所需的分组列和聚合输入，且 `PhysicalStreamAgg` 复用同一聚合执行路径；新增 `TestParallelExecutorDerivesHashAggregateSpecFromExpressions` 与 `TestParallelExecutorExecutesPhysicalStreamAggregate`，旧实现分别因缺少聚合规格或返回空结果失败，修复后通过。
- plan、engine 与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation232/cluster-report.json` 于 `2026-08-26T05:50:05.3033187Z` 为 `PASS`。
- continuation232 发布证据：`reports/compatibility/release-candidate-current-continuation232/release-candidate.json` 于 `2026-08-26T06:00:40.7341191Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 233

- 继续收敛 P1-EXE-003 的错误契约和物理计划闭环：ParallelExecutor 遇到尚未接入的物理节点时不再返回“成功但空结果”，而是返回明确错误；引擎侧 `generatePhysicalPlan` 不再把未知逻辑节点伪装成空表扫描，并把 Join、Aggregation、Projection、Selection 生成时的真实子计划挂回物理树。新增 `TestParallelExecutorRejectsUnsupportedPhysicalPlan`、`TestGeneratePhysicalPlanPreservesUnaryChild`、`TestGeneratePhysicalPlanPreservesJoinAndAggregateChildren` 与 `TestGeneratePhysicalPlanRejectsUnknownLogicalPlan`，均按先失败后修复回归通过。
- plan/engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation233/cluster-report.json` 于 `2026-08-26T06:07:47.7622317Z` 为 `PASS`。
- continuation233 发布证据：`reports/compatibility/release-candidate-current-continuation233/release-candidate.json` 于 `2026-08-26T06:18:28.8873299Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 234

- 继续补齐 P1-EXE-002：`SortOperator` 现在实现 bounded `NextBatch`，排序物化阶段通过 `nextOperatorBatch` 拉取子算子数据，并在 `Open` 时重置惰性排序状态；新增 `TestVolcanoBatchOperatorsPropagateThroughSort`，旧实现先确认排序根算子退回逐行读取，修复后确认 batch 链路和稳定排序结果均通过。
- engine 专项与全仓回归通过；`reports/compatibility/p1-cluster-current-continuation234/cluster-report.json` 于 `2026-08-26T06:24:18.6493073Z` 为 `PASS`。
- continuation234 发布证据：`reports/compatibility/release-candidate-current-continuation234/release-candidate.json` 于 `2026-08-26T06:35:49.1892054Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 235

- 继续补齐 P1-EXE-002：`LimitOperator` 新增 bounded `NextBatch`，按 batch 跳过 offset、限制输出并传播子算子批量读取；`HashJoinOperator` 新增 bounded `NextBatch`，哈希构建侧通过 `nextOperatorBatch` 读取，保留既有连接结果与外连接语义。新增 `TestVolcanoBatchOperatorsPropagateThroughLimit` 与 `TestVolcanoHashJoinBuildsFromBatches`，旧实现先确认两条路径退回逐行读取，修复后通过。
- engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation235/cluster-report.json` 于 `2026-08-26T06:41:06.2587989Z` 为 `PASS`。
- continuation235 发布证据：`reports/compatibility/release-candidate-current-continuation235/release-candidate.json` 于 `2026-08-26T06:52:35.6846019Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 236

- 对 continuation235 的批量 Sort/Limit/HashJoin 执行路径完成新一轮独立发布复核；本轮未新增生产代码，专门确认批量算子改动在当前工作区和 Connector/J 集成环境中仍可发布。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation236/cluster-report.json` 于 `2026-08-28T05:57:32Z` 为 `PASS`。
- 发布证据：`reports/compatibility/release-candidate-current-continuation236/release-candidate.json` 于 `2026-08-28T06:08:35Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 237

- 对批量算子与物理计划执行基线完成另一轮独立门禁复核；该门禁在 continuation238 的关联 Apply/子查询与列级 CHECK 改动前启动，因此只作为基线证据，不替代 continuation238 的新代码验证。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation237/cluster-report.json` 于 `2026-08-28T06:12:43Z` 为 `PASS`。
- 发布证据：`reports/compatibility/release-candidate-current-continuation237/release-candidate.json` 于 `2026-08-28T06:23:43Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 238

- 继续补齐 P1-EXE-003/P1-SQL-001：Volcano `ApplyOperator` 修复 `SEMI` 状态清理顺序，确保每个匹配的外层行只返回一次；ParallelExecutor 支持关联 Apply 按外层行重执行右侧计划，并向嵌套 Selection/Projection/Values 传播限定外层列；关联物理 Subquery 在已有外层上下文时可执行，顶层缺少上下文时仍返回明确错误。新增 SEMI 多行回归、关联 PhysicalApply 与关联 PhysicalSubquery 回归，均先失败后转绿。
- 继续补齐 P1-SQL-004：解析器兼容普通列级 `CHECK (...)`、命名 `CONSTRAINT ... CHECK (...)` 及 `ENFORCED/NOT ENFORCED` 后缀的常见语法，保留约束表达式进入现有持久化与运行时校验路径；新增解析器和存储集成回归，确认非法 INSERT 被拒绝。
- plan/sqlparser/engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation238/cluster-report.json` 于 `2026-08-28T06:25:01Z` 为 `PASS`。
- continuation238 发布证据：`reports/compatibility/release-candidate-current-continuation238/release-candidate.json` 于 `2026-08-28T06:35:54Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 239

- 继续补齐 P1-EXE-003：Volcano HashJoin 不再假设连接键位于输出第一列，按等值条件提取实际单列/复合列键，并对 NULL/类型保留稳定键编码；新增“连接列不在第一列”回归，旧实现先失败，修复后通过。
- engine 专项通过；`reports/compatibility/p1-cluster-current-continuation239/cluster-report.json` 于 `2026-08-28T06:41:53Z` 为 `PASS`。
- continuation239 发布证据：`reports/compatibility/release-candidate-current-continuation239/release-candidate.json` 于 `2026-08-28T06:52:44Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 240

- 继续补齐 P1-SQL-004：CHECK 约束现在保留可选名称与 `ENFORCED`/`NOT ENFORCED` 状态；创建表和 ALTER TABLE ADD CHECK 均持久化该状态，运行时只校验 enforced 约束，`INFORMATION_SCHEMA.CHECK_CONSTRAINTS.ENFORCED` 返回 YES/NO。新增解析器、创建表和 ALTER 表回归，旧实现先确认 NOT ENFORCED 仍被错误拒绝，修复后通过。
- 继续补齐索引维护路径：`UpdateOperator.checkIndexColumnsChanged` 遍历主键及所有二级索引的全部列，不再假设索引键位于第一列；新增非首列索引键更新回归，旧实现先失败，修复后通过。
- sqlparser/engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation240/cluster-report.json` 于 `2026-08-28T07:01:40Z` 为 `PASS`。
- continuation240 发布证据：`reports/compatibility/release-candidate-current-continuation240/release-candidate.json` 于 `2026-08-28T07:12:19Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`。

## Continuation 241

- 继续补齐 P1-SQL-004：支持 `ALTER TABLE ... ALTER CHECK <name> ENFORCED/NOT ENFORCED`，状态变更原子写入 `.frm`，运行时校验和 `INFORMATION_SCHEMA.CHECK_CONSTRAINTS` 同步反映切换结果；新增“先放宽、再恢复强制”的回归。
- continuation241 的全仓 `go test ./... -count=1 -timeout 5m`、cluster smoke 均通过；`reports/compatibility/p1-cluster-current-continuation241/cluster-report.json` 于 `2026-08-28T07:26:21Z` 为 `PASS`。
- continuation241 发布证据：`reports/compatibility/release-candidate-current-continuation241/release-candidate.json` 于 `2026-08-28T07:37:06Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 242

- 继续补齐 P1-OPT-004/P1-OPT-005：计划表达式层新增独立 `OpNotIn`，并修复 `IN/NOT IN` 遇到 NULL 时的 SQL UNKNOWN 语义；`NOT IN` 不再误降级为等值比较，也不再因 `nil` 强转 bool 触发 panic。CNF 原子谓词识别、否定映射和字符串格式化同步覆盖。
- plan 专项测试通过；`reports/compatibility/p1-cluster-current-continuation242/cluster-report.json` 于 `2026-08-28T07:37:53Z` 为 `PASS`。
- continuation242 最终发布证据：`reports/compatibility/release-candidate-current-continuation242/release-candidate.json` 于 `2026-08-28T07:48:53Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 243

- 继续补齐 P1-EXE-004：计划表达式比较现在按 MySQL 规则处理混合数值类型和数值字符串（如 `'1' = 1`、`'2' < 10`），同时保留纯字符串的字典序比较；新增混合类型比较回归，避免 JDBC/并行物理计划因 Go 值类型差异产生错误结果。
- plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation243/cluster-report.json` 于 `2026-08-28T07:52:15Z` 为 `PASS`。
- continuation243 最终发布证据：`reports/compatibility/release-candidate-current-continuation243/release-candidate.json` 于 `2026-08-28T08:03:33Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 244

- 继续补齐 P1-OPT-005/P1-EXE-004：修复表达式优化器对可空算术的错误零元折叠，`nullable_col * 0`/`0 * nullable_col` 保留运行时求值并返回 SQL NULL；表达式加减乘除遇 NULL 统一返回 NULL，除零仍明确报错。新增 NULL 算术回归并修正旧的非 MySQL 断言。
- engine/plan 定向回归通过；`reports/compatibility/p1-cluster-current-continuation244/cluster-report.json` 于 `2026-08-28T08:08:37Z` 为 `PASS`。
- continuation244 最终发布证据：`reports/compatibility/release-candidate-current-continuation244/release-candidate.json` 于 `2026-08-28T08:19:49Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 245

- 继续补齐 P1-OPT-004/P1-OPT-005：计划表达式层新增独立 `OpNullSafeEQ`，正确实现 `<=>` 的双 NULL、单 NULL 和普通值语义；CNF 原子谓词、比较映射和字符串化同步支持，避免把 NULL-safe equality 错当普通 `=`。
- plan/engine 定向回归与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation245/cluster-report.json` 于 `2026-08-28T08:22:57Z` 为 `PASS`。
- continuation245 最终发布证据：`reports/compatibility/release-candidate-current-continuation245/release-candidate.json` 于 `2026-08-28T08:33:49Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 246

- 继续补齐 P1-EXE-004：计划表达式算术现在支持 MySQL 常见的数值字符串转换（如 `'2' + 3`、`'2.5' * 2`），并保留 NULL 算术返回 NULL、除零返回明确错误的语义；新增算术兼容回归。
- plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation246/cluster-report.json` 于 `2026-08-28T08:37:00Z` 为 `PASS`。
- continuation246 最终发布证据：`reports/compatibility/release-candidate-current-continuation246/release-candidate.json` 于 `2026-08-28T08:47:37Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 247

- 继续补齐 P1-EXE-004/P1-OPT-005：LIKE 执行统一使用转义安全的模式编译，`%`/`_` 保持 MySQL 通配符，`.` 等正则元字符按字面量匹配，反斜杠可转义通配符；BinaryOperation 与 LikeExpression 两条路径均覆盖回归。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；`reports/compatibility/p1-cluster-current-continuation247/cluster-report.json` 于 `2026-08-28T08:50:52Z` 为 `PASS`。
- continuation247 最终发布证据：`reports/compatibility/release-candidate-current-continuation247/release-candidate.json` 于 `2026-08-28T09:01:22Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 248

- 继续补齐 P1-OPT-001/P1-OPT-004：索引访问优化命中索引后保留上层 `LogicalSelection`，不再因替换成 `LogicalIndexScan` 丢失未由存储层证明已求值的 WHERE 谓词；`IS NULL`/`IS NOT NULL` 索引条件保留各自语义，NULL-safe equality `<=>` 也可作为安全索引候选。
- 继续补齐 P1-OPT-005/P1-EXE-004：存储集成 WHERE 求值引入 SQL 三值逻辑，修复 `NOT UNKNOWN`、`UNKNOWN AND/OR`、`NOT BETWEEN` 和多段 WHERE 条件只执行第一段的问题；比较路径补齐数值列与数值字符串的 MySQL 风格转换，并保留纯字符串字典序。
- 新增索引、逻辑计划、WHERE 三值逻辑与混合类型比较回归。首轮发布门禁发现分区数值排序和 JOIN DECIMAL 排序仍受小写类型元数据影响，已在后续 continuation249 修复；因此 continuation248 的 release 结果不作为发布证据。

## Continuation 249

- 修复小写 `ColumnMeta.Type` 未进入数值转换分支的问题；单表排序、分区重组后的数值排序以及 JOIN 排序现在按真实数值类型处理，JOIN 排序额外保留列类型上下文并支持 DECIMAL/浮点字符串值。
- 分区重组、JOIN 排序、plan/engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 均通过。

## Continuation 250

- 集群证据：`reports/compatibility/p1-cluster-current-continuation250/cluster-report.json` 于 `2026-08-28T09:47:39.5301107Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation250/release-candidate.json` 于 `2026-08-28T09:58:23Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 251

- 继续审计计划构建链路：补齐 `IS TRUE`/`IS FALSE`/`IS NOT TRUE`/`IS NOT FALSE` 的计划表达式与 NULL 规则；补齐一元 `+/-/!/~`、搜索/简单 `CASE`、`SUBSTRING` AST，以及十六进制/位字面量，避免这些解析成功的表达式在 `PlanBuilder.buildExpr` 中静默变成 `nil`。
- 新增 plan 层构建与求值回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation251/cluster-report.json` 于 `2026-08-28T10:13:27.2558484Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation251/release-candidate.json` 于 `2026-08-28T10:24:05.6505251Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 252

- 继续补齐计划层动态谓词：`BETWEEN`/`NOT BETWEEN` 现在保留列或表达式上下界并在运行时按当前行求值；仅常量上下界继续进入索引下推和选择率估算，动态上下界安全地保留残余过滤。
- 新增动态 `BETWEEN` 构建与执行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation252/cluster-report.json` 于 `2026-08-28T10:28:27.7357417Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation252/release-candidate.json` 于 `2026-08-28T10:39:10.8203123Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 253

- 继续补齐计划层动态集合谓词：`IN (列/表达式, ...)` 现在构建为可逐行求值的 `TupleExpression`，不再只保留常量成员而静默丢弃动态成员；常量 tuple 仍走原有索引候选路径，动态 tuple 保留为残余过滤。
- 新增动态 `IN` 构建与执行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation253/cluster-report.json` 于 `2026-08-28T10:42:18.4268934Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation253/release-candidate.json` 于 `2026-08-28T10:53:02.3680207Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 254

- 继续补齐 SQL truth-value 执行一致性：存储集成 WHERE 现在支持裸数值/布尔表达式作为谓词，`WHERE active`、`WHERE NOT active` 和 NULL 均按 MySQL 三值逻辑处理；计划层 `NOT` 同步支持数值布尔转换。
- 新增裸布尔谓词回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation254/cluster-report.json` 于 `2026-08-28T10:56:41.3639945Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation254/release-candidate.json` 于 `2026-08-28T11:07:23.8460723Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、100 次 concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 255

- 继续补齐 P1-OPT-001/P1-OPT-005：存储集成 WHERE 支持裸数值/布尔谓词；内连接等值键可安全传递单侧常量范围（`=`、`!=`、`<`、`<=`、`>`、`>=`），外连接不做该推导。
- 新增裸布尔谓词与内连接范围传递回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation255/cluster-report.json` 于 `2026/8/28 11:11:50` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation255/release-candidate.json` 于 `2026/8/28 11:22:42` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 256

- 继续补齐 P1-OPT-005/P1-EXE-004：常量折叠覆盖一元运算、`CASE`、动态 Tuple 和动态 `BETWEEN` 上下界；表达式规范化递归进入 `CASE`、Tuple、`BETWEEN`、`IS NULL` 和 `IS TRUE/FALSE` 子表达式，避免嵌套表达式漏掉安全改写。
- 新增嵌套表达式规范化与常量折叠回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation256/cluster-report.json` 于 `2026/8/28 11:29:18` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation256/release-candidate.json` 于 `2026/8/28 11:40:02` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 257

- 继续补齐 P1-OPT-005/P1-EXE-004：计划表达式 `AND/OR` 现在接受 MySQL 数值/数值字符串布尔上下文，同时保留 NULL 三值逻辑；旧版 `InExpression` 对 NULL 候选和 NULL 左值返回 UNKNOWN，不再触发空接口类型断言 panic。
- 新增布尔运算与 `IN` NULL 语义回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation257/cluster-report.json` 于 `2026/8/28 11:43:42` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation257/release-candidate.json` 于 `2026/8/28 11:54:24` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 258

- 继续补齐 P1-EXE-004：计划层 `LIKE`/`NOT LIKE` 两条执行路径现在按 MySQL 文本上下文将数值等标量转换为字符串，NULL 仍返回 UNKNOWN；新增数值参与 `LIKE` 的回归。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation258/cluster-report.json` 于 `2026/8/28 11:57:39` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation258/release-candidate.json` 于 `2026/8/28 12:08:20` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 259

- 继续补齐 P1-OPT-005：存储集成通用表达式求值现在覆盖直接比较、`NOT`、`AND/OR`、NULL-aware `IN`、Tuple，以及一元/算术 AST；这些表达式可用于 DML、约束和投影求值，不再统一落到“不支持的表达式类型”。
- 新增执行器级表达式回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation259/cluster-report.json` 于 `2026/8/28 12:11:59` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation259/release-candidate.json` 于 `2026/8/28 12:22:40` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 260

- 继续补齐 P1-OPT-005：常量 truth-value 简化现在支持数值/数值字符串形式的 `AND/OR` 恒等元和零元规则，并保持 NULL 不被错误折叠为 FALSE。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation260/cluster-report.json` 于 `2026/8/28 12:25:59` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation260/release-candidate.json` 于 `2026/8/28 12:36:35` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 261

- 继续补齐 P1-SQL-004：CHECK 约束运行时改用 SQL 三值逻辑判断，只有 TRUE/UNKNOWN 通过，FALSE 必须拒绝；修复了 `CHECK(value IS NOT NULL)` 在 NULL 值上被错误放行的问题，同时保留普通比较遇 NULL 的 UNKNOWN 通过规则。
- 新增 CHECK TRUE/FALSE/UNKNOWN 回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation261/cluster-report.json` 于 `2026/8/28 12:40:01` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation261/release-candidate.json` 于 `2026/8/28 12:50:46` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 262

- 继续补齐 P1-OPT-001：列依赖收集现在递归覆盖动态 `BETWEEN` 上下界、CASE、Tuple、一元表达式、`IS TRUE/FALSE` 等嵌套节点，避免跨 JOIN 表的过滤条件被错误下推；聚合谓词检测同步递归这些节点，避免 CASE/Tuple 中的聚合条件下推到聚合之前。
- 新增外连接动态 `BETWEEN` 安全性与 CASE 内聚合谓词回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation262/cluster-report.json` 于 `2026/8/28 12:55:16` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation262/release-candidate.json` 于 `2026/8/28 13:05:59` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 263

- 继续补齐 P1-OPT-003：旧版数值/字符串/时间/通用直方图构造器在 0 桶、空统计、恒定值和无效边界下不再除零、panic 或生成反向区间；恒定列收敛为单桶。
- 新增退化统计回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation263/cluster-report.json` 于 `2026-08-28T13:11:02.1462032Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation263/release-candidate.json` 于 `2026-08-28T13:22:51.9471768Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 264

- 继续补齐 P1-OPT-005/P1-SQL-003：`ON DUPLICATE KEY UPDATE VALUES(col)` 的专用 `ValuesFuncExpr` 现在由存储集成和传统 InsertOperator 两条执行路径统一求值；候选 INSERT 行值沿约束校验、更新落盘和二级索引同步链路传递，支持与其他更新表达式组合。
- 新增 `ValuesFuncExpr` 解析、求值和更新上下文回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation264/cluster-report.json` 于 `2026-08-28T13:30:50.7227202Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation264/release-candidate.json` 于 `2026-08-28T13:41:33.2772669Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 265

- 继续补齐 P1-OPT-005：存储集成表达式求值现在支持 `date_expr + INTERVAL n unit` 与 `date_expr - INTERVAL n unit`，并复用日期函数的 MySQL 兼容单位解析；`INTERVAL` AST 不再在更新、生成列和投影相关路径中直接报“不支持”。
- 新增日期间隔加减回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation265/cluster-report.json` 于 `2026-08-28T13:45:20.165451Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation265/release-candidate.json` 于 `2026-08-28T13:55:56.8518192Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 266

- 继续补齐 P1-OPT-005/P1-SQL-003：`INSERT ... VALUES (DEFAULT)` 和 `UPDATE ... SET column=DEFAULT` 现在按目标列元数据读取并规范化持久化默认值，覆盖字符串、数值及 `CURRENT_TIMESTAMP` 默认值路径。
- 新增 DEFAULT 表达式解析、INSERT 数据构造和 UPDATE 行重写回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation266/cluster-report.json` 于 `2026-08-28T14:00:06.1638208Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation266/release-candidate.json` 于 `2026-08-29T01:33:39.2767538Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 267

- 继续补齐 P1-SQL-003：传统 `InsertOperator` 的 `ON DUPLICATE KEY UPDATE` 现在复用存储集成表达式求值器处理常见标量函数；普通列从冲突行读取，`VALUES(col)` 仍从候选插入行读取，覆盖 `COALESCE/IFNULL` 等嵌套表达式。
- 新增 legacy ON DUP 标量表达式回归；引擎包与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation267/cluster-report.json` 于 `2026-08-29T01:41:44.6854641Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation267/release-candidate.json` 于 `2026-08-29T01:52:54.8256004Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 268

- 继续补齐 PX-LOG-002：组提交 worker 现在使用运行时窗口和批次上限配置，并在真实异步提交批次完成后记录提交数、批次数、fsync 次数、平均/最大延迟；补充关闭排空与真实批次统计回归。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation268/cluster-report.json` 于 `2026-08-29T01:55:30.3619062Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation268/release-candidate.json` 于 `2026-08-29T02:06:30.3984813Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 269

- 继续补齐 P1-OPT-003/P1-OPS-001：`OPTIMIZE TABLE` 现在执行真实表扫描并刷新优化器行数、列 distinct count 和索引统计；修复统计持久化重写 `.frm` 时遗漏 `storage_root_page` 导致 ANALYZE 后后续 SELECT/OPTIMIZE 丢失表空间根页的问题。
- 新增 OPTIMIZE 统计刷新与 ANALYZE 重启加载回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation269/cluster-report.json` 于 `2026-08-29T02:12:37.0772499Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation269/release-candidate.json` 于 `2026-08-29T02:23:13.5526076Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 270

- 继续补齐 PX-LOG-002：`RedoLogManager.Flush(untilLSN)` 现在只写入、同步和归档不超过目标 LSN 的 redo entries，目标之后的日志保留在 buffer，避免异步组提交越过事务提交边界。
- 新增目标 LSN 边界回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation270/cluster-report.json` 于 `2026-08-29T02:27:36.4216961Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation270/release-candidate.json` 于 `2026-08-29T02:38:11.4658554Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 271

- 继续补齐 PX-LOG-002：`RedoLogManager` 公开并校验运行时组提交配置（窗口、最大批次），并提供并发安全的统计快照 API；组提交能力现覆盖批次执行、提交边界、关闭排空、配置和观测。
- 新增组提交配置/统计 API 回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation271/cluster-report.json` 于 `2026-08-29T02:41:01.9991023Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation271/release-candidate.json` 于 `2026-08-29T02:52:03.7362817Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 272

- 继续补齐 P1-SQL-003/P1-OPT-003 的双入口一致性：旧 `executeAdminReadQuery` 的 `CHECK/ANALYZE/OPTIMIZE` 现在复用真实维护实现，`OPTIMIZE` 也会刷新统计而不是只返回静态 OK。
- 新增旧维护入口统计回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation272/cluster-report.json` 于 `2026-08-29T02:55:22.7951598Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation272/release-candidate.json` 于 `2026-08-29T03:06:53.7587977Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 273

- 继续补齐 PX-SQL-001：存储过程调用现在支持带限定名的 `CALL schema.routine(...)`，解析、参数模式/OUT-INOUT 传播和递归过程调用统一使用实际目标 schema，避免跨库调用错误读取当前默认库或把语句交给通用 SQL parser。
- 新增跨 schema qualified `CALL` 回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation273/cluster-report.json` 于 `2026-08-29T03:14:06.8816236Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation273/release-candidate.json` 于 `2026-08-29T03:25:51.8345845Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 274

- 继续补齐 PX-SQL-001：实现 `ALTER PROCEDURE`/`ALTER FUNCTION` 的常用 `SQL SECURITY DEFINER/INVOKER` 与 `COMMENT` 元数据持久化；`INFORMATION_SCHEMA.ROUTINES.SECURITY_TYPE` 和 `ROUTINE_COMMENT` 不再固定返回默认值，原有 ALTER EVENT 行为保持不变。
- 新增 routine ALTER 元数据回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation274/cluster-report.json` 于 `2026-08-29T03:30:26.738497Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation274/release-candidate.json` 于 `2026-08-29T03:40:38.7173006Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 275

- 继续补齐 PX-SQL-001 routine 元数据：CREATE routine 现在持久化 `COMMENT`；`INFORMATION_SCHEMA.ROUTINES` 按定义准确报告 `IS_DETERMINISTIC`（不会把 `NOT DETERMINISTIC` 误判为 YES）和 `SQL_DATA_ACCESS`（`NO/CONTAINS/READS/MODIFIES SQL DATA`），并保留 ALTER 后的 `SQL SECURITY`/comment。
- 新增 routine 定义元数据回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation275/cluster-report.json` 于 `2026-08-29T03:44:05.2108968Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation275/release-candidate.json` 于 `2026-08-29T03:56:15.2316212Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 276

- 继续补齐 PX-SQL-001 的权限边界：`SHOW CREATE PROCEDURE/FUNCTION` 现在对已认证会话校验全局 `SHOW_ROUTINE` 动态权限，并复用现有角色继承/effective grants；未认证或内部兼容调用保持可用，未授权会话返回明确拒绝。
- 新增 `SHOW_ROUTINE` 授权前后回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation276/cluster-report.json` 于 `2026-08-29T04:01:11.2728117Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation276/release-candidate.json` 于 `2026-08-29T04:12:25.3530447Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 277

- 继续补齐 PX-SQL-001 routine ALTER 元数据：`ALTER PROCEDURE/FUNCTION` 现在处理 `DETERMINISTIC/NOT DETERMINISTIC` 与 `NO/CONTAINS/READS/MODIFIES SQL DATA`，并将选项写回持久化定义，使 `INFORMATION_SCHEMA.ROUTINES` 反映 ALTER 后的真实值。
- 新增 ALTER routine 选项回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation277/cluster-report.json` 于 `2026-08-29T04:15:23.1780617Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation277/release-candidate.json` 于 `2026-08-29T04:26:38.923755Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 278

- 继续补齐 PX-STG-003/P1-OPS-001：`StorageManager.ReclaimSpace` 不再只返回 segment 的估算空闲字节，而是实际释放已空 extent、从 segment 所有权移除并更新分配/空间统计；`CHECK TABLE` 通过 legacy B+Tree adapter 校验 XMySQL 持久化 clustered-index record block，并在无扩展块时回退校验通用页头。
- 新增空 extent 回收回归；修正 XMySQL 扩展记录块与通用 InnoDB header 计数口径差异后，engine/manager 全量专项及全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation278/cluster-report.json` 于 `2026-08-29T04:34:24.0728207Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation278/release-candidate.json` 于 `2026-08-29T04:45:50.7732255Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 279

- 继续收紧 PX-STG-003：`StorageManager.OptimizeStorage` 不再无条件调用 `PreallocateSpace`，优化操作只执行碎片整理和空 extent 回收；需要扩容时仍由调用方显式调用预分配 API，避免 `OPTIMIZE` 无故增大表空间。
- 新增 `TestOptimizeStorageDoesNotPreallocateUnconditionally` 回归测试；变更后 Manager 存储优化专项通过，全仓 `go test ./...` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation279/cluster-report.json` 于 `2026-08-29T04:50:52.6273081Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation279/release-candidate.json` 于 `2026-08-29T05:02:19.8922935Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 280

- 继续修正存储分配统计：新 extent 首次分配页面现在同步更新 segment/global `TotalPages` 与剩余空间；自动释放超过保留阈值的空 extent 时同步扣减 segment/global free-space 统计，并在 extent-manager 释放失败时保留原状态。
- 补强外置 BLOB 管理器的边界行为：Blob ID 使用单调游标避免删除后复用，按 segment 元数据解析所属 tablespace，不再固定读写 space 0；空范围读取和截断长度不会触发下溢，读取到截断链时返回明确错误。
- 新增 Manager/BLOB 回归测试；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation280/cluster-report.json` 于 `2026-08-29T05:11:22.4392089Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation280/release-candidate.json` 于 `2026-08-29T05:22:44.8447086Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 281

- 继续完成 PX-STG-003 的物理尾部回收：新增 `IBD_File.TruncateToPages` 与 `IBDSpace.ShrinkToFit`，只按当前分配表计算尾部安全边界；`StorageManager.OptimizeStorage` 在实现提供该可选能力时自动调用，并通过加密 space wrapper 透传。
- 增加安全边界：至少保留一个完整 extent，截断后重置下一 extent/page 高水位，保证后续分配仍按 64 页边界对齐；活动尾部页存在时不会截断。
- 新增真实 IBD 文件大小、尾部活动页和再次分配对齐回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation281/cluster-report.json` 于 `2026-08-29T05:30:26.7383532Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation281/release-candidate.json` 于 `2026-08-29T05:41:24.1217683Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 282

- 继续补齐 P1-SQL-003：常见 `ALTER TABLE ... ENGINE=InnoDB` 现在走兼容 no-op 路径并保留表数据/元数据；非 InnoDB 引擎仍返回明确的 unsupported 错误，避免伪造存储引擎转换。
- 新增执行级回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation282/cluster-report.json` 于 `2026-08-29T05:46:10.2636631Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation282/release-candidate.json` 于 `2026-08-29T05:57:47.1031894Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 283

- 继续补齐 P1-SQL-003：`ALTER TABLE ... AUTO_INCREMENT=N` 现在校验目标值和 AUTO_INCREMENT 列，并更新持久化自增游标；后续缺省值插入从指定值生成，非法值/无自增列返回明确错误。
- 新增跨执行器持久化自增回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation283/cluster-report.json` 于 `2026-08-29T06:01:49.7484576Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation283/release-candidate.json` 于 `2026-08-29T06:12:43.5520019Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 284

- 继续补齐 P1-SQL-003 表级元数据：`CREATE TABLE ... COMMENT` 现在持久化到 `.frm`，`ALTER TABLE ... COMMENT` 可原子更新；`SHOW CREATE TABLE` 与 `INFORMATION_SCHEMA.TABLES.TABLE_COMMENT` 返回实际值，重载 `.frm` 时也恢复表注释。
- 新增创建/修改/查询/SHOW CREATE 全链路回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation284/cluster-report.json` 于 `2026-08-29T06:20:47.0493584Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation284/release-candidate.json` 于 `2026-08-29T06:31:53.598911Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 285

- 继续补齐 P1-SQL-003 列级元数据：`.frm` 中已有的列 COMMENT 现在在 `TableMeta` 和 `INFORMATION_SCHEMA.COLUMNS.REMARKS` 中正确恢复；`SHOW CREATE TABLE` 也会输出列注释，且信息架构查询按请求列正确投影。
- 新增创建/查询/SHOW CREATE 回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation285/cluster-report.json` 于 `2026-08-29T06:37:46.3545957Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation285/release-candidate.json` 于 `2026-08-29T06:49:18.9067288Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 286

- 继续补齐 P1-SQL-003 客户端元数据入口：新增 `SHOW FULL COLUMNS/FIELDS` 的真实 `.frm` 读取路径，返回列排序、类型、字符序、可空性、键、默认值、Extra、Privileges 和 Comment。
- 回归覆盖列注释在 INFORMATION_SCHEMA、SHOW CREATE 和 SHOW FULL COLUMNS 三条路径的一致性；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation286/cluster-report.json` 于 `2026-08-29T06:52:45.474329Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation286/release-candidate.json` 于 `2026-08-29T07:04:01.2103078Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 287

- 继续收紧 `SHOW FULL COLUMNS/FIELDS` 语义：字符列返回实际/默认字符序，数值和其他非字符列的 `Collation` 返回 `NULL`，避免把非字符字段伪装成字符字段。
- 增加非字符列 collation 回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation287/cluster-report.json` 于 `2026-08-29T07:07:04.6201387Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation287/release-candidate.json` 于 `2026-08-29T07:18:24.0983645Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 288

- 继续推进 PX-STG-001：`IOOptimizer` 新增可注入的 `PageCompression` durable-boundary 接口，写入路径（含批量写）压缩后端字节，读取/缓存/预读路径保持解压后的页面视图；现有 `manager.CompressionManager` 可直接作为该接口实现。
- 修复 `CompressionManager` 返回池内 buffer 切片导致后续压缩覆盖旧页的风险，并对截断压缩页头返回明确错误而不是越界 panic；新增 IO 边界、buffer 稳定性和截断头回归。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation288/cluster-report.json` 于 `2026-08-29T07:24:16.8219061Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation288/release-candidate.json` 于 `2026-08-29T07:36:26.5631233Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 289

- 继续补齐 PX-STG-001：新增 `InnodbCompressionConfig` 配置入口；`StorageManager` 现在可将 zlib 页面压缩接入默认 `StorageProvider` 与直接 `Space/FileTableSpace` 访问路径，并在启用加密时保持“先压缩、后加密”的组合顺序；新建/重载表空间会继承默认压缩策略，`SpaceInfo.IsCompressed` 反映实际策略。
- 新增配置解析、StorageManager 直接空间读写和压缩组合回归；manager、IO、engine 分包测试通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation289/cluster-report.json` 于 `2026-08-29T07:44:40.1674848Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation289/release-candidate.json` 于 `2026-08-29T07:55:49.6947144Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 290

- 继续收敛 P1-OPT-001：内连接等值条件的安全传递现在覆盖两侧 `IS NULL`/`IS NOT NULL` 与常量 `IN` 谓词，生成的谓词替换为对应 join-side 列；外连接仍不把条件推到 NULL 扩展侧。
- 新增内连接 NULL 可空性推导回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation290/cluster-report.json` 于 `2026-08-29T08:02:58.7340021Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation290/release-candidate.json` 于 `2026-08-29T08:13:21.2705348Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 291

- 继续补齐 PX-STG-001：`StorageManager` 新增压缩策略持久化，支持显式表空间压缩开关、全空间默认策略、排除空间和压缩参数在关闭/重启后的恢复；`AllSpaces=false` 不再错误地把已有空间隐式标成压缩，恢复后的空间会重新绑定压缩参数并保持页面读回一致。
- 新增 `TestStorageManagerPersistsCompressionPolicyAcrossRestart`，覆盖策略文件、显式空间、重启后的 `SpaceInfo.IsCompressed` 和压缩页读回；manager/config 专项与全仓 `go test ./... -count=1 -timeout 5m` 均通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation291/cluster-report.json` 于 `2026-08-29T08:22:27.0684785Z` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation291/release-candidate.json` 于 `2026-08-29T08:32:47.3441602Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 292

- 继续收敛 P1-OPT-001/P1-SQL-001：子查询优化器的自动递归入口现在保留独立 `LogicalSubquery` 边界，只递归优化其 `Subplan`；不会在缺少外层 child 时生成非法的一子节点 `LogicalApply`。显式拥有左右 row source 的 Apply/Join 改写入口保持不变。
- 继续收敛子查询执行生命周期：非关联标量子查询的 `NULL` 结果现在通过显式物化状态缓存，重复 `ExecuteForRow` 不会重复打开/扫描子计划；缺少子计划时返回明确错误。
- 新增 standalone-subquery plan 回归和 uncorrelated-NULL-scalar 执行器回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation292/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation292/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 293

- 继续收敛 P1-OPT-001/P1-SQL-001：非关联 `LogicalApply` 的 `SEMI/ANTI` 类型现在保持双 child 和原语义，不再错误降级成 `INNER`，避免重复外层行和错误暴露内层列；两侧子计划仍递归优化。
- 新增 `TestSubqueryOptimizerPreservesUncorrelatedSemiAndAntiApply`，并保留 standalone subquery boundary 与 NULL 标量缓存回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation293/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation293/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 294

- 继续收敛 P1-EXE-004：新增可复用的 `CompiledExpression` 热路径，支持常量、列引用、二元运算、`IS NULL`/`IS NOT NULL` 和 `BETWEEN`；投影与 Volcano 过滤谓词在算子初始化时预编译，暂不支持的函数、CASE、UNARY 等表达式保持解释器回退，确保语义不变。
- 新增编译表达式与投影接入回归，覆盖编译结果和解释器结果一致、复杂表达式回退以及投影实际使用编译函数；受影响包与全仓 `go test ./...` 均通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation294/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation294/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 295

- 继续收敛 P1-EXE-004：编译表达式路径扩展到 `UNARY`、搜索/简单 `CASE` 和动态元组成员判断，`IN/NOT IN` 的元组子表达式可在初始化时编译；不支持的函数及其他表达式仍按原解释器执行。
- 新增 unary、CASE、动态 tuple membership 的编译结果等价回归；plan/engine 专项测试通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation295/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation295/release-candidate.json` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 296

- 继续收敛 P1-OPT-001/P1-SQL-001：修正显式 `decorrelateSubquery` 入口，在没有 enclosing outer row source 时保留 correlated `LogicalSubquery` 边界，不再伪造单 child `LogicalApply`；只有拥有两侧 row source 的上层改写才允许构造 Apply/Join，去关联统计也不再误计数。
- 新增“无外层 row source 不去关联”回归，并更新旧去关联测试契约；subquery optimizer 全量专项通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation296/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation296/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 297

- 继续收敛 P1-EXE-004：编译表达式覆盖补充 `NOT` 与 `IS TRUE`/`IS FALSE`/`IS NOT TRUE`/`IS NOT FALSE`，保留 NULL、数值和布尔真值的原解释器语义。
- 新增组合布尔谓词的编译/解释结果等价回归；plan 专项测试通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation297/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation297/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 298

- 继续收敛 P1-EXE-004：legacy `SelectExecutor` 现在按表达式文本缓存解析后的 AST，并复用 plan 编译器执行安全的算术/谓词表达式；为保持旧执行器的字符串数值转换和大小写兼容，`CASE` 在该入口保留 AST 解释器回退，Volcano 入口仍使用已验证的 CASE 编译路径。
- 新增 legacy 投影编译缓存回归，并补充 CASE 语义回归，修复编译路径与 legacy coercion 不一致的问题。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation298/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation298/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 299

- 继续收敛 P1-EXE-004：编译器新增确定性标量函数白名单，函数参数先编译为可复用适配器，再复用原有 `Function.Eval` 的实现，覆盖常见字符串、数值、日期、NULL 处理和 JSON 基础函数；`NOW` 等非确定性函数以及不在白名单的函数继续回退解释器。
- 新增 `CONCAT` 动态列参数的编译/解释结果等价回归；既有不支持函数回退回归保持通过。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation299/cluster-report.json` 为 `PASS`。
- 最终发布证据：`reports/compatibility/release-candidate-current-continuation299/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 和 Connector/J 检查全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 300

- 继续收敛 P1-SEC-001：`ALTER USER ... REQUIRE NONE/SSL/X509` 现在按 MySQL 语义替换既有传输要求，`REQUIRE NONE` 可清除之前的 SSL/X509 强制状态，`REQUIRE SSL` 也会清除旧的 X509 标记；新增持久化状态与 `SHOW CREATE USER` 回归。
- 修复官方静态 `PROXY` 权限的授权生命周期：GRANT 校验不再把 PROXY 错判为未知动态权限；包含 `ON 'proxied'@'host'` 与 `TO 'grantee'@'host'` 两个账户身份的语句现在将权限写入真正的 grantee，并由 `SHOW GRANTS` 回归验证。
- 本轮专项测试与 `go test ./... -count=1 -timeout 5m` 通过；集群证据 `reports/compatibility/p1-cluster-current-continuation300/cluster-report.json` 于 `2026-08-29T10:52:05.7160634Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation300/release-candidate.json` 于 `2026-08-29T11:02:23.7826147Z` 为 `GO`；clean-data/build/unit/integration/go-core、crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、`exit_code=0`，JDBC 状态为 `PASS`。

## Continuation 301

- 继续收敛 P1-SEC-001：`mysql.user.ssl_type` 现在由持久化 `REQUIRE SSL/X509` 状态投影为 `SSL`/`X509`，`REQUIRE NONE` 投影为空，并由真实系统表查询回归验证。
- 补齐 `mysql.proxies_priv` 兼容入口：PROXY 授权会投影 grantee 的 `Host/User`、被代理账户的 `Proxied_host/Proxied_user`、`With_grant`、`Grantor` 和 `Timestamp` 形状，且与 `SHOW GRANTS` 和 durable account grant 保持一致。
- 本轮专项测试与 `go test ./... -count=1 -timeout 5m` 通过；集群证据 `reports/compatibility/p1-cluster-current-continuation301/cluster-report.json` 于 `2026-08-29T11:07:16.2526454Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation301/release-candidate.json` 于 `2026-08-29T11:17:43.6295902Z` 为 `GO`；clean-data/build/unit/integration/go-core、crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、`exit_code=0`，JDBC 状态为 `PASS`。

## Continuation 302

- 继续收敛 P1-SEC-001 授权元数据：`INFORMATION_SCHEMA.TABLE_PRIVILEGES`、`SCHEMA_PRIVILEGES` 和 `COLUMN_PRIVILEGES` 现在按实际权限输出 `IS_GRANTABLE=YES/NO`，不再把 `GRANT OPTION` 作为独立业务权限行；新增表级、库级和列级 `WITH GRANT OPTION` 回归。
- 本轮专项测试与 `go test ./... -count=1 -timeout 5m` 通过；集群证据 `reports/compatibility/p1-cluster-current-continuation302/cluster-report.json` 于 `2026-08-29T11:22:14.2495766Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation302/release-candidate.json` 于 `2026-08-29T11:32:35.3913517Z` 为 `GO`；clean-data/build/unit/integration/go-core、crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、`exit_code=0`，JDBC 状态为 `PASS`。

## Continuation 303

- 继续收敛 P1-SEC-001：`INFORMATION_SCHEMA.USER_PRIVILEGES.IS_GRANTABLE` 现在使用 `YES/NO` 形状，并过滤 `GRANT OPTION` 控制项，只返回实际全局权限；`mysql.global_grants.WITH_GRANT_OPTION` 继续保持其系统表使用的 `Y/N` 形状。
- 新增全局 `WITH GRANT OPTION` 的 `INFORMATION_SCHEMA.USER_PRIVILEGES` 回归，并保留表级、库级、列级授权元数据回归。
- 本轮专项测试与 `go test ./... -count=1 -timeout 5m` 通过；集群证据 `reports/compatibility/p1-cluster-current-continuation303/cluster-report.json` 于 `2026-08-29T11:37:33.9416469Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation303/release-candidate.json` 于 `2026-08-29T11:48:00.6001584Z` 为 `GO`；clean-data/build/unit/integration/go-core、crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、`exit_code=0`，JDBC 状态为 `PASS`。

## Continuation 304

- 继续收敛 P1-OPT-005：表达式归一化现在真正扁平化同类 boolean `AND/OR` 结合结构，并用三值逻辑（`TRUE/FALSE/NULL`）逐行对照原表达式验证；算术结合律不再重排，避免浮点舍入和字符串转数值差异。
- 计划级专项测试与 `go test ./... -count=1 -timeout 5m` 通过；集群证据 `reports/compatibility/p1-cluster-current-continuation304/cluster-report.json` 于 `2026-08-29T11:51:28.6340132Z` 为 `PASS`。
- continuation304 的首次发布门禁未形成报告，未计为通过；随后用独立目录重跑同一代码状态，见 continuation305。

## Continuation 305

- 对 continuation304 的发布验证重新执行并形成可采信证据：`reports/compatibility/release-candidate-current-continuation305/release-candidate.json` 于 `2026-08-29T12:13:40.7855358Z` 为 `GO`；clean-data/build/unit/integration/go-core、crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、`exit_code=0`，JDBC 状态为 `PASS`。

## Continuation 306

- 继续收敛 P1-SEC-001 账号生命周期：`DROP USER` 与 `DROP ROLE` 现在支持多个 `'user'@'host'` 目标，先完整校验目标再一次性提交；不存在目标会阻止整条语句产生部分删除，DROP ROLE 同时清理现有账户的角色、default role 和 admin option 绑定。
- 新增多用户、多角色、原子失败和绑定清理回归；本轮 `go test ./... -count=1 -timeout 5m` 通过，集群证据 `reports/compatibility/p1-cluster-current-continuation306/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation306/release-candidate.json` 于 `2026-08-29T12:29:53.73625Z` 为 `GO`；clean-data/build/unit/integration/go-core、crash-recovery、concurrency、observability 全部 `PASS`，Connector/J `test_count=136`、`exit_code=0`，JDBC 状态为 `PASS`。

## Continuation 307

- 继续收敛 P1-OPT-005：优化器表达式归一化现在覆盖 `LogicalValues.Exprs`，以及 CTE 的 `Query`、递归 CTE 的 `Anchor/Recursive` 和 CTE Statement 的 `Definitions/Body` 字段，嵌套 VALUES/CTE 不再绕过常量折叠与布尔规范化。
- 新增 VALUES、递归 CTE 和 CTE Statement 计划树回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`gofmt` 已执行。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation307/cluster-report.json` 于 `2026-08-29T12:34:53.4053533Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation307/release-candidate.json` 于 `2026-08-29T12:45:41.2885627Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 308

- 继续收敛 P1-OPT-002：列裁剪现在递归处理 CTE 的独立计划字段，包括 `LogicalCTE.Query`、递归 CTE 的 `Anchor/Recursive` 和 CTE Statement 的 `Definitions/Body`，避免这些字段绕过列依赖分析。
- 新增真实表扫描 schema 的 CTE 列裁剪回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`gofmt` 已执行。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation308/cluster-report.json` 于 `2026-08-29T12:50:47.3024516Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation308/release-candidate.json` 于 `2026-08-29T13:01:20.7274052Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 309

- 继续收敛 P1-OPT-001：谓词下推现在递归处理 CTE 的 `Query`、递归 `Anchor/Recursive` 和 CTE Statement 的 `Definitions/Body` 字段；同时补齐 CTE/递归 CTE/CTE Statement/UNION 的计划输出列发现和连接列归属分析。
- 新增 CTE 谓词下推回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`gofmt` 已执行。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation309/cluster-report.json` 于 `2026-08-29T13:05:13.6661684Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation309/release-candidate.json` 于 `2026-08-29T13:15:40.9892348Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 310

- 继续收敛 P1-OPT-001/P1-OPT-002：`LogicalCTEScan` 现在按所需列裁剪 schema，CTE 计划字段递归逻辑避免列依赖和谓词下推绕过独立字段；普通 CTE、递归 CTE、CTE Statement 和 UNION 的输出列推导保持可用于后续连接归属分析。
- 新增 CTE Scan 列裁剪回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 无 whitespace 错误（仅保留既有 LF/CRLF 转换提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation310/cluster-report.json` 于 `2026-08-29T13:18:21.9398488Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation310/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 311

- 继续收敛 P1-OPT-001：索引访问优化现在递归进入 CTE 的 `Query`、递归 `Anchor/Recursive` 和 CTE Statement 的 `Definitions/Body` 字段；CTE 内的可索引等值谓词可选择对应二级索引，同时保留上层残余 Selection。
- 新增 CTE 内 `idx_col1` 访问路径回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation311/cluster-report.json` 于 `2026-08-29T13:32:38.0126792Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation311/release-candidate.json` 于 `2026-08-29T13:43:06.3351364Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 312

- 继续收敛 P1-OPT-001：聚合消除阶段现在递归处理 CTE 的 `Query`、递归 `Anchor/Recursive` 和 CTE Statement 的 `Definitions/Body` 字段，CTE 内的安全 `MIN/MAX` 聚合可复用既有消除规则。
- 新增普通、递归和 CTE Statement 聚合消除回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation312/cluster-report.json` 于 `2026-08-29T13:47:15.0819280Z` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation312/release-candidate.json` 于 `2026-08-29T13:57:52.2034000Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 313

- 继续收敛 P1-SEC-001 账号授权生命周期：普通对象权限与 PROXY 权限的 `GRANT ... TO a,b`、`REVOKE ... FROM a,b` 现在解析授权子句中的全部目标账户，先完整校验目标再一次性更新，避免只更新最后一个账户或在目标缺失时产生部分授权。
- 新增多账户对象权限、批量撤销、缺失目标原子性和多目标 PROXY 回归；定向 engine 测试与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation313/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation313/release-candidate.json` 于 `2026-08-29T14:14:42.3374164Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 314

- 继续收敛 P1-SEC-001 账号生命周期：`CREATE USER`、`CREATE ROLE`、`ALTER USER` 现在支持多个账户目标，并在统一追加/更新前完整校验目标与权限；`RENAME USER` 增加源账户存在性、重复源和重复目标校验，避免批量操作产生部分或重复状态。
- 新增多账户创建、批量 ALTER、批量重命名原子性回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation314/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation314/release-candidate.json` 于 `2026-08-29T14:33:36.7174412Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 315

- 继续收敛 P1-SEC-001 特性级权限路由：dispatcher 现在收集常见查询中的全部直接表引用，JOIN 的每张表都会执行表级权限检查；GRANT/REVOKE 的账户 `FROM` 子句、JOIN `ON` 条件和 `ON DUPLICATE` 控制语法不会被误判为表名，复杂派生表继续保守回退。
- 新增 JOIN 多表权限检查与第二张表拒绝回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation315/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation315/release-candidate.json` 于 `2026-08-29T14:49:35.3360761Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 316

- 继续收敛 P1-OPT-001：谓词下推现在进入 `LogicalApply`，INNER Apply 可将左右单侧谓词分别下推；LEFT/SEMI/ANTI Apply 的右侧谓词保留在边界上方，避免改变 NULL 扩展与存在性语义。
- 继续收敛 P1-SEC-001：dispatcher 的特性级权限路由现在对常见 JOIN 检查全部直接表引用，并排除 JOIN `ON` 条件与 GRANT/REVOKE 账户子句误识别。
- 新增 Apply 谓词边界和多表权限拒绝回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation316/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation316/release-candidate.json` 于 `2026-08-29T15:04:00.000447Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 317

- 继续收敛 P1-OPT-002：UNION 输出列发现现在以首分支定义列名和列序，列裁剪按输出 ordinal 映射到每个分支；不再把多个分支的列名直接拼接，避免需求列错位或裁剪失效。
- 新增 UNION 输出列序和双分支列裁剪回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation317/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation317/release-candidate.json` 于 `2026-08-29T15:18:28.7725547Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 318

- 继续收敛 P1-OPT-001/P1-OPT-005：`MIN/MAX` 投影简化现在递归处理 CTE、递归 CTE、CTE Statement 和 UNION 分支，CTE 内安全的投影消除不再被计划字段边界跳过。
- 新增 CTE/递归 CTE/CTE Statement 投影简化回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation318/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation318/release-candidate.json` 于 `2026-08-29T15:32:48.2509062Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 319

- 继续收敛 P1-OPT-001/P1-OPT-002：UNION 上方仅引用输出列的过滤条件现在会安全复制到每个分支；无法证明某分支具备所需列时保留上层过滤，避免错误下推。
- 新增 UNION 分支谓词下推回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation319/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation319/release-candidate.json` 于 `2026-08-29T15:46:28.2228393Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 320

- 继续收敛 P1-SQL-003：`ALTER TABLE ... FORCE` 现在作为兼容 no-op 进入已有元数据原子路径；不伪造后台物理重建，但常见 MySQL 维护语句不再被误报为 unsupported。
- 新增 `FORCE` 解析回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation320/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation320/release-candidate.json` 于 `2026-08-29T15:59:56.0373528Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 321

- 本轮验证发现尝试接管 `ADD/DROP PRIMARY KEY` 会绕过已有 executor 的重复值/NULL 校验，已依据失败回归撤回重复路径；主键 DDL 继续由原有完整实现处理，仅保留并验证 `ALTER TABLE ... FORCE` 兼容 no-op。
- 原有主键重复/NULL 拒绝、主键增删约束元数据和 FORCE 解析回归均通过；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation321/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation321/release-candidate.json` 于 `2026-08-29T16:16:55.2037095Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 322

- 继续收敛 P1-OPT-001：LEFT/RIGHT 等值连接现在可从保留侧单列谓词安全推导 nullable 侧谓词；支持普通比较、`IS NULL`/`IS NOT NULL`、`IN`、`LIKE` 和常量边界 `BETWEEN`，原保留侧过滤仍保留，NULL 扩展侧的原始过滤仍不下推。
- 新增外连接比较谓词及结构化谓词推导回归；`go test ./server/innodb/plan -count=1`、全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation322/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation322/release-candidate.json` 于 `2026-08-29T16:36:22.5112734Z` 为 `GO`；clean-data/build/unit/integration/go-core、3 次 crash-recovery、并发、observability 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 323

- 继续收敛 P1-SEC-001：权限路由现在按 DML 对象区分常见 `INSERT ... SELECT` 的目标表与来源表（目标检查 `INSERT`、来源检查 `SELECT`）；`INSERT ... ON DUPLICATE KEY UPDATE` 目标表同时检查 `UPDATE`，并避免把更新表达式误识别为表名。
- 新增目标/来源权限分离与重复键更新权限回归；dispatcher 专项、全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation323/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation323/release-candidate.json` 于 `2026-08-29T16:53:48Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 324

- 继续收敛 P1-SEC-001：`CREATE TABLE ... AS SELECT` 现在对目标表检查 `CREATE`、对来源表检查 `SELECT`；`CREATE TABLE ... LIKE ...` 支持普通及 `IF NOT EXISTS` 目标/来源拆分，普通查询中的 `LIKE` 不会被误识别为表名。
- 新增建表复制/建表查询权限路由回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation324/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation324/release-candidate.json` 于 `2026-08-29T17:11:03Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 325

- 继续收敛 P1-SEC-001：单目标 `UPDATE ... JOIN` 现在对目标表检查 `UPDATE`、连接来源检查 `SELECT`；单目标 `DELETE ... JOIN/USING` 对目标表检查 `DELETE`、连接来源检查 `SELECT`，多目标 DML 保持保守检查。
- 新增 UPDATE JOIN / DELETE JOIN 权限路由回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation325/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation325/release-candidate.json` 于 `2026-08-29T17:24:37Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 326

- 继续收敛 P1-SEC-001：已有 `REPLACE` 执行路径现在被 dispatcher 正确识别；普通 `REPLACE` 检查目标表 `INSERT + DELETE`，`REPLACE ... SELECT` 另外检查来源表 `SELECT`。
- 新增 REPLACE 与 REPLACE SELECT 权限回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation326/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation326/release-candidate.json` 于 `2026-08-29T17:38:27Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 327

- 继续收敛 P1-OPT-004：索引条件提取现在识别 parser 生成的 BinaryOperation `LIKE`/`NOT LIKE`/`IN`/`NOT IN`；前缀 `LIKE` 才进入可下推路径，模糊 `LIKE` 继续保守回退。
- 新增 binary LIKE index-pushdown 回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation327/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation327/release-candidate.json` 于 `2026/8/29 17:52:21` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 328

- 继续收敛 P1-OPT-004：Binary `IN`/`NOT IN` 现在只有在右值为非空常量列表时才生成索引候选，避免异常或不完整 AST 的标量右值被误下推；Binary `NOT LIKE` 复用前缀模式安全性检查，前缀模式可进入候选，模糊模式保守回退。
- 新增 Binary `NOT IN`、Binary `NOT LIKE` 与非法标量 `IN` 右值回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation328/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation328/release-candidate.json` 于 `2026/8/29 18:07:53` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 329

- 继续收敛 P1-EXE-004：独立的 `InExpression`/`LikeExpression` AST 现在可进入 `CompileExpression`，分别复用 `evalIn`/`evalLike`，并保留 NULL/UNKNOWN 结果语义，避免因 AST 形态不同而无谓回退解释器。
- 新增结构化 `IN`/`LIKE` 编译 evaluator 回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation329/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation329/release-candidate.json` 于 `2026/8/29 18:21:41` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 330

- 继续收敛 PX-SQL-002：触发器运行时现在按创建顺序构建同表/同事件/同 timing 的触发器组，并通过拓扑排序遵守 `FOLLOWS`/`PRECEDES`；BEFORE/AFTER 共用该顺序，AFTER 简单主体会剥离排序子句后再执行副作用。
- 新增 BEFORE `FOLLOWS`/`PRECEDES` 与 AFTER 有序副作用回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation330/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation330/release-candidate.json` 于 `2026/8/29 18:43:03` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 331

- 继续收敛 PX-SQL-002：普通重复 `CREATE TRIGGER` 现在返回“already exists”错误并保留原定义；`CREATE OR REPLACE TRIGGER` 仍可原子覆盖已有定义。
- 新增重复创建与 `OR REPLACE` 回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation331/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation331/release-candidate.json` 于 `2026/8/29 18:56:41` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 332

- 继续收敛 PX-SQL-002：带 `FOLLOWS/PRECEDES` 的触发器现在在创建前校验引用必须存在且属于同一表/事件/timing 组，不满足时拒绝创建并不落盘。
- 新增缺失排序引用回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation332/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation332/release-candidate.json` 于 `2026/8/29 19:10:02` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 333

- 继续收敛 P1-SEC-001：`CREATE OR REPLACE TRIGGER` 现在与普通 `CREATE TRIGGER` 一样路由到专用 `TRIGGER` 权限，不再错误使用通用 `CREATE` 权限。
- 新增替换触发器权限路由回归；dispatcher 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation333/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation333/release-candidate.json` 于 `2026/8/29 19:23:40` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 334

- 继续收敛 P1-OPT-004：索引条件提取现在支持常量左置比较；`10 < col1` 等安全反转为 `col1 > 10`，`=`/`<>`/`<=`/`>=`/`<=>` 同理；`IN`/`LIKE` 不做不安全左右置换。
- 新增 constant-left comparison index-pushdown 回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation334/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation334/release-candidate.json` 于 `2026/8/29 19:39:25` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 335

- 继续收敛 P1-OPT-001/P1-SQL-001：`optimizeInSubquery` 与 `optimizeExistsSubquery` 在只拿到子查询节点、没有外层行源时不再制造非法的单子节点 `LogicalApply`，而是保留 `LogicalSubquery` 边界；只有拥有外层与内层两个 child 的上层重写才能物化 SEMI Apply。
- 新增/调整独立 IN/EXISTS 子查询边界回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation335/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation335/release-candidate.json` 于 `2026/8/29 19:56:02` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 336

- 继续收敛 P1-EXE-004/P1-SQL-001：计划构建器现在正确映射 MySQL `DIV` 与 `MOD/%`，表达式解释执行和编译执行均支持整数除法、取模、NULL 传播与除零错误；此前这些 parser 已接受的运算会错误回落为加法。
- 新增 `BuildExpression` 的 `MOD/%` 与 `DIV` 回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation336/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation336/release-candidate.json` 于 `2026/8/29 20:11:03` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 337

- 继续收敛 P1-EXE-004/P1-SQL-001：计划构建器、解释执行与编译执行现在正确支持 MySQL `&`、`|`、`^`、`<<`、`>>`，采用 64 位整数位运算/移位结果并保留 NULL 传播；此前这些 parser 已接受的运算会错误落到加法。
- 新增 bitwise/shift 计划构建回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation337/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation337/release-candidate.json` 于 `2026/8/29 20:24:50` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 338

- 继续收敛 P1-EXE-004/P1-SQL-001：计划构建器现在将 MySQL JSON `->` 映射为 `JSON_EXTRACT`，将 `->>` 映射为 `JSON_UNQUOTE(JSON_EXTRACT(...))`；`JSON_UNQUOTE` 同时进入编译 evaluator 白名单，避免 parser 已接受的 JSON 操作符被错误执行为字符串加法。
- 新增 JSON `->`/`->>` 计划构建与执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation338/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation338/release-candidate.json` 于 `2026/8/29 20:38:37` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 339

- 继续收敛 P1-EXE-004/P1-SQL-001：计划构建器、解释执行与编译执行现在正确支持 MySQL `REGEXP`/`NOT REGEXP` 比较，返回匹配布尔值并保留 NULL 与非法模式错误传播；此前 parser 已接受的比较会错误落到普通等值比较。
- 新增 REGEXP/NOT REGEXP 计划构建与执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation339/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation339/release-candidate.json` 于 `2026/8/29 20:52:30` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 340

- 继续收敛 P1-EXE-004：编译 evaluator 白名单扩展到已有 `Function.Eval` 支持的确定性正则、JSON、日期、数值、字符串、格式化和网络/摘要函数；`REGEXP_LIKE` 等常用函数不再因白名单缺失而回退解释器。
- 新增确定性 `REGEXP_LIKE` 编译执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation340/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation340/release-candidate.json` 于 `2026/8/29 21:06:11` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 341

- 继续收敛 P1-SEC-001：分发层现在识别 `TRUNCATE TABLE` 并要求 MySQL 对应的 `DROP` 权限，不再将该语句归入 `OTHER` 后错误检查 `SELECT`。
- 新增 TRUNCATE 权限路由回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation341/cluster-report.json` 为 `PASS`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation341/release-candidate.json` 于 `2026/8/29 21:19:23` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 342

- 继续收敛 P1-EXE-003：并行排序比较器现在对字符串和字节字符串使用字典序；数值仍按数值比较，NULL 顺序与现有升/降序规则保持一致，修复非数字文本全部被当作 `0` 导致 `ORDER BY` 保持输入顺序的问题。
- 新增文本排序回归；专项排序测试与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation342/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 21:26:39`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation342/release-candidate.json` 于 `2026/8/29 21:37:42` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 343

- 继续收敛 P1-SQL-002：逻辑/物理 UNION 现在保留并执行顶层 `ORDER BY` 与 `LIMIT`（含 offset）；`UNION ALL` 或 DISTINCT 合并完成后再排序和分页，`ORDER BY` 序号按 UNION 首分支输出列定位。
- 新增 `SELECT 2 UNION ALL SELECT 1 ORDER BY 1 LIMIT 1` 计划构建与端到端并行执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation343/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 21:42:37`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation343/release-candidate.json` 于 `2026/8/29 21:53:39` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 344

- 继续收敛 P1-SQL-002：普通 `SELECT` 计划现在保留顶层 `ORDER BY` 与 `LIMIT`（含 offset），投影执行完成后按输出列排序并分页；`LIMIT 0` 不再错误返回一行。
- 新增普通 SELECT 顶层 LIMIT 端到端回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation344/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 21:56:49`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation344/release-candidate.json` 于 `2026/8/29 22:07:58` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 345

- 继续收敛 P1-SQL-002：普通 `SELECT` 的 `DISTINCT` 现在进入逻辑/物理投影计划；物理执行先对完整投影行去重，再执行顶层排序与 LIMIT，避免分页前保留重复结果。
- 新增重复行 `DISTINCT + LIMIT` 端到端回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation345/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 22:10:51`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation345/release-candidate.json` 于 `2026/8/29 22:21:54` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 346

- 继续收敛 P1-SQL-002：计划构建器现在保留 `GROUP BY ... HAVING ...`，并将 HAVING 放在聚合之后、最终投影之前的 Selection 节点中；无法构建的 HAVING 表达式会显式报错。
- 新增 HAVING 计划树回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation346/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 22:25:03`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation346/release-candidate.json` 于 `2026/8/29 22:35:55` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 347

- 继续收敛 P1-EXE-004：编译 evaluator 白名单现在覆盖已有确定性网络/摘要函数 `MD5`、`SHA`/`SHA1`、`SHA2`、`CRC32`、`INET_ATON`/`INET_NTOA`、IP 判断及 UUID 二进制转换；这些函数不再因白名单遗漏而回退解释器。
- 新增 `MD5` 与 `INET_ATON` 编译执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation347/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 22:39:52`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation347/release-candidate.json` 于 `2026/8/29 22:50:44` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 348

- 继续收敛 P1-EXE-004：编译 evaluator 白名单现在覆盖已有确定性日期函数 `YEAR`、`MONTH`、`QUARTER`、`DAY`、`HOUR`、`MINUTE`、`SECOND`、`DATE`、`TIME`、`WEEKDAY`、`DAYOFWEEK`、`DAYOFYEAR`、`LAST_DAY`，避免这些函数无必要地回退解释器。
- 新增日期函数编译执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation348/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 22:53:28`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation348/release-candidate.json` 于 `2026/8/29 23:04:21` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 349

- 继续收敛 P1-EXE-004：编译 evaluator 白名单补齐已有确定性 `OCTET_LENGTH`，保持 UTF-8 字节长度语义，避免从编译路径不必要地回退解释器。
- 新增 Unicode `OCTET_LENGTH` 编译执行回归；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation349/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 23:07:07`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation349/release-candidate.json` 于 `2026/8/29 23:18:11` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 350

- 继续收敛 P1-OPT-002/P1-EXE-003：逻辑投影现在按实际投影表达式维护输出列顺序，并保留 `AS` 别名；物理投影和并行执行行上下文同步传递输出列名，避免 `SELECT name, id` 被误识别为子表的 `[id, name]`，也避免上层算子使用错误的列名。
- 引擎侧物理计划生成现在保留逻辑投影的 `DISTINCT`、`ORDER BY`、`OFFSET`、`LIMIT` 和输出列名属性，不再只复制表达式而丢失执行语义。
- 新增投影顺序、列别名、物理投影属性以及“上层 Selection 使用投影别名”的并行执行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation350/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 23:26:37`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation350/release-candidate.json` 于 `2026/8/29 23:37:39` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 351

- 继续收敛 P1-EXE-003：引擎侧物理排序计划生成器现在将已生成的真实 child 挂回 `PhysicalSort`；此前虽然递归校验了 child，却返回无 child 的排序节点，Volcano 执行时会直接失败。
- 新增物理排序 child 链接回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation351/cluster-report.json` 为 `PASS`，生成时间 `2026/8/29 23:42:33`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation351/release-candidate.json` 于 `2026/8/29 23:54:07` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 352

- 继续收敛 P1-SQL-002/P1-EXE-003：并行聚合现在向上游暴露真实的分组列与聚合输出列名；HAVING 在聚合结果上解析 `COUNT`、`SUM` 等聚合表达式并复用已计算值，不再把聚合后的标量误当作明细列重新计算。
- 并行执行器现在能在 Selection、Join 等上层算子读取并执行嵌套的 `ParallelHashAgg`、`ParallelHashJoin`、`ParallelSort`，避免嵌套并行节点被通用 row reader 绕过。
- 新增 HAVING 依赖聚合输出的端到端并行执行回归；专项测试通过。全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation352/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 0:04:49`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation352/release-candidate.json` 于 `2026/8/30 0:16:21` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 353

- 继续收敛 P1-OPT-001：外连接等值条件推导现在支持只引用单个连接列的包裹表达式，例如从 `a.id = b.id` 与保留侧 `(a.id + 1) > 7` 推导 nullable 侧 `(b.id + 1) > 7`；递归克隆覆盖二元/一元/函数/CASE/元组及结构化谓词，并拒绝含无关列的表达式，原始保留侧谓词仍保留以维持 NULL-extended 行语义。
- 新增包裹列表达式的 LEFT JOIN 推导回归，先行失败后转绿；`server/innodb/plan` 专项测试与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation353/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 0:21:22`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation353/release-candidate.json` 于 `2026/8/30 0:32:21` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 354

- 继续收敛 P1-OPT-001：外连接等值条件推导现在支持只引用单个连接列的 `IS TRUE`/`IS FALSE` 和 `NOT` 布尔包装；递归替换保持包装节点与原有三值逻辑，含无关列的表达式仍不会被推导。
- 新增布尔包装谓词的 LEFT JOIN 推导回归，先行失败后转绿；全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation354/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 0:36:23`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation354/release-candidate.json` 于 `2026/8/30 0:48:08` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 355

- 继续收敛 P1-OPT-002/P1-EXE-003：Volcano 覆盖索引扫描不再把索引字节平均切片成字符串，也不再把主键误当二级索引 key；扫描过程保留真实 index key，`ReadIndexEntry` 保留 key/value 边界，按 `XSI1`/`XSIV1` 持久化格式解码索引列与聚簇主键列，并按列类型生成结果值。
- 索引元数据现在携带真实主键列名，覆盖索引判定支持复合主键；保留旧调用方缺失主键元数据时的 `id` 兼容回退。
- 新增持久化 covering-index entry 解码回归；engine 全包、全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation355/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 1:00:20`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation355/release-candidate.json` 于 `2026/8/30 1:11:31` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 356

- 继续收敛 P1-OPT-002/P1-EXE-003：覆盖索引回归扩展到“索引列 + 主键列”同时读取，验证持久化主键值按列类型解码；扫描 schema 现在即使列数未减少，也按 required-column 的逻辑顺序重排，避免 `SELECT name, id` 返回列和值错位。
- engine 全包、全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation356/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 1:15:42`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation356/release-candidate.json` 于 `2026/8/30 1:27:31` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 357

- 继续收敛 P1-OPT-002/P1-EXE-003：覆盖索引判定现在按 MySQL 标识符的大小写不敏感规则规范化索引列、复合主键列和查询所需列，避免元数据大小写差异导致错误回表。
- 继续收敛 P1-EXE-003：CBO 集成物理计划生成器保留表扫描/索引扫描/连接的专用选择，同时将聚合、VALUES、UNION、CTE、子查询等其他逻辑节点委托到完整转换器，避免默认分支伪造空 `PhysicalTableScan` 丢失计划树。
- 新增大小写不敏感覆盖索引判定与 CBO 非扫描节点计划树回归；engine、plan 专项测试及全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation357/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 1:35:48`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation357/release-candidate.json` 于 `2026/8/30 1:47:01` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 358

- 继续收敛 P1-SQL-003：`RENAME COLUMN`、`CHANGE COLUMN` 和 `RENAME INDEX` 现在在目标名称已存在时显式拒绝，避免产生重复元数据对象。
- ALTER 多子句执行现在基于隔离的 columns/indexes 工作副本，任一后续子句失败时不会泄漏前面子句对原始 `.frm` 元数据 map 的修改，保持 compound ALTER 的回滚语义。
- 新增重复重命名与“前一子句成功、后一子句失败”回滚回归；engine 全包、全仓 `go test ./... -count=1 -timeout 5m` 与 `git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation358/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 1:52:57`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation358/release-candidate.json` 于 `2026/8/30 2:03:50` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 359

- 继续收敛 P1-OPT-001：外连接等值谓词推导现在识别只引用单个连接列的包裹 `IN`、`LIKE`、`IS NULL`、`BETWEEN` 表达式，并在复制到 nullable 侧时保留结构化表达式与 NULL 语义。
- 新增包裹结构化谓词的 LEFT JOIN 推导回归，先行失败后转绿；plan、engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation359/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 2:09:31`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation359/release-candidate.json` 于 `2026/8/30 2:20:26` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 360

- 继续收敛 P1-EXE-004：编译表达式 evaluator 的列读取现在先保留精确键命中，再对行上下文做大小写不敏感匹配，避免 SQL 标识符大小写变化导致编译路径报“列不存在”。
- 新增编译列表达式大小写不敏感回归；plan、engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation360/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 2:24:37`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation360/release-candidate.json` 于 `2026/8/30 2:35:25` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 361

- 继续收敛 P1-OPT-005/P1-EXE-004：存储集成标量表达式求值补齐 `%`、`DIV`、位与/位或/位异或及左右移位，并复用 plan 层已有的 MySQL 数值语义实现。
- 新增存储集成表达式矩阵回归，先行失败后转绿；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation361/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 2:40:27`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation361/release-candidate.json` 于 `2026/8/30 2:51:22` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 362

- 继续收敛 P1-SQL-003：`MODIFY COLUMN`/`CHANGE COLUMN` 现在识别并执行 `FIRST`/`AFTER` 列位置，单条位置 ALTER 不再被误判为未处理，CHANGE 重命名后的索引列引用仍同步更新。
- 新增 MODIFY/CHANGE 列位置回归；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation362/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 2:55:50`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation362/release-candidate.json` 于 `2026/8/30 3:06:53` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 363

- 继续收敛 P1-EXE-004：存储集成表达式 evaluator 的限定列读取现在统一支持精确键、规范化键和大小写不敏感键匹配，避免 `USERS.NAME` 与 `Users.Name` 等行上下文键名差异导致列不存在。
- 新增限定列大小写不敏感回归，先行失败后转绿；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation363/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 3:12:33`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation363/release-candidate.json` 于 `2026/8/30 3:23:31` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 364

- 继续收敛 P1-OPT-005：表达式规范化器现在把 `BETWEEN` 展开为 `>= AND <=`，把 `NOT BETWEEN` 展开为 `< OR >`，使后续 CNF/DNF 与谓词规则可统一处理，并保留动态边界表达式。
- 修复 plan 层 SQL 三值逻辑缺陷：`UNKNOWN AND UNKNOWN` 与 `UNKNOWN OR UNKNOWN` 现在均保持 `UNKNOWN`，同时仍遵循 FALSE/TRUE 对 UNKNOWN 的支配规则；新增边界与 NULL 语义回归，先行失败后转绿。
- plan、engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation364/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 3:30:42`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation364/release-candidate.json` 于 `2026/8/30 3:41:25` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 365

- 继续收敛 P1-IDX-001/P1-IDX-002：存储集成索引键构造的单列、复合列、旧值和更新表达式路径现在统一按大小写不敏感规则读取列，避免元数据与行 map 命名差异造成唯一性检查、索引重建或更新索引键失败。
- 新增单列/复合列索引键大小写不敏感回归，先行失败后转绿；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation365/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 3:45:44`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation365/release-candidate.json` 于 `2026/8/30 3:58:39` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 366

- 继续收敛 P1-SQL-001：legacy `ON DUPLICATE KEY UPDATE` 的二元表达式现在委托统一存储集成 evaluator，补齐 `%`、`DIV` 及后续已有的位运算、日期间隔和 NULL 语义，避免 legacy 与当前 DML 路径行为分叉。
- legacy ON DUPLICATE 旧记录值现在通过 `basicValueToInterface` 按 typed value 暴露，避免 `basic.Value.Raw()` 的字节值进入数值表达式；新增 `%`/`DIV` 回归，先行失败后转绿。
- engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation366/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 4:05:14`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation366/release-candidate.json` 于 `2026/8/30 4:21:40` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 367

- 继续收敛 P1-SQL-001：legacy `ON DUPLICATE KEY UPDATE` 的直接列引用现在按大小写不敏感规则解析插入行值和现有记录列，避免 `PRICE`/`price` 命名差异导致更新表达式报列不存在。
- 新增 legacy ON DUPLICATE 列引用大小写不敏感回归，先行失败后转绿；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation367/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 4:26:14`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation367/release-candidate.json` 于 `2026/8/30 4:40:39` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 368

- 继续收敛 P1-SQL-001：legacy `FuncExpr` 形式的 `VALUES(col)` 现在与专用 `ValuesFuncExpr` 一样按大小写不敏感规则读取插入行列值，避免旧 AST 形态在 `VALUES(PRICE)` 与行键 `price` 不一致时失败。
- 新增 legacy VALUES 函数大小写不敏感回归，先行失败后转绿；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation368/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 4:45:07`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation368/release-candidate.json` 于 `2026/8/30 4:59:37` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 369

- 继续收敛 P1-EXE-003/P1-SQL-001：Volcano `ApplyOperator` 的关联条件求值现在同时注入普通列名和由列/Schema 元数据生成的 `table.column` 限定键，支持 `users.id = orders.user_id` 等限定关联表达式。
- 新增限定 JOIN 条件回归，先行失败后转绿；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过，`git diff --check` 通过（仅保留既有 LF/CRLF 提示）。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation369/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 5:05:45`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation369/release-candidate.json` 于 `2026/8/30 5:20:44` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 370

- 继续收敛 P1-EXE-001/P1-EXE-003：Volcano `PhysicalMergeJoin` 在无法使用单一等值键时，现在在嵌套循环回退路径中真实求值全部连接条件；不再把非等值条件恒定视为匹配，也不再丢弃等值键之外的残余条件。
- `NestedLoopJoinOperator` 使用的基础算子生命周期现在支持关闭后重开，修复扫描右侧子算子后重新开始下一左行时的 `operator already opened` 错误。
- 新增非等值连接与残余谓词端到端回归，遵循先失败后修复；engine、plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation370/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 5:30:28`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation370/release-candidate.json` 于 `2026/8/30 5:45:06` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 371

- 继续收敛 P1-EXE-004：Volcano/metadata 的查询列、执行记录和底层默认行元数据读取现在统一遵循 MySQL 标识符大小写不敏感规则，覆盖 `QuerySchema.GetColumn`、`EngineExecutorRecord` 读写及 `DefaultTableRow.GetColumnDescInfo`。
- 新增执行记录与底层行元数据的大小写不敏感回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation371/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 5:49:59`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation371/release-candidate.json` 于 `2026/8/30 6:04:26` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 372

- 继续收敛 P1-EXE-004/P1-OPT-005：计划层普通 `Column.Eval` 现在与编译 evaluator 保持一致，按精确键优先、大小写不敏感键回退的规则读取行上下文，避免解释执行路径与编译执行路径对同一 SQL 标识符产生分歧。
- 新增普通表达式解释器的大小写不敏感列读取回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation372/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 6:07:31`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation372/release-candidate.json` 于 `2026/8/30 6:21:36` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 373

- 继续收敛 P1-IDX-001/P1-EXE-003：存储集成主键生成现在在单列、复合列及无元数据回退路径中统一按大小写不敏感规则解析输入行，避免主键列名与 INSERT/扫描行 map 大小写不同导致误报缺列或生成错误隐藏键。
- 新增复合主键大小写不敏感解析回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation373/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 6:25:36`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation373/release-candidate.json` 于 `2026/8/30 6:41:13` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 374

- 继续收敛 P1-EXE-003：Volcano `PhysicalHashJoin` 仅在全部连接条件都是列对列等值条件时使用 hash 路径；非等值条件、等值键之外的残余条件及无条件连接现在回退到完整谓词求值的嵌套循环路径，避免错误笛卡尔匹配或丢弃残余过滤。
- 新增 Hash Join 非等值条件端到端回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation374/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 6:44:44`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation374/release-candidate.json` 于 `2026/8/30 7:00:54` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 375

- 继续收敛 P1-EXE-003：并行 `PhysicalMergeJoin` 现在覆盖 RIGHT/FULL 外连接，正确维护未匹配右行并生成两侧 NULL 扩展，同时保留条件求值与 INNER/LEFT 行为。
- 新增 RIGHT/FULL 并行 Merge Join 回归；修复测试夹具中表列名与限定条件不一致的问题；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation375/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 7:07:46`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation375/release-candidate.json` 于 `2026/8/30 7:24:14` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 376

- 继续收敛 P1-OPT-002/P1-EXE-003：并行物理投影在缺少显式 `OutputNames` 时按投影表达式推导输出列名，保证上层 Selection/HAVING 能引用计算列；显式别名仍优先，无法推导时保留底层列名回退。
- 新增“计算投影列 -> 上层 Selection”端到端回归，先行失败后修复；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation376/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 7:29:05`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation376/release-candidate.json` 于 `2026/8/30 7:45:34` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 377

- 继续收敛 P1-OPT-002/P1-EXE-003：并行 Merge Join 的 Projection/Selection/Sort/Aggregation 子计划现在向上透传真实底层表名，CTE Scan 也暴露 CTE 名称，使 `table.computed_alias` 形式的限定连接条件能够在投影后的 row context 中正确解析。
- 新增“两个投影子计划 -> 限定列 Merge Join”端到端回归，先行失败后修复；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation377/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 7:49:47`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation377/release-candidate.json` 于 `2026/8/30 8:06:09` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 378

- 继续收敛 P1-OPT-002/P1-EXE-003：并行 `PhysicalUnion` 现在从首个分支推导输出列名，`PhysicalCTEStatement` 从主体计划推导输出列名，保证 Union/CTE 上层 Selection 能按输出列正确解析；原有投影表达式和显式别名优先级保持不变。
- 新增“两个投影分支 -> Union -> 上层 Selection”端到端回归，先行失败后修复；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation378/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 8:09:20`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation378/release-candidate.json` 于 `2026/8/30 8:25:36` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 379

- 继续收敛 P1-OPT-002/P1-EXE-003：并行 Hash/Merge Join 的输出列名现在按左右子计划拼接，Apply 的 SEMI/ANTI 只暴露左侧列，其余 Apply 暴露左右列；连接/Apply/Union/CTE 的 row context 递归合并子计划限定列，支持连接后对 `table.column` 输出进行过滤。
- 新增 Hash Join 输出列和限定输出列回归，先行失败后修复；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation379/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 8:30:32`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation379/release-candidate.json` 于 `2026/8/30 8:47:00` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 380

- 继续收敛 P1-EXE-003：并行 Hash Join 在缺少显式 `HashJoinKeys` 时可从直接列等值条件自动提取单列/多列键；无法安全提取时回退完整条件求值。Hash Join 现在对残余条件执行真实 SQL 谓词判断，并支持 LEFT/RIGHT/FULL 外连接的 NULL 扩展与未匹配行保留。
- 新增自动提取 hash key、残余谓词及 LEFT/RIGHT/FULL 外连接回归，先行失败后修复；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation380/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 8:54:36`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation380/release-candidate.json` 于 `2026/8/30 9:10:41` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 381

- 继续收敛 P1-OPT-002/P1-EXE-003：并行 Table/Index Scan 的输出列名现在遵循实际 `RequiredColumns` 顺序和大小写映射，避免列裁剪后的短行仍按整表 schema 解码，导致上层过滤/投影错位或报列不存在。
- 新增 required-column scan -> 上层 Selection 回归，先行失败后修复；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation381/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 9:14:32`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation381/release-candidate.json` 于 `2026/8/30 9:30:31` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 382

- 继续收敛 P1-EXE-003：Hash Join 已补齐跨类型数值/数值字符串 hash key 归一化；本轮 Go 回归、集群冒烟均通过。
- 首次发布候选门禁发现并发 JDBC 会话触发 `DecoupledMySQLMessageHandler.OnMessage` 对 `sessionMap` 的无锁读，服务发生 `concurrent map read and map write` 并退出；该轮报告明确为 `NO-GO`，未将失败误判为 Connector/J 语义问题。

## Continuation 383

- 修复 P0 并发安全缺口：`OnMessage` 读取 `sessionMap` 现在与连接建立、认证更新、关闭删除路径统一使用 `RWMutex` 读锁，避免高并发 Connector/J 会话触发运行时 map 崩溃。
- `server/net` 专项与全仓 `go test ./... -count=1 -timeout 5m` 通过；Go race 检测因当前 Windows 环境 `CGO_ENABLED=0` 且无 gcc 无法启动，已明确记录为环境限制。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation383/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 9:41:00`。
- 修复后的发布证据 `reports/compatibility/release-candidate-current-continuation383/release-candidate.json` 于 `2026/8/30 9:57:04` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务日志未再出现 `concurrent map`/`panic`。

## Continuation 384

- 继续收敛 P1-OPT-002：并行 `PhysicalValues` 现在按字面量/表达式推导输出列名，保证无 FROM 的 `SELECT 1` 经过物理 Selection 时可以按 MySQL 输出列名解析。
- 新增 Values 输出列回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation384/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 10:01:47`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation384/release-candidate.json` 于 `2026/8/30 10:17:30` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务日志未出现 `concurrent map`/`panic`。

## Continuation 385

- 继续收敛 P1-OPT-002/P1-EXE-003：并行计划在使用 `PlanRowReader` 适配器执行 `ParallelTableScan`/`ParallelIndexScan` 时，现在也会按 `RequiredColumns` 投影行数据，避免适配器返回整行而计划元数据已经裁剪时发生列错位；大小写映射和原有存储扫描路径保持一致。
- 新增“PlanRowReader + RequiredColumns + 上层 Selection”回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation385/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 10:21:02`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation385/release-candidate.json` 于 `2026/8/30 10:36:47` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS。

## Continuation 386

- 继续收敛 P1-OPT-002：物理计划列 lineage 现在覆盖 `PhysicalSubquery`、`PhysicalCTE`、`PhysicalRecursiveCTE` 和显式列名的 `PhysicalCTEScan`；标量/EXISTS 子查询只暴露单列，CTE 定义列名优先于子计划推导，避免这些计划边界后的 Selection/Join 因缺少输出列名而无法解析表达式。
- 新增 lineage 回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation386/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 10:41:56`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation386/release-candidate.json` 于 `2026/8/30 11:00:06` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 387

- 继续收敛 P1-OPT-002：CTE statement 在执行时把定义的列名绑定到 schema-less body 和递归成员中的同名 `PhysicalCTEScan`，使 `WHERE`、投影和连接表达式在没有 metadata schema 的适配器计划中仍能按 CTE 输出列解析。
- 新增 schema-less CTE body 列绑定回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation387/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 11:03:28`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation387/release-candidate.json` 于 `2026/8/30 11:18:46` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 388

- 继续收敛 P1-OPT-004/P1-IDX-001：索引条件提取现在支持带 `LowerExpr`/`UpperExpr` 的结构化常量 `BETWEEN`；常量 `NOT BETWEEN` 会安全展开为 `< lower OR > upper` 的有界 index-merge 分支，动态边界继续保守回退，避免错误索引下推。
- 新增结构化 `BETWEEN` 与 `NOT BETWEEN` 访问路径回归，先行失败后修复；plan 全包与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation388/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 11:23:30`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation388/release-candidate.json` 于 `2026/8/30 11:40:43` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 389

- 继续收敛 P1-SQL-002：并行 `PhysicalUnion` 现在正确执行 `UNION`/`UNION ALL`、`INTERSECT`/`INTERSECT ALL`、`EXCEPT`/`EXCEPT ALL`；DISTINCT 路径按首分支稳定顺序去重，ALL 路径按多重集计数，保留统一的 ORDER BY/LIMIT 处理。
- 新增四种 set-operation 及重复行回归；发现并修复历史 `UnionType="DISTINCT"` 别名兼容问题后，plan 全包与全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation389/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 11:47:51`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation389/release-candidate.json` 于 `2026/8/30 12:01:34` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 390

- 继续收敛 P1-OPT-003：增强统计收集器的表、列、索引缓存读取及 `Invalidate` 现在按大小写不敏感匹配，避免同一 SQL 标识符在 metadata/cache key 大小写不一致时读不到统计或留下旧统计。
- 新增大小写混用统计缓存读取与失效回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation390/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 12:05:31`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation390/release-candidate.json` 于 `2026/8/30 12:17:41` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 391

- 继续收敛 P1-OPT-003：`IndexPushdownOptimizer` 消费统计信息时，表、列、索引 key 现在大小写不敏感匹配，避免 metadata 名称与 SQL 引用大小写不同导致 NDV/行数/索引统计失效并退回默认成本。
- 新增大小写混用统计消费者回归，先行失败后修复；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation391/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 12:20:47`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation391/release-candidate.json` 于 `2026/8/30 12:32:30` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 393

- 收敛 P0/P1 统计与观测链路：旧版 `StatisticsCollector` 的表/列/索引缓存、内部 row-count 读取、按表刷新和过期关联现在大小写不敏感；新增回归覆盖混合大小写读缓存与 `UpdateTypeAll` 边界。
- P0-OBS 真实连接数接入：`DecoupledMySQLMessageHandler` 在 OnOpen、OnClose、OnError 和 COM_QUIT 后更新 `xmysql_connections_active{listener="mysql"}`，并新增 handler 回归验证 1→0 的实时值。
- 修复 P0-B/P0-C 证据生成脚本在相对 `ReportDir` 下把 Go 测试快照写入 package 工作目录的问题：两脚本现在先解析报告目录为绝对路径；独立 P0-B/P0-C 证据生成均 PASS。
- 标准 P0 delivery candidate 技术链全部通过：P0-B 状态/diff、P0-C consistency/concurrency、P0-D metrics/logging/alert、P0-E rollback/full-chain、最终回归和 evidence bundle 均 PASS；candidate 的 readiness 为 `NOT_READY` 仅因真实治理输入缺失（审批 HOLD、17 项未签 owner checklist、7 个 TBD owner/开放风险），未伪造 owner sign-off。
- `go test ./... -count=1 -timeout 5m` 在连接数 metrics 变更后全仓通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation393/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 13:02:29`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation393/release-candidate.json` 于 `2026/8/30 13:14:40` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 392

- 继续收敛 P1-OPT-003：旧版 `StatisticsCollector` 的表、列、索引缓存读取现在按大小写不敏感匹配，和 `EnhancedStatisticsCollector`、`IndexPushdownOptimizer` 的统计 key 语义保持一致。
- 旧版收集器的列/索引内部表统计读取、按表 `UpdateTypeAll` 刷新，以及后台过期扫描的表名关联也统一为大小写不敏感；整表刷新使用带点边界的表名判断，避免误删同前缀表。
- 新增 `TestStatisticsCollectorUsesCaseInsensitiveCacheKeys`，覆盖混合大小写的表/列/索引读缓存、索引内部 row-count 取数和整表刷新。
- `go test ./... -count=1 -timeout 5m` 全仓通过。
- 集群证据 `reports/compatibility/p1-cluster-current-continuation392/cluster-report.json` 为 `PASS`，生成时间 `2026/8/30 12:38:51`。
- 发布证据 `reports/compatibility/release-candidate-current-continuation392/release-candidate.json` 于 `2026/8/30 12:50:34` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 394

- 继续收敛 P1-OPT-002/P1-EXE-003 的列 lineage：逻辑表/索引扫描在缺少预挂载 schema 时会从 `Table` 自恢复单表 schema；列裁剪、UNION 按序裁剪、并行 scan 投影和 parallel hash aggregate 绑定现在都接受大小写混用的 `table.column`/反引号限定名，并只按最终列组件匹配。
- 优化器的 UNION 分支谓词归属、identity projection 谓词穿透和 UNION `ORDER BY` 列定位统一使用去限定符的大小写不敏感列 key；覆盖索引判断也统一处理限定名、反引号和大小写，避免误判为非 covering index。
- 新增 qualified scan/aggregate/pruning/predicate-ownership/UNION-order/covering-index 回归；按 TDD 先验证失败，再完成最小实现。`go test ./server/innodb/plan -count=1` 与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation394/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 15:26:45`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation394/release-candidate.json` 于 `2026/9/4 15:40:29` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 395

- 继续收敛 P1-EXE-004：解释器 `Column.Eval` 与编译表达式路径现在都能把 `` `orders`.`ID` `` 与 `orders.id` 规范化为同一列引用，避免反引号限定列在热路径和 fallback 路径行为不一致。
- 新增解释执行/编译执行双路径回归，覆盖带反引号的限定列与大小写混用；先行验证失败后完成最小实现。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation395/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 15:44:44`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation395/release-candidate.json` 于 `2026/9/4 15:58:20` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 396

- 继续收敛 P1-EXE-004：执行器现在支持常用 MySQL 当前时间函数别名：`CURDATE/CURRENT_DATE` 返回 `YYYY-MM-DD`，`CURTIME/CURRENT_TIME/LOCALTIME` 返回 `HH:MM:SS`，`CURRENT_TIMESTAMP/LOCALTIMESTAMP` 返回本地 `time.Time`，`UTC_TIMESTAMP` 返回 UTC `time.Time`。
- 新增当前时间函数别名回归；此前 `CURDATE` 等会报 unknown function，本轮已先红后绿。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation396/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 16:02:07`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation396/release-candidate.json` 于 `2026/9/4 16:15:49` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 397

- 继续收敛 P1-EXE-004：执行器新增 `RAND()` 和 `RAND(seed)`；无参调用返回 `[0,1)` 内的随机 double，单参数调用使用数值 seed 产生可重复结果。该非确定函数未加入编译缓存，避免跨行复用错误结果。
- 新增 RAND 无参/seed 形式回归，先行验证 unknown function 失败后完成实现；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation397/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 16:19:01`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation397/release-candidate.json` 于 `2026/9/4 16:32:29` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 398

- 继续收敛 P1-EXE-004：执行器新增 `UUID()`，使用加密随机源生成 RFC 4122 version 4、variant 1 的标准字符串；保持其非确定性，不加入编译表达式缓存。`UUID_SHORT()` 仍需 server-id 与持久化单调序列设计，不能以随机 UUID 替代。
- 新增 UUID 格式/version/variant 回归；plan 专项与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation398/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 16:35:16`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation398/release-candidate.json` 于 `2026/9/4 16:48:50` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 399

- 继续收敛 P1-OPT-002/P1-EXE-004：Volcano 执行器的 `findColumnIndex` 现在对排序和分组列定位统一支持大小写不敏感、`table.column` 以及反引号限定引用，避免物理计划阶段因 schema 列名与 SQL 引用形式不同而丢失排序键并退回默认列。
- 新增 `TestVolcanoExecutor_FindColumnIndex` 的大小写/限定名/反引号回归；先行失败后完成最小实现，`BuildSortKeys` 与 `BuildGroupByExprs` 共享该修复路径。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation399/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 16:57:27`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation399/release-candidate.json` 于 `2026/9/4 17:11:09` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 400

- 继续收敛 P1-OPT-002/P1-EXE-003：clustered index scanner 的主键列识别现在按大小写不敏感方式匹配 `TableMeta.PrimaryKey` 与列元数据，避免没有 `IsPrimary` 标记时因 `.frm` 名称大小写差异退化为错误的扫描边界。
- 新增 `TestClusteredPrimaryKeyColumnMatchesMetadataCaseInsensitively`；先行失败后完成实现，clustered scan 及 storage-integrated DML 相关回归通过。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation400/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 17:14:01`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation400/release-candidate.json` 于 `2026/9/4 17:27:34` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 401

- 继续收敛 P1-OPT-002/P1-EXE-004：统一执行器的 `ORDER BY` 列绑定现在与 Volcano 执行器一致，支持大小写不敏感、`table.column` 和反引号限定列名；避免 SQL 层已解析成功但 Unified 物理排序阶段报“列不存在”。
- 新增 `TestUnifiedExecutorFindColumnIndexAcceptsQualifiedNames`；先行失败后完成实现，Volcano/Unified 排序列绑定回归均通过。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation401/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 17:30:15`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation401/release-candidate.json` 于 `2026/9/4 17:43:50` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 402

- 继续收敛 P1-EXE-004：执行器新增 `TIME_TO_SEC()` 和 `SEC_TO_TIME()`，支持 MySQL 常见的 `HH:MM:SS`、负时间和微秒精度转换；两者同时加入确定性表达式编译白名单，解释执行与热路径结果保持一致。
- 新增时间转换解释/编译双路径回归；先行验证 unknown function 失败后完成实现，并修正分钟换算缺少乘 60 的实现错误。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation402/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 17:47:39`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation402/release-candidate.json` 于 `2026/9/4 18:00:57` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 403

- 继续收敛 P1-EXE-004：执行器新增 `MAKEDATE()` 与 `MAKETIME()`，支持闰年日序构造、标准时间及微秒时间构造；非法日期/时间返回 NULL，并将函数加入确定性表达式编译白名单。
- 新增构造函数有效值、非法值及编译路径回归；先行验证 unknown function 失败后完成实现。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation403/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 18:03:43`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation403/release-candidate.json` 于 `2026/9/4 18:17:19` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 404

- 继续收敛 P1-EXE-004：执行器新增 `STRCMP()` 三路字符串比较，NULL 输入保持 NULL 语义，并加入确定性表达式编译白名单，解释执行和热路径结果一致。
- 新增 `TestStrcmpFunctionEvaluatesThreeWayResult`，先行验证 unknown function 失败后完成实现。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation404/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 18:20:01`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation404/release-candidate.json` 于 `2026/9/4 18:33:21` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 405

- 继续收敛 P1-EXE-004：表达式编译器补齐当前时间、随机数和 UUID 运行时函数白名单：`CURDATE/CURRENT_DATE`、`CURTIME/CURRENT_TIME/LOCALTIME`、`NOW/CURRENT_TIMESTAMP/LOCALTIMESTAMP/UTC_TIMESTAMP`、`RAND`、`UUID`。这些函数原本已有解释执行实现，但热路径会错误回退；本轮先以失败回归锁定缺口，再完成编译路径接入，并同步更新过时的 `NOW()` fallback 测试契约。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation405/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 18:40:09`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation405/release-candidate.json` 于 `2026/9/4 18:53:35` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 406

- 继续收敛 P1-EXE-004：执行器新增常用 MySQL 标量函数 `ISNULL`、`DAYOFMONTH`、`MONTHNAME`、`DAYNAME`、`RADIANS`、`DEGREES`、`LOG2`、`COT`，并将它们纳入编译表达式白名单；新增解释/编译路径回归，均按先红后绿完成。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation406/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 18:57:49`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation406/release-candidate.json` 于 `2026/9/4 19:11:31` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 407

- 继续收敛 P1-OPT-003：`EnhancedStatisticsCollector` 的 `CollectTableStatistics`、`CollectColumnStatistics`、`CollectIndexStatistics` 缓存读取统一采用大小写不敏感 key 匹配，与 `Get*`/旧版 collector 的语义一致；新增混合大小写缓存命中回归，避免 ANALYZE/CBO 因标识符大小写差异重复估算或丢失已持久化统计。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation407/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 19:14:41`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation407/release-candidate.json` 于 `2026/9/4 19:28:15` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 408

- 继续收敛 P1-EXE-004：执行器新增 `WEEK()` 与 `YEARWEEK()`，支持默认 mode 0、ISO 风格 mode 3、week-0 跨年归属及 `YEARWEEK` 年份拼接；新增边界回归，并加入编译表达式白名单，避免日期报表 SQL 在热路径回退或得到错误周年度。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation408/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 19:31:43`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation408/release-candidate.json` 于 `2026/9/4 19:45:23` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 409

- 继续收敛 P1-EXE-004：执行器新增 `TIME_FORMAT()`，复用已验证的 MySQL 日期/时间格式化实现，并将其纳入编译表达式白名单；新增解释/编译路径回归，避免常见时间格式投影因函数别名未接入而失败。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation409/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 19:48:18`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation409/release-candidate.json` 于 `2026/9/4 20:01:45` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 410

- 继续收敛 P1-EXE-002：`SortMergeJoinOperator` 的输入物化由逐行 `Next()` 改为复用 `nextOperatorBatch`，并提供连接节点 `NextBatch`，因此支持批量子算子的排序合并连接能够真正走 batch 读取；新增回归确认左右子输入均调用 `NextBatch`，同时验证输出行和生命周期语义。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation410/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 20:05:14`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation410/release-candidate.json` 于 `2026/9/4 20:18:43` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 411

- 继续收敛 P1-EXE-002：Volcano `NestedLoopJoinOperator` 新增 bounded `NextBatch`，重扫侧通过 `nextOperatorBatch` 一次物化，外层侧按批流式处理；INNER/LEFT/RIGHT/FULL 的匹配、NULL 扩展、FULL 未匹配右行阶段与 row-oriented 路径保持一致，避免批量子算子在 NestedLoopJoin 处退回逐行 `Next()`。
- 新增 `TestVolcanoNestedLoopJoinConsumesBatchChildren` 与 `TestVolcanoNestedLoopJoinBatchPreservesOuterRows`，先行验证批量调用缺失，再验证四种连接语义及 batch 生命周期；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation411/cluster-report.json` 于 `2026/9/4 20:26:53` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation411/release-candidate.json` 于 `2026/9/4 20:40:25` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 412

- 继续收敛 P1-EXE-002/P1-EXE-003：NestedLoopJoin 的 row/batch 入口统一归一化 `LEFT/RIGHT/FULL OUTER` 别名，补充批量外连接回归，避免常见 JOIN 拼写在不同执行入口产生不同语义。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation412/cluster-report.json` 为 `PASS`，生成时间 `2026/9/4 20:43:09`。
- 本轮 Connector/J 门禁首次因上一轮隔离服务残留占用 3311 而被脚本安全拒绝复用；清理精确路径的本任务进程后，由 Continuation 413 对包含本轮及后续 Apply 改动的最终工作树统一完成发布候选验证。

## Continuation 413

- 继续收敛 P1-EXE-002：Volcano `ApplyOperator` 新增 bounded `NextBatch`，外层算子批量读取，相关内层计划每个外层行重开后复用 `nextOperatorBatch`；INNER/LEFT/SEMI/ANTI 结果语义与原行路径保持一致，批量子算子不再在 Apply 边界退回逐行 `Next()`。
- 新增 `TestVolcanoApplyConsumesBatchChildren`，并保留 NestedLoopJoin 的 batch/外连接回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation413/cluster-report.json` 于 `2026/9/4 20:48:35` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation413/release-candidate.json` 于 `2026/9/4 21:05:16` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 414

- 继续收敛 P1-EXE-002：Volcano `HashJoinOperator` 的 probe 侧新增 bounded `NextBatch`，保留跨批次的匹配游标、LEFT/RIGHT/FULL NULL 扩展及 FULL 未匹配 build 行阶段；build/probe 两侧均通过 `nextOperatorBatch` 读取，不再由 HashJoin 根节点退回逐行 `Next()`。
- 新增 `TestVolcanoHashJoinBuildsFromBatches` 对 probe 批量调用的断言，以及 `TestVolcanoHashJoinBatchPreservesFullOuterRows`；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation414/cluster-report.json` 于 `2026/9/4 21:09:41` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation414/release-candidate.json` 于 `2026/9/4 21:23:03` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 415

- 继续收敛 P1-EXE-004：补齐 Volcano/并行 HashAggregate 的常用 `GROUP_CONCAT(expr)` 能力，支持 NULL 忽略、`DISTINCT` 去重和 `SEPARATOR` 自定义分隔符；逻辑计划可从 SQL AST 传递聚合参数，基础 `ORDER BY`/多表达式排序语义仍保留在后续兼容范围。
- 新增单节点 Volcano、逻辑计划转换和并行 HashAggregate 回归；先行验证后完成实现，定向测试通过。
- `go test ./... -count=1 -timeout 5m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation415/cluster-report.json` 于 `2026/9/4 21:31:09` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation415/release-candidate.json` 于 `2026/9/4 21:44:56` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 416

- 继续收敛 P1-EXE-004：补齐 Volcano/并行 HashAggregate 已被 SQL parser 识别但此前会退化为 COUNT 的 `BIT_AND`、`BIT_OR`、`BIT_XOR`、`STD/STDDEV/STDDEV_POP/STDDEV_SAMP`、`VAR_POP/VAR_SAMP/VARIANCE`；实现 NULL 忽略、空集/样本方差边界和对应结果类型，统计方差采用在线状态更新避免保存全量输入。
- 新增单节点 Volcano 与并行 HashAggregate 回归，并验证 parser/plan 聚合识别；定向测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation416/cluster-report.json` 于 `2026/9/4 21:50:29` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation416/release-candidate.json` 于 `2026/9/4 22:04:00` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 417

- 继续收敛 P1-EXE-004：补齐 Volcano/并行 HashAggregate 的 `JSON_ARRAYAGG(expr)`，保留 NULL 为 JSON `null`，并按输入值类型输出 JSON 数字、布尔、字符串或 JSON 文档；`JSON_OBJECTAGG(key,value)` 暂不接入简化的单输入聚合接口，避免错误聚合 key/value。
- 新增 parser 聚合识别、计划表达式直接求值、单节点 Volcano 和并行 HashAggregate 回归；定向测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation417/cluster-report.json` 于 `2026/9/4 22:07:51` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation417/release-candidate.json` 于 `2026/9/4 22:21:25` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 418

- 继续收敛 P1-EXE-004：补齐 Volcano HashAggregate 的多输入 `JSON_OBJECTAGG(key,value)`，新增聚合表达式到子列的绑定和 `UpdateValues` 接口，支持 NULL value、重复 key 的后值覆盖及稳定 JSON 输出；并行 HashAggregate 对该多输入形态明确返回 unsupported，避免错误地只消费 key 列。
- 新增 JSON_OBJECTAGG 直接求值、单节点多输入聚合和并行边界回归；定向测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation418/cluster-report.json` 于 `2026/9/4 22:26:05` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation418/release-candidate.json` 于 `2026/9/4 22:39:40` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 419

- 继续收敛 P1-EXE-004：补齐 `ANY_VALUE(expr)` 聚合，parser 不再把它当作普通标量函数，Volcano/并行 HashAggregate 均保留分组内首个输入值并正确处理全 NULL 组；结果类型按动态字符串兼容类型暴露。
- 新增 parser、直接求值、单节点 Volcano 和并行 HashAggregate 回归；定向测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation419/cluster-report.json` 于 `2026/9/4 22:42:51` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation419/release-candidate.json` 于 `2026/9/4 22:56:36` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 420

- 继续收敛 P1-EXE-004：generic aggregate plan 现在传递 `FuncExpr.Distinct`；Volcano 与并行 HashAggregate 补齐 `COUNT/SUM/AVG/MIN/MAX(DISTINCT ...)` 去重，`COUNT(col)` 正确忽略 NULL，并保留 GROUP_CONCAT 的既有 DISTINCT 语义。
- 新增单节点与并行 DISTINCT 聚合回归；定向测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation420/cluster-report.json` 于 `2026/9/4 23:00:18` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation420/release-candidate.json` 于 `2026/9/4 23:14:06` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，Maven BUILD SUCCESS，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 421

- 继续验证 P1-EXE-004/P1 事件运行时稳定性时，发现发布候选 integration 检查暴露 `TestSQLEventOnCompletionControlsOneShotMetadataRetention` 的一次性 EVENT 元数据删除竞态；本轮报告 `reports/compatibility/release-candidate-current-continuation421/release-candidate.json` 为 `NO-GO`，但 JDBC 136/136 仍通过，其余检查均通过。
- 根因是一次性非 `ON COMPLETION PRESERVE` EVENT 在 DML 提交可见后才删除 `.event.json`，调用方可能在窗口内观察到事件结果但仍看到元数据文件；修复转入下一轮验证。

## Continuation 422

- 修复一次性非 PRESERVE SQL EVENT 的元数据生命周期：在执行用户事件语句前删除 durable `.event.json`，消除“结果已可见、元数据尚未删除”的竞态；PRESERVE 和重复事件保持原语义。
- 相关事件测试重复 5 次通过，integration 四包组合通过；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation422/cluster-report.json` 于 `2026/9/4 23:35:21` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation422/release-candidate.json` 于 `2026/9/4 23:49:18` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，所有检查通过，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 423

- 继续收敛 P1-EXE-002：IN/ANY/ALL 子查询的结果物化改为复用 `nextOperatorBatch`，批量子算子不再在子查询边界退回逐行 `Next()`；标量子查询与 EXISTS 的单行/短路语义保持不变。
- 新增批量子查询消费回归，先行验证批量调用缺失后完成实现；定向子查询测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation423/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation423/release-candidate.json` 于 `2026/9/5 0:06:46` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，所有检查通过，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 424

- 继续收敛 P1-EXE-003：并行 HashAggregate 的聚合输入从单列扩展为有序输入列列表，补齐并行多列 `COUNT(DISTINCT a,b)` 与多输入 `JSON_OBJECTAGG(key,value)`；多列 DISTINCT 任一输入为 NULL 时忽略该组合，JSON_OBJECTAGG 保留 NULL value、忽略 NULL key，并保持重复 key 的后值覆盖。
- 新增并行多输入聚合回归；定向 plan 测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation424/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation424/release-candidate.json` 于 `2026/9/5 0:24:37` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，所有检查通过，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 425

- 继续收敛 P1-EXE-002：CTE、递归 CTE、CTE Scan 和窗口函数算子新增 bounded `NextBatch`；CTE/递归 CTE 复用主查询批量读取，CTE Scan 按物化结果切片，窗口函数保持一次物化计算但批量输出，EOF/生命周期语义不变。
- 新增 CTE Scan + `ROW_NUMBER()` 批量回归；定向 engine 测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation425/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation425/release-candidate.json` 于 `2026/9/5 0:42:24` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，所有检查通过，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 426

- 继续收敛窗口函数执行器：通用 `WindowFunctionOperator` 新增 `NTH_VALUE`、`CUME_DIST`、`PERCENT_RANK`，分别遵守当前窗口 frame、peer 组边界和单行分区的 MySQL 结果规则，并为分布函数暴露 DOUBLE 结果类型。
- 新增 frame/peer 分布函数回归；定向 engine 测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation426/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation426/release-candidate.json` 于 `2026/9/5 1:00:49` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，所有检查通过，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 427

- 继续收敛窗口聚合 frame：SQL 兼容入口新增 `STD/STDDEV/STDDEV_POP/STDDEV_SAMP`、`VAR_POP/VAR_SAMP/VARIANCE` 与 `BIT_AND/BIT_OR/BIT_XOR` 窗口聚合，复用 ROWS/RANGE frame，处理 NULL、空 frame、样本统计和位聚合空集边界，并暴露正确的 DOUBLE/BIGINT 结果类型。
- 新增真实 SQL 窗口统计/位聚合回归；定向 engine 测试与全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation427/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation427/release-candidate.json` 于 `2026/9/5 1:18:24` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，所有检查通过，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 428

- 继续收敛 P1-SQL-002：混合 `INTERSECT/EXCEPT` 集合表达式作为派生表来源时，旧 SQL parser 无法生成派生 `SelectStatement`，且会被标量子查询改写器误判；新增原始集合派生表兼容入口，复用集合物化器，并支持外层投影、`SELECT *`、过滤、表达式投影、排序、LIMIT/OFFSET 和 `AS`/裸别名。
- 新增真实回归覆盖嵌套 `UNION ALL` + `INTERSECT`、`EXCEPT`、外层 `WHERE/ORDER BY/LIMIT`、星号投影和数值表达式；engine 专项与全仓 `go test ./... -count=1 -timeout 5m` 均通过，目标文件 `git diff --check` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation428/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation428/release-candidate.json` 于 `2026/9/5 1:41:56` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 429

- 继续收敛 P1-SQL-002：混合集合派生表外层查询现在复用既有派生聚合构建器，补齐 `COUNT(*)` 及 `GROUP BY`，避免将聚合函数当作普通行表达式求值。
- 新增真实 SQL 回归覆盖混合 `UNION ALL` + `INTERSECT` 派生结果的全量聚合和分组聚合；全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation429/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation429/release-candidate.json` 于 `2026/9/5 1:59:49` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 430

- 继续收敛 P1-OPT-005：计划表达式数值强制转换统一接受文本字符串和存储常见的文本 `[]byte`，补齐 `[]byte` 与整数的比较、加法路径，并保持十进制字节值的浮点算术语义。
- 新增计划层 numeric-coercion 回归；全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation430/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation430/release-candidate.json` 于 `2026/9/5 2:16:50` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 431

- 继续收敛 P1-SQL-003/P1-SQL-004：`ALTER TABLE ... RENAME/CHANGE COLUMN` 现在同步更新当前表外键的本地 `columns` 元数据；改名后运行时外键校验、非法父键拒绝和 `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` 均使用新列名。
- 新增真实 ALTER/FK 回归；全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation431/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation431/release-candidate.json` 于 `2026/9/5 2:34:58` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 432

- 继续收敛 P1-SQL-003/P1-SQL-004：父表 `RENAME/CHANGE COLUMN` 现在扫描同库持久化 `.frm`，同步子表外键的 `ref_columns`；改名后有效父键仍可插入，非法父键被拒绝，`INFORMATION_SCHEMA.KEY_COLUMN_USAGE` 暴露新引用列名。
- 新增真实跨表 ALTER/FK 回归；全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation432/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation432/release-candidate.json` 于 `2026/9/5 2:52:23` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 433

- 继续收敛 P1-SQL-003/P1-SQL-004：列改名现在同步当前表 `checks`/`check_names` 中的 SQL 标识符，并同步外键 `raw_definition`；改名后合法数据不再因 CHECK 仍引用旧列名而失败，非法值仍被约束拒绝。
- 新增真实 CHECK 改名回归；全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation433/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation433/release-candidate.json` 于 `2026/9/5 3:09:04` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 434

- 继续收敛 P1-SQL-003：`ALTER TABLE ... RENAME/CHANGE COLUMN` 现在同步当前表生成列的 `generated_expression`；新增真实生成列改名与重新 INSERT 计算回归，生成值保持正确。
- 全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation434/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation434/release-candidate.json` 于 `2026/9/5 3:25:43` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 435

- 继续收敛 P1-SQL-002：混合 `INTERSECT/EXCEPT` 集合表达式作为派生表来源时，外层查询现在补齐 `DISTINCT` 去重和多列 `ORDER BY` 稳定排序；该路径同时保留既有外层投影、WHERE、聚合/GROUP BY、LIMIT/OFFSET 语义。
- 新增真实回归覆盖混合集合派生结果的外层 `DISTINCT` 与多列排序；目标专项测试和全仓 `go test ./... -count=1 -timeout 5m` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation435/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation435/release-candidate.json` 于 `2026/9/5 3:44:41` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 436

- 继续收敛 P1-SQL-002：混合 `INTERSECT/EXCEPT` 集合表达式作为派生表来源时，物化结果现在可作为外层 `INNER/LEFT/RIGHT/USING` JOIN 的真实左源，与普通持久化表组合并执行连接条件、投影和排序；避免旧 raw 派生表路径将 JOIN 尾部误当作普通查询尾部而丢失右表列。
- 新增真实回归覆盖混合集合派生表与持久化表 INNER JOIN；现有派生表 INNER/LEFT/USING JOIN 回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 与目标文件 `git diff --check` 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation436/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation436/release-candidate.json` 于 `2026/9/5 4:02:18` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 437

- 继续补强 P1-SQL-002 的组合回归：混合集合派生表物化结果与普通表 JOIN 后，现在验证外层 `GROUP BY` + `COUNT(*)` 聚合以及分组排序，确保 JOIN 桥接不仅覆盖行级投影。
- 新增真实回归覆盖混合集合派生表 JOIN 普通表后的分组聚合；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation437/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation437/release-candidate.json` 于 `2026/9/5 4:19:16` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 438

- 继续收敛 P1-OPT-005：计划表达式与兼容执行器新增 MySQL `GET_FORMAT()`，支持 `DATE`、`DATETIME`、`TIME` 的 `USA/JIS/ISO/EUR/INTERNAL` 格式矩阵，并接入编译表达式热路径。
- 新增计划层、编译路径和真实 SQL 查询回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation438/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation438/release-candidate.json` 于 `2026/9/5 4:36:46` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 439

- 继续收敛 P1-OPT-005：计划表达式与兼容执行器新增固定偏移 `CONVERT_TZ()`，正确按源时区墙上时间解析后转换到目标时区，支持 `UTC/GMT/Z` 和 `±HH:MM`/`±HHMM`；无时区表的命名时区与夏令时规则仍保留边界。
- 新增计划层、编译路径和真实 SQL 双向时区转换回归；全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation439/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation439/release-candidate.json` 于 `2026/9/5 4:54:30` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 440

- 继续收敛 P1-OPT-001：相关 `INNER/SEMI/ANTI Apply` 的等值 `JoinConds` 现在参与安全谓词传递；当一侧已有常量、范围、`IN`/`LIKE`、NULL 判断或 `BETWEEN` 条件时，另一侧可获得等价候选过滤。`LEFT Apply` 保留 nullable-side 边界，不做该推导。
- 新增真实优化器回归覆盖 INNER Apply 的关联列范围谓词推导，以及 LEFT Apply 不误推导；原有 Apply/Join/外连接推导测试保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation440/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation440/release-candidate.json` 于 `2026/9/5 5:13:17` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 441

- 继续收敛 P1-SQL-001：相关标量投影现在保留外层 `LIMIT/OFFSET` 和 `GROUP BY/HAVING` 语义；分组结果中的内部整数编码会按结果列类型恢复为 SQL 标量，再用于关联条件替换，避免把编码字节误当字符串导致内层聚合返回 `NULL`。相关派生外层和谓词路径同步采用可读的直接投影值。
- 新增真实回归覆盖相关标量投影的外层 `LIMIT/OFFSET`、`GROUP BY/HAVING`；相关标量表达式、多子查询、派生外层和 EXISTS 专项回归均通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation441/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation441/release-candidate.json` 于 `2026/9/5 5:37:47` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 442

- 继续收敛 P1-OPT-005：`CONVERT_TZ()` 现在除固定偏移、`UTC/GMT/Z` 外支持 IANA 命名时区，并按时区数据库处理夏令时转换；无法加载的时区仍按 MySQL 兼容路径返回 `NULL`。
- 新增计划层冬夏季 DST 回归和真实 SQL 查询回归；固定偏移与现有函数矩阵保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation442/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation442/release-candidate.json` 于 `2026/9/5 5:54:50` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 443

- 继续收敛 P1-SQL-002：混合 `INTERSECT/EXCEPT` 派生结果现在可以作为普通 `INNER JOIN` 的右侧来源；集合分支先物化为受控结果，再通过现有派生 JOIN 执行器完成右侧别名、`ON` 条件、投影和排序。
- 新增真实回归覆盖混合集合派生表右侧 JOIN；原有左侧 JOIN、聚合、DISTINCT、多列排序和 LIMIT/OFFSET 回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation443/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation443/release-candidate.json` 于 `2026/9/5 6:13:42` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 444

- 继续收敛 P1-SQL-002：混合 `INTERSECT/EXCEPT` 派生结果作为 JOIN 右侧来源时，新增 `LEFT JOIN`/`RIGHT JOIN` 路径，保留左侧或集合侧的 NULL 扩展与未匹配行语义；INNER JOIN 路径保持不变。
- 新增真实回归覆盖右侧派生集合的 INNER/LEFT/RIGHT JOIN 结果；已有集合派生投影、聚合、DISTINCT、排序和 LIMIT/OFFSET 回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation444/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation444/release-candidate.json` 于 `2026/9/5 6:32:40` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 445

- 继续收敛 P1-SQL-001：相关标量投影支持外层 `DISTINCT`，去重在每行完成相关标量计算后执行，避免仅按外层直接列提前去重而丢失不同标量结果；常见外层分页也在去重后应用，覆盖 `DISTINCT ... ORDER BY ... LIMIT/OFFSET`。
- 新增真实回归覆盖重复外层键与相关 `MAX()` 结果去重；相关标量的 `LIMIT/OFFSET`、`GROUP BY/HAVING` 和多子查询回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation445/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation445/release-candidate.json` 于 `2026/9/5 6:49:39` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 446

- 继续完善 P1-SQL-001 的相关标量外层尾部处理：当存在外层 `DISTINCT` 时，`LIMIT/OFFSET` 不再提前作用于未去重的外层行，而是在相关标量计算和 DISTINCT 完成后分页；非 DISTINCT 路径保持原有执行顺序。
- 新增真实回归覆盖相关标量 `DISTINCT ... LIMIT/OFFSET`；相关标量的普通分页、GROUP BY/HAVING、表达式和多子查询回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation446/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation446/release-candidate.json` 于 `2026/9/5 7:07:12` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 447

- 继续收敛 P1-SQL-001：相关标量投影现在支持外层 `ORDER BY` 相关标量别名；排序延后到每行标量求值完成后，并在需要时按数值语义排序，再执行外层 `DISTINCT`/`LIMIT/OFFSET`，避免把标量别名错误下推到外层源查询。
- 新增真实回归覆盖相关 `MAX()` 别名倒序、两位数数值排序、`LIMIT`，以及 `DISTINCT + 标量别名排序 + LIMIT`；原有相关标量分页、分组、去重和多子查询回归保持通过。

## Continuation 448

- 继续收敛 P1-SQL-001：相关标量别名与外层普通列混合出现在 `ORDER BY` 时，整段排序安全地延后到相关标量求值之后；支持限定外层列作为并列排序键，并在后置排序后再执行 `LIMIT/OFFSET`。
- 新增真实回归覆盖 `ORDER BY max_score DESC, p.id ASC` 的并列排序和分页；相关标量单别名排序、DISTINCT、分组及多子查询回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation448/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation448/release-candidate.json` 于 `2026/9/5 7:44:06` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 449

- 继续收敛 P1-SQL-001：相关标量投影支持在外层 `GROUP BY` 后使用相关标量别名进行 `HAVING` 过滤；包含标量别名的 `HAVING` 会在每行相关标量求值后执行，普通外层列条件仍保留并可与标量条件组合。
- 新增真实回归覆盖 `HAVING max_score >= 9` 以及 `HAVING max_score >= 9 AND p.id <> 3`；原有外层 `GROUP BY/HAVING`、排序、DISTINCT、分页和多子查询回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation449/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation449/release-candidate.json` 于 `2026/9/5 8:02:24` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 450

- 继续收敛 P1-SQL-001：相关标量别名可出现在外层 `ORDER BY` 表达式中，例如 `ORDER BY COALESCE(max_score, 0)`；后置排序现在支持列键、混合列键和投影表达式，并保持 NULL 排序及数值字符串的数值比较语义。
- 新增真实回归覆盖 `COALESCE(max_score, 0)` 的升序/降序和分页；相关标量 `HAVING`、`DISTINCT`、分组、普通/混合排序和多子查询回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation450/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation450/release-candidate.json` 于 `2026/9/5 8:21:14` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 451

- 继续收敛 P1-SQL-006/P1-OPT-005：补齐 MySQL `BIT` 列的整数兼容链路。metadata 校验、basic value 转换、默认值归一化、DML 结果转换和表达式谓词现在将 `BIT` 作为数值类型处理；`INFORMATION_SCHEMA.COLUMNS` 新增 `COLUMN_TYPE` 投影，`BIT(n)` 返回带长度的类型定义，并将 JDBC `DATA_TYPE` 报告为 `java.sql.Types.BIT`。信息模式按请求列投影时的排序也改为按列名定位，避免只查询单列时访问固定列下标。
- 新增真实 SQL 回归覆盖 `BIT(8)` 建表、插入、读取、等值过滤、更新和信息模式类型元数据；相关 engine/metadata 回归通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation451/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation451/release-candidate.json` 于 `2026/9/5 8:44:53` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 452

- 继续收敛 P1-SQL-007：新增常见 `CREATE TABLE target AS SELECT ...` raw 兼容入口。源 `SELECT` 先通过真实查询执行，目标列名/类型由结果元数据生成，目标表通过普通 InnoDB DDL 创建，源行通过 storage-integrated `INSERT` 物化；解析或物化失败时删除目标表，避免 CTAS 半成功。当前边界明确为无显式目标列定义的常见 CTAS，临时表生命周期、显式列定义和更宽 CTAS 语法仍未完成。
- 新增真实 SQL 回归覆盖跨表 `CREATE TABLE ... AS SELECT`、投影列、过滤条件、目标持久化读取；相关 DDL、信息模式和 CREATE TABLE LIKE 回归保持通过。
- 全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation452/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation452/release-candidate.json` 于 `2026/9/5 9:03:04` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 453

- 继续收敛 P1-SQL-007：CTAS 兼容入口新增显式目标列定义，例如 `CREATE TABLE copied (ident BIGINT, label VARCHAR(10)) AS SELECT ...`；目标列数量与源投影数量校验，目标 schema 按显式定义创建，源行仍通过 storage-integrated `INSERT` 写入，物化失败回滚目标表。
- 新增真实 SQL 回归覆盖无显式列定义和显式列定义两种 CTAS；全仓 `go test ./... -count=1 -timeout 5m` 通过，目标文件 `git diff --check` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation453/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation453/release-candidate.json` 于 `2026/9/5 9:19:48` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 454

- 继续收敛 P1-SQL-007：CTAS 现在支持省略 `AS` 的 `CREATE TABLE target SELECT ...`，并可将 `UNION ALL`/现有集合操作结果作为源查询；集合结果复用既有 set-operation 执行器，目标表仍走统一 schema 创建、storage-integrated INSERT 和失败清理路径。
- 新增真实 SQL 回归覆盖无 `AS` CTAS、显式列定义 CTAS、`UNION ALL` 源 CTAS；全仓 `go test ./... -count=1 -timeout 5m` 通过，目标文件 `git diff --check` 通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation454/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation454/release-candidate.json` 于 `2026/9/5 9:36:24` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 455

- 继续收敛 P1-SQL-008：补齐基础 `CREATE TEMPORARY TABLE` 会话生命周期。临时表使用连接状态保存逻辑名到物理表的映射，同一会话可遮蔽同名持久表，不同会话保持隔离；普通 `DROP TABLE` 会优先删除当前会话的临时表并恢复持久表可见性，支持限定表引用；COM_RESET_CONNECTION 和网络连接关闭/错误路径会删除物理临时表及其存储映射。
- 新增真实回归覆盖同名持久表遮蔽、跨会话隔离、限定列引用、普通 DROP 恢复和 RESET 清理；相关引擎、dispatcher、网络层回归通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation455/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation455/release-candidate.json` 于 `2026/9/5 10:00:06` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 456

- 继续收敛 P1-SQL-008：临时表现在支持 `CREATE TEMPORARY TABLE ... AS SELECT` 和 `CREATE TEMPORARY TABLE ... LIKE ...`；CTAS 复用已验证的 schema 推导与 storage-integrated materialization，LIKE 复用定义复制路径，源表引用会按当前会话的临时表映射解析。
- 新增真实回归覆盖临时 CTAS 投影读取、临时 LIKE 空表创建及后续插入读取；临时表会话隔离、遮蔽、DROP、RESET 清理回归保持通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation456/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation456/release-candidate.json` 于 `2026/9/5 10:18:18` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 457

- 继续收敛 P1-SQL-008：`SHOW TABLES` 和 `SHOW FULL TABLES` 现在隐藏内部物理临时表名；创建临时表的当前会话看到逻辑表名并正确遮蔽同名持久表，其他会话看不到该临时表。
- 新增真实 SHOW 元数据回归；临时表的 CTAS/LIKE、DML、DROP、RESET、跨会话隔离回归保持通过。`INFORMATION_SCHEMA` 全套临时表投影仍明确保留为后续边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation457/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation457/release-candidate.json` 于 `2026/9/5 10:37:10` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 458

- 继续收敛 P1-SQL-008：`INFORMATION_SCHEMA.TABLES` 和 `INFORMATION_SCHEMA.COLUMNS` 现在按连接会话解析临时表。当前会话看到逻辑表名、列名和列定义，临时表遮蔽同名持久表；其他会话不会看到该临时表或内部物理名。
- 新增真实信息模式回归覆盖会话内 TABLES/COLUMNS 可见性和跨会话隔离；SHOW、CTAS/LIKE、DML、DROP、RESET 回归保持通过。约束/索引等其他信息模式视图仍明确保留为后续边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation458/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation458/release-candidate.json` 于 `2026/9/5 10:53:51` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 459

- 继续收敛 P1-SQL-008：`INFORMATION_SCHEMA.STATISTICS` 现在按会话解析临时表逻辑名；当前会话可通过索引元数据看到临时 `LIKE` 表的 PRIMARY/列信息，其他会话不会看到对应物理表索引。
- 新增真实索引元数据回归；临时表的 SHOW、TABLES/COLUMNS、CTAS/LIKE、DML、DROP、RESET 和跨会话隔离回归保持通过。其他约束视图和完整临时表语法仍明确保留为后续边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation459/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation459/release-candidate.json` 于 `2026/9/5 11:10:19` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 460

- 继续收敛 P1-SQL-008：`INFORMATION_SCHEMA.TABLE_CONSTRAINTS` 和 `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` 现在按会话解析临时表逻辑名；临时 `LIKE` 表的 PRIMARY KEY/列约束元数据可被当前连接读取，其他连接不会读取该物理临时表。
- 新增真实约束元数据回归；临时表的 SHOW、TABLES/COLUMNS/STATISTICS、CTAS/LIKE、DML、DROP、RESET 和跨会话隔离回归保持通过。CHECK/外键专用临时表视图仍明确保留为后续边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation460/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation460/release-candidate.json` 于 `2026/9/5 11:26:49` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 461

- 继续收敛 P1-SQL-008：`INFORMATION_SCHEMA.CHECK_CONSTRAINTS` 和 `INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` 现在按会话解析临时表逻辑名；临时表约束元数据不会把内部物理表名泄露给客户端。
- 相关信息模式回归、临时表 SHOW/TABLES/COLUMNS/STATISTICS/TABLE_CONSTRAINTS/KEY_COLUMN_USAGE、CTAS/LIKE、DML、DROP、RESET 和跨会话隔离回归保持通过。临时表事务回滚和启动孤儿清理仍是明确剩余边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation461/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation461/release-candidate.json` 于 `2026/9/5 11:42:57` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 462

- 继续收敛 P1-SQL-008：引擎启动阶段现在扫描并删除保留物理前缀 `__xmysql_tmp_` 的遗留临时表文件；只有该内部前缀的对象允许被启动清理，避免误删普通用户表。新增真实回归，验证进程关闭后遗留的临时表在新引擎启动时被清理，并从 `INFORMATION_SCHEMA.TABLES` 消失。
- 临时表的会话隔离、SHOW/TABLES/COLUMNS/STATISTICS/TABLE_CONSTRAINTS/KEY_COLUMN_USAGE/CHECK_CONSTRAINTS/REFERENTIAL_CONSTRAINTS、CTAS/LIKE、DML、DROP、RESET、网络断开和启动清理回归均通过；事务回滚语义和完整 MySQL 临时表语法仍是剩余边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation462/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation462/release-candidate.json` 于 `2026/9/5 12:01:48` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 463

- 继续收敛 P1-SQL-008：临时表现在支持常见 DML 事务边界，`ROLLBACK` 会撤销未提交临时表行变更但保留临时表本身，提交后的行保持可见；新增真实回归覆盖回滚和提交。
- 修正临时表元数据路由：`SHOW CREATE TABLE` 与 `SHOW INDEX` 先按会话逻辑名定位内部物理表，再向客户端返回逻辑表名和定义，避免泄露 `__xmysql_tmp_*` 物理名称；新增真实回归覆盖两条入口。
- 临时表的 SHOW/TABLES/COLUMNS/STATISTICS/TABLE_CONSTRAINTS/KEY_COLUMN_USAGE/CHECK_CONSTRAINTS/REFERENTIAL_CONSTRAINTS、CTAS/LIKE、DML、提交/回滚、DROP、RESET、网络断开、启动清理和跨会话隔离回归均通过；完整 MySQL 临时表语法仍是剩余边界。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation463/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation463/release-candidate.json` 于 `2026/9/5 12:23:00` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 464

- 继续收敛 P1-SQL-008：`DROP TEMPORARY TABLE` 现在支持多个表名和 `IF EXISTS`；普通 `DROP TABLE` 在混合持久表/临时表场景不会被兼容分支部分消费，而是安全交回普通 DDL 路径处理。
- 新增真实回归覆盖多表临时 DROP、重复 `IF EXISTS`、会话映射清理；之前的临时表 SHOW/信息模式、CTAS/LIKE、DML 事务、启动清理、逻辑元数据和跨会话隔离回归保持通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation464/cluster-report.json` 为 `PASS`。

## Continuation 465

- 继续收敛 P1-OPT-005：增加 MySQL 常用 `EXTRACT(unit FROM expr)` 语法兼容重写，将 `YEAR`、`QUARTER`、`MONTH`、`WEEK`、`DAY`、`HOUR`、`MINUTE`、`SECOND`、`MICROSECOND` 映射到已有日期部件求值；日期部件统一复用存储层兼容时间解析，正确处理 `time.Time` 行值和微秒结果。
- 重写器采用引号感知扫描，避免改写 SQL 字符串字面量中的 `extract(...)` 文本；复合单位如 `DAY_MICROSECOND` 明确返回 unsupported，而不是静默产生错误结果；新增真实 SQL、字符串边界和不支持单位回归。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation465/cluster-report.json` 于 `2026/9/5 12:52:28` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation465/release-candidate.json` 于 `2026/9/5 13:06:09` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 466

- 继续收敛 P1-SQL-008：修复临时表 `ALTER TABLE ... RENAME TO ...` 的会话逻辑映射；重命名只更新当前连接的逻辑名到内部物理名绑定，不移动或暴露 `__xmysql_tmp_*` 物理对象。同步支持常见单表 `RENAME TABLE ... TO ...` 形式，并拒绝跨数据库临时表重命名，普通持久表重命名仍交回原有 DDL 路径。
- 新增真实回归覆盖临时表重命名后的查询、映射迁移、物理前缀保持，以及 `TRUNCATE TABLE` 后行清空；临时表已有 SHOW/信息模式、CTAS/LIKE、DML 事务、DROP、启动清理和跨会话隔离回归保持通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation466/cluster-report.json` 于 `2026/9/5 13:12:46` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation466/release-candidate.json` 于 `2026/9/5 13:26:25` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 467

- 继续收敛 P1-OPT-005：补齐常用 MySQL `DATE_FORMAT()` 说明符 `%j/%w/%U/%u/%V/%v/%X/%x/%T/%r/%D`，覆盖年内日、星期数字、Sunday/Monday 起始周、周所属年份、24/12 小时格式和英文序数日；周数复用已有 MySQL week-mode 计算，避免简单使用 Go ISO 周数造成语义偏差。
- 新增真实 SQL 回归覆盖 `2024-03-05 14:06:07` 的完整组合输出，既验证格式化结果，也验证存储集成路径对日期时间列的解析；既有 `EXTRACT`、临时表和 Connector/J 回归保持通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation467/cluster-report.json` 于 `2026/9/5 13:30:15` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation467/release-candidate.json` 于 `2026/9/5 13:43:58` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 468

- 继续收敛 P1-OPT-005：`STR_TO_DATE()` 现在补齐常用 `%W/%a/%b/%j` 说明符，与 `DATE_FORMAT()` 的星期、月份和年内日格式形成可逆的常用解析链路；相关查询回归覆盖真实日期字符串解析。
- 继续扩展 P1-SQL-001 回归矩阵：相关标量子查询现在确认可嵌入外层 `CASE WHEN` 表达式，并按每个外层行重新绑定相关列，空/非空分支保持正确。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation468/cluster-report.json` 于 `2026/9/5 13:49:00` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation468/release-candidate.json` 于 `2026/9/5 14:02:38` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 469

- 重新验证 P0-OBS-001 当前工作区：`scripts/compatibility/observability_smoke.ps1` 运行指标、Performance Schema 和慢查询相关回归，报告 `reports/compatibility/p0-observability-current-continuation469/observability.json` 于 `2026-09-05T19:04:03Z` 为 `PASS`。
- 该轮没有把历史 Connector/J 或旧 observability 报告当作当前实现证明；前一轮 continuation468 的全量 Go、集群和 Connector/J `GO` 证据继续有效。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation464/release-candidate.json` 于 `2026/9/5 12:44:51` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 470

- 继续收敛 P1-OPT-005/P1-EXE-004：`STR_TO_DATE()` 现在补齐常用 `%r`（12 小时制）、`%T`（24 小时制）、`%k/%l`（非零 24/12 小时制）和 `%f`（微秒）格式，并将 `STR_TO_DATE` 纳入编译表达式白名单回归，确保运行时与编译执行路径一致。
- 修复格式布局生成器把字面量小数点转成正则转义的缺陷；`STR_TO_DATE('...123456', '%Y-%m-%d %H:%i:%s.%f')` 现可正确解析，不影响已有日期/时间格式。
- 定向 plan/engine 回归通过；全仓 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation470/cluster-report.json` 于 `2026/9/5 14:09:04` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation470/release-candidate.json` 于 `2026/9/5 14:22:34` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 471

- 扩展 `STR_TO_DATE()` 回归覆盖 `%k/%l` 非零 24/12 小时制，确认新增布局在实际 SQL 路径可用；全量 Go 回归、集群 smoke 和 Connector/J 发布候选门禁均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation471/cluster-report.json` 于 `2026/9/5 14:24:54` 为 `PASS`；Connector/J 报告 `reports/compatibility/release-candidate-current-continuation471/release-candidate.json` 于 `2026/9/5 14:38:26` 为 `GO`，JDBC `136/136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 无 `fatal error`/`concurrent map`/`panic`。

## Continuation 472

- 修复 `STR_TO_DATE()` 的微秒精度丢失：带 `%f` 的结果现在保留 6 位微秒，无 `%f` 的结果继续保持秒级格式；新增真实 SQL 回归验证 `2024-03-05 14:06:07.123456` 原样保留。
- 定向 plan/engine 回归通过；全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation472/cluster-report.json` 于 `2026/9/5 14:40:37` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation472/release-candidate.json` 于 `2026/9/5 14:54:16` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 473

- 继续收敛 P1-OPT-005/P1-EXE-004：统一表达式执行器新增 MySQL `CONV(N, from_base, to_base)`，支持 2–36 进制、有符号输入和大写输出；使用任意精度整数转换，避免大数在中间步骤溢出，并对非法进制返回明确错误。
- 新增真实 SQL、编译表达式和非法进制回归；实现前回归先命中 `unknown function: conv`，实现后定向 plan/engine 测试通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation473/cluster-report.json` 于 `2026/9/5 14:58:59` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation473/release-candidate.json` 于 `2026/9/5 15:12:45` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 474

- 继续收敛 P1-OPT-005/P1-EXE-004：统一表达式执行器新增 MySQL `ORD()`、`MAKE_SET()` 和 `EXPORT_SET()`；分别覆盖多字节首字符数值、按位选择字符串集合、按位导出 on/off 字符串，并接入编译表达式白名单。
- 新增真实 SQL 回归覆盖 `ORD('A')`、`MAKE_SET(5, ...)` 和 `EXPORT_SET(5, ..., 4)`；实现前回归先命中 `unknown function: ord`，实现后 plan/engine 定向测试通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation474/cluster-report.json` 于 `2026/9/5 15:15:33` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation474/release-candidate.json` 于 `2026/9/5 15:29:13` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 475

- 继续收敛 P1-OPT-005/P1-EXE-004：统一表达式执行器新增 MySQL `SOUNDEX()`，按常见 ASCII 发音编码规则返回四字符结果，并接入编译表达式白名单和 SQL 回归。
- 本轮首次 Connector/J 门禁在并发断开场景暴露 `fatal error: concurrent map read and map write`；该问题已转入下一轮 P0 稳定性修复，不以“测试偶发失败”结案。

## Continuation 476

- 修复 Connector/J 长连接/并发断开场景暴露的 P0 稳定性问题：`DecoupledMySQLMessageHandler.OnClose` 和 `OnError` 原先在删除前无锁读取 `sessionMap`，多个连接同时退出时可能触发 `fatal error: concurrent map read and map write`；新增原子取出并删除的会话注册表路径，关闭与错误回调均复用该路径，避免重复清理和 map 并发访问。
- 新增并发关闭回归测试；实现前测试可稳定复现运行时 map 崩溃，修复后并发关闭测试重复 100 次通过。
- `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation476/cluster-report.json` 于 `2026/9/5 15:41:03` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation476/release-candidate.json` 于 `2026/9/5 15:54:45` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 477

- 同步收口旧版 `MySQLMessageHandler` 的会话生命周期并发风险：`OnOpen` 将会话数量检查与注册合并到同一锁区间，`OnClose`/`OnError` 使用原子取出并删除，`OnMessage` 和认证更新使用受保护的 map 访问，避免备用协议入口留下与解耦 handler 相同的并发 map 崩溃路径。
- 新增旧版 handler 并发关闭回归测试；与解耦 handler 的并发关闭测试各重复 100 次通过。
- 旧版 handler 改动后 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation477/cluster-report.json` 于 `2026/9/5 16:00:07` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation477/release-candidate.json` 于 `2026/9/5 16:13:50` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 478

- 继续收敛 P1-OPT-001：外连接等值键传递现在会复制 preserved side 上所有可安全传递的单列谓词，而不是只复制第一条；因此 `a.id > 7 AND a.id < 10` 等范围交集会同时下推到 nullable side，原始 preserved-side 谓词仍保留以维持 LEFT/RIGHT JOIN 的 NULL 扩展语义。
- 新增优化器回归验证多条范围谓词均被传递；实现前测试先因只得到 1 条推导谓词失败，实现后 plan 定向测试重复 20 次通过，plan/engine 专项回归通过。
- `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation478/cluster-report.json` 于 `2026/9/5 16:18:44` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation478/release-candidate.json` 于 `2026/9/5 16:32:23` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures/errors/skips，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 479

- 继续收敛 P1-SQL-008：临时表 CTAS 现在支持 MySQL 常见的显式目标列且省略 `AS` 的形式，例如 `CREATE TEMPORARY TABLE selected_rows (ident BIGINT, text_value VARCHAR(20)) SELECT id, label FROM source_rows`；此前该语句会被普通 CREATE TABLE 路径静默建出空表，现改由真实 CTAS 物化路径执行源查询并写入行数据。
- 新增真实引擎回归验证显式列、无 `AS` 的临时 CTAS 行数据和列映射；既有临时表会话隔离、元数据、事务、清理以及 P0/P1 主链路回归保持通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation479/cluster-report.json` 于 `2026/9/5 16:38:36` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation479/release-candidate.json` 于 `2026/9/5 16:52:10` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 480

- 继续收敛 P1-OPT-001：优化器现在可安全穿透无 `LIMIT`、`ORDER BY`、`DISTINCT` 的直接列投影别名，将 `SELECT id AS user_id ... WHERE user_id > 1` 的条件改写为子计划上的 `id > 1`；计算列、重复别名和会改变行集语义的投影仍保留在投影上方。
- 新增优化器回归验证别名条件被改写并下推到底层选择，同时保留原有 identity projection、外连接、Apply、索引和列裁剪语义；定向回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation480/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation480/release-candidate.json` 于 `2026/9/5 17:11:57` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 481

- 继续收敛 P1-OPT-002：列裁剪现在会把直接列投影的输出别名反向映射为子计划真实列，例如 `id AS user_id` 在上层只需要 `user_id` 时，底层只保留 `id`，不再把别名当作独立物理列继续向下传播；计算列和重复别名继续采用保守路径。
- 新增列裁剪回归验证底层存在同名 `user_id` 时仍只扫描 `id`；别名谓词下推、qualified identity projection、外连接/Apply/索引相关回归保持通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation481/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation481/release-candidate.json` 于 `2026/9/5 17:29:06` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 482

- 继续收敛 P1-OPT-001：优化器现在识别同长度、逐位置且全部为纯列的元组等值连接键，例如 `(a.k1, a.k2) = (b.k1, b.k2)`，并将连接键上的常量、范围、模式和 NULL 安全谓词按对应列安全传递到另一侧；计算元组、长度不一致元组和左右子计划混杂的元组继续保守跳过。
- 新增 LEFT JOIN 复合键谓词传递回归：`a.k1 > 7` 会在保留原条件的同时安全推导出 nullable side 的 `b.k1 > 7`；定向计划回归重复 20 次通过。
- 补齐 INNER JOIN 的另一条入口：WHERE 中的同长度纯列元组等值条件现在会被移动到 Join 条件，复合键连接不再停留在顶层过滤；定向计划回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation482/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation482/release-candidate.json` 于 `2026/9/5 17:48:19` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 483

- 继续收敛 P1-OPT-001：复用元组等值键识别器，将 INNER JOIN 上方 WHERE 中的同长度纯列元组等值条件移动到 Join 条件；复合键连接不再停留在顶层 Selection，计算元组、长度不一致和左右混杂元组仍保守保留。
- 新增 INNER JOIN 元组等值移动回归；定向计划回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation483/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation483/release-candidate.json` 于 `2026/9/5 18:05:12` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 484

- 继续收敛 P1-SQL-007/P1-SQL-008：持久表和临时表 CTAS 现在接受常见且可由当前存储兑现的 `ENGINE=InnoDB`、`DEFAULT CHARSET`/`CHARSET` 和 `COLLATE` 选项；选项被识别并剥离后进入真实 CTAS schema 推导与行物化路径，临时表路由与持久表共用同一 CTAS 定义识别器。
- 新增持久表/临时表 CTAS 表选项真实回归，先行失败后转绿并重复 20 次通过；不接受未实现的非 InnoDB 存储引擎。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation484/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation484/release-candidate.json` 于 `2026/9/5 18:25:03` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 485

- 继续收敛 P1-SQL-003/P1-SQL-004：普通 `CREATE TABLE` 现在会把常见表级 `ENGINE`、`CHARSET`、`COLLATE` 和 `COMMENT` 选项解析为规范化的持久元数据；此前语句虽可执行，但显式 `COLLATE` 会错误保留默认排序规则。列级 `REFERENCES` 增加运行时回归，确认外键校验与 `ON DELETE CASCADE` 均实际生效。
- 新增普通建表选项元数据断言和内联外键执行断言；两项定向回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation485/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation485/release-candidate.json` 于 `2026/9/5 18:44:57` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 486

- 继续收敛表元数据兼容：`INFORMATION_SCHEMA.TABLES` 现在从持久 `.frm` 选项读取 `ENGINE`、`COLLATION` 和原始 `CREATE_OPTIONS`，显式 `COLLATE=utf8mb4_bin` 不再被固定的 `utf8mb4_0900_ai_ci` 覆盖。
- 新增 `INFORMATION_SCHEMA.TABLES.TABLE_COLLATION` 回归，普通表级选项与外键运行时回归保持通过，并重复执行 20 次验证稳定性。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation486/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation486/release-candidate.json` 于 `2026/9/5 19:01:51` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 487

- 继续收敛表定义可见性：`SHOW CREATE TABLE` 现在从持久 `raw_options` 恢复显式 `DEFAULT CHARSET` 和 `COLLATE`，并规范化 `ENGINE=InnoDB` 的展示；建表、`INFORMATION_SCHEMA.TABLES` 和 JDBC 元数据看到的排序规则保持一致。
- 新增 `SHOW CREATE TABLE` 表级字符集/排序规则断言；定向回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation487/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation487/release-candidate.json` 于 `2026/9/5 19:18:33` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 488

- 继续收敛 P1-SQL-003：`CREATE TABLE ... AUTO_INCREMENT=n` 现在会在建表成功后初始化持久的自增序列，第一条隐式主键从指定值开始；`INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` 会反映当前下一值，表无自增列时建表失败并回滚。
- 新增建表自增起始值、信息模式下一值以及已有 `ALTER TABLE ... AUTO_INCREMENT` 兼容性回归；定向回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation488/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation488/release-candidate.json` 于 `2026/9/5 19:37:27` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 489

- 继续收敛 P1-SQL-003：`ALTER TABLE ... DEFAULT CHARACTER SET/CHARSET ... COLLATE ...` 现在通过原子 `.frm` 更新持久化表级字符集/排序规则，并同步影响 `INFORMATION_SCHEMA.TABLES.TABLE_COLLATION` 与 `SHOW CREATE TABLE`；不支持的 ALTER 变体仍明确返回错误。
- 新增 ALTER 表级字符集/排序规则的失败先行回归，修复后重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation489/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation489/release-candidate.json` 于 `2026/9/5 19:55:01` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 490

- 继续收敛 P1-SQL-003：ALTER 表级字符集兼容现在覆盖三类常见变体：`DEFAULT CHARACTER SET/CHARSET ... COLLATE ...`、`CONVERT TO CHARACTER SET ... COLLATE ...`、`DEFAULT COLLATE=...`；统一通过原子 `.frm` 更新，并让 `INFORMATION_SCHEMA.TABLES` 与 `SHOW CREATE TABLE` 读取更新后的值。
- 新增上述两个扩展变体和仅排序规则变体的失败先行回归；三项定向回归重复 20 次通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation490/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation490/release-candidate.json` 于 `2026/9/5 20:12:41` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、1 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 493

- 继续收口 P0 发布证据：套件内重新生成并校验 P0-B 行/页/WAL 状态差分、P0-C 一致性证据、P0-D live `/metrics` probe、P0-E 定时回滚合同证据，避免复用上一轮 artifact 导致当前运行时间门禁失败。
- P0-B recovery state diff、P0-C concurrency consistency、P0-D observability/metrics/logging/alert、P0-E full-chain、最终 Go 回归和 evidence bundle 均通过；当前套件汇总为 `NOT_READY` 仅因为外部审批包仍为 HOLD、17 个 owner sign-off 未完成、7 个 P0 风险仍未关闭且 owner 为 TBD。
- 当前 P0 证据套件：`reports/compatibility/p0-evidence-current-continuation493/p0_evidence_suite_20260905_151916.summary.json`；readiness audit：`reports/compatibility/p0-evidence-current-continuation493/delivery_readiness_audit_20260905_152018.json`。

## Continuation 494

- 继续实现 P1-SQL-003：普通建表现在识别并持久化常见 InnoDB `ROW_FORMAT=Dynamic/Compact/Compressed/Redundant` 选项；`INFORMATION_SCHEMA.TABLES.ROW_FORMAT` 返回规范化值，`SHOW CREATE TABLE` 恢复请求的 `ROW_FORMAT`，未指定时保持 MySQL 兼容的 `Dynamic` 默认值。
- 新增 `TestCreateTablePersistsRowFormatMetadata`，实现前先因 `.frm` 缺少 `row_format` 元数据失败，实现后定向回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation494/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation494/release-candidate.json` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 495

- 继续补齐 P1-SQL-007/P1-SQL-008：raw 持久表 CTAS 与临时表 CTAS 现在不会丢弃 `ROW_FORMAT` 选项；选项会进入生成的真实 `CREATE TABLE`，并可从 `INFORMATION_SCHEMA.TABLES.ROW_FORMAT` 与 `SHOW CREATE TABLE` 读回。
- 新增持久 CTAS 和临时 CTAS 的 `ROW_FORMAT=COMPRESSED` 回归；实现前持久 CTAS 先因 parser/选项路径失败，实现后 CTAS/普通建表/临时表组合定向回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation495/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation495/release-candidate.json` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 496

- 继续补齐 P1-SQL-003：`ALTER TABLE ... ROW_FORMAT=Dynamic/Compact/Compressed/Redundant` 现在通过原子 `.frm` 更新持久化表选项，并同步 `INFORMATION_SCHEMA.TABLES.ROW_FORMAT` 与 `SHOW CREATE TABLE`；已有字符集/排序规则 ALTER 路径保持通过。
- 新增 `TestAlterTablePersistsRowFormatOption`，实现前先返回 `unsupported ALTER TABLE action`，实现后与既有 ALTER 字符集回归重复 20 次通过。

## Continuation 497

- 继续收敛 P1-OPT-001：优化器现在会展开 parser 生成的嵌套 `AND` 关联条件，再识别其中的多列等值键；因此 INNER JOIN/INNER Apply 在 `ON (a.id=b.id AND a.col1=b.col1)` 形式下也能安全进行常量/范围谓词传递，LEFT Apply 的 NULL 扩展边界保持不变。
- 新增 `TestPredicatePushdownInfersInnerApplyWithConjunctiveCorrelation`，实现前右侧没有推导条件，实现后与既有关联 Apply 回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation497/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation497/release-candidate.json` 为 `GO`；integration 与 JDBC 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，release checks 失败数为 0，服务 stderr 未出现 `fatal error`/`concurrent map`/`panic`。

## Continuation 498

- 继续收敛 P1-OPT-001：`moveInnerJoinEqualities` 现在会展开嵌套 `AND`，把 parser 生成的多个 INNER JOIN 跨表等值条件分别放入 Join 条件；非等值或无法证明安全的残余条件仍留在上层选择中。
- 新增 `TestPredicatePushdownMovesNestedInnerJoinEqualitiesIntoJoinCondition`；实现前嵌套条件无法移动，实现后两个等值键均被识别，定向回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation498/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation498/release-candidate.json` 为 `GO`；integration、JDBC、构建、并发、崩溃恢复和可观测性检查均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 50 秒。

## Continuation 499

- 继续收敛 P1-OPT-001/P1-EXE-003：非关联 Apply 转换为普通 Join 时，现在会展开 `JoinConds` 中的嵌套 `AND`，让多个跨表等值键分别进入 Join 条件；SEMI/ANTI Apply 仍保持原语义，不会被降级为 INNER JOIN。
- 新增 `TestSubqueryOptimizerFlattensUncorrelatedApplyJoinConditions`；实现前转换结果只有 1 个嵌套条件，实现后两个等值条件均保留，定向回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation499/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation499/release-candidate.json` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 45 秒。

## Continuation 500

- 继续收敛 P1-OPT-003：`AdaptiveSampler.GetSampleRate` 现在真正读取可配置的行数阈值表，并对配置值做 0～1 范围约束；默认四档采样策略保持不变，统计采样配置不再出现“保存但不生效”。
- 新增 `TestAdaptiveSamplerUsesConfiguredThresholds`；实现前自定义阈值被忽略，实现后各阈值边界按配置生效，统计回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation500/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation500/release-candidate.json` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 58 秒。

## Continuation 501

- 继续收敛 P1-OPT-004：索引条件提取现在只把“非空 literal 前缀 + 尾部 `%`”识别为可下推的 LIKE/NOT LIKE；`abc%def`、包含未转义 `_` 或通配符后的非 `%` 内容的模式会保留残余精确过滤，避免把中间通配符误当成前缀范围。
- 新增 `TestLikeWithWildcardAfterPrefixIsNotPushable`；实现前 `abc%def` 被错误标记为可下推，实现后所有 LIKE/NOT LIKE 定向回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation501/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation501/release-candidate.json` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 47 秒。

## Continuation 502

- 继续收敛 P1-OPT-001：非关联 Apply 只有在拥有恰好两个子计划时才允许转换为普通 Join；异常的一子节点/无效 Apply 现在保留原边界并递归优化已有子计划，避免制造非法物理计划。
- 新增 `TestSubqueryOptimizerPreservesApplyWithInvalidArity`；实现前一子节点 Apply 被转换为一子节点 Join，实现后边界保持，相关子查询优化回归重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation502/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation502/release-candidate.json` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 11 分 04 秒。

## Continuation 503

- 继续补齐 P4 JSON 函数兼容：新增 `JSON_STORAGE_SIZE` 的函数分派、参数校验、`NULL` 传播和 JSON 解析路径；当前返回规范化 JSON 的字节长度，作为可验证的兼容近似，完整 MySQL 二进制 JSON 存储布局计量仍属于后续边界。
- 新增 `JSON_STORAGE_SIZE` 失败先行回归，定向测试重复 20 次通过；非法 JSON 会返回解析错误，不会静默产生错误大小。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation503/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation503/release-candidate.json` 于 `2026-09-05T23:02:10Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 52 秒。

## Continuation 504

- 继续补齐 P4 JSON 函数兼容：新增 `JSON_STORAGE_FREE` 的函数分派、参数校验、`NULL` 传播和 JSON 解析路径；独立 JSON 文档无可回收的 partial-update 空间，因此返回 0，列存储级二进制 JSON free-space 统计仍属于后续边界。
- 新增 `JSON_STORAGE_FREE` 失败先行回归，定向测试重复 20 次通过；非法 JSON 会返回解析错误。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation504/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation504/release-candidate.json` 于 `2026-09-05T23:18:45Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 50 秒。

## Continuation 505

- 继续补齐 P4 JSON 函数兼容：新增 `JSON_VALUE` 函数分派和常见两参数标量路径提取；缺失路径与 JSON `null` 返回 SQL NULL，对象/数组不伪装成标量，`RETURNING`/`ON EMPTY`/`ON ERROR` 等完整扩展语法仍保留边界。
- 新增直接 evaluator 与 `Function.Eval` 回归，覆盖数字、字符串和函数分派，定向测试重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation505/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation505/release-candidate.json` 于 `2026-09-05T23:36:49Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 11 分 59 秒。

## Continuation 506

- 继续补齐 P4 正则函数兼容：新增 `REGEXP_INSTR`，支持常见 `position`、`occurrence`、`return_option` 和 `match_type=i` 参数，返回匹配起点/终点的 MySQL 风格 1-based 位置；完整 ICU/collation 规则矩阵仍保留边界。
- 新增 `REGEXP_INSTR` 起点、第二次匹配和返回终点回归，定向测试重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation506/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation506/release-candidate.json` 于 `2026-09-05T23:53:26Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 53 秒。

## Continuation 507

- 继续收敛 P1-EXE-004：`JSON_VALUE`、`JSON_STORAGE_SIZE`、`JSON_STORAGE_FREE` 和 `REGEXP_INSTR` 现在进入编译表达式白名单；编译 evaluator 与解释 evaluator 对常量路径/正则输入保持一致，避免新兼容函数在热点投影/过滤路径退回解释执行。
- 新增四类函数的编译/解释结果一致性回归，定向测试重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation507/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation507/release-candidate.json` 于 `2026-09-06T00:11:21Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 52 秒。

## Continuation 508

- 继续收敛 P3/P4 PERFORMANCE_SCHEMA 兼容：新增 `performance_schema.events_waits_history` 查询路由，并按请求视图区分 `current`、`history` 与 `history_long` 的结果标识；锁等待字段继续来自真实 wait graph，不生成伪造等待记录。
- 新增锁等待 history 视图回归，定向测试重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation508/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation508/release-candidate.json` 于 `2026-09-06T00:28:25Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 51 秒。

## Continuation 509

- 继续补齐信息模式变量视图：新增 `INFORMATION_SCHEMA.SYSTEM_VARIABLES`、`GLOBAL_VARIABLES`、`SESSION_VARIABLES` 的常见兼容行，并支持按 `VARIABLE_NAME='...'` 过滤；三者复用同一变量投影逻辑，避免返回伪造的空列。
- 新增三个信息模式变量视图回归，定向测试重复 20 次通过。
- 全量 `go test ./... -count=1 -timeout 5m` 通过；集群报告 `reports/compatibility/p1-cluster-current-continuation509/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation509/release-candidate.json` 于 `2026-09-06T00:45:42Z` 为 `GO`；所有 release checks 均 `PASS`，Connector/J `test_count=136`，0 failures、0 errors、0 skipped，JDBC 总耗时约 10 分 53 秒。

## Continuation 510

- 继续收敛 P1-OPT-001/P1-OPT-002：输出列血缘识别现在覆盖 `LogicalApply`；INNER/LEFT Apply 暴露左右子计划输出列，SEMI/ANTI Apply 仅暴露外侧列，避免关联子查询的隐藏列参与列归属判断和列裁剪。
- 新增 Apply 输出列血缘失败先行回归，确认 INNER 与 SEMI 语义分别保留正确列集合；定向优化器回归通过。
- 本轮改动后的 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation510/cluster-report.json` 于 `2026-09-06T00:50:48Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation510/release-candidate.json` 于 `2026-09-06T01:06:01Z` 为 `GO`；build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 均 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 总耗时约 11 分 02 秒。

## Continuation 511

- 继续收敛 P1-OPT-001/P1-OPT-002：输出列血缘识别现在从 `LogicalSubquery.Subplan` 暴露内部输出列名；子查询作为 Apply/Union 分支时，外层列归属和列裁剪不再得到空输出集合。
- 新增 `LogicalSubquery` 输出列血缘失败先行回归；Apply 与 Subquery 定向优化器回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation511/cluster-report.json` 于 `2026-09-06T01:07:37Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation511/release-candidate.json` 于 `2026-09-06T01:22:51Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 512

- 继续收敛 P1-OPT-001/P1-OPT-002：输出列血缘识别补齐 `LogicalValues` 与 `LogicalAggregation`；优先读取真实 schema 列名，无 schema 时分别从 Values 表达式和聚合的分组/聚合表达式推导输出名。
- 新增 Values/聚合输出列血缘失败先行回归；定向优化器回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation512/cluster-report.json` 于 `2026-09-06T01:24:37Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation512/release-candidate.json` 于 `2026-09-06T01:39:41Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 513

- 继续收敛 P1-OPT-005/P1-EXE-004：`EXTRACT` 现在支持 MySQL 复合时间单位 `YEAR_MONTH`、`DAY_HOUR`、`DAY_MINUTE`、`DAY_SECOND`、`DAY_MICROSECOND`、`HOUR_MINUTE`、`HOUR_SECOND`、`HOUR_MICROSECOND`、`MINUTE_SECOND`、`MINUTE_MICROSECOND`、`SECOND_MICROSECOND`；复合单位进入独立 evaluator 和编译函数白名单，未知单位仍明确报错。
- 新增复合单位重写、编译/解释执行一致性以及真实引擎 `SELECT` 回归；修正浮点投影的定点输出，避免 `DAY_MICROSECOND` 等结果被编码成科学计数法。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation513/cluster-report.json` 于 `2026-09-06T01:48:51Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation513/release-candidate.json` 于 `2026-09-06T02:04:02Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 47 秒。

## Continuation 514

- 继续收敛 P1-OPT-001/P1-OPT-002：普通 CTE、递归 CTE 的显式列名现在会传递到 `LogicalCTEScan`，`getPlanOutputColumnNames` 优先使用 `WITH cte(col1, col2)` 声明的可见列名，避免按内部查询 schema 误判谓词归属和列裁剪需求。
- 新增显式 CTE 列名的失败先行回归，覆盖普通 CTE、递归 CTE 定义以及真实 `LogicalCTEScan` 绑定路径；`server/innodb/plan` 全包测试通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation514/cluster-report.json` 于 `2026-09-06T02:07:52Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation514/release-candidate.json` 于 `2026-09-06T02:22:56Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 47 秒。

## Continuation 515

- 继续收敛 P1-OPT-002：列裁剪现在处理显式 CTE 列别名到内部查询列的序号映射；例如 `WITH recent(recent_id) AS (SELECT id ...)` 在上层只请求 `recent_id` 时，底层 `LogicalCTEScan` 会保留真实 `id` 列，而不会生成空 schema。
- 新增真实 CTE 构建、输出血缘和列裁剪失败先行回归；`server/innodb/plan` 全包测试通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation515/cluster-report.json` 于 `2026-09-06T02:24:48Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation515/release-candidate.json` 于 `2026-09-06T02:39:57Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 51 秒。

## Continuation 516

- 继续收敛 P1-EXE-004：实现 `UUID_SHORT()`，按 `server_id + server_start_time + increment` 生成单调值；序列状态通过原子临时文件发布并持久化，进程快速重启时不会复用同一启动时间窗口内的已发放值；无配置时保留 server_id=1 的进程级默认生成器。
- 新增直接 evaluator、序列并发安全边界和真实引擎 SQL 重启回归；`UUID_SHORT()` 不进入编译表达式缓存，保持其非确定性/序列语义。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation516/cluster-report.json` 于 `2026-09-06T02:44:40Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation516/release-candidate.json` 于 `2026-09-06T02:59:50Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 47 秒。

## Continuation 517

- 继续收敛 P1-OPT-001：谓词下推/关联子查询 Apply 现在把 NULL-safe equality（`<=>`）识别为合法的单列或同元组关联键；因此 INNER/SEMI/ANTI Apply 上的 `IS NULL`、范围、模式等安全谓词可以沿 NULL-safe 关联键推断到另一侧，同时 LEFT Apply 仍禁止向 NULL 扩展侧下推。
- 新增 NULL-safe Apply 关联谓词失败先行回归；实现前右侧没有推断条件，实现后真实生成 `right.id IS NULL`，计划器全包通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation517/cluster-report.json` 于 `2026-09-06T03:04:07Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation517/release-candidate.json` 于 `2026-09-06T03:19:25Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 54 秒。

## Continuation 518

- 继续收敛 P1-OPT-002：`LogicalSubquery` 作为 Apply/Union 等实际子计划时，列裁剪现在同时保留 `OuterRefs` 与上层传入的可见输出列；相关子查询仍保留关联列，非相关子查询不再因边界节点丢失上层需要的投影列。
- 新增子查询输出列裁剪失败先行回归；实现前 `IN` 子查询底层扫描保留全部列，实现后只保留请求的 `name` 列；计划器相关回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation518/cluster-report.json` 于 `2026-09-06T03:21:42Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation518/release-candidate.json` 于 `2026-09-06T03:36:46Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 54 秒。

## Continuation 519

- 继续收敛 P1-OPT-001：Apply 谓词下推现在可从右侧关联子查询的直接 `LogicalSelection` 中提取常量/范围/模式/NULL-safe 谓词，并沿 INNER/SEMI/ANTI 的单列或同元组等值关联键反推到外层；LEFT Apply、跨子计划表达式和非确定性形状保持保守不改写。
- 新增“内层 `right.id = 7` 反推外层 `left.id = 7`”失败先行回归，并与 NULL-safe、复合关联条件和 LEFT Apply 边界回归一起验证；计划器全包通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation519/cluster-report.json` 于 `2026-09-06T03:40:38Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation519/release-candidate.json` 于 `2026-09-06T03:55:51Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 54 秒。

## Continuation 520

- 继续收敛 P1-OPT-003：增强统计收集器不再对所有 UNIQUE 索引无条件使用表行数作为基数；只要索引包含可空列，就通过真实解码记录采样 distinct key（包括 NULL key）计算 Cardinality，非空 UNIQUE 索引继续走精确行数路径。
- 新增 nullable UNIQUE 索引统计失败先行回归：三行数据中 `name` 为 `alice,NULL,NULL` 时基数从错误的 3 修正为 2；既有持久化/实时索引页统计回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation520/cluster-report.json` 于 `2026-09-06T03:57:46Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation520/release-candidate.json` 于 `2026-09-06T04:12:58Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 54 秒。

## Continuation 521

- 继续收敛 P1-OPT-001：关联子查询内层常量谓词反推现在可穿过无语义屏障的直接列投影（`Projection(Selection(...))`），同时拒绝 DISTINCT、ORDER BY、LIMIT/OFFSET 投影，避免改变重复行、顺序或分页语义。
- 新增投影包裹形态的失败先行回归；`right.id = 9` 经 INNER Apply 等值关联可反推为 `left.id = 9`，既有 LEFT Apply 和复杂投影边界保持不推断。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation521/cluster-report.json` 于 `2026-09-06T04:14:56Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation521/release-candidate.json` 于 `2026-09-06T04:30:01Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 54 秒。

## Continuation 522

- 继续收敛 P1-OPT-001/P1-OPT-002：Apply 右侧直接列投影的输出别名现在进入 Join ownership 与谓词血缘映射；`SELECT right.id AS right_key ...` 的内层 `right.id = constant` 可以沿外层 `left.id = right_key` 安全反推，物理隐藏列不会再被误认为可见输出。
- 新增派生列别名反推失败先行回归；别名回归、NULL-safe 关联、LEFT Apply 保护和复杂投影屏障均通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation522/cluster-report.json` 于 `2026-09-06T04:32:33Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation522/release-candidate.json` 于 `2026-09-06T04:47:37Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped，JDBC 门禁耗时约 10 分 54 秒。

## Continuation 523

- 继续收敛 P1-OPT-002：Join ownership 现在按异构行源的可见输出边界取列名，覆盖 `LogicalSubquery`、`LogicalValues`、`LogicalAggregation`、`LogicalUnion`、`LogicalApply`；隐藏的物理子列不再从这些边界泄漏到关联列归属推断。
- 新增子查询可见输出列失败先行回归，并保持 UNION 可见别名回归通过；优化器、全量 Go、集群与 Connector/J 门禁均通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation523/cluster-report.json` 于 `2026-09-06T04:49:43Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation523/release-candidate.json` 于 `2026-09-06T05:05:19Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 524

- 继续收敛 P1-OPT-002：CTE、递归 CTE 和 CTE Statement 的 Join ownership 现在只使用 CTE 声明列名或 Statement Body 输出契约，不再递归暴露定义查询中的物理列；这与显式 CTE 列裁剪和 `LogicalCTEScan` 输出血缘保持一致。
- 新增普通 CTE、递归 CTE、CTE Statement 三种边界的失败先行回归；实现前会泄漏 `cte_physical_source.id` 且缺少 `cte_visible_id`，实现后仅保留可见输出。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation524/cluster-report.json` 于 `2026-09-06T05:05:58Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation524/release-candidate.json` 于 `2026-09-06T05:21:13Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 525

- 继续收敛 P1-OPT-001：Apply 右侧 `LogicalSubquery` 的 `IN`/`EXISTS`/`SEMI`/`ANTI` 子查询现在可穿透到内部 `Selection`/直接列投影，沿 INNER/SEMI/ANTI 等值关联键反推常量、范围、模式或 NULL-safe 谓词；标量子查询和带语义屏障的形态仍不反推。
- Join ownership 对带限定名的关联列现在可匹配子查询/CTE 的可见未限定输出别名，但不把隐藏物理列重新加入可见集合；新增半连接子查询失败先行回归并验证实现前后差异。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation525/cluster-report.json` 于 `2026-09-06T05:23:45Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation525/release-candidate.json` 于 `2026-09-06T05:38:52Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 526

- 继续收敛 P1-OPT-001：Apply 子查询条件提取现在递归合并多层 `LogicalSelection`，不再遗漏内层范围/常量谓词；当 Selection 位于无语义屏障的直接列投影之上时，会把可见输出别名重写回底层列后再沿关联键反推。
- 新增多层 Selection 以及“Selection → Projection(alias) → Selection”失败先行回归；标量子查询、DISTINCT/ORDER/LIMIT/OFFSET 等语义屏障仍保持保守不改写。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation526/cluster-report.json` 于 `2026-09-06T05:44:03Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation526/release-candidate.json` 于 `2026-09-06T06:00:34Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 527

- 继续收敛 P1-SQL-001：相关标量子查询现在支持出现在外层 `WHERE` 的单个 `AND` 条件中，例如 `(SELECT COUNT(*) ... WHERE inner.key = outer.key) >= 2`；标量子查询按每个外层行重新绑定，空结果保持 `NULL`、`COUNT(*)` 空结果保持 `0`，外层普通过滤与 `LIMIT` 在标量条件之后保持正确顺序。
- 新增相关标量谓词失败先行回归，覆盖基本比较以及“外层 `AND` 条件 + `ORDER BY` + `LIMIT`”；复杂 `OR` 组合和多个标量谓词仍保守交给通用路径，未宣称已支持。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation527/cluster-report.json` 于 `2026-09-06T06:11:11.3254633Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation527/release-candidate.json` 于 `2026-09-06T06:24:55.0161581Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 528

- 继续收敛 P1-SQL-001：外层 `WHERE` 现在可以在同一个 `AND` 合取中包含多个相关标量子查询谓词；每个标量子查询都按当前外层行独立绑定和执行，普通外层条件仍留在正常过滤路径，`LIMIT` 不会在标量条件执行前截断候选行。
- 新增多个相关标量谓词失败先行回归，覆盖 `COUNT(*) >= ... AND MAX(...) >= ...`；复杂 `OR` 组合、同一谓词内多个标量表达式仍保守未扩展。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation528/cluster-report.json` 于 `2026-09-06T06:29:43.4757609Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation528/release-candidate.json` 于 `2026-09-06T06:43:28.3250007Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 529

- 继续收敛 P1-SQL-001：相关标量子查询谓词现在保留完整的布尔表达式，可处理顶层 `OR` 以及同一 OR 表达式中的多个标量子查询；多个子查询按外层行逐一求值后从右向左替换，避免替换位置偏移，并保持 `AND` 优先级与 SQL 三值逻辑。
- 新增 OR 语义和 OR 内多标量子查询失败先行回归；`EXISTS`、`IN`、`ANY/SOME/ALL` 不会误进入标量路径，仍由各自专用兼容处理。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation529/cluster-report.json` 于 `2026-09-06T06:49:07.8571675Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation529/release-candidate.json` 于 `2026-09-06T07:02:59.1829846Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 530

- 继续收敛 P1-OPT-005：存储集成执行器现在支持 `REGEXP`/`NOT REGEXP` 比较，使用正则模式匹配并保持 MySQL `NULL` 三值逻辑；计划层已有的正则表达式能力不再在 SQL 扫描路径丢失。
- 新增 `REGEXP`/`NOT REGEXP` 失败先行回归，覆盖匹配、不匹配和 NULL 行；相关子查询回归保持通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation530/cluster-report.json` 于 `2026-09-06T07:08:31.1805004Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation530/release-candidate.json` 于 `2026-09-06T07:22:20.6145838Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 531

- 继续收敛 P1-OPT-005：普通 JOIN 执行路径现在支持 `REGEXP`/`NOT REGEXP` 连接条件，与存储集成扫描路径保持一致；正则编译失败返回不匹配，NULL 行不满足谓词。
- 新增 JOIN `REGEXP` 失败先行回归，覆盖连接条件中的匹配与不匹配行；普通扫描、JOIN 和相关子查询正则回归均通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation531/cluster-report.json` 于 `2026-09-06T07:29:48.7138889Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation531/release-candidate.json` 于 `2026-09-06T07:43:38.9182539Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 532

- 继续收敛 P1-OPT-005：CNF 转换器现在把 `REGEXP`/`NOT REGEXP` 识别为原子谓词，并支持 `NOT (col REGEXP pattern)` 与 `NOT (col NOT REGEXP pattern)` 的运算符取反；优化器不再因该比较形式保留无法下推的 `NOT` 包装。
- 新增 CNF `REGEXP` 失败先行回归，覆盖原子识别和 `REGEXP` 到 `NOT REGEXP` 的 NULL-safe 结构转换。
- 本轮 `go test ./server/innodb/plan -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation532/cluster-report.json` 于 `2026-09-06T07:46:52.7733856Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation532/release-candidate.json` 于 `2026-09-06T08:00:47.8875697Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 533

- 继续收敛 P1-OPT-005：普通 JOIN 执行路径现在支持 `IN/NOT IN` 和 `LIKE/NOT LIKE` 连接条件；`IN` 复用 NULL-aware 三值逻辑，`LIKE` 复用兼容模式匹配，避免 parser 已接受的连接谓词统一返回 false。
- 新增 JOIN `LIKE` 与跨表 `IN` 失败先行回归，覆盖真实连接行匹配；既有 REGEXP、范围、布尔和函数连接回归保持通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation533/cluster-report.json` 于 `2026-09-06T08:05:07.1867153Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation533/release-candidate.json` 于 `2026-09-06T08:18:53.2116258Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 534

- 继续收敛 P1-OPT-005：JOIN 谓词现在使用 SQL 三值逻辑计算 `AND/OR/NOT`、比较、范围和 `IN/NOT IN`；`UNKNOWN` 不再被 `NOT` 错误翻转为 TRUE，只有 TRUE 才保留连接结果。
- 新增 `NOT (expr = NULL)` 的失败先行回归，固化 MySQL UNKNOWN 语义；JOIN 的 REGEXP、LIKE、IN、范围、函数和普通比较回归保持通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation534/cluster-report.json` 于 `2026-09-06T08:23:28.6484219Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation534/release-candidate.json` 于 `2026-09-06T08:37:15.8955206Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 535

- 继续收敛 P1-SQL-007/P1-SQL-008：CTAS 现在支持 MySQL 常见的 `IGNORE`/`REPLACE` 行物化模式；持久表和临时表均把模式映射到真实 `INSERT IGNORE`/`REPLACE`，重复主键分别保留首行或替换为后行。
- 存储集成 DML 现在对 `INSERT IGNORE` 过滤已有行和批内重复键，对 `REPLACE` 合并批内重复键并执行删除后插入，避免 CTAS 只识别语法却仍按普通 INSERT 报错。
- 新增 CTAS `IGNORE`/`REPLACE` 失败先行回归，覆盖持久表、临时表和重复键物化结果。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation535/cluster-report.json` 于 `2026-09-06T08:49:32.8012052Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation535/release-candidate.json` 于 `2026-09-06T09:03:16.8450190Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 536

- 继续收敛 P1-SQL-003：`ALTER TABLE ... DROP COLUMN` 不再只修改 `.frm` 元数据；现有聚簇记录会按旧列布局读取、按新列布局重写，支持删除尾列和中间列，保留主键/隐藏行标识，并在失败时恢复旧元数据和已处理记录。
- 删除列后的后续 INSERT 与 `SELECT *` 已加入失败先行回归；中间列删除验证后续列值不会发生位置错位，二级索引刷新仍走统一重建路径。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation536/cluster-report.json` 于 `2026-09-06T09:12:47.9446231Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation536/release-candidate.json` 于 `2026-09-06T09:27:01.9178161Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 537

- 继续收敛 P1-SQL-003：`ALTER TABLE ... ADD COLUMN ... FIRST/AFTER` 现在会在变更 `.frm` 后按旧/新列布局重写已有聚簇记录，避免新增列插入到中间或首位时把旧行字段错位。
- 新增 `ADD COLUMN ... FIRST` 失败先行回归，验证已有行在新增首列后保留原值并以 `NULL`/default 填充新列；DROP COLUMN 尾列/中间列与 ADD COLUMN 的共享重写路径回归通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation537/cluster-report.json` 于 `2026-09-06T09:38:58.2836342Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation537/release-candidate.json` 于 `2026-09-06T09:52:54.5793680Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 538

- 继续收敛 P1-SQL-003：`ALTER TABLE ... MODIFY COLUMN`、`CHANGE COLUMN` 和原始 `RENAME COLUMN` 现在会在元数据更新后按旧列名/新列名映射重写已有聚簇记录，支持列位置变化、类型变化和改名而不丢失已有值。
- `CHANGE COLUMN` 的改名映射、`RENAME COLUMN` 的原始 DDL 路径和 `MODIFY ... FIRST` 的已有行数据均加入失败先行回归；二级索引在重写后统一刷新。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation538/cluster-report.json` 于 `2026-09-06T10:03:57.0187978Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation538/release-candidate.json` 于 `2026-09-06T10:17:51.8507572Z` 为 `GO`；Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 539

- 继续收敛 P1-SQL-004：`ALTER TABLE ... ADD [CONSTRAINT] ... CHECK (...)` 在新增强制约束落盘前扫描已有聚簇记录并按 MySQL 三值逻辑校验；若已有行返回 FALSE，ALTER 失败且 CHECK 元数据不写入，`NOT ENFORCED` 仍允许保留不满足约束的历史数据。
- 新增“已有违规行添加强制 CHECK”失败先行回归，验证错误返回、`information_schema.check_constraints` 不出现新约束，以及失败 ALTER 后后续 DML 行为不受影响。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation539/cluster-report.json` 于 `2026-09-06T10:25:47.0811691Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation539/release-candidate.json` 于 `2026-09-06T10:39:44.3843675Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 540

- 继续收敛 P1-SQL-004：`ALTER TABLE ... ALTER CHECK ... ENFORCED` 现在也会在切换元数据前扫描既有聚簇记录；历史数据违反约束时切换失败并保持 `NOT ENFORCED`，清理违规数据后才允许切换为强制约束。
- 新增切换既有违规数据的失败先行回归，并扩展原有切换测试覆盖“失败不改变状态、清理后成功、后续违规 INSERT 被拒绝”；新增 CHECK 与切换 CHECK 两条路径共用历史行验证。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation540/cluster-report.json` 于 `2026-09-06T10:44:26.0689509Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation540/release-candidate.json` 于 `2026-09-06T10:58:25.8507644Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 541

- 继续收敛 P1-SQL-004：`ALTER TABLE ... ADD FOREIGN KEY` 现在会在元数据写入前扫描已有子表聚簇记录，验证每个非 NULL 外键值都能匹配父表引用列，并校验引用列存在；历史数据不满足时 ALTER 失败且外键/自动辅助索引不落盘。
- 新增“已有违规子行添加外键”失败先行回归，覆盖失败后无外键元数据、修复父表数据后同一 ALTER 成功，以及后续非法子行写入被拒绝；已有外键改名、级联和运行时校验回归保持通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation541/cluster-report.json` 于 `2026-09-06T11:03:55.3307223Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation541/release-candidate.json` 于 `2026-09-06T11:18:08.5327577Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 542

- 继续收敛 P1-SQL-003/P1-SQL-004：`ALTER TABLE ... DROP COLUMN` 现在同步处理被删除列引用的 CHECK 约束；只引用该列的 CHECK 随列自动删除，仍引用其他列的 CHECK 会阻止 DDL，避免留下悬空约束表达式。
- 新增单列 CHECK 自动删除和多列 CHECK 拒绝 DROP 的失败先行回归；既有 DROP COLUMN 行重写、后续 INSERT 和 CHECK 元数据查询回归保持通过。
- 本轮 `go test ./server/innodb/engine -count=1 -timeout 5m` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation542/cluster-report.json` 于 `2026-09-06T11:25:28.6820694Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation542/release-candidate.json` 于 `2026-09-06T11:39:34.3945570Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 543

- 继续收敛 P1-SQL-004：`ALTER TABLE ... DROP COLUMN` 现在拒绝删除本表外键使用的本地列；必须先显式 `DROP FOREIGN KEY`，避免外键元数据和运行时约束引用不存在的列。
- 新增删除外键本地列的失败先行回归，验证 DDL 失败、外键元数据保留以及普通列删除和 CHECK 约束协调路径不回归。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation543/cluster-report.json` 于 `2026-09-06T11:46:16.4711146Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation543/release-candidate.json` 于 `2026-09-06T12:00:13.0473094Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 544

- 继续收敛 P1-SQL-004：`ALTER TABLE ... DROP COLUMN` 现在会扫描同库其他表的外键引用；父表列仍被外键引用时 DDL 失败，必须先解除外键，避免子表 `ref_columns` 悬空。
- 新增“删除被引用父列”的失败先行回归，验证错误返回和子表外键元数据保留；本表外键列、CHECK 协调、普通列重写以及 Connector/J 既有回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation544/cluster-report.json` 于 `2026-09-06T12:04:18.9301169Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation544/release-candidate.json` 于 `2026-09-06T12:18:17.8164838Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 545

- 继续收敛 P1-SQL-004：`DROP TABLE` 现在拒绝删除仍被同库其他表外键引用的父表，并保留父表及子表外键元数据；必须先解除引用外键。
- 新增被外键引用父表的失败先行回归，覆盖错误返回和父表 `.frm` 保留；既有外键运行时校验、级联动作以及 `ALTER TABLE` 外键列/父列保护回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation545/cluster-report.json` 于 `2026-09-06T12:22:16.9129258Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation545/release-candidate.json` 于 `2026-09-06T12:36:20.1416772Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 546

- 继续收敛 P1-SQL-004：会话级 `SET FOREIGN_KEY_CHECKS=0/1` 现在真正控制存储集成 INSERT 的外键校验；关闭时允许导入暂时不满足父键的子行，恢复为 1 后重新拒绝非法子行，默认值仍为开启。
- 新增会话变量关闭/恢复外键检查的失败先行回归；外键运行时校验、DROP TABLE 依赖保护、ALTER 外键列/父列保护和级联回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation546/cluster-report.json` 于 `2026-09-06T12:41:31.2554311Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation546/release-candidate.json` 于 `2026-09-06T12:55:23.4837891Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 547

- 继续收敛 P1-SQL-003/P1-SQL-004：`ALTER TABLE ... DROP INDEX` 现在检查外键本地列的左前缀覆盖；删除唯一可用的外键辅助索引会失败，存在其他覆盖索引时允许删除，避免留下不可执行的外键定义。
- 新增删除外键必需索引的失败先行回归，验证错误返回和索引元数据保留；外键校验开关、DROP TABLE 依赖保护、父列/本地列保护及普通索引 DDL 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation547/cluster-report.json` 于 `2026-09-06T12:59:14.6110786Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation547/release-candidate.json` 于 `2026-09-06T13:13:31.8523433Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 548

- 继续收敛 P1-SQL-003/P1-SQL-004：`ALTER TABLE ... DROP INDEX` 现在也检查父表唯一/普通索引是否仍被同库外键的 `ref_columns` 使用；删除唯一可用的被引用索引会失败，另有覆盖索引时允许删除。
- 新增删除被引用父索引的失败先行回归，验证错误返回和索引元数据保留；子表本地外键索引保护、外键校验开关、DROP TABLE/列依赖保护及普通索引 DDL 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation548/cluster-report.json` 于 `2026-09-06T13:16:51.0226565Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation548/release-candidate.json` 于 `2026-09-06T13:30:52.8439529Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 549

- 继续收敛 P1-SQL-004：存储集成 UPDATE 现在在事务写入前复用外键引用校验，非法子表外键值会失败并回滚；`FOREIGN_KEY_CHECKS=0` 时仍按会话开关跳过该校验，恢复为 1 后重新生效。
- 新增非法子外键 UPDATE 的失败先行回归，验证原值保留；INSERT、级联动作、外键依赖 DDL 和索引保护回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation549/cluster-report.json` 于 `2026-09-06T13:34:05.9257190Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation549/release-candidate.json` 于 `2026-09-06T13:48:04.0803029Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 550

- 继续收敛 P1-SQL-004：`DROP TABLE` 的外键依赖保护现在尊重会话 `FOREIGN_KEY_CHECKS`；默认/开启时仍拒绝删除被引用父表，关闭时允许删除父表，匹配 MySQL 导入/清理场景。
- 新增 `FOREIGN_KEY_CHECKS=0` 删除被引用父表的失败先行回归，并保持默认保护、INSERT/UPDATE 外键校验、索引和列依赖保护回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation550/cluster-report.json` 于 `2026-09-06T13:51:30.4742977Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation550/release-candidate.json` 于 `2026-09-06T14:05:30.4065099Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 551

- 继续收敛 P1-SQL-004：新增 `check_constraint_checks` 会话开关，存储集成 INSERT/UPDATE 会按会话值启用或跳过已定义 CHECK 约束校验，默认仍启用；同时注册 SET/系统变量识别和默认变量定义。
- 新增 CHECK 校验开关失败先行回归；修正直接构造 DML 执行器时的默认值兼容，确保未显式设置会话开关的既有单元测试仍按默认启用 CHECK 约束执行。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation551/cluster-report.json` 于 `2026-09-06T14:12:13.4209576Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation551/release-candidate.json` 于 `2026-09-06T14:26:17.3060783Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 552

- 继续收敛 P1-SQL-004：`ALTER TABLE` 的外键依赖保护现在尊重会话 `FOREIGN_KEY_CHECKS`；关闭时允许删除被引用父索引/父列，开启或默认状态仍拒绝破坏外键依赖，并保持不能删除主键及不能把表删成无列等独立约束。
- 新增父索引和父列在 `FOREIGN_KEY_CHECKS=0` 下可删除的失败先行回归，同时保留默认外键保护回归；会话状态从两条 ALTER 入口及复合 ALTER 路径贯通到兼容性元数据应用层。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation552/cluster-report.json` 于 `2026-09-06T14:31:47.877894Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation552/release-candidate.json` 于 `2026-09-06T14:46:17.151051Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 553

- 继续收敛 P1-SQL-004：`UNIQUE_CHECKS` 现在真正贯穿存储集成 INSERT/UPDATE；关闭时跳过重复键发现和唯一约束校验，允许批量导入阶段出现重复唯一值，恢复为 1 后重新执行唯一性保护。
- 新增 INSERT 与 UPDATE 的 `UNIQUE_CHECKS=0/1` 失败先行回归；主键、非唯一字段、数据完整性校验及 `REPLACE`/`ON DUPLICATE KEY` 既有路径保持不变。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation553/cluster-report.json` 于 `2026-09-06T14:51:09.035126Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation553/release-candidate.json` 于 `2026-09-06T15:05:08.6332581Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 554

- 继续收敛 P1-SQL-003/P1-SQL-004：`ALTER TABLE ... DROP PRIMARY KEY` 现在检查同库外键对父表 `PRIMARY` 索引的引用；默认/开启 `FOREIGN_KEY_CHECKS` 时拒绝删除被引用主键，关闭时允许按 MySQL 导入/清理场景执行。
- 新增默认拒绝与 `FOREIGN_KEY_CHECKS=0` 放行的失败先行回归；普通父索引、父列、父表删除保护及唯一性开关回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation554/cluster-report.json` 于 `2026-09-06T15:08:54.1412158Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation554/release-candidate.json` 于 `2026-09-06T15:22:58.5400892Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 555

- 继续收敛 P1-SQL-004：`ALTER TABLE ... ADD FOREIGN KEY` 现在尊重会话 `FOREIGN_KEY_CHECKS`；关闭时允许把已有历史孤儿数据纳入外键元数据，开启或默认状态仍扫描已有行并拒绝不满足引用的 ALTER。
- 新增 `FOREIGN_KEY_CHECKS=0` 添加外键的失败先行回归，并保持默认历史数据校验、运行时 INSERT/UPDATE 校验和外键依赖 DDL 回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation555/cluster-report.json` 于 `2026-09-06T15:26:13.4608706Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation555/release-candidate.json` 于 `2026-09-06T15:40:11.6527872Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 556

- 继续收敛 P1-SQL-007：`CREATE TABLE ... AS SELECT` 现在识别并执行 MySQL 常见的非递归 CTE 来源；CTE 会先复用现有重写/派生表执行路径，再按查询结果生成目标表并物化数据，避免 `CREATE TABLE ... AS WITH ...` 被普通 DDL 解析器忽略。
- CTAS 入口同时覆盖现有递归 CTE 兼容执行路径；新增非递归和递归 CTE-CTAS 失败先行回归，并保持多 CTE 链、CTE 外层过滤和既有 CTAS 回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation556/cluster-report.json` 于 `2026-09-06T15:52:13.7745067Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation556/release-candidate.json` 于 `2026-09-06T16:06:26.0346439Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 557

- 继续收敛 P1-SQL-004：外键元数据现在保留可选引用库名，跨库 `CREATE TABLE` 与 `ALTER TABLE ... ADD FOREIGN KEY` 可以校验父表，并在 INSERT、父键 UPDATE/DELETE 的 CASCADE 路径中访问正确的库；默认未带库名的外键仍按子表所在库解析。
- 跨库外键依赖保护同步覆盖父表 DROP、父索引/父列 DDL 以及被引用列改名，避免跨库约束在元数据变更后悬空；新增跨库创建、ALTER 添加、非法写入、更新级联和删除级联失败先行回归。
- 同时新增多级非递归 CTE-CTAS 和临时表 CTE-CTAS 回归；第 556 轮的 CTAS CTE 入口在多级/临时表形态下保持可用。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation557/cluster-report.json` 于 `2026-09-06T16:16:30.4472405Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation557/release-candidate.json` 于 `2026-09-06T16:30:33.3649468Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 558

- 继续收敛 P1-SQL-004：跨库外键的 `KEY_COLUMN_USAGE.REFERENCED_TABLE_SCHEMA` 与 `REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_SCHEMA` 现在返回真实引用库名，不再把父表库错误显示为子表库；旧格式未保存库名的同库外键继续回退到子表库。
- 新增跨库信息架构元数据回归，并保持跨库外键写入、级联、ALTER 添加以及父表/索引/列依赖保护回归通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation558/cluster-report.json` 于 `2026-09-06T16:33:43.8653750Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation558/release-candidate.json` 于 `2026-09-06T16:47:48.7028183Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 559

- 继续收敛 Connector/J/协议游标边界：prepared statement 的 server-side cursor 未取尽时再次 `COM_STMT_EXECUTE` 不再覆盖旧游标，而是返回 MySQL `ER_EXEC_STMT_WITH_OPEN_CURSOR (1420)`；`COM_STMT_RESET` 仍可清理游标后重新执行。
- legacy `MySQLProtocolHandler` 的 `COM_STMT_EXECUTE`/`COM_STMT_FETCH` 游标路径现在发送 cursor 元数据、binary row，并按批次设置 `SERVER_STATUS_CURSOR_EXISTS` 或 `SERVER_STATUS_LAST_ROW_SENT`；与 decoupled handler 的协议行为对齐。
- 新增打开游标重复设置的失败先行回归；PreparedStatement cursor lifecycle、全量 Go 回归、集群 smoke 与 Connector/J 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation559/cluster-report.json` 于 `2026-09-06T16:56:12.7086438Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation559/release-candidate.json` 于 `2026-09-06T17:10:36.6282756Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 560

- 继续收敛 P4 函数矩阵：新增 `INET6_ATON`/`INET6_NTOA`，支持 IPv4 四字节与 IPv6 十六字节二进制转换；新增 `IS_IPV4_COMPAT` 与 `IS_IPV4_MAPPED` 判断，并同步接入普通 evaluator 与 compiled-expression whitelist。
- 新增 IPv4/IPv6 转换、IPv4-compatible/mapped 失败先行回归；既有摘要、UUID、IPv4 网络函数以及 compiled expression 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation560/cluster-report.json` 于 `2026-09-06T17:15:27.1265030Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation560/release-candidate.json` 于 `2026-09-06T17:29:28.9270743Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 561

- 继续收敛 P1-SQL-001：派生表外层的相关 `EXISTS/NOT EXISTS` 现在支持常见 `ORDER BY ... LIMIT`、`LIMIT ... OFFSET` 与逗号形式分页；先完成逐外层行相关绑定、过滤和稳定排序，再按 offset/count 截断，避免把 LIMIT 误解析为排序表达式。
- 新增派生相关 EXISTS 的 LIMIT 与 OFFSET 失败先行回归；相关 scalar/derived/outer-source 既有回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation561/cluster-report.json` 于 `2026-09-06T17:33:30.7970211Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation561/release-candidate.json` 于 `2026-09-06T17:47:29.7513174Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 562

- 继续收敛 P4 函数矩阵：新增 `IS_UUID()`，按 MySQL UUID 文本格式返回 1/0，并同步接入 compiled-expression whitelist；已有 `UUID_TO_BIN/BIN_TO_UUID`、IPv4/IPv6、摘要函数保持通过。
- 新增合法/非法 UUID 失败先行回归；本轮同时包含派生相关 EXISTS 的分页实现。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation562/cluster-report.json` 于 `2026-09-06T17:50:29.1554925Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation562/release-candidate.json` 于 `2026-09-06T18:04:27.2433169Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 563

- 继续收敛 P1-IDX-001/P1-EXE-003：复合索引合并的范围分支在增强索引读路径上现在直接解码持久化索引键，并按列类型执行数值或文本范围残余判断，再决定是否回表；旧版只暴露索引值的门面继续保留安全回退，避免改变既有存储兼容性。
- 新增数值范围不能按字符串顺序误判（例如 `10 > 2`）和文本范围边界回归；复合二级索引的 AND/OR 范围查询保持结果与访问路径正确。
- 继续收敛 P1-EXE-002：无分区、无排序、无窗口帧的 `ROW_NUMBER()` 现在直接通过子算子的 `NextBatch` 流式生成结果，不再一次性物化全部输入；需要排序、分区、前后行或帧语义的窗口函数继续使用原有物化路径。
- 新增流式窗口批处理回归，并修复“子批次不足请求大小但尚未返回 EOF”边界；窗口/CTE/批处理全量 engine 回归保持通过。
- 本轮 `go test ./server/innodb/engine -count=1` 与 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation563/cluster-report.json` 于 `2026-09-06T18:14:49.1856157Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation563/release-candidate.json` 于 `2026-09-06T18:28:56.8693805Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 564

- 继续收敛 P1-IDX-001/P1-EXE-003：复合二级索引“等值前缀 + 尾列范围”在增强索引读路径上现在使用索引键中的尾列值做类型感知残余过滤，数值比较不再受持久化文本编码的字典序影响；不提供键记录的旧索引门面仍回退到原有安全路径。
- 复合前缀范围、复合 OR/AND 范围、数值 `10 > 2` 和文本边界回归保持通过；该改动只减少不匹配行的聚簇回表，不改变最终 WHERE 语义。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation564/cluster-report.json` 于 `2026-09-06T18:32:36.9619968Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation564/release-candidate.json` 于 `2026-09-06T18:46:44.2955474Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 565

- 继续收敛 P4 函数矩阵：新增 MySQL `BIT_COUNT()`，按 64 位补码语义统计整数中 1 的数量，覆盖正数、负数和 NULL 行为，并同步接入 compiled-expression whitelist。
- 新增普通 evaluator 与 compiled evaluator 回归；本轮 `go test ./... -count=1 -timeout 5m` 全量通过，集群报告 `reports/compatibility/p1-cluster-current-continuation565/cluster-report.json` 于 `2026-09-06T18:49:43.8590786Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation565/release-candidate.json` 于 `2026-09-06T19:03:57.2535406Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 566

- 继续收敛 P1-EXE-002：无分区、无排序、无窗口帧的 `RANK()` 与 `DENSE_RANK()` 现在像安全的常量窗口一样通过子算子的 `NextBatch` 流式输出，每行返回 1，不再把整个输入物化；带分区、排序或帧的排名窗口继续保留原有物化语义。
- 新增失败先行回归，验证两类排名窗口使用批量路径、不会退回子算子的逐行 `Next()`；同时保留 `ROW_NUMBER()`、分布/百分位/NTH_VALUE 等窗口回归。
- 增加相关标量投影与外层多表 JOIN 的兼容性回归，确认外层 JOIN 来源不会在相关值绑定时丢失；P1-SQL-001 更复杂的相关外层 AST 仍未宣称完成。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 包完整通过；集群报告 `reports/compatibility/p1-cluster-current-continuation566/cluster-report.json` 于 `2026-09-06T19:10:59.0437501Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation566/release-candidate.json` 于 `2026-09-06T19:25:03.9202135Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 567

- 继续收敛 P1-SEC-001 的 feature-specific privilege enforcement：dispatcher 现在把 `RENAME USER` 路由到 `CREATE USER` 权限，不再把账号重命名误判为普通 `SELECT`；执行层已有的多目标预校验和账号绑定清理保持不变。
- `SET PASSWORD` 现在区分管理员修改指定其他账号与当前账号自助修改：指定其他账号需要 `CREATE USER`，无 `FOR`、显式当前账号或 `CURRENT_USER()` 不额外要求全局权限；新增正向/失败先行路由回归。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过；集群报告 `reports/compatibility/p1-cluster-current-continuation567/cluster-report.json` 于 `2026-09-06T19:30:11.1839137Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation567/release-candidate.json` 于 `2026-09-06T19:44:14.9921607Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 568

- 继续收敛 P1-SQL-001/P1-SQL-002：非递归 CTE 的 UNION 定义现在加入外层聚合、`HAVING COUNT(*)`、排序和 LIMIT 的真实回归；同时修正行级表达式求值对 `COUNT(*)` parser `StarExpr` 的处理，避免常见聚合过滤路径把 `*` 误判为不支持的函数参数。
- 继续收敛 P1-SEC-001：`CREATE INDEX`/`DROP INDEX` 现在按 MySQL 的 `INDEX` 权限路由；`CREATE OR REPLACE VIEW` 同时要求 `CREATE VIEW` 与 `DROP`，并继续检查定义源表的 `SELECT`，避免对象替换退化为普通创建。
- 本轮失败先行路由/SQL 回归转绿；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 75.433s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation568/cluster-report.json` 于 `2026-09-06T19:51:31.4655672Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation568/release-candidate.json` 于 `2026-09-06T20:05:30.6440668Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 569

- 继续收敛 P1-SQL-001：相关标量子查询现在支持“不出现在 SELECT 投影中、直接用于外层 `ORDER BY`”的常见形态；执行器按每个外层行计算隐藏排序列，支持多排序项和 LIMIT/OFFSET，排序完成后剔除辅助列，避免原路径静默忽略相关标量排序。
- 同步收紧 P1-SEC-001：`CREATE OR REPLACE VIEW` 的 `DROP` 检查落到具体视图对象，定义源表仍独立检查 `SELECT`；`CREATE INDEX`/`DROP INDEX` 的 `INDEX` 权限与前轮路由保持通过。
- 相关标量/CTE/集合运算专项回归与 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 74.298s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation569/cluster-report.json` 于 `2026-09-06T20:10:38.8793073Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation569/release-candidate.json` 于 `2026-09-06T20:24:48.4259238Z` 为 `GO`；所有 release checks `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 570

- 继续收敛 P1-SEC-001：统一 `CREATE OR REPLACE VIEW` 权限路由，即使视图定义为 `AS SELECT 1`、没有源表，也会把 `CREATE VIEW` 与目标视图对象 `DROP` 分开检查；带源表的定义继续追加各源表 `SELECT`，避免空表依赖导致权限范围错误。
- 继续补强 P1-SQL-001 回归：相关标量直接用于外层 `ORDER BY` 的多排序项（COUNT/MAX）和 LIMIT 已验证，隐藏排序列不会出现在最终结果或结果列元数据中。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 74.126s；集群报告 `reports/compatibility/p1-cluster-current-continuation570/cluster-report.json` 于 `2026-09-06T20:29:02.9846933Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation570/release-candidate.json` 于 `2026-09-06T20:43:07.8306654Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 571

- 继续收敛 P1-SEC-001 的 feature-specific privilege enforcement：`DROP VIEW` 现在把 `DROP` 权限检查落到具体视图对象，而不是退化为无表名的全局/库级检查；支持 `IF EXISTS`、多视图和带库名对象，多个目标逐个检查。
- 新增 `DROP VIEW` 单对象和多对象失败先行回归；dispatcher 全包与全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 74.677s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation571/cluster-report.json` 于 `2026-09-06T20:49:22.7812772Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation571/release-candidate.json` 于 `2026-09-06T21:03:28.8656889Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 572

- 继续收敛 P1-SEC-001：`ALTER VIEW` 现在按 MySQL 8.4 规则检查数据库级 `CREATE VIEW`、目标视图对象 `DROP` 以及定义源表的 `SELECT`；无源表定义也保持目标对象级检查，不再误用普通 `ALTER` 权限。
- 新增带源表与无源表的 `ALTER VIEW` 权限路由回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 76.671s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation572/cluster-report.json` 于 `2026-09-06T21:06:02.3870175Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation572/release-candidate.json` 于 `2026-09-06T21:20:09.173932Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 573

- 继续收敛 P1-SEC-001：包含外键的 `CREATE TABLE` 与 `ALTER TABLE ... ADD FOREIGN KEY` 现在分别检查目标表的 `CREATE`/`ALTER`，并对每个 `REFERENCES` 父表检查 `REFERENCES` 权限；支持带列括号、带库名以及 `IF NOT EXISTS` 的目标表名解析。
- 新增 CREATE/ALTER 外键权限路由失败先行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 75.117s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation573/cluster-report.json` 于 `2026-09-06T21:23:04.3803576Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation573/release-candidate.json` 于 `2026-09-06T21:37:13.9058015Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 574

- 继续收敛 P1-SEC-001 的对象级权限路由：多目标 `DROP TABLE` 现在逐表检查 `DROP`，正确处理 `IF EXISTS`、`TEMPORARY`、逗号分隔目标、反引号和带库名对象；不再把 `a,b` 或 `IF` 当成单一表名。
- 新增多目标 DROP TABLE 失败先行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 75.319s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation574/cluster-report.json` 于 `2026-09-06T21:39:31.7217463Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation574/release-candidate.json` 于 `2026-09-06T21:53:31.7180607Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 575

- 继续收敛 P1-SEC-001：新增 `RENAME TABLE` 权限路由，按 MySQL 8.4 规则对每个旧对象检查 `ALTER` 与 `DROP`，对每个新对象检查 `CREATE` 与 `INSERT`；支持多组重命名、带库名和反引号对象。
- 新增多目标 RENAME TABLE 失败先行回归，并修正 SQL 类型识别不再把 RENAME 退化为默认查询权限；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 75.053s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation575/cluster-report.json` 于 `2026-09-06T21:55:58.5467676Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation575/release-candidate.json` 于 `2026-09-06T22:10:08.4244069Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 576

- 继续收敛 P1-SEC-001：普通 `ALTER TABLE` 及带外键的 `ALTER TABLE ... ADD FOREIGN KEY` 现在对目标表按 MySQL 规则检查 `ALTER`、`CREATE`、`INSERT`，外键父表额外检查 `REFERENCES`；避免只校验 `ALTER` 导致权限不足时错误放行。
- 新增普通 ALTER 与 ALTER 外键权限路由回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 75.390s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation576/cluster-report.json` 于 `2026-09-06T22:12:29.4713297Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation576/release-candidate.json` 于 `2026-09-06T22:26:32.7756483Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 577

- 继续收敛 P1-SQL-001：相关标量子查询现在支持按相关标量投影别名进行 `GROUP BY`，先逐外层行物化标量，再执行分组和常见 `COUNT/SUM/AVG/MIN/MAX` 聚合；相关标量别名的 `HAVING`、排序和分页继续在分组结果上执行。
- 新增相关标量 `GROUP BY` 失败先行回归，并验证既有相关标量、派生表和集合表达式回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 76.351s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation577/cluster-report.json` 于 `2026-09-06T22:35:24.1901820Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation577/release-candidate.json` 于 `2026-09-06T22:49:26.5082392Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 578

- 继续收敛 P1-SQL-001：`GROUP BY` 现在支持直接重复相关标量表达式（例如 `GROUP BY COALESCE((SELECT ...), 0)`），按已物化的相关标量投影值分组；同时覆盖空集合 `COALESCE` 以及 `SUM/AVG` 聚合。
- 新增直接相关标量表达式分组失败先行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 75.610s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation578/cluster-report.json` 于 `2026-09-06T22:53:09.1254565Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation578/release-candidate.json` 于 `2026-09-06T23:07:23.1981239Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 579

- 继续收敛 P1-SQL-001：相关标量表达式分组路径现在也支持直接重复该表达式的 `HAVING` 条件；执行时将相关子查询绑定为当前分组行的已物化值，再按 MySQL 三值谓词语义过滤。
- 新增直接相关标量 `HAVING` 失败先行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 76.924s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation579/cluster-report.json` 于 `2026-09-06T23:10:44.0234359Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation579/release-candidate.json` 于 `2026-09-06T23:24:50.1841230Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 580

- 继续收敛 P1-SQL-002：非递归 CTE 定义现在支持已识别的混合集合表达式（例如 `UNION ALL ... INTERSECT ...`）作为物化派生源，并保留普通 CTE 的结构化语法校验；外层聚合、排序和 LIMIT 继续复用既有集合/派生表执行路径。
- 新增 CTE 混合集合表达式失败先行回归；全仓 `go test ./... -count=1 -timeout 5m` 通过，其中 engine 75.049s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation580/cluster-report.json` 于 `2026-09-06T23:28:44.6588476Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation580/release-candidate.json` 于 `2026-09-06T23:42:48.6562729Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 581

- 继续收敛 P1-SQL-003：`RENAME TABLE source_db.table TO target_db.table` 现在支持跨库移动；持久化 `.frm` 元数据会原子写入目标库并更新 schema/table 标识，原表文件被移除，现有表空间映射同步迁移到目标逻辑名。
- 跨库改名会清理源库表管理器元数据缓存并刷新源/目标信息架构；源库查询不再因残留缓存或旧的示例数据回退而“复活”，目标库可继续读取原有数据；目标已存在、源文件缺失和存储映射迁移失败仍保留失败回滚路径。
- 新增 `TestRenameTableMovesTableAcrossSchemas` 失败先行回归；同库 RENAME、ALTER/DROP/TRUNCATE 相关回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 75.442s；集群报告 `reports/compatibility/p1-cluster-current-continuation581/cluster-report.json` 于 `2026-09-06T23:53:27.1520334Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation581/release-candidate.json` 于 `2026-09-07T00:07:46.9341900Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 582

- 继续补齐 P1-SQL-003 的跨库 RENAME 物理生命周期：跨库 `RENAME TABLE` 不再只移动 `.frm`，而是通过 `StorageManager/SpaceManager` 在关闭旧文件句柄后重命名真实 `.ibd`，按原 Space ID 重新打开并恢复页分配，再同步逻辑表存储映射。
- 为 Windows 文件锁增加失败先行回归；跨库改名现在同时保证目标 `.frm/.ibd` 存在、源 `.frm/.ibd` 不残留，数据可继续读取，tablespace rename 失败时恢复旧元数据和存储映射。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.576s、manager 11.968s；集群报告 `reports/compatibility/p1-cluster-current-continuation582/cluster-report.json` 于 `2026-09-07T00:13:12.1540834Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation582/release-candidate.json` 于 `2026-09-07T00:27:31Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 583

- 继续收敛 P1-SQL-003：`RENAME TABLE` 现在支持多个逗号分隔的重命名对，并在执行前统一校验源表、目标表、重复源/目标和数据库存在性；任一目标冲突时整句保持原状态，不再出现前几个表已改名、后续表失败的半成功结果。
- 合法的多目标改名按顺序迁移 `.frm`、`.ibd`、tablespace handle、表存储映射和缓存；执行中途失败会按逆序尝试恢复已经完成的改名。新增多目标失败先行回归，同时覆盖冲突原子性和合法两目标改名后的数据读取。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.225s、manager 12.219s；集群报告 `reports/compatibility/p1-cluster-current-continuation583/cluster-report.json` 于 `2026-09-07T00:31:59.4690646Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation583/release-candidate.json` 于 `2026-09-07T00:46:16.3893567Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 584

- 继续收敛 P1-SQL-004：跨库 `RENAME TABLE` 父表现在会扫描并更新子表外键的 `ref_schema`、`ref_table` 及 `raw_definition`；子表运行时引用校验和 `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` 均跟随新库/新表名，改名后的合法子表写入不再指向已移动的旧对象。
- 新增跨库父表改名失败先行回归，覆盖外键运行时写入和引用元数据；多目标 RENAME 的逆序回滚也会复用该引用重写路径。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 75.533s、manager 11.468s；集群报告 `reports/compatibility/p1-cluster-current-continuation584/cluster-report.json` 于 `2026-09-07T00:50:13.0152105Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation584/release-candidate.json` 于 `2026-09-07T01:04:27.4977991Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 585

- 继续收敛 P1-SQL-003：`ALTER TABLE source_db.old_name RENAME TO target_db.new_name` 现在支持带库名的源表和目标表，复用跨库 RENAME 的 `.frm/.ibd` 迁移、tablespace 重开、存储映射刷新和外键引用更新路径。
- 新增带库名 `ALTER TABLE ... RENAME TO` 失败先行回归；现有同库 ALTER RENAME、跨库 RENAME、多目标原子性和外键依赖回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 83.882s、manager 20.048s；集群报告 `reports/compatibility/p1-cluster-current-continuation585/cluster-report.json` 于 `2026-09-07T01:08:00.7337562Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation585/release-candidate.json` 于 `2026-09-07T01:22:07.3667007Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 586

- 继续补强 P1-SQL-003/P1-SQL-004 的跨库改名持久化：新增重启回归，验证跨库 RENAME 后 `.frm` 中的 schema/table 与 storage identity、真实 `.ibd` 文件名、Space ID 和表存储映射在新引擎实例中仍能恢复。
- 重启后目标表数据可读，源表不会被旧 `.ibd` 或旧映射重新发现；该回归同时覆盖此前 `ALTER TABLE ... RENAME TO` 和 `RENAME TABLE` 共用的 tablespace 生命周期路径。
- 外键缓存边界回归还验证了改名前已访问/写入的子表，在父表跨库改名后仍能按新父表进行合法写入，并返回更新后的引用元数据。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.681s、manager 10.887s；集群报告 `reports/compatibility/p1-cluster-current-continuation586/cluster-report.json` 于 `2026-09-07T01:24:54.7432317Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation586/release-candidate.json` 于 `2026-09-07T01:39:14.9364359Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 587

- 继续收敛 P1-SQL-003：补齐普通表多目标 `DROP TABLE` 的原始 SQL 兼容路径，支持逗号分隔目标、`IF EXISTS`、带库名目标，并在实际 `XMySQLEngine.ExecuteQuery` 入口统一接入。
- 多目标 DROP 会在删除前统一校验所有数据库、表存在性、外键依赖和存储对象依赖；缺失目标或后续依赖错误不会先删除前面的表。新增失败先行回归覆盖前置目标保留、`IF EXISTS` 跳过缺失目标及限定库名目标删除；临时表多目标 DROP 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.738s、manager 12.466s；集群报告 `reports/compatibility/p1-cluster-current-continuation587/cluster-report.json` 于 `2026-09-07T01:48:39.5016968Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation587/release-candidate.json` 于 `2026-09-07T02:02:49.0685967Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 588

- 继续收敛 P1-SQL-003：补齐基础 `ALTER VIEW view_name AS SELECT ...` 执行语义；现在会校验目标视图已存在，原地替换持久化 `.view.json` 定义，并复用既有视图重写路径让后续查询立即使用新定义。
- 新增失败先行回归，验证 ALTER VIEW 后查询结果和持久化定义同步变化，原有 CREATE/DROP VIEW 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.413s、manager 10.730s；集群报告 `reports/compatibility/p1-cluster-current-continuation588/cluster-report.json` 于 `2026-09-07T02:06:47.6158930Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation588/release-candidate.json` 于 `2026-09-07T02:21:00.2098653Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 589

- 继续收敛 P1-SQL-003：补齐多目标 `DROP VIEW` 执行语义，支持逗号分隔目标、`IF EXISTS`、反引号和带库名对象；执行前统一确认所有非忽略目标存在，避免缺失目标导致前面的视图已被删除。
- 同时修正单目标 `DROP VIEW IF EXISTS` 的目标解析，新增多视图失败先行与成功删除回归；基础 CREATE/DROP/ALTER VIEW 路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.713s、manager 10.744s；集群报告 `reports/compatibility/p1-cluster-current-continuation589/cluster-report.json` 于 `2026-09-07T02:25:37.9446708Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation589/release-candidate.json` 于 `2026-09-07T02:39:59.3728318Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 590

- 继续收敛 P1-SQL-003：视图定义现在识别 `WITH LOCAL CHECK OPTION`/`WITH CASCADED CHECK OPTION`，将其从可执行 SELECT 中分离并持久化为视图元数据，避免后续查询把检查选项误交给 SELECT 解析器。
- 多目标 DROP VIEW 回归同步覆盖普通删除、缺失目标前置原子性、`IF EXISTS`、反引号和带库名目标；CREATE/DROP/ALTER VIEW 基础路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.613s、manager 14.325s；集群报告 `reports/compatibility/p1-cluster-current-continuation590/cluster-report.json` 于 `2026-09-07T02:43:52.3756592Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation590/release-candidate.json` 于 `2026-09-07T02:58:08.3618462Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 591

- 继续收敛 P1-SQL-003：`ALTER VIEW` 现在接受常用的 `ALGORITHM=UNDEFINED|MERGE|TEMPTABLE`、`DEFINER`、`SQL SECURITY DEFINER|INVOKER` 前置选项，并将已识别的视图选项持久化；基础定义替换仍校验目标视图存在。
- 新增带 `ALGORITHM`、`SQL SECURITY` 和 `WITH CHECK OPTION` 的真实查询/元数据回归；多目标 DROP VIEW、CREATE VIEW 和普通 ALTER VIEW 路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.222s、manager 10.733s；集群报告 `reports/compatibility/p1-cluster-current-continuation591/cluster-report.json` 于 `2026-09-07T03:02:25.5473011Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation591/release-candidate.json` 于 `2026-09-07T03:16:44.7575453Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 592

- 继续收敛 P1-SQL-003/P1-SQL-008 的视图元数据契约：`SHOW CREATE VIEW` 现在读取并输出持久化的 `ALGORITHM`、`DEFINER`、`SQL SECURITY` 与 `WITH CHECK OPTION`，不再固定伪造 `ALGORITHM=UNDEFINED` 或丢失视图选项。
- 新增 ALTER VIEW 后 SHOW CREATE VIEW 的真实元数据回归；视图查询、创建、删除和多目标 DROP 路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.299s、manager 10.634s；集群报告 `reports/compatibility/p1-cluster-current-continuation592/cluster-report.json` 于 `2026-09-07T03:21:01.2169700Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation592/release-candidate.json` 于 `2026-09-07T03:35:15.8968995Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 593

- 继续收敛 P1-SQL-003/P1-SQL-008 的视图 DDL 语法：`CREATE VIEW` 现在与 `ALTER VIEW` 共用常见 `ALGORITHM=UNDEFINED|MERGE|TEMPTABLE`、`DEFINER`、`SQL SECURITY DEFINER|INVOKER` 前置选项识别，并将 `WITH LOCAL/CASCADED CHECK OPTION` 与定义一起持久化；创建后的视图可立即查询，后续 `SHOW CREATE VIEW` 返回实际选项元数据。
- 新增 `TestCreateViewOptionsPersistAndExecute` 失败先行回归；既有视图 CREATE/ALTER/DROP、多目标删除和 SHOW CREATE 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.524s、manager 11.341s；集群报告 `reports/compatibility/p1-cluster-current-continuation593/cluster-report.json` 于 `2026-09-07T03:41:55.5007427Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation593/release-candidate.json` 于 `2026-09-07T03:56:03.0122553Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 594

- 继续收敛 P1-SQL-003：`RENAME TABLE` 现在可在同一数据库内重命名视图，校验逻辑统一识别表/视图对象，迁移并更新 `.view.json` 的 `view_name/schema` 元数据；视图跨库移动暂不放行，避免未限定源表名在新库中改变解析语义。
- 继续收敛 P1-SQL-003/P1-SQL-008：普通重复 `CREATE VIEW` 现在返回对象已存在错误并保留原定义，`CREATE OR REPLACE VIEW` 才执行原地替换；视图名与真实表名冲突时也返回明确错误。
- 新增视图重命名及重复创建/替换失败先行回归；视图选项、SHOW CREATE、ALTER、DROP 和多目标 DROP 路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.827s、manager 11.830s；集群报告 `reports/compatibility/p1-cluster-current-continuation594/cluster-report.json` 于 `2026-09-07T04:00:13.8551426Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation594/release-candidate.json` 于 `2026-09-07T04:14:31.4436168Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 595

- 继续收敛 P1-SQL-003/P1-SQL-008 的视图定义语法：`CREATE/ALTER VIEW view_name (column_list) AS SELECT ...` 现在校验显式列名与 SELECT 投影列数，并将列名转换为投影别名持久化；后续查询和 `SHOW CREATE VIEW` 返回声明的视图列名。
- 新增 `TestCreateViewExplicitColumnListRenamesProjection` 失败先行回归；重复 CREATE/OR REPLACE、视图选项、同库视图重命名、SHOW CREATE、DROP 和多目标 DROP 路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.664s、manager 10.613s；集群报告 `reports/compatibility/p1-cluster-current-continuation595/cluster-report.json` 于 `2026-09-07T04:19:54.6441115Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation595/release-candidate.json` 于 `2026-09-07T04:34:06.9726495Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 596

- 继续收敛 P1-SQL-003 的元数据契约：`INFORMATION_SCHEMA.TABLES` 现在枚举持久化 `.view.json`，视图返回 `TABLE_TYPE=VIEW`、空引擎/物理统计和正确的库表过滤；临时表同名遮蔽规则保持生效。
- 同时修正请求列裁剪后的排序逻辑：只请求 `TABLE_NAME/TABLE_TYPE` 等子集时不再按固定列下标访问或跳过排序；新增视图发现回归，JDBC 表元数据回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.774s、manager 11.402s；集群报告 `reports/compatibility/p1-cluster-current-continuation596/cluster-report.json` 于 `2026-09-07T04:39:15.7416697Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation596/release-candidate.json` 于 `2026-09-07T04:53:24.2651086Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 597

- 继续收敛 P1-SQL-003 的视图元数据契约：`INFORMATION_SCHEMA.VIEWS` 不再固定返回 `CHECK_OPTION=NONE`、`DEFINER=root@localhost`、`SECURITY_TYPE=DEFINER`，而是读取 `.view.json` 中持久化的 `WITH LOCAL/CASCADED CHECK OPTION`、定义者和 `SQL SECURITY`；定义者格式按信息架构表形状规范化。
- 新增 `TestInformationSchemaViewsReflectsPersistedOptions` 失败先行回归；`INFORMATION_SCHEMA.TABLES` 的 `VIEW` 行、SHOW CREATE、视图列名、ALTER/DROP 和 Connector/J 元数据路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.719s、manager 11.120s；集群报告 `reports/compatibility/p1-cluster-current-continuation597/cluster-report.json` 于 `2026-09-07T04:56:50.3638486Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation597/release-candidate.json` 于 `2026-09-07T05:11:09.9828929Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 598

- 继续收敛 P1-SQL-003/P1-SQL-008 的视图元数据契约：`SHOW COLUMNS`、`SHOW FULL COLUMNS` 和 `INFORMATION_SCHEMA.COLUMNS` 现在识别持久化 `.view.json`，通过视图定义展开结果列名/基础列类型；显式视图列名、字符列排序以及临时表遮蔽语义保持生效。
- 修正 `SHOW COLUMNS` 对视图先调用通用 `ShowExecutor` 导致的误报缺表，并新增 `TestViewColumnMetadataIsVisibleToShowAndInformationSchema` 失败先行回归；普通表列元数据路径保持不变。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 79.666s、manager 10.948s；`git diff --check` 仅报告现有工作树的 LF/CRLF 转换提示，无 whitespace error。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation598/cluster-report.json` 于 `2026-09-07T05:25:38.4213954Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation598/release-candidate.json` 于 `2026-09-07T05:39:53.8896539Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 599

- 继续收敛 P1-SQL-003/P1-SQL-008 的视图 DDL 语义：`CREATE VIEW`、`ALTER VIEW` 和 `CREATE OR REPLACE VIEW` 现在在持久化前校验直接表/视图源、派生源和自引用；缺失源或自引用会明确失败，`OR REPLACE` 失败时原视图定义保持不变。
- 新增 `TestViewDDLRejectsMissingSourceAtomically` 失败先行回归，覆盖首次创建不落盘和替换失败不覆盖旧定义；视图列元数据、SHOW/INFORMATION_SCHEMA、CREATE/ALTER/DROP/RENAME 路径保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.405s、manager 10.983s；`git diff --check` 仅报告现有工作树的 LF/CRLF 转换提示，无 whitespace error。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation599/cluster-report.json` 于 `2026-09-07T05:46:35.3865392Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation599/release-candidate.json` 于 `2026-09-07T06:00:40.3256656Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 600

- 继续收敛 P1-SQL-003/P1-SQL-008 的 SHOW 元数据契约：视图手工列元数据路径现在支持 `SHOW COLUMNS/FIELDS ... LIKE` 和 `WHERE` 过滤，复用与普通表一致的 SQL LIKE/条件求值；新增 `TestShowColumnsFiltersViewMetadata` 回归覆盖列名模式和 `Field` 条件。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 80.658s、manager 12.675s；`git diff --check` 仅报告现有工作树的 LF/CRLF 转换提示，无 whitespace error。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation600/cluster-report.json` 于 `2026-09-07T06:05:29.0692632Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation600/release-candidate.json` 于 `2026-09-07T06:19:47.0760742Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 601

- 继续收敛 P1-SQL-003/P1-SQL-008 的 SHOW 元数据契约：`SHOW FULL COLUMNS/FIELDS ... LIKE` 和 `WHERE` 现在同时作用于持久化视图与普通表的完整列元数据结果，避免普通 `SHOW COLUMNS` 与 `SHOW FULL COLUMNS` 行为不一致。
- 新增 `TestShowColumnsFiltersViewMetadata` 对 `SHOW FULL COLUMNS ... LIKE` 的回归覆盖；视图列类型、排序规则、权限和注释列仍保持返回。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.353s、manager 10.906s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation601/cluster-report.json` 于 `2026-09-07T06:23:37.9616276Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation601/release-candidate.json` 于 `2026-09-07T06:37:58.4843873Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 602

- 继续收敛 P1-SQL-001/P1-SQL-003：视图源校验现在支持 `CREATE/ALTER VIEW ... AS WITH ... SELECT`，区分 CTE 可见名与真实表/视图源，并复用既有 CTE 语法校验拒绝空定义、重复/非法引用；视图查询和列元数据路径会将可执行 CTE 定义接入现有重写器。
- 新增 `TestCreateViewWithCTESourceValidatesAndExecutes` 失败先行回归，覆盖 CTE 视图创建、真实数据查询、`SHOW FULL COLUMNS` 的 `INT/VARCHAR` 类型元数据以及源表校验；普通视图缺失源原子性、SHOW 元数据和完整视图 DDL 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.629s、manager 10.769s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation602/cluster-report.json` 于 `2026-09-07T06:42:53.2158757Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation602/release-candidate.json` 于 `2026-09-07T06:57:02.1407216Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 603

- 继续收敛 P1-SQL-001/P1-SQL-003：视图定义现在支持派生表内部的嵌套 `WITH ... SELECT`，在 Vitess SQL AST 解析前将嵌套 CTE 展开到现有派生表执行路径；视图源校验、查询改写和列元数据执行共用该归一化逻辑。
- 新增 `TestCreateViewWithNestedDerivedCTESourceExecutes` 失败先行回归，覆盖 `FROM (WITH ... SELECT ...)` 的视图创建和真实结果查询；顶层 CTE、视图源原子性校验、SHOW FULL COLUMNS 及完整视图 DDL 回归保持通过。
- 本轮 `go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 79.215s、manager 11.096s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation603/cluster-report.json` 于 `2026-09-07T07:08:34.9931955Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation603/release-candidate.json` 于 `2026-09-07T07:23:57.4812407Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 604

- 继续收敛 P1-SQL-001/P1-SQL-002：通用查询入口现在会递归归一化任意括号层级中的嵌套 CTE，使 `WITH outer_rows AS (SELECT ... FROM (WITH inner_rows AS (...) ...))` 在 CTE 语法校验、普通执行器和存储集成路径上与视图查询共用同一派生表重写逻辑。
- 新增 `TestCTESupportsNestedDerivedCTESource` 失败先行回归，并补充 `TestCreateTableAsSelectSupportsNestedDerivedCTE` 固化 CTAS 同一边界；同时保留视图嵌套 CTE、顶层 CTE、派生表/集合运算和 CTE DML 回归。
- 本轮 CTE/视图/派生表宽回归通过（engine 14.649s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 95.727s、manager 17.408s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation604/cluster-report.json` 于 `2026-09-07T07:31:21Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation604/release-candidate.json` 于 `2026-09-07T07:43:34Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 605

- 继续收敛 P1-SQL-001/P1-EXE-003：相关标量子查询现在支持出现在普通 JOIN 的 `ON` 条件中。执行器会暂缓在 JOIN 前物化该标量，并按每个候选连接行绑定外层限定列后执行子查询；INNER/LEFT JOIN 保持比较、NULL 和未匹配行语义，普通相关投影路径不变。
- 新增 `TestCorrelatedScalarSubquerySupportsJoinOnPredicate` 失败先行回归，覆盖 `MAX()` 相关标量的 INNER JOIN 和无匹配父行的 LEFT JOIN；相关投影、隐式别名、CASE、多子查询和派生 JOIN 回归保持通过。
- 本轮相关/派生矩阵通过（engine 8.140s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 75.586s、manager 14.317s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation605/cluster-report.json` 于 `2026-09-07T07:55:56Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation605/release-candidate.json` 于 `2026-09-07T08:10:00Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 606

- 继续收敛 P1-SQL-001/P1-EXE-003：相关标量子查询现在不仅支持直接作为 JOIN `ON` 比较值，也支持嵌套在 `COALESCE` 等表达式参数中；JOIN 表达式求值会递归发现并按当前候选连接行替换子查询结果，再复用现有函数/算术/NULL 语义求值。
- 扩展 `TestCorrelatedScalarSubquerySupportsJoinOnPredicate`，覆盖 `LEFT JOIN ... ON ... COALESCE((SELECT MAX(...)), 0)` 的匹配父行和无子行父行；直接标量子查询的 INNER/LEFT 语义保持通过。
- 本轮相关/派生矩阵通过（engine 8.460s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 75.528s、manager 9.699s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation606/cluster-report.json` 于 `2026-09-07T08:14:47.0898062Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation606/release-candidate.json` 于 `2026-09-07T08:26:49.3889071Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 607

- 继续收敛 P1-SQL-001/P1-EXE-003：派生 JOIN 路径现在沿用普通 JOIN 的相关标量子查询 evaluator；`FROM (SELECT ...) p LEFT JOIN ... ON ... (SELECT ...)` 会按合并后的候选行绑定外层列并执行子查询，不再落回旧的 `evalPredicate` 而报 `*sqlparser.Subquery` 不支持。
- 扩展 `TestCorrelatedScalarSubquerySupportsJoinOnPredicate`，覆盖派生表外层来源、普通表右侧、LEFT JOIN 未匹配行和聚合标量结果；普通基表 JOIN 与 `COALESCE` 包装路径继续通过。
- 本轮相关/派生矩阵通过（engine 8.316s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.249s、manager 10.498s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation607/cluster-report.json` 于 `2026-09-07T08:31:24.4573734Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation607/release-candidate.json` 于 `2026-09-07T08:43:14.1018721Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 608

- 继续收敛 P1-SQL-001/P1-EXE-003 的错误契约：JOIN `ON` 中的相关标量子查询若返回多于一行，现在通过 JOIN evaluator 向上返回明确错误，不再被错误吞掉并当作“不匹配”；普通 JOIN 与派生 JOIN 两条路径共用该错误传播机制。
- 扩展 `TestCorrelatedScalarSubquerySupportsJoinOnPredicate`，覆盖多行标量子查询失败，并保持直接标量、`COALESCE`、派生表外层和 LEFT JOIN NULL 扩展回归。
- 本轮相关/派生矩阵通过（engine 8.306s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.346s、manager 10.370s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation608/cluster-report.json` 于 `2026-09-07T08:47:39.4788765Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation608/release-candidate.json` 于 `2026-09-07T08:59:25.271975Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 609

- 继续收敛 P1-SQL-001/P1-EXE-003：JOIN `ON` 中的相关 `IN/NOT IN (SELECT ...)` 现在不会被语句级 IN 重写器提前物化，而是按每个候选连接行执行集合子查询；集合结果保留空集、NULL 和 `NOT IN` 的 UNKNOWN 三值语义。
- 普通表 JOIN 与派生表 JOIN 均贯通集合子查询 evaluator；新增回归覆盖 `IN` 多行匹配、派生外层来源，以及含 NULL 的 `NOT IN`，同时保留标量/`COALESCE`/多行标量错误边界。
- 本轮相关/派生矩阵通过（engine 9.945s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.811s、manager 10.400s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation609/cluster-report.json` 于 `2026-09-07T09:08:26.1023949Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation609/release-candidate.json` 于 `2026-09-07T09:20:15.1596471Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 610

- 继续收敛 P1-SQL-001/P1-EXE-003：相关集合子查询 evaluator 现在同时贯通普通 JOIN 和派生 JOIN 的候选行匹配，并将 `IN/NOT IN` 子查询结果作为集合处理；JOIN 前置重写器只对非 JOIN `ON` 形态做静态 IN 物化，避免相关列被提前求值。
- JOIN 路径保留 `NOT IN` 的 NULL UNKNOWN 行为；本轮新增普通/派生 `IN` 多行匹配、含 NULL 的 `NOT IN` 回归，标量子查询和多行标量错误边界继续通过。
- 本轮相关/派生矩阵通过（engine 8.507s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.451s、manager 10.812s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation610/cluster-report.json` 于 `2026-09-07T09:24:10.8956366Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation610/release-candidate.json` 于 `2026-09-07T09:36:40.5344217Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 611

- 继续收敛 P1-SQL-001/P1-EXE-003：JOIN `ON` 中的相关 `EXISTS/NOT EXISTS` 不再被旧的外层 WHERE 兼容重写器误判；执行器现在按候选连接行进入结构化 JOIN evaluator，并复用集合子查询结果保持 EXISTS/NOT EXISTS 真假语义。
- 同时修正 JOIN/WHERE 边界识别：JOIN 后的外层 WHERE EXISTS 不会再被误识别为 JOIN ON，既有普通相关 EXISTS/派生外层 EXISTS 兼容路径保持生效。
- 扩展 `TestCorrelatedScalarSubquerySupportsJoinOnPredicate`，覆盖普通表 JOIN、派生表 JOIN、`NOT EXISTS` 以及 JOIN 后 WHERE 相关 EXISTS 边界；标量、`COALESCE`、`IN/NOT IN` 和多行标量错误回归保持通过。
- 本轮相关/派生矩阵通过（engine 8.929s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.590s、manager 12.770s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation611/cluster-report.json` 于 `2026-09-07T09:44:03.8581821Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation611/release-candidate.json` 于 `2026-09-07T09:57:15.1683056Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 612

- 继续收敛 P1-SQL-003/P1-SQL-008 元数据兼容：新增 `SHOW TABLE STATUS` 原始 SQL 路由，返回 MySQL 18 列结果形状，并从真实 `.frm`、行扫描、表选项和自增状态读取 `Engine/Row_format/Rows/Auto_increment/Collation/Create_options/Comment` 等字段。
- `SHOW TABLE STATUS` 支持 `FROM/IN` 选择数据库、`LIKE` 和 `WHERE` 过滤；当前会话的临时表按逻辑名展示并隐藏其他会话临时表，普通表排序和信息架构已有路径保持不变。
- 新增 `TestShowTableStatusReturnsMySQLMetadataShape` 失败先行回归，覆盖 18 列契约、真实行数、自增下一值、ROW_FORMAT、注释以及 LIKE/WHERE 过滤。
- 本轮 engine 全量通过（87.067s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.671s、manager 10.675s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation612/cluster-report.json` 于 `2026-09-07T10:07:26.1590401Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation612/release-candidate.json` 于 `2026-09-07T10:23:52.583617Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 613

- 继续收敛 P1-SQL-003/P1-SQL-008 存储对象元数据兼容：新增 `SHOW TRIGGERS` 原始 SQL 路由，读取已持久化的 trigger 定义，返回 MySQL 11 列结果形状，包括 `Trigger/Event/Table/Statement/Timing/Created/Definer` 和字符集、排序规则字段。
- `SHOW TRIGGERS` 支持 `FROM/IN` 选择数据库、`LIKE` 和 `WHERE` 过滤；trigger 的实际执行、排序和 `INFORMATION_SCHEMA.TRIGGERS` 路径保持复用，不新增另一份对象存储。
- 新增 `TestShowTriggersReturnsTriggerMetadata` 失败先行回归，覆盖真实 trigger 创建、结果列契约、事件/时机/对象表和 LIKE 过滤。
- 本轮 engine 全量通过（72.059s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.087s、manager 11.671s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation613/cluster-report.json` 于 `2026-09-07T10:29:17.3480648Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation613/release-candidate.json` 于 `2026-09-07T10:41:38.4684629Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 614

- 继续收敛 P1-SQL-003/P1-SQL-008 存储对象元数据兼容：新增 `SHOW EVENTS` 原始 SQL 路由，读取已持久化 event 定义，返回 MySQL 15 列调度结果形状，包括数据库、定义者、时区、事件类型、执行时间、周期值/单位、开始/结束时间、状态和字符集信息。
- `SHOW EVENTS` 支持 `FROM/IN` 选择数据库和按事件名 `LIKE` 过滤；启用/禁用状态复用事件对象持久化字段，调度器与 `INFORMATION_SCHEMA.EVENTS` 路径保持不变。
- 新增 `TestShowEventsReturnsScheduleMetadata` 失败先行回归，覆盖真实周期事件、结果列契约、周期调度字段、状态和 LIKE 过滤。
- 本轮 engine 全量通过（71.977s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.038s、manager 9.825s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation614/cluster-report.json` 于 `2026-09-07T10:46:39.3483392Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation614/release-candidate.json` 于 `2026-09-07T11:00:09.8650816Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 615

- 继续收敛 P1-SQL-003/P1-SQL-008 存储对象元数据兼容：新增 `SHOW PROCEDURE STATUS` 与 `SHOW FUNCTION STATUS` 原始 SQL 路由，复用持久化 routine 定义，返回 MySQL 11 列状态形状并区分 `PROCEDURE/FUNCTION`。
- routine status 支持 `FROM/IN` 选择数据库、按名称 `LIKE` 过滤，并返回定义者、创建/修改时间、SQL SECURITY、注释和字符集/排序规则字段；CALL、routine 权限和 `INFORMATION_SCHEMA.ROUTINES` 路径保持不变。
- 新增 `TestShowRoutineStatusReturnsProcedureAndFunctionMetadata` 失败先行回归，覆盖 procedure/function 两类对象、结果列契约、数据库和名称过滤。
- 本轮 engine 全量通过（73.012s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 77.204s、manager 9.852s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation615/cluster-report.json` 于 `2026-09-07T11:05:13.453372Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation615/release-candidate.json` 于 `2026-09-07T11:17:37.9575066Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 616

- 继续收敛 P1-SQL-003/P1-SQL-008 存储对象元数据兼容：`SHOW CREATE PROCEDURE/FUNCTION/TRIGGER/EVENT` 不再统一返回两列，而是分别返回 MySQL 约定的完整结果形状；routine 增加 `sql_mode` 与字符集/排序规则列，trigger 返回 `SQL Original Statement`，event 增加 `time_zone` 与 `Create Event` 列。
- 新增 `TestShowCreateStoredObjectsReturnsMySQLMetadataShape` 失败先行回归，覆盖四类对象的列顺序、对象名和 CREATE 定义；既有存储对象权限、调用、SHOW STATUS、INFORMATION_SCHEMA 路径保持通过。
- 本轮 engine 全量通过（73.973s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.303s、manager 13.904s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation616/cluster-report.json` 于 `2026-09-07T11:28:20.705444Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation616/release-candidate.json` 于 `2026-09-07T11:40:49.4201714Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 617

- 继续收敛 P1-SQL-003/P1-SQL-008 客户端元数据兼容：新增 `SHOW CHARACTER SET`/`SHOW CHARSET` 与 `SHOW COLLATION` 路由，返回 MySQL 约定的字符集/排序规则列形状，并支持 `LIKE` 与 `WHERE` 过滤；常用 `utf8mb4`、`utf8mb3`、`utf8`、`binary` 元数据可直接通过 SHOW 查询。
- 新增 `TestShowCharacterSetAndCollationReturnMetadataShape` 失败先行回归，覆盖列顺序、LIKE 过滤、字符集/排序规则结果和 WHERE 条件；同时修正新 raw SHOW 解析辅助函数的 Go 正则兼容性，避免不支持反向引用导致 panic。
- 本轮 engine 全量通过（73.882s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 76.942s、manager 9.937s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation617/cluster-report.json` 于 `2026-09-07T11:47:46.9050302Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation617/release-candidate.json` 于 `2026-09-07T12:00:02.0618426Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 618

- 继续收敛 P1-SQL-001：外层 `HAVING` 现在支持直接引用未投影的相关标量子查询；执行器会将相关标量按分组后的外层行物化为隐藏列，再在 HAVING 阶段执行比较，保留空结果 `NULL`、逐行外层绑定和最终可见列裁剪。
- 新增 `TestCorrelatedScalarHavingCanUseUnprojectedSubquery` 失败先行回归，覆盖父表分组、子表 `COUNT(*)` 相关过滤和排序结果；已有相关投影、HAVING 别名、GROUP BY 表达式、JOIN/派生查询回归保持通过。
- 本轮 engine 全量通过（74.881s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 80.076s、manager 12.198s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation618/cluster-report.json` 于 `2026-09-07T12:06:30.800192Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation618/release-candidate.json` 于 `2026-09-07T12:19:29.9057735Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 619

- 继续收敛 P1-OPT-005/P1-EXE-004 聚合函数矩阵：传统存储集成 SELECT 聚合路径现在支持 `JSON_ARRAYAGG` 与 `JSON_OBJECTAGG`，保留数组中的 SQL `NULL`、对象键值映射和确定性 JSON 序列化，并正确返回字符串结果类型。
- 新增 `TestJSONArrayAggregateThroughStorageSelect` 失败先行回归，覆盖数值/NULL 数组聚合、对象键值聚合及 ALTER 后列读取；原有 GROUP_CONCAT、标准差、方差、位聚合、派生聚合和 Volcano 聚合回归保持通过。
- 本轮 engine 全量通过（73.283s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 79.706s、manager 10.713s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation619/cluster-report.json` 于 `2026-09-07T12:26:28.4497878Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation619/release-candidate.json` 于 `2026-09-07T12:38:45.9419273Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 620

- 继续收敛 P1-OPT-005/P1-EXE-004 聚合函数矩阵：传统存储集成 SELECT 聚合路径现在支持 `ANY_VALUE()`，按每个分组取首个非 NULL 值，并沿用源列类型返回，避免将该函数按普通逐行表达式执行。
- 扩展 `TestAggregateCompatibilityFunctionsThroughSQL` 失败先行回归，覆盖普通表聚合结果和已有 GROUP_CONCAT/标准差/方差/位聚合；JSON_ARRAYAGG/JSON_OBJECTAGG 专项回归保持通过。
- 本轮 engine 全量通过（75.143s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.287s、manager 13.131s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation620/cluster-report.json` 于 `2026-09-07T12:45:32.2802197Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation620/release-candidate.json` 于 `2026-09-07T12:58:15.653506Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 621

- 继续收敛 P1-OPT-005/P1-EXE-004 聚合函数 NULL 语义：传统存储集成 SELECT 聚合路径现在区分 `COUNT(*)` 与 `COUNT(expr)`；前者统计分组内全部行，后者忽略 NULL 表达式值，`COUNT(DISTINCT expr)` 原有去重与 NULL 忽略保持不变。
- 扩展 `TestAggregateCompatibilityFunctionsThroughSQL` 失败先行回归，新增含 NULL 的 `COUNT(*)`、`COUNT(score)` 与 `ANY_VALUE(name)` 联合查询，确保修复不改变已有聚合结果。
- 本轮 engine 全量通过（74.694s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.465s、manager 10.406s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation621/cluster-report.json` 于 `2026-09-07T13:04:14.5568051Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation621/release-candidate.json` 于 `2026-09-07T13:16:38.4344495Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 622

- 继续收敛 P1-OPT-005/P1-EXE-004 聚合函数空集语义：传统存储集成 SELECT 对无 `GROUP BY` 的空表聚合现在保留一个聚合组，`COUNT(*)`/`COUNT(expr)` 返回 0，`SUM`/其他无数据聚合按 MySQL 语义返回 NULL；有 `GROUP BY` 的空输入仍不产生分组行。
- 扩展 `TestAggregateCompatibilityFunctionsThroughSQL` 失败先行回归，覆盖空表上的 `COUNT(*)`、`COUNT(score)` 和 `SUM(score)` 返回契约。
- 本轮 engine 全量通过（74.760s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.833s、manager 10.645s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation622/cluster-report.json` 于 `2026-09-07T13:21:11.5761333Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation622/release-candidate.json` 于 `2026-09-07T13:33:25.008926Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 623

- 继续收敛 P1-OPT-005/P1-EXE-004 窗口聚合矩阵：窗口执行路径现在支持 `JSON_ARRAYAGG(expr) OVER (...)`，复用已有 partition/order/frame 边界，保留 NULL 元素，并依据源列类型将数值序列化为 JSON 数字而不是字符串。
- 新增 `TestWindowAggregateFunctionsHonorPartitionAndFrame` 回归，覆盖按 team 分区、按 id 排序、运行 frame 的 JSON 数组结果；既有 SUM/AVG/COUNT、统计、位聚合、ROWS/RANGE 和 named window 回归保持通过。
- 本轮 engine 全量通过（74.844s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.376s、manager 10.042s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation623/cluster-report.json` 于 `2026-09-07T13:39:49.4129211Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation623/release-candidate.json` 于 `2026-09-07T13:52:05.370982Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 624

- 继续收敛 P1-OPT-005/P1-EXE-004 窗口聚合矩阵：窗口执行路径现在支持 `JSON_OBJECTAGG(key, value) OVER (...)`，按 partition/order/frame 逐帧构建对象，保留 JSON NULL value，并按 MySQL 结果形状返回 JSON 字符串。
- 扩展 `TestWindowAggregateFunctionsHonorPartitionAndFrame` 失败先行回归，覆盖按 team 分区、按 id 排序的累计对象聚合；已有窗口数组、数值、统计和位聚合回归保持通过。
- 本轮 engine 全量通过（74.196s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.727s、manager 10.848s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation624/cluster-report.json` 于 `2026-09-07T13:57:28.4075199Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation624/release-candidate.json` 于 `2026-09-07T14:09:45.0554985Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 625

- 继续收敛 P1-SQL-003/P1-SQL-008/P1-EXE-004 存储程序执行：过程体现在支持常见单行 `SELECT expr INTO local_or_user_variable FROM ...`，复用真实查询执行器读取结果并写回局部变量或会话用户变量；查询无行时接入已有 `NOT FOUND` continue/exit handler 语义，结果多列与目标变量数量不一致会明确报错。
- 新增 `TestProcedureSelectIntoPublishesQueryValueToOutParameter` 与 `TestProcedureSelectIntoNoRowsInvokesNotFoundHandler` 失败先行回归，覆盖 OUT 参数回写、单行查询和无数据 handler 后续语句执行。
- 本轮 engine 全量通过（76.374s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 79.449s、manager 13.110s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation625/cluster-report.json` 于 `2026-09-07T14:18:50.9283161Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation625/release-candidate.json` 于 `2026-09-07T14:31:17.0603403Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 626

- 继续收敛 P1-SQL-003/P1-SQL-008/P1-EXE-004 存储程序错误语义：`SELECT ... INTO` 返回多行时现在构造 `SQLEXCEPTION` 条件并复用已有 continue/exit handler；无匹配行和多行结果均不再绕过过程 handler 直接终止。
- 新增 `TestProcedureSelectIntoMultipleRowsInvokesSQLExceptionHandler` 失败先行回归，覆盖多行错误、handler 写回 OUT 参数以及 handler 后续语句继续执行；单行赋值和 `NOT FOUND` 回归保持通过。
- 本轮 engine 全量通过（74.738s），`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine 78.567s、manager 10.375s。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation626/cluster-report.json` 于 `2026-09-07T14:35:42.6360482Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation626/release-candidate.json` 于 `2026-09-07T14:48:18.5620284Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、exit code 0、0 failures、0 errors、0 skipped。

## Continuation 627

- 继续收敛 P1-SQL-003/P1-SQL-008/P1-EXE-004：修正单操作 `ALTER TABLE ... RENAME INDEX old TO new` raw 兼容路径，目标索引名已存在时现在明确报 duplicate error 且不写入部分元数据；新增 `TestAlterTableRenameIndexRejectsDuplicateTargetAtomically` 失败先行回归。
- 继续收敛存储过程错误诊断语义：handler 内支持 `GET DIAGNOSTICS CONDITION 1` 的 `MESSAGE_TEXT`、`RETURNED_SQLSTATE`、`MYSQL_ERRNO` 赋值，`SIGNAL ... SET MESSAGE_TEXT` 会保留独立消息文本，同时不改变既有 `RESIGNAL` 行为；新增 `TestProcedureGetDiagnosticsReadsSignaledMessage` 失败先行回归。
- 本轮专项 ALTER/存储对象回归通过；engine 全量 `75.320s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `81.006s`、manager `38.716s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation627/cluster-report.json` 于 `2026-09-07T14:58:48.5132697Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation627/release-candidate.json` 于 `2026-09-07T15:11:19.6237123Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 628

- 继续收敛存储过程诊断语义：普通 SQL 语句触发 `SQLEXCEPTION` 时，handler 现在建立并在执行后恢复 active condition，`GET DIAGNOSTICS CONDITION 1` 可读取该语句错误的 `MESSAGE_TEXT`；新增 `TestProcedureGetDiagnosticsReadsStatementErrorMessage` 失败先行回归，`SIGNAL` 诊断和既有 handler/RESIGNAL 回归保持通过。
- 本轮 engine 全量 `75.930s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `79.821s`、manager `10.867s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation628/cluster-report.json` 于 `2026-09-07T15:15:59.1439683Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation628/release-candidate.json` 于 `2026-09-07T15:28:21.9362878Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 629

- 继续收敛存储过程错误诊断语义：`SELECT ... INTO` 多行结果现在以 SQLSTATE `21000` 建立 active condition，`SQLEXCEPTION` handler 内的 `GET DIAGNOSTICS CONDITION 1` 可读取 cardinality error 文本；新增 `TestProcedureGetDiagnosticsReadsSelectIntoCardinalityError` 失败先行回归，SIGNAL、普通 SQL 错误和既有 SELECT INTO handler 回归保持通过。
- 本轮 engine 全量 `75.462s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `79.693s`、manager `10.771s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation629/cluster-report.json` 于 `2026-09-07T15:32:44.8994846Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation629/release-candidate.json` 于 `2026-09-07T15:45:07.0081656Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 630

- 继续收口 P1-06 legacy storage wrapper provider wiring：`PageFactory` 新增 `CreatePageWithStorage`，统一将同一个 `StorageProvider` 传递给主要页面类型的 provider-backed wrapper；默认 `CreatePage` 改为复用该分发路径，INDEX/INODE 走完整 `IPageWrapper` 的 unified provider wrapper，避免旧 INDEX runtime type assertion 进入不完整 wrapper。
- 新增 `TestPageFactoryCreatesProviderBackedPage`，覆盖 INDEX/FSP/INODE/IBUF/SYS/XDES/UNDO/ALLOCATED/BLOB/COMPRESSED/ENCRYPTED/IBUF_BITMAP/TRX_SYS 的 provider 写入与读取；补充 data-dictionary 与 allocated wrapper 的 WithStorage 构造。
- wrapper 全包通过；engine 全量 `77.182s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `80.753s`、manager `13.267s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation630/cluster-report.json` 于 `2026-09-07T15:54:56.1155309Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation630/release-candidate.json` 于 `2026-09-07T16:07:33.2508039Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 631

- 继续收口 P1-06 PageFactory provider 边界：`CreateBlobPageWithStorage`、`CreateRollbackPageWithStorage` 和 `ParsePageWithStorage` 已加入统一工厂；默认创建/解析入口继续保留并委托到 provider 为空的兼容路径，解析后的页面会保留 provider 供后续 `Read/Write/Flush` 使用。
- 新增 `TestPageFactorySpecialPagesAndParseUseProvider`，覆盖 Blob、Rollback 的工厂持久化以及带 provider 的解析后回读；wrapper 全包回归保持通过。
- 本轮 engine 全量 `76.494s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `78.964s`、manager `9.706s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation631/cluster-report.json` 于 `2026-09-07T16:12:14.4136498Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation631/release-candidate.json` 于 `2026-09-07T16:24:43.6421034Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、0 failures、0 errors、0 skipped。

## Continuation 632

- 继续收口 P1-06 deprecated wrapper provider migration：旧 `BlobPage` 新增 `NewBlobPageWithStorage`，旧 `IBufPageWrapper` 新增 `NewIBufPageWrapperWithStorage`；原有构造函数保持兼容并委托到 provider 为空的路径。
- 修复旧 `BlobPage` 的实际页布局缺陷：BLOB body 现在写入固定 16KB 页面并位于文件头之后，避免原先将 content 缩成 payload 后由基础页序列化越界；`SetData`/`SetNextPartPage` 改用已持锁的 dirty helper，消除旧路径自锁死结。
- 新增 `TestRemainingDirectLegacyPagesUseStorageProvider`，验证 direct Blob/IBuf wrapper 的 provider 写入、重载读取和真实数据 round-trip。
- wrapper 全包通过；engine 全量 `110.157s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `105.397s`、manager `15.311s`，page wrapper `1.347s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation632/cluster-report.json` 于 `2026-09-07T16:35:20.2055543Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation632/release-candidate.json` 于 `2026-09-07T16:47:57.5894946Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 07 秒。

## Continuation 633

- 继续收口 P1-06 direct legacy wrapper provider migration：旧 `NewInodePageWrapper` 新增 `NewInodePageWrapperWithStorage`，system `INode` 增加 provider-backed `Read/Write`；历史无 provider 构造保持兼容。
- 修复 `store/pages.INodePage.SerializeBytes` 丢失 `INodePageList` 的持久化缺陷：序列化现在写入前后节点页号和 offset，和解析端 38..50 字节布局一致，provider 重载不会再把 inode 链接信息重置为零。
- 新增 `TestDirectInodePageWrapperUsesStorageProvider`，验证 inode list 字段写入 provider、重新加载和 round-trip；同时保留 direct Blob/IBuf provider 回归。
- `go test ./server/innodb/storage/store/pages ./server/innodb/storage/wrapper/... -count=1 -timeout 5m` 通过；engine 全量 `109.232s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `104.209s`、manager `12.033s`、page wrapper `1.077s`、store/pages `0.630s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation633/cluster-report.json` 于 `2026-09-07T16:52:54.8677445Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation633/release-candidate.json` 于 `2026-09-07T17:08:16.4077751Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 11 秒。

## Continuation 634

- 继续收口 P1-06 direct legacy page migration：旧 `Allocated` 页面新增 `NewAllocatedPageWithStorage(spaceID, pageNo, storage)`，provider 模式下 `Read/Write` 真正读写固定 16KB 页面；`NewAllocatedPage` 与 `NewAllocatedPageByBytes` 保持原有无 provider 兼容路径。
- 新增 `TestDirectAllocatedPageUsesStorageProvider`，验证 direct allocated page 的内容写入 provider、由新实例加载并恢复页面 body；无 provider 的内存状态路径保持不变。
- 页面存储与 wrapper 全包通过（store/pages `0.903s`、page `0.820s`）；engine 全量 `107.107s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `102.946s`、manager `14.251s`、page wrapper `1.043s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation634/cluster-report.json` 于 `2026-09-07T17:11:54.1038014Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation634/release-candidate.json` 于 `2026-09-07T17:27:07.3523126Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 04 秒。

## Continuation 635

- 继续收口 P1-06 direct legacy wrapper provider migration：旧 `IBuf` bitmap wrapper 新增 `NewIBufWithStorage(spaceID, pageNo, storage)`、真实 `Read/Write` 和固定 16KB 页校验；历史 `NewIBuf`/`NewIBufByLoadBytes` 保持兼容。
- 新增 `TestDirectIBufPageUsesStorageProvider`，验证 change-buffer bitmap 字节通过 provider 写入并由新实例重载恢复。
- 页面存储与 wrapper 全包通过（store/pages `0.838s`、page wrapper `0.715s`）；engine 全量 `111.842s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `105.903s`、manager `13.911s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation635/cluster-report.json` 于 `2026-09-07T17:31:46.3262588Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation635/release-candidate.json` 于 `2026-09-07T17:47:18.1486867Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 07 秒。

## Continuation 636

- 继续收口 P1-06 基础页包装器 provider migration：`basic.BasePageWrapper` 新增 `NewBasePageWrapperWithStorage`，provider 模式下 `Read/Write` 现在实际读写固定 16KB 页面并更新读取/写入统计、状态和 dirty 标记；历史无 provider 构造保持兼容，但调用 I/O 时返回明确的 provider unavailable 错误，避免空实现伪装成功。
- 新增 `TestBasicBasePageWrapperUsesStorageProvider`，验证基础 INDEX 页通过 provider 写入、由新实例读取并恢复内容。
- 页面存储与 wrapper 全包通过（store/pages `1.312s`、page wrapper `0.802s`）；engine 全量 `113.078s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `106.532s`、manager `12.665s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation636/cluster-report.json` 于 `2026-09-07T17:52:18.0852606Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation636/release-candidate.json` 于 `2026-09-07T18:06:55.7523557Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 06 秒。

## Continuation 637

- 修复四类 legacy page wrapper 的真实自死锁：`BlobPageWrapper.SetBlobData`、`CompressedPageWrapper.SetData`、`IBufFreeListPageWrapper` 的写操作以及 `RollbackPageWrapper` 的字段/undo-slot 写操作，原先持有 `BasePageWrapper` 写锁后再次调用 `MarkDirty()`，现统一改用已持锁的 `markDirtyLocked()`。
- 新增 `TestLegacyPageMutationsDoNotSelfDeadlock`；失败先行测试确认四个路径均会超时，修复后四个路径均即时完成，并保留 provider wrapper 与 factory 回归。
- 页面存储与 wrapper 全包通过（store/pages `1.151s`、page wrapper `1.039s`）；engine 全量 `108.494s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `105.098s`、manager `12.197s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation637/cluster-report.json` 于 `2026-09-07T18:11:11.2211956Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation637/release-candidate.json` 于 `2026-09-07T18:26:22.1491685Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 03 秒。

## Continuation 638

- 补齐四类 legacy 专用页面的真实 payload 持久化：Blob、Compressed、IBuf free-list、Rollback wrapper 现在分别覆盖 `Read/Write` 的专用反序列化/序列化，不再只写基类旧 content；Rollback 的专用字段写入 16KB 页面布局，IBuf free-list 的文件头/尾序列化也改为保留真实 header/trailer。
- 新增 `TestLegacySpecializedWrappersRoundTripPayload`，先验证旧实现写入成功但重载 payload 丢失/压缩数据无效，再修复为四个 payload round-trip 全部通过。
- 页面存储与 wrapper 全包通过（store/pages `1.186s`、page wrapper `1.323s`）；engine 全量 `110.002s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `105.394s`、manager `14.064s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation638/cluster-report.json` 于 `2026-09-07T18:31:35.6305200Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation638/release-candidate.json` 于 `2026-09-07T18:46:50.4592656Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 04 秒。

## Continuation 639

- 修复 legacy `BasePageWrapper` 的 I/O 假成功：当既没有 `StorageProvider`、也没有 `BufferPage` 时，`Write` 和 `Flush` 现在返回 `ErrPageStorageUnavailable`，不会再把未落盘页面标记为 flushed/clean；失败写入保留 dirty 状态。
- 新增 `TestBasePageWrapperRequiresStorageForWriteAndFlush`，覆盖无 provider 的 Write/Flush 错误契约和 dirty 状态保持。
- 页面存储与 wrapper 全包通过（store/pages `1.046s`、page wrapper `1.088s`）；engine 全量 `107.686s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `103.793s`、manager `12.121s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation639/cluster-report.json` 于 `2026-09-07T18:50:04.2850522Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation639/release-candidate.json` 于 `2026-09-07T19:05:21.7393242Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 09 秒。

## Continuation 640

- 收紧 6 个专用 wrapper 的无存储写入契约：FSP、IBUF bitmap、Encrypted、TRX_SYS、XDES、UNDO 在没有 `StorageProvider` 且没有 `BufferPool` 时不再静默返回成功，统一返回 `ErrPageStorageUnavailable`；正常 provider/buffer 写路径保持不变。
- 新增 `TestSpecializedPageWrappersRequireStorageForWrite`，先行测试确认六个 wrapper 均存在假成功，修复后全部返回明确错误。
- 页面存储与 wrapper 全包通过（store/pages `1.197s`、page wrapper `1.048s`）；engine 全量 `109.071s`，`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `104.620s`、manager `12.349s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation640/cluster-report.json` 于 `2026-09-07T19:09:32.4002428Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation640/release-candidate.json` 于 `2026-09-07T19:24:52.7339190Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 运行约 9 分 11 秒。

## Continuation 641

- 继续收紧 P1-06 专用页面 I/O 契约：`DataDictionaryPageWrapper.Write` 在没有 `StorageProvider` 且没有 `BufferPool` 时不再静默返回成功，统一返回 `ErrPageStorageUnavailable`。
- `TestSpecializedPageWrappersRequireStorageForWrite` 扩展覆盖第七类专用 wrapper；页面存储与 wrapper 全包通过（store/pages `1.203s`、page wrapper `1.021s`）。
- engine 全量 `111.369s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `106.567s`、manager `12.727s`、page wrapper `1.341s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation641/cluster-report.json` 于 `2026-09-07T19:28:07.9244794Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation641/release-candidate.json` 于 `2026-09-07T19:43:34.7575351Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`Failures: 0`、`Errors: 0`、`Skipped: 0`，JDBC 运行约 9 分 13 秒。

## Continuation 642

- 继续收口 `DataDictionaryPageWrapper` 的 BufferPool 持久化路径：`Write` 现在复用实际更新过的缓存页执行 `FlushPage`，不再用同坐标的空 `BufferPage` 覆盖刚写入的数据。
- 新增 `TestDataDictionaryWrapperFlushesTheUpdatedBufferPage`，验证配置 BufferPool 时字典 payload 会通过正确缓存页刷新并保留 `buffered_table` 内容；修复前测试先行失败，修复后通过。
- 页面存储与 wrapper 全包通过（store/pages `0.801s`、page wrapper `0.915s`）；engine 全量 `107.702s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `102.809s`、manager `87.900s`、page wrapper `0.822s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation642/cluster-report.json` 于 `2026-09-07T19:50:39.2919925Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation642/release-candidate.json` 于 `2026-09-07T20:03:17.7187764Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`Failures: 0`、`Errors: 0`、`Skipped: 0`，JDBC 运行约 9 分 05 秒。

## Continuation 643

- 继续补齐 FSP 页面包装器的 provider-backed 分配：当 legacy in-memory extent 列表尚未装载且存在 `StorageProvider` 时，`AllocatePages` 现在委托真实 `AllocatePage`；批量分配中途失败会回收本次已分配页面，避免半成功。
- 取消 `TestFspPageWrapper_AllocatePage` 的占位跳过，新增 provider 分配回归，验证真实页号序列和分配路径。
- 页面存储与 wrapper 全包通过（store/pages `1.103s`、page wrapper `1.206s`）；engine 全量 `111.151s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `106.447s`、manager `12.250s`、page wrapper `0.968s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation643/cluster-report.json` 于 `2026-09-07T20:07:04.9986279Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation643/release-candidate.json` 于 `2026-09-07T20:22:38.1043085Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`Failures: 0`、`Errors: 0`、`Skipped: 0`，JDBC 运行约 9 分 03 秒。

## Continuation 644

- 清理数据字典页面的遗留占位测试：`TestDataDictWrapperPlaceholder` 改为 `TestDataDictionaryWrapperProviderRoundTrip`，真实验证 provider 写入、重载和表定义读取，不再使用 `Skip` 掩盖已实现能力。
- page wrapper 全量回归通过（`0.539s`），`git diff --check` 无空白错误；Continuation 642/643 的 engine、全仓、集群和 Connector/J 门禁证据继续有效。

## Continuation 645

- 继续收敛存储过程游标语义：`DECLARE ... CURSOR FOR` 现在只保存游标查询，查询会在 `OPEN` 时绑定当前局部变量和 session 用户变量后执行；重复 `OPEN` 现在明确返回 already-open 错误，`CLOSE` 后仍可正常重新打开。
- 新增 `TestProcedureCursorEvaluatesQueryAtOpen` 与 `TestProcedureCursorRejectsOpeningAnAlreadyOpenCursor`，并保留游标 handler/loop、未打开 FETCH、关闭后重开回归。
- engine 专项通过 `1.784s`；engine 全量 `106.175s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `102.907s`、manager `14.403s`、page wrapper `0.653s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation645/cluster-report.json` 于 `2026-09-07T20:28:47.5785158Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation645/release-candidate.json` 于 `2026-09-07T20:43:38.9205409Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`Failures: 0`、`Errors: 0`、`Skipped: 0`，JDBC 运行约 9 分 16 秒。

## Continuation 646

- 继续补齐存储过程词法作用域：嵌套 block 的局部 `DECLARE` 变量现在不会污染外层同名变量；继承的外层变量在内层修改后仍按引用语义回写，既有嵌套局部 IN/OUT 参数传播保持不变。
- 新增 `TestProcedureNestedBlockPreservesVariableScope`，修复前先行验证内层 `value=3` 错误覆盖外层值，修复后验证外层输出 `1,4`；游标 OPEN/重复 OPEN 回归保持通过。
- engine 全量 `107.633s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `106.241s`、manager `12.946s`、page wrapper `0.730s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation646/cluster-report.json` 于 `2026-09-07T20:50:14.9402739Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation646/release-candidate.json` 于 `2026-09-07T21:04:28.2767922Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`，JDBC 门禁运行约 14 分 08 秒。

## Continuation 647

- 继续收敛存储过程诊断语义：`GET DIAGNOSTICS ... MYSQL_ERRNO` 现在从真实 `common.SQLError` 或已知错误文本推导错误号，不再把所有语句错误固定成 SIGNAL 的 1644；重复键错误返回 1062，SELECT INTO 多行错误保留 1172 等映射。
- `SIGNAL SQLSTATE ... SET MYSQL_ERRNO = n, MESSAGE_TEXT = '...'` 已支持自定义错误号和消息，handler 内可读取自定义诊断；默认 SIGNAL 仍使用 1644。
- 新增 `TestProcedureGetDiagnosticsReadsStatementErrorNumber` 与 `TestProcedureSignalCanSetMySQLErrorNumber`，失败先行均已复现并修复；存储对象专项通过 `1.634s`。
- engine 全量 `106.062s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `104.263s`、manager `11.982s`、page wrapper `0.817s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation647/cluster-report.json` 于 `2026-09-07T21:10:25.7657489Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation647/release-candidate.json` 于 `2026-09-07T21:25:55.4480473Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 648

- 继续收敛 routine 错误契约：未被 handler 捕获的 `SIGNAL SQLSTATE ... SET MYSQL_ERRNO/MESSAGE_TEXT` 现在返回标准 `common.SQLError`，协议层可获得自定义 errno 和 SQLSTATE；`RESIGNAL` 同样支持 errno 重写并保持原 SQLSTATE。
- 语句错误诊断同时补齐 SQLSTATE 映射：重复键错误的 `GET DIAGNOSTICS` 返回 `MYSQL_ERRNO=1062`、`RETURNED_SQLSTATE=23000`，而非固定的 1644/HY000。
- 新增 `TestProcedureSignalReturnsCustomErrorContractWithoutHandler`、`TestProcedureResignalCanRewriteMySQLErrorNumber` 和 `TestProcedureGetDiagnosticsReadsStatementSQLState`；失败先行均已复现并修复，存储对象专项通过 `1.840s`。
- engine 全量 `106.193s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `102.733s`、manager `12.076s`、page wrapper `0.768s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation648/cluster-report.json` 于 `2026-09-07T21:31:35.3836548Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation648/release-candidate.json` 于 `2026-09-07T21:47:17.4119706Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 649

- 继续收紧存储过程词法作用域：同一 `BEGIN...END` block 内重复 `DECLARE` 局部变量现在返回 duplicate 错误；嵌套 block 的同名变量遮蔽仍可正常使用，外层变量传播回归保持通过。
- 新增 `TestProcedureRejectsDuplicateLocalVariableInSameBlock`，失败先行确认旧实现会静默覆盖，修复后通过。
- engine 全量 `105.148s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `102.738s`、manager `12.594s`、page wrapper `0.775s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation649/cluster-report.json` 于 `2026-09-07T21:50:58.6676044Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation649/release-candidate.json` 于 `2026-09-07T22:06:39.145373Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 650

- 补齐 P0 核心 DML 的主键更新：`UPDATE` 修改主键时现在删除旧聚簇键、写入新聚簇键，并同步所有二级索引中的主键载荷；不再返回 `unsupported primary key UPDATE`。
- 新增 `TestUpdateCanMoveRowToANewPrimaryKey`，覆盖主键迁移、旧键消失、二级索引列查询和真实行数据；外键 `ON UPDATE CASCADE` 及多级级联回归保持通过。
- engine 全量 `106.410s`；`go test ./... -count=1 -timeout 5m` 全量通过，其中 engine `105.044s`、manager `15.147s`、page wrapper `0.941s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation650/cluster-report.json` 于 `2026-09-07T22:12:31.9407092Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation650/release-candidate.json` 于 `2026-09-07T22:27:06.3643078Z` 为 `GO`；全部检查 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 651

- 继续补齐核心 DML 的 `INSERT ... SELECT` 投影：来源查询不再局限于裸列复制，现在按源行计算算术、标量函数等表达式，并支持 `表别名.列`/`表名.列` 限定引用；目标列映射、WHERE 过滤和后续统一事务写入保持不变。
- 新增 `TestP0InsertSelectEvaluatesRowExpressions`，覆盖 `u.id + 10`、`upper(u.username)`、`u.age + 1` 和别名 WHERE 条件；修复前先行复现“INSERT SELECT仅支持列投影”，修复后通过。
- engine 全量 `76.769s`；P0 交付候选证据套件中恢复、并发、可观测性、回滚、全链路和默认回归均为 `PASS`，但交付审计仍为 `NOT_READY`，仅剩真实负责人签字、7 条风险项关闭/认领和发布决策等治理动作（代码门禁无失败）。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation651/cluster-report.json` 于 `2026-09-07T22:36` 左右生成并为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation651/release-candidate.json` 于 `2026-09-07T22:49:21.7217617Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 652

- 继续补齐 P1-EXE-003/P1-SQL-001 的 DML 查询来源：`INSERT ... SELECT` 遇到 JOIN、聚合、排序、LIMIT 或其他非单裸表来源时，改由普通 SELECT 执行器产出结果，再复用存储集成 INSERT 的目标列映射、约束、索引、事务和回滚链路；单表快速路径保持不变。
- JOIN 专用 SELECT 投影现在保存并执行原始 AST 表达式，不再只按字符串列名取值；因此 `CONCAT(u.name, ...)`、算术和限定列表达式可正确生成写入值。新增 `TestP1InsertSelectSupportsJoinProjection`，覆盖两表 JOIN、JOIN ON、WHERE、函数投影及目标表真实落盘。
- 定向 DML/JOIN 回归通过；engine 全量 `77.582s`，`git diff --check` 无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation652/cluster-report.json` 于 `2026-09-07T22:56:25.6497246Z` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation652/release-candidate.json` 于 `2026-09-07T23:09:07.0542613Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 653

- 继续完善 `INSERT ... SELECT` 的通用来源路由：派生表来源不再被全局标量子查询重写器误消费，改由派生表执行器物化全部列；分组聚合来源（如 `GROUP BY ... SUM(...) ORDER BY ...`）通过普通 SELECT 执行器生成结果后进入统一存储写入链路。
- 新增 `TestP1InsertSelectSupportsDerivedTableSource` 与 `TestP1InsertSelectSupportsGroupedAggregateSource`，分别覆盖派生表过滤投影和分组聚合结果真实落盘；JOIN、派生表、聚合、标量子查询回归均通过。
- engine 全量 `77.541s`，`go test ./... -count=1 -timeout 10m` 全仓通过；后续继续刷新包含本轮新增测试的集群与 Connector/J 门禁。

## Continuation 654

- 继续收敛 P1-EXE-003 的多表来源：存储集成 JOIN 执行器现在支持 `FROM a, b` 的隐式 CROSS JOIN，并正确消费 `JOIN ... USING (...)` 的列对匹配；原有 ON、LEFT/RIGHT JOIN 和限定列投影路径保持不变。
- 新增 `TestP1InsertSelectSupportsCommaJoinSource` 与 `TestP1InsertSelectSupportsUsingJoinSource`，覆盖逗号多表来源、USING 等值匹配、过滤和目标表真实落盘。
- JOIN/派生表/聚合/标量子查询专项回归通过；后续需要在本轮所有代码变更完成后刷新完整 engine、集群和 Connector/J 门禁。

## Continuation 655

- 继续补齐 P1-SQL-002/P1-EXE-003 的集合来源：`INSERT ... SELECT` 现在支持解析为 `UNION` AST 的 `UNION/UNION ALL` 来源，外层 dispatcher 会跳过 INSERT 的通用集合查询分派，统一集合结果再进入目标表的类型、约束、索引、事务和复制记录路径。
- 新增 `TestP1InsertSelectSupportsUnionSource`，覆盖两张来源表的 `UNION ALL` 结果真实写入；同时修复引擎包装层和查询执行层对 INSERT 集合来源的误路由。
- engine 全量 `79.051s`，专项 JOIN/派生表/聚合/UNION/标量子查询回归通过，`git diff --check` 无空白错误；集群与 Connector/J 门禁将在本轮代码收口后刷新。

## Continuation 656

- 本轮 JOIN/集合来源改动完成当前门禁收口：`UNION ALL` 来源、`JOIN ... USING`、隐式 CROSS JOIN、派生表和分组聚合 `INSERT ... SELECT` 均有真实落盘回归。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation655/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation655/release-candidate.json` 于 `2026-09-07T23:40:02.8433507Z` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136`、0 failures、0 errors、0 skipped。
- 当前仍未宣称“全部 MySQL 8.4 兼容”：FULLTEXT 与全量非 Connector/J 客户端按用户要求后置；原生 MySQL binlog/GTID 文件互操作、完整 MDL/online DDL、复杂任意 AST 组合和生产级外部 fencing 仍是明确边界。继续按剩余优先级逐项实现，不以本轮 GO 作为总完成标志。

## Continuation 657

- 在 UNION/USING/CROSS JOIN 改动后重新执行 `go test ./... -count=1 -timeout 10m`，全仓通过；engine 本轮 `82.086s`，manager `10.010s`，net `2.126s`，其余业务、协议、存储和复制包均通过。

## Continuation 658

- 完成并验证 `INSERT ... SELECT` 的默认 `UNION DISTINCT` 去重回归；JOIN、派生表、分组聚合、`UNION ALL` 与默认 `UNION` 来源均进入统一目标表写入路径。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation658/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation658/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136` 且 failures/errors/skips 均为 0。

## Continuation 659

- 继续补齐 P1 DML JOIN 执行：单目标 `UPDATE ... JOIN` 与 `DELETE ... JOIN/USING` 先物化命中目标主键，再复用既有单目标存储事务路径；支持目标列常量更新、JOIN 来源列/表达式更新、复合主键条件、目标主键去重以及 `ORDER BY/LIMIT` 命中集合，避免多匹配来源重复更新或把 JOIN 条件静默丢失。
- 新增 `TestP1UpdateJoinOnlyUpdatesMatchingTargetRows`、`TestP1UpdateJoinCanProjectJoinedSourceValues`、`TestP1DeleteJoinOnlyDeletesMatchingTargetRows`，均覆盖真实表数据落盘；engine 全量通过 `78.623s`，随后全仓 `go test ./... -count=1 -timeout 10m` 首轮暴露的既有 page-wrapper 预读 panic 已定位并修复。
- 修复 buffer pool 的真实根因：仅配置 `StorageProvider` 时 `FreeBlockList.storageManager` 为空，后台预读调用 `GetPage` 会解引用空接口；新增 `TestFreeBlockListWithoutStorageManagerReturnsNil` 失败先行回归并返回安全空结果。修复后 buffer_pool 专项、page wrapper 重复回归和全仓测试均通过；全仓最终 engine `84.750s`，无失败。
- 新代码 `git diff --check` 无空白错误。集群报告 `reports/compatibility/p1-cluster-current-continuation659/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation659/release-candidate.json` 于 `2026-09-08T00:18:50.1227229Z` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136`、failures=0、errors=0、skips=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按 P1/P3 顺序推进。

## Continuation 660

- 继续补齐 P1 JOIN DML 的目标表覆盖：`UPDATE ... JOIN` 与 `DELETE ... JOIN/USING` 不再要求目标表存在显式主键；无主键表按全部可见列生成 NULL-safe 行定位条件，保留存储层隐藏聚簇键，不将内部列暴露给 SQL。显式单列/复合主键仍优先走主键定位路径。
- 新增 `TestP1UpdateJoinSupportsTargetWithoutPrimaryKey` 与 `TestP1DeleteJoinSupportsTargetWithoutPrimaryKey`，先行测试确认旧实现会分别返回 “requires a primary key”，修复后验证命中行真实更新/删除且未命中行保持不变。
- engine 专项 JOIN 回归通过；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `83.867s`、manager `13.882s`，net、protocol、replication、全部存储 wrapper 均通过。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation660/cluster-report.json` 于 `2026-09-08T00:28:28Z` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation660/release-candidate.json` 于 `2026-09-08T00:41:07Z` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136`、failures/errors/skips 均为 0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 755

- 继续收口 P4 类型转换矩阵：`CAST/CONVERT` 计划表达式现在保留 `CHAR(N)`/`BINARY(N)` 长度修饰，并对 `DECIMAL(M,D)` 应用常见小数位舍入；`CHAR(N) CHARACTER SET charset` 在源字符截断后再执行目标字符集转码，避免 latin1 等非 UTF-8 字节被错误解释。
- 转换元数据已贯通逻辑计划、编译表达式和 storage-integrated DML/查询路径；新增 plan 解释执行/编译执行测试，以及真实引擎 SQL 对 `CHAR(3)`、`DECIMAL(8,2)`、`CHAR(1) CHARACTER SET latin1` 的回归。
- `go test ./... -count=1 -timeout 10m` 全仓通过（engine `90.123s`、plan `0.819s`、replication `3.172s`）；`git diff --check` 本批次文件无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation755/cluster-report.json` 为 `PASS`。
- 发布候选报告 `reports/compatibility/release-candidate-current-continuation755/release-candidate.json` 为 `GO`：build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 全部 PASS；Connector/J `136` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；完整类型/函数/collation 矩阵、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 760

- 收口 `JSON_VALUE` 常用策略：新增受限 standalone `JSON_VALUE(... RETURNING type [DEFAULT value|NULL|ERROR ON EMPTY] [DEFAULT value|NULL|ERROR ON ERROR])` 兼容入口；缺失路径/JSON null 按 EMPTY 策略处理，对象/数组、非法 JSON 或返回类型转换失败按 ERROR 策略处理。默认值继续复用统一表达式执行器。
- 新增解析、重写、真实引擎执行回归，覆盖 `DEFAULT`/`NULL` 的 EMPTY 与 ERROR 分支，以及 `RETURNING UNSIGNED`、`DECIMAL(8,2)`、`CHAR(3)`；普通两参数 `JSON_VALUE` 路径不变，复杂查询尾部组合仍不被误改写。
- 本轮 `go test ./... -count=1 -timeout 10m` 全仓通过：engine `92.719s`、manager `12.603s`、plan `0.449s`、replication `0.939s`；集群报告 `reports/compatibility/p1-cluster-current-continuation760/cluster-report.json` 为 `PASS`。
- 发布候选报告 `reports/compatibility/release-candidate-current-continuation760/release-candidate.json` 为 `GO`：clean-data、build、unit、integration、go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`；Connector/J `136` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`JSON_VALUE` 的任意 `FROM`/复杂尾部组合、完整类型/函数/collation 矩阵、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 761

- 扩展 `JSON_VALUE(... RETURNING ...)` 到普通查询：通过词法级安全重写接入统一执行器，现支持投影、`FROM`、`WHERE`、`ORDER BY` 及同一查询中的多个调用；文档、路径、默认值仍由表达式执行器按行求值。
- 新增真实引擎回归：建表写入 JSON 文本后，在投影和谓词中执行 `RETURNING UNSIGNED DEFAULT ... ON EMPTY`，验证按行转换、缺失路径默认值和排序结果。
- 本轮最终验证：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `89.810s`、manager `10.252s`、plan `1.088s`、net `2.281s`、replication `3.138s`）；集群报告 `reports/compatibility/p1-cluster-current-continuation761/cluster-report.json` 为 `PASS`。
- 发布候选报告 `reports/compatibility/release-candidate-current-continuation761/release-candidate.json` 为 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、100 次并发、observability 全部 `PASS`，Connector/J `136` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`JSON_VALUE` 的完整 SQL 语法/所有返回类型与复杂 JSON path、完整类型/函数/collation 矩阵、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 762

- 将 `JSON_VALUE(... RETURNING ...)` 纳入 Connector/J PreparedStatement 回归：新增 `PreparedStatementTest.testPreparedStatementJsonValueReturning`，验证 `COM_STMT_PREPARE`、参数绑定、`COM_STMT_EXECUTE` 和 ResultSet 数值读取的完整链路。
- Maven test-compile 通过；随后完整发布门禁 `reports/compatibility/release-candidate-current-continuation762/release-candidate.json` 为 `GO`，所有 Go、恢复、并发和 observability 检查均 `PASS`，Connector/J `137` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；复杂 JSON path、完整类型/函数/collation 矩阵、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 759

## Continuation 763

- 补齐发布证据中的外部进程级 crash-recovery 演练：服务进程被强制终止后重启，分别验证已提交数据仍存在、未提交事务已回滚，以及 DDL/索引重建元数据仍可查询。
- 使用隔离端口 `3317` 和独立数据目录连续执行 3 轮，3/3 轮均为 `PASS`；报告为 `reports/compatibility/external-crash-current-continuation763/external-crash-20260908-212559.json`。
- 该证据证明本地存储恢复路径在真实进程重启场景下可用，但不等同于生产环境外部 fencing、跨节点故障转移或上游 binlog/GTID 文件互操作；这些仍需按剩余边界推进或由外部平台提供验证。

## Continuation 764

- 将 Connector/J 服务器端游标加入真实回归：使用 `useServerPrepStmts=true&useCursorFetch=true`、`setFetchSize(2)` 执行 5 行查询，覆盖 `COM_STMT_PREPARE`、带 cursor flag 的 `COM_STMT_EXECUTE`、分批 `COM_STMT_FETCH`、结果耗尽和关闭。
- 新增 `PreparedStatementTest.testPreparedStatementServerSideCursor`；完整发布门禁 `reports/compatibility/release-candidate-current-continuation764/release-candidate.json` 为 `GO`，Go 核心、恢复、并发、可观测性均 PASS，Connector/J `138` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；游标的更多边界状态、完整 AuthSwitch/多因素认证、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍需继续推进。

## Continuation 765

- 补齐认证插件兼容边界：新增 `sha256_password` 验证器，明确区别于 `caching_sha2_password` 的双 SHA-256 存储格式，并纳入认证插件工厂。
- 网络认证在初始握手和 `COM_CHANGE_USER` 中按账户插件发送 `sha256_password` AuthSwitch；支持 TLS 明文口令与非 TLS RSA-OAEP 加密口令解包、scramble 还原、账号哈希校验和会话状态切换。
- 新增 auth/net 失败优先与协议回归；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `89.137s`、manager `10.373s`、net `2.652s`），集群报告 `reports/compatibility/p1-cluster-current-continuation765/cluster-report.json` 为 `PASS`。
- 发布候选报告 `reports/compatibility/release-candidate-current-continuation765/release-candidate.json` 为 `GO`：clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，Connector/J `138` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；更多认证插件/多因素状态机、游标完整边界、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍需继续推进。

## Continuation 766

- 账号管理与认证插件的密码存储格式已对齐：`CREATE USER`、`ALTER USER`、`SET PASSWORD` 会按 `caching_sha2_password` 使用双 SHA-256、按 `sha256_password` 使用单 SHA-256，并保留 native 插件的既有格式；新增真实持久化集成测试覆盖插件选择、账号文件哈希和密码变更。
- 认证插件变更后的回归结果：`go test ./... -count=1 -timeout 10m` 全仓通过；集群报告 `reports/compatibility/p1-cluster-current-continuation766/cluster-report.json` 为 `PASS`。
- 发布候选报告 `reports/compatibility/release-candidate-current-continuation766/release-candidate.json` 为 `GO`：clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，Connector/J `138` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；更多认证插件/多因素状态机、游标完整边界、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍需继续推进。

## Continuation 769

- 执行完整 P0 交付候选套件：P0-B 状态快照/row-page-WAL diff、P0-C consistency/range/lock evidence、P0-D metrics endpoint/export/logging/alert、P0-E timed rollback/full-chain、最终全仓回归和 evidence bundle 均通过；候选报告为 `reports/p0-delivery-current-continuation769/p0_delivery_candidate_20260908_222947.json`。
- 修复 P0 交付验证器在 PowerShell 7 下的真实缺陷：`ConvertFrom-Json` 将 ISO 时间反序列化为本地化 `DateTime` 时，验证器不再直接比较文化相关字符串，而是解析原始 ISO header 后比较时间点；P0-B 及相关报告 schema verifier 同步兼容日期值。
- 当前候选底层证据全部 PASS，但 delivery readiness 仍为 `NOT_READY`，原因是外部治理事项：审批决策仍为 `HOLD`、17 项 owner sign-off 未完成、7 个 P0 风险开放且 7 个风险负责人仍为 `TBD`；这些不能由代码自动代签或关闭。

## Continuation 770

- 扩展 `SHOW WARNINGS` 观察接口，支持 MySQL 常见的 `LIMIT row_count`、`LIMIT offset,row_count` 和 `LIMIT row_count OFFSET offset`，并在 session warning 为空/offset 超界时返回正确空集。
- 新增直接执行器和真实 `ExecuteQuery` 回归，确认 `SHOW WARNINGS` 分页不会改变 warning 状态，也不会影响普通语句开始新 warning context 的既有行为。
- 本项仍未宣称完整 warning 矩阵：`SHOW ERRORS` 错误历史、多语句/多结果集 warning 协商、全部 sql_mode 转换告警和完整客户端边界仍需继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 771

- 补齐 warning 计数观察接口：`SHOW COUNT(*) WARNINGS` 与 `SHOW COUNT(*) ERRORS` 现在返回当前 session 的 `@@session.warning_count`/`@@session.error_count`，并与 `SHOW ERRORS` 的 Error 级 warning 过滤保持一致。
- 修复计数查询的 session 边界：`SHOW COUNT(*) ...` 不会在读取前清空上一个语句的 warning/error 状态；新增真实 `ExecuteQuery` 回归覆盖 warning 和 error 两类计数。
- 本项仍未宣称完整错误历史语义：执行失败错误的持久化历史、多语句/多结果集 warning 协商、全部 sql_mode 转换告警和完整客户端边界仍需继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 772

- `SHOW ERRORS` 现在与 `SHOW WARNINGS` 对齐支持 `LIMIT row_count`、`LIMIT offset,row_count` 和 `LIMIT row_count OFFSET offset`，并只对 Error 级记录分页；新增真实 `ExecuteQuery` 回归覆盖错误记录读取。
- warning/error 观察接口的当前实现已覆盖 session 状态、分页和计数三类常见 Connector/J/运维查询；执行失败错误历史、多语句/多结果集协商、完整 sql_mode 告警矩阵仍需继续推进。

## Continuation 773

- warning/error 兼容变更后的完整回归通过：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `90.419s`、manager `10.720s`、net `2.757s`），集群报告 `reports/compatibility/p1-cluster-current-continuation773/cluster-report.json` 为 `PASS`。
- 发布候选报告 `reports/compatibility/release-candidate-current-continuation773/release-candidate.json` 为 `GO`：clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，Connector/J `138` tests、0 failures、0 errors、0 skipped；JDBC 性能阶段耗时约 9 分钟，已完整结束并释放隔离实例。
- 当前仍未宣称全量 MySQL 8.4：执行失败错误历史、多语句/多结果集完整客户端协商、更多认证插件/多因素状态机、完整游标边界、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍需继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

- 扩展 `JSON_VALUE(... RETURNING ...)` 回归覆盖：`RETURNING UNSIGNED`、`DECIMAL(8,2)` 和 `CHAR(3)` 均通过真实引擎 SQL，确认 raw 兼容入口复用统一 `CAST/CONVERT` 类型转换实现。
- `git diff --check` 本批次文件无空白错误；上一轮完整门禁 `reports/compatibility/release-candidate-current-continuation758/release-candidate.json` 已为 `GO`，本次新增仅为回归覆盖。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`JSON_VALUE` 的 `ON EMPTY/ON ERROR` 完整策略、复杂查询尾部组合、完整类型/函数/collation 矩阵、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界。

## Continuation 758

- 继续收敛 P4 JSON 函数矩阵：新增严格限定的 `JSON_VALUE(... RETURNING SIGNED|UNSIGNED|DECIMAL|CHAR|DATE|DATETIME|TIME|JSON)` raw compatibility 入口，将 parser 尚不接受的 `RETURNING` 语法安全映射到已有 `CAST` 执行语义；复杂查询尾部不被误改写，普通两参数 `JSON_VALUE` 路径保持不变。
- 新增 raw 解析/执行回归，并通过真实引擎 SQL 验证 `RETURNING UNSIGNED`；该入口同时接入 `XMySQLExecutor` 和 `XMySQLEngine` 两套执行链，避免 JDBC/引擎外层先行解析导致绕过兼容逻辑。
- `go test ./... -count=1 -timeout 10m` 全仓通过（engine `89.141s`、manager `9.533s`、plan `0.998s`、replication `1.520s`）；集群报告 `reports/compatibility/p1-cluster-current-continuation757/cluster-report.json` 为 `PASS`。
- 首次 release candidate 运行曾因 engine integration 阶段偶发 goroutine/生命周期抖动产生 `NO-GO`；独立 `go test ./server/innodb/engine -count=1 -timeout 3m` 明确 `EXIT=0`，随后完整门禁复跑 `reports/compatibility/release-candidate-current-continuation758/release-candidate.json` 为 `GO`，全部 Go 阶段、3 次 crash-recovery、100 次并发和 observability 均 PASS，Connector/J `136` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；完整类型/函数/collation 矩阵、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 739

- 存储过程执行新增简单 CASE 与搜索 CASE，支持 `WHEN`/`THEN`/`ELSE`/`END CASE` 的真实分支执行；新增 `FETCH [NEXT] [FROM] cursor INTO ...` 语法兼容，简写形式保持不变。
- 复制管理继续补齐 `RESET REPLICA ALL`/`RESET SLAVE ALL` 的实际清理语义：停止线程后同时删除本地副本状态与持久化 source 地址；普通 RESET 仍保留 source 配置。
- 本轮新增能力已通过对应 engine/replication 专项及全仓 Go 回归；集群报告继续为 PASS。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；完整 native binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍继续推进。

## Continuation 740

- 本轮继续收敛 P0/P1 协议与运维路径：`COM_PROCESS_KILL` 现在按连接 ID 定位活动会话，补齐建立连接时的 `ConnectionID` 登记、同账号目标终止、跨账号 `PROCESS`/`SUPER` 权限校验、未知线程 `1094` 和权限拒绝 `1095` 错误契约，并返回 OK 包。
- SQL 管理路径新增 `KILL CONNECTION <thread_id>`，复用网络层会话控制回调；`KILL QUERY` 因当前执行器尚未暴露可安全中断的单语句取消句柄，保持明确不支持，不伪装成连接终止。
- 新增 `server/net` 协议回归和执行器 admin 委托回归；本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 89.098s、manager 10.664s、net 2.842s、replication 1.432s），`reports/compatibility/p1-cluster-current-continuation740/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议、`KILL QUERY` 取消语义和外部租约/fencing 仍是明确边界。

## Continuation 741

- 本轮继续收敛 P0/P1 会话运维：活动查询现在登记独立的 `context.CancelFunc`，按连接 ID提供 `KILL QUERY` 取消；并发查询不再共享旧的全局执行 context，查询结束后会按 token 清理登记。
- SQL `KILL QUERY <thread_id>` 已接入网络层会话归属与 `PROCESS`/`SUPER` 权限校验，再调用引擎的活动查询取消原语；目标连接空闲时按成功 no-op 处理，未知连接和非法 thread id 保持错误路径。
- 新增执行器 context 取消回归及 `KILL` admin 委托回归；本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 91.776s、manager 12.766s、net 3.075s、replication 2.420s），`reports/compatibility/p1-cluster-current-continuation741/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议和外部租约/fencing 仍是明确边界；复杂非协作算子取消和完整 KILL/管理权限矩阵仍需后续专项。

## Continuation 742

- 本轮继续收敛 P0/P2 运维可观测性：`SHOW PROCESSLIST` 与 `INFORMATION_SCHEMA.PROCESSLIST` 新增活动会话 provider，真实 handler 会向引擎注入线程快照；查询入口登记 `processlist_query`，从而返回同一 server 上的全部活动连接及其当前 SQL，旧的无 provider 单会话回退保持不变。
- 新增多会话 PROCESSLIST 回归，覆盖连接 ID、用户、Host、Sleep/Query 状态和当前 SQL；本轮 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 89.095s、manager 11.905s、net 2.771s、protocol 2.202s、replication 1.799s），`reports/compatibility/p1-cluster-current-continuation742/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议和外部租约/fencing 仍是明确边界；PROCESSLIST 的完整时间/行计数、完整权限过滤和复杂管理权限矩阵仍需后续专项。

## Continuation 743

- 本轮补齐 PROCESSLIST 的权限边界：认证成功后将静态/动态全局权限写入连接上下文；拥有 `PROCESS`、`SUPER` 或 `ALL` 的账号可查看全部线程，普通账号只能查看同账号线程，当前内部/测试会话即使缺少用户名也不会被错误过滤。
- 活动 SQL 在 `ExecuteQuery`/`ExecuteWithQuery` 生命周期内登记到 `processlist_query`，结束后恢复原值；新增隐私过滤与旧锁等待观测回归，并修复 provider 引入的无用户当前会话空结果回归。
- 本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 90.535s、manager 10.182s、net 2.398s、protocol 1.267s、replication 1.699s），`reports/compatibility/p1-cluster-current-continuation743/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议和外部租约/fencing 仍是明确边界；PROCESSLIST 的完整时间/行计数与复杂管理权限矩阵仍需后续专项。

## Continuation 744

- 本轮补齐 PROCESSLIST 的运行时字段：活动查询登记开始时间，快照计算 `Time`；会话可提供 `processlist_rows_sent` 与 `processlist_rows_examined` 时同步返回计数，Sleep/Query 状态和当前 SQL 保持一致。
- 新增 PROCESSLIST 耗时与计数回归；此前 provider 引入的当前会话过滤回归已修复并由锁等待观测测试覆盖。本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 90.182s、manager 10.694s、net 2.295s、protocol 1.118s、replication 1.508s），`reports/compatibility/p1-cluster-current-continuation744/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议和外部租约/fencing 仍是明确边界；PROCESSLIST 的完整历史时间采样、精确行计数来源和复杂管理权限矩阵仍需后续专项。

## Continuation 745

- 本轮补齐 PROCESSLIST 的基础条件过滤：支持 `ID`、`USER`、`HOST`、`DB`、`COMMAND`、`STATE`、`INFO` 的简单 `=`/`LIKE` 条件及顶层 `AND` 组合；无法安全解析的复杂表达式保守保留结果，避免误过滤线程。
- 新增 `ID = ...` 与 `INFO LIKE ...` 回归；本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 89.242s、manager 10.979s、net 2.359s、protocol 0.608s、replication 1.710s），`reports/compatibility/p1-cluster-current-continuation745/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议和外部租约/fencing 仍是明确边界；PROCESSLIST 的历史采样、精确计数来源、复杂 WHERE 语法和完整管理权限矩阵仍需后续专项。

## Continuation 746

- 本轮继续收敛 PROCESSLIST 条件语义：新增 `<>`/`!=`、`NOT LIKE`、`IS NULL`/`IS NOT NULL`、`IN`/`NOT IN` 以及顶层 `OR` + `AND` 组合；无法安全解析的复杂谓词仍保守保留结果，不把管理视图误过滤成空集。
- 新增比较、集合、NULL 和析取条件回归；本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 88.949s、manager 10.318s、net 2.189s、protocol 1.290s、replication 3.499s），`reports/compatibility/p1-cluster-current-continuation746/cluster-report.json` 为 PASS。
- 当前版本仍不宣称完整 MySQL 8.4 兼容；FULLTEXT、非 Connector/J 全量客户端、完整 ALTER/在线 DDL、完整 INFORMATION_SCHEMA/PERFORMANCE_SCHEMA、通用递归/相关子查询 AST、完整存储对象执行、完整类型/函数/collation 矩阵、物理分区表空间、原生 binlog/GTID 全量协议和外部租约/fencing 仍是明确边界；PROCESSLIST 的历史采样、精确计数来源、复杂表达式解析和完整管理权限矩阵仍需后续专项。

## Continuation 747

- 本轮刷新 Connector/J 发布门禁：`reports/compatibility/release-candidate-current-continuation746/release-candidate.json` 为 `GO`，build、unit、integration、go-core、3 次 crash-recovery、并发、observability 和 JDBC 全部 PASS；Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。
- 该门禁对应当前工作树 revision `0e9a387682575d9f0df4825888c5064fb23d0c8b`；本轮没有执行提交或部署。

## Continuation 748

- 本轮继续收敛 Performance Schema：`performance_schema.threads` 改为复用实时会话 provider，不再固定返回单个样例线程；返回真实连接 ID、`thread/sql/one_connection`、数据库、命令和锁等待状态，并沿用 `PROCESS`/同账号可见性规则。
- 新增多会话 Performance Schema threads 与锁等待回归；本轮 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 89.417s、manager 11.209s、net 2.257s、protocol 1.045s、replication 1.373s），`reports/compatibility/p1-cluster-current-continuation748/cluster-report.json` 为 PASS。
- Connector/J 最新发布门禁仍为 `reports/compatibility/release-candidate-current-continuation746/release-candidate.json` 的 GO（136 tests、0 failures、0 errors、0 skipped）；本轮 P_S 改动之后未重复执行 9 分钟级 JDBC 门禁。

## Continuation 750

- 本轮继续收敛 PROCESSLIST 管理查询：支持 `ID =/!=/<> CONNECTION_ID()`，按当前连接真实 ID 求值，覆盖常见的“排除当前连接”查询模式。
- `performance_schema.threads` 的实时会话 provider、动态连接 ID 过滤和锁等待路径回归均通过；本轮最终 `go test ./... -count=1 -timeout 10m` 全仓通过（engine 89.727s、manager 13.519s、net 3.479s、protocol 1.282s、replication 1.654s），`reports/compatibility/p1-cluster-current-continuation749/cluster-report.json` 为 PASS。
- 当前工作树最新 Connector/J 发布门禁 `reports/compatibility/release-candidate-current-continuation749/release-candidate.json` 为 GO；build、unit、integration、go-core、3 次 crash-recovery、并发、observability 和 JDBC 全部 PASS，Connector/J `test_count=136`、`failures=0`、`errors=0`、`skipped=0`。

## Continuation 738

- 存储过程 handler 现在支持 `DECLARE ... HANDLER FOR` 多条件列表，条件别名可正确区分 SQLSTATE 与 MySQL errno；handler 选择按错误码、SQLSTATE、通用条件的优先级匹配，并保留活动条件供 `GET DIAGNOSTICS`/`RESIGNAL` 使用。
- 游标 `FETCH ... INTO` 现在持有查询列数，拒绝目标数量不匹配、未声明局部变量和用户变量误用；新增多条件、条件别名、handler 优先级和 FETCH 边界回归。
- 复制运维新增 `RESET REPLICA ALL`/`RESET SLAVE ALL`：停止复制线程后同时清理副本 GTID/应用状态、source position、错误状态及持久化源地址，要求后续重新配置 CHANGE SOURCE；普通 RESET REPLICA 保持原有保留 source 配置的语义。
- 本轮验证：存储对象专项通过；`go test ./... -count=1 -timeout 10m` 全仓通过（engine 90.061s、manager 18.266s、replication 1.900s）；`reports/compatibility/p1-cluster-current-continuation738/cluster-report.json` 为 PASS。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 仍继续推进。

## Continuation 722

- 继续收敛 P1 复制诊断：`SHOW BINLOG EVENTS` 现在解析并执行 `IN 'log_name'`、`FROM position` 和 `LIMIT row_count`/`LIMIT offset,row_count`，不再固定从 position 4 返回全量逻辑事件。
- 新增 `TestShowBinlogEventsHonorsInFromAndLimit`，覆盖带日志名、起始位置和分页限制的真实 source 查询；engine 定向回归通过。
- continuation721 的 blocking `COM_BINLOG_DUMP` 改动已通过 net/replication 专项和集群 smoke；发布候选报告仍待当前批次稳定后重新取得有效输出。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 723

- 继续收敛 P1 复制诊断：`SHOW BINARY LOGS` 的 `File_size` 现在读取 source 的持久化 JSONL binlog 实际字节数，不再误用逻辑 position；增加 `BinlogWriter.FileSize`/`Source.FileSize` 生命周期接口。
- 扩展 `TestShowBinaryLogsAndBinlogEventsUseConfiguredSource`，确认真实 source 的文件大小为正；binlog 查询与复制专项回归通过。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 715

- 收口 P0-B prepared statement 并发生命周期：`PreparedStatementManager.Get` 在更新 `LastUsedAt`/`ExecuteCount` 时改用写锁，避免连接池或并发请求下在读锁内写状态产生数据竞争。
- 新增并发 Get 回归，验证同一 prepared statement 的并发执行计数和最后使用时间稳定更新；普通并发专项、协议/网络/dispatcher 专项和全仓 Go 均通过。
- `go test ./... -count=1 -timeout 10m` 通过，其中 engine `86.987s`、manager `9.883s`、net `2.175s`、protocol `0.700s`、replication `2.576s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation715/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation715/release-candidate.json` 为 `GO`，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- `go test -race` 在当前 Windows 环境因 `CGO_ENABLED=0` 无法启动，未计入通过证据；已保留普通并发回归结果，后续具备 CGO 环境时仍需补跑 race 门禁。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 716

- 继续收口 P1 原生复制初始化：解耦网络处理器现在支持 `COM_REGISTER_SLAVE`，严格解析副本 server-id、report-host、report-user、report-password 长度字段和 report-port，并返回标准 OK；密码只用于边界校验，不写入 session 或日志。
- 新增 `TestRegisterSlaveStoresSafeRegistrationMetadata` 与 `TestRegisterSlaveRejectsTruncatedPacket`，验证注册元数据、敏感字段不落盘和截断包 ERR；内部 HTTP source/replica 复制路径保持不变。
- `go test ./server/net ./server/replication -count=1` 通过；全仓 `go test ./... -count=1 -timeout 10m` 通过，其中 engine `87.702s`、manager `10.798s`、net `1.963s`、protocol `0.924s`、replication `2.580s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation716/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation716/release-candidate.json` 为 `GO`，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 717

- 继续收口 P1 原生复制可观测性：`COM_REGISTER_SLAVE` 注册的副本现在进入监听器级线程安全注册表，支持 `SHOW REPLICAS` 与 `SHOW SLAVE HOSTS` 返回 `Server_id`、`Host`、`Port`、`Master_id`、`Slave_UUID` 五列；结果按 server-id 稳定排序。
- 注册表只保存非敏感元数据；同一 server-id 的新连接会替换旧连接，旧连接关闭时不会误删新注册；连接关闭、异常和 `COM_QUIT` 均清理所属注册。
- 新增 replication registry、engine SHOW、net registration/cleanup 回归测试；`go test ./server/replication ./server/innodb/engine ./server/net ./server/innodb/sqlparser -count=1` 通过，其中 engine `83.446s`，net `1.102s`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 718

- 继续收口 P1 原生复制请求校验：`COM_BINLOG_DUMP` 现在要求 command、position、flags、replica server-id 四类固定字段完整；`COM_BINLOG_DUMP_GTID` 在进入 source 流之前校验文件名长度、position 和 GTID 数据块前缀，截断或未知命令统一返回 ERR。
- 新增 `TestBinlogDumpRejectsTruncatedLegacyRequest` 与 `TestBinlogDumpRejectsTruncatedGTIDRequest`；net/replication 专项和全仓 `go test ./... -count=1 -timeout 10m` 通过，其中 engine `93.690s`、manager `11.849s`、net `2.036s`、protocol `0.675s`、replication `1.394s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation718/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation718/release-candidate.json` 为 `GO`，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 719

- 继续收口 P1 原生 binlog wire：发送给 `COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` 客户端的原生 event 现在在 19 字节 event header 和 event body 后追加 CRC32 checksum，并同步更新 `event_size`；JSONL/GTID 内部复制路径不变。
- 新增 `TestNativeBinlogEventsCarryCRC32Checksum`，验证 checksum 覆盖 event header+body（不含尾部 checksum）且 header 长度一致；net/replication 专项通过。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 720

- 继续收口 P1 原生 binlog 初始流：从 position 4 开始的 `COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` 现在先发送最小合法 `FORMAT_DESCRIPTION_EVENT`，声明 19 字节 event header 以及 QUERY/TABLE_MAP/row/GTID 等事件的 post-header 长度；非初始 position 不重复发送。
- `FORMAT_DESCRIPTION_EVENT` 与后续原生 event 共用 CRC32 尾校验，source server-id 来自真实 binlog writer；内部 JSONL/HTTP 复制不改变。
- 原生 net/replication 专项通过，既有 native event stream 数量和顺序回归同步覆盖 FDE；继续保留 schema-aware 完整 row codec、checksum negotiation、原生文件互操作和复杂复制语义边界。
- FDE 变更后的全仓 `go test ./... -count=1 -timeout 10m` 通过，其中 engine `87.988s`、manager `10.885s`、net `2.233s`、replication `3.029s`；集群报告 `reports/compatibility/p1-cluster-current-continuation720/cluster-report.json` 为 `PASS`。
- 发布报告 `reports/compatibility/release-candidate-current-continuation720/release-candidate.json` 与当前 revision `0e9a387682575d9f0df4825888c5064fb23d0c8b` 一致，为 `GO`；build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，JDBC `136/136`、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 714

- 继续补齐 P0-B/P1 协议能力协商：握手包现在同时声明 `CLIENT_MULTI_RESULTS` 与 `CLIENT_PS_MULTI_RESULTS`，支持 prepared statement server-side cursor 的客户端不会因缺少 prepared multi-results 能力被错误降级。
- 新增握手能力回归，验证多结果和 prepared 多结果标志均在高 16 位正确编码；协议与网络专项、全仓 Go 均通过。
- 第二次全仓 `go test ./... -count=1 -timeout 10m` 通过，其中 engine `87.391s`、manager `10.866s`、net `2.172s`、protocol `0.922s`、replication `0.827s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation714/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation714/release-candidate.json` 为 `GO`，JDBC `test_count=136`、failures=0、errors=0、skipped=0，完整性能套件构建成功。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 713

- 继续补齐 P0-B Connector/J prepared protocol：`COM_STMT_PREPARE` 与参数绑定现在使用 MySQL 引号/注释感知的占位符扫描器，字符串字面量、反引号标识符以及 `#`/`--`/`/*...*/` 注释中的 `?` 不再被误计数或替换。
- `COM_STMT_EXECUTE` 二进制参数解析现在尊重 unsigned 类型标志，并支持 DATE、DATETIME、TIMESTAMP、TIME 及 fractional seconds 的常见 wire 形态；字符串回绑补齐反斜杠、NUL、换行、回车和 Ctrl-Z 转义。
- 新增 prepared 参数失败先行回归：占位符边界、反斜杠字符串、unsigned LONG/LONGLONG、DATE/DATETIME binary 参数；协议与网络专项、全仓 Go 均通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过，其中 engine `87.562s`、manager `20.652s`、net `2.390s`、protocol `1.465s`、replication `1.736s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation713/cluster-report.json` 为 `PASS`；Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation713/release-candidate.json` 为 `GO`，JDBC `test_count=136`、failures=0、errors=0、skipped=0，完整性能套件用时约 9 分 35 秒。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 709

- 补齐会话级 `LAST_INSERT_ID` 兼容链路：系统变量引擎和默认 SQL 路由器现在识别 `LAST_INSERT_ID()`/`LAST_INSERT_ID(expr)`；无参形式读取当前会话值，带参形式写入并返回新的会话值。
- InnoDB DML 返回非零自增 ID 后写回会话状态，使后续 `SELECT LAST_INSERT_ID()` 与 Connector/J 获取到同一连接级结果；没有新自增 ID 的 UPDATE/DELETE 不会错误清零历史值。
- `COM_RESET_CONNECTION` 与成功的 `COM_CHANGE_USER` 清理 `last_insert_id`，同时保留已有事务、prepared statement、多语句能力和认证状态重置行为；新增 dispatcher、engine、network 回归测试。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.714s`、manager `10.749s`、net `2.041s`、protocol `0.705s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation709/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation709/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0；JDBC 性能套件约 9 分钟后正常完成。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 710

- 补齐结果状态函数 `ROW_COUNT()`：默认路由和系统变量引擎现在识别该函数；DML 完成后将 affected rows 写入会话，SELECT 按 MySQL 语义记录 `-1`，DDL/SET/USE 记录 `0`，查询错误不会覆盖既有状态。
- `LAST_INSERT_ID` 与 `ROW_COUNT` 的写回统一进入 InnoDB dispatcher 执行结果链路；`COM_RESET_CONNECTION` 和成功的 `COM_CHANGE_USER` 同时清理两项连接级结果状态，避免连接池复用旧值。
- 新增 `TestSystemVariableEngine_RowCountReadsSessionValue`、`TestPersistSessionExecutionStateFromDMLResult`、`TestPersistSessionExecutionStateUsesMySQLSelectRowCount` 及路由/重置回归。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.034s`、manager `10.022s`、net `1.925s`、protocol `0.767s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation710/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation710/release-candidate.json` 为 `GO`；全部检查通过，Connector/J `test_count=136`、failures=0、errors=0、skipped=0，性能套件约 9 分钟后正常完成。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 711

- 收口 server-side cursor 的边界语义：`COM_STMT_FETCH` 的 `row_count=0` 不再被错误转换为 1；现在返回空批次、不推进游标，并在结果已耗尽时返回 `LAST_ROW_SENT`，避免客户端请求零行时意外消费数据。
- 新增 `TestPreparedStatementManager_ZeroRowFetchDoesNotAdvanceCursor`，覆盖零行 FETCH 后继续读取首行；协议和网络 cursor 回归均通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.799s`、manager `10.305s`、net `1.909s`、protocol `0.927s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation711/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation711/release-candidate.json` 为 `GO`；全部检查通过，Connector/J `test_count=136`、failures=0、errors=0、skipped=0，性能套件约 9 分钟后正常完成。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 712

- 补齐 P0 协议心跳：解耦网络处理器现在真正处理已认证连接的 `COM_PING`，按请求包序号返回标准 OK，不再把连接池心跳误报为未支持命令。
- 新增 `TestHandlePacketPingAcknowledgesAuthenticatedSession`，覆盖已认证会话、响应序号和 OK 包类型；网络专项回归通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.751s`、manager `9.890s`、net `2.147s`、protocol `0.786s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation712/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation712/release-candidate.json` 为 `GO`；全部检查通过，Connector/J `test_count=136`、failures=0、errors=0、skipped=0，性能套件约 9 分钟后正常完成。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 705

- 收口 COM_QUERY 多语句能力协商：多条顶层语句现在必须先通过握手 `CLIENT_MULTI_STATEMENTS` 能力，或由 `COM_SET_OPTION ON` 显式启用；未协商或 `COM_SET_OPTION OFF` 后只返回标准 ERR，不再执行已拆分语句。
- 新增 `TestHandleQueryRejectsMultiStatementsWhenDisabled` 与 `TestHandleQueryAllowsNegotiatedMultiStatementsAndSetOptionOverride`，覆盖默认拒绝、能力协商放行及显式关闭优先级；单语句路径和多结果状态传播保持不变。
- `go test ./server/net -run 'TestHandle(SetOptionTracksMultiStatements|QueryRejectsMultiStatementsWhenDisabled|QueryAllowsNegotiatedMultiStatementsAndSetOptionOverride)' -count=1` 通过；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `89.362s`、manager `13.521s`、net `2.311s`、protocol `1.083s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation705/cluster-report.json` 为 `PASS`（`2026-09-08T15:03:57.2816990Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation705/release-candidate.json` 为 `GO`（`2026-09-08T15:18:13.0572955Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`COM_CHANGE_USER` 等需要完整重认证/权限状态机的命令、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 707

- 实现协议层 `COM_CHANGE_USER` 的最小可用重认证闭环：解析新用户、secure-connection 认证数据、数据库、字符集和可选插件字段；复用现有账户/TLS/密码校验，认证成功后切换当前用户、库和 active roles，并返回正确响应序号的 OK；认证失败或截断包返回 ERR。
- `COM_CHANGE_USER` 成功路径复用连接状态清理，关闭旧 prepared statements、事务、用户变量/锁等 session state，并恢复多语句能力到握手基线；不把复杂 AuthSwitch/多因素流程伪装成成功。
- 新增 `TestHandleComChangeUserReauthenticatesAndResetsSessionState` 与 `TestHandleComChangeUserRejectsTruncatedPacket`；网络、认证、dispatcher 专项通过；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.086s`、manager `10.201s`、net `2.047s`、protocol `0.712s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation707/cluster-report.json` 为 `PASS`（`2026-09-08T15:42:31.3631098Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation707/release-candidate.json` 为 `GO`（`2026-09-08T15:55:34.8358098Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`COM_CHANGE_USER` 的 AuthSwitch/多因素完整状态机、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 708

- 收口 `COM_CHANGE_USER` 的 AuthSwitch 状态机：已认证连接在等待 `caching_sha2_password` AuthSwitchResponse 时，网络处理器会优先消费 pending 认证状态，不把认证数据误解析成普通命令；fast-auth、TLS cleartext 和 RSA key-exchange 分支共用现有校验逻辑。
- AuthSwitch 成功完成后复用连接清理路径，清除旧 prepared statement/事务/session state，再写入新用户、数据库和 active roles，并按客户端响应包序号返回 OK；新增 `TestChangeUserCachingSHA2UsesAuthSwitchAndResetsSession` 覆盖真实 pending 状态机和状态清理。
- `go test ./server/net -count=1` 与 `go test ./... -count=1 -timeout 10m` 全部通过，其中 engine `87.453s`、manager `10.561s`、net `2.088s`、protocol `0.682s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation708/cluster-report.json` 为 `PASS`（`2026-09-08T16:00:24.8103622Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation708/release-candidate.json` 为 `GO`（`2026-09-08T16:14:03.3560531Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：AuthSwitch 多因素/更多认证插件矩阵、完整游标边界语义、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界；FULLTEXT 与非 Connector/J 全量客户端继续按用户要求后置。

## Continuation 706

- 补齐连接池复用场景的 `COM_RESET_CONNECTION` 会话状态：reset 后清理 prepared statement、事务和结果集状态的同时，将 `client_multi_statements` 恢复到握手阶段 `CLIENT_MULTI_STATEMENTS` 基线，避免前一用户的 `COM_SET_OPTION OFF` 泄漏到下一用户。
- 新增回归断言，覆盖 reset 前关闭多语句、存在 `__more_results__` 状态，reset 后能力恢复且结果集状态清零；已有 prepared statement、autocommit 和数据库重置行为保持通过。
- `go test ./server/net -run 'TestHandlePacketResetConnectionClearsPreparedState|TestHandle(SetOptionTracksMultiStatements|QueryRejectsMultiStatementsWhenDisabled|QueryAllowsNegotiatedMultiStatementsAndSetOptionOverride)' -count=1` 通过；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `86.986s`、manager `10.324s`、net `2.356s`、protocol `0.921s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation706/cluster-report.json` 为 `PASS`（`2026-09-08T15:23:26.2267116Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation706/release-candidate.json` 为 `GO`（`2026-09-08T15:36:41.2791372Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`COM_CHANGE_USER` 等需要完整重认证/权限状态机的命令、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 702

- 补齐协议层 `COM_SET_OPTION`：解耦网络处理器现在解析 `MYSQL_OPTION_MULTI_STATEMENTS_ON/OFF`，将状态写入连接会话并返回标准 OK 包；未知选项和截断包返回明确错误，不再统一返回 unsupported。
- 新增 `TestHandleSetOptionTracksMultiStatements`，覆盖开关状态、包级 OK 响应和非法输入保护；数据库管理命令及前序协议回归保持通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.954s`、manager `10.922s`、net `1.948s`、protocol `0.685s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation702/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation702/release-candidate.json` 为 `GO`（`2026-09-08T14:13:04Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 701

- 补齐协议层数据库管理命令：`COM_CREATE_DB` 与 `COM_DROP_DB` 现在解析二进制命令中的数据库名，复用带权限检查的 `CREATE DATABASE`/`DROP DATABASE` DDL 路径，并返回标准 OK/错误包，不再统一返回 unsupported。
- 新增 `TestHandleDatabaseCommandDispatchesCreateAndDrop`，覆盖命令到 SQL 的转换、数据库名校验以及 OK 包返回；此前协议元数据/进程命令回归保持通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `89.030s`、manager `11.673s`、net `1.879s`、protocol `0.856s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation701/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation701/release-candidate.json` 为 `GO`（`2026-09-08T13:55:01Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 700

- 补齐协议层 `COM_PROCESS_INFO`：解耦网络处理器现在把该命令路由到带权限检查的 `SHOW PROCESSLIST`，并按标准文本结果集发送进程列表，不再直接返回 unsupported。
- 新增 `TestHandleProcessInfoReturnsProcessListResultSet`，覆盖列数包、8 个字段定义、EOF、数据行和最终 EOF 的完整包序列；`COM_FIELD_LIST` 与 `COM_STATISTICS` 回归保持通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `88.211s`、manager `11.436s`、net `2.394s`、protocol `0.773s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation700/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation700/release-candidate.json` 为 `GO`（`2026-09-08T13:36:23Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 699

- 补齐协议层 `COM_STATISTICS`：解耦网络处理器现在返回 MySQL 兼容的统计文本包（Uptime/Threads/Questions/Slow queries/Opens/Flush tables/Open tables/QPS），不再把命令直接判为 unsupported；详细运行指标仍由现有 metrics 端点提供。
- 新增 `TestHandleStatisticsReturnsTextStatisticsPacket`，覆盖包序号和关键统计字段；`COM_FIELD_LIST` 回归保持通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `89.066s`、manager `10.649s`、net `2.220s`、protocol `0.694s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation699/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation699/release-candidate.json` 为 `GO`（`2026-09-08T13:18:42Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 698

- 补齐协议层 `COM_FIELD_LIST`：解耦网络处理器现在解析表名/通配符，复用带权限检查的 `SHOW FULL COLUMNS` 元数据路径，并按 MySQL 协议发送每个字段的 `ColumnDefinition` 包及 EOF 终止包，不再把该命令直接返回为 unsupported。
- 新增 `TestHandleFieldListReturnsColumnDefinitionPackets`，覆盖字段定义包数量、字段包与 EOF 的包级边界；全量 net 回归通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `88.447s`、manager `13.067s`、net `2.522s`、protocol `0.828s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation698/cluster-report.json` 为 `PASS`（`2026-09-08T12:46:47Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation698/release-candidate.json` 为 `GO`（`2026-09-08T13:01:20Z`）；build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 693

- 继续收口 P1-OPT-001：INNER/LEFT/RIGHT 等值 JOIN 的单列常量 OR 谓词现在可安全做传递推导，例如 `a.id = 1 OR a.id = 2` 会在匹配的另一侧生成对应 OR 过滤；仅接受同一列且叶子属于已有安全常量比较/`IN`/`LIKE`/`BETWEEN`/NULL 形状，跨列和非确定性表达式保持不下推。
- 新增 `TestPredicatePushdownInfersSingleColumnOrPredicateAcrossOuterJoin`，验证 OR 两个分支都替换到 nullable-side JOIN key；原有外连接 NULL 语义和结构化谓词回归继续通过。
- engine/plan 专项与全仓 Go 回归通过；本轮全仓 engine `87.331s`、manager `10.506s`、plan `0.639s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation693/cluster-report.json` 为 `PASS`（`2026-09-08T10:58:54.847Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation693/release-candidate.json` 为 `GO`（`2026-09-08T11:13:50.266Z`）；build/unit/integration/go-core、3 次 crash-recovery、100 次并发、observability 和 JDBC 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 692

- 继续收口 P1-OPT-005/P1-EXE-004 的 JSON 表达式矩阵：计划表达式执行器新增 `JSON_MERGE_PRESERVE()`，保留重复对象键并按 MySQL 语义合并为数组，同时拼接数组；兼容已废弃但仍被客户端使用的 `JSON_MERGE()` 别名。
- 编译表达式白名单同步覆盖两个函数，避免热点投影/过滤路径因函数未列入编译器而退回不一致路径；新增解释执行和编译执行回归，覆盖重复键、数组追加和别名。
- `go test ./... -count=1 -timeout 10m` 全仓通过，engine `89.046s`、plan `0.615s`、manager `18.996s`；本轮未重新执行集群与 Connector/J 门禁，上一轮 continuation691 的集群 `PASS` 与 Connector/J `GO` 仍是最近有效发布门禁证据。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 683

- 继续收口 P1-SEC-001 的列级授权执行：普通 `SELECT` 现在解析物理表来源、投影列以及 `WHERE`/`ON`/`ORDER BY` 等表达式引用；存在列级 `SELECT` 授权时，未授权列不能通过投影、过滤或 `*` 访问，角色继承的列授权也会参与判断。
- `INSERT` 的显式列列表/全列写入、`INSERT ... ON DUPLICATE KEY UPDATE` 的更新列以及单表 `UPDATE` assignment 现在支持列级 `INSERT`/`UPDATE` 授权；无列级授权的既有表级权限路径保持兼容。
- 新增 `TestColumnSelectPrivilegeRestrictsProjectionAndPredicateColumns` 与 `TestColumnDMLPrivilegesRestrictInsertAndUpdateColumns`；一次全量回归发现存储函数旧测试只授予 `EXECUTE` 的兼容边界，随后调整为仅在目标表存在列级授权时启用细粒度收紧。
- engine 全量通过（`81.709s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.987s`、manager `10.101s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation683/cluster-report.json` 为 `PASS`（`2026-09-08T07:58:07Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation683/release-candidate.json` 为 `GO`（`2026-09-08T08:10:48Z`）；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 682

- 继续收口临时表权限：`CREATE TEMPORARY TABLE` raw 路径现在要求目标数据库上的 `CREATE TEMPORARY TABLES`；内部物理临时表创建使用受控会话标记绕过持久表 `CREATE` 校验，创建完成后仅当前会话可按临时表语义自由执行 DML/DDL。
- 新增 `TestTemporaryTableRequiresCreateTemporaryTablesPrivilege`，覆盖未授权拒绝、授权创建以及创建后插入成功。
- 修复一次回归误判：临时表物理名识别只对当前会话真实拥有的 `__xmysql_tmp_` 表放行，不再把普通持久表当作临时表。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `82.036s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.707s`、manager `9.731s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation682/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation682/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 681

- 对齐官方 MySQL 的 `ALTER TABLE` 权限语义：结构化和 raw 变体现在同时要求目标表 `ALTER`、`CREATE`、`INSERT`，而不再只校验 `ALTER`；`RENAME TABLE` 的独立源/目标权限规则保持不变。
- 扩展 `TestTableDDLRequiresTablePrivileges` 与 `TestRawAlterRequiresAlterPrivilege`，覆盖缺少 `CREATE`、缺少 `INSERT` 时的拒绝，以及三项权限齐备后的成功。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `81.573s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `86.466s`、manager `9.842s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation681/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation681/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 680

- 继续收口 P1-SEC-001 的表重命名权限：`RENAME TABLE` 现在逐对校验源表 `ALTER`+`DROP` 与目标表 `CREATE`+`INSERT`，所有权限检查通过后才进入原子重命名流程。
- 新增 `TestRenameTableRequiresSourceAndDestinationPrivileges`，覆盖源表权限缺失、目标表权限缺失和补齐全部权限后的真实重命名。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `81.781s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.792s`、manager `10.004s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation680/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation680/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 679

- 继续收口 P1-SEC-001 的 DDL 旁路：raw `ALTER TABLE` 变体（COMMENT/CHARSET/ROW_FORMAT 等）现在统一要求目标表 `ALTER`；多表 `DROP TABLE` 在任何删除动作前先校验每个目标表的 `DROP`，避免权限不足时部分删除。
- 新增 `TestRawAlterRequiresAlterPrivilege` 与 `TestMultiTableDropRequiresDropPrivilegeForEveryTable`，覆盖 raw ALTER 拒绝/授权成功，以及多表 DROP 的逐目标权限和无部分删除。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `82.246s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.947s`、manager `10.032s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation679/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation679/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 678

- 继续收口 P1-SEC-001 的冲突 DML 权限：`INSERT ... ON DUPLICATE KEY UPDATE` 现在额外要求目标表 `UPDATE`；`REPLACE` 现在额外要求目标表 `DELETE`，避免仅有 `INSERT` 权限时隐式改写/删除已有行。
- 新增 `TestConflictDMLRequiresAdditionalPrivileges`，覆盖冲突更新与替换在缺少附加权限时拒绝、补齐权限后成功。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `82.483s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.690s`、manager `9.913s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation678/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation678/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 677

- 继续收口 P1-SEC-001 的基础 DML 权限：engine 直接执行路径的 `INSERT`、`UPDATE`、`DELETE` 现在分别要求目标表上的对应权限；单目标 JOIN DML 在读取/物化目标行前也执行同一目标权限校验。
- 新增 `TestDMLRequiresTablePrivileges`，覆盖未授权拒绝、逐项授权后的真实插入/更新/删除。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `81.859s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.722s`、manager `9.806s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation677/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation677/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 676

- 继续收口 P1-SEC-001 的普通表 DDL 权限：`CREATE TABLE` 需要目标数据库上的 `CREATE`，`ALTER TABLE` 需要目标表 `ALTER`，`DROP TABLE` 需要目标表 `DROP`；临时表仍由临时表专用路径处理。
- 新增 `TestTableDDLRequiresTablePrivileges`，覆盖未授权建表/改表/删表拒绝，以及分别授予 `CREATE`、`ALTER`、`DROP` 后真实执行成功。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `81.585s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `86.189s`、manager `9.987s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation676/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation676/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 675

- 继续收口 P1-SEC-001 的维护与锁表权限：`CHECK TABLE` 需要目标表至少存在一个实际权限；`ANALYZE TABLE`/`OPTIMIZE TABLE` 按 MySQL 规则要求 `SELECT` 与 `INSERT`；`LOCK TABLES` 同时要求 `LOCK TABLES` 与目标表 `SELECT`，并保留 `UNLOCK TABLES` 的会话清理行为。
- 新增 `TestTableMaintenanceRequiresMySQLPrivileges` 与 `TestLockTablesRequiresLockAndSelectPrivileges`，覆盖无权限拒绝、逐项授权后的真实执行，以及锁表授权缺失时的拒绝。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `91.145s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `86.148s`、manager `10.275s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation675/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation675/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 674

- 继续收口 P1-SEC-001 的数据库/表 DDL 权限：直接执行路径的 `CREATE DATABASE`/`DROP DATABASE` 分别要求 `CREATE`/`DROP`，`TRUNCATE TABLE` 要求目标表 `DROP`；临时表所有者、root/bootstrap 和已有 dispatcher 权限路径保持兼容。
- 新增 `TestDatabaseDDLRequiresDatabasePrivileges` 与 `TestTruncateRequiresDropPrivilege`，覆盖未授权拒绝、授权后真实创建/删除/清空。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `81.611s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `84.943s`、manager `13.553s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation674/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation674/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 673

- 继续收口信息模式结果契约：`INFORMATION_SCHEMA.SCHEMATA` 现在按 SELECT 请求列投影，不再始终返回固定五列；`SELECT SCHEMA_NAME ...` 与 Connector/J 元数据列形状保持一致。
- `TestInformationSchemaTableMetadataRequiresTablePrivilege` 同时覆盖库可见性和单列 `SCHEMA_NAME` 投影；数据库、表、列、索引权限过滤回归保持通过。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `80.827s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `84.517s`、manager `10.491s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation673/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation673/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 672

- 继续收口数据库级元数据入口：`SHOW CREATE DATABASE/SCHEMA` 现在要求当前账号对目标数据库具有可见权限；无权限账号返回明确 privilege 错误，授予目标表或数据库权限后恢复查询。
- `TestInformationSchemaTableMetadataRequiresTablePrivilege` 扩展覆盖 `SHOW CREATE DATABASE` 的拒绝/授权路径；此前新增的 `SHOW DATABASES`、信息模式和 `SHOW TABLE STATUS` 权限过滤回归保持通过。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `81.337s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `84.912s`、manager `10.861s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation672/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation672/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 671

- 继续收口 P1-SEC-001 的数据库级元数据权限：`SHOW DATABASES` 与 `SHOW TABLE STATUS` 现在复用库/表可见性判断；无权限账号不能枚举数据库名称或表状态，授予目标表权限后恢复对应结果。
- `TestInformationSchemaTableMetadataRequiresTablePrivilege` 扩展覆盖 `SHOW DATABASES LIKE` 的无权限/授权结果；`TestShowTablesHidesObjectsWithoutTablePrivilege` 扩展覆盖 `SHOW TABLE STATUS` 的无权限空集与授权后单表结果。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `80.202s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.389s`、manager `10.445s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation671/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation671/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 670

- 继续收口 P1-SEC-001 的信息模式权限边界：`INFORMATION_SCHEMA.SCHEMATA`、`TABLES`、`COLUMNS`、`STATISTICS` 以及约束相关视图现在按当前账号的实际库/表权限过滤；无权限账号不能通过信息模式绕过 `SHOW TABLES`、`SHOW COLUMNS`、`SHOW INDEX` 的可见性规则。
- 新增 `TestInformationSchemaTableMetadataRequiresTablePrivilege`，覆盖无权限下的库、表、列、索引元数据空集，以及授予 `SELECT` 后恢复可见；临时表、root/bootstrap 和已有 JDBC 元数据路径保持兼容。
- `go test ./server/innodb/engine -count=1 -timeout 10m` 通过（engine `80.216s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `84.114s`、manager `10.205s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation670/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation670/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 669

- 继续收口 P1-SEC-001 的列/索引元数据权限：`SHOW COLUMNS`/`SHOW FIELDS` 与 `SHOW INDEX`/`SHOW KEYS` 现在复用表级对象可见性检查；无权限账号对持久化表返回空元数据，临时表仍保留所属会话可见性。
- `TestShowTablesHidesObjectsWithoutTablePrivilege` 扩展覆盖无权限下的 `SHOW COLUMNS` 与 `SHOW INDEX` 空结果，同时保持授权后的表元数据与 `SHOW CREATE TABLE` 回归路径。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.890s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation669/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation669/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 664

- 继续收口 PX-SQL-002/P1-SEC 的存储对象权限：事件现在统一校验 `EVENT` 权限，覆盖 `CREATE EVENT`、`ALTER EVENT`、`DROP EVENT`、`SHOW EVENTS`、`SHOW CREATE EVENT`；`INFORMATION_SCHEMA.EVENTS` 对无权限数据库隐藏事件行。
- 触发器同步统一校验 `TRIGGER` 权限，覆盖 `CREATE TRIGGER`、`DROP TRIGGER`、`SHOW TRIGGERS`、`SHOW CREATE TRIGGER`；`INFORMATION_SCHEMA.TRIGGERS` 对无权限数据库隐藏触发器行。新增未授权/授权后的真实 DDL、SHOW 和元数据回归测试。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.259s`；`git diff --check` 对本轮触及源文件无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation664/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation664/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 665

- 继续收口 P1-SEC-001 的 routine DDL 权限：`CREATE PROCEDURE/FUNCTION` 现在要求目标库的 `CREATE ROUTINE`；`ALTER` 和 `DROP PROCEDURE/FUNCTION` 要求 `ALTER ROUTINE`，普通账号不能仅凭登录身份改写或删除存储程序。
- 新增 `TestRoutineDDLRequiresRoutinePrivileges`，覆盖无权限拒绝、仅 CREATE ROUTINE 不能 ALTER/DROP、授予 ALTER ROUTINE 后完整 DDL 成功；routine 执行权限与 `SHOW_ROUTINE` 展示权限保持独立。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `83.853s`；`git diff --check` 对本轮触及文件无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation665/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation665/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 666

- 继续收口 P1-SEC-001 的视图元数据权限：`SHOW CREATE VIEW` 现在要求目标库的 `SHOW VIEW`；`INFORMATION_SCHEMA.VIEWS` 按库过滤，无权限账号不再读取视图定义元数据，授予 `SHOW VIEW` 后恢复可见。
- 新增 `TestViewMetadataRequiresShowViewPrivilege`，覆盖无权限拒绝/隐藏和授权后 `SHOW CREATE`/信息模式查询成功；root/bootstrap 和 Connector/J 元数据路径保持兼容。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `83.887s`；`git diff --check` 对本轮触及文件无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation666/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation666/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 667

- 继续收口 P1-SEC-001 的表元数据可见性：`SHOW TABLES` 与 `SHOW FULL TABLES` 现在只返回当前账号在目标表/视图上拥有实际权限的对象；无权限对象被隐藏，授予 `SELECT` 等对象权限后恢复可见。
- 新增 `TestShowTablesHidesObjectsWithoutTablePrivilege`，覆盖普通表的无权限空集、授权后表类型结果，以及 `SHOW FULL TABLES` 的同步行为。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `84.347s`；`git diff --check` 对本轮触及文件无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation667/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation667/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 668

- 继续收口 P1-SEC-001 的表定义元数据权限：`SHOW CREATE TABLE` 现在复用表级对象可见性检查；临时表仍允许所属会话访问，持久化表需要目标表上的实际权限（如 `SELECT`）。
- `TestShowTablesHidesObjectsWithoutTablePrivilege` 扩展覆盖无权限/授权后的 `SHOW CREATE TABLE`，并保持 `SHOW TABLES`/`SHOW FULL TABLES` 过滤回归。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `85.078s`；`git diff --check` 对本轮触及文件无空白错误。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation668/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation668/release-candidate.json` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 661

- 继续收口 PX-SQL-001 的 routine 元数据权限：`SHOW PROCEDURE STATUS`、`SHOW FUNCTION STATUS` 现在复用 `SHOW CREATE PROCEDURE/FUNCTION` 的 `SHOW_ROUTINE` 动态权限校验；未授权用户不能读取 routine status。
- `INFORMATION_SCHEMA.ROUTINES` 与 `INFORMATION_SCHEMA.PARAMETERS` 的特殊查询分派也加入同一权限校验，防止绕过 `SHOW_ROUTINE` 读取 routine 定义和参数元数据。
- 新增 `TestShowRoutineStatusRequiresShowRoutinePrivilege`，覆盖无权限失败、授权后 `SHOW`/`INFORMATION_SCHEMA` 四类查询成功；engine 与全仓 Go 回归均通过。
- 首次 Connector/J 门禁报告 `reports/compatibility/release-candidate-current-continuation661/release-candidate.json` 为 `NO-GO`，根因是 clean JDBC 数据目录中 bootstrap `root@localhost` 没有持久化账户，新权限校验将其误判为不存在；该报告保留作为失败证据，不视为实现完成。

## Continuation 662

- 对 661 根因进行修复尝试后重新执行完整门禁；由于首次补丁落在 routine 执行权限函数而非 routine 展示权限函数，`reports/compatibility/release-candidate-current-continuation662/release-candidate.json` 仍为 `NO-GO`，同样明确记录为修复过程中的失败证据。
- 该错误补丁随后已撤回，避免改变普通 routine `EXECUTE` 权限语义。

## Continuation 663

- 正确修复 bootstrap root 边界：仅在 `checkStoredRoutineShow` 中允许 isolated JDBC server 尚未物化系统账户时的 `root` metadata discovery；普通用户仍必须拥有全局 `SHOW_ROUTINE`，routine 执行权限检查未放宽。
- routine 权限专项回归通过；集群报告 `reports/compatibility/p1-cluster-current-continuation663/cluster-report.json` 为 `PASS`。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation663/release-candidate.json` 于 `2026-09-08T01:15Z` 为 `GO`；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，JDBC `test_count=136`、failures/errors/skips 均为 0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 684

- 继续收口列级授权的 JOIN DML 旁路：`UPDATE ... JOIN` 目标 assignment 现在复用列级 `UPDATE` 检查，JOIN 来源列则由物化 SELECT 的列访问检查覆盖。
- 新增 `TestColumnUpdatePrivilegeAppliesToUpdateJoin`，覆盖仅授予目标更新列、授予来源表 SELECT 后的真实 JOIN 更新。
- engine 全量通过（`82.342s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `86.632s`、manager `9.528s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation684/cluster-report.json` 为 `PASS`（`2026-09-08T08:15:29`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation684/release-candidate.json` 为 `GO`（`2026-09-08T08:28:09Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 685

- 补齐普通索引 DDL 入口：新增独立 `CREATE INDEX`/`DROP INDEX` raw compatibility path，在 parser 之前翻译到既有持久化 ALTER-index 实现，保留索引元数据刷新、重建和外键校验；支持常见唯一索引、限定库名和反引号标识符。
- 独立索引 DDL 按 MySQL `INDEX` 表权限校验，不再把 `DROP INDEX` 误套到 `ALTER`+`CREATE`+`INSERT` 组合权限；新增 `TestStandaloneIndexDDLRequiresIndexPrivilege` 覆盖拒绝、授权创建、撤权删除拒绝和再次授权删除。
- engine 全量通过（`83.809s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `86.114s`、manager `9.912s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation685/cluster-report.json` 为 `PASS`（`2026-09-08T08:35:54`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation685/release-candidate.json` 为 `GO`（`2026-09-08T08:48:53Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 686

- 继续收口列级 DML 权限：`UPDATE` 在仅拥有目标列 `UPDATE` 权限时，还会检查赋值表达式、`WHERE` 和 `ORDER BY` 读取的来源列是否拥有 `SELECT` 权限；`UPDATE ... JOIN` 保留来源表列访问校验，并对内部生成的 CASE 更新路径设置受控旁路，避免把实现细节错误当成用户读取。
- 扩展 `TestColumnDMLPrivilegesRestrictInsertAndUpdateColumns`，覆盖 UPDATE 条件读取列的拒绝与授权后成功；此前的列级 SELECT、INSERT、UPDATE、UPDATE JOIN 测试继续通过。
- engine 全量通过（`83.748s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.084s`、manager `10.812s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation686/cluster-report.json` 为 `PASS`（`2026-09-08T08:55:11` 左右）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation686/release-candidate.json` 为 `GO`（`2026-09-08T09:09:21.658Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 687

- 补齐普通索引 DDL 的幂等语法：独立 `DROP INDEX IF EXISTS index_name ON table_name` 现在在索引不存在时成功返回，并在索引存在时正常删除；原有 `INDEX` 表权限检查、库名限定、反引号标识符和实际索引重建/元数据路径保持不变。
- 新增 `TestStandaloneDropIndexIfExistsIsIdempotent`，覆盖缺失索引、真实索引删除和重复删除三种路径；失败先行测试曾稳定复现 legacy parser 在 `IF` 处的语法错误，修复后专项通过。
- engine 全量通过（`84.045s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.154s`、manager `9.653s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation687/cluster-report.json` 为 `PASS`（`2026-09-08T09:15:40.579Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation687/release-candidate.json` 为 `GO`（`2026-09-08T09:28:20.753Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 688

- 补齐普通索引 DDL 的另一条幂等语法：独立 `CREATE INDEX IF NOT EXISTS index_name ON table_name (...)` 现在在索引不存在时创建，重复执行时成功返回且不重复写入索引元数据；普通/唯一索引、库名限定、反引号标识符和 `INDEX` 表权限路径保持不变。
- 新增 `TestStandaloneCreateIndexIfNotExistsIsIdempotent`，通过 `SHOW INDEX` 真实结果确认重复执行后目标索引只出现一次；与 `DROP INDEX IF EXISTS`、普通索引权限回归共同覆盖索引迁移的幂等闭环。
- engine 全量通过（`83.594s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.610s`、manager `10.006s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation688/cluster-report.json` 为 `PASS`（`2026-09-08T09:34:12.918Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation688/release-candidate.json` 为 `GO`（`2026-09-08T09:46:50.722Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 689

- 补齐 P1-SQL-003 的复合带库名 ALTER：`ALTER TABLE db.table ADD ..., ADD CONSTRAINT/INDEX ...` 现在先解析目标库和目标表，再在目标库逐个执行子句；后续子句失败仍恢复目标表原始 `.frm`，不会误用当前会话库或把限定名交给 legacy parser。
- 新增 `TestCompoundAlterQualifiedNameUsesTargetDatabase`，覆盖当前库与目标库不同的带库名复合 ALTER，并验证列元数据和唯一索引均落在目标库；同时保留单操作带库名 ALTER 回归。
- engine 全量通过（`83.146s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.023s`、manager `10.182s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation689/cluster-report.json` 为 `PASS`（`2026-09-08T09:53:43.157Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation689/release-candidate.json` 为 `GO`（`2026-09-08T10:06:46.147Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 690

- 继续收口 P1-SQL-003 的限定对象语义：带库名的 `ALTER TABLE db.table COMMENT/CHARSET/CONVERT TO CHARSET/COLLATE/ROW_FORMAT/AUTO_INCREMENT` 现在统一解析目标库和目标表；权限检查、DDL 锁键、`.frm` 更新和自增状态写入均不再错误使用当前会话库。
- 扩展 `TestQualifiedAlterTableOptionsUseTargetDatabase`，覆盖当前库与目标库不同的 `ALTER TABLE app.qualified_options COMMENT`；复合 ALTER 和列/索引元数据回归继续通过。
- engine 全量通过（`84.560s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.432s`、manager `9.865s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation690/cluster-report.json` 为 `PASS`（`2026-09-08T10:12:24.863Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation690/release-candidate.json` 为 `GO`（`2026-09-08T10:25:26.638Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 691

- 继续收口 P1-SQL-003 的带库名约束 DDL：`ALTER TABLE db.table ADD/DROP CHECK`、FOREIGN KEY、PRIMARY KEY、列默认值等 raw 兼容分支现在统一通过目标库/表规范化进入既有元数据处理器；同一目标库也用于权限检查和 DDL 锁协调，避免当前库与目标库不一致时误报 `unsupported ALTER TABLE action` 或写错 `.frm`。
- 新增 `TestQualifiedAlterConstraintUsesTargetDatabase`，覆盖当前库为 `other`、目标表在 `app` 的 CHECK 添加与真实非法写入拒绝；复合 ALTER、表选项和列/索引回归继续通过。
- engine 全量通过（`84.556s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.439s`、manager `10.333s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation691/cluster-report.json` 为 `PASS`（`2026-09-08T10:31:28.275Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation691/release-candidate.json` 为 `GO`（`2026-09-08T10:44:35.252Z`）；JDBC `test_count=136` 且 `PASS`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 694

- 继续收口 P1-SQL-003/P1-IDX-001 的索引可见性：实现 `ALTER TABLE ... ALTER INDEX idx VISIBLE|INVISIBLE` 和 `ADD INDEX ... VISIBLE|INVISIBLE`，可见性写入 `.frm` 元数据，并禁止修改 PRIMARY 的可见性。
- `SHOW INDEX` 的 `Visible`、`INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE` 现在读取持久化状态；历史元数据没有可见性字段时按 MySQL 默认值 `YES` 处理。
- 二级索引管理器和选择器传播/消费可见性状态：不可见二级索引不再成为 equality、range、OR、prefix、skip-scan 等访问路径候选；DML 仍维护索引，重新设为 VISIBLE 后恢复使用。
- 新增 `TestAlterIndexVisibilityIsParsedAndPersisted`、`TestAlterIndexVisibilityPersistsInShowIndexAndInformationSchema`，覆盖解析、持久化、展示、恢复可见和新增不可见索引。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `89.935s`、manager `10.484s`、plan `1.010s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation694/cluster-report.json` 为 `PASS`（`2026-09-08T11:23:38Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation694/release-candidate.json` 为 `GO`（`2026-09-08T11:36:55Z`）；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 695

- 补齐独立索引 DDL 的可见性入口：`CREATE [UNIQUE] INDEX ... VISIBLE|INVISIBLE` 现在进入既有持久化索引路径，实际写入索引可见性元数据；普通 `CREATE INDEX` 的默认状态仍为可见。
- 新增 `TestStandaloneCreateIndexVisibilityIsPersisted`，并通过失败先行定位了 standalone DDL 绕过通用 ALTER 处理器的问题，修复后确认 `.frm` 与 `SHOW INDEX.Visible` 均为 `NO`。
- 独立索引入口与 `ALTER TABLE ... ADD INDEX ... VISIBLE|INVISIBLE` 共用索引重建/刷新和权限路径；此前 `ALTER INDEX ... VISIBLE|INVISIBLE`、`INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE` 和优化器候选过滤保持通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `88.499s`、manager `10.230s`、plan `0.982s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation695/cluster-report.json` 为 `PASS`（`2026-09-08T11:43:03Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation695/release-candidate.json` 为 `GO`（`2026-09-08T11:58:26Z`）；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 696

- 补齐 `CREATE TABLE` 内普通索引可见性：`INDEX/KEY ... VISIBLE|INVISIBLE` 现在由 raw 兼容层剥离 parser 不认识的尾选项，复用正常建表流程后将不可见状态写入 `.frm`；显式 `VISIBLE` 和默认状态均保持可见。
- 新增 `TestCreateTableIndexVisibilityIsPersisted`，失败先行确认 legacy parser 原先在 `INVISIBLE` 处报语法错，修复后真实建表与 `SHOW INDEX.Visible=NO` 通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `88.481s`、manager `10.612s`、plan `1.041s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation696/cluster-report.json` 为 `PASS`（`2026-09-08T12:03:22Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation696/release-candidate.json` 为 `GO`（`2026-09-08T12:18:25Z`）；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 697

- 收口索引可见性展示闭环：`SHOW CREATE TABLE` 现在从持久化索引元数据恢复不可见普通索引，并输出 `INVISIBLE`；默认/显式 `VISIBLE` 不改变现有输出。
- 扩展 `TestAlterIndexVisibilityPersistsInShowIndexAndInformationSchema`，覆盖 `SHOW INDEX.Visible`、`SHOW CREATE TABLE`、`INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE` 三个观察面。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `88.002s`、manager `9.730s`、plan `0.997s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation697/cluster-report.json` 为 `PASS`（`2026-09-08T12:22:54Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation697/release-candidate.json` 为 `GO`（`2026-09-08T12:36:01Z`）；clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 `PASS`，Connector/J `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 704

- 补齐协议层 `COM_REFRESH`：解耦网络处理器现在接受合法刷新标志并返回标准 OK 包；当前服务没有 MySQL legacy query-cache/table-cache 刷新对象，因此该命令明确作为无状态协议确认，不伪造缓存刷新副作用；截断包返回标准 ERR。
- 新增 `TestHandleRefreshAcknowledgesValidCommand`，覆盖合法包响应和截断输入保护；此前 `COM_SET_OPTION`、数据库管理、进程信息、统计信息和字段列表命令回归保持通过。
- `go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `87.409s`、manager `10.753s`、plan `1.011s`、net `2.168s`、protocol `0.651s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation704/cluster-report.json` 为 `PASS`（`2026-09-08T14:42:55.954Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation704/release-candidate.json` 为 `GO`（`2026-09-08T14:55:51Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；`COM_CHANGE_USER` 等需要完整重认证/权限状态机的命令、原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 703

- 继续收口 P1-OPT-005/P1-EXE-004 的谓词规范化：常量列表 `col NOT IN (v1, v2, ...)` 现在安全展开为 `col != v1 AND col != v2 ...`；包含 `NULL` 时通过 SQL 三值逻辑保持原始 `NOT IN` 结果，不把空/动态列表误改写。
- 索引访问优化器同步递归提取规范化后的 `AND` 条件，并允许 `!=` 条件下推；原始 `NOT IN` 直接索引候选路径保持兼容，规范化后仍能得到同列的可下推条件。
- 新增 `TestPredicateNormalizationExpandsConstantNotInWithoutChangingThreeValuedSemantics` 与 `TestNormalizedNotInPredicatesUseIndexPushdown`；plan 专项和全仓 `go test ./... -count=1 -timeout 10m` 均通过，其中 engine `88.101s`、manager `10.791s`、net `2.209s`、protocol `0.766s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation703/cluster-report.json` 为 `PASS`（`2026-09-08T14:24:21.668Z`）。
- Connector/J 发布候选报告 `reports/compatibility/release-candidate-current-continuation703/release-candidate.json` 为 `GO`（`2026-09-08T14:37:11.535Z`）；全部检查通过，JDBC `test_count=136`、failures=0、errors=0、skipped=0。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 724

- 当前批次验证收口：`go test ./server/innodb/engine -count=1 -timeout 10m` 通过（`241.165s`）；`go test ./... -count=1 -timeout 10m` 通过，engine `217.076s`、manager `19.555s`、net `2.891s`、replication `1.503s`；`scripts/compatibility/cluster_smoke.ps1 -ReportDir reports/compatibility/p1-cluster-current-continuation723` 通过。
- Connector/J 发布候选脚本已启动隔离 server 并完成 JDBC 进程，但 Maven wrapper 收尾阶段未退出，未生成 `release-candidate.json`，因此本批次不宣称新的 JDBC GO；最近有效的 continuation720 报告仍为 JDBC `136/136`、0 failures/errors/skips，且本轮改动未触及 JDBC 路径。
- 本批次实现已完成的复制增强包括 blocking/non-blocking native dump、GTID 过滤后的 position 推进、SHOW BINLOG EVENTS 的 `IN/FROM/LIMIT`、SHOW BINARY LOGS 的真实持久化文件大小；下一轮继续处理剩余 native file interoperability、完整 MDL/online DDL、外部 fencing 和复杂 AST 边界。

## Continuation 725

- 修正 P1 内部复制拉流的 position 契约：`/replication/binlog` 的 `next_position` 现在返回最后事件 position 后的下一个 position，副本下一轮不会从同一事件重新读取。
- 新增 `TestSourceBinlogResponseAdvancesPastLastEvent`，通过真实 HTTP handler 验证三事件事务返回 `last_position+1`；replication 专项通过。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 726

- 修正 P1 复制并发一致性：`BinlogWriter.ReadFrom` 现在与 `AppendTransaction`/`Rotate` 共用 writer 互斥锁，避免读端在 JSONL 事务追加过程中读取半行并产生 checksum/JSON 解码失败。
- 新增 `TestBinlogWriterReadFromSerializesWithConcurrentAppend`，覆盖并发追加与多轮读取；replication 专项通过。
- 之前的全仓 Go 与集群 smoke 证据仍有效到本轮 replication-only 改动前；当前新增变更需在下一轮完整门禁中重新确认。

## Continuation 727

- 修正 P1 主库状态位置语义：新增 `BinlogWriter.NextPosition`/`Source.NextPosition`，`SHOW MASTER STATUS`/`SHOW SOURCE STATUS` 现在返回下一条事件的 position；空日志仍从 4 开始，已写入事件后返回最后事件 position+1。
- 扩展 `TestShowMasterStatusExposesSourcePositionAndGTIDs`，验证状态 position 与 source 事务末尾位置的关系；复制、主库状态和 binlog 查询定向回归通过。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 728

- 收口 P1 复制 HTTP 输入校验：`/replication/binlog?position=...` 对非数字 position 不再静默降级为 4，而是返回 HTTP 400；缺省或显式 0 仍按空日志起始 position 4 处理。
- 新增 `TestSourceBinlogRejectsInvalidPosition`，并保持正常 position 推进与 GTID 过滤回归通过。

## Continuation 729

- 修正 P1 GTID 过滤拉流推进：新增 `Source.DumpWithGTIDPosition`，HTTP source endpoint 根据原始事件而不是过滤后事件计算 `next_position`；当本批全部 GTID 已执行时也能前进，不会无限重复扫描。
- 新增 `TestSourceBinlogAdvancesWhenAllEventsAreGTIDFiltered`，验证两笔事务全部过滤后返回空事件但 `next_position=11`；replication 专项通过。

## Continuation 730

- 统一 P1 内存复制 position 语义：`Replica.ReplicateFrom` 现在也返回已应用事件之后的下一个 position，与 HTTP source endpoint 和 native dump 保持一致，避免手动/测试拉流重复读取最后一笔事件。
- 扩展 `TestSourceReplicaCommitExactlyOnceAcrossRestart`，验证首次复制返回最后事件 position+1；replication 全包通过。

## Continuation 731

- 收口 P1 复制运行时生命周期：`Runtime.Close` 现在取消并等待 replica poll loop 退出（含有限超时），source/standalone 不再为不存在的 poll loop 等待；避免关闭/重启/提升时后台请求残留。
- 新增 `TestReplicaRuntimeCloseWaitsForPollLoop`，验证关闭返回前副本 poll channel 已关闭；replication 全包通过。

## Continuation 732

- 收口 P1 大事务复制边界：`BinlogWriter.ReadFrom` 为 JSONL scanner 设置 16 MiB 受控单行上限，避免默认 64 KiB 限制导致大 SQL/row event 读取失败。
- 新增 `TestBinlogWriterReadsLargeTransactionRecord`，覆盖约 100 KiB statement 的持久化、读取和内容保持；replication 全包通过。

## Continuation 733

- 补齐 P1/P3 复制运维控制：新增 `START REPLICA`/`START SLAVE` 与 `STOP REPLICA`/`STOP SLAVE` SQL 路径，执行器通过回调控制副本 poll loop；重复启动/停止幂等，停止时等待后台拉流退出，HTTP 控制面保持可用。
- 新增 `CHANGE REPLICATION SOURCE TO SOURCE_HOST=..., SOURCE_PORT=...` 及 `CHANGE MASTER TO MASTER_HOST=..., MASTER_PORT=...` 的受限安全路径，当前仅接受内部 HTTP(S) source endpoint，不接受或持久化密码；source URL 写入 `replication/source.json`，副本重启后恢复。
- 新增 runtime/engine 失败先行与成功回归：副本线程启停、源地址持久化/重载、当前/legacy 语法路由、线程选项与缺失 host/凭据拒绝；专项测试通过。

## Continuation 734

- 继续补齐 P1/P3 复制运维控制：新增 `RESET REPLICA`/`RESET SLAVE` SQL 路径，要求副本 apply loop 已停止后才允许执行，并原子清理副本 GTID、已应用 change set、source position、最近错误和应用时间。
- `RESET REPLICA` 与 source URL 配置保持分离，不会误删复制源配置；新增 runtime/engine 成功与拒绝并发 reset 回归，专项测试通过。

## Continuation 735

- 修正复制状态可观测性：runtime 现在记录 replica poll loop 的真实运行状态，`SHOW REPLICA STATUS`/`SHOW SLAVE STATUS` 在 `STOP REPLICA` 后返回 `Replica_IO_Running=No`、`Replica_SQL_Running=No`，重新启动后恢复为 `Yes`。
- 新增启停状态回归；engine 与 replication 专项测试通过。
- 当前批次最终验证：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `87.930s`、manager `10.743s`、replication `1.472s`）；`reports/compatibility/p1-cluster-current-continuation735/cluster-report.json` 为 `PASS`；`git diff --check` 针对本批次文件无空白错误。

## Continuation 736

- 补齐 source 侧 binlog 运维：`FLUSH BINARY LOGS` 持久化 rotate marker，`RESET MASTER` 原子清空 source JSONL binlog、GTID 执行集合并回到 position 4；新增 writer/source/runtime 与 SQL 管理路径。
- `RESET MASTER` 不会影响副本状态；rotate/reset 后重新加载 source 仍保持空日志和 GTID 初始状态。专项回归通过。
- 当前批次最终验证：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `87.983s`、manager `9.891s`、replication `1.364s`）；`reports/compatibility/p1-cluster-current-continuation736/cluster-report.json` 为 `PASS`。

## Continuation 737

- 复制运维批次最终回归：在 source binlog 管理命令加入后，`go test ./... -count=1 -timeout 10m` 仍全仓通过（engine `87.983s`、manager `9.891s`、replication `1.364s`）；`reports/compatibility/p1-cluster-current-continuation736/cluster-report.json` 仍为 `PASS`。
- 当前仍不宣称总完成：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；完整 native binlog/GTID 物理文件互操作、完整 MDL/online DDL、生产级外部 fencing、复杂任意 AST 仍继续推进。

## Continuation 721

- 继续收敛 P1 原生复制流：`COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` 现在区分 `NON_BLOCK` 与 blocking 请求；前者保持一次性拉取，后者在当前日志没有新事务时持续等待后续提交，并在复制会话关闭后安全退出。
- blocking 轮询会推进已观察到的原始 binlog position，即使 GTID 过滤掉已执行事务也不会重复扫描同一批事件；新事务仍按原有 native event + CRC32/FDE wire 路径发送。
- 新增 `TestBinlogDumpBlockingWaitsForNewEventsUntilSessionCloses`，补齐 GTID 回归的非阻塞 flag；`go test ./server/net ./server/replication -count=1` 通过。
- 全仓、集群和 Connector/J 发布门禁需在本轮后续改动稳定后重新生成；上一轮 continuation720 的 GO 报告不覆盖本轮新增 blocking 行为。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。
## Continuation 753

- 继续收口 P4 类型/字符集边界：`CONVERT(expr USING charset)` 不再被降级为无字符集语义的 `CAST(... AS CHAR)`；计划表达式保留目标字符集，并对已支持的 UTF-8、latin1、latin2、ASCII、CP125x、GBK/GB2312、Big5、EUC-KR、SJIS 等映射执行实际转码；未知映射明确返回错误。
- 新增 `TestBuildExpressionPreservesConvertUsingCharset` 与 `TestCompiledConvertUsingPreservesCharsetSemantics`，验证 SQL AST→逻辑表达式、解释执行、编译执行及 latin1 字节结果。
- 定向 plan/engine 回归通过（plan `0.990s`，engine `85.848s`）；`go test ./... -count=1 -timeout 10m` 全仓通过，其中 engine `89.078s`、manager `9.544s`、net `2.438s`、protocol `0.671s`、replication `1.408s`。
- 集群报告 `reports/compatibility/p1-cluster-current-continuation753/cluster-report.json` 为 `PASS`。
- 本轮 release-candidate 脚本在 `go-core` 阶段出现 idle hang，未生成最终 JSON；同一核心包组合的独立带 `-timeout 10m` 重跑也出现相同挂起，已结束无进展进程。单独 `TestTransactionStatementsReturnOK` 在 `-timeout 30s -v` 下通过；因此本轮不宣称新的 JDBC GO，最近有效的 continuation752 仍为 Connector/J `136/136`、0 failures/errors/skips。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 754

- 重新完成本轮发布候选门禁，消除上一轮仅因长时间观察而未收口的验证缺口：`reports/compatibility/release-candidate-current-continuation754/release-candidate.json` 为 `GO`，clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability 全部 PASS，隔离 Connector/J 套件 `136` tests、0 failures、0 errors、0 skipped。
- 报告对应当前工作树 revision `0e9a387682575d9f0df4825888c5064fb23d0c8b`；集群 smoke `reports/compatibility/p1-cluster-current-continuation753/cluster-report.json` 为 `PASS`，全仓 Go 与本轮 `CONVERT USING` 专项回归均通过。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 774

- 继续收敛 P0/P1 客户端可观测性：失败语句现在会在执行器和集成引擎的结果转发边界写入当前会话错误历史，`SHOW ERRORS`、`SHOW ERRORS LIMIT/OFFSET` 与 `SHOW COUNT(*) ERRORS` 可读取真实失败结果；常见重复键、表/元数据不存在、校验和存储失败映射到稳定 MySQL 错误码，其余执行错误使用 1105 兼容码。
- 新增 Go 失败先行回归 `TestFailedStatementIsAvailableThroughShowErrors`，并新增 Connector/J `JdbcConnectionTest.testShowErrorsAfterFailedStatement`，覆盖失败 SQL、错误行、错误码和错误数量；本轮 Go 全仓、集群 smoke、发布候选均通过，Connector/J 为 139 tests、0 failures、0 errors、0 skipped，最终状态 `GO`。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 物理文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 775

- 继续收敛 `P0-JDBC-002` 错误协议：legacy `MySQLMessageHandler.handleQueryResults` 不再把所有执行错误固定编码为 1064/42000，改为复用统一 `protocol.ClassifyGoError`/`EncodeErrorFromGoError`，使旧结果通道与解耦协议路径对重复键、未知表、权限、死锁等错误保持一致的 errno/SQLSTATE。
- 新增 `TestMySQLMessageHandlerPreservesClassifiedQueryError`，验证 legacy handler 对未知表返回 1146/42S02；`go test ./server/net ./server/protocol -count=1` 通过。
- 本轮最终证据：`go test ./... -count=1 -timeout 10m` 通过；`reports/compatibility/p1-cluster-current-continuation775/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation775/release-candidate.json` 为 `GO`，包含 3 次 crash-recovery、并发、可观测性与 Connector/J 139 tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 物理文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 776

- 继续收敛 `P0-JDBC-002` prepared statement 错误协议：`COM_STMT_EXECUTE` 的服务端游标执行失败不再固定返回 1064/42000，而是复用统一 `protocol.ClassifyGoError` 映射；`protocol.ErrorMessage` 也保留原始 errno/SQLSTATE，避免 Connector/J 在 prepared/cursor 通道丢失未知表、重复键、权限和死锁语义。
- 新增失败先行回归 `TestHandleComStmtExecuteCursorPreservesClassifiedQueryError`，先稳定复现 1064，再验证未知表错误返回 1146/42S02；`go test ./server/net ./server/protocol -count=1` 与专项回归通过。
- 本轮最终证据：`go test ./... -count=1 -timeout 10m` 通过（engine 91.571s、manager 11.417s、net 2.587s）；`reports/compatibility/p1-cluster-current-continuation776/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation776/release-candidate.json` 为 `GO`，全部 9 个检查 PASS，包含 3 次 crash-recovery、并发、可观测性与 Connector/J 139 tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 物理文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 777

- 继续收口 `COM_CHANGE_USER` 的连接状态机：解析并校验协议携带的 2 字节 collation id，成功换用户后同步 `character_set_client`、`character_set_connection`、`character_set_results` 和 `collation_connection`；direct re-auth 与 `caching_sha2_password`/`sha256_password` AuthSwitch 两条路径共用同一状态应用逻辑，避免连接池复用旧用户字符集。
- 新增 `TestHandleComChangeUserAppliesRequestedCollation`，扩展 `TestChangeUserCachingSHA2UsesAuthSwitchAndResetsSession` 覆盖 direct/AuthSwitch 两条路径；专项 net/auth/protocol 回归通过。
- 本轮最终证据：`go test ./... -count=1 -timeout 10m` 通过（engine 90.699s、manager 10.615s、net 2.554s）；`reports/compatibility/p1-cluster-current-continuation777/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation777/release-candidate.json` 为 `GO`，全部检查 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 物理文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 778

- 继续收口 `COM_CHANGE_USER` 认证包解析：认证响应现在复用握手阶段的 `CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA` length-encoded 解析，同时兼容 `CLIENT_SECURE_CONNECTION` 单字节长度和旧式 NUL 终止格式；长认证响应不会再吞掉后续 database、collation 或 plugin 字段。
- 新增失败先行回归 `TestHandleComChangeUserReadsLengthEncodedAuthResponse`，用 252 字节 length-encoded auth response 先复现 database 错位，再验证 `new_db` 和后续字符集状态正常；direct/AuthSwitch、net/protocol/auth 专项均通过。
- 本轮最终证据：`go test ./... -count=1 -timeout 10m` 通过（engine 90.408s、manager 10.429s、net 2.645s）；`reports/compatibility/p1-cluster-current-continuation778/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation778/release-candidate.json` 为 `GO`，全部检查 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍未宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；原生 binlog/GTID 物理文件互操作、完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界，继续按剩余优先级推进。

## Continuation 780

- 继续补齐 P1 原生复制持久化：`BinlogWriter` 现在在保留 JSONL 恢复日志的同时维护 MySQL 格式的 `binlog.000001`，写入 binlog magic、Format Description、GTID、QUERY、XID/ROTATE 事件及 CRC32 校验；`SHOW BINARY LOGS` 的 `File_size` 改为返回 native 文件实际字节数。
- 增加 native 文件丢失重建路径：启动时依据 durable JSONL 重建 native 文件，`RESET MASTER` 同步重置两种日志；新增持久化、事件帧边界、重建回归测试。
- 本轮验证：`go test ./server/replication -count=1` 通过；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `90.486s`、manager `17.349s`、net `2.432s`、replication `4.344s`）；`reports/compatibility/p1-cluster-current-continuation779/cluster-report.json` 为 `PASS`。
- 最终发布门禁：`reports/compatibility/release-candidate-current-continuation780/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；native binlog/GTID 目前已具备物理文件基础互操作，但跨文件轮转/完整 GTID index 与上游全量行为仍需继续收敛；完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界。

## Continuation 781

- 继续补齐 native binlog 文件轮转：`FLUSH BINARY LOGS`/`Source.Rotate` 现在在当前文件写入 ROTATE 事件后创建下一个 `binlog.%06d` 文件并写入新的 magic/FDE，后续事务进入新文件；`SHOW BINARY LOGS` 改为按文件名和实际字节数列出全部 native 文件。
- 新增轮转与多文件管理面回归：验证 `binlog.000001`/`binlog.000002` 的事件序列和 `SHOW BINARY LOGS` 两条记录；保留现有 JSONL 逻辑流和重建路径。
- 本轮验证：`go test ./server/replication ./server/innodb/engine -run 'TestBinlogWriterRotateCreatesNextNativeBinlogFile|TestShowBinaryLogsListsRotatedNativeFiles' -count=1` 通过；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `90.391s`、manager `10.300s`、net `2.528s`、replication `2.415s`）；`reports/compatibility/p1-cluster-current-continuation781/cluster-report.json` 为 `PASS`。
- 最终发布门禁：`reports/compatibility/release-candidate-current-continuation781/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；native binlog/GTID 已具备基础文件和轮转互操作，但按文件过滤的完整 dump、GTID index/跨重启恢复和上游全量行为仍需继续收敛；完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界。

## Continuation 782

- 继续收口 native binlog 管理语义：`SHOW BINLOG EVENTS IN 'binlog.NNNNNN'` 现在依据 durable ROTATE 边界只返回目标文件的逻辑事件，不再把多个物理文件的事件串在一起；`FROM` 与 `LIMIT` 仍在筛选后的文件范围内生效。
- 新增 rotated-file 管理面回归，覆盖 `binlog.000002` 的事件隔离；原有 `SHOW BINLOG EVENTS IN/FROM/LIMIT` 回归继续通过。
- 本轮验证：`go test ./server/innodb/engine ./server/net ./server/protocol ./server/replication -count=1 -timeout 10m` 独立通过（engine `87.350s`、net `2.727s`、protocol `0.821s`、replication `4.085s`）；此前同一代码批次全仓 Go 通过，`reports/compatibility/p1-cluster-current-continuation782/cluster-report.json` 为 `PASS`。
- `reports/compatibility/release-candidate-current-continuation782/release-candidate.json` 的 JDBC `139` tests、0 failures/errors/skips，go-core/crash-recovery/concurrency/observability 均 PASS；整体状态为 `NO-GO` 的唯一原因是首个 integration 子命令 42s 超时，之后 go-core 重跑通过，独立 integration 重跑亦通过。该脚本时延波动需后续单独治理。
- 当前仍不宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；native binlog/GTID 已具备基础文件、轮转和 `SHOW BINLOG EVENTS IN` 文件筛选，但按文件的 native dump、GTID index/跨重启恢复和上游全量行为仍需继续收敛；完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界。

## Continuation 783

- 继续补齐原生复制协议的文件选择：`COM_BINLOG_DUMP` 与 `COM_BINLOG_DUMP_GTID` 现在解析请求中的 binlog filename，并通过 source 的 ROTATE 边界只发送目标 `binlog.NNNNNN` 文件事件；默认 filename、非阻塞行为和 GTID 过滤保持兼容。
- 新增协议层 rotated-file 回归，验证请求 `binlog.000002` 只返回 FDE、GTID、QUERY、XID，不包含前一文件事务。
- 本轮验证：`go test ./server/net -run 'TestBinlogDumpUsesRequestedRotatedFile|TestBinlogDumpStreamsCommittedEventsFromConfiguredSource|TestBinlogDumpGTIDFiltersCommittedTransaction' -count=1` 通过；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `91.689s`、manager `11.394s`、net `2.955s`、replication `1.964s`）；`reports/compatibility/p1-cluster-current-continuation783/cluster-report.json` 为 `PASS`。
- 最终发布门禁：`reports/compatibility/release-candidate-current-continuation783/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；native binlog/GTID 已具备基础文件、轮转、管理面筛选和协议 filename 选择，但按文件的真实 native event position、GTID index/跨重启恢复和上游全量行为仍需继续收敛；完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界。

## Continuation 784

- 继续修复门禁中暴露的并发缺陷：`ExecuteWithQuery` 的异步 worker 会在结果交付后执行 session 状态清理；engine 测试替身原先使用裸参数 map，造成并发读写崩溃。为使测试替身符合生产 session 的线程安全契约，给 `testMySQLSession.params` 增加 RWMutex，保留生产执行语义不变。
- 新增/保留异步事务回归，连续 10 次通过；engine 全套通过，随后全仓 Go 通过，未再出现 `concurrent map read and map write`。
- 本轮最终验证：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `90.806s`、manager `10.140s`、net `2.438s`、replication `2.218s`）；`reports/compatibility/p1-cluster-current-continuation784/cluster-report.json` 为 `PASS`。
- 最终发布门禁：`reports/compatibility/release-candidate-current-continuation784/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称全量 MySQL 8.4：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；native binlog/GTID 已具备基础文件、轮转、管理面筛选、协议 filename 选择及跨重启轮转重建，但按文件的真实 native event position、GTID index 和上游全量行为仍需继续收敛；完整 MDL/online DDL、生产级外部 fencing，以及复杂任意 AST 组合仍是后续边界。

## Continuation 785

- 继续收口 P1 native binlog 物理互操作：每个 `binlog.NNNNNN` 现在写入标准 `PREVIOUS_GTIDS_EVENT`，并维护 MySQL 工具可识别的 `binlog.index` 文件；轮转、`RESET MASTER` 和启动恢复都会同步维护文件列表与前置 GTID 集合。
- 增加物理 native frame 读取与校验接口，按真实 event start/end offset 暴露 CRC32 校验后的事件；`SHOW BINLOG EVENTS` 的 `Pos/End_log_pos` 和 `FROM` 过滤现在基于物理 frame 位置，内部 JSONL replica position 契约保持不变。
- 启动时按 durable JSONL 的 ROTATE 边界校验 native 文件数量、事件类型、CRC32 和文件序号；单个轮转文件丢失、全部 native 文件丢失或旧格式缺少 `PREVIOUS_GTIDS_EVENT` 时都会重建完整文件集，避免只恢复当前文件造成静默缺日志。
- 新增 `TestBinlogWriterMaintainsNativeIndexAndPreviousGTIDs`、`TestShowBinlogEventsUsesNativePhysicalPositions` 和 `TestBinlogWriterRebuildsMissingRotatedNativeFileAfterRestart`；replication+engine 联合回归和全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `91.604s`）。
- 最新集群证据：`reports/compatibility/p1-cluster-current-continuation785-final/cluster-report.json` 为 `PASS`；最新发布门禁 `reports/compatibility/release-candidate-current-continuation785-final/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入本轮；native row-event 的完整 schema-aware codec、GTID index 的上游全量语义、完整 MDL/online DDL、生产级外部 fencing、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍需继续实现和验证。

## Continuation 786

- 继续收口 P1 native binlog 物理互操作：`EventRow` 且含真实 `RowChange` 的事务现在在物理 `binlog.NNNNNN` 中写入 `TABLE_MAP_EVENT` + `WRITE_ROWS_EVENTv2`/`UPDATE_ROWS_EVENTv2`/`DELETE_ROWS_EVENTv2` + `XID_EVENT`；statement-only logical rows 仍保留标准 `QUERY_EVENT`，内部 JSONL recovery semantics 不变。
- native writer 的物理 frame parser 对每个 frame 执行长度与 CRC32 校验；启动重建、轮转、`binlog.index` 和 `PREVIOUS_GTIDS_EVENT` 复用同一文件校验契约。新增 `TestBinlogWriterPersistsSchemaAwareNativeRowEvents`，并通过 replication/net/engine 定向回归。
- 本轮验证：`go test ./server/replication ./server/net ./server/innodb/engine -count=1 -timeout 10m` 通过；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `89.984s`、manager `10.003s`、net `2.430s`、protocol `1.317s`、replication `2.165s`）。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation786/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation786/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入本轮；native row-event 仍未宣称完整上游 schema/metadata/flags、完整 native dump/PITR/GTID 语义；完整 MDL/online DDL、生产级外部 fencing、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍需继续实现和验证。

## Continuation 787

- 继续补齐 P1 表锁语义：`LOCK TABLES` 现在支持同一语句中的多个表和 `READ`/`READ LOCAL`/`WRITE` 模式，按全限定表名排序获取进程内协调锁，避免多表锁定顺序造成死锁；原有 `locked_tables` 会话状态仍保留用于兼容检查。
- 持锁会话对自身已锁表不会重复获取 DML/读锁，其他会话的 SELECT/DML 会按 READ/WRITE 锁正确等待；`UNLOCK TABLES`、`COM_RESET_CONNECTION` 会释放实际 coordinator lease，而不是只清空状态 map。
- 新增 `TestLockTablesSupportsMultipleTablesAndResetReleasesCoordinatorLocks`，覆盖多表解析、READ/WRITE 组合、持锁会话访问、跨会话阻塞和 reset 释放；engine/net/protocol 定向回归与全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `90.976s`、manager `11.509s`、net `2.789s`、replication `2.358s`）。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation787/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation787/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入本轮；完整跨节点 MDL/fencing、锁等待超时与死锁检测、native binlog 上游全量语义、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍需继续实现和验证。

## Continuation 788

- 继续收口 P1 锁等待生命周期：表级 coordinator 的读锁、DML 锁、共享 DDL 锁和排他 DDL 锁均支持基于执行上下文的可中断获取；连接被 `KILL QUERY` 或上层 context 取消时，等待不会遗留后台 goroutine 或持有半成品锁。
- `LOCK TABLES` 的多表获取也改为可取消获取；多表按全限定名排序后逐个申请，部分申请失败会释放已申请的 lease 并清理会话状态。
- 新增 `TestTableDDLCoordinatorContextCancellationInterruptsWait`、`TestTableDDLCoordinatorContextCancellationInterruptsDMLWait` 和 `TestLockWaitRespondsToKillQueryCancellation`；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `90.887s`、manager `10.606s`、net `2.808s`、replication `2.068s`）。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation788/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation788/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：此轮实现的是执行取消语义，不等同于完整 MySQL `innodb_lock_wait_timeout`、跨节点 MDL/fencing 和死锁受害者选择；native binlog 上游全量 GTID/PITR 语义、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍需继续实现和验证。

## Continuation 789

- 继续补齐 P1 锁等待超时：系统变量管理器新增 `innodb_lock_wait_timeout`，默认值为 50 秒，支持会话级 `SET SESSION innodb_lock_wait_timeout = N`；表锁、DML 和 SELECT 的 coordinator 等待使用该值建立可取消 deadline。
- deadline 到期后返回稳定的 `lock wait timeout exceeded` 错误文本，协议错误分类可映射到 MySQL 1205；连接取消/KILL 仍保持 `context.Canceled` 中断语义，多表锁申请超时会回滚已取得的部分 lease。
- 新增 `TestSystemVariablesManagerSupportsInnoDBLockWaitTimeout` 和 `TestLockWaitReturnsMySQLTimeoutError`；engine/manager/net/protocol 定向回归、清缓存后的全仓 `go test ./... -count=1 -timeout 10m` 均通过（engine `341.740s`、manager `45.962s`、net `4.829s`、replication `5.162s`）。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation789/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation789/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：完整 MDL 图、跨节点 fencing、死锁受害者选择和所有 row/transaction lock 的上游等待语义仍需继续实现；native binlog 全量 GTID/PITR、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍在后续范围内。

## Continuation 790

- 继续扩大 P1 同表协调范围：语句级访问分析现在收集并按全限定名排序 `FROM/JOIN/UPDATE/INTO` 的所有表源；SELECT、JOIN、UPDATE/DELETE 等多表语句不再只保护第一张表，统一持有所有读锁或 DML 锁，结束时逆序释放。
- 多表锁申请支持执行 context 和 `innodb_lock_wait_timeout`，中途失败会释放已获得的锁；`LOCK TABLES` 状态校验也会逐表检查，避免 JOIN 绕过未锁定表或 READ 锁写入。
- 新增 `TestStatementTableAccessesCollectsAndSortsJoinSources`、`TestJoinWaitsForEveryReferencedTableLock`；核心包与全仓回归通过（engine `271.918s`、manager `24.505s`、net `4.300s`、replication `2.975s`）。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation790/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation790/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：当前是进程内多表协调，不等同于完整 MySQL MDL 图、跨节点 fencing、锁升级/死锁受害者选择；native binlog 全量 GTID/PITR、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍需继续实现和验证。

## Continuation 791

- 继续收口 P1-SEC 对象权限边界：`CREATE VIEW` 现在要求目标 schema 的 `CREATE VIEW` 权限；视图定义引用的每个真实表/视图源要求 `SELECT`；`ALTER VIEW` 要求目标视图的 `ALTER`；`DROP VIEW` 的多目标路径逐个要求 `DROP`。授权前拒绝、授权后放行，且不影响 root/匿名测试会话的既有兼容行为。
- 新增 `TestViewDDLRequiresObjectPrivileges`，覆盖目标权限、源表 SELECT、ALTER 和 DROP 的拒绝/授权回归；VIEW、routine、trigger、event 相关现有测试继续通过。
- 本轮验证：核心包 `go test ./server/innodb/engine ./server/innodb/manager ./server/net ./server/protocol -count=1 -timeout 10m` 通过（engine `169.927s`）；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `167.729s`、manager `17.509s`、net `4.293s`、protocol `0.991s`、replication `2.852s`）。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation791/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation791/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：VIEW 源权限目前按已解析的表/视图源检查，完整 definer/invoker 权限链、grant-table 全生命周期、PROXY 登录代理语义和更宽的对象权限矩阵仍需继续收敛；用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入当前批次；完整 MDL/跨节点 fencing、native binlog 全量 GTID/PITR、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍在后续范围内。

## Continuation 792

- 对齐 MySQL 8.4 VIEW 权限语义：`ALTER VIEW` 现在要求目标 schema 的 `CREATE VIEW` 与目标视图的 `DROP`，而不是使用非官方的 `ALTER` 权限；`CREATE OR REPLACE VIEW` 额外要求目标视图的 `DROP`。显式 `DEFINER='user'@'host'` 创建和修改现在要求当前用户是 definer，或持有 `SET_ANY_DEFINER`/`ALLOW_NONEXISTENT_DEFINER` 动态权限。
- 新增 `TestCreateOrReplaceViewRequiresDropPrivilege` 与 `TestCreateViewDefinerRequiresDefinerPrivilege`，并扩展 `TestViewDDLRequiresObjectPrivileges` 覆盖 source `SELECT`、`CREATE VIEW`、`DROP`、原 definer 和动态 definer 授权路径；相关 VIEW 元数据、创建、替换、显示测试继续通过。
- 本轮验证：核心包 `go test ./server/innodb/engine ./server/innodb/manager ./server/net ./server/protocol -count=1 -timeout 10m` 通过（engine `89.806s`）；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `91.546s`、manager `10.516s`、net `2.809s`、protocol `0.909s`、replication `2.027s`）；`git diff --check` 对本批次文件无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation792/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation792/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：definer/invoker 执行时的完整权限链、grant-table 全生命周期、PROXY 登录代理语义、跨 schema/列级 VIEW 权限细节仍需继续收敛；用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入当前批次；完整 MDL/跨节点 fencing、native binlog 全量 GTID/PITR、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍在后续范围内。

## Continuation 793

- 继续收口 VIEW 的细粒度权限：源表权限检查复用现有 SELECT 列级权限 helper；拥有 `GRANT SELECT(id) ON app.source` 的用户可以创建只引用 `id` 的视图，未覆盖的 `SELECT *` 或表达式列仍会拒绝；表级 `SELECT` 仍作为快速通过路径。
- 新增/扩展 VIEW 权限回归覆盖列级 source grant、目标 `CREATE VIEW`、`CREATE OR REPLACE` 的 `DROP`、`ALTER VIEW` 的 `CREATE VIEW + DROP`、原 definer 和动态 definer 授权；相关 VIEW 元数据和既有 DDL 测试继续通过。
- 本轮验证：核心包 `go test ./server/innodb/engine ./server/innodb/manager ./server/net ./server/protocol -count=1 -timeout 10m` 通过（engine `89.886s`）；`go test ./... -count=1 -timeout 10m` 全仓通过（engine `92.317s`、manager `10.522s`、net `2.902s`、protocol `1.043s`、replication `2.125s`）；`git diff --check` 对本批次文件无空白错误。
- 集群证据：`reports/compatibility/p1-cluster-current-continuation793/cluster-report.json` 的 `result` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation793/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：VIEW 被引用时的完整 `SQL SECURITY DEFINER/INVOKER` 执行权限链、grant-table 全生命周期、PROXY 登录代理语义、跨 schema/复杂表达式的列级权限解析仍需继续收敛；用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入当前批次；完整 MDL/跨节点 fencing、native binlog 全量 GTID/PITR、复杂任意 AST 组合，以及其余 P1/P3/P4 宽矩阵仍在后续范围内。

## Continuation 794

- 继续补齐 VIEW 运行时权限链：引擎门面 `XMySQLEngine.ExecuteQuery` 与直接执行器路径统一应用 `SQL SECURITY DEFINER/INVOKER`；`DEFINER` 视图的底层 SELECT 使用持久化定义者身份，`INVOKER` 视图使用调用者身份；VIEW 运行时强制底层表/列 SELECT，而普通查询继续沿用既有权限路径，避免改变存储函数 EXECUTE 和普通 DML 的错误优先级。
- 继续补齐授权生命周期：`DROP USER` 清理其他账户对被删除账户的角色、默认角色、admin option 和 PROXY 目标引用；`RENAME USER` 同步更新角色引用和 PROXY 目标；新增真实持久化回归。
- 继续补齐 P1-SEC 的 PROXY 登录执行身份：认证成功后解析精确 PROXY grant，加载被代理账户的全局动态权限和默认角色，并将执行会话身份切换为被代理账户；原始代理账户仍负责密码认证，未实现解析器的轻量认证替身不受影响；新增认证层回归。
- 本轮核心回归：`go test ./server/innodb/engine ./server/innodb/manager ./server/net ./server/protocol ./server/auth ./server/replication -count=1 -timeout 10m` 通过，engine `90.190s`、manager `8.734s`、net `1.659s`、protocol `0.302s`、auth `0.894s`、replication `1.319s`；VIEW、权限、账户生命周期和 PROXY 定向测试均通过。
- 集群 smoke、全仓 Go 和 Connector/J 发布门禁将在本轮新增权限/认证改动稳定后重新生成；本轮仍不宣称总完成，FULLTEXT 与非 Connector/J 全量客户端按用户要求后置，完整跨节点 MDL/fencing、native binlog 全量 GTID/PITR、复杂任意 AST 和剩余 P1/P3/P4 宽矩阵继续推进。
- 本轮最终门禁已完成：`go test ./... -count=1 -timeout 10m` 通过（engine `92.905s`、manager `9.943s`、net `2.561s`、replication `4.338s`）；`reports/compatibility/p1-cluster-current-continuation794/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation794/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。

## Continuation 795

- 统一两条认证入口的 PROXY 语义：`AuthService.AuthenticateUser` 现在先用代理账户完成密码认证，再在默认数据库权限检查前切换到被代理账户；因此 legacy AuthService 调用和 decoupled 握手都使用被代理账户的身份、全局/动态权限和默认角色。
- 新增 `InnoDBEngineAccess` 对持久化 `mysql.proxies_priv` 的真实解析回归；PROXY 目标不存在或锁定时拒绝登录，代理账户本身仍只负责认证，不会被错误地当作被代理账户授权。
- 本轮集群 smoke `reports/compatibility/p1-cluster-current-continuation795/cluster-report.json` 为 `PASS`；最终发布门禁 `reports/compatibility/release-candidate-current-continuation795/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：用户明确后置的 FULLTEXT 与非 Connector/J 全量客户端不纳入当前门禁；完整跨节点 MDL/fencing、native binlog 全量 GTID/PITR、复杂任意 AST、完整 stored object cursor/HANDLER/dynamic SQL、物理分区表空间和剩余 P1/P3/P4 宽矩阵继续按依赖顺序推进。

## Continuation 796

- 继续收口 P1 native binlog 物理互操作：`COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` 现在直接读取经过 CRC32 校验的 `binlog.NNNNNN` 物理 frame，不再把内部 JSONL 逻辑 position 重新编码成 wire event；因此 TABLE_MAP/ROWS、GTID、QUERY、XID 和物理 `log_pos` 保持与 durable native 文件一致。
- native dump 的 GTID 过滤按完整物理事务边界执行，blocking 拉流继续推进观察到的物理结束 offset；新增物理 offset 回归，验证从第二个 GTID event 开始传输时每个 wire header 的 `log_pos` 等于对应 native frame `EndPosition`，既有 rotated-file、GTID filter 和 row-event 回归继续通过。
- 本轮复制/网络专项通过；`go test ./... -count=1 -timeout 10m` 全仓通过，engine `91.829s`、manager `10.200s`、net `2.404s`、replication `2.538s`。集群 smoke、Connector/J 发布门禁将在本轮物理流改动后重新生成。
- 最终门禁已完成：`reports/compatibility/p1-cluster-current-continuation796/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation796/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：native row-event 的完整上游 metadata/schema negotiation、GTID index 全量语义、PITR 与跨重启全部边界、完整跨节点 MDL/fencing、复杂任意 AST、剩余 stored object/物理分区及用户明确后置的 FULLTEXT/非 Connector/J 全量客户端继续推进。

## Continuation 797

- 继续收口 P1 MDL 等待语义：ALTER TABLE 的共享/排他协调锁由不可中断的阻塞获取改为复用执行 context 与 `innodb_lock_wait_timeout` 的可取消获取；`KILL QUERY`/context 取消现在可以中断 ALTER 的 MDL 等待，并将失败映射到既有 lock-wait 错误契约。
- 修正 ALTER 锁重入计数：同一执行上下文的嵌套 ALTER 路径只在最外层真正释放底层表锁，内层释放不会提前放行其他会话；获取失败时同步清理重入标记，避免后续语句误判为持锁。
- 新增 `TestExecutorDDLWriteLockContextCancellationInterruptsWait` 与 `TestExecutorDDLWriteLockReentrancyReleasesOnlyAtOuterScope`；engine 全量回归通过（`88.438s`）。
- 同步修正 native `SHOW BINLOG EVENTS` 的物理映射：一个逻辑 row change 对应 TABLE_MAP + ROWS 两个 native frame，现在逐 frame 保留 `Pos/End_log_pos`，不会在第一个 row frame 后把 XID 或后续事务错绑到错误事件；新增 `TestDumpFileWithNativePositionsKeepsRowFramesAligned`，replication 定向回归通过。
- 增加 `backup.RestoreNativeUntilPosition`：按指定 `binlog.NNNNNN` 的物理 `End_log_pos` 聚合多 frame row transaction，只有 XID 已落在边界内才恢复整笔事务；截在 TABLE_MAP/ROWS 中间不会产生部分行，新增 `TestRestoreNativeUntilPositionUsesCommittedPhysicalBoundary`。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `91.768s`、manager `9.791s`、net `2.614s`、replication `2.317s`）；`reports/compatibility/p1-cluster-current-continuation797/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation797/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：这是进程内 MDL 等待/取消和重入边界的补强，不等同于完整 MySQL MDL 图、跨节点 fencing、在线 DDL 和死锁受害者选择；native GTID/PITR 全量语义、复杂任意 AST、剩余 stored object/物理分区以及用户明确后置的 FULLTEXT/非 Connector/J 全量客户端继续推进。

## Continuation 798

- 继续补齐 native PITR 的可调用恢复边界：新增 `backup.RestoreNativeUntilPosition`，按指定 `binlog.NNNNNN` 的物理 `End_log_pos` 聚合逻辑事件；TABLE_MAP/ROWS 多 frame 未完整结束时不提交，只有包含 XID 的完整事务才进入恢复结果。
- 新增 `TestRestoreNativeUntilPositionUsesCommittedPhysicalBoundary`，验证截在 ROWS frame 内恢复 0 行，到第一笔 XID 的物理结束位置只恢复第一笔事务；既有 logical backup/PITR 回归继续通过。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `92.783s`、manager `10.447s`、net `2.812s`、replication `2.074s`）；`reports/compatibility/p1-cluster-current-continuation798/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation798/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：native PITR 目前按单个 native 文件提供物理边界恢复，跨文件连续恢复、GTID index 的上游全量语义和所有恢复介质边界仍需继续收敛；完整跨节点 MDL/fencing、在线 DDL、复杂任意 AST、剩余 stored object/物理分区以及用户明确后置的 FULLTEXT/非 Connector/J 全量客户端继续推进。

## Continuation 799

- 扩展 native PITR 到跨轮转文件：新增 `backup.RestoreNativeUntilPositions`，按 `binlog.index` 的 native 文件顺序处理每个文件自己的 `End_log_pos` 边界，并在跨文件拼接后按逻辑 GTID/position 去重，避免轮转边界重复回放。
- 新增 `TestRestoreNativeUntilPositionsReplaysAcrossRotatedFiles`，验证第一文件和第二文件各自截到 XID 后可以连续恢复两笔事务；单文件 native PITR 与 logical PITR 回归继续通过。
- 该改动的全仓、集群和 Connector/J 最终门禁将在本轮代码稳定后生成；当前仍不宣称总完成：GTID index 的上游全量语义、跨重启恢复全部边界、完整跨节点 MDL/fencing、在线 DDL、复杂任意 AST、剩余 stored object/物理分区以及用户明确后置的 FULLTEXT/非 Connector/J 全量客户端继续推进。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `92.214s`、manager `10.052s`、net `2.486s`、replication `2.439s`）；`reports/compatibility/p1-cluster-current-continuation799/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation799/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。

## Continuation 800

- 继续补齐 native dump 文件头语义：当 `COM_BINLOG_DUMP`/`COM_BINLOG_DUMP_GTID` 从物理 position 4 开始时，直接发送请求目标 `binlog.NNNNNN` 中 CRC 校验过的 `FORMAT_DESCRIPTION_EVENT` 与 `PREVIOUS_GTIDS_EVENT`，不再丢弃前置 GTID 历史或用合成 FDE 替换 durable frame；非 4 起始 position 仍按物理 offset 过滤，blocking dump 不重复发送头事件。
- 更新 binlog 协议回归覆盖 default/rotated file、row event、GTID filter 和 blocking stream 的 FDE + PREVIOUS_GTIDS + transaction 顺序；net/replication 定向回归通过。
- 本轮最终全仓、集群和 Connector/J 门禁完成后再记录；当前仍不宣称总完成：native GTID index 上游全量语义、跨重启恢复全部边界、完整跨节点 MDL/fencing、在线 DDL、复杂任意 AST、剩余 stored object/物理分区以及用户明确后置的 FULLTEXT/非 Connector/J 全量客户端继续推进。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `92.214s`、manager `9.827s`、net `2.329s`、replication `2.609s`）；`reports/compatibility/p1-cluster-current-continuation800/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation800/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。

## Continuation 801

- 继续补齐 P1/P3 集群 fencing 边界：Runtime 新增持久化单调 `fencing_epoch`，提供 `/replication/fence` 控制面；候选副本提升前向成员广播 fence，旧 source 被 fence 后拒绝提交/轮转/RESET MASTER，并从 `Source()`/binlog 暴露中退出。
- fence 采用相同 epoch 下候选 UUID 的确定性优先级，重复请求和旧 epoch 会被拒绝；提升前要求达到成员多数派确认，提升后的 epoch 和 fence 状态可跨 Runtime 重启恢复；自动选主排除已 fenced 副本。
- 新增 `TestRuntimeFenceStopsSourceWritesAndHidesBinlogSource`、`TestRuntimePromoteFencesReachableSourceBeforePromotion` 与 `TestRuntimeFencingStateSurvivesRestartAndRejectsStaleEpoch`；replication/net/engine 定向回归通过。
- 当前仍不宣称总完成：这是当前 HTTP 控制面内的协作式 fencing，不等同于云盘/STONITH/外部租约提供的硬 fencing；native GTID index 上游全量语义、跨重启恢复全部边界、完整 MDL 图/在线 DDL、复杂 AST、剩余 stored object/物理分区以及用户明确后置的 FULLTEXT/非 Connector/J 全量客户端继续推进。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `92.467s`、manager `10.182s`、net `2.485s`、replication `2.637s`）；`reports/compatibility/p1-cluster-current-continuation801/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation801/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。

## Continuation 802

- 修正 LockManager 死锁受害者选择：检测到环路后只在该环路内选择最早等待事务，并用事务 ID 作为同一时间的确定性 tie-breaker；环路外更早等待者不会再被误选为受害者。
- 新增 `TestDeadlockVictimSelectionStaysInsideDetectedCycle`，验证存在环路外老等待者时仍选择环路内事务；manager 定向回归通过。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `93.528s`、manager `10.167s`、net `2.907s`、replication `4.093s`）；`reports/compatibility/p1-cluster-current-continuation802/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation802/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：死锁受害者选择已补齐基础环路边界，但完整 MySQL 兼容目标仍包括 native GTID index 上游全量语义、跨重启恢复全部边界、完整 MDL 图/在线 DDL、复杂 AST、剩余 stored object/物理分区；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 803

- 修正 native 复制管理面在 binlog 轮转后的状态契约：新增 `Source.NativeCurrentFilePosition`/`BinlogWriter.NativeCurrentFilePosition`，`SHOW MASTER STATUS`/`SHOW SOURCE STATUS` 现在返回当前 durable native 文件名及物理末端 Position，不再固定返回 `binlog.000001` 或内部 JSONL 位置。
- `SHOW BINLOG EVENTS` 的 ROTATE 记录现在报告实际下一个 native 文件名；新增轮转、物理 Position、重启恢复回归，replication/engine 专项通过。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `92.889s`、manager `10.027s`、net `2.881s`、replication `1.969s`）；`reports/compatibility/p1-cluster-current-continuation803/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation803/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：native GTID index 的完整上游语义、完整跨节点 MDL 图/在线 DDL、复杂 AST、剩余 stored object/物理分区继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 804

- 继续补齐 LockManager 等待图生命周期：等待锁被授予后从当前锁表重建 wait-for graph，清除已经不存在的等待边；`WaitGraphSnapshot` 的边和死锁检测起点按事务/资源稳定排序，保持诊断和受害者选择确定性。
- 新增 `TestWaitGraphRemovesEdgeAfterWaitingLockIsGranted`，验证阻塞事务获得锁后等待图为空且可继续重入获取；manager 专项与全仓 Go 回归通过。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `94.854s`、manager `10.119s`、net `2.386s`、replication `2.626s`）；`reports/compatibility/p1-cluster-current-continuation804/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation804/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：LockManager 等待图生命周期已继续收口，但完整 MySQL MDL 图/跨节点硬 fencing/在线 DDL、native GTID index 的完整上游语义、复杂 AST、剩余 stored object/物理分区仍需继续实现；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 805

- 继续补齐 native COM_BINLOG_DUMP_GTID 的上游区间语义：新增 `GTIDIntervals`，用合并后的 inclusive ranges 保存执行历史；wire 层的 `[start,end)` 区间直接解析为 range，native dump 通过区间 membership 过滤完整事务，不再按每个 sequence 展开。
- 保留原有 `GTIDSet` 和 `binlogDumpGTIDSet` 兼容接口；新增大区间回归，验证 `1..2^62` 仍只占一个 interval，并保留 half-open 边界验证。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `93.957s`、manager `11.077s`、net `3.110s`、replication `3.121s`）；`reports/compatibility/p1-cluster-current-continuation805/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation805/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：GTID intervals 已避免大范围展开，但完整上游 GTID index/UUID 重映射/跨重启恢复语义仍需继续收敛；完整 MySQL MDL 图/跨节点硬 fencing/在线 DDL、复杂 AST、剩余 stored object/物理分区继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 806

- 继续优化 GTID interval 生命周期：`GTIDIntervalsFromSet` 先按 SID 排序并压缩连续 sequence，再生成合并区间，避免逐条 `Add` 的重复排序；新增稳定的 `GTIDIntervals.String()`，统一输出已合并、按 SID 排序的 GTID 区间。
- 新增区间合并/格式化及 GTIDSet 压缩回归；随后 replication/net 定向包、全仓 Go、集群 smoke 和发布门禁均通过。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `94.304s`、manager `10.400s`、net `2.661s`、replication `2.195s`）；`reports/compatibility/p1-cluster-current-continuation806/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation806/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：GTID range 存储和性能边界已继续收口，但完整上游 GTID index/UUID 重映射/跨重启恢复语义仍需继续实现；完整 MySQL MDL 图/跨节点硬 fencing/在线 DDL、复杂 AST、剩余 stored object/物理分区继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 807

- 继续统一逻辑复制与 native 复制的 GTID 区间处理：新增 `ParseGTIDIntervals`，文本 `gtids=` 请求直接解析为合并区间；`DumpWithGTIDPosition` 和 native dump 都按区间 membership 过滤完整事务，旧 `ParseGTIDSet`/单序号 API 保持兼容。
- 新增大文本区间回归，验证 `1-2^62` 不物化为数十亿个 map entry；replication/net 定向回归通过。
- 最终门禁已完成：`go test ./... -count=1 -timeout 10m` 全仓通过（engine `95.349s`、manager `11.255s`、net `2.979s`、replication `2.184s`）；`reports/compatibility/p1-cluster-current-continuation807/cluster-report.json` 的 `result` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation807/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：两条复制入口已避免大 GTID 区间展开，但完整上游 GTID index/UUID 重映射/跨重启恢复语义仍需继续实现；完整 MySQL MDL 图/跨节点硬 fencing/在线 DDL、复杂 AST、剩余 stored object/物理分区继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 808

- 开始收敛完整 MDL 的诊断基础：DDL 协调器新增 owner-aware lock acquisition，记录真实 granted/pending owner、模式和时间；释放、超时、取消和会话表锁清理都会移除对应状态，避免产生伪造或 stale metadata lock。
- 新增 `performance_schema.metadata_locks` 查询路由，直接读取协调器快照并投影对象、schema、表名、锁类型、状态和 owner thread；新增 owner/waiter 生命周期及查询回归。
- 定向测试与 engine 全量通过（engine `94.950s`），全仓 Go 通过（engine `94.720s`、manager `10.352s`、net `2.649s`、replication `2.136s`）；`reports/compatibility/p1-cluster-current-continuation808/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation808/release-candidate.json` 为 `GO`，9 项检查全 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：这是完整 MDL 图的诊断基础，还不等同于跨节点 MDL、锁升级/死锁整合和真正 online DDL；GTID index/跨重启全量语义、复杂 AST、剩余 stored object/物理分区继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。当前 Windows 环境没有 gcc，`go test -race` 无法启动，普通并发/全量回归已通过。

## Continuation 809

- 继续补齐剩余 stored-object 客户端路径：新增连接级 `HANDLER table OPEN`、`HANDLER table READ [index] FIRST/NEXT/LAST/PREV`、可选 `WHERE` 和 `HANDLER table CLOSE`，复用真实 SELECT/storage 读取，保持 session cursor 位置，并在 `COM_RESET_CONNECTION` 时清理。
- MDL 诊断继续向等待图收敛：协调器根据真实 owner-aware granted/pending 状态生成 metadata wait-for edges，并投影到 `performance_schema.data_lock_waits`、`data_locks` 和 `events_waits_current/history*`；新增 HANDLER 生命周期、大小写敏感 WHERE 和 MDL wait-for 回归。
- engine 全量通过（`90.288s`）；全仓 Go 通过（engine `94.980s`、manager `9.454s`、net `2.366s`、replication `2.289s`）；`reports/compatibility/p1-cluster-current-continuation809/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation809/release-candidate.json` 为 `GO`，9 项检查全 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：HANDLER 仍是兼容子集，不等同于全部索引访问/锁定模式；完整 native GTID index/UUID 重映射/跨重启语义、跨节点 MDL/锁升级整合/online DDL、复杂 AST、物理分区表空间和其他 stored-object 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 810

- 继续收敛 native GTID index：`BinlogWriter` 新增 durable `binlog.gtid.index`，记录每个物理 `GTID_EVENT` 的 native SID、sequence、文件名及真实 `Position/End`；追加事务、native 文件重建、启动恢复和 `RESET MASTER` 都会增量维护或原子重建该 index，并暴露稳定排序的 snapshot API。
- 新增回归验证轮转后两个 GTID 分别落在 `binlog.000001`/`binlog.000002`，重启重新加载后 index 仍包含两条物理定位；replication 专项通过。
- 全仓 Go 通过（engine `95.233s`、manager `11.504s`、net `2.576s`、replication `3.126s`）；`reports/compatibility/p1-cluster-current-continuation810/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation810/release-candidate.json` 为 `GO`，9 项检查全 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：该 index 已覆盖本项目 native writer 的 GTID 物理定位，但还不是上游 MySQL 全量 UUID 重映射、跨源 GTID reconciliation 或所有外部工具格式；跨节点 MDL/锁升级整合/online DDL、复杂 AST、物理分区表空间和剩余 stored-object 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 811

- 继续补强 MDL 运维可观测性：`SHOW ENGINE INNODB STATUS` 现在输出真实 metadata lock wait-for edge 的表名、waiting/blocking owner 和等待时长，并与 `performance_schema` 的 metadata wait 投影共用同一协调器快照。
- 相关 engine 专项回归通过；全仓 Go 通过（engine `94.941s`、manager `10.040s`、net `2.745s`、replication `4.367s`）；`reports/compatibility/p1-cluster-current-continuation811/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation811/release-candidate.json` 为 `GO`，9 项检查全 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。
- 当前仍不宣称总完成：完整 MDL 锁升级/死锁整合、跨节点 MDL 与 online DDL、复杂 AST、物理分区表空间及剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 812

- 继续收敛存储 DDL 兼容：新增 SQL 层 `CREATE TABLESPACE ... ADD DATAFILE ... ENGINE=...`、`ALTER TABLESPACE ... RENAME TO ...` 和 `DROP TABLESPACE ... ENGINE=...` 的真实执行路径，复用 StorageManager/SpaceManager 创建、重命名和删除物理 `.ibd`；不安全或未知选项显式报错，避免静默改变语义。
- `StorageManager.DropTablespace` 增加系统表空间保护、表占用保护、flush 后删除和 catalog 清理；新增 SQL/物理文件回归，覆盖 create/rename/drop、占用拒绝和未知选项拒绝。
- `CREATE TABLESPACE IF NOT EXISTS` 与重复创建错误语义已补齐；dispatcher 对 CREATE/ALTER/DROP TABLESPACE 统一要求 `CREATE TABLESPACE` 全局权限，避免绕过权限边界。
- MDL owner-aware coordinator 新增同一 owner 的重入计数，并支持在持有 SHARED 时原子升级到 WRITE；释放按真实物理模式和嵌套计数执行，避免 Connector/J/DDL 组合场景自锁或过早解锁；新增升级与最终释放回归。
- MDL wait-for edge 现在按 READ/DML/SHARED/WRITE 的实际冲突矩阵生成，不再把兼容的 reader/owner 误报为 blocker；新增兼容模式诊断回归。
- 表空间专项、dispatcher 权限专项和当前工作树全仓 `go test ./... -count=1 -timeout 10m` 均通过；此前同一轮的 cluster smoke 为 `PASS`、Connector/J 139 tests 全通过、发布门禁为 `GO`。
- 最后追加的 MDL 冲突矩阵与 tablespace 权限路由修正后，核心包完整回归和当前工作树全仓 Go 回归仍通过（engine `96.289s`、manager `11.694s`、dispatcher `1.146s`）；随后 cluster smoke `p1-cluster-current-continuation812b` 仍为 `PASS`。上一轮 Connector/J 发布门禁已为 `GO`，139 tests 全通过。
- 当前仍不宣称总完成：跨节点 MDL/fencing 的强一致语义、在线 DDL、完整 general-tablespace 多表挂载/数据移动、复杂 AST、物理分区独立表空间、剩余 stored-object/P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。



## Continuation 814

- 继续补齐 InnoDB 运维元数据：新增 `INFORMATION_SCHEMA.INNODB_TABLESTATS`，从真实 `.frm`、TableStorageManager 映射和物理表空间读取表名、空间 ID、行数及聚簇索引页规模；新增 `INFORMATION_SCHEMA.INNODB_INDEXES`，投影已持久化的 PRIMARY 聚簇索引空间/根页关系。
- 增加表统计和索引元数据的 schema/table 过滤回归，确认表空间 DDL、表存储映射和查询投影保持一致；unknown/无法由当前内部字典证明的计数不伪造为上游完整值。
- 当前仍不宣称总完成：跨节点 MDL/fencing 的强一致语义、在线 DDL、完整 general-tablespace 多表挂载/数据移动、复杂 AST、物理分区独立表空间、剩余 stored-object/P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

- continuation814 门禁证据：INFORMATION_SCHEMA.INNODB_TABLESTATS/INNODB_INDEXES 与物理路径专项通过；全仓 go test ./... -count=1 -timeout 10m 通过（engine 95.657s、manager 9.817s、net 3.057s、replication 2.224s）；reports/compatibility/p1-cluster-current-continuation814/cluster-report.json 为 PASS；reports/compatibility/release-candidate-current-continuation814/release-candidate.json 为 GO，所有检查 PASS，Connector/J 139 tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 813

- 继续补齐表空间可观测性：新增 `INFORMATION_SCHEMA.INNODB_TABLESPACES` 查询投影，直接读取 StorageManager 的 durable tablespace catalog，返回空间 ID、名称、类型、文件大小、状态等字段；未知 SDI/上游专有字段保持 `NULL`，不伪造完整 InnoDB 内部语义。
- 新增创建、重命名、删除后的元数据回归，验证 SQL DDL、物理 `.ibd` 和 `INFORMATION_SCHEMA` 三条链路保持一致；表空间专项测试通过。
- 当前仍不宣称总完成：跨节点 MDL/fencing 的强一致语义、在线 DDL、完整 general-tablespace 多表挂载/数据移动、复杂 AST、物理分区独立表空间、剩余 stored-object/P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

- continuation813 门禁证据：表空间与 `INFORMATION_SCHEMA.INNODB_TABLESPACES/INNODB_DATAFILES` 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `102.177s`、manager `10.717s`、net `2.344s`、replication `4.502s`）；`reports/compatibility/p1-cluster-current-continuation813/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation813/release-candidate.json` 为 `GO`，所有检查 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 815

- 继续补齐 InnoDB 字典投影：新增 `INFORMATION_SCHEMA.INNODB_COLUMNS`，从 durable `.frm` 与 TableStorageManager 读取列顺序、名称和可证明的存储长度；内部 `MTYPE/PRTYPE/DEFAULT_VALUE` 等尚无原生字典证据的字段保持 `NULL`。
- 新增列字典过滤与类型长度回归；当前专项通过，后续全仓、集群和 Connector/J 门禁将在本轮代码稳定后刷新。
- 当前仍不宣称总完成：跨节点 MDL/fencing 的强一致语义、在线 DDL、完整 general-tablespace 多表挂载/数据移动、复杂 AST、物理分区独立表空间、剩余 stored-object/P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

- continuation815 门禁证据：INNODB_COLUMNS 与表空间元数据专项通过；全仓 go test ./... -count=1 -timeout 10m 通过（engine 97.028s、manager 10.381s、net 2.543s、replication 2.459s）；reports/compatibility/p1-cluster-current-continuation815/cluster-report.json 为 PASS；reports/compatibility/release-candidate-current-continuation815/release-candidate.json 为 GO，所有检查 PASS，Connector/J 139 tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 816

- 继续补齐 P1 存储布局：分区表不再只保存逻辑 descriptor；每个分区拥有独立 tablespace 和 clustered B+Tree，INSERT/UPDATE/DELETE/全表扫描通过分区路由器访问，REORGANIZE 会搬迁受影响行，DROP/REORGANIZE 会回收移除的分区空间。
- 分区 physical locator（space/root/data segment）写回 `.frm`，重启映射可按持久化 locator 恢复；新增独立 SpaceID/root、重组数据可见性和空间生命周期回归。
- general tablespace 支持多表挂载；共享 space 下每张表分配独立 clustered root；`ALTER TABLE ... TABLESPACE` 通过真实行复制完成数据迁移并回收旧 file-per-table space，DROP TABLE 不误删仍被其他表使用的 general tablespace。
- 当前仍不宣称总完成：跨节点 MDL/fencing 的强一致语义、在线 DDL、完整 general-tablespace import/discard、复杂 AST、完整 stored-object cursor/HANDLER/dynamic SQL 宽矩阵、native binlog/GTID 全协议与剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 817

- 继续补齐 stored-object 客户端路径：`HANDLER` 现在读取真实表索引元数据，支持索引顺序的 `FIRST/NEXT/LAST/PREV`，以及单列和复合索引的 `= / < / <= / > / >=` key lookup；复合索引按 MySQL 的字典序谓词展开，keyed `FIRST` 后的 `NEXT` 会继续读取后续 key，而不是重新打开整表。
- 修正存储过程动态 SQL 参数绑定：`EXECUTE ... USING` 按 SQL 引号、标识符引号和转义扫描参数标记，不会替换字符串字面量中的 `?`，并校验参数标记与 USING 参数数量；新增多参数和 quoted-question-mark 回归。
- 继续补齐物理分区 pruning：常量分区谓词在 clustered fan-out 前就限制物理分区集合，未知/复杂谓词保守扫描全部分区；新增 RANGE physical fan-out pruning 回归。
- `ALTER TABLE ... TABLESPACE` 扩展到分区表：所有分区 clustered root 迁入同一 general tablespace，但每个分区仍分配独立 root；分区 locator 持久化 tablespace 名称/ownership，重启恢复可复用共享空间，旧 partition `.ibd` 文件回收且不会误删共享 general tablespace。
- 修复跨 schema `RENAME TABLE` 的持久化 identity：重命名后的 `.frm` 同步更新 file-per-table tablespace 名称，避免重启时按旧 `source_db/table` 创建空映射；对应 restart regression 复验通过。
- 本轮 engine、manager 和分区/表空间/存储对象专项回归通过；全仓、集群和 Connector/J 门禁将在本轮代码稳定后刷新。当前仍不宣称总完成：跨节点 MDL/fencing 的强一致语义、在线 DDL、general-tablespace import/discard、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 818

- 继续补齐 tablespace 生命周期：file-per-table 支持 `ALTER TABLE ... DISCARD TABLESPACE` 与 `IMPORT TABLESPACE` 的真实物理删除/重新挂载，保留 discarded Space ID、clustered root 和 `.frm` 状态；缺失导入文件或未处于 discarded 状态时显式报错。
- 分区 file-per-table 表支持逐分区 discard/import，导入前逐个校验并挂载分区 `.ibd`，随后恢复独立 partition locator、clustered root 和数据可见性；共享/general tablespace 上的表继续拒绝 DISCARD，避免误删共享物理文件。
- 重启扫描会从父表 `.frm` 的 partition locator 或 `discarded_space_id` 恢复物理 Space ID，避免导入/重启时将既有 `.ibd` 误分配为新的空空间；新增普通表、分区表和 general tablespace 拒绝回归。
- 本轮专项与全仓 Go 已通过；集群 smoke 与 Connector/J 发布门禁将在本轮代码稳定后刷新。当前仍不宣称总完成：跨节点 MDL/fencing 强一致语义、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 818 门禁证据：`go test ./... -count=1 -timeout 10m` 通过（engine `97.508s`、manager `10.395s`、net `2.738s`、replication `4.561s`）；`reports/compatibility/p1-cluster-current-continuation818b/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation818b/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。

## Continuation 819

- 继续补齐 HANDLER 客户端语义：`HANDLER table OPEN AS alias` 现在把连接级 cursor 按 alias 注册，后续 `HANDLER alias READ ...`/`CLOSE` 可访问并清理同一个真实表 cursor；新增 alias 生命周期回归。
- HANDLER 仍明确保留边界：未将所有 MySQL key-part/locking/handler 状态选项伪装成已实现，继续复用真实 SELECT 与 storage 路径。
- Continuation 819 门禁证据：`go test ./... -count=1 -timeout 10m` 通过（engine `103.096s`、manager `11.515s`、net `2.711s`、replication `2.149s`）；`reports/compatibility/p1-cluster-current-continuation819/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation819/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped。

## Continuation 820

- 继续补齐 InnoDB 运维元数据：新增 `INFORMATION_SCHEMA.INNODB_TRX`，从 `TransactionManager` 的真实 active transaction snapshot 投影事务 ID、状态、开始时间、隔离级别、只读标志和已知锁计数；没有 session registry 或底层锁表证据的字段保持 `NULL`，不伪造 MySQL 内部信息。
- 新增 `INFORMATION_SCHEMA.INNODB_LOCK_WAITS` 与 `INFORMATION_SCHEMA.INNODB_LOCKS`，复用真实 `LockManager` wait graph 投影 requesting/blocking transaction 和 lock identity；metadata lock 继续由 Performance Schema 路径负责，未将两类锁混成同一数据源。
- 新增活跃事务、行锁等待和锁投影回归；engine + manager 全包通过（engine `97.035s`、manager `8.604s`）。全仓、集群和 Connector/J 门禁待本轮代码稳定后刷新。
- 当前仍不宣称总完成：跨节点 MDL/fencing 强一致语义、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 821

- 继续收紧 HANDLER 生命周期：同一连接对同一物理表名或 alias 重复 `HANDLER ... OPEN` 不再覆盖已有 cursor，而是返回明确的 already-open 错误；已有 cursor 仍可继续 `READ`，避免客户端状态被静默重置。
- 新增重复 OPEN 回归；本轮定向 HANDLER 回归和全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.500s`、manager `10.100s`、net `2.537s`、replication `2.341s`）。集群与 Connector/J 门禁在下一批 stored-object 改动稳定后刷新。
- 当前仍不宣称总完成：HANDLER 的完整 key-part/locking/handler 状态选项、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 822

- 继续补齐 InnoDB 运维元数据：新增 `INFORMATION_SCHEMA.INNODB_METRICS`，从真实 `TransactionManager` 活跃事务和 `LockManager` 当前等待图投影当前事务数、当前等待边数和当前等待时长；没有历史计数器或上游持久化证据的字段保持 `NULL`，不返回伪造的零值。
- 支持按 `NAME = ...` 与 `NAME LIKE ...` 过滤，新增实时指标与过滤回归；engine 定向专项通过。
- 当前仍不宣称总完成：HANDLER 的完整 key-part/locking/handler 状态选项、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 822 门禁证据：`go test ./... -count=1 -timeout 10m` 通过（engine `99.214s`、manager `10.230s`、net `2.716s`、replication `2.089s`）；`reports/compatibility/p1-cluster-current-continuation822/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation822/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 823

- 继续收紧 HANDLER 生命周期：同一连接对同一物理表通过不同名字或 alias 重复 `OPEN` 现在都会返回 already-open 错误；`HANDLER physical_table CLOSE` 也能关闭以 alias 注册的 cursor，随后 alias 读取明确失败，不留下悬挂 session 状态。
- 新增物理表重复 OPEN 与按物理表名关闭 alias cursor 回归；engine HANDLER 专项通过。
- 当前仍不宣称总完成：HANDLER 的完整 key-part/locking/handler 状态选项、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 823 门禁证据：`go test ./... -count=1 -timeout 10m` 通过（engine `99.189s`、manager `10.456s`、net `2.587s`、replication `2.888s`）；`reports/compatibility/p1-cluster-current-continuation823/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation823/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 824

- 继续补齐 InnoDB 字典兼容：新增 `INFORMATION_SCHEMA.INNODB_FOREIGN` 与 `INFORMATION_SCHEMA.INNODB_FOREIGN_COLS`，从持久化 `.frm` 外键定义投影约束 ID、子表/父表名称、列数量及逐列映射；无法由当前字典证明的 InnoDB `TYPE` 位标志保持 `NULL`。
- 支持按 `FOR_NAME`、`REF_NAME`、`ID` 过滤，新增跨表外键字典与列映射回归；engine 定向专项通过。
- 当前仍不宣称总完成：HANDLER 的完整 key-part/locking/handler 状态选项、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 824 门禁证据：`go test ./... -count=1 -timeout 10m` 通过（engine `98.788s`、manager `9.957s`、net `2.746s`、replication `4.300s`）；`reports/compatibility/p1-cluster-current-continuation824/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation824/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 825

- 继续补齐存储过程动态 SQL：`EXECUTE prepared_stmt USING local_var` 现在同时支持过程局部变量与会话用户变量；局部变量按保存的 SQL 字面量重新求值后绑定参数标记，避免把带引号的内部表示再次当作字符串包裹。
- 新增 `DECLARE`/`SET` 局部变量驱动 `PREPARE`/`EXECUTE USING` 的真实 `CALL` 回归；原有用户变量、多参数及 quoted-question-mark 回归继续通过。
- 当前仍不宣称总完成：HANDLER 的完整 key-part/locking/handler 状态选项、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 825 门禁证据：`go test ./... -count=1 -timeout 10m` 通过（engine `98.847s`、manager `10.214s`、net `2.713s`，其余 Go 包全部通过）；`reports/compatibility/p1-cluster-current-continuation825/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation825/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 826

- 继续补齐 HANDLER key-part 语义：复合索引的 `READ index = (prefix)` 现在允许省略尾部 key part，首次读取按前缀排序返回首行，后续 `NEXT` 保留同一前缀范围继续读取；完整 key 仍使用严格字典序后继谓词。
- 新增复合索引部分 key-part 的首次读取与连续 `NEXT` 回归；基础单列/完整复合键、范围读取和 alias 生命周期继续覆盖。
- 当前仍不宣称总完成：HANDLER 的 locking/handler 状态选项、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 stored-object 宽矩阵、native binlog/GTID 全协议和其他 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 826 门禁证据：HANDLER 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.321s`、manager `10.312s`、net `2.751s`）；`reports/compatibility/p1-cluster-current-continuation826/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation826/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- Continuation 827 门禁证据：HANDLER `WHERE/LIMIT` 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.518s`、manager `10.583s`、net `2.777s`）；`reports/compatibility/p1-cluster-current-continuation827/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation827/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 828

- 继续补齐 native row-event schema 语义：`replication.RowChange` 增加可选 `Columns` 列序列；native `TABLE_MAP_EVENT`/row image 在提供 schema 证据时保持真实表定义顺序，未标注但存在于 row image 的字段按确定性顺序追加，不静默丢失。
- 新增 schema 顺序回归；没有提供 schema 的旧调用方继续使用兼容的确定性排序路径。
- 当前仍不宣称总完成：完整列类型/metadata、schema negotiation、row image flags、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 828 门禁证据：replication/net/engine 专项通过（engine `94.248s`）；全仓 `go test ./... -count=1` 通过（engine `98.936s`、manager `10.315s`、net `2.663s`、replication `2.528s`）；`reports/compatibility/p1-cluster-current-continuation828/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation828/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 829

- 继续补齐 native row-event 列类型 metadata：`RowChange.ColumnTypes` 现在驱动常见整数、浮点、`VARCHAR` 和 `NEWDECIMAL` 的 TABLE_MAP 类型码与可证明 metadata；缺失类型提示时保持旧的 Go 值类型推导，兼容旧调用方。
- 新增类型提示与 metadata 回归；JSON binary serialization、完整 decimal binary encoding、schema negotiation、row image flags 仍保持明确边界，不将文本或错误长度伪装成上游 wire 格式。
- 当前仍不宣称总完成：完整 native row codec、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 829 门禁证据：replication/net/engine 专项通过（engine `94.248s`）；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.168s`、manager `9.760s`、net `2.627s`、replication `2.513s`）；`reports/compatibility/p1-cluster-current-continuation829/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation829/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 830

- 继续补齐 native row-event 实值编码：JSON 列现在写入 MySQL binary JSON 的 length-encoded payload，支持字面量、整数、浮点、字符串、数组和对象；对象键按 MySQL 的长度优先、同长度字典序规范排序。
- `DECIMAL/NUMERIC` 列现在按 `precision/scale` 写入 packed decimal，覆盖正负值与定点分组；无法由类型提示确定精度或值无法安全编码时保留旧兼容路径，不伪造 wire bytes。
- 修复 UUID_SHORT 跨秒重启的单调性：持久化序列恢复不再依赖恰好处于同一 Unix 秒，避免出现 `2^24-1` 跳变；重复重启回归通过。
- 当前仍不宣称总完成：native row-event 完整 schema negotiation/flags、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 830 门禁证据：replication 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `103.612s`、manager `11.456s`、plan `0.400s`、net `4.907s`、replication `5.289s`）；`reports/compatibility/p1-cluster-current-continuation830/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation830/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 831

- 继续补齐 native schema negotiation：`RowChange.ColumnMetadata` 允许上游调用方为每列传入已协商的原始 TABLE_MAP metadata，编码器复制并优先使用该值，避免对复杂类型做不安全猜测；未提供时仍沿用 `ColumnTypes` 和旧值推导路径。
- 新增原始 metadata 不可变拷贝回归；JSON binary、packed DECIMAL、对象键规范和 UUID_SHORT 稳定性回归继续通过。
- 当前仍不宣称总完成：完整 native row flags/多行 image、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 831 门禁证据：replication 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.857s`、manager `10.143s`、plan `1.102s`、net `2.663s`、replication `2.248s`）；`reports/compatibility/p1-cluster-current-continuation831/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation831/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 832

- 继续补齐 native row-event 类型码映射：`CHAR/ENUM/SET` 使用 `MYSQL_TYPE_STRING`，BLOB 使用 `MYSQL_TYPE_BLOB`，并识别 GEOMETRY、YEAR、TIME2、DATETIME2、TIMESTAMP2；复杂类型的真实 metadata 继续通过 `ColumnMetadata` 原样协商，不猜测长度/字符集。
- 新增常见 MySQL 类型码回归；跨文件 `NativeDumpFrom` 能从指定物理位置连续读取后续轮转文件，缺失起始文件明确报错。
- 当前仍不宣称总完成：时间/LOB/空间类型的完整值编码、native row flags/多行 image、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 832 门禁证据：replication 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.272s`、manager `10.960s`、plan `1.030s`、net `2.786s`、replication `3.279s`）；`reports/compatibility/p1-cluster-current-continuation832/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation832/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 833

- 继续补齐 native row-event 字符串/LOB 实值编码：当 `ColumnMetadata` 已协商时，BLOB 按 pack length、VARCHAR/VAR_STRING 按最大长度选择固定宽度 length prefix；没有协商 metadata 时保留旧兼容推导路径。
- 新增 BLOB/VARCHAR 长度前缀回归；replication、net、engine 定向回归均通过。
- 当前仍不宣称总完成：时间/LOB/空间类型完整值编码、native row flags/多行 image、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 833 门禁证据：replication/net/engine 专项通过（engine `97.034s`）；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.537s`、manager `10.211s`、plan `0.861s`、net `2.347s`、replication `2.214s`）；`reports/compatibility/p1-cluster-current-continuation833/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation833/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 834

- 继续细化 native TABLE_MAP metadata：`BIT(n)` 现在生成 `{bit_length, byte_length}` 两字节 metadata；`TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB` 以及带长度声明的 `BLOB(n)` 生成对应 pack length metadata；调用方已经提供 `ColumnMetadata` 时仍优先使用原始协商字节。
- 新增 BIT/BLOB metadata 回归；未提供类型提示的旧调用方路径保持不变，避免把不确定的字符集、长度或空间类型信息伪造成 wire metadata。
- 当前仍不宣称总完成：时间类型及 LOB/空间类型完整值编码、native row flags/多行 image、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 834 门禁证据：replication 专项通过（`1.827s`），UUID_SHORT 重启回归 `count=20` 通过（`0.980s`）；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.983s`、manager `11.103s`、plan `1.290s`、net `2.466s`、replication `2.705s`）；`reports/compatibility/p1-cluster-current-continuation834/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation834/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 835

- 继续补齐 native temporal row image：`DATE` 按 3-byte packed date 编码；`TIME2/DATETIME2/TIMESTAMP2` 支持 `fsp=0..6` metadata 和对应整数/微秒 binary payload，接受 `time.Time` 及常见文本输入；负 TIME interval 和无法安全解析的输入保留旧兼容回退，不伪造符号/小数补码。
- native Rows event 现在会把连续、同表、同 action 且 schema-compatible 的多条 `RowChange` 合并为一个 Rows event，并为每行写入独立 null bitmap/image；跨表、action 或 schema 不兼容时仍生成独立 TABLE_MAP/Rows 对。
- 新增 temporal precision/value、字符串解析安全回退、多行 row image 回归；原有 JSON、DECIMAL、LOB、GTID/rotate/position 路径保持通过。
- 当前仍不宣称总完成：负 TIME 精确补码、LOB/空间类型完整值编码、native row flags/多表 image 高级语义、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 835 门禁证据：replication 专项通过（`1.712s`），UUID_SHORT 重启回归 `count=20` 通过（`0.729s`）；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.732s`、manager `10.233s`、plan `1.083s`、net `2.646s`、replication `2.618s`）；`reports/compatibility/p1-cluster-current-continuation835/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation835/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 836

- 继续补齐 native Rows event：`RowChange.Flags` 现在透传 MySQL row-level flags；同一 Rows event 的合并条件同时要求 table、action、flags、列顺序、类型和 negotiated metadata 一致，避免把不同语义的行错误合并。
- 新增 flags 保留与多行 row image 回归；默认 flags 为 0，现有逻辑复制调用方的 JSON 结构和旧 native 路径保持兼容。
- 当前仍不宣称总完成：负 TIME 精确补码、LOB/空间类型完整值编码、native 多表 image/extra-row-info 高级语义、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 836 门禁证据：replication 专项通过（`4.532s`），UUID_SHORT 重启回归 `count=20` 通过（`0.729s`）；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `104.579s`、manager `14.212s`、plan `0.933s`、net `4.376s`、replication `4.532s`）；`reports/compatibility/p1-cluster-current-continuation836/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation836/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 837

- 继续修正 native LOB/VARCHAR 值编码：当调用方只提供 `ColumnTypes` 而没有显式 `ColumnMetadata` 时，编码器现在使用类型声明推导出的 pack length/最大长度选择 1-byte 或 2/3/4-byte length prefix；显式 negotiated metadata 仍优先。
- 新增 `TINYBLOB` 与 `VARCHAR(300)` 的类型提示回归，覆盖“schema metadata 已生成且实际 value encoder 使用它”的闭环；不改变无类型提示的旧回退路径。
- 当前仍不宣称总完成：负 TIME 精确补码、LOB/空间类型完整值编码、native partial JSON/多表 image/extra-row-info 全语义、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 837 门禁证据：replication 专项通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.075s`、manager `10.686s`、plan `1.032s`、net `3.180s`、replication `3.287s`）；`reports/compatibility/p1-cluster-current-continuation837/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation837/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 838

- 继续收口 `BIT(n)` native row 编解码：依据 MySQL `Field_bit` metadata 规则，metadata 修正为 `{bit_length % 8, full_byte_length}`；值编码改为固定宽度 binary bytes，覆盖 BIT(8)/BIT(9) 及无符号整数/数字文本输入，越界值保守回退而不静默截断。
- 新增 BIT metadata、fixed-width value、越界回退和类型提示回归；LOB/VARCHAR、temporal、JSON、DECIMAL、多行 image、flags/extra-row-info 路径继续通过。
- 当前仍不宣称总完成：负 TIME 精确补码、LOB/空间类型完整值编码、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。
- Continuation 838 门禁证据：replication 专项 `1.760s`、engine BIT/Native 专项 `1.895s` 通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.624s`、manager `10.424s`、plan `1.114s`、net `2.675s`、replication `2.628s`）；`reports/compatibility/p1-cluster-current-continuation838/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation838/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 839

- 继续修正 native temporal row image 的 MySQL wire 语义：`TIME2` 使用 `hms` 位打包和 `TIMEF_INT_OFS/TIMEF_OFS` 偏移，支持负 TIME 的 fractional two's-complement/complement 规则；`DATETIME2` 使用 `ymd/hms` 位打包和 5-byte little-endian 整数部分；`TIME2/DATETIME2/TIMESTAMP2` 的 fsp 小数部分按 MySQL 的 1/2/3-byte little-endian 规则输出。
- 新增正负 TIME 的 fsp=0/3/6、DATETIME2/TIMESTAMP2 小数精度回归；保留无法安全解析输入的兼容回退，不伪造 temporal payload。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.544s`、manager `10.090s`、net `2.613s`、replication `2.738s`）；`reports/compatibility/p1-cluster-current-continuation839/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation839/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS（总计约 09:56）。
- 当前仍不宣称总完成：native partial JSON/更复杂 extra-row-info、空间类型完整值编码、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 840

- 继续补齐 native spatial value 的安全编码边界：`GEOMETRY` 在调用方提供已编码的二进制 payload（`[]byte`/`*[]byte`）时按 length-encoded bytes 写入 row image，并复制输入缓冲区避免后续修改污染 binlog；无法证明为二进制 geometry 的文本值继续走旧兼容回退，不猜测 WKB/SRID。
- 新增 GEOMETRY 原始 payload 回归；temporal、BIT、JSON、DECIMAL、LOB/VARCHAR、多行 image、flags/extra-row-info 路径保持通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.049s`、manager `9.577s`、plan `1.118s`、net `2.943s`、replication `2.479s`）；`reports/compatibility/p1-cluster-current-continuation840/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation840/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS（总计约 09:23）。
- 当前仍不宣称总完成：空间类型的 WKB/SRID/空间函数与 R-tree 索引维护、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 878

- 继续扩展 MySQL 空间函数矩阵：新增 `ST_Validate`，对当前支持的几何值仅返回语法/几何校验通过的原值，否则返回 `NULL`；新增 `ST_PointAtDistance`、`ST_LineInterpolatePoint`、`ST_LineInterpolatePoints`，支持二维 LineString 的距离/比例插值、端点和越界校验。
- 新增 GeoHash 编解码能力：`ST_GeoHash`、`ST_LatFromGeoHash`、`ST_LongFromGeoHash`、`ST_PointFromGeoHash`，覆盖经纬度边界、长度限制、SRID 返回和非法字符校验；补齐类型化 WKT 构造器、`ST_LineString` 与 `ST_MakeEnvelope`，并接入普通表达式、compiled-expression 和真实 SQL 投影。
- 计划继续推进：native partial JSON 的真实 SQL 变更捕获/副本应用、完整 row-event 解析互操作、真实 InnoDB R-tree 页结构、球面线段精确距离、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 879

- 继续扩展空间函数矩阵：新增 `ST_LineInterpolatePoint(s)`、`ST_PointAtDistance`、`ST_Simplify`、`ST_Transform`（当前支持 4326 与 3857 的二维坐标变换）、`ST_HausdorffDistance` 和 `ST_FrechetDistance`；类型化 WKT 构造器、`ST_LineString`、`ST_MakeEnvelope`、GeoHash 编解码均纳入同一表达式兼容路径。
- 所有新增函数均增加边界校验和 plan/compiled/真实 SQL 回归；空间能力仍明确限于当前二维几何模型，未将完整 EPSG 数据库、复杂地理椭球算法或全部 GIS 布尔运算宣称为已完成。
- 计划继续推进：native partial JSON 的真实 SQL 变更捕获/副本应用、完整 row-event 解析互操作、真实 InnoDB R-tree 页结构、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 880

- 继续收口空间有效性与几何运算：`ST_IsValid`/`ST_Validate` 现在检查 LineString 最小点数、Polygon 闭合/非退化/非自交、洞位于外环内且环间不相交；新增 `ST_HausdorffDistance` 与离散 `ST_FrechetDistance`。
- 继续推进 native partial JSON：新增 `DeriveJSONPartialUpdates`，可从 before/after JSON row image 自动生成排序稳定的对象成员增删改路径；native writer 在没有显式 diff 元数据时自动尝试生成紧凑 diff，过大、非法 JSON 或 SQL NULL 仍安全回退完整 JSON 行值。
- 以上能力均有 plan/replication 回归；仍明确未完成真实 SQL DML 到 RowChange 的自动捕获、replica 对 partial JSON 的原子应用、完整 row-event 解析互操作，以及其它剩余 P1/P3/P4 宽矩阵。FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 881

- 继续补齐 native partial JSON 的真实源端闭环：`StorageIntegratedDMLExecutor` 现在把 INSERT/UPDATE/DELETE 的提交前后行镜像和列类型随事务变化保留下来；source runtime 新增单一 GTID 的 rows+statements 提交接口，普通 statement replay 仍沿用原有 SQL 应用路径。
- source engine 的真实 SQL UPDATE 已验证可同时写入 logical row image 与 SQL statement；JSON 列在 before/after row image 可形成更紧凑 diff 时，native 文件实际输出 `PARTIAL_UPDATE_ROWS_EVENT`（type 39），不能安全压缩时继续输出完整 JSON 值。
- 仍明确未宣称完成 replica 对 partial JSON 二进制 diff 的直接原子应用、完整 native row-event 解析互操作和所有复杂 SQL/存储格式；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 882

- 继续收口 partial JSON 事件持久化：`BinlogWriter.AppendTransaction` 现在会对没有显式 diff 的 UPDATE 行镜像自动推导紧凑 JSON 对象成员变更，并将可安全编码的 diff 回写到 logical row event；因此重启后读取 logical binlog 仍能看到与 native type 39 一致的 partial metadata。
- 新增 source engine 真实 SQL 回归，验证 JSON before/after、列类型、SQL statement 和单 GTID 事务边界同时保留；原有 statement-based replica apply 继续优先执行 SQL batch，不会因新增 row image 产生重复写入。
- 仍明确未宣称完成 replica 对 partial JSON 二进制 diff 的直接原子应用、完整 native row-event 解析互操作和所有复杂 SQL/存储格式；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 872

- 继续收口 native binlog JSON 高级语义：新增 `JSONPartialUpdate` 描述及 `PARTIAL_UPDATE_ROWS_EVENT`（39）编码；更新事件在 before image 与 after image 之间写入 `value_options=PARTIAL_JSON` 和按表列位图，after-image 对 `JSON_REPLACE/JSON_INSERT/JSON_REMOVE` 风格 diff 使用 MySQL packed length 编码。
- partial diff 序列化后如果不小于完整 binary JSON，自动回退 `UPDATE_ROWS_EVENTv2` 与完整 JSON 值；未提供 partial metadata 的既有更新路径保持完全兼容；事件分组/物理文件重建同步识别 39 类型。
- 新增 partial JSON 成功编码、事件类型切换和 oversized diff 回退回归；`go test ./server/replication -count=1` 通过。
- 当前仍继续推进：partial JSON 的真实 SQL 变更捕获/副本应用、完整 row-event 解析互操作、真实 R-tree 页结构、球面线段精确距离、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 873

- 修正 `SYSDATE(fsp)` 的时间精度截断：按 MySQL 0–6 位小数精度截断到秒、毫秒或微秒，不再使用错误的 `1.x` 秒级截断间隔。
- 新增固定时间基准的精度回归，并保留 `SYSDATE` 普通调用与其它日期函数路径通过。

## Continuation 874

- 继续收口 `PX-IDX-002`：空间索引 sidecar 从平面 MBR 条目升级为版本 2 的确定性 R-tree 层级，节点按空间中心排序、固定 fanout 构建并随 `.frm` 原子持久化；空索引和旧版本平面条目保持兼容。
- `ST_INTERSECTS` 等相交谓词优先按 R-tree 节点 MBR 裁剪，`MBRDISJOINT` 支持对完全不相交子树快速纳入候选，最终仍执行精确几何判断；新增确定性树结构、根节点、候选结果回归。

## Continuation 866

- 继续扩展空间谓词矩阵：新增 `ST_CROSSES`，支持线-线的真正内部交叉、线-面穿越判断，并排除仅端点接触、共线重叠和完全包含等不满足 crosses 关系的情况。
- `ST_CROSSES` 已接入普通表达式、compiled-expression 和引擎空间查询两条路径，支持列在前及几何常量在前的 SQL 形式；新增 plan 与 engine 回归。
- 当前仍继续推进：空间 MBR sidecar 的候选裁剪/真实 R-tree 页结构、球面线段精确距离、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 867

- 继续补齐常用日期时间函数：新增 `UTC_DATE()` 与 `UTC_TIME()`，按 UTC 时区返回日期/时间字符串，并接入 compiled-expression 白名单与回归测试。
- 当前仍继续推进：空间 MBR sidecar 的候选裁剪/真实 R-tree 页结构、球面线段精确距离、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 868

- 继续补齐日期时间函数矩阵：新增 `SYSDATE([fsp])`、`TO_DAYS`、`FROM_DAYS`、`TO_SECONDS`、`PERIOD_ADD`、`PERIOD_DIFF`、`TIMEDIFF` 与 `WEEKOFYEAR`，覆盖 UTC/日历序号、年月周期运算、跨日时间差和微秒保持等常见形式。
- 新函数已接入普通解释执行与 compiled-expression 路径，并新增日期/周期/负时间差/周数回归；日期时间相关既有测试保持通过。
- 当前仍继续推进：空间 MBR sidecar 的候选裁剪/真实 R-tree 页结构、球面线段精确距离、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 869

- 继续补齐常用编码/随机函数：新增 `TO_BASE64`、`FROM_BASE64` 和受上限保护的 `RANDOM_BYTES`，支持空白字符容忍、NULL 传播、随机字节生成和长度校验。
- 新函数已接入普通解释执行与 compiled-expression 路径，并新增 Base64 往返及随机字节长度回归。
- Continuation 868 的全仓、集群和发布候选门禁证据保持有效；当前仍继续推进空间索引真正候选裁剪、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 870

- 继续收口 `PX-IDX-002`：空间查询现在优先消费持久化 MBR sidecar 生成候选主键集合，再执行精确空间谓词；`ST_INTERSECTS`、`ST_WITHIN`、`ST_CONTAINS`、`ST_TOUCHES`、`ST_OVERLAPS`、`ST_CROSSES` 等需要相交的谓词按 MBR 相交安全裁剪，`ST_DISJOINT` 保留全量精确路径，避免误剪枝。
- 新增 MBR 候选集合关系回归；已有空间 SELECT 过滤回归继续验证候选过滤与精确几何判断组合。
- 当前仍继续推进：true R-tree 页/增量结构、无主键结果的候选键传递、球面线段精确距离、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 871

- 继续补齐 JSON 高级函数：新增 `JSON_SCHEMA_VALID` 与 `JSON_SCHEMA_VALIDATION_REPORT`，支持受边界明确的 Schema 子集（boolean schema、type、required、properties、items、enum、const、minimum/maximum、字符串/数组长度、pattern、allOf/anyOf/oneOf/not）。
- 新函数已接入普通解释执行与 compiled-expression 路径；非法实例返回 0，报告函数返回 `valid` 与首个校验错误，未覆盖的完整 JSON Schema 关键字仍保持未宣称支持。
- 当前仍继续推进：true R-tree 页/增量结构、无主键结果的候选键传递、球面线段精确距离、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 858

- 继续补齐空间兼容执行链：空间列未出现在最终投影时，查询内部自动补入 probe 列，过滤完成后恢复原始结果列；支持 `ST_CONTAINS(query_geometry, column)` 的参数顺序以及带 SRID 的 `ST_GEOMFROMTEXT`，避免把可执行查询限制在单一 SQL 写法。
- 新增 `ST_DISTANCE_SPHERE` 的经纬度米制 haversine 计算和范围校验；补齐 `ST_AREA`、`ST_LENGTH`、`ST_CENTROID`、`ST_ENVELOPE`、`ST_SWAPXY`、`ST_LATITUDE`、`ST_LONGITUDE`、`ST_SRID(g, srid)`，并补齐 `MBRCONTAINS/MBRWITHIN/MBRINTERSECTS/MBREQUALS/MBRDISJOINT` 的包络谓词。
- P1 GTID SQL 兼容新增 `GTID_SUBSET()` 与 `GTID_SUBTRACT()`，基于合并区间计算，不展开大范围 GTID；索引 DDL 删除/重命名会同步清理/迁移 FULLTEXT/SPATIAL 辅助状态，避免持久化 metadata 残留。
- 针对性 engine/plan 空间、GTID 和高级索引 DDL 回归通过；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.236s`、manager `11.430s`、net `2.702s`、replication `2.639s`）。
- 新鲜集群报告 `reports/compatibility/p1-cluster-current-continuation857/cluster-report.json` 为 `PASS`；新鲜发布报告 `reports/compatibility/release-candidate-current-continuation857/release-candidate.json` 为 `GO`，build/unit/integration/go-core/crash-recovery/concurrency/observability 全部 PASS，隔离 Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 继续执行未完成项：真正 R-tree 增量维护、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游边界、跨节点强 fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵；FULLTEXT 和非 Connector/J 全量客户端仍按用户明确要求后置。

## Continuation 859

- 继续扩展空间函数矩阵：增加几何维度、集合/点/环访问器、闭合性判断、包络和坐标变换相关函数的执行与 compiled-expression 入口；修正标准多环 `POLYGON((outer),(hole))` 的 WKT 解析。
- 新增空间访问器与多环解析回归；修复后 `go test ./server/innodb/plan -run 'TestSpatial|TestGTID' -count=1` 通过。
- 复制、空间和高级索引 DDL 的实现继续保持现有发布门禁要求；全仓、集群、Connector/J 门禁将在本轮代码稳定后再次刷新。
- 本轮新鲜证据：全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.144s`、manager `9.986s`、plan `0.706s`、net `3.291s`、replication `2.330s`）；`reports/compatibility/p1-cluster-current-continuation859/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation859/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 860

- 继续补齐 native GTID/PITR 使用面：`BinlogWriter.NativeGTIDPosition` 支持按逻辑 UUID 或原生 SID 查询持久化 GTID event 的文件、起始位置和结束位置，重启后通过 native index rebuild 仍返回同一物理范围。
- 新增跨重启 GTID 物理定位回归；GTID 区间过滤、native dump、cluster smoke 和 Connector/J 路径保持不变。
- GTID 上游事件完整字段/跨源 UUID reconciliation、partial JSON/row-event 高级语义、真正 R-tree 增量维护、跨节点强 fencing、在线 DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵继续推进。
- 本轮最终新鲜证据：全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `98.390s`、manager `10.441s`、plan `1.131s`、net `2.764s`、replication `2.893s`）；`reports/compatibility/p1-cluster-current-continuation860/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation860/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS，总计 `09:25 min`。

## Continuation 861

- 继续扩展空间执行兼容：`ST_IsSimple`/`ST_PointOnSurface` 已接入解释执行和 compiled-expression 入口；`ST_INTERSECTS` 支持线-线、面-面及集合递归，`ST_CONTAINS/ST_WITHIN` 支持面-线；`ST_DISTANCE` 支持点、线、面之间的欧氏最短距离，并保留 SRID 不匹配错误。
- 新增空间简单性、面上点、线/面谓词和线距离回归；GTID 函数真实 SQL 投影/非法参数回归通过。
- 修复引擎空间查询对 WKT 文本 geometry 的降级问题：当存储值不是已解码 WKB 时，统一交给空间谓词 evaluator，而不是只按点坐标做 fallback；新增真实 `ExecuteQuery` 的线/面过滤回归。
- 空间谓词继续补齐 `ST_DISJOINT`，接入 SQL/compiled-expression 路径，并覆盖线/面/集合递归的相交判断；新增 plan 与真实引擎查询回归。
- 本轮仍继续推进 native partial JSON/完整 row-event 高级语义、真正 R-tree 增量维护、完整 GTID/PITR 上游互操作、跨节点强 fencing、在线 DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 和非 Connector/J 全量客户端继续按用户明确要求后置。
- 本轮全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.247s`、manager `10.277s`、plan `1.068s`、net `3.164s`、replication `2.367s`）；集群 smoke 与 Connector/J 发布门禁随后刷新。
- 最终新鲜门禁：`reports/compatibility/p1-cluster-current-continuation861/cluster-report.json` 的 result 为 `PASS`、exit code `0`；`reports/compatibility/release-candidate-current-continuation861/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 共 9 项全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS，总计 `09:20 min`。

## Continuation 862

- 继续扩展空间谓词矩阵：增加 `ST_TOUCHES`、`ST_OVERLAPS`，补充空间边界相交、同维度重叠和点落在多边形边界时的 `ST_INTERSECTS` 判断；SQL 正则、解释执行和 compiled-expression 白名单同步接入。
- 新增共享边/部分重叠多边形回归；engine/plan 的空间与 GTID 定向回归通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.799s`、manager `10.175s`、plan `0.617s`、net `3.306s`、replication `2.618s`）；`reports/compatibility/p1-cluster-current-continuation862/cluster-report.json` result 为 `PASS`、exit code `0`；Connector/J 发布门禁正在刷新。
- 最终新鲜发布证据：`reports/compatibility/release-candidate-current-continuation862/release-candidate.json` 为 `GO`，git revision `0e9a387682575d9f0df4825888c5064fb23d0c8b`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS，总计 `09:23 min`。

## Continuation 856

- 用户要求继续完成全部未收口能力后，继续补齐 native spatial/SQL 兼容边界：`TABLE_MAP_EVENT` 现在在能从显式类型或 WKB/SRID payload 证明几何子类型时写出 `GEOMETRY_TYPE` optional metadata；plan 层新增有边界的 `ST_GeomFromText`/`ST_GeomFromWKB`、`ST_MakePoint`、`ST_AsText`/`ST_AsWKB`、`ST_SRID`、`ST_GeometryType`、`ST_X/Y`、`ST_IsEmpty/IsValid`、点/多边形 `ST_Equals/Contains/Within/Intersects` 和点距离函数，compiled expression 也纳入同一 evaluator。
- 引擎空间查询路径现在可消费 decoded binary geometry，也保留文本 WKT 兼容回退；Query 类 native binlog 物理事件补充 `Info` 内容，避免 SHOW BINLOG EVENTS 只有类型没有详情。
- 定向 spatial/native/engine 回归、全仓 `go test ./... -count=1 -timeout 10m` 通过；`reports/compatibility/p1-cluster-current-continuation855/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation855/release-candidate.json` 为 `GO`，build/unit/integration/go-core、3 次 crash-recovery、并发、可观测性和 Connector/J `139` tests 全部 PASS，0 failures、0 errors、0 skipped。
- 当前仍继续推进：空间 R-tree 真正索引维护/更广空间函数、native partial JSON 和完整上游 GTID/PITR 互操作、跨节点强一致 MDL/硬 fencing、真正 online DDL、复杂任意 AST 组合及其余 P1/P3/P4 宽矩阵；FULLTEXT 与非 Connector/J 全量客户端仍按用户明确要求后置。

## Continuation 852

- 继续修正 native Rows event 的 row image 格式：每个 before/after image 现在分别写入 columns-present bitmap 和针对已出现列的 NULL bitmap；缺失列不再被错误编码成 NULL，标准复制解析器可以正确区分最小行镜像与实际 NULL 值，多行及 UPDATE image 同步适用。
- 继续补齐 legacy temporal/DDL 值边界：旧 `TIME` 按 MySQL `Field_time` 的 3-byte 有符号 `HHMMSS` 存储支持负值；零 `DATE/NEWDATE` 与零旧 `TIMESTAMP` 使用零值 binary payload；ENUM/SET 定义解析支持 SQL doubled-quote 转义。
- 新增 row-image present/null bitmap、负 legacy TIME、零日期/时间戳和 ENUM/SET SQL 转义回归；全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `111.596s`、manager `11.902s`、net `2.813s`、replication `3.390s`）；`reports/compatibility/p1-cluster-current-continuation852/cluster-report.json` 为 `PASS`；发布候选门禁将在本轮代码稳定后刷新。
- 当前仍不宣称总完成：legacy temporal 的全部时区/异常边界、GTID index 的完整上游格式与跨重启边界、native partial JSON/更复杂 extra-row-info、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 844

- 继续修正 native row-event 的 MySQL 类型码：`ENUM/SET` 使用 `MYSQL_TYPE_ENUM/SET`（247/248）；`TINY/MEDIUM/LONG/BLOB` 与对应 `TEXT` 使用 249/250/251/252；`VARBINARY` 使用 `MYSQL_TYPE_VAR_STRING`，`CHAR/BINARY` 使用 `MYSQL_TYPE_STRING`。
- `ENUM/SET` 的 TABLE_MAP metadata 现在包含真实类型码与 pack length，并支持 ordinal/label 值的固定宽度编码；四类 BLOB/TEXT 使用各自 1/3/4/2 字节 length prefix。新增逗号标签、转义定义和全部 LOB 宽度回归。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.857s`、manager `10.510s`、net `2.468s`、replication `2.717s`）；`reports/compatibility/p1-cluster-current-continuation844/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation844/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：`CHAR/BINARY` 的字符集/定长 padding、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 845

- 继续补齐 native `MYSQL_TYPE_STRING` 行镜像：`CHAR/BINARY` 的 TABLE_MAP metadata 现在按 MySQL `Field_string` 规则编码真实类型与长度高位，行值按字段长度选择 1/2 字节 little-endian length prefix；长度超过 255 的 `CHAR/BINARY` 不再错误地使用单字节前缀。
- 新增 `CHAR(10)`、`BINARY(300)` metadata 与实值回归；ENUM/SET、四类 BLOB/TEXT、VARCHAR、BIT、temporal、JSON、DECIMAL 和 geometry 路径继续通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.869s`、manager `10.523s`、net `2.374s`、replication `2.637s`）；`reports/compatibility/p1-cluster-current-continuation845/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation845/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：CHAR/BINARY 的字符集转换与 padding 细节、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 846

- 继续补齐 native GTID 过滤的 UUID/SID 边界：逻辑复制事件使用源 UUID 时，`COM_BINLOG_DUMP_GTID` 传入的 16 字节 wire SID 现在按同一 `nativeGTIDSID` 规则参与 GTID interval membership，避免非标准源 UUID 或格式差异造成已执行事务重复发送。
- 新增逻辑 UUID 与 wire SID 的过滤回归；原有 native 文件 SID、rotated-file、GTID interval、PITR 和 Connector/J 路径保持通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `101.630s`、manager `11.320s`、net `2.474s`、replication `2.819s`）；`reports/compatibility/p1-cluster-current-continuation846/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation846/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：GTID index 的完整上游格式、跨源 UUID reconciliation 与所有跨重启边界、native partial JSON/更复杂 extra-row-info、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 848

- 继续补齐 native 旧时态值编码：`YEAR` 使用 MySQL 1-byte 年份偏移（`year-1900`，零年为 0），旧 `TIMESTAMP` 使用 4-byte little-endian Unix seconds；不满足类型范围或无法安全解析时保留兼容回退。
- 新增 `YEAR` 数值/文本值和 `TIMESTAMP` 文本值回归；现代 temporal、BIT、ENUM/SET、CHAR/BINARY、LOB/VARCHAR、JSON、DECIMAL、geometry 以及 GTID/SID 过滤路径继续通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.812s`、manager `10.044s`、net `2.621s`、replication `2.718s`）；`reports/compatibility/p1-cluster-current-continuation848/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation848/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：旧 `TIME/NEWDATE` 的全量 legacy wire 语义、GTID index 的完整上游格式与跨重启边界、native partial JSON/更复杂 extra-row-info、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 849

- 继续补齐 native legacy temporal row image：`NEWDATE` 复用 3-byte packed date；旧 `TIME` 使用 MySQL `HHMMSS` 3-byte little-endian 表示，并对负值、非法分钟/秒和超过 838 小时的输入保守回退；`YEAR` 与旧 `TIMESTAMP` 路径保持兼容。
- 新增 `NEWDATE`、旧 `TIME`、`YEAR` 和旧 `TIMESTAMP` 的宽度/值回归；现代 temporal、LOB/字符串、JSON、DECIMAL、空间值、GTID/SID 与 Connector/J 路径继续通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.908s`、manager `10.478s`、net `2.625s`、replication `2.658s`）；`reports/compatibility/p1-cluster-current-continuation849/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation849/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：legacy `TIME` 的全部历史负值/时区边界、GTID index 的完整上游格式与跨重启边界、native partial JSON/更复杂 extra-row-info、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 850

- 继续收口 native legacy temporal row image：`NEWDATE` 使用与 `DATE` 一致的 3-byte packed date；旧 `TIME` 使用 MySQL `HHMMSS` 的 3-byte little-endian 表示，并对负值、非法分钟/秒和超过 838 小时的输入保守回退。
- 新增 `NEWDATE` 与旧 `TIME` 回归；`YEAR`、旧 `TIMESTAMP`、现代 temporal、LOB/字符串、JSON、DECIMAL、空间值、GTID/SID 和 Connector/J 路径继续通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.271s`、manager `10.413s`、net `2.504s`、replication `2.753s`）；`reports/compatibility/p1-cluster-current-continuation850/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation850/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：legacy 时态全部历史边界、GTID index 的完整上游格式与跨重启边界、native partial JSON/更复杂 extra-row-info、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 875

- 继续补齐空间类型的元数据闭环：新增一等 `metadata.TypeGeometry`，`Column`/`ColumnMeta` 验证、SQL 类型回显、值转换、DML 类型校验和 clustered record 编解码均识别 `GEOMETRY`；从 `.frm` 加载时仅规范化 GEOMETRY 类型，保留其它历史类型大小写行为，避免影响已有查询结果表示。
- 新增元数据与真实空间表加载回归，确认 `GEOMETRY` 不再依赖未知类型 fallback；现有空间谓词、空间索引 sidecar/R-tree、Connector/J 和其它类型路径保持通过。
- 当前仍继续推进：native partial JSON 的真实 SQL 变更捕获/副本应用、完整 row-event 解析互操作、真实 InnoDB R-tree 页结构、球面线段精确距离、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 877

- 继续扩展空间函数的真实 SQL 投影闭环：`ST_CONVEXHULL` 已支持点、共线线、多边形以及组合几何的二维坐标凸包，使用确定性 monotonic-chain 算法返回 Point/LineString/Polygon，并保留 SRID。
- 普通表达式、compiled-expression 及引擎投影查询均已接入并回归；三维坐标、地理椭球和完整 GIS 拓扑语义仍不宣称覆盖。
- 当前仍继续推进：native partial JSON 的真实 SQL 变更捕获/副本应用、完整 row-event 解析互操作、真实 InnoDB R-tree 页结构、球面线段精确距离、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 876

- 继续扩展空间函数矩阵：新增 `ST_CONVEXHULL`，对当前支持的二维 Point/LineString/Polygon/Multi*/GeometryCollection 坐标使用确定性的 monotonic-chain 算法返回点、线或闭合多边形凸包，并保留输入 SRID。
- 普通解释执行和 compiled-expression 均已接入，新增点、共线线、多边形凸包回归；不将三维坐标、地理椭球和完整 GIS 拓扑语义伪装成已支持。
- 当前仍继续推进：native partial JSON 的真实 SQL 变更捕获/副本应用、完整 row-event 解析互操作、真实 InnoDB R-tree 页结构、球面线段精确距离、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 847

- 继续收口 GTID 区间安全边界：保留 `GTIDIntervals` 的大范围、合并和 membership 语义；历史 `ParseGTIDSet` 及 `COM_BINLOG_DUMP_GTID` 的 materialized 兼容 helper 对超过 100 万序号的区间明确返回错误，避免意外展开造成内存耗尽，并提示调用方使用 interval form。
- 新增超大文本区间回归；正常 GTID set、native GTID/SID 过滤、跨文件 PITR 和复制重启路径保持通过。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `127.552s`、manager `14.010s`、net `2.673s`、replication `2.778s`）；`reports/compatibility/p1-cluster-current-continuation847/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation847/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：GTID index 的完整上游格式、跨源 UUID reconciliation 与所有跨重启边界、native partial JSON/更复杂 extra-row-info、空间类型 WKB/SRID/空间函数与 R-tree 索引维护、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 841

- 继续修正 native logical-to-physical position mapping：`DumpFileWithNativePositions` 现在按兼容 `RowChange` 分组计算一个逻辑 row event 对应的 TABLE_MAP/ROWS 物理事件数；grouped multi-row event 不再把后续 XID 映射成 ROW。
- 新增 grouped Rows event 的物理位置连续性回归，验证 GTID、TABLE_MAP、ROWS、XID 的类型和边界仍按真实 native frame 对齐。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `100.901s`、manager `10.193s`、plan `1.076s`、net `2.812s`、replication `2.654s`）；`reports/compatibility/p1-cluster-current-continuation841/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation841/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：空间类型的 WKB/SRID/空间函数与 R-tree 索引维护、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 842

- 继续统一 native binlog 协议路径：网络层备用 `encodeNativeBinlogEventPayloads` 现在复用 replication 的 schema-aware builder，直接协议编码不再丢失 `ColumnTypes`、JSON binary value、temporal precision、BIT/LOB metadata 或 grouped rows。
- `DumpFile` 对不存在的物理 binlog 文件返回明确错误，与 native dump 路径保持一致；新增缺失文件错误契约回归。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `101.758s`、manager `10.922s`、plan `1.137s`、net `2.872s`、replication `2.732s`）；`reports/compatibility/p1-cluster-current-continuation842/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation842/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。
- 当前仍不宣称总完成：空间类型的 WKB/SRID/空间函数与 R-tree 索引维护、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 843

- 继续收口 native dump/PITR 与协议编码的一致性：`DumpFile`、`NativeDumpFile` 和 `RestoreNativeUntilPositions` 对缺失物理 binlog 文件统一返回稳定的 `native binlog file ... does not exist` 错误；grouped Rows 的逻辑/物理边界和网络 schema-aware row codec 一并纳入回归。
- 新增缺失 boundary 文件、缺失 dump 文件和 grouped row position 映射回归；不改变正常 rotate、GTID interval filtering、checksum validation 和 Connector/J 路径。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `99.435s`、manager `10.650s`、plan `1.149s`、net `2.698s`、replication `2.912s`）；`reports/compatibility/p1-cluster-current-continuation843/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation843/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven BUILD SUCCESS。

## Continuation 864

- 继续扩展空间函数矩阵：plan 层新增 `ST_GeomFromGeoJSON`/`ST_GeometryFromGeoJSON` 与 `ST_AsGeoJSON`，支持 Point、LineString、Polygon、Multi*、GeometryCollection 的 GeoJSON 转换、SRID 参数和 compiled-expression 路径。
- 继续收口 P1-OPT-005/P1-EXE-004 的日期时间函数覆盖：新增 `ADDTIME`、`SUBTIME` 与双参数 `TIMESTAMP` 构造，支持微秒和跨日进位，并通过真实表达式解析路径验证。
- 新增 GeoJSON WKT/SRID/JSON 结构回归、compiled evaluator 回归以及真实引擎表达式回归；空间、高级索引、plan 专项保持通过。
- 当前仍继续推进：空间 MBR sidecar 的候选裁剪/真实 R-tree 页结构、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 865

- 继续扩展空间距离函数：`ST_DISTANCE_SPHERE` 现在支持有顶点的线、面和集合，计算有效顶点对的最小球面距离；保留经纬度范围、空几何和 SRID 不匹配校验。
- 新增线几何球面距离回归；全部空间 plan/compiled、空间索引 DDL/DML 和 FULLTEXT 既有回归保持通过。
- 当前仍继续推进：球面线段精确距离与真实 R-tree 页结构/候选裁剪、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 863

- 继续收口 `PX-IDX-002` 空间索引生命周期：`SPATIAL INDEX` 创建时初始化版本化 MBR sidecar；INSERT、UPDATE、DELETE、REPLACE 和 `ON DUPLICATE KEY UPDATE` 提交后重建空间条目，确保数据变化与可恢复索引状态一致。
- `ALTER TABLE ... ADD SPATIAL INDEX` 现在可在已有数据上构建条目；普通 ALTER 的 DROP/RENAME INDEX 同步删除或迁移 FULLTEXT/SPATIAL 辅助状态，避免只更新索引字典而遗留旧 sidecar。
- 新增空间索引 INSERT/UPDATE/DELETE 以及 ALTER 构建/重命名/删除回归；engine 空间、高级索引与 plan 空间定向测试通过，`git diff --check` 仅保留工作树现有的 LF/CRLF 提示。
- 当前仍继续推进：将 MBR sidecar 接入候选裁剪/真实 R-tree 页结构、native partial JSON/完整 row-event 高级语义、完整 GTID/PITR 上游互操作、跨节点强 fencing、真正 online DDL、复杂 AST 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。
- 当前仍不宣称总完成：空间类型的 WKB/SRID/空间函数与 R-tree 索引维护、native partial JSON/更复杂 extra-row-info、GTID/PITR 全语义、跨节点 MDL/fencing、在线 DDL、复杂 AST、剩余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 884

- 继续扩展空间结果运算：新增 `ST_Intersection` 的点/点、点/几何、轴对齐矩形多边形相交，支持面/线/点退化结果及标准空 `GEOMETRYCOLLECTION`；新增 `ST_Union` 的点集合、包含矩形和不相交矩形结果；新增 `ST_Difference` 的点/矩形安全子集及 `ST_SymDifference` 的点集合结果。
- 空间内部 WKB 对空 Point 使用标准 NaN 坐标表示，`POINT EMPTY` 现在可继续经过 `ST_AsText`、集合运算和结果集编码；普通表达式、compiled-expression 和真实引擎投影入口均已接入。
- 本轮专项与全仓回归通过：`go test ./... -count=1 -timeout 10m`，engine 约 `101.591s`；`reports/compatibility/p1-cluster-current-continuation883/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation883/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 复杂多边形布尔运算、地理 SRS 的精确拓扑、真实 InnoDB R-tree 页结构、partial JSON 二进制 diff 直接原子应用、完整 native row-event/GTID/PITR 上游互操作、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵仍未宣称完成；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 883

- 继续扩展空间算子：新增 `ST_Buffer` 的二维 Cartesian 点缓冲和两点 `LineString` capsule 缓冲，支持零距离原值返回、SRID 保留、空点结果和负距离/不支持几何的明确错误；结果通过现有内部 geometry/WKT/WKB、普通表达式、compiled-expression 与引擎投影路径。
- 新增 plan 与真实 engine 投影回归；点缓冲面积与圆理论值、线缓冲面积与 capsule 理论值均有误差边界断言。复杂折线、多边形/集合缓冲、地理 SRS strategy 与完整 GEOS 拓扑仍不宣称完成。
- 最新 Connector/J 发布门禁 `reports/compatibility/release-candidate-current-continuation882/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`；本轮空间专项回归通过。
- 仍明确未宣称完成：`ST_Buffer` 的全几何/地理/strategy 语义、replica 对 partial JSON 二进制 diff 的直接原子应用、完整 native row-event 解析互操作、完整 GTID/PITR 上游互操作、真实 InnoDB R-tree 页结构、跨节点强 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 885

- 继续收口 native row-event 消费链路：新增有状态 `NativeBinlogDecoder`，按 TABLE_MAP 保存列名、类型、metadata 和 unsigned 标记，解析 WRITE/UPDATE/DELETE_ROWS_EVENTv2 的列位图、NULL 位图、常见整数/浮点/字符串/日期时间类型及 extra-row-info。
- 解码器支持 `PARTIAL_UPDATE_ROWS_EVENT` 的对象成员 diff，并使用 before image 重建 JSON after image；新增 INSERT、UPDATE、temporal 和 partial JSON 原生帧回归，未改变既有 logical JSONL/statement replay 路径。
- 本轮专项与全仓回归通过：`go test ./server/replication -count=1`、`go test ./... -count=1 -timeout 10m`（engine `103.312s`）；最新发布门禁 `reports/compatibility/release-candidate-current-continuation885/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍明确未宣称完成：上游全部类型/小数/复杂 enum-set metadata 的完整 native row-event 互操作、跨源 GTID/PITR 全语义、真正 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 886

- 继续扩展 `NativeBinlogDecoder` 的类型消费覆盖：支持 NEWDECIMAL 定点值解码、TIME/DATETIME2/TIMESTAMP2 小数精度还原，以及 INSERT/UPDATE/DELETE 的 NULL/unsigned 位图语义。
- 新增金额/定点数和 temporal 原生帧回归；解码器仍保持有状态 TABLE_MAP 生命周期，既有 native writer、logical JSONL、GTID 和 statement replay 不变。
- 本轮专项回归通过：`go test ./server/replication -run 'TestNativeDecoder|TestApplyJSONPartialUpdatesReconstructsObject' -count=1`。
- 仍明确未宣称完成：上游全部类型及复杂 ENUM/SET optional metadata 的完整 native row-event 互操作、跨源 GTID/PITR 全语义、真正 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 887

- 继续补齐 native TABLE_MAP/ROWS 消费：`NativeBinlogDecoder` 现在读取 ENUM/SET 的 real type、pack length 与 optional member metadata，恢复 ENUM 文本和 SET 位集合；同时保留对未知 optional metadata 的安全忽略。
- Continuation 888：继续修正 native 行值消费边界，`CHAR`/`BINARY` 的字符串长度前缀按 TABLE_MAP metadata 选择 1/2 字节；VARBINARY、BLOB 系列和 GEOMETRY 保留为二进制 `[]byte`，避免把非文本列误转成字符串；解码后的 RowChange 同时回填可用的 ColumnTypes。该项新增 CHAR 原生帧回归，`go test ./server/replication -count=1 -timeout 5m` 通过。
- 同一轮补充 `DecodeNativeBinlogTransactions`/`Source.DecodeNativeDumpFrom`，按 GTID/XID 重组 native 事务并附带 ROWS/QUERY 内容；集群 smoke `reports/compatibility/p1-cluster-current-continuation888/cluster-report.json` 为 `PASS`，完整发布门禁 `reports/compatibility/release-candidate-current-continuation888/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 最新工作树复验：`reports/compatibility/release-candidate-current-continuation889/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`；native decoder 事务重组、定点数、ENUM/SET、CHAR/二进制列和时间类型改动均通过门禁。
- 新增 ENUM/SET 原生帧回归；`go test ./server/replication -count=1 -timeout 5m` 通过，native writer、logical stream、GTID 和副本 statement/row apply 路径未改变。
- 当前 native 解码已覆盖项目写端的常见整数、浮点、字符串、定点数、时间、JSON partial、ENUM/SET、NULL/unsigned 和行级动作；上游全部类型、复杂 metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵仍未宣称完成；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 888

- 继续收口 native replication 消费兼容：`NativeBinlogDecoder` 现在同时接受 v1/v2 `WRITE/UPDATE/DELETE_ROWS_EVENT` 布局，并补齐旧 `MYSQL_TYPE_DECIMAL` 类型码的定点数消费；旧 v1 行事件不再被误当作含 extra-row-info 的 v2 事件。
- `Replica.Apply` 的提交分支现在也归集事务对象携带的 `Changes/Statements`，因此 `DecodeNativeBinlogTransactions` 生成的事务可直接走同一原子 row/statement hook、GTID 去重和持久化状态路径；新增 native `ApplyNative` exactly-once 回归。
- 新增 Source-aware native decoder：已知 `Source.UUID` 时，非十六进制源名经 native SID 哈希后仍能还原为逻辑源身份，避免跨重启/再次拉流时 GTID 身份漂移；新增非 UUID 源 native dump 回归。
- 本轮 replication 专项回归：`go test ./server/replication -count=1 -timeout 5m` 通过；完整全仓、集群 smoke 和发布候选门禁在本轮修改完成后刷新。上游全部类型/复杂 metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵仍未宣称完成；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 889

- native 副本不再只提供“解码”能力：新增 `Replica.ReplicateNativeFrom`，按物理 binlog 文件/position 拉取、使用 Source-aware decoder 恢复源 UUID，并复用现有原子 row/statement apply、GTID 去重、状态持久化、错误与 source-position 更新。
- 新增跨 native 物理拉流的 exactly-once 回归：首次拉流应用事务，重复从观察到的位置拉流不重复应用；非 UUID 源名仍能在 Source-aware 路径保持逻辑 GTID 身份。
- 新增的 native v1/v2 行事件、旧 DECIMAL、事务提交应用和副本物理拉流专项均通过；完整全仓、集群 smoke 和包含本轮全部改动的发布候选门禁待本轮收口后刷新。上游全部类型/复杂 metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵仍未宣称完成；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 890

- 本轮最新工作树验证完成：`go test ./... -count=1 -timeout 10m` 通过（engine `102.133s`、manager `9.941s`、replication `2.802s`）；cluster smoke `reports/compatibility/p1-cluster-current-continuation888/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation891/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 共 9 项全部 PASS；Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`，JDBC 总耗时约 `09:27 min`。
- 当前可交付范围进一步确认：P0 单机核心、P0 Connector/J、P1 基础集群与本轮 native 物理复制消费路径均有源码和回归证据；仍未宣称完整上游 binlog/GTID/PITR 互操作、所有 native 类型/复杂 metadata、真实 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式、全量窗口/函数/collation 矩阵和其余 P1/P3/P4 宽矩阵。FULLTEXT 与非 Connector/J 全量客户端继续按用户明确要求后置。

## Continuation 891

- 最新工作树复验：`gofmt -d` 无输出，`go test ./... -count=1 -timeout 10m` 通过（engine `101.256s`、manager `10.634s`、replication `2.142s`）；此前同一工作树的 cluster smoke 为 `PASS`，release candidate 891 为 `GO`。
- native 复制能力本轮已从“可编码/可解码”推进到“可按物理位置拉流并原子应用”，但仍明确保持协议边界：未把完整上游 binlog/GTID/PITR 互操作、所有 native 类型和复杂 optional metadata、在线 DDL、真实 R-tree 页结构、复杂 SQL 全组合、FULLTEXT 或非 Connector/J 全量客户端标记为完成。

## Continuation 892

- 继续补齐空间距离语义：`ST_DISTANCE_SPHERE` 现在按二维经纬度的大圆弧计算点到线段、线段相交和有顶点几何之间的最小球面距离，不再只比较几何顶点；同时支持 MySQL 形式的第三个 `radius` 参数，并校验半径为有限正数。
- 新增内部几何的线段内部最近点、弧段相交、自定义半径和错误边界回归；默认地球半径保持既有结果，空几何、越界经纬度和 SRID 不匹配仍返回原有契约。
- native row-event 消费再补齐 MySQL `BOOL` 类型码（244）的固定宽度值解码和类型回显；不对尚未具备存储层语义的复杂类型伪造 metadata 或值转换。
- 本轮完成后仍需刷新全仓 Go、集群 smoke 和包含本轮修改的 Connector/J 发布门禁；完整上游 native 类型/复杂 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式和其余 P1/P3/P4 宽矩阵仍未宣称完成；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 893

- 本轮验证收口：`go test ./... -count=1 -timeout 10m` 全部通过；`reports/compatibility/p1-cluster-current-continuation892/cluster-report.json` 为 `PASS`，覆盖 1 主 2 从复制、回滚隔离和副本提升；`reports/compatibility/release-candidate-current-continuation892/release-candidate.json` 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 共 9 项全部 PASS。
- Connector/J 本轮实际执行 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:23 min`；新增球面线段距离、半径参数、弧段相交和 native BOOL 解码未造成既有 P0/P1 回归。
- 当前仍明确保持边界：完整上游 native 类型/复杂 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式、全量窗口/函数/collation 矩阵和其它 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 894

- P3 fencing 继续收口：fenced source 现在同时阻断客户端 DML/DDL，避免节点虽然停止暴露 binlog，却仍能提交新的本地写入；复制回放 session 仍可安全执行，以保证 fencing 不破坏已在途事务的受控应用。
- native 事务消费继续补齐：`NativeBinlogDecoder` 支持 `ANONYMOUS_GTID_EVENT` 以及无 GTID_EVENT 的 QUERY/XID 匿名事务，按物理文件和起始位置生成稳定的内部事务身份，使副本仍能执行 exactly-once 去重，而不会把匿名事务伪装成真实源 GTID。
- 新增 fenced source 真实引擎回归和 anonymous native transaction 回归；本轮全仓、集群和发布门禁将在继续收口后刷新。

## Continuation 895

- native replication 消费继续收口：`PREVIOUS_GTIDS_EVENT` 现在按 MySQL 半开区间格式解析并保留多 SID 历史，支持 Source-aware 非 UUID 映射；非法计数、区间和尾随数据会明确失败。
- `TRANSACTION_PAYLOAD_EVENT` 现在支持 TLV 头、NONE payload、ZSTD payload，以及无 checksum 的内嵌 QUERY/TABLE_MAP/ROWS/XID 事件展开；内嵌事件会重新进入事务解码和 exactly-once 应用链路。单阶段 XA prepare 可作为提交终点，普通 prepared XA 在当前没有 XA recovery/prepare store 的前提下 fail-closed，避免误提交或静默丢数据。
- 新增 PREVIOUS_GTIDS、无压缩/ZSTD payload、one-phase XA、prepared-XA fail-closed 回归；`go test ./... -count=1 -timeout 10m` 通过，engine `102.442s`、manager `10.453s`、replication `3.625s`；集群 smoke `reports/compatibility/p1-cluster-current-continuation895/cluster-report.json` 为 `PASS`；发布门禁 `reports/compatibility/release-candidate-current-continuation895/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`，总计 `09:26 min`。
- 当前仍继续推进：完整上游 GTID_TAGGED/所有 binlog 控制事件、XA prepare/recovery、全部 native 类型与 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构、跨节点 MDL/硬 fencing、真正 online DDL、复杂 SQL/存储格式及剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 896

- native dump 过滤继续收口：`NativeDumpFileWithIntervals` 现在识别 `ANONYMOUS_GTID_EVENT`，按 `anonymous:<binlog-file>` 与起始物理位置参与 executed interval 过滤，并在跳过时完整跳过该匿名事务的事件组。
- native 事务解码新增 statement-based `QUERY_EVENT` 边界：`BEGIN`/`START TRANSACTION` 不再作为业务语句提交，`COMMIT` 可直接收口匿名事务，`ROLLBACK` 丢弃当前事务，`ROLLBACK TO SAVEPOINT` 不会误结束事务；压缩 payload 内嵌 QUERY 复用同一规则。
- 新增匿名事务 dump 过滤、SBR commit/rollback 边界回归；本轮变更已通过 replication 定向测试。Continuation 895 的全仓 Go、集群 smoke 和 Connector/J 发布门禁证据继续有效，新增代码将在下一轮门禁刷新。

## Continuation 897

- native replication 继续补齐 MySQL 8.4 控制事件：`GTID_TAGGED_LOG_EVENT` 现在解析 serialization archive 的版本、字段边界、SID、GNO、tag、逻辑时钟及可选元数据，并将 tag 纳入内部 GTID identity；Source-aware 路径仍能还原非 UUID source 名称，非法版本、字段、长度和 tag 会 fail-closed。
- `NativeDumpFileWithIntervals` 现在按 tagged GTID 做完整事务过滤，`ParseGTIDIntervals`/`GTIDSet` 保留 `uuid:tag:interval` 形式；`MYSQL_TYPE_NULL` 原生行值不再消耗错误字节，压缩事务中的事务上下文/视图变更/heartbeat 控制事件在校验后安全忽略。
- 新增 tagged GTID 解码、损坏帧、tagged dump 过滤、GTID interval round-trip 和 `MYSQL_TYPE_NULL` 回归；`go test ./server/replication -count=1 -timeout 5m` 通过。
- 本轮最新验证：`go test ./... -count=1 -timeout 10m` 通过（engine `103.321s`、manager `11.096s`、replication `3.037s`）；`reports/compatibility/p1-cluster-current-continuation897/cluster-report.json` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation897/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`，总计 `09:25 min`。
- 当前仍继续推进：完整上游所有 binlog 控制事件及 checksum/FDE 变体、XA prepare/recovery、全部 native 类型与 optional metadata、跨源 GTID/PITR 全语义、真正 InnoDB R-tree 页结构、跨节点硬 fencing、真正 online DDL、复杂 SQL/存储格式及剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 898

- 继续收口 native binlog 消费互操作：`NativeBinlogDecoder` 现在明确消费完整流中的 `STOP/ROTATE/FORMAT_DESCRIPTION/INCIDENT/HEARTBEAT/IGNORABLE/ROWS_QUERY/TRANSACTION_CONTEXT/VIEW_CHANGE/HEARTBEAT_LOG` 等已知控制事件；压缩 `TRANSACTION_PAYLOAD_EVENT` 内嵌这些控制帧时不再误报 unsupported，同时仍对帧长度、checksum 和事务边界执行校验。
- 扩展 `TABLE_MAP_EVENT` optional metadata 解码：补齐 `DEFAULT_CHARSET`、`COLUMN_CHARSET`、`GEOMETRY_TYPE`、`SIMPLE_PRIMARY_KEY`、`PRIMARY_KEY_WITH_PREFIX`、ENUM/SET charset、`COLUMN_VISIBILITY` 和 `VECTOR_DIMENSIONALITY` 的结构化保存；native writer 对 VECTOR 列按类型声明或 float32/float64/原始字节推导 dimensionality 并写出对应 TLV。
- native dump 从物理事务中间 position 开始时不再输出孤立 ROWS/XID 帧：已开始的事务会被完整跳过，随后从下一个 GTID/anonymous transaction 边界继续，避免副本收到不可应用的半事务。
- clustered record 补齐 `CHAR/BINARY` 定长边界：CHAR 按字符长度校验并保持规范化读取，BINARY 按字节长度补零并在溢出时拒绝；VARCHAR/VARBINARY 仍保持变长语义。
- 本轮定向回归通过：replication native/control/metadata/dump tests、record codec fixed-length tests；随后 `go test ./... -count=1 -timeout 10m` 全部通过，engine 约 `183.322s`、manager `13.928s`、replication `3.169s`。
- 当前仍继续推进：XA prepare/recovery、全部 native 类型的上游 wire fidelity 与 optional metadata 校验、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构和物理候选扫描、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式、完整 Performance Schema 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 899

- 修复真实 SQL 执行路径的定长列兼容：`.frm` 历史类型名保持原有大小写与数值存储行为；`CHAR/BINARY` 的 clustered record 编解码改为大小写不敏感识别，确保 `.frm` 读取出的 `char`/`binary` 不会绕过定长规范化。`CHAR` 查询结果去除尾随填充空格，`BINARY` 按字节长度补零，`VARCHAR/VARBINARY` 继续保持变长语义。
- native replication 补齐 prepared XA 的 decoder 状态机：两阶段 `XA_PREPARE_EVENT` 保存完整事务，后续 `XA COMMIT` 才生成可应用的 `EventCommit`，`XA ROLLBACK` 清理 prepared 状态；未知 prepared XID 仍 fail-closed，避免误提交或静默丢数据。
- 新增真实引擎 CHAR/BINARY 回归与 prepared-XA commit 回归；`go test ./server/innodb/engine -count=1 -timeout 5m` 通过（约 `97.822s`），`go test ./server/replication -count=1 -timeout 5m` 通过（约 `1.859s`），全仓 `go test ./... -count=1 -timeout 10m` 通过（engine 约 `102.438s`、manager 约 `10.818s`、replication 约 `2.783s`）。
- 集群 smoke `reports/compatibility/p1-cluster-current-continuation899/cluster-report.json` 为 `PASS`；发布候选 `reports/compatibility/release-candidate-current-continuation899/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:36 min`。
- 当前仍继续推进：全部 native 类型的上游 wire fidelity 与 optional metadata 校验、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构和物理候选扫描、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式、完整 Performance Schema 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 900

- 继续推进空间索引物理执行：空间 MBR/R-tree sidecar 产生的候选 storage keys 现在通过 `ExecutionContext` 下推到单表 clustered scan，在 clustered record 解码和结果集构造前跳过非候选行；没有主键或 `ST_DISJOINT` 等无法安全裁剪的查询仍回退完整扫描并执行精确几何谓词，避免错误丢行。
- 修复方向保持最小化：没有把所有 `.frm` 类型强制改成大写，避免破坏既有数值列持久化表示；只对 `CHAR/BINARY` 定长判断做大小写不敏感处理。
- 新增空间候选下推专项回归；`go test ./... -count=1 -timeout 10m` 通过（engine 约 `104.400s`、manager 约 `10.640s`、replication 约 `2.485s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation900/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation900/release-candidate.json` 为 `GO`：clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、jdbc 共 9 项全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:38 min`。
- 当前仍继续推进：真实 InnoDB R-tree 页结构与增量页维护、全部 native 类型的上游 wire fidelity 与 optional metadata 校验、跨源 GTID/PITR 全语义、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式、完整 Performance Schema 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 903

- native 行事件继续补齐 legacy wire 类型：`MYSQL_TYPE_DATETIME` 现在按 MySQL 旧格式使用 8 字节 little-endian `YYYYMMDDHHMMSS` 编码/解码，零日期保持 `0000-00-00 00:00:00`，不再被误当作长度编码字符串。
- `NativeBinlogDecoder` 的 row-image API 对已知上游控制事件显式安全消费，对未知事件 fail-closed；新增完整事件类型名称映射、顶层控制帧和未知帧回归。
- replication 专项已通过；全仓 Go、集群 smoke 和含本轮代码的 Connector/J 发布门禁仍需继续刷新，剩余 native 复杂 metadata、跨源 GTID/PITR 全语义、真实 R-tree 页、跨节点硬 fencing、online DDL、复杂 SQL/Performance Schema 及其他后置宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 902

- 继续收敛 native XA recovery：prepared `XA_PREPARE_EVENT` 不再只存在于单次解码调用内；decoder 暴露防御性 prepared-XA 快照，replica state 将未决 XA 事务与 executed GTID/row state 一起原子持久化，后续 `XA COMMIT/ROLLBACK` 可跨 native dump 分批到达并跨副本重启恢复。
- 新增标准 TABLE_MAP 列绑定入口：`NativeTableMapResolver` 可用本地数据字典为不携带列名的上游 TABLE_MAP 提供真实列顺序；无 resolver 时仍保持 `column_N` 兼容回退，列数和空列名错误会在 ROWS_EVENT 应用前 fail-closed。
- 新增跨 dump 调用、跨副本重启、标准 TABLE_MAP 本地列绑定回归；replication 专项通过。
- 当前仍继续推进：完整上游 native 类型/metadata 互操作、跨源 GTID/PITR 全语义、真正 R-tree 页与物理候选扫描、跨节点外部 fencing、真正 online DDL、复杂 SQL/存储格式、完整 Performance Schema 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户要求后置。

## Continuation 901

## Continuation 904

- 副本新增持久化 relay 事件快照：新应用的逻辑事务事件随 GTID/AppliedRows 一起落盘，重复 GTID 不重复追加，重启后仍可观察。
- 新增 `SHOW RELAYLOG EVENTS` SQL 入口，支持默认 relay 文件名、`FROM`、`LIMIT`/offset 以及 INSERT/UPDATE/DELETE/Query/Xid 事件展示，并接入真实 replica runtime。
- replication 与 engine 定向回归通过；全仓 Go、集群 smoke 和 Connector/J 发布门禁仍继续刷新，FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

- 为空间候选下推补充独立回归，验证候选 storage key 在 clustered record 投影前过滤，保留无主键和不可安全裁剪谓词的完整扫描回退。
- 本轮最终验证：全仓 `go test ./... -count=1 -timeout 10m` 通过（engine 约 `103.077s`、manager 约 `10.050s`、replication 约 `2.282s`）；发布候选 `reports/compatibility/release-candidate-current-continuation901/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:37 min`。
- 当前仍继续推进：真实 InnoDB R-tree 页结构与增量页维护、全部 native 类型的上游 wire fidelity 与 optional metadata 校验、跨源 GTID/PITR 全语义、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式、完整 Performance Schema 与剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 905

- Performance Schema 运维兼容继续收口：新增 `setup_objects` 的对象类型配置视图和 `setup_timers` 计时器视图；`setup_objects` 支持按 `OBJECT_TYPE` 查询和更新 `ENABLED/TIMED`，与已有 consumers/instruments/actors 配置更新入口保持一致。
- 新增 `setup_objects/setup_timers` 查询与更新回归；本轮全仓 `go test ./... -count=1 -timeout 10m` 通过，集群 smoke `reports/compatibility/p1-cluster-current-continuation905/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation905/release-candidate.json` 为 `GO`：clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、jdbc 共 9 项全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:37 min`。
- 当前仍继续推进：完整 Performance Schema 生命周期与 wait/memory/socket/file 矩阵、全部 native 类型的上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 906

- Performance Schema 连接观测继续收口：新增 `performance_schema.users/accounts/hosts` 视图，从实时 session 快照聚合当前连接数、用户/主机维度，并支持基础 `USER/HOST` 过滤；无历史连接计数时只报告当前可证明的活动计数。
- 新增多视图连接聚合回归；最新全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `102.475s`、manager `10.402s`、net `2.786s`、replication `2.157s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation906/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation906/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:36 min`。
- 当前仍继续推进：完整 Performance Schema 生命周期与 wait/memory/socket/file 矩阵、全部 native 类型的上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 907

- Performance Schema 运维视图继续补齐：新增 `setup_threads`，暴露实时连接对应的线程类型、连接 ID、用户、主机、连接方式、启用状态和 history 状态；与 `users/accounts/hosts` 的活动连接汇总保持同一 session 来源。
- 新增 `setup_threads` 当前连接回归；最新全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `103.164s`、manager `11.088s`、net `2.788s`、replication `4.052s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation907/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation907/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:36 min`。
- 当前仍继续推进：完整 Performance Schema 生命周期与 wait/memory/socket/file 矩阵、全部 native 类型的上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及剩余 P1/P3/P4 宽矩阵；FULLTEXT 查询语义与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 908

- Performance Schema 等待与阶段诊断继续补齐：新增 `events_waits_summary_global_by_event_name`、`events_waits_summary_by_thread_by_event_name` 和 `events_stages_summary_global_by_event_name`，分别从真实锁等待图和已记录语句阶段聚合 `COUNT/SUM/MIN/AVG/MAX_TIMER_WAIT`，不虚构运行时没有保留的历史 IO 数据。
- 新增等待汇总与阶段汇总回归；本轮全仓 Go `go test ./... -count=1 -timeout 10m` 通过（engine `102.179s`、manager `9.727s`、net `2.770s`、replication `2.187s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation908/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation908/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:36 min`。
- 当前仍继续推进：Performance Schema 线程级阶段汇总、连接属性及 wait/memory/socket/file 其余矩阵、全部 native 类型上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及其他 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户要求后置。

## Continuation 909

- Performance Schema 阶段观测继续补齐：新增 `events_stages_summary_by_thread_by_event_name`，按真实连接线程聚合阶段执行次数和计时；新增 `session_connect_attrs` 查询入口，并把握手中声明的 `CLIENT_CONNECT_ATTRS` 属性保存到真实 server session，支持按 `PROCESSLIST_ID` 查询。
- 新增线程级阶段汇总、连接属性视图回归，并保持既有 wait/stage 汇总回归通过；最新全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `103.041s`、manager `10.478s`、net `2.554s`、replication `2.002s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation909/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation909/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `09:33 min`。
- 当前仍继续推进：Performance Schema wait/memory/socket/file 其余矩阵、完整 native 类型上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及其他 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户要求后置。

## Continuation 910

- Performance Schema 网络观测继续补齐：新增 `socket_instances`、`socket_summary_by_instance`、`socket_summary_by_event_name`，从当前 server session 的真实 remote address 输出 socket event、线程/套接字 ID、IP、端口、状态及当前可证明的读写/杂项计数。
- 网络认证成功后保存连接的 `remote_addr`，`CLIENT_CONNECT_ATTRS` 连接属性和 socket 端点可由后续 Performance Schema 查询复用；新增 socket 实例/汇总回归，engine 与 net 专项通过。
- 最新全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `108.212s`、manager `9.991s`、net `2.857s`、replication `2.035s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation910/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation910/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，门禁总耗时约 `11:41 min`。
- 当前仍继续推进：Performance Schema file/table IO、memory 生命周期与其他 wait 矩阵、完整 native 类型上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及其他 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户要求后置。

## Continuation 911

- Performance Schema 表级观测继续补齐：新增 `table_lock_waits_summary_by_table`，从真实行锁/MDL wait-for edge 汇总当前等待次数和等待计时；新增 `table_io_waits_summary_by_table` 与 `table_io_waits_summary_by_index_usage`，基于有界的真实语句 history 投影近期表访问、读写及 INSERT/UPDATE/DELETE/FETCH 计数，不虚构未保留的底层页 IO 历史。
- 新增 `memory_summary_by_thread_by_event_name` 和 `performance_timers` 兼容入口；线程 memory 视图仅报告当前可观察线程及零分配计数，计时器频率/分辨率使用稳定的运行时兼容值。
- 新增表锁、表 IO、线程 memory、performance timers 回归；`go test ./server/innodb/engine -count=1 -timeout 10m` 通过（约 `119.345s`），全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `162.052s`）。
- 集群 smoke `reports/compatibility/p1-cluster-current-continuation911/cluster-report.json` 为 `PASS`；发布候选 `reports/compatibility/release-candidate-current-continuation911/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `11:03 min`。
- 当前仍继续推进：Performance Schema 真正 memory allocation lifecycle、底层 file/table IO 历史和其余 wait/instance 矩阵、全部 native 类型上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及其他 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 912

- Performance Schema 其余常用元数据入口继续补齐：新增 `host_cache`、`mutex_instances`、`rwlock_instances`、`objects_summary_global_by_type` 查询路由；host cache 使用当前真实连接端点并对未保留的错误/时间历史返回零或 NULL，mutex/rwlock 仅在运行时有真实实例记录时才应扩展，避免伪造锁持有者。
- 新增辅助 Performance Schema 路由回归；当前工作树全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `115.746s`、manager `13.320s`、replication `6.589s`），集群 smoke `reports/compatibility/p1-cluster-current-continuation912/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation912/release-candidate.json` 为 `GO`，9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `10:54 min`。
- 当前仍继续推进：Performance Schema 真正 memory allocation lifecycle、底层 file/table IO 历史和其余 wait/instance 矩阵、全部 native 类型上游 wire fidelity 与 optional metadata、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构与增量页维护、跨节点强 fencing/MDL、真正 online DDL、复杂 SQL/存储格式及其他 P1/P3/P4 宽矩阵；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 913

- native row-event wire fidelity 继续收口：`MYSQL_TYPE_STRING`（CHAR/BINARY）按 TABLE_MAP 字段宽度使用定长 row image；CHAR 补空格、BINARY 补零，decoder 固定宽度消费并保留 SQL-facing CHAR 结果；ENUM/SET 的 real type 与 packed width 不受影响。新增 `MYSQL_TYPE_NULL` writer/type 回显。
- 新增 `NativeTableMapSchemaResolver`，可在标准上游 TABLE_MAP 不携带列名/SQL 类型时绑定本地列名和类型；`ApplyNative`、`ReplicateNativeFrom`、`DecodeNativeDumpFrom` 均提供 schema-aware 入口，BINARY 在有本地类型证据时保留定长零字节。
- Performance Schema wait 观测继续补齐：LockManager 保留最近 128 条已完成 row-lock wait 的真实边、等待时长和 blocker；`events_waits_history/history_long` 不再复用当前等待，wait summary 与 table-lock summary 会合并已完成等待和当前等待。metadata-lock 历史仍不伪造。
- 新增 native string/schema resolver、锁等待历史回归；`go test -p 1 ./server/replication -count=1 -timeout 5m`、manager/Performance Schema 定向回归通过。
- 本轮全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `104.341s`、manager `11.544s`、replication `3.187s`）；集群 smoke `reports/compatibility/p1-cluster-current-continuation913/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation913/release-candidate.json` 为 `GO`，9 项检查全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `11:00 min`。
- 当前仍未宣称 MySQL 8.4 全量兼容：Performance Schema 真正 allocator/file-page IO 全生命周期、全部 native 类型与复杂 optional metadata 的全量上游互操作、跨源 GTID/PITR 全语义、真实 InnoDB R-tree 页结构与物理维护、跨节点外部 fencing/完整 MDL、真正 online DDL、复杂 SQL/存储格式及剩余 P1/P3/P4 宽矩阵仍需继续；FULLTEXT 查询语义和非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 914

- 对最后补入的 native schema-aware `ApplyNative`/`DecodeNativeDumpFrom`/`ReplicateNativeFrom` 入口完成当前工作树复验；未发现对既有单机、集群或 JDBC 路径的回归。
- 当前工作树全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `103.169s`、manager `11.149s`、replication `4.842s`）；集群 smoke `reports/compatibility/p1-cluster-current-continuation914/cluster-report.json` 为 `PASS`。
- 最终发布候选 `reports/compatibility/release-candidate-current-continuation914/release-candidate.json` 为 `GO`，9 项检查全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `10:58 min`。报告中的实际构建 revision 为 `0e9a387682575d9f0df4825888c5064fb23d0c8b`；工作树仍包含未提交变更，不能把该 revision 等同于提交完成。

## Continuation 915

- Performance Schema transaction history 继续收口：`events_transactions_history_long` 由执行器维护有界的跨会话历史，不再错误地只返回当前 session 的事务；全局长历史在无当前 session 时也可查询，history/history_long 支持请求列投影，current 保持既有完整行形状兼容。
- transaction 事件现在记录真实 savepoint 计数：创建、`ROLLBACK TO SAVEPOINT`、`RELEASE SAVEPOINT` 分别进入 `NUMBER_OF_SAVEPOINTS`、`NUMBER_OF_ROLLBACK_TO_SAVEPOINT`、`NUMBER_OF_RELEASE_SAVEPOINT`。
- metadata-lock 等待历史新增有界真实记录：等待者在 blocker 释放时保存冲突 owner 快照，只有后续实际获锁才写入历史；取消等待、兼容模式和旧 helper lock 不会生成伪事件。`events_waits_history/history_long`、wait summary、table-lock summary 均可读取已完成 MDL 等待。
- 新增跨 session transaction history、savepoint counters、completed MDL wait history 回归；全仓与最终 cluster/JDBC 门禁完成后再记录对应报告。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `105.496s`、manager `10.950s`）；集群 smoke `reports/compatibility/p1-cluster-current-continuation915/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation915/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `11:06 min`；工作树仍为未提交变更。

## Continuation 916

- Performance Schema wait 观测继续收口：`setup_consumers` 新增并实际控制 `events_waits_current/history/history_long`；wait instrument 新增 metadata-lock 项，并对 row-lock/metadata-lock 的 current、history、data-lock、summary 视图应用 `enabled/timed` 配置。
- 新增 wait consumer/instrument 配置回归；当前定向 engine 用例通过。全仓、集群和 Connector/J 门禁需在本轮代码完成后刷新。
- 全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `105.482s`、manager `11.495s`、net `3.442s`、replication `7.093s`）；集群 smoke `reports/compatibility/p1-cluster-current-continuation916/cluster-report.json` 为 `PASS`。
- 发布候选 `reports/compatibility/release-candidate-current-continuation916/release-candidate.json` 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`；本轮 JDBC 总耗时约 `11:05 min`。race 复验因当前 Windows 环境没有 `gcc`（`CGO_ENABLED=1` 时 cgo 编译器缺失）未执行，不将其记为通过。

## Continuation 917

- joined DML 的内部 SELECT 构造不再通过 `panic` 解析：`UPDATE JOIN`/`DELETE JOIN` 现在将生成 SQL 的 parse/type 错误作为结构化执行错误返回，避免复杂或异常 AST 在 DML 路径导致进程崩溃。
- 新增 joined-DML parser safety 回归，既有 UPDATE JOIN/DELETE JOIN P1 矩阵继续通过；本轮全仓、集群和 Connector/J 门禁待刷新。

## Continuation 918

- 优化器递归覆盖继续收口：`LogicalCTE.Query`、递归 CTE 的 `Anchor/Recursive` 和 CTE Statement 的 `Definitions/Body` 现在会被子查询优化器遍历；镜像字段去重，避免同一子计划重复统计。新增 CTE/递归 CTE 遍历回归。
- ANALYZE 统计修正：维护路径按结果列真实类型规范化数值 `[]byte`，`MIN/MAX` 使用 MySQL 数值比较而不是字典序；`2` 与 `10` 等值域的统计结果不再颠倒。新增 typed numeric min/max 回归。
- 相关谓词安全性继续收口：相关 `EXISTS` 与 `IN/NOT IN/ANY/SOME/ALL` 的前置或后置 `OR` 外围条件现在按外层行计算，保留 OR 真值语义；不再把 `outer_predicate OR subquery` 静默改写为 AND。新增 EXISTS 和 predicate 两组真实表回归，完整 `TestCorrelated*` 通过。
- joined DML 的生成 SELECT 解析错误改为结构化返回；默认逻辑优化流水线遇到静态规则校验错误时保留原计划的 identity fallback，生产入口不再因规则配置触发 panic。
- 本轮全仓 `go test ./... -count=1 -timeout 10m` 通过（engine `107.459s`、manager `11.984s`、net `3.251s`、replication `2.758s`）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation918b/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation918b/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation918b/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation918b/release-candidate.json) 为 `GO`：clean-data、build、unit、integration、go-core、3 次 crash-recovery、100 次 concurrency、observability 和 JDBC 共 9 项全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，JDBC 总耗时约 `11:03 min`。
- 当前仍未宣称 MySQL 8.4 全量兼容：通用递归/复杂相关 AST、完整统计采样与成本模型、完整 Performance Schema allocator/file-page IO、完整 native wire/GTID/PITR、真实 R-tree/FULLTEXT 物理维护、外部 fencing/完整 MDL、真正 online DDL、复杂存储对象和非 Connector/J 全量客户端仍需继续；FULLTEXT 与全量客户端继续按用户要求后置。

## Continuation 919/920

- 相关子查询继续收口：多个相关谓词子查询现在支持顶层 `AND` 组合，覆盖 `IN/NOT IN/ANY/SOME/ALL`；同时支持多个相关 `EXISTS/NOT EXISTS` 的 `AND` 组合，并支持全相关子查询的顶层 `OR` 组合。每个内层查询仍按当前外层行重新绑定执行，保留 `NOT EXISTS`、量化比较和 NULL 语义；混合普通外层条件的复杂 OR 组仍交由通用路径，避免把 OR 错误改写为 AND。
- 新增多个相关 predicate/EXISTS 的真实引擎回归；`go test ./server/innodb/engine -run TestCorrelated -count=1 -timeout 10m` 通过，新增 AND/OR 组合定向测试通过。
- 本轮完整复验：`go test ./... -count=1 -timeout 10m` 通过（engine `106.452s`、manager `11.069s`、net `3.285s`、replication `2.717s`）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation920/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation920/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation920/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation920/release-candidate.json) 为 `GO`：9 项检查全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，JDBC 总耗时约 `10:52 min`。
- 当前仍未宣称 MySQL 8.4 全量兼容：通用复杂相关 AST、任意混合布尔子查询、完整统计采样/成本模型、完整 Performance Schema allocator/file-page IO、完整 native wire/GTID/PITR、真实 R-tree/FULLTEXT 物理维护、外部 fencing/完整 MDL、真正 online DDL、复杂存储对象和非 Connector/J 全量客户端仍需继续；FULLTEXT 与全量客户端按用户要求后置。

## Continuation 921

- 相关子查询再次扩展：多个相关谓词子查询支持顶层 `OR` 组合；混合 `普通外层条件 OR 多个相关 EXISTS/NOT EXISTS` 时，普通条件也按当前外层行参与同一布尔计算，避免错误预过滤。未把任意复杂 OR AST 宣称为已支持，无法安全识别的形态仍回退通用执行路径。
- 新增混合 OR 真实表回归；定向多个相关子查询和完整 `TestCorrelated*` 回归通过。
- 本轮最终复验：`go test ./... -count=1 -timeout 10m` 通过（engine `108.788s`、manager `13.586s`、net `3.849s`、replication `2.563s`）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation921/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation921/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation921/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation921/release-candidate.json) 为 `GO`：9 项检查全部 PASS；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，JDBC 总耗时约 `10:53 min`。
- 当前仍继续推进：复杂相关/递归 AST、通用混合布尔子查询、完整统计采样/成本模型、Performance Schema allocator/file-page IO、native wire/GTID/PITR 全语义、真实 R-tree/FULLTEXT 物理维护、外部 fencing/完整 MDL、online DDL、复杂存储对象和非 Connector/J 全量客户端；FULLTEXT 与全量客户端按用户要求后置。

## Continuation 922

- 补齐 P1-OPS-002 性能基线：运行三次重复的 engine/manager benchmark，覆盖 HashJoin 小/中表、HashAgg 小/中分组和 TableScan，并记录 `ns/op`、`B/op`、`allocs/op`、迭代次数及退出状态的 JSON 报告，为后续批处理、并行执行和 buffer-pool 调优提供可比较基线。
- [`reports/compatibility/performance-benchmark-current-continuation922/benchmark-report.json`](../../reports/compatibility/performance-benchmark-current-continuation922/benchmark-report.json) 为 `PASS`，共 15 个样本，命令退出码为 0。
- 性能基线不等于性能目标完成；稳定吞吐目标、SIMD/vectorized 热路径和高级 buffer-pool 调优仍需基于该基线继续推进。

## Continuation 923/924

- P1-OPS-002 基准报告补齐可比较汇总：`performance_benchmark.ps1` 现在按 benchmark 名称输出样本数、`min/mean/max ns/op`、`mean ops/sec`、平均 `B/op` 和平均 `allocs/op`；同时修复 PowerShell ordered-hashtable 不能直接可靠 `Group-Object` 的问题，避免把所有样本错误合并为空名称。
- 修复后实际运行三次重复基准，报告 [`reports/compatibility/performance-benchmark-current-continuation924/benchmark-report.json`](../../reports/compatibility/performance-benchmark-current-continuation924/benchmark-report.json) 为 `PASS`，5 个 benchmark 各有 3 个样本，汇总名称和指标均正确。
- 该项现在具备基线采集和比较输出；性能目标本身、SIMD/vectorized 热路径和高级 buffer-pool 调优仍属于后续工作。

## Continuation 925

- 继续收敛 P1-OPT-004：管理器级复合索引前缀识别现在覆盖 `IS NULL`、`IS NOT NULL`、`BETWEEN`、`NOT IN`、`NOT LIKE`、NULL-safe equality（`<=>`）以及同前导列连续的元组等值条件（例如 `(tenant_id, created_at) = (7, 100)`）。缺少前导列时仍不会错误跳过索引前缀。
- 新增 `TestOptimizerRecognizesStructuredIndexPredicates` 与 `TestOptimizerRecognizesTupleEqualityIndexPrefix`；manager 全包和相关 plan 优化器回归通过。
- 管理器级条件识别再补齐常量左置比较（例如 `7 = tenant_id`、`'7' <=> tenant_id`），与执行器的比较操作符归一化保持一致；新增用例已通过。
- 继续收敛 P1-OPT-003：增强统计收集器的 `findMinMax` 现在跳过前导/中间 `NULL` 样本并以首个非空值初始化边界，避免含 NULL 的数值样本错误得到 `MIN=NULL` 而放弃直方图；普通与增强收集器回归均通过。

## Continuation 926

- P1-OPS-002 基准工具增加可选基线比较：`performance_benchmark.ps1` 支持 `-BaselineReport` 和 `-MaxRegressionPercent`，按 benchmark 名称比较 `mean_ns_per_op`，报告每项回归比例、缺失基线和总体状态；超过阈值时以 `FAIL` 退出，未配置基线时保持原有采集行为。
- 实际基线比较报告 [`reports/compatibility/performance-benchmark-current-continuation926/benchmark-report.json`](../../reports/compatibility/performance-benchmark-current-continuation926/benchmark-report.json) 为 `PASS`，5 项比较均通过（阈值 100%，最大实测回归约 0.95%）；阈值 0% 的负向验证按预期生成 `FAIL` 并返回退出码 1。
- P3-PERF-002 缓冲池预读并发边界继续收口：预读 worker 不再为每个队列请求额外创建无界 goroutine，而是由 `PrefetchWorkers` 直接限制存储读取并在预读完成后归还临时页面句柄；新增并发上限回归，20 个预读请求在 2 个 worker 下最大并发保持为 2。
- 本轮新增 manager 定向回归通过；后续继续推进前仍需刷新全仓、集群和 Connector/J 发布门禁，并保留 FULLTEXT 与非 Connector/J 全量客户端后置边界。

## Continuation 927

- 修复优化版缓冲池的脏页一致性：`GetPage` 现在返回 LRU 中缓存页本体，`GetDirtyPage/MarkDirty` 的内容和脏标记与 `FlushPage/FlushAllPages` 共享同一页对象；移除复制句柄和对象池回收造成的“修改成功但 Flush 看不到脏页”问题。
- 新增 `TestOptimizedBufferPoolManagerFlushesMutationsFromCachedPage`，先复现缓存复制句柄导致落盘内容仍为零页，再修复并验证修改后的页能通过 `FlushPage` 写回存储；预读并发、关闭幂等、预读 hint 回归继续通过。
- 发布候选 `continuation926b` 的持久会话最终完成为 `GO`：8 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时约 `12:07 min`。该报告包含本轮缓冲池修复前的代码状态，待 927 代码完成后再刷新一次最终门禁。

## Continuation 928

- 优化版缓冲池的脏页注册改为在同一互斥区内按 page ID 去重；重复 `GetDirtyPage`/`MarkDirty` 不再重复增加 `DirtyPages`，刷新后统计能回到零。新增 `TestOptimizedBufferPoolManagerDoesNotDoubleCountDirtyPage`，并与缓存页写回、预读并发、关闭幂等回归一起通过。
- 最新全仓 `go test ./... -count=1 -timeout 10m` 明确通过（退出码 0）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation927/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation927/cluster-report.json) 为 `PASS`。
- 针对当前工作树最新代码重新执行发布门禁，报告 [`reports/compatibility/release-candidate-current-continuation927/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation927/release-candidate.json) 为 `GO`；clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability 和 JDBC 全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped。
- 当前仍继续推进未完成的 P1/P3/P4 兼容边界；FULLTEXT 查询/物理索引和非 Connector/J 全量客户端继续按用户要求后置，不作为本轮门禁条件。

## Continuation 929

- 索引维护边界继续收口：`StorageIntegratedDMLExecutor` 的索引受影响判断现在对 UPDATE 表达式和受影响列使用大小写不敏感匹配，并忽略 nil 更新表达式；避免 SQL 列名大小写变化时漏维护二级/唯一索引。
- `IndexManager.extractIndexKey` 与 `isIndexAffected` 统一支持大小写不敏感的行列名查找；索引值比较改用安全的深度比较，`[]byte` 等不可比较值不会触发 Go runtime panic，也不会把内容相同的二进制索引值误判为变化。新增 manager/engine 回归通过。
- 该批变更已通过 `go test ./server/innodb/engine ./server/innodb/manager -count=1 -timeout 10m`；下一步继续按 P1/P3/P4 剩余边界推进并刷新全量发布证据。

## Continuation 930

- 复制 lag 语义补强：`Replica.ReplicateFrom`、native replicate 和 HTTP pull 在源端返回空事件、且副本已追平时清除旧的 `LastAppliedAt`；`SHOW REPLICA STATUS.Seconds_Behind_Source` 不再因历史应用时间持续增长。新增 caught-up lag 回归通过。
- 索引维护再补强：`IndexManager.extractIndexKey/isIndexAffected` 与存储集成索引更新判断统一使用大小写不敏感列名解析和安全深度比较，覆盖二进制切片值；相关 engine/manager 全包通过。
- 最新全仓 `go test ./... -count=1 -timeout 10m` 通过（退出码 0）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation929/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation929/cluster-report.json) 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation929/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation929/release-candidate.json) 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- P1/P3/P4 仍有未完成的完整语义边界，尤其是 Performance Schema allocator/file-page IO、跨源原生 GTID/PITR 全互操作、完整 MDL/online DDL、复杂存储对象和高级索引物理维护；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 931

- P1-OPS-002 性能基线默认矩阵扩展为 11 个样本：除 HashJoin/HashAgg/TableScan 外，新增并发写入 `BenchmarkConcurrentInsert`、范围读取 `BenchmarkRangeQuery`、MVCC 可见性/提交，以及行序列化/反序列化；脚本仍记录迭代次数、`ns/op`、吞吐、`B/op`、`allocs/op` 和退出状态。
- 新默认矩阵运行报告 [`reports/compatibility/performance-benchmark-current-continuation930/benchmark-report.json`](../../reports/compatibility/performance-benchmark-current-continuation930/benchmark-report.json) 为 `PASS`，单次采集 11 个样本；三次基线报告 [`reports/compatibility/performance-benchmark-current-continuation930-baseline/benchmark-report.json`](../../reports/compatibility/performance-benchmark-current-continuation930-baseline/benchmark-report.json) 也为 `PASS`。
- 性能基线扩展不等于 SIMD/vectorized 或吞吐目标完成；高级性能优化仍以后续工作处理，FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 932

- 唯一索引约束校验修正：成功的 `SearchKey` 命中现在无论物理页号是否为 `0` 都判定为重复；此前仅判断 `pageNo > 0` 会放过首个叶页上的重复键。
- 唯一索引查找只把明确的缺键结果作为“可插入”；存储读取、索引状态等其他错误会向上返回，避免异常路径静默绕过唯一约束。新增页 0、缺键和搜索失败回归均通过。
- 扩展后的 11 项性能基线已与三次基线完成真实比较，报告 [`reports/compatibility/performance-benchmark-current-continuation932/benchmark-report.json`](../../reports/compatibility/performance-benchmark-current-continuation932/benchmark-report.json) 为 `PASS`，阈值 100%。
- 本轮改动继续遵守用户边界：MySQL 核心、集群和 Connector/J 优先；FULLTEXT 与非 Connector/J 全量客户端不纳入当前实现批次。

## Continuation 933

- 约束校验继续补齐：真实 DML 的唯一冲突检测现在读取表级 `UNIQUE(a,b)` 等复合唯一索引元数据，覆盖已有行、批量 INSERT 和批量 UPDATE；不再只依赖单列 `ColumnMeta.IsUnique`。
- 复合唯一索引列名解析使用大小写不敏感查找，任一索引列为 `NULL` 时保留 MySQL 允许多个 NULL 的语义；新增复合索引冲突、批内冲突和 NULL 回归通过。
- 本轮 engine 约束定向回归通过；后续继续刷新 engine 全包、集群 smoke 和 Connector/J 发布门禁。

## Continuation 934

- 复合唯一约束实现已完成 engine 全包验证：`go test ./server/innodb/engine -count=1 -timeout 10m` 通过，耗时约 101 秒。
- 该实现只扩展表级唯一索引冲突识别，不改变 FULLTEXT、非 Connector/J 客户端或其他明确后置范围；全仓、集群和 Connector/J 发布门禁将在本轮剩余改动稳定后统一刷新。

## Continuation 935

- 修复复合唯一二级索引的 MySQL NULL 语义闭环：物理索引同步在解析到任一 NULL 索引分量时跳过唯一重复探测，仍把真实索引条目写入 durable B+Tree，因此同一租户的重复非 NULL 组合继续拒绝，而包含 NULL 的重复组合可以共存。
- 修正 DDL 元数据回归断言，确认 `UNIQUE(tenant_id,email)` 保持索引级复合约束，不会错误退化为两个单列 UNIQUE；新增物理键 NULL 识别单测和真实 engine 端到端回归。
- 验证：`go test ./server/innodb/engine -run '^TestCompositeUniqueIndexEnforcesMySQLNullSemantics$' -count=1 -v -timeout 5m` 通过；相关 manager 物理键回归随后刷新。
- 继续遵守用户边界：MySQL 核心、集群和 Connector/J 优先；FULLTEXT 查询/物理索引与非 Connector/J 全量客户端仍后置。

## Continuation 936

- Performance Schema 内存观测从固定零值改为真实生命周期快照：运行时 recorder 按线程和 instrument 记录分配/释放次数、字节数、当前值和 high-water 值，并在 SQL 执行路径按实际 SQL 缓冲长度记录 allocation/free；`memory_summary_global_by_event_name` 与 `memory_summary_by_thread_by_event_name` 读取稳定快照并保留零当前使用量的历史行。
- 新增 recorder 生命周期单测及真实引擎查询回归；复合唯一二级索引 NULL 语义、物理键识别和 Performance Schema 内存专项均通过。
- 当前实现仍只报告已被 recorder 证明的内存 instrument，不伪造完整 Go allocator/page-cache 历史；底层 file/page IO 全生命周期、完整 native wire/GTID/PITR、完整 MDL/online DDL、复杂存储对象和高级索引物理维护继续按优先级推进。FULLTEXT 与非 Connector/J 全量客户端仍后置。

## Continuation 937

- Performance Schema 内存/语句生命周期覆盖补到直连执行器：`XMySQLExecutor.ExecuteWithQuery` 现在和网络引擎入口一样，按真实 SQL 缓冲长度记录 `memory/sql/THD::main_mem_root` 的 allocation/free，并写入带连接线程 ID 的 statement history；执行错误仍沿用既有 `metricStatus` 与错误计数。
- 新增直连执行器回归，验证 allocation/free 次数、字节数、当前使用量归零及语句线程归属；`go test ./server/innodb/engine -run '^TestPerformanceSchemaDirectExecutorRecordsMemoryAndStatementLifecycle$' -count=1 -v -timeout 5m` 通过。
- 当前仍不把 SQL 缓冲观测扩大解释为完整 Go allocator、buffer-pool 或文件页 IO 追踪；完整 native wire/GTID/PITR、跨节点强 fencing/完整 MDL、真正 online DDL、复杂存储对象和高级索引物理维护继续按优先级推进。FULLTEXT 与非 Connector/J 全量客户端仍后置。

## Continuation 938

- ALTER 表数据重写的失败恢复继续补强：列布局/定义变更在改写多行时，任一 checkpoint 写许可、旧行删除、新行插入或根页持久化失败，都会回滚本轮已完成的全部新旧键替换；不再只覆盖新行插入失败这一条分支。
- `TestAlter*` 与 `TestPerformanceSchema*` engine 回归通过；本轮全仓 Go 已通过，engine 定向 ALTER/Performance Schema 回归通过。
- 该修复强化的是进程内兼容 DDL 的原子回滚，不等同于完整 MySQL MDL、后台 online rebuild 或跨节点 DDL 协调；这些边界继续按后续优先级推进。FULLTEXT 与非 Connector/J 全量客户端仍后置。

## Continuation 939

- 复制 API 防御性继续补齐：逻辑 `Replica.ReplicateFrom` 在 source 为空时现在返回结构化错误并保留请求 position，不再因直接调用 `Source.Dump` 发生 nil pointer panic；副本同时记录 `LastError`，与 native 拉取路径的错误状态保持一致。
- 新增 nil source 回归；`go test ./server/replication -run '^TestReplicaRejectsNilLogicalReplicationSourceWithoutPanic$' -count=1 -v -timeout 5m` 通过。
- 之前的本轮交付证据：全仓 Go、P1 集群 smoke 和 Connector/J 发布候选均通过（`139` tests、`0` failures、`0` errors、`0` skipped，`GO`）。跨源 GTID/PITR 全语义、外部 fencing、完整 MDL/online DDL、完整 Performance Schema file/page IO 和上游 native 全量 wire 仍是未完成边界；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 940

- Performance Schema 文件生命周期继续接入真实存储：`IBD_File` 的打开、关闭、物理页读和物理页写现在由 runtime recorder 记录，`file_instances` 暴露当前打开计数，`file_summary_by_instance/event_name` 暴露真实读写次数及纳秒级 min/avg/max/sum 计时；未发生物理操作的已发现文件仍保持零值，不用文件枚举伪造 I/O 历史。
- 新增 runtime file-I/O 生命周期单测与真实 `.ibd` 建表/写入回归；Performance Schema file 专项通过。
- 该项已覆盖本项目实际 `.ibd` 物理页入口，但不等同于上游 Performance Schema 全部 allocator、页缓存命中、日志文件和所有事件类型的完整历史；native 全量 wire/GTID/PITR、完整 MDL/online DDL、复杂存储对象和其余 P1/P3/P4 宽矩阵继续推进。FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 941

- LIKE 过滤语义继续对齐：存储集成执行器的 LIKE 匹配现在按默认反斜杠转义处理 `\\%`/`\\_`，支持尾部转义符匹配字面反斜杠，并保留默认文本匹配的大小写不敏感行为。
- 解析后的 `LIKE ... ESCAPE <expr>` 现在使用显式单字符转义符，并对 NULL 或多字符转义符返回明确错误；新增默认转义和显式 ESCAPE 回归通过。
- 该项修复执行器精确残余过滤与 JOIN 条件的 LIKE 边界，不涉及 FULLTEXT；FULLTEXT、非 Connector/J 全量客户端及更广泛排序规则/字符集矩阵仍按用户要求后置。

## Continuation 942

- 规划器表达式路径的 LIKE 默认文本语义已与存储集成路径统一为大小写不敏感，同时保留 `%`/`_` 通配符、反斜杠转义和正则元字符字面匹配。
- 新增 `BuildExpression`/运行时 LIKE 回归，避免同一 SQL 在规划器编译路径和存储残余过滤路径得到不同的大小写结果。
- 二进制排序规则、完整字符集/校对规则矩阵和 FULLTEXT 仍不在本轮范围内；非 Connector/J 全量客户端继续后置。

## Continuation 943

- 本轮验证完成：全仓 `go test ./... -count=1 -timeout 10m` 通过，涉及的 engine、plan、manager、replication、net、metrics 包均 PASS；集群 smoke 通过单源双副本、提交 DML/DDL、回滚隔离和副本提升场景。
- 最新 Connector/J 发布候选门禁 [`reports/compatibility/release-candidate-current-continuation942/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation942/release-candidate.json) 为 `GO`：9 项检查全部 PASS，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 当前工作树仍包含用户既有未提交修改，门禁的 `git_revision` 是基线提交 `0e9a387682575d9f0df4825888c5064fb23d0c8b`，实际 build/test 使用了工作树内容；race 未能启动是本机缺少 `gcc` 的环境限制，不是代码失败。
- 继续推进的明确边界：FULLTEXT 与非 Connector/J 全量客户端按用户要求后置；完整字符集/校对矩阵、跨源 GTID/PITR 全互操作、跨节点强 fencing/完整 MDL、真正 online DDL、复杂 AST、真实 R-tree 和完整 Performance Schema allocator/page-cache 仍未完成。

## Continuation 944

- RuntimeRecorder 的文件 I/O 记录补齐零值实例并发初始化保护：首次由多个存储 goroutine 记录 `.ibd` 生命周期时不再存在未同步指针初始化/快照读取窗口。
- 新增零值 RuntimeRecorder 并发打开/读取/关闭回归；metrics、engine 专项和复制边界回归通过。

## Continuation 945

- 发布门禁第一次刷新暴露了直连执行器观测的真实时序缺陷：结果 channel 关闭早于 statement/memory defer，调用方在收到结束信号时可能读到未完成的生命周期记录；现已调整 defer 注册顺序，保证 channel 关闭前完成观测清理。
- 同时修正全局 RuntimeRecorder 测试隔离：断言改为相对本次执行的 allocation/free 增量，避免重复测试因跨用例累计计数产生假失败；该测试连续 10 次通过，engine/net/protocol/replication integration 全包通过。
- 第一次 `continuation944` 门禁的唯一失败项为上述 engine integration 测试，JDBC 本身 `139/0/0/0` 仍通过；修复后将重新生成最终门禁报告。

## Continuation 946

- 修复后的最终发布候选门禁 [`reports/compatibility/release-candidate-current-continuation945/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation945/release-candidate.json) 为 `GO`：clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 9 项全部 PASS。
- Connector/J 结果为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`；当前工作树的最新 engine integration、全仓 Go 和集群 smoke 均已通过。
- 这轮没有停在旧的 `continuation944 NO-GO`：旧报告因观测 defer 时序/测试累计断言失败而保留，新报告已经覆盖修复后的工作树。

## Continuation 947

- 规划器 `BuildExpression` 现在保留 parser 生成的 `ComparisonExpr.Escape`，`LIKE`/`NOT LIKE` 的解释执行和编译执行均使用显式单字符转义符；此前 `LIKE ... ESCAPE` 在进入计划后会丢失 ESCAPE，可能把字面通配符错误地当成 `%`/`_` 通配符。
- `BinaryOperation` 与结构化 `LikeExpression` 的回显也保留 `ESCAPE`，新增回归覆盖计划构造、解释路径、编译路径和字符串表示；`go test ./server/innodb/plan -count=1 -timeout 10m` 通过。
- 该项继续不扩展 FULLTEXT 或非 Connector/J 全量客户端范围；完整字符集/校对矩阵、复杂 AST、在线 DDL、跨源 GTID/PITR 和完整 Performance Schema allocator/page-cache 仍需后续实现。

## Continuation 948

- P0 运行时观测端点改为由 `MySQLServer` 实例持有独立的 `http.Server` 和 `ServeMux`，不再依赖进程级 `http.DefaultServeMux`/一次性注册；启动、重复实例化和关闭路径不会共享全局 handler 状态。
- profiling server 关闭时会释放监听资源，并忽略正常的 `http.ErrServerClosed`；新增 `/metrics` GET、非 GET 方法和注册指标可见性回归，`go test ./server/net -count=1 -timeout 10m` 通过。

## Continuation 949

- 索引下推分析现在识别 `LIKE`/`NOT LIKE` 的显式 ESCAPE，并按该转义符判断是否为安全的简单前缀模式；例如 `LIKE 'abc#%' ESCAPE '#'` 中的 `%` 是字面量，不再被错误当成前缀通配符。
- 不确定或动态 ESCAPE 表达式会保守放弃前缀下推，由精确残余过滤处理；新增索引候选回归，`go test ./server/innodb/plan -count=1 -timeout 10m` 通过。

## Continuation 950

- LIKE ESCAPE 语义继续贯穿优化器：CNF 克隆、谓词标准化、跨 JOIN/Apply 的列替换、结构化 LIKE 克隆和列依赖收集均保留/遍历 ESCAPE 表达式，避免优化重写后重新退回默认反斜杠语义。
- 新增归一化与 CNF 后的执行回归；plan 包完整测试通过。Connector/J 门禁 [`reports/compatibility/release-candidate-current-continuation949/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation949/release-candidate.json) 刷新为 `GO`，9 项检查全部 PASS，JDBC `139/0/0/0`。

## Continuation 951

- 对最新优化器/CNF/表达式重写改动完成最终发布门禁：[`reports/compatibility/release-candidate-current-continuation951/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation951/release-candidate.json) 为 `GO`，clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 共 9 项全部 PASS。
- Connector/J 本轮结果为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`；本轮全仓 Go 与最新集群 smoke 也已通过。

## Continuation 952

- 页分配器释放边界继续收口：`FreePage`/`FreePages` 现在拒绝未分配页、重复释放和越界 extent 位，避免统计下溢或把无效页误放回空闲链；批量释放先完成全量预校验，失败时不会产生半批次状态。
- `AllocatePages` 的 `AllocatedPages`、`FragmentPages`、`ExtentPages` 统计改为按实际返回页数维护；缺少 `SpaceManager` 时返回明确错误而不是在批量分配路径 panic。新增分配/释放统计回归，manager 全包通过。

## Continuation 953

- profiling listener 正式提供实例级 `/healthz`，并支持 readiness 回调；`MySQLServer` 的 health 检查绑定 `XMySQLEngine.IsReady()`，只有完成恢复、检查点和复制运行时启动后才返回 `200`，未就绪返回 `503`。
- 引擎增加原子 readiness 生命周期：启动失败/关闭时清零，完整启动成功后置位；新增 readiness 与 HTTP 方法/状态回归，engine、net、manager 定向测试及全仓 Go 回归通过。

## Continuation 954

- 当前工作树最终验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation953/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation953/cluster-report.json) 为 `PASS`；release-candidate [`reports/compatibility/release-candidate-current-continuation953/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation953/release-candidate.json) 为 `GO`。
- 发布门禁的 clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 共 9 项全部 `PASS`；Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- `git_revision` 字段仍是门禁脚本记录的基线提交 `0e9a387682575d9f0df4825888c5064fb23d0c8b`；本轮实际验证使用的是包含未提交工作树修改的本地源码，不能把该字段当作本轮代码提交证明。FULLTEXT 与非 Connector/J 全量客户端继续按用户要求后置，其余上游全量兼容边界仍需后续逐项推进。

## Continuation 955

- 表维护语句现在支持一次处理多个表，并逐表执行权限校验、物理行扫描、CHECK 一致性检查和 ANALYZE/OPTIMIZE 统计刷新；新增 `CHECK/ANALYZE/OPTIMIZE TABLE t1, t2` 回归。
- 维护语法兼容了常见的 `LOCAL`、`NO_WRITE_TO_BINLOG` 和 `CHECK ... FOR UPGRADE` 形式；新增 `FLUSH TABLES` 与指定表形式，真实调用 `StorageManager.Flush()` 完成持久化刷新，而不是直接返回空成功。`FLUSH TABLES WITH READ LOCK` 仍保留明确错误，待全局读锁协调器完成后实现。
- engine 维护专项测试通过；FULLTEXT、非 Connector/J 全量客户端、全局读锁/完整 MDL、上游 native wire/GTID/PITR、R-tree 和完整 Performance Schema allocator/page-cache 仍不在本次切片内。

## Continuation 956

- `FLUSH TABLES WITH READ LOCK` 已从明确不支持改为真实实例级读屏障：先调用 `StorageManager.Flush()`，再持有锁；SELECT 继续执行，INSERT/UPDATE/DELETE/REPLACE 以及常见表级 DDL 在锁期间等待，`UNLOCK TABLES` 释放持有者的全局读锁。
- 修复了引擎公开 `ExecuteQuery` 入口与直接执行器入口不一致的问题：两条路径都接入全局写侧锁，避免只在单测调用路径生效。
- 新增并通过并发回归；更完整的全局 MDL、锁等待超时/取消和跨节点备份协议仍需后续推进。FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 957

- 本轮回归：`go test ./server/innodb/engine ./server/innodb/manager ./server/net ./server/observability/metrics -count=1 -timeout 10m` 通过；engine 293.457s、manager 10.295s、net 2.801s、metrics 0.445s，退出码 0。
- 集群 smoke [`reports/compatibility/p1-cluster-current-continuation956/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation956/cluster-report.json) 返回 `PASS`，覆盖单源双副本、提交 DML/DDL、回滚隔离和副本提升。
- release candidate gate 将在该切片后重新刷新；FULLTEXT 与非 Connector/J 全量客户端按用户决定继续后置。

## Continuation 958

- 最新 release-candidate gate [`reports/compatibility/release-candidate-current-continuation956/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation956/release-candidate.json) 返回 `GO`，9 项全部 `PASS`：clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC。
- Connector/J 本轮为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`；门禁生成时间为 `2026-09-12T14:39:13Z`。
- `git_revision` 仍是脚本记录的基线提交 `0e9a387682575d9f0df4825888c5064fb23d0c8b`，实际验证使用含本地未提交改动的工作树；不能将其解释为本轮提交证明。

## Continuation 959

- 全局读锁生命周期继续收口：`FLUSH TABLES WITH READ LOCK` 改用可被 `context.Context` 取消的读写屏障，写入端等待期间支持 `KILL QUERY`，避免不可中断的 `sync.RWMutex` 等待；`COM_RESET_CONNECTION`、临时表清理和网络断开清理统一释放锁持有者状态。
- 新增并通过全局读锁的会话重置释放、写入阻塞取消回归；`go test ./... -count=1 -timeout 30m` 全仓通过，engine、manager、net、metrics 相关包回归通过。
- 本轮 `go test -race` 未执行：当前 Windows Go 环境 `CGO_ENABLED=0` 且未发现可用 gcc，工具链直接拒绝 race 模式；这不影响普通全仓回归结果。FULLTEXT 与非 Connector/J 全量客户端继续按用户要求后置。

## Continuation 960

- 本轮集群验收 [`reports/compatibility/p1-cluster-current-continuation959/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation959/cluster-report.json) 为 `PASS`，覆盖单源双副本、提交 DML/DDL、回滚隔离和副本提升。
- 最新发布门禁 [`reports/compatibility/release-candidate-current-continuation959/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation959/release-candidate.json) 为 `GO`；clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`。Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，门禁时间为 `2026-09-12T15:09:45Z`。
- 报告中的 `git_revision` 仍为脚本记录的基线提交 `0e9a387682575d9f0df4825888c5064fb23d0c8b`；本轮验证包含本地未提交工作树修改，不能将该字段解释为已提交版本证明。

## Continuation 961

- 继续补齐 `FLUSH TABLES` 语法：支持带目标表的 `FLUSH TABLES t1[, t2] WITH READ LOCK`，目标表仍先经过名称校验，再执行真实 storage flush 并进入全局读锁屏障。
- 新增带目标表读锁回归；修改前测试先失败并暴露为目标名误解析，修复后 `TestFlushTablesWithReadLock*` 全部通过。

## Continuation 962

- `FLUSH TABLES` 继续兼容官方常用修饰符 `NO_WRITE_TO_BINLOG` 与 `LOCAL`，支持无目标表和带目标表两种形式；修饰符不会绕过真实 `StorageManager.Flush()` 或 `WITH READ LOCK` 屏障。
- 新增修饰符回归；修改前测试先失败并落入 parser error，修复后 `TestAdminCompatibilityFlushTables*` 与 `TestFlushTablesWithReadLock*` 全部通过。

## Continuation 963

- 最新发布门禁 [`reports/compatibility/release-candidate-current-continuation962/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation962/release-candidate.json) 返回 `GO`，9 项全部 `PASS`，其中 Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 最新集群 smoke [`reports/compatibility/p1-cluster-current-continuation963/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation963/cluster-report.json) 返回 `PASS`，单主双从、提交 DML/DDL、回滚隔离和副本提升均通过。

## Continuation 964

- 补齐 `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` 的 CHECK 约束投影：已持久化的命名/未命名 CHECK 约束现在与 `CHECK_CONSTRAINTS` 一致返回 `CONSTRAINT_TYPE=CHECK` 和 `ENFORCED=YES/NO`，避免 Connector/J 只读取标准表约束视图时遗漏约束。
- 新增 `TestTableConstraintsIncludesCheckConstraints`，覆盖命名的 `NOT ENFORCED` CHECK 和未命名 CHECK；定向 engine 回归通过。

## Continuation 965

- 补齐 `INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_NAME`：外键元数据现在会读取被引用表的 PRIMARY/UNIQUE 索引，并返回实际约束名；同时保留跨库引用的 schema 信息，找不到历史/损坏元数据时保守返回 NULL。
- `information_schema` 元数据过滤新增 `CONSTRAINT_TYPE` 条件，避免标准 JDBC 查询把 PRIMARY/FOREIGN KEY/CHECK 混在一起；新增 `TestReferentialConstraintsExposeReferencedUniqueConstraint`，engine 定向回归通过。

## Continuation 966

- `SHOW CREATE TABLE` 现在保留 CHECK 约束名和 `ENFORCED/NOT ENFORCED` 状态，并为未命名约束生成稳定的兼容名称；此前输出只保留表达式，客户端通过 DDL 反射会丢失约束身份和 enforcement 状态。
- 新增 `TestShowCreateTablePreservesCheckConstraintNamesAndEnforcement`，engine 定向回归通过。

## Continuation 967

- 修正 `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` 的索引边界：普通非唯一二级索引不再被错误投影为 `UNIQUE` 约束，仅 PRIMARY/UNIQUE 索引、FOREIGN KEY 和 CHECK 约束进入该视图。
- 新增 `TestTableConstraintsExcludesNonUniqueIndexes`；修改前测试先失败并显示普通 `KEY` 被误报为 `UNIQUE`，修复后约束、外键、CHECK 相关定向回归通过。

## Continuation 968

- 修正 `INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` 的默认动作：外键未声明 `ON UPDATE/ON DELETE` 时现在按 MySQL 返回 `RESTRICT`，显式 `CASCADE/SET NULL/NO ACTION` 仍保留原值。
- 扩展 `TestReferentialConstraintsExposeReferencedUniqueConstraint` 校验默认规则；engine 定向回归通过。

## Continuation 969

- 本轮标准约束元数据改动完成全量验证：`go test ./... -count=1 -timeout 30m` 通过，engine `109.590s`、net `2.952s`、replication `2.169s` 等全部包通过。
- 最新集群 smoke [`reports/compatibility/p1-cluster-current-continuation968/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation968/cluster-report.json) 返回 `PASS`。
- 最新发布门禁 [`reports/compatibility/release-candidate-current-continuation968/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation968/release-candidate.json) 返回 `GO`；clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`。Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 970

- 继续补齐 P1-SQL-004 的父键定义边界：`CREATE TABLE` 与 `ALTER TABLE ... ADD FOREIGN KEY` 现在都要求父表的被引用列由某个索引按左前缀覆盖；缺失索引时在发布 `.frm` 前失败，不留下外键或自动辅助索引。该路径允许 InnoDB 兼容范围内的非唯一父索引。
- 修复联动元数据缺口：列级 `UNIQUE` 现在持久化为隐式唯一索引，保证外键父键校验、`SHOW/INFORMATION_SCHEMA` 索引视图和运行时唯一约束使用同一份索引定义；`UNIQUE_CHECKS=0` 的 INSERT/UPDATE 使用可容纳重复导入行的键形状，恢复检查后仍由行级校验拒绝新冲突。
- 新增并通过 `TestCreateForeignKeyRequiresReferencedIndex`、`TestAlterForeignKeyRequiresReferencedIndex`、`TestQualifiedAlterForeignKeyUsesQualifiedChildSchema`；完整 engine `go test ./server/innodb/engine -count=1 -timeout 20m` 通过（约 `103.836s`），全仓 `go test ./... -count=1 -timeout 30m` 通过（engine `110.706s`、manager `11.730s`、net `2.948s`、replication `1.975s` 等）。
- 本轮仍不宣称总完成：跨节点强 fencing/完整 MDL、真正 online DDL、复杂 AST、完整 native GTID/PITR 互操作、真实 R-tree/空间物理格式、完整 Performance Schema allocator/page-cache、其余 P1/P3/P4 宽矩阵继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 971

- 本轮最终验收证据：集群 smoke [`reports/compatibility/p1-cluster-current-continuation970/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation970/cluster-report.json) 返回 `PASS`，覆盖单源双副本、提交 DML/DDL、回滚隔离和副本提升。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation970/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation970/release-candidate.json) 返回 `GO`；clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`。Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时 `11:55 min`。
- 门禁报告的 `git_revision` 仍是脚本记录的基线 `0e9a387682575d9f0df4825888c5064fb23d0c8b`；本轮实际验证使用含本地未提交改动的工作树，不能把该字段解释为本轮提交证明。

## Continuation 972

- 继续补齐 P1-SQL-004 外键定义：CREATE/ALTER 外键现在校验父表引用列的左前缀索引，以及子列与父列的整数宽度/UNSIGNED、定点数精度/小数位、字符串类型和显式字符集/排序规则兼容性；非法定义在落盘前失败。
- 外键生命周期现在清理自动辅助索引：删除约束后重新评估剩余外键，保留共享索引，最后一个依赖解除后才从 `.frm`/标准索引元数据移除；不影响用户显式创建的同名索引。
- `UNIQUE_CHECKS=0` 的二级索引同步现在使用包含聚簇键的可重复键形状，支持关闭检查期间 INSERT/UPDATE 重复唯一值导入，同时兼容更新在开关关闭前写入的正常唯一键。
- 新增并通过 `TestAlterForeignKeyRejectsMismatchedReferencedColumnType`、`TestCreateForeignKeyRejectsMismatchedReferencedColumnType`、`TestDropForeignKeyKeepsSharedAutomaticIndex`；完整 engine `go test ./server/innodb/engine -count=1 -timeout 20m` 通过（`153.018s`），全仓 `go test ./... -count=1 -timeout 30m` 通过（engine `109.802s`、manager `10.849s`、net `2.820s`、replication `2.213s` 等）。

## Continuation 973

- 本轮最终集群验收 [`reports/compatibility/p1-cluster-current-continuation972/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation972/cluster-report.json) 返回 `PASS`，覆盖单源双副本、提交 DML/DDL、回滚隔离和副本提升。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation972/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation972/release-candidate.json) 返回 `GO`；clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`。Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`，总耗时 `12:02 min`。
- 报告 `git_revision` 仍为门禁脚本记录的基线 `0e9a387682575d9f0df4825888c5064fb23d0c8b`；实际验证使用的是含本地未提交改动的工作树，不能把该字段解释为本轮提交证明。

## Continuation 974

- 继续补齐 P1-SQL-004 外键动作边界：CREATE TABLE 与 ALTER TABLE ADD FOREIGN KEY 现在在发布约束元数据前校验 `ON DELETE/UPDATE SET NULL` 的子列必须允许 NULL，避免把只能在运行时失败的非法外键定义落盘。
- 回退外键解析器现在识别 `SET NULL`、`NO ACTION`、`RESTRICT` 和 `CASCADE` 动作；同时将 SQL parser 的 `BoolVal` 正确转换为普通布尔元数据，修复 nullable/UNSIGNED 等 DDL 校验在 parser 路径下的类型判断。
- 外键 symbol 现在在同一 schema 内执行大小写不敏感的唯一性校验，覆盖同表多约束和不同子表之间，避免重复 symbol 被分别写入多个 `.frm`。
- CREATE TABLE 外键校验不再跳过缺失的本地列或被引用列；引用列不存在时会在元数据发布前明确失败。
- 回退解析路径也保留显式 `CONSTRAINT <symbol>` 名称及 `SET NULL/NO ACTION/RESTRICT/CASCADE` 动作，避免 fallback 元数据与标准 parser 路径分叉。
- 新增并通过 `TestFallbackForeignKeyParsesReferentialActions`、`TestCreateForeignKeySetNullRequiresNullableChildColumns`、`TestAlterForeignKeySetNullRequiresNullableChildColumns`；本轮完整 engine、全仓、集群和发布候选门禁仍需继续刷新后再记录最终证据。
- 最终验证已完成：完整 engine `go test ./server/innodb/engine -count=1 -timeout 20m`、全仓 `go test ./... -count=1 -timeout 30m`、集群 smoke [`reports/compatibility/p1-cluster-current-continuation974-final/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation974-final/cluster-report.json) 和发布候选 [`reports/compatibility/release-candidate-current-continuation974-final/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation974-final/release-candidate.json) 均通过；发布门禁 9 项全部 PASS，Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 在补齐 fallback 外键列/名称后重新验证：完整 engine `106.355s`、全仓 Go `110.368s`、集群 smoke [`reports/compatibility/p1-cluster-current-continuation974-final2/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation974-final2/cluster-report.json) 和最新发布候选 [`reports/compatibility/release-candidate-current-continuation974-final2/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation974-final2/release-candidate.json) 均通过；最新门禁仍为 `GO`，9 项全部 PASS，Connector/J `139/0/0/0`，Maven `BUILD SUCCESS`。

## Continuation 975

- 继续补齐 P1-OPT-005/P1-EXE-004：计划层新增 MySQL `INTERVAL(N, N1, ...)` 函数，返回最后一个小于等于目标值的边界下标，覆盖低于首边界、命中边界、区间内、超过末边界和 NULL 输入；解释执行与编译执行共用同一实现。
- SQL parser 现在允许保留关键字 `INTERVAL` 以函数形式出现，同时保留 `INTERVAL expr unit` 日期算术语法；新增存储集成 SQL 回归验证 `interval(...)` 可实际执行。
- `server/innodb/sqlparser`、`server/innodb/plan` 全包回归与 engine 定向函数回归通过；本轮完整 engine、全仓 Go、集群和发布候选门禁将在继续审计后刷新，不能把本轮切片测试等同于项目总完成。
- 最终验证已刷新：完整 engine `go test ./server/innodb/engine -count=1 -timeout 20m` 通过（`191.336s`），全仓 `go test ./... -count=1 -timeout 30m` 通过（engine `218.486s`、manager `14.387s`、net `3.014s` 等）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation975/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation975/cluster-report.json) 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation975/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation975/release-candidate.json) 为 `GO`，clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 在继续审计中又补齐正则 match_type：`REGEXP_LIKE` 及位置/替换/子串函数现在统一支持 `c/i/m/n/u`，大小写选项按最后一次声明生效，`m`/`n` 分别映射多行锚点和点号匹配换行，未知选项明确报错；新增 `TestRegexpLikeHonorsMatchTypeAndRejectsUnknownFlags`，plan 正则/表达式回归与 engine 正则定向回归通过。
- 正则改动后的最终验收已完成：完整 engine `202.077s`、全仓 Go engine `235.686s` 均通过；集群 smoke [`reports/compatibility/p1-cluster-current-continuation975-final/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation975-final/cluster-report.json) 为 `PASS`；最新发布候选 [`reports/compatibility/release-candidate-current-continuation975-final/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation975-final/release-candidate.json) 为 `GO`，九项全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 976

- 继续补齐 Connector/J 依赖的会话函数：计划表达式新增 `LAST_INSERT_ID()` 无参读取和 `LAST_INSERT_ID(expr)` 会话写入语义；编译表达式与解释表达式共用同一实现。
- 引擎将 `last_insert_id` 会话状态贯通到无 FROM 常量 SELECT、普通表投影和过滤表达式；DML 成功生成自增值后同步会话状态，`COM_RESET_CONNECTION` 仍清零该状态。
- 多行 INSERT/REPLACE/ON DUPLICATE KEY UPDATE 的结果现在返回本语句首个生成的自增 ID，满足 Connector/J `getGeneratedKeys` 的首键约定；显式无自增生成的 DML 不覆盖已有会话值。
- 新增并通过 `TestLastInsertIDUsesAndUpdatesSessionValue`、`TestLastInsertIDThroughSessionSQL`，后者覆盖 INSERT 后读取、表达式写入、普通表投影、多行 INSERT 及返回首个 ID。
- 修复默认路由的实际系统函数路径：`SystemVariableEngine` 现在除纯数字外也能计算 `LAST_INSERT_ID(40 + 2)` 这类标量表达式并回写会话值；新增 dispatcher 回归 `TestSystemVariableEngine_LastInsertIDReadsAndSetsSessionValue` 覆盖该场景。
- 本切片改动后的完整 engine、全仓 Go、集群 smoke 和发布候选门禁需要继续刷新；FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置，其余 P1/P3/P4 能力继续按依赖推进。
- 路由层补丁后的最终验收已完成：`go test ./... -count=1 -timeout 30m` 全仓通过；组合专项 `dispatcher/engine/net` 通过，其中 engine `107.504s`。
- 最新集群报告 [`reports/compatibility/p1-cluster-current-continuation976-final2/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation976-final2/cluster-report.json) 为 `PASS`；最新发布候选 [`reports/compatibility/release-candidate-current-continuation976-final2/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation976-final2/release-candidate.json) 为 `GO`，clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 九项全部 `PASS`。

## Continuation 977

- 继续补齐 Connector/J 依赖的结果状态函数：计划表达式新增 `ROW_COUNT()`，从连接级 `row_count` 读取最近一条成功语句的影响行数；非法带参调用明确报错，解释执行和编译执行共用同一语义。
- InnoDB 执行链将 `row_count` 贯通到常量 SELECT、普通表投影/过滤以及 INSERT/UPDATE/DELETE 结果；SELECT 在表达式计算期间读取前一条语句的值，成功结束后按 MySQL 语义写回 `-1`，DML 写回 affected rows，错误不覆盖旧值。
- 同时修复 `XMySQLEngine.ExecuteQuery` 直连入口的状态遗漏：该入口现在和 dispatcher 执行入口一样，在 INSERT/UPDATE/DELETE 成功后写回 affected rows，避免同一连接因入口不同看到不同的 `ROW_COUNT()`。
- 直连结果汇聚层现在按最终 `Result` 统一处理状态：DML 写回 affected rows，SELECT 写回 `-1`，DDL/SET/其他成功语句写回 `0`，错误结果保持既有 `row_count`，覆盖早退的 metadata/admin/DDL 兼容路径。
- 新增并通过 `TestRowCountReadsStatementSessionValue`、`TestCompiledRowCountReadsSessionValue` 和 `TestRowCountThroughSessionSQLPreservesPriorValueDuringSelect`；`P1-OPT-005`、`P1-EXE-004` 计划条目同步更新。
- 本切片最终验证已完成：`go test ./... -count=1 -timeout 30m` 的全仓执行已结束，发布门禁中的 `go-core=PASS`；集群 smoke [`reports/compatibility/p1-cluster-current-continuation977-final/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation977-final/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation977-final/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation977-final/release-candidate.json) 为 `GO`，九项检查全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 978

- 继续收口 P1-SQL-004 外键动作边界：`CREATE TABLE` 与 `ALTER TABLE ... ADD FOREIGN KEY` 现在都会显式拒绝 MySQL InnoDB 不支持的 `ON DELETE/UPDATE SET DEFAULT`，不再把未知动作静默降级为 `RESTRICT` 或留下半成品外键元数据。
- 新增并通过 `TestCreateForeignKeyRejectsSetDefaultAction`、`TestAlterForeignKeyRejectsSetDefaultAction`；现有外键和 referential-action 回归同步通过。
- 继续补强 `ROW_COUNT()` 直连结果状态：结果汇聚层统一处理 DML/SELECT/DDL/SET 的状态写回，错误保持旧值；新增 DDL 成功和确定性失败回归。受影响包 `plan/engine/dispatcher/net` 全量回归通过，engine `105.485s`，退出码 `0`。

## Continuation 979

- 继续补齐 P1 事务语义：`SET TRANSACTION READ ONLY`、`START TRANSACTION READ ONLY/READ WRITE` 和会话只读状态现在在 INSERT、UPDATE、DELETE（包括 JOIN DML）进入执行路径前统一拒绝或放行写入，并返回明确的 `READ ONLY` 错误；SELECT 仍可执行，读写事务切换后写入恢复。
- 复制回放通过 `replication_replay` 标记豁免客户端只读保护，避免副本应用源端变更时被本地事务变量拦截；新增 SQL 级回归 `TestReadOnlyTransactionRejectsSQLDML`，同时覆盖 INSERT/UPDATE/DELETE 和 SELECT。
- `START TRANSACTION READ ONLY` 与 `START TRANSACTION READ WRITE` 已在事务命令预解析层识别，避免依赖通用 parser 的非完整分支；新增 `TestNormalizedTransactionCommandSupportsReadOnlyStart` 并覆盖读写切换后的真实 INSERT。
- `START TRANSACTION READ ONLY/READ WRITE` 会同步设置或清除 `tx_read_only` 与 `transaction_read_only` 两个兼容别名，避免旧别名残留导致 READ WRITE 仍被错误拒绝；回归额外覆盖别名残留场景。
- 修复 P0 集群并发安全：复制专用会话的参数 map 改为读写锁保护；新增并发回归先稳定复现 `concurrent map writes`，修复后通过。`auth` 角色激活测试会话同步补齐读写锁，避免引擎结果收尾与下一条 SQL 并发更新测试会话时崩溃。
- 发布门禁脚本的 Go unit/integration/go-core 检查改为 `go test -p 1`，降低共享测试数据和包级后台任务的并发干扰；本轮非 JDBC 门禁的 build、unit、integration、go-core、crash-recovery、concurrency、observability 均通过，集群 smoke 通过。
- 前一轮完整门禁报告 [`reports/compatibility/release-candidate-current-continuation979-final/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation979-final/release-candidate.json) 中 Connector/J 已通过 `139` tests、`0` failures、`0` errors、`0` skipped；该报告唯一失败是修复前 `auth` 测试会话并发 map 崩溃。修复后 unit 四包以 `-count=2` 通过。
- 修复后的最终发布候选 [`reports/compatibility/release-candidate-current-continuation979-final2/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation979-final2/release-candidate.json) 返回 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、100 次并发、observability 和 Connector/J 九项全部通过，Connector/J 为 `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。集群 smoke [`reports/compatibility/p1-cluster-current-continuation979-final/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation979-final/cluster-report.json) 同样为 `PASS`。
- 在追加 `START TRANSACTION READ ONLY/READ WRITE` 后，engine 全量回归 `go test ./server/innodb/engine -count=1 -timeout 20m` 仍通过（`106.833s`）；集群 smoke 更新报告 [`reports/compatibility/p1-cluster-current-continuation979-final2/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation979-final2/cluster-report.json) 为 `PASS`。完整 GO 报告对应的是前一项并发修复后的工作树，新增事务命令扩展由该 engine 全量回归覆盖。
- 本轮仍不宣称 MySQL 8.4 全量兼容：FULLTEXT 与非 Connector/J 全量客户端继续按用户明确要求后置；完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 native binlog/GTID/PITR 互操作、真实 R-tree/空间物理格式和更宽的 P1/P3/P4 矩阵继续按后续优先级推进。

## Continuation 980

- 继续补齐 P1 服务器只读语义：`read_only` 全局变量现在在真实 `XMySQLEngine.ExecuteQuery` 入口阻断普通客户端 INSERT/UPDATE/DELETE，同时保留 SELECT 可读。
- `SET GLOBAL read_only = ON/OFF` 现在写入真实系统变量管理器；操作要求 `SUPER`、`SYSTEM_VARIABLES_ADMIN` 或等效全局权限，避免把全局配置误当成会话变量。
- `SHOW GLOBAL VARIABLES LIKE 'read_only'` 现在从同一全局变量存储读取并按 MySQL 的 `ON/OFF` 形式返回；同时修复 `ListVariables` 的锁重入风险，避免系统变量枚举在写锁竞争下死锁。
- 全局只读保护对复制回放显式豁免；客户端持有 `SUPER`/`CONNECTION_ADMIN` 时保留 MySQL 管理员写入通道。新增 SQL 级回归覆盖普通客户端、管理员、复制回放以及关闭只读后的恢复写入。
- `SET TRANSACTION READ ONLY/READ WRITE` 现在在执行器和系统变量兼容路径同步维护 `tx_read_only` 与 `transaction_read_only` 两个别名，新增真实 SQL 回归覆盖只读阻断和读写切换恢复。
- 存储集成 INSERT/UPDATE/DELETE 启动真实 `TransactionManager` 事务时，现在从连接状态传入隔离级别（RU/RC/RR/Serializable）和只读标志，避免底层事务元数据始终回落到默认 RR/READ WRITE。
- 本切片聚焦 Connector/J/P0-P1 运行链路；FULLTEXT、非 Connector/J 全量客户端以及开发计划中列出的完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 native binlog/GTID/PITR 互操作和真实 R-tree 物理格式仍未完成。

## Continuation 981

- 继续收敛 P1-TXN-003：`PERFORMANCE_SCHEMA.events_transactions_current/history/history_long` 的事务 `ACCESS_MODE` 不再固定返回 `READ WRITE`；`START TRANSACTION READ ONLY` 会在会话事务状态中锁定 `READ ONLY`，当前事务和提交后的历史事件均返回真实访问模式。事务状态清理时同步清除该快照，避免复用连接后污染下一事务。
- 回归证据：`go test ./server/innodb/engine -run 'TestPerformanceSchemaTransaction|TestTransactionStatements|TestReadOnlyTransaction|TestGlobalReadOnly|TestTransactionContext' -count=1 -timeout 20m` 通过。
- 本轮新代码门禁证据：`go test ./server/innodb/engine -count=1 -timeout 20m` 通过（106.311s）；`reports/compatibility/p1-cluster-current-continuation981/cluster-report.json` 于 `2026-09-13T00:01:12Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation981/release-candidate.json` 于 `2026-09-13T00:22:21Z` 为 `GO`，engine integration/go-core、crash-recovery、concurrency、observability 和 Connector/J 139 tests 全部通过（0 failures、0 errors、0 skipped）。
- 本轮仍不宣称 MySQL 8.4 全量兼容：完整事务变量生命周期、XA prepare/recovery、完整 online DDL/MDL、全量 I_S/P_S、复杂 AST、原生 binlog/GTID/PITR 全互操作、真实 R-tree 物理格式继续推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 982

- 继续收敛 P1 事务能力：新增原始 SQL XA 状态机，支持基础 `XA START`、`XA END`、`XA PREPARE`、`XA COMMIT`、`XA ROLLBACK` 和 `XA RECOVER`；预提交阶段复用存储集成 DML 的可逆行变更日志，二阶段提交/回滚会进入现有 replication commit hook 和 Performance Schema 事务历史边界，协调会话可提交其他会话持有的 prepared XID。
- 回归证据：`TestParseXAStatementSupportsBasicXIDForms`、`TestXACompatibilitySupportsPrepareRecoverCommitAndRollback`、`TestXACompatibilityAllowsPreparedTransactionToBeCommittedByAnotherSession`、`TestXACompatibilitySupportsOnePhaseCommit` 均通过。
- 当前 XA 仍有明确边界：prepared XID 尚未做跨进程/重启后的完整资源恢复，XA JOIN/RESUME/SUSPEND 选项、完整二进制 XA 日志互操作和 XA_RECOVER 权限矩阵仍需继续收敛；不能据此宣称完整 XA 兼容。FULLTEXT 与非 Connector/J 全量客户端继续后置。
- 新代码门禁证据：`go test ./server/innodb/engine -count=1 -timeout 20m` 通过（108.399s）；`reports/compatibility/p1-cluster-current-continuation982/cluster-report.json` 于 `2026-09-13T00:32:08Z` 为 `PASS`；`reports/compatibility/release-candidate-current-continuation982/release-candidate.json` 于 `2026-09-13T00:54:51Z` 为 `GO`，核心 Go、恢复、并发、观测及 Connector/J 139 tests 全部通过。
- 本轮最终门禁 [`reports/compatibility/release-candidate-current-continuation980/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation980/release-candidate.json) 返回 `GO`；clean-data、build、unit、integration、go-core、crash-recovery、concurrency、observability 和 JDBC 九项全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`（12:00 min）。engine/manager 全量回归分别为 `109.274s`、`9.057s`，集群 smoke [`reports/compatibility/p1-cluster-current-continuation980/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation980/cluster-report.json) 为 `PASS`。

## Continuation 983

- 继续收口 XA 生命周期：`COM_RESET_CONNECTION` 现在拒绝清理处于 `PREPARED` 状态的 XA 会话，避免把仍可由 `XA RECOVER` 找到的 prepared XID 及其可逆 DML 日志静默丢失；完成 XA 后仍可正常 reset。
- 回归证据：新增 `TestXACompatibilityDoesNotDiscardPreparedTransactionOnSessionReset`，与基础 XA/XID/跨会话/ONE PHASE 回归共同通过；`go test ./server/innodb/engine -count=1 -timeout 20m` 通过（108.151s）；`reports/compatibility/p1-cluster-current-continuation983/cluster-report.json` 于 `2026-09-13T00:58:05Z` 为 `PASS`。
- 本切片仅刷新了 engine 与集群证据，上一轮完整发布门禁仍是 continuation982 的 `GO`；加入 reset 防护后的完整 Connector/J 发布门禁需要下一步继续刷新。XA 跨进程/重启恢复、JOIN/RESUME/SUSPEND、完整原生 XA 日志互操作和权限矩阵仍未完成。

## Continuation 984

- 继续完成 P1 XA 恢复闭环：`XA PREPARE` 现在将 XID、可逆 DML 行变更、复制语句和 journal 标识写入 `transactions/prepared/*.json` 原子 manifest；引擎启动时先恢复 prepared manifest，再执行普通孤儿事务回滚，避免误回滚仍可由 XA 协调者提交/回滚的事务。
- `XA RECOVER`、跨会话 `XA COMMIT/XA ROLLBACK` 现在支持重启后加载的 prepared XID；回滚会使用持久化行镜像恢复数据，提交会进入 replication commit hook，成功完成后删除 manifest 和 active journal。
- `COM_RESET_CONNECTION`、普通 `BEGIN/COMMIT/ROLLBACK` 和普通 INSERT/UPDATE/DELETE 在 XA `PREPARED` 状态均不再静默清理或修改 prepared 事务；XA PREPARE 暂拒绝未持久化建模的事务性账号 metadata 变更，避免部分提交。
- 回归证据：新增并通过 `TestXACompatibilityRecoversPreparedTransactionAcrossEngineRestart`、`TestXACompatibilityRejectsRegularSessionMutationWhilePrepared`；engine 全量 `107.282s` 通过，集群 smoke [`reports/compatibility/p1-cluster-current-continuation984/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation984/cluster-report.json) 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation984/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation984/release-candidate.json) 返回 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- XA 基础 SQL/跨重启恢复已完成当前最小正确路径；JOIN/RESUME/SUSPEND、完整 XA 权限矩阵、原生二进制 XA 日志互操作仍保留边界。FULLTEXT 与非 Connector/J 全量客户端继续按用户要求后置，项目整体继续推进，不能据此宣称 MySQL 8.4 全量兼容。

## Continuation 985

- 继续收口 XA 安全边界：`XA RECOVER` 现在要求 `XA_RECOVER_ADMIN`，同时保留 `SUPER/ALL` 管理员通道；prepared 状态下普通 `BEGIN/COMMIT/ROLLBACK` 和 DML 均明确拒绝，避免误清理或篡改二阶段事务。
- 变更后的专项回归与完整 engine 回归通过：`go test ./server/innodb/engine -count=1 -timeout 20m` 用时 `107.834s`。
- 集群 smoke [`reports/compatibility/p1-cluster-current-continuation985/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation985/cluster-report.json) 为 `PASS`；最新发布候选 [`reports/compatibility/release-candidate-current-continuation985/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation985/release-candidate.json) 为 `GO`。
- 本轮发布门禁九项全部 `PASS`：clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability、JDBC；Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 当前 XA 剩余边界收窄为 JOIN/RESUME/SUSPEND、完整 XA 二进制日志互操作及更细的所有者/协调者权限矩阵；FULLTEXT 与非 Connector/J 全量客户端继续后置，不能据此宣称 MySQL 8.4 全量兼容。

## Continuation 986

- 继续收敛 P1-TXN-003：`SET SESSION TRANSACTION ISOLATION LEVEL` 现在校验 `READ UNCOMMITTED`、`READ COMMITTED`、`REPEATABLE READ`、`SERIALIZABLE` 四种级别，并同步维护 `transaction_isolation` 与 `tx_isolation` 两个兼容别名；实际事务上下文会使用设置后的隔离级别。
- 新增 `TestSetTransactionIsolationSynchronizesCompatibilityAliases`，验证别名同步、事务状态快照和底层 `isolation_level` 上下文；engine、dispatcher、net、protocol 专项回归均通过。
- 本轮 engine 全量 `107.190s` 通过，集群 smoke [`reports/compatibility/p1-cluster-current-continuation986/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation986/cluster-report.json) 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation986/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation986/release-candidate.json) 返回 `GO`；九项门禁全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 当前仍需继续收敛的是无作用域 `SET TRANSACTION` 的 next-transaction 生命周期、完整隔离级别变量的 global/session 继承及其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 987

- 继续收口事务系统变量的多入口一致性：dispatcher 现在识别 `SET GLOBAL` 语句级作用域，不再把 `SET GLOBAL TRANSACTION ISOLATION LEVEL ...` 错误写入会话；`transaction_isolation/tx_isolation` 与 `transaction_read_only/tx_read_only` 在全局作用域下成对原子更新。
- dispatcher 全局系统变量写入补齐 `SUPER`、`ALL` 和 `SYSTEM_VARIABLES_ADMIN` 权限校验，并统一隔离级别规范化与非法值拒绝；新增 `TestSystemVariableEngine_SetGlobalTransactionAliasesStaySynchronized`，覆盖 SERIALIZABLE 和 READ ONLY 两组别名。
- 专项回归：`go test ./server/dispatcher ./server/net ./server/protocol -count=1` 通过；集群 smoke [`reports/compatibility/p1-cluster-current-continuation987/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation987/cluster-report.json) 于 `2026-09-13T02:33:47Z` 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation987/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation987/release-candidate.json) 于 `2026-09-13T02:57:45Z` 返回 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`，engine integration `109.827s`、go-core `232.943s`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 当前仍不宣称 MySQL 8.4 全量兼容：XA JOIN/RESUME/SUSPEND、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作及其他 P1/P3/P4 高级语义仍需继续逐项收敛；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 988

- 继续完成 P1 XA 生命周期：`XA END ... SUSPEND [FOR MIGRATE]` 会把当前 XA 事务置为不可执行 DML 的挂起态；`XA START ... RESUME/JOIN` 可恢复同一会话挂起事务，也可由另一连接接管共享事务状态和 journal，接管后继续 DML、PREPARE 并由原连接 COMMIT/ROLLBACK。
- 共享 XA 事务在完成时会清理所有参与连接的 `xa_state`、事务上下文、journal 标记和连接事务状态；新增解析回归、挂起期间写保护、RESUME、JOIN、跨连接 PREPARE/COMMIT 回归，避免 JOIN 只停留在语法接受层。
- dispatcher 的全局事务变量别名修复与本轮 XA 变更共同通过完整发布门禁：[`reports/compatibility/release-candidate-current-continuation988/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation988/release-candidate.json) 于 `2026-09-13T03:26:41Z` 为 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- engine 全量回归 `go test ./server/innodb/engine -count=1 -timeout 20m` 通过，用时 `108.084s`；同版本集群 smoke [`reports/compatibility/p1-cluster-current-continuation988/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation988/cluster-report.json) 于 `2026-09-13T03:26:57Z` 为 `PASS`。
- XA 当前剩余边界收窄为跨进程挂起态恢复、完整 XA 所有者/协调者权限矩阵、原生二进制 XA 日志互操作；完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作及其他 P1/P3/P4 高级语义仍需继续收敛。FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 989

- 继续收口 P1 事务变量继承：`SET GLOBAL TRANSACTION ISOLATION LEVEL` 与 `SET GLOBAL TRANSACTION READ ONLY` 设置的默认值，现在会在新连接第一次 `START TRANSACTION` 建立事务快照时继承；已有会话级覆盖和无作用域 `SET TRANSACTION` 的一次性 next-transaction 值优先级不变，两个兼容别名保持同步。
- 新增 `TestGlobalTransactionDefaultsApplyToNewSession`，覆盖新连接继承 `READ COMMITTED`/`READ ONLY`；修复过程中发现并回归了指标标签不应从 `REPEATABLE-READ` 静默变为 `REPEATABLE READ` 的兼容边界。
- XA 的跨连接 `JOIN/RESUME/SUSPEND`、全局事务默认继承和 dispatcher 全局变量入口共同通过 engine 全量：`go test ./server/innodb/engine -count=1 -timeout 20m` 用时 `108.374s`；集群 smoke [`reports/compatibility/p1-cluster-current-continuation989/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation989/cluster-report.json) 于 `2026-09-13T03:56:27Z` 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation989/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation989/release-candidate.json) 于 `2026-09-13T03:56:17Z` 返回 `GO`；九项门禁全部 `PASS`，integration engine `213.870s`、go-core engine `143.037s`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 当前剩余边界仍包括 XA 跨进程挂起态恢复、完整 XA 权限/原生日志互操作、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 990

- 继续补齐 P1 事务语法：`START TRANSACTION WITH CONSISTENT SNAPSHOT`、`WITH CONSISTENT SNAPSHOT, READ ONLY` 和 `WITH CONSISTENT SNAPSHOT READ WRITE` 现在进入事务命令规范化路径，并保留已有 `READ ONLY/READ WRITE` 语义及只读写入保护。
- 新增并通过 `TestNormalizedTransactionCommandSupportsReadOnlyStart` 的组合回归；随后 engine 全量 `go test ./server/innodb/engine -count=1 -timeout 20m` 通过，用时 `201.894s`。
- 集群 smoke [`reports/compatibility/p1-cluster-current-continuation990/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation990/cluster-report.json) 于 `2026-09-13T04:03:33Z` 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation990/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation990/release-candidate.json) 于 `2026-09-13T04:24:11Z` 返回 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`（12:08 min）。
- 当前仍不宣称 MySQL 8.4 全量兼容：XA 跨进程挂起态恢复、完整 XA 权限/原生日志互操作、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作及其他 P1/P3/P4 高级语义继续逐项推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 993

- 继续补齐 P1 事务命令兼容：`BEGIN WORK` 现在与 `BEGIN` 走同一真实事务状态初始化路径，不再落入通用 SQL parser 的不支持分支。
- 新增 `TestNormalizedTransactionCommandSupportsReadOnlyStart` 的 `BEGIN WORK` 回归；engine 全量 `go test ./server/innodb/engine -count=1 -timeout 20m` 通过，用时 `109.199s`。
- 集群 smoke [`reports/compatibility/p1-cluster-current-continuation993/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation993/cluster-report.json) 于 `2026-09-13T05:17:31Z` 为 `PASS`；最新发布候选 [`reports/compatibility/release-candidate-current-continuation993/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation993/release-candidate.json) 于 `2026-09-13T05:40:57Z` 返回 `GO`，九项门禁全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`（11:59 min）。
- 当前仍不宣称 MySQL 8.4 全量兼容：XA 跨进程挂起态恢复、完整 XA 权限/原生日志互操作、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作及其他 P1/P3/P4 高级语义继续逐项推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 992

- 继续收口 P1 事务变量边界：活动事务中执行无作用域或 `SESSION` 作用域的 `SET TRANSACTION ...` 现在明确返回 `Transaction characteristics can't be changed while a transaction is in progress`，不再错误修改下一事务配置；`SET GLOBAL TRANSACTION ...` 仍允许管理员更新未来连接默认值。
- engine 与 dispatcher 两条 SET 入口共同接入该保护；新增并通过 `TestSetTransactionCharacteristicsRejectsActiveTransaction` 和 `TestSetGlobalTransactionCharacteristicsAllowedDuringActiveTransaction`，覆盖隔离级别、只读特征及 global 例外。
- 本轮 engine 与 dispatcher 包级回归均通过，engine 用时 `108.924s`；最新发布候选 [`reports/compatibility/release-candidate-current-continuation992/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation992/release-candidate.json) 于 `2026-09-13T05:14:32Z` 返回 `GO`，九项门禁全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`（12:07 min）。
- 当前仍不宣称 MySQL 8.4 全量兼容：XA 跨进程挂起态恢复、完整 XA 权限/原生日志互操作、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作及其他 P1/P3/P4 高级语义继续逐项推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 991

- 继续收口 P1 事务生命周期：`COMMIT AND CHAIN` 与 `ROLLBACK AND CHAIN` 现在在原事务完成后立即开启下一事务，并继承刚结束事务的隔离级别及 `READ ONLY/READ WRITE` 访问模式；`NO CHAIN` 保持普通提交/回滚，`RELEASE` 变体继续明确拒绝而不静默接受。
- 新增并通过 `TestNormalizedTransactionCommandSupportsCommitAndRollbackChain`、`TestCommitAndRollbackChainStartTheNextTransaction`，覆盖语法识别、链式事务仍 active、隔离级别/访问模式继承和最终回滚收尾。
- 本轮 engine 全量回归 `go test ./server/innodb/engine -count=1 -timeout 20m` 通过，用时 `109.533s`；集群 smoke [`reports/compatibility/p1-cluster-current-continuation991/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation991/cluster-report.json) 于 `2026-09-13T04:29:32Z` 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation991/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation991/release-candidate.json) 于 `2026-09-13T04:52:09Z` 返回 `GO`；clean-data、build、unit、integration、go-core、3 次 crash-recovery、concurrency、observability、JDBC 九项全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`（12:01 min）。
- 当前仍不宣称 MySQL 8.4 全量兼容：XA 跨进程挂起态恢复、完整 XA 权限/原生日志互操作、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 AST、完整 stored object/type/function/collation 矩阵、物理分区路由、原生 binlog/GTID/PITR 全互操作及其他 P1/P3/P4 高级语义继续逐项推进；FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

## Continuation 994

- 继续收口 P1 事务命令兼容：`COMMIT WORK`、`ROLLBACK WORK` 及其 `AND CHAIN/NO CHAIN` 变体现在与不带 `WORK` 的形式进入同一真实提交、回滚和链式事务路径；不支持的 `RELEASE` 变体仍明确拒绝。
- 新增回归覆盖六种 `WORK` 语法组合；按 TDD 先确认旧实现对 `commit work` 失败，再完成规范化实现，定向 engine 回归通过。
- 本切片尚未刷新完整 engine/集群/发布门禁；继续推进下一个剩余 P1/P3/P4 兼容边界。FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 995

- 在 `WORK` 变体基础上继续补齐事务完成语法：`COMMIT/ROLLBACK RELEASE`、`WORK RELEASE`、`AND CHAIN RELEASE` 和 `AND NO CHAIN RELEASE` 现在可被识别；完成事务后设置连接关闭标记，网络层在发送成功响应后关闭连接，避免丢失 OK 包。
- 新增 `TestCommitReleaseMarksSessionForClose`，验证真实事务提交和连接关闭标记；engine 与 net 定向回归通过。
- 本切片尚未刷新完整 engine/集群/发布门禁；项目仍继续推进剩余 P1/P3/P4 边界，FULLTEXT 与非 Connector/J 全量客户端按用户要求后置。

- 修复包级回归：新增完成语法的前缀识别收窄为 `commit/rollback work|and|release`，不再截获已有 `ROLLBACK TO SAVEPOINT`；engine 定向保存点矩阵和 engine/net 包级回归均通过（engine `109.675s`、net `1.489s`）。

- 兼容两条协议入口：legacy `MySQLMessageHandler` 与 decoupled handler 都会在成功发送 `COMMIT/ROLLBACK RELEASE` 的 OK 包后关闭传输连接；net 包回归通过。

## Continuation 996

- 继续完成 XA 剩余生命周期边界：`XA END ... SUSPEND` 现在把事务的 change journal、replication statements、保存点和事务特征写入 `transactions/suspended/<sha256>.json`；engine 重启时加载挂起 manifest，并将其 journal 从孤儿事务回滚集合中排除。
- 新连接可以在重启后执行 `XA START ... JOIN/RESUME`，成功接管后删除挂起 manifest，继续 `XA END → XA PREPARE → XA COMMIT/ROLLBACK`；接管失败会恢复内存索引，完成时清理共享 journal 绑定，避免旧连接 journal 文件泄漏。
- 新增并通过 `TestXACompatibilityRecoversSuspendedTransactionAcrossEngineRestart`，XA 全套 `TestXACompatibility*` 回归通过；完整 engine/release/集群证据待本轮刷新。
- 当前 XA 剩余边界收窄为完整 XA 所有者/协调者权限矩阵与原生二进制 XA 日志互操作；FULLTEXT 与非 Connector/J 全量客户端按用户要求继续后置。

- engine 全量回归 `go test ./server/innodb/engine -count=1 -timeout 20m` 通过，用时 `109.387s`；集群 smoke [`reports/compatibility/p1-cluster-current-continuation996/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation996/cluster-report.json) 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation996/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation996/release-candidate.json) 返回 `GO`；九项门禁全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 997

- 继续收口 XA XID 生命周期：普通 `XA START` 现在同时检查内存中的 prepared/suspended XID，挂起事务不能被其他连接以普通 START 重复创建；重复会返回 `XAER_DUPID`，只有 `JOIN/RESUME` 才能接管。
- 新增跨进程挂起回归中的重复 XID 检查，先行测试确认旧实现错误放行，修复后 XA 专项 `TestXACompatibility*` 全部通过；engine/release 证据待本轮刷新。

- 本轮 engine 全量回归 `go test ./server/innodb/engine -count=1 -timeout 20m` 通过，用时 `293.329s`；集群 smoke [`reports/compatibility/p1-cluster-current-continuation997/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation997/cluster-report.json) 为 `PASS`。
- 最新发布候选 [`reports/compatibility/release-candidate-current-continuation997/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation997/release-candidate.json) 返回 `GO`；九项门禁全部 `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 998

- 继续收口 P1-TXN-003：`SET TRANSACTION` 现在允许同一条语句组合一个隔离级别与一个访问模式（例如 `ISOLATION LEVEL READ COMMITTED, READ ONLY`），并在执行前拒绝重复/冲突的隔离级别或访问模式，避免“最后一个值覆盖前一个值”及半状态写入。
- 新增 `TestSetTransactionCharacteristicsCanBeCombined` 与 `TestSetTransactionRejectsConflictingAccessModes`；先行回归确认旧实现错误接受冲突 characteristic，修复后定向 engine 回归通过。
- 本切片已验证：`go test ./server/innodb/engine -run '^(TestSetTransactionCharacteristicsCanBeCombined|TestSetTransactionRejectsConflictingAccessModes)$' -count=1` 通过；完整 engine/集群/release 门禁尚未因本切片刷新，继续推进剩余 P1/P3/P4 边界。FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 999

- 继续收口 P1-TXN-003 的默认值生命周期：`SET SESSION TRANSACTION READ ONLY` 现在只影响后续显式事务，不会在当前未开启事务时错误阻断自动提交 DML；显式 `START TRANSACTION` 后仍按 READ ONLY 拒绝写入。无作用域 `SET TRANSACTION READ ONLY` 仍对下一条事务生效。
- `transactionContextForSession` 与 SQL DML guard 现在区分“当前事务访问模式”和“后续事务默认值”，避免把 session/global 默认值误当作当前只读事务；同步调整单元回归，使 READ ONLY 断言明确建立在事务上下文中。
- 回归证据：专项事务矩阵通过；`go test ./server/innodb/engine -count=1 -timeout 20m` 通过（`117.574s`）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation999/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation999/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation999/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation999/release-candidate.json) 返回 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 当前仍继续推进剩余 P1/P3/P4 兼容边界；复杂 AST、完整 online DDL/MDL、全量 Performance Schema/native binlog/GTID/PITR 上游互操作、真实 R-tree/空间物理格式和完整授权矩阵尚未宣称完成。FULLTEXT 查询语义与非 Connector/J 全量客户端仍按用户明确要求后置。

## Continuation 100

- 继续收口 P1-TXN-003 的 `START TRANSACTION` 语法：`WITH CONSISTENT SNAPSHOT` 与 `READ ONLY/READ WRITE` 现在支持 MySQL 允许的逗号分隔、任意顺序组合；重复快照、重复访问模式、冲突访问模式和未知 characteristic 均在事务启动前拒绝。
- 新增交换顺序及非法组合回归；事务/XA 专项通过，完整 engine `go test ./server/innodb/engine -count=1 -timeout 20m` 通过（`112.839s`）。
- 当前工作树验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation100/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation100/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation100/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation100/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 继续推进剩余 P1/P3/P4：复杂 AST、真正 online DDL/完整 MDL、全量 Performance Schema、完整 native binlog/GTID/PITR 上游互操作、真实 R-tree/空间物理格式和完整授权矩阵仍未完成；FULLTEXT 与非 Connector/J 全量客户端按用户明确要求后置。

## Continuation 101

- 继续收口 P1-TXN-003：`START TRANSACTION` 的 `WITH CONSISTENT SNAPSHOT`、`READ ONLY`、`READ WRITE` characteristic 现在按 MySQL 语法允许任意顺序组合，并拒绝重复/冲突/未知选项；此前仅固定顺序可识别的边界已补齐。
- 新增交换顺序与非法组合回归；事务/XA 专项通过，完整 engine `go test ./server/innodb/engine -count=1 -timeout 20m` 通过（`112.839s`）。
- 当前工作树验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation100/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation100/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation100/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation100/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 102

- 继续收口 P1-SQL-002：集合运算外层 `ORDER BY` 不再按空格拆分后误把表达式中的 `-`、函数参数等内容当作排序方向；现在通过 SQL parser 解析每个完整排序项，支持列序号、结果别名、函数/算术表达式及多列 ASC/DESC，并在排序前对每行求值。
- 新增 `TestSetOperationOuterOrderByEvaluatesExpressions`，覆盖 `abs(n - 2)` 与第二排序项的稳定组合；先行回归确认旧实现将 `-` 报为错误方向，修复后集合运算定向矩阵通过。
- 本切片修改 `server/innodb/engine/executor.go`、`server/innodb/engine/complex_select_compatibility_test.go` 及 P1 backlog；完整 engine、集群及发布候选门禁尚未因本切片刷新，继续推进剩余 P1/P3/P4 边界。FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 103

- 继续收口 P1-IDX-002：`ANALYZE TABLE` 已将扫描得到的逻辑非空载荷字节数持久化到 `InfoTableStats.DataSize`，并由 `RowCount` 推导 `AvgRowSize`；此前只更新列/索引基数而留下 `DataSize=0`，会误导 INFORMATION_SCHEMA/优化器消费者。
- 新增 `TestAnalyzeTablePersistsOptimizerStatistics` 的 `DataSize` 与 `AvgRowSize` 回归断言；先行测试确认旧实现返回 0，修复后通过。
- 该切片只关闭逻辑数据大小的零值缺口，物理页级估算、索引字节大小及完整存储采样仍明确为后续工作；继续推进其余 P1/P3/P4 边界。FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 104

- 继续收口 P1-SQL-001：派生表相关 `EXISTS` 兼容路径现在通过 parser 解析完整的外层 `ORDER BY` 项，支持多个排序表达式及各自的 ASC/DESC，而不是把整个逗号列表当成一个表达式；每个派生外层行先求出全部排序键，再稳定排序并执行原有 `LIMIT/OFFSET`。
- 新增 `TestCorrelatedExistsSupportsMultiSourceDerivedOuterSource` 的多列排序回归；先行测试确认旧实现将 `desc` 误解析为表达式导致语法错误，修复后通过。
- 继续保留边界：更通用相关外层 AST、复杂相关 DML、物理页级统计和 FULLTEXT 等未完成项不因本切片扩大而宣称完成。

## Continuation 105

- 继续收口 P1-IDX-002：`information_schema.statistics.PAGES` 不再固定返回 0；`PRIMARY` 使用现有 clustered B+Tree 的叶页链计数，二级索引使用现有 `IndexManager` 持久化页计数，并明确这是兼容性页数估计而非上游 InnoDB 的完整物理采样。
- 新增 `TestInformationSchemaStatisticsExposesIndexPages`，覆盖真实表的 `PRIMARY` 与二级索引；先行测试确认旧实现对 `PRIMARY` 返回 0，接入已有页统计来源后通过。
- 该切片只关闭 `PAGES` 零值缺口；物理页级估算精度、索引字节大小和完整存储采样仍未完成。继续推进其余 P1/P3/P4 边界；FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 106

- 继续收口 P1-OPT-003：`INFORMATION_SCHEMA.TABLES` 不再把已通过 `ANALYZE TABLE` 持久化的 `TABLE_ROWS`、`AVG_ROW_LENGTH`、`DATA_LENGTH` 丢弃为固定零值；优先读取信息模式统计缓存/sidecar，未分析表保留物理行数回退，`INDEX_LENGTH` 使用现有 clustered/secondary index 页计数乘 InnoDB 页大小的兼容估计。
- 新增 `TestInformationSchemaTablesExposesAnalyzedLogicalSizes`，覆盖真实表、二级索引和 ANALYZE 后的四项统计投影；旧路径无法满足非零逻辑大小断言，接入统计投影后通过。
- 该切片仍不宣称完整上游物理统计采样：页碎片/物理字节精度、全量采样和所有统计刷新边界继续保留；FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 107

- 继续收口 P1-OPT-003：`ANALYZE TABLE` 现在把 clustered/secondary index 的现有页计数转换为持久化的 `InfoTableStats.IndexSize` 兼容估算，避免统计缓存和 sidecar 始终留下 `IndexSize=0`；`INFORMATION_SCHEMA.TABLES.INDEX_LENGTH` 因此优先读取同一持久化值。
- 扩展 `TestAnalyzeTablePersistsOptimizerStatistics`，要求分析后的索引大小非零；先行测试确认旧实现为 0，接入页计数估算后通过。
- 该切片仍不等同于上游 InnoDB 的精确物理字节统计，页碎片/压缩/分区采样等边界继续保留；FULLTEXT 与非 Connector/J 全量客户端仍按用户要求后置。

## Continuation 108

- 继续收口 P1-OPT-003：`SHOW TABLE STATUS` 现在复用 `INFORMATION_SCHEMA.TABLES` 的统计投影，优先读取 ANALYZE 后的 `TABLE_ROWS`、`AVG_ROW_LENGTH`、`DATA_LENGTH`、`INDEX_LENGTH`，未分析表继续保留物理行数回退。
- 扩展 `TestShowTableStatusReturnsMySQLMetadataShape`，覆盖 ANALYZE 后四项统计值均为非零；旧路径的长度字段固定为 0，修复后通过。
- 物理统计采样精度、压缩/分区/碎片细节及 FULLTEXT/非 Connector/J 全量客户端仍未完成，继续推进下一项兼容边界。

## Continuation 109

- 继续收口 P1-SQL-001/P1-SQL-002：派生表、聚合派生和 CTE 兼容物化分支的 `ORDER BY` 现在逐项比较完整排序键，支持多列及各自 ASC/DESC；此前这些分支只使用第一项排序键，存在并列值下 `LIMIT` 选错行的风险。
- 新增派生表多列排序与聚合派生多列排序回归，并统一 CTE/派生路径的稳定排序比较器；engine 定向回归通过。
- 本切片不扩大 FULLTEXT、非 Connector/J 全量客户端、复杂通用 AST、物理统计采样和跨节点 fencing 的范围；这些边界继续按用户确定的优先级后置推进。

## Continuation 110

- 继续补齐 Connector/J/P1 表达式与会话元数据：`DATABASE()`、`SCHEMA()`、`USER()`、`CURRENT_USER()`、`SESSION_USER()`、`SYSTEM_USER()`、`CURRENT_ROLE()`、`VERSION()`、`CONNECTION_ID()` 现在从连接会话状态求值，并同时覆盖常量 SELECT、普通表投影和编译表达式路径。
- `SCHEMA()` 因内置 parser 将 `SCHEMA` 保留给 DDL，增加引号/标识符边界感知的兼容重写为 `DATABASE()`；不会改写字符串、反引号内容或 `schema.table` 引用。`CURRENT_ROLE()` 无激活角色返回 `NONE`，连接 ID 优先读取协议连接 ID。
- 新增 `TestSessionMetadataFunctionsThroughSessionSQL` 与 `TestCompiledSessionMetadataFunctionsReadSessionValues`，覆盖会话值、角色、连接 ID、常量/表查询和编译路径；两项定向回归通过。
- 本切片继续保留 FULLTEXT、非 Connector/J 全量客户端、复杂通用 AST、完整 online DDL/MDL、全量 I_S/P_S、真实物理 R-tree、跨节点强 fencing 等后置边界；完整 engine、集群和发布门禁在本轮继续刷新。
- 验证完成：`go test ./server/innodb/engine -count=1 -timeout 20m` 通过（107.766s）；`go test ./... -count=1 -timeout 30m` 全仓通过，其中 engine 115.744s、manager 11.604s、net 2.852s、replication 2.139s。
- 集群 smoke [`reports/compatibility/p1-cluster-current-continuation110/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation110/cluster-report.json) 于 `2026-09-14T03:04:57.6531019Z` 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation110/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation110/release-candidate.json) 于 `2026-09-14T03:33:03.0094045Z` 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 111

- 继续补齐 P1-SQL-001/P1-Connector.J 的嵌套会话表达式语义：派生表兼容物化行和递归 CTE 的 anchor/member/main projection 现在携带同一份会话表达式上下文，`DATABASE()`、`USER()`、`CURRENT_ROLE()`、`CONNECTION_ID()` 等不会在嵌套路径退化为空值或 `NONE/0`。
- 新增 `TestSessionMetadataFunctionsPropagateThroughDerivedProjection`、`TestSessionMetadataFunctionsPropagateThroughCTEProjection` 和 `TestSessionMetadataFunctionsPropagateThroughRecursiveCTE`；先行回归确认派生/递归路径旧实现返回空值或 `NONE/0`，修复后通过。
- 本切片仍不扩大 FULLTEXT、非 Connector/J 全量客户端、复杂通用 AST、完整 online DDL/MDL、全量 I_S/P_S、真实物理 R-tree 和跨节点强 fencing 的范围；这些边界继续按用户确定的优先级后置推进。

## Continuation 112

- 继续补齐 P1-SQL-001/P1-Connector.J 的集合物化嵌套路径：集合运算生成的派生表、派生 JOIN 及其外层投影/过滤现在统一挂载所属连接的会话表达式上下文，避免 `DATABASE()`、`CURRENT_ROLE()`、`CONNECTION_ID()` 在 raw set/derived-join 入口返回空值或默认值。
- 新增 `TestSessionMetadataFunctionsPropagateThroughSetDerivedProjection` 与 `TestSessionMetadataFunctionsPropagateThroughDerivedJoinProjection`；先行回归确认旧实现返回空值或 `NONE/0`，修复后定向 engine 回归通过。
- 该切片仍保留复杂通用 AST、FULLTEXT、非 Connector/J 全量客户端、完整 online DDL/MDL、全量 I_S/P_S、真实物理 R-tree 和跨节点强 fencing 等明确边界。

## Continuation 113

- 继续补齐 P0-Connector.J/P1-SYS 的连接初始化语义：`SET NAMES <charset> COLLATE <collation>` 现在在 engine 与 dispatcher 两条 SET 路径都更新 `character_set_client/connection/results` 及 `collation_connection`；dispatcher 不再把 `names` 当成未注册系统变量而跳过会话同步。
- 新增 engine 回归 `TestSetNamesCollationUpdatesConnectionSession` 与 dispatcher 回归 `TestSystemVariableEngine_SetNamesCollationSyncsSessionState`；旧实现先行回归确认显式 COLLATE 丢失，修复后通过。
- 本切片不改变 FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界的后置优先级。

- 验证完成：`go test ./server/innodb/engine ./server/dispatcher -count=1 -timeout 20m` 通过（engine 115.723s、dispatcher 0.854s）；`go test ./... -count=1 -timeout 30m` 全仓通过（engine 114.025s、manager 11.250s、net 3.098s、replication 2.292s）。
- 集群 smoke [`reports/compatibility/p1-cluster-current-continuation113/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation113/cluster-report.json) 于 `2026-09-14T03:52:05.6853284Z` 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation113/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation113/release-candidate.json) 于 `2026-09-14T04:22:02.9370755Z` 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 114

- 继续补齐 P1-SYS-003：全局 `read_only=ON` 现在识别当前连接拥有的临时表，允许临时表 `INSERT/REPLACE/UPDATE/DELETE`，同时继续拒绝普通持久表 DML；识别同时覆盖逻辑临时表名和已改写的内部物理临时表名。
- 扩展 `TestGlobalReadOnlyRejectsClientWritesButAllowsAdminAndReplay`，旧实现先行确认临时表写入被错误拦截，修复后临时表写入/读取与持久表只读保护均通过。
- 本切片仍不扩大 FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界的后置优先级。

## Continuation 115

- 继续补齐 P1-TXN-003：显式 `READ ONLY` 事务现在允许当前连接对自己的临时表执行 DML，同时仍拒绝持久表写入；engine 在 DML 执行入口按逻辑/内部临时表名识别该例外，复制回放例外保持不变。
- 扩展 `TestReadOnlyTransactionRejectsSQLDML`，覆盖只读事务内创建临时表并 INSERT；持久表 INSERT/UPDATE/DELETE 仍继续被拒绝。
- 本切片仍保留更广泛事务变量生命周期、FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界。

- 验证完成：`go test ./server/innodb/engine -count=1 -timeout 20m` 通过（113.835s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation115/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation115/cluster-report.json) 于 `2026-09-14T04:31:47.7581685Z` 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation115/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation115/release-candidate.json) 于 `2026-09-14T04:54:51.1168849Z` 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。

## Continuation 116

- 继续补齐 P1-SYS-003：新增全局 `super_read_only` 系统变量；启用时自动联动 `read_only=ON`，即使 `SUPER`/`CONNECTION_ADMIN` 也不能写持久表，复制回放和当前连接自己的临时表仍允许；关闭 `super_read_only` 不会错误清除既有的 `read_only`，而 `read_only=OFF` 在其开启期间会被拒绝。
- engine 与 dispatcher 的 `SET GLOBAL` 路径、`SHOW GLOBAL VARIABLES` 投影和 DML 拦截均已覆盖；新增 `TestSuperReadOnlyBlocksPrivilegedClientsAndSynchronizesReadOnly` 与 `TestSystemVariableEngine_SuperReadOnlySynchronizesGlobalState`，先行回归确认未知变量/管理员绕过缺口后通过。
- 本切片仍不宣称完整的只读 DDL/锁等待语义；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续按用户确定的优先级后置。

## Continuation 117

- 继续收口 P1-SQL-003/P0-Connector.J 清理生命周期：`DROP DATABASE` 删除每个受管 tablespace 前现在复用 `DROP TABLE` 的有界 Windows 文件句柄重试（50 次、每次 20ms），避免大批量 JDBC 事务后最后一个 `.ibd` 短暂仍被系统占用而返回 `Access is denied`。
- 新增 `TestDropDatabaseReleasesActiveTableSpacesBeforeDirectoryRemoval`，覆盖建库、建表、DML、SELECT 后删除数据库及目录消失；定向 engine 回归通过。
- 验证完成：全仓 `go test ./... -count=1 -timeout 30m` 通过（engine 113.452s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation116/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation116/cluster-report.json) 为 `PASS`；最新发布候选 [`reports/compatibility/release-candidate-current-continuation117/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation117/release-candidate.json) 于 `2026-09-20T12:02:58.94336Z` 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- 本切片不宣称完整 online DDL/MDL 或上游物理备份语义；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续按用户确定的优先级后置。

## Continuation 118

- 按官方 MySQL 语义修正 P1-SYS-003：`SET GLOBAL read_only = OFF` 现在成功执行时会隐式同步 `super_read_only=OFF`，而不是在 `super_read_only=ON` 时返回错误；`super_read_only=ON` 仍会自动设置 `read_only=ON`。
- engine 与 dispatcher 两条设置路径均已修正，回归覆盖管理员解除只读后恢复持久表写入；定向 engine/dispatcher 测试通过。

## Continuation 119

- 继续收口 P1-TXN-003：`autocommit=0` 会在首个 DML 后 materialize 连接可见的活动事务状态；`SET autocommit=1` 在该隐式事务仍活动时执行提交、清理 journal/事务状态并保留已提交数据。
- 新增 `TestSetAutocommitOnCommitsActiveTransaction`，覆盖跨连接可见的提交结果和 `in_transaction` 生命周期；事务相关 engine 回归组通过。

## Continuation 120

- 最新验证证据：全仓 `go test ./... -count=1 -timeout 30m` 通过；集群 smoke [`reports/compatibility/p1-cluster-current-continuation119/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation119/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation119/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation119/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J `139` tests、`0` failures、`0` errors、`0` skipped，Maven `BUILD SUCCESS`。
- Connector/J 与集群 P0 主链路仍保持通过；FULLTEXT、非 Connector/J 全量客户端、完整 online DDL/MDL、原生 MySQL binlog/GTID/PITR 互操作和生产级跨节点 fencing 仍明确是后置或未完成边界。

## Continuation 121

- 继续收口 P1-SYS-003：全局 `read_only` 现在也阻止普通客户端对持久表执行 CREATE/ALTER/DROP/TRUNCATE 等结构变更；`super_read_only` 下连 `SUPER`/`CONNECTION_ADMIN` 也不能执行持久 DDL。
- 保留 MySQL 例外：临时表 DDL、`ANALYZE TABLE`，以及复制回放 DDL 不被该客户端保护拦截；新增回归覆盖普通客户端、管理员、临时表、分析语句和复制回放。

## Continuation 122

- 继续收口 P1-SYS-003：启用全局 `read_only`/`super_read_only` 时，当前连接存在活动事务或显式 `LOCK TABLES` 时会被拒绝；engine 与 dispatcher 路径均覆盖。
- 修复多表 `DROP TABLE` 的只读保护边界，避免混合临时表/持久表时仅检查首个目标而绕过持久写保护；当前实现对该复杂多目标语句保守拦截。
- 新增未提交事务、显式表锁和混合多表 DROP 回归；另新增 `START TRANSACTION` 结束上一活动事务的跨连接可见性回归。
- 验证完成：全仓 `go test ./... -count=1 -timeout 30m` 通过（engine 117.310s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation122/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation122/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation122/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation122/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:26）。
- 完整 MDL/锁等待与复杂多目标 DDL 精确矩阵仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 123

- 继续补齐 P1-TXN-003：`START TRANSACTION` 遇到已有活动事务时现在显式执行隐式提交，覆盖账户变更、复制语句、事务历史与 active-transaction 计数，再创建新事务；不再只清理旧事务状态。
- 新增跨连接可见性回归 `TestStartTransactionCommitsPreviousActiveTransaction`；engine 定向回归、全仓 Go 回归均通过（engine 110.208s，全仓 engine 116.604s）。
- 验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation123/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation123/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation123/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation123/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:12）。
- 更广泛的 isolation/transaction-variable 生命周期、完整 MDL/锁等待与复杂多目标 DDL 精确矩阵仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 124

- 继续收口 P1-SYS-003：当多表 `DROP TABLE` 的全部目标都是当前连接临时表时，`read_only` 下允许执行；混合临时表/持久表时继续阻断，避免首目标检查造成写保护绕过。
- 新增全临时多表 DROP 与混合 DROP 回归；engine 全量回归通过（110.656s）。
- 验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation124/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation124/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation124/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation124/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:18）。
- 完整 MDL/锁等待、复杂多目标 DDL 精确矩阵、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 125

- 继续补齐 P1-SQL-003 MDL 语义：锁定读 `FOR UPDATE`、`LOCK IN SHARE MODE` 与 `SKIP LOCKED` 不再被当成普通 SELECT，而是获取 DML 级表协调锁，使 DDL 正确等待；普通 SELECT 行为保持不变。
- 新增 `TestStatementTableWritesRecognizesLockingReads`，锁协调器定向回归与 engine 全量回归通过（111.160s）。
- 验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation125/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation125/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation125/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation125/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:10）。
- 完整 MDL/在线 DDL 的更多锁模式、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 126

- 继续补齐 P1-SQL-003：MySQL 8 的 `SELECT ... FOR SHARE` 现在与 `FOR UPDATE`、`LOCK IN SHARE MODE` 共用 DML 级表协调锁，DDL 会等待锁定读完成。
- 先行回归确认 `FOR SHARE` 缺口，修复后 engine 全量回归通过（110.602s）。
- 验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation126/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation126/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation126/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation126/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`（11:09）。
- 完整 MDL/在线 DDL 的更多锁模式、剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 127

- 继续收口 P1-SQL-003 事务级 MDL：显式事务或 `autocommit=0` 下的普通 SELECT、DML、锁定读获取的表元数据锁现在进入 session transaction lease，并在 `COMMIT`、`ROLLBACK`、`SET autocommit=1`、`START TRANSACTION` 边界或 `COM_RESET_CONNECTION` 时释放。
- 新增 `TestExplicitTransactionHoldsMetadataLockUntilCommit`，先行回归确认旧实现会让 ALTER TABLE 在事务提交前完成；修复后 DDL 正确等待 COMMIT。另修正 `LOCK TABLES` lease 的请求模式与物理协调器模式字段分离，消除 READ lease 释放时的 RWMutex 解锁 panic。
- engine 全量回归通过（`go test ./server/innodb/engine -count=1 -timeout 30m`，111.189s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation127/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation127/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation127/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation127/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。本轮没有重复执行全仓 `go test ./...`，不将其写作本轮证据。

## Continuation 128-129

- 将 CREATE/DROP/TRUNCATE 的解析 DDL 与 raw 多表 DROP 接入共享 DDL 写锁；engine 全量回归通过（111.119s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation128/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation128/cluster-report.json) 为 `PASS`。
- continuation128/129 的 release gate 分别为 `NO-GO`：Connector/J 各有 11 个错误，均为同一连接在 `autocommit=0` 普通 SELECT 后保留 read MDL，随后 TRUNCATE 申请 write MDL 时发生 owner 冲突；这两轮报告只作为失败证据，不作为发布通过依据。

## Continuation 130

- 修复 P1-SQL-003/P1-TXN-003 的隐式提交边界：`SET autocommit=1` 和同连接 CREATE/DROP/TRUNCATE DDL 现在会清理未 materialize `in_transaction` 的事务级 MDL lease；若已有活动事务，则先走隐式提交并释放 lease。
- 新增 `TestAutocommitOnReleasesReadMetadataLeaseWithoutMaterializedTransaction`，覆盖 `autocommit=0` 普通 SELECT → `SET autocommit=1` → 同连接 TRUNCATE；定向回归通过。
- 验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation130/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation130/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation130/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation130/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`；本轮 gate 的 integration/go-core engine 均通过。
- 更完整的 MDL 锁兼容矩阵、ALTER/RENAME/数据库级 DDL 的全部隐式提交边界、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 131-132

- continuation131 尝试将 ALTER/RENAME 纳入 DDL 隐式提交时，engine 全量回归发现 raw ALTER 识别条件误排除 `RENAME TABLE`，该轮未作为通过证据。
- continuation132 恢复 RENAME TABLE raw 处理并保留真正 ALTER/RENAME 的隐式提交；engine 全量回归通过（`go test ./server/innodb/engine -count=1 -timeout 30m`，110.496s）。
- 验证完成：集群 smoke [`reports/compatibility/p1-cluster-current-continuation132/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation132/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation132/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation132/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 数据库级 DDL 的完整隐式提交边界、更完整的 MDL 锁兼容矩阵、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 133-134

- continuation133 将 CREATE/DROP DATABASE 纳入 DDL 隐式提交；新增数据库级事务 lease 回归，engine 全量回归通过（110.469s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation133/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation133/cluster-report.json) 为 `PASS`，release candidate 为 `GO`。
- continuation134 将 `RENAME TABLE` 的所有源/目标表接入有序 DDL MDL 写锁，新增 `TestExplicitTransactionBlocksRenameUntilCommit`；engine 全量回归通过（110.897s），全仓 `go test ./... -count=1 -timeout 30m` 通过（engine 114.726s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation134/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation134/cluster-report.json) 为 `PASS`。
- 验证完成：发布候选 [`reports/compatibility/release-candidate-current-continuation134/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation134/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、数据库级/存储对象 DDL 的全部隐式提交边界、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 135-136

- continuation135 重新验证 `RENAME TABLE` 多表源/目标锁 key 去重后的实现：engine 全量回归通过（109.666s），cluster smoke 为 `PASS`，release candidate 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- continuation136 补齐 `DROP DATABASE` 的数据库级 MDL：删除数据库前按稳定顺序获取库内受管表的 DDL 写锁；跨连接事务内普通 SELECT 持有表级 lease 时 DROP DATABASE 会等待，COMMIT 后继续完成；新增 `TestExplicitTransactionBlocksDropDatabaseUntilCommit`，事务/DDL 锁专项回归和 engine 全量通过（110.632s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation136/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation136/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation136/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation136/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、未受管表/跨库对象的 DDL 协调、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 142-143

- continuation142 首次补账户管理 DDL 隐式提交时误把 `GRANT/REVOKE` 也提前提交，触发现有 `TestAccountGrantChangesCommitAndRollbackWithSessionTransaction` 的暂存/回滚契约；该轮 engine 为 `NO-GO`，不作为通过证据。
- continuation143 收窄账户边界：`CREATE/ALTER/DROP USER/ROLE`、`SET PASSWORD`、`RENAME USER` 在执行前隐式提交，项目既有会话暂存的 `GRANT/REVOKE` 保持 COMMIT/ROLLBACK 语义；新增 `TestAccountManagementDDLImplicitlyCommitsActiveTransaction`，engine 全量回归通过（110.515s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation143/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation143/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation143/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation143/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的账户语句与 MySQL 隐式提交矩阵、MDL 锁兼容矩阵、未受管表/跨库对象的 DDL 协调、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 141

- 补齐 admin DDL 的隐式提交：`FLUSH TABLES` 与 `ANALYZE/CHECK/OPTIMIZE/REPAIR TABLE` 执行前结束活动事务并释放事务级 MDL lease，新增 `TestFlushAndTableMaintenanceImplicitlyCommitActiveTransaction`；engine 全量回归通过（111.128s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation141/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation141/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation141/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation141/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、未受管表/跨库对象的 DDL 协调、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 140

- 补齐 `LOCK TABLES` 的隐式提交：获取显式表锁前提交活动 DML 并释放旧事务级 MDL lease，新增 `TestLockTablesImplicitlyCommitsActiveTransaction` 覆盖跨连接可见性和会话状态；engine 全量回归通过（110.796s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation140/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation140/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation140/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation140/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、未受管表/跨库对象的 DDL 协调、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 138-139

- continuation138/139 继续收口表空间 DDL：`CREATE/ALTER/DROP TABLESPACE` 与 `ALTER TABLE ... TABLESPACE`/`DISCARD|IMPORT TABLESPACE` 在已实现路径上统一执行隐式提交；表级迁移、discard/import 现在还获取 DDL 写锁，事务内读会阻塞跨连接表空间迁移。新增 `TestTablespaceDDLImplicitlyCommitsTransaction` 与 `TestExplicitTransactionBlocksAlterTableTablespaceUntilCommit`；engine 全量回归通过（109.803s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation139/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation139/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation139/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation139/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、未受管表/跨库对象的 DDL 协调、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 137

- 按 MySQL 8.4 隐式提交语义补齐已实现的存储对象/视图 DDL：`CREATE/DROP/ALTER PROCEDURE/FUNCTION/EVENT/TRIGGER` 与 `CREATE/ALTER/DROP VIEW` 在执行前结束当前事务级 MDL lease；临时表路径不受影响。新增 `TestStoredObjectAndViewDDLImplicitlyCommitTransaction`，engine 全量回归通过（110.075s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation137/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation137/cluster-report.json) 为 `PASS`，release candidate [`reports/compatibility/release-candidate-current-continuation137/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation137/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 更完整的 MDL 锁兼容矩阵、未受管表/跨库对象的 DDL 协调、online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 147

- P1-SQL-003 继续收口 View 依赖对象 MDL：`CREATE/ALTER VIEW` 在持有 View 自身 DDL 写锁之外，现在递归收集嵌套 View 的源对象，并对嵌套 View 与最终基表获取有序共享元数据锁；源对象被另一连接以 `LOCK TABLES ... WRITE` 持有时，View DDL 会等待释放。
- 新增 `TestViewDDLWaitsForExplicitViewMetadataLock`、`TestCreateViewWaitsForExplicitSourceMetadataLock` 与 `TestCreateViewWaitsForNestedViewSourceMetadataLock`；嵌套 View 测试先行复现旧实现会提前完成，修复后定向回归通过。engine 全量回归通过（112.530s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation147/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation147/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation147/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation147/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 更完整的跨库 View 依赖、循环/复杂 AST 依赖解析、MDL 锁兼容矩阵、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 153

- P1-SQL-003 补齐 View 元数据读取路径：`SHOW CREATE VIEW` 现在获取会话级共享 MDL；另一连接持有 `LOCK TABLES view WRITE` 时，查询会等待释放，避免专用 metadata handler 绕过 View 锁协议。
- 新增 `TestShowCreateViewWaitsForExplicitViewMetadataLock`；先行回归确认旧实现会在锁持有期间直接返回，修复后 View/嵌套 View 定向回归通过。engine 全量回归通过（111.505s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation148/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation148/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation148/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation148/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 更完整的 View 依赖/metadata handler 矩阵、跨库对象协调、MDL 锁兼容矩阵、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL 边界仍未完成；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 156

- P1-SQL-003 补齐表元数据读取路径：`SHOW CREATE TABLE` 现在对持久表获取会话级共享 MDL；另一连接持有 `LOCK TABLES table WRITE` 时，查询会等待释放，避免专用 SHOW handler 绕过表锁协议；临时表路径保持原有逻辑。
- 新增 `TestShowCreateTableWaitsForExplicitTableMetadataLock`；先行回归确认旧实现会在锁持有期间直接返回，修复后通过。engine 全量回归通过（111.360s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation154/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation154/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation154/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation154/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Global Scope Decision 2026-09-21

- 纳入全局任务：完整 `INFORMATION_SCHEMA`、完整 `PERFORMANCE_SCHEMA`、非 Connector/J 客户端兼容矩阵、XA 与原生 binlog/复制/崩溃恢复互操作。
- 保持 P0 基线：MySQL 可启动、InnoDB 核心 CRUD、集群复制/故障切换、Connector/J 当前 139 项门禁。
- 本轮暂缓：FULLTEXT 及全文检索生态。
- 明确不处理：MyISAM、ARCHIVE、CSV 等非 InnoDB 引擎、非 InnoDB `REPAIR TABLE`、非 InnoDB 引擎转换；这些不再作为全局任务的未完成项。
- 对应执行计划：[`docs/superpowers/plans/2026-09-21-global-compatibility-completion-plan.md`](2026-09-21-global-compatibility-completion-plan.md)。

## Continuation 1034

- P1/分区维护继续推进：实现 `ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE`，支持普通表与分区表分区之间的列元数据校验、分区归属校验、行交换和 `WITHOUT VALIDATION` 选项；交换失败时保持原元数据不变。
- 新增 `TestExchangePartitionSwapsRowsWithOrdinaryTable`；定向测试与 engine 全量回归通过（119.025s），全仓 Go 回归通过（engine 124.283s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1034/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1034/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1034/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1034/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。

## Continuation 1035

- P1/分区 ALTER 维护继续推进：实现 `ALTER TABLE ... REMOVE PARTITIONING`，把已有分区行搬回普通表存储，清除持久化分区描述并验证转换后的普通表继续支持读写；同时修复 `.frm` 索引元数据回填 `TableMeta.PrimaryKey` 的缺口。
- 新增 `TestRemovePartitioningMigratesRowsToOrdinaryTable`；定向测试、engine 全量回归通过（117.361s），全仓 Go 回归通过（engine 122.220s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1035/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1035/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1035/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1035/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:31。
- 仍未完成的边界：无主键表的重复行保持型分区迁移、更复杂分区 ALTER/REORGANIZE 和类型/校对序语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1036

- P1/分区 ALTER 迁移继续收口：`REMOVE PARTITIONING` 的行迁移不再要求主键；无主键表按完整行条件逐条 `DELETE ... LIMIT 1`，可保留完全重复的多行记录。
- 新增 `TestRemovePartitioningPreservesDuplicateRowsWithoutPrimaryKey`；engine 全量回归通过（119.350s），全仓 Go 回归通过（engine 124.584s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1036/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1036/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1036/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1036/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:34。
- 仍未完成的边界：更复杂分区 ALTER/REORGANIZE 和类型/校对序语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1011

- 补齐 `INFORMATION_SCHEMA.COLUMN_STATISTICS`、`TABLESPACES`、`FILES` 的客户端元数据路由和列形状；当前按明确边界返回合法空集，不伪造尚未持久化的直方图或通用 tablespace-file 数据。
- 递归 CTE 支持后续普通定义消费前序递归定义的物化结果；`WITH RECURSIVE` 校验按实际自引用区分 recursive/non-recursive definition。新增 `TestRecursiveCTECompatibilitySupportsDependentNonRecursiveDefinition`。
- 分区裁剪支持谓词 `AND` 求交、`OR` 求并，并同步物理分区 fan-out 与候选行裁剪；新增 `TestPartitionedBTreeManagerUnionsTopLevelOrRanges`。
- fresh engine 113.768s、全仓 Go 124.196s；集群 [`reports/compatibility/p1-cluster-current-continuation1011/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1011/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1011/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1011/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0/0。

## Continuation 1012

- 分区访问继续支持数值常量 `IN (...)` 的 RANGE/LIST 剪枝；多个点位求并，与既有 `AND` 求交和 `OR` 求并组合。无法证明的函数、动态值或复杂类型表达式继续保守全量扫描。
- 新增 `TestPartitionedBTreeManagerPrunesConstantInValues`；最新 engine 全量 113.768s；集群 [`reports/compatibility/p1-cluster-current-continuation1012/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1012/cluster-report.json) PASS；发布候选 [`reports/compatibility/release-candidate-current-continuation1012/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1012/release-candidate.json) GO，9/9 checks PASS，Connector/J 139/0/0/0。
- 当前仍保留的明确边界：动态插件装载/卸载、全量 I_S/P_S、递归 CTE 任意复杂 AST/相关递归、完整函数/类型分区剪枝、物理分区路由、真正 online DDL/多表 DDL/完整 MDL、XA 原生日志互操作和其他 P1/P3/P4 高阶语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1001

- 继续补齐 P1 XA 兼容：`XA RECOVER CONVERT XID` 现在支持官方可选语法；`XA_RECOVER_ADMIN` 权限校验保持不变，返回的 `data` 按 XID 字节拼接后转为十六进制文本，`formatID/gtrid_length/bqual_length` 保持原值。
- 新增 `TestXACompatibilitySupportsRecoverConvertXID` 与解析回归；旧实现先返回原始 `abcdef`，修复后返回 `616263646566`，全部 XA 专项通过。
- fresh 验证：engine `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（118.928s）；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 123.084s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation1001/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1001/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation1001/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1001/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括 XA 原生二进制日志/更细权限矩阵、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 SQL AST、物理分区路由、真正多引擎 `REPAIR TABLE` 和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1003

- 继续补齐 P1-SQL-001 递归 CTE 主查询：同一个递归 CTE 现在可以在最终查询中用不同别名被引用两次，支持自连接、ON 条件、WHERE 过滤、ORDER BY 和 LIMIT；物化结果按每个别名挂载到派生 JOIN 执行器，避免重复执行递归成员。
- 新增 `TestRecursiveCTECompatibilitySupportsMainQuerySelfJoin`；旧实现先返回 `recursive CTE main query has unsupported FROM expression`，修复后返回自连接结果 `3/3`、`2/2`。
- fresh 验证：engine `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.353s）；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 130.123s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation1003/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1003/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation1003/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1003/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1005

- 继续补齐 P1-SQL-003/P0-Connector.J 客户端元数据：`SHOW PLUGINS` 现在返回 MySQL 六列结果形状（`Name/Status/Type/Library/License/Load_option`），暴露当前 InnoDB 与认证插件，并支持 `LIKE` 过滤；此前 parser 虽可识别语句，但执行器返回 `unsupported SHOW type: plugins`。
- 新增 `TestXMySQLExecutor_ShowPluginsReturnsBuiltinsAndSupportsLike`；覆盖全量结果列、内置插件存在性和 `SHOW PLUGINS LIKE 'InnoDB'` 单行过滤。
- fresh 验证：engine `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（114.747s）；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 120.322s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation1005/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1005/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation1005/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1005/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更完整的插件动态装载/卸载生命周期、递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1006

- P1/客户端元数据：补齐 `SHOW OPEN TABLES`（同时覆盖 parser 暴露的 `SHOW OPEN SCHEMAS` 别名）执行路由；结果按 MySQL 四列 `Database/Table/In_use/Name_locked` 返回，支持 `FROM/IN` 数据库过滤和 `LIKE` 表名过滤。
- 该实现枚举当前会话可见的持久化 `.frm` 表和视图元数据；XMySQL 没有 MySQL 内部 table-cache 计数器，因此 `In_use`、`Name_locked` 明确返回 0，不伪造锁状态。
- 新增 `TestXMySQLExecutor_ShowOpenTablesReturnsVisibleTablesAndSupportsLike`；旧实现先复现 `unsupported SHOW type: open`，修复后通过。engine 全量 115.003s、全仓 Go 全量 119.257s；集群 [`reports/compatibility/p1-cluster-current-continuation1006/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1006/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1006/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1006/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0。
- 仍未完成的边界包括 `SHOW OPEN TABLES` 的真实 table-cache 使用计数、递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1007

- P1/客户端元数据：补齐 `SHOW STORAGE ENGINES` 执行路由，复用 `SHOW ENGINES` 的六列结果形状与 InnoDB 能力声明；此前 parser 虽已将该语句识别为 `storage`，执行器仍会落到 unsupported。
- 新增 `TestXMySQLExecutor_ShowStorageEnginesUsesShowEnginesShape`；目标测试通过。fresh engine 113.306s、全仓 Go 120.062s；集群 [`reports/compatibility/p1-cluster-current-continuation1007/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1007/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1007/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1007/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0/0。
- 仍未完成的边界包括真实动态插件装载/卸载生命周期、SHOW OPEN TABLES 的 table-cache 使用计数、递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1008

- P1-SQL-001：多个独立递归 CTE 的最终查询现在可与普通持久表混合组成嵌套 JOIN；递归 CTE 物化结果通过统一的派生 JOIN 执行器注入，普通表继续沿用 storage-integrated 路径。
- 新增 `TestRecursiveCTECompatibilitySupportsIndependentDefinitionsJoinedWithBaseTable`；旧实现先复现 `multiple recursive CTE join source is unsupported`，修复后通过。递归 CTE 专项回归通过，engine 全量 114.466s、全仓 Go 119.903s；集群 [`reports/compatibility/p1-cluster-current-continuation1008/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1008/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1008/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1008/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成的边界包括递归 CTE 更复杂的多层/任意 AST、相关子查询通用谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由、动态插件生命周期和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1009

- P1-SYS/I_S：新增 `INFORMATION_SCHEMA.PLUGINS` 元数据路由，返回 MySQL 14 列结果形状，支持按投影列读取，并支持 `PLUGIN_NAME`、`PLUGIN_STATUS`、`PLUGIN_TYPE`、`LOAD_OPTION` 的常见等值/LIKE 过滤；内置 InnoDB 与认证插件与 `SHOW PLUGINS` 保持一致。
- 新增 `TestXMySQLExecutor_InformationSchemaPluginsSupportsProjectionAndFilter`；旧路径先复现未识别元数据表后落入普通 SELECT 的空指针，修复后通过。engine 全量 114.283s、全仓 Go 123.039s；集群 [`reports/compatibility/p1-cluster-current-continuation1009/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1009/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1009/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1009/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成的边界包括动态插件装载/卸载生命周期、完整 I_S/P_S 表覆盖、递归 CTE 更复杂的多层/任意 AST、相关子查询通用谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1010

- P1-OPT/分区访问：分区路由现在对多个同一分区表达式的常量范围谓词求交，例如 `id >= 100 AND id < 200` 只保留可能命中的 RANGE 分区；无法证明的谓词仍保留完整分区集合，保证正确性。storage-integrated DML 的候选行分区裁剪同步采用同一求交逻辑。
- 新增 `TestPartitionedBTreeManagerIntersectsMultipleConstantRanges`；旧实现先因只接受单个条件而保留全部三分区，修复后只保留 `p1`。专项分区回归通过；engine 全量 118.512s、全仓 Go 122.137s；集群 [`reports/compatibility/p1-cluster-current-continuation1010/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1010/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation1010/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1010/release-candidate.json) 为 `GO`，9/9 checks PASS，Connector/J 139/0/0。
- 仍未完成的边界包括更复杂的表达式/OR/函数分区剪枝、复杂 index-range partitioning 的完整类型矩阵、动态插件生命周期、完整 I_S/P_S、递归 CTE 任意 AST、相关子查询通用谓词推导、完整 online DDL/MDL、XA 原生二进制日志/更细权限矩阵和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1004

- 继续补齐 P1-SQL-001 递归 CTE 多源主查询：多个独立递归 CTE 现在支持三路及更深的嵌套 INNER JOIN 物化，统一复用派生 JOIN 执行器；原有两路 CTE JOIN、聚合、排序和别名语义保持兼容。
- 新增 `TestRecursiveCTECompatibilitySupportsThreeIndependentDefinitionsInJoinChain`；旧实现先返回 `multiple recursive CTE join source is unsupported`，修复后返回 `1/10/20`、`2/10/20`。
- fresh 验证：engine `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（117.434s）；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 131.548s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation1004/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1004/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation1004/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1004/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括递归 CTE 更复杂的多层/任意 AST 组合、通用相关子查询谓词推导、复杂 index-range partitioning、完整 online DDL/MDL、全量 I_S/P_S、XA 原生二进制日志/更细权限矩阵、物理分区路由和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1002

- 继续补齐 P1 XA 语法兼容：`XA BEGIN xid` 现在归一化到既有 `XA START` 状态机，支持 BEGIN、END、COMMIT ONE PHASE 的完整事务路径；新增 `TestParseXAStatementSupportsBasicXIDForms` 与 `TestXACompatibilitySupportsBeginAlias`，旧实现先将 BEGIN 判为 unsupported，修复后提交并校验数据成功。
- fresh 验证：XA 专项通过；engine `go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.848s）；全仓 `go test ./... -count=1 -timeout 30m` 通过，engine 126.303s；集群 smoke [`reports/compatibility/p1-cluster-current-continuation1002/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1002/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation1002/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1002/release-candidate.json) 为 `GO`：clean-data/build/unit/integration/go-core/crash-recovery/concurrency/observability/jdbc 共 9/9 checks `PASS`，Connector/J `139` tests、0 failures、0 errors、0 skipped，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括 XA 原生二进制日志/更细权限矩阵、完整 online DDL/MDL、全量 I_S/P_S、通用复杂 SQL AST、物理分区路由、真正多引擎 `REPAIR TABLE` 和其他 P1/P3/P4 高级语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 185

- P1-OPT-004 继续补齐常量类型转换边界：索引条件提取现在折叠无行列依赖的确定性 `CAST`/`CONVERT` 常量表达式，支持把 `col1 >= CAST('42' AS SIGNED)` 转为可下推的 `col1 >= 42`；不确定函数与含行列表达式保持保守路径。
- 新增 `TestConstantCastExpressionUsesIndexPushdown`；计划包、全仓 `go test ./... -count=1 -timeout 30m` 和 engine 回归均通过（engine 118.730s）。
- 本轮真实门禁：[`p1-cluster-current-continuation185/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation185/cluster-report.json) 为 `PASS`；[`release-candidate-current-continuation185/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation185/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、异构 row-source 完整列裁剪、通用子查询谓词推导、更复杂 View/触发器 MDL、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 186

- P1-OPT-004 继续补齐结构化范围条件：`BETWEEN` 上下界现在复用确定性常量表达式折叠，算术与 `CAST/CONVERT` 常量边界可生成精确的范围索引条件。
- 新增 `TestStructuredRangeWithConstantExpressionsUsesIndexes`；计划包和 engine 全量回归通过（116.471s）。
- 本轮真实门禁：[`p1-cluster-current-continuation186/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation186/cluster-report.json) 为 `PASS`；[`release-candidate-current-continuation186/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation186/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 190

- P1-OPT-005/P1-OPT-004 继续补齐确定性函数常量：`MD5/SHA/SHA2/CRC32/INET_*`、UUID 编解码、Base64/HEX 等纯输入函数现在可参与索引常量折叠；`UUID()`、`RAND()`、时间/会话函数保持排除。
- 新增 `TestConstantDigestFunctionExpressionUsesIndexPushdown`；计划包和 engine 全量回归通过（117.292s）。
- 本轮真实门禁：[`p1-cluster-current-continuation190/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation190/cluster-report.json) 为 `PASS`；[`release-candidate-current-continuation190/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation190/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、复杂 index-range 分区、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL、触发器跨表副作用/递归依赖的完整 MDL 矩阵；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 189

- P1-OPT-004 继续补齐确定性日期函数常量：`YEAR/MONTH/DAY/.../DATE_FORMAT/TIME_FORMAT/STR_TO_DATE` 等无会话状态函数现在可参与常量索引边界折叠；时间、随机、会话函数仍排除。
- 新增 `TestConstantDateFunctionExpressionUsesIndexPushdown`；计划包和 engine 全量回归通过（117.825s）。
- 本轮真实门禁：[`p1-cluster-current-continuation189/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation189/cluster-report.json) 为 `PASS`；[`release-candidate-current-continuation189/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation189/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 188

- P1-OPT-004 继续补齐常量边界表达式：无行列依赖的确定性比较/逻辑表达式与 `CASE` 分支现在可折叠为索引边界；求值失败或不确定表达式仍拒绝下推。
- 新增 `TestConstantCaseExpressionUsesIndexPushdown`；计划包和 engine 全量回归通过（115.428s）。
- 本轮真实门禁：[`p1-cluster-current-continuation188/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation188/cluster-report.json) 为 `PASS`；[`release-candidate-current-continuation188/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation188/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 187

- P1-OPT-004 继续补齐索引列表常量：`IN` 列表内的确定性算术/`CAST` 表达式现在可折叠为常量列表并走索引下推；含列或不确定函数的列表仍回退。
- 新增 `TestConstantExpressionTupleUsesInIndexPushdown`；计划包和 engine 全量回归通过（115.915s）。
- 本轮真实门禁：[`p1-cluster-current-continuation187/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation187/cluster-report.json) 为 `PASS`；[`release-candidate-current-continuation187/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation187/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range 分区、通用子查询谓词推导、异构 row-source 完整列裁剪、复杂 SQL AST、真正 online DDL/多表 DDL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 179

- P1-OPT-001 继续补齐外连接表达式推导：LEFT/RIGHT 等值连接现在可识别两侧同形、单侧、确定性的算术连接键（例如 `a.id + 1 = b.id + 1`），将保留侧对该完整表达式的比较/结构化谓词复制到 nullable 侧；含无关列、非确定性/聚合函数和不受支持表达式仍保守跳过。
- 新增 `TestPredicatePushdownInfersOuterJoinWrappedJoinKeyExpression`，先行验证旧实现无法从表达式连接键推导过滤，修复后计划包与已有外连接结构化谓词回归通过；原始保留侧条件保持不变。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.843s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation179/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation179/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation179/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation179/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、更广泛 outer-join 表达式/函数推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、触发器跨表副作用/递归依赖的更细 MDL 矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 180

- P1-OPT-001 继续扩大外连接等值键推导：在算术表达式基础上，现支持白名单内的确定性标量函数包裹连接键（如 `ABS(a.id) = ABS(b.id)`），并可复制保留侧的比较谓词；`RAND` 等非确定性函数、聚合函数和含跨侧/无关列表达式仍明确拒绝。
- 新增 `TestPredicatePushdownInfersOuterJoinFunctionWrappedJoinKeyExpression` 与 `TestPredicatePushdownRejectsNondeterministicOuterJoinFunctionKey`，先行失败后转绿，覆盖正向推导和安全拒绝边界。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（117.394s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation180/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation180/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation180/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation180/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、更广泛函数/类型和 NULL 敏感表达式推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、触发器跨表副作用/递归依赖的更细 MDL 矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 181

- P1-OPT-004 补齐索引边界常量表达式：索引条件提取现在可折叠不引用列、无副作用的算术常量（例如 `col1 >= 1 + 2`），再按 `col1 >= 3` 生成索引候选；含列引用、非算术函数或求值错误的表达式仍不折叠。
- 新增 `TestConstantArithmeticExpressionUsesIndexPushdown`，覆盖先失败后通过的候选生成与折叠值断言。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.903s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation181/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation181/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation181/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation181/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range partitioning、通用子查询谓词推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 183

- P1-OPT-001/P1-SQL-001 继续补齐关联 Apply 推导：INNER/SEMI/ANTI Apply 的等值关联键现在可使用两侧同形、确定性的算术/标量函数表达式（例如 `ABS(left.id) = ABS(right.id)`），把一侧的安全比较谓词复制到另一侧；LEFT Apply 仍保持 nullable-side 禁止推导。
- 新增 `TestPredicatePushdownInfersApplyFunctionWrappedCorrelation`，先行验证旧实现无法推导函数包裹关联键，修复后与直接列、NULL-safe、嵌套选择条件回归一起通过。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.987s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation183/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation183/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation183/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation183/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括通用子查询谓词推导、复杂/多层相关 AST、完整 Apply 语义矩阵、复杂 index-range partitioning、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 184

- P1-OPT-002 补齐派生投影的输出列裁剪：当上层 row-source 只请求投影中的部分输出时，当前投影现在会删除未使用的直接/计算表达式，并只向子计划保留实际依赖列；`DISTINCT`、`ORDER BY`、`LIMIT`、`OFFSET` 语义屏障下保持完整投影。
- 新增 `TestColumnPruningDropsUnusedComputedProjectionOutputs` 与 `TestColumnPruningRetainsDistinctProjectionOutputs`，覆盖计算列血缘和 DISTINCT 保留边界。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.227s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation184/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation184/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation184/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation184/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛异构 row-source/复杂输出血缘、复杂 index-range partitioning、通用子查询谓词推导、统计采样全覆盖、复杂 SQL AST、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 182

- P1-OPT-004 继续扩展索引边界常量折叠：索引条件提取现在也支持白名单内确定性标量函数对常量的计算（例如 `col1 >= ABS(-3)`），并复用相同的无列/无副作用/求值成功约束。
- 新增 `TestConstantScalarFunctionExpressionUsesIndexPushdown`；算术常量、函数常量和既有字面量索引条件回归均通过。
- 本轮验证：`go test ./server/innodb/plan -count=1` 通过；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.471s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation182/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation182/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation182/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation182/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更广泛动态/类型转换常量、复杂 index-range partitioning、通用子查询谓词推导、并行/异构 row-source 的完整列裁剪、统计采样全覆盖、复杂 SQL AST、真正 online DDL/多表 DDL 精确语义；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 177

- P1-SQL-004/P1-SEC-001 补齐外键 DDL 的 `REFERENCES` 权限：`CREATE TABLE` 与 `ALTER TABLE ... ADD FOREIGN KEY` 现在均检查被引用表权限；原始 ALTER 兼容路径也纳入检查，自引用 CREATE 保留数据库级 CREATE 语义。
- 新增 `TestForeignKeyDDLRequiresReferencesPrivilegeOnParent`，覆盖无授权拒绝、授权后 CREATE 成功、撤权后 ALTER 拒绝和重新授权后 ALTER 成功。
- 本轮验证：`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（117.079s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation177/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation177/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation177/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation177/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 176

- P1-SQL-005 补齐自引用外键边界：CREATE TABLE 现在允许引用自身已定义的主键/唯一键，并校验自引用列存在、类型兼容和父侧索引覆盖；随后 `TRUNCATE` 仍按 MySQL 语义返回 `ER_TRUNCATE_ILLEGAL_FK`（1701/42000）。
- 新增 `TestTruncateSelfReferencingTableIsRejected`；自引用外键 CREATE/TRUNCATE、普通父表引用和错误码专项回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.496s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation176/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation176/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation176/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation176/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 175

- P1-SQL-005 完善 `TRUNCATE TABLE` 外键错误契约：被外键引用的父表在 `FOREIGN_KEY_CHECKS=1` 时返回 MySQL `ER_TRUNCATE_ILLEGAL_FK`（1701/42000），而不是普通文本错误；关闭该会话开关时仍允许执行。
- `TestTruncateReferencedParentTableIsRejected` 现在同时校验原数据保留与 errno 1701；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.529s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation175/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation175/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation175/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation175/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 174

- P1-SQL-005 补齐 `TRUNCATE TABLE` 的外键依赖边界：`FOREIGN_KEY_CHECKS=1` 时，存在子表外键引用的父表不能被 TRUNCATE，且原表数据保持不变；关闭该会话开关时保留导入/重建场景的放行语义。
- 新增 `TestTruncateReferencedParentTableIsRejected`；外键 TRUNCATE 专项与 `TRUNCATE` 原有元数据重建回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.689s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation174/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation174/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation174/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation174/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 173

- P1-SQL-003 修正 AFTER UPDATE 触发器的 `OLD`/`NEW` 行语义：副作用语句现在分别使用更新前与更新后的行值，避免 `OLD.col` 被错误替换为新值；新增 `TestAfterUpdateTriggerReceivesDistinctOldAndNewRows`，并通过触发器递归/失败补偿专项回归。
- 本轮验证：`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.768s）；集群 smoke [`reports/compatibility/p1-cluster-current-continuation173/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation173/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation173/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation173/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 172

- P1-SQL-003 补齐触发器失败语义：AFTER INSERT/UPDATE/DELETE 现在建立触发器级原子日志边界；触发器失败会补偿基础行写入和触发器副作用，递归触发器链会在进入重复 frame 前返回明确错误并回滚本次 DML。
- 新增 `TestAfterTriggerRejectsRecursiveTriggerCycle` 与 `TestAfterUpdateAndDeleteTriggerFailuresRestoreBaseRows`，并强化 `TestAfterTriggerRunsAtPostWriteBoundaryAndRejectsNewMutation` 的副作用回滚断言；触发器递归、UPDATE/DELETE 补偿和 INSERT 副作用回滚专项回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（114.996s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation172/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation172/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation172/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation172/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips，Maven `BUILD SUCCESS`。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、更细触发器跨表锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT、非 Connector/J 全量客户端及更高阶存储/集群边界继续后置。

## Continuation 163

- P1-SQL-003 补齐已实现存储对象的元数据锁协调：`SHOW CREATE PROCEDURE/FUNCTION/TRIGGER/EVENT` 现在获取共享元数据锁，`CREATE/DROP/ALTER` 对应对象获取 DDL 写锁；另一连接持有同名显式锁时，读取和对象 DDL 都会等待释放，避免存储对象专用路径绕过公共锁协议。
- 新增 `TestShowCreateStoredObjectWaitsForExplicitMetadataLock` 与 `TestStoredObjectDDLWaitsForExplicitMetadataLock`；先行回归确认旧实现会在锁持有期间直接完成，修复后定向回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（113.035s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation163/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation163/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation163/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation163/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象引用依赖的完整 MDL 矩阵、`REPAIR TABLE`/其他专用 metadata handler、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 164

- P1-SQL-003 继续补齐专用 metadata handler 的 MDL：`SHOW TABLE STATUS`、`SHOW COLUMNS/FIELDS`、`SHOW FULL COLUMNS/FIELDS` 获取共享表元数据锁；`SHOW PROCEDURE STATUS`、`SHOW EVENT(S)`、`SHOW TRIGGERS` 对已实现存储对象获取共享对象锁，并按稳定顺序避免多对象读取死锁。
- 新增 `TestShowTableStatusWaitsForExplicitTableMetadataLock`、`TestShowColumnsWaitsForExplicitTableMetadataLock`、`TestShowFullColumnsWaitsForExplicitTableMetadataLock` 与 `TestShowRoutineStatusWaitsForExplicitMetadataLock`；同时修复 `SHOW COLUMNS` 对真实持久化 `.frm` 元数据的读取优先级。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（113.590s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation164/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation164/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation164/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation164/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括 `REPAIR TABLE` 的 InnoDB 语义、存储对象引用依赖的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 165

- 明确 `REPAIR TABLE` 的 InnoDB 边界：已实现的 InnoDB 路径现在返回可识别的“不支持 repair”错误，而不是落到通用 `unsupported statement type`；`CHECK/ANALYZE/OPTIMIZE TABLE` 既有执行路径保持不变。该行为符合 MySQL 对 InnoDB 不提供 REPAIR 的边界。
- 新增 `TestRepairTableReportsInnoDBUnsupported`；`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（113.197s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation165/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation165/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation165/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation165/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括 MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、存储对象引用依赖的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 166

- P1-SQL-003 继续补齐存储对象执行路径的 MDL：顶层 `CALL procedure` 与存储函数 `SELECT function(...)` 在读取持久化定义前获取共享对象锁；另一连接持有同名对象显式锁时，例程执行等待释放，避免执行路径绕过对象元数据协议。
- 新增 `TestCallStoredObjectWaitsForExplicitMetadataLock` 与 `TestStoredFunctionCallWaitsForExplicitMetadataLock`；定向回归通过，`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（118.962s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation166/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation166/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation166/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation166/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象内部嵌套调用/触发器依赖的完整锁矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 167

- P1-SQL-003 补齐 `INFORMATION_SCHEMA.ROUTINES/TRIGGERS/EVENTS` 的对象级 MDL：这些查询现在在扫描持久化对象定义前按稳定顺序获取共享对象锁，锁等待错误向上层返回；不再绕过 `SHOW ... STATUS` 与 `SHOW CREATE` 已使用的对象锁协议。
- 新增 `TestInformationSchemaRoutinesWaitsForExplicitMetadataLock`；定向回归通过，`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（110.508s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation167/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation167/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation167/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation167/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象触发器执行期/内部嵌套调用的完整锁矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 1013

- 分区兼容性继续收口：LIST 分区现在在 planner 规则中保留带引号的文本值，支持安全的字符串常量 `=`/`IN` 剪枝；物理路由对带引号值采用同一规范化逻辑。数值 RANGE/LIST 行为和不支持/动态谓词仍保守保留全分区扫描。
- 新增 `TestStringListPartitionRoutingAndPruning`、`TestPartitionedBTreeManagerPrunesStringListValues` 以及 plan 层字符串 LIST 用例。plan/engine 定向测试通过；engine 全包 114.924s；全仓 `go test ./... -count=1 -timeout 30m` 通过（engine 119.430s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1013/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1013/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1013/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1013/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`。其中 JDBC 性能阶段耗时 23:43，记录为性能风险观察，不是功能失败。
- 仍未完成的边界：复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1014

- 分区剪枝继续补齐：RANGE 分区支持数值常量 `BETWEEN lower AND upper`，通过上下界分区集合求交实现安全剪枝；不支持的表达式继续保留全分区扫描。
- 新增 `TestPartitionedBTreeManagerIntersectsConstantBetweenRange`，并在真实分区表查询覆盖 `WHERE id BETWEEN 10 AND 20`。plan/engine 定向测试通过；engine 全包 115.429s；全仓 Go 通过，engine 120.680s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1014/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1014/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1014/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1014/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：复杂分区表达式/类型转换、`NOT BETWEEN` 等更广区间推导、物理分区路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1015

- P1/客户端元数据继续收口：`SHOW OPEN TABLES` 的 `In_use`/`Name_locked` 不再固定返回 `0,0`；执行器现在从当前 MDL 协调器快照投影已授予锁、等待状态和写模式，覆盖同一连接持有 `LOCK TABLES ... WRITE` 时的实时兼容视图。该实现明确是锁支持的兼容视图，不宣称复制 MySQL 私有 table-cache 内部计数。
- 新增 `TestXMySQLExecutor_ShowOpenTablesProjectsLiveLockUsage`；验证持锁时返回 `app/users/1/1`，解锁后恢复 `0/0`。定向 SHOW 测试通过；engine 全包 116.529s；全仓 Go `go test ./... -count=1 -timeout 30m` 通过（engine 121.535s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1015/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1015/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1015/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1015/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:15。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换与物理路由覆盖、`NOT BETWEEN` 等更广区间推导、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1016

- P1/分区优化：数值 RANGE 分区支持安全的常量 `NOT BETWEEN lower AND upper` 剪枝，将结果转换为 `< lower OR > upper` 的分区集合并集；无法证明的表达式、类型转换和其他分区方法继续保守保留全分区扫描。
- 新增 `TestPartitionedBTreeManagerUnionsConstantNotBetweenRanges`，并在真实 RANGE 分区查询中覆盖 `WHERE id NOT BETWEEN 10 AND 20`；专项回归通过。engine 全包 117.502s；全仓 Go 通过（engine 119.316s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1016/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1016/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1016/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1016/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:06。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1017

- P1/分区优化继续收口：LIST 分区支持字符串常量 `NOT IN` 的安全剪枝；仅当某分区的所有已枚举值都在排除集合中时移除该分区，未枚举/default、动态表达式、RANGE 及其他无法证明的情况保持保守扫描。
- 新增 `TestPartitionedBTreeManagerPrunesConstantNotInListValues`，并在真实字符串 LIST 分区查询覆盖 `WHERE region NOT IN ('east','west')`；分区兼容测试组通过。engine 全包 116.880s；全仓 Go 通过（engine 120.102s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1017/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1017/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1017/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1017/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:17。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1018

- P0/运行时可观测性：把已有 metrics recorder 接入三条真实运行路径。owner-aware MDL 等待记录 lock-wait counter/histogram；崩溃恢复成功/失败记录 recovery counters；检查点记录更新 dirty-pages gauge 和 checkpoint counter。
- 新增 `TestTableDDLCoordinatorRecordsRuntimeLockWaitMetric` 与 `TestCrashRecoveryAndCheckpointPathsRecordRuntimeMetrics`；相关 engine/manager/metrics/net 包通过，engine 116.850s、manager 10.903s、全仓 Go 通过（engine 120.320s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1018/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1018/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1018/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1018/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：完整 table-cache 私有计数、复杂分区表达式/类型转换和物理路由覆盖、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1019

- P1/分区物理路由：补齐单列字符串 `RANGE COLUMNS` 的持久化行路由；`VALUES LESS THAN ('m')` 等边界现在可正确把字符串行写入对应物理分区并读回。数值 `RANGE COLUMNS` 既有行为保持不变。
- 新增 `TestRangeColumnsStringPartitionRoutingAndReadback`，并保留 `TestRangeColumnsPartitionRoutingAndReadback` 覆盖数值路径；定向分区回归通过，engine 全包 115.440s；全仓 Go 通过（engine 120.170s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1019/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1019/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1019/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1019/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:12。
- 仍未完成的边界：多列 `RANGE/LIST COLUMNS`、完整 MySQL 字符集/校对序排序和字符串分区剪枝、复杂分区表达式/类型转换及更广物理路由、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1021

- P1/分区剪枝继续收口：单列字符串 `RANGE COLUMNS` 支持常量 `BETWEEN` 的上下界求交和 `NOT BETWEEN` 的 `< lower OR > upper` 集合并；文本边界解析只接受明确字符串字面量，未知/混合类型继续保守扫描。
- 新增 `TestPartitionedBTreeManagerIntersectsStringBetweenRange` 与 `TestPartitionedBTreeManagerUnionsStringNotBetweenRange`；engine 全包 115.095s；全仓 Go 通过，engine 119.592s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1021/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1021/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1021/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1021/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。

## Continuation 1022

- P1/分区物理路由继续推进：`RANGE COLUMNS` 支持单列之外的多列元组边界，按 MySQL 风格字典序比较复合数值/字符串键；建表、插入和持久化分区读回已覆盖。多列查询剪枝仍保守走全分区扫描，不宣称已完成。
- 新增 `TestRangeColumnsMultiColumnPartitionRoutingAndReadback`，覆盖 `(tenant_id, region)` 对 `(10, 'm')` 边界的路由；engine 全包 115.778s；全仓 Go 通过，engine 119.850s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1022/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1022/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1022/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1022/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：多列 `LIST COLUMNS`、多列 RANGE 查询剪枝、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1023

- P1/分区物理路由继续推进：`LIST COLUMNS` 支持多列元组枚举值，例如 `VALUES IN ((1, 'east'), (2, 'west'))`；插入按复合键精确匹配到独立物理分区并读回。未知元组、混合类型和多列 LIST 查询剪枝仍保守处理。
- 新增 `TestListColumnsMultiColumnPartitionRoutingAndReadback`；engine 全包 116.143s；全仓 Go 通过，engine 121.176s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1023/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1023/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1023/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1023/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：多列 LIST/RANGE 查询剪枝、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1024

- P1/分区查询剪枝继续推进：多列 `RANGE COLUMNS` 支持 `(col1, col2) =/>=/<... (constant1, constant2)` 的字典序边界裁剪，多列 `LIST COLUMNS` 支持 tuple `IN` 精确裁剪；无法解析的元组、混合类型和复杂表达式继续保守保留全分区。
- 新增 `TestPartitionedBTreeManagerPrunesMultiColumnRangeValues`、`TestPartitionedBTreeManagerPrunesMultiColumnListValues` 与 plan 层多列边界回归；engine 全包 118.786s；全仓 Go 通过，engine 122.587s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1024/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1024/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1024/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1024/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:12。
- 仍未完成的边界：多列分区 `BETWEEN/NOT BETWEEN` 与更广谓词推导、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1025

- P1/分区查询剪枝继续收口：多列 `RANGE COLUMNS` 支持 tuple `BETWEEN`/`NOT BETWEEN`，分别通过上下界集合求交和 `< lower OR > upper` 集合求并裁剪；首分区无下界、末分区无上界时保持正确保守语义。
- 新增 `TestPartitionedBTreeManagerIntersectsMultiColumnBetweenRange` 与 `TestPartitionedBTreeManagerUnionsMultiColumnNotBetweenRange`；engine 全包 115.185s；全仓 Go 通过，engine 120.615s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1025/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1025/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1025/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1025/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:14。
- 仍未完成的边界：多列分区更广谓词推导、分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1026

- P1/分区查询剪枝继续收口：多列 `LIST COLUMNS` 支持 tuple `NOT IN` 的安全裁剪；只有分区内所有已枚举元组均在排除集合时才移除该分区，未知/default 元组和不完整排除集合保持保守扫描。
- 新增 `TestPartitionedBTreeManagerPrunesMultiColumnNotInListValues`；engine 全包 115.873s；全仓 Go 通过，engine 120.077s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1026/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1026/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1026/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1026/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:14。
- 仍未完成的边界：分区 ALTER/REORGANIZE 的多列边界验证、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及更广物理路由，以及其余 P1/P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1028

- P1/分区维护继续收口：多列 `RANGE COLUMNS` 的 `ALTER TABLE ... TRUNCATE PARTITION` 与 `DROP PARTITION` 现在按完整 tuple 表达式定位行，确保目标分区数据被真实删除而不是只更新元数据；`ADD PARTITION` 现在对新增后的完整 RANGE 定义统一校验 tuple 边界升序、`MAXVALUE` 位置以及 scalar/tuple 不能混用。
- 新增 `TestMultiColumnDropAndTruncatePartitionDeletesRows` 与 `TestAddMultiColumnRangePartitionExtendsTupleDescriptor`；engine 全包通过（114.995s），全仓 Go 通过（engine 120.589s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1028/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1028/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1028/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1028/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:14。
- 仍未完成的边界：更广分区 ALTER/REORGANIZE 语义、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及物理路由覆盖、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1029

- P1/分区维护继续收口：多列 `LIST COLUMNS` 现在支持 `REORGANIZE PARTITION` 的 tuple 物理搬迁与元数据替换；CREATE/ADD/REORGANIZE 共用 LIST 定义校验，拒绝跨分区重复 tuple、tuple 元数不一致以及 scalar/tuple 混用。
- 新增 `TestReorganizeMultiColumnListPartitionsPreservesRows` 与 `TestReorganizeMultiColumnListPartitionsRejectsDuplicateTuple`；engine 全包通过（116.414s）；全仓 Go 通过（engine 125.829s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1029/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1029/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1029/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1029/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：更广分区 ALTER/REORGANIZE 语义、完整字符集/校对序排序和复杂类型转换、复杂分区表达式及物理路由覆盖、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1030

- P1/分区物理路由继续推进：支持确定性日期函数分区表达式 `YEAR(date_col)`、`MONTH(date_col)`、`DAY(date_col)`/`DAYOFMONTH(date_col)` 的行路由；`RANGE (YEAR(event_date))` 已覆盖建表、插入、物理分区读回以及 `WHERE YEAR(event_date)=...` 的安全分区剪枝。
- 新增 `TestRangeFunctionPartitionRoutingAndReadback` 与 `TestPartitionedBTreeManagerPrunesYearFunctionRange`；engine 全包通过（116.340s）；全仓 Go 通过（engine 122.865s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1030/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1030/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1030/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1030/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：更多日期/算术/类型转换分区表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1031

- P1/分区物理路由继续推进：增加 `TO_DAYS(date_col)` 确定性日期分区表达式，使用 MySQL 日序计算参与 RANGE 路由；`RANGE (TO_DAYS(event_date))` 已覆盖建表、插入和物理分区读回。
- 新增 `TestRangeToDaysFunctionPartitionRoutingAndReadback`；engine 全包通过（116.190s）；全仓 Go 通过（engine 121.881s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1031/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1031/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1031/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1031/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:18。
- 仍未完成的边界：更多日期/算术/类型转换分区表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1032

- P1/分区物理路由继续推进：支持单列确定性算术分区表达式 `column + constant`、`-`、`*`、`/`、`DIV` 和 `%` 的 RANGE 路由，并保留表达式边界的安全剪枝。
- 新增 `TestRangeArithmeticPartitionRoutingAndReadback` 与 `TestPartitionedBTreeManagerPrunesArithmeticRange`；engine 全包通过（121.125s）；全仓 Go 通过（engine 126.266s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1032/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1032/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1032/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1032/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:15。
- 仍未完成的边界：更复杂/多列算术、类型转换和非确定性表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1033

- P1/分区物理路由继续推进：支持单列 `CAST(column AS SIGNED/UNSIGNED)` 类型转换分区表达式，字符串列可在 RANGE 分区写入时按数值转换结果路由。
- 新增 `TestRangeCastPartitionRoutingAndReadback`；engine 全包通过（119.576s）；全仓 Go 通过（engine 121.281s）。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1033/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1033/cluster-report.json) 为 `PASS`。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1033/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1033/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:13。
- 仍未完成的边界：更复杂/多列类型转换和非确定性表达式、完整字符集/校对序排序、更广物理路由和 ALTER 维护语义、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 1020

- P1/分区优化继续收口：单列字符串 `RANGE COLUMNS` 现在支持计划层常量 `=`、`<`、`<=`、`>`、`>=` 的安全剪枝；字符串边界使用 `LessThanText`，遇到混合数值/文本边界、复杂表达式或无法证明的类型转换时保守保留全分区扫描。1019 已补齐的字符串物理行路由与本轮计划层裁剪保持一致。
- 新增 `TestPartitionedBTreeManagerPrunesStringRangeValues` 与 plan 矩阵用例，覆盖 `'alpha' < 'm'` 和 `'zulu' >= 'm'`；定向 plan/engine 测试通过。engine 全包 115.842s；全仓 Go `go test ./... -count=1 -timeout 45m` 通过，engine 120.293s。
- fresh 集群 smoke [`reports/compatibility/p1-cluster-current-continuation1020/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation1020/cluster-report.json) 为 `PASS`：一主两从、提交 DML/DDL、回滚隔离和从库提升场景通过。
- fresh 发布候选 [`reports/compatibility/release-candidate-current-continuation1020/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation1020/release-candidate.json) 为 `GO`：9/9 checks `PASS`，Connector/J 139 项、0 failures/errors/skips，Maven `BUILD SUCCESS`，JDBC 总耗时 10:09。
- 仍未完成的边界：多列 `RANGE/LIST COLUMNS`、完整 MySQL 字符集/校对序排序和复杂字符串类型转换、复杂分区表达式及更广物理路由、任意复杂递归/相关 SQL AST、完整 online DDL/MDL、完整 I_S/P_S 保真度、XA/binlog 原生互操作、非 InnoDB 专用 repair 以及 P3/P4 语义；FULLTEXT 和非 Connector/J 全量客户端按当前优先级继续后置。

## Continuation 169

- P1-SQL-003 补齐两项存储对象依赖边界：跨 schema 的 `CREATE VIEW` 源表依赖现在参与递归共享 MDL；嵌套 `CALL` 在进入被调用 procedure 前也获取对应对象共享锁，不再只依赖顶层 procedure 的锁。
- 新增 `TestCreateViewWaitsForCrossDatabaseSourceMetadataLock` 与 `TestNestedCallStoredObjectWaitsForExplicitMetadataLock`；专项回归通过，`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（115.386s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation169/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation169/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation169/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation169/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括触发器执行期对象锁的完整矩阵、复杂跨库/循环 View 依赖、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 170

- P1-SQL-003 补齐触发器执行期的对象级 MDL：storage-integrated INSERT/UPDATE/DELETE 在解析目标表后，按稳定顺序对该表全部已持久化 Trigger 获取共享对象锁，并持有到 AFTER Trigger 副作用完成；另一连接持有 Trigger 对象写锁时，DML 会等待释放，避免执行期间 DROP/ALTER TRIGGER 与触发器定义读取竞态。
- 新增 `TestDMLWaitsForExplicitTriggerMetadataLock`；专项回归通过，证明 `LOCK TABLES app.trigger_lock_before WRITE` 会阻塞目标 INSERT，释放后 INSERT 和 BEFORE Trigger 改写均成功。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（116.758s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation170/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation170/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation170/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation170/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括触发器跨表副作用/递归触发器与更细锁兼容矩阵、复杂跨库/循环 View 依赖、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 171

- P1-SQL-003 收口循环 View 依赖：`CREATE/ALTER/CREATE OR REPLACE VIEW` 在获取目标 View DDL 写锁前先递归解析源 View 依赖；检测到自引用或多级循环立即返回明确错误，避免通用语句读锁与目标 View 写锁形成约 50 秒等待超时。
- 新增 `TestCreateOrReplaceViewRejectsDependencyCycle`；循环检测修复后，自引用 View、跨 schema 源依赖和嵌套 View 源锁专项回归通过。`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（114.881s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation171/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation171/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation171/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation171/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括更复杂 View AST/跨库循环锁兼容矩阵、触发器跨表副作用/递归触发器、更细 MDL 锁模式、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 168

- P1-SQL-003 继续补齐 `INFORMATION_SCHEMA.PARAMETERS` 的对象级 MDL：参数元数据扫描现在在读取 procedure/function 持久化定义前按稳定顺序获取共享对象锁，锁等待错误向上层返回。
- 新增 `TestInformationSchemaParametersWaitsForExplicitMetadataLock`；定向回归通过，`go test ./server/innodb/engine -count=1 -timeout 30m` 通过（114.545s）。
- 本轮验证：集群 smoke [`reports/compatibility/p1-cluster-current-continuation168/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation168/cluster-report.json) 为 `PASS`；发布候选 [`reports/compatibility/release-candidate-current-continuation168/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation168/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括存储对象触发器执行期/内部嵌套调用的完整锁矩阵、MyISAM/ARCHIVE/CSV 专用 `REPAIR TABLE`、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 162

- P1-SQL-003 补齐 `ANALYZE/CHECK/OPTIMIZE TABLE` 专用维护路径的 MDL：维护语句现在先解析全部目标，按稳定顺序获取共享表元数据锁，再执行统计/一致性维护，避免绕过公共 statement lock 协调。
- 新增 `TestAnalyzeTableWaitsForExplicitTableMetadataLock`；先行回归确认旧实现会直接完成，修复后维护、隐式提交与锁等待矩阵通过。engine 全量回归通过（112.086s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation160/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation160/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation160/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation160/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括 `REPAIR TABLE`/其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。

## Continuation 158

- P1-SQL-003 补齐 `DESCRIBE/DESC table` 专用 metadata 路径：持久表现在获取会话级共享 MDL；另一连接持有 `LOCK TABLES table WRITE` 时，描述查询会等待释放；临时表继续沿用原有物理名称解析。
- 新增 `TestDescribeWaitsForExplicitTableMetadataLock`；先行回归确认旧实现会直接返回，修复后通过。engine 全量回归通过（111.929s），cluster smoke [`reports/compatibility/p1-cluster-current-continuation157/cluster-report.json`](../../reports/compatibility/p1-cluster-current-continuation157/cluster-report.json) 为 `PASS`。
- 发布候选 [`reports/compatibility/release-candidate-current-continuation157/release-candidate.json`](../../reports/compatibility/release-candidate-current-continuation157/release-candidate.json) 为 `GO`，9/9 checks `PASS`，Connector/J 139 tests、0 failures/errors/skips。
- 仍未完成的边界包括其他专用 metadata handler 的完整 MDL 矩阵、跨库对象协调、真正 online DDL/多表 DDL 精确语义和剩余 P1 优化器/SQL；FULLTEXT 与非 Connector/J 全量客户端继续后置。
