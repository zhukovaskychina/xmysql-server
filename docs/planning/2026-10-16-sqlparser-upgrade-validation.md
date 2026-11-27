# sqlparser 升级验证记录

日期：2026-10-17

已完成并有当次输出支持的验证：

- `go test ./server/innodb/sqlparser -count=1`
- `go test ./server/innodb/sqlparser ./server/dispatcher ./server/protocol ./server/innodb/plan ./server/innodb/manager -count=1`
- 解析器、执行器、分发器定向 SHOW、ALTER、账户、SET、管理、表 DDL、存储程序、视图、元数据和 AST 重写测试。
- `BenchmarkParseForExecution` 已加入并已采集两次结果，详见下方。

尚未完成的验收：

- 独立测试服务上的文本协议、JDBC、协议级预编译和重启回归。
- 完整 ALTER/表 DDL、复制命令、账户授权、存储程序控制流、触发器行语义和事件重载的运行时全量切换。
- 全量 engine 测试仍需单独取得退出码；工作区存在与本任务无关的测试数据删除和其他脏改动，不能把无输出等待当作通过。
- 扫描清单中所有 A 类位置的逐项关闭记录、性能对比原始数据和最终残留分类。

本文件只记录实际执行范围，不将已添加的 AST 接口等同于完整运行时验收。

补充验证：

- `go test ./server/innodb/engine -count=1 -timeout 5m` 实际运行约 306 秒后失败。输出包含大量现有存储/后台任务与 DDL 兼容测试日志，当前摘要未把该结果视为通过；失败输出过长，需后续按测试组拆分定位，不能用它证明本次 AST 修改全部正确。
- 最新扫描输出 `docs/planning/2026-10-16-sql-regex-audit-latest.json`：74 个 regexp 导入文件、619 个扫描位置，其中 544 个非 QuoteMeta 调用、75 个 QuoteMeta，10 个解析异常。新增 AST 代码没有新增 SQL 正则入口的结论尚未满足，仍需逐项分类。

- SQL PREPARE/EXECUTE/DEALLOCATE now has a typed execution adapter and the existing performance_schema lifecycle regression passed.

- Parser package rerun after prepared/view/stored-program changes: `go test ./server/innodb/sqlparser -count=1` passed.
- Prepared performance-schema regression and focused engine groups passed after the typed PREPARE adapter.

- `go test ./server/net -count=1` initially exposed a metric regression from the direct parsed-engine adapter; `XMySQLEngine.ExecuteParsedQuery` now preserves the engine-owned lifecycle by delegating through `ExecuteQuery`, and the focused metric test plus full net package pass.

- Final focused parser matrix passed: `go test ./server/innodb/sqlparser -run 'Test(ParseForExecution|ShowUpgrade|ExplainUpgrade|AlterUpgrade|AdminUpgrade|ExtendedAdminUpgrade|AccountUpgrade|VariableUpgrade|ViewUpgrade|StoredObjectUpgrade|StoredProgram|TableDDL|PreparedUpgrade)' -count=1`.
- Final focused engine matrix passed, including parsed context/locks, AST metadata/predicate/rewrite, stored program/view, prepared lifecycle, SHOW and ALTER routes.
- Combined `go test ./server/dispatcher ./server/protocol ./server/net -count=1` passed.

- Scanner now emits a stable heuristic `category` per site (`pattern_definition`, `syntax_candidate_or_value_match`, `value_match_or_escaping`) so residual review can separate pattern declarations from call sites. Latest category counts: 530, 14 and 75 respectively; chained regexp calls remain a known scanner limitation and are listed in the audit design.

- Benchmark evidence collected: Windows amd64, Go 1.27.0 runtime, i5-13500H; `BenchmarkParseForExecution -benchmem -count=2` produced 21,464/25,032 ns/op, 19,323/19,322 B/op and 135 allocs/op. This is the upgraded-parser baseline; no pre-upgrade comparison is available yet.

- Full SHOW/EXPLAIN/DESCRIBE matrix was attempted with the plan command and timed out after 606.615s. It exposed cascading metadata-lock waits in the shared broad engine suite, with stack traces ending in view DDL lock acquisition. The new engine-level staged parse/typed PREPARE/component hook was removed from `XMySQLEngine.ExecuteQuery`; direct `XMySQLExecutor.executeQuery` staged paths remain. Focused SHOW and lock tests pass after the rollback.

- JDBC client compile check: `mvn -q -DskipTests test` in `jdbc_client` completed successfully. Live JDBC/text protocol execution was not run because no separately authorized test service was established.

- Added disabled `SQLParserUpgradeTest` JDBC acceptance skeleton covering SHOW COLUMNS, ALTER TABLE and JDBC PreparedStatement execution; Maven compile passes. The class stays disabled until an isolated service is explicitly provisioned.

- Focused account/auth regression command passed: `go test ./server/auth ./server/dispatcher ./server/net ./server/innodb/engine -run 'Test.*Account|Test.*Password|Test.*Privilege|Test.*Role' -count=1`.

- `go test ./server/replication -count=1` passed.

- `go test ./server/observability/... -count=1` passed.

- Latest combined non-broad package run passed: sqlparser, dispatcher, protocol, net, replication and observability packages.
- Latest focused engine matrix passed for SHOW AST, parsed routing/locks, ALTER atomicity, metadata/predicate/query AST, stored program/view/object/event, prepared lifecycle and admin isolation.

- Re-ran the exact plan SHOW/EXPLAIN/DESCRIBE command after reverting engine-level staged parse injection: it now passes in 24.445s. The earlier 606-second lock cascade was caused by the temporary engine-owned parse hook and is no longer current evidence.

- Current full engine authority run: `go test ./server/innodb/engine -count=1 -timeout 10m` ran 604.004 seconds and failed/timeout. Output is dominated by existing DDL parser compatibility logs and many storage-manager background goroutines; the broad package is not green. Narrowed SHOW/EXPLAIN/DESCRIBE, auth, predicate, stored-object, prepared and lock matrices pass separately.

- After removing the engine-level staged parse hook, the exact SHOW/EXPLAIN/DESCRIBE matrix passed in 24.445s. Full engine package remains non-green: the current 10-minute authority run failed at 604.004s amid large storage initialization/DDL compatibility output.

- Latest parser/dispatcher/protocol/net/replication/observability package run passed after metadata AST routing changes.
- Full engine run remains the only broad package gate that is not green; exact targeted matrices pass.

- T16 broad metadata matrix ran 288.108s; dispatcher metadata tests passed, but engine matrix failed in `TestInformationSchemaEventsExposeLastExecutedAfterSchedulerRun` with `decode table metadata ... unexpected end of JSON input` amid many concurrent storage/DDL compatibility logs. This remains an integration gate; focused MetadataAST/predicate tests pass.

- Current plan audit after this turn: 45 checklist items checked, 49 unchecked. Unchecked work is concentrated in full runtime cutovers (replication/account/view/control-flow/trigger semantics), residual regex elimination, broad metadata/query rewrite integration, and live service/restart acceptance.

- Current plan audit: 49 checklist items checked, 45 unchecked. The remaining items are explicit runtime cutovers and live/full-service gates, not missing test scaffolding.

- Latest package regression run passed for parser/dispatcher/protocol/net/replication/observability; focused engine matrix for SHOW, DDL persistence, admin/prepared, metadata/predicate/query AST, stored program/view/trigger/reload and parsed routing also passed.

- Root-cause progress: partial `.frm` JSON writes were possible in three runtime metadata writers; converted them to atomic rename writes. The isolated event metadata test that previously failed with unexpected end of JSON now passes.

- Re-ran `TestInformationSchemaEventsExposeLastExecutedAfterSchedulerRun` in isolation after atomic metadata changes; it passes. The broad metadata matrix failure is therefore a shared-resource/concurrency cascade, while the previously observed partial JSON write is fixed.

- Full-engine root-cause work continued: atomic metadata writes, structured event scheduling, performance-schema references, replication/admin typed adapters and stored-program compatibility fixes were applied. Isolated event scheduler and focused engine regressions pass; broad package/service gates remain open.

- Latest focused engine matrix now includes atomic metadata/event scheduler and Performance Schema disabled-instrument regressions; it passes alongside parser/dispatcher/protocol/net/replication/observability packages.

- Current focused view dependency/lock tests pass after AST object-reference migration. Plan audit is now 61 checked / 33 unchecked; remaining items are full runtime cutovers, residual regex classification, and live service/restart gates.

- Combined package run surfaced a replication full-package position assertion mismatch; isolated `TestNativeRuntimePullAppliesSplitTransactionAndPersistsPosition` passes. This is recorded as a replication suite isolation/flakiness gate, not caused by dispatcher AST routing.

- Re-ran full `go test ./server/replication -count=1` after the isolated pass; it passed. The prior combined mismatch was nondeterministic suite ordering/state.

- Added `docs/planning/2026-10-16-sql-regex-residual-classification.md`, generated from the latest scanner JSON. It records counts, heuristic categories, parse-error boundaries and explicitly avoids claiming full A-class closure.

- Latest account-focused engine matrix passes after selective engine-owned AccountStatement routing (GRANT/REVOKE remains on the proven compatibility branch).

- Latest residual scan parse errors are confined to malformed legacy demo `cmd/` sources (`cmd/demo_*`, `cmd/test_*`); CI scope excludes cmd/client per plan. They remain explicitly listed rather than silently treated as complete.

- Added scanner `-exclude` support and generated CI-scope audit JSON: cmd/client excluded, 74 regexp-import files, 619 sites, zero parse errors. Default full-repository scan still preserves legacy cmd parse errors for transparency.

- SystemVariableAnalyzer `IsSystemVariableQuery` now uses ParseForExecution AST; CI-scope scanner rerun: 73 regexp-import files, 616 sites, 12 non-pattern MatchString candidates, zero parse errors.

- Final focused verification rerun passed: parser, dispatcher, protocol, net, replication, observability packages and the focused engine matrix including atomic metadata and disabled Performance Schema instrument regressions.

- Local ports 3309/3306 have no listening test service in the current environment. JDBC SQLParserUpgradeTest remains intentionally disabled; live protocol/JDBC/restart evidence is unavailable and not counted as passed.

- Latest combined focused package verification passed: manager, dispatcher, sqlparser, protocol, net, replication and observability.

## 本轮增量验证（2026-10-17）

- 计划台账当前为 64 项已核对、31 项未核对。新增已验证项：SHOW FULL TABLES AST 输出、普通对象 GRANT/REVOKE、角色多源/多目标授权、PROXY 授权、ALTER 分区操作分类、ALTER TABLE TABLESPACE AST 入口、元数据路由的字面量隔离。
- 通过命令：`go test ./server/innodb/sqlparser ./server/dispatcher -count=1`；以及 SHOW、账户/角色、复制权限、表空间、分区的精确引擎测试矩阵。
- 本轮未把宽泛 SHOW 引擎命令当成通过：该命令长时间无输出后被中止，随后拆分的 `TestShowFullTablesReturnsTableTypeColumn` 和 `TestShowASTExecution*` 均单独通过。
- 新增基准比较记录：`docs/planning/2026-10-17-sqlparser-benchmark-comparison.md`。外部 JDBC、服务重启和完整引擎包仍没有当前环境证据。

## 本轮增量验证（续）

- 复制：`CHANGE REPLICATION SOURCE TO` 结构化选项、channel 回调和权限门禁已通过；`CHANGE REPLICATION FILTER` 仍明确保留专用兼容解析。
- 新增交叉回归通过：sqlparser、dispatcher、protocol，以及账户/角色、SHOW FULL TABLES、表空间/分区、触发器/事件引擎精确矩阵。
- 计划台账当前为 67 项已核对、27 项未核对；剩余项仍集中在复杂视图、完整存储程序控制流、全量元数据正则清理、协议/JDBC/重启与完整引擎包验收。

- 本轮新增：复制源 AST 适配、CREATE/ALTER VIEW AST 入口、触发器/事件结构化元数据读取。已用精确测试验证；复杂视图 CTE 展开、DROP VIEW 多目标和存储程序控制流仍保留兼容边界。

- 存储函数简单 SET/RETURN 已改为优先消费 StoredProgramBlock；复杂过程体仍由原状态机处理。对应函数调用和元数据测试通过。
- CREATE/ALTER VIEW 已优先消费 ViewStatement AST；宽泛视图回归命令曾因共享资源长时间无输出，未计为通过，单个关键用例已拆分验证。

- 本轮新增视图 DDL AST 入口和存储函数简单 SET/RETURN AST 解析；关键视图创建/修改、函数调用和元数据测试通过。复杂控制流、复杂 CTE 展开仍未切换。

- 新增协议预编译元数据绑定与实际执行绑定隔离测试；模板、元数据 SQL 和每次执行 SQL 不共享已绑定结果。

- 复制过滤器补齐结构化选项：解析器保留括号列表，执行适配器直接构造 ReplicationFilterConfig；普通过滤列表和重写规则测试通过。

- 最新 ALTER 综合验证通过：parser AlterUpgrade 与 engine ALTER/ForeignKey/CHECK/Generated/Tablespace/Partition 矩阵。

- CTE 视图展开已改为 AST 可嵌入路径：With 实现 SelectStatement，视图定义可直接作为 Subquery 保留 CTE 节点；视图 updatability/定义重建测试通过。复杂过程控制流与外部验收仍未完成。

- 最新跨包回归通过：observability、replication、sqlparser、dispatcher、protocol，以及 CTE 视图、存储函数 AST 替换、Performance Schema 引用、事件、复制过滤器、外键和分区交换精确引擎测试。

- T16 宽泛元数据矩阵按计划命令启动后持续无输出，确认仍是共享存储/后台资源级联等待，已中止且未计为通过；精确 MetadataAST/InformationSchema/PerformanceSchema 测试继续作为有效证据。

- 最新跨包矩阵通过；修复表达式 AST 替换对解析器合成 DUAL 的处理，字符串/限定列不再被替换。

## 临时服务验收（2026-10-17）

使用临时端口 `13308`、临时数据目录启动隔离服务，未触碰共享/生产数据。仓库已有 Go MySQL 驱动执行了真实文本协议矩阵并全部成功：

- `SELECT 1`
- `SHOW VARIABLES LIKE 'max_connections'`
- `CREATE DATABASE app`
- `CREATE TABLE app.t (id INT PRIMARY KEY)`
- `ALTER TABLE app.t ADD COLUMN value INT`
- `CREATE VIEW app.v AS SELECT id FROM app.t`
- `CREATE USER`、`GRANT`、`SHOW GRANTS`
- SQL `PREPARE`、`EXECUTE`
- `SHOW TABLES`、`SHOW CREATE TABLE`、`SHOW CREATE VIEW`

临时服务进程已停止。JDBC 专用路径和重启矩阵仍未完成，不能把这次文本协议验收扩展为完整服务验收。

## JDBC 临时服务验收（2026-10-17）

在隔离临时服务 `127.0.0.1:13308` 上运行已有 `SQLParserUpgradeTest`，Maven/JUnit 返回成功：连接建立、CREATE TABLE、ALTER TABLE、SHOW COLUMNS、JDBC PreparedStatement 均通过。测试结束后恢复测试类的 `@Disabled` 状态并停止临时服务。

该证据只覆盖 JDBC 的 SHOW/DDL/PreparedStatement 子集；完整服务矩阵、重启和所有权限拒绝场景仍未完成。

- 在同一隔离服务尝试普通用户权限矩阵：CREATE USER 成功，但当前临时认证配置下普通用户登录被拒绝（1045），因此没有把它误报为“已登录后对象权限拒绝”。服务已停止，临时测试源码已清理。

- CI 范围正则扫描刷新：1072 个 Go 文件、73 个 regexp 导入文件、615 个位置、0 个解析错误；分类计数保持 528/12/75。

## 2026-10-17 resumed implementation checks

- go test ./server/dispatcher -run 'TestSystemVariableEngine_(DirectScopedSelectUsesASTExpressions|InformationSchemaTables|MetadataRouting|DoesNotRoute)' -count=1 passed.
- go test ./server/innodb/engine -run '^TestProcedure' -count=1 -timeout=5m passed after preserving control statements in AST substitution.
- go test ./server/innodb/engine -run 'Test(EventScheduler|StoredDefinitionReload|TriggerEventAST)' -count=1 -timeout=5m passed.
- No service, Go test, or engine test processes were left running after validation.


- go test ./server/innodb/engine -run '^TestMySQLProcsPrivReflectsRoutineExecuteGrant$|^TestAccountCompatibility' -count=1 -timeout=4m passed; parsed mysql.procs_priv predicates use AST evaluation.

- CI-scope scan rerun after resumed changes: 1072 Go files, 73 regexp-import files, 615 sites, zero parse errors; no temporary inspection tests or long-running Go/engine/service processes remain.

- go test ./server/innodb/engine -run 'Test(MySQLProcsPriv|InformationSchema.*Privilege|InformationSchema.*Role|InformationSchema.*Proxy|AccountCompatibility)' -count=1 -timeout=6m passed (51.7s).

- go test ./server/innodb/engine -run '^TestInformationSchemaEvents(ReflectCompletionPolicy|ExposeLastExecutedAfterSchedulerRun)$' -count=1 -timeout=4m passed.

- go test ./server/innodb/engine -run '^TestProcedure(Signal|Handler|Condition|GetDiagnostics|OutParameter|SelectInto)' -count=1 -timeout=6m passed.

- CI-scope scan after typed SQLSTATE/errno matching: 1072 Go files, 73 regexp-import files, 608 sites, 0 parse errors.

- go test ./server/innodb/engine -run 'TestStoredRoutine(ParameterSubstitutionPreservesSQLLiteralsAndQualifiedColumns|ValueSubstitutionUsesASTScopes)|^TestProcedure' -count=1 -timeout=6m passed.
- CI-scope scan after routine parameter token migration: 1072 Go files, 73 regexp-import files, 606 sites, 0 parse errors.

- Cross-package rerun passed: go test ./server/innodb/sqlparser ./server/dispatcher -run 'Test(ParseForExecution|StoredObject|StoredProgram|SystemVariableEngine)' -count=1 -timeout=5m and go test ./server/innodb/engine -run 'Test(StoredRoutine|StoredProgram|Procedure|MySQLProcsPriv|InformationSchemaEvents|TriggerEventAST)' -count=1 -timeout=8m; git diff --check passed.

- 计划要求的完整 engine 元数据命令 go test ./server/innodb/engine -run 'TestMetadataAST|Test.*InformationSchema|Test.*PerformanceSchema|Test.*MySQL.*Priv|Test.*OptimizerCost' -count=1 -timeout=8m 实际运行约 320 秒后退出失败；失败包含既有复制权限/元数据锁超时，以及视图列、例程参数/返回类型、Firewall 元数据等差异。未计为通过。
- 该命令暴露的 AST 触发器 ACTION_STATEMENT 差异已修复并由 TestInformationSchemaTriggersExposeActionOrder 单测复核通过。

- Cross-module package gate passed: go test ./server/auth ./server/net ./server/dispatcher ./server/observability/... ./server/replication -count=1 -timeout=8m (auth 7.6s, net 18.9s, dispatcher 2.8s, metrics 1.2s, replication 17.8s). Engine full metadata gate remains failed as recorded above.

- 新增 docs/planning/2026-10-17-sql-regex-site-closure.md，逐行列出当前 CI 范围 606 个扫描站点及分类。原计划中的 511 处历史基线文件未在仓库中找到，因此 T19 的历史基线闭环仍不勾选。

- 运行 scripts/generate_sqlparser.ps1 成功，goyacc exit 0（46 shift/reduce、7 reduce/reduce 冲突为现有语法报告）；生成文件与索引内容一致。T20 可重复生成/旧对象/性能证据项已满足。

## 交付状态

- 本次仅修改工作区代码和验证文档；未提交、未推送、未部署。
- 当前仍未完成的验收包括 Go 1.24 CI、完整独立服务/JDBC/权限/重启矩阵、历史 511 站点基线闭环和完整 engine 元数据门禁。

- 修复视图列名列表持久化和列元数据投影：CREATE VIEW active_users (user_id) ... 现在在 SHOW COLUMNS、SHOW FULL COLUMNS、INFORMATION_SCHEMA.COLUMNS 均保留 user_id；TestViewColumnMetadataIsVisibleToShowAndInformationSchema 通过。

- 修复例程元数据类型精度：存储对象参数解析保留嵌套类型括号/逗号，RETURN 类型 AST 提取保留 DECIMAL/VARCHAR 精度；TestInformationSchemaParametersProjectTypePrecision 与 TestInformationSchemaRoutinesProjectReturnTypePrecision 通过。

- 修复 INFORMATION_SCHEMA.columns 的元数据 IN 列表大小写匹配：AST 求值失败时仅对 INFORMATION_SCHEMA 使用既有元数据大小写不敏感兼容过滤；Enterprise Firewall 虚拟表列契约回归通过。

- CI-scope scan after view/routine/metadata fixes: 1072 Go files, 73 regexp-import files, 604 sites, 0 parse errors (pattern 519, syntax/value 12, value/escaping 73).

- 修复后交叉回归通过：view 列元数据、例程参数/返回类型精度、Firewall 虚拟表列、触发器 ACTION_STATEMENT、存储过程 token/AST 替换和全部 TestProcedure；sqlparser/dispatcher 包无目标测试项但编译通过。

- 新增 TestParseForExecutionPreservesExtractSyntax，验证 EXTRACT(YEAR FROM ...) 进入 ExtractExpr；parser、date_extract、JSON_VALUE、fulltext/spatial 相关测试通过。

- 原有 TestSQLParserUpgradeBaselineFixtures 通过；该测试仍是基线变更检测，不被当作 49 条完整兼容率。

- 元数据过滤回归通过：go test ./server/innodb/engine -run 'Test(InformationSchemaRegistryIncludesEnterpriseFirewallTables|ViewColumnMetadataIsVisibleToShowAndInformationSchema|InformationSchemaParametersProjectTypePrecision|InformationSchemaRoutinesProjectReturnTypePrecision|MySQLProcsPrivReflectsRoutineExecuteGrant|InformationSchemaTables)' -count=1 -timeout=6m; executor INFORMATION_SCHEMA filter no longer has raw SQL regex fallback.

- CI-scope scan after removing INFORMATION_SCHEMA raw filter fallback: 1072 Go files, 73 regexp-import files, 603 sites, 0 parse errors (pattern 518, syntax/value 12, value/escaping 73).

- 跨模块包门禁重跑通过：auth、net、dispatcher、observability/...、replication、sqlparser，均在 Go 1.27 环境退出 0。

- 49 条附录 B 样例已按 ParseForExecution 逐条复核，结果文件为 docs/planning/2026-10-17-sqlparser-49-fixture-parse-results.json：44 条进入 AST，3 条非法尾部被拒绝，2 条明确返回不支持（ALTER algorithm/lock 组合、HANDLER）；合法样例执行语义仍由 SQL-01 至 SQL-14 矩阵和对应引擎测试覆盖。

- 独立端口 13309 临时服务（临时数据目录）验证：文本协议 CREATE/ALTER/VIEW/SHOW/SQL PREPARE、CREATE USER/GRANT/SHOW GRANTS、触发器、INFORMATION_SCHEMA.TABLES/COLUMNS 通过；JDBC SQLParserUpgradeTest 通过；普通用户 SELECT 允许、INSERT 被拒绝；停止并重启服务后旧表、SHOW CREATE VIEW、SHOW COLUMNS 可读取。临时服务已停止，数据目录位于用户 Temp。
- 过程/事件的同一 JDBC 驱动多语句服务脚本遇到 driver commands-out-of-sync，未把该子项误报为服务矩阵通过；过程/事件单元和 engine 回归仍通过。

- 重启后复核命令：SHOW CREATE TABLE t、SHOW CREATE VIEW v、SHOW COLUMNS FROM t、SHOW INDEX FROM t、INFORMATION_SCHEMA.TABLE_CONSTRAINTS 均成功并返回 1 行；服务已停止。

- 完整 engine 元数据门禁重跑后，视图/例程/Firewall/精度差异已消失；剩余失败集中在复制运行时权限、分区/生成列旧 parser 警告、共享元数据锁等待和 Performance Schema table handle 状态，仍未计为通过。
- 直接锁门禁组合也复现共享锁超时与 replica runtime 前置条件失败；锁单测证据保留，但最终 T20 锁门禁不勾选。

- Go 1.24.0 toolchain was fetched and verified. CI-scope build of 57 packages passed. CI-scope test executed under Go 1.24: all packages passed except server/innodb/engine; engine failed on shared-lock/replica-runtime prerequisites, parser warnings for partition/generated DDL, and two view/CTE compatibility cases. Full output was captured by the command session and the failures remain explicitly listed rather than hidden.

- 修复网络层 COM_QUERY 多语句切分：BEGIN/END 过程体内部分号不再被拆开；TestSplitTopLevelStatementsPreservesStoredProgramBody 和完整 server/net 包测试通过。独立服务日志确认 JDBC CREATE PROCEDURE/CALL 已进入服务端；事件 JDBC 子项仍需独立服务稳定运行后再复核。

- 最新 CI-scope scan 未新增站点：1072 Go 文件、73 regexp-import 文件、603 sites、0 parse errors，分类 518/12/73。账户/角色元数据仍保留部分 parse-error 兼容过滤模式，因此 T16 provider 全量删除项未勾选。
- 最后锁门禁的失败已稳定归为共享元数据锁/replica runtime 前置条件和 engine 后台资源级联等待；没有把这些结果改写为通过。

- 账户/角色元数据过滤全部移除 regexp 依赖，改为 ParseForExecution + AST 谓词求值；account_metadata_compatibility.go 已无 regexp 调用。扫描降至 72 regexp-import 文件、592 sites、0 parse errors (pattern 507, syntax/value 12, value/escaping 73)。

- 非账户元数据 AST 过滤继续收口：information_schema role/keyword/view usage/extension filters 已去除 regexp，扫描降至 71 regexp-import 文件、584 sites、0 parse errors (pattern 499, syntax/value 12, value/escaping 73).

## 2026-10-17 metadata regex removal continuation

- 已将非账户元数据提供器的剩余 SQL 结构识别改为 AST：optimizer cost 的 INSERT/UPDATE/DELETE、INFORMATION_SCHEMA 参数/字符集/排序规则/引擎过滤、Performance Schema setup 表的 DML 与谓词、SHOW 的 LIKE/WHERE/目标提取、例程/触发器/事件元数据兼容路径均不再使用原始 SQL 正则。
- 元数据 LIKE 和大小写不敏感目录匹配改为手写 SQL LIKE 状态机；SQL `REGEXP` 运算符、JSON 校验、账户主机模式等值语义保留，不把值匹配误标为 SQL 结构解析。
- 当前 CI 范围扫描命令：`go run ./scripts/sql-regex-audit -root . -exclude cmd,client -output docs/planning/2026-10-16-sql-regex-audit-ci-scope.json`；结果为 1072 个 Go 文件、51 个 regexp 导入文件、450 个站点、0 个解析异常，分类为 pattern_definition 386、syntax_candidate_or_value_match 9、value_match_or_escaping 55。9 个 syntax/value 候选均为值语义或兼容边界。
- 定向回归通过：元数据 Firewall/扩展 NULL 谓词、optimizer cost DML、Performance Schema setup IN/NOT IN/NULL/UPDATE/INSERT/DELETE、SHOW 路由与 LIKE、SHOW CREATE TABLE/VIEW、SHOW FULL TABLES/COLUMNS。
- 仍未把完整 engine 元数据门禁、锁/复制运行时前置条件、JDBC 过程/事件和历史 511 站点可比性写成通过；这些边界保留在计划的未完成项中。

- 本轮继续迁移 AST：SHOW BINLOG/RELAYLOG/REPLICA STATUS、SHOW CREATE DATABASE、SQL PREPARE/EXECUTE/DEALLOCATE、INSTALL/UNINSTALL COMPONENT、CREATE TABLE LIKE、ALTER TABLESPACE、INFORMATION_SCHEMA foreign filter 等结构解析已改用 `sqlparser` AST；相关值语义正则保留。
- 本轮定向回归通过：dispatcher 全包、ALTER/结构化 ALTER、SHOW 复制/日志/数据库、SQL Prepared Statement、Component、CREATE TABLE LIKE、TABLESPACE 及外键过滤测试。
- 当前 CI 范围扫描为 1072 个 Go 文件、51 个正则导入文件、450 个站点、0 个解析异常；分类为 pattern_definition 386、syntax/value 9、value/escaping 55。（扫描 JSON 为当前权威数据。）


## 2026-10-18 AST 迁移继续验证

当前扫描为 1076 Go 文件、28 个 regexp 导入文件、150 个直接调用位置、0 Go 解析异常，分类 99/9/42。前文 450 等为历史快照，不是当前数量；Compile/MustCompile 和 QuoteMeta 不能用来证明剩余正则全部是值匹配。

新增复制管理、表维护/直方图、FLUSH、PERSIST、改密权限判断、表空间传输、CTAS 与建表索引可见性等 AST 迁移。修复通用 AST 锁入口与显式锁处理器的自身等待，相关失败锁用例复测通过。dispatcher/sqlparser 全包、engine 38.089s 组合定向回归及 57 包 CI 范围构建通过。

异步事件单独重复测试仍有 Windows 元数据文件替换 Access is denied，未计通过。完整实施边界、通过与失败证据见 [2026-10-18 进展记录](2026-10-18-sql-regex-ast-migration-progress.md)。尚未声明全部迁移完成。
