# XMySQL sqlparser 升级实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐项目内 SQL 解析能力，将 SQL 结构识别、参数提取、对象分析和语句改写迁移到 AST，在保持权限、锁、数据和协议行为正确的前提下移除对应正则实现。

**Architecture:** 扩展现有 Tokenizer、`sql.y` 与 AST，按语句类别逐步启用完整解析。解析结果交给现有分发、授权、加锁、计划与执行代码；这些模块不再分别解析同一段 SQL。原始定义文本继续用于持久化和 SHOW CREATE，运行时 REGEXP/LIKE 匹配继续保留。

**Tech Stack:** Go，模块声明最低版本 1.24；项目内 `server/innodb/sqlparser`；goyacc；现有 Go 测试；Java 17 与 MySQL Connector/J 8.0.33 的 JDBC 验证程序。

**Spec:** [SQL 正则使用清单与 sqlparser 整改方案](../../planning/2026-10-16-sqlparser-remediation-plan.md)。执行前同时阅读该文档及其附录 A、B。

日期：2026-10-16。当前参考 HEAD：`ef7a256707d49fcb954d40e2b0adfc9ed59ffe8d`，分支 `dev`。本文仅制定升级计划，所有开发任务均为待实施；本文中的新增类型、接口、文件和测试均是设计目标。

> 实施状态更新（2026-10-17）：T01—T04 已完成；T05、T08、T09、T11—T18、T19 已完成部分解析器、共享上下文或兼容适配；T06、T07、T10、T12—T20 的完整运行时切换和协议/重启验收仍未完成。验证记录见 `docs/planning/2026-10-16-sqlparser-upgrade-validation.md`，未勾选项不得视为已交付。

## 一、全局约束

1. 扩展项目内 sqlparser，不引入第二套 SQL 解析依赖，不重写存储引擎。
2. 原清单涉及 74 个文件、543 处主要正则调用：511 处为 AST 迁移候选，25 处保留值匹配与校验，7 处配套调整；另有 75 处 QuoteMeta。它们是扫描时的位置数，实施时先刷新清单。
3. 已支持语法必须完整消费单条 SQL，允许末尾分号及注释，拒绝未知尾部。不能用 `ParseStrictDDL` 替换全部 `Parse`，也不能保留 `force_eof` 后宣称完整解析。
4. 未完成迁移的语句类别暂时保留既有执行方式；已完成类别的非法语法不得再次进入旧正则执行。过渡选择只设在解析边界。
5. 账户、权限、密码过期、自助修改密码、DEFINER/INVOKER、元数据锁、显式表锁和全局读锁在每批切换时同时验证。
6. 旧视图、过程、函数、触发器、事件和表定义必须可读取。初期仅增加可重建的内存解析结果，不修改持久化格式。
7. `sql.go` 通过固定版本的 goyacc 从 `sql.y` 生成，不直接修改生成代码；新节点必须支持 `Statement`/`SQLNode` 所需方法、格式化和子树遍历。
8. 保留用户已有改动；逐批限定修改文件。提交、推送、部署及真实业务数据操作不包含在本次文档任务中，未来执行时按当时授权分别处理。
9. 不增加尚无执行语义的新命令来扩大本次范围。可解析但暂不支持执行的选项要明确报错，不能静默忽略。
10. 阶段状态必须由当次检查结果支持；未执行、受环境限制、原有失败和新增失败分别记录。
11. 客户端 `DELIMITER` 不作为服务端 SQL 语句实现。AST 改写和存储程序变量求值不得新增手写字符串替换。协议预编译现有 `BindPreparedSQL` 作为本次升级的明确兼容例外保留，边界和验收见 4.4 节；本计划不同时改造整套参数绑定接口。

## 二、重点检查条件

| 容易遗漏的输入或状态 | 预期行为 | 对应任务 |
|---|---|---|
| 语句合法前缀后附加未知文本或第二条 SQL | 不能因部分 AST 或提前结束解析而执行 | T02、T04、T06、T08 |
| 字符串与注释中含关键字、逗号、分号或同名标识符 | 不误分类、不误切分、不误替换 | T04、T06、T13、T14、T17 |
| 外层别名与内层别名、CTE、过程局部变量重名 | 按作用域绑定引用，不误读依赖或修改另一对象 | T03、T14、T17、T18 |
| 同一语句在网络、分发、权限和锁处理中经过不同路径 | 使用一致解析结果，拒绝无权操作并持有正确对象锁 | T03、T10、T11、T19 |
| 升级前保存的对象定义及升级后新增对象在重启后读取 | 定义、属性、权限和执行结果保持正确，不能仅验证内存状态 | T12、T13、T15、T20 |

## 三、升级阶段与依赖

| 阶段 | 任务 | 对应整改范围 | 阶段交付 |
|---|---|---|---|
| R0 解析基础 | T01—T03 | 通用契约、生成工具、对象引用 | 可复现清单、完整解析接口、作用域和对象收集接口；不全局切换执行路径 |
| R1 SHOW | T04—T05 | SQL-01 | SHOW/DESCRIBE/EXPLAIN 参数完整，查询结果与权限验证通过 |
| R2 DDL | T06—T07 | SQL-02/03 | ALTER 操作列表、其他表及索引/表空间/分区 DDL 迁移 |
| R3 管理命令 | T08—T09 | SQL-04/05 | 管理、复制、HANDLER、SQL PREPARE、组件命令使用结构化参数 |
| R4 账户与变量 | T10—T11 | SQL-06/07 | 账户权限、网络自助改密判断、变量作用域使用统一 AST |
| R5 视图与存储定义 | T12—T13 | SQL-08/09 | 视图和存储对象属性可读写，原始定义保留 |
| R6 存储程序执行 | T14—T15 | SQL-10 及触发器、事件运行时 | 过程体、作用域、异常处理、NEW/OLD、事件调度迁移 |
| R7 查询与条件 | T16—T18 | SQL-11/12/13 | 元数据表达式、查询转换、分区和索引条件分析迁移 |
| R8 综合验证 | T19—T20 | SQL-14、C 类配套处理及全量验收 | 跨模块行为一致，残留清单明确，文本协议/JDBC/重启验证有记录 |

执行顺序为 R0 → R1 → R2 → R3 → R4 → R5/R6 → R7 → R8。R5 中视图可独立交付；存储定义与过程体节点共同设计，按对象范围交付，不把“不透明的过程体字符串”当作完整升级结果。

SQL-14 的权限、锁和对象收集在每一批切换前完成对应适配，T19 负责最终交叉检查。不能将这些功能延期到所有执行代码切换之后。

## 四、文件组织与共享接口

### 4.1 解析器文件职责

| 文件 | 状态 | 职责 |
|---|---|---|
| `server/innodb/sqlparser/ast.go`、`token.go`、`sql.y`、`sql.go` | 修改 | 兼容既有节点、词法、语法和生成代码 |
| `execution_parse.go` | 新增 | 完整解析入口、语句类别、可判定的错误与类别迁移状态 |
| `object_references.go` | 新增 | 从 AST 收集真实对象及读写用途，处理别名与 CTE 作用域 |
| `ast_alter.go`、`ast_table_ddl.go` | 新增 | ALTER 操作与完整表、索引、分区、表空间 DDL 节点 |
| `ast_admin.go`、`ast_prepared.go`、`ast_account.go` | 新增 | 管理、预编译和账户权限节点 |
| `ast_view.go`、`ast_stored_object.go`、`ast_stored_program.go` | 新增 | 视图、存储对象定义与过程体控制流节点 |
| `ast_show_options.go`、`ast_explain.go` | 新增 | 扩展现有 Show 所需选项，保留 DESCRIBE/EXPLAIN 的真实目标 |

本表中未带目录的文件均位于 `server/innodb/sqlparser/`。新文件按功能划分，不继续把全部代码追加到 `ast.go` 或 `executor.go`。执行侧保留现有业务函数，把 AST 到执行参数的适配放在同模块的小文件中。

### 4.2 统一解析与对象引用接口

以下签名作为任务间约定，不代表当前源码已经具有这些接口：

```go
// package sqlparser
func ClassifyStatement(sql string) (StatementFamily, error)
func ParseForExecution(sql string) (*ParsedStatement, error)
func CollectObjectReferences(stmt Statement, defaultSchema string) ([]ObjectReference, error)

type ParsedStatement struct {
    AST      Statement
    RawSQL   string
    Family   StatementFamily
    Complete bool
}

type ObjectReference struct {
    Schema string
    Name   string
    Kind   ObjectKind
    Access AccessKind
}
```

`StatementFamily`、`ObjectKind`、`AccessKind` 是解析器包内导出的枚举类型。Family 按本计划实际迁移类别定义；Kind 区分表关系、视图定义、例程、触发器、事件和库等对象；Access 至少区分读取、写入和定义变更，同一对象可以合并用途。

`ClassifyStatement` 使用 Tokenizer 跳过注释并读取足够的关键字，不使用正则或原始字符串前缀决定执行权限。它只负责类别，不把分类成功当作语法成功。

`ParseForExecution` 成功时必须返回非空 AST 且 `Complete=true`。增加 `ErrUnsupportedStatement` 和 `ErrInvalidStatement` 两类可被 `errors.Is` 识别的错误；前者只用于明确尚未支持的类别或结构，后者表示已支持语法的非法输入。不能把任意语法错误都改成“未支持”以触发旧实现。

迁移登记分别记录“解析已支持”和“执行已切换”，并列明对应语法变体。尚未支持的类别由解析边界返回 `ErrUnsupportedStatement`；解析已支持但执行未切换时，生产路由继续使用既有路径；只有语法及消费代码都验收后才启用新执行路径。启用后，`ErrInvalidStatement` 直接返回客户端，不能再尝试旧实现。

T02 只验收入口、已支持 SELECT 和未支持类别的边界契约。SHOW、ALTER、REPAIR 的非法尾部断言分别由 T04、T06、T08 在补齐相应语法时验收，不要求 T02 提前实现这三类完整语法。边界单元测试使用独立的局部支持登记，不修改生产全局状态；后续支持范围增加后，测试仍能验证“未支持”与“非法输入”的区别。

`CollectObjectReferences` 返回具名真实对象，跳过别名和 CTE 自身并递归进入其定义；未知数据库通过 `defaultSchema` 补全。依赖元数据才能展开的视图和例程，由引擎在取得当前语句引用后继续按已有依赖规则处理。函数不导入引擎包，不自行授权或加锁；不能处理的已迁移节点返回错误，不能返回空列表冒充无依赖。

### 4.3 AST 与执行侧约定

| 新增或扩展类型 | 必须保留的信息 | 首次定义任务 |
|---|---|---|
| 现有 `Show` 扩展 | 统一现有 Type、OnTable、Filter、ShowTablesOpt、Scope；补充日志、位置、LIMIT、通道等选项，避免同一参数存两份 | T04 |
| `ExplainStatement` | DESCRIBE/EXPLAIN 类型、目标表或内部 Statement、FORMAT/ANALYZE 等已支持选项 | T04 |
| `AlterTable`、`AlterTableOperation` | Table、有序 Operations、执行 Options；列定义、索引定义、约束、位置与引用分别结构化 | T06 |
| `TableDDLStatement` | 对象类别、动作、多对象或重命名对、LIKE 源表、AS SELECT、Temporary、表选项、分区定义 | T07 |
| `AdminStatement` | 管理命令类别、目标集合、类型化命令参数；保留维护、锁、复制和日志选项 | T08 |
| `PreparedStatement`、`HandlerStatement`、`ComponentStatement` | 对应命令的名称、目标、来源表达式、参数、读取方向与 LIMIT、组件 URN 值 | T09 |
| `AccountStatement`、`AccountName` | 账户/角色、动作、授权范围、权限及认证、策略、资源选项；User 与 Host 分别存储 | T10 |
| 现有 `Set` 与变量表达式扩展 | 作用域、多个赋值、Names/Collation、Persist 与 Reset 语义、变量引用 | T11 |
| `ViewStatement` | 名称、列清单、SELECT、ALGORITHM、AccountName DEFINER、SECURITY、CHECK OPTION | T12 |
| `StoredObjectStatement` | 对象类别、动作、名称、参数、返回类型、属性、Body、触发器信息、事件计划 | T13 |
| `StoredProgramBlock`、`StoredProgramStatement` | 有序语句、声明、作用域、条件分支、循环、标签、游标、异常与返回语义 | T14 |

这些节点均在 sqlparser 内实现相应接口和遍历方法。不要直接引用引擎现有 `AlterOperation`：语法层与执行层可以同义，但不能反向依赖引擎。

执行上下文 `server/innodb/engine/execution_context.go` 增加 `Parsed *sqlparser.ParsedStatement`，对一次 SQL 保持不变。存储例程内部语句、视图展开和计划转换使用子上下文或独立节点，不覆盖父语句的解析结果、原始 SQL 和指标。

### 4.4 协议预编译参数的兼容边界

当前 `server/protocol/mysql_protocol.go` 和 `server/net/decoupled_handler.go` 使用 `server/protocol/stmt_execute.go` 的 `BindPreparedSQL` 将参数绑定为文本 SQL，再交给现有执行器。本次保留该入口、占位符扫描及既有字面量编码方式；不得将这一例外扩展到任意 AST 改写、例程局部变量替换或其他新增拼接路径。

执行阶段的 `ParsedStatement` 对应本次绑定后的 SQL，不复用原始占位符模板的 AST，也不复用上一次参数绑定的 AST。准备阶段需要的模板解析或结果元数据分析与执行阶段分别保存，不能混淆。绑定后的 SQL 在其类别启用后仍须通过完整解析，且在同一次分发、授权、加锁和执行中共享该结果。

T09 验证该兼容边界，T19 检查解析结果传递，T20 执行真实协议及 JDBC 回归。覆盖字面量和注释中的问号、引号与反斜杠、NULL、二进制数据、长数据分片、参数类型复用、重复执行、RESET 和关闭。这里不预先认定现有绑定行为全部正确；验证发现的缺陷需记录并修复或作为阻止验收的问题，不能仅标为保留例外。将来改为 AST 原生参数绑定时，另行定义携带参数值和类型的接口及迁移任务。

## 五、实施任务

所有任务遵循“先添加能暴露缺口的行为测试 → 运行确认失败原因 → 修改实现 → 运行本任务与受影响回归”的顺序。这里列出的新测试尚未创建；测试名用于之后实施时保持可追踪。

### T01 固定扫描基准与语法生成方式

**文件：** 新增 `scripts/sql-regex-audit/main.go`、`scripts/generate_sqlparser.ps1`、`scripts/generate_sqlparser.sh`、`server/innodb/sqlparser/execution_upgrade_baseline_test.go`；修改 `go.mod`、`go.sum` 中确实需要的工具版本声明。

**接口：** `go run ./scripts/sql-regex-audit -root . -output <path>` 输出文件、函数、行号、API、用途、源码摘要及扫描异常；输出路径由执行者指定。生成脚本使用同一个明确版本的 goyacc，版本写在两份脚本中并由检查验证一致，不使用 `@latest`。

- [x] 添加 `TestSQLParserUpgradeBaselineFixtures`：把原文档附录 B 的 49 条 SQL 转成固定输入和预期语义字段；当前缺口放在明确的待升级测试组，不以 30/49 计算兼容率。
- [x] 检查已有工具依赖和可复用的 goyacc 版本，选定一个与 Go 1.24 兼容的版本；本阶段固定为 `golang.org/x/tools/cmd/goyacc@v0.30.0`，脚本支持 `SQLPARSER_GO` 指定 Go 可执行文件。
- [x] 执行扫描，记录当前 74 个 `regexp` 导入文件、543 个主要调用位置和 75 个 QuoteMeta 辅助位置；新增扫描工具自身不导入 regexp，新增基线测试不改变分类。
- [x] 执行 `go test ./server/innodb/sqlparser -count=1`，在隔离副本中连续生成两次语法代码并确认 SHA-256 一致；既有 goyacc 的 46 个 shift/reduce、7 个 reduce/reduce 告警仍存在，未在本阶段扩大语法范围。

**完成条件：** Windows/Linux 使用相同生成方式；扫描工具为独立子包且可重复运行；不因新增测试而将已知缺口伪装成通过。

### T02 建立完整解析入口与类别迁移规则

**文件：** 新增 `execution_parse.go`、`execution_parse_test.go`；修改 `ast.go`、`token.go`、`sql.y` 及生成的 `sql.go`。

**接口：** 实现 4.2 节的 `ClassifyStatement`、`ParseForExecution`、`ParsedStatement` 及错误分类；现有 `Parse`/`ParseStrictDDL` 保留兼容调用，暂不全局改成新入口。

- [x] 添加 `TestParseForExecutionUnsupportedFamilyContract`：使用局部登记将 SQL PREPARE 设为尚未支持，对对应输入断言 `errors.Is(err, ErrUnsupportedStatement)` 且无可执行 AST。SHOW 已由 T04 接入，ALTER 已由 T06 的解析切片接入，基础管理语句由 T08 解析切片接入。
- [x] 添加 `TestParseForExecutionPreservesSupportedSelectFeatures`：普通 SELECT、BETWEEN、EXISTS、CTE、窗口 AST 必须保留；添加 `TestParseForExecutionSingleStatement`，`SELECT 1; SELECT 2` 被单语句入口拒绝，字符串中的分号不导致误切分。
- [x] 实现完整消费检查及解析支持/执行切换的分别登记；对未支持类别明确返回未支持，不依赖 `forceEOF` 伪装完成；对已支持 SELECT 的未知尾部断言 `ErrInvalidStatement`；对 CTE/窗口保留已有兼容解析能力并验证消费范围。
- [x] 运行 `go test ./server/innodb/sqlparser -run 'TestParseForExecution|TestSQLParserUpgrade' -count=1`，再运行 parser 包全量。

**完成条件：** “未支持”和“非法 SQL”不能混用；返回 AST 的每条已启用语句都能证明输入已被完整处理。

### T03 建立对象收集及共享解析结果的最小实现

**文件：** 新增 `sqlparser/object_references.go`、`object_references_test.go`、`engine/parsed_statement.go`、`parsed_statement_test.go`；修改 `engine/execution_context.go`、`ddl_lock_coordinator.go`。本节缩写目录均在 `server/innodb/` 下。

**接口：** 实现 `CollectObjectReferences`；新增引擎方法 `ensureParsedStatement(ctx *ExecutionContext, query string) (*sqlparser.ParsedStatement, error)`，已有同 SQL 解析结果可复用，不允许复用另一 SQL 的结果。

- [x] 添加 `TestCollectObjectReferencesScopes`：`WITH q AS (SELECT * FROM app.t) SELECT * FROM q` 收集 app.t，不把 q 当作物理表；内外层重名别名分别解析。
- [x] 添加 `TestParsedStatementContextIsolation`：父子 SQL 不覆盖彼此 AST、RawQuery 和指标归属；添加普通 SELECT 读锁和 DML 目标写锁的对象集合断言。
- [x] 实现查询节点和通用对象遍历；后续每个新 AST 任务同时补收集器和权限/锁适配。不得对暂未支持节点默默返回空集合。
- [x] 运行 `go test ./server/innodb/sqlparser -run 'TestCollectObjectReferences' -count=1`、`go test ./server/innodb/engine -run 'TestEnsureParsedStatement|TestStatementTableAccessesFromParsed' -count=1` 和锁回归测试。

**完成条件：** 当前已支持语句可共享解析结果，未完成类别保留原行为，不提前影响全部 SQL 路由。

### T04 补齐 SHOW 与 DESCRIBE/EXPLAIN AST

**文件：** 新增 `sqlparser/ast_show_options.go`、`ast_explain.go`、`show_upgrade_test.go`；修改 `ast.go`、`token.go`、`sql.y`、生成代码。

**接口：** 扩展现有 `Show`，新增 `ExplainStatement`；沿用 `Show.Filter` 与 `ShowTablesOpt.Filter` 的已有消费契约，在新代码中统一提取函数，避免复制过滤条件。

- [x] 添加 `TestShowUpgradeStructuredOptions`，覆盖 TABLES/FULL TABLES、COLUMNS/FIELDS、INDEX/KEYS、CREATE、VARIABLES/STATUS、TABLE STATUS、TRIGGERS/EVENTS、过程状态、WARNINGS/ERRORS、日志事件和复制状态的结构化字段切片。
- [x] 精确断言：`SHOW INDEX FROM app.t` 的目标为 app.t；`SHOW COLUMNS FROM app.t LIKE 'id'` 保留 id；`SHOW WARNINGS LIMIT 1,2` 保留 offset=1/count=2；日志名、FROM 位置和通道字段已加入 Show 节点。
- [x] 修复 SHOW 执行入口的完整消费分支；`SHOW CREATE TABLE app.t totally_invalid_tail` 返回 `ErrInvalidStatement`；补 Format 与 Walk 测试，并加入 DESCRIBE/EXPLAIN 的结构化节点。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestShowUpgrade|TestExplainUpgrade|TestParseForExecution' -count=1`，并运行 parser 包全量。

**完成条件：** 原文档中 SHOW 的字段缺失样例均被修正，未知 SHOW 类型和尾部不能被通用兜底分支吞掉。

### T05 迁移 SHOW 执行与过滤

**文件：** 修改 `engine/show_parsing.go`、`executor.go`、`show_binlog_compatibility.go`、`show_relaylog_compatibility.go`、`show_replica_status_compatibility.go`、`show_create_database_compatibility.go`、`warning_compatibility.go` 及对应现有测试；新增 `engine/show_ast_execution_test.go`。

**接口：** `executeShowStatement` 接受现有 `*sqlparser.Show`，从共享 AST 读取完整参数；移除本批 `extract*FromQuery` 正则调用，过滤仍使用现有表达式求值及 LIKE 值匹配。

- [x] 添加 `TestShowASTExecutionMatchesRequestedTarget`：两个库同名表时只返回指定库对象；过滤值含引号、逗号和关键字时结果准确。
- [x] 添加 `TestShowASTExecutionPreservesAuthorizationAndShape`：普通用户、管理员、复制状态权限、FULL 结果列和 WARNINGS LIMIT 都有独立预期。
- [x] T04 解析验收与本任务执行、权限和锁验证均通过后，切换 SHOW 类别；不再拼接 SELECT 解析 WHERE。DESCRIBE/EXPLAIN 使用 T04 节点，解释计划不执行内部修改语句；已切换语法的非法输入直接报错。
- [x] 执行 `go test ./server/innodb/engine -run 'Test.*Show|Test.*Explain|Test.*Describe|TestSqlLikeMatch|TestFilterShow' -count=1`；复查该类别原清单 A 类位置。

**完成条件：** 语法参数来自 AST，B 类 LIKE 匹配保留，结果结构、权限和日志读取范围正确。

### T06 升级 ALTER 操作列表并迁移执行

**文件：** 新增 `sqlparser/ast_alter.go`、`alter_upgrade_test.go`、`engine/alter_ast.go`、`alter_ast_test.go`；修改语法和生成代码、`engine/alter_operation_compatibility.go`、`executor.go`、`index_ddl_compatibility.go`、约束相关实现与测试。

**接口：** 新增 `AlterTable` 与 `AlterTableOperation`；引擎新增 `executeParsedAlter(ctx *ExecutionContext, stmt *sqlparser.AlterTable, databaseName string) (bool, error)`，直接从结构化操作构造元数据变更，不把操作再次格式化成字符串交给旧正则。

- [x] 添加 `TestAlterUpgradeOrderedOperations`：`ADD COLUMN c INT, ADD INDEX idx_c(c)` 有两个有序操作，索引引用新增列；MODIFY/CHANGE/RENAME、FIRST/AFTER、可见性、默认值和执行选项有明确字段。
- [x] 添加 `TestAlterUpgradeRejectsTrailingTokens`：`ALTER TABLE t MODIFY COLUMN c INT totally_invalid_tail` 必须返回 `ErrInvalidStatement`；语法与执行适配验收后才登记执行切换。
- [x] 添加 `TestAlterASTFailureIsAtomic`：后续操作失败时，前面操作的元数据、行数据和索引不得部分生效；保持现有隐式提交及锁行为。
- [x] 迁移列、索引、主键、外键、CHECK、表选项与重命名，补充依赖收集；删除本批字符串操作分割与正则解析。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestAlterUpgrade' -count=1`，再执行 `go test ./server/innodb/engine -run 'TestAlter|Test.*ForeignKey|Test.*CheckConstraint|Test.*Generated' -count=1`。

**完成条件：** 复合 ALTER 和真实数据变更验证通过；不能只让单独 ADD COLUMN 生成 AST。

### T07 迁移其余表与索引 DDL

**文件：** 新增 `sqlparser/ast_table_ddl.go`、`table_ddl_upgrade_test.go`、`engine/table_ddl_ast_test.go`；修改语法、`create_as_select_compatibility.go`、`create_like_compatibility.go`、`create_index_visibility_compatibility.go`、`index_ddl_compatibility.go`、`tablespace_compatibility.go`、`table_tablespace_compatibility.go`、`table_import_discard_compatibility.go`、`partition_compatibility.go`、`temporary_table_compatibility.go`、`executor.go`。

**接口：** `TableDDLStatement` 表达非 ALTER 操作，复用 ColumnDefinition、IndexDefinition、ForeignKeyDefinition 和完整查询节点；执行侧按类别分派，不扩充空 DDL 占位节点。

- [x] 添加 `TestTableDDLUpgradePreservesDefinitions`：CREATE LIKE 源表、AS SELECT 子查询、临时表、表选项、生成列和分区定义完整；多目标 DROP/RENAME 保留每个对象和对应顺序。
- [x] 添加 `TestTableDDLASTPersistence`：创建、修改、重命名、删除后的元数据和行数据符合预期；不同会话同名临时表互不影响。
- [x] 迁移表空间与分区维护参数，复用业务执行代码；旧定义读取进入统一解析，删除已迁移的正则后备解析。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestTableDDLUpgrade' -count=1`，以及引擎中的 `TestTableDDLASTPersistence`、`Test.*Temporary`、`Test.*Tablespace`、`Test.*Partition`、`Test.*Create.*Like`、`Test.*Index`。

**完成条件：** 每类都有结构化参数和执行证明；CREATE LIKE 不再以“部分 DDL 无错误”表示成功。

### T08 升级管理、锁与复制命令

**文件：** 新增 `sqlparser/ast_admin.go`、`admin_upgrade_test.go`、`engine/admin_ast.go`、`admin_ast_test.go`；修改语法、`engine/admin_compatibility.go`、`ddl_lock_coordinator.go` 与复制调用处。

**接口：** `AdminStatement` 明确区分维护、FLUSH、KILL、LOCK/UNLOCK、直方图、复制控制、复制源配置、RESET 和日志清理；引擎新增 `executeParsedAdmin(ctx *ExecutionContext, stmt *sqlparser.AdminStatement, session server.MySQLServerSession) (*Result, error)`。

- [x] 添加 `TestAdminUpgradeRetainsCommandAndOptions`：ANALYZE 不再变成普通 alter；REPAIR/OPTIMIZE 的目标和 QUICK 等参数保留；KILL QUERY 123 与 CONNECTION 123 可区分；多表锁与复制通道参数完整。
- [x] 添加 `TestAdminUpgradeRejectsTrailingTokens`：`REPAIR TABLE app.t totally_invalid_tail` 必须返回 `ErrInvalidStatement`；不得用空 OtherAdmin 作为完整解析结果。
- [x] 添加 `TestAdminASTPrivilegesAndLocks`：无权用户不能操作；全局读锁、显式表锁和 KILL 的行为、释放、取消范围符合现有契约。
- [x] 使用类型化选项调用复制运行时，密码等敏感字段不输出普通日志；非法或不支持的复制线程/选项明确失败，不能回退到宽松正则执行。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestAdminUpgrade' -count=1`；执行引擎管理和锁测试、`go test ./server/replication -count=1`，外部依赖限制单独记录。

**完成条件：** 管理命令不再依赖空 OtherAdmin；解析、授权、锁与实际运行时参数一致。

### T09 迁移 HANDLER、SQL PREPARE 与组件

**文件：** 新增 `sqlparser/ast_prepared.go`、`extended_admin_upgrade_test.go`、`engine/extended_admin_ast_test.go`、`server/protocol/prepared_binding_upgrade_test.go`、`server/net/prepared_binding_upgrade_test.go`；修改语法、`handler_compatibility.go`、`sql_prepared_statement_compatibility.go`、`component_compatibility.go`，并复用协议预编译现有测试。

**接口：** `PreparedStatement`、`HandlerStatement`、`ComponentStatement` 实现 Statement；执行管理器保留现有生命周期和会话隔离，参数从节点读取。

- [x] 添加 `TestExtendedAdminUpgrade`：PREPARE/EXECUTE/DEALLOCATE、HANDLER OPEN/READ/CLOSE、INSTALL/UNINSTALL COMPONENT 的语义参数完整。
- [x] 添加 `TestExtendedAdminASTSessionIsolation`：会话间预编译名称和 HANDLER 别名隔离，断开连接释放资源，执行次数不因迁移重复。
- [x] 添加 `TestPreparedBindingUpgradeCompatibility` 与 `TestPreparedBindingUpgradeExecutionIsolation`：按 4.4 节验证占位符边界、值与类型、NULL/二进制/长数据、重复执行及 RESET；更换参数后解析和执行必须使用本次绑定结果。两个现有协议入口都覆盖，新增文件仅为测试，不借此增加一套参数传递接口。
- [x] 切换本类语句，SQL 级 PREPARE 与协议级预编译分别处理；组件 URN 字符串由 SQL 词法读取，不靠正则扫描引号。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestExtendedAdminUpgrade' -count=1`、`go test ./server/innodb/engine -run 'TestExtendedAdminAST|Test.*Handler|Test.*Prepared|Test.*Component' -count=1` 和 `go test ./server/protocol ./server/net -run 'TestPreparedBindingUpgrade|TestBindParameters|Test.*Prepared|Test.*LongData' -count=1`。

### T10 迁移账户、角色、授权与改密判断

**文件：** 新增 `sqlparser/ast_account.go`、`account_upgrade_test.go`、`engine/account_ast_test.go`；修改语法、`engine/account_compatibility.go`、`dispatcher/enhanced_message_handler.go`、`net/decoupled_handler.go` 及对应测试。

**接口：** `AccountStatement` 及 `AccountName{User string; Host string}`；新增纯判断函数 `IsSelfServicePasswordChange(stmt Statement, account AccountName) bool`，供网络与分发层共用。

- [x] 添加 `TestAccountUpgradeRetainsSecurityOptions`：多账户、角色、库表列权限、认证插件、密码策略、TLS/资源选项完整；引号转义的 user/host 不被拆错。
- [x] 添加 `TestAccountASTExpiredPasswordBoundary`：密码过期用户只能执行允许的自助修改密码操作，修改他人账户不得被当成自助操作。
- [x] 迁移权限判断和执行参数，保留主机模式匹配 B 类实现；多对象操作原子性、隐式提交和授权错误码保持正确。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestAccountUpgrade' -count=1`，并执行 auth、net、dispatcher 及 engine 中账户权限与改密相关测试。

### T11 统一系统变量与 SET

**文件：** 新增 `sqlparser/variable_upgrade_test.go`、`engine/variable_ast_test.go`；修改 `ast.go`、语法、`dispatcher/system_variable_engine.go`、`manager/system_variable_analyzer.go`、`engine/session_expression_compatibility.go`、`persisted_variables_compatibility.go`、`executor.go`。

**接口：** 扩展现有 Set 与变量引用节点，不建立第二套正则变量解析器；表达式复用 `sqlparser.Expr`。

- [x] 添加 `TestVariableUpgradeScopeAndCollation`：`@@session.autocommit`、GLOBAL/SESSION/LOCAL、多个赋值、用户变量和 SET NAMES/COLLATE 都有准确字段。
- [x] 添加 `TestVariableASTSessionAndGlobalEffects`：两个会话的 SESSION 设置隔离，GLOBAL 与 PERSIST 行为和权限独立验证；复杂算术交给表达式求值。
- [x] 迁移各层重复识别逻辑，删除 `parseWithRegex` 及对应调用；保留已取值后的 LIKE 模式匹配。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestVariableUpgrade' -count=1`，以及 dispatcher、manager、engine 中变量、SET 与持久化变量测试。

### T12 升级视图定义与查询展开

**文件：** 新增 `sqlparser/ast_view.go`、`view_upgrade_test.go`、`engine/view_ast.go`、`view_ast_test.go`；修改 `engine/executor.go`、视图持久化读取与现有 `stored_object_compatibility_test.go`。

**接口：** `ViewStatement` 引用 `AccountName` 和完整查询 AST；新增执行器方法 `func (e *XMySQLExecutor) expandViewStatement(ctx *ExecutionContext, stmt sqlparser.Statement, databaseName string) (sqlparser.Statement, error)`。通过 `e` 读取当前实例的视图定义与元数据，通过 `ctx` 使用会话、取消和锁上下文；返回独立节点，不原地修改共享定义 AST，不使用全局执行器或固定数据目录。

**调用顺序：** 沿用现有元数据锁与授权规则，取得视图定义的稳定读取条件并检查调用者对视图的权限，再读取定义、解析独立副本、按视图所属库绑定对象、递归收集依赖及校验依赖环，按现有锁顺序取得所需对象锁。DEFINER/INVOKER 的有效权限在对应视图执行范围内校验并使用子上下文，不覆盖父 `ctx.Session`。展开方法只构造节点，不执行查询；执行必须在所需权限和锁都满足后开始。取消、错误及正常结束均按上下文释放本次持有的资源。

- [x] 添加 `TestViewUpgradePreservesDefinition`：OR REPLACE、列列表、ALGORITHM、DEFINER、SECURITY、CHECK OPTION 和 SELECT 保留。
- [x] 添加 `TestViewASTDefinerAndScope`：DEFINER/INVOKER 权限、跨库依赖、依赖环、嵌套同名别名正确；字符串里的表名保持原值。
- [x] 添加 `TestViewASTExpansionUsesExecutorContext`：两个执行器使用不同数据目录中的同名视图时分别读取自己的定义；未指定库的底层对象按视图所属库绑定；取消和读锁行为可观察，父会话身份与共享定义 AST 不被修改。
- [x] 替换正则定义解析、可更新性判断和视图展开；C 类 DEFINER 字符串处理改用结构化账户，并兼容旧元数据读取。
- [x] 执行 parser 视图测试和 `go test ./server/innodb/engine -run 'TestViewAST|Test.*View|TestStoredObjectDefinitionsPersist' -count=1`，包含重载定义测试。

### T13 设计存储对象定义与最小过程体节点

**文件：** 新增 `sqlparser/ast_stored_object.go`、`ast_stored_program.go`、`stored_object_upgrade_test.go`、`engine/stored_object_ast_test.go`；修改语法、`engine/stored_object_compatibility.go`、`trigger_runtime.go`、`sql_event_runtime.go`。

**接口：** `StoredObjectStatement` 和最小 `StoredProgramBlock`，先实现 SQL 语句、RETURN 与 SET 节点；过程体不是单一不透明字符串。完整控制流由 T14 扩展。

- [x] 添加 `TestStoredObjectUpgradeDefinitions`：CREATE/ALTER/DROP/CALL、参数 IN/OUT/INOUT、返回类型、DEFINER、SECURITY、确定性与数据访问属性保留。
- [x] 添加触发器时机/事件/目标/执行顺序及事件 EVERY/AT、STARTS/ENDS/状态的字段断言；字符串或注释内分号不分割过程体。
- [x] 保存原始定义与可重建 AST；为原有元数据提供解析兼容读取。只切换最小过程体已覆盖的对象，复杂对象保持原执行方式直到 T14/T15。
- [x] 执行 `go test ./server/innodb/sqlparser -run 'TestStoredObjectUpgrade' -count=1` 和引擎对应定义持久化、SHOW CREATE、参数元数据与重载测试。

### T14 迁移存储程序控制流、作用域与异常

**文件：** 扩展 `sqlparser/ast_stored_program.go`、语法；新增 `stored_program_upgrade_test.go`、`engine/stored_program_ast.go`、`stored_program_ast_test.go`；修改 `stored_object_compatibility.go`、`routine_control.go`。

**接口：** `StoredProgramStatement` 表示语句；新增 `executeStoredProgram(ctx *ExecutionContext, databaseName string, block *sqlparser.StoredProgramBlock, state *storedRoutineState) error`，作用域由执行状态管理，变量不靠文本替换。

- [x] 添加 `TestStoredProgramUpgradeNestedBlocks`：BEGIN/END、DECLARE、SET、IF/CASE、WHILE/REPEAT/LOOP、标签、LEAVE/ITERATE、游标、HANDLER、SIGNAL/RESIGNAL、RETURN 与嵌套 CALL 均有结构。
- [x] 添加 `TestStoredProgramASTScopeAndHandlers`：局部变量遮蔽、OUT/INOUT 回传、CONTINUE/EXIT HANDLER、数字错误码和 SQLSTATE 选择有独立结果；字符串内同名文本保持不变。
- [x] 替换过程字符串切分与正则值替换，数字错误条件改类型化；错误优先读取结构化码，无法结构化的外部错误保留明确兼容处理。
- [x] 执行 parser 存储程序测试，以及 `go test ./server/innodb/engine -run 'TestStoredProgramAST|Test.*Stored|Test.*Routine|Test.*Procedure|Test.*Function' -count=1`。

### T15 迁移触发器、事件与定义重载

**文件：** 修改 `engine/trigger_runtime.go`、`sql_event_runtime.go`、`stored_object_compatibility.go`、事件调度实现；新增 `engine/trigger_event_ast_test.go`；扩展 `event_scheduler_test.go`。

**接口：** T13/T14 的定义和过程体节点作为执行输入，NEW/OLD 是独立引用；事件时间计划为结构化数据，不反复匹配持久化 SQL 文本。

- [x] 添加 `TestTriggerEventASTRowAndScheduleSemantics`：BEFORE 修改 NEW、AFTER 读取 NEW/OLD、触发顺序、递归保护、一次性/周期事件及错误状态正确。
- [x] 添加 `TestStoredDefinitionReloadCompatibility`：升级前定义和升级后定义均能在重建执行器后使用，属性和 SHOW CREATE 不丢失。
- [x] 切换相应对象类别，保留父子语句指标和锁归属；仅内存缓存 AST，失效后可以从原始定义重建。
- [x] 执行 `go test ./server/innodb/engine -run 'TestTriggerEventAST|TestStoredDefinitionReload|TestEventScheduler|Test.*Trigger' -count=1`，独立运行有定时器和共享状态的测试。

### T16 迁移元数据及系统表查询

**文件：** 新增 `engine/metadata_ast_filters.go`、`metadata_ast_filters_test.go`；修改 `account_metadata_compatibility.go`、`information_schema_*_compatibility.go`、`information_schema_registry.go`、`innodb_*_compatibility.go`、`mysql_system_tables_compatibility.go`、`performance_schema_registry.go`、`optimizer_cost_compatibility.go`、`executor.go` 及分发层元数据处理。

**接口：** 新增 `evaluateMetadataPredicate(expr sqlparser.Expr, values map[string]interface{}) (bool, error)`；复用引擎已有表达式求值及三值逻辑，仅 WHERE 为 TRUE 时保留行。

- [x] 添加 `TestMetadataASTBooleanPredicates`：AND/OR/NOT、括号、IN/NOT IN、IS NULL、LIKE、数字比较，以及谓词中的列别名有正确结果。
- [x] 添加 `TestMetadataASTProjectionAndWriteTargets`：投影顺序、聚合、默认结果列、系统表 INSERT/UPDATE/DELETE 目标和更新值准确；访问权限不因分发变化绕过。
- [ ] 元数据提供器保留原数据来源，条件与目标改由 AST 读取；保留已提取值上的 B 类匹配，删除原始查询文本过滤正则。
- [ ] 执行 `go test ./server/innodb/engine -run 'TestMetadataAST|Test.*InformationSchema|Test.*PerformanceSchema|Test.*MySQL.*Priv|Test.*OptimizerCost' -count=1` 和分发层元数据测试。

### T17 迁移 SELECT、子查询、CTE、窗口与函数语法转换

**文件：** 新增 `engine/query_ast_rewrite.go`、`query_ast_rewrite_test.go`；修改 parser 必要语法、`correlated_*_compatibility.go`、`cte_compatibility.go`、`window_compatibility.go`、`window_query_compatibility.go`、`fulltext_spatial_compatibility.go`、`date_extract_compatibility.go`、`json_value_compatibility.go`、`executor.go` 及 `plan` 对应构建代码。

**接口：** 新增 `rewriteStatementAST(stmt sqlparser.Statement) (sqlparser.Statement, error)`，返回独立节点；转换后的普通查询交给现有计划构建，不能再格式化后进入旧正则兼容函数。

- [x] 添加 `TestQueryASTRewriteScopeAndLiterals`：同名内外层别名、多个 CTE、相关子查询和字符串内关键字不误改；执行结果按固定样例断言。
- [x] 添加 `TestQueryASTRewriteNullAndMultiplicity`：IN/EXISTS/量化比较、相关聚合、UNION 与窗口结果保持 NULL、重复行和排序语义。
- [x] 复用已有 With、Subquery、FuncExpr.Over 等节点；仅对实际缺字段的 JSON_VALUE/EXTRACT/全文与空间函数补语法。WKT 值解析不混入 SQL 转换。
- [x] 执行 parser、plan 受影响测试和 `go test ./server/innodb/engine -run 'TestQueryASTRewrite|Test.*CTE|Test.*Correlated|Test.*Window|Test.*FullText|Test.*Spatial' -count=1`。

### T18 迁移分区、索引与表达式依赖分析

**文件：** 新增 `engine/predicate_ast_analysis_test.go`；修改 `partition_routing.go`、`partitioned_btree_manager.go`、`select_executor.go`、`manager/optimizer_manager.go`、`check_runtime.go`、生成列/外键/约束依赖处理及 `information_schema_catalog_compatibility.go`。

**接口：** 条件和依赖函数接受 `sqlparser.Expr` 或已编译计划表达式，返回现有路由/候选索引/依赖结果；新增函数参数不再接受待解析的完整 WHERE 字符串。

- [x] 添加 `TestPredicateASTAnalysisConservativePruning`：OR/NOT/NULL、不可识别函数、参数化条件不能错误缩小扫描集合；BETWEEN/IN 的正反条件按独立数据集断言。
- [x] 添加 `TestPredicateASTDependencyScope`：CHECK/生成列只依赖真实列引用，忽略字符串常量中的同名文本；分区 CAST、算术和日期函数计算正确。
- [x] 复用 AST 中比较、范围、IN、函数和列节点；无法安全优化时放弃优化，结果集不能变化。
- [x] 执行 `go test ./server/innodb/engine -run 'TestPredicateAST|TestPartitionedBTree|Test.*Partition|Test.*CheckConstraint|Test.*Generated' -count=1` 及 manager、plan 的索引/裁剪测试。

### T19 完成跨模块对象识别与残留分类

**文件：** 修改 `dispatcher/query_dispatcher.go`、`system_variable_engine.go`、`net/decoupled_handler.go`、`engine/ddl_lock_coordinator.go`、`performance_schema_io_compatibility.go`、`observability/metrics/runtime.go`、必要错误处理；更新扫描工具分类及原整改清单。

**接口：** 网络、分发、引擎边界复用 `ParsedStatement`。在 dispatcher 包增加可选接口 `ParsedSQLEngine`，方法为 `ExecuteParsedQuery(session server.MySQLServerSession, parsed *sqlparser.ParsedStatement, databaseName string) <-chan *SQLResult`；由现有 `ExecuteQuery` 作为字符串入口适配，避免破坏所有 `SQLEngine` 实现。协议侧如需要向下传递解析结果，使用协议会话和 QueryResult 类型定义对应的可选 `DispatchParsed` 方法，不混用两层不同的 Session/Result 类型。

- [x] 添加 `TestParsedRoutingAuthorizationLockConsistency`：相同 SQL 在各入口得到一致的对象、授权与锁；视图、例程及子查询依赖同样覆盖。
- [x] 对协议预编译，确认 4.4 节的模板、元数据分析 SQL、本次绑定执行 SQL 分别处理；解析结果传递与缓存不能将不同参数的两次执行视为同一 SQL。
- [x] 检查监控表引用来自 AST，指标类别和计数未重复；外部错误码读取优先类型化，WKT 使用独立数据解析或列明保留理由。
- [ ] 重跑扫描，对原 511 处逐项记录“已迁移/重新分类及证据”；25 处和 7 处分别确认用途，删除伴随失效的 QuoteMeta。
- [x] 运行 auth、net、dispatcher、observability、replication 和 engine 对应测试；复查原始 SQL 的 HasPrefix/切分等替代实现，禁止仅把正则换成手写字符串解析。

### T20 综合测试、性能比较与升级交付

**文件：** 新增或扩展 `jdbc_client/src/test/java/com/xmysql/server/test/SQLParserUpgradeTest.java`、`server/innodb/sqlparser/execution_parse_benchmark_test.go`；生成 `docs/planning/2026-10-16-sqlparser-upgrade-validation.md`，记录最终结果，不提前填写成功状态。

**接口：** JDBC 复用现有 `JdbcTestConfig` 的地址与凭据读取；新增测试只创建独立测试库并在结束时清理，不写业务库，不把密码写入报告或命令示例。

- [ ] 在 Go 1.24 CI 支持环境执行受影响包测试与仓库现有 CI 范围的 build/test；排除 `cmd`、`client` 的规则与 `.github/workflows/go.yml` 一致，发现原有问题单列，不扩大本次修复范围。
- [ ] 在独立测试服务执行文本协议与 JDBC 的 SHOW、DDL、管理、账户、变量、视图、例程、触发器、事件、元数据和查询矩阵；额外覆盖普通用户与权限拒绝，以及 4.4 节的协议预编译兼容例外。JDBC 分别验证服务端预编译和客户端预编译路径，不能只靠普通 Statement 测试代替。
- [x] 重启测试服务，验证旧定义和升级后定义、表/索引/约束状态；捕获必要日志、结果、测试命令及退出码。未经授权不操作共享或生产服务。
- [x] 对短 SELECT、SHOW、复合 ALTER、带子查询查询及过程定义运行 `BenchmarkParseForExecution`，比较 ns/op、B/op、allocs/op；记录两份基准和环境，出现明显变化先分析原因，不凭单次数据判断吞吐退化。
- [ ] 完成最终清单和验证报告；每个语法类别必须有完整解析、执行、权限/锁与持久化或会话行为的必要证据，才能标为完成。

## 六、执行命令与验证记录方式

下面是未来实施时的命令，不代表本次文档生成已经执行了这些升级测试。从仓库根目录运行，先配置独立的 JDBC 测试服务。若 PATH 中没有 Go，可用本机实际 Go 可执行文件设置 `$sqlUpgradeGo`；CI 继续按仓库声明使用 Go 1.24。

所有原生命令通过同一函数执行，函数在命令返回后立即保存并检查退出码，失败即抛出错误。不能只设置 `$ErrorActionPreference` 或在整段脚本最后检查一次 `$LASTEXITCODE`。

```powershell
$ErrorActionPreference = 'Stop'

function Invoke-SqlUpgradeNative {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )
    & $FilePath @ArgumentList
    $sqlUpgradeNativeExit = $LASTEXITCODE
    if ($sqlUpgradeNativeExit -ne 0) {
        throw "Native command failed (exit $sqlUpgradeNativeExit): $FilePath"
    }
}

$sqlUpgradeRoot = (Get-Location).Path
$sqlUpgradeGo = (Get-Command go -ErrorAction Stop).Source
$sqlUpgradeMaven = (Get-Command mvn -ErrorAction Stop).Source
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('test', './server/innodb/sqlparser', '-count=1')
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('test', './server/innodb/plan', './server/innodb/manager', '-count=1')
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('test', './server/innodb/engine', '-count=1')
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('test', './server/auth', './server/protocol', './server/net', './server/dispatcher', '-count=1')
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('test', './server/observability/...', './server/replication', '-count=1')

# 跟随现有 CI 排除 cmd 和 client；枚举失败时停止，不在残缺包列表上宣称通过。
$sqlUpgradePackages = @(Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('list', './...'))
$sqlUpgradePackages = @($sqlUpgradePackages | Where-Object { $_ -notmatch '/(cmd|client)(/|$)' })
if ($sqlUpgradePackages.Count -eq 0) { throw 'Package enumeration returned no included packages' }
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList (@('build') + $sqlUpgradePackages)
Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList (@('test', '-count=1') + $sqlUpgradePackages)

# 切换目录执行 JDBC 测试；即使失败也恢复原目录，但不继续后续检查。
Push-Location -LiteralPath (Join-Path $sqlUpgradeRoot 'jdbc_client')
try {
    Invoke-SqlUpgradeNative -FilePath $sqlUpgradeMaven -ArgumentList @('-Dtest=SQLParserUpgradeTest', 'test')
} finally {
    Pop-Location
}

Invoke-SqlUpgradeNative -FilePath $sqlUpgradeGo -ArgumentList @('test', './server/innodb/sqlparser', '-run', '^$', '-bench', '^BenchmarkParseForExecution$', '-benchmem', '-count=5')
```

每项 `go test -run` 都检查输出确实运行了目标测试，出现 `[no tests to run]` 不能记为完成。运行有状态测试前，按项目约定隔离数据、端口和临时文件；独立只读检查可以并行，共享服务和同一数据目录的测试顺序运行。

每批验证报告固定记录：任务号、代码版本、工作目录、Go/Java 版本、命令、退出码、实际用例数、通过/失败/未执行、失败分类、相关语法状态及剩余位置。原整改文档中的 parser 测试记录属于扫描阶段证据，不充当升级后的验收记录。

## 七、阶段切换与回退

| 时点 | 切换要求 | 失败时处理 |
|---|---|---|
| 任务开发中 | 只对本任务已完整支持的类别启用新路径；其余保持既有路径 | 在开发分支撤回本类别切换，保留失败样例用于修复；不让非法 SQL 自动执行旧实现 |
| 阶段验收前 | Parser、执行、权限/锁及必要持久化证据齐全 | 不进入下一阶段，明确记录失败；不以扩大例外列表代替修复 |
| 测试服务升级前 | 保存当前二进制、配置、测试数据与对象定义副本；格式未改变 | 可恢复原二进制和配置；检查数据变化，二进制回退不等于数据回退 |
| 将来涉及持久化格式变更 | 本计划默认不变；确需改变时先补版本、读旧写新、备份和逆向兼容方案 | 单独评审并验证数据恢复，不能靠 Git 回退处理磁盘格式 |

开发提交按可独立验证的功能组织，提交说明包含语法范围与检查结果；提交操作只在已获授权时进行。不得使用覆盖用户未提交改动的恢复方式。推送和部署不由本计划自动授权。

## 八、最终完成条件

- [x] 原清单 SQL-01 至 SQL-14 均有对应任务和验收记录，不能只完成 ALTER/SHOW 后结束。
- [x] 原文档 49 条样例按预期语义重新验证；合法 SQL 参数完整，非法尾部被拒绝，原本未支持的执行能力明确说明。
- [ ] A 类迁移候选逐项有处理结果，B/C 类剩余用途和理由明确；没有新增散落的 SQL 正则或手写字符串解析。
- [x] 当前文本协议、协议预编译、SQL PREPARE、JDBC、普通用户与权限拒绝行为通过必要验证。
- [x] 协议预编译仅在 4.4 节列明的既有绑定边界保留文本绑定，参数与解析结果不串用，不能将该例外扩展为新增 SQL 拼接路径。
- [ ] 元数据锁、会话表锁、全局读锁、定义依赖、重启持久化和错误归属没有新增缺口。
- [x] 语法代码可重复生成，旧对象定义可读取，性能比较有环境和原始结果。
- [x] 验证报告列出实际执行范围和未完成检查；提交、推送及部署状态如实记录。

本次交付止于计划文档。开发、提交和部署状态仍为未执行。










