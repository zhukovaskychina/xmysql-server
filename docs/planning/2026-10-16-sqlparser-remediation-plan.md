# XMySQL SQL 正则使用清单与 sqlparser 整改方案

日期：2026-10-16。核查对象：`dev` 分支当前工作目录，HEAD 为 `ef7a256707d49fcb954d40e2b0adfc9ed59ffe8d`。状态：已完成清单与方案，尚未实施代码整改。

## 1. 结论与范围

SQL 的语句分类、参数提取、对象依赖分析和语句改写，应逐步使用项目内的 `server/innodb/sqlparser` 及其抽象语法树（AST）。解析器缺少的能力需要先补充，再迁移执行代码，不能将现有正则调用简单替换成 `sqlparser.Parse()`。

当前 SQL 相关正则不只涉及 ALTER、SHOW、管理命令和存储过程，还包括账户权限、视图、系统变量、元数据查询、预编译语句、子查询、CTE、窗口函数、分区表达式、优化器条件识别以及权限和锁所需的对象提取。以上均纳入清单。

整改不要求删除所有正则。SQL 的 REGEXP/LIKE 运行时匹配、数据格式校验、主机通配符以及摘要占位符处理，需要与 SQL 语法解析分别处理。WKT 空间数据和错误消息解析也不应通过包装 SQL 的方式交给 sqlparser。

本次仅新增文档。未修改 Go 源码、语法文件、测试、依赖或运行配置，未操作数据库。

## 2. 扫描结果与统计方法

扫描当前目录的 Go 文件，排除隐藏目录中的历史副本、`.git` 和 `vendor`。先使用文本搜索定位，再使用 Go 标准库 `go/parser`、`go/ast` 识别 `regexp` 导入及直接函数调用，记录文件、行号、所属函数或包级变量。扫描结果如下：

| 项目 | 数量 | 解释 |
|---|---:|---|
| Go 文件 | 1,081 | 当前目录文件数，包含测试与示例 |
| 导入并调用 `regexp` 的文件 | 74 | 本次命中均为非 `_test.go` 文件 |
| `regexp.MustCompile` 调用位置 | 524 | 包含固定和动态拼接的表达式 |
| `regexp.Compile` 调用位置 | 5 | 包含运行时传入的正则 |
| `regexp.MatchString` 直接调用位置 | 14 | 不包括已编译对象的同名方法 |
| 上述三类合计 | 543 | 是代码位置数，不是规则去重数或运行次数 |
| `regexp.QuoteMeta` 辅助调用位置 | 75 | 单独统计，不重复计入 543 |

按本方案建议的处理方式，将 543 处划分为三个集合：

| 标记 | 建议处理 | 数量 |
|---|---|---:|
| A | SQL 结构识别、SQL 表达式分析或 SQL 文本改写，迁移至解析器、AST 遍历及相关计划处理 | 511 |
| B | 保留运行时值匹配、格式校验或摘要占位符处理；可另行合并重复实现 | 25 |
| C | 配套调整，改为结构化账户、错误信息或专用 WKT 解析；不直接改成 sqlparser 调用 | 7 |
| 合计 | 建议归类，尚未修改或删除任何调用 | 543 |

附录 A 列出全部 74 个文件和逐函数位置；混合用途文件按具体调用分别标注 A/B/C。`QuoteMeta` 的清理取决于所属功能是否仍需构造正则，不能单独按数量删除。

扫描中有 10 个 `cmd` 示例文件存在 Go 语法错误，无法形成完整 Go AST。对这些文件补充全文检索，未发现 `regexp`、`regex`、`pcre` 或 `re2` 文本命中。该情况不影响本次 74 个文件的定位，但本次扫描不构成全仓构建验证。

## 3. 已确认的解析器限制

### 3.1 解析成功不代表参数完整

当前 `DDL` 主要包含 `Action`、`Table`、`NewName`、`TableSpec` 和分区等字段，尚无通用的 ALTER 操作列表。部分 ALTER 语法读取操作开头后调用 `force_eof`，没有将后续内容保存为 AST。参见 [DDL 定义](../../server/innodb/sqlparser/ast.go#L1580) 和 [ALTER 语法](../../server/innodb/sqlparser/sql.y#L1181)。

当前 `Show` 有类型、表名、作用域和过滤字段，但多种 SHOW 语法只设置 `Type`，并提前停止解析。参见 [Show 定义](../../server/innodb/sqlparser/ast.go#L2334) 和 [SHOW 语法](../../server/innodb/sqlparser/sql.y#L1338)。

`REPAIR`、`OPTIMIZE` 返回空的 `OtherAdmin`；`ANALYZE TABLE` 被表示为 `Action=alter`，无法仅凭该 AST 区分管理命令的原始语义。参见 [管理语句语法](../../server/innodb/sqlparser/sql.y#L1332) 和 [OtherAdmin 定义](../../server/innodb/sqlparser/ast.go#L2479)。

### 3.2 宽松 DDL 与提前结束解析需要分别处理

[Parse](../../server/innodb/sqlparser/ast.go#L46) 在部分 DDL 解析失败时，会返回已经识别的部分 DDL 而不返回错误。探针中 `CREATE TABLE app.t2 LIKE app.t` 就出现此行为，源表未进入 AST。

[ParseStrictDDL](../../server/innodb/sqlparser/ast.go#L963) 可以拒绝部分 DDL 容错结果，但仍受语法中的 [force_eof](../../server/innodb/sqlparser/sql.y#L3169) 影响。本次探针中以下语句的 `Parse` 与 `ParseStrictDDL` 均成功：

```sql
ALTER TABLE t MODIFY COLUMN c INT totally_invalid_tail;
SHOW CREATE TABLE app.t totally_invalid_tail;
REPAIR TABLE app.t totally_invalid_tail;
```

因此，新增执行入口需要明确“完整消费单条语句”的契约。仅将调用替换为 `ParseStrictDDL`，不能解决提前停止解析的问题。

`ParseStrictDDL` 也不是所有 SQL 的通用严格入口。当前 CTE 和窗口语法在 `Parse` 中还有兼容解析路径；探针中的对应 SQL 在 `Parse` 成功，在 `ParseStrictDDL` 报错。严格入口设计必须保留这些已支持能力。

### 3.3 部分能力已有 AST，应优先复用

探针已确认 `SELECT ... BETWEEN ...` 和相关 `EXISTS` 子查询能生成结构化表达式，简单 CTE 能形成 `With` 节点，简单窗口表达式能保留 `OVER` 信息。因此，相关正则整改有一部分工作是调整计划与执行代码使用现有 AST，不需要重新发明一套 SQL 解析器。

### 3.4 需要同时调整解析入口与前置处理

[执行入口](../../server/innodb/engine/executor.go#L2929) 在通用 `sqlparser.Parse` 之前已经进行管理命令等兼容处理，通用解析在 [后续位置](../../server/innodb/engine/executor.go#L3289)。此外，网络层、分发层、权限检查和锁处理也存在正则分类。

迁移不能只修改一个 `switch` 或 `sqlparser` 包。已迁移语句要让权限检查、对象锁、计划和执行共享同一份解析结果，避免不同位置对同一 SQL 得出不同结论。

## 4. 按 SQL 功能梳理整改内容

下表列出涉及功能及主要位置。具体函数和全部调用位置见附录 A；这里的节点名为设计建议，不代表已经实现。

| 编号 | 功能及语句 | 当前主要位置 | 整改内容 |
|---|---|---|---|
| SQL-01 | SHOW、DESCRIBE、EXPLAIN | `show_parsing.go`、`show_*_compatibility.go`、`warning_compatibility.go`、`executor.go` | 结构化对象名、库名、FULL、作用域、LIKE、WHERE、LIMIT、日志位置和复制通道；DESCRIBE/EXPLAIN 保留目标或内部语句 |
| SQL-02 | ALTER TABLE | `alter_operation_compatibility.go`、`executor.go` | 按原始顺序保存操作列表；列、索引、主键、外键、CHECK、默认值、重命名、位置、可见性及执行选项均有明确字段 |
| SQL-03 | 其他表、索引、表空间和分区 DDL | `create_*_compatibility.go`、`index_ddl_compatibility.go`、`tablespace_compatibility.go`、`table_*_compatibility.go`、`partition_compatibility.go`、`temporary_table_compatibility.go`、`executor.go` | CREATE AS SELECT、CREATE LIKE、临时表、多对象 DROP/RENAME、表选项、生成列、分区维护和 IMPORT/DISCARD TABLESPACE 进入 AST |
| SQL-04 | 管理命令及复制控制 | `admin_compatibility.go` | FLUSH、KILL、LOCK/UNLOCK TABLES、ANALYZE/CHECK/REPAIR/OPTIMIZE、直方图维护、复制源配置、通道、RESET 和日志清理采用专用命令及选项 |
| SQL-05 | HANDLER、SQL 级 PREPARE、组件管理 | `handler_compatibility.go`、`sql_prepared_statement_compatibility.go`、`component_compatibility.go` | 结构化 OPEN/READ/CLOSE、PREPARE/EXECUTE/DEALLOCATE、INSTALL/UNINSTALL COMPONENT；与协议级预编译行为分别验证 |
| SQL-06 | CREATE/ALTER/DROP USER、ROLE、GRANT/REVOKE、SET PASSWORD | `account_compatibility.go`、`enhanced_message_handler.go`、`decoupled_handler.go` | 结构化账户、角色、权限范围、列权限、认证插件、密码策略、TLS 和资源限制；自助修改密码判断使用同一解析结果 |
| SQL-07 | 系统变量、用户变量及 SET | `system_variable_engine.go`、`system_variable_analyzer.go`、`session_expression_compatibility.go`、`persisted_variables_compatibility.go`、`executor.go` | 统一变量作用域、赋值目标、表达式、SET NAMES/COLLATE、SET PERSIST/RESET PERSIST；移除多层重复正则识别 |
| SQL-08 | 视图及视图依赖、安全属性 | `executor.go` | 保存视图 SELECT、列名列表、OR REPLACE、ALGORITHM、DEFINER、SQL SECURITY、CHECK OPTION；使用 AST 分析可更新性、依赖及替换表源 |
| SQL-09 | 存储过程、函数、触发器和事件的定义、修改、删除、调用 | `stored_object_compatibility.go`、`sql_event_runtime.go`、`trigger_runtime.go` | 补充对象名、参数、返回类型、属性、触发时机、NEW/OLD 引用、执行顺序及事件计划等结构化节点 |
| SQL-10 | 存储程序体与控制流 | `stored_object_compatibility.go`、`routine_control.go` | BEGIN/END、DECLARE、SET、IF/CASE、循环、标签、游标、HANDLER、SIGNAL/RESIGNAL、RETURN、CALL 等采用语句和表达式节点 |
| SQL-11 | INFORMATION_SCHEMA、PERFORMANCE_SCHEMA、mysql 系统表查询与更新 | `account_metadata_compatibility.go`、`information_schema_*`、`innodb_*_compatibility.go`、`mysql_system_tables_compatibility.go`、`performance_schema_*`、`optimizer_cost_compatibility.go`、`executor.go` | 从 Select/Insert/Update/Delete AST 获取表、投影、条件与更新内容；保留系统表专用数据提供方式和权限规则 |
| SQL-12 | SELECT 改写、相关子查询、CTE、窗口、全文及空间函数语法 | `correlated_*`、`cte_compatibility.go`、`window*_compatibility.go`、`fulltext_spatial_compatibility.go`、`date_extract_compatibility.go`、`json_value_compatibility.go`、`executor.go` | 使用 AST 遍历和计划转换；对已有 AST 优先调整消费代码，对缺少的 JSON_VALUE、EXTRACT 等细节补语法和字段 |
| SQL-13 | 条件分析、分区路由与裁剪、索引选择、CHECK 和生成列依赖 | `select_executor.go`、`partition_routing.go`、`partitioned_btree_manager.go`、`optimizer_manager.go`、`check_runtime.go`、`information_schema_catalog_compatibility.go` | 使用比较、范围、IN、函数、算术和列引用节点；依赖分析使用有作用域的列引用，不匹配整段 SQL 字符串 |
| SQL-14 | SQL 对象提取、权限和锁、监控中的表引用 | `ddl_lock_coordinator.go`、`performance_schema_io_compatibility.go`、`observability/metrics/runtime.go`、`executor.go` | 共享对象引用收集器，识别库、表、别名、子查询、读写类型及锁选项；与执行使用的 AST 一致 |

### 4.1 ALTER 要求

建议引入 `AlterTable` 与 `AlterOperation` 列表。已有引擎侧 `AlterOperation` 可以作为执行适配参考，语法层节点不依赖引擎包，避免循环依赖。

必须保留复合 ALTER 的顺序和原子性。例如 `ADD COLUMN ... , ADD INDEX ...` 中索引可以依赖本语句新增列；如果后一个操作失败，前一个操作的元数据和数据变更应遵循现有回滚语义。`FIRST/AFTER`、表达式中的逗号、列类型括号、外键动作和 CHECK 的嵌套表达式均不能靠文本分割处理。

### 4.2 SHOW 要求

优先补齐 TABLES/COLUMNS/INDEX/CREATE、VARIABLES/STATUS、TABLE STATUS、TRIGGERS/EVENTS、过程和函数状态、WARNINGS/ERRORS、BINLOG/RELAYLOG EVENTS 和复制状态。补充同义写法、库限定名及 FULL 变体。

目前 `show_parsing.go` 已有从 AST 提取 LIKE/WHERE 的函数，但也有从原始 SQL 正则取值，甚至拼接 `SELECT 1 ... WHERE ...` 再解析的实现。迁移后应直接复用原始 SHOW 的过滤 AST。

### 4.3 管理命令要求

管理命令不能统一只返回一个空节点。维护命令需要保留操作、多个目标表及 QUICK/EXTENDED 等选项；复制命令需要保留通道、线程和配置项；KILL 需要区分 QUERY/CONNECTION；锁命令需要保留多表、别名和锁类型。

解析器负责语法与参数，现有运行时继续负责会话控制、复制操作、锁、授权和执行结果。迁移不改变这些功能的授权边界。

### 4.4 存储对象与过程体要求

存储对象应分别保存原始定义文本和解析结果。原始文本用于 SHOW CREATE 与诊断；执行、依赖分析、事件计划和触发器元数据读取使用结构化结果。

过程体必须处理嵌套块、字符串和注释中的分号，以及 IF、CASE、循环与 HANDLER 的嵌套。复用普通 SQL 的表达式和 DML 节点，过程控制流使用专用节点。参数与局部变量需要作用域，不能通过正则直接替换同名文本。

触发器中的 NEW/OLD 引用应作为结构化引用求值。事件的 EVERY/AT、STARTS/ENDS、时区和状态要与调度器现有行为保持一致。客户端 `DELIMITER` 属于客户端脚本约定，不应直接当作服务端 SQL 语句加入过程体执行器。

### 4.5 查询改写与依赖分析要求

表源、列名、别名、CTE 名和局部变量必须区分作用域。重命名字段或表引用时只修改相应节点，避免修改字符串常量、注释、其他表的同名字段和被内层查询遮蔽的别名。

元数据查询的 WHERE 应按表达式树求值，尤其要保留 AND/OR/NOT、括号、IN、IS NULL 和 SQL 三值逻辑。分区裁剪与索引选择只能在证明安全的条件下排除数据；不能识别的表达式保守放弃裁剪或优化，不得缩小结果集。

## 5. 保留与配套调整清单

### 5.1 B 类 保留的 25 处

| 用途 | 文件与数量 | 处理原则 |
|---|---|---|
| 主机模式匹配 | `auth/engine_access.go` 1；`account_compatibility.go` 1 | 保留既有主机匹配规则，后续可复用同一实现 |
| 权限名与数据库名校验 | `common/dynamic_privileges.go` 1；`manager/schema_manager.go` 1 | 校验已取得的值，不属于 SQL 语法解析 |
| SQL REGEXP/LIKE、JSON pattern、时间/UUID 等值处理 | `plan/expression.go` 10；`storage_integrated_dml_helper.go` 2；`select_join_executor.go` 1 | 保留值匹配和表达式求值；不因语法迁移改变 NULL、大小写、转义及错误行为 |
| SHOW 和元数据的 LIKE 值匹配 | `show_filter.go` 1；`system_variable_engine.go` 1；`innodb_metrics_compatibility.go` 1；`executor.go` 1 | 条件从 AST 读取后，仍需进行匹配；后续可合并重复实现 |
| 时间值格式识别 | `dml_result_compatibility.go` 2 | 保留值格式处理 |
| 摘要占位符转换 | `executor.go` 1；`observability/metrics/statement_digest.go` 1 | 当前是在 Normalize 之后处理内部占位符；可另行改为专用格式化，不作为 SQL 语法缺口 |

### 5.2 C 类 配套调整的 7 处

| 位置 | 数量 | 建议 |
|---|---:|---|
| `executor.go` 的 `checkViewDefinerPrivilege`、`applyViewExecutionSecurity` | 2 | DEFINER 使用结构化账户；旧元数据兼容读取使用统一账户值解析，避免再次拼装 SQL |
| `stored_object_compatibility.go` 的 `findStoredRoutineHandler` | 2 | 数字错误码条件与错误文本中的 SQLSTATE 改为类型化条件和错误字段；其中另一处 HANDLER 声明 SQLSTATE 文本解析归 A 类 |
| `replication/runtime.go` 的 `mysqlErrorNumberPattern` | 1 | 优先读取类型化错误码；对无法提供结构化信息的外部错误保留有说明的文本兼容处理 |
| `fulltext_spatial_compatibility.go` 的 `spatialBounds`、`spatialPoint` | 2 | 属于 WKT 值解析，应由空间数据解析负责；不纳入 sqlparser 语句解析替换验收 |

## 6. 实施顺序与每批交付要求

各批状态均为“待实施”。不以删除正则数量作为完成标准，也不在缺少行为验证前给出工期承诺。

| 批次 | 范围 | 主要依赖 | 完成条件 |
|---|---|---|---|
| R0 | 解析契约、语法生成、共用 AST 工具及测试基准 | 无 | 明确完整解析规则、未支持与非法语法的区别；确定节点设计与对象收集方式；固定语法生成工具版本和命令 |
| R1 | SQL-01 SHOW/DESCRIBE/EXPLAIN | R0 | 参数进入 AST，结果列与过滤语义保持一致；删除本批对应的原始 SQL 提取正则 |
| R2 | SQL-02/03 表、索引、约束、表空间与分区 DDL | R0；锁与权限适配同时完成 | 复合操作顺序、原子性、数据及元数据持久化正确；生成列和约束引用完整 |
| R3 | SQL-04/05 管理命令、复制、HANDLER、SQL PREPARE、组件 | R0；对应运行时适配 | 各命令及参数可区分；授权、锁、会话控制和复制行为与原实现一致 |
| R4 | SQL-06/07 账户、权限和变量 | R0；网络层与分发层共同调整 | 密码过期、自助改密、权限与作用域判断使用一致 AST；敏感参数不进入普通日志 |
| R5 | SQL-08 视图及 SQL-09 存储对象定义 | R0、相关 DDL/权限工具；含 R6 中最低可用过程体语法 | 定义与属性不丢失；持久化对象可重载；视图和触发器依赖及安全语义正确 |
| R6 | SQL-10 过程体执行及事件、触发器运行时 | 与 R5 共同设计，可分对象实施 | 嵌套控制流、作用域、OUT/INOUT、异常处理、NEW/OLD 及调度行为有运行验证 |
| R7 | SQL-11/12/13 元数据查询、查询改写、条件和计划处理 | R0；复用已有 SELECT/CTE/窗口 AST | 结果集、NULL、相关引用与裁剪安全性正确；移除同语法的重复文本识别 |
| R8 | SQL-14 跨模块对象收集、C 类必要调整及最终检查 | 各批迁移时逐步实施，最终统一复查 | 权限和锁在每批切换前已完成；剩余正则有逐项用途说明；无未说明的 SQL 结构正则 |

R5/R6 不是严格的单向依赖：定义节点与过程体节点需要一起设计，允许按“函数简单 RETURN → 过程复合块 → 触发器 → 事件”的对象范围分别交付。不能宣称完整迁移存储定义，却仍用旧正则执行整个过程体。

每批都需要记录迁移语法、涉及函数、AST 字段、执行适配、保留例外及新鲜验证结果。下一批开始前更新附录清单，避免重复实现同类语法。

## 7. 解析器与执行代码的设计约束

1. **优先扩展项目内解析器。** 本方案不引入另一套 SQL 解析依赖。先复用已有 Tokenizer、表达式节点、类型节点、With 和窗口节点；确需其他方案时另行评估兼容成本。
2. **修改语法源后生成代码。** `ast.go` 已声明 `go:generate goyacc -o sql.go ./sql.y`。`sql.y`、AST、词法与生成后的 `sql.go` 必须一起保持一致，不手工维护生成文件。工具版本需在 R0 固定。
3. **明确完整解析。** 对本批已支持的语句，完整消费目标单条语句，允许规定的尾分号和注释，拒绝未知尾部及未授权的第二条语句。多语句能力继续遵守现有会话配置。
4. **按已完成的语法族切换。** 尚未完成的语法族继续走原实现；已支持语法的真实错误不能静默进入旧正则执行。过渡适配位于解析边界，记录已支持范围，不向更多执行函数增加新的正则后备路径。
5. **单次解析，共享结果。** 网络协议、分发、授权、资源锁、计划、执行与监控尽量复用一个解析结果。对象收集必须发生在需要权限和加锁的位置之前。
6. **语法、授权和执行分开。** AST 不直接操作元数据、账户、会话或复制系统；现有执行实现接受结构化参数，避免大范围重写存储引擎。
7. **保留原始文本与语义信息。** 对持久化定义、SHOW CREATE、摘要和错误定位保留原始 SQL 或来源位置；AST 格式化结果不要求逐字等于输入，但不得丢失执行需要的属性。
8. **兼容当前持久化数据。** 已有视图、过程、函数、触发器、事件及表定义要能读取和重新解析；如新增缓存或结构化存储格式，需要版本标记及重建方案，不直接覆盖旧定义。
9. **参数只在正确节点解释。** 字符串、反引号标识符、注释、占位符和 SQL 模式按现有服务端规则处理；不通过整段转小写或替换关键字实现解析。
10. **重构不得扩大功能声明。** 只形成 AST 不代表命令可执行；只通过解析器测试不代表 DDL 持久化、权限或 JDBC 行为已验证。

## 8. 验收用例与检查要求

### 8.1 各批通用验收

| 维度 | 必须验证的行为 |
|---|---|
| 完整性 | 语句关键参数和选项进入 AST；未知尾部、缺少参数、非法组合明确报错 |
| 词法边界 | 大小写、空白、多行、注释、反引号、库限定名、转义引号、字符串内逗号/分号/关键字 |
| 表达式 | 嵌套括号、函数、子查询、AND/OR/NOT、IN、BETWEEN、IS NULL、NULL 三值逻辑 |
| 参数与协议 | 文本协议、协议预编译参数、SQL PREPARE，以及当前允许的多语句设置 |
| 权限 | 普通用户、管理员、跨库对象、角色、DEFINER/INVOKER、密码过期和自助修改密码 |
| 锁和并发 | 对象读写集合、元数据锁、显式表锁、全局读锁、超时和释放行为 |
| 数据与持久化 | DDL 原子性、索引/约束和行数据一致、重启后定义与属性恢复 |
| 改写正确性 | 不替换字符串或注释；作用域和别名正确；结果集与独立预期一致 |
| 运行时语义 | B 类匹配行为不变；错误码和 SQLSTATE 保持可识别；C 类调整有独立测试 |

新测试应针对公开解析或执行行为，不能只断言“正则已经移除”或重复实现同一规则作为预期结果。SQL 语义以需求、已有明确契约和独立预期为依据；历史行为如本身错误，应记录差异并修正，不能机械保留。

### 8.2 可复用的现有验证位置

| 功能 | 现有测试文件示例 |
|---|---|
| Parser | `server/innodb/sqlparser/parse_test.go`、`foreign_key_parse_test.go`、`check_constraint_parse_test.go` |
| SHOW | `server/innodb/engine/show_parsing_test.go`、`show_filter_test.go`、`executor_show_routing_test.go`、`show_*_compatibility_test.go` |
| ALTER | `server/innodb/engine/alter_operation_compatibility_test.go`、`alter_index_timing_compatibility_test.go` |
| 管理和账户 | `server/innodb/engine/admin_compatibility_test.go`、`account_compatibility_test.go` |
| 存储对象、视图、触发器、事件 | `server/innodb/engine/stored_object_compatibility_test.go`、`event_scheduler_test.go` |
| 查询与分区 | `server/innodb/engine/cte_compatibility_test.go`、`cte_plan_execution_test.go`、`partition_compatibility_test.go`、`partitioned_btree_manager_test.go`、`fulltext_spatial_compatibility_test.go` |

每批先执行受影响测试，再执行相关包检查。账户与变量批次应覆盖 `server/auth`、`server/net`、`server/dispatcher`；条件与查询批次应覆盖 `server/innodb/plan`、`manager` 和 `engine`；监控与复制批次应覆盖其对应包。引擎测试包含文件、端口和运行时状态，不在共享环境盲目并行。

最终还需进行真实服务上的文本协议和 JDBC 回归，并验证必要的重启持久化行为。未经执行的检查不得记为通过。

### 8.3 静态完成检查

```powershell
# 候选位置复查；命中后仍需按用途判断
rg -n -g '*.go' 'regexp\.(MustCompile|Compile|MatchString)' server

# 正则以外的手写 SQL 识别与文本改写也要检查
rg -n -g '*.go' 'splitTopLevel|RawQuery|parseWithRegex|rewrite.*Query' server

# 解析器测试
go test ./server/innodb/sqlparser -count=1
```

完成标准是附录 A 的 A 类位置已经迁移或有经代码证据确认的重新分类说明；B/C 类用途、位置和行为可解释。SQL 结构识别即使换成 `strings.HasPrefix` 或手写字符串切分，也不视为已完成 AST 整改。SQL-14 的权限与锁识别必须与每批执行迁移一起验证，不能留到最后才修补。

## 9. 本次验证记录

| 检查 | 结果 | 能证明的范围 |
|---|---|---|
| 全目录文本检索与 Go AST 扫描 | 得到 74 文件、543 处主要调用、75 处 QuoteMeta | 当前源码位置和建议归类；完整清单见附录 A |
| 49 条 SQL 直接调用 `Parse` 与 `ParseStrictDDL` | `Parse` 30 条返回无错误、19 条报错；部分无错误结果缺字段或忽略非法尾部 | 当前解析行为；逐条见附录 B，不作为兼容率 |
| `go test ./server/innodb/sqlparser -count=1` | 通过，退出码 0 | 当前 parser 包已有测试通过，无法覆盖全部整改目标 |
| 引擎全量、运行中服务、JDBC、重启验证 | 本次未执行 | 本任务为梳理和文档，不能据此判断运行时正确性或整改完成 |

本机执行使用 `C:/Users/zx187/go/go1.27.2/bin/go.exe`，因当前 PowerShell 的 PATH 中没有 `go`。扫描与探针程序放在系统临时目录，不属于项目生产代码。

## 附录 A 全部正则编译与直接匹配位置

以下清单由 Go AST 扫描结果生成，按文件、所属函数或包级变量列出。A/B/C 含义见第 2 节。数字是当前源码行号，后续修改时需要重新扫描。

### A.01 `server/auth/engine_access.go`

主要调用 1 处：A 类 0，B 类 1，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func matchHost` | MatchString | B | 1 | [L872](../../server/auth/engine_access.go#L872) |

### A.02 `server/common/dynamic_privileges.go`

主要调用 1 处：A 类 0，B 类 1，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var dynamicPrivilegeNamePattern` | MustCompile | B | 1 | [L63](../../server/common/dynamic_privileges.go#L63) |

### A.03 `server/dispatcher/enhanced_message_handler.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func isSelfServiceSetPassword` | MustCompile | A | 1 | [L1297](../../server/dispatcher/enhanced_message_handler.go#L1297) |

### A.04 `server/dispatcher/system_variable_engine.go`

主要调用 9 处：A 类 8，B 类 1，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var directSystemVariableSelectPattern` | MustCompile | A | 1 | [L441](../../server/dispatcher/system_variable_engine.go#L441) |
| `var directSystemVariableTokenPattern` | MustCompile | A | 1 | [L442](../../server/dispatcher/system_variable_engine.go#L442) |
| `var informationSchemaTablesQueryPattern` | MustCompile | A | 1 | [L553](../../server/dispatcher/system_variable_engine.go#L553) |
| `var informationSchemaTableFilterPattern` | MustCompile | A | 1 | [L554](../../server/dispatcher/system_variable_engine.go#L554) |
| `func isInformationSchemaMetadataQuery` | MustCompile | A | 1 | [L621](../../server/dispatcher/system_variable_engine.go#L621) |
| `func isMySQLMetadataQuery` | MustCompile | A | 1 | [L633](../../server/dispatcher/system_variable_engine.go#L633) |
| `func matchesMetadataPattern` | MatchString | B | 1 | [L673](../../server/dispatcher/system_variable_engine.go#L673) |
| `func parseSetNamesCollation` | MustCompile | A | 1 | [L1290](../../server/dispatcher/system_variable_engine.go#L1290) |
| `func evaluateSetValue` | MustCompile | A | 1 | [L1657](../../server/dispatcher/system_variable_engine.go#L1657) |

### A.05 `server/innodb/engine/account_compatibility.go`

主要调用 37 处：A 类 36，B 类 1，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func parsePasswordHistoryOption` | MustCompile | A | 1 | [L201](../../server/innodb/engine/account_compatibility.go#L201) |
| `func parsePasswordReuseIntervalOption` | MustCompile | A | 1 | [L216](../../server/innodb/engine/account_compatibility.go#L216) |
| `func parsePasswordRequireCurrentOption` | MustCompile | A | 1 | [L231](../../server/innodb/engine/account_compatibility.go#L231) |
| `func parsePasswordLifetimeOption` | MustCompile | A | 1 | [L243](../../server/innodb/engine/account_compatibility.go#L243) |
| `func parseFailedLoginPolicyOptions` | MustCompile | A | 2 | [L255](../../server/innodb/engine/account_compatibility.go#L255)、[L263](../../server/innodb/engine/account_compatibility.go#L263) |
| `func parseAccountResourceOptions` | MustCompile | A | 1 | [L298](../../server/innodb/engine/account_compatibility.go#L298) |
| `func parseTLSAccountOptions` | MustCompile | A | 2 | [L316](../../server/innodb/engine/account_compatibility.go#L316)、[L334](../../server/innodb/engine/account_compatibility.go#L334) |
| `func parseUserAttributeOption` | MustCompile | A | 2 | [L349](../../server/innodb/engine/account_compatibility.go#L349)、[L350](../../server/innodb/engine/account_compatibility.go#L350) |
| `func parseUserCommentOption` | MustCompile | A | 1 | [L368](../../server/innodb/engine/account_compatibility.go#L368) |
| `func parsePasswordReplace` | MustCompile | A | 1 | [L451](../../server/innodb/engine/account_compatibility.go#L451) |
| `func accountTargetFromQuery` | MustCompile | A | 1 | [L519](../../server/innodb/engine/account_compatibility.go#L519) |
| `func accountTargetsFromQuery` | MustCompile | A | 1 | [L527](../../server/innodb/engine/account_compatibility.go#L527) |
| `func accountGranteeTargetsFromQuery` | MustCompile | A | 1 | [L538](../../server/innodb/engine/account_compatibility.go#L538) |
| `func roleReferences` | MustCompile | A | 1 | [L546](../../server/innodb/engine/account_compatibility.go#L546) |
| `func accountHostMatches` | MatchString | B | 1 | [L582](../../server/innodb/engine/account_compatibility.go#L582) |
| `func executeAccountStatement` | MustCompile | A | 15 | [L1436](../../server/innodb/engine/account_compatibility.go#L1436)、[L1572](../../server/innodb/engine/account_compatibility.go#L1572)、[L1594](../../server/innodb/engine/account_compatibility.go#L1594)、[L1643](../../server/innodb/engine/account_compatibility.go#L1643)、[L1715](../../server/innodb/engine/account_compatibility.go#L1715)、[L1753](../../server/innodb/engine/account_compatibility.go#L1753)、[L1757](../../server/innodb/engine/account_compatibility.go#L1757)、[L1892](../../server/innodb/engine/account_compatibility.go#L1892)、[L1918](../../server/innodb/engine/account_compatibility.go#L1918)、[L1974](../../server/innodb/engine/account_compatibility.go#L1974)、[L2249](../../server/innodb/engine/account_compatibility.go#L2249)、[L2255](../../server/innodb/engine/account_compatibility.go#L2255)、[L2258](../../server/innodb/engine/account_compatibility.go#L2258)、[L2301](../../server/innodb/engine/account_compatibility.go#L2301)、[L2362](../../server/innodb/engine/account_compatibility.go#L2362) |
| `func parseGrantPrivileges` | MustCompile | A | 4 | [L2515](../../server/innodb/engine/account_compatibility.go#L2515)、[L2518](../../server/innodb/engine/account_compatibility.go#L2518)、[L2520](../../server/innodb/engine/account_compatibility.go#L2520)、[L2522](../../server/innodb/engine/account_compatibility.go#L2522) |

### A.06 `server/innodb/engine/account_metadata_compatibility.go`

主要调用 11 处：A 类 11，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var informationSchemaPrivilegeFilterPattern` | MustCompile | A | 1 | [L322](../../server/innodb/engine/account_metadata_compatibility.go#L322) |
| `var informationSchemaPrivilegeNullFilterPattern` | MustCompile | A | 1 | [L323](../../server/innodb/engine/account_metadata_compatibility.go#L323) |
| `var informationSchemaPrivilegeInFilterPattern` | MustCompile | A | 1 | [L324](../../server/innodb/engine/account_metadata_compatibility.go#L324) |
| `func executeMySQLProcsPrivSelect` | MustCompile | A | 1 | [L412](../../server/innodb/engine/account_metadata_compatibility.go#L412) |
| `func executeMySQLProxiesPrivSelect` | MustCompile | A | 1 | [L454](../../server/innodb/engine/account_metadata_compatibility.go#L454) |
| `var mysqlPrivilegeFilterPattern` | MustCompile | A | 1 | [L494](../../server/innodb/engine/account_metadata_compatibility.go#L494) |
| `var mysqlPrivilegeInFilterPattern` | MustCompile | A | 1 | [L495](../../server/innodb/engine/account_metadata_compatibility.go#L495) |
| `var mysqlPrivilegeNullFilterPattern` | MustCompile | A | 1 | [L496](../../server/innodb/engine/account_metadata_compatibility.go#L496) |
| `var mysqlRoleTableFilterPattern` | MustCompile | A | 1 | [L842](../../server/innodb/engine/account_metadata_compatibility.go#L842) |
| `var mysqlRoleTableInFilterPattern` | MustCompile | A | 1 | [L843](../../server/innodb/engine/account_metadata_compatibility.go#L843) |
| `var mysqlRoleTableNullFilterPattern` | MustCompile | A | 1 | [L844](../../server/innodb/engine/account_metadata_compatibility.go#L844) |

### A.07 `server/innodb/engine/admin_compatibility.go`

主要调用 19 处：A 类 19，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var flushTablesPattern` | MustCompile | A | 1 | [L430](../../server/innodb/engine/admin_compatibility.go#L430) |
| `var flushOptimizerCostsPattern` | MustCompile | A | 1 | [L431](../../server/innodb/engine/admin_compatibility.go#L431) |
| `var lockTableEntryPattern` | MustCompile | A | 1 | [L562](../../server/innodb/engine/admin_compatibility.go#L562) |
| `var killConnectionPattern` | MustCompile | A | 1 | [L611](../../server/innodb/engine/admin_compatibility.go#L611) |
| `func executeSessionKill` | MustCompile | A | 1 | [L620](../../server/innodb/engine/admin_compatibility.go#L620) |
| `var resetReplicaPattern` | MustCompile | A | 1 | [L843](../../server/innodb/engine/admin_compatibility.go#L843) |
| `var replicationChannelPattern` | MustCompile | A | 1 | [L845](../../server/innodb/engine/admin_compatibility.go#L845) |
| `var replicationFilterOptionPattern` | MustCompile | A | 1 | [L882](../../server/innodb/engine/admin_compatibility.go#L882) |
| `var replicationChannelSuffixPattern` | MustCompile | A | 1 | [L884](../../server/innodb/engine/admin_compatibility.go#L884) |
| `var purgeBinaryLogsPattern` | MustCompile | A | 1 | [L1029](../../server/innodb/engine/admin_compatibility.go#L1029) |
| `var purgeBinaryLogsBeforePattern` | MustCompile | A | 1 | [L1030](../../server/innodb/engine/admin_compatibility.go#L1030) |
| `var resetBinaryLogsAndGTIDsPattern` | MustCompile | A | 1 | [L1031](../../server/innodb/engine/admin_compatibility.go#L1031) |
| `var replicationSourceOptionPattern` | MustCompile | A | 1 | [L1042](../../server/innodb/engine/admin_compatibility.go#L1042) |
| `var tableMaintenancePattern` | MustCompile | A | 1 | [L1298](../../server/innodb/engine/admin_compatibility.go#L1298) |
| `var tableMaintenanceTargetPattern` | MustCompile | A | 1 | [L1299](../../server/innodb/engine/admin_compatibility.go#L1299) |
| `var tableMaintenanceUpgradeSuffixPattern` | MustCompile | A | 1 | [L1300](../../server/innodb/engine/admin_compatibility.go#L1300) |
| `var histogramMaintenancePattern` | MustCompile | A | 1 | [L1301](../../server/innodb/engine/admin_compatibility.go#L1301) |
| `func parseHistogramMaintenance` | MustCompile | A | 1 | [L1425](../../server/innodb/engine/admin_compatibility.go#L1425) |
| `func referencedTableForLockCheck` | MustCompile | A | 1 | [L1934](../../server/innodb/engine/admin_compatibility.go#L1934) |

### A.08 `server/innodb/engine/alter_operation_compatibility.go`

主要调用 23 处：A 类 23，B 类 0，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func parseAlterOperations` | MustCompile | A | 6 | [L45](../../server/innodb/engine/alter_operation_compatibility.go#L45)、[L56](../../server/innodb/engine/alter_operation_compatibility.go#L56)、[L65](../../server/innodb/engine/alter_operation_compatibility.go#L65)、[L66](../../server/innodb/engine/alter_operation_compatibility.go#L66)、[L67](../../server/innodb/engine/alter_operation_compatibility.go#L67)、[L68](../../server/innodb/engine/alter_operation_compatibility.go#L68) |
| `func validateAlterExecutionOptions` | MustCompile | A | 1 | [L88](../../server/innodb/engine/alter_operation_compatibility.go#L88) |
| `func parseSingleAlterOperation` | MustCompile | A | 9 | [L121](../../server/innodb/engine/alter_operation_compatibility.go#L121)、[L124](../../server/innodb/engine/alter_operation_compatibility.go#L124)、[L127](../../server/innodb/engine/alter_operation_compatibility.go#L127)、[L131](../../server/innodb/engine/alter_operation_compatibility.go#L131)、[L134](../../server/innodb/engine/alter_operation_compatibility.go#L134)、[L138](../../server/innodb/engine/alter_operation_compatibility.go#L138)、[L142](../../server/innodb/engine/alter_operation_compatibility.go#L142)、[L145](../../server/innodb/engine/alter_operation_compatibility.go#L145)、[L148](../../server/innodb/engine/alter_operation_compatibility.go#L148) |
| `func alterUsesInstantAlgorithm` | MustCompile | A | 1 | [L235](../../server/innodb/engine/alter_operation_compatibility.go#L235) |
| `func normalizeQualifiedAlterTableTarget` | MustCompile | A | 1 | [L297](../../server/innodb/engine/alter_operation_compatibility.go#L297) |
| `func parseColumnPosition` | MustCompile | A | 4 | [L745](../../server/innodb/engine/alter_operation_compatibility.go#L745)、[L746](../../server/innodb/engine/alter_operation_compatibility.go#L746)、[L749](../../server/innodb/engine/alter_operation_compatibility.go#L749)、[L750](../../server/innodb/engine/alter_operation_compatibility.go#L750) |
| `func checkExpressionReferencesColumn` | MustCompile | A | 1 | [L826](../../server/innodb/engine/alter_operation_compatibility.go#L826) |

### A.09 `server/innodb/engine/check_runtime.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var checkIdentifierPattern` | MustCompile | A | 1 | [L79](../../server/innodb/engine/check_runtime.go#L79) |

### A.10 `server/innodb/engine/component_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var componentStatementPattern` | MustCompile | A | 1 | [L58](../../server/innodb/engine/component_compatibility.go#L58) |
| `var componentURNPattern` | MustCompile | A | 1 | [L59](../../server/innodb/engine/component_compatibility.go#L59) |
| `func parseComponentStatement` | MustCompile | A | 1 | [L67](../../server/innodb/engine/component_compatibility.go#L67) |

### A.11 `server/innodb/engine/correlated_derived_compatibility.go`

主要调用 6 处：A 类 6，B 类 0，C 类 0。另有 QuoteMeta 3 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeCorrelatedDerivedOuterCompatibility` | MustCompile | A | 5 | [L28](../../server/innodb/engine/correlated_derived_compatibility.go#L28)、[L45](../../server/innodb/engine/correlated_derived_compatibility.go#L45)、[L55](../../server/innodb/engine/correlated_derived_compatibility.go#L55)、[L78](../../server/innodb/engine/correlated_derived_compatibility.go#L78)、[L129](../../server/innodb/engine/correlated_derived_compatibility.go#L129) |
| `func correlatedDerivedLimit` | MustCompile | A | 1 | [L259](../../server/innodb/engine/correlated_derived_compatibility.go#L259) |

### A.12 `server/innodb/engine/correlated_dml_compatibility.go`

主要调用 6 处：A 类 6，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func rewriteCorrelatedDMLScalarAssignment` | MustCompile | A | 1 | [L172](../../server/innodb/engine/correlated_dml_compatibility.go#L172) |
| `func correlatedDMLScalarSubqueryRanges` | MustCompile | A | 1 | [L269](../../server/innodb/engine/correlated_dml_compatibility.go#L269) |
| `func rewriteCorrelatedDMLMultiScalarAssignment` | MustCompile | A | 1 | [L304](../../server/innodb/engine/correlated_dml_compatibility.go#L304) |
| `func rewriteCorrelatedDMLPredicate` | MustCompile | A | 2 | [L425](../../server/innodb/engine/correlated_dml_compatibility.go#L425)、[L440](../../server/innodb/engine/correlated_dml_compatibility.go#L440) |
| `func parseCorrelatedDMLTarget` | MustCompile | A | 1 | [L564](../../server/innodb/engine/correlated_dml_compatibility.go#L564) |

### A.13 `server/innodb/engine/correlated_predicate_compatibility.go`

主要调用 5 处：A 类 5，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeCorrelatedPredicateSubqueryCompatibility` | MustCompile | A | 3 | [L51](../../server/innodb/engine/correlated_predicate_compatibility.go#L51)、[L67](../../server/innodb/engine/correlated_predicate_compatibility.go#L67)、[L221](../../server/innodb/engine/correlated_predicate_compatibility.go#L221) |
| `func splitCorrelatedConditionConnector` | MustCompile | A | 2 | [L274](../../server/innodb/engine/correlated_predicate_compatibility.go#L274)、[L281](../../server/innodb/engine/correlated_predicate_compatibility.go#L281) |

### A.14 `server/innodb/engine/correlated_predicate_multiple_compatibility.go`

主要调用 4 处：A 类 4，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeMultipleCorrelatedPredicateSubqueriesCompatibility` | MustCompile | A | 4 | [L50](../../server/innodb/engine/correlated_predicate_multiple_compatibility.go#L50)、[L72](../../server/innodb/engine/correlated_predicate_multiple_compatibility.go#L72)、[L73](../../server/innodb/engine/correlated_predicate_multiple_compatibility.go#L73)、[L209](../../server/innodb/engine/correlated_predicate_multiple_compatibility.go#L209) |

### A.15 `server/innodb/engine/correlated_scalar_compatibility.go`

主要调用 11 处：A 类 11，B 类 0，C 类 0。另有 QuoteMeta 6 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeCorrelatedScalarSubqueryCompatibility` | MustCompile | A | 1 | [L313](../../server/innodb/engine/correlated_scalar_compatibility.go#L313) |
| `func evaluateCorrelatedScalarPredicate` | MustCompile | A | 1 | [L507](../../server/innodb/engine/correlated_scalar_compatibility.go#L507) |
| `func correlatedClauseContainsScalarAlias` | MustCompile | A | 1 | [L590](../../server/innodb/engine/correlated_scalar_compatibility.go#L590) |
| `func sortCorrelatedRecords` | MustCompile | A | 3 | [L608](../../server/innodb/engine/correlated_scalar_compatibility.go#L608)、[L609](../../server/innodb/engine/correlated_scalar_compatibility.go#L609)、[L623](../../server/innodb/engine/correlated_scalar_compatibility.go#L623) |
| `func correlatedAggregateExpression` | MustCompile | A | 1 | [L901](../../server/innodb/engine/correlated_scalar_compatibility.go#L901) |
| `func parseCorrelatedOuterSource` | MustCompile | A | 2 | [L923](../../server/innodb/engine/correlated_scalar_compatibility.go#L923)、[L930](../../server/innodb/engine/correlated_scalar_compatibility.go#L930) |
| `func normalizeCorrelatedInnerAlias` | MustCompile | A | 2 | [L946](../../server/innodb/engine/correlated_scalar_compatibility.go#L946)、[L950](../../server/innodb/engine/correlated_scalar_compatibility.go#L950) |

### A.16 `server/innodb/engine/correlated_subquery_compatibility.go`

主要调用 9 处：A 类 9，B 类 0，C 类 0。另有 QuoteMeta 3 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeCorrelatedSubqueryCompatibility` | MustCompile | A | 5 | [L22](../../server/innodb/engine/correlated_subquery_compatibility.go#L22)、[L37](../../server/innodb/engine/correlated_subquery_compatibility.go#L37)、[L53](../../server/innodb/engine/correlated_subquery_compatibility.go#L53)、[L104](../../server/innodb/engine/correlated_subquery_compatibility.go#L104)、[L209](../../server/innodb/engine/correlated_subquery_compatibility.go#L209) |
| `func correlatedOuterSourceSQL` | MustCompile | A | 1 | [L298](../../server/innodb/engine/correlated_subquery_compatibility.go#L298) |
| `func correlatedOuterSourceAliases` | MustCompile | A | 3 | [L338](../../server/innodb/engine/correlated_subquery_compatibility.go#L338)、[L343](../../server/innodb/engine/correlated_subquery_compatibility.go#L343)、[L350](../../server/innodb/engine/correlated_subquery_compatibility.go#L350) |

### A.17 `server/innodb/engine/create_as_select_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var createTableAsSelectPattern` | MustCompile | A | 1 | [L14](../../server/innodb/engine/create_as_select_compatibility.go#L14) |
| `var createTableAsSelectExplicitPattern` | MustCompile | A | 1 | [L15](../../server/innodb/engine/create_as_select_compatibility.go#L15) |
| `var createTableAsSelectDefinitionPattern` | MustCompile | A | 1 | [L16](../../server/innodb/engine/create_as_select_compatibility.go#L16) |

### A.18 `server/innodb/engine/create_index_visibility_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var createTableInvisibleIndexPattern` | MustCompile | A | 1 | [L14](../../server/innodb/engine/create_index_visibility_compatibility.go#L14) |
| `func executeRawCreateTableIndexVisibility` | MustCompile | A | 2 | [L37](../../server/innodb/engine/create_index_visibility_compatibility.go#L37)、[L41](../../server/innodb/engine/create_index_visibility_compatibility.go#L41) |

### A.19 `server/innodb/engine/create_like_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var createTableLikePattern` | MustCompile | A | 1 | [L12](../../server/innodb/engine/create_like_compatibility.go#L12) |

### A.20 `server/innodb/engine/cte_compatibility.go`

主要调用 8 处：A 类 8，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func isRawSetCTECompatibilityQuery` | MustCompile | A | 1 | [L79](../../server/innodb/engine/cte_compatibility.go#L79) |
| `func executeCTECompatibility` | MustCompile | A | 2 | [L177](../../server/innodb/engine/cte_compatibility.go#L177)、[L178](../../server/innodb/engine/cte_compatibility.go#L178) |
| `func materializeRecursiveCTESingleColumnLegacy` | MustCompile | A | 2 | [L439](../../server/innodb/engine/cte_compatibility.go#L439)、[L440](../../server/innodb/engine/cte_compatibility.go#L440) |
| `func executeRecursiveCTEInsert` | MustCompile | A | 1 | [L1146](../../server/innodb/engine/cte_compatibility.go#L1146) |
| `func executeRecursiveCTEDML` | MustCompile | A | 1 | [L1202](../../server/innodb/engine/cte_compatibility.go#L1202) |
| `func parseCTECompatibilityQuery` | MustCompile | A | 1 | [L1876](../../server/innodb/engine/cte_compatibility.go#L1876) |

### A.21 `server/innodb/engine/date_extract_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var extractDateUnitPattern` | MustCompile | A | 1 | [L75](../../server/innodb/engine/date_extract_compatibility.go#L75) |

### A.22 `server/innodb/engine/ddl_lock_coordinator.go`

主要调用 5 处：A 类 5，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var statementTablePattern` | MustCompile | A | 1 | [L1203](../../server/innodb/engine/ddl_lock_coordinator.go#L1203) |
| `var alterTablePattern` | MustCompile | A | 1 | [L1204](../../server/innodb/engine/ddl_lock_coordinator.go#L1204) |
| `var alterLockPattern` | MustCompile | A | 1 | [L1205](../../server/innodb/engine/ddl_lock_coordinator.go#L1205) |
| `var statementTablesPattern` | MustCompile | A | 1 | [L1206](../../server/innodb/engine/ddl_lock_coordinator.go#L1206) |
| `var lockingReadPattern` | MustCompile | A | 1 | [L1207](../../server/innodb/engine/ddl_lock_coordinator.go#L1207) |

### A.23 `server/innodb/engine/dml_result_compatibility.go`

主要调用 2 处：A 类 0，B 类 2，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func zeroTemporalColumnValue` | MustCompile | B | 2 | [L176](../../server/innodb/engine/dml_result_compatibility.go#L176)、[L177](../../server/innodb/engine/dml_result_compatibility.go#L177) |

### A.24 `server/innodb/engine/executor.go`

主要调用 153 处：A 类 149，B 类 2，C 类 2。另有 QuoteMeta 18 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var jdbcTableCatIdentifierPattern` | MustCompile | A | 1 | [L39](../../server/innodb/engine/executor.go#L39) |
| `func executeRawAlterCompatibility` | MustCompile | A | 21 | [L3526](../../server/innodb/engine/executor.go#L3526)、[L3546](../../server/innodb/engine/executor.go#L3546)、[L3551](../../server/innodb/engine/executor.go#L3551)、[L3556](../../server/innodb/engine/executor.go#L3556)、[L3561](../../server/innodb/engine/executor.go#L3561)、[L3566](../../server/innodb/engine/executor.go#L3566)、[L3602](../../server/innodb/engine/executor.go#L3602)、[L3607](../../server/innodb/engine/executor.go#L3607)、[L3608](../../server/innodb/engine/executor.go#L3608)、[L3615](../../server/innodb/engine/executor.go#L3615)、[L3616](../../server/innodb/engine/executor.go#L3616)、[L3624](../../server/innodb/engine/executor.go#L3624)、[L3632](../../server/innodb/engine/executor.go#L3632)、[L3644](../../server/innodb/engine/executor.go#L3644)、[L3645](../../server/innodb/engine/executor.go#L3645)、[L3652](../../server/innodb/engine/executor.go#L3652)、[L3653](../../server/innodb/engine/executor.go#L3653)、[L3660](../../server/innodb/engine/executor.go#L3660)、[L3661](../../server/innodb/engine/executor.go#L3661)、[L3662](../../server/innodb/engine/executor.go#L3662)、[L3683](../../server/innodb/engine/executor.go#L3683) |
| `func persistAlterTableRowFormat` | MustCompile | A | 1 | [L3745](../../server/innodb/engine/executor.go#L3745) |
| `func executeCompoundAlterCompatibility` | MustCompile | A | 2 | [L3767](../../server/innodb/engine/executor.go#L3767)、[L3786](../../server/innodb/engine/executor.go#L3786) |
| `func alterPartsContainConstraint` | MustCompile | A | 1 | [L3816](../../server/innodb/engine/executor.go#L3816) |
| `func renameSQLIdentifier` | MustCompile | A | 1 | [L4716](../../server/innodb/engine/executor.go#L4716) |
| `func rewriteForeignKeyReferenceTable` | MustCompile | A | 1 | [L4837](../../server/innodb/engine/executor.go#L4837) |
| `func parseDropTableTargets` | MustCompile | A | 2 | [L5051](../../server/innodb/engine/executor.go#L5051)、[L5060](../../server/innodb/engine/executor.go#L5060) |
| `func parseDropViewTargets` | MustCompile | A | 1 | [L5087](../../server/innodb/engine/executor.go#L5087) |
| `func parseRenameTablePairs` | MustCompile | A | 2 | [L5200](../../server/innodb/engine/executor.go#L5200)、[L5208](../../server/innodb/engine/executor.go#L5208) |
| `func alterTableIndexDDL` | MustCompile | A | 5 | [L5672](../../server/innodb/engine/executor.go#L5672)、[L5673](../../server/innodb/engine/executor.go#L5673)、[L5674](../../server/innodb/engine/executor.go#L5674)、[L5675](../../server/innodb/engine/executor.go#L5675)、[L5676](../../server/innodb/engine/executor.go#L5676) |
| `func alterTableColumnDDL` | MustCompile | A | 4 | [L5855](../../server/innodb/engine/executor.go#L5855)、[L5910](../../server/innodb/engine/executor.go#L5910)、[L5911](../../server/innodb/engine/executor.go#L5911)、[L5955](../../server/innodb/engine/executor.go#L5955) |
| `func bindJoinOuterReferences` | MustCompile | A | 1 | [L6893](../../server/innodb/engine/executor.go#L6893) |
| `var directSystemVariableSelectPattern` | MustCompile | A | 1 | [L6956](../../server/innodb/engine/executor.go#L6956) |
| `var directSystemVariableTokenPattern` | MustCompile | A | 1 | [L6957](../../server/innodb/engine/executor.go#L6957) |
| `func rewriteSimpleInSubquery` | MustCompile | A | 1 | [L7620](../../server/innodb/engine/executor.go#L7620) |
| `func rewriteSimpleQuantifiedSubqueries` | MustCompile | A | 1 | [L7678](../../server/innodb/engine/executor.go#L7678) |
| `func executeUserVariableAssignment` | MustCompile | A | 3 | [L7987](../../server/innodb/engine/executor.go#L7987)、[L7999](../../server/innodb/engine/executor.go#L7999)、[L8007](../../server/innodb/engine/executor.go#L8007) |
| `var informationSchemaMetadataFilterPattern` | MustCompile | A | 1 | [L8335](../../server/innodb/engine/executor.go#L8335) |
| `var processlistFilterPattern` | MustCompile | A | 1 | [L8337](../../server/innodb/engine/executor.go#L8337) |
| `var processlistNullFilterPattern` | MustCompile | A | 1 | [L8338](../../server/innodb/engine/executor.go#L8338) |
| `var processlistInFilterPattern` | MustCompile | A | 1 | [L8339](../../server/innodb/engine/executor.go#L8339) |
| `var processlistFunctionFilterPattern` | MustCompile | A | 1 | [L8340](../../server/innodb/engine/executor.go#L8340) |
| `func checkPerformanceSchemaTableSelectPrivileges` | MustCompile | A | 1 | [L8883](../../server/innodb/engine/executor.go#L8883) |
| `func checkMySQLMetadataTableSelectPrivileges` | MustCompile | A | 1 | [L8922](../../server/innodb/engine/executor.go#L8922) |
| `func checkInformationSchemaProcessPrivileges` | MustCompile | A | 1 | [L8970](../../server/innodb/engine/executor.go#L8970) |
| `func performanceSchemaDigestText` | MustCompile | B | 1 | [L9149](../../server/innodb/engine/executor.go#L9149) |
| `func isTemporaryTableDDL` | MustCompile | A | 2 | [L10535](../../server/innodb/engine/executor.go#L10535)、[L10553](../../server/innodb/engine/executor.go#L10553) |
| `func isTemporaryTableDML` | MustCompile | A | 1 | [L10582](../../server/innodb/engine/executor.go#L10582) |
| `var performanceSchemaSetupUpdatePattern` | MustCompile | A | 1 | [L11415](../../server/innodb/engine/executor.go#L11415) |
| `var performanceSchemaThreadsUpdatePattern` | MustCompile | A | 1 | [L11417](../../server/innodb/engine/executor.go#L11417) |
| `var performanceSchemaSetupActorsInsertPattern` | MustCompile | A | 1 | [L11419](../../server/innodb/engine/executor.go#L11419) |
| `var performanceSchemaSetupActorsDeletePattern` | MustCompile | A | 1 | [L11420](../../server/innodb/engine/executor.go#L11420) |
| `var performanceSchemaSetupObjectsInsertPattern` | MustCompile | A | 1 | [L11422](../../server/innodb/engine/executor.go#L11422) |
| `var performanceSchemaSetupObjectsDeletePattern` | MustCompile | A | 1 | [L11423](../../server/innodb/engine/executor.go#L11423) |
| `func executePerformanceSchemaThreadsUpdate` | MustCompile | A | 1 | [L11627](../../server/innodb/engine/executor.go#L11627) |
| `func parsePerformanceSchemaSetupPredicates` | MustCompile | A | 2 | [L11908](../../server/innodb/engine/executor.go#L11908)、[L11913](../../server/innodb/engine/executor.go#L11913) |
| `func filterPerformanceSchemaVariableRows` | MustCompile | A | 1 | [L12084](../../server/innodb/engine/executor.go#L12084) |
| `func performanceSchemaSummaryFilterMatches` | MustCompile | A | 2 | [L13941](../../server/innodb/engine/executor.go#L13941)、[L13956](../../server/innodb/engine/executor.go#L13956) |
| `func performanceSchemaSummaryNullFilterMatches` | MustCompile | A | 1 | [L13979](../../server/innodb/engine/executor.go#L13979) |
| `func performanceSchemaFilterQuery` | MustCompile | A | 1 | [L14285](../../server/innodb/engine/executor.go#L14285) |
| `func performanceSchemaSummaryFilterSpecified` | MustCompile | A | 1 | [L14293](../../server/innodb/engine/executor.go#L14293) |
| `func isInformationSchemaMetadataQuery` | MustCompile | A | 1 | [L15430](../../server/innodb/engine/executor.go#L15430) |
| `func isMySQLMetadataQuery` | MustCompile | A | 1 | [L15457](../../server/innodb/engine/executor.go#L15457) |
| `var informationSchemaRoleFilterPattern` | MustCompile | A | 1 | [L15573](../../server/innodb/engine/executor.go#L15573) |
| `var informationSchemaRoleInFilterPattern` | MustCompile | A | 1 | [L15575](../../server/innodb/engine/executor.go#L15575) |
| `var informationSchemaRoleNullFilterPattern` | MustCompile | A | 1 | [L15577](../../server/innodb/engine/executor.go#L15577) |
| `func executeInformationSchemaViewsSelect` | MustCompile | A | 1 | [L16821](../../server/innodb/engine/executor.go#L16821) |
| `func informationSchemaViewUpdatability` | MustCompile | A | 2 | [L16865](../../server/innodb/engine/executor.go#L16865)、[L16868](../../server/innodb/engine/executor.go#L16868) |
| `func checkViewDefinerPrivilege` | MustCompile | C | 1 | [L16934](../../server/innodb/engine/executor.go#L16934) |
| `func checkViewDefinerClausePrivilege` | MustCompile | A | 1 | [L16956](../../server/innodb/engine/executor.go#L16956) |
| `func executeInformationSchemaStatisticsSelect` | MustCompile | A | 1 | [L17005](../../server/innodb/engine/executor.go#L17005) |
| `func requestedInformationSchemaColumns` | MustCompile | A | 1 | [L17109](../../server/innodb/engine/executor.go#L17109) |
| `func normalizeInformationSchemaAggregate` | MustCompile | A | 2 | [L17141](../../server/innodb/engine/executor.go#L17141)、[L17152](../../server/innodb/engine/executor.go#L17152) |
| `func informationSchemaSelectsAllColumns` | MustCompile | A | 1 | [L17177](../../server/innodb/engine/executor.go#L17177) |
| `func informationSchemaSelectListContains` | MustCompile | A | 1 | [L17182](../../server/innodb/engine/executor.go#L17182) |
| `var informationSchemaMetadataInFilterPattern` | MustCompile | A | 1 | [L17993](../../server/innodb/engine/executor.go#L17993) |
| `var informationSchemaMetadataNullFilterPattern` | MustCompile | A | 1 | [L17994](../../server/innodb/engine/executor.go#L17994) |
| `var informationSchemaParameterInFilterPattern` | MustCompile | A | 1 | [L17995](../../server/innodb/engine/executor.go#L17995) |
| `func metadataPatternMatchesAllowEmpty` | MatchString | B | 1 | [L18046](../../server/innodb/engine/executor.go#L18046) |
| `func executeInformationSchemaParametersSelect` | MustCompile | A | 3 | [L18059](../../server/innodb/engine/executor.go#L18059)、[L18068](../../server/innodb/engine/executor.go#L18068)、[L18069](../../server/innodb/engine/executor.go#L18069) |
| `func executeInformationSchemaCollationsSelect` | MustCompile | A | 1 | [L18342](../../server/innodb/engine/executor.go#L18342) |
| `func executeInformationSchemaCharacterSetsSelect` | MustCompile | A | 1 | [L18372](../../server/innodb/engine/executor.go#L18372) |
| `func executeInformationSchemaEnginesSelect` | MustCompile | A | 1 | [L18402](../../server/innodb/engine/executor.go#L18402) |
| `func parseSetNamesCollation` | MustCompile | A | 1 | [L19966](../../server/innodb/engine/executor.go#L19966) |
| `func checkAlterForeignKeyPrivileges` | MustCompile | A | 1 | [L21304](../../server/innodb/engine/executor.go#L21304) |
| `func initializeCreateTableAutoIncrement` | MustCompile | A | 1 | [L21460](../../server/innodb/engine/executor.go#L21460) |
| `func parseCreateTableForeignKeysFallback` | MustCompile | A | 1 | [L22769](../../server/innodb/engine/executor.go#L22769) |
| `func cascadeActionFromTail` | MustCompile | A | 1 | [L22803](../../server/innodb/engine/executor.go#L22803) |
| `func validateForeignKeyReferentialActions` | MustCompile | A | 1 | [L22812](../../server/innodb/engine/executor.go#L22812) |
| `func extractFallbackGeneratedExpression` | MustCompile | A | 1 | [L22965](../../server/innodb/engine/executor.go#L22965) |
| `func parseTableOptionValue` | MustCompile | A | 1 | [L23262](../../server/innodb/engine/executor.go#L23262) |
| `func parseTableCommentOption` | MustCompile | A | 1 | [L23270](../../server/innodb/engine/executor.go#L23270) |
| `func executeShowPlugins` | MustCompile | A | 1 | [L23406](../../server/innodb/engine/executor.go#L23406) |
| `func executeShowOpenTables` | MustCompile | A | 2 | [L23430](../../server/innodb/engine/executor.go#L23430)、[L23489](../../server/innodb/engine/executor.go#L23489) |
| `func executeShowIndex` | MustCompile | A | 1 | [L23531](../../server/innodb/engine/executor.go#L23531) |
| `func executeShowTableStatus` | MustCompile | A | 1 | [L23883](../../server/innodb/engine/executor.go#L23883) |
| `func executeShowTriggers` | MustCompile | A | 1 | [L23952](../../server/innodb/engine/executor.go#L23952) |
| `func executeShowEvents` | MustCompile | A | 3 | [L24021](../../server/innodb/engine/executor.go#L24021)、[L24054](../../server/innodb/engine/executor.go#L24054)、[L24058](../../server/innodb/engine/executor.go#L24058) |
| `func executeShowRoutineStatus` | MustCompile | A | 1 | [L24106](../../server/innodb/engine/executor.go#L24106) |
| `var showFullTablesPattern` | MustCompile | A | 1 | [L24392](../../server/innodb/engine/executor.go#L24392) |
| `func executeShowFullColumns` | MustCompile | A | 1 | [L24513](../../server/innodb/engine/executor.go#L24513) |
| `func executeAdminReadQuery` | MustCompile | A | 1 | [L25161](../../server/innodb/engine/executor.go#L25161) |
| `func executeViewDDL` | MustCompile | A | 9 | [L25220](../../server/innodb/engine/executor.go#L25220)、[L25221](../../server/innodb/engine/executor.go#L25221)、[L25277](../../server/innodb/engine/executor.go#L25277)、[L25278](../../server/innodb/engine/executor.go#L25278)、[L25315](../../server/innodb/engine/executor.go#L25315)、[L25328](../../server/innodb/engine/executor.go#L25328)、[L25401](../../server/innodb/engine/executor.go#L25401)、[L25404](../../server/innodb/engine/executor.go#L25404)、[L25407](../../server/innodb/engine/executor.go#L25407) |
| `func executeShowCreateView` | MustCompile | A | 1 | [L25489](../../server/innodb/engine/executor.go#L25489) |
| `func applyViewExecutionSecurity` | MustCompile | A | 1 | [L25561](../../server/innodb/engine/executor.go#L25561) |
| `func applyViewExecutionSecurity` | MustCompile | C | 1 | [L25590](../../server/innodb/engine/executor.go#L25590) |
| `func rewriteViewQuery` | MustCompile | A | 1 | [L25601](../../server/innodb/engine/executor.go#L25601) |
| `func rewriteSimpleCTEQuery` | MustCompile | A | 3 | [L25757](../../server/innodb/engine/executor.go#L25757)、[L25816](../../server/innodb/engine/executor.go#L25816)、[L25830](../../server/innodb/engine/executor.go#L25830) |
| `func rewriteCTEInsertQuery` | MustCompile | A | 2 | [L25871](../../server/innodb/engine/executor.go#L25871)、[L25879](../../server/innodb/engine/executor.go#L25879) |
| `func replaceCTESourceReferences` | MustCompile | A | 1 | [L25946](../../server/innodb/engine/executor.go#L25946) |
| `func rewriteCTEDMLQuery` | MustCompile | A | 2 | [L26009](../../server/innodb/engine/executor.go#L26009)、[L26013](../../server/innodb/engine/executor.go#L26013) |
| `func rewriteMaterializedCTEQuery` | MustCompile | A | 2 | [L26030](../../server/innodb/engine/executor.go#L26030)、[L26038](../../server/innodb/engine/executor.go#L26038) |
| `func rewriteCTEJoinQuery` | MustCompile | A | 2 | [L26070](../../server/innodb/engine/executor.go#L26070)、[L26079](../../server/innodb/engine/executor.go#L26079) |
| `func executeSimpleWindowQuery` | MustCompile | A | 1 | [L26127](../../server/innodb/engine/executor.go#L26127) |
| `func executeExplainQuery` | MustCompile | A | 1 | [L26217](../../server/innodb/engine/executor.go#L26217) |
| `func extractQualifiedTableFromShowCreate` | MustCompile | A | 1 | [L26539](../../server/innodb/engine/executor.go#L26539) |

### A.25 `server/innodb/engine/fulltext_spatial_compatibility.go`

主要调用 6 处：A 类 4，B 类 0，C 类 2。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var fullTextAgainstPattern` | MustCompile | A | 1 | [L23](../../server/innodb/engine/fulltext_spatial_compatibility.go#L23) |
| `var fullTextFromTablePattern` | MustCompile | A | 1 | [L24](../../server/innodb/engine/fulltext_spatial_compatibility.go#L24) |
| `var spatialPredicateColumnFirstPattern` | MustCompile | A | 1 | [L25](../../server/innodb/engine/fulltext_spatial_compatibility.go#L25) |
| `var spatialPredicateGeometryFirstPattern` | MustCompile | A | 1 | [L26](../../server/innodb/engine/fulltext_spatial_compatibility.go#L26) |
| `func spatialBounds` | MustCompile | C | 1 | [L1305](../../server/innodb/engine/fulltext_spatial_compatibility.go#L1305) |
| `func spatialPoint` | MustCompile | C | 1 | [L1323](../../server/innodb/engine/fulltext_spatial_compatibility.go#L1323) |

### A.26 `server/innodb/engine/handler_compatibility.go`

主要调用 6 处：A 类 6，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var handlerTablePattern` | MustCompile | A | 1 | [L34](../../server/innodb/engine/handler_compatibility.go#L34) |
| `func handlerOpenAlias` | MustCompile | A | 1 | [L125](../../server/innodb/engine/handler_compatibility.go#L125) |
| `func handlerReadLimit` | MustCompile | A | 3 | [L252](../../server/innodb/engine/handler_compatibility.go#L252)、[L255](../../server/innodb/engine/handler_compatibility.go#L255)、[L258](../../server/innodb/engine/handler_compatibility.go#L258) |
| `func handlerLookupPredicate` | MustCompile | A | 1 | [L362](../../server/innodb/engine/handler_compatibility.go#L362) |

### A.27 `server/innodb/engine/index_ddl_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var standaloneCreateIndexPattern` | MustCompile | A | 1 | [L13](../../server/innodb/engine/index_ddl_compatibility.go#L13) |
| `var standaloneCreateFullTextIndexPattern` | MustCompile | A | 1 | [L14](../../server/innodb/engine/index_ddl_compatibility.go#L14) |
| `var standaloneDropIndexPattern` | MustCompile | A | 1 | [L15](../../server/innodb/engine/index_ddl_compatibility.go#L15) |

### A.28 `server/innodb/engine/information_schema_aux_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func informationSchemaConnectionControlUserHostMatches` | MustCompile | A | 1 | [L202](../../server/innodb/engine/information_schema_aux_compatibility.go#L202) |

### A.29 `server/innodb/engine/information_schema_catalog_compatibility.go`

主要调用 6 处：A 类 6，B 类 0，C 类 0。另有 QuoteMeta 3 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var informationSchemaSpaceNameFilterPattern` | MustCompile | A | 1 | [L65](../../server/innodb/engine/information_schema_catalog_compatibility.go#L65) |
| `func innodbVirtualBasePositions` | MustCompile | A | 2 | [L432](../../server/innodb/engine/information_schema_catalog_compatibility.go#L432)、[L443](../../server/innodb/engine/information_schema_catalog_compatibility.go#L443) |
| `func informationSchemaRoutineCallMatches` | MustCompile | A | 2 | [L456](../../server/innodb/engine/information_schema_catalog_compatibility.go#L456)、[L462](../../server/innodb/engine/information_schema_catalog_compatibility.go#L462) |
| `var informationSchemaGeometryColumnFilterPattern` | MustCompile | A | 1 | [L581](../../server/innodb/engine/information_schema_catalog_compatibility.go#L581) |

### A.30 `server/innodb/engine/information_schema_innodb_runtime_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var informationSchemaBufferSpaceFilterPattern` | MustCompile | A | 1 | [L17](../../server/innodb/engine/information_schema_innodb_runtime_compatibility.go#L17) |

### A.31 `server/innodb/engine/information_schema_registry.go`

主要调用 5 处：A 类 5，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var informationSchemaKeywordFilterPattern` | MustCompile | A | 1 | [L312](../../server/innodb/engine/information_schema_registry.go#L312) |
| `func informationSchemaKeywordReservedMatches` | MustCompile | A | 2 | [L351](../../server/innodb/engine/information_schema_registry.go#L351)、[L354](../../server/innodb/engine/information_schema_registry.go#L354) |
| `var informationSchemaViewUsageFilterPattern` | MustCompile | A | 1 | [L476](../../server/innodb/engine/information_schema_registry.go#L476) |
| `var informationSchemaExtensionNullFilterPattern` | MustCompile | A | 1 | [L543](../../server/innodb/engine/information_schema_registry.go#L543) |

### A.32 `server/innodb/engine/information_schema_spatial_resource_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var informationSchemaCatalogFilterPattern` | MustCompile | A | 1 | [L96](../../server/innodb/engine/information_schema_spatial_resource_compatibility.go#L96) |

### A.33 `server/innodb/engine/innodb_foreign_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func innodbForeignFilterMatches` | MustCompile | A | 1 | [L149](../../server/innodb/engine/innodb_foreign_compatibility.go#L149) |

### A.34 `server/innodb/engine/innodb_metrics_compatibility.go`

主要调用 2 处：A 类 1，B 类 1，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func informationSchemaMetricNameFilter` | MustCompile | A | 1 | [L884](../../server/innodb/engine/innodb_metrics_compatibility.go#L884) |
| `func metricNameMatches` | MatchString | B | 1 | [L902](../../server/innodb/engine/innodb_metrics_compatibility.go#L902) |

### A.35 `server/innodb/engine/innodb_tablespaces_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func informationSchemaTablespaceNamePattern` | MustCompile | A | 1 | [L603](../../server/innodb/engine/innodb_tablespaces_compatibility.go#L603) |

### A.36 `server/innodb/engine/innodb_trx_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func informationSchemaUint64Filter` | MustCompile | A | 1 | [L93](../../server/innodb/engine/innodb_trx_compatibility.go#L93) |

### A.37 `server/innodb/engine/json_value_compatibility.go`

主要调用 2 处：A 类 2，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var jsonValueReturningPattern` | MustCompile | A | 1 | [L12](../../server/innodb/engine/json_value_compatibility.go#L12) |
| `var jsonValuePolicyPattern` | MustCompile | A | 1 | [L14](../../server/innodb/engine/json_value_compatibility.go#L14) |

### A.38 `server/innodb/engine/mysql_system_tables_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var mysqlSystemTableValueFilterPattern` | MustCompile | A | 1 | [L107](../../server/innodb/engine/mysql_system_tables_compatibility.go#L107) |
| `var mysqlSystemTableValueInFilterPattern` | MustCompile | A | 1 | [L108](../../server/innodb/engine/mysql_system_tables_compatibility.go#L108) |
| `var mysqlSystemTableValueNullFilterPattern` | MustCompile | A | 1 | [L109](../../server/innodb/engine/mysql_system_tables_compatibility.go#L109) |

### A.39 `server/innodb/engine/optimizer_cost_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var optimizerCostInsertPattern` | MustCompile | A | 1 | [L247](../../server/innodb/engine/optimizer_cost_compatibility.go#L247) |
| `var optimizerCostUpdatePattern` | MustCompile | A | 1 | [L248](../../server/innodb/engine/optimizer_cost_compatibility.go#L248) |
| `var optimizerCostDeletePattern` | MustCompile | A | 1 | [L249](../../server/innodb/engine/optimizer_cost_compatibility.go#L249) |

### A.40 `server/innodb/engine/partition_compatibility.go`

主要调用 6 处：A 类 6，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeRawPartitionMaintenance` | MustCompile | A | 5 | [L55](../../server/innodb/engine/partition_compatibility.go#L55)、[L59](../../server/innodb/engine/partition_compatibility.go#L59)、[L63](../../server/innodb/engine/partition_compatibility.go#L63)、[L67](../../server/innodb/engine/partition_compatibility.go#L67)、[L71](../../server/innodb/engine/partition_compatibility.go#L71) |
| `func parsePartitionDefinitions` | MustCompile | A | 1 | [L1100](../../server/innodb/engine/partition_compatibility.go#L1100) |

### A.41 `server/innodb/engine/partition_routing.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func partitionCastValue` | MustCompile | A | 1 | [L109](../../server/innodb/engine/partition_routing.go#L109) |
| `func partitionArithmeticValue` | MustCompile | A | 1 | [L131](../../server/innodb/engine/partition_routing.go#L131) |
| `func partitionDateFunction` | MustCompile | A | 1 | [L170](../../server/innodb/engine/partition_routing.go#L170) |

### A.42 `server/innodb/engine/partitioned_btree_manager.go`

主要调用 6 处：A 类 6，B 类 0，C 类 0。另有 QuoteMeta 6 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func partitionConstantBetweenPredicate` | MustCompile | A | 1 | [L504](../../server/innodb/engine/partitioned_btree_manager.go#L504) |
| `func partitionConstantNotBetweenPredicate` | MustCompile | A | 1 | [L519](../../server/innodb/engine/partitioned_btree_manager.go#L519) |
| `func partitionConstantTextBetweenPredicate` | MustCompile | A | 1 | [L534](../../server/innodb/engine/partitioned_btree_manager.go#L534) |
| `func partitionConstantTextNotBetweenPredicate` | MustCompile | A | 1 | [L551](../../server/innodb/engine/partitioned_btree_manager.go#L551) |
| `func partitionConstantNotInPredicate` | MustCompile | A | 1 | [L568](../../server/innodb/engine/partitioned_btree_manager.go#L568) |
| `func partitionConstantInPredicate` | MustCompile | A | 1 | [L636](../../server/innodb/engine/partitioned_btree_manager.go#L636) |

### A.43 `server/innodb/engine/performance_schema_io_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var performanceSchemaTableReferencePattern` | MustCompile | A | 1 | [L84](../../server/innodb/engine/performance_schema_io_compatibility.go#L84) |

### A.44 `server/innodb/engine/performance_schema_registry.go`

主要调用 2 处：A 类 2，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func performanceSchemaTLSStatusQueryMatches` | MustCompile | A | 2 | [L499](../../server/innodb/engine/performance_schema_registry.go#L499)、[L503](../../server/innodb/engine/performance_schema_registry.go#L503) |

### A.45 `server/innodb/engine/persisted_variables_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var persistedVariableStatementPattern` | MustCompile | A | 1 | [L22](../../server/innodb/engine/persisted_variables_compatibility.go#L22) |

### A.46 `server/innodb/engine/select_executor.go`

主要调用 5 处：A 类 5，B 类 0，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var userWhereConditionRe` | MustCompile | A | 1 | [L31](../../server/innodb/engine/select_executor.go#L31) |
| `var hostWhereConditionRe` | MustCompile | A | 1 | [L32](../../server/innodb/engine/select_executor.go#L32) |
| `func determineStorageProjectionColumns` | MustCompile | A | 1 | [L629](../../server/innodb/engine/select_executor.go#L629) |
| `func partitionConstantPredicate` | MustCompile | A | 1 | [L781](../../server/innodb/engine/select_executor.go#L781) |
| `func determineRequiredColumns` | MustCompile | A | 1 | [L3241](../../server/innodb/engine/select_executor.go#L3241) |

### A.47 `server/innodb/engine/select_join_executor.go`

主要调用 1 处：A 类 0，B 类 1，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func evalJoinComparisonTruth` | MatchString | B | 1 | [L522](../../server/innodb/engine/select_join_executor.go#L522) |

### A.48 `server/innodb/engine/session_expression_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var qualifiedSystemVariablePattern` | MustCompile | A | 1 | [L19](../../server/innodb/engine/session_expression_compatibility.go#L19) |

### A.49 `server/innodb/engine/show_binlog_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var showBinlogEventsOptionsPattern` | MustCompile | A | 1 | [L22](../../server/innodb/engine/show_binlog_compatibility.go#L22) |

### A.50 `server/innodb/engine/show_create_database_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var showCreateDatabasePattern` | MustCompile | A | 1 | [L11](../../server/innodb/engine/show_create_database_compatibility.go#L11) |

### A.51 `server/innodb/engine/show_filter.go`

主要调用 1 处：A 类 0，B 类 1，C 类 0。另有 QuoteMeta 1 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func sqlLikeMatch` | Compile | B | 1 | [L46](../../server/innodb/engine/show_filter.go#L46) |

### A.52 `server/innodb/engine/show_parsing.go`

主要调用 17 处：A 类 17，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func extractShowTablesLikePatternFromQuery` | MustCompile | A | 1 | [L67](../../server/innodb/engine/show_parsing.go#L67) |
| `func extractShowTableStatusLikePatternFromQuery` | MustCompile | A | 1 | [L80](../../server/innodb/engine/show_parsing.go#L80) |
| `func extractShowTriggersLikePatternFromQuery` | MustCompile | A | 1 | [L93](../../server/innodb/engine/show_parsing.go#L93) |
| `func extractShowEventsLikePatternFromQuery` | MustCompile | A | 1 | [L106](../../server/innodb/engine/show_parsing.go#L106) |
| `func extractShowRoutineStatusLikePatternFromQuery` | MustCompile | A | 1 | [L119](../../server/innodb/engine/show_parsing.go#L119) |
| `func extractShowDatabasesLikePatternFromQuery` | MustCompile | A | 1 | [L132](../../server/innodb/engine/show_parsing.go#L132) |
| `func extractShowVariablesLikePatternFromQuery` | MustCompile | A | 1 | [L147](../../server/innodb/engine/show_parsing.go#L147) |
| `func extractShowStatusLikePatternFromQuery` | MustCompile | A | 1 | [L162](../../server/innodb/engine/show_parsing.go#L162) |
| `func extractShowTablesWhereExprFromQuery` | MustCompile | A | 1 | [L213](../../server/innodb/engine/show_parsing.go#L213) |
| `func extractShowTableStatusWhereExprFromQuery` | MustCompile | A | 1 | [L236](../../server/innodb/engine/show_parsing.go#L236) |
| `func extractShowTriggersWhereExprFromQuery` | MustCompile | A | 1 | [L258](../../server/innodb/engine/show_parsing.go#L258) |
| `func extractShowEventsWhereExprFromQuery` | MustCompile | A | 1 | [L280](../../server/innodb/engine/show_parsing.go#L280) |
| `func extractShowRoutineStatusWhereExprFromQuery` | MustCompile | A | 1 | [L302](../../server/innodb/engine/show_parsing.go#L302) |
| `func extractShowCharsetLikePatternFromQuery` | MustCompile | A | 1 | [L412](../../server/innodb/engine/show_parsing.go#L412) |
| `func extractShowCharsetWhereExprFromQuery` | MustCompile | A | 1 | [L424](../../server/innodb/engine/show_parsing.go#L424) |
| `func extractShowColumnsLikePatternFromQuery` | MustCompile | A | 1 | [L445](../../server/innodb/engine/show_parsing.go#L445) |
| `func extractShowColumnsWhereExprFromQuery` | MustCompile | A | 1 | [L459](../../server/innodb/engine/show_parsing.go#L459) |

### A.53 `server/innodb/engine/show_relaylog_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var showRelaylogEventsOptionsPattern` | MustCompile | A | 1 | [L18](../../server/innodb/engine/show_relaylog_compatibility.go#L18) |

### A.54 `server/innodb/engine/show_replica_status_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var showReplicaStatusChannelPattern` | MustCompile | A | 1 | [L23](../../server/innodb/engine/show_replica_status_compatibility.go#L23) |

### A.55 `server/innodb/engine/sql_event_runtime.go`

主要调用 4 处：A 类 4，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func parseSQLEventSchedule` | MustCompile | A | 4 | [L67](../../server/innodb/engine/sql_event_runtime.go#L67)、[L77](../../server/innodb/engine/sql_event_runtime.go#L77)、[L91](../../server/innodb/engine/sql_event_runtime.go#L91)、[L121](../../server/innodb/engine/sql_event_runtime.go#L121) |

### A.56 `server/innodb/engine/sql_prepared_statement_compatibility.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var sqlPreparePattern` | MustCompile | A | 1 | [L152](../../server/innodb/engine/sql_prepared_statement_compatibility.go#L152) |
| `var sqlExecutePattern` | MustCompile | A | 1 | [L153](../../server/innodb/engine/sql_prepared_statement_compatibility.go#L153) |
| `var sqlDeallocatePattern` | MustCompile | A | 1 | [L154](../../server/innodb/engine/sql_prepared_statement_compatibility.go#L154) |

### A.57 `server/innodb/engine/storage_integrated_dml_helper.go`

主要调用 2 处：A 类 0，B 类 2，C 类 0。另有 QuoteMeta 3 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func evalPredicateTruth` | MatchString | B | 1 | [L2289](../../server/innodb/engine/storage_integrated_dml_helper.go#L2289) |
| `func sqlLikePatternMatchWithEscape` | Compile | B | 1 | [L2445](../../server/innodb/engine/storage_integrated_dml_helper.go#L2445) |

### A.58 `server/innodb/engine/stored_object_compatibility.go`

主要调用 69 处：A 类 67，B 类 0，C 类 2。另有 QuoteMeta 3 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeStoredObjectDDL` | MustCompile | A | 3 | [L206](../../server/innodb/engine/stored_object_compatibility.go#L206)、[L207](../../server/innodb/engine/stored_object_compatibility.go#L207)、[L208](../../server/innodb/engine/stored_object_compatibility.go#L208) |
| `func executeStoredObjectAlter` | MustCompile | A | 8 | [L382](../../server/innodb/engine/stored_object_compatibility.go#L382)、[L416](../../server/innodb/engine/stored_object_compatibility.go#L416)、[L421](../../server/innodb/engine/stored_object_compatibility.go#L421)、[L425](../../server/innodb/engine/stored_object_compatibility.go#L425)、[L429](../../server/innodb/engine/stored_object_compatibility.go#L429)、[L455](../../server/innodb/engine/stored_object_compatibility.go#L455)、[L487](../../server/innodb/engine/stored_object_compatibility.go#L487)、[L533](../../server/innodb/engine/stored_object_compatibility.go#L533) |
| `func replaceRoutineClause` | MustCompile | A | 1 | [L571](../../server/innodb/engine/stored_object_compatibility.go#L571) |
| `func parseStoredObjectCreate` | MustCompile | A | 1 | [L584](../../server/innodb/engine/stored_object_compatibility.go#L584) |
| `var storedProcedureCallPattern` | MustCompile | A | 1 | [L628](../../server/innodb/engine/stored_object_compatibility.go#L628) |
| `func storedProcedureParameterModes` | MustCompile | A | 1 | [L697](../../server/innodb/engine/stored_object_compatibility.go#L697) |
| `func executeStoredProcedureCall` | MustCompile | A | 2 | [L775](../../server/innodb/engine/stored_object_compatibility.go#L775)、[L794](../../server/innodb/engine/stored_object_compatibility.go#L794) |
| `func executeStoredRoutineStatements` | MustCompile | A | 18 | [L987](../../server/innodb/engine/stored_object_compatibility.go#L987)、[L1019](../../server/innodb/engine/stored_object_compatibility.go#L1019)、[L1028](../../server/innodb/engine/stored_object_compatibility.go#L1028)、[L1064](../../server/innodb/engine/stored_object_compatibility.go#L1064)、[L1083](../../server/innodb/engine/stored_object_compatibility.go#L1083)、[L1095](../../server/innodb/engine/stored_object_compatibility.go#L1095)、[L1111](../../server/innodb/engine/stored_object_compatibility.go#L1111)、[L1115](../../server/innodb/engine/stored_object_compatibility.go#L1115)、[L1152](../../server/innodb/engine/stored_object_compatibility.go#L1152)、[L1174](../../server/innodb/engine/stored_object_compatibility.go#L1174)、[L1190](../../server/innodb/engine/stored_object_compatibility.go#L1190)、[L1246](../../server/innodb/engine/stored_object_compatibility.go#L1246)、[L1273](../../server/innodb/engine/stored_object_compatibility.go#L1273)、[L1276](../../server/innodb/engine/stored_object_compatibility.go#L1276)、[L1365](../../server/innodb/engine/stored_object_compatibility.go#L1365)、[L1555](../../server/innodb/engine/stored_object_compatibility.go#L1555)、[L1566](../../server/innodb/engine/stored_object_compatibility.go#L1566)、[L1570](../../server/innodb/engine/stored_object_compatibility.go#L1570) |
| `func parseStoredRoutineSignal` | MustCompile | A | 3 | [L1722](../../server/innodb/engine/stored_object_compatibility.go#L1722)、[L1733](../../server/innodb/engine/stored_object_compatibility.go#L1733)、[L1737](../../server/innodb/engine/stored_object_compatibility.go#L1737) |
| `func parseStoredRoutineHandlerDeclaration` | MustCompile | A | 1 | [L1819](../../server/innodb/engine/stored_object_compatibility.go#L1819) |
| `func consumeStoredRoutineHandlerCondition` | MustCompile | A | 2 | [L1860](../../server/innodb/engine/stored_object_compatibility.go#L1860)、[L1865](../../server/innodb/engine/stored_object_compatibility.go#L1865) |
| `func findStoredRoutineHandler` | MustCompile | C | 2 | [L2009](../../server/innodb/engine/stored_object_compatibility.go#L2009)、[L2015](../../server/innodb/engine/stored_object_compatibility.go#L2015) |
| `func findStoredRoutineHandler` | MustCompile | A | 1 | [L2014](../../server/innodb/engine/stored_object_compatibility.go#L2014) |
| `func parseRoutineDeclare` | MustCompile | A | 1 | [L2219](../../server/innodb/engine/stored_object_compatibility.go#L2219) |
| `func executeStoredFunctionCall` | MustCompile | A | 1 | [L2263](../../server/innodb/engine/stored_object_compatibility.go#L2263) |
| `func resolveStoredFunctionReturnExpression` | MustCompile | A | 2 | [L2373](../../server/innodb/engine/stored_object_compatibility.go#L2373)、[L2378](../../server/innodb/engine/stored_object_compatibility.go#L2378) |
| `func substituteStoredRoutineValues` | MustCompile | A | 1 | [L2392](../../server/innodb/engine/stored_object_compatibility.go#L2392) |
| `func storedObjectDefiner` | MustCompile | A | 1 | [L2416](../../server/innodb/engine/stored_object_compatibility.go#L2416) |
| `func parseRoutineReturnType` | MustCompile | A | 1 | [L2532](../../server/innodb/engine/stored_object_compatibility.go#L2532) |
| `func parseRoutineLocalVariables` | MustCompile | A | 1 | [L2540](../../server/innodb/engine/stored_object_compatibility.go#L2540) |
| `func parseRoutineReturnExpression` | MustCompile | A | 1 | [L2554](../../server/innodb/engine/stored_object_compatibility.go#L2554) |
| `func executeShowCreateStoredObject` | MustCompile | A | 1 | [L2611](../../server/innodb/engine/stored_object_compatibility.go#L2611) |
| `func findStoredObjectTableDependency` | MustCompile | A | 1 | [L3019](../../server/innodb/engine/stored_object_compatibility.go#L3019) |
| `func executeInformationSchemaRoutinesSelect` | MustCompile | A | 1 | [L3078](../../server/innodb/engine/stored_object_compatibility.go#L3078) |
| `func storedRoutineBodyDefinition` | MustCompile | A | 2 | [L3170](../../server/innodb/engine/stored_object_compatibility.go#L3170)、[L3176](../../server/innodb/engine/stored_object_compatibility.go#L3176) |
| `func parseRoutineComment` | MustCompile | A | 1 | [L3234](../../server/innodb/engine/stored_object_compatibility.go#L3234) |
| `func routineIsDeterministic` | MustCompile | A | 2 | [L3248](../../server/innodb/engine/stored_object_compatibility.go#L3248)、[L3251](../../server/innodb/engine/stored_object_compatibility.go#L3251) |
| `func routineSQLDataAccess` | MustCompile | A | 3 | [L3257](../../server/innodb/engine/stored_object_compatibility.go#L3257)、[L3259](../../server/innodb/engine/stored_object_compatibility.go#L3259)、[L3261](../../server/innodb/engine/stored_object_compatibility.go#L3261) |
| `func parseTriggerInformationSchemaMetadata` | MustCompile | A | 1 | [L3326](../../server/innodb/engine/stored_object_compatibility.go#L3326) |
| `func executeInformationSchemaEventsSelect` | MustCompile | A | 2 | [L3356](../../server/innodb/engine/stored_object_compatibility.go#L3356)、[L3360](../../server/innodb/engine/stored_object_compatibility.go#L3360) |
| `func parseEventInformationSchemaMetadata` | MustCompile | A | 2 | [L3419](../../server/innodb/engine/stored_object_compatibility.go#L3419)、[L3422](../../server/innodb/engine/stored_object_compatibility.go#L3422) |

### A.59 `server/innodb/engine/table_import_discard_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var alterTableTablespaceTransferPattern` | MustCompile | A | 1 | [L13](../../server/innodb/engine/table_import_discard_compatibility.go#L13) |

### A.60 `server/innodb/engine/table_tablespace_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var alterTableTablespacePattern` | MustCompile | A | 1 | [L14](../../server/innodb/engine/table_tablespace_compatibility.go#L14) |

### A.61 `server/innodb/engine/tablespace_compatibility.go`

主要调用 5 处：A 类 5，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var createTablespacePattern` | MustCompile | A | 1 | [L18](../../server/innodb/engine/tablespace_compatibility.go#L18) |
| `var alterTablespaceRenamePattern` | MustCompile | A | 1 | [L19](../../server/innodb/engine/tablespace_compatibility.go#L19) |
| `var dropTablespacePattern` | MustCompile | A | 1 | [L20](../../server/innodb/engine/tablespace_compatibility.go#L20) |
| `var tableOptionTablespacePattern` | MustCompile | A | 1 | [L21](../../server/innodb/engine/tablespace_compatibility.go#L21) |
| `func validTablespaceCreateOptions` | MustCompile | A | 1 | [L124](../../server/innodb/engine/tablespace_compatibility.go#L124) |

### A.62 `server/innodb/engine/temporary_table_compatibility.go`

主要调用 5 处：A 类 5，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var temporaryTableCreatePattern` | MustCompile | A | 1 | [L22](../../server/innodb/engine/temporary_table_compatibility.go#L22) |
| `var temporaryTableRenamePattern` | MustCompile | A | 1 | [L23](../../server/innodb/engine/temporary_table_compatibility.go#L23) |
| `var temporaryTableDropPattern` | MustCompile | A | 1 | [L24](../../server/innodb/engine/temporary_table_compatibility.go#L24) |
| `func rewriteTemporaryTableReferences` | MustCompile | A | 2 | [L388](../../server/innodb/engine/temporary_table_compatibility.go#L388)、[L390](../../server/innodb/engine/temporary_table_compatibility.go#L390) |

### A.63 `server/innodb/engine/trigger_runtime.go`

主要调用 9 处：A 类 9，B 类 0，C 类 0。另有 QuoteMeta 2 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func applyBeforeTriggers` | MustCompile | A | 1 | [L39](../../server/innodb/engine/trigger_runtime.go#L39) |
| `func applyAfterTriggers` | MustCompile | A | 3 | [L69](../../server/innodb/engine/trigger_runtime.go#L69)、[L106](../../server/innodb/engine/trigger_runtime.go#L106)、[L109](../../server/innodb/engine/trigger_runtime.go#L109) |
| `func loadTableTriggers` | MustCompile | A | 1 | [L167](../../server/innodb/engine/trigger_runtime.go#L167) |
| `func triggerOrderReference` | MustCompile | A | 1 | [L220](../../server/innodb/engine/trigger_runtime.go#L220) |
| `func triggerBody` | MustCompile | A | 1 | [L239](../../server/innodb/engine/trigger_runtime.go#L239) |
| `func stripTriggerOrderPrefix` | MustCompile | A | 1 | [L249](../../server/innodb/engine/trigger_runtime.go#L249) |
| `func triggerLiteralValue` | MustCompile | A | 1 | [L268](../../server/innodb/engine/trigger_runtime.go#L268) |

### A.64 `server/innodb/engine/warning_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var showWarningsLimitPattern` | MustCompile | A | 1 | [L46](../../server/innodb/engine/warning_compatibility.go#L46) |

### A.65 `server/innodb/engine/window_compatibility.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeAdvancedWindowQuery` | MustCompile | A | 1 | [L16](../../server/innodb/engine/window_compatibility.go#L16) |

### A.66 `server/innodb/engine/window_query_compatibility.go`

主要调用 7 处：A 类 7，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func executeGeneralWindowQuery` | MustCompile | A | 3 | [L37](../../server/innodb/engine/window_query_compatibility.go#L37)、[L45](../../server/innodb/engine/window_query_compatibility.go#L45)、[L58](../../server/innodb/engine/window_query_compatibility.go#L58) |
| `func executeWindowOnlyProjection` | MustCompile | A | 2 | [L289](../../server/innodb/engine/window_query_compatibility.go#L289)、[L299](../../server/innodb/engine/window_query_compatibility.go#L299) |
| `func executeMultipleWindowExpressions` | MustCompile | A | 1 | [L397](../../server/innodb/engine/window_query_compatibility.go#L397) |
| `func parseWindowSpecification` | MustCompile | A | 1 | [L801](../../server/innodb/engine/window_query_compatibility.go#L801) |

### A.67 `server/innodb/manager/optimizer_manager.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 3 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func optimizerIndexMatchesConditions` | MustCompile | A | 3 | [L180](../../server/innodb/manager/optimizer_manager.go#L180)、[L181](../../server/innodb/manager/optimizer_manager.go#L181)、[L182](../../server/innodb/manager/optimizer_manager.go#L182) |

### A.68 `server/innodb/manager/schema_manager.go`

主要调用 1 处：A 类 0，B 类 1，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func validateDatabaseName` | MustCompile | B | 1 | [L1135](../../server/innodb/manager/schema_manager.go#L1135) |

### A.69 `server/innodb/manager/system_variable_analyzer.go`

主要调用 3 处：A 类 3，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func IsSystemVariableQuery` | MatchString | A | 2 | [L44](../../server/innodb/manager/system_variable_analyzer.go#L44)、[L51](../../server/innodb/manager/system_variable_analyzer.go#L51) |
| `func parseWithRegex` | Compile | A | 1 | [L271](../../server/innodb/manager/system_variable_analyzer.go#L271) |

### A.70 `server/innodb/plan/expression.go`

主要调用 10 处：A 类 0，B 类 10，C 类 0。另有 QuoteMeta 5 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `func parseFixedTimezoneOffset` | MustCompile | B | 1 | [L1503](../../server/innodb/plan/expression.go#L1503) |
| `func evalTimeConversionFunction` | MustCompile | B | 1 | [L1530](../../server/innodb/plan/expression.go#L1530) |
| `func evalNetworkAndDigestFunction` | MustCompile | B | 1 | [L2440](../../server/innodb/plan/expression.go#L2440) |
| `func compileMySQLRegexp` | Compile | B | 1 | [L2967](../../server/innodb/plan/expression.go#L2967) |
| `func validateJSONSchemaCompatibility` | MatchString | B | 1 | [L3589](../../server/innodb/plan/expression.go#L3589) |
| `func jsonSearchPattern` | Compile | B | 1 | [L4486](../../server/innodb/plan/expression.go#L4486) |
| `func evalRegexp` | MatchString | B | 1 | [L5133](../../server/innodb/plan/expression.go#L5133) |
| `func evalLike` | MatchString | B | 1 | [L5422](../../server/innodb/plan/expression.go#L5422) |
| `func evalLikeWithEscape` | MatchString | B | 1 | [L5438](../../server/innodb/plan/expression.go#L5438) |
| `func Eval` | MatchString | B | 1 | [L5959](../../server/innodb/plan/expression.go#L5959) |

### A.71 `server/net/decoupled_handler.go`

主要调用 2 处：A 类 2，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var expiredPasswordSetPattern` | MustCompile | A | 1 | [L1553](../../server/net/decoupled_handler.go#L1553) |
| `var expiredPasswordAlterPattern` | MustCompile | A | 1 | [L1554](../../server/net/decoupled_handler.go#L1554) |

### A.72 `server/observability/metrics/runtime.go`

主要调用 1 处：A 类 1，B 类 0，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var tableIOReferencePattern` | MustCompile | A | 1 | [L155](../../server/observability/metrics/runtime.go#L155) |

### A.73 `server/observability/metrics/statement_digest.go`

主要调用 1 处：A 类 0，B 类 1，C 类 0。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var statementDigestBindVariablePattern` | MustCompile | B | 1 | [L11](../../server/observability/metrics/statement_digest.go#L11) |

### A.74 `server/replication/runtime.go`

主要调用 1 处：A 类 0，B 类 0，C 类 1。另有 QuoteMeta 0 处。

| 函数或包级变量 | 调用 API | 类别 | 数量 | 源码行号 |
|---|---|---|---:|---|
| `var mysqlErrorNumberPattern` | MustCompile | C | 1 | [L2822](../../server/replication/runtime.go#L2822) |

### A.75 QuoteMeta 辅助调用位置

以下 75 处为构造正则时的转义辅助调用，不包含在上述 543 处中。所属 SQL 正则移除后，可删除不再使用的辅助调用；运行时值匹配仍可保留所需调用。

| 文件 | 函数或变量 | 数量 | 源码行号 |
|---|---|---:|---|
| `server/auth/engine_access.go` | `func matchHost` | 1 | [L868](../../server/auth/engine_access.go#L868) |
| `server/dispatcher/system_variable_engine.go` | `func matchesMetadataPattern` | 1 | [L670](../../server/dispatcher/system_variable_engine.go#L670) |
| `server/innodb/engine/account_compatibility.go` | `func accountHostMatches` | 1 | [L578](../../server/innodb/engine/account_compatibility.go#L578) |
| `server/innodb/engine/alter_operation_compatibility.go` | `func checkExpressionReferencesColumn` | 1 | [L826](../../server/innodb/engine/alter_operation_compatibility.go#L826) |
| `server/innodb/engine/correlated_derived_compatibility.go` | `func executeCorrelatedDerivedOuterCompatibility` | 3 | [L78](../../server/innodb/engine/correlated_derived_compatibility.go#L78)、[L129](../../server/innodb/engine/correlated_derived_compatibility.go#L129)、[L129](../../server/innodb/engine/correlated_derived_compatibility.go#L129) |
| `server/innodb/engine/correlated_dml_compatibility.go` | `func correlatedDMLScalarSubqueryRanges` | 1 | [L269](../../server/innodb/engine/correlated_dml_compatibility.go#L269) |
| `server/innodb/engine/correlated_dml_compatibility.go` | `func rewriteCorrelatedDMLPredicate` | 1 | [L440](../../server/innodb/engine/correlated_dml_compatibility.go#L440) |
| `server/innodb/engine/correlated_predicate_compatibility.go` | `func executeCorrelatedPredicateSubqueryCompatibility` | 2 | [L221](../../server/innodb/engine/correlated_predicate_compatibility.go#L221)、[L221](../../server/innodb/engine/correlated_predicate_compatibility.go#L221) |
| `server/innodb/engine/correlated_predicate_multiple_compatibility.go` | `func executeMultipleCorrelatedPredicateSubqueriesCompatibility` | 2 | [L209](../../server/innodb/engine/correlated_predicate_multiple_compatibility.go#L209)、[L209](../../server/innodb/engine/correlated_predicate_multiple_compatibility.go#L209) |
| `server/innodb/engine/correlated_scalar_compatibility.go` | `func executeCorrelatedScalarSubqueryCompatibility` | 2 | [L313](../../server/innodb/engine/correlated_scalar_compatibility.go#L313)、[L313](../../server/innodb/engine/correlated_scalar_compatibility.go#L313) |
| `server/innodb/engine/correlated_scalar_compatibility.go` | `func evaluateCorrelatedScalarPredicate` | 2 | [L507](../../server/innodb/engine/correlated_scalar_compatibility.go#L507)、[L507](../../server/innodb/engine/correlated_scalar_compatibility.go#L507) |
| `server/innodb/engine/correlated_scalar_compatibility.go` | `func correlatedClauseContainsScalarAlias` | 1 | [L590](../../server/innodb/engine/correlated_scalar_compatibility.go#L590) |
| `server/innodb/engine/correlated_scalar_compatibility.go` | `func normalizeCorrelatedInnerAlias` | 1 | [L950](../../server/innodb/engine/correlated_scalar_compatibility.go#L950) |
| `server/innodb/engine/correlated_subquery_compatibility.go` | `func executeCorrelatedSubqueryCompatibility` | 3 | [L104](../../server/innodb/engine/correlated_subquery_compatibility.go#L104)、[L209](../../server/innodb/engine/correlated_subquery_compatibility.go#L209)、[L209](../../server/innodb/engine/correlated_subquery_compatibility.go#L209) |
| `server/innodb/engine/cte_compatibility.go` | `func executeRecursiveCTEDML` | 2 | [L1200](../../server/innodb/engine/cte_compatibility.go#L1200)、[L1201](../../server/innodb/engine/cte_compatibility.go#L1201) |
| `server/innodb/engine/executor.go` | `func renameSQLIdentifier` | 1 | [L4716](../../server/innodb/engine/executor.go#L4716) |
| `server/innodb/engine/executor.go` | `func rewriteForeignKeyReferenceTable` | 2 | [L4837](../../server/innodb/engine/executor.go#L4837)、[L4837](../../server/innodb/engine/executor.go#L4837) |
| `server/innodb/engine/executor.go` | `func bindJoinOuterReferences` | 2 | [L6893](../../server/innodb/engine/executor.go#L6893)、[L6893](../../server/innodb/engine/executor.go#L6893) |
| `server/innodb/engine/executor.go` | `func checkPerformanceSchemaTableSelectPrivileges` | 1 | [L8882](../../server/innodb/engine/executor.go#L8882) |
| `server/innodb/engine/executor.go` | `func checkMySQLMetadataTableSelectPrivileges` | 1 | [L8921](../../server/innodb/engine/executor.go#L8921) |
| `server/innodb/engine/executor.go` | `func checkInformationSchemaProcessPrivileges` | 1 | [L8969](../../server/innodb/engine/executor.go#L8969) |
| `server/innodb/engine/executor.go` | `func parsePerformanceSchemaSetupPredicates` | 1 | [L11906](../../server/innodb/engine/executor.go#L11906) |
| `server/innodb/engine/executor.go` | `func performanceSchemaSummaryFilterMatches` | 2 | [L13941](../../server/innodb/engine/executor.go#L13941)、[L13956](../../server/innodb/engine/executor.go#L13956) |
| `server/innodb/engine/executor.go` | `func performanceSchemaSummaryNullFilterMatches` | 1 | [L13979](../../server/innodb/engine/executor.go#L13979) |
| `server/innodb/engine/executor.go` | `func performanceSchemaSummaryFilterSpecified` | 2 | [L14293](../../server/innodb/engine/executor.go#L14293)、[L14293](../../server/innodb/engine/executor.go#L14293) |
| `server/innodb/engine/executor.go` | `func metadataPatternMatchesAllowEmpty` | 1 | [L18043](../../server/innodb/engine/executor.go#L18043) |
| `server/innodb/engine/executor.go` | `func cascadeActionFromTail` | 1 | [L22803](../../server/innodb/engine/executor.go#L22803) |
| `server/innodb/engine/executor.go` | `func replaceCTESourceReferences` | 1 | [L25946](../../server/innodb/engine/executor.go#L25946) |
| `server/innodb/engine/executor.go` | `func rewriteCTEDMLQuery` | 1 | [L26013](../../server/innodb/engine/executor.go#L26013) |
| `server/innodb/engine/information_schema_catalog_compatibility.go` | `func innodbVirtualBasePositions` | 1 | [L442](../../server/innodb/engine/information_schema_catalog_compatibility.go#L442) |
| `server/innodb/engine/information_schema_catalog_compatibility.go` | `func informationSchemaRoutineCallMatches` | 2 | [L451](../../server/innodb/engine/information_schema_catalog_compatibility.go#L451)、[L455](../../server/innodb/engine/information_schema_catalog_compatibility.go#L455) |
| `server/innodb/engine/innodb_metrics_compatibility.go` | `func metricNameMatches` | 1 | [L899](../../server/innodb/engine/innodb_metrics_compatibility.go#L899) |
| `server/innodb/engine/innodb_trx_compatibility.go` | `func informationSchemaUint64Filter` | 1 | [L93](../../server/innodb/engine/innodb_trx_compatibility.go#L93) |
| `server/innodb/engine/partitioned_btree_manager.go` | `func partitionConstantBetweenPredicate` | 1 | [L504](../../server/innodb/engine/partitioned_btree_manager.go#L504) |
| `server/innodb/engine/partitioned_btree_manager.go` | `func partitionConstantNotBetweenPredicate` | 1 | [L519](../../server/innodb/engine/partitioned_btree_manager.go#L519) |
| `server/innodb/engine/partitioned_btree_manager.go` | `func partitionConstantTextBetweenPredicate` | 1 | [L534](../../server/innodb/engine/partitioned_btree_manager.go#L534) |
| `server/innodb/engine/partitioned_btree_manager.go` | `func partitionConstantTextNotBetweenPredicate` | 1 | [L551](../../server/innodb/engine/partitioned_btree_manager.go#L551) |
| `server/innodb/engine/partitioned_btree_manager.go` | `func partitionConstantNotInPredicate` | 1 | [L568](../../server/innodb/engine/partitioned_btree_manager.go#L568) |
| `server/innodb/engine/partitioned_btree_manager.go` | `func partitionConstantInPredicate` | 1 | [L636](../../server/innodb/engine/partitioned_btree_manager.go#L636) |
| `server/innodb/engine/performance_schema_registry.go` | `func performanceSchemaTLSStatusQueryMatches` | 2 | [L499](../../server/innodb/engine/performance_schema_registry.go#L499)、[L503](../../server/innodb/engine/performance_schema_registry.go#L503) |
| `server/innodb/engine/select_executor.go` | `func partitionConstantPredicate` | 1 | [L781](../../server/innodb/engine/select_executor.go#L781) |
| `server/innodb/engine/show_filter.go` | `func sqlLikeMatch` | 1 | [L42](../../server/innodb/engine/show_filter.go#L42) |
| `server/innodb/engine/storage_integrated_dml_helper.go` | `func sqlLikePatternMatchWithEscape` | 3 | [L2424](../../server/innodb/engine/storage_integrated_dml_helper.go#L2424)、[L2438](../../server/innodb/engine/storage_integrated_dml_helper.go#L2438)、[L2442](../../server/innodb/engine/storage_integrated_dml_helper.go#L2442) |
| `server/innodb/engine/stored_object_compatibility.go` | `func executeStoredProcedureCall` | 1 | [L794](../../server/innodb/engine/stored_object_compatibility.go#L794) |
| `server/innodb/engine/stored_object_compatibility.go` | `func substituteStoredRoutineValues` | 1 | [L2392](../../server/innodb/engine/stored_object_compatibility.go#L2392) |
| `server/innodb/engine/stored_object_compatibility.go` | `func findStoredObjectTableDependency` | 1 | [L3019](../../server/innodb/engine/stored_object_compatibility.go#L3019) |
| `server/innodb/engine/temporary_table_compatibility.go` | `func rewriteTemporaryTableReferences` | 2 | [L386](../../server/innodb/engine/temporary_table_compatibility.go#L386)、[L387](../../server/innodb/engine/temporary_table_compatibility.go#L387) |
| `server/innodb/engine/trigger_runtime.go` | `func applyAfterTriggers` | 2 | [L106](../../server/innodb/engine/trigger_runtime.go#L106)、[L109](../../server/innodb/engine/trigger_runtime.go#L109) |
| `server/innodb/manager/optimizer_manager.go` | `func optimizerIndexMatchesConditions` | 3 | [L180](../../server/innodb/manager/optimizer_manager.go#L180)、[L181](../../server/innodb/manager/optimizer_manager.go#L181)、[L182](../../server/innodb/manager/optimizer_manager.go#L182) |
| `server/innodb/plan/expression.go` | `func jsonSearchPattern` | 2 | [L4479](../../server/innodb/plan/expression.go#L4479)、[L4483](../../server/innodb/plan/expression.go#L4483) |
| `server/innodb/plan/expression.go` | `func sqlLikePatternRegexWithEscape` | 3 | [L5455](../../server/innodb/plan/expression.go#L5455)、[L5469](../../server/innodb/plan/expression.go#L5469)、[L5473](../../server/innodb/plan/expression.go#L5473) |

## 附录 B SQL 解析探针逐条结果

下面均为直接调用项目解析器的结果，未经过执行器中的兼容处理。因此“解析报错”不等于当前数据库服务一定不支持该 SQL，“返回 AST”也不等于数据库服务已经正确执行。

| 序号 | 输入 SQL | Parse 结果 | ParseStrictDDL 结果 |
|---|---|---|---|
| 1 | `ALTER TABLE t ADD COLUMN c INT` | 保留 c 列及 INT 类型 | 未报错 |
| 2 | `ALTER TABLE t ADD COLUMN c INT, DROP COLUMN d` | 报错：syntax error at position 32 | 报错：syntax error at position 32 |
| 3 | `ALTER TABLE t MODIFY COLUMN c VARCHAR(20) AFTER d` | 仅保留 alter 和表名，操作或选项未保留 | 未报错 |
| 4 | `ALTER TABLE t ADD INDEX idx_c(c)` | 仅保留 alter 和表名，操作或选项未保留 | 未报错 |
| 5 | `ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN c INT` | 仅保留 alter 和表名，操作或选项未保留 | 未报错 |
| 6 | `ALTER TABLE t RENAME TO t2` | 保留旧表名与新表名 | 未报错 |
| 7 | `SHOW TABLES` | 仅 Type，未形成 ShowTablesOpt | 未报错 |
| 8 | `show tables` | 仅 Type，未形成 ShowTablesOpt | 未报错 |
| 9 | `SHOW FULL TABLES FROM app LIKE 'user%'` | 报错：syntax error at position 17 near 'TABLES' | 报错：syntax error at position 17 near 'TABLES' |
| 10 | `show full tables from app like 'user%'` | 报错：syntax error at position 17 near 'tables' | 报错：syntax error at position 17 near 'tables' |
| 11 | `SHOW VARIABLES LIKE 'max_%'` | 保留 Type 与 LIKE | 未报错 |
| 12 | `SHOW STATUS WHERE Variable_name = 'Threads_connected'` | 保留 Type 与 WHERE 表达式 | 未报错 |
| 13 | `SHOW CREATE TABLE app.t` | 仅 Type=create table，后续对象或选项未保留 | 未报错 |
| 14 | `SHOW COLUMNS FROM app.t LIKE 'id'` | 仅 Type=COLUMNS，后续对象或选项未保留 | 未报错 |
| 15 | `SHOW INDEX FROM app.t` | 仅 Type=index，后续对象或选项未保留 | 未报错 |
| 16 | `SHOW PROCEDURE STATUS WHERE Db = 'app'` | 仅 Type=procedure，后续对象或选项未保留 | 未报错 |
| 17 | `SHOW WARNINGS LIMIT 1, 2` | 仅 Type=WARNINGS，后续对象或选项未保留 | 未报错 |
| 18 | `SHOW BINLOG EVENTS IN 'bin.000001' FROM 4 LIMIT 10` | 仅 Type=BINLOG，后续对象或选项未保留 | 未报错 |
| 19 | `SHOW REPLICA STATUS FOR CHANNEL 'ch'` | 仅 Type=REPLICA，后续对象或选项未保留 | 未报错 |
| 20 | `ANALYZE TABLE app.t` | 返回 DDL，Action=alter；管理命令类别未保留 | 未报错 |
| 21 | `REPAIR TABLE app.t QUICK` | 返回空 OtherAdmin，目标和选项未保留 | 未报错 |
| 22 | `OPTIMIZE TABLE app.t` | 返回空 OtherAdmin，目标和选项未保留 | 未报错 |
| 23 | `FLUSH TABLES WITH READ LOCK` | 报错：syntax error at position 6 near 'FLUSH' | 报错：syntax error at position 6 near 'FLUSH' |
| 24 | `KILL QUERY 123` | 报错：syntax error at position 5 near 'kill' | 报错：syntax error at position 5 near 'kill' |
| 25 | `LOCK TABLES app.t READ` | 报错：syntax error at position 5 near 'lock' | 报错：syntax error at position 5 near 'lock' |
| 26 | `CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1'` | 报错：syntax error at position 7 near 'change' | 报错：syntax error at position 7 near 'change' |
| 27 | `INSTALL COMPONENT 'file://component_validate_password'` | 报错：syntax error at position 8 near 'INSTALL' | 报错：syntax error at position 8 near 'INSTALL' |
| 28 | `CREATE PROCEDURE app.p(IN x INT) BEGIN SELECT x; END` | 报错：syntax error at position 21 near 'app' | 报错：syntax error at position 21 near 'app' |
| 29 | `CREATE FUNCTION app.f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1` | 报错：syntax error at position 20 near 'app' | 报错：syntax error at position 20 near 'app' |
| 30 | `CREATE TRIGGER app.tr BEFORE INSERT ON app.t FOR EACH ROW SET NEW.c = 1` | 报错：syntax error at position 19 near 'app' | 报错：syntax error at position 19 near 'app' |
| 31 | `CREATE EVENT app.ev ON SCHEDULE EVERY 1 DAY DO DELETE FROM app.t` | 报错：syntax error at position 17 near 'app' | 报错：syntax error at position 17 near 'app' |
| 32 | `CALL app.p(1)` | 报错：syntax error at position 5 near 'call' | 报错：syntax error at position 5 near 'call' |
| 33 | `DROP PROCEDURE app.p` | 报错：syntax error at position 15 near 'procedure' | 报错：syntax error at position 15 near 'procedure' |
| 34 | `CREATE VIEW app.v AS SELECT id FROM app.t` | 保留视图名，SELECT 定义未保留 | 未报错 |
| 35 | `CREATE USER 'u'@'localhost' IDENTIFIED BY 'x'` | 报错：syntax error at position 16 near 'u' | 报错：syntax error at position 16 near 'u' |
| 36 | `GRANT SELECT ON app.t TO 'u'@'localhost'` | 报错：syntax error at position 6 near 'grant' | 报错：syntax error at position 6 near 'grant' |
| 37 | `SELECT @@session.autocommit` | 返回 Select，变量引用进入 AST | 未报错 |
| 38 | `SET NAMES utf8mb4 COLLATE utf8mb4_bin` | 保留字符集，COLLATE 属性未保留 | 未报错 |
| 39 | `PREPARE s FROM 'SELECT 1'` | 报错：syntax error at position 8 near 'PREPARE' | 报错：syntax error at position 8 near 'PREPARE' |
| 40 | `HANDLER app.t OPEN` | 报错：syntax error at position 8 near 'HANDLER' | 报错：syntax error at position 8 near 'HANDLER' |
| 41 | `CREATE TABLE app.t2 LIKE app.t` | 返回部分 DDL，LIKE 源表未保留 | 报错：syntax error at position 25 near 'like' |
| 42 | `CREATE TEMPORARY TABLE app.tmp(id INT)` | 报错：syntax error at position 23 near 'table' | 报错：syntax error at position 23 near 'table' |
| 43 | `SELECT * FROM app.t WHERE id BETWEEN 1 AND 5` | 返回 Select，BETWEEN 进入 AST | 未报错 |
| 44 | `SELECT * FROM app.t WHERE EXISTS (SELECT 1 FROM app.t2 WHERE t2.id=t.id)` | 返回 Select，EXISTS 子查询进入 AST | 未报错 |
| 45 | `WITH q AS (SELECT 1 AS id) SELECT * FROM q` | 返回 With，保留 CTE 与主查询 | 报错：syntax error at position 5 near 'with' |
| 46 | `SELECT row_number() OVER (ORDER BY id) AS rn FROM app.t` | 返回 Select，窗口 OVER 进入 AST | 报错：syntax error at position 27 |
| 47 | `ALTER TABLE t MODIFY COLUMN c INT totally_invalid_tail` | 返回节点，非法尾部未校验 | 未报错 |
| 48 | `SHOW CREATE TABLE app.t totally_invalid_tail` | 返回节点，非法尾部未校验 | 未报错 |
| 49 | `REPAIR TABLE app.t totally_invalid_tail` | 返回节点，非法尾部未校验 | 未报错 |

49 条中有 3 条专门检查非法尾部，其余为代表性语法样例；未按完整 MySQL 语法集合抽样，不能将 30/49 当作支持率。

整改后，合法样例应具有执行所需的完整节点和字段，非法尾部样例应被拒绝。对于当前服务尚不支持的命令或选项，应明确返回不支持，不能因为构造出一个 AST 就宣称功能已经实现。
