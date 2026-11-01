# SQL 正则迁移进展（2026-10-18）

当前状态：继续实施，尚未全部完成。以下扫描、代码和测试证据更新了此前过于宽泛的“仅剩值语义”结论。

## 当前扫描

执行 `go run ./scripts/sql-regex-audit -root . -exclude cmd,client -output docs/planning/2026-10-16-sql-regex-audit-ci-scope.json`：1077 个 Go 文件、28 个 regexp 导入文件、150 个 regexp 包直接调用位置，Go 源码解析异常 0。分类为 pattern_definition 99、syntax_candidate_or_value_match 9、value_match_or_escaping 42。

相对于本次连续实施起点的 51 文件、450 位置，减少 23 个 regexp 导入文件和 300 个直接调用位置。这不是独立正则条数，也不表示剩余 150 位置均适合 AST。审计脚本现记录首个参数源码，并修复包级声明误归到上一个函数的问题。

完整清单：[逐位置清单](2026-10-17-sql-regex-remaining-inventory.md)。JSON 和清单覆盖当前剩余位置，不把 Compile/MustCompile 声明自动认作可保留。

追加批次验证：CTAS/临时 CTAS、原生索引可见性、ALTER/独立索引可见性、表 DDL 组合回归通过；最终组合 38.089s 通过。

## 已实施

| 路径 | 实际变化 |
|---|---|
| 存储对象解析及消费者 | CREATE/DROP、显式 DEFINER、OR REPLACE、IF EXISTS/IF NOT EXISTS、例程注释/返回类型/确定性/数据访问属性、CALL 名称与参数改读 AST；保留嵌套调用、字面量逗号和括号，拒绝 CALL/DROP 的多余语句 |
| 事件调度 | 调度头部、执行时间、间隔及 ON COMPLETION 读取 AST，删除原始 SQL 兼容正则 |
| mysql 系统表与 Processlist | 完整 WHERE 布尔表达式通过 AST 求值；覆盖 OR、NOT、NULL、带限定名列及字面量中的伪 SQL |
| 分区路由表达式 | CAST、算术表达式、日期函数通过表达式 AST 读取；分区维护语句仍需继续迁移 |
| 管理命令 | admin_compatibility.go 不再导入 regexp；KILL、LOCK 目标及模式、SOURCE/FILTER/PURGE/RESET、FLUSH、CHECK/ANALYZE/OPTIMIZE 与直方图改用结构化 AST 参数 |
| 复制选项 | 新增递归的 AdminOptionValue AST；逗号和括号仅在语法位置分组，已解码的密码、过滤规则、数据库重写规则直接传递，取消拼回 SQL 再正则解析的循环 |
| SET | dispatcher 数值算术使用 AST；SET NAMES COLLATE、PERSIST/PERSIST_ONLY 使用 AST；修正字符串中 persist/reset 误触发 SET 模式及持久化修饰符换行/注释边界 |
| 表空间传输 | DISCARD/IMPORT TABLESPACE 新增操作 AST，移除专门解析该语句的正则；其他一般表空间语句正则仍在清单中 |
| CTAS | CREATE TABLE AS SELECT 及临时表 CTAS 的三个结构正则移除，AST 记录源 SELECT、目标、显式列、表选项和 IGNORE/REPLACE；原生 lexer 区分换行、注释、字面量与括号，非法 SELECT 在创建前返回错误 |
| 建表索引可见性 | VISIBLE/INVISIBLE 由原生 yacc 索引选项产生 AST，删除三个正则；保持 visible/invisible 可作普通标识符，验证带逗号的索引名与注释字面量 |
| DROP VIEW 多目标 | ViewStatement AST 现在保留多目标和 IF EXISTS，DROP VIEW 执行层删除目标列表正则，保留原权限、锁和原子校验流程 |
| View DDL 入口 | ViewStatement AST 已用于可解析的定义、选项和 CHECK OPTION；CREATE/ALTER VIEW 入口仍保留兼容检测，需继续迁移 |
| 窄窗口查询 | `ROW_NUMBER() OVER (ORDER BY ...)` 窄兼容入口改用 Select/FuncExpr/WindowSpec AST，保留现有排序和结果输出协议 |
| EXPLAIN 目标表 | EXPLAIN 的目标表和查询引用通过 ExplainStatement/对象引用 AST 获取，删除表名提取正则 |
| 临时表 CREATE | 临时表目标、`IF NOT EXISTS`、定义、LIKE 和 CTAS 通过 TableDDLStatement AST，删除 CREATE TEMPORARY TABLE 入口正则 |
| 临时表 DROP | `DROP TEMPORARY TABLE` 目标和 `IF EXISTS` 通过 TableDDLStatement AST，保留普通 DROP TABLE 混合目标的原有处理 |
| JDBC 与权限入口 | JDBC TABLE_CAT 列/别名通过 AST 查找；SET PASSWORD 自助改密权限判断删除解析失败后的正则回退 |
| 系统变量与用户变量 | `SELECT @@...`、`SET @...` 通过 SELECT/SET AST；AST 解析不足时只使用 sqlparser 原生 tokenizer，不再使用正则识别结构 |
| DDL 锁协调 | DML 表引用和加锁读通过 AST/tokenizer 读取；ALTER 表目标和 LOCK= 模式仍保留专用兼容正则，避免改变锁职责 |
| 账户策略选项 | 密码历史、密码复用、当前密码、密码过期、失败登录、资源限制和 TLS 选项直接从 AccountStatement.Options 读取，删除兼容正则回退 |
| 账户属性与注释 | USER ATTRIBUTE/COMMENT 读取 AccountStatement.Options，删除用户属性和账户注释提取正则 |
| 密码替换 | `ALTER USER ... REPLACE` 的旧密码读取通过 AccountStatement.Options，删除密码替换正则 |
| 当前用户显示 | `SHOW CREATE USER CURRENT_USER()` 通过归一化命令前缀和当前会话账户处理，删除固定命令正则 |
| SET PASSWORD 当前用户 | `SET PASSWORD FOR USER()/CURRENT_USER()` 通过归一化命令前缀和 AccountStatement 处理，删除固定目标正则 |
| ALTER USER 当前用户 | `ALTER USER USER()/CURRENT_USER()` 通过归一化命令前缀和当前会话账户处理，删除固定目标正则 |
| 用户认证选项 | `IDENTIFIED BY/WITH` 的插件和密码值通过 AccountStatement.Options，删除 CREATE/ALTER USER 认证结构正则 |
| View DEFINER 权限 | ViewStatement AST 提供 DEFINER 用户/主机，删除 View 权限入口的 Definer 正则扫描 |
| ALTER 顶层拆分与执行选项 | ALTER TABLE 表目标、复合操作约束识别、ALGORITHM=INSTANT 和执行选项跳过逻辑改用 AlterTable AST，保留原有元数据与回滚流程 |
| 触发器 NEW 赋值 | `SET NEW.column = expression` 通过 SET AST 读取；新增 SET NEW 兼容 AST 入口，移除 BEFORE/AFTER 触发器中的结构识别正则，保留 DML 中 NEW/OLD 值替换逻辑 |
| DDL 锁与临时表 DML | ALTER 表目标、LOCK 选项、INSERT/REPLACE/UPDATE/DELETE 临时表目标和普通 DROP 临时表目标通过 AST 判断，保留混合持久表语句的原有保护边界 |
| 存储对象 ALTER | ALTER PROCEDURE/FUNCTION/EVENT 的对象名、例程属性、事件启停和重命名通过 StoredObjectStatement AST，删除对应外层结构正则 |
| 递归 CTE | 锚点/递归成员的整数表达式、列绑定、递归 WHERE、CTE 头部和 DML 中的 IN/NOT IN 子查询通过 With/Select/ComparisonExpr/Subquery AST，物化值通过 ValTuple 注入 |
| SIGNAL | SIGNAL SQLSTATE 及 MESSAGE_TEXT、MYSQL_ERRNO 诊断项通过 StoredSignalStatement AST，移除执行层的信号结构正则 |
| 窗口单列投影 | 单个窗口函数、别名、来源表和排序通过 Select/FuncExpr/WindowSpec AST，删除窗口单列入口的结构正则 |
| 命名窗口 | WINDOW 声明和 `OVER window_name` 通过 Select/NamedWindow/WindowSpec AST 展开，删除命名窗口入口的结构正则 |
| EXTRACT | EXTRACT 的单位和表达式通过 ExtractExpr AST 读取，保留嵌套函数重写语义 |
| 存储过程游标 OPEN/CLOSE | OPEN/CLOSE 游标名称通过 StoredProgramStatement AST 读取，移除对应语句结构正则 |
| 存储过程动态 SQL | DEALLOCATE PREPARE 和 EXECUTE 名称/USING 子句通过 StoredProgramStatement 语句边界读取，移除对应结构正则 |
| 存储过程 FETCH | FETCH 游标名称和 INTO 目标列表通过语句边界读取，移除游标 FETCH 结构正则 |
| 相关 DML 赋值列 | UPDATE 单列/多列相关标量赋值的目标列通过 SELECT/ColName AST 校验 |
| CHECK 约束回退 | 谓词解析失败时通过 SQL tokenizer 读取标识符，移除 CHECK 引用检查回退正则 |
| 指标表引用回退 | SQL AST 无法解析时通过 sqlparser tokenizer 提取 FROM/JOIN/INTO/UPDATE 表名，移除指标回退正则 |
| TABLESPACE 管理语句 | CREATE/ALTER/DROP TABLESPACE 通过 TablespaceStatement AST 获取名称、重命名目标和选项 |
| HANDLER 入口 | HANDLER 表名、操作、别名、方向、条件和 LIMIT 通过 HandlerStatement AST 读取，保留索引查找语义 |
| HANDLER 索引查找 | 索引比较符和值通过 ComparisonExpr/ValTuple AST 读取，移除索引查找正则 |
| FULLTEXT 来源表 | FULLTEXT 查询来源表通过 SELECT AST 的 From 表达式读取 |
| 独立索引 DDL | CREATE INDEX/FULLTEXT INDEX/DROP INDEX 通过 IndexDDLStatement AST，删除三条独立索引入口正则 |
| 派生 EXISTS 相关引用 | 内层 SELECT 是否引用外层派生别名通过 ColName AST 限定名判断 |
| 相关 DML 标量范围 | 内层 SELECT 是否引用外层别名通过 ColName AST 限定名判断 |
| StoredProgram AST 边界 | BEGIN/END、IF/LOOP 等块在无法缩短输入时停止递归，避免大小写混合正文导致 AST 栈溢出 |
| 高级窗口入口 | ROW_NUMBER/RANK/DENSE_RANK 单表窗口查询通过 Select/FuncExpr/WindowSpec AST，删除入口正则 |
| RENAME USER | AccountStatement AST 保存源账户/目标账户对，删除多账户 RENAME USER 目标正则解析 |
| GRANT/REVOKE 目标 | 授权目标账户直接从 AccountStatement.Accounts 读取，删除 TO/FROM 目标尾部正则 |
| ALTER TABLE RENAME TO | AlterTable AST 新增 RENAME_TABLE 操作和目标 TableName，临时表及普通表重命名入口删除目标正则 |
| ALTER TABLE 表选项 | 字符集、排序规则、转换字符集、ROW_FORMAT 和 AUTO_INCREMENT 通过 AlterTableOperation 读取，删除对应固定结构正则 |
| ALTER TABLE 约束和默认值 | UNIQUE/PRIMARY/FOREIGN/CHECK 约束及列默认值通过 AlterTableOperation 读取，删除对应固定结构正则 |
| 账户授权和默认角色 | GRANT/REVOKE 权限范围、列授权、GRANT OPTION、ALL 和 SET DEFAULT ROLE 目标通过 AccountStatement/词法分组读取，删除对应固定结构正则 |
| View CREATE OR REPLACE | ViewStatement 新增 OrReplace 标记，执行层删除 CREATE OR REPLACE 前缀正则 |
| SHOW FULL COLUMNS/FIELDS | Show AST 支持 FULL COLUMNS/FIELDS 目标，执行层删除固定前缀正则 |
| View 显式列名 | View AST 的列列表转换为投影别名，保证查询和 SHOW CREATE VIEW 使用同一结构 |
| ALTER 单操作兼容入口 | add/drop/modify/change/rename column/index 及 index visibility 通过 AlterTable AST 映射，删除 parseSingleAlterOperation 的结构正则 |
| ALTER 索引专用入口 | 普通、FULLTEXT、SPATIAL 索引增删改名通过 AlterTable AST，删除索引专用匹配正则 |
| ALTER 列专用入口 | RENAME/DROP COLUMN、SET/DROP DEFAULT 通过 AlterTable AST，删除列操作专用匹配正则 |
| 存储函数调用入口 | SELECT 标量函数调用通过 Select/FuncExpr AST 获取函数名、参数和别名 |
| 存储例程元数据 | DECLARE 局部变量和 FUNCTION RETURN 通过 StoredProgramBlock AST 读取 |
| 存储程序局部 SET | 例程局部变量 SET 通过 Set AST 解析并执行 |
| 递归 CTE 单列物化 | 递归单列 CTE 的旧数值结构匹配由通用 Select/表达式 AST 路径接管 |
| View CTE 派生执行 | CTE 视图查询在执行时展开为 Select AST，保留诊断入口的 CTE 结构输出 |
| 递归 CTE INSERT SELECT | INSERT 目标、列列表和 SELECT 源通过 Insert AST 读取 |
| CTE 非 UNION 兼容判断 | WITH 头部边界通过字符串/括号词法读取，删除 CTE 头部结构正则 |
| Trigger ordering | FOLLOWS/PRECEDES 通过 StoredObject AST 读取并排序 |
| Trigger NEW 引用 | NEW.col 触发器表达式通过 ColName AST 读取，删除直接列名匹配正则 |
| Trigger body 边界 | FOR EACH ROW/FOLLOWS/PRECEDES 前缀通过词法边界处理 |
| 窗口排序边界 | Window ORDER BY 的 ROWS/RANGE 截断改为词法边界处理 |
| HANDLER OPEN 别名 | HANDLER 别名标识符验证改为逐字符检查 |
| HANDLER LIMIT | HANDLER LIMIT/offset 形式通过词法字段分组处理 |
| 相关子查询外层来源 | JOIN/派生表别名通过 TableExpr AST 收集，删除外层来源匹配正则 |
| 分区常量谓词 | BETWEEN/IN/NOT IN 和文本范围条件通过 RangeCond/ComparisonExpr AST 读取 |
| 分区维护操作 | REMOVE/EXCHANGE/REORGANIZE/ADD PARTITION 通过 AlterTable AST 读取 |
| 分区 DROP/TRUNCATE | DROP/TRUNCATE PARTITION 目标通过 AlterTable AST 读取 |
| SHOW CREATE 存储对象 | PROCEDURE/FUNCTION/TRIGGER/EVENT 目标通过 Show AST 读取 |
| 锁职责修正 | 通用语句 AST 锁仅覆盖 SELECT/DML；DDL、存储对象与显式锁由其专用处理器决定锁模式和依赖，避免 LOCK TABLES 先持有通用锁再等待自身 |

sqlparser 中扩展的语句节点仍包含由原生 tokenizer 构建的适配器；这不等于整个存储程序语法已由 yacc 完整覆盖。存储程序体、ALTER 等现有正则仍需逐项迁移。

## 新鲜验证

使用本机 Go 1.27.2，以下最新命令均退出 0：

- 完整 `go test ./server/dispatcher ./server/innodb/sqlparser -count=1 -timeout=90s`：dispatcher 2.618s，sqlparser 1.637s。
- 本轮新增变量/锁协调定向回归中，dispatcher/sqlparser 全包通过；变量、DML 表引用、ALTER 锁模式和 LOCK TABLES 相关定向检查通过。存储对象元数据锁等待仍按下文记录为失败。
- engine 组合定向回归：38.089s。覆盖 CTAS/临时 CTAS、建表索引可见性/独立及 ALTER 可见性、Procedure/StoredRoutine、CALL 与元数据、同步及并发事件 AST 测试、mysql/Processlist 谓词、分区路由表达式、WARNINGS、复制控制及权限、表维护及直方图、FLUSH、PERSIST、DISCARD/IMPORT、元数据锁等。
- CI 范围构建：`go list ./...` 后排除 cmd/client，再执行 `go build`，共 57 个包通过。
- `git diff --check` 无差异格式错误（Git 提示既有 CRLF 转换信息）。

LOCK TABLES 的会话写模式、多表锁和重置释放，以及 ANALYZE 等待等定向用例通过；存储对象 DDL、函数调用和 INFORMATION_SCHEMA.ROUTINES 的显式元数据锁等待在本次组合运行中仍出现 50 秒锁等待/测试超时，继续保留为未解决问题。新单测已复现通用入口错误地为 LOCK TABLES 申请语句锁，并验证该职责修正。

## 尚未解决的测试失败

没有声明完整 engine 测试通过。异步 `TestSQLEventExecutesDoStatementWhenSchedulerIsEnabled` 在单独 `-count=3` 重跑中仍失败：事件 INSERT 初始化 B+ 树时，替换 event_log.frm 文件遭到 Windows `Access is denied`。错误链为 `persistTableStorageIdentity -> writeMetadataFileAtomic -> os.Rename`。后续清理期间出现的 `btree manager is shutdown` 是另一个观测结果，不能把它当作唯一根因。

此前 `TestSQLEventProgramSummaryTracksExecutionResults` 和事件重命名注册组合也曾失败；最近组合运行未报告这两项失败，但尚未取得稳定重复通过证据。新增的受控调度和并发读取测试通过，不能据此替代上述异步测试的失败记录。没有通过恢复正则、跳过原测试或延长其超时掩盖此问题。

历史完整 engine、JDBC 存储程序/事件及跨版本兼容性等验收仍需按原计划继续验证。

## 下一步仍须实施

- executor.go 中的 ALTER、CTE/视图、表定义和 SQL 依赖改写。
- account_compatibility.go 的账户语句和安全属性，而不仅是已经改完的账户元数据过滤。
- stored_object_compatibility.go 中剩余存储程序体、DECLARE/HANDLER/SIGNAL/控制语句等解析。
- 分区维护、相关子查询、窗口、部分一般表空间/独立索引/临时表等结构匹配。
- executor.go 中 ALTER 兼容入口和存储程序体仍有结构正则；继续迁移时必须保持已修复的专用锁职责和旧 AST 缓存身份检查。
- 逐条核对全部残留及保留原因、处理异步事件 Windows 文件竞争失败，并完成原计划最后的验收项。

尚未提交、推送或部署；既有工作区变更保留。
