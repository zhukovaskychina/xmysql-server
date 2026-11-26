# MySQL 8.4 查询差分验收

该可选测试将同一批 SQL 分别发给本分支的临时 XMySQL 服务和指定的本地 MySQL 8.4 实例，覆盖普通查询和原生预处理协议。结果值、NULL、重复行、显式排序、列名、协议列类型及错误码/SQLSTATE 分别比较。当前源码定义 50 条 SQL、100 个执行用例；最新实时结果中 92 个成功查询结果完全匹配，8 个预期错误的 errno/SQLSTATE 匹配。它不代表 MySQL 全量特性或性能对标。

测试入口：`TestMySQLDriverQueryCompatibility/mysql84_reference_comparison`。未设置参考实例地址时明确跳过；启用后连接失败、版本不符或差异均使测试失败。参考实例必须通过显式回环 IP 访问，并已有 `wire_compat` 数据库。测试在两端创建带本次唯一前缀的八张表，只清理自己成功创建的表。

PowerShell 示例（参考实例由测试者另行启动）：

```powershell
$env:XMYSQL_TEST_MYSQL84_ADDR = '127.0.0.1:<参考实例端口>'
$env:XMYSQL_TEST_MYSQL84_USER = '<测试用户>'
$env:XMYSQL_TEST_MYSQL84_PASSWORD = '<测试口令>'
$env:XMYSQL_TEST_MYSQL84_REPORT = Join-Path $env:TEMP 'xmysql-mysql84-comparison.json'
go test -mod=readonly -p 1 ./server/net `
  -run '^TestMySQLDriverQueryCompatibility$/^mysql84_reference_comparison$' `
  -count=1 -timeout=4m
```

JSON 报告保留参考版本，以及每条 SQL 在两端的列名、类型、原始结果和错误信息。它不会记录连接口令。报错用例比较 MySQL 错误码与 SQLSTATE，不要求厂商错误文案相同。成功用例严格比较结果与协议类型，不通过归一化协议类型掩盖差异。

## 2026-11-26 首轮结果

参考实例为任务专用 Docker 容器，实际 `SELECT VERSION()` 为 **8.4.13**；本地镜像 ID 为 `sha256:8c233bad33d520a7aae1c1e619c3a9775f4a1ba2d463d692fada04ec7f40be00`。连接只映射回环地址，没有连接用户现有数据库。

36 个用例中，34 个成功查询的值、列名及顺序一致；另外 2 个标量子查询多行错误的错误码和 SQLSTATE 一致。**8 个用例仍因协议列类型差异失败**，普通查询和预处理各出现一次：

| SQL 形态 | 当前 XMySQL | MySQL 8.4.13 | 后续修正位置 |
| --- | --- | --- | --- |
| `k <=> NULL` 投影 | `TINYINT` | `BIGINT` | SQL 谓词结果类型推断；不改变真实 TINYINT 列或原生检索标志列 |
| 相关标量子查询投影 | `VARCHAR,VARCHAR` | `INT,INT` | 相关投影保留外层列与标量子查询结果类型 |
| CTE 的普通 INT 列投影 | `BIGINT,BIGINT` | `INT,INT` | 物化结果传递声明类型，避免按 Go int64 猜测 |
| 单个派生表的普通 INT 列投影 | `BIGINT,BIGINT` | `INT,INT` | 简单派生表投影保留输入列类型 |

原始日志、报告及实例身份位于 `C:/Users/zx187/AppData/Local/Temp/xmysql-native-table-schema-e1089b3c5e1c48dd985f091223a43f9e/`。这个失败结果是新的修复基线；已有原生多模态测试通过不能覆盖或替代它。

## 2026-11-26 类型与列名修正后的扩展对照（历史结果，随后已修复）

原 36 个用例中的 8 项差异已修正。CTE 和单个派生表使用输入结果的声明类型，不再按 Go int64 推断；相关标量投影从 SQL/表目录取得可解析的列名和类型，普通列名去掉表限定符，显式别名保留。空输入、首行 NULL 和隐藏排序辅助列不再决定可解析输出列的类型。普通谓词及 TRUE/FALSE 报告 BIGINT，真实 TINYINT 列和原生向量检索标志列继续保留 TINYINT。

扩展到 52 个用例后，当前 **50 个通过，2 个仍失败**。这两个失败对应同一条排序 JOIN 在普通协议和预处理协议下的列类型：XMySQL 返回 BIGINT，参考库返回 INT；值和列名相同。

独立参考诊断把同一行依赖谓词分别用于有排序和无排序的 JOIN：MySQL CLI 列元数据分别为 LONG 与 LONGLONG；排序计划的 EXPLAIN JSON 显示 `using_temporary_table=true`、`using_filesort=true`。该差异与参考库的物化字段类型一致。当前没有为了该 SQL 形态增加类型特判，也没有将 INT/BIGINT 在测试中归一化为通过；物化结果类型契约仍是待处理项。

扩展用例还覆盖物理 TINYINT 与谓词同时投影、多个谓词算子、空 CTE、空派生表、空外层相关查询，以及相关子查询的 NULL 首行。当前报告和参考诊断位于 `C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-type-parity-3e40953543c746f4a1ecd10af57c7283/`，其中最终 `current-report.json`、`final-client.jsonl` 和 `reference-plan-probe.log` 记录上述结果。最终客户端整包启用了真实对照和 Python/JDBC：只在这 2 个差异用例及其父测试组失败，因此整包验收仍未通过。参考实例已停止，测试夹具已恢复。

## 2026-11-26 排序 JOIN 类型差异已修复

修复根据 AST 中是否存在 JOIN 与 ORDER BY 决定谓词投影类型，并同时应用于执行响应和 schema-only PREPARE 描述。MySQL 8.4.13 对有排序的 JOIN 布尔投影返回 `INT`；无排序 JOIN 和单表谓词仍返回 `BIGINT`。新用例验证 `=`, `IS NULL`, `<`，并分别按源列、投影别名、序号和投影列排序，覆盖升序、降序及 LIMIT。

最新 68 项差分（34 条 SQL × 2 种协议）全部通过：66 个成功结果的值/NULL/列名/顺序/类型相同，2 个标量子查询基数错误均为 1242 / `21000`。定向引擎测试与启用 MySQL 参考实例的 TCP 驱动差分通过。报告位于 `C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-followup-final.json`，运行日志位于同目录 `.log`。这关闭的是当前有限矩阵中的差异，不代表全部 MySQL 能力已追平。

## 2026-11-26 瑞典语 locale 排序规则修正

新增 `utf8mb4_swedish_ci` 查询用例后，首次差分复现了两项错误：ORDER BY 将 `å/ä` 排到 `a` 前，`COUNT(DISTINCT)` 把 `a/å/ä` 合并为三个基础字母。现已对该目录支持的 Swedish collation 使用 Swedish locale 比较权重，DISTINCT 键保留 `å/ä/ö` 的独立权重，同时维持大小写不敏感和 PAD SPACE。

最新矩阵共 72 项（36 条 SQL × 2 种协议）：70 个成功结果全部相同，两项标量子查询错误码/SQLSTATE 也相同。定向 SQL 回归和启用参考 MySQL 的客户端差分通过。报告：`C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-collation-final.json`。其他 locale、完整 UCA 版本权重和混合排序规则 coercibility 尚未完成。

## 2026-11-26 German DIN-2 排序规则扩展

`utf8mb4_german2_ci` 现在按 MySQL DIN-2 规则归一化 `ä/ö/ü/ß` 为 `ae/oe/ue/ss`；Unicode German2 collations 还将 `æ/œ` 归一化为 `ae/oe`，而 `latin1_german2_ci` 保留 MySQL 的不同等价规则。官方说明见 [MySQL Unicode Character Sets](https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html)。

新增排序与 DISTINCT SQL 后，差分矩阵为 38 条 SQL × 2 种协议，共 76 项；74 个成功结果完全匹配，2 个预期错误匹配错误码和 SQLSTATE。定向 engine 测试通过，实时 MySQL 8.4.13 驱动差分通过。证据：`C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-german2-final.json`。其他 locale/版本的完整权重仍未覆盖。

## 2026-11-26 latin1 German2 等价规则复核

补充覆盖 `latin1_german2_ci`，确保与 Unicode German2 区分：`Ä=AE`、`Ö=OE`、`ß=ss`，但 `Æ` 不等于 `AE`、`Œ` 不等于 `OE`。ORDER BY 和 DISTINCT 在普通、原生预处理两种协议均与 MySQL 8.4.13 一致。最终矩阵为 40 条 SQL × 2 种协议 = 80 项，78 个成功结果一致，2 个预期错误一致。证据：`C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-german2-latin-final.json`。

## 2026-11-26 混合排序规则 coercibility 本地回归

新增六条实时差分 SQL，覆盖同字符集不兼容隐式排序规则报 1267、两个显式 COLLATE 冲突报 1267、显式规则优先、同字符集 `_bin` 优先、Unicode 字符集优先，以及 JOIN 谓词中的隐式冲突。矩阵扩大至 46 条 SQL × 两种协议 = 92 项。MySQL 8.4.13 实时对照全部通过：84 个成功查询的值、列名、类型和排序一致，8 个预期错误的错误码与 SQLSTATE 一致。

本地 engine 回归 `TestMySQLExplicitCollationConflictIsRejectedInProjection` 覆盖 SELECT 投影和 JOIN 谓词中的显式及隐式冲突 1267 传播，以及 `_bin` 和 Unicode 选择规则；全量 `server/innodb/engine` 测试以 `-count=1` 通过（426.963s，早于最后加入的 JOIN 用例），JOIN 用例另行定向通过。最终实时 MySQL 8.4.13 差分通过。报告：`C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-coercibility-20261126.json`。

## 2026-11-26 相关 IN/EXISTS 投影与 HAVING 验收

扩充差分矩阵，增加相关 `IN` 投影、相关 `EXISTS` 投影、JOIN 后的相关 `EXISTS` WHERE 和分组 HAVING 相关 `EXISTS`。首轮实时对照复现 3 类差异：XMySQLEngine 协议执行路由漏调相关集合子查询处理器；`EXISTS` AST 未被识别为布尔投影；带 HAVING 子查询的旧重写器错误改写分组查询并把语句变成 `ORDER BY` 前语法错误。修正路由与布尔分类后，`HAVING` 子查询继续由分组后 AST 执行器计算；为此查询暂不构建通用逻辑计划，因为计划表达式层尚未表示子查询过滤器。

最新矩阵共 50 条 SQL × 普通/预处理协议 = 100 项。MySQL 8.4.13 实时差分全部通过：92 个成功结果在列名、类型、值及顺序上匹配，8 个预期错误在 errno/SQLSTATE 上匹配。报告：`C:/Users/zx187/AppData/Local/Temp/xmysql-mysql84-correlated-verified-20261126.json`。本地 `TestMySQLQueryCompatibilitySemanticMatrix` 通过；完整 engine 套件需针对这轮路由与 HAVING 变更重新运行。该结果验证这些子查询形状，不代表聚合混合表达式、复杂嵌套相关作用域或完整 MySQL 查询兼容。

## 2026-11-26 错误结果元数据严格对照

将错误场景的元数据存在性也纳入比较，并用新鲜的本地 MySQL 8.4.13 临时容器重新运行 50 条 SQL × 两种协议。100 项全部通过：92 个成功查询匹配列名、类型、值及顺序；2 个标量子查询基数错误匹配 1242/21000 并保留列名/类型；6 个排序规则冲突匹配 1267/HY000 且双方均不返回结果元数据。对应普通 MySQL 驱动回归与完整 `server/net` 包通过。报告：`%TEMP%/xmysql-mysql84-current-20261126-final.json`。该结果仅覆盖矩阵列出的错误形态。
