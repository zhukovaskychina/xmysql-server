# SQL 正则残留分类（自动扫描基线）

生成时间：2026-10-17（CI 范围复扫）

本文件只描述扫描结果和分类规则，不把正则存在本身判定为缺陷。字符串 LIKE/REGEXP 值匹配、密码/主机模式校验、外部 WKT 或协议兼容边界仍需按调用点单独审查。

扫描 Go 文件：1072
regexp 导入文件：73
扫描位置：603
解析异常：0（排除 `cmd`、`client` 后）

## 分类计数

- pattern_definition: 518
- syntax_candidate_or_value_match: 12
- value_match_or_escaping: 73

## 当前处理规则
- `pattern_definition`：只登记正则模式声明，不能仅凭声明判定需要 AST 迁移。
- `value_match_or_escaping`：通常属于 LIKE/主机/值校验或 QuoteMeta，保留候选需有输入值边界证据。
- `syntax_candidate_or_value_match`：需要逐调用点确认是否仍解析 SQL 语法；已接入 AST 的调用应在执行入口记录证据。
- 解析异常必须单独修复或列明文件范围，不能把扫描成功当成完整迁移。

## 逐位置复核

CI 范围复扫的 12 个 `syntax_candidate_or_value_match` 均为值语义或兼容边界，不是 SQL 结构识别：

| 位置 | 用途结论 |
|---|---|
| `server/auth/engine_access.go:872` | 账户主机模式匹配，输入是已解析的 host 值 |
| `server/dispatcher/system_variable_engine.go:748` | 元数据 LIKE 模式匹配，输入是字段值 |
| `server/innodb/engine/account_compatibility.go:687` | 账户主机模式匹配，保留 `%`/`_` 语义 |
| `server/innodb/engine/executor.go:18171` | 元数据 LIKE 模式匹配，输入是字段值 |
| `server/innodb/engine/innodb_metrics_compatibility.go:902` | 指标名称模式匹配 |
| `server/innodb/engine/select_join_executor.go:522` | 连接条件值比较 |
| `server/innodb/engine/storage_integrated_dml_helper.go:2289` | 已求值谓词的三值逻辑比较 |
| `server/innodb/plan/expression.go:3589` | JSON Schema 值校验 |
| `server/innodb/plan/expression.go:5133` | SQL `REGEXP` 运算符的值匹配 |
| `server/innodb/plan/expression.go:5422` | SQL `LIKE` 运算符的值匹配 |
| `server/innodb/plan/expression.go:5438` | SQL `LIKE ... ESCAPE` 的值匹配 |
| `server/innodb/plan/expression.go:5959` | 表达式求值中的正则值匹配 |

这些位置不读取 SQL 关键字、对象名、参数或语句边界，因此不应替换为 `sqlparser`；扫描器没有发现 CI 范围内未分类的 SQL 语法正则。

## 未完成边界
- 默认全量扫描仍可包含 `cmd`、`client` 之外的历史演示源；CI 范围本次已无解析异常。
- 完整账户、视图、复制、存储程序运行时切换和 JDBC/重启验收仍未完成。
- 该文件不声明全量完成。

- Scanner now accepts `-exclude` directory names. CI-scoped run `go run ./scripts/sql-regex-audit -root . -exclude cmd,client -output docs/planning/2026-10-16-sql-regex-audit-ci-scope.json` produces 73 regexp-import files, 603 sites and zero parse errors; default full scan remains available and still reports legacy cmd parse errors.

- After migrating SystemVariableAnalyzer query detection to AST, CI-scope scanner dropped to 73 regexp-import files and 603 sites (12 syntax/value MatchString candidates, 0 parse errors). The remaining candidates are host/value matching and SQL REGEXP/LIKE semantics, requiring their own non-AST treatment.





