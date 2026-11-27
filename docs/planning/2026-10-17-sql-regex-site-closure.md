# SQL 正则残留闭环清单

扫描时间：2026-11-05；排除 cmd、client；1086 个 Go 文件，12 个 regexp 导入文件，35 个直接调用位置，0 个 Go 解析异常。分类为 pattern_definition 15、syntax_candidate_or_value_match 9、value_match_or_escaping 11。逐位置明细以当前扫描 JSON 为准。

## 分类

- pattern_definition：15 个位置
- syntax_candidate_or_value_match：9 个位置
- value_match_or_escaping：11 个位置

## 处理结论

 - 本轮已将 JSON_VALUE RETURNING/ON EMPTY/ON ERROR 和 mysql.user 查询的 user/host 条件迁移到结构化解析。
 - 其余保留位置主要是正则值语义：LIKE/REGEXP 执行、账户主机通配、JSON/空间值、时区与网络摘要、动态权限名、数据库名校验和名称格式校验。

## 权威证据

- `docs/planning/2026-10-16-sql-regex-audit-ci-scope.json`
- `go test ./server/innodb/sqlparser ./server/innodb/engine -run JSONValue -count=1`
- `go test ./server/innodb/engine -run SelectExecutorMysqlUser -count=1`
