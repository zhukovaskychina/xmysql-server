# A/B/C 正则候选处理闭环

本报告与 `docs/planning/2026-10-17-sql-regex-site-closure.md` 配套。当前 CI 范围扫描为 603 个站点：pattern_definition 518、syntax_candidate_or_value_match 12、value_match_or_escaping 73，0 个解析异常。

## A 类

A 类候选逐站点记录在逐项清单中。已迁移的 SQL 结构识别使用 ParseForExecution/AST；仍保留的模式属于兼容边界，均在对应源码函数和验证报告中注明。新一轮扫描没有新增 SQL 正则或手写 SQL 字段解析。

## B 类

原整改文档定义的 25 个 B 类位置继续保留：主机模式、权限/数据库名校验、SQL REGEXP/LIKE/JSON/时间值处理、SHOW/元数据 LIKE 值匹配、时间格式和摘要占位符。它们处理已经解析出的值或外部格式，不承担 SQL 结构识别。

## C 类

原整改文档定义的 7 个 C 类位置均有对应处理：视图 DEFINER 结构化账户、存储过程 errno/SQLSTATE 类型化优先、复制错误类型化优先，以及空间 WKT 留在空间值解析边界。无法提供结构化外部错误时保留明确兼容回退。

## 证据

- 逐站点清单：`docs/planning/2026-10-17-sql-regex-site-closure.md`
- 原 B/C 位置：`docs/planning/2026-10-16-sqlparser-remediation-plan.md` 第 5.1/5.2 节
- 最新扫描：`docs/planning/2026-10-16-sql-regex-audit-ci-scope.json`
