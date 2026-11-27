# SQL 正则残留逐调用位置清单

扫描时间：2026-11-05；排除 cmd、client；1086 个 Go 文件，12 个 regexp 导入文件，35 个直接调用位置，0 个 Go 解析异常。分类为 pattern_definition 15、syntax_candidate_or_value_match 9、value_match_or_escaping 11。逐位置明细以当前扫描 JSON 为准。

本清单统计 Go 源码中对 regexp 包函数的直接调用位置，包含编译、匹配和 QuoteMeta。同一条正则可能对应多个位置。残留位置必须按输入与消费者判断；其中 SQL LIKE/REGEXP、账户主机通配、JSON/空间值和时区处理是值语义，不属于 SQL 结构解析。

| 文件 | 行 | API | 所属函数 | API 分类 | 首个参数源码 |
|---|---:|---|---|---|---|
| server/auth/engine_access.go | 868 | QuoteMeta | func matchHost | value_match_or_escaping | `pattern` |
| server/auth/engine_access.go | 872 | MatchString | func matchHost | syntax_candidate_or_value_match | `regexPattern` |
| server/common/dynamic_privileges.go | 63 | MustCompile | package | pattern_definition | ```^[A-Z][A-Z0-9_]*$``` |
| server/innodb/engine/account_compatibility.go | 627 | QuoteMeta | func accountHostMatches | value_match_or_escaping | `string(char)` |
| server/innodb/engine/account_compatibility.go | 631 | MatchString | func accountHostMatches | syntax_candidate_or_value_match | `builder.String()` |
| server/innodb/engine/dml_result_compatibility.go | 177 | MustCompile | func zeroTemporalColumnValue | pattern_definition | ```^0{4}-0{2}-0{2}(?:[ T]0{2}:0{2}:0{2})?$``` |
| server/innodb/engine/dml_result_compatibility.go | 178 | MustCompile | func zeroTemporalColumnValue | pattern_definition | ```^(?:\d{4}-00-\d{2}\\|\d{4}-\d{2}-00)(?:[ T].*)?$``` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 1493 | MustCompile | func spatialBounds | pattern_definition | ```[-+]?[0-9]*\.?[0-9]+\s+[-+]?[0-9]*\.?[0-9]+``` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 1511 | MustCompile | func spatialPoint | pattern_definition | ```[-+]?[0-9]*\.?[0-9]+\s+[-+]?[0-9]*\.?[0-9]+``` |
| server/innodb/engine/select_join_executor.go | 522 | MatchString | func evalJoinComparisonTruth | syntax_candidate_or_value_match | `fmt.Sprint(right)` |
| server/innodb/engine/show_filter.go | 43 | QuoteMeta | func sqlLikeMatch | value_match_or_escaping | `string(r)` |
| server/innodb/engine/show_filter.go | 47 | Compile | func sqlLikeMatch | pattern_definition | `b.String()` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2289 | MatchString | func evalPredicateTruth | syntax_candidate_or_value_match | `fmt.Sprint(right)` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2425 | QuoteMeta | func sqlLikePatternMatchWithEscape | value_match_or_escaping | `string(r)` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2439 | QuoteMeta | func sqlLikePatternMatchWithEscape | value_match_or_escaping | `string(r)` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2443 | QuoteMeta | func sqlLikePatternMatchWithEscape | value_match_or_escaping | `"\\"` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2446 | Compile | func sqlLikePatternMatchWithEscape | pattern_definition | `b.String()` |
| server/innodb/manager/schema_manager.go | 1135 | MustCompile | func validateDatabaseName | pattern_definition | ```^[a-zA-Z0-9_$]+$``` |
| server/innodb/plan/expression.go | 1503 | MustCompile | func parseFixedTimezoneOffset | pattern_definition | ```^([+-])(\d{2})(?::?(\d{2}))?$``` |
| server/innodb/plan/expression.go | 1530 | MustCompile | func evalTimeConversionFunction | pattern_definition | ```^([+-]?)([0-9]+):([0-5][0-9]):([0-5][0-9])(?:\.([0-9]{1,6}))?$``` |
| server/innodb/plan/expression.go | 2440 | MustCompile | func evalNetworkAndDigestFunction | pattern_definition | ```(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$``` |
| server/innodb/plan/expression.go | 2967 | Compile | func compileMySQLRegexp | pattern_definition | `patternText` |
| server/innodb/plan/expression.go | 3589 | MatchString | func validateJSONSchemaCompatibility | syntax_candidate_or_value_match | `pattern` |
| server/innodb/plan/expression.go | 4479 | QuoteMeta | func jsonSearchPattern | value_match_or_escaping | `string(char)` |
| server/innodb/plan/expression.go | 4483 | QuoteMeta | func jsonSearchPattern | value_match_or_escaping | `escape` |
| server/innodb/plan/expression.go | 4486 | Compile | func jsonSearchPattern | pattern_definition | `builder.String()` |
| server/innodb/plan/expression.go | 5133 | MatchString | func evalRegexp | syntax_candidate_or_value_match | `fmt.Sprint(right)` |
| server/innodb/plan/expression.go | 5422 | MatchString | func evalLike | syntax_candidate_or_value_match | `pattern` |
| server/innodb/plan/expression.go | 5438 | MatchString | func evalLikeWithEscape | syntax_candidate_or_value_match | `pattern` |
| server/innodb/plan/expression.go | 5455 | QuoteMeta | func sqlLikePatternRegexWithEscape | value_match_or_escaping | `string(character)` |
| server/innodb/plan/expression.go | 5469 | QuoteMeta | func sqlLikePatternRegexWithEscape | value_match_or_escaping | `string(character)` |
| server/innodb/plan/expression.go | 5473 | QuoteMeta | func sqlLikePatternRegexWithEscape | value_match_or_escaping | `string(escapeCharacter)` |
| server/innodb/plan/expression.go | 5959 | MatchString | func Eval | syntax_candidate_or_value_match | `pattern` |
| server/observability/metrics/statement_digest.go | 11 | MustCompile | package | pattern_definition | ```::digest\d+\\|:digest\d+``` |
| server/replication/runtime.go | 2822 | MustCompile | package | pattern_definition | ```(?i)\berror\s+([0-9]{1,6})\b``` |
