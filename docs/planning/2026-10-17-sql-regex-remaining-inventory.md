# SQL 正则残留逐调用位置清单

扫描时间：2026-10-18；排除 cmd、client；1077 个 Go 文件，28 个 regexp 导入文件，150 个直接调用位置，0 个 Go 解析异常。分类为 pattern_definition 99、syntax_candidate_or_value_match 9、value_match_or_escaping 42。逐位置明细以当前扫描 JSON 为准。

本清单统计 Go 源码中对 regexp 包函数的直接调用位置，包含编译、匹配和 QuoteMeta。同一条正则可能对应多个位置，实例方法调用未单独计数。

`pattern_definition` 仅说明调用了 Compile/MustCompile，不能证明该正则无需迁移；`value_match_or_escaping` 也只反映 API 特征，其中包含 SQL 对象/依赖改写，仍需按输入和消费者复核。当前仍有 SQL 结构正则，尚未全部迁移。

| 文件 | 行 | API | 所属函数 | API 分类 | 首个参数源码 |
|---|---:|---|---|---|---|
| server/auth/engine_access.go | 868 | QuoteMeta | func matchHost | value_match_or_escaping | `pattern` |
| server/auth/engine_access.go | 872 | MatchString | func matchHost | syntax_candidate_or_value_match | `regexPattern` |
| server/common/dynamic_privileges.go | 63 | MustCompile | package | pattern_definition | ``^[A-Z][A-Z0-9_]*$`` |
| server/innodb/engine/account_compatibility.go | 556 | MustCompile | func accountTargetsFromQuery | pattern_definition | ``(?is)'([^']*)'\s*@\s*'([^']*)'`` |
| server/innodb/engine/account_compatibility.go | 633 | QuoteMeta | func accountHostMatches | value_match_or_escaping | `string(char)` |
| server/innodb/engine/account_compatibility.go | 637 | MatchString | func accountHostMatches | syntax_candidate_or_value_match | `builder.String()` |
| server/innodb/engine/correlated_derived_compatibility.go | 28 | MustCompile | func executeCorrelatedDerivedOuterCompatibility | pattern_definition | ``(?is)\b(not\s+)?exists\s*\(`` |
| server/innodb/engine/correlated_derived_compatibility.go | 131 | MustCompile | func executeCorrelatedDerivedOuterCompatibility | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`"))` |
| server/innodb/engine/correlated_derived_compatibility.go | 131 | QuoteMeta | func executeCorrelatedDerivedOuterCompatibility | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_derived_compatibility.go | 131 | QuoteMeta | func executeCorrelatedDerivedOuterCompatibility | value_match_or_escaping | `strings.Trim(column, "`")` |
| server/innodb/engine/correlated_dml_compatibility.go | 459 | MustCompile | func rewriteCorrelatedDMLPredicate | pattern_definition | ``(?is)^(.+?)\s+(not\s+in&#124;in)\s*\(`` |
| server/innodb/engine/correlated_dml_compatibility.go | 474 | MustCompile | func rewriteCorrelatedDMLPredicate | pattern_definition | ``(?i)` + regexp.QuoteMeta(target.alias) + `\s*\.`` |
| server/innodb/engine/correlated_dml_compatibility.go | 474 | QuoteMeta | func rewriteCorrelatedDMLPredicate | value_match_or_escaping | `target.alias` |
| server/innodb/engine/correlated_predicate_compatibility.go | 51 | MustCompile | func executeCorrelatedPredicateSubqueryCompatibility | pattern_definition | `"(?is)^([a-zA-Z0-9_$]+(?:\\.[a-zA-Z0-9_$]+)?)(?:\\s+(?:as\\s+)?([a-zA-Z0-9_$]+))?"` |
| server/innodb/engine/correlated_predicate_compatibility.go | 67 | MustCompile | func executeCorrelatedPredicateSubqueryCompatibility | pattern_definition | ``(?is)([a-zA-Z0-9_$\.]+)\s*(not\s+in&#124;in&#124;(?:<=>&#124;<>&#124;!=&#124;<=&#124;>=&#124;=&#124;<&#124;>)\s*(?:any&#124;some&#124;all))\s*\(`` |
| server/innodb/engine/correlated_predicate_compatibility.go | 221 | MustCompile | func executeCorrelatedPredicateSubqueryCompatibility | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`"))` |
| server/innodb/engine/correlated_predicate_compatibility.go | 221 | QuoteMeta | func executeCorrelatedPredicateSubqueryCompatibility | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_predicate_compatibility.go | 221 | QuoteMeta | func executeCorrelatedPredicateSubqueryCompatibility | value_match_or_escaping | `strings.Trim(column, "`")` |
| server/innodb/engine/correlated_predicate_multiple_compatibility.go | 50 | MustCompile | func executeMultipleCorrelatedPredicateSubqueriesCompatibility | pattern_definition | `"(?is)^([a-zA-Z0-9_$]+(?:\\.[a-zA-Z0-9_$]+)?)(?:\\s+(?:as\\s+)?([a-zA-Z0-9_$]+))?"` |
| server/innodb/engine/correlated_predicate_multiple_compatibility.go | 72 | MustCompile | func executeMultipleCorrelatedPredicateSubqueriesCompatibility | pattern_definition | ``(?is)^\s*([a-zA-Z0-9_$\.]+)\s*(not\s+in&#124;in&#124;(?:<=>&#124;<>&#124;!=&#124;<=&#124;>=&#124;=&#124;<&#124;>)\s*(?:any&#124;some&#124;all))\s*\(`` |
| server/innodb/engine/correlated_predicate_multiple_compatibility.go | 73 | MustCompile | func executeMultipleCorrelatedPredicateSubqueriesCompatibility | pattern_definition | ``(?is)^\s*(not\s+)?exists\s*\(`` |
| server/innodb/engine/correlated_predicate_multiple_compatibility.go | 209 | MustCompile | func executeMultipleCorrelatedPredicateSubqueriesCompatibility | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`"))` |
| server/innodb/engine/correlated_predicate_multiple_compatibility.go | 209 | QuoteMeta | func executeMultipleCorrelatedPredicateSubqueriesCompatibility | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_predicate_multiple_compatibility.go | 209 | QuoteMeta | func executeMultipleCorrelatedPredicateSubqueriesCompatibility | value_match_or_escaping | `strings.Trim(column, "`")` |
| server/innodb/engine/correlated_scalar_compatibility.go | 313 | MustCompile | func executeCorrelatedScalarSubqueryCompatibility | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`"))` |
| server/innodb/engine/correlated_scalar_compatibility.go | 313 | QuoteMeta | func executeCorrelatedScalarSubqueryCompatibility | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_scalar_compatibility.go | 313 | QuoteMeta | func executeCorrelatedScalarSubqueryCompatibility | value_match_or_escaping | `strings.Trim(column, "`")` |
| server/innodb/engine/correlated_scalar_compatibility.go | 507 | MustCompile | func evaluateCorrelatedScalarPredicate | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(column, "`"))` |
| server/innodb/engine/correlated_scalar_compatibility.go | 507 | QuoteMeta | func evaluateCorrelatedScalarPredicate | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_scalar_compatibility.go | 507 | QuoteMeta | func evaluateCorrelatedScalarPredicate | value_match_or_escaping | `strings.Trim(column, "`")` |
| server/innodb/engine/correlated_scalar_compatibility.go | 926 | MustCompile | func correlatedAggregateExpression | pattern_definition | ``(?is)^\s*(count&#124;sum&#124;avg&#124;min&#124;max)\s*\((.*)\)\s*$`` |
| server/innodb/engine/correlated_scalar_compatibility.go | 959 | MustCompile | func parseCorrelatedOuterSource | pattern_definition | `"(?is)^([a-zA-Z0-9_$]+(?:\\.[a-zA-Z0-9_$]+)?)(?:\\s+(?:as\\s+)?([a-zA-Z0-9_$]+))?"` |
| server/innodb/engine/correlated_subquery_compatibility.go | 22 | MustCompile | func executeCorrelatedSubqueryCompatibility | pattern_definition | ``(?is)\b(not\s+)?exists\s*\(`` |
| server/innodb/engine/correlated_subquery_compatibility.go | 37 | MustCompile | func executeCorrelatedSubqueryCompatibility | pattern_definition | ``(?is)\s+where(?:\s&#124;$)`` |
| server/innodb/engine/correlated_subquery_compatibility.go | 53 | MustCompile | func executeCorrelatedSubqueryCompatibility | pattern_definition | `"(?is)^([a-zA-Z0-9_$]+)(?:\\s+([a-zA-Z0-9_$]+))?"` |
| server/innodb/engine/correlated_subquery_compatibility.go | 104 | MustCompile | func executeCorrelatedSubqueryCompatibility | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.`` |
| server/innodb/engine/correlated_subquery_compatibility.go | 104 | QuoteMeta | func executeCorrelatedSubqueryCompatibility | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_subquery_compatibility.go | 209 | MustCompile | func executeCorrelatedSubqueryCompatibility | pattern_definition | ``(?i)` + regexp.QuoteMeta(outerAlias) + `\s*\.\s*` + regexp.QuoteMeta(strings.Trim(outerColumn, "`"))` |
| server/innodb/engine/correlated_subquery_compatibility.go | 209 | QuoteMeta | func executeCorrelatedSubqueryCompatibility | value_match_or_escaping | `outerAlias` |
| server/innodb/engine/correlated_subquery_compatibility.go | 209 | QuoteMeta | func executeCorrelatedSubqueryCompatibility | value_match_or_escaping | `strings.Trim(outerColumn, "`")` |
| server/innodb/engine/correlated_subquery_compatibility.go | 298 | MustCompile | func correlatedOuterSourceSQL | pattern_definition | `"(?i)\\b([a-zA-Z0-9_$]+)\\s*\\.\\s*([a-zA-Z0-9_$]+)"` |
| server/innodb/engine/dml_result_compatibility.go | 176 | MustCompile | func zeroTemporalColumnValue | pattern_definition | ``^0{4}-0{2}-0{2}(?:[ T]0{2}:0{2}:0{2})?$`` |
| server/innodb/engine/dml_result_compatibility.go | 177 | MustCompile | func zeroTemporalColumnValue | pattern_definition | ``^(?:\d{4}-00-\d{2}&#124;\d{4}-\d{2}-00)(?:[ T].*)?$`` |
| server/innodb/engine/executor.go | 3875 | MustCompile | func persistAlterTableRowFormat | pattern_definition | ``(?is)\brow_format\s*=\s*(?:dynamic&#124;compact&#124;compressed&#124;redundant&#124;default)`` |
| server/innodb/engine/executor.go | 4884 | MustCompile | func renameSQLIdentifier | pattern_definition | ``(?i)(^&#124;[^a-zA-Z0-9_$])` + regexp.QuoteMeta(oldName) + `([^a-zA-Z0-9_$]&#124;$)`` |
| server/innodb/engine/executor.go | 4884 | QuoteMeta | func renameSQLIdentifier | value_match_or_escaping | `oldName` |
| server/innodb/engine/executor.go | 5005 | MustCompile | func rewriteForeignKeyReferenceTable | pattern_definition | ``(?i)(references\s+)(?:\x60?` + regexp.QuoteMeta(oldDB) + `\x60?\s*\.\s*)?\x60?` + regexp.QuoteMeta(oldTable) + `\x60?`` |
| server/innodb/engine/executor.go | 5005 | QuoteMeta | func rewriteForeignKeyReferenceTable | value_match_or_escaping | `oldDB` |
| server/innodb/engine/executor.go | 5005 | QuoteMeta | func rewriteForeignKeyReferenceTable | value_match_or_escaping | `oldTable` |
| server/innodb/engine/executor.go | 7105 | MustCompile | func bindJoinOuterReferences | pattern_definition | ``(?i)\b` + regexp.QuoteMeta(reference.qualifier) + `\s*\.\s*` + regexp.QuoteMeta(reference.column) + `\b`` |
| server/innodb/engine/executor.go | 7105 | QuoteMeta | func bindJoinOuterReferences | value_match_or_escaping | `reference.qualifier` |
| server/innodb/engine/executor.go | 7105 | QuoteMeta | func bindJoinOuterReferences | value_match_or_escaping | `reference.column` |
| server/innodb/engine/executor.go | 7925 | MustCompile | func rewriteSimpleInSubquery | pattern_definition | ``(?is)([a-zA-Z0-9_$\.]+)\s+(not\s+)?in\s*\(\s*(select\s+[^()]+)\s*\)`` |
| server/innodb/engine/executor.go | 7983 | MustCompile | func rewriteSimpleQuantifiedSubqueries | pattern_definition | ``(?is)([a-zA-Z0-9_$\.]+)\s*(<=>&#124;<>&#124;!=&#124;<=&#124;>=&#124;=&#124;<&#124;>)\s*(any&#124;some&#124;all)\s*\(`` |
| server/innodb/engine/executor.go | 22075 | MustCompile | func initializeCreateTableAutoIncrement | pattern_definition | ``(?is)\bauto_increment\s*=\s*([0-9]+)`` |
| server/innodb/engine/executor.go | 23384 | MustCompile | func parseCreateTableForeignKeysFallback | pattern_definition | `"(?is)^\\s*(?:constraint\\s+`?([a-zA-Z0-9_$]+)`?\\s+)?foreign\\s+key\\s*\\(([^)]+)\\)\\s+references\\s+(?:(?:`?([a-zA-Z0-9_]+)`?)\\.)?`?([a-zA-Z0-9_]+)`?\\s*\\(([^)]+)\\)(.*)$"` |
| server/innodb/engine/executor.go | 23418 | MustCompile | func cascadeActionFromTail | pattern_definition | ``(?i)\bon\s+` + regexp.QuoteMeta(action) + `\s+(restrict&#124;cascade&#124;set\s+null&#124;no\s+action)\b`` |
| server/innodb/engine/executor.go | 23418 | QuoteMeta | func cascadeActionFromTail | value_match_or_escaping | `action` |
| server/innodb/engine/executor.go | 23427 | MustCompile | func validateForeignKeyReferentialActions | pattern_definition | ``(?i)\bon\s+(delete&#124;update)\b`` |
| server/innodb/engine/executor.go | 23580 | MustCompile | func extractFallbackGeneratedExpression | pattern_definition | ``(?is)\b(?:generated\s+always\s+)?as\s*\((.+)\)\s*(?:stored&#124;virtual)?\s*$`` |
| server/innodb/engine/executor.go | 23877 | MustCompile | func parseTableOptionValue | pattern_definition | ``(?is)\b` + keyPattern + `\s*(?:=\s*)?([a-zA-Z0-9_]+)`` |
| server/innodb/engine/executor.go | 23885 | MustCompile | func parseTableCommentOption | pattern_definition | ``(?is)\bcomment\s*(?:=\s*)?(?:'((?:''&#124;[^'])*)'&#124;"((?:""&#124;[^"])*)")`` |
| server/innodb/engine/executor.go | 26364 | MustCompile | func rewriteSimpleCTEQuery | pattern_definition | ``(?is)^([a-zA-Z0-9_$]+)(?:\s*\(([^)]*)\))?\s+as\s*\(`` |
| server/innodb/engine/executor.go | 26423 | MustCompile | func rewriteSimpleCTEQuery | pattern_definition | ``(?is)^select\s+\*\s+from\s+([a-zA-Z0-9_$]+)\s*$`` |
| server/innodb/engine/executor.go | 26437 | MustCompile | func rewriteSimpleCTEQuery | pattern_definition | ``(?is)^select\s+\*\s+from\s+([a-zA-Z0-9_$]+)(.*)$`` |
| server/innodb/engine/executor.go | 26478 | MustCompile | func rewriteCTEInsertQuery | pattern_definition | ``(?is)^insert\s+into\s+(.+?)\s+select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?\s*;?$`` |
| server/innodb/engine/executor.go | 26486 | MustCompile | func rewriteCTEInsertQuery | pattern_definition | ``(?is)^select\s+.+?\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?\s*$`` |
| server/innodb/engine/executor.go | 26553 | MustCompile | func replaceCTESourceReferences | pattern_definition | ``(?is)(\b(?:from&#124;join)\s+)` + regexp.QuoteMeta(name) + `\b(?:\s+(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_$]*))?`` |
| server/innodb/engine/executor.go | 26553 | QuoteMeta | func replaceCTESourceReferences | value_match_or_escaping | `name` |
| server/innodb/engine/executor.go | 26616 | MustCompile | func rewriteCTEDMLQuery | pattern_definition | ``(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?$`` |
| server/innodb/engine/executor.go | 26620 | MustCompile | func rewriteCTEDMLQuery | pattern_definition | ``(?is)\(\s*select\s+(.+?)\s+from\s+` + regexp.QuoteMeta(name) + `\s*\)`` |
| server/innodb/engine/executor.go | 26620 | QuoteMeta | func rewriteCTEDMLQuery | value_match_or_escaping | `name` |
| server/innodb/engine/executor.go | 26637 | MustCompile | func rewriteMaterializedCTEQuery | pattern_definition | ``(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?(?:\s+order\s+by\s+(.+?))?(?:\s+limit\s+(.+?))?$`` |
| server/innodb/engine/executor.go | 26645 | MustCompile | func rewriteMaterializedCTEQuery | pattern_definition | ``(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?$`` |
| server/innodb/engine/executor.go | 26677 | MustCompile | func rewriteCTEJoinQuery | pattern_definition | ``(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_$]+))?\s+join\s+(.+)$`` |
| server/innodb/engine/executor.go | 26686 | MustCompile | func rewriteCTEJoinQuery | pattern_definition | ``(?is)^select\s+(.+?)\s+from\s+([a-zA-Z0-9_$]+)(?:\s+where\s+(.+?))?$`` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 22 | MustCompile | package | pattern_definition | ``(?is)MATCH\s*\(([^)]*)\)\s+AGAINST\s*\(\s*(?:'([^']*)'&#124;"([^"]*)")(?:\s+IN\s+(BOOLEAN&#124;NATURAL\s+LANGUAGE)\s+MODE(?:\s+WITH\s+QUERY\s+EXPANSION)?&#124;\s+WITH\s+QUERY\s+EXPANSION)?\s*\)`` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 23 | MustCompile | package | pattern_definition | `"(?is)(ST_WITHIN&#124;ST_CONTAINS&#124;ST_INTERSECTS&#124;ST_DISJOINT&#124;ST_TOUCHES&#124;ST_OVERLAPS&#124;ST_CROSSES&#124;ST_EQUALS&#124;MBRCONTAINS&#124;MBRWITHIN&#124;MBRINTERSECTS&#124;MBREQUALS&#124;MBRDISJOINT)\\s*\\(\\s*([a-zA-Z0-9_`.]+)\\s*,\\s*ST_GEOMFROMTEXT\\s*\\(\\s*'([^']+)'(?:\\s*,\\s*([0-9]+))?\\s*\\)\\s*\\)"` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 24 | MustCompile | package | pattern_definition | `"(?is)(ST_WITHIN&#124;ST_CONTAINS&#124;ST_INTERSECTS&#124;ST_DISJOINT&#124;ST_TOUCHES&#124;ST_OVERLAPS&#124;ST_CROSSES&#124;ST_EQUALS&#124;MBRCONTAINS&#124;MBRWITHIN&#124;MBRINTERSECTS&#124;MBREQUALS&#124;MBRDISJOINT)\\s*\\(\\s*ST_GEOMFROMTEXT\\s*\\(\\s*'([^']+)'(?:\\s*,\\s*([0-9]+))?\\s*\\)\\s*,\\s*([a-zA-Z0-9_`.]+)\\s*\\)"` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 1315 | MustCompile | func spatialBounds | pattern_definition | ``[-+]?[0-9]*\.?[0-9]+\s+[-+]?[0-9]*\.?[0-9]+`` |
| server/innodb/engine/fulltext_spatial_compatibility.go | 1333 | MustCompile | func spatialPoint | pattern_definition | ``[-+]?[0-9]*\.?[0-9]+\s+[-+]?[0-9]*\.?[0-9]+`` |
| server/innodb/engine/json_value_compatibility.go | 12 | MustCompile | package | pattern_definition | ``(?is)^(.*?)\s+returning\s+(signed&#124;unsigned&#124;decimal(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?&#124;char(?:\s*\(\s*\d+\s*\))?&#124;date&#124;datetime&#124;time&#124;json)(.*)$`` |
| server/innodb/engine/json_value_compatibility.go | 14 | MustCompile | package | pattern_definition | ``(?is)^(null&#124;error&#124;default\s+(.+?))\s+on\s+(empty&#124;error)(?:\s+(.*))?$`` |
| server/innodb/engine/select_executor.go | 31 | MustCompile | package | pattern_definition | `"(?i)\\b(?:`?user`?)\\s*=\\s*(?:'([^']*)'&#124;\"([^\"]*)\"&#124;([^\\s]+))"` |
| server/innodb/engine/select_executor.go | 32 | MustCompile | package | pattern_definition | `"(?i)\\b(?:`?host`?)\\s*=\\s*(?:'([^']*)'&#124;\"([^\"]*)\"&#124;([^\\s]+))"` |
| server/innodb/engine/select_executor.go | 638 | MustCompile | func determineStorageProjectionColumns | pattern_definition | ``(?i)^[a-z_][a-z0-9_$]*(?:\.[a-z_][a-z0-9_$]*)?$`` |
| server/innodb/engine/select_executor.go | 790 | MustCompile | func partitionConstantPredicate | pattern_definition | ``(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s*(<=&#124;>=&#124;=&#124;<&#124;>)\s*(.+?)\s*$`` |
| server/innodb/engine/select_executor.go | 790 | QuoteMeta | func partitionConstantPredicate | value_match_or_escaping | `identifier` |
| server/innodb/engine/select_executor.go | 3253 | MustCompile | func determineRequiredColumns | pattern_definition | ``(?i)(?:[a-zA-Z0-9_$]+\s*\.\s*)?([a-zA-Z0-9_$]+)`` |
| server/innodb/engine/select_join_executor.go | 522 | MatchString | func evalJoinComparisonTruth | syntax_candidate_or_value_match | `fmt.Sprint(right)` |
| server/innodb/engine/session_expression_compatibility.go | 19 | MustCompile | package | pattern_definition | ``(?i)@@(?:global&#124;session&#124;local)\s*\.\s*`` |
| server/innodb/engine/show_filter.go | 42 | QuoteMeta | func sqlLikeMatch | value_match_or_escaping | `string(r)` |
| server/innodb/engine/show_filter.go | 46 | Compile | func sqlLikeMatch | pattern_definition | `b.String()` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2289 | MatchString | func evalPredicateTruth | syntax_candidate_or_value_match | `fmt.Sprint(right)` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2424 | QuoteMeta | func sqlLikePatternMatchWithEscape | value_match_or_escaping | `string(r)` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2438 | QuoteMeta | func sqlLikePatternMatchWithEscape | value_match_or_escaping | `string(r)` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2442 | QuoteMeta | func sqlLikePatternMatchWithEscape | value_match_or_escaping | `"\\"` |
| server/innodb/engine/storage_integrated_dml_helper.go | 2445 | Compile | func sqlLikePatternMatchWithEscape | pattern_definition | `b.String()` |
| server/innodb/engine/stored_object_compatibility.go | 679 | MustCompile | func replaceRoutineClause | pattern_definition | ``(?is)\s+` + pattern + `\b`` |
| server/innodb/engine/stored_object_compatibility.go | 1083 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^declare\s+([a-zA-Z0-9_$]+)\s+condition\s+for\s+(?:sqlstate\s+'([^']+)'&#124;([0-9]+))$`` |
| server/innodb/engine/stored_object_compatibility.go | 1115 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^get\s+(?:current\s+)?diagnostics\s+condition\s+([0-9]+)\s+(.+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 1124 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^\s*([@a-zA-Z0-9_$]+)\s*=\s*(` + common.RoutineDiagnosticReturnedSQLState + `&#124;` + common.RoutineDiagnosticMessageText + `&#124;` + common.RoutineDiagnosticMySQLErrno + `)\s*$`` |
| server/innodb/engine/stored_object_compatibility.go | 1160 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^declare\s+([a-zA-Z0-9_$]+)\s+cursor\s+for\s+(.+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 1347 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^select\s+(.+?)\s+into\s+(.+?)\s+from\s+(.+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 1374 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^(` + routineControlLeave + `&#124;` + routineControlIterate + `)\s+([a-zA-Z0-9_$]+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 1377 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^(?:([a-zA-Z0-9_$]+)\s*:\s*)?loop(?:\s+(.*))?$`` |
| server/innodb/engine/stored_object_compatibility.go | 1466 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)\s+end\s+repeat\s*$`` |
| server/innodb/engine/stored_object_compatibility.go | 1656 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^resignal(?:\s+set\s+(.+))?$`` |
| server/innodb/engine/stored_object_compatibility.go | 1667 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^` + common.RoutineDiagnosticMessageText + `\s*=\s*'([^']*)'$`` |
| server/innodb/engine/stored_object_compatibility.go | 1671 | MustCompile | func executeStoredRoutineStatements | pattern_definition | ``(?is)^mysql_errno\s*=\s*([0-9]+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 1978 | MustCompile | func parseStoredRoutineHandlerDeclaration | pattern_definition | ``(?is)^declare\s+(` + common.RoutineHandlerContinue + `&#124;` + common.RoutineHandlerExit + `)\s+handler\s+for\s+(.+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 2629 | MustCompile | func resolveStoredFunctionReturnExpression | pattern_definition | `"(?is)^set\\s+`?([a-zA-Z0-9_$]+)`?\\s*=\\s*(.+)$"` |
| server/innodb/engine/stored_object_compatibility.go | 2634 | MustCompile | func resolveStoredFunctionReturnExpression | pattern_definition | ``(?is)^return\s+(.+)$`` |
| server/innodb/engine/stored_object_compatibility.go | 3444 | MustCompile | func findStoredObjectTableDependency | pattern_definition | ``(?i)(?:^&#124;[^a-z0-9_$])` + regexp.QuoteMeta(table) + `(?:[^a-z0-9_$]&#124;$)`` |
| server/innodb/engine/stored_object_compatibility.go | 3444 | QuoteMeta | func findStoredObjectTableDependency | value_match_or_escaping | `table` |
| server/innodb/engine/tablespace_compatibility.go | 19 | MustCompile | package | pattern_definition | ``(?is)\btablespace\s*(?:=\s*)?(` + compatibilityIdentifierPattern + `)`` |
| server/innodb/engine/tablespace_compatibility.go | 126 | MustCompile | func validTablespaceCreateOptions | pattern_definition | ``(?is)^\s*(?:add\s+datafile\s+(?:'[^']*'&#124;"[^"]*"))?\s*(?:file_block_size\s*=\s*\d+\s*)?(?:engine\s*(?:=\s*)?\w+\s*)?$`` |
| server/innodb/engine/temporary_table_compatibility.go | 456 | QuoteMeta | func rewriteTemporaryTableReferences | value_match_or_escaping | `binding.Logical` |
| server/innodb/engine/temporary_table_compatibility.go | 457 | QuoteMeta | func rewriteTemporaryTableReferences | value_match_or_escaping | `binding.Database` |
| server/innodb/engine/temporary_table_compatibility.go | 458 | MustCompile | func rewriteTemporaryTableReferences | pattern_definition | ``(?is)(\b(?:from&#124;join&#124;update&#124;into&#124;table)\s+)(?:(?:` + "`?" + database + "`?" + `)\s*\.\s*)?(?:` + "`?" + logical + "`?" + `)(\s&#124;,&#124;\)&#124;;&#124;$)`` |
| server/innodb/engine/temporary_table_compatibility.go | 460 | MustCompile | func rewriteTemporaryTableReferences | pattern_definition | ``(?is)(^&#124;[^a-zA-Z0-9_$])(?:` + "`?" + logical + "`?" + `)\s*\.`` |
| server/innodb/engine/trigger_runtime.go | 137 | MustCompile | func applyAfterTriggers | pattern_definition | ``(?i)\bnew\.` + regexp.QuoteMeta(name) + `\b`` |
| server/innodb/engine/trigger_runtime.go | 137 | QuoteMeta | func applyAfterTriggers | value_match_or_escaping | `name` |
| server/innodb/engine/trigger_runtime.go | 140 | MustCompile | func applyAfterTriggers | pattern_definition | ``(?i)\bold\.` + regexp.QuoteMeta(name) + `\b`` |
| server/innodb/engine/trigger_runtime.go | 140 | QuoteMeta | func applyAfterTriggers | value_match_or_escaping | `name` |
| server/innodb/engine/window_query_compatibility.go | 39 | MustCompile | func executeGeneralWindowQuery | pattern_definition | ``(?is)^select\s+(.+?),\s*(row_number&#124;rank&#124;dense_rank&#124;ntile&#124;lag&#124;lead&#124;first_value&#124;last_value&#124;nth_value&#124;cume_dist&#124;percent_rank&#124;sum&#124;avg&#124;min&#124;max&#124;count&#124;std&#124;stddev&#124;stddev_pop&#124;stddev_samp&#124;var_pop&#124;var_samp&#124;variance&#124;bit_and&#124;bit_or&#124;bit_xor&#124;json_arrayagg&#124;json_objectagg)\s*\((.*?)\)\s+over\s*\(`` |
| server/innodb/manager/optimizer_manager.go | 180 | MustCompile | func optimizerIndexMatchesConditions | pattern_definition | ``(?i)(^&#124;[^a-z0-9_$])` + regexp.QuoteMeta(column) + `\s*(?:<=>&#124;=&#124;<=&#124;>=&#124;<>&#124;<&#124;>&#124;\b(?:not\s+)?in\b&#124;\b(?:not\s+)?like\b&#124;\b(?:not\s+)?between\b&#124;\bis\s+(?:not\s+)?null\b)`` |
| server/innodb/manager/optimizer_manager.go | 180 | QuoteMeta | func optimizerIndexMatchesConditions | value_match_or_escaping | `column` |
| server/innodb/manager/optimizer_manager.go | 181 | MustCompile | func optimizerIndexMatchesConditions | pattern_definition | ``(?i)(^&#124;[^a-z0-9_$])(?:'[^']*'&#124;"[^"]*"&#124;[-+]?[0-9]+(?:\.[0-9]+)?&#124;\bnull\b)\s*(?:<=>&#124;=&#124;<=&#124;>=&#124;<>&#124;<&#124;>)\s*` + regexp.QuoteMeta(column) + `($&#124;[^a-z0-9_$])`` |
| server/innodb/manager/optimizer_manager.go | 181 | QuoteMeta | func optimizerIndexMatchesConditions | value_match_or_escaping | `column` |
| server/innodb/manager/optimizer_manager.go | 182 | MustCompile | func optimizerIndexMatchesConditions | pattern_definition | ``(?i)\([^()]*\b` + regexp.QuoteMeta(column) + `\b[^()]*\)\s*(?:<=>&#124;=)\s*\(`` |
| server/innodb/manager/optimizer_manager.go | 182 | QuoteMeta | func optimizerIndexMatchesConditions | value_match_or_escaping | `column` |
| server/innodb/manager/schema_manager.go | 1135 | MustCompile | func validateDatabaseName | pattern_definition | ``^[a-zA-Z0-9_$]+$`` |
| server/innodb/plan/expression.go | 1503 | MustCompile | func parseFixedTimezoneOffset | pattern_definition | ``^([+-])(\d{2})(?::?(\d{2}))?$`` |
| server/innodb/plan/expression.go | 1530 | MustCompile | func evalTimeConversionFunction | pattern_definition | ``^([+-]?)([0-9]+):([0-5][0-9]):([0-5][0-9])(?:\.([0-9]{1,6}))?$`` |
| server/innodb/plan/expression.go | 2440 | MustCompile | func evalNetworkAndDigestFunction | pattern_definition | ``(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`` |
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
| server/observability/metrics/statement_digest.go | 11 | MustCompile | package | pattern_definition | ``::digest\d+&#124;:digest\d+`` |
| server/replication/runtime.go | 2822 | MustCompile | package | pattern_definition | ``(?i)\berror\s+([0-9]{1,6})\b`` |
