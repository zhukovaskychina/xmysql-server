# sqlparser 基准对比（2026-10-17）

环境：Windows amd64，13th Gen Intel(R) Core(TM) i5-13500H，Go 1.27.0。
命令：`go test ./server/innodb/sqlparser -run '^$' -bench 'BenchmarkParse(ForExecution|LegacyComparable)$' -benchmem -count=2`

`BenchmarkParseForExecution` 覆盖 SELECT、SHOW、复合 ALTER、CTE/子查询和过程定义；`BenchmarkParseLegacyComparable` 只使用升级前 yacc 能完整接受的 SELECT、SHOW、子查询、系统变量和 SET 样例，避免把旧解析错误当作成功解析。

| 基准 | 第一次 | 第二次 |
|---|---:|---:|
| ParseForExecution | 16682 ns/op, 19329 B/op, 135 allocs/op | 19891 ns/op, 19329 B/op, 135 allocs/op |
| Legacy comparable | 18416 ns/op, 28198 B/op, 30 allocs/op | 25106 ns/op, 28198 B/op, 30 allocs/op |

两次运行有明显抖动，当前结果只作为环境记录，不据此宣称吞吐结论；升级解析器的分配量和复合语句覆盖范围都已留下可重复命令。
