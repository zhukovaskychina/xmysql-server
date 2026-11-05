# SQL 正则残留分类（2026-10-18 实际扫描）

当前 CI 范围排除 cmd、client：1076 个 Go 文件，28 个 regexp 导入文件，150 个 regexp 包直接调用位置，Go 文件解析异常 0。

| 扫描器 API 分类 | 数量 | 实际含义 |
|---|---:|---|
| pattern_definition | 99 | Compile/MustCompile 调用，包含仍在工作的 SQL 结构解析正则，需要逐项迁移审查 |
| syntax_candidate_or_value_match | 9 | 包级 MatchString 等调用，当前包含主机模式、SQL REGEXP/LIKE 和 JSON 值验证 |
| value_match_or_escaping | 42 | QuoteMeta 调用；部分用于 SQL 标识符或依赖改写，不能统一视为可保留的值匹配 |

该统计不包含每一次 regexp 实例方法调用；150 不是独立正则表达式的精确数量。API 分类不等于用途分类，模式声明不能自动排除出 AST 迁移范围。

本轮扫描补充首个参数源码，修正包级变量被错误归入前一个函数的问题。完整路径、行号、所属函数、模式参数见同目录的 2026-10-17-sql-regex-remaining-inventory.md 和扫描 JSON。

已迁移的本轮路径包括事件调度、存储对象 CREATE/DROP 入口、DEFINER/例程属性、CALL 对象及参数、分区 CAST/算术/日期函数、mysql 系统表过滤、Processlist WHERE、KILL、LOCK TABLES 请求解析和 SHOW WARNINGS/ERRORS LIMIT；随后完成复制 SOURCE/FILTER/PURGE/RESET、FLUSH、表维护与直方图、SET PERSIST/PERSIST_ONLY、改密权限判断及 DISCARD/IMPORT TABLESPACE、CTAS 及建表索引可见性的正则移除。

仍存在适合 AST 迁移的候选，集中在 executor.go、stored_object_compatibility.go、account_compatibility.go、alter_operation_compatibility.go、admin_compatibility.go，以及 correlated/CTE/window/partition 代码。因此此前“剩余全部只是值语义”“SQL 结构正则已清零”的结论无效。本文件不声明迁移完成。

测试证据及尚未解决的锁、异步事件运行问题另见 2026-10-18-sql-regex-ast-migration-progress.md。
