# 修改清单（待改项目，2026-06-11）

目标：把“还差什么、先改哪个、验收方式”固定下来，作为下一步执行基线。

## 当前快照

- 代码扫描口径：`server/**/*.go` + 关键词  
  `TODO|FIXME|not implemented|notImplemented|unimplemented|stub|placeholder|暂未实现|暂时|待实现|not implemented in test|not implemented yet`
- 命中：约 `214` 处
- 覆盖文件：约 `95` 个
- P0 未闭环项：`P0-03 / P0-06 / P0-07 / P0-08 / P0-09`

## 必须先改（P0）

### P0-03 执行链路失败可见性与错误分类（未闭环）

- 文件：
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/executor.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/unified_executor.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/dml_operators.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/storage_adapter.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/storage_integrated_dml_executor.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/storage_integrated_dml_helper.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/storage_integrated_index_helper.go`
- 具体任务：
  - 统一失败返回使用结构化错误，包含 `module / stage / schema / table / txn_id / error_code`
  - 回滚、提交、事务开始失败要可见，不允许只写日志后返回泛化错误
  - 先补关键失败路径用例：表映射缺失、存储读写失败、事务 begin/commit/rollback 失败、唯一键冲突
  - 不再以错误字符串做主分支判断，尽量用错误码或包装类型
- 验收：
  - `go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|DDL|DML|Index)' -count=1`

### P0-06 崩溃恢复演练闭环（未闭环）

- 文件：
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/crash_recovery.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/scripts/crash_recovery_drill.sh`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/scripts/crash_recovery_process_drill.sh`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/scripts/p0_b_recovery_audit.sh`
- 具体任务：
  - 固定三类场景：`redo`、`undo`、`half-commit`
  - 每类至少 `3` 次复盘演练
  - 每轮产出：前后快照、差异报告、耗时、复盘日志路径

### P0-07 灰度写入阶段阻塞修复（未闭环）

- 文件：
  - `/Users/zhukovasky/GolandProjects/xmysql-server/scripts/p0_e_backup_snapshot.sh`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager`
- 具体任务：
  - 复现并修复 `table ... not found in storage mapping`
  - 将 read/write/restore 三阶段串成单链路，可重复执行
  - 错误日志补齐 `stage/schema/table/error_code/log_path/duration_ms`

### P0-08 慢查询日志（未闭环）

- 文件：
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/conf`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/session`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/dispatcher`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine`
- 具体任务：
  - 打通 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms`
  - 记录字段至少包含：`sql、cost_ms、rows_affected、conn_id、txn_id、schema、table、error_code、error_msg、stage`

### P0-09 指标与告警（未闭环）

- 文件：
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/transaction_manager.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/checkpoint_monitor.go`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/conf`
  - `/Users/zhukovasky/GolandProjects/xmysql-server/server/net`
- 具体任务：
  - 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99
  - 输出 checkpoint/redo/undo/锁等待基础指标
  - 做一条可触发、可恢复的告警演练并形成复盘记录

## 近期 P1（可排期）

- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/subquery_executor*.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/cte_executor.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/window_function_executor.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/physical_plan.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/parallel.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/cost_estimator.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/statistics_collector_helpers.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/storage_integrated_index_helper.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/index_manager.go`
- `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/enhanced_btree_manager.go`

## TODO 热点文件（优先清理）

1. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/statistics_collector_helpers.go`（9）
3. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/parallel.go`（8）
4. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/index_reading_test.go`（8）
5. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/page_initialization_fix.go`（7）
6. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/plan/physical_plan.go`（6）
7. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/metadata/convert.go`（6）
8. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
9. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
10. `/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/engine/storage_integrated_index_helper.go`（5）

## 推荐执行顺序

1. 先把 P0-03 关掉（错误可见性）
2. 同时推进 P0-06 与 P0-07
3. 并行推进 P0-08、P0-09
4. 之后进入 P1 里子查询、执行器、统计和索引类工作
