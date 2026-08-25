# 修改清单（执行版，2026-06-11）

用途：回答“现在还有哪些要改”，并把需要改的点固定成可排期清单。

## 0) 当前快照

- 扫描命令：`rg -n "TODO|FIXME|not implemented|unimplemented|placeholder|暂未|暂时返回|stub|暂时" server --glob '*.go'`
- 命中总数（含 `_test.go`）：**217**
- 生产代码命中：**187**，约 **95** 个文件。
- 当前关键结论：`ExecutionError` 体系已定义，但核心执行链路还没形成闭环。

## 1) P0（必须先改，阻塞发布）

### P0-03 执行器失败可见性与错误分类（优先级最高）

核心文件：
- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/dml_operators.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/storage_integrated_dml_executor.go`
- `server/innodb/engine/storage_integrated_dml_helper.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/engine/index_transaction_adapter.go`

待改项：
- 将关键失败路径从普通 `fmt.Errorf` 改为 `ExecutionError`（带 `module/stage/schema/table/txn_id/error_code/cause`）。
- 去掉关键分支里“看错误文本决定业务”的模式，改为错误码驱动。
- 在上层返回时保留失败原因，不允许只打印日志后静默回滚/放弃。
- 在 `dml_operators.go` 补齐关键简化实现：
  - `findDuplicateRecord`
  - `parseInsertRows`
  - `insertRow`
  - `updateRecord`
  - `updateInPlace`
  - `deleteOldRecord`
  - `insertNewRecord`
  - `deleteRecord`
- 为元数据缺失、存储读写失败、事务 begin/commit/rollback 失败、唯一键冲突增加失败路径测试，使用 `errors.As(err, *ExecutionError)` 与 `ErrorCode` 断言。

### P0-06 崩溃恢复演练闭环

核心文件：
- `server/innodb/manager/crash_recovery.go`
- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

待改项：
- 固化 redo / undo / half-commit 三类输入。
- 每类至少 3 次 `start -> crash -> restart -> verify` 演练。
- 每次保留恢复前后快照、差异报告、耗时、日志路径。

### P0-07 灰度写入阻塞修复

核心文件：
- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`
- `server/innodb/manager/`

待改项：
- 复现并修复 `table mysql.t1 not found in storage mapping`。
- 串起 read/write/restore 三阶段脚本。
- 阶段日志增加 `stage / schema / table / error_code / log_path / duration_ms`。

### P0-08 慢查询日志闭环

核心文件：
- `server/conf/`
- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`

待改项：
- 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 配置链路。
- 采样字段至少：`sql,cost_ms,rows_affected,conn_id,txn_id,schema,table,error_code,error_msg,stage`。
- 至少一条复现测试。

### P0-09 指标与告警导出

核心文件：
- `server/innodb/manager/transaction_manager.go`
- `server/innodb/manager/checkpoint_monitor.go`
- `server/net/`
- `server/conf/`

待改项：
- 指标：QPS、错误率、连接数、活跃事务、P50/P95/P99。
- 系统指标：checkpoint、redo、undo、锁等待。
- 加一条可触发、可恢复、可复查的告警演练。

## 2) P1（P0 全绿后）

- `server/innodb/engine/subquery_executor*.go`
- `server/innodb/engine/cte_executor.go`
- `server/innodb/engine/window_function_executor.go`
- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/parallel.go`
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/manager/index_manager.go`
- `server/innodb/manager/enhanced_btree_manager.go`

## 3) P2（可延后）

- `server/innodb/storage/wrapper/page/page_impl.go`
- `server/innodb/storage/wrapper/page/page_inode_wrapper.go`
- `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/storage/store/pages/compressed_page.go`
- `server/innodb/storage/store/mvcc/*`

## 4) 最近改动优先（高频 TODO Top 15，非测试文件）

1. `server/innodb/plan/parallel.go`（8）
2. `server/innodb/plan/statistics_collector_helpers.go`（7）
3. `server/innodb/plan/physical_plan.go`（6）
4. `server/innodb/metadata/convert.go`（6）
5. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
6. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
7. `server/innodb/plan/join_order_optimizer_helpers.go`（5）
8. `server/innodb/manager/page.go`（5）
9. `server/innodb/engine/storage_integrated_index_helper.go`（5）
10. `server/innodb/storage/wrapper/mvcc/mvcc_page.go`（4）
11. `server/innodb/storage/store/pages/compressed_page.go`（4）
12. `server/innodb/manager/dictionary_manager.go`（4）
13. `server/innodb/storage/wrapper/record/row_cluster_index_internal_row.go`（3）
14. `server/innodb/storage/wrapper/page/undo_log_page_wrapper.go`（3）
15. `server/innodb/metadata/util.go`（3）

## 5) 推荐执行顺序

1. 优先闭环 **P0-03**
2. 并行推进 **P0-06 / P0-07**
3. 并行推进 **P0-08 / P0-09**
4. P0 全绿后开始 **P1**
