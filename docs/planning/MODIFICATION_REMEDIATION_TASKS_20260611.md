# XMySQL Server 修改清单（更新版，2026-06-11）

用途：明确“项目还差什么需要改”，并给出可直接排期的任务列表。

## 0) 当前扫描快照

基于仓库当前状态执行：

- `rg -n "TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂未实现|暂时返回|待实现|not implemented in test|not implemented yet|stub" server/**/*.go`
- `rg -n "TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂未实现|暂时返回|待实现|not implemented in test|not implemented yet|stub" server/**/*.sh docs/**/**/*.md`

命中数：

- `server/**/*.go`：`188`
- `scripts + docs + README`：`226`

命中 Top10（以 `server` 内文件为准）：

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/parallel.go`（8）
3. `server/innodb/engine/index_reading_test.go`（8）
4. `server/innodb/plan/statistics_collector_helpers.go`（7）
5. `server/innodb/plan/physical_plan.go`（6）
6. `server/innodb/metadata/convert.go`（6）
7. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
8. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
9. `server/innodb/plan/join_order_optimizer_helpers.go`（5）
10. `server/innodb/engine/storage_integrated_index_helper.go`（5）

## 1) 已经做的关键改动（本轮）

- `server/innodb/engine/execution_error.go` 已创建，补充了结构化错误码和 `ExecutionError` 工具方法。
- `server/innodb/engine/storage_adapter.go` 已加依赖空值防护（`tableManager`、`tableStorageManager`、`bufferPoolManager`、`schema`、`page` 等路径）。
- `server/innodb/engine/dml_operators.go` 等少量路径已修正了回滚/提交失败未返回风险（已在前一轮提交中提到）。

> 注意：`execution_error.go` 目前仍主要是定义层面，`unified_executor.go`、`executor.go` 等主链路尚未广泛接入结构化错误。

## 2) P0（必须先改，阻塞发布）

### P0-03 执行器错误可见性与分类（未闭环）

影响文件：

- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/dml_operators.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/storage_integrated_dml_helper.go`
- `server/innodb/engine/storage_integrated_dml_executor.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/engine/index_transaction_adapter.go`

改动清单：

1. 主链路返回错误改为 `ExecutionError`，并携带 `module/stage/schema/table/txn_id/error_code`。
2. 将 `where`/`commit`/`rollback`/重复键/表/存储映射缺失等关键失败路径从字符串判断改为错误码/类型判断。
3. 禁止关键失败“只写日志不返回”的降级行为；交易失败必须可观察。
4. 增补测试：使用 `errors.As(err, *ExecutionError)` 断言，包括：
   - 表映射缺失
   - 存储读写失败
   - begin / commit / rollback 失败
   - duplicate key

建议验收：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|DDL|DML|Index)' -count=1
```

### P0-06 崩溃恢复演练闭环（未闭环）

影响文件：

- `server/innodb/manager/crash_recovery.go`
- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

改动清单：

1. 固定三类场景：`redo` / `undo` / `half-commit`。
2. 每类至少 3 次复盘演练，保留前后快照、差异报告、耗时。
3. 归档目录、文件名与报告字段统一，避免脚本复用时路径歧义。

### P0-07 灰度写入阶段阻塞修复（未闭环）

影响文件：

- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`
- `server/innodb/manager/`

改动清单：

1. 复现并修掉 `table ... not found in storage mapping` 的阻塞点。
2. read / write / restore 三阶段链路脚本化并可重复执行。
3. 失败日志补齐 `stage/schema/table/error_code/log_path/duration_ms`。

### P0-08 慢查询日志（未闭环）

影响文件：

- `server/conf/`
- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`

改动清单：

1. 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 配置链路。
2. slow log 写入至少包含：
   `sql, cost_ms, rows_affected, conn_id, txn_id, schema, table, error_code, error_msg, stage`。
3. 至少补一条可复放慢查询场景测试。

### P0-09 指标与告警（未闭环）

影响文件：

- `server/innodb/manager/transaction_manager.go`
- `server/innodb/manager/checkpoint_monitor.go`
- `server/net/`
- `server/conf/`

改动清单：

1. 输出至少以下指标：QPS、错误率、连接数、活跃事务、P50/P95/P99、redo、undo、锁等待。
2. 增加一条可触发、可恢复的告警演练并归档。

## 3) P1（P0 关闭后推进）

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

## 4) 建议排期（直接落地）

1. 先把 `P0-03` 关闭：这会把执行错误可观测性和失败路径测试先打通。
2. 同时推进 `P0-06` 与 `P0-07`，两者都依赖演练链路和日志字段统一。
3. 并行补齐 `P0-08`、`P0-09`，完成发布可观测性最小闭环。
4. P0 全绿后再处理 `P1` 项。
