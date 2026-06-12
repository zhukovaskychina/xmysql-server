# XMySQL Server 修改清单（最新版，待改事项）

> 生成时间：2026-06-11（基于当前工作树快照）
> 目标：回答“还差什么要改”，并给出可执行修改清单。

## 快速结论

- P0 里最关键的是 `P0-03`，核心执行链条仍未闭环到结构化错误码。
- 其余 P0（恢复、灰度、慢查询、指标）仍有交付空洞。
- 2026-06-11 扫描口径（`TODO/FIXME/placeholder/simplified/not implemented`）在 `server/**/*.go` 大约有 **217** 个命中，约 **95** 个文件。

> 说明：这里只列“还没闭环的工程阻塞项”，不是复述所有历史测试报告。

## P0（必须先改）

### P0-03 执行器失败可见性与错误分类（未闭环）

- 影响范围：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/index_transaction_adapter.go`
  - `server/innodb/engine/dml_executor.go`

- 还差项：
  1. 将关键失败返回改为 `ExecutionError`（含错误码、stage、schema、table、txn_id）。
  2. 统一替换关键路径中文本错误判断，统一走 `errors.As(err, *ExecutionError)`。
  3. `dml_operators.go` 的 DML 核心函数不能再占位/简化返回：
     - `findDuplicateRecord`
     - `parseInsertRows`
     - `insertRow`
     - `updateRecord`
     - `updateInPlace`
     - `deleteOldRecord`
     - `insertNewRecord`
     - `deleteRecord`
  4. `unified_executor.go` 与 `executor.go` 的关键失败分支加错误码断言测试。
  5. `index_transaction_adapter.go` 增加锁/事务失败分支的可观测返回，不要只打印日志。

- 验收：
  - 关键失败场景用例断言 `errors.As(err, *engine.ExecutionError)` 成功。
  - 断言 `err.(*ExecutionError).ErrorCode` 对应 `E_*` 取值。

### P0-06 崩溃恢复演练闭环（未闭环）

- 影响范围：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

- 还差项：
  - 固化 `redo / undo / half-commit` 三类案例。
  - 每类做完整 `start -> crash -> restart -> verify` 至少 3 次。
  - 每次输出快照前后对比和日志路径。

### P0-07 灰度写入阶段阻塞修复（未闭环）

- 影响范围：
  - `scripts/p0_e_backup_snapshot.sh`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`

- 还差项：
  - 修复 `table mysql.t1 not found in storage mapping` 阶段阻塞。
  - 串起 read/write/restore 全链路脚本。
  - 补齐 `stage/schema/table/error_code/log_path/duration_ms` 日志。

### P0-08 慢查询日志（未闭环）

- 影响范围：`server/conf/`、`server/session/`、`server/dispatcher/`、`server/innodb/engine/`
- 还差项：
  - 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 全链路。
  - 最小字段集合：`sql,cost_ms,rows_affected,conn_id,txn_id,schema,table,error_code,error_msg,stage`。
  - 至少一条可复放慢查询样例。

### P0-09 指标与告警（未闭环）

- 影响范围：`server/innodb/manager/transaction_manager.go`、`server/innodb/manager/checkpoint_monitor.go`、`server/net/`、`server/conf/`
- 还差项：
  - 导出 QPS、错误率、连接数、活跃事务。
  - 导出 P50/P95/P99。
  - 导出 redo/undo/锁等待/checkpoint 关键指标。
  - 提供一条可复现的告警触发-恢复-复核链路。

## P1（P0 之后）

- `server/innodb/engine/subquery_executor.go`、`engine/cte_executor.go`（子查询/CTE）
- `server/innodb/engine/window_function_executor.go`（窗口函数）
- `server/innodb/plan/physical_plan.go`、`plan/parallel.go`、`plan/cost_estimator.go`（优化器与并行）
- `server/innodb/plan/statistics_collector_helpers.go`（统计信息）
- `server/innodb/engine/storage_integrated_index_helper.go`（索引同步细化）
- `server/innodb/storage/wrapper/page/*.go`（若要发布级稳定性）

## 建议执行顺序

1. 先把 `P0-03` 改完并补失败路径测试。
2. 同时推进 `P0-06` 与 `P0-07`。
3. 并行补 `P0-08`、`P0-09`。
4. 全部 P0 打绿后，再处理 P1。
