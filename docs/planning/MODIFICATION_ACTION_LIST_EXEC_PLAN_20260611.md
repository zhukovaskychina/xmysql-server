# 修改清单（执行计划版，2026-06-11）

## 0. 基线结论

按 `server` 目录扫描（关键词：`TODO|FIXME|not implemented|简化实现|暂时`），仍有大量待补点；
其中和发布风险直接相关的是 P0 组。本文只列“可执行闭环清单”。

扫描时间：2026-06-11

关键数字（粗略）：
- `engine` 目录中的显式 `TODO/FIXME/not implemented/简化实现/暂时` 相关命中持续存在，且 `dml_operators.go` 有多处明确简化/占位。
- `P0-03`（执行与错误可见性）仍未闭环。

## 1. P0：当前必须改完

### P0-03 执行器失败可见性与结构化错误（优先级最高）

涉及文件：
- `server/innodb/engine/dml_operators.go`
- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/storage_integrated_dml_executor.go`
- `server/innodb/engine/storage_integrated_dml_helper.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/engine/index_transaction_adapter.go`
- `server/innodb/engine/execution_error.go`

还差项：
- `dml_operators.go` 中关键函数不能再返回伪成功或空实现，必须实现至少：
  - `findDuplicateRecord`
  - `parseInsertRows`
  - `insertRow`
  - `updateRecord`
  - `updateInPlace`
  - `deleteOldRecord`
  - `insertNewRecord`
  - `deleteRecord`
- `executor.go` 与 `unified_executor.go` 的关键失败分支，把“文本错误”改成 `ExecutionError`。
- `index_transaction_adapter.go` 与 `storage_adapter.go` 里的失败返回要补齐 `ErrorCode` 与 `txn_id`。
- 增加失败路径单测：
  - 元数据缺失
  - 表映射缺失
  - begin/commit/rollback 失败
  - 存储读写失败
  - 唯一键冲突
  - 断言 `errors.As(err, *ExecutionError)` 和 `ErrorCode`

验收（可直接接入）：
```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|DML|Index)' -count=1
```

### P0-06 崩溃恢复演练闭环

涉及文件：
- `server/innodb/manager/crash_recovery.go`
- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`

还差项：
- 固化 `redo` / `undo` / `half-commit` 三类场景输入。
- 每类连跑至少 3 次 `start -> crash -> restart -> verify`。
- 报告中必须保留：场景名、阶段耗时、日志路径、恢复前后状态差异。

### P0-07 灰度写入脚本闭环

涉及文件：
- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`
- `server/innodb/manager/`

还差项：
- 修复 `table mysql.t1 not found in storage mapping` 卡点。
- 写入、回放、回退三段串成单条链路。
- 每段日志补齐 `stage/schema/table/error_code/log_path/duration_ms`。

### P0-08 慢查询日志

涉及文件：
- `server/conf/`
- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`

还差项：
- 完成配置链路：`slow_query_log`、`slow_query_log_file`、`long_query_time_ms`。
- 统一日志字段：`sql, cost_ms, rows_affected, conn_id, txn_id, schema, table, error_code, error_msg, stage`。
- 增加一条可复放慢查询用例。

### P0-09 指标与告警

涉及文件：
- `server/innodb/manager/transaction_manager.go`
- `server/innodb/manager/checkpoint_monitor.go`
- `server/net/`
- `server/conf/`

还差项：
- 指标最小集：QPS、错误率、连接数、活跃事务、P50/P95/P99。
- 系统指标：checkpoint、redo、undo、锁等待。
- 一条可复现告警触发与恢复的验证链路。

## 2. P1：P0 通过后继续

- `server/innodb/engine/subquery_executor.go`、`engine/cte_executor.go`：子查询、CTE。
- `server/innodb/engine/window_function_executor.go`：窗口函数与窗口帧。
- `server/innodb/plan/physical_plan.go`、`plan/parallel.go`、`plan/cost_estimator.go`：代价与并行。
- `server/innodb/plan/statistics_collector_helpers.go`：统计信息真实来源。
- `server/innodb/engine/storage_integrated_index_helper.go`：索引同步边界条件。
- `server/innodb/manager/page.go`、`manager/dictionary_manager.go`：分页与字典存取。

## 3. P2：后续清理

- `server/innodb/storage/wrapper/page/*.go`（页包装与IO真实化）
- `server/innodb/storage/store/pages/compressed_page.go`
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/manager/space_expansion_manager.go` 等占位逻辑

## 4. 本地优先级建议（先后顺序）

1. 先把 P0-03 做完。
2. 并行推进 P0-06 与 P0-07。
3. 同步补 P0-08 与 P0-09。
4. P0 全绿后再切 P1。

## 5. 当前最值得优先清理的高频 TODO 文件（非测试项）

1. `server/innodb/plan/physical_plan.go`
2. `server/innodb/engine/executor.go`
3. `server/innodb/engine/dml_operators.go`
4. `server/innodb/plan/statistics_collector_helpers.go`
5. `server/innodb/manager/storage_manager.go`
6. `server/innodb/manager/page.go`
7. `server/innodb/manager/dictionary_manager.go`
8. `server/innodb/manager/lock_manager.go`
9. `server/innodb/manager/index_manager.go`
10. `server/innodb/plan/join_order_optimizer_helpers.go`
