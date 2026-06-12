# XMySQL Server 修改清单（最新版，2026-06-11）

用于回答“还差什么”和“下一步先改什么”。按 P0 / P1 / P2 排序，直接可排期。

## 当前基线

- 代码扫描范围：`server/**/*.go`  
- 关键词命中数：`TODO|FIXME|not implemented|notImplemented|unimplemented|stub|placeholder|暂未实现|暂时|待实现|not implemented in test|not implemented yet`
- 命中：**214** 处（约 **95** 个文件）
- 关键结论：`ExecutionError` 已定义但未接入主流程；主要阻断项仍是执行链路错误可见性和可观测性闭合。

## P0（必须先改）

### P0-03 执行器失败可见性与错误分类（未闭环）

- 影响文件：
  - `server/innodb/engine/execution_error.go`
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/engine/index_transaction_adapter.go`
- 缺项：
  - 将关键失败分支改为 `ExecutionError`，补充 `module / stage / schema / table / txn_id / error_code / SQL / cause`
  - begin / commit / rollback 失败应返回分类错误码，不要只记录日志
  - `rollback` / `commit` 返回值在调用处必须检查，不允许忽略
  - 删除以错误文本做核心分支判断的写法（除了日志输出）
  - 新增失败路径测试：表不存在、存储读写失败、事务 begin/commit/rollback 失败、重复键冲突
- 验收：
  - `go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|DDL|DML|Index)' -count=1`

### P0-06 崩溃恢复演练闭环（未闭环）

- 影响文件：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 缺项：
  - 固化三类场景：`redo` / `undo` / `half-commit`
  - 每类至少执行 3 次，保存：
    - 恢复前快照
    - 恢复后快照
    - 差异说明
    - 耗时记录
  - 统一报告目录命名，方便复核
- 验收：
  - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### P0-07 灰度写入阻塞修复（未闭环）

- 影响文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
- 缺项：
  - 复现并修复 `table ... not found in storage mapping`
  - read / write / restore 三阶段脚本闭合
  - 失败日志补齐 `stage / schema / table / error_code / log_path / duration_ms`

### P0-08 慢查询日志（未闭环）

- 影响文件：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 缺项：
  - 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 的配置链路
  - slow log 至少包含：`sql / cost_ms / rows_affected / conn_id / txn_id / schema / table / error_code / error_msg / stage`
- 验收：
  - `go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Log' -count=1`

### P0-09 指标与告警导出（未闭环）

- 影响文件：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 缺项：
  - 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99
  - 输出 redo / undo / 锁等待基础指标
  - 增加一条可触发、可恢复、可复现的告警演练
- 验收：
  - `go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`

## P1（P0 先完成后排期）

- 子查询与 CTE：
  - `server/innodb/engine/subquery_executor*.go`
  - `server/innodb/engine/cte_executor.go`
- 窗口函数：
  - `server/innodb/engine/window_function_executor.go`
- 优化器与并行：
  - `server/innodb/plan/physical_plan.go`
  - `server/innodb/plan/parallel.go`
  - `server/innodb/plan/cost_estimator.go`
  - `server/innodb/plan/statistics_collector_helpers.go`
- 索引与一致性：
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/manager/index_manager.go`
  - `server/innodb/manager/enhanced_btree_manager.go`
- 存储页层实现：
  - `server/innodb/storage/wrapper/page/page_impl.go`
  - `server/innodb/storage/wrapper/page/page_inode_wrapper.go`
  - `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`

## P2（可延后）

- 压缩页、MVCC、空间管理的低优先级补齐：
  - `server/innodb/storage/store/pages/compressed_page.go`
  - `server/innodb/storage/store/mvcc/*`
  - `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
  - `server/innodb/storage/wrapper/record/*`

## 最近一轮优先修复的 TODO 热点（Top 10）

- `server/innodb/manager/space_expansion_concurrent_test.go`（10）
- `server/innodb/plan/statistics_collector_helpers.go`（9）
- `server/innodb/plan/parallel.go`（8）
- `server/innodb/engine/index_reading_test.go`（8）
- `server/innodb/manager/page_initialization_fix.go`（7）
- `server/innodb/plan/physical_plan.go`（6）
- `server/innodb/metadata/convert.go`（6）
- `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
- `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
- `server/innodb/plan/join_order_optimizer_helpers.go`（5）

## 建议顺序

1. 先完成 P0-03。
2. 并行推进 P0-06、P0-07。
3. 并行推进 P0-08、P0-09。
4. 全部 P0 过后再进入 P1，再到 P2。
