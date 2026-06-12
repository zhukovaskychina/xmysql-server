# XMySQL Server 修改清单（可执行版，2026-06-11）

目的：回答“还有哪些要改”，并明确下一步可直接排期执行的项。  
说明：以 `server/` 和 `scripts/` 的代码扫描与当前工程改动状态为准。

## 一、基线快照（按代码文本标记）

- 扫描口径：`server/**/*.go`、`server/**/*.sh` 中匹配  
  `TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂未实现|暂时返回|待实现|stub`
- 命中数：`197`
- 受影响文件：`86`
- 目前最关键的未闭环项仍是 P0 级：`P0-03 / P0-06 / P0-07 / P0-08 / P0-09`

## 二、P0：必须先改（阻塞发布）

### P0-03 执行器失败可见性与错误分类（未闭环）

涉及文件：
- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`
- `server/innodb/engine/dml_operators.go`
- `server/innodb/engine/storage_adapter.go`
- `server/innodb/engine/storage_integrated_dml_helper.go`
- `server/innodb/engine/storage_integrated_dml_executor.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/engine/index_transaction_adapter.go`

缺项：
- 关键失败返回还未全面改成 `ExecutionError`；
- `table/schema/not found`、`begin/commit/rollback`、重复键冲突的错误仍缺少统一错误码和上下文；
- 不能用错误文本作为主分支判断条件（应以 `error code`/类型断言为主）；
- 缺少对应失败路径的回归测试（建议使用 `errors.As` 断言 `ExecutionError`）。

验收命令（建议）：
```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML)' -count=1
```

### P0-06 崩溃恢复演练闭环（未闭环）

涉及文件：
- `server/innodb/manager/crash_recovery.go`
- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

缺项：
- 固定 `redo` / `undo` / `half-commit` 三类场景；
- 每类至少复放 3 次并保留“恢复前快照 / 恢复后快照 / 差异 / 耗时”；
- 报告目录和命名要统一，便于复核。

验收命令（建议）：
```bash
CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh
```

### P0-07 灰度写入阶段阻塞修复（未闭环）

涉及文件：
- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`
- `server/innodb/manager/`

缺项：
- 复现并修复 `table ... not found in storage mapping`；
- read -> write -> restore 三阶段链路脚本闭环；
- 失败日志补齐：`stage / schema / table / error_code / log_path / duration_ms`。

### P0-08 慢查询日志（未闭环）

涉及文件：
- `server/conf/`
- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`

缺项：
- 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 全链路；
- slow log 写入字段至少包含：`sql、cost_ms、rows_affected、conn_id、txn_id、schema、table、error_code、error_msg、stage`。

### P0-09 指标与告警（未闭环）

涉及文件：
- `server/innodb/manager/transaction_manager.go`
- `server/innodb/manager/checkpoint_monitor.go`
- `server/net/`
- `server/conf/`

缺项：
- 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99；
- 输出 redo/undo/锁等待关键指标；
- 增加可触发、可恢复的告警演练。

## 三、P1：P0 闭环后排期

### 执行器与 SQL 能力
- `server/innodb/engine/subquery_executor*.go`
- `server/innodb/engine/cte_executor.go`
- `server/innodb/engine/window_function_executor.go`

### 计划与优化
- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/parallel.go`
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/plan/join_order_optimizer_helpers.go`

### 索引与一致性
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/manager/index_manager.go`
- `server/innodb/manager/enhanced_btree_manager.go`

## 四、P2：可延后
- `server/innodb/storage/wrapper/page/*.go`
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/storage/store/mvcc/*`
- `server/innodb/storage/store/pages/compressed_page.go`

## 五、代码 TODO 热点 Top 15（当前扫描计数）

- `server/innodb/manager/space_expansion_concurrent_test.go`（10）
- `server/innodb/plan/parallel.go`（8）
- `server/innodb/engine/index_reading_test.go`（8）
- `server/innodb/plan/statistics_collector_helpers.go`（7）
- `server/innodb/plan/physical_plan.go`（6）
- `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（6）
- `server/innodb/metadata/convert.go`（6）
- `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
- `server/innodb/plan/join_order_optimizer_helpers.go`（5）
- `server/innodb/manager/page.go`（5）
- `server/innodb/engine/storage_integrated_index_helper.go`（5）
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`（4）
- `server/innodb/storage/store/pages/compressed_page.go`（4）
- `server/innodb/manager/index_manager.go`（3）
- `server/innodb/manager/enhanced_btree_manager.go`（3）

## 六、建议执行顺序

1. 先把 `P0-03` 关掉（执行器错误链路）；
2. 同时推进 `P0-06`、`P0-07`；
3. 并行补齐 `P0-08`、`P0-09`；
4. 全部 P0 验收通过后再进入 P1。
