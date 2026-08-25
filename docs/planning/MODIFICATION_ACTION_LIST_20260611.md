# 修改清单（2026-06-11）

> 目标：明确“项目还差哪些改动”，并给出可直接执行的清单与验收方式。  
> 说明：本清单用于推进到下一步，不替代 `MODIFICATION_CHECKLIST_20260610.md`，也不覆盖 P1/P2 的全部细节。

## 一、当前可复核基线

- 代码扫描口径（`server/**/*.go`）命中 `TODO|FIXME|not implemented|unimplemented|stub|placeholder` 约 `184` 处。
- 受影响文件约 `83` 个。
- `P0` 未闭环项目：`P0-03`、`P0-06`、`P0-07`、`P0-08`、`P0-09`。

## 二、必须先改（P0）

### P0-03 执行器失败可见性与错误分类

- 状态：未闭环
- 影响文件：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 需要做：
  - 统一失败路径返回的错误结构（含模块、SQL、schema、table、txnID、error_code、stage）。
  - 兜底分支不再依赖字符串判断做主决策（尤其是回滚/提交/唯一键冲突）。
  - 为存储失败、表不存在、提交失败、回滚失败补失败类单测，断言结构字段。
- 验收命令：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML)' -count=1`

### P0-06 崩溃恢复演练闭环

- 状态：未闭环
- 影响文件：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 需要做：
  - 固定三个场景：`redo`、`undo`、`half-commit`。
  - 每类至少三轮演练，产出恢复前快照、恢复后快照、差异文件、耗时。
  - 报告目录和文件命名可复放。
- 验收命令：
  - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### P0-07 灰度写入阶段阻塞修复

- 状态：未闭环
- 影响文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/manager/`
  - `server/innodb/engine/`
- 需要做：
  - 复现并修复 `table mysql.t1 not found in storage mapping`。
  - 将只读、写入、回退三阶段串成可重放链路。
  - 失败输出应带阶段号、schema、table、error_code、日志路径、耗时。
- 验收命令：
  - `./scripts/p0_e_backup_snapshot.sh list`
  - 产出一次完整 `read -> write -> restore` 通过记录。

### P0-08 慢查询日志

- 状态：未闭环
- 影响文件：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 需要做：
  - 增加配置：`slow_query_log`、`slow_query_log_file`、`long_query_time_ms`。
  - 记录字段至少包含：`sql, cost_ms, rows_affected, conn_id, txn_id, schema, table, error_code, error_msg, stage`。
- 验收命令：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/innodb/engine -run 'Test.*(Slow|Log|SlowQuery)' -count=1`

### P0-09 指标与告警导出

- 状态：未闭环
- 影响文件：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 需要做：
  - 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99。
  - 输出 checkpoint/redo/undo/锁等待核心指标。
  - 增加一条可触发并可恢复的告警演练。
- 验收命令：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`

## 三、P1/P2 计划（未完成但可排期）

### P1（稳定后优先）
- `server/innodb/engine/subquery_executor*.go`
- `server/innodb/engine/cte_executor.go`
- `server/innodb/engine/window_function_executor.go`
- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/parallel.go`
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/manager/enhanced_btree_manager.go`

### P2（可延后）
- `server/innodb/storage/wrapper/page/page_impl.go`
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/storage/store/mvcc/*`
- `server/innodb/storage/store/pages/compressed_page.go`

## 四、下一步执行顺序（建议）

1. 先闭环 `P0-03`（当前最优先）。
2. 同步推进 `P0-06` 与 `P0-07`。
3. 并行打通 `P0-08` 与 `P0-09`。
4. `P0` 全部闭环后进入 `P1`。

## 五、热点 TODO 清理（按文件）

- `server/innodb/manager/space_expansion_concurrent_test.go`（10）
- `server/innodb/plan/parallel.go`（8）
- `server/innodb/engine/index_reading_test.go`（8）
- `server/innodb/plan/physical_plan.go`（6）
- `server/innodb/metadata/convert.go`（6）
- `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
- `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
- `server/innodb/plan/statistics_collector_helpers.go`（5）
- `server/innodb/manager/page.go`（5）
- `server/innodb/engine/storage_integrated_index_helper.go`（5）

