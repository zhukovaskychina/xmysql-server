# XMySQL Server 修改清单（截至 2026-06-11）

目标：把当前仓库里「还没改完」的项按优先级拍平，直接用于下一步开发排期。

## 当前可复核基线

- 关键风险清单：`docs/planning/MODIFICATION_PENDING_ACTIONS_20260610.md`（P0 未闭环项仍是 `P0-03/P0-06/P0-07/P0-08/P0-09`）
- 当前扫描口径（`server/**/*.go`，关键词 `TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂时|暂未实现|not implemented in test|not implemented yet`）：
  - 命中：约 `212` 处
  - 受影响文件：约 `93` 个
- 说明：本轮只列“可执行清单”，不是全部代码审计覆盖项。

## P0（必须先改）

### P0-03 执行器失败可见性与错误分类（未闭环）

- 文件：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 要做：
  - 将失败返回统一为可读性更高的结构化错误（含阶段、schema/table、txnID、error_code）。
  - 去掉“字符串拼接回退”作为业务分支依据。
  - 增加失败路径测试（存储写入失败、唯一键冲突、提交失败、回滚失败），断言错误类型/码而非文本片段。
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DDL|DML|Storage|Transaction|Index)' -count=1`

### P0-06 崩溃恢复演练闭环（未闭环）

- 文件：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 要做：
  - 固定三类场景：`redo / undo / half-commit`。
  - 每类至少 3 轮复放。
  - 每轮产出：恢复前快照、恢复后快照、差异报告、耗时与重放命令。
- 验收：
  - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### P0-07 灰度写入阶段阻塞修复（未闭环）

- 文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
- 要做：
  - 复现并修复 `table mysql.t1 not found in storage mapping`。
  - 将 read/write/restore 三阶段串成可复放链路。
  - 失败时输出阶段号、schema/table、error_code、日志路径与耗时。
- 验收：
  - 三阶段链路完整通过，并有一次重演报告。

### P0-08 慢查询日志（未闭环）

- 文件：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 要做：
  - 完成 `slow_query_log` 开关、`slow_query_log_file`、`long_query_time_ms` 配置链路。
  - 强制记录字段：`sql / cost_ms / rows_affected / conn_id / txn_id / schema / table / error_code / error_msg / stage`
- 验收：
  - `go test` 覆盖慢查询记录分支与样例日志路径输出。

### P0-09 指标与告警导出（未闭环）

- 文件：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 要做：
  - 输出基础指标：QPS、错误率、连接数、活跃事务数、P50/P95/P99、redo/undo/锁等待。
  - 增加一条可触发并可恢复的告警演练。
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`

## P1（建议下一阶段）

### 查询执行能力缺口
- `server/innodb/engine/subquery_executor*.go`
- `server/innodb/engine/cte_executor.go`
- `server/innodb/engine/window_function_executor.go`
- `server/innodb/engine/select_executor.go`
- 目标：补齐子查询、CTE、窗口函数并补失败路径回归测试。

### 计划与统计器
- `server/innodb/plan/physical_plan.go`
- `server/innodb/plan/parallel.go`
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/statistics_collector_helpers.go`
- `server/innodb/plan/join_order_optimizer_helpers.go`
- `server/innodb/plan/statistics_collector_enhanced.go`

### 索引与存储关键缺口
- `server/innodb/engine/storage_integrated_index_helper.go`
- `server/innodb/manager/index_manager.go`
- `server/innodb/manager/enhanced_btree_manager.go`
- `server/innodb/storage/wrapper/record/*`

## P2（可以延后）

- `server/innodb/storage/wrapper/page/*.go`
- `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- `server/innodb/storage/store/mvcc/*`
- `server/innodb/storage/store/pages/compressed_page.go`
- `server/innodb/storage/wrapper/page/page_impl.go`

## 当前 TODO 热点文件 Top10（按扫描计数）

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/statistics_collector_helpers.go`（9）
3. `server/innodb/plan/parallel.go`（8）
4. `server/innodb/engine/index_reading_test.go`（8）
5. `server/innodb/manager/page_initialization_fix.go`（7）
6. `server/innodb/plan/physical_plan.go`（6）
7. `server/innodb/metadata/convert.go`（6）
8. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
9. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
10. `server/innodb/manager/page.go` / `server/innodb/engine/storage_integrated_index_helper.go`（5）

## 建议执行顺序

1. 先闭环 P0-03（错误可见性）；
2. 同步补齐 P0-06（崩溃恢复）；
3. 再补 P0-07（灰度写入阻塞）；
4. 并行推进 P0-08/P0-09（可观测）；
5. P0 全部可验收后再推进 P1。
