# 修改清单（截至 2026-06-11）

用途：回答“项目还差哪些改动”时直接可用。此表以可落地执行为准，优先级按 P0/P1/P2 分组。

## 1) 当前基线

- `server/**/*.go` 关键词扫描（`TODO|FIXME|not implemented|unimplemented|placeholder|暂未实现|暂时|待实现|stub`）约 **215** 处，涉及约 **85** 个文件。
- 当前关键未闭环依然是：**P0-03、P0-06、P0-07、P0-08、P0-09**。
- 需要优先把 P0 问题闭环，才有意义继续推进 P1/P2。

## 2) P0（必须先改）

### P0-03 执行器失败可见性与错误分类（未闭环）
- 主要文件：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 待改：
  - 引入统一失败类型/错误码（含 `module/stage/sql/schema/table/txn_id/error_code/cause`）。
  - 关键失败路径不要再以错误文本做判断分支。
  - 为存储失败、表不存在、提交失败、回滚失败、唯一键冲突补结构化失败测试（`errors.As` 按错误码断言，而不是字符串包含）。
- 验收：
  - `go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML)' -count=1`

### P0-06 崩溃恢复演练闭环（未闭环）
- 文件：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 待改：
  - 固化 `redo` / `undo` / `half-commit` 场景输入与重放链路。
  - 每类至少 3 次复放，产出恢复前后快照、差异、耗时。
  - 统一命名报告目录，保证可复查。
- 验收：
  - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### P0-07 灰度写入阶段阻塞修复（未闭环）
- 文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
- 待改：
  - 复现并修复 `table mysql.t1 not found in storage mapping`。
  - read -> write -> restore 三阶段固定脚本化。
  - 失败日志补齐阶段号、schema、table、error_code、日志路径、耗时。

### P0-08 慢查询日志（未闭环）
- 文件：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 待改：
  - 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 配置链路。
  - slow log 内容至少包含 `sql,cost_ms,rows_affected,conn_id,txn_id,schema,table,error_code,error_msg,stage`。

### P0-09 指标与告警导出（未闭环）
- 文件：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 待改：
  - 输出 QPS、错误率、连接数、活跃事务、P50/P95/P99、redo/undo/锁等待指标。
  - 落地一条可触发可恢复的告警演练。

## 3) P1（在 P0 收口后排期）

- 查询执行与优化器能力
  - `server/innodb/engine/subquery_executor*.go`
  - `server/innodb/engine/cte_executor.go`
  - `server/innodb/engine/window_function_executor.go`
  - `server/innodb/plan/physical_plan.go`
  - `server/innodb/plan/parallel.go`
  - `server/innodb/plan/cost_estimator.go`
  - `server/innodb/plan/statistics_collector_helpers.go`
- 索引与页管理
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/manager/index_manager.go`
  - `server/innodb/manager/enhanced_btree_manager.go`
  - `server/innodb/storage/wrapper/page/*.go`
  - `server/innodb/storage/wrapper/mvcc/mvcc_page.go`
- 典型动作：
  - 每一块先补关键失败场景测试，再补功能实现。

## 4) P2（可延后）
- `server/innodb/storage/store/pages/compressed_page.go`
- `server/innodb/storage/store/mvcc/*`
- 页面/空间管理器中的低优先级重构项（已标注 TODO 的实现位）。

## 5) 这批文件是近期最值得直接改的 TODO 热点（Top）
1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/statistics_collector_helpers.go`（9）
3. `server/innodb/plan/parallel.go`（8）
4. `server/innodb/engine/index_reading_test.go`（8）
5. `server/innodb/metadata/convert.go`（6）
6. `server/innodb/manager/page_initialization_fix.go`（7）
7. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
8. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
9. `server/innodb/plan/join_order_optimizer_helpers.go`（5）
10. `server/innodb/engine/storage_integrated_index_helper.go`（5）

## 6) 立即执行建议顺序

1. 先闭环 P0-03（执行器错误分类是阻断级）。
2. 同步推进 P0-06 与 P0-07 的脚本化演练。
3. 并行补 P0-08、P0-09，补齐可观测。
4. P0 全部通过后进入 P1。
