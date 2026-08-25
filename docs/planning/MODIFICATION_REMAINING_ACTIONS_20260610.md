# XMySQL Server 修改清单（截至 2026-06-10｜剩余项）

目标：明确当前还没改完的点，按执行顺序给出可直接落地动作和验收命令。

## 当前状态基线（可复核）

- `server/**/*.go` 中命中文本标记（TODO/FIXME/not implemented/unimplemented/stub/placeholder/待实现/未实现）共 `186` 处。
- 涉及文件 `85` 个。
- Top10 高频文件见本文最后。
- P0 风险项中，**当前明确未闭环：`P0-03`、`P0-06`、`P0-07`、`P0-08`、`P0-09`**。

## P0（必须先改）

- [ ] **P0-03 执行器失败可见性与错误分类**
  - 文件：
    - `server/innodb/engine/executor.go`
    - `server/innodb/engine/unified_executor.go`
    - `server/innodb/engine/storage_adapter.go`
    - `server/innodb/engine/dml_operators.go`
    - `server/innodb/engine/storage_integrated_dml_helper.go`
    - `server/innodb/engine/storage_integrated_index_helper.go`
  - 动作：
    - 统一错误上下文（module/sql/schema/table/txnID/stage/error_code）。
    - 彻底去掉文本判定为主流程分支（包括回滚失败、提交失败、去重冲突、表不存在/映射缺失）。
    - 加失败路径单测，直接断言错误字段而非字符串片段。
  - 验收：
    - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DDL|DML|Index|Storage|Transaction)' -count=1`

- [ ] **P0-06 崩溃恢复演练闭环**
  - 文件：
    - `server/innodb/manager/crash_recovery.go`
    - `scripts/crash_recovery_drill.sh`
    - `scripts/crash_recovery_process_drill.sh`
    - `scripts/p0_b_recovery_audit.sh`
    - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
  - 动作：
    - 固定 `redo / undo / half-commit` 三类输入脚本。
    - 每类至少 3 轮复放，输出恢复前快照、恢复后快照、差异文件。
    - 形成可复查目录结构与命名规范。
  - 验收：
    - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

- [ ] **P0-07 灰度写入阶段阻塞修复**
  - 文件：
    - `scripts/p0_e_backup_snapshot.sh`
    - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
    - `server/innodb/engine/`
    - `server/innodb/manager/`
  - 动作：
    - 复现并修复 `table mysql.t1 not found in storage mapping`。
    - 将只读/写入/回退三阶段串成可重放流程。
    - 写入阻塞输出：阶段号、schema、table、error_code、日志路径。
  - 验收：
    - 重新跑一次阶段链路，产出通过样例和耗时。

- [ ] **P0-08 慢查询日志**
  - 文件：
    - `server/conf/`
    - `server/session/`
    - `server/dispatcher/`
    - `server/innodb/engine/`
  - 动作：
    - 完成慢查询开关、阈值和输出配置。
    - 记录：`sql / cost_ms / rows_affected / conn_id / txn_id / schema / table / error_code / error_msg`。

- [ ] **P0-09 指标与告警导出**
  - 文件：
    - `server/innodb/manager/transaction_manager.go`
    - `server/innodb/manager/checkpoint_monitor.go`
    - `server/net/`
    - `server/conf/`
  - 动作：
    - 落地最小指标：QPS、错误率、连接数、活跃事务、P50/P95/P99、redo/undo/锁等待。
    - 增加至少一条可触发且可恢复的告警演练。
  - 验收：
    - 指标采集与告警路径有可复查输出，演练有成功/恢复记录。

## P1（P0 收口后排期）

- 子查询与复杂 SQL：`server/innodb/engine/subquery_executor*.go`、`server/innodb/engine/cte_executor.go`、`server/innodb/engine/window_function_executor.go`
- 执行/计划：`server/innodb/plan/physical_plan.go`、`server/innodb/plan/parallel.go`、`server/innodb/plan/cost_estimator.go`
- 统计与优化：`server/innodb/plan/statistics_collector_helpers.go`、`server/innodb/plan/statistics_collector_enhanced.go`
- 索引能力：`server/innodb/engine/storage_integrated_index_helper.go`、`server/innodb/manager/index_manager.go`、`server/innodb/manager/enhanced_btree_manager.go`
- 元数据转换：`server/innodb/metadata/convert.go`
- 表空间与页面读写：`server/innodb/manager/space_expansion_manager.go`、`server/innodb/storage/wrapper/page/*.go`（与 `mvcc`）

每项至少要求：

1. 一次有据可查的代码改动；
2. 一条失败路径/边界条件测试；
3. 一条可复跑的验证命令或脚本。

## Top10 高频 TODO 文件（本轮清理优先）

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/parallel.go`（8）
3. `server/innodb/engine/index_reading_test.go`（8）
4. `server/innodb/plan/physical_plan.go`（6）
5. `server/innodb/metadata/convert.go`（6）
6. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
7. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
8. `server/innodb/plan/statistics_collector_helpers.go`（5）
9. `server/innodb/manager/page.go`（5）
10. `server/innodb/engine/storage_integrated_index_helper.go`（5）

## 立即建议执行顺序

1. 先把 `P0-03` 闭环；
2. 并行推进 `P0-06`、`P0-07`；
3. 再推进 `P0-08`、`P0-09`；
4. 收口 P0 后进入 P1 阶段。
