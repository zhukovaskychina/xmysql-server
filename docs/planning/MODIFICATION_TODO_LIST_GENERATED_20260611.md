# XMySQL Server 修改清单（生成版，2026-06-11）

> 目标：回答“还有哪些需要改”，并给出可以直接排期执行的待办列表。
> 说明：基线来自 `docs/planning/MODIFICATION_CHECKLIST_20260610.md` 与近期扫描结果，仅做整理，不替代单条修复方案。

## 1. 当前基线

- 当前勾选状态（基于 `MODIFICATION_CHECKLIST_20260610.md`）：
  - 已完成：17
  - 未完成：57
- 关键结论：P0 里 5 项（P0-03、P0-06、P0-07、P0-08、P0-09）仍未闭环。
- 关键词扫描基线（`server/**/*.go`）：
  - TODO/FIXME/未实现 等命中约 `214` 处
  - 受影响文件约 `95` 个

## 2. P0：必须优先改的项

### [ ] P0-03 关键执行路径错误可见性（优先级 P0）
- 目标：失败不再吞错，不再靠文本匹配做核心分支
- 涉及文件：
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/unified_executor.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/dml_operators.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 子任务：
  - [ ] 补齐失败场景统一结构化错误（SQL、schema、table、txn_id、stage、error_code）
  - [ ] 去掉对错误字符串的分支判断
  - [ ] 增加失败路径单测（表不存在、存储失败、唯一键冲突、提交失败、回滚失败）
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DDL|DML|Storage|Transaction|Index)' -count=1`

### [ ] P0-06 崩溃恢复演练闭环（优先级 P0）
- 目标：3 类场景稳定可复放，给出可复核证据
- 涉及文件：
  - `server/innodb/manager/crash_recovery.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 子任务：
  - [ ] 固定 `redo / undo / half-commit` 三类输入
  - [ ] 每类至少 3 次复现，含 start→crash→restart→verify
  - [ ] 每轮产出恢复前/后快照、差异报告、耗时记录
- 验收：
  - `CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh`

### [ ] P0-07 灰度写入阶段修复（优先级 P0）
- 目标：消除 `table mysql.t1 not found in storage mapping` 并串通 3 阶段
- 涉及文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `server/innodb/engine/`
  - `server/innodb/manager/`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- 子任务：
  - [ ] 修复只读/写入阶段映射阻断问题
  - [ ] 串接只读→写入→回退脚本链路
  - [ ] 增加阶段耗时、错误码、日志路径输出
- 验收：
  - 三阶段链路通过，产出一次重演报告

### [ ] P0-08 慢查询日志（优先级 P0）
- 目标：支持最低可观测闭环
- 涉及文件：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`
- 子任务：
  - [ ] 完成 `slow_query_log`、`slow_query_log_file`、`long_query_time_ms` 配置链路
  - [ ] 记录字段：`sql / cost_ms / rows_affected / conn_id / txn_id / schema / table / error_code / error_msg / stage`
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/dispatcher ./server/innodb/engine -run 'Test.*Slow.*Query|Test.*Log' -count=1`

### [ ] P0-09 指标与告警（优先级 P0）
- 目标：形成最小可接入监控和告警闭环
- 涉及文件：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`
- 子任务：
  - [ ] 导出 QPS、错误率、连接数、活跃事务、P50/P95/P99
  - [ ] 导出 checkpoint/redo/undo/锁等待
  - [ ] 实现最小告警触发与恢复演练
- 验收：
  - `/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager ./server/net -run 'Test.*(Stats|Metrics|Alert|Monitor)' -count=1`

## 3. P1：建议下一阶段

- [ ] P1-01 子查询执行与优化（`subquery_executor*`、`apply_operator*`、`physical_plan.go`）
- [ ] P1-02 窗口函数（`window_function_executor.go`）
- [ ] P1-03 HAVING/DISTINCT/LIMIT OFFSET（`select_executor.go`、`volcano_executor.go`）
- [ ] P1-04 代价估算与选择率（`plan/physical_plan.go`、`plan/cost_estimator.go`、`plan/selectivity_estimator.go`）
- [ ] P1-05 统计信息采集真实化（`plan/statistics_collector_helpers.go`、`plan/statistics_collector_enhanced.go`）
- [ ] P1-06 页读写实现闭环（`storage/wrapper/page/*.go`）
- [ ] P1-07 MVCC 页序列化（`storage/wrapper/mvcc/mvcc_page.go`）
- [ ] P1-08 索引一致性/重建/优化（`engine/storage_integrated_index_helper.go`、`manager/index_manager.go`、`manager/enhanced_btree_manager.go`）

## 4. P2：可延后

- [ ] 协议与高级对象能力补齐（`server/net/`、`server/protocol/`、`jdbc_client/`）
- [ ] `COM_STMT_SEND_LONG_DATA`、`COM_FIELD_LIST`、多结果集、binlog/GTID
- [ ] 触发器、视图、分区表、在线 DDL

## 5. TODO 热点文件 Top 10（用于分批处理）

1. `server/innodb/manager/space_expansion_concurrent_test.go`（10）
2. `server/innodb/plan/statistics_collector_helpers.go`（9）
3. `server/innodb/plan/parallel.go`（8）
4. `server/innodb/engine/index_reading_test.go`（8）
5. `server/innodb/manager/page_initialization_fix.go`（7）
6. `server/innodb/plan/physical_plan.go`（6）
7. `server/innodb/metadata/convert.go`（6）
8. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`（5）
9. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`（5）
10. `server/innodb/plan/join_order_optimizer_helpers.go`（5）

## 6. 建议本周执行顺序

1. 先闭环 P0-03（错误分类 + 失败测试）
2. 然后补 P0-06（恢复演练 + 报告）
3. 再推进 P0-07（灰度读写链路）
4. 并行推进 P0-08 / P0-09（慢查询与指标告警）
5. P0 全部绿灯后进入 P1
