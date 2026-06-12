# XMySQL Server 修改清单（截至 2026-06-10）

## 0. 说明

这份清单基于最近一次仓库扫描（`server/**/*.go`）和现有 P0 清单补齐后的状态。
目标是把“下一步可执行工作”按优先级整理出来，便于直接排期。

- 代码扫描命中标记数量：`191`（`TODO|FIXME|not implemented|未实现|未完成|stub`）
- 受影响文件：约 `78` 个
- 说明：文档里的 `P0` 与 `P1` 以发布风险为准，`P2` 可在稳定后推进

## 1. 本轮已完成（你让我继续后的补丁）

- [x] `server/innodb/engine/dml_operators.go`
  - `InsertOperator.Next`、`UpdateOperator.Next`、`DeleteOperator.Next` 的回滚失败现在不再 `_ = RollbackTransaction(...)`，错误返回中会带上回滚失败信息。
  - 给提交失败和回滚失败增加了操作上下文日志（schema/table/txnID）。

- [x] `server/innodb/engine/storage_adapter.go`
  - `GetTableMetadata` 增加依赖检查：`tableManager`、`tableStorageManager`。
  - `ReadPage` 增加 `bufferPoolManager` 空值防护。
  - `ParseRecords` 增加 `page` 与 `page content` 空值防护。

- [x] `server/innodb/engine/storage_adapter_test.go`
  - 增补了 `GetTableMetadata`、`ReadPage`、`ParseRecords` 的依赖缺失/空值错误路径断言。

## 2. P0：发布前必须补齐

### P0-02 发布与测试门禁

- [x] 明确并固定 `go test` 门禁边界：
  - 默认走发布包集合。
  - 通过 `P0_RELEASE_TEST_SCOPE=full` 可尝试全量 `go test ./...`。
  - 通过 `P0_RELEASE_ENFORCE_FULL_TEST=1` 可将 `go test ./...` 设为严格门禁。
- [x] `scripts/p0_release_package_tests.sh` 已补齐可审计字段：
  - Go 版本
  - 命令
  - 通过/失败包
  - 失败样例、失败原因
  - 耗时与重试次数
  - 证据：`reports/p0_release_tests/20260610_*/summary.log`

### P0-03 执行器错误可见性（未完成项）

- [ ] `server/innodb/engine/executor.go`
  - 统一失败上下文（SQL、schema、table、阶段）
  - 降低“返回成功但语义不清晰”的路径

- [ ] `server/innodb/engine/unified_executor.go`
  - DML / SELECT 失败路径补充统一错误上下文
  - `collectSelectResult` 的错误要带上结果语义，不只返回底层原始错误

- [ ] `server/innodb/engine/storage_integrated_index_helper.go`
  - 从“文本判断”逐步减少到“结构化错误分支”

- [ ] `server/innodb/engine/storage_adapter.go`
  - `GetTableMetadata`、`ReadPage`、`ParseRecords` 仍有可优化空间（比如输入参数校验统一化）

- [ ] `server/innodb/engine/dml_operators.go`
  - 本轮已修复回滚吞掉问题；下一步补齐 `Next` 的失败分类测试：事务失败、回滚失败、提交失败三类均需可断言。

### P0-06 崩溃恢复演练

- [ ] `server/innodb/manager/crash_recovery.go`
  - 删除显式返回“未完成/未实现”分支，补齐 redo/undo/半提交闭环

- [ ] `scripts/crash_recovery_drill.sh`、`scripts/crash_recovery_process_drill.sh`
  - 固定演练场景输入，生成可复用 report（含重放差异）
  - 连续至少 3 轮演练，保留历史记录与比对

- [ ] `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`、`reports/p0_b_audit/`
  - 演练入口、结果目录、失败回放步骤需统一规范

### P0-07 灰度写入阶段阻塞修复

- [ ] `scripts/p0_e_backup_snapshot.sh`
  - 对齐只读/写入/回退三阶段，输出分阶段耗时与失败路径

- [ ] `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - 补齐可复现步骤与回放命令

- [ ] `server/innodb/manager/`、`server/innodb/engine/`
  - 复核 `table mysql.t1 not found in storage mapping` 根因，加入定位日志

### P0-08 慢查询日志

- [ ] `server/conf/`
  - 增加慢查询开关与阈值配置

- [ ] `server/session/`、`server/dispatcher/`、`server/innodb/engine/`
  - 记录完整字段：SQL、耗时、影响行数、连接 ID、事务 ID、错误码

### P0-09 指标与告警

- [ ] `server/innodb/manager/transaction_manager.go`
- [ ] `server/innodb/manager/checkpoint_monitor.go`
- [ ] `server/net/`
  - 输出最小监控指标（QPS、错误率、连接数、活跃事务、P50/P95/P99）

- [ ] `server/innodb/manager/checkpoint_monitor.go`
  - 至少一条可触发告警与恢复记录

## 3. P1：中优先（建议下一阶段）

- [ ] `server/innodb/engine/`：子查询 / CTE / 窗口函数
- [ ] `server/innodb/plan/physical_plan.go`、`server/innodb/plan/parallel.go`、`server/innodb/plan/cost_estimator.go`
  - 代价估算与执行路径能力
- [ ] `server/innodb/engine/storage_integrated_index_helper.go`
  - 索引重建、优化、完整性检查（本文件当前 `TODO` 密集）

## 4. P2：可延后

- [ ] `server/innodb/storage/wrapper/*`：页面/序列化/读写 TODO 仍多
- [ ] `server/innodb/storage/store/mvcc/*`：MVCC 与 undo/trx sys 若干未实现项
- [ ] `server/innodb/metadata/convert.go`、`server/innodb/storage/wrapper/record/*`

## 5. 本次扫描建议优先查看（按 TODO 数）

1. `server/innodb/manager/space_expansion_concurrent_test.go`
2. `server/innodb/plan/parallel.go`
3. `server/innodb/engine/index_reading_test.go`
4. `server/innodb/plan/physical_plan.go`
5. `server/innodb/storage/wrapper/page/page_inode_wrapper.go`
6. `server/innodb/storage/wrapper/page/page_allocated_wrapper.go`
7. `server/innodb/plan/statistics_collector_helpers.go`
8. `server/innodb/manager/page.go`
9. `server/innodb/manager/crash_recovery.go`
10. `server/innodb/engine/storage_integrated_index_helper.go`

## 6. 你要我继续的下一步（建议）

1. 先把 `P0-03` 剩余项切成测试驱动补齐：至少增加 3 个失败路径测试。
2. 接着处理 `P0-06` 的管理器阶段未完成标记，目标是演练脚本可以连续运行 3 轮且可复放。
3. 再处理 `P0-07` 的灰度演练链路。
