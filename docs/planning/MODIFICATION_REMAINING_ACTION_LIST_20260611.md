# 修改清单（截至 2026-06-11）

目的：把“还差什么要改”变成可以直接排期和验收的任务单。数据基于当前仓库状态（`server/**/*.go`, `scripts/*.sh`）生成。

## 一、先看结论

- `P0-03`（执行器失败可见性）仍是发布前第一阻塞项。
- `P0-06`（恢复演练）、`P0-07`（灰度写入阻塞）、`P0-08`（慢查询）、`P0-09`（指标告警）尚未闭环。
- `ExecutionError` 已有定义：`server/innodb/engine/execution_error.go`，但尚未在主要执行链路作为统一返回路径。

当前高频文本错误热区（关键文件）

- `server/innodb/engine/executor.go`：`90` 个 `fmt.Errorf`
- `server/innodb/engine/unified_executor.go`：`54` 个 `fmt.Errorf`
- `server/innodb/engine/dml_executor.go`：`67` 个 `fmt.Errorf`
- `server/innodb/engine/dml_operators.go`：`57` 个 `fmt.Errorf`
- `server/innodb/engine/storage_adapter.go`：`12` 个 `fmt.Errorf`
- `server/innodb/engine/storage_integrated_dml_executor.go`：`54` 个 `fmt.Errorf`
- `server/innodb/engine/storage_integrated_dml_helper.go`：`30` 个 `fmt.Errorf`
- `server/innodb/engine/storage_integrated_index_helper.go`：`17` 个 `fmt.Errorf`
- `server/innodb/engine/index_transaction_adapter.go`：`23` 个 `fmt.Errorf`

> 注：`fmt.Errorf` 不等于 bug，但当前场景下它意味着很多错误路径尚未标准化。

## 二、P0（必须先做）

### P0-03 执行器错误可见性与失败闭环（最优先）

- `server/innodb/engine/executor.go`
  - `buildExecutorTree` 及 `execute*` 中错误返回改为 `ExecutionError`
  - 消除“解析/元数据/优化/算子构建”失败时仅靠文本判断的路径

- `server/innodb/engine/unified_executor.go`
  - SELECT/INSERT/UPDATE/DELETE 的关键路径把关键失败返回改为结构化错误码
  - `Where/OrderBy/TableExpr` 相关的失败不再以纯文本分支驱动业务

- `server/innodb/engine/dml_executor.go`
  - 保留 DML 执行主链路的 `ExecutionError` 入场（而不是字符串错误返回）

- `server/innodb/engine/dml_operators.go`
  - `insert/update/delete` 关键路径的错误码改造
  - `findDuplicateRecord`、`update`、`delete`、`parseInsertRows` 等失败码统一

- `server/innodb/engine/storage_adapter.go`
  - 继续使用 `newStorageAdapterError`，覆盖元数据/读写/解析失败点

- `server/innodb/engine/storage_integrated_dml_executor.go`
  - `INSERT/UPDATE/DELETE`、主事务、索引变更、回滚都补充 `ExecutionError`

- `server/innodb/engine/storage_integrated_dml_helper.go`
  - `serialize` / `deserialize` / `begin` / `commit` / `rollback` / `getPage` 错误码统一

- `server/innodb/engine/storage_integrated_index_helper.go`
  - 索引约束失败与序列化失败统一返回 `ExecutionError`，移除文本兜底分支

- `server/innodb/engine/index_transaction_adapter.go`
  - 继续统一锁与事务边界错误码路径
  - `RangeScan/GetLockManager/AcquireLock/ReleaseLock` 的错误返回补全结构化信息

- `server/innodb/engine/unified_executor_test.go`
  - 补失败路径测试：`errors.As(err, *engine.ExecutionError)` + `ErrorCode`
  - 至少覆盖：
    - 无表/无 schema
    - 元数据缺失
    - 开启/提交/回滚事务失败
    - 重复键
    - storage 读写失败

### P0-06 崩溃恢复演练闭环

- `scripts/crash_recovery_drill.sh`
- `scripts/crash_recovery_process_drill.sh`
- `scripts/p0_b_recovery_audit.sh`
- `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

待改：

- 固定 redo / undo / half-commit 三类场景输入
- 每类至少 3 次 `start -> crash -> restart -> verify`
- 每轮固定输出：`before.json`、`after.json`、`diff.json`、日志路径、耗时、摘要

### P0-07 灰度写入阻塞

- `scripts/p0_e_backup_snapshot.sh`
- `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- `server/innodb/engine/`、`server/innodb/manager/`（storage mapping 和 DDL/DML 映射链路）

待改：

- 修复 `table ... not found in storage mapping` 阻塞场景
- read/write/restore 三阶段做成可复放流程
- 每次写入演练带 `stage/schema/table/error_code/log_path/duration_ms`

### P0-08 慢查询日志

- `server/conf/`
- `server/session/`
- `server/dispatcher/`
- `server/innodb/engine/`

待改：

- 完成慢查询配置链路（开关、文件、阈值）
- 结构化落盘字段至少包含
  - `sql,cost_ms,rows_affected,conn_id,txn_id,schema,table,error_code,error_msg,stage`

### P0-09 指标与告警

- `server/innodb/manager/transaction_manager.go`
- `server/innodb/manager/checkpoint_monitor.go`
- `server/net/`
- `server/conf/`

待改：

- 导出并可验证：QPS、错误率、活跃连接、活跃事务、P50/P95/P99、redo/undo/锁等待
- 建立最小可复盘告警演练

## 三、P1（P0 全绿后）

- 子查询执行：`server/innodb/engine/subquery_executor.go`
- CTE：`server/innodb/engine/cte_executor.go`
- 窗口函数：`server/innodb/engine/window_function_executor.go`
- 优化器相关：`server/innodb/plan/physical_plan.go`、`plan/cost_estimator.go`、`plan/statistics_collector_helpers.go`
- 索引一致性：`server/innodb/engine/storage_integrated_index_helper.go`、`server/innodb/manager/index_manager.go`、`server/innodb/manager/enhanced_btree_manager.go`

## 四、执行建议

1. 今日先收敛 `P0-03`，其余 P0 并行推进。
2. 每个文件改完后先补 1-2 个失败路径测试再合并。
3. `P0-06` 和 `P0-07` 使用脚本输出文件作为验收证据。

## 五、建议验收命令

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|Index|DDL|DML)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'Test.*(Recovery|Metric|Monitor|Alert|Snapshot)' -count=1
CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh
./scripts/p0_e_backup_snapshot.sh list
```
