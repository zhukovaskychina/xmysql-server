# XMySQL Server 修改清单（生成版，2026-06-11）

生成时间：2026-06-11 09:30（本地时区 Asia/Shanghai）

本清单用于回答“还差什么要改”，用于下一轮提交前的改动排期。优先级按 P0 / P1 / P2 排序。

## 0. 当前基线（最近扫描）

- 范围：`server/innodb/engine`  
- 关键统计：
  - `fmt.Errorf` 仍大量存在，核心文件 Top：
    - `executor.go`（90）
    - `dml_operators.go`（57）
    - `dml_executor.go`（67）
    - `unified_executor.go`（54）
    - `storage_integrated_dml_executor.go`（54）
    - `storage_integrated_dml_helper.go`（30）
    - `storage_integrated_index_helper.go`（17）
    - `volcano_executor.go`（67）
    - `select_executor.go`（31）
  - `server/innodb/engine` 中未看到 `errors.As(err, *ExecutionError)` 的测试断言。
  - 仍可见未实装/占位型返回：
    - `server/innodb/engine/show_executor.go`（3 处返回“暂未接入真实元数据字典”）
    - `server/innodb/engine/index_reading_test.go` / `unified_executor_test.go`（测试桩）
    - `server/innodb/engine/dml_operators.go`（`findDuplicateRecord not fully implemented`）

## 1. 已经闭环（本轮前置基础）

- 已完成：
  - `storage_adapter.go` 的关键读写/元数据路径引入了 `ExecutionError`。
  - `index_transaction_adapter.go` 的 begin/commit/rollback、锁申请、范围扫描、点查询等关键路径已引入结构化错误。
  - `dml_executor.go` 的元数据读取与值校验已从临时 ID 迁移到字典/Manager 获取。
  - `storage_integrated_dml_executor.go` 已改为字典元数据驱动。
  - `record` 与 `manager` 的事务 ID 写入偏移位置已统一到 `[5:13]`。

## 2. P0：必须先补齐（阻塞发布）

### P0-03 执行器错误统一（未闭环）

目标：把执行链路失败从字符串错误切到可观测错误码，关键失败必须返回 `ExecutionError`。

- [ ] `executor.go`
  - `buildExecutorTree`, `executeInsertStatement`, `executeUpdateStatement`, `executeDeleteStatement`, `executeCreate/Drop/DML Database` 入口
  - `checkStorageManagerReady`、`validateSchemaAndTable`、建树/生成计划失败分支
  - `CREATE TABLE / DROP TABLE / SHOW` 等在执行前后的错误上下文要带 `schema/table/stage/txnID`

- [ ] `unified_executor.go`
  - `Execute`, `executeInsert/Update/Delete`, `buildUnifiedOperatorTree`, `buildPhysicalPlan`, `resolveSQLLiteral`
  - `physicalPlan == nil / operator == nil / metadata nil / adapter nil` 的分支改为 `ExecutionError`

- [ ] `dml_executor.go`
  - `processINSERT/UPDATE/DELETE` 的事务开始/提交/回滚错误码化
  - `parseInsertRows`, `parseTableName`, `validateValueType`, `getColumnValue` 等校验失败错误码化
  - `createStorageManager/getTableStorageManager` 走统一错误码分支（metadata/storage missing）

- [ ] `dml_operators.go`
  - `findDuplicateRecord` 取消 `not fully implemented`
  - `insert`, `update`, `delete` 与 `insertRow/updateRecord/deleteOldRecord/insertNewRecord` 的错误改为代码可比对
  - 避免通过错误字符串做业务分支，改为显式错误码（例如重复键、记录未找到、算子未打开）

- [ ] `storage_integrated_dml_executor.go`
  - `processINSERT/UPDATE/DELETE`、`syncSecondaryIndexForUpdate/Delete`、`commit`/`rollback`、`buildPrimaryKey` 路径错误码化

- [ ] `storage_integrated_dml_helper.go`
  - 序列化/反序列化、事务上下文、页面读取、WAL/索引操作失败路径错误码化

- [ ] `storage_integrated_index_helper.go`
  - 索引项构造、主键提取、`insert/delete index entry`、唯一索引冲突、重建/一致性检查 TODO 区域清理

- [ ] `volcano_executor.go`
  - 67 处 `fmt.Errorf` 的高风险边界按模块补齐 `ExecutionError`
  - 对 `operator not opened`、`failed to open/reopen`、`failed to open root operator` 统一加错误码与阶段

- [ ] 测试补齐（直接验收要求）
  - `server/innodb/engine` 新增失败路径测试，至少覆盖：
    - 表不存在
    - 元数据缺失
    - 存储读写失败
    - 唯一键冲突
    - 事务 begin/commit/rollback 失败
  - 每条失败用例都断言：
    - `var ee *engine.ExecutionError; errors.As(err, &ee)`
    - `ee.ErrorCode` 为预期值
    - 日志带 `stage / schema / table / txn_id / error_code`

建议验收命令：
```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|Index|DDL|DML)' -count=1
```

### P0-06 崩溃恢复演练闭环（未闭环）

- [ ] 固定三类场景：redo、undo、half-commit
- [ ] 每类执行 3 轮 `start -> crash -> restart -> verify`
- [ ] 每轮归档：
  - 演练前后快照
  - 差异报告
  - 耗时与错误码汇总
- [ ] 主要文件：
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `server/innodb/manager/crash_recovery.go`

### P0-07 灰度写入阻塞修复（未闭环）

- [ ] 修复 `table mysql.t1 not found in storage mapping` 真实复现场景
- [ ] 将 read/write/restore 三阶段写成单链路脚本
- [ ] 输出阶段日志字段：`stage/schema/table/error_code/log_path/duration_ms`
- [ ] 主要文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/engine/*`
  - `server/innodb/manager/*`

### P0-08 慢查询与执行追踪（未闭环）

- [ ] 配置链路：`slow_query_log / slow_query_log_file / long_query_time_ms`
- [ ] 执行记录字段最少包含：
  - `sql, cost_ms, rows_affected, conn_id, txn_id, schema, table, error_code, error_msg, stage`
- [ ] 文件范围：
  - `server/conf/`
  - `server/session/`
  - `server/dispatcher/`
  - `server/innodb/engine/`

### P0-09 可观测指标与告警（未闭环）

- [ ] 增加基础指标：QPS、错误率、活跃事务、P50/P95/P99、redo/undo/锁等待
- [ ] 增加告警触发与恢复验证流程，避免指标写在日志但不可复放
- [ ] 文件范围：
  - `server/innodb/manager/transaction_manager.go`
  - `server/innodb/manager/checkpoint_monitor.go`
  - `server/net/`
  - `server/conf/`

## 3. P1：非阻塞但建议同步推进

- 优化与算子覆盖：`server/innodb/plan/*`（并行、代价估算、CTE、窗口函数）
- 索引与一致性边界：`server/innodb/engine/storage_integrated_index_helper.go`、`server/innodb/manager/index_manager.go`
- 一些功能模块仍是 TODO 压力点（不影响当下 P0 先行）：
  - `server/innodb/storage/wrapper/record/*`
  - `server/innodb/storage/store/mvcc/*`
  - `server/innodb/storage/wrapper/system/*`

## 4. 立刻可执行顺序（建议）

1. 当天冲刺先关掉 `P0-03`：优先做 `executor.go`、`unified_executor.go`、`dml_operators.go`、`storage_integrated_dml_executor.go`，每个文件完成后补失败路径断言测试。
2. 同步推进 `P0-06` 与 `P0-07`（脚本+证据）以避免演练卡点。
3. 并行执行 `P0-08`、`P0-09` 最小闭环。
4. P0 全部过完后再开始 P1。
