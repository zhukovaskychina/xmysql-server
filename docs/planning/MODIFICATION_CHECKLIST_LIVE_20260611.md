# XMySQL Server 修改清单（2026-06-11 可执行版）

适用范围：当前工作树快照（以 `/Users/zhukovasky/GolandProjects/xmysql-server` 为准）

## 1. 当前基线（先看是否继续改）

- `ExecutionError` 已经有定义，但主执行链路还未闭环：
  - `server/innodb/engine/executor.go` 中已做了部分结构化改造。
  - `server/innodb/engine/unified_executor.go`、`dml_operators.go`、`storage_integrated_dml_executor.go`、`storage_integrated_dml_helper.go` 仍大量是 `fmt.Errorf`。
- 重点文件当前 `fmt.Errorf` 计数（最新扫描）：
  - `executor.go`：66
  - `unified_executor.go`：54
  - `dml_operators.go`：57
  - `storage_integrated_dml_executor.go`：54
  - `storage_integrated_dml_helper.go`：30
  - `dml_executor.go`：67
  - `storage_integrated_index_helper.go`：17
  - `storage_adapter.go`：4（已有辅助转换函数，但未全部接入）
  - `index_transaction_adapter.go`：23
- `engine` 内 `TODO/FIXME/unimplemented` 仍有未闭环点（关键）
  - `findDuplicateRecord not fully implemented`（`dml_operators.go`）
  - `index_transaction_adapter.go` 存在锁行为 TODO
  - `storage_integrated_index_helper.go` 仍有索引重建/一致性 TODO
- `errors.As(err, *ExecutionError)` 的用例几乎没有；当前未形成失败码断言的回归。

结论：最核心仍是 **P0-03 执行器错误统一**，其次是 **P0-06~P0-09 可观测与演练闭环**。

## 2. P0-03：执行器错误统一（阻断级）

> 目标：失败返回都能用结构化错误码判断，减少字符串分支。

### 2.1 `server/innodb/engine/executor.go`

- [ ] 把这些路径改成 `ExecutionError`：
  - `executeSetStatement`：
    - `session is required for SET statements`
    - `SET statement failed: ...`
  - `setSessionVariable`：
    - `session is nil`
    - `variable name cannot be empty`
  - `evaluateSetValue`：
    - `invalid integer/float value`
  - `resolveTableExprSchema`：不支持表表达式的分支
  - `executeCreateTableStatement`：
    - `table '<name>' already exists`
  - `executeDropTableStatement`：
    - `table name cannot be empty`
    - `no database selected`
    - `DROP TABLE failed` 分支中的下游错误信息
  - `create/drop` 及 `DROP DATABASE` / `CREATE DATABASE` 辅助函数里仍有文本错误（例如 `database 'x' does not exist`、目录删除/创建失败类）
  - `executeShowStatement` / `executeShowStatementWithQuery` / `executeShowTables` / `executeShowColumns` / `resolveShowColumnsTarget` / `getShowColumnsTableMetadata`
    - 上述分支仍有直出 `fmt.Errorf`，应补 `ErrorCode` 与上下文
- [ ] 对已改造路径统一字段完整性：`module/stage/schema/table/sql/txn_id/error_code/cause`

### 2.2 `server/innodb/engine/unified_executor.go`

- [ ] 全量改造 `Err` 返回：
  - `Execute` 的入口分支（SELECT/INSERT/UPDATE/DELETE）
  - `buildSelectOperatorTree` / `collectSelectResult` / `buildOperatorTree`
  - `resolveSQLLiteral` / `resolveLimit` / `resolveOrderBy`
  - `storage adapter is not initialized`、`metadata/table missing` 分支
- [ ] 去掉“用错误文本控制流程”的写法，关键分支改为阶段错误码。

### 2.3 `server/innodb/engine/dml_operators.go`

- [ ] 处理 DML 核心未闭环点：
  - `findDuplicateRecord` 当前仍返回 `not fully implemented`
  - `insertRow` / `updateRow` / `deleteRecord` 关键错误返回统一
  - `operator not opened`、事务 begin/commit/rollback 失败路径显式返回结构化错误码
  - `VALUES()`、`ON DUPLICATE`、表达式计算错误分支统一结构化
- [ ] 与 `storage_adapter` / `index_transaction_adapter` 对齐错误码含义（至少区分元数据、存储、索引、事务、验证失败）

### 2.4 `server/innodb/engine/storage_integrated_dml_executor.go`

- [ ] 所有 DML 入口（INSERT/UPDATE/DELETE）及事务入口改为 `ExecutionError`
  - `storageManager`、`tableManager`、`metadata` 空值
  - 主键生成、序列化、索引同步、存储事务 begin/commit/rollback
- [ ] 补“失败场景回归”测试用例：
  - 元数据缺失
  - storage 读写失败
  - 唯一键冲突
  - begin/commit/rollback 失败

### 2.5 `server/innodb/engine/storage_adapter.go`

- [ ] 检查并替换剩余文本返回：
  - 页面读取/解析失败、记录检索失败、元数据找不到、注册索引失败
- [ ] 确认每条失败都带 `schema/table`（能从路径推得时尽量补）

### 2.6 `server/innodb/engine/index_transaction_adapter.go`

- [ ] 事务与锁的错误码统一（acquire/release、锁类型、事务状态）
- [ ] `ReleaseLock` TODO 行为明确化（当前偏向全量释放）

### 2.7 `server/innodb/engine/storage_integrated_index_helper.go`

- [ ] `TODO` 位点与中文错误文案统一成错误码断言友好的结构，不要只在日志里体现。

### 2.8 `server/innodb/engine/storage_integrated_dml_helper.go`

- [ ] 表达式/反序列化/事务上下文/页读取失败改为 `ExecutionError`。

### 2.9 `server/innodb/engine/storage_adapter.go`（补充）

- [ ] 验证 `newStorageAdapterError` 已覆盖的路径是否被所有新分支使用，避免混回到 `fmt.Errorf`。

### 验收（最小）

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*show|Test.*Set|Test.*CreateTable|Test.*DropTable' -count=1
```

要求：关键用例必须有 `errors.As(err, &ee)` 且 `ee.ErrorCode` 命中预期码。

## 3. P0-06 崩溃恢复演练（未闭环）

- [ ] 固化并复放三类场景：`redo`、`undo`、`half-commit`
- [ ] 每类至少 3 次演练（`start -> crash -> restart -> verify`）
- [ ] 每次输出：
  - 恢复前快照
  - 恢复后快照
  - 差异报告
  - 耗时与错误码
- [ ] 受影响文件：
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `server/innodb/manager/crash_recovery.go`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

## 4. P0-07 灰度阶段阻塞修复（未闭环）

- [ ] 修复 `table mysql.t1 not found in storage mapping` 真实复现场景
- [ ] read/write/restore 做成一条链式演练脚本
- [ ] 失败日志必须带：`stage/schema/table/error_code/log_path/duration_ms`
- [ ] 受影响文件：
  - `scripts/p0_e_backup_snapshot.sh`
  - `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
  - `server/innodb/engine/*`
  - `server/innodb/manager/*`

## 5. P0-08 慢查询日志

- [ ] 完成配置链路：`slow_query_log`、`slow_query_log_file`、`long_query_time_ms`
- [ ] 输出字段至少包含：
  - `sql, cost_ms, rows_affected, conn_id, txn_id, schema, table, error_code, error_msg, stage`
- [ ] 受影响：`server/conf/`、`server/session/`、`server/dispatcher/`、`server/innodb/engine/`

## 6. P0-09 指标与告警

- [ ] 补充并导出最小指标：QPS、错误率、连接数、活跃事务、P50/P95/P99
- [ ] 补充 redo/undo/锁等待指标的可读路径
- [ ] 建立告警触发与恢复的复放用例
- [ ] 受影响：`server/innodb/manager/transaction_manager.go`、`server/innodb/manager/checkpoint_monitor.go`、`server/net/`、`server/conf/`

## 7. 执行顺序（建议）

1. 先闭环 P0-03（优先文件顺序：`unified_executor.go` -> `dml_operators.go` -> `storage_integrated_dml_executor.go` -> `storage_integrated_dml_helper.go` -> `executor.go` 中剩余分支）
2. 同步推进 P0-06 与 P0-07
3. 并行推进 P0-08、P0-09
4. P0 全部绿灯后再做 P1
