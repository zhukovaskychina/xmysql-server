# XMySQL Server 2026-06-11 修改清单（待处理项）

更新时间：2026-06-11  
生成依据：仓库当前代码与 `rg` 扫描结果（以 `server/innodb` 关键路径为主），不包含用户语义变更建议，仅列出尚需落地的工程改动。

## 一、总体结论

- P0 级改动尚未闭环，最关键是 `P0-03`（执行器失败可见性）与稳定性演练链路 `P0-06~P0-09`。
- `ExecutionError` 已经有定义且可复用，但关键执行链路仍大量返回 `fmt.Errorf`，无法统一按错误码处理。
- 与“错误可观测性”直接相关的最紧急文件：
  - `server/innodb/engine/executor.go`（约 90 处）
  - `server/innodb/engine/unified_executor.go`（约 54 处）
  - `server/innodb/engine/dml_operators.go`（约 57 处）
  - `server/innodb/engine/storage_adapter.go`（约 12 处）
  - `server/innodb/engine/storage_integrated_dml_executor.go`（约 54 处）
  - `server/innodb/engine/storage_integrated_index_helper.go`（约 17 处）
  - `server/innodb/engine/storage_integrated_dml_helper.go`（约 30 处）
  - `server/innodb/engine/dml_executor.go`（约 67 处）

---

## 二、必须先改（P0）

### P0-03 执行器错误可观测性未闭环

- [ ] `server/innodb/engine/executor.go`
  - [ ] `buildExecutorTree`：`tableManager is nil`、`bufferPoolManager is nil`、`failed to build operator tree` 改为 `ExecutionError` 并带 `schema/table/sql/stage`。
  - [ ] `executeSelectStatement`：`execute SELECT failed` 改为结构化错误。
  - [ ] DDL/DML 入口（`executeInsertStatement`/`executeUpdateStatement`/`executeDeleteStatement`）所有 `execute ... failed` 等文本错误统一化。
  - [ ] `createDatabaseImpl`、`validateDatabaseName`、`createTable`、`drop` 等路径错误返回统一到分类码。
  - [ ] `buildWhereConditions`、`evaluateSetValue` 等“执行参数校验”也统一为结构化错误码，避免上层用字符串判断。

- [ ] `server/innodb/engine/unified_executor.go`
  - [ ] 所有入口错误：SELECT/INSERT/UPDATE/DELETE 的 `failed to ...`、`WHERE`/`ORDER BY`/`LIMIT` 构建错误改为 `ExecutionError`。
  - [ ] `BuildOperatorTree` / `collectSelectResult` / `buildSelectOperatorTree` 的 `nil` 与 `open/fetch` 分支统一化。
  - [ ] 元数据读取 `getQuerySchemaFromTable`、`storageAdapter is not initialized`、`table metadata or schema missing` 使用统一 `ErrorCode`。

- [ ] `server/innodb/engine/storage_adapter.go`
  - [ ] 使用已有 `newStorageAdapterError` 替换：
    - `GetTableMetadata`：表管理器/存储映射/获取失败
    - `ReadPage`：page 读取失败
    - `ParseRecords`：空页面、非法页、解析失败
    - `GetRecordByPrimaryKey`：storageInfo、search、parse、slot 越界
  - [ ] 保留原始错误作为 Cause，便于上层打印堆栈链。

- [ ] `server/innodb/engine/dml_operators.go`（优先级高）
  - [ ] `findDuplicateRecord` 当前返回 `not fully implemented`，必须完成至少最小可用实现。
  - [ ] `InsertOperator`/`UpdateOperator`/`DeleteOperator` 的关键失败：
    - `operator not opened`
    - `failed to begin/commit/rollback transaction`
    - `storage adapter is nil` / `transaction adapter is nil`
    - `scan record`/`insert/update/delete` 失败
  - [ ] `insertRow`、`updateRecord`、`updateOnDuplicate`、`deleteRecord` 等当前仅 stub 的关键函数要补最低可用实现并返回 `ExecutionError`。

- [ ] `server/innodb/engine/index_transaction_adapter.go`
  - [ ] 锁路径仍有文本错误（`invalid resource format`、`unknown lock type`、`failed to acquire lock`）。
  - [ ] `ReleaseLock` 目前仅释放全量锁且有 `TODO`：优先实现最小可控行为并补错误码。

- [ ] `server/innodb/engine/storage_integrated_dml_executor.go`
  - [ ] 这条路径仍大量是文本错误（insert/update/delete 全链路）。  
  - [ ] 关键动作（存储映射、元数据、事务、插入/更新/删除、索引更新、提交回滚）改为结构化错误码映射。

- [ ] `server/innodb/engine/dml_executor.go`
  - [ ] 虽已做了部分校验增强，但很多失败路径仍是 `fmt.Errorf`，按 `P0-03` 口径补齐覆盖。

- [ ] `server/innodb/engine/storage_integrated_dml_helper.go`、`storage_integrated_index_helper.go`
  - [ ] 仍保留大量文本错误，补 `ExecutionError`，并把注释中的 TODO 先标出为待测项。

验收建议：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Storage|Transaction|Index|DDL|DML)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Duplicate|Not.*Found|Metadata|Insert|Update|Delete)' -count=1
```

---

### P0-06 崩溃恢复演练

- [ ] `scripts/crash_recovery_process_drill.sh`
- [ ] `scripts/p0_b_recovery_audit.sh`
- [ ] `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`

缺口：
- redo / undo / 半提交三类场景未形成固定可复现脚本闭环；
- 3 轮以上复放输出报告未形成统一命名与对比脚本。

---

### P0-07 灰度写入阶段阻塞修复

- [ ] `scripts/p0_e_backup_snapshot.sh`
- [ ] `docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- [ ] `server/innodb/manager/`、`server/innodb/engine/`

缺口：
- `table ... not found in storage mapping` 的链路尚未完整修复并产出跨阶段报告。

---

### P0-08 慢查询日志

- [ ] `server/session/`
- [ ] `server/dispatcher/`
- [ ] `server/innodb/engine/`
- [ ] `server/conf/`

缺口：
- `slow_query_log`、慢查询阈值、输出字段未完成；
- SQL/耗时/行数/错误码/事务信息联动字段未统一落盘。

---

### P0-09 指标与告警

- [ ] `server/innodb/manager/checkpoint_monitor.go`
- [ ] `server/innodb/manager/transaction_manager.go`
- [ ] `server/net/`
- [ ] `server/conf/`

缺口：
- 关键指标（QPS、错误率、连接数、活跃事务、P50/P95/P99）未形成统一读数接口；
- redo/undo/锁等待指标和最小告警流未形成回放验证。

---

## 三、次优先级（建议排进 P1）

- [ ] 子查询、窗口函数、HAVING/DISTINCT/LIMIT OFFSET、复杂排序等能力在 `server/innodb/engine` 与 `server/innodb/plan` 尚不完整。
- [ ] 成本估算、统计收集、页面 IO 读写、MVCC 页序列化仍有明显 placeholder 风险。

---

## 四、执行顺序建议

1. 先闭掉 `P0-03`（按文件优先级：`executor -> unified_executor -> storage_adapter -> dml_operators -> index_transaction_adapter`）。
2. 同步补两个失败路径回归测试，验证：
   - `errors.As(err, *ExecutionError)`
   - `ExecutionError.ErrorCode == ...`
3. `P0-06`、`P0-07` 并行推进一阶段证据产出；
4. `P0-08`、`P0-09` 打通最小采集与告警演练后再推进 P1。

