# 修改清单（执行链路与错误可观测性）

- 生成时间：2026-06-11
- 适用路径：`/Users/zhukovasky/GolandProjects/xmysql-server`
- 目标：列出当前仍未闭环的修改点，按优先级输出为可执行项

## 0. 当前扫描基线（本次核对）

- `server/innodb/engine` 关键链路 `fmt.Errorf` 计数：
  - `executor.go`：90
  - `unified_executor.go`：54
  - `dml_operators.go`：57
  - `storage_adapter.go`：12
  - `storage_integrated_dml_executor.go`：54
  - `storage_integrated_dml_helper.go`：30
  - `storage_integrated_index_helper.go`：17
  - `index_transaction_adapter.go`：23（含 1 条 TODO）
  - `dml_executor.go`：67
- 说明：`ExecutionError` 已有定义（`server/innodb/engine/execution_error.go`），但结构化错误尚未在上述关键执行链路成批落地。

## 1. 仍需改（按优先级）

### P0-03 执行失败可见性（未闭环）

1. `server/innodb/engine/executor.go`
   - 将关键返回路径从字符串错误转为 `ExecutionError`：
     - 存储管理器/事务管理器未初始化（如 1557、1754、1759）
     - DML 分支失败返回（`execute storage-integrated INSERT/UPDATE/DELETE failed` 等）
     - 表/DDL/DATA 创建删除流程失败（元数据、目录、文件、表空间）
   - 与用户态结果拼装处避免把错误文本作为上层判断主依据。

2. `server/innodb/engine/unified_executor.go`
   - 改 `Open/Next` 失败链路：
     - 算子构建失败
     - 表元数据失败
     - open operator 失败
     - ORDER BY/WHERE 解析失败
   - 统一在失败返回里带 `schema/table/stage/error_code`。

3. `server/innodb/engine/dml_operators.go`
   - 核心失败路径仍大量使用 `fmt.Errorf`。
   - 需要优先处理的高影响点：
     - `findDuplicateRecord` 目前返回 `findDuplicateRecord not fully implemented`
     - `operator not opened`、`begin/commit`、`rollback` 失败仍是文本错误
     - `storage adapter is nil`、`storage adapter/table metadata nil` 等关键分支
   - 建议统一改为 `ExecutionError`，并保留原始错误作 `Cause`。

4. `server/innodb/engine/storage_adapter.go`
   - 文件已加 `newStorageAdapterError` helper，但主要返回仍为原生 `fmt.Errorf`。
   - 补齐：
     - `GetTableMetadata`
     - `ReadPage`
     - `ParseRecords`
     - `GetRecordByPrimaryKey`

5. `server/innodb/engine/storage_integrated_dml_executor.go`
   - 事务 begin/commit/rollback、插入/更新/删除、行查找、索引同步、解析、序列化失败仍为文本错误。
   - 建议按分层映射到：
     - `E_METADATA_MISSING`
     - `E_STORAGE_READ_FAIL`
     - `E_STORAGE_WRITE_FAIL`
     - `E_TXN_BEGIN` / `E_TXN_COMMIT` / `E_TXN_ROLLBACK`
     - `E_DUPLICATE_KEY`

6. `server/innodb/engine/storage_integrated_dml_helper.go`
   - 值反序列化、事务适配、页面读取、表达式/SQL 解析相关路径仍为文本错误。
   - 统一迁移到结构化错误。

7. `server/innodb/engine/storage_integrated_index_helper.go`
   - 索引值提取、序列化、插入/删除、唯一键冲突、批量写入仍为文本错误。
   - 同时保留的 TODO（验证/重建/优化）说明该文件仍未闭环。

8. `server/innodb/engine/index_transaction_adapter.go`
   - 已完成大部分事务 begin/commit/rollback 错误码化，但尚有存量文本路径：
     - `RangeScan` 的返回策略
     - 锁管理方法（`AcquireLock`/`ReleaseLock`/`GetLockManager`）
     - TODO：`TODO: 在LockManager中实现单个锁的释放功能`。

9. `server/innodb/engine/dml_executor.go`
   - 虽已补充部分元数据与约束校验，但仍有大量执行失败路径未结构化。
   - 若保留该实现路径，需同步替换，至少覆盖下列动作：
     - 表不存在/表元数据失败
     - 事务 begin/commit
     - 插入/更新/删除失败
     - 二级索引同步失败

### 测试缺口（P0-03 直接关联）

- 当前未看到稳定的 `errors.As(err, &ExecutionError)` 与 `ErrorCode` 断言。
- 建议新增最少 6 条失败路径测试：
  1. 表/库元数据不存在 → `E_METADATA_MISSING`
  2. 页面读取失败 → `E_STORAGE_READ_FAIL`
  3. 存储写入失败 → `E_STORAGE_WRITE_FAIL`
  4. begin 失败 → `E_TXN_BEGIN`
  5. commit 失败 → `E_TXN_COMMIT`
  6. rollback 失败 → `E_TXN_ROLLBACK`

## 2. 其他仍未闭环 P0

- P0-06：崩溃恢复演练（`scripts/crash_recovery_process_drill.sh`、`scripts/p0_b_recovery_audit.sh`）
- P0-07：灰度写入阶段阻塞（`scripts/p0_e_backup_snapshot.sh`）
- P0-08：慢查询日志链路（`server/conf/`、`server/session/`、`server/dispatcher/`）
- P0-09：指标与告警闭环（`server/innodb/manager/`、`server/net/`）

## 3. 建议执行顺序

1. 先把 P0-03 做完（`executor` -> `unified_executor` -> `dml_operators` -> `storage_adapter` -> `storage_integrated_*`）。
2. 再补充 6~8 个失败路径测试。
3. P0-03 通过后同步推进 P0-06 与 P0-07。

## 4. 验收命令（建议）

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|Transaction|Storage|Index|DDL|DML)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine ./server/innodb/manager -run 'Test.*(Recovery|Recovery|Metric|Snapshot|Backup)' -count=1
```
