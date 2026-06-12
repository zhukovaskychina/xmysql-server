# XMySQL 修改清单（2026-06-11，当前快照）

## 一、结论（先说要点）

当前仍有多个 P0 风险点未闭环，项目不建议直接定为发布可用：

- 崩溃恢复演练链路仍不稳定（本次 `CR_PROC_ROUNDS=1` 直接失败）。
- `dml_operators.go` 的核心 DML 算子仍大量是简化/占位实现。
- `SHOW` 与锁、索引维护、检查点仍有阻断级缺口。

下面是按优先级排序的“需要改”清单。

## 二、待改 P0（先做）

### P0-01 崩溃恢复演练闭环：先拿到可复现、可判定
- 文件：
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `cmd/recovery_drill_client/main.go`
- 必改项：
  1. 修掉 `wait_client_ready` 失败：`client` 先启动后仍经常 20s 未通过，当前日志出现 `commands out of sync`。
  2. `recovery_drill_client` 在每次连接关闭前确保读写通道完全收敛，`ping`、`setup_redo`、`snapshot`、`verify_*` 不应共享脏状态。
  3. 对关键 marker 做一致标准化。
  4. 审计脚本按“每个场景每轮 PASS”输出并判定，保持 `summary.log`/`client.log` 对齐。
- 复核命令：
  - `CR_PROC_ROUNDS=1 ./scripts/crash_recovery_process_drill.sh`
  - `CR_PROC_REPORT_DIR=./reports ./scripts/p0_b_recovery_audit.sh`

### P0-02 DML 算子主链路：从占位逻辑变为真实路径
- 文件：`server/innodb/engine/dml_operators.go`
- 必改项：
  - `InsertOperator.findDuplicateRecord`（当前直接返回 `not fully implemented`）
  - `parseInsertRows`（当前返回空列表）
  - `insertRow`、`updateRecord`（当前返回空操作）
  - `UpdateOperator.getTableSchema`（当前返回空结构）
  - `applySetClause`（当前直接返回原记录）
  - `updateInPlace`/`deleteOldRecord`/`insertNewRecord`（当前是简化 no-op）
  - `DeleteOperator.deleteRecord`（当前是简化标记）
- 验收：至少有一个 `INSERT/UPDATE/DELETE` 端到端成功和一个主键冲突失败用例，且用 `errors.As(..., *ExecutionError)` 校验。

### P0-03 SHOW 元数据链路
- 文件：`server/innodb/engine/show_executor.go`
- 必改项：
  - `Schema()` 不允许继续返回 `nil`。
  - `SHOW DATABASES/TABLES/COLUMNS` 使用真实元数据返回结果，不再返回“暂未接入真实元数据字典”。
- 验收：`SHOW DATABASES`、`SHOW TABLES FROM`、`SHOW COLUMNS FROM` 能拿到实际数据或返回结构化失败码。

### P0-04 锁管理与事务边界
- 文件：`server/innodb/engine/index_transaction_adapter.go`
- 必改项：
  - `ReleaseLock` 当前只 `ReleaseLocks` 全量清空，需支持资源级释放。
  - `lockManager == nil` 不能静默返回成功。
  - 与 `AcquireLock` 一致写清 `txn_id/scope/resource`。

### P0-05 索引和存储 helper 的安全语义
- 文件：
  - `server/innodb/engine/storage_integrated_index_helper.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
- 必改项：
  - 索引键构建支持多列索引。
  - `check`/`rebuild`/`optimize`/`checkSingleIndexConsistency` 提供最小可执行路径。
  - 无 WHERE 的更新/删除策略明确：拒绝或显式参数要求，不允许静默跳过。
  - 删除不应“清空整页”模拟，必须按记录位点清理并保留同页其他记录。

### P0-06 检查点闭环
- 文件：`server/innodb/engine/storage_integrated_checkpoint.go`
- 必改项：
  - `WriteSharpCheckpoint` 实现写操作阻塞与释放。
  - `collectTableSpaceInfo`、`collectActiveTxns` 至少给出可观测输出，不再空实现。

### P0-07 灰度演练链路
- 文件：`scripts/p0_e_backup_snapshot.sh`、`docs/planning/P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md`
- 必改项：
  - read/write/restore 串行执行并统一上下文日志。
  - 每条失败日志都带 `schema/table/stage/error_code/log_path/duration_ms`。

## 三、建议执行顺序

1. 先做 **P0-01**，确认崩溃恢复可重复 PASS。
2. 并行推进 **P0-02** 与 **P0-03**。
3. 清理 **P0-04**、**P0-05**。
4. 完成 **P0-06**、**P0-07**。

## 四、已完成（当前阶段确认）

- 脚本已具备：`cmd/recovery_drill_client` 的 `snapshot` 与 `ping` 模式
- 演练脚本已具备：`crash_recovery_process_drill.sh` 支持 `CR_PROC_ROUNDS`
- 审计脚本已具备：`p0_b_recovery_audit.sh` 能按目录汇总场景和轮次

这些是“有了结构”，但并非可发布绿灯。
