# XMySQL Server 修改清单（2026-06-11，待改项汇总）

> 生成方式：对 `server/innodb` 核心执行链、事务/锁、DML 辅助、索引、恢复脚本做直接扫描后汇总。  
> 目标：先补齐会影响正确性的项，再推进可用性增强。

## 一、结论（先看这个）

当前最关键是 **engine 核心执行链和元数据/锁/索引一致性** 还不闭环。下面清单是“能直接开始改”的版本。

## 二、P0：必须先改（影响正确性与可上线）

### P0-01 DML 执行算子仍有空实现/假实现

- 文件
  - `server/innodb/engine/dml_operators.go`
- 关键点
  - `InsertOperator.findDuplicateRecord` 返回 `findDuplicateRecord not fully implemented`。
  - `InsertOperator.parseInsertRows` 当前始终返回空行。
  - `InsertOperator.insertRow`、`updateRecord` 仅日志返回，不执行实际落盘。
  - `UpdateOperator.getTableSchema` 返回空列结构，`applySetClause`/`checkIndexColumnsChanged`/`updateInPlace`/`deleteOldRecord`/`insertNewRecord` 为简化行为。
  - `DeleteOperator.deleteRecord` 仅日志返回，未做物理删除与索引清理。
- 风险
  - INSERT/UPDATE/DELETE 在多数场景不生效。
  - Undo/索引更新路径不可验证。
- 说明目标
  - 将这些函数接入真实存储路径：定位记录 → 更新版本/写 Undo → 同步二级索引 → 事务提交。

### P0-02 SHOW 结果仍是元数据桩

- 文件
  - `server/innodb/engine/show_executor.go`
- 关键点
  - `Schema()` 返回 `nil`。
  - `showDatabases/showTables/showColumns` 当前返回“暂未接入真实元数据字典，当前返回空结果”。
- 风险
  - SHOW 系列返回不可信，客户端依赖断链。
- 说明目标
  - 接入 metadata/storage mapping，返回稳定的列定义和记录；无结果时返回空列表不报错。

### P0-03 锁释放边界与错误可见性

- 文件
  - `server/innodb/engine/index_transaction_adapter.go`
- 关键点
  - `ReleaseLock` 说明和实现都按“释放事务全部锁”，注释有 TODO。
  - `lockManager == nil` 分支返回成功，缺少错误反馈。
- 风险
  - 并发下无法按资源粒度释放，问题被静默吞掉。
- 说明目标
  - 支持按资源释放锁；无锁管理器时返回可归因错误码。

### P0-04 无 WHERE 的更新/删除策略不明确

- 文件
  - `server/innodb/engine/storage_integrated_dml_helper.go`
- 关键点
  - `findRowsToUpdateInStorage`、`findRowsToDeleteInStorage` 在无 WHERE 时直接返回空。
- 风险
  - 与 MySQL 行为不一致，运维脚本和批量修复逻辑可能静默跳过。
- 说明目标
  - 明确策略：默认拒绝无 WHERE（返回错误码），或通过明确配置才允许全表扫描。

### P0-05 删除路径改为清页是高风险

- 文件
  - `server/innodb/engine/storage_integrated_dml_helper.go`
- 关键点
  - `markRowAsDeletedInStorage` 通过 `SetContent(空)` 表示删除。
- 风险
  - 容易误伤同页记录，影响数据完整性。
- 说明目标
  - 使用记录级别删除标记和页重构策略，保留可恢复语义。

### P0-06 索引键构建与校验不完整

- 文件
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 关键点
  - `buildIndexKey` / `buildIndexKeyFromOldValues` / `buildIndexKeyFromUpdateExpressions` 仍按单列处理。
  - `validateIndexKey` 仅做 NULL 检查。
- 风险
  - 多列索引与类型边界校验缺失，唯一键误判。
- 说明目标
  - 支持多列键构建，补齐类型和长度校验，加入冲突边界测试。

### P0-07 索引重建、优化、一致性检查尚未落地

- 文件
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 关键点
  - `rebuildIndexForTable`、`optimizeIndexes`、`checkSingleIndexConsistency` 仍是 TODO。
- 风险
  - 索引退化或损坏无自检路径。
- 说明目标
  - 至少提供最小实现：扫描表、重建索引、基本一致性校验、返回偏差报告。

### P0-08 Checkpoint 写入缺少阻塞控制

- 文件
  - `server/innodb/engine/storage_integrated_checkpoint.go`
- 关键点
  - `WriteSharpCheckpoint` 的阻塞写/解阻塞 TODO 未完成。
- 风险
  - 全量检查点期间写入与状态并发不一致。
- 说明目标
  - 引入明确状态锁或写流控窗口，保证检查点时的事务边界一致。

### P0-09 元数据回退路径会掩盖问题

- 文件
  - `server/innodb/engine/index_transaction_adapter.go`
- 关键点
  - `GetIndexMetadata` 在索引管理器缺失时返回默认元数据而非错误。
- 风险
  - 索引找错表/字段时被“假成功”掩盖。
- 说明目标
  - 能查到返回真实元数据；不能查到返回可归因错误码。

### P0-10 崩溃恢复演练闭环不足

- 文件
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_b_recovery_audit.sh`
  - `docs/planning/P0_B_RECOVERY_EVIDENCE_MANIFEST.md`
- 关键点
  - redo/undo/半提交与 3 轮复放报告还不稳定。
- 风险
  - 发布窗口缺失可复放恢复证据。
- 说明目标
  - 固定三类场景、固定循环次数、固定报告格式和差异比对。

## 三、P1：建议后续

- `server/innodb/engine/cte_executor.go`、`window_function_executor.go`、`select_executor.go`（子查询、窗口、HAVING/DISTINCT/LIMIT）
- `server/innodb/plan/*`（代价估算、选择率、物理计划）
- `server/innodb/storage/wrapper/page/*.go`、`storage/wrapper/mvcc/mvcc_page.go`（序列化/反序列化）
- `server/net/`、`server/protocol/`（协议信令增强）

## 四、建议执行顺序（优先）

1. P0-01 → P0-02 → P0-03 → P0-04 → P0-05  
2. P0-06 → P0-07 → P0-08 → P0-09  
3. P0-10（并行补演练与报告脚本）

## 五、验收命令（建议）

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(DML|Insert|Update|Delete|Show|Show.*|Duplicate|Transaction|Index|Checkpoint|Error|Rollback|Recovery)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Index|Rebuild|Consistency|DML)' -count=1
CR_PROC_REPORT_DIR=./reports B_AUDIT_MAX_DIRS=5 ./scripts/p0_b_recovery_audit.sh
```
