# XMySQL Server 修改清单（最终版草案，2026-06-11）

> 口径：`TODO|FIXME|not implemented|notImplemented|unimplemented|placeholder|暂未实现|暂时返回|暂时|未实现|未完成|简化实现`
>
> 当前扫描范围：`server/innodb/{engine,manager,storage}` + 关键演练脚本。
>
## 先说结论

项目不是“没完成”，而是“核心链路有简化实现未闭环”。当前最关键的是 DML/索引、SHOW 元数据、存储辅助读写、锁与检查点、演练闭环。`rg` 命中 298 个位置，且约半数是测试桩或长期扩展项。

以下清单只列 **可推进的阻塞项**，按优先级排序。

---

## P0（先改）

### P0-01 真实DML算子闭环
**文件：**
- `server/innodb/engine/dml_operators.go`

**问题**
- `findDuplicateRecord` 目前直接返回 `findDuplicateRecord not fully implemented`（`~381-390`）。
- `insertRow` / `updateRecord` 仅日志注释后 `return nil`（`~393-416`）。
- `parseInsertRows` 目前返回空列表（`~435-440`）。
- `UpdateOperator.getTableSchema`、`applySetClause`、`checkIndexColumnsChanged`、`updateInPlace`、`deleteOldRecord`、`insertNewRecord`、`deleteRecord`均为简化实现。

**验收标准**
- INSERT/UPDATE/DELETE 的主路径有真实行级修改、唯一键冲突错误码可追踪。
- 支持 WHERE 条件过滤更新与删除（至少主键条件），非空 WHERE 不直接 `return nil` 空结果。

---

### P0-02 存储辅助层行级读写修正
**文件：**
- `server/innodb/engine/storage_integrated_dml_helper.go`

**问题**
- 无 WHERE 时 `findRowsToUpdateInStorage` / `findRowsToDeleteInStorage` 直接跳过扫描返回空（`~391-445`）。
- `readRowFromStorage` 把“整页”当单行反序列化。
- `markRowAsDeletedInStorage` 直接清空整页内容。

**验收标准**
- 行读取按页内位点读取目标行。
- 删除仅清理目标记录位点，不误伤同页其他记录。
- 无 WHERE 行为明确化：要么全表扫描，要么明确报错，不可静默为空。

---

### P0-03 SHOW 元数据链路
**文件：**
- `server/innodb/engine/show_executor.go`

**问题**
- `Schema()` 返回 `nil`。
- `showDatabases/showTables/showColumns` 目前返回“暂未接入真实元数据字典”的报错占位。

**验收标准**
- `SHOW DATABASES / TABLES / COLUMNS` 返回可用元数据结构。
- `Schema()` 返回正确输出模式，`Next/GetRow` 正常遍历。

---

### P0-04 锁与并发一致性
**文件：**
- `server/innodb/engine/index_transaction_adapter.go`

**问题**
- `ReleaseLock` 仅释放事务全量锁（`ReleaseLocks`），注释说明单资源释放缺失。
- `lockManager == nil` 时直接返回成功，吞掉问题。

**验收标准**
- 按 `resource` 释放单个锁；`lockManager` 缺失时返回明确错误。
- 至少补一条失败/非法resource的单测。

---

### P0-05 检查点边界与写阻塞
**文件：**
- `server/innodb/engine/storage_integrated_checkpoint.go`

**问题**
- `WriteSharpCheckpoint` 仍有 TODO：写阻塞与解阻塞。

**验收标准**
- Sharp checkpoint 期间阻塞新写，完成后恢复。
- 故障路径可回滚到稳定态，不留脏状态。

---

### P0-06 索引功能闭环（最低可用）
**文件：**
- `server/innodb/engine/storage_integrated_index_helper.go`

**问题**
- 仅假设“单列索引”构建；`validateIndexKey` 缺少类型/长度校验。
- `rebuildIndexForTable`、`optimizeIndexes`、`checkSingleIndexConsistency` 为 TODO。

**验收标准**
- 多列索引可至少正确构建与查找。
- 基本一致性检查可运行并能发现典型异常。

---

## P1（可并行）

### P1-01 子查询/CTE/窗口函数
**文件：** `server/innodb/engine/subquery_executor.go`, `server/innodb/engine/cte_executor.go`, `server/innodb/engine/window_function_executor.go`

- 子查询/CTE 依赖有简化与 TODO。
- 窗口帧逻辑有 TODO，当前不完整。

### P1-02 运行期元数据与统计
**文件：** `server/innodb/manager/dictionary_manager.go`, `storage/wrapper/page/*`, `storage/store/*`

- 多处 TODO 仍在元数据、页序列化、mvcc页和系统页包装层。
- 影响长期稳定性与恢复一致性，建议拆成“页层清理包”。

### P1-03 优化与执行计划
**文件：** `server/innodb/engine/executor.go`, `plan/*`

- 优化器里仍有“简化实现”行为；需逐步替换为可回退且有断言的策略。

### P1-04 崩溃恢复演练闭环
**文件：** `scripts/crash_recovery_drill.sh`, `scripts/crash_recovery_process_drill.sh`, `scripts/p0_b_recovery_audit.sh`

- 演练已有骨架，缺少固定轮次（redo/undo/半提交）和一致的证据输出。

---

## 交付次序（建议）
1. P0-01 → P0-02 → P0-03
2. P0-04 与 P0-05 并行
3. P0-06
4. P1-01 → P1-02 → P1-03 → P1-04

---

## 建议验收命令

```bash
rg -n "findDuplicateRecord|not fully implemented|暂未接入真实元数据字典|simplified|TODO: 实现|TODO: 解除" server/innodb/engine/{dml_operators.go,show_executor.go,storage_integrated_dml_helper.go,storage_integrated_index_helper.go,index_transaction_adapter.go,storage_integrated_checkpoint.go}

go test ./server/innodb/engine -run 'Test.*(Insert|Update|Delete|Show|Index|Checkpoint|Lock)' -count=1
go test ./server/innodb/manager -run 'Test.*(Recovery|Checkpoint|Index|Lock|Crash)' -count=1
```

## 说明

- 该清单聚焦“能卡住功能闭环的项”。
- 不是否定现有进展，而是把下一步改动集中到会直接影响正确性和可交付的路径。
