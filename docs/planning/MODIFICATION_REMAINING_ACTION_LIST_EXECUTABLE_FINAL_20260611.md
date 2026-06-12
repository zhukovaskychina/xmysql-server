# XMySQL Server 修改清单（可执行版，2026-06-11）

> 用途：下一轮直接开工，不再重复清理旧列表。  
> 约束：不写阿里风格黑话；优先处理影响正确性和恢复能力的 P0 项。

## 结论

- 仍有多个关键链路是“能启动但不完整”：`DML`、`SHOW`、存储层行处理、锁与检查点。  
- 下列 P0 项完成前不建议继续扩大新功能。

## P0（先做）

### P0-01 DML 核心算子闭环
**文件：** `server/innodb/engine/dml_operators.go`

**问题点（行号）**
- `findDuplicateRecord`：返回 `findDuplicateRecord not fully implemented`，没有真实唯一性检测（约 `381-390`）
- `insertRow` / `updateRecord`：简化日志返回，未落盘索引和事务（`393-416`）
- `parseInsertRows`：始终返回空列表（`435-440`）
- `UpdateOperator.getTableSchema`：返回空列结构（`624-635`）
- `applySetClause`：直接返回旧记录（`638-655`）
- `checkIndexColumnsChanged`：只比较第一列（`657-677`）
- `updateInPlace` / `deleteOldRecord` / `insertNewRecord`：只打日志（`680-754`）
- `DeleteOperator.deleteRecord`：仅打印日志返回成功（`915-926`）

**验收**
- `INSERT / UPDATE / DELETE` 走真实写入链路，能返回可断言的错误码。
- `UPDATE` 中主键/索引列变更触发“删旧 + 新插”流程。
- `DELETE` 删除动作不影响同页其他记录。

---

### P0-02 Storage 集成 DML 辅助关闭简化
**文件：** `server/innodb/engine/storage_integrated_dml_helper.go`

**问题点（行号）**
- `findRowsToUpdateInStorage` / `findRowsToDeleteInStorage`：无 WHERE 时直接返回空列表（`391-395`, `441-445`）
- `readRowFromStorage`：将整页当一条记录反序列化（`499-503`）
- `markRowAsDeletedInStorage`：清空整页内容表示删除（`524-527`）

**验收**
- 无 WHERE 时有明确执行策略，不是默认空结果。
- 行读取按行位点解析。
- 删除只清理目标位点，不误伤同页其他记录。

---

### P0-03 SHOW 元数据链路
**文件：** `server/innodb/engine/show_executor.go`

**问题点**
- `Schema()` 返回 `nil`（`35`）
- `showDatabases/showTables/showColumns` 返回“未接入真实元数据字典”占位错误（`102-114`）

**验收**
- `SHOW DATABASES / TABLES / COLUMNS` 返回基础真实结果。
- `Schema()` 不再返回 nil。

---

### P0-04 锁释放与检查点边界
**文件：** 
- `server/innodb/engine/index_transaction_adapter.go`
- `server/innodb/engine/storage_integrated_checkpoint.go`

**问题点**
- `ReleaseLock` 仍是“整事务释放”，未按资源释放（`470-497`）
- lockManager 为空时直接返回成功，隐藏了异常（`485-489`）
- `WriteSharpCheckpoint` 缺少写阻塞与恢复逻辑（`485`, `509`）

**验收**
- 支持按资源释放；无权限/资源不存在返回清晰错误。
- sharp checkpoint 的阻塞与恢复行为可复现，失败能回到稳定状态。

---

### P0-05 索引键构建与校验
**文件：** `server/innodb/engine/storage_integrated_index_helper.go`

**问题点**
- `buildIndexKey*` 仅基于第一列，未完整处理多列索引（`25-90`）
- `validateIndexKey` 只做空值判断，缺少类型长度校验（`221-236`）

**验收**
- 支持多列组合键构建、比较与唯一性校验。

## P1（下一阶段）

- `server/innodb/engine/dml_executor.go`：`parseWhereConditions` 仅转字符串（`495-503`）  
- `server/innodb/engine/cte_executor.go`：递归/依赖图为简化假设  
- `server/innodb/engine/subquery_executor_test.go`：join 条件全匹配假设  
- `server/innodb/plan/*`：代价估算、行数估计仍有常量化 fallback  
- `server/innodb/metadata/convert.go`：表结构转换仍未完整实现

## 执行顺序

1. P0-01 → P0-02 → P0-03  
2. P0-04 与 P0-05 并行  
3. 全部通过后再推进 P1  

## 建议验证命令

```bash
go test ./server/innodb/engine -run 'Test.*(DML|Insert|Update|Delete|Show|Index|Checkpoint|Lock)' -count=1
go test ./server/innodb/manager -run 'Test.*(Checkpoint|TableStorage|Index|Recovery|Lock)' -count=1
go test ./server/innodb/plan -run 'Test.*(Subquery|CTE|Window|Cost|Plan|Select)' -count=1
```

每条 P0 建议补一条成功路径与一条失败路径测试，并把结果写入 `reports/p0_fix_batch_20260611`.
