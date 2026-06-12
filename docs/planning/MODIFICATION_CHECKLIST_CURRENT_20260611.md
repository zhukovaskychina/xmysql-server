# XMySQL Server 修改清单（当前可执行版，2026-06-11）

> 目标：回答“项目还差什么”，给出按优先级能立即执行的修改项，避免分散成一堆重复清单。

## 结论

- 当前可运行性最大风险仍在 **engine 核心执行链**：DML、SHOW、锁释放和无条件清空删除路径。
- `ExecutionError` 已具备类型，但还需要继续接入到关键路径，减少文本错误。
- 下面是按“先修复正确性，再修覆盖率”排序的清单。

## P0（先改，直接影响生产可用性）

### P0-01 DML 算子未闭环（高优先）
文件：
- `server/innodb/engine/dml_operators.go`

待改函数：
- `findDuplicateRecord`（当前返回 `findDuplicateRecord not fully implemented`）
- `parseInsertRows`（当前返回空）
- `insertRow`
- `updateRecord`
- `UpdateOperator.getTableSchema`
- `applySetClause`
- `checkIndexColumnsChanged`
- `updateInPlace`
- `deleteOldRecord`
- `insertNewRecord`
- `DeleteOperator.deleteRecord`

验收要求：
- `INSERT/UPDATE/DELETE` 按聚簇索引和二级索引完整落盘；
- 唯一约束冲突可复现并返回结构化错误码；
- 不再出现“只记录日志不真实落库”的行为。

### P0-02 SHOW 仍未接入真实元数据
文件：
- `server/innodb/engine/show_executor.go`

问题：
- `Schema()` 返回 `nil`
- `SHOW DATABASES/TABLES/COLUMNS` 返回“暂未接入真实元数据字典”文本错误

验收要求：
- 返回真实 metadata 查询结果；
- 元数据缺失时返回 `ExecutionError` 分支（非空字符串）。

### P0-03 锁释放边界错误
文件：
- `server/innodb/engine/index_transaction_adapter.go`

问题：
- `ReleaseLock` 只做“释放事务全部锁”，注释写明缺少单资源释放；
- `lockManager == nil` 分支返回成功（吞掉潜在错误）

验收要求：
- 支持按资源/资源类型释放锁；
- 不可用 lockManager 时返回明确错误码，不再静默。

### P0-04 无 WHERE 与删除语义安全策略不完整
文件：
- `server/innodb/engine/storage_integrated_dml_helper.go`

问题：
- `findRowsToUpdateInStorage` / `findRowsToDeleteInStorage` 无 WHERE 时返回空结果（可能造成语义误解）；
- `markRowAsDeletedInStorage` 通过清空整页模拟删除（高风险）。

验收要求：
- 明确策略：无 WHERE 直接拒绝、或要求显式开关；
- 删除改为按记录位点清理，不破坏同页其他记录。

### P0-05 索引关键路径仍是简化版
文件：
- `server/innodb/engine/storage_integrated_index_helper.go`

问题：
- `buildIndexKey` 等仅按单列处理；
- `validateIndexKey` 仅做空值检查；
- `rebuildIndexForTable`、`optimizeIndexes`、`checkSingleIndexConsistency` 未落地。

验收要求：
- 支持多列索引键构建；
- 补长度/类型基础校验；
- 至少给出重建与一致性检查的可运行路径（哪怕最小实现）。

### P0-06 关键执行错误码覆盖
文件：
- `server/innodb/engine/executor.go`
- `server/innodb/engine/unified_executor.go`

问题：
- 仍存在多处占位实现/简化分支；
- 部分失败分支难以通过 `ExecutionError` 统一归因。

验收要求：
- DML/SHOW/DML 计划构建链路返回结构化错误；
- 失败路径用例明确断言：`errors.As(err, *ExecutionError)` 与 `ErrorCode`。

建议回归：
```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(Error|Rollback|Duplicate|DML|Transaction|Storage|Index|Show)' -count=1
```

## P1（P0 通过后）

### P1-01 查询特性与执行路径不完整
- `server/innodb/engine/select_executor.go`
- `server/innodb/engine/cte_executor.go`
- `server/innodb/engine/window_function_executor.go`

### P1-02 优化器/计划与检查点可观测性增强
- `server/innodb/plan/cost_estimator.go`
- `server/innodb/plan/physical_plan.go`
- `server/innodb/engine/storage_integrated_checkpoint.go`
- `scripts/crash_recovery_drill.sh`（演练脚本输出标准化）

## P2（后续稳定性优化）

- `server/innodb/storage/wrapper/page/*.go` 与 `storage/wrapper/mvcc/*` 的持久化 TODO
- `server/innodb/manager/*` 中的存储分配、空间管理、压测类补齐点

## 推荐执行顺序

1. 先收口 P0-01～P0-06（不按顺序也可并行展开）。
2. 每条 P0 至少补 1~2 个失败路径用例（含并发/边界）。
3. P0 通过后再推进 P1、P2。

