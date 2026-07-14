# XMySQL Server 修改清单（当前可执行版，2026-06-11）

> 目标：回答“项目还差什么”，给出按优先级能立即执行的修改项，避免分散成一堆重复清单。

> 2026-07-13 更新：本文是 2026-06-11 的历史执行清单。DML、SHOW、StorageIntegratedDML、BufferPool pinned-page 驱逐、EnhancedBTree rebuild/drop 和 P0 core 验证脚本已完成一轮核心闭环。当前状态以 `docs/planning/P0_CORE_STATUS_20260713.md` 为准。

## 结论

- 2026-06-11 时，最大风险在 **engine 核心执行链**：DML、SHOW、锁释放和无条件清空删除路径。
- 截至 2026-07-13，DML、SHOW、StorageIntegratedDML、checkpoint 写阻塞、BufferPool pinned-page 驱逐、EnhancedBTree 最小 rebuild/drop 已完成 P0 core 闭环。
- 仍需要继续收口的 P0 是：P0-03 残余错误码/锁释放/索引一致性、P0-06 崩溃恢复、P0-07 灰度写入、P0-08 慢查询、P0-09 指标告警。

## P0（先改，直接影响生产可用性）

### P0-01 DML 算子未闭环（高优先，2026-07-13 core 已关闭）
文件：
- `server/innodb/engine/dml_operators.go`

2026-06-11 历史待改函数：
- `findDuplicateRecord`（当时返回 `findDuplicateRecord not fully implemented`）
- `parseInsertRows`（当时返回空）
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

2026-07-13 更新：
- `dml_operators.go` 已通过 `StorageAdapter` 接到底层写接口；
- `storage_adapter.go` 已补 `InsertRecord`、`FindDuplicateRecord`、`UpdateRecord`、`DeleteRecord` 窄写接口；
- 当前 P0 core 验证入口：`scripts/verify_p0_core.sh`。

### P0-02 SHOW 未接入真实元数据（2026-07-13 core 已关闭）
文件：
- `server/innodb/engine/show_executor.go`

2026-06-11 历史问题：
- `Schema()` 返回 `nil`
- `SHOW DATABASES/TABLES/COLUMNS` 当时返回“暂未接入真实元数据字典”文本错误

验收要求：
- 返回真实 metadata 查询结果；
- 元数据缺失时返回 `ExecutionError` 分支（非空字符串）。

2026-07-13 更新：
- `executor.go` 的 SHOW DATABASES / TABLES / COLUMNS 已优先走 `ShowExecutor + InfoSchemaManager`；
- 旧 data-dir 路径保留为 fallback，不再作为主路径判断依据。

### P0-03 锁释放边界错误
文件：
- `server/innodb/engine/index_transaction_adapter.go`

问题：
- `ReleaseLock` 只做“释放事务全部锁”，注释写明缺少单资源释放；
- `lockManager == nil` 分支返回成功（吞掉潜在错误）

验收要求：
- 支持按资源/资源类型释放锁；
- 不可用 lockManager 时返回明确错误码，不再静默。

### P0-04 无 WHERE 与删除语义安全策略不完整（2026-07-13 DML 页内删除 core 已关闭）
文件：
- `server/innodb/engine/storage_integrated_dml_helper.go`

2026-06-11 历史问题：
- `findRowsToUpdateInStorage` / `findRowsToDeleteInStorage` 无 WHERE 时返回空结果（可能造成语义误解）；
- `markRowAsDeletedInStorage` 当时通过清空整页模拟删除（高风险）。

验收要求：
- 明确策略：无 WHERE 直接拒绝、或要求显式开关；
- 删除改为按记录位点清理，不破坏同页其他记录。

2026-07-13 更新：
- `storage_integrated_dml_helper.go` 已补空页、追加、覆盖、按 slot 删除；
- `storage_integrated_dml_executor.go` 已接入 INSERT append、UPDATE replace、DELETE slot 删除；
- 无 WHERE 策略仍应在更高层继续明确，但“清空整页模拟删除”不再代表当前 core 状态。

### P0-05 索引关键路径仍是简化版（2026-07-13 EnhancedBTree 最小 rebuild/drop 已关闭，index helper 仍有残余）
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

2026-07-13 更新：
- `manager/enhanced_btree_manager.go` 已补基于已加载记录的 rebuild 和 drop 释放路径；
- `engine/storage_integrated_index_helper.go` 的复合索引、一致性检查和全量表扫描重建仍属于后续 P0-03/P1 交界项。

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

1. 当前不再按 P0-01～P0-06 原顺序推进；先处理 P0-03 残余错误码/锁释放/索引一致性。
2. 然后推进 P0-06、P0-07 的恢复与灰度验收。
3. 同步补 P0-08、P0-09 的观测链路。
