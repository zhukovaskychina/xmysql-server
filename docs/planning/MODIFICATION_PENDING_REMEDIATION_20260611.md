# XMySQL Server 修改清单（待改项，2026-06-11）

项目路径：`/Users/zhukovasky/GolandProjects/xmysql-server`  
扫描口径：`server/innodb/{engine,manager}`（排除 `*_test.go`）

## 结论（先说结论）
- P0 可用性仍受阻的项集中在 `engine`，主要是 DML、SHOW 和锁处理三类。
- P1/P2 的管理器级 TODO 可按能力分批。

## P0（先做，直接影响正确性）

### P0-01 DML 关键算子仍是简化实现
文件：`server/innodb/engine/dml_operators.go`

- `InsertOperator.findDuplicateRecord` 仍直接返回 `findDuplicateRecord not fully implemented`（行 389）。
- `InsertOperator.parseInsertRows` 直接返回空列表（行 437-440）。
- `InsertOperator.insertRow` / `InsertOperator.updateRecord` 仍是空实现，返回 nil（行 394-403、406-416）。
- `UpdateOperator.getTableSchema / applySetClause / checkIndexColumnsChanged / updateInPlace / deleteOldRecord / insertNewRecord` 为简化/占位实现（行 630-754）。
- `DeleteOperator.deleteRecord` 仅记录日志，没有真正索引/Undo/事务回写（行 915-926）。

**验收动作**
1. 按最小可用路径补全唯一键冲突检测和真实 INSERT/UPDATE/DELETE 的落盘链路（聚簇索引 + 二级索引 + Undo/事务上下文）。
2. 对 `unsupported`/`nil`/失败分支补单测，确保不再静默成功。

---

### P0-02 锁释放边界未闭环（并发一致性风险）
文件：`server/innodb/engine/index_transaction_adapter.go`

- `ReleaseLock` 注释写明“当前只释放事务全部锁”（行 492），无法按单资源回收。
- 锁管理器缺失时 `AcquireLock`/`ReleaseLock` 走“可跳过”路径，错误可被吞掉（行 427-430、486-488）。

**验收动作**
1. 给 `ReleaseLock` 增加按资源/资源类型的释放能力。
2. 对“无 lockManager / 空资源 / 非法事务”返回结构化错误码而不是静默返回。
3. 增加并发测试：重复加锁、顺序解锁、错误解锁场景。

---

### P0-03 SHOW 语句元数据链路未打通
文件：`server/innodb/engine/show_executor.go`

- `showDatabases/showTables/showColumns` 仍返回“暂未接入真实元数据字典，当前返回空结果”（行 104-114）。
- `Schema()` 仍返回 `nil`（行 35）。

**验收动作**
1. SHOW 路径接数据字典（`metadata/info_schema`）返回真实结构。
2. 保留现有过滤/排序能力，补齐元数据不存在时的结构化错误码。

---

### P0-04 索引辅助器仅做“最小路径”，缺失校验与一致性修复
文件：`server/innodb/engine/storage_integrated_index_helper.go`

- 仅按单列构建索引键（行 25-70），缺少多列/类型/长度场景处理。
- `validateIndexKey` 仅做空值检查，TODO 标注待加长度/类型校验（行 233）。
- 重建、优化、单索引一致性检查都未实现，直接返回成功（行 384、400、437）。

**验收动作**
1. 实现多列索引键编码（至少拼接 + 类型兼容约束）。
2. `rebuildIndexForTable`/`optimizeIndexes`/`checkSingleIndexConsistency` 增加最小可运行实现和错误返回。

---

### P0-05 DML 辅助器的无 WHERE/删除路径仍有数据安全风险
文件：`server/innodb/engine/storage_integrated_dml_helper.go`

- 无 `WHERE` 时默认返回空结果（行 391-395、441-444），会造成“静默不更新/不删除”语义。
- 删除路径把整页内容清空（行 524-530），可能扩大误删范围。

**验收动作**
1. 明确无 WHERE 的安全策略：明确报错或需要显式开关，不允许隐式清空行为。
2. 增加条件下界测试：空条件、无效条件、匹配多行、匹配 0 行。

---

## P1（功能完整性，下一阶段）

- `server/innodb/engine/executor.go`
  - `OptimizeLogicalPlan`、`BuildShowPlan` 仍是简化包装。
  - 部分 SHOW/变量类路径仍以固定返回值模拟。
- `server/innodb/engine/storage_integrated_checkpoint.go`：写入阻塞/解锁 TODO（行 485、509）。
- `server/innodb/engine/cte_executor.go`、`window_function_executor.go` 有执行路径 TODO。

## P2（分批处理）

- 管理器层页面与存储路径存在较多 TODO（`page_allocator.go`、`buffer_pool_manager.go`、`page.go`、`index_manager.go` 的完整性/压缩等）。
- 目前建议按“存储路径修复先于优化路径”执行，避免引入更多行为不一致。

## 建议执行顺序

1. 先关掉 P0-01～P0-05（这是当前可交付障碍）。
2. 每项至少配 1~2 个失败路径测试：返回错误码、边界条件、并发条件。
3. 回归命令：

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine \
  -run 'Test.*(Error|Rollback|Duplicate|DML|Transaction|Storage|Index|Show)' -count=1
```

4. P0 全绿后再处理 P1/P2。
