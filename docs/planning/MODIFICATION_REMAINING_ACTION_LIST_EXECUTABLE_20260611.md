# XMySQL Server 修改清单（可执行，2026-06-11）

> 目标：把当前仓库里“还没闭环”的点整理成可执行清单，便于下次直接开工。
> 约束：遵循 `/AGENTS.md` 中的要求（避免阿里风格黑话），优先处理影响正确性的 P0。

## 当前总体结论

1) P0 关键链路里仍有多个“可运行但未闭环”实现（尤其 DML 和存储层）
2) P0-01 到 P0-07 的 80% 仍可直接继续补齐，且有明确落地点和验收方式。

---

## P0（必须先关）

### P0-01 DML 核心算子落地（最先）
- 文件
  - `server/innodb/engine/dml_operators.go`
- 未闭环
  - `InsertOperator.findDuplicateRecord` 返回 `not fully implemented`（`391` 附近）
  - `InsertOperator.insertRow / updateRecord / parseInsertRows` 仍是简化路径（`381-440`）
  - `UpdateOperator.getTableSchema` 直接返回空列（`624-635`）
  - `UpdateOperator.applySetClause / updateInPlace / deleteOldRecord / insertNewRecord` 为“直接返回旧值/无实际操作”
  - `DeleteOperator.deleteRecord` 仅打印日志（`915-926`）
- 验收标准
  - `INSERT` 能走到真实重复键检测、真实插入分支
  - `UPDATE` 能应用 SET，且索引列变更时执行重写路径
  - `DELETE` 对记录的状态更新/版本更新/索引清理可验证
- 建议测试
  - `TestInsertOperator_FindDuplicateRecord`
  - `TestInsertOperator_ApplySetClause`
  - `TestUpdateOperator_UpdateInPlaceAndIndexChange`
  - `TestDeleteOperator_DeleteRecord`

### P0-02 storage-integrated DML 扫描与存储读写不能再简化
- 文件
  - `server/innodb/engine/storage_integrated_dml_executor.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
- 未闭环
  - `findRowsToUpdateInStorage / findRowsToDeleteInStorage` 只在有主键条件时处理
  - `readRowFromStorage` 将整页当成一条记录
  - `markRowAsDeletedInStorage` 用清空页面内容代表删除
- 验收标准
  - 无 WHERE 时要有明确“全表扫描策略”与安全限制，不是直接返回空结果
  - 行读取按槽位解析，不再把整页当单行
  - 删除行为更新可追踪，并且不会误清空整页

### P0-03 SHOW 元数据链路不能返回 `nil` 或空字典错误
- 文件
  - `server/innodb/engine/show_executor.go`
- 未闭环
  - `Schema()` 返回 `nil`
  - `showDatabases/showTables/showColumns` 均返回“未接入真实元数据字典”错误
- 验收标准
  - 能返回数据库/表/列的基础结果，且带 schema/table 元信息

### P0-04 统一错误码与失败上下文
- 文件
  - `server/innodb/engine/executor.go`
  - `server/innodb/engine/storage_adapter.go`
  - `server/innodb/engine/storage_integrated_dml_helper.go`
  - `server/innodb/engine/storage_integrated_dml_executor.go`
- 目标
  - 关键失败路径不应只靠通用文本返回
  - 统一字段：`stage/schema/table/txn_id/error_code/sql/cause`
  - 日志包含错误码，便于告警和问题分层

### P0-05 锁释放和灰度写阻塞
- 文件
  - `server/innodb/engine/index_transaction_adapter.go`（`ReleaseLock`）
  - `server/innodb/engine/storage_integrated_checkpoint.go`
- 未闭环
  - `ReleaseLock` 当前仍使用“释放事务所有锁”的粗粒度实现（注释有 TODO）
  - `WriteSharpCheckpoint` 仅有前后 TODO，缺少写操作阻塞与恢复边界
- 验收标准
  - 支持按 resource 释放单个锁
  - sharp checkpoint 可以稳定阻塞并按阶段恢复写，失败可回滚到一致状态

### P0-06 表存储映射与演练链路
- 文件
  - `server/innodb/manager/table_storage_mapping.go`
  - `scripts/crash_recovery_drill.sh`
  - `scripts/crash_recovery_process_drill.sh`
  - `scripts/p0_e_backup_snapshot.sh`
- 未闭环
  - 新建表后偶发 `table ... not found in storage mapping`
  - 灰度演练缺少固定轮次和固定产物
  - 恢复演练缺少 redo/undo/half-commit 分场景产物对比
- 验收标准
  - 读写路径用同一映射来源，错误带 mapping 快照输出
  - 每轮演练都产出 `before.json / after.json / diff.json / summary.log`

### P0-07 索引维护闭环（P0-06 后）
- 文件
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 未闭环
  - 多列索引构建/一致性检查有 TODO（`rebuild/check/optimize`）
  - `validateIndexKey` 仅检查 nil，缺少类型/长度约束
- 验收标准
  - 至少支持重建/一致性校验的最小路径（不通过则明确失败）

---

## P1（建议下一步）

### P1-01 子查询和关联路径
- 文件
  - `server/innodb/engine/subquery_executor_test.go`
  - `server/innodb/engine/cte_executor.go`
  - `server/innodb/engine/volcano_executor.go`
- 目标：去掉“joinConds 为空全匹配”的简化假设。

### P1-02 窗口和聚合边界行为
- 文件
  - `server/innodb/engine/window_function_executor.go`
  - `server/innodb/engine/select_executor.go`
- 目标：补齐 ROWS/RANGE BETWEEN 与 LIMIT/HAVING/DISTINCT 的边界行为。

### P1-03 成本估算与统计
- 文件
  - `server/innodb/plan/physical_plan.go`
  - `server/innodb/plan/cost_estimator.go`
- 目标：真实估算路径替代常量/静态 fallback。

---

## 一次性执行顺序（建议）

1. P0-01 -> P0-02 -> P0-03
2. P0-04 同步推进（可与上一步并行）
3. P0-05 -> P0-06 -> P0-07
4. 然后执行 P1 清单

---

## 建议执行命令（便于收敛）

```bash
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(DML|Insert|Update|Delete|Duplicate|Error|Storage|Show|Index)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'Test.*(Index|TableStorage|Checkpoint|Lock|Recovery)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/plan -run 'Test.*(Subquery|Window|Cost|Plan|Aggregate|Limit)' -count=1
CR_PROC_REPORT_DIR=./reports/p0_b_audit B_AUDIT_MAX_DIRS=3 ./scripts/p0_b_recovery_audit.sh
./scripts/crash_recovery_process_drill.sh
./scripts/p0_e_backup_snapshot.sh list
```

