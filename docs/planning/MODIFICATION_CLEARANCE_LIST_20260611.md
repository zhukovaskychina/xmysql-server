# 修改清单（2026-06-11）

## 目标
把项目从“可运行的演示”推进到“可执行 SQL 语句（DML/SHOW/事务）且可复现的正确行为”。

生成时间：2026-06-11  
项目路径：`/Users/zhukovasky/GolandProjects/xmysql-server`  
主模块：`server/innodb`

---

## P0（优先先改，功能正确性与数据一致性）

### 1) DML 核心算子仍为简化实现
- 文件：
  - `server/innodb/engine/dml_operators.go`
- 主要问题：
  - `InsertOperator.findDuplicateRecord` 返回 `findDuplicateRecord not fully implemented`
  - `InsertOperator.parseInsertRows` 返回空列表
  - `insertRow` / `updateRecord` / `DeleteOperator.deleteRecord` 只记录日志，不做真实存储变更
  - `UpdateOperator.getTableSchema` 返回空列结构
  - `applySetClause`、`updateInPlace`、`deleteOldRecord`、`insertNewRecord` 为占位逻辑
- 目标结果：
  - INSERT/UPDATE/DELETE 能完成主键生成、唯一性校验、聚簇索引与二级索引更新、事务上下文落盘

### 2) 存储层 UPDATE/DELETE 行筛选与删除逻辑不完整
- 文件：
  - `server/innodb/engine/storage_integrated_dml_helper.go`
- 主要问题：
  - `findRowsToUpdateInStorage`、`findRowsToDeleteInStorage`：无 WHERE 时直接返回空
  - `markRowAsDeletedInStorage` 通过清空整页内容模拟删除
- 目标结果：
  - 支持无 WHERE 的行为（按 SQL 标准返回错误或执行策略）
  - 删除仅标记目标行，不清空整个页面
  - 能正确回传更新/删除影响行数

### 3) SHOW 系列未接入真实元数据字典
- 文件：
  - `server/innodb/engine/show_executor.go`
- 主要问题：
  - `Schema()` 返回 `nil`
  - `showDatabases/showTables/showColumns` 返回“未接入真实元数据字典”
- 目标结果：
  - SHOW 的结果来自真实元数据源（数据库/表/列）
  - 与客户端元数据视图一致

### 4) 索引元数据与锁释放行为不安全
- 文件：
  - `server/innodb/engine/index_transaction_adapter.go`
- 主要问题：
  - `GetIndexMetadata` 在 `indexManager==nil` 时返回默认元数据而非错误
  - `ReleaseLock` 解析资源但实际释放整事务锁；无 lockManager 时静默成功
- 目标结果：
  - 错误信息正确传播，避免“假成功”
  - 支持按资源 ID 释放，失败可重试

### 5) 索引键与一致性工具链不闭环
- 文件：
  - `server/innodb/engine/storage_integrated_index_helper.go`
- 主要问题：
  - `buildIndexKey`/`buildIndexKeyFromOldValues`/`buildIndexKeyFromUpdateExpressions` 假设单列索引
  - `validateIndexKey` 只有空值校验
  - `rebuildIndexForTable` / `optimizeIndexes` / `checkSingleIndexConsistency` 仍是 TODO
- 目标结果：
  - 支持多列索引键构建
  - 校验类型/长度/唯一性边界
  - 实装索引重建与一致性检查

### 6) Sharp Checkpoint 缺少写阻塞机制
- 文件：
  - `server/innodb/engine/storage_integrated_checkpoint.go`
- 主要问题：
  - `WriteSharpCheckpoint` 仅有 TODO 注释，未阻塞/放开写入
- 目标结果：
  - sharp checkpoint 时阻塞写入、刷脏页、写 checkpoint、恢复写入

---

## P1（可并行推进，影响可见性/兼容性）

### 7) SELECT 回退路径仍偏“假数据”
- 文件：
  - `server/innodb/engine/select_executor.go`
  - `server/innodb/engine/dml_executor.go`
- 主要问题：
  - WHERE 条件仅转字符串透传，排序/过滤/返回值存在简化行为
  - 在缺少元数据场景下回退示例数据/空结果
- 目标结果：
  - 对主干查询路径使用统一解析与执行，不走演示式回退

### 8) CTE 与窗口函数不完整
- 文件：
  - `server/innodb/engine/cte_executor.go`
  - `server/innodb/engine/window_function_executor.go`
- 主要问题：
  - 多处 TODO（递归结构校验、依赖图、窗口帧完整实现）
- 目标结果：
  - 至少支持常见标准用法，避免 SQL 执行错误

### 9) 表元数据转换有硬跳过项
- 文件：
  - `server/innodb/metadata/convert.go`
- 主要问题：
  - `ConvertTable` 直接返回 `not implemented`
- 目标结果：
  - 完成 Table -> TableMeta 的转换路径

### 10) 事务/一致性相关管理器中的若干 TODO
- 文件：
  - `server/innodb/manager/index_manager.go`（完整性检查、压缩）
  - `server/innodb/manager/page_allocator.go`（内部分配位图/释放）
  - `server/innodb/manager/extent_manager.go`（内存加载）
- 目标结果：
  - 让索引与空间管理链路不再依赖占位返回

---

## 执行顺序建议

1. P0-1（DML）→ P0-2（DML helper）  
2. P0-3（SHOW）→ P0-4（索引适配器）  
3. P0-5（索引键）→ P0-6（checkpoint）  
4. P1-7（SELECT）→ P1-8（CTE/窗口）→ P1-9（metadata convert）

---

## 验收建议（可直接执行）

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/engine -run 'Test.*(DML|Show|Insert|Update|Delete|Where|Select|Checkpoint|Index)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/manager -run 'Test.*(Index|Storage|Checkpoint|Lock|Recovery)' -count=1
/Users/zhukovasky/sdk/go1.24.3/bin/go test ./server/innodb/metadata -run 'Test.*Convert' -count=1
```

> 说明：优先把 P0 全部过一遍再开 P1。每做完一项建议补一组“成功写入 + 回滚 + 再读”的端到端用例，否则下一个改动会掩盖回归。  

