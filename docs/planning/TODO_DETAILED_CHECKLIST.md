# XMySQL Server - TODO详细清单

## 📋 说明

本清单列出了项目中所有259个TODO项，按模块和优先级分类。

> **2026-04**：原 `TODO_SUMMARY.md`（统计型总览）已合并为跳转页；**按文件展开的 TODO 仍以本文为准**。路线图级缺口见 [未实现功能梳理.md](../未实现功能梳理.md)。

**统计日期**: 2025-10-31  
**TODO总数**: 259个  
**分类方式**: 模块 > 文件 > 优先级  

---

## 🔴 P0级别 - 核心功能缺失（必须修复）

### 执行引擎 (volcano_executor.go)

- [ ] **子查询执行器** - 无法执行子查询
- [ ] **窗口函数执行器** - 高级SQL功能缺失
- [ ] **CTE执行器** - 公共表表达式不支持
- [ ] **递归查询执行器** - 递归查询不支持

### B+树管理 (bplus_tree_manager.go)

- [ ] **FindSiblings方法** (行1104) - 删除操作依赖
- [ ] **节点合并逻辑** (行1108) - 删除后重平衡
- [ ] **节点重分配** (行1119) - 删除优化
- [ ] **范围删除** (行1123) - 批量删除支持

### 存储层

- [ ] **页面校验和计算** (page_wrapper_base.go:193) - 数据完整性
- [ ] **页面校验和验证** (system/base.go:136) - 数据完整性
- [ ] **记录版本链管理** (row_cluster_index_leaf_row.go) - MVCC依赖
- [ ] **Undo日志指针** (bplus_tree_manager.go:767, 789) - 事务回滚

---

## 🟡 P1级别 - 重要功能（近期修复）

### 执行引擎

#### volcano_executor.go (20个TODO)

- [ ] 列名查找优化 (行111, 125)
- [ ] Schema赋值修复 (行173, 269, 451, 534, 642, 874)
- [ ] Value到bytes转换 (行311, 312)
- [ ] 索引直接读取 (行329)
- [ ] 回表逻辑实现 (行346)
- [ ] 表达式求值 (行475)
- [ ] StorageAdapter创建 (行1260, 1276)
- [ ] Conditions转换为predicate (行1303)
- [ ] Hash key函数构建 (行1342)
- [ ] 聚合函数构建 (行1381)
- [ ] 排序键构建 (行1404)

#### unified_executor.go (10个TODO)

- [ ] 集成查询优化器 (行74)
- [ ] Records获取 (行106)
- [ ] Columns获取 (行108)
- [ ] WHERE条件过滤 (行167, 216)
- [ ] 物理计划构建 (行257)
- [ ] WHERE条件解析 (行277)
- [ ] SELECT表达式解析 (行284)

#### dml_operators.go (8个TODO)

- [ ] INSERT ... ON DUPLICATE KEY UPDATE
- [ ] REPLACE语句支持
- [ ] LOAD DATA INFILE
- [ ] 批量插入优化
- [ ] 更新/删除WHERE优化

### 查询优化

#### parallel.go (8个TODO)

- [ ] 行数估算 (行291)
- [ ] 分片扫描 (行296)
- [ ] 分区哈希表构建 (行301)
- [ ] 分区探测 (行305)
- [ ] 局部聚合 (行310)
- [ ] 全局聚合 (行314)
- [ ] 分片排序 (行319)
- [ ] 归并排序 (行324)

#### physical_plan.go (6个TODO)

- [ ] 哈希连接决策 (行285)
- [ ] 哈希聚合决策 (行290)
- [ ] 哈希连接代价估算 (行295)
- [ ] 归并连接代价估算 (行300)
- [ ] 哈希聚合代价估算 (行305)
- [ ] 流式聚合代价估算 (行310)

### 管理器

#### schema_manager.go (7个TODO)

- [ ] Schema版本管理
- [ ] Schema变更DDL
- [ ] 在线DDL支持
- [ ] Schema缓存优化
- [ ] 表结构变更通知
- [ ] 索引创建优化
- [ ] 外键约束管理

#### storage_manager.go (4个TODO)

- [ ] 存储空间预分配
- [ ] 碎片整理
- [ ] 空间回收
- [ ] 存储统计信息

#### ibuf_manager.go (4个TODO)

- [ ] Insert Buffer实现
- [ ] Change Buffer支持
- [ ] 合并策略优化
- [ ] 缓冲区大小管理

---

## 🟢 P2级别 - 优化增强（可延后）

### 存储层优化

#### 页面管理 (page_wrapper_base.go)

- [ ] 页面状态管理 (行228, 237)
- [ ] 引用计数 (行243, 248, 253)
- [ ] 统计信息 (行259)
- [ ] 读取逻辑 (行281)
- [ ] 刷新逻辑 (行310)

#### 页面类型实现

**page_allocated_wrapper.go** (5个TODO)
- [ ] 从字节数据解析 (行32)
- [ ] 状态字段 (行83)
- [ ] 脏页标记 (行91)
- [ ] 引用计数 (行95, 99)

**page_inode_wrapper.go** (4个TODO)
- [ ] 从磁盘读取 (行86)
- [ ] 写入buffer pool (行92)
- [ ] Segment接口匹配 (行136)

**ibuf_bitmap_page_wrapper.go** (2个TODO)
- [ ] 从磁盘读取 (行382)
- [ ] 写入磁盘 (行389)

**trx_sys_page_wrapper.go** (2个TODO)
- [ ] 从磁盘读取 (行352)
- [ ] 写入磁盘 (行359)

**data_dictionary_page_wrapper.go** (2个TODO)
- [ ] 从磁盘读取 (行286)
- [ ] 写入磁盘 (行293)

**undo_log_page_wrapper.go** (3个TODO)
- [ ] 从磁盘读取 (行275)
- [ ] 写入磁盘 (行280)
- [ ] 刷新策略 (行285)

**encrypted_page_wrapper.go** (2个TODO)
- [ ] 从磁盘读取 (行273)
- [ ] 写入磁盘 (行280)

**fsp_page_wrapper.go** (2个TODO)
- [ ] 从磁盘读取 (行354)
- [ ] 写入磁盘 (行361)

**xdes_page_wrapper.go** (2个TODO)
- [ ] 从磁盘读取 (行520)
- [ ] 写入磁盘 (行527)

### 记录格式

**row_cluster_index_leaf_row.go** (3个TODO)
- [ ] Store package可用性 (行327)
- [ ] ToByte方法调用 (行366)
- [ ] ClusterSysIndexInternalRow (行502)

**row_cluster_index_internal_row.go** (3个TODO)
- [ ] valueImpl引用修复 (行284, 292, 299)

**未完成的记录类型**:
- [ ] row_secondary_index_leaf_row.go - 完整实现
- [ ] row_cluster_sys_index_leaf_row.go - 完整实现
- [ ] row_secondary_index_internal_row.go - 完整实现
- [ ] row_cluster_sys_index_internal_row.go - 完整实现

### 基础类型 (value.go)

- [ ] 时间值实现 (行300, 304, 331, 343, 351, 355, 454)
- [ ] 类型转换 (行546, 551, 560)

### 系统功能

**system/base.go** (4个TODO)
- [ ] 页面备份 (行121)
- [ ] 页面恢复 (行130)
- [ ] 校验和验证 (行136)
- [ ] 校验和计算 (行142)

**extent/unified_extent.go**
- [ ] 碎片整理逻辑 (行340)

**extent/extent.go**
- [ ] 碎片整理 (行231)

### 其他模块

**dispatcher/system_variable_engine.go** (5个TODO)
- [ ] Schema类型问题修复 (行875, 1012, 1018, 1076, 1081)

**metadata/util.go** (3个TODO)
- [ ] 元数据工具函数

**sqlparser** (4个TODO)
- [ ] SQL解析优化

**buffer_pool** (2个TODO)
- [ ] LRU优化
- [ ] Hashcode安全性验证 (buffer_lru.go:284)

**auth/auth_service.go** (1个TODO)
- [ ] 认证功能放开 (行258)

---

## 📊 统计汇总

### 按优先级

| 优先级 | 数量 | 占比 |
|--------|------|------|
| P0 (核心功能) | 12 | 4.6% |
| P1 (重要功能) | 97 | 37.5% |
| P2 (优化增强) | 150 | 57.9% |
| **总计** | **259** | **100%** |

### 按模块

| 模块 | P0 | P1 | P2 | 总计 |
|------|----|----|----|----|
| 执行引擎 | 4 | 38 | 38 | 80 |
| 存储层 | 4 | 10 | 52 | 66 |
| 管理器 | 4 | 15 | 41 | 60 |
| 查询优化 | 0 | 14 | 7 | 21 |
| 基础类型 | 0 | 2 | 10 | 12 |
| 其他 | 0 | 18 | 2 | 20 |
| **总计** | **12** | **97** | **150** | **259** |

---

## 🎯 修复建议

### 第1周: P0核心功能

1. 页面校验和 (2天)
2. FindSiblings方法 (2天)
3. Undo日志指针 (1天)

### 第2-3周: P1执行引擎

1. Schema赋值修复 (3天)
2. Value转换 (2天)
3. 索引读取和回表 (3天)
4. 表达式求值 (2天)

### 第4-5周: P1查询优化

1. 物理计划构建 (5天)
2. 并行查询框架 (5天)

### 第6-8周: P1管理器

1. Schema管理 (5天)
2. 存储管理 (3天)
3. Insert Buffer (4天)

### 第9-12周: P2优化

1. 页面管理优化 (10天)
2. 记录格式完善 (10天)
3. 系统功能增强 (8天)

---

**预计总工作量**: 约530-1260天（2-5人年）  
**建议团队规模**: 3-5人  
**预计完成时间**: 6-12个月

---

## 附录：TODO 统计快照（原 `TODO_STATISTICS_REPORT.md`）

> **口径**：统计日期 **2025-10-31**，仅 Go 代码；**当前仓库计数可能已变化**，本附录保留的是原报告中的**分布结构**，用于定位热点目录。更新计数请对 `server/` 自行 `rg`。

### 总体（历史快照）

| 指标 | 数值 |
|------|------|
| TODO 总数 | 259（仅 Go） |
| 含「实现」类关键字 | 122 |
| 覆盖文件数 | 约 80 |

### 一级模块

| 模块 | TODO 数量 | 占比 |
|------|-----------|------|
| server/innodb | 253 | 97.7% |
| server/dispatcher | 5 | 1.9% |
| server/auth | 1 | 0.4% |
| **总计** | **259** | **100%** |

### InnoDB 二级模块

| 子模块 | TODO 数量 | 占比 |
|--------|-----------|------|
| engine | 80 | 30.9% |
| storage | 66 | 25.5% |
| manager | 60 | 23.2% |
| plan | 21 | 8.1% |
| basic | 12 | 4.6% |
| metadata | 6 | 2.3% |
| sqlparser | 4 | 1.5% |
| buffer_pool | 2 | 0.8% |
| 其他 | 8 | 3.1% |

### TODO 密度 Top 20 文件（历史快照）

| 排名 | 文件路径 | TODO 数 |
|------|---------|---------|
| 1 | server/innodb/engine/volcano_executor.go | 20 |
| 2 | server/innodb/manager/bplus_tree_manager.go | 12 |
| 3 | server/innodb/engine/unified_executor.go | 10 |
| 4 | server/innodb/engine/index_transaction_adapter.go | 10 |
| 5 | server/innodb/basic/value.go | 10 |
| 6 | server/innodb/storage/wrapper/page/page_wrapper_base.go | 9 |
| 7 | server/innodb/plan/parallel.go | 8 |
| 8 | server/innodb/engine/executor.go | 8 |
| 9 | server/innodb/engine/dml_operators.go | 8 |
| 10 | server/innodb/manager/schema_manager.go | 7 |
| 11 | server/innodb/plan/physical_plan.go | 6 |
| 12 | server/innodb/engine/dml_executor.go | 6 |
| 13 | server/innodb/storage/wrapper/page/page_allocated_wrapper.go | 5 |
| 14 | server/innodb/manager/page.go | 5 |
| 15 | server/innodb/engine/storage_integrated_index_helper.go | 5 |
| 16 | server/dispatcher/system_variable_engine.go | 5 |
| 17 | server/innodb/storage/wrapper/system/base.go | 4 |
| 18 | server/innodb/storage/wrapper/page/page_inode_wrapper.go | 4 |
| 19 | server/innodb/manager/storage_manager.go | 4 |
| 20 | server/innodb/manager/ibuf_manager.go | 4 |

---

**清单生成时间**: 2025-10-31  
**下次更新**: 每周更新进度

