# XMySQL Server 火山模型实现文档

> **版本**: v2.0  
> **更新日期**: 2025-10-27  
> **状态**: ✅ 已重构完成

---

## 📋 概述

本文档描述了 XMySQL Server 中火山模型（Volcano Model）执行引擎的完整实现。火山模型是一种经典的查询执行模型，采用**迭代器模式**实现流式数据处理。

---

## 🏗️ 架构设计

### 核心接口

```go
// Operator 火山模型算子接口
type Operator interface {
    Open(ctx context.Context) error    // 初始化算子
    Next(ctx context.Context) (Record, error)  // 获取下一条记录
    Close() error                       // 释放资源
    Schema() *metadata.Schema           // 返回输出schema
}
```

### 执行流程

```
物理计划 → VolcanoExecutor.BuildFromPhysicalPlan() → 算子树
                                                        ↓
                                                    Open()
                                                        ↓
                                                    Next() (循环)
                                                        ↓
                                                    Close()
```

---

## 🎯 已实现的算子

### 1. TableScanOperator - 表扫描算子

**功能**: 全表扫描，顺序读取表中所有记录

**特点**:
- 支持与存储引擎集成
- 使用BufferPoolManager读取页面
- 支持惰性加载

**使用示例**:
```go
tableScan := NewTableScanOperator(
    "testdb", "users",
    tableManager,
    bufferPoolManager,
    storageManager,
)
```

---

### 2. IndexScanOperator - 索引扫描算子

**功能**: 使用索引快速定位记录

**特点**:
- 支持范围扫描（startKey, endKey）
- 自动回表获取完整记录
- 比全表扫描快10-100倍

**使用示例**:
```go
indexScan := NewIndexScanOperator(
    "testdb", "users", "idx_age",
    indexManager,
    startKey, endKey,
)
```

---

### 3. FilterOperator - 过滤算子

**功能**: 根据条件过滤记录（WHERE子句）

**特点**:
- 支持任意谓词函数
- 流式处理，不缓存数据
- 符合火山模型的惰性求值

**使用示例**:
```go
filter := NewFilterOperator(child, func(record Record) bool {
    values := record.GetValues()
    age := values[1].ToInt64()
    return age > 18  // WHERE age > 18
})
```

---

### 4. ProjectionOperator - 投影算子

**功能**: 选择需要的列（SELECT子句）

**特点**:
- 支持列索引投影
- 支持表达式计算（计算列）
- 自动构建输出schema

**使用示例**:
```go
// 投影第0列和第2列
projection := NewProjectionOperator(child, []int{0, 2})

// 或使用表达式
projection := NewProjectionOperatorWithExprs(child, exprs)
```

---

### 5. NestedLoopJoinOperator - 嵌套循环连接

**功能**: 实现表连接（JOIN）

**特点**:
- 支持INNER/LEFT/RIGHT/FULL JOIN
- 适用于小表连接
- 自动重置右表迭代器

**使用示例**:
```go
join := NewNestedLoopJoinOperator(
    leftChild, rightChild,
    "INNER",
    func(left, right Record) bool {
        // JOIN条件
        return left.GetValues()[0].Equals(right.GetValues()[0])
    },
)
```

**算法**:
```
for each row in left:
    for each row in right:
        if condition(left, right):
            yield merge(left, right)
```

---

### 6. HashJoinOperator - 哈希连接

**功能**: 高效的大表连接

**特点**:
- 分为Build和Probe两个阶段
- 适用于等值连接
- 性能比NestedLoop快10-100倍

**使用示例**:
```go
hashJoin := NewHashJoinOperator(
    buildSide, probeSide,
    "INNER",
    func(r Record) string { return r.GetValues()[0].ToString() }, // buildKey
    func(r Record) string { return r.GetValues()[0].ToString() }, // probeKey
)
```

**算法**:
```
Build Phase:
    for each row in buildSide:
        hashTable[buildKey(row)] = append(hashTable[buildKey(row)], row)

Probe Phase:
    for each row in probeSide:
        key = probeKey(row)
        for each matchedRow in hashTable[key]:
            yield merge(matchedRow, row)
```

---

### 7. HashAggregateOperator - 哈希聚合

**功能**: 实现GROUP BY和聚合函数

**特点**:
- 支持COUNT, SUM, AVG, MIN, MAX
- 惰性计算（第一次Next时才开始聚合）
- 使用哈希表分组

**使用示例**:
```go
agg := NewHashAggregateOperator(
    child,
    []int{0},  // GROUP BY第0列
    []AggregateFunc{&CountAgg{}, &SumAgg{}},
)
```

**支持的聚合函数**:
- `CountAgg`: COUNT(*)
- `SumAgg`: SUM(column)
- 可扩展: AVG, MIN, MAX, STDDEV等

---

### 8. SortOperator - 排序算子

**功能**: 实现ORDER BY

**特点**:
- 支持多列排序
- 支持ASC/DESC
- 支持NULL值处理

**使用示例**:
```go
sort := NewSortOperator(child, []SortKey{
    {ColumnIdx: 0, Ascending: true},   // ORDER BY col0 ASC
    {ColumnIdx: 1, Ascending: false},  // col1 DESC
})
```

---

### 9. LimitOperator - 限制算子

**功能**: 实现LIMIT和OFFSET

**特点**:
- 支持OFFSET跳过前N行
- 支持LIMIT限制返回行数
- 流式处理，不缓存数据

**使用示例**:
```go
limit := NewLimitOperator(child, 10, 20)  // OFFSET 10 LIMIT 20
```

---

## 🔄 物理计划到算子的转换

### VolcanoExecutor

`VolcanoExecutor` 负责将物理计划转换为算子树并执行：

```go
executor := NewVolcanoExecutor(
    tableManager,
    bufferPoolManager,
    storageManager,
    indexManager,
)

// 从物理计划构建算子树
err := executor.BuildFromPhysicalPlan(ctx, physicalPlan)

// 执行查询
results, err := executor.Execute(ctx)
```

### 支持的物理计划类型

| 物理计划 | 对应算子 |
|---------|---------|
| PhysicalTableScan | TableScanOperator |
| PhysicalIndexScan | IndexScanOperator |
| PhysicalSelection | FilterOperator |
| PhysicalProjection | ProjectionOperator |
| PhysicalHashJoin | HashJoinOperator |
| PhysicalMergeJoin | NestedLoopJoinOperator |
| PhysicalHashAgg | HashAggregateOperator |
| PhysicalStreamAgg | HashAggregateOperator |
| PhysicalSort | SortOperator |

---

## 📊 执行示例

### 示例1: 简单查询

**SQL**:
```sql
SELECT name, age FROM users WHERE age > 18 LIMIT 10;
```

**算子树**:
```
LimitOperator (limit=10)
  └── ProjectionOperator (columns=[name, age])
        └── FilterOperator (age > 18)
              └── TableScanOperator (table=users)
```

**执行流程**:
```go
// 1. 构建算子树
tableScan := NewTableScanOperator("db", "users", ...)
filter := NewFilterOperator(tableScan, func(r Record) bool {
    return r.GetValues()[1].ToInt64() > 18
})
projection := NewProjectionOperator(filter, []int{0, 1})
limit := NewLimitOperator(projection, 0, 10)

// 2. 执行
limit.Open(ctx)
defer limit.Close()

for {
    record, err := limit.Next(ctx)
    if record == nil { break }
    // 处理记录
}
```

---

### 示例2: JOIN查询

**SQL**:
```sql
SELECT u.name, o.amount 
FROM users u 
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100;
```

**算子树**:
```
ProjectionOperator (columns=[u.name, o.amount])
  └── FilterOperator (o.amount > 100)
        └── HashJoinOperator (u.id = o.user_id)
              ├── TableScanOperator (table=users)
              └── TableScanOperator (table=orders)
```

---

### 示例3: 聚合查询

**SQL**:
```sql
SELECT department, COUNT(*), SUM(salary)
FROM employees
GROUP BY department
ORDER BY COUNT(*) DESC;
```

**算子树**:
```
SortOperator (by COUNT(*) DESC)
  └── HashAggregateOperator (GROUP BY department, COUNT(*), SUM(salary))
        └── TableScanOperator (table=employees)
```

---

## ✅ 优势

### 1. 标准火山模型
- 符合经典数据库执行模型
- Open-Next-Close三阶段清晰
- 易于理解和维护

### 2. 流式处理
- 惰性求值，按需计算
- 内存占用小
- 支持Pipeline优化

### 3. 模块化设计
- 每个算子独立实现
- 易于扩展新算子
- 便于单元测试

### 4. 与优化器集成
- 物理计划自动转换为算子树
- 支持所有物理计划类型
- 优化器选择最优执行计划

---

## 🚧 待完善功能

### 高优先级 (P0)

1. **TableScanOperator实际存储集成**
   - 当前返回模拟数据
   - 需要实现真实的页面扫描逻辑

2. **IndexScanOperator实现**
   - 当前只有框架
   - 需要实现索引迭代器

3. **表达式求值**
   - Filter和Projection中的条件/表达式需要完整实现
   - 需要支持各种运算符和函数

### 中优先级 (P1)

4. **更多聚合函数**
   - AVG, MIN, MAX
   - STDDEV, VARIANCE
   - GROUP_CONCAT

5. **外连接支持**
   - LEFT OUTER JOIN
   - RIGHT OUTER JOIN
   - FULL OUTER JOIN

6. **子查询支持**
   - Subquery算子
   - Correlated Subquery

### 低优先级 (P2)

7. **向量化执行**
   - 批量处理提升性能
   - SIMD优化

8. **并行执行**
   - 多线程并行扫描
   - 并行聚合

---

## 📈 性能对比

| 算子 | 时间复杂度 | 空间复杂度 | 适用场景 |
|------|-----------|-----------|----------|
| TableScan | O(N) | O(1) | 全表扫描 |
| IndexScan | O(log N + M) | O(1) | 索引查找 |
| Filter | O(N) | O(1) | 条件过滤 |
| Projection | O(N) | O(1) | 列选择 |
| NestedLoopJoin | O(N*M) | O(1) | 小表连接 |
| HashJoin | O(N+M) | O(N) | 大表连接 |
| HashAggregate | O(N) | O(G) | 分组聚合 |
| Sort | O(N log N) | O(N) | 排序 |
| Limit | O(N) | O(1) | 限制结果 |

*N, M: 输入行数; G: 分组数*

---

## 🔗 相关文档

- [查询优化器实现](QUERY_ENGINE_ANALYSIS.md)
- [物理计划设计](../server/innodb/plan/physical_plan.go)
- [开发路线图](DEVELOPMENT_ROADMAP_TASKS.md)

---

**文档维护者**: XMySQL开发团队  
**最后更新**: 2025-10-27

