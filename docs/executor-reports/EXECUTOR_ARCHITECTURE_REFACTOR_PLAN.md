# 执行器架构重构计划

> **问题编号**: EXEC-001  
> **问题类型**: 代码重复 / 架构混乱  
> **优先级**: P0（严重）  
> **预计工作量**: 3-5天

> **文档导航（2026-04）**：执行结果与闭环见 [EXECUTOR_REFACTOR_COMPLETION_REPORT.md](./EXECUTOR_REFACTOR_COMPLETION_REPORT.md)；总索引见 [EXECUTOR_DOCUMENTATION_INDEX.md](./EXECUTOR_DOCUMENTATION_INDEX.md)。

## 文档状态（2026-04 重估）

**EXEC-001 已在代码与完成报告中闭环**；下文为 **2025-10 前后的问题分析归档**，其中的行数、文件规模、接口名称可能与**当前** `server/innodb/**/executor` 不一致。当前架构与测试覆盖请以 **完成报告 + 源码** 为准；本文件**不再作为待办计划维护**。

---

## 📋 问题分析

### 当前状态

项目中存在**两套执行器实现**，导致代码重复和维护困难：

#### 1️⃣ executor.go（旧实现 - 1437行）

**职责**:

- SQL执行入口（XMySQLExecutor）
- SQL语句解析和分派
- 基础执行器接口定义（BaseExecutor, Executor）

**问题**:

```go
// ❌ 问题1: 定义了基础执行器接口，但未真正实现火山模型
type BaseExecutor struct {
    schema   *metadata.Table
    children []Executor
}

type Executor interface {
    Schema() *metadata.Table
    Children() []Executor
    SetChildren(children []Executor)
}

// ❌ 问题2: buildExecutorTree返回VolcanoExecutor，但自身未实现算子
func (e *XMySQLExecutor) buildExecutorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (*VolcanoExecutor, error) {
    // 实际调用 volcano_executor.go 的 VolcanoExecutor
    volcanoExec := NewVolcanoExecutor(...)
    return volcanoExec, nil
}

// ❌ 问题3: 大量DML/SELECT执行逻辑分散在executor.go中
func (e *XMySQLExecutor) executeSelectStatement(...)
func (e *XMySQLExecutor) executeInsertStatement(...)
func (e *XMySQLExecutor) executeUpdateStatement(...)
func (e *XMySQLExecutor) executeDeleteStatement(...)
```

#### 2️⃣ volcano_executor.go（新实现 - 1451行）

**职责**:

- 火山模型算子定义（Operator接口）
- 具体算子实现（TableScanOperator, IndexScanOperator, FilterOperator等）
- 流式迭代执行（Open-Next-Close模式）

**实现**:

```go
// ✅ 标准火山模型算子接口
type Operator interface {
    Open(ctx context.Context) error
    Next(ctx context.Context) (Record, error)
    Close() error
    Schema() *metadata.Schema
}

// ✅ 具体算子实现
type TableScanOperator struct { ... }
type IndexScanOperator struct { ... }
type FilterOperator struct { ... }
type ProjectionOperator struct { ... }
type NestedLoopJoinOperator struct { ... }
type HashJoinOperator struct { ... }
type AggregateOperator struct { ... }
type SortOperator struct { ... }

// ✅ VolcanoExecutor作为执行引擎
type VolcanoExecutor struct {
    rootOperator Operator
    // ...
}
```

#### 3️⃣ 其他执行器文件


| 文件                                   | 职责          | 状态                |
| ------------------------------------ | ----------- | ----------------- |
| `select_executor.go`                 | SELECT专用执行器 | 🟡 保留，重构为Operator |
| `dml_executor.go`                    | DML执行器      | 🟡 保留，重构为Operator |
| `storage_integrated_dml_executor.go` | 存储引擎集成DML   | 🟡 保留             |
| `show_executor.go`                   | SHOW语句执行器   | ✅ 保留（特殊处理）        |
| `unified_executor.go`                | 统一执行器       | 🟡 待评估            |


---

## 🎯 重构目标

### 核心原则

```
┌─────────────────────────────────────────────────────┐
│              1. 统一火山模型                        │
│  所有查询执行统一使用Operator算子树 + 迭代器模式   │
└─────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────┐
│         2. 职责分离                                  │
│  - executor.go: SQL解析 + 分派                      │
│  - volcano_executor.go: 算子定义 + 执行引擎         │
│  - xxx_executor.go: 特定算子实现                    │
└─────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────┐
│         3. 消除代码重复                              │
│  删除executor.go中的BaseExecutor、buildExecutorTree │
│  统一使用Operator接口                                │
└─────────────────────────────────────────────────────┘
```

---

## 🔧 重构方案

### 阶段1: 代码清理（1天）

#### 1.1 删除executor.go中的重复定义

**删除内容**:

```go
// ❌ 删除 - 已被volcano_executor.go的Operator替代
type BaseExecutor struct {
    schema   *metadata.Table
    children []Executor
}

// ❌ 删除 - 已被Operator接口替代
type Executor interface {
    Schema() *metadata.Table
    Children() []Executor
    SetChildren(children []Executor)
}
```

**保留内容**:

```go
// ✅ 保留 - SQL执行入口
type XMySQLExecutor struct {
    infosSchemaManager metadata.InfoSchemaManager
    conf               *conf.Cfg
    // ...管理器字段
}

// ✅ 保留 - SQL解析和分派
func (e *XMySQLExecutor) ExecuteWithQuery(...)
func (e *XMySQLExecutor) executeQuery(...)
func (e *XMySQLExecutor) executeDDL(...)
func (e *XMySQLExecutor) executeDBDDL(...)
```

#### 1.2 重构buildExecutorTree方法

**当前（executor.go）**:

```go
func (e *XMySQLExecutor) buildExecutorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (*VolcanoExecutor, error) {
    // ...类型断言
    volcanoExec := NewVolcanoExecutor(...)
    if err := volcanoExec.BuildFromPhysicalPlan(ctx, physicalPlan); err != nil {
        return nil, err
    }
    return volcanoExec, nil
}
```

**重构后（移动到volcano_executor.go）**:

```go
// volcano_executor.go中新增工厂方法
func NewVolcanoExecutorFromPlan(
    ctx context.Context,
    physicalPlan plan.PhysicalPlan,
    tableManager *manager.TableManager,
    bufferPoolManager *manager.OptimizedBufferPoolManager,
    storageManager *manager.StorageManager,
    indexManager *manager.IndexManager,
) (*VolcanoExecutor, error) {
    volcanoExec := NewVolcanoExecutor(
        tableManager,
        bufferPoolManager,
        storageManager,
        indexManager,
    )
    
    if err := volcanoExec.BuildFromPhysicalPlan(ctx, physicalPlan); err != nil {
        return nil, fmt.Errorf("failed to build operator tree: %w", err)
    }
    
    return volcanoExec, nil
}
```

**executor.go中简化调用**:

```go
func (e *XMySQLExecutor) executeSelectStatement(ctx *ExecutionContext, stmt *sqlparser.Select, databaseName string) (*SelectResult, error) {
    // 步骤1: 生成物理计划
    physicalPlan, err := e.generateAndOptimizePlan(stmt, databaseName)
    if err != nil {
        return nil, err
    }
    
    // 步骤2: 构建火山执行器（调用volcano_executor.go）
    volcanoExec, err := NewVolcanoExecutorFromPlan(
        ctx.Context,
        physicalPlan,
        e.tableManager.(*manager.TableManager),
        e.bufferPoolManager.(*manager.OptimizedBufferPoolManager),
        e.storageManager,
        e.indexManager,
    )
    if err != nil {
        return nil, err
    }
    
    // 步骤3: 执行查询
    return volcanoExec.Execute(ctx.Context)
}
```

---

### 阶段2: 架构优化（2-3天）

#### 2.1 统一执行流程

**目标架构**:

```
                     XMySQLExecutor (executor.go)
                            │
                            │ SQL解析
                            ▼
                    ┌───────────────┐
                    │ sqlparser     │
                    └───────┬───────┘
                            │
                ┌───────────┴───────────┐
                │                       │
            SELECT/DML              DDL/SHOW
                │                       │
                ▼                       ▼
         逻辑计划生成           直接执行
                │
                ▼
         物理计划优化
                │
                ▼
    VolcanoExecutor (volcano_executor.go)
                │
                ▼
         算子树构建 + 执行
                │
                ▼
    ┌───────────┴───────────┐
    │                       │
 TableScan            IndexScan
    │                       │
    ▼                       ▼
 Filter               Projection
    │                       │
    └───────────┬───────────┘
                ▼
              Join
                │
                ▼
           Aggregate
                │
                ▼
              Sort
                │
                ▼
             Limit
```

#### 2.2 算子工厂模式

在`volcano_executor.go`中新增算子工厂：

```go
// OperatorFactory 算子工厂，根据物理计划节点创建算子
type OperatorFactory struct {
    storageAdapter *StorageAdapter
    indexAdapter   *IndexAdapter
}

func NewOperatorFactory(
    storageAdapter *StorageAdapter,
    indexAdapter *IndexAdapter,
) *OperatorFactory {
    return &OperatorFactory{
        storageAdapter: storageAdapter,
        indexAdapter:   indexAdapter,
    }
}

// CreateOperator 从物理计划节点创建算子
func (f *OperatorFactory) CreateOperator(ctx context.Context, planNode plan.PhysicalPlan) (Operator, error) {
    switch node := planNode.(type) {
    case *plan.PhysicalTableScan:
        return f.createTableScanOperator(node)
    
    case *plan.PhysicalIndexScan:
        return f.createIndexScanOperator(node)
    
    case *plan.PhysicalFilter:
        childOp, err := f.CreateOperator(ctx, node.Child)
        if err != nil {
            return nil, err
        }
        return NewFilterOperator(childOp, node.Predicate), nil
    
    case *plan.PhysicalProjection:
        childOp, err := f.CreateOperator(ctx, node.Child)
        if err != nil {
            return nil, err
        }
        return NewProjectionOperatorWithExprs(childOp, node.Expressions), nil
    
    case *plan.PhysicalNestedLoopJoin:
        leftOp, err := f.CreateOperator(ctx, node.Left)
        if err != nil {
            return nil, err
        }
        rightOp, err := f.CreateOperator(ctx, node.Right)
        if err != nil {
            return nil, err
        }
        return NewNestedLoopJoinOperator(leftOp, rightOp, node.JoinType, node.Condition), nil
    
    case *plan.PhysicalHashJoin:
        leftOp, err := f.CreateOperator(ctx, node.Left)
        if err != nil {
            return nil, err
        }
        rightOp, err := f.CreateOperator(ctx, node.Right)
        if err != nil {
            return nil, err
        }
        return NewHashJoinOperator(leftOp, rightOp, node.JoinType, node.HashKeys), nil
    
    case *plan.PhysicalAggregate:
        childOp, err := f.CreateOperator(ctx, node.Child)
        if err != nil {
            return nil, err
        }
        return NewAggregateOperator(childOp, node.GroupByExprs, node.AggFuncs), nil
    
    case *plan.PhysicalSort:
        childOp, err := f.CreateOperator(ctx, node.Child)
        if err != nil {
            return nil, err
        }
        return NewSortOperator(childOp, node.OrderByItems), nil
    
    case *plan.PhysicalLimit:
        childOp, err := f.CreateOperator(ctx, node.Child)
        if err != nil {
            return nil, err
        }
        return NewLimitOperator(childOp, node.Offset, node.Count), nil
    
    default:
        return nil, fmt.Errorf("unsupported physical plan node type: %T", planNode)
    }
}
```

---

### 阶段3: 测试验证（1天）

#### 3.1 单元测试

创建 `volcano_executor_refactor_test.go`:

```go
func TestVolcanoExecutorRefactor(t *testing.T) {
    // 测试1: 简单表扫描
    t.Run("SimpleTableScan", func(t *testing.T) {
        // ...
    })
    
    // 测试2: 索引扫描
    t.Run("IndexScan", func(t *testing.T) {
        // ...
    })
    
    // 测试3: 带过滤的查询
    t.Run("TableScanWithFilter", func(t *testing.T) {
        // ...
    })
    
    // 测试4: 连接查询
    t.Run("JoinQuery", func(t *testing.T) {
        // ...
    })
    
    // 测试5: 聚合查询
    t.Run("AggregateQuery", func(t *testing.T) {
        // ...
    })
}
```

#### 3.2 集成测试

使用现有测试：

- `executor_test.go` - 基础执行器测试
- `dml_executor_test.go` - DML操作测试
- `unified_executor_test.go` - 统一执行器测试

---

## 📝 详细修改清单

### 文件: executor.go

**删除（约50行）**:

```diff
- // BaseExecutor 基础执行器，提供公共字段
- type BaseExecutor struct {
-     schema   *metadata.Table
-     children []Executor
- }
- 
- // Executor 执行器接口
- type Executor interface {
-     Schema() *metadata.Table
-     Children() []Executor
-     SetChildren(children []Executor)
- }
```

**修改（约100行）**:

```diff
// buildExecutorTree 从物理计划构建VolcanoExecutor
func (e *XMySQLExecutor) buildExecutorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (*VolcanoExecutor, error) {
-   // 验证管理器实例有效性
-   var tableManager *manager.TableManager
-   var bufferPoolManager *manager.OptimizedBufferPoolManager
-   // ...类型断言代码...
-   
-   volcanoExec := NewVolcanoExecutor(...)
-   if err := volcanoExec.BuildFromPhysicalPlan(ctx, physicalPlan); err != nil {
-       return nil, err
-   }
-   return volcanoExec, nil
+   // 直接调用volcano_executor.go的工厂方法
+   return NewVolcanoExecutorFromPlan(
+       ctx,
+       physicalPlan,
+       e.tableManager.(*manager.TableManager),
+       e.bufferPoolManager.(*manager.OptimizedBufferPoolManager),
+       e.storageManager,
+       e.indexManager,
+   )
}
```

### 文件: volcano_executor.go

**新增（约200行）**:

```go
// NewVolcanoExecutorFromPlan 工厂方法：从物理计划创建VolcanoExecutor
func NewVolcanoExecutorFromPlan(...) (*VolcanoExecutor, error) {
    // ...实现代码...
}

// OperatorFactory 算子工厂
type OperatorFactory struct {
    // ...
}

func (f *OperatorFactory) CreateOperator(...) (Operator, error) {
    // ...实现代码...
}
```

---

## ✅ 预期效果

### 代码质量提升


| 指标            | 重构前   | 重构后   | 改进       |
| ------------- | ----- | ----- | -------- |
| 代码重复          | 2套执行器 | 1套统一  | ✅ 消除重复   |
| executor.go行数 | 1437行 | 1200行 | ⬇️ 减少16% |
| 职责清晰度         | 混乱    | 清晰    | ✅ 提升     |
| 可维护性          | 低     | 高     | ✅ 显著提升   |


### 架构优势

1. **统一火山模型**: 所有查询执行统一使用算子树
2. **职责分离**: executor.go负责解析，volcano_executor.go负责执行
3. **易于扩展**: 新增算子只需实现Operator接口
4. **性能优化空间**: 标准算子树便于优化（如并行执行、向量化）

---

## 🚀 实施步骤

### Day 1: 代码清理

- 删除executor.go中的BaseExecutor和Executor接口
- 重构buildExecutorTree方法
- 修复编译错误

### Day 2-3: 架构优化

- 实现OperatorFactory
- 实现NewVolcanoExecutorFromPlan工厂方法
- 重构executeSelectStatement使用新架构
- 重构executeDML方法

### Day 4: 测试验证

- 运行所有现有测试
- 编写新的单元测试
- 性能回归测试

### Day 5: 文档和清理

- 更新代码注释
- 更新开发文档
- 代码格式化和Lint检查

---

## ⚠️ 风险和注意事项

### 风险1: 现有功能破坏

**缓解措施**: 

- 每次修改后立即运行测试
- 保留原有代码备份
- 增量重构，分步提交

### 风险2: 性能退化

**缓解措施**:

- 运行基准测试对比
- 监控查询执行时间
- 必要时回滚

### 风险3: 其他模块依赖

**缓解措施**:

- 搜索所有对BaseExecutor和Executor的引用
- 逐一修改依赖代码
- 保持接口兼容性

---

**总预计工作量**: 3-5天  
**优先级**: P0（严重） - 影响代码质量和可维护性  
**建议立即开始**: ✅ 是