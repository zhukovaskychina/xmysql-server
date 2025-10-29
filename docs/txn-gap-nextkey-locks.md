# Gap锁和Next-Key锁实现文档

## 概述

本文档描述了XMySQL Server中Gap锁（间隙锁）和Next-Key锁的实现，这两种锁机制是InnoDB存储引擎中用于防止幻读的核心技术。

**实现任务**：
- TXN-012：实现Gap锁机制
- TXN-013：实现Next-Key锁机制
- TXN-014：集成Gap锁和Next-Key锁到锁管理器

## 锁类型概述

### 1. Gap锁（间隙锁）

Gap锁用于锁定索引记录之间的间隙，防止其他事务在间隙中插入新记录。

**特性**：
- Gap锁之间互相兼容（S-Gap与S-Gap兼容，S-Gap与X-Gap兼容，X-Gap与X-Gap兼容）
- Gap锁与插入意向锁冲突
- Gap锁不关心记录锁本身

**使用场景**：
- REPEATABLE READ隔离级别下的范围查询
- 防止幻读问题

### 2. Next-Key锁

Next-Key锁是Record Lock（记录锁）和Gap Lock（间隙锁）的组合。

**组成**：
- Record Lock：锁定索引记录本身
- Gap Lock：锁定记录之前的间隙

**特性**：
- 包含Record Lock的所有特性（S与S兼容，S与X不兼容，X与X不兼容）
- 包含Gap Lock的所有特性（阻止插入意向锁）
- 用于REPEATABLE READ隔离级别的默认锁定策略

### 3. 插入意向锁

插入意向锁是一种特殊的Gap锁，表示插入意图。

**特性**：
- 插入意向锁之间互相兼容
- 插入意向锁与Gap锁冲突
- 插入意向锁与Next-Key锁冲突

## 核心数据结构

### 锁粒度枚举

```go
type LockGranularity int

const (
    LOCK_RECORD   LockGranularity = iota // 记录锁(Record Lock)
    LOCK_GAP                             // 间隙锁(Gap Lock)
    LOCK_NEXT_KEY                        // Next-Key锁(Record + Gap)
    LOCK_INSERT_INTENTION                // 插入意向锁
)
```

### Gap范围定义

```go
type GapRange struct {
    LowerBound interface{} // 下界值 (不包含)
    UpperBound interface{} // 上界值 (不包含)
    TableID    uint32      // 表ID
    IndexID    uint32      // 索引ID
}
```

### Gap锁信息

```go
type GapLockInfo struct {
    TxID       uint64    // 事务ID
    LockType   LockType  // 锁类型 (S/X)
    GapRange   *GapRange // 间隙范围
    Granted    bool      // 是否已授予
    WaitChan   chan bool // 等待通道
    CreateTime time.Time // 创建时间
}
```

### Next-Key锁信息

```go
type NextKeyLockInfo struct {
    TxID       uint64      // 事务ID
    LockType   LockType    // 锁类型 (S/X)
    RecordKey  interface{} // 记录键值
    GapRange   *GapRange   // 间隙范围 (记录之前的gap)
    Granted    bool        // 是否已授予
    WaitChan   chan bool   // 等待通道
    CreateTime time.Time   // 创建时间
}
```

## 核心API

### Gap锁操作

#### 获取Gap锁

```go
func (lm *LockManager) AcquireGapLock(
    txID uint64,        // 事务ID
    gapRange *GapRange, // 间隙范围
    lockType LockType,  // 锁类型 (S/X)
) error
```

**功能**：为指定事务获取Gap锁

**返回值**：
- `nil`：成功获取锁
- `error`：获取失败（如死锁检测到）

#### 释放Gap锁

```go
func (lm *LockManager) ReleaseGapLock(
    txID uint64,        // 事务ID
    gapRange *GapRange, // 间隙范围
) error
```

#### 释放所有Gap锁

```go
func (lm *LockManager) ReleaseAllGapLocks(txID uint64)
```

### Next-Key锁操作

#### 获取Next-Key锁

```go
func (lm *LockManager) AcquireNextKeyLock(
    txID uint64,        // 事务ID
    recordKey interface{}, // 记录键值
    gapRange *GapRange, // 间隙范围
    lockType LockType,  // 锁类型 (S/X)
) error
```

#### 释放Next-Key锁

```go
func (lm *LockManager) ReleaseNextKeyLock(
    txID uint64,
    recordKey interface{},
    gapRange *GapRange,
) error
```

#### 释放所有Next-Key锁

```go
func (lm *LockManager) ReleaseAllNextKeyLocks(txID uint64)
```

### 插入意向锁操作

#### 获取插入意向锁

```go
func (lm *LockManager) AcquireInsertIntentionLock(
    txID uint64,        // 事务ID
    insertKey interface{}, // 待插入的键值
    gapRange *GapRange, // 目标间隙
) error
```

## 锁兼容性矩阵

### Record Lock兼容性

|          | S-Record | X-Record |
|----------|----------|----------|
| S-Record | ✓        | ✗        |
| X-Record | ✗        | ✗        |

### Gap Lock兼容性

|          | S-Gap | X-Gap | Insert Intention |
|----------|-------|-------|------------------|
| S-Gap    | ✓     | ✓     | ✗                |
| X-Gap    | ✓     | ✓     | ✗                |
| Insert I | ✗     | ✗     | ✓                |

**注意**：Gap锁之间总是兼容的，无论是S还是X类型。

### Next-Key Lock兼容性

|            | S-Next-Key | X-Next-Key | Insert Intention |
|------------|------------|------------|------------------|
| S-Next-Key | ✓*         | ✗          | ✗                |
| X-Next-Key | ✗          | ✗          | ✗                |
| Insert I   | ✗          | ✗          | ✓                |

*注：仅当锁定相同记录时检查Record Lock部分的兼容性

## 实现细节

### 1. Gap范围检查

使用`gapRangeContains`函数检查键值是否在Gap范围内：

```go
func gapRangeContains(gapRange *GapRange, key interface{}) bool {
    // key应该在 (LowerBound, UpperBound) 范围内
    // 注意：下界和上界都不包含在范围内
    if gapRange.LowerBound != nil && compareKeys(key, gapRange.LowerBound) <= 0 {
        return false
    }
    if gapRange.UpperBound != nil && compareKeys(key, gapRange.UpperBound) >= 0 {
        return false
    }
    return true
}
```

### 2. 键值比较

`compareKeys`函数支持多种数据类型的比较：

```go
func compareKeys(k1, k2 interface{}) int {
    // 返回: -1 (k1 < k2), 0 (k1 == k2), 1 (k1 > k2)
    // 支持类型: int, int64, string, nil
}
```

### 3. 死锁检测

集成到现有的死锁检测机制中：
- 当获取锁需要等待时，更新等待图
- 定期检查等待图是否存在环
- 发现死锁时，中止最老的等待事务

### 4. 等待队列管理

- Gap锁等待插入意向锁释放
- Next-Key锁等待Record Lock和插入意向锁释放
- 插入意向锁等待Gap锁和Next-Key锁释放

## 使用示例

### 示例1：Gap锁防止幻读

```go
// 事务1：扫描范围 (10, 20)
gapRange := &GapRange{
    LowerBound: 10,
    UpperBound: 20,
    TableID:    1,
    IndexID:    1,
}
lm.AcquireGapLock(tx1, gapRange, LOCK_S)

// 事务2：尝试在范围内插入键值15
// 将被阻塞，直到事务1释放Gap锁
lm.AcquireInsertIntentionLock(tx2, 15, gapRange) // 等待中...

// 事务1提交，释放Gap锁
lm.ReleaseGapLock(tx1, gapRange)
// 现在事务2的插入意向锁被授予
```

### 示例2：Next-Key锁的使用

```go
// 事务1：锁定记录20及其之前的Gap (10, 20)
gapRange := &GapRange{
    LowerBound: 10,
    UpperBound: 20,
    TableID:    1,
    IndexID:    1,
}
lm.AcquireNextKeyLock(tx1, 20, gapRange, LOCK_S)

// 此锁会阻止：
// 1. 其他事务修改记录20（Record Lock部分）
// 2. 其他事务在(10, 20)范围内插入（Gap Lock部分）
```

### 示例3：多个Gap锁共存

```go
// 多个事务可以同时持有相同的Gap锁
gapRange := &GapRange{
    LowerBound: 10,
    UpperBound: 20,
    TableID:    1,
    IndexID:    1,
}

lm.AcquireGapLock(tx1, gapRange, LOCK_S) // 成功
lm.AcquireGapLock(tx2, gapRange, LOCK_X) // 也成功（Gap锁之间兼容）
lm.AcquireGapLock(tx3, gapRange, LOCK_S) // 也成功

// 但所有这些Gap锁都会阻止插入意向锁
lm.AcquireInsertIntentionLock(tx4, 15, gapRange) // 等待中...
```

## 文件结构

```
server/innodb/manager/
├── lock_types.go           # 锁类型定义（包含Gap锁和Next-Key锁类型）
├── lock_manager.go         # 锁管理器（扩展支持Gap和Next-Key锁）
├── gap_lock.go             # Gap锁和Next-Key锁核心实现
├── lock_compatibility.go   # 锁兼容性检查
└── gap_lock_test.go        # Gap锁和Next-Key锁测试用例
```

## 测试用例

实现了以下测试用例：

1. **TestGapLockBasic**：Gap锁基本功能测试
2. **TestGapLockAndInsertIntention**：Gap锁与插入意向锁冲突测试
3. **TestNextKeyLockBasic**：Next-Key锁基本功能测试
4. **TestNextKeyLockAndInsertIntention**：Next-Key锁与插入意向锁冲突测试
5. **TestMultipleGapLocks**：多个Gap锁兼容性测试
6. **TestGapLockRangeCheck**：Gap锁范围检查测试
7. **TestLockCompatibilityMatrix**：锁兼容性矩阵验证
8. **TestReleaseAllGapLocks**：释放所有Gap锁测试
9. **TestReleaseAllNextKeyLocks**：释放所有Next-Key锁测试
10. **TestCompareKeys**：键值比较函数测试
11. **TestExplainLockConflict**：锁冲突解释功能测试
12. **BenchmarkGapLockAcquire**：Gap锁获取性能测试
13. **BenchmarkNextKeyLockAcquire**：Next-Key锁获取性能测试

## 性能考虑

### 内存优化

- 使用独立的映射表存储不同类型的锁
- 键值使用`tableID_indexID`格式，减少内存占用
- 及时清理已释放的锁信息

### 并发优化

- 使用读写锁保护锁管理器
- 最小化临界区范围
- 异步授予等待的锁

### 死锁检测优化

- 仅在需要等待时更新等待图
- 限制死锁检测深度
- 定期清理等待图中的无效节点

## 与事务隔离级别的关系

### READ UNCOMMITTED
- 不使用Gap锁和Next-Key锁
- 只使用Record Lock

### READ COMMITTED
- 不使用Gap锁和Next-Key锁
- 只使用Record Lock
- 锁在语句执行完毕后立即释放

### REPEATABLE READ（默认）
- 使用Next-Key锁防止幻读
- 范围查询时锁定范围和间隙
- 锁在事务提交时释放

### SERIALIZABLE
- 所有SELECT语句隐式转换为SELECT ... FOR SHARE
- 使用Next-Key锁实现完全串行化
- 最严格的隔离级别

## 未来改进

1. **优化Gap锁粒度**：支持更细粒度的Gap锁定
2. **智能锁降级**：在某些情况下自动将Next-Key锁降级为Gap锁或Record Lock
3. **锁监控和诊断**：提供更详细的锁等待和死锁信息
4. **自适应锁超时**：根据系统负载动态调整锁超时时间
5. **锁统计信息**：收集锁使用统计，用于性能分析和优化

## 参考资料

1. MySQL官方文档：InnoDB Locking
2. 《高性能MySQL》第7章：锁和事务
3. InnoDB源码：lock/lock0lock.cc

## 版本历史

- v1.0 (2025-10-28)：初始实现
  - 实现Gap锁基本功能
  - 实现Next-Key锁基本功能
  - 实现插入意向锁
  - 集成到锁管理器
  - 添加测试用例

---

文档更新时间：2025-10-28
