# MVCC Format Layer

## 概述

这是MVCC的**格式层（Format Layer）**，定义了MVCC相关的核心数据结构和序列化方法。

## 设计原则

### 1. 纯数据结构
- ✅ 只包含数据结构定义
- ✅ 无状态，无副作用
- ✅ 可独立测试

### 2. 纯函数
- ✅ 所有方法都是纯函数
- ✅ 无全局状态依赖
- ✅ 可预测的行为

### 3. 无业务逻辑
- ✅ 不包含业务逻辑
- ✅ 不依赖其他层
- ✅ 只提供基础功能

## 核心数据结构

### ReadView
MVCC读视图，用于实现快照隔离。

**文件**: `read_view.go`

**主要字段**:
- `TxID`: 当前事务ID
- `CreateTS`: 创建时间
- `LowWaterMark`: 最小活跃事务ID
- `HighWaterMark`: 下一个要分配的事务ID
- `ActiveTxIDs`: 活跃事务ID列表（排序）
- `ActiveTxMap`: 活跃事务map（快速查找）

**主要方法**:
- `NewReadView()`: 创建新的ReadView
- `IsVisible()`: 判断事务ID是否可见（使用map查找）
- `IsVisibleFast()`: 判断事务ID是否可见（使用二分查找）
- `Clone()`: 克隆ReadView

---

### RecordVersion
记录版本，用于维护记录的版本链。

**文件**: `record_version.go`

**主要字段**:
- `Version`: 版本号
- `TxID`: 创建该版本的事务ID
- `RollPtr`: 回滚指针
- `CreateTS`: 创建时间
- `Key`: 记录键值
- `Value`: 记录数据
- `Deleted`: 删除标记
- `Next`: 下一个版本（旧版本）

**主要方法**:
- `NewRecordVersion()`: 创建新的记录版本
- `IsVisible()`: 检查对指定ReadView是否可见
- `GetLatestVisibleVersion()`: 获取对指定ReadView可见的最新版本
- `FindVersionByTxID()`: 在版本链中查找指定事务的版本
- `GetVersionChainLength()`: 获取版本链长度

---

### VersionChain
版本链管理器，管理单个记录的所有版本。

**文件**: `version_chain.go`

**主要字段**:
- `head`: 链表头（最新版本）
- `length`: 版本链长度
- `minTxID`: 最小事务ID（用于GC）

**主要方法**:
- `NewVersionChain()`: 创建新的版本链
- `InsertVersion()`: 插入新版本
- `FindVisibleVersion()`: 查找对指定ReadView可见的版本
- `GetLatestVersion()`: 获取最新版本
- `PurgeOldVersions()`: 清理旧版本

---

### VersionChainManager
版本链管理器，管理所有记录的版本链。

**文件**: `version_chain.go`

**主要字段**:
- `chains`: key -> VersionChain映射
- `gcChan`: GC触发通道

**主要方法**:
- `NewVersionChainManager()`: 创建版本链管理器
- `GetOrCreateChain()`: 获取或创建版本链
- `PurgeAllChains()`: 清理所有版本链中的旧版本
- `StartGC()`: 启动后台垃圾回收
- `StopGC()`: 停止后台垃圾回收

## 使用示例

### 创建ReadView

```go
import "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"

// 活跃事务ID列表
activeTxIDs := []uint64{100, 102, 105}

// 当前事务ID
txID := uint64(103)

// 下一个要分配的事务ID
nextTxID := uint64(110)

// 创建ReadView
readView := mvcc.NewReadView(activeTxIDs, txID, nextTxID)

// 检查可见性
visible := readView.IsVisible(101) // true（已提交）
visible = readView.IsVisible(102) // false（活跃中）
visible = readView.IsVisible(103) // true（自己的事务）
visible = readView.IsVisible(110) // false（未开始）
```

### 创建RecordVersion

```go
import (
    "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

// 创建记录版本
version := uint64(1)
txID := uint64(100)
rollPtr := uint64(12345)
key := basic.NewValue([]byte("key1"))
value := basic.NewRow([]byte("value1"))

record := mvcc.NewRecordVersion(version, txID, rollPtr, key, value)

// 检查可见性
visible := record.IsVisible(readView)
```

### 使用VersionChain

```go
import "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"

// 创建版本链
chain := mvcc.NewVersionChain()

// 插入版本
chain.InsertVersion(1, 100, 12345, key, value, false)
chain.InsertVersion(2, 101, 12346, key, value2, false)

// 查找可见版本
visibleVersion := chain.FindVisibleVersion(readView)

// 清理旧版本
purged := chain.PurgeOldVersions(minTxID)
```

### 使用VersionChainManager

```go
import (
    "time"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

// 创建版本链管理器
manager := mvcc.NewVersionChainManager()

// 获取或创建版本链
chain := manager.GetOrCreateChain("table1:key1")

// 启动后台GC
manager.StartGC(time.Minute, func() uint64 {
    return getMinActiveTxID()
})

// 停止GC
defer manager.StopGC()
```

## 与其他层的关系

### Format Layer（当前层）
- **职责**: 定义数据结构和序列化
- **特点**: 纯数据，纯函数，无状态

### Wrapper Layer
- **职责**: 业务逻辑和高级抽象
- **依赖**: 依赖Format Layer
- **位置**: `server/innodb/storage/wrapper/mvcc/`

### Manager Layer
- **职责**: 全局管理和协调
- **依赖**: 依赖Wrapper Layer
- **位置**: `server/innodb/manager/`

## 迁移说明

### 从store/mvcc迁移
原来的`server/innodb/storage/store/mvcc`包已被废弃，请使用本包。

**主要变化**:
- `TrxId` → `uint64`
- `DeleteMark` → `Deleted`
- 添加了`Version`字段
- 添加了`Key`字段
- 统一了`Value`类型为`basic.Row`

### 从wrapper/mvcc迁移
原来的`server/innodb/storage/wrapper/mvcc`包中的数据结构已迁移到本包。

**主要变化**:
- 添加了`RollPtr`字段
- 统一了事务ID类型为`uint64`
- 添加了`ActiveTxMap`用于快速查找

## 测试

运行测试：
```bash
go test ./server/innodb/storage/format/mvcc/...
```

## 性能优化

### ReadView可见性判断
- `IsVisible()`: 使用map查找，O(1)时间复杂度
- `IsVisibleFast()`: 使用二分查找，O(log n)时间复杂度

**建议**:
- 活跃事务数 < 100: 使用`IsVisible()`
- 活跃事务数 >= 100: 使用`IsVisibleFast()`

### VersionChain垃圾回收
- 定期调用`PurgeOldVersions()`清理旧版本
- 使用`VersionChainManager.StartGC()`启动后台GC
- 建议GC间隔：1分钟

## 注意事项

1. **线程安全**: VersionChain和VersionChainManager是线程安全的
2. **内存管理**: 定期清理旧版本，避免内存泄漏
3. **性能**: 版本链过长会影响查询性能，建议定期GC
4. **可见性**: 使用ReadView判断可见性，不要直接比较事务ID

## 参考文档

- [MVCC架构分析报告](../../../../../docs/MVCC_ARCHITECTURE_ANALYSIS.md)
- [P3.1架构设计文档](../../../../../docs/P3_1_NEW_ARCHITECTURE_DESIGN.md)
- [编码规范](../../../../../docs/CODING_STANDARDS.md)

