# MVCC (Multi-Version Concurrency Control) 包

## 概述

这个包实现了InnoDB存储引擎的MVCC（多版本并发控制）功能，提供了事务隔离、版本管理和锁控制等核心功能。

## 主要组件

### 1. RecordVersion (记录版本)
- **文件**: `record_version.go`
- **功能**: 管理记录的多个版本，支持版本链
- **主要方法**:
  - `NewRecordVersion()`: 创建新的记录版本
  - `GetLatestVisibleVersion()`: 获取对指定事务可见的最新版本
  - `FindVersionByTxID()`: 按事务ID查找版本
  - `GetVersionChainLength()`: 获取版本链长度

### 2. LockMode (锁模式)
- **文件**: `lock_mode.go`
- **功能**: 定义和管理各种锁模式
- **锁类型**:
  - `LockModeShared`: 共享锁(S锁)
  - `LockModeExclusive`: 排他锁(X锁)
  - `LockModeIntentionShared`: 意向共享锁(IS锁)
  - `LockModeIntentionExclusive`: 意向排他锁(IX锁)
  - `LockModeUpdate`: 更新锁(U锁)
  - `LockModeSchemaShared/SchemaModification`: 模式锁

### 3. PageSnapshot (页面快照)
- **文件**: `page_snapshot.go`
- **功能**: 管理页面的快照，用于事务隔离
- **主要方法**:
  - `NewPageSnapshot()`: 创建页面快照
  - `AddRecord()`: 添加记录到快照
  - `GetVisibleRecords()`: 获取可见记录
  - `Merge()`: 合并快照

### 4. MVCCIndexPage (MVCC索引页)
- **文件**: `mvcc_page.go`
- **功能**: 实现支持MVCC的索引页面
- **主要功能**:
  - 版本控制
  - 事务管理
  - 锁管理
  - 快照创建和恢复

### 5. 接口定义
- **文件**: `interfaces.go`
- **功能**: 定义MVCC相关的接口
- **主要接口**:
  - `MVCCPage`: MVCC页面接口
  - `RecordVersionManager`: 记录版本管理器
  - `SnapshotManager`: 快照管理器
  - `LockManager`: 锁管理器
  - `TransactionVisibility`: 事务可见性

### 6. 错误定义
- **文件**: `errors.go`
- **功能**: 定义MVCC相关的错误类型

## 核心特性

### 事务隔离级别支持
- 读未提交 (READ_UNCOMMITTED)
- 读已提交 (READ_COMMITTED)
- 可重复读 (REPEATABLE_READ)
- 串行化 (SERIALIZABLE)

### 锁兼容性矩阵
实现了完整的锁兼容性检查，支持：
- 共享锁与共享锁兼容
- 排他锁与任何锁不兼容
- 意向锁的层次化管理

### 版本链管理
- 支持记录的多版本存储
- 版本链遍历和查找
- 垃圾回收支持

### 快照隔离
- 页面级快照
- 记录级可见性判断
- 快照合并和清理

## 使用示例

```go
// 创建记录版本
key := basic.NewStringValue("test_key")
row := basic.NewRow([]byte("test_data"))
record := NewRecordVersion(1, 100, key, row)

// 创建页面快照
snapshot := NewPageSnapshot(1, 0, 1, 100)
snapshot.AddRecord(1, record)

// 创建MVCC页面
page := NewMVCCIndexPage(1, 0)
page.Init()

// 获取锁
err := page.AcquireLock(100, LockModeShared)
if err != nil {
    // 处理锁冲突
}

// 创建读取视图
activeTxIDs := []uint64{90, 95, 105}
readView := NewReadView(100, activeTxIDs)
```

## 测试

运行测试：
```bash
go test -v .
```

测试覆盖了所有主要功能：
- 记录版本管理
- 锁模式兼容性
- 页面快照操作
- MVCC页面功能
- 事务可见性

## 架构集成

这个MVCC包与以下组件集成：
- `basic` 包：基础类型和接口
- `common` 包：页面类型和常量
- `storage/store/pages` 包：底层页面实现

## 注意事项

1. 所有MVCC操作都是线程安全的
2. 版本链需要定期清理以避免内存泄漏
3. 锁获取可能导致死锁，需要超时机制
4. 快照大小会影响内存使用，需要监控

## 未来扩展

- 死锁检测算法
- 自动垃圾回收
- 性能优化
- 更多隔离级别支持 