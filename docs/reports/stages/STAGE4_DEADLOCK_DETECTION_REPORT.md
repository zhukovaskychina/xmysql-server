# Stage 4: 锁与死锁检测器修复（完成报告）

## 概述
在阶段 3 完成后，MVCC 可见性测试全部通过，但 `server/innodb/storage/store/mvcc` 包中的死锁检测相关测试失败：
- TestDeadlockScenarios/TestSimpleDeadlock：正常通过
- TestDeadlockScenarios/TestComplexDeadlock：期望返回 basic.ErrDeadlockDetected，但返回了普通 lock conflict 错误

本阶段聚焦修复死锁检测算法与调用路径，并完善测试隔离，最终使死锁检测测试全部通过，同时回归确认 ReadView 相关测试不受影响。

## 死锁检测算法原理
- 使用等待图（Wait-For Graph）：节点为事务 ID，边 A→B 表示事务 A 正在等待事务 B
- 判断新增等待关系是否导致死锁：
  - 如果在当前等待图中已存在从被等待者 `holder` 到等待者 `waiter` 的路径，则在图中增加 `waiter→holder` 会形成环（即死锁）
  - 检测方式：从 `holder` 出发进行 DFS/BFS，是否可达 `waiter`
- 复杂度：单次检测 O(V+E)，其中 V 为事务数，E 为等待边数；在本项目规模下，性能充足

## 关键实现位置
- 死锁检测器：`server/innodb/storage/store/mvcc/deadlock.go`
  - 结构：`waitForGraph map[uint64]map[uint64]bool`
  - 方法：
    - `AddWaitFor(waiter, holder uint64)` 添加等待边
    - `RemoveWaitFor(waiter, holder uint64)` 移除等待边
    - `WouldCauseCycle(waiter, holder uint64) bool` 检查新增边是否造成环（本次修复）
- 事务管理器（Store 层）：`server/innodb/storage/store/mvcc/isolation.go`
  - 方法：`AcquireLock(txn *Transaction, resourceID string, lockType LockType) error`
  - 本次修复：先判兼容；若不兼容，取 `holderID` 并调用 `WouldCauseCycle(txn.ID, holderID)`，若为真返回 `basic.ErrDeadlockDetected`，否则记录等待边并返回 `basic.ErrLockConflict`

## 根因分析与修复说明
1. 根因
   - 旧实现 `WouldCauseCycle(waiter, resourceID string)` 未使用 `resourceID` 获取持有者，也未按“新增边 waiter→holder 前提”进行环检测，而是仅对 `waiter` 自身做 DFS，导致无法在关键一步（例如三方循环的最后一边）检测到死锁
   - 测试 `TestDeadlockScenarios` 复用同一个 `TransactionManager`，子测试之间状态（锁、等待边、活跃事务）相互影响，复杂场景在整组运行时受到前一子测试残留状态干扰

2. 修复
   - 算法修复：
     - 将 `WouldCauseCycle` 签名改为 `WouldCauseCycle(waiter, holder uint64) bool`
     - 逻辑改为“从 `holder` 出发 DFS 是否可达 `waiter`”（若可达，新增 `waiter→holder` 将形成环）
   - 调用路径修复：
     - 在 `AcquireLock` 中，先判 `isLockCompatible`；若不兼容，计算 `holderID := tm.getLockHolder(resourceID)`，然后执行 `WouldCauseCycle(txn.ID, holderID)` 决策是返回 `ErrDeadlockDetected` 还是添加等待边并返回 `ErrLockConflict`
   - 测试隔离改进：
     - 将 `TestDeadlockScenarios` 的每个子测试使用独立的 `TransactionManager`，避免跨子测试残留状态干扰（仅修改测试，不影响产品代码路径）

3. 兼容性
   - 未更改 MVCC 可见性逻辑（阶段 3 结果保持）
   - 锁管理器接口无破坏性变更（内部调用调整；死锁错误类型与语义符合预期）

## 代码摘要（关键变更）
- `deadlock.go`（新增基于 holder→waiter 可达性判断）
```go
// WouldCauseCycle 检查添加 waiter->holder 是否导致死锁
func (dd *DeadlockDetector) WouldCauseCycle(waiter, holder uint64) bool {
    dd.mu.RLock(); defer dd.mu.RUnlock()
    if holder == 0 { return false }
    visited := make(map[uint64]bool)
    return dd.hasPath(holder, waiter, visited)
}
```
- `isolation.go`（在不兼容时以 holderID 进行死锁判定；区分死锁与普通冲突）
```go
if !tm.isLockCompatible(resourceID, lockType) {
    holderID := tm.getLockHolder(resourceID)
    if tm.deadlockDetector.WouldCauseCycle(txn.ID, holderID) {
        return basic.ErrDeadlockDetected
    }
    tm.deadlockDetector.AddWaitFor(txn.ID, holderID)
    return basic.ErrLockConflict
}
```
- `isolation_test.go`（子测试隔离：每个子测试独立创建 TransactionManager）
```go
// t.Run(...) { tm := NewTransactionManager(RepeatableRead); ... }
```

## 测试验证结果
- 死锁检测：
  - 命令：`go test ./server/innodb/storage/store/mvcc -v -run TestDeadlockScenarios`
  - 结果：全部通过（Simple/Complex/Performance 全通过）
- 回归 ReadView：
  - 命令：`go test ./server/innodb/storage/store/mvcc -v -run TestReadView`
  - 结果：全部通过
- Manager 包：当前工程其他模块存在编译问题（与本阶段无关），因此未运行其测试；本阶段改动未触及 manager 包

## 性能考量
- 每次死锁判定为一次 DFS：O(V+E)，在单测规模（≤100 事务、稀疏等待边）下耗时远低于 2s 门限（性能子测通过）
- 读写锁保护等待图，写入（添加/删除等待边）为互斥，判定为读锁，保证并发安全与开销平衡

## 结论
- 修复了死锁检测算法与调用路径，正确区分了：
  - 普通锁冲突：无环，仅记录等待边，返回 `basic.ErrLockConflict`
  - 真正死锁：形成环，返回 `basic.ErrDeadlockDetected`
- 完成了测试隔离优化，确保子测试互不干扰
- 全部相关测试通过，MVCC 可见性逻辑不受影响

## 后续建议
- 在真实事务管理流程中，检测到死锁后应选定受害者并回滚释放锁（当前单元测试仅断言错误类型；未来可补充 victim 策略与回滚流程）
- 在 ReleaseLock 或事务结束处调用 `DeadlockDetector.RemoveTransaction` 以清理等待边，避免长时间悬挂
- 增加更多随机化/压力测试覆盖复杂等待图形态

