# MVCC 集成测试使用指南

## 快速开始

### 运行所有 MVCC 集成测试

```bash
go test -v ./server/innodb/storage/store/mvcc/
```

### 运行特定测试

```bash
# 只运行 RC 隔离级别测试
go test -v -run TestReadView_ReadCommitted ./server/innodb/storage/store/mvcc/

# 只运行 RR 隔离级别测试
go test -v -run TestReadView_RepeatableRead ./server/innodb/storage/store/mvcc/

# 运行可见性规则测试
go test -v -run TestReadView_VisibilityRules ./server/innodb/storage/store/mvcc/
```

### 运行性能基准测试

```bash
# 运行所有基准测试
go test -bench=BenchmarkReadView -benchmem ./server/innodb/storage/store/mvcc/

# 只运行可见性判断基准测试
go test -bench=BenchmarkReadView_IsVisible -benchmem ./server/innodb/storage/store/mvcc/
```

## 测试列表

### 功能测试

1. **TestReadView_ReadCommitted_Visibility**
   - 验证 RC 隔离级别的语句级快照行为

2. **TestReadView_RepeatableRead_Visibility**
   - 验证 RR 隔离级别的事务级快照行为

3. **TestReadView_RepeatableRead_CanSeeCommittedBeforeStart**
   - 验证 RR 能看到开始前已提交的版本

4. **TestReadView_TransactionCanSeeOwnChanges**
   - 验证事务能看到自己的修改

5. **TestReadView_VisibilityRules**
   - 验证 MVCC 的 4 条核心可见性规则

6. **TestReadView_MultipleTransactions**
   - 验证多事务复杂场景

7. **TestReadView_Clone**
   - 验证 ReadView 克隆功能

### 性能测试

1. **BenchmarkReadView_IsVisible**
   - 可见性判断性能（使用 HashMap）

2. **BenchmarkReadView_IsVisibleFast**
   - 可见性判断性能（使用二分查找）

3. **BenchmarkReadView_NewReadView**
   - ReadView 创建性能

## 测试场景说明

### Read Committed (RC) 场景

```
时间线:
T1: BEGIN → WRITE(V1) → ────────────── → COMMIT
T2:         BEGIN ────→ READ(V1)=❌ → READ(V1)=✅
            (创建 RV1)                (创建 RV2)
```

- T2 在 T1 提交前读取：看不到 V1（T1 未提交）
- T2 在 T1 提交后读取：能看到 V1（创建新 ReadView）

### Repeatable Read (RR) 场景

```
时间线:
T1: BEGIN → WRITE(V1) → ────────────── → COMMIT
T3:         BEGIN ────→ READ(V1)=❌ ──→ READ(V1)=❌
            (创建 RV)                    (使用同一 RV)
```

- T3 在 T1 提交前读取：看不到 V1（T1 未提交）
- T3 在 T1 提交后读取：仍看不到 V1（使用事务开始时的 ReadView）

## 预期输出示例

```
=== RUN   TestReadView_ReadCommitted_Visibility
    integration_test.go:16: Step 1: T1 begins and writes version V1
    integration_test.go:20: Step 2: T2 begins (T1 is still active)
    integration_test.go:31: T2 reads before T1 commits (ReadView 1): V1 visible=false (expected: false)
    integration_test.go:36: Step 3: T1 commits
    integration_test.go:39: Step 4: T2 creates new ReadView (simulating RC statement-level snapshot)
    integration_test.go:49: T2 reads after T1 commits (ReadView 2): V1 visible=true (expected: true)
--- PASS: TestReadView_ReadCommitted_Visibility (0.00s)
```

## 性能基准参考

在 Intel Core i9-9880H @ 2.30GHz 上的测试结果：

```
BenchmarkReadView_IsVisible-16          43030804    31.09 ns/op    0 B/op    0 allocs/op
BenchmarkReadView_IsVisibleFast-16      24607449    61.80 ns/op    0 B/op    0 allocs/op
BenchmarkReadView_NewReadView-16           63018    16631 ns/op    4526 B/op  23 allocs/op
```

## 故障排查

### 测试失败

如果测试失败，检查：
1. ReadView 的可见性规则实现
2. 活跃事务列表的管理
3. minTrxID 和 maxTrxID 的计算

### 性能下降

如果性能显著下降，检查：
1. 活跃事务列表的大小
2. HashMap vs 二分查找的选择
3. 是否有不必要的内存分配

## 相关文档

- [MVCC 集成测试总结](../../../../../docs/MVCC_INTEGRATION_TEST_SUMMARY.md)
- [ReadView 实现](./read_view.go)
- [事务管理器](../../../manager/transaction_manager.go)
