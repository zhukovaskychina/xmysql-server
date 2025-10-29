package manager

import (
	"testing"
	"time"
)

// ============ TXN-014: Gap锁和Next-Key锁测试 ============

// TestGapLockBasic 测试Gap锁基本功能
func TestGapLockBasic(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// 定义一个Gap范围: (10, 20)
	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	// 事务1获取Gap锁
	err := lm.AcquireGapLock(1, gapRange, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Gap lock: %v", err)
	}

	// 事务2也获取相同的Gap锁（应该成功，因为Gap锁之间兼容）
	err = lm.AcquireGapLock(2, gapRange, LOCK_X)
	if err != nil {
		t.Fatalf("Failed to acquire second Gap lock: %v", err)
	}

	// 释放Gap锁
	err = lm.ReleaseGapLock(1, gapRange)
	if err != nil {
		t.Fatalf("Failed to release Gap lock: %v", err)
	}

	err = lm.ReleaseGapLock(2, gapRange)
	if err != nil {
		t.Fatalf("Failed to release second Gap lock: %v", err)
	}

	t.Log("Gap lock basic test passed")
}

// TestGapLockAndInsertIntention 测试Gap锁与插入意向锁的冲突
func TestGapLockAndInsertIntention(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// 定义一个Gap范围: (10, 20)
	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	// 事务1获取Gap锁
	err := lm.AcquireGapLock(1, gapRange, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Gap lock: %v", err)
	}

	// 事务2尝试获取插入意向锁，插入键值15在Gap范围内
	// 应该等待，因为Gap锁与插入意向锁冲突
	done := make(chan error, 1)
	go func() {
		err := lm.AcquireInsertIntentionLock(2, 15, gapRange)
		done <- err
	}()

	// 等待一小段时间，确保事务2进入等待状态
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Insert intention lock should be blocked by Gap lock")
		}
	case <-time.After(100 * time.Millisecond):
		// 预期行为：事务2应该等待
		t.Log("Insert intention lock is correctly blocked by Gap lock")
	}

	// 释放Gap锁
	err = lm.ReleaseGapLock(1, gapRange)
	if err != nil {
		t.Fatalf("Failed to release Gap lock: %v", err)
	}

	// 现在插入意向锁应该被授予
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Insert intention lock should be granted after Gap lock release: %v", err)
		}
		t.Log("Insert intention lock granted after Gap lock release")
	case <-time.After(1 * time.Second):
		t.Fatal("Insert intention lock not granted in time")
	}
}

// TestNextKeyLockBasic 测试Next-Key锁基本功能
func TestNextKeyLockBasic(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// 定义Next-Key锁: 记录20 + Gap(10, 20)
	recordKey := 20
	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	// 事务1获取Next-Key锁
	err := lm.AcquireNextKeyLock(1, recordKey, gapRange, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Next-Key lock: %v", err)
	}

	// 事务2尝试获取相同记录的排他Next-Key锁（应该等待或失败）
	err = lm.AcquireNextKeyLock(2, recordKey, gapRange, LOCK_X)
	if err != nil {
		// 预期行为：可能因为兼容性检查而失败
		t.Logf("Expected conflict with existing Next-Key lock: %v", err)
	}

	// 释放Next-Key锁
	err = lm.ReleaseNextKeyLock(1, recordKey, gapRange)
	if err != nil {
		t.Fatalf("Failed to release Next-Key lock: %v", err)
	}

	t.Log("Next-Key lock basic test passed")
}

// TestNextKeyLockAndInsertIntention 测试Next-Key锁与插入意向锁的冲突
func TestNextKeyLockAndInsertIntention(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// 定义Next-Key锁: 记录20 + Gap(10, 20)
	recordKey := 20
	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	// 事务1获取Next-Key锁
	err := lm.AcquireNextKeyLock(1, recordKey, gapRange, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Next-Key lock: %v", err)
	}

	// 事务2尝试在Gap范围内插入（插入键值15）
	done := make(chan error, 1)
	go func() {
		err := lm.AcquireInsertIntentionLock(2, 15, gapRange)
		done <- err
	}()

	// 等待一小段时间，确保事务2进入等待状态
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Insert intention lock should be blocked by Next-Key lock")
		}
	case <-time.After(100 * time.Millisecond):
		// 预期行为：事务2应该等待
		t.Log("Insert intention lock is correctly blocked by Next-Key lock")
	}

	// 释放Next-Key锁
	err = lm.ReleaseNextKeyLock(1, recordKey, gapRange)
	if err != nil {
		t.Fatalf("Failed to release Next-Key lock: %v", err)
	}

	// 现在插入意向锁应该被授予
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Insert intention lock should be granted after Next-Key lock release: %v", err)
		}
		t.Log("Insert intention lock granted after Next-Key lock release")
	case <-time.After(1 * time.Second):
		t.Fatal("Insert intention lock not granted in time")
	}
}

// TestMultipleGapLocks 测试多个Gap锁的兼容性
func TestMultipleGapLocks(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	// 多个事务同时获取Gap锁
	for i := 1; i <= 5; i++ {
		err := lm.AcquireGapLock(uint64(i), gapRange, LOCK_S)
		if err != nil {
			t.Fatalf("Transaction %d failed to acquire Gap lock: %v", i, err)
		}
	}

	// 所有Gap锁应该都成功获取
	t.Log("Multiple Gap locks acquired successfully")

	// 释放所有Gap锁
	for i := 1; i <= 5; i++ {
		err := lm.ReleaseGapLock(uint64(i), gapRange)
		if err != nil {
			t.Fatalf("Transaction %d failed to release Gap lock: %v", i, err)
		}
	}

	t.Log("Multiple Gap locks test passed")
}

// TestGapLockRangeCheck 测试Gap锁范围检查
func TestGapLockRangeCheck(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	// 事务1获取Gap锁
	err := lm.AcquireGapLock(1, gapRange, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Gap lock: %v", err)
	}

	// 测试gapRangeContains函数
	testCases := []struct {
		key      interface{}
		expected bool
	}{
		{5, false},  // 在下界之前
		{10, false}, // 等于下界（不包含）
		{15, true},  // 在范围内
		{20, false}, // 等于上界（不包含）
		{25, false}, // 在上界之后
	}

	for _, tc := range testCases {
		result := gapRangeContains(gapRange, tc.key)
		if result != tc.expected {
			t.Errorf("gapRangeContains(%v) = %v, expected %v", tc.key, result, tc.expected)
		}
	}

	t.Log("Gap lock range check test passed")
}

// TestLockCompatibilityMatrix 测试锁兼容性矩阵
func TestLockCompatibilityMatrix(t *testing.T) {
	matrix := GetLockCompatibilityMatrix()

	// 验证一些关键兼容性规则
	testCases := []struct {
		lock1    string
		lock2    string
		expected bool
	}{
		{"RECORD_S", "RECORD_S", true},                 // S-S兼容
		{"RECORD_S", "RECORD_X", false},                // S-X不兼容
		{"RECORD_X", "RECORD_X", false},                // X-X不兼容
		{"GAP_S", "GAP_S", true},                       // Gap锁之间兼容
		{"GAP_S", "GAP_X", true},                       // Gap锁之间兼容
		{"GAP_X", "GAP_X", true},                       // Gap锁之间兼容
		{"GAP_S", "INSERT_INTENTION", false},           // Gap与插入意向锁不兼容
		{"INSERT_INTENTION", "INSERT_INTENTION", true}, // 插入意向锁之间兼容
	}

	for _, tc := range testCases {
		result := matrix[tc.lock1][tc.lock2]
		if result != tc.expected {
			t.Errorf("Compatibility[%s][%s] = %v, expected %v",
				tc.lock1, tc.lock2, result, tc.expected)
		}
	}

	t.Log("Lock compatibility matrix test passed")
}

// TestReleaseAllGapLocks 测试释放所有Gap锁
func TestReleaseAllGapLocks(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// 事务1获取多个Gap锁
	gapRange1 := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}
	gapRange2 := &GapRange{
		LowerBound: 30,
		UpperBound: 40,
		TableID:    1,
		IndexID:    1,
	}

	err := lm.AcquireGapLock(1, gapRange1, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Gap lock 1: %v", err)
	}

	err = lm.AcquireGapLock(1, gapRange2, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Gap lock 2: %v", err)
	}

	// 释放所有Gap锁
	lm.ReleaseAllGapLocks(1)

	// 验证Gap锁已被释放
	lm.mu.RLock()
	if len(lm.txnGapLocks[1]) != 0 {
		t.Fatalf("Transaction 1 should have no Gap locks after release")
	}
	lm.mu.RUnlock()

	t.Log("Release all Gap locks test passed")
}

// TestReleaseAllNextKeyLocks 测试释放所有Next-Key锁
func TestReleaseAllNextKeyLocks(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	// 事务1获取多个Next-Key锁
	gapRange1 := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}
	gapRange2 := &GapRange{
		LowerBound: 30,
		UpperBound: 40,
		TableID:    1,
		IndexID:    1,
	}

	err := lm.AcquireNextKeyLock(1, 20, gapRange1, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Next-Key lock 1: %v", err)
	}

	err = lm.AcquireNextKeyLock(1, 40, gapRange2, LOCK_S)
	if err != nil {
		t.Fatalf("Failed to acquire Next-Key lock 2: %v", err)
	}

	// 释放所有Next-Key锁
	lm.ReleaseAllNextKeyLocks(1)

	// 验证Next-Key锁已被释放
	lm.mu.RLock()
	if len(lm.txnNextKeyLocks[1]) != 0 {
		t.Fatalf("Transaction 1 should have no Next-Key locks after release")
	}
	lm.mu.RUnlock()

	t.Log("Release all Next-Key locks test passed")
}

// TestCompareKeys 测试键值比较函数
func TestCompareKeys(t *testing.T) {
	testCases := []struct {
		k1       interface{}
		k2       interface{}
		expected int
	}{
		{10, 20, -1},               // int: 10 < 20
		{20, 10, 1},                // int: 20 > 10
		{15, 15, 0},                // int: 15 == 15
		{int64(10), int64(20), -1}, // int64: 10 < 20
		{"abc", "def", -1},         // string: "abc" < "def"
		{"xyz", "abc", 1},          // string: "xyz" > "abc"
		{"test", "test", 0},        // string: "test" == "test"
		{nil, nil, 0},              // nil == nil
		{nil, 10, -1},              // nil < any
		{10, nil, 1},               // any > nil
	}

	for _, tc := range testCases {
		result := compareKeys(tc.k1, tc.k2)
		if result != tc.expected {
			t.Errorf("compareKeys(%v, %v) = %d, expected %d",
				tc.k1, tc.k2, result, tc.expected)
		}
	}

	t.Log("Compare keys test passed")
}

// TestExplainLockConflict 测试锁冲突解释功能
func TestExplainLockConflict(t *testing.T) {
	explanation := ExplainLockConflict(LOCK_GAP, LOCK_INSERT_INTENTION, LOCK_S, LOCK_X)
	if explanation == "" {
		t.Fatal("ExplainLockConflict should return non-empty explanation")
	}
	t.Logf("Lock conflict explanation: %s", explanation)

	explanation2 := ExplainLockConflict(LOCK_GAP, LOCK_GAP, LOCK_S, LOCK_X)
	t.Logf("Gap lock compatibility explanation: %s", explanation2)

	t.Log("Explain lock conflict test passed")
}

// BenchmarkGapLockAcquire 性能测试：Gap锁获取
func BenchmarkGapLockAcquire(b *testing.B) {
	lm := NewLockManager()
	defer lm.Close()

	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txID := uint64(i % 1000)
		_ = lm.AcquireGapLock(txID, gapRange, LOCK_S)
	}
}

// BenchmarkNextKeyLockAcquire 性能测试：Next-Key锁获取
func BenchmarkNextKeyLockAcquire(b *testing.B) {
	lm := NewLockManager()
	defer lm.Close()

	gapRange := &GapRange{
		LowerBound: 10,
		UpperBound: 20,
		TableID:    1,
		IndexID:    1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txID := uint64(i % 1000)
		recordKey := 20
		_ = lm.AcquireNextKeyLock(txID, recordKey, gapRange, LOCK_S)
	}
}
