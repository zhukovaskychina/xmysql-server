package mvcc

import (
	"testing"
)

// TestReadView_ReadCommitted_Visibility 测试 Read Committed 隔离级别的可见性
// 场景：验证 RC 隔离级别下，每次读取都创建新的 ReadView（语句级快照）
func TestReadView_ReadCommitted_Visibility(t *testing.T) {
	// 模拟场景：
	// 1. T1 (ID=1) 开始并写入数据
	// 2. T2 (ID=2) 开始，此时 T1 还活跃
	// 3. T1 提交
	// 4. T2 创建新的 ReadView（模拟 RC 的语句级快照）

	t.Log("Step 1: T1 begins and writes version V1")
	t1ID := int64(1)
	versionV1 := t1ID

	t.Log("Step 2: T2 begins (T1 is still active)")
	t2ID := int64(2)

	// T2 的第一个 ReadView：T1 还在活跃列表中
	activeIDs := []int64{t1ID}
	minTrxID := t1ID
	maxTrxID := int64(3) // 下一个事务 ID
	rv1 := NewReadView(activeIDs, minTrxID, maxTrxID, t2ID)

	// T2 在 T1 提交前读取 - 应该看不到 V1
	visible := rv1.IsVisible(versionV1)
	t.Logf("T2 reads before T1 commits (ReadView 1): V1 visible=%v (expected: false)", visible)
	if visible {
		t.Errorf("T2 should NOT see uncommitted V1 from T1")
	}

	t.Log("Step 3: T1 commits")
	// T1 提交后，不再在活跃列表中

	t.Log("Step 4: T2 creates new ReadView (simulating RC statement-level snapshot)")
	// T2 创建新的 ReadView（RC 隔离级别的语句级快照）
	// 此时活跃列表中没有 T1
	activeIDs2 := []int64{}             // T1 已提交，不在活跃列表中
	minTrxID2 := int64(^uint64(0) >> 1) // 没有活跃事务
	maxTrxID2 := int64(3)
	rv2 := NewReadView(activeIDs2, minTrxID2, maxTrxID2, t2ID)

	// T2 在 T1 提交后读取 - 应该能看到 V1
	visible = rv2.IsVisible(versionV1)
	t.Logf("T2 reads after T1 commits (ReadView 2): V1 visible=%v (expected: true)", visible)
	if !visible {
		t.Errorf("T2 (RC) should see committed V1 from T1")
	}
}

// TestReadView_RepeatableRead_Visibility 测试 Repeatable Read 隔离级别的可见性
// 场景：验证 RR 隔离级别下，事务始终使用开始时的 ReadView（事务级快照）
func TestReadView_RepeatableRead_Visibility(t *testing.T) {
	// 模拟场景：
	// 1. T1 (ID=1) 开始并写入数据
	// 2. T3 (ID=3) 开始，此时 T1 还活跃，创建 ReadView
	// 3. T1 提交
	// 4. T3 继续使用开始时的 ReadView（模拟 RR 的事务级快照）

	t.Log("Step 1: T1 begins and writes version V1")
	t1ID := int64(1)
	versionV1 := t1ID

	t.Log("Step 2: T3 (RR) begins before T1 commits")
	t3ID := int64(3)

	// T3 创建 ReadView：T1 在活跃列表中
	activeIDs := []int64{t1ID}
	minTrxID := t1ID
	maxTrxID := int64(4) // 下一个事务 ID
	rv := NewReadView(activeIDs, minTrxID, maxTrxID, t3ID)

	// T3 在 T1 提交前读取 - 应该看不到 V1
	visible := rv.IsVisible(versionV1)
	t.Logf("T3 reads before T1 commits: V1 visible=%v (expected: false)", visible)
	if visible {
		t.Errorf("T3 should NOT see uncommitted V1 from T1")
	}

	t.Log("Step 3: T1 commits")
	// T1 提交后，但 T3 仍然使用开始时的 ReadView

	t.Log("Step 4: T3 reads after T1 commits (using same ReadView)")
	// RR 隔离级别：T3 继续使用开始时的 ReadView
	// 即使 T1 已提交，但在 T3 的 ReadView 中 T1 仍在活跃列表中
	visible = rv.IsVisible(versionV1)
	t.Logf("T3 reads after T1 commits: V1 visible=%v (expected: false)", visible)
	if visible {
		t.Errorf("T3 (RR) should NOT see V1 - T1 was active when T3's ReadView was created")
	}
}

// TestReadView_RepeatableRead_CanSeeCommittedBeforeStart 测试 RR 能看到开始前已提交的版本
func TestReadView_RepeatableRead_CanSeeCommittedBeforeStart(t *testing.T) {
	// 模拟场景：
	// 1. T1 (ID=1) 开始、写入并提交
	// 2. T2 (ID=2) 在 T1 提交后开始

	t.Log("Step 1: T1 begins, writes V1, and commits")
	t1ID := int64(1)
	versionV1 := t1ID
	t.Logf("T1 (ID=%d) writes version V1=%d and commits", t1ID, versionV1)

	t.Log("Step 2: T2 (RR) begins after T1 commits")
	t2ID := int64(2)

	// T2 创建 ReadView：T1 已提交，不在活跃列表中
	activeIDs := []int64{}             // T1 已提交
	minTrxID := int64(^uint64(0) >> 1) // 没有活跃事务
	maxTrxID := int64(3)               // 下一个事务 ID
	rv := NewReadView(activeIDs, minTrxID, maxTrxID, t2ID)

	// T2 应该能看到 V1（因为 T1 在 T2 开始前已提交）
	visible := rv.IsVisible(versionV1)
	t.Logf("T2 reads V1: visible=%v (expected: true)", visible)
	if !visible {
		t.Errorf("T2 (RR) should see V1 - T1 committed before T2 started")
	}
}

// TestReadView_TransactionCanSeeOwnChanges 测试事务能看到自己的修改
func TestReadView_TransactionCanSeeOwnChanges(t *testing.T) {
	t.Log("Testing transaction can see its own changes")

	txID := int64(5)
	versionCreatedByTx := txID

	// 创建 ReadView
	activeIDs := []int64{1, 2, 3, txID, 6, 7}
	minTrxID := int64(1)
	maxTrxID := int64(10)
	rv := NewReadView(activeIDs, minTrxID, maxTrxID, txID)

	// 事务应该能看到自己创建的版本
	visible := rv.IsVisible(versionCreatedByTx)
	t.Logf("Transaction sees its own version: visible=%v (expected: true)", visible)
	if !visible {
		t.Errorf("Transaction should see its own changes")
	}
}

// TestReadView_VisibilityRules 测试 MVCC 可见性规则
func TestReadView_VisibilityRules(t *testing.T) {
	// 创建测试场景：
	// - 活跃事务：[5, 10, 15, 20]
	// - minTrxID: 5
	// - maxTrxID: 25
	// - creatorTrxID: 12

	activeIDs := []int64{5, 10, 15, 20}
	minTrxID := int64(5)
	maxTrxID := int64(25)
	creatorTrxID := int64(12)

	rv := NewReadView(activeIDs, minTrxID, maxTrxID, creatorTrxID)

	testCases := []struct {
		version  int64
		expected bool
		reason   string
	}{
		// 规则 1: 自己创建的版本
		{12, true, "transaction sees its own changes"},

		// 规则 2: version < minTrxID (已提交)
		{1, true, "version < minTrxID (committed before ReadView)"},
		{4, true, "version < minTrxID (committed before ReadView)"},

		// 规则 3: version >= maxTrxID (未开始)
		{25, false, "version >= maxTrxID (started after ReadView)"},
		{30, false, "version >= maxTrxID (started after ReadView)"},

		// 规则 4: minTrxID <= version < maxTrxID
		// 在活跃列表中 - 不可见
		{5, false, "in active list (uncommitted)"},
		{10, false, "in active list (uncommitted)"},
		{15, false, "in active list (uncommitted)"},
		{20, false, "in active list (uncommitted)"},

		// 不在活跃列表中 - 可见（已提交）
		{6, true, "not in active list (committed)"},
		{8, true, "not in active list (committed)"},
		{11, true, "not in active list (committed)"},
		{13, true, "not in active list (committed)"},
		{18, true, "not in active list (committed)"},
		{22, true, "not in active list (committed)"},
	}

	for _, tc := range testCases {
		visible := rv.IsVisible(tc.version)
		if visible != tc.expected {
			t.Errorf("Version %d: expected visible=%v, got %v (reason: %s)",
				tc.version, tc.expected, visible, tc.reason)
		} else {
			t.Logf("✓ Version %d: visible=%v (%s)", tc.version, visible, tc.reason)
		}
	}
}

// TestReadView_MultipleTransactions 测试多事务场景
func TestReadView_MultipleTransactions(t *testing.T) {
	// 模拟场景：
	// - T1 (ID=1), T2 (ID=2), T3 (ID=3) 都已开始
	// - T4 (ID=4) 新开始，创建 ReadView
	// - T1 提交
	// - T5 (ID=5) 新开始，创建 ReadView

	t.Log("Initial state: T1, T2, T3 are active")

	// T4 开始时的 ReadView
	t.Log("T4 begins and creates ReadView")
	t4ID := int64(4)
	activeIDs4 := []int64{1, 2, 3}
	rv4 := NewReadView(activeIDs4, 1, 5, t4ID)

	// T4 的可见性检查
	tests4 := []struct {
		version  int64
		expected bool
		desc     string
	}{
		{1, false, "T1 is active"},
		{2, false, "T2 is active"},
		{3, false, "T3 is active"},
		{4, true, "T4 sees its own changes"},
	}

	for _, tc := range tests4 {
		visible := rv4.IsVisible(tc.version)
		if visible != tc.expected {
			t.Errorf("T4: %s - expected %v, got %v", tc.desc, tc.expected, visible)
		}
	}

	// T1 提交后，T5 开始
	t.Log("T1 commits, then T5 begins")
	t5ID := int64(5)
	activeIDs5 := []int64{2, 3, 4} // T1 已提交，不在活跃列表中
	rv5 := NewReadView(activeIDs5, 2, 6, t5ID)

	// T5 的可见性检查
	tests5 := []struct {
		version  int64
		expected bool
		desc     string
	}{
		{1, true, "T1 committed before T5 started"},
		{2, false, "T2 is active"},
		{3, false, "T3 is active"},
		{4, false, "T4 is active"},
		{5, true, "T5 sees its own changes"},
	}

	for _, tc := range tests5 {
		visible := rv5.IsVisible(tc.version)
		if visible != tc.expected {
			t.Errorf("T5: %s - expected %v, got %v", tc.desc, tc.expected, visible)
		}
	}
}

// TestReadView_Clone 测试 ReadView 克隆
func TestReadView_Clone(t *testing.T) {
	activeIDs := []int64{5, 10, 15}
	minTrxID := int64(5)
	maxTrxID := int64(20)
	creatorTrxID := int64(12)

	original := NewReadView(activeIDs, minTrxID, maxTrxID, creatorTrxID)
	cloned := original.Clone()

	// 验证克隆的 ReadView 与原始的行为一致
	testVersions := []int64{1, 5, 8, 10, 12, 15, 18, 20, 25}
	for _, version := range testVersions {
		originalVisible := original.IsVisible(version)
		clonedVisible := cloned.IsVisible(version)

		if originalVisible != clonedVisible {
			t.Errorf("Version %d: original visible=%v, cloned visible=%v",
				version, originalVisible, clonedVisible)
		}
	}

	// 验证克隆的字段
	if cloned.GetMinTrxID() != original.GetMinTrxID() {
		t.Errorf("MinTrxID mismatch")
	}
	if cloned.GetMaxTrxID() != original.GetMaxTrxID() {
		t.Errorf("MaxTrxID mismatch")
	}
	if cloned.GetCreatorTrxID() != original.GetCreatorTrxID() {
		t.Errorf("CreatorTrxID mismatch")
	}
	if cloned.GetActiveCount() != original.GetActiveCount() {
		t.Errorf("ActiveCount mismatch")
	}
}

// BenchmarkReadView_IsVisible 性能测试：可见性判断
func BenchmarkReadView_IsVisible(b *testing.B) {
	// 创建一个有 100 个活跃事务的 ReadView
	activeIDs := make([]int64, 100)
	for i := 0; i < 100; i++ {
		activeIDs[i] = int64(i * 2) // 偶数事务 ID
	}

	rv := NewReadView(activeIDs, 0, 1000, 500)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 测试不同的版本
		rv.IsVisible(int64(i % 1000))
	}
}

// BenchmarkReadView_IsVisibleFast 性能测试：快速可见性判断
func BenchmarkReadView_IsVisibleFast(b *testing.B) {
	// 创建一个有 100 个活跃事务的 ReadView
	activeIDs := make([]int64, 100)
	for i := 0; i < 100; i++ {
		activeIDs[i] = int64(i * 2)
	}

	rv := NewReadView(activeIDs, 0, 1000, 500)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rv.IsVisibleFast(int64(i % 1000))
	}
}

// BenchmarkReadView_NewReadView 性能测试：创建 ReadView
func BenchmarkReadView_NewReadView(b *testing.B) {
	activeIDs := make([]int64, 100)
	for i := 0; i < 100; i++ {
		activeIDs[i] = int64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewReadView(activeIDs, 0, 1000, 500)
	}
}
