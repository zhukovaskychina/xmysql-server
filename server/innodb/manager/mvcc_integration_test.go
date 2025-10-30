package manager

import (
	"testing"
)

// TestMVCC_ReadCommitted_Visibility 测试 Read Committed 隔离级别的可见性
// 场景：验证 RC 隔离级别下，事务能读到其他事务已提交的最新版本（语句级快照）
func TestMVCC_ReadCommitted_Visibility(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 步骤 1: 事务 T1 开始并写入数据版本 V1
	t.Log("Step 1: T1 begins and writes version V1")
	t1, err := tm.Begin(false, TRX_ISO_READ_COMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T1: %v", err)
	}
	t.Logf("T1 started with ID=%d", t1.ID)

	// 模拟 T1 写入数据（版本号为 T1.ID）
	versionV1 := t1.ID
	t.Logf("T1 writes version V1=%d", versionV1)

	// 步骤 2: 事务 T2 (RC 隔离级别) 开始，在 T1 提交前读取
	t.Log("Step 2: T2 (RC) begins before T1 commits")
	t2, err := tm.Begin(false, TRX_ISO_READ_COMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T2: %v", err)
	}
	t.Logf("T2 started with ID=%d", t2.ID)

	// T2 在 T1 提交前读取 - 应该看不到 V1（因为 T1 还未提交）
	visibleBeforeCommit := tm.IsVisible(t2, versionV1)
	t.Logf("T2 reads before T1 commits: V1 visible=%v (expected: false)", visibleBeforeCommit)
	if visibleBeforeCommit {
		t.Errorf("T2 should NOT see uncommitted V1 from T1")
	}

	// 步骤 3: T1 提交
	t.Log("Step 3: T1 commits")
	if err := tm.Commit(t1); err != nil {
		t.Fatalf("Failed to commit T1: %v", err)
	}
	t.Log("T1 committed successfully")

	// 步骤 4: T2 在 T1 提交后再次读取
	// RC 隔离级别：每次读取都创建新的 ReadView（语句级快照）
	// 因此 T2 应该能看到 T1 已提交的 V1
	t.Log("Step 4: T2 reads after T1 commits")
	visibleAfterCommit := tm.IsVisible(t2, versionV1)
	t.Logf("T2 reads after T1 commits: V1 visible=%v (expected: true)", visibleAfterCommit)
	if !visibleAfterCommit {
		t.Errorf("T2 (RC) should see committed V1 from T1")
	}

	// 清理
	if err := tm.Commit(t2); err != nil {
		t.Fatalf("Failed to commit T2: %v", err)
	}
}

// TestMVCC_RepeatableRead_Visibility 测试 Repeatable Read 隔离级别的可见性
// 场景：验证 RR 隔离级别下，事务始终读到开始时的快照版本（事务级快照）
func TestMVCC_RepeatableRead_Visibility(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 步骤 1: 事务 T1 开始并写入数据版本 V1
	t.Log("Step 1: T1 begins and writes version V1")
	t1, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T1: %v", err)
	}
	t.Logf("T1 started with ID=%d", t1.ID)

	// 模拟 T1 写入数据（版本号为 T1.ID）
	versionV1 := t1.ID
	t.Logf("T1 writes version V1=%d", versionV1)

	// 步骤 2: 事务 T3 (RR 隔离级别) 在 T1 提交前开始
	t.Log("Step 2: T3 (RR) begins before T1 commits")
	t3, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T3: %v", err)
	}
	t.Logf("T3 started with ID=%d", t3.ID)

	// T3 在 T1 提交前读取 - 应该看不到 V1（因为 T1 还未提交）
	visibleBeforeCommit := tm.IsVisible(t3, versionV1)
	t.Logf("T3 reads before T1 commits: V1 visible=%v (expected: false)", visibleBeforeCommit)
	if visibleBeforeCommit {
		t.Errorf("T3 should NOT see uncommitted V1 from T1")
	}

	// 步骤 3: T1 提交
	t.Log("Step 3: T1 commits")
	if err := tm.Commit(t1); err != nil {
		t.Fatalf("Failed to commit T1: %v", err)
	}
	t.Log("T1 committed successfully")

	// 步骤 4: T3 在 T1 提交后读取
	// RR 隔离级别：使用事务开始时创建的 ReadView（事务级快照）
	// 因此 T3 仍然看不到 V1（因为 T1 在 T3 的 ReadView 创建时还是活跃的）
	t.Log("Step 4: T3 reads after T1 commits")
	visibleAfterCommit := tm.IsVisible(t3, versionV1)
	t.Logf("T3 reads after T1 commits: V1 visible=%v (expected: false)", visibleAfterCommit)
	if visibleAfterCommit {
		t.Errorf("T3 (RR) should NOT see V1 - T1 was active when T3's ReadView was created")
	}

	// 清理
	if err := tm.Commit(t3); err != nil {
		t.Fatalf("Failed to commit T3: %v", err)
	}
}

// TestMVCC_RepeatableRead_CanSeeCommittedBeforeStart 测试 RR 能看到开始前已提交的版本
// 场景：验证 RR 隔离级别下，事务能看到在其开始前已提交的版本
func TestMVCC_RepeatableRead_CanSeeCommittedBeforeStart(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 步骤 1: 事务 T1 开始、写入并提交
	t.Log("Step 1: T1 begins, writes V1, and commits")
	t1, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T1: %v", err)
	}
	versionV1 := t1.ID
	t.Logf("T1 (ID=%d) writes version V1=%d", t1.ID, versionV1)

	if err := tm.Commit(t1); err != nil {
		t.Fatalf("Failed to commit T1: %v", err)
	}
	t.Log("T1 committed successfully")

	// 步骤 2: 事务 T2 (RR) 在 T1 提交后开始
	t.Log("Step 2: T2 (RR) begins after T1 commits")
	t2, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T2: %v", err)
	}
	t.Logf("T2 started with ID=%d", t2.ID)

	// T2 应该能看到 V1（因为 T1 在 T2 开始前已提交）
	visible := tm.IsVisible(t2, versionV1)
	t.Logf("T2 reads V1: visible=%v (expected: true)", visible)
	if !visible {
		t.Errorf("T2 (RR) should see V1 - T1 committed before T2 started")
	}

	// 清理
	if err := tm.Commit(t2); err != nil {
		t.Fatalf("Failed to commit T2: %v", err)
	}
}

// TestMVCC_TransactionCanSeeOwnChanges 测试事务能看到自己的修改
// 场景：验证事务总是能看到自己创建的版本
func TestMVCC_TransactionCanSeeOwnChanges(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 测试 RC 隔离级别
	t.Log("Testing RC isolation level")
	t1, err := tm.Begin(false, TRX_ISO_READ_COMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T1: %v", err)
	}
	versionT1 := t1.ID
	visible := tm.IsVisible(t1, versionT1)
	t.Logf("T1 (RC) reads its own version: visible=%v (expected: true)", visible)
	if !visible {
		t.Errorf("T1 (RC) should see its own changes")
	}
	tm.Commit(t1)

	// 测试 RR 隔离级别
	t.Log("Testing RR isolation level")
	t2, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T2: %v", err)
	}
	versionT2 := t2.ID
	visible = tm.IsVisible(t2, versionT2)
	t.Logf("T2 (RR) reads its own version: visible=%v (expected: true)", visible)
	if !visible {
		t.Errorf("T2 (RR) should see its own changes")
	}
	tm.Commit(t2)
}

// TestMVCC_MultipleTransactions_ComplexScenario 测试复杂的多事务场景
// 场景：多个事务交错执行，验证不同隔离级别的可见性
func TestMVCC_MultipleTransactions_ComplexScenario(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 场景：
	// 1. T1 (RR) 开始
	// 2. T2 (RC) 开始
	// 3. T3 (RR) 开始并写入 V3
	// 4. T4 (RC) 开始
	// 5. T3 提交
	// 6. 验证各事务的可见性

	t.Log("Step 1: T1 (RR) begins")
	t1, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T1: %v", err)
	}
	t.Logf("T1 (RR) started with ID=%d", t1.ID)

	t.Log("Step 2: T2 (RC) begins")
	t2, err := tm.Begin(false, TRX_ISO_READ_COMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T2: %v", err)
	}
	t.Logf("T2 (RC) started with ID=%d", t2.ID)

	t.Log("Step 3: T3 (RR) begins and writes V3")
	t3, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin T3: %v", err)
	}
	versionV3 := t3.ID
	t.Logf("T3 (RR) started with ID=%d, writes V3=%d", t3.ID, versionV3)

	t.Log("Step 4: T4 (RC) begins")
	t4, err := tm.Begin(false, TRX_ISO_READ_COMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T4: %v", err)
	}
	t.Logf("T4 (RC) started with ID=%d", t4.ID)

	// 在 T3 提交前，所有事务都不应该看到 V3
	t.Log("Before T3 commits:")
	for _, tx := range []*Transaction{t1, t2, t4} {
		visible := tm.IsVisible(tx, versionV3)
		t.Logf("  T%d sees V3: %v (expected: false)", tx.ID, visible)
		if visible && tx.ID != t3.ID {
			t.Errorf("T%d should NOT see uncommitted V3", tx.ID)
		}
	}

	t.Log("Step 5: T3 commits")
	if err := tm.Commit(t3); err != nil {
		t.Fatalf("Failed to commit T3: %v", err)
	}

	// 在 T3 提交后：
	// - T1 (RR): 不应该看到 V3（T3 在 T1 的 ReadView 创建时还是活跃的）
	// - T2 (RC): 应该看到 V3（RC 使用语句级快照）
	// - T4 (RC): 应该看到 V3（RC 使用语句级快照）
	t.Log("After T3 commits:")

	visibleT1 := tm.IsVisible(t1, versionV3)
	t.Logf("  T1 (RR) sees V3: %v (expected: false)", visibleT1)
	if visibleT1 {
		t.Errorf("T1 (RR) should NOT see V3 - T3 was active when T1 started")
	}

	visibleT2 := tm.IsVisible(t2, versionV3)
	t.Logf("  T2 (RC) sees V3: %v (expected: true)", visibleT2)
	if !visibleT2 {
		t.Errorf("T2 (RC) should see committed V3")
	}

	visibleT4 := tm.IsVisible(t4, versionV3)
	t.Logf("  T4 (RC) sees V3: %v (expected: true)", visibleT4)
	if !visibleT4 {
		t.Errorf("T4 (RC) should see committed V3")
	}

	// 清理
	tm.Commit(t1)
	tm.Commit(t2)
	tm.Commit(t4)
}

// TestMVCC_ReadUncommitted_Visibility 测试 Read Uncommitted 隔离级别
// 场景：验证 RU 隔离级别下，事务能读到未提交的数据
func TestMVCC_ReadUncommitted_Visibility(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// T1 开始并写入
	t1, err := tm.Begin(false, TRX_ISO_READ_UNCOMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T1: %v", err)
	}
	versionV1 := t1.ID

	// T2 (RU) 开始
	t2, err := tm.Begin(false, TRX_ISO_READ_UNCOMMITTED)
	if err != nil {
		t.Fatalf("Failed to begin T2: %v", err)
	}

	// RU 隔离级别：应该能看到未提交的数据
	visible := tm.IsVisible(t2, versionV1)
	t.Logf("T2 (RU) sees uncommitted V1: %v (expected: true)", visible)
	if !visible {
		t.Errorf("T2 (RU) should see uncommitted V1")
	}

	// 清理
	tm.Commit(t1)
	tm.Commit(t2)
}

// TestMVCC_ConcurrentTransactions 测试并发事务场景
// 场景：验证多个并发事务的 MVCC 可见性
func TestMVCC_ConcurrentTransactions(t *testing.T) {
	// 创建临时目录用于日志
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	// 创建事务管理器
	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 创建 5 个并发事务
	const numTxs = 5
	txs := make([]*Transaction, numTxs)

	// 所有事务都使用 RR 隔离级别
	for i := 0; i < numTxs; i++ {
		tx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", i, err)
		}
		txs[i] = tx
		t.Logf("Transaction T%d started with ID=%d", i, tx.ID)
	}

	// 提交前半部分事务
	for i := 0; i < numTxs/2; i++ {
		if err := tm.Commit(txs[i]); err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", i, err)
		}
		t.Logf("Transaction T%d committed", i)
	}

	// 后半部分事务应该看不到前半部分的版本（因为它们在 ReadView 创建时都是活跃的）
	for i := numTxs / 2; i < numTxs; i++ {
		for j := 0; j < numTxs/2; j++ {
			visible := tm.IsVisible(txs[i], txs[j].ID)
			t.Logf("T%d sees T%d's version: %v (expected: false)", i, j, visible)
			if visible {
				t.Errorf("T%d (RR) should NOT see T%d's version - T%d was active when T%d started",
					i, j, j, i)
			}
		}
	}

	// 清理剩余事务
	for i := numTxs / 2; i < numTxs; i++ {
		tm.Commit(txs[i])
	}
}

// BenchmarkMVCC_IsVisible 性能测试：可见性判断
func BenchmarkMVCC_IsVisible(b *testing.B) {
	// 创建临时目录
	redoDir := b.TempDir()
	undoDir := b.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		b.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 创建测试事务
	tx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}

	version := tx.ID - 1 // 一个已提交的版本

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.IsVisible(tx, version)
	}
	b.StopTimer()

	tm.Commit(tx)
}

// BenchmarkMVCC_BeginCommit 性能测试：事务开始和提交
func BenchmarkMVCC_BeginCommit(b *testing.B) {
	// 创建临时目录
	redoDir := b.TempDir()
	undoDir := b.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		b.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := tm.Begin(true, TRX_ISO_REPEATABLE_READ)
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}
		if err := tm.Commit(tx); err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}
	}
}
