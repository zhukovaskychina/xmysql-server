package mvcc

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"testing"
	"time"
)

func TestRecordVersion(t *testing.T) {
	// 测试记录版本创建
	key := basic.NewStringValue("test_key")
	row := basic.NewRow([]byte("test_data"))

	record := NewRecordVersion(1, 100, key, row)

	if record.GetVersion() != 1 {
		t.Errorf("Expected version 1, got %d", record.GetVersion())
	}

	if record.GetTxID() != 100 {
		t.Errorf("Expected txID 100, got %d", record.GetTxID())
	}

	if record.IsDeleted() {
		t.Error("New record should not be deleted")
	}

	// 测试标记删除
	record.MarkDeleted()
	if !record.IsDeleted() {
		t.Error("Record should be marked as deleted")
	}
}

func TestLockMode(t *testing.T) {
	// 测试锁模式
	shared := LockModeShared
	exclusive := LockModeExclusive

	if shared.String() != "SHARED" {
		t.Errorf("Expected SHARED, got %s", shared.String())
	}

	if exclusive.String() != "EXCLUSIVE" {
		t.Errorf("Expected EXCLUSIVE, got %s", exclusive.String())
	}

	// 测试锁兼容性
	if !shared.IsCompatible(LockModeShared) {
		t.Error("Shared locks should be compatible with each other")
	}

	if exclusive.IsCompatible(LockModeShared) {
		t.Error("Exclusive lock should not be compatible with shared lock")
	}

	if exclusive.IsCompatible(LockModeExclusive) {
		t.Error("Exclusive locks should not be compatible with each other")
	}
}

func TestPageSnapshot(t *testing.T) {
	pageID := uint32(1)
	spaceID := uint32(0)
	version := uint64(1)
	txID := uint64(100)

	// 创建页面快照
	snapshot := NewPageSnapshot(pageID, spaceID, version, txID)

	if snapshot.GetPageID() != pageID {
		t.Errorf("Expected pageID %d, got %d", pageID, snapshot.GetPageID())
	}

	if snapshot.GetVersion() != version {
		t.Errorf("Expected version %d, got %d", version, snapshot.GetVersion())
	}

	if snapshot.GetState() != SnapshotStateActive {
		t.Errorf("Expected state ACTIVE, got %s", snapshot.GetState().String())
	}

	// 测试添加记录
	key := basic.NewStringValue("test_key")
	row := basic.NewRow([]byte("test_data"))
	record := NewRecordVersion(1, 100, key, row)

	snapshot.AddRecord(1, record)

	if snapshot.GetRecordCount() != 1 {
		t.Errorf("Expected 1 record, got %d", snapshot.GetRecordCount())
	}

	// 测试获取记录
	retrievedRecord, exists := snapshot.GetRecord(1)
	if !exists {
		t.Error("Record should exist")
	}

	if retrievedRecord.GetVersion() != record.GetVersion() {
		t.Error("Retrieved record version should match original")
	}
}

func TestMVCCIndexPage(t *testing.T) {
	pageID := uint32(1)
	spaceID := uint32(0)

	// 创建MVCC索引页
	page := NewMVCCIndexPage(pageID, spaceID)

	if page.ID() != pageID {
		t.Errorf("Expected pageID %d, got %d", pageID, page.ID())
	}

	if page.SpaceID() != spaceID {
		t.Errorf("Expected spaceID %d, got %d", spaceID, page.SpaceID())
	}

	// 测试初始化
	err := page.Init()
	if err != nil {
		t.Errorf("Init failed: %v", err)
	}

	// 测试锁获取
	txID := uint64(100)
	err = page.AcquireLock(txID, LockModeShared)
	if err != nil {
		t.Errorf("AcquireLock failed: %v", err)
	}

	// 测试锁冲突
	err = page.AcquireLock(txID+1, LockModeExclusive)
	if err == nil {
		t.Error("Expected lock conflict")
	}

	// 测试释放锁
	err = page.ReleaseLock(txID)
	if err != nil {
		t.Errorf("ReleaseLock failed: %v", err)
	}

	// 现在应该可以获取排他锁
	err = page.AcquireLock(txID+1, LockModeExclusive)
	if err != nil {
		t.Errorf("AcquireLock after release failed: %v", err)
	}
}

func TestReadView(t *testing.T) {
	txID := uint64(100)
	activeTxIDs := []uint64{90, 95, 105, 110}

	readView := NewReadView(txID, activeTxIDs)

	if readView.TxID != txID {
		t.Errorf("Expected txID %d, got %d", txID, readView.TxID)
	}

	// 测试可见性
	if !readView.IsVisible(txID) {
		t.Error("Own transaction should be visible")
	}

	if !readView.IsVisible(80) {
		t.Error("Transaction before low watermark should be visible")
	}

	if readView.IsVisible(120) {
		t.Error("Transaction after high watermark should not be visible")
	}

	if readView.IsVisible(95) {
		t.Error("Active transaction should not be visible")
	}

	if !readView.IsVisible(97) {
		t.Error("Non-active transaction in range should be visible")
	}
}

func TestIsolationLevel(t *testing.T) {
	levels := []IsolationLevel{
		IsolationReadUncommitted,
		IsolationReadCommitted,
		IsolationRepeatableRead,
		IsolationSerializable,
	}

	expectedStrings := []string{
		"READ_UNCOMMITTED",
		"READ_COMMITTED",
		"REPEATABLE_READ",
		"SERIALIZABLE",
	}

	for i, level := range levels {
		if level.String() != expectedStrings[i] {
			t.Errorf("Expected %s, got %s", expectedStrings[i], level.String())
		}
	}
}

func TestVersionChain(t *testing.T) {
	key := basic.NewStringValue("test_key")
	row1 := basic.NewRow([]byte("version1"))
	row2 := basic.NewRow([]byte("version2"))
	row3 := basic.NewRow([]byte("version3"))

	// 创建版本链
	v1 := NewRecordVersion(1, 100, key, row1)
	v2 := NewRecordVersion(2, 101, key, row2)
	v3 := NewRecordVersion(3, 102, key, row3)

	// 建立版本链：v3 -> v2 -> v1
	v3.SetNext(v2)
	v2.SetNext(v1)

	// 测试版本链长度
	if v3.GetVersionChainLength() != 3 {
		t.Errorf("Expected chain length 3, got %d", v3.GetVersionChainLength())
	}

	// 测试按事务ID查找
	found := v3.FindVersionByTxID(101)
	if found == nil || found.GetTxID() != 101 {
		t.Error("Should find version by txID 101")
	}

	// 测试可见性
	readTime := time.Now()
	visible := v3.GetLatestVisibleVersion(100, readTime)
	if visible == nil {
		t.Error("Should find visible version")
	}
}
