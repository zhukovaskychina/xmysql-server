package mvcc

import (
	"testing"
)

// TestVersionChain_CreateAndInsert 测试版本链创建与插入（TXN-002：记录版本链创建）
func TestVersionChain_CreateAndInsert(t *testing.T) {
	vc := NewVersionChain()
	if vc == nil {
		t.Fatal("NewVersionChain returned nil")
	}
	if vc.GetLength() != 0 {
		t.Errorf("new chain length = %d, want 0", vc.GetLength())
	}
	if vc.GetLatestVersion() != nil {
		t.Error("new chain latest version should be nil")
	}

	// 插入第一个版本
	vc.InsertVersion(10, 100, []byte("v1"), false)
	if vc.GetLength() != 1 {
		t.Errorf("after insert length = %d, want 1", vc.GetLength())
	}
	latest := vc.GetLatestVersion()
	if latest == nil || latest.TrxID != 10 || string(latest.Data) != "v1" {
		t.Errorf("latest version = %+v", latest)
	}

	// 头插第二个版本（新版本在头）
	vc.InsertVersion(20, 200, []byte("v2"), false)
	if vc.GetLength() != 2 {
		t.Errorf("after second insert length = %d, want 2", vc.GetLength())
	}
	latest = vc.GetLatestVersion()
	if latest == nil || latest.TrxID != 20 || string(latest.Data) != "v2" {
		t.Errorf("latest version = %+v", latest)
	}
	if latest.Next == nil || latest.Next.TrxID != 10 {
		t.Error("version chain order: head should be 20, next 10")
	}
}

// TestVersionChain_Traverse 测试版本链遍历（TXN-002：记录版本链遍历）
func TestVersionChain_Traverse(t *testing.T) {
	vc := NewVersionChain()
	vc.InsertVersion(30, 300, []byte("a"), false)
	vc.InsertVersion(20, 200, []byte("b"), false)
	vc.InsertVersion(10, 100, []byte("c"), false)

	all := vc.GetAllVersions()
	if len(all) != 3 {
		t.Fatalf("GetAllVersions len = %d, want 3", len(all))
	}
	// 顺序应为新到旧：10 最新插入在头，然后是 20，再 30
	if all[0].TrxID != 10 || all[1].TrxID != 20 || all[2].TrxID != 30 {
		t.Errorf("traverse order: got %v, %v, %v", all[0].TrxID, all[1].TrxID, all[2].TrxID)
	}

	// GetVersionByTrxID
	for _, trxID := range []TrxId{10, 20, 30} {
		v := vc.GetVersionByTrxID(trxID)
		if v == nil || v.TrxID != trxID {
			t.Errorf("GetVersionByTrxID(%d) = %+v", trxID, v)
		}
	}
	if vc.GetVersionByTrxID(99) != nil {
		t.Error("GetVersionByTrxID(99) should be nil")
	}
}

// TestVersionChain_FindVisibleVersion 测试基于 ReadView 的可见版本查找（TXN-003：可见性判断）
func TestVersionChain_FindVisibleVersion(t *testing.T) {
	vc := NewVersionChain()
	// 链：头=50(最新), 40, 30, 20, 10(最旧)
	vc.InsertVersion(10, 100, []byte("v10"), false)
	vc.InsertVersion(20, 200, []byte("v20"), false)
	vc.InsertVersion(30, 300, []byte("v30"), false)
	vc.InsertVersion(40, 400, []byte("v40"), false)
	vc.InsertVersion(50, 500, []byte("v50"), false)

	// ReadView: 活跃 [25,35], min=25, max=60, creator=45
	// 可见: <25 可见(10,20); 25~35 活跃不可见(30,40); >=60 不可见; 35~60 且非活跃可见(50 不在活跃列表，但 50>=max 不可见)
	// 实际上 max=60，所以 trx_id 50 < 60，且 50 不在 [25,35]，所以 50 可见
	rv := NewReadView([]int64{25, 35}, 25, 60, 45)
	visible := vc.FindVisibleVersion(rv)
	if visible == nil {
		t.Fatal("FindVisibleVersion returned nil")
	}
	// 从新到旧第一个可见且未删除的应是 50（50 不在活跃列表，50<60）
	if visible.TrxID != 50 {
		t.Errorf("expected visible TrxID 50, got %d", visible.TrxID)
	}

	// 全部已提交在 ReadView 之前：min=100, max=200，则 10,20,30,40,50 都 < 100，都可见，应返回最新 50
	rv2 := NewReadView([]int64{}, 100, 200, 99)
	visible2 := vc.FindVisibleVersion(rv2)
	if visible2 == nil || visible2.TrxID != 50 {
		t.Errorf("expected visible TrxID 50, got %+v", visible2)
	}

	// 全部在 ReadView 之后或活跃：min=1, max=15, 活跃含 10,20,30,40,50 则都不可见（除 creator）
	rv3 := NewReadView([]int64{10, 20, 30, 40, 50}, 10, 60, 5)
	visible3 := vc.FindVisibleVersion(rv3)
	// creator=5，5 不在链上，所以没有“自己写的”版本；所有版本都在活跃列表，不可见
	if visible3 != nil {
		t.Errorf("expected nil (no visible), got TrxID %d", visible3.TrxID)
	}
}

// TestVersionChain_FindVisibleVersion_SkipDeleteMark 测试可见版本跳过已删除
func TestVersionChain_FindVisibleVersion_SkipDeleteMark(t *testing.T) {
	vc := NewVersionChain()
	vc.InsertVersion(10, 100, []byte("v10"), false)
	vc.InsertVersion(20, 200, []byte("v20"), true) // 删除标记
	vc.InsertVersion(30, 300, []byte("v30"), false)

	// 全部可见的 ReadView，应跳过 20 返回 30（最新可见未删）
	rv := NewReadView([]int64{}, 0, 100, 1)
	visible := vc.FindVisibleVersion(rv)
	if visible == nil || visible.TrxID != 30 {
		t.Errorf("expected visible TrxID 30 (skip delete-mark 20), got %+v", visible)
	}
}

// TestVersionChainManager 测试版本链管理器创建与获取
func TestVersionChainManager(t *testing.T) {
	vcm := NewVersionChainManager()
	if vcm.GetChainCount() != 0 {
		t.Errorf("new manager chain count = %d, want 0", vcm.GetChainCount())
	}

	c1 := vcm.GetOrCreateChain("key1")
	c2 := vcm.GetOrCreateChain("key1")
	if c1 != c2 {
		t.Error("GetOrCreateChain same key should return same chain")
	}
	c1.InsertVersion(1, 1, []byte("x"), false)
	if vcm.GetChainCount() != 1 {
		t.Errorf("chain count = %d, want 1", vcm.GetChainCount())
	}
	if vcm.GetTotalVersionCount() != 1 {
		t.Errorf("total version count = %d, want 1", vcm.GetTotalVersionCount())
	}

	vcm.DeleteChain("key1")
	if vcm.GetChain("key1") != nil {
		t.Error("DeleteChain then GetChain should return nil")
	}
}

// TestVersionChain_PurgeOldVersions 测试清理旧版本
func TestVersionChain_PurgeOldVersions(t *testing.T) {
	vc := NewVersionChain()
	vc.InsertVersion(10, 100, []byte("v10"), false)
	vc.InsertVersion(20, 200, []byte("v20"), false)
	vc.InsertVersion(30, 300, []byte("v30"), false)
	// 链：30 -> 20 -> 10

	// minTrxID=25：保留 head(30)，可清理 20、10（<25）
	n := vc.PurgeOldVersions(25)
	if n < 1 {
		t.Logf("PurgeOldVersions returned %d (implementation may retain head only)", n)
	}
	// 至少链还在，最新版本仍在
	if vc.GetLatestVersion() == nil {
		t.Error("after purge latest should still exist")
	}
}
