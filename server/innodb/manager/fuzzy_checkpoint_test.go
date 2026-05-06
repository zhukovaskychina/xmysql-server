package manager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestFuzzyCheckpoint_CreateAndRead LOG-015: 模糊检查点创建与读取
func TestFuzzyCheckpoint_CreateAndRead(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "xmysql_fuzzy_checkpoint_test")
	defer os.RemoveAll(dir)

	lsnMgr := NewLSNManager(1)
	_ = lsnMgr.AllocateLSN()
	_ = lsnMgr.AllocateLSN()

	fc, err := NewFuzzyCheckpoint(dir, lsnMgr)
	if err != nil {
		t.Fatalf("NewFuzzyCheckpoint: %v", err)
	}

	err = fc.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}

	lsn := fc.GetCheckpointLSN()
	if lsn == 0 {
		t.Error("GetCheckpointLSN 应在创建检查点后 > 0")
	}
	stats := fc.GetStats()
	if stats.TotalCheckpoints != 1 {
		t.Errorf("TotalCheckpoints = %d, want 1", stats.TotalCheckpoints)
	}
	t.Logf("模糊检查点 LSN=%d, TotalCheckpoints=%d - 通过", lsn, stats.TotalCheckpoints)
}
