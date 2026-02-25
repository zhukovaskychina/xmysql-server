package manager_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// TestLSNManager 测试LSN管理器
func TestLSNManager(t *testing.T) {
	lsnMgr := manager.NewLSNManager(1)

	// 测试单个LSN分配
	lsn1 := lsnMgr.AllocateLSN()
	lsn2 := lsnMgr.AllocateLSN()

	if lsn2 <= lsn1 {
		t.Errorf("LSN应该单调递增: lsn1=%d, lsn2=%d", lsn1, lsn2)
	}

	// 测试批量LSN分配
	start, end := lsnMgr.AllocateLSNRange(10)
	if end-start+1 != 10 {
		t.Errorf("批量分配LSN范围错误: start=%d, end=%d", start, end)
	}

	// 测试统计信息
	stats := lsnMgr.GetStats()
	if stats.TotalAllocated == 0 {
		t.Error("LSN分配统计错误")
	}
	t.Logf("LSN Stats: Current=%d, Total=%d", stats.CurrentLSN, stats.TotalAllocated)
}

// TestGroupCommit 测试组提交
func TestGroupCommit(t *testing.T) {
	gc := manager.NewGroupCommit(10*time.Millisecond, 100)

	// 模拟批量提交
	gc.RecordCommit(50, 5*time.Millisecond)
	gc.RecordCommit(30, 3*time.Millisecond)

	stats := gc.GetStats()
	if stats.TotalBatches != 2 {
		t.Errorf("批次统计错误: expected=2, got=%d", stats.TotalBatches)
	}

	t.Logf("GroupCommit Stats: Batches=%d, AvgSize=%.2f, AvgLatency=%v",
		stats.TotalBatches, stats.AvgBatchSize, stats.AvgCommitLatency)
}

// TestRedoLogFormatter 测试Redo日志格式化
func TestRedoLogFormatter(t *testing.T) {
	formatter := manager.NewRedoLogFormatter()

	// 测试INSERT日志格式化
	insertData := []byte("test record data")
	logData, err := formatter.FormatInsertLog(100, 1, 1000, 0, insertData)
	if err != nil {
		t.Fatalf("格式化INSERT日志失败: %v", err)
	}

	// 测试解析
	entry, header, err := formatter.ParseLog(logData)
	if err != nil {
		t.Fatalf("解析日志失败: %v", err)
	}

	if entry.LSN != 100 {
		t.Errorf("LSN不匹配: expected=100, got=%d", entry.LSN)
	}
	if header.Type != manager.LOG_TYPE_INSERT {
		t.Errorf("日志类型不匹配: expected=%d, got=%d", manager.LOG_TYPE_INSERT, header.Type)
	}

	t.Logf("Redo Log: LSN=%d, Type=%s, Size=%d bytes",
		entry.LSN, formatter.GetLogTypeName(header.Type), len(logData))
}

// TestUndoLogFormatter 测试Undo日志格式化
func TestUndoLogFormatter(t *testing.T) {
	formatter := manager.NewUndoLogFormatter()

	// 测试UPDATE Undo日志
	oldData := []byte("old record")
	bitmap := []byte{0xFF, 0x00} // 列位图
	logData, err := formatter.FormatUpdateUndo(200, 2, 100, 500, oldData, bitmap)
	if err != nil {
		t.Fatalf("格式化UPDATE Undo日志失败: %v", err)
	}

	// 测试解析
	entry, header, err := formatter.ParseLog(logData)
	if err != nil {
		t.Fatalf("解析Undo日志失败: %v", err)
	}

	if entry.LSN != 200 {
		t.Errorf("LSN不匹配: expected=200, got=%d", entry.LSN)
	}
	if header.TableID != 100 {
		t.Errorf("TableID不匹配: expected=100, got=%d", header.TableID)
	}

	// 解析UPDATE Undo数据
	parsedBitmap, parsedOldData, err := formatter.ParseUpdateUndo(entry.Data)
	if err != nil {
		t.Fatalf("解析UPDATE Undo数据失败: %v", err)
	}

	if string(parsedOldData) != string(oldData) {
		t.Errorf("旧值不匹配: expected=%s, got=%s", oldData, parsedOldData)
	}

	t.Logf("Undo Log: LSN=%d, TableID=%d, RecordID=%d, Bitmap=%v",
		entry.LSN, header.TableID, header.RecordID, parsedBitmap)
}

// TestRedoLogManager 测试Redo日志管理器
func TestRedoLogManager(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "redo_log_test")
	defer os.RemoveAll(tempDir)

	redoMgr, err := manager.NewRedoLogManager(tempDir, 100)
	if err != nil {
		t.Fatalf("创建Redo日志管理器失败: %v", err)
	}
	defer redoMgr.Close()

	// 测试日志追加
	entry := &manager.RedoLogEntry{
		TrxID:  1,
		PageID: 1000,
		Type:   manager.LOG_TYPE_INSERT,
		Data:   []byte("test data"),
	}

	lsn, err := redoMgr.Append(entry)
	if err != nil {
		t.Fatalf("追加日志失败: %v", err)
	}

	if lsn == 0 {
		t.Error("LSN应该大于0")
	}

	// 测试刷新
	if err := redoMgr.Flush(lsn); err != nil {
		t.Fatalf("刷新日志失败: %v", err)
	}

	// 测试统计信息
	stats := redoMgr.GetStats()
	t.Logf("Redo Log Stats: CurrentLSN=%d, BufferedLogs=%d",
		stats.CurrentLSN, stats.BufferedLogs)
}

// TestUndoLogManager 测试Undo日志管理器
func TestUndoLogManager(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "undo_log_test")
	defer os.RemoveAll(tempDir)

	undoMgr, err := manager.NewUndoLogManager(tempDir)
	if err != nil {
		t.Fatalf("创建Undo日志管理器失败: %v", err)
	}
	defer undoMgr.Close()

	// 测试日志追加
	entry := &manager.UndoLogEntry{
		LSN:     100,
		TrxID:   1,
		TableID: 100,
		Type:    manager.LOG_TYPE_INSERT,
		Data:    []byte("pk data"),
	}

	if err := undoMgr.Append(entry); err != nil {
		t.Fatalf("追加Undo日志失败: %v", err)
	}

	// 测试活跃事务
	activeTxns := undoMgr.GetActiveTxns()
	if len(activeTxns) == 0 {
		t.Error("应该有活跃事务")
	}

	// 测试Purge阈值
	undoMgr.SetPurgeThreshold(1 * time.Minute)
	if undoMgr.GetPurgeThreshold() != 1*time.Minute {
		t.Error("Purge阈值设置失败")
	}

	// 测试统计信息
	stats := undoMgr.GetStats()
	t.Logf("Undo Log Stats: ActiveTxns=%d, TotalLogs=%d",
		stats.ActiveTxns, stats.TotalLogs)
}

// TestCrashRecovery 测试崩溃恢复
func TestCrashRecovery(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "crash_recovery_test")
	defer os.RemoveAll(tempDir)

	// 创建日志管理器
	redoMgr, _ := manager.NewRedoLogManager(filepath.Join(tempDir, "redo"), 100)
	defer redoMgr.Close()

	undoMgr, _ := manager.NewUndoLogManager(filepath.Join(tempDir, "undo"))
	defer undoMgr.Close()

	// 创建崩溃恢复管理器
	recovery := manager.NewCrashRecovery(redoMgr, undoMgr, 1000)

	// 注意：这里只是测试框架，实际恢复需要真实的日志数据
	// 在实际使用中，Recover()会执行完整的三阶段恢复

	// 测试恢复状态
	status := recovery.GetRecoveryStatus()
	t.Logf("Recovery Status: Phase=%s, CheckpointLSN=%d",
		status.Phase, status.CheckpointLSN)

	// 测试分析结果
	analysisResult := recovery.GetAnalysisResult()
	t.Logf("Analysis Result: RedoStartLSN=%d, RedoEndLSN=%d, ActiveTxns=%d",
		analysisResult.RedoStartLSN, analysisResult.RedoEndLSN, len(analysisResult.ActiveTransactions))
}

// TestLSNRange 测试LSN范围
func TestLSNRange(t *testing.T) {
	r1 := &manager.LSNRange{Start: 100, End: 200}
	r2 := &manager.LSNRange{Start: 150, End: 250}

	// 测试包含
	if !r1.Contains(150) {
		t.Error("LSN 150应该在范围内")
	}

	// 测试大小
	if r1.Size() != 101 {
		t.Errorf("范围大小错误: expected=101, got=%d", r1.Size())
	}

	// 测试重叠
	if !r1.Overlaps(r2) {
		t.Error("范围应该重叠")
	}

	// 测试合并
	merged := r1.Merge(r2)
	if merged.Start != 100 || merged.End != 250 {
		t.Errorf("合并范围错误: start=%d, end=%d", merged.Start, merged.End)
	}

	t.Logf("LSN Range: %d-%d (size=%d)", r1.Start, r1.End, r1.Size())
}
