package manager

import (
	"testing"
)

// TestLSNManagerBasic 测试LSN管理器基本功能
func TestLSNManagerBasic(t *testing.T) {
	lsnMgr := NewLSNManager(1)

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
	t.Logf("✅ LSN Manager 测试通过")
}

// TestRedoLogFormatterBasic 测试Redo日志格式化基本功能
func TestRedoLogFormatterBasic(t *testing.T) {
	formatter := NewRedoLogFormatter()

	// 测试INSERT日志
	insertData := []byte("test record")
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
	if header.Type != LOG_TYPE_INSERT {
		t.Errorf("日志类型不匹配")
	}
	t.Logf("✅ Redo Log Formatter 测试通过")
}

// TestUndoLogFormatterBasic 测试Undo日志格式化基本功能
func TestUndoLogFormatterBasic(t *testing.T) {
	formatter := NewUndoLogFormatter()

	// 测试UPDATE Undo日志
	oldData := []byte("old data")
	bitmap := []byte{0xFF}
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
		t.Errorf("LSN不匹配")
	}
	if header.TableID != 100 {
		t.Errorf("TableID不匹配")
	}
	t.Logf("✅ Undo Log Formatter 测试通过")
}

// TestLSNRangeBasic 测试LSN范围基本功能
func TestLSNRangeBasic(t *testing.T) {
	r1 := &LSNRange{Start: 100, End: 200}
	r2 := &LSNRange{Start: 150, End: 250}

	if !r1.Contains(150) {
		t.Error("Contains方法错误")
	}

	if r1.Size() != 101 {
		t.Errorf("Size方法错误: expected=101, got=%d", r1.Size())
	}

	if !r1.Overlaps(r2) {
		t.Error("Overlaps方法错误")
	}

	merged := r1.Merge(r2)
	if merged.Start != 100 || merged.End != 250 {
		t.Error("Merge方法错误")
	}
	t.Logf("✅ LSN Range 测试通过")
}
