package record

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestClusterLeafRowData_HiddenColumns 测试隐藏系统列的访问
func TestClusterLeafRowData_HiddenColumns(t *testing.T) {
	// 创建测试用的tuple
	tuple := createTestTuple()

	// 创建ClusterLeafRowData
	content := make([]byte, 100)
	rowData := NewClusterLeafRowDataWithContents(content, tuple).(*ClusterLeafRowData)

	// 测试DB_ROW_ID
	testRowID := uint64(12345)
	rowData.SetDBRowID(testRowID)
	if got := rowData.GetDBRowID(); got != testRowID {
		t.Errorf("GetDBRowID() = %d, want %d", got, testRowID)
	}
	t.Logf("✅ DB_ROW_ID: set=%d, get=%d", testRowID, rowData.GetDBRowID())

	// 测试DB_TRX_ID
	testTrxID := uint64(100)
	rowData.SetDBTrxID(testTrxID)
	if got := rowData.GetDBTrxID(); got != testTrxID {
		t.Errorf("GetDBTrxID() = %d, want %d", got, testTrxID)
	}
	t.Logf("✅ DB_TRX_ID: set=%d, get=%d", testTrxID, rowData.GetDBTrxID())

	// 测试DB_ROLL_PTR
	testRollPtr := uint64(200)
	rowData.SetDBRollPtr(testRollPtr)
	if got := rowData.GetDBRollPtr(); got != testRollPtr {
		t.Errorf("GetDBRollPtr() = %d, want %d", got, testRollPtr)
	}
	t.Logf("✅ DB_ROLL_PTR: set=%d, get=%d", testRollPtr, rowData.GetDBRollPtr())
}

// TestClusterLeafRow_VersionInfo 测试版本信息管理
func TestClusterLeafRow_VersionInfo(t *testing.T) {
	// 创建测试用的tuple
	tuple := createTestTuple()

	// 创建ClusterLeafRow
	content := make([]byte, 100)
	row := NewClusterLeafRow(content, tuple).(*ClusterLeafRow)

	// 测试单独设置
	testTrxID := uint64(100)
	testRollPtr := uint64(200)

	row.SetDBTrxID(testTrxID)
	row.SetDBRollPtr(testRollPtr)

	if got := row.GetDBTrxID(); got != testTrxID {
		t.Errorf("GetDBTrxID() = %d, want %d", got, testTrxID)
	}

	if got := row.GetDBRollPtr(); got != testRollPtr {
		t.Errorf("GetDBRollPtr() = %d, want %d", got, testRollPtr)
	}

	t.Logf("✅ Version info: TrxID=%d, RollPtr=%d", row.GetDBTrxID(), row.GetDBRollPtr())

	// 测试UpdateVersionInfo
	newTrxID := uint64(300)
	newRollPtr := uint64(400)

	row.UpdateVersionInfo(newTrxID, newRollPtr)

	gotTrxID, gotRollPtr := row.GetVersionInfo()
	if gotTrxID != newTrxID {
		t.Errorf("GetVersionInfo() TrxID = %d, want %d", gotTrxID, newTrxID)
	}
	if gotRollPtr != newRollPtr {
		t.Errorf("GetVersionInfo() RollPtr = %d, want %d", gotRollPtr, newRollPtr)
	}

	t.Logf("✅ Updated version info: TrxID=%d, RollPtr=%d", gotTrxID, gotRollPtr)
}

// TestClusterLeafRow_VersionChain 测试版本链场景
func TestClusterLeafRow_VersionChain(t *testing.T) {
	// 创建测试用的tuple
	tuple := createTestTuple()

	// 创建ClusterLeafRow
	content := make([]byte, 100)
	row := NewClusterLeafRow(content, tuple).(*ClusterLeafRow)

	// 模拟版本链：多个事务修改同一条记录
	versions := []struct {
		trxID   uint64
		rollPtr uint64
	}{
		{100, 1000}, // 第一个版本
		{200, 2000}, // 第二个版本
		{300, 3000}, // 第三个版本（最新）
	}

	// 模拟事务修改记录
	for i, v := range versions {
		row.UpdateVersionInfo(v.trxID, v.rollPtr)

		gotTrxID, gotRollPtr := row.GetVersionInfo()
		if gotTrxID != v.trxID {
			t.Errorf("Version %d: TrxID = %d, want %d", i, gotTrxID, v.trxID)
		}
		if gotRollPtr != v.rollPtr {
			t.Errorf("Version %d: RollPtr = %d, want %d", i, gotRollPtr, v.rollPtr)
		}

		t.Logf("✅ Version %d: TrxID=%d, RollPtr=%d", i+1, gotTrxID, gotRollPtr)
	}

	// 验证最终版本
	finalTrxID, finalRollPtr := row.GetVersionInfo()
	if finalTrxID != versions[len(versions)-1].trxID {
		t.Errorf("Final TrxID = %d, want %d", finalTrxID, versions[len(versions)-1].trxID)
	}
	if finalRollPtr != versions[len(versions)-1].rollPtr {
		t.Errorf("Final RollPtr = %d, want %d", finalRollPtr, versions[len(versions)-1].rollPtr)
	}

	t.Logf("✅ Final version: TrxID=%d, RollPtr=%d", finalTrxID, finalRollPtr)
}

// TestClusterLeafRow_DBRowID 测试行ID（无主键表）
func TestClusterLeafRow_DBRowID(t *testing.T) {
	// 创建测试用的tuple
	tuple := createTestTuple()

	// 创建ClusterLeafRow
	content := make([]byte, 100)
	row := NewClusterLeafRow(content, tuple).(*ClusterLeafRow)

	// 测试行ID（用于没有主键的表）
	testRowID := uint64(999)
	row.SetDBRowID(testRowID)

	if got := row.GetDBRowID(); got != testRowID {
		t.Errorf("GetDBRowID() = %d, want %d", got, testRowID)
	}

	t.Logf("✅ DB_ROW_ID: %d", row.GetDBRowID())
}

// TestClusterLeafRow_ZeroValues 测试零值
func TestClusterLeafRow_ZeroValues(t *testing.T) {
	// 创建测试用的tuple
	tuple := createTestTuple()

	// 创建ClusterLeafRow
	content := make([]byte, 100)
	row := NewClusterLeafRow(content, tuple).(*ClusterLeafRow)

	// 验证初始值为0
	if got := row.GetDBTrxID(); got != 0 {
		t.Errorf("Initial DBTrxID = %d, want 0", got)
	}

	if got := row.GetDBRollPtr(); got != 0 {
		t.Errorf("Initial DBRollPtr = %d, want 0", got)
	}

	if got := row.GetDBRowID(); got != 0 {
		t.Errorf("Initial DBRowID = %d, want 0", got)
	}

	t.Log("✅ All hidden columns initialized to 0")
}

// TestClusterLeafRow_LargeValues 测试大值
func TestClusterLeafRow_LargeValues(t *testing.T) {
	// 创建测试用的tuple
	tuple := createTestTuple()

	// 创建ClusterLeafRow
	content := make([]byte, 100)
	row := NewClusterLeafRow(content, tuple).(*ClusterLeafRow)

	// 测试大值（接近uint64最大值）
	largeTrxID := uint64(1<<48 - 1)   // 6字节最大值
	largeRollPtr := uint64(1<<56 - 1) // 7字节最大值
	largeRowID := uint64(1<<48 - 1)   // 6字节最大值

	row.SetDBTrxID(largeTrxID)
	row.SetDBRollPtr(largeRollPtr)
	row.SetDBRowID(largeRowID)

	if got := row.GetDBTrxID(); got != largeTrxID {
		t.Errorf("Large DBTrxID = %d, want %d", got, largeTrxID)
	}

	if got := row.GetDBRollPtr(); got != largeRollPtr {
		t.Errorf("Large DBRollPtr = %d, want %d", got, largeRollPtr)
	}

	if got := row.GetDBRowID(); got != largeRowID {
		t.Errorf("Large DBRowID = %d, want %d", got, largeRowID)
	}

	t.Logf("✅ Large values: TrxID=%d, RollPtr=%d, RowID=%d", largeTrxID, largeRollPtr, largeRowID)
}

// createTestTuple 创建测试用的tuple
func createTestTuple() metadata.RecordTableRowTuple {
	// 创建一个简单的mock tuple
	return &mockTuple{
		columnLength: 3,
	}
}

// mockTuple 模拟的tuple实现
type mockTuple struct {
	columnLength int
}

func (m *mockTuple) GetColumnLength() int {
	return m.columnLength
}

func (m *mockTuple) GetVarColumns() []metadata.RecordColumnInfo {
	return []metadata.RecordColumnInfo{
		{
			FieldType:   "VARCHAR",
			FieldLength: 100,
		},
	}
}

func (m *mockTuple) GetColumnInfos(index byte) metadata.RecordColumnInfo {
	return metadata.RecordColumnInfo{
		FieldType:   "VARCHAR",
		FieldLength: 100,
	}
}
