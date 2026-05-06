package record

import (
	"testing"
)

// TestDynamicRowFormat_NoOverflow STG-016: Dynamic 行格式无溢出路径（走 Compact 编码）
func TestDynamicRowFormat_NoOverflow(t *testing.T) {
	columns := []*ColumnDef{
		{Name: "id", Type: TypeInt, Length: 8, IsNullable: false, IsVarLen: false},
		{Name: "v", Type: TypeBigInt, Length: 8, IsNullable: false, IsVarLen: false},
	}
	// nil blobManager 时仅测试无溢出路径（无 BLOB/TEXT 列则 shouldOverflow 为 false）
	drf := NewDynamicRowFormat(columns, nil)
	values := []interface{}{int64(1), int64(2)}

	data, err := drf.EncodeRow(values, 100, 200, 1)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("EncodeRow returned empty")
	}

	row, err := drf.DecodeRow(data, false)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if row == nil || row.CompactRow == nil {
		t.Fatal("DecodeRow returned nil row")
	}
	if len(row.ColumnValues) != 2 {
		t.Errorf("expected 2 columns, got %d", len(row.ColumnValues))
	}
	t.Log("Dynamic row format (no overflow path) encode/decode - passed")
}
