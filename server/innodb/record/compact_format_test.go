package record

import (
	"testing"
)

// TestCompactRowFormat_EncodeDecodeRoundtrip STG-015: Compact 行格式编码/解码往返
func TestCompactRowFormat_EncodeDecodeRoundtrip(t *testing.T) {
	columns := []*ColumnDef{
		{Name: "id", Type: TypeInt, Length: 8, IsNullable: false, IsVarLen: false},
		{Name: "name", Type: TypeVarchar, Length: 0, IsNullable: true, IsVarLen: true},
	}
	crf := NewCompactRowFormat(columns)

	values := []interface{}{int64(1), "abc"}
	data, err := crf.EncodeRow(values, 100, 200)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	if len(data) < RecordHeaderSize+HiddenColumnSize {
		t.Errorf("编码数据过短: %d", len(data))
	}

	row, err := crf.DecodeRow(data)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if row.Header == nil || row.Hidden == nil {
		t.Error("解码后应包含 Header 与 Hidden")
	}
	if len(row.ColumnValues) != 2 {
		t.Fatalf("解码列数应为 2, got %d", len(row.ColumnValues))
	}
	// 第一列定长 8 字节
	if len(row.ColumnValues[0]) != 8 {
		t.Errorf("id 列长度应为 8, got %d", len(row.ColumnValues[0]))
	}
	// 第二列变长 "abc"
	if row.ColumnValues[1] == nil || string(row.ColumnValues[1]) != "abc" {
		t.Errorf("name 列应为 \"abc\", got %v", row.ColumnValues[1])
	}
	t.Log("Compact 行格式编码/解码往返 - 通过")
}
