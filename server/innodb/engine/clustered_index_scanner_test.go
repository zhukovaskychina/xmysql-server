package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestClusteredIndexScannerDecodesClusteredRecords(t *testing.T) {
	tableMeta := clusteredScannerTestTableMeta()
	rows := []*InsertRowData{
		{ColumnValues: map[string]interface{}{"id": int64(1), "name": "alice", "age": int64(30)}},
		{ColumnValues: map[string]interface{}{"id": int64(2), "name": "bob", "age": int64(40)}},
	}
	btree := &fakeClusteredScannerBTree{rows: encodeClusteredScannerRows(t, tableMeta, rows)}

	scanner := NewClusteredIndexScanner(btree, tableMeta)
	got, err := scanner.Scan(context.Background(), nil)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Scan() row count = %d, want 2", len(got))
	}
	if got[0].ColumnValues["name"] != "alice" || got[1].ColumnValues["name"] != "bob" {
		t.Fatalf("Scan() decoded rows = %#v", got)
	}
}

func TestClusteredIndexScannerAppliesWhereConditions(t *testing.T) {
	tableMeta := clusteredScannerTestTableMeta()
	rows := []*InsertRowData{
		{ColumnValues: map[string]interface{}{"id": int64(1), "name": "alice", "age": int64(30)}},
		{ColumnValues: map[string]interface{}{"id": int64(2), "name": "bob", "age": int64(40)}},
	}
	btree := &fakeClusteredScannerBTree{rows: encodeClusteredScannerRows(t, tableMeta, rows)}

	scanner := NewClusteredIndexScanner(btree, tableMeta)
	got, err := scanner.Scan(context.Background(), []string{"name = 'bob'"})
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Scan() row count = %d, want 1", len(got))
	}
	if got[0].ColumnValues["id"] != int64(2) {
		t.Fatalf("Scan() matched id = %#v, want int64(2)", got[0].ColumnValues["id"])
	}
}

type fakeClusteredScannerBTree struct {
	rows []basic.Row
}

func (f *fakeClusteredScannerBTree) Init(context.Context, uint32, uint32) error { return nil }
func (f *fakeClusteredScannerBTree) GetAllLeafPages(context.Context) ([]uint32, error) {
	return nil, nil
}
func (f *fakeClusteredScannerBTree) Search(context.Context, interface{}) (uint32, int, error) {
	return 0, 0, nil
}
func (f *fakeClusteredScannerBTree) Insert(context.Context, interface{}, []byte) error { return nil }
func (f *fakeClusteredScannerBTree) Delete(context.Context, interface{}) error         { return nil }
func (f *fakeClusteredScannerBTree) RangeSearch(context.Context, interface{}, interface{}) ([]basic.Row, error) {
	return f.rows, nil
}
func (f *fakeClusteredScannerBTree) GetFirstLeafPage(context.Context) (uint32, error) { return 0, nil }

type fakeClusteredScannerRow struct {
	data []byte
}

func (r fakeClusteredScannerRow) Less(basic.Row) bool                     { return false }
func (r fakeClusteredScannerRow) ToByte() []byte                          { return append([]byte(nil), r.data...) }
func (r fakeClusteredScannerRow) IsInfimumRow() bool                      { return false }
func (r fakeClusteredScannerRow) IsSupremumRow() bool                     { return false }
func (r fakeClusteredScannerRow) GetPageNumber() uint32                   { return 0 }
func (r fakeClusteredScannerRow) WriteWithNull([]byte)                    {}
func (r fakeClusteredScannerRow) WriteBytesWithNullWithsPos([]byte, byte) {}
func (r fakeClusteredScannerRow) GetRowLength() uint16                    { return uint16(len(r.data)) }
func (r fakeClusteredScannerRow) GetHeaderLength() uint16                 { return 0 }
func (r fakeClusteredScannerRow) GetPrimaryKey() basic.Value              { return basic.NewNull() }
func (r fakeClusteredScannerRow) GetFieldLength() int                     { return 1 }
func (r fakeClusteredScannerRow) ReadValueByIndex(int) basic.Value        { return basic.NewBytes(r.data) }
func (r fakeClusteredScannerRow) SetNOwned(byte)                          {}
func (r fakeClusteredScannerRow) GetNOwned() byte                         { return 0 }
func (r fakeClusteredScannerRow) GetNextRowOffset() uint16                { return 0 }
func (r fakeClusteredScannerRow) SetNextRowOffset(uint16)                 {}
func (r fakeClusteredScannerRow) GetHeapNo() uint16                       { return 0 }
func (r fakeClusteredScannerRow) SetHeapNo(uint16)                        {}
func (r fakeClusteredScannerRow) SetTransactionId(uint64)                 {}
func (r fakeClusteredScannerRow) GetValueByColName(string) basic.Value    { return basic.NewBytes(r.data) }
func (r fakeClusteredScannerRow) ToString() string                        { return string(r.data) }

func encodeClusteredScannerRows(t *testing.T, tableMeta *metadata.TableMeta, rows []*InsertRowData) []basic.Row {
	t.Helper()
	encoded := make([]basic.Row, 0, len(rows))
	for _, row := range rows {
		data, err := EncodeClusteredRecord(row, tableMeta)
		if err != nil {
			t.Fatalf("EncodeClusteredRecord() error = %v", err)
		}
		encoded = append(encoded, fakeClusteredScannerRow{data: data})
	}
	return encoded
}

func clusteredScannerTestTableMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsPrimary: true},
			{Name: "name", Type: metadata.TypeVarchar},
			{Name: "age", Type: metadata.TypeInt},
		},
		PrimaryKey: []string{"id"},
	}
}
