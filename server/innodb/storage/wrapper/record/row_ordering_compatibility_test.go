package record

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestClusterLeafRowLessUsesPrimaryKey(t *testing.T) {
	tuple := createTestTuple()
	newRow := func(key []byte) *ClusterLeafRow {
		return &ClusterLeafRow{
			header: NewClusterLeafRowHeader(tuple),
			value: &ClusterLeafRowData{
				Content:   make([]byte, 5),
				RowValues: []basic.Value{basic.NewBytes(key)},
			},
		}
	}

	low := newRow([]byte("a"))
	high := newRow([]byte("b"))
	equal := newRow([]byte("a"))
	if !low.Less(high) {
		t.Fatal("lower primary key should sort before higher primary key")
	}
	if high.Less(low) || low.Less(equal) {
		t.Fatal("primary-key ordering is not strict")
	}
}

type primaryKeyTuple struct {
	metadata.RecordTableRowTuple
	tableMeta *metadata.TableMeta
}

func (t *primaryKeyTuple) GetTableMeta() *metadata.TableMeta {
	return t.tableMeta
}

func newCompositePrimaryKeyTuple() metadata.RecordTableRowTuple {
	base := &mockTuple{columnLength: 3}
	tableMeta := metadata.CreateTableMeta("composite_pk")
	tableMeta.AddColumn(&metadata.ColumnMeta{Name: "tenant_id", Type: metadata.TypeInt, Length: 8, IsPrimary: true})
	tableMeta.AddColumn(&metadata.ColumnMeta{Name: "code", Type: metadata.TypeVarchar, Length: 32, IsPrimary: true})
	tableMeta.AddColumn(&metadata.ColumnMeta{Name: "payload", Type: metadata.TypeVarchar, Length: 32})
	tableMeta.PrimaryKey = []string{"tenant_id", "code"}
	return &primaryKeyTuple{RecordTableRowTuple: base, tableMeta: tableMeta}
}

func TestClusterLeafRowLessUsesCompositePrimaryKeyOrder(t *testing.T) {
	tuple := newCompositePrimaryKeyTuple()
	newRow := func(tenant int64, code string) *ClusterLeafRow {
		return &ClusterLeafRow{
			FrmMeta: tuple,
			header:  NewClusterLeafRowHeader(tuple),
			value: &ClusterLeafRowData{
				Content: make([]byte, 5),
				RowValues: []basic.Value{
					basic.NewInt64Value(tenant),
					basic.NewStringValue(code),
					basic.NewStringValue("payload"),
				},
			},
		}
	}

	if !newRow(1, "a").Less(newRow(1, "b")) {
		t.Fatal("composite primary-key comparison must use the second key column")
	}
	if !newRow(1, "z").Less(newRow(2, "a")) {
		t.Fatal("composite primary-key comparison must preserve the first key column precedence")
	}
	if newRow(1, "b").Less(newRow(1, "a")) || newRow(1, "a").Less(newRow(1, "a")) {
		t.Fatal("composite primary-key ordering must be strict")
	}
}
