package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestSecondaryIndexInternalRow_SetTransactionIdNoop(t *testing.T) {
	meta := metadata.NewDefaultTableRow(&metadata.TableMeta{Name: "idx", Columns: []*metadata.ColumnMeta{}})
	row := NewSecondaryIndexInternalRow(meta, basic.NewStringValue("a"), 10)
	origin := append([]byte(nil), row.ToByte()...)

	row.SetTransactionId(12345)

	assert.Equal(t, origin, row.ToByte())
}

func TestSecondaryIndexLeafRow_SetTransactionIdNoop(t *testing.T) {
	meta := metadata.NewDefaultTableRow(&metadata.TableMeta{Name: "idx", Columns: []*metadata.ColumnMeta{}})
	row := NewSecondaryIndexLeafRow(meta, []basic.Value{basic.NewStringValue("idx")}, []basic.Value{basic.NewStringValue("pk")})
	origin := append([]byte(nil), row.ToByte()...)

	row.SetTransactionId(12345)

	assert.Equal(t, origin, row.ToByte())
}

type secondaryIndexPageResolver struct {
	page uint32
	seen []basic.Value
}

func (r *secondaryIndexPageResolver) GetPageNumberByPrimaryKey(keys []basic.Value) (uint32, bool) {
	r.seen = append([]basic.Value(nil), keys...)
	return r.page, true
}

func TestSecondaryIndexLeafRowResolvesPageThroughPrimaryKey(t *testing.T) {
	meta := metadata.NewDefaultTableRow(&metadata.TableMeta{Name: "idx", Columns: []*metadata.ColumnMeta{}})
	row := NewSecondaryIndexLeafRow(meta, []basic.Value{basic.NewStringValue("idx")}, []basic.Value{basic.NewStringValue("pk")}).(*SecondaryIndexLeafRow)
	resolver := &secondaryIndexPageResolver{page: 73}
	row.SetPageResolver(resolver)

	if got := row.GetPageNumber(); got != 73 {
		t.Fatalf("GetPageNumber() = %d, want 73", got)
	}
	if len(resolver.seen) != 1 || resolver.seen[0].ToString() != "pk" {
		t.Fatalf("resolver received unexpected primary key: %#v", resolver.seen)
	}
}
