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
