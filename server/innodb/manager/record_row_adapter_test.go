package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	pagepkg "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
)

func TestRecordRowAdapter_WriteWithNull(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("a")},
	}

	adapter.WriteWithNull([]byte("bc"))

	assert.Equal(t, []byte{'a', 'b', 'c', 0}, adapter.record.Data)
}

func TestRecordRowAdapter_WriteBytesWithNullWithsPos(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("x")},
	}

	adapter.WriteBytesWithNullWithsPos([]byte("yz"), 0)

	assert.Equal(t, []byte{'x', 'y', 'z', 0}, adapter.record.Data)
}

func TestRecordRowAdapter_GetPrimaryKey(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("pk")},
	}

	pk := adapter.GetPrimaryKey()

	assert.Equal(t, []byte("pk"), pk.Raw())
}

func TestRecordRowAdapter_ReadValueByIndex(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("v")},
	}

	assert.Equal(t, []byte("v"), adapter.ReadValueByIndex(0).Raw())
	assert.True(t, adapter.ReadValueByIndex(1).IsNull())
}

func TestRecordRowAdapter_GetValueByColName(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("val")},
	}

	assert.Equal(t, []byte("val"), adapter.GetValueByColName("data").Raw())
	assert.Equal(t, []byte("val"), adapter.GetValueByColName("value").Raw())
	assert.Equal(t, []byte("val"), adapter.GetValueByColName("DATA").Raw())
	assert.True(t, adapter.GetValueByColName("unknown").Compare(basic.NewNull()) == 0)
}

func TestRecordRowAdapter_SetAndGetNOwned(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("v")},
	}

	adapter.SetNOwned(7)

	assert.Equal(t, byte(7), adapter.GetNOwned())
}

func TestRecordRowAdapter_SetAndGetNextRowOffset(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("v")},
	}

	adapter.SetNextRowOffset(128)

	assert.Equal(t, uint16(128), adapter.GetNextRowOffset())
}

func TestRecordRowAdapter_SetAndGetHeapNo(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("v")},
	}

	adapter.SetHeapNo(9)

	assert.Equal(t, uint16(9), adapter.GetHeapNo())
}

func TestRecordRowAdapter_SetTransactionId(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("v")},
	}

	adapter.SetTransactionId(12345)

	assert.Equal(t, uint64(12345), adapter.transactionID)
}

func TestRecordRowAdapter_GetPageNumber(t *testing.T) {
	adapter := &RecordRowAdapter{
		record:     &pagepkg.Record{Data: []byte("v")},
		pageNumber: 42,
	}

	assert.Equal(t, uint32(42), adapter.GetPageNumber())
}

func TestRecordRowAdapter_Less(t *testing.T) {
	adapter := &RecordRowAdapter{
		record: &pagepkg.Record{Data: []byte("a")},
	}

	assert.True(t, adapter.Less(basic.NewRow([]byte("b"))))
	assert.False(t, adapter.Less(basic.NewRow([]byte("a"))))
	assert.False(t, adapter.Less(basic.NewRow([]byte("0"))))
}
