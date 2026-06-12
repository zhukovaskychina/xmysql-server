package page

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
)

func TestBasePageWrapperPinAndStats(t *testing.T) {
	t.Parallel()

	p := NewBasePageWrapper(100, 1, common.FIL_PAGE_INDEX)
	require.NotNil(t, p)
	require.Equal(t, basic.PageStateClean, p.GetState())

	p.Pin()
	p.Pin()
	require.EqualValues(t, 2, p.GetPinCount())
	require.Equal(t, basic.PageStatePinned, p.GetState())

	p.Unpin()
	require.EqualValues(t, 1, p.GetPinCount())
	require.Equal(t, basic.PageStatePinned, p.GetState())

	p.Unpin()
	require.EqualValues(t, 0, p.GetPinCount())
	require.Equal(t, basic.PageStateLoaded, p.GetState())

	stats := p.GetStats()
	require.NotNil(t, stats)
	require.EqualValues(t, 2, stats.PinCount)

	p.MarkDirty()
	require.True(t, p.IsDirty())
	require.Equal(t, basic.PageStateDirty, p.GetState())

	p.ClearDirty()
	require.False(t, p.IsDirty())
}

func TestRollbackPageWrapperParseFromBytes(t *testing.T) {
	t.Parallel()

	base := NewBasePageWrapper(1, 2, common.FIL_PAGE_UNDO_LOG)
	serialized, err := base.ToBytes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(serialized), int(common.PageSize))

	data := make([]byte, common.PageSize)
	copy(data, serialized[:common.PageSize])

	const (
		trxRsegMaxSizeSize       = 4
		trxRsegHistorySizeSize   = 4
		trxRsegHistoryOffsetSize = 20
		trxRsegFsegHeaderSize    = 10
	)

	offset := pages.FileHeaderSize
	binary.LittleEndian.PutUint32(data[offset:], 100)
	offset += 4
	binary.LittleEndian.PutUint32(data[offset:], 5)
	offset += trxRsegHistorySizeSize

	copy(data[offset:offset+trxRsegHistoryOffsetSize], []byte("abcdefghijklmnopqrst"))
	offset += trxRsegHistoryOffsetSize

	copy(data[offset:offset+trxRsegFsegHeaderSize], []byte("qwertyuio1"))
	offset += trxRsegFsegHeaderSize

	// Undo slots: set first slot to known value
	binary.LittleEndian.PutUint32(data[offset:], 201)
	offset += 4
	copy(data[offset:], []byte("empty-space-sentinel"))

	for i := 0; i < 4; i++ {
		binary.LittleEndian.PutUint32(data[offset+(i*4):], uint32(201+i))
	}
	copy(data[pages.FileHeaderSize+4+4+20+10+rollbackPageUndoSlotsFieldSize:], []byte("empty-space-sentinel"))

	rpw := NewRollbackPageWrapper(1, 2)
	require.NoError(t, rpw.ParseFromBytes(data))

	require.EqualValues(t, 100, binary.LittleEndian.Uint32(rpw.GetTrxRsegMaxSize()))
	require.EqualValues(t, 5, binary.LittleEndian.Uint32(rpw.GetTrxRsegHistorySize()))
	require.True(t, bytes.Equal([]byte("abcdefghijklmnopqrst"), rpw.GetTrxRsegHistory()[:len("abcdefghijklmnopqrst")]))
	require.True(t, bytes.Equal([]byte("qwertyuio1"), rpw.GetTrxRsegFsegHeader()[:len("qwertyuio1")]))
	require.EqualValues(t, 201, binary.LittleEndian.Uint32(rpw.GetTrxRsegUndoSlots()[:4]))
	require.Equal(t, []byte("empty-space-sentinel"), rpw.GetEmptySpace()[:len("empty-space-sentinel")])
	require.NoError(t, rpw.Validate())
}

func TestRollbackPageWrapperRejectShortBuffer(t *testing.T) {
	t.Parallel()

	rpw := NewRollbackPageWrapper(1, 2)
	err := rpw.ParseFromBytes(make([]byte, common.PageSize-1))
	require.ErrorIs(t, err, ErrInvalidRollbackData)
}
