package page

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

func TestAllocatedPageMaintainsInMemoryIOStateAndStats(t *testing.T) {
	page := NewAllocatedPageByBytes(9, 12).(*Allocated)
	page.MarkDirty()
	require.NoError(t, page.Write())
	require.False(t, page.IsDirty())
	require.Equal(t, basic.PageStateFlushed, page.GetState())
	require.EqualValues(t, 1, page.GetStats().WriteCount)

	serialized, err := page.ToBytes()
	require.NoError(t, err)
	require.Len(t, serialized, common.PageSize)

	loaded := &Allocated{}
	require.NoError(t, loaded.ParseFromBytes(serialized))
	require.NoError(t, loaded.Read())
	require.EqualValues(t, 1, loaded.GetStats().ReadCount)
	require.EqualValues(t, 9, loaded.GetSpaceID())
	require.EqualValues(t, 12, loaded.GetPageNo())
}
