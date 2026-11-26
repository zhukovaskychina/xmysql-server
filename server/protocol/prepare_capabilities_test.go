package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestPrepareResponseHonorsNegotiatedEOFDeprecation(t *testing.T) {
	manager := NewPreparedStatementManager()
	stmt, err := manager.Prepare("SELECT ? AS body")
	require.NoError(t, err)
	stmt.ColumnCount = 1
	stmt.Columns = []*ColumnMetadata{{Name: "body", Type: common.COLUMN_TYPE_LONG_BLOB, Charset: 63}}
	legacy := EncodePrepareResponse(stmt, 7)
	require.Len(t, legacy, 5)
	require.Equal(t, byte(0xfe), legacy[2][4])
	require.Equal(t, byte(0xfe), legacy[4][4])
	modern := EncodePrepareResponseWithCapabilities(stmt, 7, common.CLIENT_DEPRECATE_EOF)
	require.Len(t, modern, 3, "PREPARE has no metadata terminators when EOF is deprecated")
	for i, packet := range modern {
		require.Equal(t, byte(7+i), packet[3])
		if i != 0 {
			require.NotEqual(t, byte(0xfe), packet[4])
		}
	}
}
