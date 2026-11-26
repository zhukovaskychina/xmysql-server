package net

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

func TestBinaryColumnDefinitionsPreserveTypeAndCharset(t *testing.T) {
	encoder := protocol.NewMySQLResultSetEncoder()
	for name, expected := range map[string]byte{
		"binary": common.COLUMN_TYPE_STRING, "varbinary": common.COLUMN_TYPE_VAR_STRING,
		"tinyblob": common.COLUMN_TYPE_TINY_BLOB, "blob": common.COLUMN_TYPE_BLOB,
		"mediumblob": common.COLUMN_TYPE_MEDIUM_BLOB, "longblob": common.COLUMN_TYPE_LONG_BLOB,
		"CONTENT": common.COLUMN_TYPE_LONG_BLOB, "VECTOR(2)": common.COLUMN_TYPE_LONG_BLOB,
	} {
		t.Run(name, func(t *testing.T) {
			definition := createColumnDefinitionForType(encoder, "body", name)
			require.Equal(t, expected, definition.ColumnType)
			require.Equal(t, uint16(63), definition.CharacterSet)
			require.NotZero(t, definition.Flags&uint16(common.BinaryFlag))
		})
	}
	text := createColumnDefinitionForType(encoder, "label", "varchar(50)")
	require.Equal(t, uint16(33), text.CharacterSet)
	require.Zero(t, text.Flags&uint16(common.BinaryFlag))
}

func TestBinaryPreparedResultMetadataTerminatorUsesCapabilities(t *testing.T) {
	for _, deprecate := range []bool{false, true} {
		session := NewMockSession("binary-result-metadata")
		if deprecate {
			session.SetAttribute("client_capabilities", uint32(common.CLIENT_DEPRECATE_EOF))
		}
		handler := &DecoupledMySQLMessageHandler{resultSetEncoder: protocol.NewMySQLResultSetEncoder()}
		result := &protocol.MessageQueryResult{Columns: []string{"body"}, ColumnTypes: []string{"content"}, Rows: [][]interface{}{{[]byte{0, 255}}}}
		require.NoError(t, handler.sendBinaryPreparedResultSet(session, result, 1))
		rowIndex := 3
		if deprecate {
			require.Len(t, session.written, 4)
			rowIndex = 2
		} else {
			require.Len(t, session.written, 5)
			require.Equal(t, byte(0xfe), session.written[2][4])
		}
		require.Equal(t, []byte{0, 0, 2, 0, 255}, session.written[rowIndex][4:], "binary row must follow the applicable metadata section")
		for index, packet := range session.written {
			require.Equal(t, byte(index+1), packet[3])
		}
	}
}
