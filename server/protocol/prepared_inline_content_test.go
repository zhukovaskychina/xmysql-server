package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

func TestPreparedInlineContentKeepsBinaryPayloadOutOfBoundSQL(t *testing.T) {
	manager := NewPreparedStatementManager()
	statement, err := manager.Prepare("insert into docs values (?, CONTENT_FROM_BYTES(?, 'application/octet-stream'), ?)")
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{0x00, 0xff, '\\', '\'', 0x89}, 240000)
	params := []interface{}{int64(1), PreparedBinaryParam(payload), int64(31415)}
	values, err := statement.BindInlineContentParameters(params, nil)
	require.NoError(t, err)
	boundSQL := BindPreparedSQL(statement.SQL, params)
	require.Less(t, len(boundSQL), 256, "SQL parsing must not scale with the binary CONTENT payload")
	require.Contains(t, boundSQL, "CONTENT_PARAM(1)")
	require.Contains(t, boundSQL, "31415")
	require.Equal(t, payload, values[1].Bytes)
	payload[0] = 1
	require.Equal(t, byte(0), values[1].Bytes[0], "execution parameter must own its bytes")
}

func TestPreparedInlineContentPreservesOtherParametersAndExistingValues(t *testing.T) {
	manager := NewPreparedStatementManager()
	stmt, err := manager.Prepare("insert into docs values (?, CONTENT_FROM_BYTES(?, 'text/plain'), ?)")
	require.NoError(t, err)
	for _, input := range []interface{}{nil, int64(42)} {
		params := []interface{}{PreparedBinaryParam("ordinary blob"), input, int64(9)}
		values, err := stmt.BindInlineContentParameters(params, nil)
		require.NoError(t, err)
		require.Empty(t, values)
		require.Equal(t, input, params[1])
		require.Equal(t, PreparedBinaryParam("ordinary blob"), params[0])
	}
	params := []interface{}{PreparedBinaryParam("ordinary blob"), []byte{}, int64(9)}
	values, err := stmt.BindInlineContentParameters(params, nil)
	require.NoError(t, err)
	require.Contains(t, values, uint16(1), "empty content is distinct from NULL")
	require.Equal(t, uint64(0), values[1].Size())

	existing, err := content.NewValue("text/plain", 7, []byte("already streamed"))
	require.NoError(t, err)
	params[1] = PreparedContentParam(1)
	values, err = stmt.BindInlineContentParameters(params, map[uint16]content.Value{1: existing})
	require.NoError(t, err)
	require.Equal(t, existing, values[1], "existing streamed values must not be rebound")
}
