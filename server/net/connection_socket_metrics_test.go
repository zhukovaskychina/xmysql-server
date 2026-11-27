package net

import (
	stdnet "net"
	"testing"

	"github.com/stretchr/testify/require"
	metrics "github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

func TestMysqlTCPConnRecordsSocketTrafficAtTransportBoundary(t *testing.T) {
	serverRaw, clientRaw := stdnet.Pipe()
	defer serverRaw.Close()
	defer clientRaw.Close()

	conn := newMySQLTCPConn(serverRaw)
	threadID := int64(conn.ID())

	requestWrite := make(chan error, 1)
	go func() {
		_, writeErr := clientRaw.Write([]byte("request"))
		requestWrite <- writeErr
	}()
	readBuffer := make([]byte, 32)
	read, err := conn.recv(readBuffer)
	require.NoError(t, err)
	require.NoError(t, <-requestWrite)
	require.Equal(t, "request", string(readBuffer[:read]))

	writeResult := make(chan []byte, 1)
	go func() {
		buffer := make([]byte, 32)
		n, readErr := clientRaw.Read(buffer)
		if readErr != nil {
			writeResult <- nil
			return
		}
		writeResult <- buffer[:n]
	}()
	_, err = conn.send([]byte("response"))
	require.NoError(t, err)
	require.Equal(t, "response", string(<-writeResult))

	var observed *metrics.SocketSummaryRow
	for _, row := range metrics.DefaultRuntimeRecorder().SocketSummary() {
		if row.ThreadID == threadID {
			copy := row
			observed = &copy
			break
		}
	}
	require.NotNil(t, observed)
	require.Equal(t, int64(1), observed.CountRead)
	require.Equal(t, int64(len("request")), observed.BytesRead)
	require.Equal(t, int64(1), observed.CountWrite)
	require.Equal(t, int64(len("response")), observed.BytesWrite)
	require.Positive(t, observed.SumTimerRead)
	require.Positive(t, observed.SumTimerWrite)
}

func TestMysqlTCPConnCompressNoneKeepsMySQLPacketsUnwrapped(t *testing.T) {
	serverRaw, clientRaw := stdnet.Pipe()
	defer serverRaw.Close()
	defer clientRaw.Close()

	conn := newMySQLTCPConn(serverRaw)
	conn.SetCompressType(CompressNone)

	requestWrite := make(chan error, 1)
	go func() {
		_, writeErr := clientRaw.Write([]byte{0x58, 0x00, 0x00, 0x00})
		requestWrite <- writeErr
	}()
	readBuffer := make([]byte, 4)
	read, err := conn.recv(readBuffer)
	require.NoError(t, err)
	require.NoError(t, <-requestWrite)
	require.Equal(t, []byte{0x58, 0x00, 0x00, 0x00}, readBuffer[:read])
}
