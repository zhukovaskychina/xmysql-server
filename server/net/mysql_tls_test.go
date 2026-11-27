package net

import (
	"crypto/tls"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestIsMySQLSSLRequest(t *testing.T) {
	packet := &MySQLPackage{Body: make([]byte, 32)}
	binary.LittleEndian.PutUint32(packet.Body[:4], common.CLIENT_SSL|common.CLIENT_PROTOCOL_41)
	require.True(t, isMySQLSSLRequest(packet))

	packet.Body = make([]byte, 31)
	require.False(t, isMySQLSSLRequest(packet))
	packet.Body = make([]byte, 32)
	binary.LittleEndian.PutUint32(packet.Body[:4], common.CLIENT_PROTOCOL_41)
	require.False(t, isMySQLSSLRequest(packet))
}

func TestMysqlTCPConnUpgradeTLS(t *testing.T) {
	certificate := newCompatibilityTestCertificate(t)
	serverRaw, clientRaw := net.Pipe()
	serverConn := newMySQLTCPConn(serverRaw)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serverConn.upgradeTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	}()

	clientConn := tls.Client(clientRaw, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
	require.NoError(t, clientConn.Handshake())
	require.NoError(t, <-serverErr)
	require.IsType(t, &tls.Conn{}, serverConn.conn)
	require.NotZero(t, serverConn.conn.(*tls.Conn).ConnectionState().Version)
	_ = clientConn.Close()
	_ = serverConn.conn.Close()
}

func TestBuildMySQLServerTLSConfigRequiresCertificate(t *testing.T) {
	_, err := buildMySQLServerTLSConfig(&conf.Cfg{TLSEnabled: true})
	require.Error(t, err)
}

type mysqlTLSPacketListener struct {
	packets chan interface{}
}

func (l *mysqlTLSPacketListener) OnOpen(Session) error   { return nil }
func (l *mysqlTLSPacketListener) OnClose(Session)        {}
func (l *mysqlTLSPacketListener) OnError(Session, error) {}
func (l *mysqlTLSPacketListener) OnCron(Session)         {}
func (l *mysqlTLSPacketListener) OnMessage(_ Session, packet interface{}) {
	l.packets <- packet
}

func TestSessionHandleTCPPackageUpgradesAfterSSLRequest(t *testing.T) {
	certificate := newCompatibilityTestCertificate(t)
	serverRaw, clientRaw := net.Pipe()
	serverConn := newMySQLTCPConn(serverRaw)
	session := newSession(osThreadTestEndpoint{}, serverConn)
	session.SetPkgHandler(NewMySQLPkgHandler())
	listener := &mysqlTLSPacketListener{packets: make(chan interface{}, 2)}
	session.SetEventListener(listener)
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	session.SetAttribute(mysqlTLSServerConfigAttribute, tlsConfig)

	serverDone := make(chan error, 1)
	go func() { serverDone <- session.handleTCPPackage() }()

	sslRequest := make([]byte, 36)
	sslRequest[0] = 32
	sslRequest[3] = 1
	binary.LittleEndian.PutUint32(sslRequest[4:8], common.CLIENT_SSL|common.CLIENT_PROTOCOL_41)
	_, err := clientRaw.Write(sslRequest)
	require.NoError(t, err)

	clientTLS := tls.Client(clientRaw, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
	require.NoError(t, clientTLS.Handshake())
	normalPacket := []byte{1, 0, 0, 2, 0}
	_, err = clientTLS.Write(normalPacket)
	require.NoError(t, err)

	first := <-listener.packets
	second := <-listener.packets
	require.True(t, isMySQLSSLRequest(first.(*MySQLPackage)))
	require.Equal(t, []byte{0}, second.(*MySQLPackage).Body)
	require.True(t, sessionUsesTLS(session))

	_ = clientTLS.Close()
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not stop after TLS client close")
	}
}
