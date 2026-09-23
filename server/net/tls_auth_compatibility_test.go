package net

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	stdnet "net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSessionUsesTLSHonorsTransportAttribute(t *testing.T) {
	session := NewMockSession("tls")
	require.False(t, sessionUsesTLS(session))
	session.SetAttribute("tls_active", true)
	require.True(t, sessionUsesTLS(session))
	session.SetAttribute("tls_active", false)
	require.False(t, sessionUsesTLS(session))
}

func TestSessionHasClientCertificateHonorsCertificateAttribute(t *testing.T) {
	session := NewMockSession("x509")
	require.False(t, sessionHasClientCertificate(session))
	session.SetAttribute("tls_client_cert", true)
	require.True(t, sessionHasClientCertificate(session))
	session.SetAttribute("tls_client_cert", false)
	require.False(t, sessionHasClientCertificate(session))
}

func TestRecordTLSConnectionStateCopiesNegotiatedProperties(t *testing.T) {
	session := NewMockSession("tls-state")
	session.SetAttribute("tls_active", true)
	mysqlSession := NewMySQLServerSession(session)

	recordTLSConnectionState(session, &mysqlSession)

	require.True(t, mysqlSession.GetParamByName("tls_active").(bool))
	// A test transport without a *tls.Conn still records the authoritative
	// active/inactive state, while negotiated version and cipher remain absent.
	require.Nil(t, mysqlSession.GetParamByName("tls_version"))
	require.Nil(t, mysqlSession.GetParamByName("tls_cipher"))
}

type tlsStateMockSession struct {
	*MockSession
	conn stdnet.Conn
}

func (s *tlsStateMockSession) Conn() stdnet.Conn {
	return s.conn
}

func TestRecordTLSConnectionStateReadsNegotiatedTLSConnState(t *testing.T) {
	certificate := newCompatibilityTestCertificate(t)
	serverRaw, clientRaw := stdnet.Pipe()
	serverConn := tls.Server(serverRaw, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	clientConn := tls.Client(clientRaw, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, ServerName: "localhost"})
	defer serverConn.Close()
	defer clientConn.Close()

	serverHandshake := make(chan error, 1)
	go func() { serverHandshake <- serverConn.Handshake() }()
	require.NoError(t, clientConn.Handshake())
	require.NoError(t, <-serverHandshake)

	session := &tlsStateMockSession{MockSession: NewMockSession("tls-negotiated"), conn: serverConn}
	mysqlSession := NewMySQLServerSession(session)
	recordTLSConnectionState(session, &mysqlSession)

	require.Equal(t, "TLSv1.3", mysqlSession.GetParamByName("tls_version"))
	require.Equal(t, tls.CipherSuiteName(serverConn.ConnectionState().CipherSuite), mysqlSession.GetParamByName("tls_cipher"))
}

func newCompatibilityTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
	)
	require.NoError(t, err)
	return certificate
}
