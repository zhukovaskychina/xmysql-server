package net

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

const mysqlTLSServerConfigAttribute = "mysql_tls_server_config"

type mysqlBufferedNetConn struct {
	net.Conn
	buffered []byte
}

func (c *mysqlBufferedNetConn) Read(p []byte) (int, error) {
	if len(c.buffered) == 0 {
		return c.Conn.Read(p)
	}
	n := copy(p, c.buffered)
	c.buffered = c.buffered[n:]
	return n, nil
}

// buildMySQLServerTLSConfig creates the server-side TLS transport used after
// a MySQL SSLRequest. It intentionally does not wrap the listening socket:
// MySQL clients expect the initial protocol handshake to be sent in cleartext
// before upgrading the same connection to TLS.
func buildMySQLServerTLSConfig(cfg *conf.Cfg) (*tls.Config, error) {
	if cfg == nil || !cfg.TLSEnabled {
		return nil, nil
	}
	if cfg.TLSCertificateFile == "" || cfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("mysql TLS requires ssl-cert and ssl-key")
	}
	certificate, err := tls.LoadX509KeyPair(cfg.TLSCertificateFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load mysql TLS certificate: %w", err)
	}

	result := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}
	if cfg.TLSCAFile == "" {
		if cfg.TLSRequireClientCert {
			result.ClientAuth = tls.RequireAnyClientCert
		}
		return result, nil
	}

	caPEM, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read mysql TLS CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse mysql TLS CA: no certificates found")
	}
	result.ClientCAs = clientCAs
	if cfg.TLSRequireClientCert {
		result.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		result.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return result, nil
}

// isMySQLSSLRequest recognizes the fixed 32-byte protocol packet sent after
// the server handshake and before the TLS ClientHello.
func isMySQLSSLRequest(packet *MySQLPackage) bool {
	if packet == nil || len(packet.Body) != 32 {
		return false
	}
	capabilities := binary.LittleEndian.Uint32(packet.Body[:4])
	return capabilities&common.CLIENT_SSL != 0
}
