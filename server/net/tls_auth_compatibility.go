package net

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/auth"
)

// enforceAccountTLS applies REQUIRE SSL/TLS account metadata at the network
// authentication boundary. Password validation alone cannot enforce this
// MySQL contract because it has no transport context.
func (h *DecoupledMySQLMessageHandler) enforceAccountTLS(ctx context.Context, session Session, username, host string) error {
	if h == nil || h.authService == nil {
		return nil
	}
	userInfo, err := h.authService.GetUserInfo(ctx, username, host)
	if err != nil || userInfo == nil || !userInfo.TLSRequired {
		return nil
	}
	if sessionUsesTLS(session) {
		if !userInfo.X509Required || sessionHasClientCertificate(session) {
			return nil
		}
		return fmt.Errorf("Access denied for user '%s'@'%s': a client certificate is required", username, host)
	}
	return fmt.Errorf("Access denied for user '%s'@'%s': secure transport is required", username, host)
}

func sessionUsesTLS(session Session) bool {
	if session == nil {
		return false
	}
	if value, ok := session.GetAttribute("tls_active").(bool); ok {
		return value
	}
	connection := session.Conn()
	tlsConnection, ok := connection.(*tls.Conn)
	if !ok || tlsConnection == nil {
		return false
	}
	return tlsConnection.ConnectionState().HandshakeComplete
}

func sessionHasClientCertificate(session Session) bool {
	if session == nil {
		return false
	}
	if value, ok := session.GetAttribute("tls_client_cert").(bool); ok {
		return value
	}
	connection, ok := session.Conn().(*tls.Conn)
	if !ok || connection == nil {
		return false
	}
	return len(connection.ConnectionState().PeerCertificates) > 0
}

// recordTLSConnectionState copies the negotiated transport properties into
// the SQL session. The engine uses these values for
// performance_schema.tls_channel_status; they are populated only from the
// actual connection or the transport test hook used by compatibility tests.
func recordTLSConnectionState(session Session, currentMysqlSession *server.MySQLServerSession) {
	if session == nil {
		return
	}
	active := sessionUsesTLS(session)
	session.SetAttribute("tls_active", active)
	if currentMysqlSession != nil && *currentMysqlSession != nil {
		(*currentMysqlSession).SetParamByName("tls_active", active)
	}
	if !active {
		return
	}
	connection, ok := session.Conn().(*tls.Conn)
	if !ok || connection == nil {
		return
	}
	state := connection.ConnectionState()
	version := tlsVersionName(state.Version)
	if version != "" {
		session.SetAttribute("tls_version", version)
		if currentMysqlSession != nil && *currentMysqlSession != nil {
			(*currentMysqlSession).SetParamByName("tls_version", version)
		}
	}
	cipher := tls.CipherSuiteName(state.CipherSuite)
	if cipher != "" {
		session.SetAttribute("tls_cipher", cipher)
		if currentMysqlSession != nil && *currentMysqlSession != nil {
			(*currentMysqlSession).SetParamByName("tls_cipher", cipher)
		}
	}
}

func tlsVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLSv1"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS13:
		return "TLSv1.3"
	default:
		return ""
	}
}

// authenticateCachingSHA2FullAuth accepts the cleartext password form of
// caching_sha2_password only after TLS has been established. RSA key exchange
// is deliberately not faked here: when TLS is absent the caller returns a
// stable authentication error instead of accepting plaintext on the wire.
func (h *DecoupledMySQLMessageHandler) authenticateCachingSHA2FullAuth(ctx context.Context, session Session, pending *authSwitchState, payload []byte) (*auth.AuthResult, bool, error) {
	if h == nil || h.authService == nil || pending == nil {
		return nil, false, nil
	}
	userInfo, err := h.authService.GetUserInfo(ctx, pending.Username, pending.Host)
	if err != nil || userInfo == nil || !strings.EqualFold(userInfo.AuthPlugin, "caching_sha2_password") || len(payload) == 32 {
		return nil, false, nil
	}
	if !sessionUsesTLS(session) {
		privateKey, keyErr := cachingSHA2RSAKeyForSession(session)
		if keyErr != nil {
			return &auth.AuthResult{Success: false, ErrorCode: 1045, ErrorMessage: "caching_sha2_password RSA key is unavailable"}, true, nil
		}
		decrypted, decryptErr := rsa.DecryptOAEP(sha1.New(), rand.Reader, privateKey, payload, nil)
		if decryptErr != nil {
			return &auth.AuthResult{Success: false, ErrorCode: 1045, ErrorMessage: "caching_sha2_password RSA authentication failed"}, true, nil
		}
		for index := range decrypted {
			decrypted[index] ^= pending.Challenge[index%len(pending.Challenge)]
		}
		payload = decrypted
	}
	if len(payload) > 0 && payload[0] == 0 {
		payload = payload[1:]
	}
	password := strings.TrimSuffix(string(payload), "\x00")
	validator := &auth.CachingSHA2PasswordValidator{}
	if !validator.ValidatePassword(password, userInfo.Password, pending.Challenge) {
		return &auth.AuthResult{Success: false, ErrorCode: 1045, ErrorMessage: "Access denied: invalid caching_sha2_password full authentication"}, true, nil
	}
	return &auth.AuthResult{Success: true, User: pending.Username, Host: pending.Host, Database: pending.Database, ActiveRoles: append([]string(nil), userInfo.DefaultRoles...)}, true, nil
}

// authenticateSHA256PasswordFullAuth handles the sha256_password AuthSwitch
// exchange. The plugin accepts a cleartext password over TLS and an RSA-OAEP
// encrypted, scramble-masked password without TLS.
func (h *DecoupledMySQLMessageHandler) authenticateSHA256PasswordFullAuth(ctx context.Context, session Session, pending *authSwitchState, payload []byte) (*auth.AuthResult, bool, error) {
	if h == nil || h.authService == nil || pending == nil {
		return nil, false, nil
	}
	userInfo, err := h.authService.GetUserInfo(ctx, pending.Username, pending.Host)
	if err != nil || userInfo == nil || !strings.EqualFold(userInfo.AuthPlugin, "sha256_password") {
		return nil, false, nil
	}
	if !sessionUsesTLS(session) {
		privateKey, keyErr := cachingSHA2RSAKeyForSession(session)
		if keyErr != nil {
			return &auth.AuthResult{Success: false, ErrorCode: 1045, ErrorMessage: "sha256_password RSA key is unavailable"}, true, nil
		}
		decrypted, decryptErr := rsa.DecryptOAEP(sha1.New(), rand.Reader, privateKey, payload, nil)
		if decryptErr != nil {
			return &auth.AuthResult{Success: false, ErrorCode: 1045, ErrorMessage: "sha256_password RSA authentication failed"}, true, nil
		}
		for index := range decrypted {
			decrypted[index] ^= pending.Challenge[index%len(pending.Challenge)]
		}
		payload = decrypted
	}
	if len(payload) > 0 && payload[0] == 0 {
		payload = payload[1:]
	}
	password := strings.TrimSuffix(string(payload), "\x00")
	validator := &auth.SHA256PasswordValidator{}
	if !validator.ValidatePassword(password, userInfo.Password, pending.Challenge) {
		return &auth.AuthResult{Success: false, ErrorCode: 1045, ErrorMessage: "Access denied: invalid sha256_password full authentication"}, true, nil
	}
	return &auth.AuthResult{Success: true, User: pending.Username, Host: pending.Host, Database: pending.Database, ActiveRoles: append([]string(nil), userInfo.DefaultRoles...)}, true, nil
}

// handleCachingSHA2PublicKeyRequest implements the non-TLS caching_sha2_password
// public-key request (payload 0x02). The key is scoped to the session so the
// subsequent encrypted password can be decrypted without sharing a process key.
func (h *DecoupledMySQLMessageHandler) handleCachingSHA2PublicKeyRequest(session Session, packet *MySQLPackage) (bool, error) {
	if packet == nil || len(packet.Body) != 1 || packet.Body[0] != 0x02 {
		return false, nil
	}
	publicKey, err := cachingSHA2PublicKeyForSession(session)
	if err != nil {
		return true, err
	}
	return true, session.WriteBytes(h.createMySQLPacket(publicKey, packet.Header.PacketId+1))
}

func cachingSHA2RSAKeyForSession(session Session) (*rsa.PrivateKey, error) {
	if session == nil {
		return nil, fmt.Errorf("session is nil")
	}
	if key, ok := session.GetAttribute("caching_sha2_rsa_private_key").(*rsa.PrivateKey); ok && key != nil {
		return key, nil
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	session.SetAttribute("caching_sha2_rsa_private_key", key)
	return key, nil
}

func cachingSHA2PublicKeyForSession(session Session) ([]byte, error) {
	key, err := cachingSHA2RSAKeyForSession(session)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}
