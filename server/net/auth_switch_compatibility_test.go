package net

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

func TestEncodeAuthSwitchRequestForCachingSHA2(t *testing.T) {
	challenge := []byte("12345678901234567890")
	packet := encodeAuthSwitchRequest("caching_sha2_password", challenge, 2)
	require.Greater(t, len(packet), 4)
	require.Equal(t, byte(2), packet[3])
	payload := packet[4:]
	require.Equal(t, byte(0xfe), payload[0])
	require.Equal(t, []byte("caching_sha2_password"), payload[1:1+len("caching_sha2_password")])
	pluginEnd := 1 + len("caching_sha2_password")
	require.Equal(t, byte(0), payload[pluginEnd])
	require.True(t, bytes.Equal(challenge, payload[pluginEnd+1:pluginEnd+1+len(challenge)]))
	require.Equal(t, byte(0), payload[len(payload)-1])
}

type authSwitchTestService struct {
	user *auth.UserInfo
}

func (s *authSwitchTestService) AuthenticateUser(context.Context, string, string, string, string) (*auth.AuthResult, error) {
	return &auth.AuthResult{Success: true}, nil
}
func (s *authSwitchTestService) ValidateDatabase(context.Context, string) error { return nil }
func (s *authSwitchTestService) CheckPrivilege(context.Context, string, string, string, string, common.PrivilegeType) error {
	return nil
}
func (s *authSwitchTestService) GetUserInfo(context.Context, string, string) (*auth.UserInfo, error) {
	return s.user, nil
}
func (s *authSwitchTestService) FlushPrivileges(context.Context) error { return nil }
func (s *authSwitchTestService) GenerateChallenge(string) ([]byte, error) {
	return []byte("12345678901234567890"), nil
}
func (s *authSwitchTestService) GetChallenge(string) []byte {
	return []byte("12345678901234567890")
}

func TestCachingSHA2AccountUsesAuthSwitchAndAcceptsFastResponse(t *testing.T) {
	password := "secret"
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	user := &auth.UserInfo{User: "root", Host: "localhost", AuthPlugin: "caching_sha2_password", Password: fmt.Sprintf("%x", stage2[:])}
	handler := &DecoupledMySQLMessageHandler{authService: &authSwitchTestService{user: user}, cfg: &conf.Cfg{}, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("auth-switch")
	challenge := []byte("12345678901234567890")
	session.SetAttribute("auth_challenge", challenge)
	mysqlSession := NewMySQLServerSession(session)
	flags := uint32(common.CLIENT_PROTOCOL_41 | common.CLIENT_SECURE_CONNECTION)
	body := make([]byte, 9)
	binary.LittleEndian.PutUint32(body[0:4], flags)
	// max packet size and character set/reserved bytes
	binary.LittleEndian.PutUint32(body[4:8], 16*1024*1024)
	body[8] = 0x21
	body = append(body, make([]byte, 23)...)
	body = append(body, []byte("root")...)
	body = append(body, 0)
	body = append(body, byte(20))
	body = append(body, make([]byte, 20)...)
	first := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 1}, Body: body}
	firstErr := handler.handleAuthentication(session, &mysqlSession, first)
	require.NoError(t, firstErr)
	require.NotNil(t, session.GetAttribute("auth_switch_pending"))
	require.Len(t, session.written, 1)

	maskHash := sha256.Sum256(append(challenge, stage2[:]...))
	response := make([]byte, 32)
	for i := range response {
		response[i] = stage1[i] ^ maskHash[i]
	}
	second := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 3}, Body: response}
	require.NoError(t, handler.handleAuthentication(session, &mysqlSession, second))
	require.Equal(t, "success", session.GetAttribute("auth_status"))
	require.Len(t, session.written, 2)
}

func TestChangeUserCachingSHA2UsesAuthSwitchAndResetsSession(t *testing.T) {
	password := "secret"
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	user := &auth.UserInfo{User: "new_user", Host: "localhost", AuthPlugin: "caching_sha2_password", Password: fmt.Sprintf("%x", stage2[:])}
	handler := &DecoupledMySQLMessageHandler{
		authService: &authSwitchTestService{user: user},
		cfg:         &conf.Cfg{},
		sessionMap:  make(map[Session]server.MySQLServerSession),
	}
	session := NewMockSession("change-user-auth-switch")
	session.SetAttribute("auth_status", "success")
	session.SetAttribute("auth_challenge", []byte("12345678901234567890"))
	session.SetAttribute("client_capabilities", uint32(common.CLIENT_PROTOCOL_41|common.CLIENT_SECURE_CONNECTION|common.CLIENT_PLUGIN_AUTH|common.CLIENT_MULTI_STATEMENTS))
	current := NewMySQLServerSession(session)
	current.SetParamByName("user", "old_user")
	current.SetParamByName("autocommit", "0")
	if _, err := handler.preparedStmtMgrFromSession(session).Prepare("select 1"); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	body := []byte{common.COM_CHANGE_USER}
	body = append(body, []byte("new_user\x00")...)
	body = append(body, byte(20))
	body = append(body, make([]byte, 20)...)
	body = append(body, []byte("new_db\x00")...)
	body = append(body, 0x21, 0x00)
	body = append(body, []byte("caching_sha2_password\x00")...)
	if err := handler.handleComChangeUser(session, &current, &MySQLPackage{Header: MySQLPkgHeader{PacketId: 0}, Body: body}); err != nil {
		t.Fatalf("COM_CHANGE_USER auth-switch request failed: %v", err)
	}
	if session.GetAttribute("auth_switch_pending") == nil {
		t.Fatal("expected caching_sha2_password AuthSwitchRequest state")
	}

	maskHash := sha256.Sum256(append([]byte("12345678901234567890"), stage2[:]...))
	response := make([]byte, 32)
	for i := range response {
		response[i] = stage1[i] ^ maskHash[i]
	}
	handler.sessionMap[session] = current
	if err := handler.handlePacket(session, &current, &MySQLPackage{Header: MySQLPkgHeader{PacketId: 2}, Body: response}); err != nil {
		t.Fatalf("COM_CHANGE_USER auth-switch response failed: %v", err)
	}
	if session.GetAttribute("auth_switch_pending") != nil {
		t.Fatal("AuthSwitchRequest state was not cleared")
	}
	if got := current.GetParamByName("user"); got != "new_user" {
		t.Fatalf("user after auth switch = %v, want new_user", got)
	}
	if got := current.GetParamByName("database"); got != "new_db" {
		t.Fatalf("database after auth switch = %v, want new_db", got)
	}
	if got := current.GetParamByName("autocommit"); got != "1" {
		t.Fatalf("autocommit after auth switch = %v, want 1", got)
	}
	for _, name := range []string{"character_set_client", "character_set_connection", "character_set_results"} {
		if got := current.GetParamByName(name); got != "utf8" {
			t.Fatalf("%s after auth switch = %v, want utf8", name, got)
		}
	}
	if got := current.GetParamByName("collation_connection"); got != "utf8_general_ci" {
		t.Fatalf("collation_connection after auth switch = %v, want utf8_general_ci", got)
	}
	if got := handler.preparedStmtMgrFromSession(session).Count(); got != 0 {
		t.Fatalf("prepared statements survived auth-switch change user: %d", got)
	}
	if len(session.written) != 2 || session.written[1][3] != 3 || session.written[1][4] != 0x00 {
		t.Fatalf("auth-switch completion response = %v, want OK sequence 3", session.written)
	}
}

func TestCachingSHA2FullAuthAcceptsCleartextOnlyOverTLS(t *testing.T) {
	password := "secret"
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	user := &auth.UserInfo{User: "root", Host: "localhost", AuthPlugin: "caching_sha2_password", Password: fmt.Sprintf("%x", stage2[:]), DefaultRoles: []string{"role_reader"}}
	handler := &DecoupledMySQLMessageHandler{authService: &authSwitchTestService{user: user}, cfg: &conf.Cfg{}, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("auth-full")
	session.SetAttribute("tls_active", true)
	mysqlSession := NewMySQLServerSession(session)
	pending := &authSwitchState{Username: "root", Host: "localhost", Database: "mysql", Challenge: []byte("12345678901234567890")}
	body := append([]byte{0}, []byte(password)...)
	body = append(body, 0)

	require.NoError(t, handler.handleAuthSwitchResponse(session, &mysqlSession, &MySQLPackage{Body: body}, pending))
	require.Equal(t, "success", session.GetAttribute("auth_status"))
	require.Equal(t, []string{"role_reader"}, mysqlSession.GetParamByName("active_roles"))
}

func TestCachingSHA2FullAuthRejectsCleartextWithoutTLS(t *testing.T) {
	password := "secret"
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	user := &auth.UserInfo{User: "root", Host: "localhost", AuthPlugin: "caching_sha2_password", Password: fmt.Sprintf("%x", stage2[:])}
	handler := &DecoupledMySQLMessageHandler{authService: &authSwitchTestService{user: user}, cfg: &conf.Cfg{}, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("auth-full-insecure")
	pending := &authSwitchState{Username: "root", Host: "localhost", Database: "mysql", Challenge: []byte("12345678901234567890")}
	body := append([]byte{0}, []byte(password)...)
	body = append(body, 0)

	require.NoError(t, handler.handleAuthSwitchResponse(session, nil, &MySQLPackage{Body: body}, pending))
	require.NotEqual(t, "success", session.GetAttribute("auth_status"))
}

func TestCachingSHA2FullAuthSupportsRSAKeyExchangeWithoutTLS(t *testing.T) {
	password := "secret"
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	user := &auth.UserInfo{User: "root", Host: "localhost", AuthPlugin: "caching_sha2_password", Password: fmt.Sprintf("%x", stage2[:])}
	handler := &DecoupledMySQLMessageHandler{authService: &authSwitchTestService{user: user}, cfg: &conf.Cfg{}, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("auth-rsa")
	mysqlSession := NewMySQLServerSession(session)
	pending := &authSwitchState{Username: "root", Host: "localhost", Database: "mysql", Challenge: []byte("12345678901234567890")}

	key, err := cachingSHA2RSAKeyForSession(session)
	require.NoError(t, err)
	cleartext := append([]byte(password), 0)
	for index := range cleartext {
		cleartext[index] ^= pending.Challenge[index%len(pending.Challenge)]
	}
	encrypted, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, &key.PublicKey, cleartext, nil)
	require.NoError(t, err)
	require.NoError(t, handler.handleAuthSwitchResponse(session, &mysqlSession, &MySQLPackage{Body: encrypted}, pending))
	require.Equal(t, "success", session.GetAttribute("auth_status"))
}

func TestSHA256PasswordFullAuthAcceptsCleartextOverTLS(t *testing.T) {
	validator := auth.NewSHA256PasswordValidator()
	hash, err := validator.HashPassword("secret")
	require.NoError(t, err)
	user := &auth.UserInfo{User: "sha_user", Host: "localhost", AuthPlugin: "sha256_password", Password: hash}
	handler := &DecoupledMySQLMessageHandler{authService: &authSwitchTestService{user: user}, cfg: &conf.Cfg{}, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("sha256-full")
	session.SetAttribute("tls_active", true)
	mysqlSession := NewMySQLServerSession(session)
	pending := &authSwitchState{Username: "sha_user", Host: "localhost", Database: "mysql", Challenge: []byte("12345678901234567890")}
	payload := append([]byte("secret"), 0)
	require.NoError(t, handler.handleAuthSwitchResponse(session, &mysqlSession, &MySQLPackage{Header: MySQLPkgHeader{PacketId: 2}, Body: payload}, pending))
	require.Equal(t, "success", session.GetAttribute("auth_status"))
}

func TestSHA256PasswordFullAuthSupportsRSAKeyExchangeWithoutTLS(t *testing.T) {
	validator := auth.NewSHA256PasswordValidator()
	hash, err := validator.HashPassword("secret")
	require.NoError(t, err)
	user := &auth.UserInfo{User: "sha_user", Host: "localhost", AuthPlugin: "sha256_password", Password: hash}
	handler := &DecoupledMySQLMessageHandler{authService: &authSwitchTestService{user: user}, cfg: &conf.Cfg{}, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("sha256-rsa")
	pending := &authSwitchState{Username: "sha_user", Host: "localhost", Database: "mysql", Challenge: []byte("12345678901234567890")}

	key, err := cachingSHA2RSAKeyForSession(session)
	require.NoError(t, err)
	cleartext := append([]byte("secret"), 0)
	for index := range cleartext {
		cleartext[index] ^= pending.Challenge[index%len(pending.Challenge)]
	}
	encrypted, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, &key.PublicKey, cleartext, nil)
	require.NoError(t, err)
	require.NoError(t, handler.handleAuthSwitchResponse(session, nil, &MySQLPackage{Header: MySQLPkgHeader{PacketId: 2}, Body: encrypted}, pending))
	require.Equal(t, "success", session.GetAttribute("auth_status"))
}

func TestChangeUserSHA256PasswordRequestsAuthSwitch(t *testing.T) {
	validator := auth.NewSHA256PasswordValidator()
	hash, err := validator.HashPassword("secret")
	require.NoError(t, err)
	user := &auth.UserInfo{User: "sha_user", Host: "localhost", AuthPlugin: "sha256_password", Password: hash}
	handler := &DecoupledMySQLMessageHandler{
		authService: &authSwitchTestService{user: user},
		cfg:         &conf.Cfg{},
		sessionMap:  make(map[Session]server.MySQLServerSession),
	}
	session := NewMockSession("sha256-change-user")
	session.SetAttribute("auth_challenge", []byte("12345678901234567890"))
	session.SetAttribute("client_capabilities", uint32(common.CLIENT_PROTOCOL_41|common.CLIENT_SECURE_CONNECTION|common.CLIENT_PLUGIN_AUTH))
	current := NewMySQLServerSession(session)
	body := append([]byte{common.COM_CHANGE_USER}, []byte("sha_user\x00")...)
	body = append(body, byte(20))
	body = append(body, make([]byte, 20)...)
	body = append(body, []byte("mysql\x00")...)
	body = append(body, 0x21, 0x00)
	body = append(body, []byte("sha256_password\x00")...)
	require.NoError(t, handler.handleComChangeUser(session, &current, &MySQLPackage{Body: body}))
	require.NotNil(t, session.GetAttribute("auth_switch_pending"))
	payload := session.written[0][4:]
	require.Equal(t, byte(0xfe), payload[0])
	require.Contains(t, string(payload), "sha256_password")
}
