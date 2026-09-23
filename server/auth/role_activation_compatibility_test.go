package auth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
)

type roleActivationSession struct {
	mu     sync.RWMutex
	params map[string]interface{}
	ctx    *server.SessionContext
}

func newRoleActivationSession() *roleActivationSession {
	return &roleActivationSession{params: map[string]interface{}{}, ctx: server.NewSessionContext("role-activation")}
}

func (s *roleActivationSession) GetSessionId() string                   { return "role-activation" }
func (s *roleActivationSession) GetLastActiveTime() time.Time           { return time.Now() }
func (s *roleActivationSession) SessionContext() *server.SessionContext { return s.ctx }
func (s *roleActivationSession) SetParamByName(name string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params[name] = value
}
func (s *roleActivationSession) GetParamByName(name string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.params[name]
}
func (s *roleActivationSession) SendOK()                                 {}
func (s *roleActivationSession) SendHandleOk()                           {}
func (s *roleActivationSession) SendSelectFields()                       {}
func (s *roleActivationSession) SendError(uint16, string)                {}
func (s *roleActivationSession) SendResultSet([]string, [][]interface{}) {}

func TestSetRoleControlsEffectivePrivilegesAndDefaultRole(t *testing.T) {
	dataDir := t.TempDir()
	executor := engine.NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newRoleActivationSession()
	// Provision accounts as an administrative session. Once the grants are
	// established, switch the same session to bob to exercise role activation.
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	exec := func(query string) {
		result := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, result.Err, query)
	}
	exec("create role 'report_reader'@'localhost'")
	exec("create role 'audit_reader'@'localhost'")
	exec("create user 'bob'@'localhost' identified by 'secret'")
	exec("grant select on app.users to 'report_reader'@'localhost'")
	exec("grant select on app.users to 'audit_reader'@'localhost'")
	exec("grant 'report_reader'@'localhost' to 'bob'@'localhost'")
	exec("grant 'audit_reader'@'localhost' to 'bob'@'localhost'")
	session.SetParamByName("user", "bob")

	access := NewInnoDBEngineAccess(&conf.Cfg{DataDir: dataDir}, executor)
	service := NewAuthService(&conf.Cfg{DataDir: dataDir}, access)

	exec("set role none")
	roles, ok := session.GetParamByName("active_roles").([]string)
	require.True(t, ok)
	require.Empty(t, roles)
	noneCtx := server.WithActiveRoles(context.Background(), roles)
	require.Error(t, service.CheckPrivilege(noneCtx, "bob", "localhost", "app", "users", common.SelectPriv))

	exec("set role 'report_reader'@'localhost'")
	roles, ok = session.GetParamByName("active_roles").([]string)
	require.True(t, ok)
	activeCtx := server.WithActiveRoles(context.Background(), roles)
	require.NoError(t, service.CheckPrivilege(activeCtx, "bob", "localhost", "app", "users", common.SelectPriv))

	exec("set role all except 'audit_reader'@'localhost'")
	roles, ok = session.GetParamByName("active_roles").([]string)
	require.True(t, ok)
	require.Equal(t, []string{"report_reader@localhost"}, roles)

	exec("set default role 'report_reader'@'localhost' to 'bob'@'localhost'")
	exec("set role default")
	roles, ok = session.GetParamByName("active_roles").([]string)
	require.True(t, ok)
	require.Equal(t, []string{"report_reader@localhost"}, roles)
	session.SetParamByName("user", "root")
	exec("revoke 'report_reader'@'localhost' from 'bob'@'localhost'")
	session.SetParamByName("user", "bob")
	result := <-executor.ExecuteQuery(session, "set role 'report_reader'@'localhost'", "")
	require.Error(t, result.Err)
}

func TestDynamicPrivilegeFollowsActiveRole(t *testing.T) {
	dataDir := t.TempDir()
	executor := engine.NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newRoleActivationSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	exec := func(query string) {
		result := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, result.Err, query)
	}
	exec("create role 'backup_operator'@'localhost'")
	exec("create user 'bob'@'localhost' identified by 'secret'")
	exec("grant backup_admin on *.* to 'backup_operator'@'localhost'")
	exec("grant 'backup_operator'@'localhost' to 'bob'@'localhost'")

	access := NewInnoDBEngineAccess(&conf.Cfg{DataDir: dataDir}, executor)
	service := NewAuthService(&conf.Cfg{DataDir: dataDir}, access).(*AuthServiceImpl)
	withoutRole := server.WithActiveRoles(context.Background(), nil)
	require.Error(t, service.CheckDynamicPrivilege(withoutRole, "bob", "localhost", "BACKUP_ADMIN"))
	withRole := server.WithActiveRoles(context.Background(), []string{"backup_operator@localhost"})
	require.NoError(t, service.CheckDynamicPrivilege(withRole, "bob", "localhost", "BACKUP_ADMIN"))
}

func TestMandatoryRolesActivateOnLoginWhenAllRoleActivationIsEnabled(t *testing.T) {
	dataDir := t.TempDir()
	executor := engine.NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newRoleActivationSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	session.SetParamByName("dynamic_privileges", []string{"SYSTEM_VARIABLES_ADMIN", "ROLE_ADMIN"})
	exec := func(query string) {
		result := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, result.Err, query)
	}
	exec("create role 'mandatory_reader'@'localhost'")
	exec("create user 'mandatory_user'@'localhost' identified by 'secret'")
	exec("grant select on app.users to 'mandatory_reader'@'localhost'")
	exec("set global mandatory_roles = 'mandatory_reader@localhost'")
	exec("set global activate_all_roles_on_login = 'ON'")

	access := NewInnoDBEngineAccess(&conf.Cfg{DataDir: dataDir}, executor)
	service := NewAuthService(&conf.Cfg{DataDir: dataDir}, access)
	result, err := service.AuthenticateUser(context.Background(), "mandatory_user", "secret", "localhost", "")
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Equal(t, []string{"mandatory_reader@localhost"}, result.ActiveRoles)
}

func TestInnoDBEngineAccessResolvesPersistedProxyGrant(t *testing.T) {
	dataDir := t.TempDir()
	executor := engine.NewXMySQLEngine(&conf.Cfg{DataDir: dataDir, InnodbDataDir: dataDir, InnodbBufferPoolSize: 16 * 1024 * 1024, InnodbPageSize: 16384})
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	session := newRoleActivationSession()
	session.SetParamByName("user", "root")
	session.SetParamByName("host", "localhost")
	for _, query := range []string{
		"create user 'proxied'@'localhost' identified by 'secret'",
		"create user 'proxy_user'@'localhost' identified by 'secret'",
		"grant proxy on 'proxied'@'localhost' to 'proxy_user'@'localhost'",
	} {
		result := <-executor.ExecuteQuery(session, query, "")
		require.NoError(t, result.Err, query)
	}

	access := NewInnoDBEngineAccess(&conf.Cfg{DataDir: dataDir}, executor)
	service := NewAuthService(&conf.Cfg{DataDir: dataDir}, access).(*AuthServiceImpl)
	user, host, found, err := service.ResolveProxyUser(context.Background(), "proxy_user", "localhost")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "proxied", user)
	require.Equal(t, "localhost", host)
}
