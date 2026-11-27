package dispatcher

import (
	"context"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

type denyAuthService struct{}

func (denyAuthService) AuthenticateUser(ctx context.Context, user, password, host, database string) (*auth.AuthResult, error) {
	return &auth.AuthResult{Success: false, ErrorCode: common.ER_ACCESS_DENIED_ERROR}, nil
}

func (denyAuthService) ValidateDatabase(ctx context.Context, database string) error {
	return nil
}

func (denyAuthService) CheckPrivilege(ctx context.Context, user, host, database, table string, privilege common.PrivilegeType) error {
	return fmt.Errorf("denied %s for %s@%s", privilege.String(), user, host)
}

func (denyAuthService) GetUserInfo(ctx context.Context, user, host string) (*auth.UserInfo, error) {
	return nil, fmt.Errorf("user denied")
}

func (denyAuthService) FlushPrivileges(ctx context.Context) error {
	return nil
}

func (denyAuthService) GenerateChallenge(sessionID string) ([]byte, error) {
	return []byte("12345678901234567890"), nil
}

func (denyAuthService) GetChallenge(sessionID string) []byte {
	return []byte("12345678901234567890")
}

type allowAuthService struct{ denyAuthService }

func (allowAuthService) AuthenticateUser(ctx context.Context, user, password, host, database string) (*auth.AuthResult, error) {
	return &auth.AuthResult{
		Success:     true,
		User:        user,
		Host:        host,
		Database:    database,
		ActiveRoles: []string{"app_reader"},
	}, nil
}

func TestAuthenticatedSessionIdentityIsBoundToOpaqueSessionID(t *testing.T) {
	handler := &EnhancedBusinessMessageHandler{authService: allowAuthService{}}
	msg := &protocol.AuthMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_AUTH_REQUEST, "opaque-42", nil),
		User:        "app",
		Password:    "secret",
		Database:    "test",
	}
	if _, err := handler.handleAuthMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleAuthMessage() error = %v", err)
	}
	identity, ok := handler.authenticatedSession("opaque-42")
	if !ok {
		t.Fatal("authenticated session identity was not persisted")
	}
	if identity.user != "app" || identity.host == "" || identity.database != "test" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if len(identity.activeRoles) != 1 || identity.activeRoles[0] != "app_reader" {
		t.Fatalf("unexpected active roles: %v", identity.activeRoles)
	}
}

func TestMysqlUserQueryDoesNotBypassPrivilegeCheck(t *testing.T) {
	handler := &EnhancedBusinessMessageHandler{authService: denyAuthService{}}
	msg := &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "app@127.0.0.1", nil),
		SQL:         "SELECT User, Host FROM mysql.user",
		Database:    "mysql",
	}

	response, err := handler.HandleMessage(msg)
	if err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if response.Type() != protocol.MSG_ERROR {
		t.Fatalf("mysql.user query response type = %v, want MSG_ERROR", response.Type())
	}
}

func TestRootMysqlUserQueryDoesNotBypassDeniedPrivilege(t *testing.T) {
	handler := &EnhancedBusinessMessageHandler{authService: denyAuthService{}}

	err := handler.checkQueryPrivilege(context.Background(), "root", "localhost", "mysql", "SELECT * FROM mysql.user")
	if err == nil {
		t.Fatalf("root mysql.user query bypassed denied privilege")
	}
}

type recordingAuthService struct {
	denyAuthService
	deny      common.PrivilegeType
	denyTable string
	seen      []common.PrivilegeType
	tables    []string
	checks    []privilegeCheck
}

type privilegeCheck struct {
	table     string
	privilege common.PrivilegeType
}

func (s *recordingAuthService) CheckPrivilege(_ context.Context, _ string, _ string, _ string, table string, privilege common.PrivilegeType) error {
	s.seen = append(s.seen, privilege)
	s.tables = append(s.tables, table)
	s.checks = append(s.checks, privilegeCheck{table: table, privilege: privilege})
	if s.denyTable != "" && table == s.denyTable {
		return fmt.Errorf("denied table %s", table)
	}
	if privilege == s.deny {
		return fmt.Errorf("denied %s", privilege.String())
	}
	return nil
}

func TestCheckQueryPrivilegePassesReferencedTableToAuthService(t *testing.T) {
	authService := &recordingAuthService{}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	if err := handler.checkQueryPrivilege(context.Background(), "app", "127.0.0.1", "test", "SELECT * FROM orders"); err != nil {
		t.Fatalf("checkQueryPrivilege() error = %v", err)
	}
	if len(authService.tables) != 1 || authService.tables[0] != "orders" {
		t.Fatalf("auth service received tables %v, want [orders]", authService.tables)
	}
}

func TestCheckRequiredPrivilegesDoesNotStopAtFirstPrivilege(t *testing.T) {
	authService := &recordingAuthService{deny: common.UpdatePriv}
	handler := &EnhancedBusinessMessageHandler{authService: authService}

	err := handler.checkRequiredPrivileges(context.Background(), "app", "127.0.0.1", "test", "", []common.PrivilegeType{
		common.SelectPriv,
		common.UpdatePriv,
	})
	if err == nil {
		t.Fatal("checkRequiredPrivileges() accepted a denied required privilege")
	}
	if len(authService.seen) != 2 {
		t.Fatalf("checked %d privileges, want all 2 required privileges", len(authService.seen))
	}
}

func TestExtractUserFromSessionIDDoesNotDefaultToRoot(t *testing.T) {
	handler := &EnhancedBusinessMessageHandler{}
	if user := handler.extractUserFromSessionID("opaque-session-id"); user != "" {
		t.Fatalf("extractUserFromSessionID() = %q, want empty unauthenticated identity", user)
	}
}
