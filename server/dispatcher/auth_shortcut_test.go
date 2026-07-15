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
