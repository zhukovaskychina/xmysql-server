package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

type noPrivilegeEngineAccess struct{}

func (noPrivilegeEngineAccess) QueryUser(ctx context.Context, user, host string) (*UserInfo, error) {
	return &UserInfo{
		User:               user,
		Host:               host,
		DatabasePrivileges: make(map[string][]common.PrivilegeType),
		TablePrivileges:    make(map[string]map[string][]common.PrivilegeType),
	}, nil
}

func (noPrivilegeEngineAccess) QueryDatabase(ctx context.Context, database string) (*DatabaseInfo, error) {
	return &DatabaseInfo{Name: database, Exists: true}, nil
}

func (noPrivilegeEngineAccess) QueryUserPrivileges(ctx context.Context, user, host string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func (noPrivilegeEngineAccess) QueryDatabasePrivileges(ctx context.Context, user, host, database string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func (noPrivilegeEngineAccess) QueryTablePrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func TestRootWithoutExplicitPrivilegeIsDenied(t *testing.T) {
	service := NewAuthService(nil, noPrivilegeEngineAccess{})

	err := service.CheckPrivilege(context.Background(), "root", "localhost", "mysql", "user", common.SelectPriv)
	if err == nil {
		t.Fatalf("root without explicit privilege was allowed")
	}
	if got := err.Error(); !strings.Contains(got, "lacks") || !strings.Contains(got, "Select privilege") {
		t.Fatalf("CheckPrivilege() error = %q, want missing privilege denial", got)
	}
}

var _ EngineAccess = noPrivilegeEngineAccess{}
