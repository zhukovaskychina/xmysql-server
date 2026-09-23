package auth

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestPrivilegeFailureMatrixCoversUserHostAndScopes(t *testing.T) {
	ctx := context.Background()
	service := NewAuthService(nil, noPrivilegeEngineAccess{}).(*AuthServiceImpl)

	cases := []struct {
		name string
		call func() error
	}{
		{name: "user-host-global", call: func() error {
			return service.CheckPrivilege(ctx, "alice", "remote.example", "", "", common.SelectPriv)
		}},
		{name: "database", call: func() error {
			return service.CheckPrivilege(ctx, "alice", "localhost", "app", "", common.SelectPriv)
		}},
		{name: "table", call: func() error {
			return service.CheckPrivilege(ctx, "alice", "localhost", "app", "users", common.SelectPriv)
		}},
		{name: "column", call: func() error {
			return service.CheckColumnPrivilege(ctx, "alice", "localhost", "app", "users", "email", common.SelectPriv)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatalf("privilege check unexpectedly succeeded for %s", tc.name)
			}
		})
	}
}

type hostRestrictedEngineAccess struct{}

func (hostRestrictedEngineAccess) QueryUser(ctx context.Context, user, host string) (*UserInfo, error) {
	if host != "localhost" {
		return nil, context.Canceled
	}
	return &UserInfo{User: user, Host: host, DatabasePrivileges: map[string][]common.PrivilegeType{}, TablePrivileges: map[string]map[string][]common.PrivilegeType{}}, nil
}

func (hostRestrictedEngineAccess) QueryDatabase(context.Context, string) (*DatabaseInfo, error) {
	return &DatabaseInfo{Exists: true}, nil
}

func (hostRestrictedEngineAccess) QueryUserPrivileges(context.Context, string, string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func (hostRestrictedEngineAccess) QueryDatabasePrivileges(context.Context, string, string, string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func (hostRestrictedEngineAccess) QueryTablePrivileges(context.Context, string, string, string, string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func TestAuthenticationRejectsHostMismatchBeforePrivilegeFallback(t *testing.T) {
	service := NewAuthService(nil, hostRestrictedEngineAccess{})
	result, err := service.AuthenticateUser(context.Background(), "alice", "secret", "remote.example", "")
	if err != nil {
		t.Fatalf("AuthenticateUser returned unexpected error: %v", err)
	}
	if result.Success || result.ErrorCode != common.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("host mismatch was accepted: %+v", result)
	}
}

type mutablePrivilegeEngineAccess struct {
	privileges []common.PrivilegeType
}

func (a *mutablePrivilegeEngineAccess) QueryUser(context.Context, string, string) (*UserInfo, error) {
	return &UserInfo{DatabasePrivileges: map[string][]common.PrivilegeType{}, TablePrivileges: map[string]map[string][]common.PrivilegeType{}}, nil
}

func (a *mutablePrivilegeEngineAccess) QueryDatabase(context.Context, string) (*DatabaseInfo, error) {
	return &DatabaseInfo{Exists: true}, nil
}

func (a *mutablePrivilegeEngineAccess) QueryUserPrivileges(context.Context, string, string) ([]common.PrivilegeType, error) {
	return append([]common.PrivilegeType(nil), a.privileges...), nil
}

func (a *mutablePrivilegeEngineAccess) QueryDatabasePrivileges(context.Context, string, string, string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func (a *mutablePrivilegeEngineAccess) QueryTablePrivileges(context.Context, string, string, string, string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func TestFlushPrivilegesReloadsCachedGrantTableState(t *testing.T) {
	access := &mutablePrivilegeEngineAccess{privileges: []common.PrivilegeType{common.SelectPriv}}
	service := NewAuthService(nil, access).(*AuthServiceImpl)
	ctx := context.Background()

	before, err := service.GetUserInfo(ctx, "alice", "localhost")
	if err != nil {
		t.Fatalf("initial user lookup failed: %v", err)
	}
	if len(before.GlobalPrivileges) != 1 || before.GlobalPrivileges[0] != common.SelectPriv {
		t.Fatalf("unexpected initial privileges: %+v", before.GlobalPrivileges)
	}
	access.privileges = []common.PrivilegeType{common.InsertPriv}
	cached, err := service.GetUserInfo(ctx, "alice", "localhost")
	if err != nil || len(cached.GlobalPrivileges) != 1 || cached.GlobalPrivileges[0] != common.SelectPriv {
		t.Fatalf("cache did not retain pre-flush grant state: %+v, %v", cached, err)
	}
	if err := service.FlushPrivileges(ctx); err != nil {
		t.Fatalf("FlushPrivileges failed: %v", err)
	}
	after, err := service.GetUserInfo(ctx, "alice", "localhost")
	if err != nil {
		t.Fatalf("post-flush user lookup failed: %v", err)
	}
	if len(after.GlobalPrivileges) != 1 || after.GlobalPrivileges[0] != common.InsertPriv {
		t.Fatalf("grant cache was not reloaded: %+v", after.GlobalPrivileges)
	}
}
