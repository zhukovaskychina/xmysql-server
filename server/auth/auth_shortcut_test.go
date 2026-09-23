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

type scopedPrivilegeEngineAccess struct{}

func (scopedPrivilegeEngineAccess) QueryUser(ctx context.Context, user, host string) (*UserInfo, error) {
	return &UserInfo{User: user, Host: host, DynamicPrivileges: []string{"BACKUP_ADMIN"}, DatabasePrivileges: map[string][]common.PrivilegeType{}, TablePrivileges: map[string]map[string][]common.PrivilegeType{}}, nil
}
func (scopedPrivilegeEngineAccess) QueryDatabase(ctx context.Context, database string) (*DatabaseInfo, error) {
	return &DatabaseInfo{Name: database, Exists: true}, nil
}
func (scopedPrivilegeEngineAccess) QueryUserPrivileges(ctx context.Context, user, host string) ([]common.PrivilegeType, error) {
	return nil, nil
}
func (scopedPrivilegeEngineAccess) QueryDatabasePrivileges(ctx context.Context, user, host, database string) ([]common.PrivilegeType, error) {
	if database == "app" {
		return []common.PrivilegeType{common.SelectPriv}, nil
	}
	return nil, nil
}
func (scopedPrivilegeEngineAccess) QueryTablePrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, error) {
	if database == "app" && table == "users" {
		return []common.PrivilegeType{common.UpdatePriv}, nil
	}
	return nil, nil
}

func TestScopedDatabaseAndTablePrivilegesAreEnforced(t *testing.T) {
	service := NewAuthService(nil, scopedPrivilegeEngineAccess{})
	ctx := context.Background()
	if err := service.CheckPrivilege(ctx, "alice", "localhost", "app", "users", common.SelectPriv); err != nil {
		t.Fatalf("database-scoped SELECT was denied: %v", err)
	}
	if err := service.CheckPrivilege(ctx, "alice", "localhost", "app", "users", common.UpdatePriv); err != nil {
		t.Fatalf("table-scoped UPDATE was denied: %v", err)
	}
	if err := service.CheckPrivilege(ctx, "alice", "localhost", "other", "users", common.SelectPriv); err == nil {
		t.Fatal("privilege from app leaked into another database")
	}
}

func TestDynamicPrivilegeIsCheckedByName(t *testing.T) {
	service := NewAuthService(nil, scopedPrivilegeEngineAccess{}).(*AuthServiceImpl)
	ctx := context.Background()
	if err := service.CheckDynamicPrivilege(ctx, "alice", "localhost", "BACKUP_ADMIN"); err != nil {
		t.Fatalf("dynamic privilege was denied: %v", err)
	}
	if err := service.CheckDynamicPrivilege(ctx, "alice", "localhost", "SYSTEM_USER"); err == nil {
		t.Fatal("ungranted dynamic privilege was allowed")
	}
	if err := service.CheckDynamicPrivilege(ctx, "alice", "localhost", "NOT_A_MYSQL_DYNAMIC_PRIVILEGE"); err == nil || !strings.Contains(err.Error(), "unknown dynamic privilege") {
		t.Fatalf("unknown dynamic privilege error = %v", err)
	}
}

type columnPrivilegeEngineAccess struct{}

func (columnPrivilegeEngineAccess) QueryUser(ctx context.Context, user, host string) (*UserInfo, error) {
	return &UserInfo{User: user, Host: host, DatabasePrivileges: map[string][]common.PrivilegeType{}, TablePrivileges: map[string]map[string][]common.PrivilegeType{}, ColumnPrivileges: map[string][]common.PrivilegeType{"app.users.email": {common.SelectPriv}}}, nil
}
func (columnPrivilegeEngineAccess) QueryDatabase(ctx context.Context, database string) (*DatabaseInfo, error) {
	return &DatabaseInfo{Name: database, Exists: true}, nil
}
func (columnPrivilegeEngineAccess) QueryUserPrivileges(ctx context.Context, user, host string) ([]common.PrivilegeType, error) {
	return nil, nil
}
func (columnPrivilegeEngineAccess) QueryDatabasePrivileges(ctx context.Context, user, host, database string) ([]common.PrivilegeType, error) {
	return nil, nil
}
func (columnPrivilegeEngineAccess) QueryTablePrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, error) {
	return nil, nil
}

func TestColumnPrivilegeDoesNotBecomeTablePrivilege(t *testing.T) {
	service := NewAuthService(nil, columnPrivilegeEngineAccess{}).(*AuthServiceImpl)
	ctx := context.Background()
	if err := service.CheckColumnPrivilege(ctx, "alice", "localhost", "app", "users", "email", common.SelectPriv); err != nil {
		t.Fatalf("column SELECT was denied: %v", err)
	}
	if err := service.CheckColumnPrivilege(ctx, "alice", "localhost", "app", "users", "password", common.SelectPriv); err == nil {
		t.Fatal("column privilege leaked to another column")
	}
}
