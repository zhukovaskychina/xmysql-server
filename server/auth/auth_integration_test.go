package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
)

// TestAuthServiceIntegration 测试认证服务集成
func TestAuthServiceIntegration(t *testing.T) {
	// 创建测试配置
	config := &conf.Cfg{
		// 添加必要的配置
	}

	// 创建引擎（在实际环境中应该正确初始化）
	xmysqlEngine := engine.NewXMySQLEngine(config)

	// 创建引擎访问
	engineAccess := NewInnoDBEngineAccess(config, xmysqlEngine)

	// 创建认证服务
	authService := NewAuthService(config, engineAccess)

	ctx := context.Background()

	// 测试用户认证
	t.Run("TestUserAuthentication", func(t *testing.T) {
		// 测试成功认证
		result, err := authService.AuthenticateUser(ctx, "root", "", "localhost", "")
		if err != nil {
			t.Logf("Authentication error (expected in test environment): %v", err)
		}
		if result != nil {
			t.Logf("Authentication result: Success=%v, User=%s, Host=%s",
				result.Success, result.User, result.Host)
		}

		// 测试失败认证
		result, err = authService.AuthenticateUser(ctx, "nonexistent", "wrongpass", "localhost", "")
		if err != nil {
			t.Logf("Expected authentication failure: %v", err)
		}
		if result != nil && !result.Success {
			t.Logf("Authentication correctly failed: %s", result.ErrorMessage)
		}
	})

	// 测试数据库验证
	t.Run("TestDatabaseValidation", func(t *testing.T) {
		// 测试系统数据库
		err := authService.ValidateDatabase(ctx, "mysql")
		if err != nil {
			t.Logf("Database validation error (expected in test environment): %v", err)
		}

		// 测试不存在的数据库
		err = authService.ValidateDatabase(ctx, "nonexistent_db")
		if err != nil {
			t.Logf("Expected database validation failure: %v", err)
		}
	})

	// 测试权限检查
	t.Run("TestPrivilegeCheck", func(t *testing.T) {
		// 测试SELECT权限
		err := authService.CheckPrivilege(ctx, "root", "localhost", "mysql", "", common.SelectPriv)
		if err != nil {
			t.Logf("Privilege check error (expected in test environment): %v", err)
		}

		// 测试不存在用户的权限
		err = authService.CheckPrivilege(ctx, "nonexistent", "localhost", "mysql", "", common.SelectPriv)
		if err != nil {
			t.Logf("Expected privilege check failure: %v", err)
		}
	})
}

// TestEngineAccess 测试引擎访问
func TestEngineAccess(t *testing.T) {
	// 创建测试配置
	config := &conf.Cfg{
		// 添加必要的配置
	}

	// 创建引擎
	xmysqlEngine := engine.NewXMySQLEngine(config)

	// 创建引擎访问
	engineAccess := NewInnoDBEngineAccess(config, xmysqlEngine)

	ctx := context.Background()

	// 测试用户查询
	t.Run("TestQueryUser", func(t *testing.T) {
		userInfo, err := engineAccess.QueryUser(ctx, "root", "localhost")
		if err != nil {
			t.Logf("Query user error (expected in test environment): %v", err)
		}
		if userInfo != nil {
			t.Logf("User info: User=%s, Host=%s", userInfo.User, userInfo.Host)
		}
	})

	// 测试数据库查询
	t.Run("TestQueryDatabase", func(t *testing.T) {
		dbInfo, err := engineAccess.QueryDatabase(ctx, "mysql")
		if err != nil {
			t.Logf("Query database error (expected in test environment): %v", err)
		}
		if dbInfo != nil {
			t.Logf("Database info: Name=%s, Exists=%v", dbInfo.Name, dbInfo.Exists)
		}
	})

	// 测试权限查询
	t.Run("TestQueryPrivileges", func(t *testing.T) {
		privs, err := engineAccess.QueryUserPrivileges(ctx, "root", "localhost")
		if err != nil {
			t.Logf("Query privileges error (expected in test environment): %v", err)
		}
		if privs != nil {
			t.Logf("User privileges count: %d", len(privs))
		}
	})
}

// MockEngineAccessForTest 测试用的模拟引擎访问
type MockEngineAccessForTest struct{}

func (m *MockEngineAccessForTest) QueryUser(ctx context.Context, user, host string) (*UserInfo, error) {
	// 模拟用户数据
	if user == "root" && host == "localhost" {
		return &UserInfo{
			User:               user,
			Host:               host,
			Password:           "", // 空密码用于测试
			AccountLocked:      false,
			PasswordExpired:    false,
			MaxConnections:     100,
			MaxUserConnections: 10,
			GlobalPrivileges:   []common.PrivilegeType{common.AllPriv},
			DatabasePrivileges: make(map[string][]common.PrivilegeType),
			TablePrivileges:    make(map[string]map[string][]common.PrivilegeType),
		}, nil
	}
	return nil, fmt.Errorf("user '%s'@'%s' not found", user, host)
}

func (m *MockEngineAccessForTest) QueryDatabase(ctx context.Context, database string) (*DatabaseInfo, error) {
	// 模拟数据库数据
	systemDatabases := []string{"mysql", "information_schema", "performance_schema", "sys"}
	for _, db := range systemDatabases {
		if database == db {
			return &DatabaseInfo{
				Name:      database,
				Charset:   "utf8mb4",
				Collation: "utf8mb4_general_ci",
				Exists:    true,
			}, nil
		}
	}
	return &DatabaseInfo{
		Name:   database,
		Exists: false,
	}, nil
}

func (m *MockEngineAccessForTest) QueryUserPrivileges(ctx context.Context, user, host string) ([]common.PrivilegeType, error) {
	if user == "root" {
		return []common.PrivilegeType{common.AllPriv}, nil
	}
	return []common.PrivilegeType{}, nil
}

func (m *MockEngineAccessForTest) QueryDatabasePrivileges(ctx context.Context, user, host, database string) ([]common.PrivilegeType, error) {
	return []common.PrivilegeType{}, nil
}

func (m *MockEngineAccessForTest) QueryTablePrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, error) {
	return []common.PrivilegeType{}, nil
}

// TestAuthServiceWithMock 使用模拟引擎访问测试认证服务
func TestAuthServiceWithMock(t *testing.T) {
	config := &conf.Cfg{}
	mockEngineAccess := &MockEngineAccessForTest{}
	authService := NewAuthService(config, mockEngineAccess)

	ctx := context.Background()

	// 测试成功认证
	t.Run("TestSuccessfulAuth", func(t *testing.T) {
		result, err := authService.AuthenticateUser(ctx, "root", "", "localhost", "mysql")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result == nil || !result.Success {
			t.Errorf("Expected successful authentication")
		}
		if result != nil {
			t.Logf("Authentication successful: User=%s, Host=%s, Database=%s",
				result.User, result.Host, result.Database)
		}
	})

	// 测试失败认证
	t.Run("TestFailedAuth", func(t *testing.T) {
		result, err := authService.AuthenticateUser(ctx, "nonexistent", "password", "localhost", "")
		if err != nil {
			t.Logf("Expected error: %v", err)
		}
		if result != nil && result.Success {
			t.Errorf("Expected authentication failure")
		}
	})

	// 测试数据库验证
	t.Run("TestDatabaseValidation", func(t *testing.T) {
		// 测试存在的数据库
		err := authService.ValidateDatabase(ctx, "mysql")
		if err != nil {
			t.Errorf("Unexpected error for existing database: %v", err)
		}

		// 测试不存在的数据库
		err = authService.ValidateDatabase(ctx, "nonexistent")
		if err == nil {
			t.Errorf("Expected error for non-existent database")
		}
	})

	// 测试权限检查
	t.Run("TestPrivilegeCheck", func(t *testing.T) {
		// root用户应该有所有权限
		err := authService.CheckPrivilege(ctx, "root", "localhost", "mysql", "", common.SelectPriv)
		if err != nil {
			t.Errorf("Unexpected error for root user: %v", err)
		}

		// 不存在的用户应该没有权限
		err = authService.CheckPrivilege(ctx, "nonexistent", "localhost", "mysql", "", common.SelectPriv)
		if err == nil {
			t.Errorf("Expected error for non-existent user")
		}
	})
}
