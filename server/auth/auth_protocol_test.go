package auth

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

// TestPasswordValidator 测试密码验证器
func TestPasswordValidator(t *testing.T) {
	validator := NewMySQLNativePasswordValidator()

	// 测试空密码
	t.Run("TestEmptyPassword", func(t *testing.T) {
		result := validator.ValidatePassword("", "", nil)
		if !result {
			t.Errorf("Empty password validation failed")
		}
	})

	// 测试密码哈希
	t.Run("TestPasswordHash", func(t *testing.T) {
		password := "testpassword"
		hash, err := validator.HashPassword(password)
		if err != nil {
			t.Errorf("Failed to hash password: %v", err)
		}
		if hash == "" {
			t.Errorf("Hash should not be empty")
		}
		t.Logf("Password hash: %s", hash)
	})

	// 测试挑战生成
	t.Run("TestChallengeGeneration", func(t *testing.T) {
		challenge, err := validator.GenerateChallenge()
		if err != nil {
			t.Errorf("Failed to generate challenge: %v", err)
		}
		if len(challenge) != 20 {
			t.Errorf("Challenge should be 20 bytes, got %d", len(challenge))
		}

		// 检查没有null字节
		for i, b := range challenge {
			if b == 0 {
				t.Errorf("Challenge contains null byte at position %d", i)
			}
		}
	})

	// 测试密码验证
	t.Run("TestPasswordValidation", func(t *testing.T) {
		password := "testpassword"

		// 生成密码哈希
		hash, err := validator.HashPassword(password)
		if err != nil {
			t.Errorf("Failed to hash password: %v", err)
		}

		// 生成挑战
		challenge, err := validator.GenerateChallenge()
		if err != nil {
			t.Errorf("Failed to generate challenge: %v", err)
		}

		// 验证正确密码
		result := validator.ValidatePassword(password, hash, challenge)
		if !result {
			t.Errorf("Password validation failed for correct password")
		}

		// 验证错误密码
		result = validator.ValidatePassword("wrongpassword", hash, challenge)
		if result {
			t.Errorf("Password validation should fail for wrong password")
		}
	})
}

// TestPasswordValidatorFactory 测试密码验证器工厂
func TestPasswordValidatorFactory(t *testing.T) {
	factory := &PasswordValidatorFactory{}

	// 测试MySQL原生密码验证器
	t.Run("TestMySQLNativeValidator", func(t *testing.T) {
		validator := factory.CreateValidator("mysql_native_password")
		if validator == nil {
			t.Errorf("Failed to create mysql_native_password validator")
		}

		// 测试类型
		if _, ok := validator.(*MySQLNativePasswordValidator); !ok {
			t.Errorf("Expected MySQLNativePasswordValidator, got %T", validator)
		}
	})

	// 测试SHA2密码验证器
	t.Run("TestSHA2Validator", func(t *testing.T) {
		validator := factory.CreateValidator("caching_sha2_password")
		if validator == nil {
			t.Errorf("Failed to create caching_sha2_password validator")
		}

		// 测试类型
		if _, ok := validator.(*CachingSHA2PasswordValidator); !ok {
			t.Errorf("Expected CachingSHA2PasswordValidator, got %T", validator)
		}
	})

	// 测试默认验证器
	t.Run("TestDefaultValidator", func(t *testing.T) {
		validator := factory.CreateValidator("unknown_plugin")
		if validator == nil {
			t.Errorf("Failed to create default validator")
		}

		// 应该返回MySQL原生验证器
		if _, ok := validator.(*MySQLNativePasswordValidator); !ok {
			t.Errorf("Expected MySQLNativePasswordValidator as default, got %T", validator)
		}
	})
}

// TestAuthServiceWithPasswordValidator 测试认证服务与密码验证器的集成
func TestAuthServiceWithPasswordValidator(t *testing.T) {
	config := &conf.Cfg{}
	mockEngineAccess := &MockEngineAccessForTest{}
	authService := NewAuthService(config, mockEngineAccess)
	ctx := context.Background()

	// 测试挑战生成
	t.Run("TestChallengeGeneration", func(t *testing.T) {
		sessionID := "test-session-123"
		challenge, err := authService.GenerateChallenge(sessionID)
		if err != nil {
			t.Errorf("Failed to generate challenge: %v", err)
		}
		if len(challenge) != 20 {
			t.Errorf("Challenge should be 20 bytes, got %d", len(challenge))
		}

		// 验证挑战是否被缓存
		cachedChallenge := authService.GetChallenge(sessionID)
		if len(cachedChallenge) != 20 {
			t.Errorf("Cached challenge should be 20 bytes, got %d", len(cachedChallenge))
		}

		// 比较挑战是否相同
		for i := range challenge {
			if challenge[i] != cachedChallenge[i] {
				t.Errorf("Challenge mismatch at position %d", i)
			}
		}
	})

	// 测试认证流程
	t.Run("TestAuthenticationFlow", func(t *testing.T) {
		sessionID := "test-session-456"
		user := "root"
		host := "localhost"
		password := ""

		// 生成挑战
		_, err := authService.GenerateChallenge(sessionID)
		if err != nil {
			t.Errorf("Failed to generate challenge: %v", err)
		}

		// 模拟认证（使用空密码）
		result, err := authService.AuthenticateUser(ctx, user, password, host, "mysql")
		if err != nil {
			t.Errorf("Authentication failed: %v", err)
		}
		if result == nil || !result.Success {
			t.Errorf("Expected successful authentication")
		}

		// 验证挑战是否被清理
		cachedChallenge := authService.GetChallenge(sessionID)
		if len(cachedChallenge) != 0 {
			t.Errorf("Challenge should be cleared after successful authentication")
		}
	})
}

// BenchmarkPasswordValidation 密码验证性能测试
func BenchmarkPasswordValidation(b *testing.B) {
	validator := NewMySQLNativePasswordValidator()
	password := "testpassword"
	hash, _ := validator.HashPassword(password)
	challenge, _ := validator.GenerateChallenge()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.ValidatePassword(password, hash, challenge)
	}
}

// BenchmarkChallengeGeneration 挑战生成性能测试
func BenchmarkChallengeGeneration(b *testing.B) {
	validator := NewMySQLNativePasswordValidator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.GenerateChallenge()
	}
}
