package auth

import (
	"encoding/hex"
	"testing"
)

// TestVerifyAuthResponse 测试认证响应验证
func TestVerifyAuthResponse(t *testing.T) {
	validator := NewMySQLNativePasswordValidator().(*MySQLNativePasswordValidator)

	tests := []struct {
		name        string
		password    string
		challenge   string // hex string
		shouldPass  bool
		description string
	}{
		{
			name:        "正确的密码",
			password:    "test123",
			challenge:   "0102030405060708090a0b0c0d0e0f1011121314",
			shouldPass:  true,
			description: "使用正确的密码和challenge生成authResponse，应该验证通过",
		},

		{
			name:        "复杂密码",
			password:    "MyP@ssw0rd!2024",
			challenge:   "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b",
			shouldPass:  true,
			description: "复杂密码应该验证通过",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. 生成存储的密码哈希
			storedPassword, err := validator.HashPassword(tt.password)
			if err != nil {
				t.Fatalf("Failed to hash password: %v", err)
			}

			// 2. 解析challenge
			challenge, err := hex.DecodeString(tt.challenge)
			if err != nil {
				t.Fatalf("Failed to decode challenge: %v", err)
			}

			// 3. 模拟客户端：计算authResponse
			authResponse := validator.calculateAuthResponse(tt.password, challenge)

			// 4. 提取存储的哈希
			storedHash, err := validator.extractHashFromStoredPassword(storedPassword)
			if err != nil {
				t.Fatalf("Failed to extract stored hash: %v", err)
			}

			// 5. 验证authResponse
			result := validator.VerifyAuthResponse(authResponse, challenge, storedHash)

			if result != tt.shouldPass {
				t.Errorf("VerifyAuthResponse() = %v, want %v\nDescription: %s\nPassword: %s\nStoredPassword: %s\nChallenge: %x\nAuthResponse: %x\nStoredHash: %x",
					result, tt.shouldPass, tt.description, tt.password, storedPassword, challenge, authResponse, storedHash)
			}

			t.Logf("✅ %s - Password: %s, Stored: %s, Challenge: %x, AuthResponse: %x",
				tt.description, tt.password, storedPassword, challenge, authResponse)
		})
	}
}

// TestVerifyAuthResponseWithWrongPassword 测试错误密码的验证
func TestVerifyAuthResponseWithWrongPassword(t *testing.T) {
	validator := NewMySQLNativePasswordValidator().(*MySQLNativePasswordValidator)

	// 正确的密码
	correctPassword := "correct_password"
	wrongPassword := "wrong_password"

	// 生成challenge
	challenge, err := validator.GenerateChallenge()
	if err != nil {
		t.Fatalf("Failed to generate challenge: %v", err)
	}

	// 生成存储的密码哈希（使用正确密码）
	storedPassword, err := validator.HashPassword(correctPassword)
	if err != nil {
		t.Fatalf("Failed to hash password: %v", err)
	}

	// 提取存储的哈希
	storedHash, err := validator.extractHashFromStoredPassword(storedPassword)
	if err != nil {
		t.Fatalf("Failed to extract stored hash: %v", err)
	}

	// 使用错误密码计算authResponse
	wrongAuthResponse := validator.calculateAuthResponse(wrongPassword, challenge)

	// 验证应该失败
	result := validator.VerifyAuthResponse(wrongAuthResponse, challenge, storedHash)
	if result {
		t.Errorf("VerifyAuthResponse() should fail with wrong password, but passed")
	}

	t.Logf("✅ Wrong password correctly rejected")

	// 使用正确密码计算authResponse
	correctAuthResponse := validator.calculateAuthResponse(correctPassword, challenge)

	// 验证应该成功
	result = validator.VerifyAuthResponse(correctAuthResponse, challenge, storedHash)
	if !result {
		t.Errorf("VerifyAuthResponse() should pass with correct password, but failed")
	}

	t.Logf("✅ Correct password correctly accepted")
}

// TestVerifyAuthResponseWithInvalidLength 测试无效长度的参数
func TestVerifyAuthResponseWithInvalidLength(t *testing.T) {
	validator := NewMySQLNativePasswordValidator().(*MySQLNativePasswordValidator)

	tests := []struct {
		name            string
		authResponseLen int
		challengeLen    int
		storedHashLen   int
		expectedResult  bool
	}{
		{"Valid lengths", 20, 20, 20, true},
		{"Invalid authResponse length", 19, 20, 20, false},
		{"Invalid challenge length", 20, 19, 20, false},
		{"Invalid storedHash length", 20, 20, 19, false},
		{"All invalid", 10, 10, 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authResponse := make([]byte, tt.authResponseLen)
			challenge := make([]byte, tt.challengeLen)
			storedHash := make([]byte, tt.storedHashLen)

			// 对于有效长度的情况，生成真实的数据
			if tt.authResponseLen == 20 && tt.challengeLen == 20 && tt.storedHashLen == 20 {
				password := "test"
				storedPassword, _ := validator.HashPassword(password)
				storedHash, _ = validator.extractHashFromStoredPassword(storedPassword)
				challenge, _ = validator.GenerateChallenge()
				authResponse = validator.calculateAuthResponse(password, challenge)
			}

			result := validator.VerifyAuthResponse(authResponse, challenge, storedHash)

			if result != tt.expectedResult {
				t.Errorf("VerifyAuthResponse() = %v, want %v for %s", result, tt.expectedResult, tt.name)
			}
		})
	}
}

// TestVerifyAuthResponseAlgorithm 测试验证算法的正确性
func TestVerifyAuthResponseAlgorithm(t *testing.T) {
	validator := NewMySQLNativePasswordValidator().(*MySQLNativePasswordValidator)

	// 使用已知的测试向量
	password := "mysql"

	// 生成密码哈希
	storedPassword, err := validator.HashPassword(password)
	if err != nil {
		t.Fatalf("Failed to hash password: %v", err)
	}

	t.Logf("Password: %s", password)
	t.Logf("Stored password hash: %s", storedPassword)

	// 生成challenge
	challenge, err := validator.GenerateChallenge()
	if err != nil {
		t.Fatalf("Failed to generate challenge: %v", err)
	}

	t.Logf("Challenge: %x", challenge)

	// 计算authResponse（模拟客户端）
	authResponse := validator.calculateAuthResponse(password, challenge)
	t.Logf("Auth response: %x", authResponse)

	// 提取存储的哈希
	storedHash, err := validator.extractHashFromStoredPassword(storedPassword)
	if err != nil {
		t.Fatalf("Failed to extract stored hash: %v", err)
	}

	t.Logf("Stored hash: %x", storedHash)

	// 验证算法步骤
	t.Log("\n=== Verification Algorithm Steps ===")

	// 步骤1: 计算 SHA1(challenge + storedHash)
	challengeHash := validator.sha1Hash(append(challenge, storedHash...))
	t.Logf("Step 1 - SHA1(challenge + storedHash): %x", challengeHash)

	// 步骤2: XOR(authResponse, challengeHash) 得到 SHA1(password)
	stage1Hash := validator.xorBytes(authResponse, challengeHash)
	t.Logf("Step 2 - XOR(authResponse, challengeHash) = SHA1(password): %x", stage1Hash)

	// 步骤3: 计算 SHA1(SHA1(password))
	stage2Hash := validator.sha1Hash(stage1Hash)
	t.Logf("Step 3 - SHA1(SHA1(password)): %x", stage2Hash)

	// 步骤4: 比较结果是否等于 storedHash
	t.Logf("Step 4 - Compare with storedHash: %x", storedHash)
	t.Logf("Match: %v", validator.bytesEqual(stage2Hash, storedHash))

	// 最终验证
	result := validator.VerifyAuthResponse(authResponse, challenge, storedHash)
	if !result {
		t.Errorf("VerifyAuthResponse() failed, but should pass")
	}

	t.Logf("\n✅ Algorithm verification passed")
}

// TestEmptyPasswordAuthResponse 测试空密码的认证响应
func TestEmptyPasswordAuthResponse(t *testing.T) {
	validator := NewMySQLNativePasswordValidator().(*MySQLNativePasswordValidator)

	// 空密码
	password := ""

	// 生成challenge
	challenge, err := validator.GenerateChallenge()
	if err != nil {
		t.Fatalf("Failed to generate challenge: %v", err)
	}

	// 计算authResponse（空密码应该返回空数组）
	authResponse := validator.calculateAuthResponse(password, challenge)

	if len(authResponse) != 0 {
		t.Errorf("Empty password should produce empty authResponse, got %d bytes", len(authResponse))
	}

	t.Logf("✅ Empty password produces empty authResponse")

	// 验证空密码
	// 注意：空密码的storedHash也应该是空的
	storedPassword, err := validator.HashPassword(password)
	if err != nil {
		t.Fatalf("Failed to hash empty password: %v", err)
	}

	if storedPassword != "" {
		t.Errorf("Empty password should produce empty stored password, got: %s", storedPassword)
	}

	t.Logf("✅ Empty password produces empty stored password")
}

// BenchmarkVerifyAuthResponse 性能测试
func BenchmarkVerifyAuthResponse(b *testing.B) {
	validator := NewMySQLNativePasswordValidator().(*MySQLNativePasswordValidator)

	password := "benchmark_password"
	storedPassword, _ := validator.HashPassword(password)
	challenge, _ := validator.GenerateChallenge()
	authResponse := validator.calculateAuthResponse(password, challenge)
	storedHash, _ := validator.extractHashFromStoredPassword(storedPassword)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.VerifyAuthResponse(authResponse, challenge, storedHash)
	}
}
