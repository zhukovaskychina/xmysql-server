package auth

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
)

// PasswordValidator 密码验证器接口
type PasswordValidator interface {
	ValidatePassword(inputPassword, storedPassword string, challenge []byte) bool
	GenerateChallenge() ([]byte, error)
	HashPassword(password string) (string, error)
}

// MySQLNativePasswordValidator MySQL原生密码验证器
type MySQLNativePasswordValidator struct{}

// NewMySQLNativePasswordValidator 创建MySQL原生密码验证器
func NewMySQLNativePasswordValidator() PasswordValidator {
	return &MySQLNativePasswordValidator{}
}

// ValidatePassword 验证密码
func (v *MySQLNativePasswordValidator) ValidatePassword(inputPassword, storedPassword string, challenge []byte) bool {
	if storedPassword == "" && inputPassword == "" {
		return true // 空密码
	}

	if len(storedPassword) != 41 || storedPassword[0] != '*' {
		// 不是标准的MySQL密码格式，尝试直接比较
		return inputPassword == storedPassword
	}

	// MySQL native password验证
	return v.verifyMySQLNativePassword(inputPassword, storedPassword, challenge)
}

// verifyMySQLNativePassword 验证MySQL原生密码
func (v *MySQLNativePasswordValidator) verifyMySQLNativePassword(inputPassword, storedPassword string, challenge []byte) bool {
	if len(challenge) != 20 {
		return false
	}

	// 计算期望的认证响应
	expectedResponse := v.calculateAuthResponse(inputPassword, challenge)

	// 从存储的密码中提取hash
	storedHash, err := v.extractHashFromStoredPassword(storedPassword)
	if err != nil {
		return false
	}

	// 计算实际的认证响应
	actualResponse := v.xorBytes(storedHash, v.sha1Hash(challenge))

	// 比较结果
	return v.bytesEqual(expectedResponse, actualResponse)
}

// calculateAuthResponse 计算认证响应
func (v *MySQLNativePasswordValidator) calculateAuthResponse(password string, challenge []byte) []byte {
	if password == "" {
		return []byte{}
	}

	// stage1_hash = SHA1(password)
	stage1Hash := v.sha1Hash([]byte(password))

	// stage2_hash = SHA1(stage1_hash)
	stage2Hash := v.sha1Hash(stage1Hash)

	// auth_response = XOR(stage1_hash, SHA1(challenge + stage2_hash))
	challengeHash := v.sha1Hash(append(challenge, stage2Hash...))

	return v.xorBytes(stage1Hash, challengeHash)
}

// extractHashFromStoredPassword 从存储的密码中提取hash
func (v *MySQLNativePasswordValidator) extractHashFromStoredPassword(storedPassword string) ([]byte, error) {
	if len(storedPassword) != 41 || storedPassword[0] != '*' {
		return nil, fmt.Errorf("invalid stored password format")
	}

	// 解析十六进制字符串
	hashStr := storedPassword[1:] // 去掉开头的'*'
	hash := make([]byte, 20)

	for i := 0; i < 20; i++ {
		var b byte
		_, err := fmt.Sscanf(hashStr[i*2:i*2+2], "%02X", &b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse hash: %v", err)
		}
		hash[i] = b
	}

	return hash, nil
}

// GenerateChallenge 生成挑战字符串
func (v *MySQLNativePasswordValidator) GenerateChallenge() ([]byte, error) {
	challenge := make([]byte, 20)
	_, err := rand.Read(challenge)
	if err != nil {
		return nil, fmt.Errorf("failed to generate challenge: %v", err)
	}

	// 确保没有null字节
	for i := range challenge {
		if challenge[i] == 0 {
			challenge[i] = 1
		}
	}

	return challenge, nil
}

// HashPassword 对密码进行哈希
func (v *MySQLNativePasswordValidator) HashPassword(password string) (string, error) {
	if password == "" {
		return "", nil
	}

	// stage1_hash = SHA1(password)
	stage1Hash := v.sha1Hash([]byte(password))

	// stage2_hash = SHA1(stage1_hash)
	stage2Hash := v.sha1Hash(stage1Hash)

	// 格式化为MySQL存储格式
	return fmt.Sprintf("*%X", stage2Hash), nil
}

// 辅助方法

// sha1Hash 计算SHA1哈希
func (v *MySQLNativePasswordValidator) sha1Hash(data []byte) []byte {
	hash := sha1.Sum(data)
	return hash[:]
}

// xorBytes 对两个字节数组进行XOR操作
func (v *MySQLNativePasswordValidator) xorBytes(a, b []byte) []byte {
	if len(a) != len(b) {
		return nil
	}

	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}

	return result
}

// bytesEqual 比较两个字节数组是否相等
func (v *MySQLNativePasswordValidator) bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// VerifyAuthResponse 验证客户端发送的认证响应
// authResponse: 客户端发送的认证响应（20字节）
// challenge: 服务器发送的挑战数据（20字节）
// storedHash: 存储的密码哈希 SHA1(SHA1(password))（20字节）
func (v *MySQLNativePasswordValidator) VerifyAuthResponse(authResponse, challenge, storedHash []byte) bool {
	if len(authResponse) != 20 || len(challenge) != 20 || len(storedHash) != 20 {
		return false
	}

	// MySQL native password验证算法：
	// authResponse = XOR(SHA1(password), SHA1(challenge + SHA1(SHA1(password))))
	// 其中 SHA1(SHA1(password)) 就是 storedHash
	//
	// 验证步骤：
	// 1. 计算 SHA1(challenge + storedHash)
	// 2. XOR(authResponse, SHA1(challenge + storedHash)) 得到 SHA1(password)
	// 3. 计算 SHA1(SHA1(password))
	// 4. 比较结果是否等于 storedHash

	// 步骤1: 计算 SHA1(challenge + storedHash)
	challengeHash := v.sha1Hash(append(challenge, storedHash...))

	// 步骤2: XOR(authResponse, challengeHash) 得到 SHA1(password)
	stage1Hash := v.xorBytes(authResponse, challengeHash)
	if stage1Hash == nil {
		return false
	}

	// 步骤3: 计算 SHA1(SHA1(password))
	stage2Hash := v.sha1Hash(stage1Hash)

	// 步骤4: 比较结果是否等于 storedHash
	return v.bytesEqual(stage2Hash, storedHash)
}

// CachingSHA2PasswordValidator SHA2密码验证器（简化实现）
type CachingSHA2PasswordValidator struct{}

// NewCachingSHA2PasswordValidator 创建SHA2密码验证器
func NewCachingSHA2PasswordValidator() PasswordValidator {
	return &CachingSHA2PasswordValidator{}
}

// ValidatePassword SHA2密码验证（简化实现）
func (v *CachingSHA2PasswordValidator) ValidatePassword(inputPassword, storedPassword string, challenge []byte) bool {
	// 简化实现，实际应该使用SHA256
	nativeValidator := &MySQLNativePasswordValidator{}
	return nativeValidator.ValidatePassword(inputPassword, storedPassword, challenge)
}

// GenerateChallenge 生成挑战字符串
func (v *CachingSHA2PasswordValidator) GenerateChallenge() ([]byte, error) {
	nativeValidator := &MySQLNativePasswordValidator{}
	return nativeValidator.GenerateChallenge()
}

// HashPassword 对密码进行哈希
func (v *CachingSHA2PasswordValidator) HashPassword(password string) (string, error) {
	nativeValidator := &MySQLNativePasswordValidator{}
	return nativeValidator.HashPassword(password)
}

// PasswordValidatorFactory 密码验证器工厂
type PasswordValidatorFactory struct{}

// CreateValidator 根据认证插件创建验证器
func (f *PasswordValidatorFactory) CreateValidator(authPlugin string) PasswordValidator {
	switch authPlugin {
	case "mysql_native_password", "":
		return NewMySQLNativePasswordValidator()
	case "caching_sha2_password":
		return NewCachingSHA2PasswordValidator()
	default:
		// 默认使用原生密码验证器
		return NewMySQLNativePasswordValidator()
	}
}
