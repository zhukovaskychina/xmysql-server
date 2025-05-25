package manager

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

// EncryptionManager 管理表空间和页面的加密
type EncryptionManager struct {
	mu sync.RWMutex

	// 主密钥
	masterKey []byte

	// 表空间密钥映射: space_id -> encryption_key
	spaceKeys map[uint32]*EncryptionKey

	// 加密设置
	settings EncryptionSettings
}

// EncryptionKey 表示加密密钥
type EncryptionKey struct {
	SpaceID  uint32 // 表空间ID
	Key      []byte // 加密密钥
	IV       []byte // 初始化向量
	Method   uint8  // 加密方法
	Rotating bool   // 是否正在轮换
	Version  uint32 // 密钥版本
	CreateAt int64  // 创建时间
}

// EncryptionSettings 表示加密设置
type EncryptionSettings struct {
	Method          uint8  // 默认加密方法
	KeyRotationDays uint32 // 密钥轮换周期(天)
	ThreadsNum      uint8  // 加密线程数
	BufferSize      uint32 // 加密缓冲区大小
}

// 加密方法常量
const (
	ENCRYPTION_METHOD_NONE uint8 = iota // 不加密
	ENCRYPTION_METHOD_AES               // AES-256-CBC
)

var (
	ErrInvalidKey  = errors.New("invalid encryption key")
	ErrInvalidIV   = errors.New("invalid initialization vector")
	ErrKeyNotFound = errors.New("encryption key not found")
)

// NewEncryptionManager 创建加密管理器
func NewEncryptionManager(masterKey []byte, settings EncryptionSettings) *EncryptionManager {
	return &EncryptionManager{
		masterKey: masterKey,
		spaceKeys: make(map[uint32]*EncryptionKey),
		settings:  settings,
	}
}

// CreateKey 为表空间创建加密密钥
func (em *EncryptionManager) CreateKey(spaceID uint32) (*EncryptionKey, error) {
	em.mu.Lock()
	defer em.mu.Unlock()

	// 生成随机密钥
	key := make([]byte, 32) // AES-256
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}

	// 生成初始化向量
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	// 创建加密密钥
	encKey := &EncryptionKey{
		SpaceID:  spaceID,
		Key:      key,
		IV:       iv,
		Method:   em.settings.Method,
		Version:  1,
		CreateAt: time.Now().Unix(),
	}

	em.spaceKeys[spaceID] = encKey
	return encKey, nil
}

// GetKey 获取表空间的加密密钥
func (em *EncryptionManager) GetKey(spaceID uint32) *EncryptionKey {
	em.mu.RLock()
	defer em.mu.RUnlock()
	return em.spaceKeys[spaceID]
}

// RotateKey 轮换表空间的加密密钥
func (em *EncryptionManager) RotateKey(spaceID uint32) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	oldKey := em.spaceKeys[spaceID]
	if oldKey == nil {
		return ErrKeyNotFound
	}

	// 生成新密钥
	newKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, newKey); err != nil {
		return err
	}

	// 生成新IV
	newIV := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, newIV); err != nil {
		return err
	}

	// 更新密钥
	oldKey.Key = newKey
	oldKey.IV = newIV
	oldKey.Version++
	oldKey.CreateAt = time.Now().Unix()

	return nil
}

// EncryptPage 加密页面内容
func (em *EncryptionManager) EncryptPage(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	key := em.GetKey(spaceID)
	if key == nil || key.Method == ENCRYPTION_METHOD_NONE {
		return data, nil
	}

	switch key.Method {
	case ENCRYPTION_METHOD_AES:
		return em.encryptAES(key, pageNo, data)
	default:
		return nil, errors.New("unsupported encryption method")
	}
}

// DecryptPage 解密页面内容
func (em *EncryptionManager) DecryptPage(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	key := em.GetKey(spaceID)
	if key == nil || key.Method == ENCRYPTION_METHOD_NONE {
		return data, nil
	}

	switch key.Method {
	case ENCRYPTION_METHOD_AES:
		return em.decryptAES(key, pageNo, data)
	default:
		return nil, errors.New("unsupported encryption method")
	}
}

// encryptAES 使用AES-256-CBC加密数据
func (em *EncryptionManager) encryptAES(key *EncryptionKey, pageNo uint32, data []byte) ([]byte, error) {
	// 创建密码块
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, err
	}

	// 生成页面特定的IV
	iv := em.deriveIV(key.IV, pageNo)

	// 填充数据
	padding := aes.BlockSize - len(data)%aes.BlockSize
	padtext := make([]byte, len(data)+padding)
	copy(padtext, data)
	for i := len(data); i < len(padtext); i++ {
		padtext[i] = byte(padding)
	}

	// 加密数据
	ciphertext := make([]byte, len(padtext))
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext, padtext)

	return ciphertext, nil
}

// decryptAES 使用AES-256-CBC解密数据
func (em *EncryptionManager) decryptAES(key *EncryptionKey, pageNo uint32, data []byte) ([]byte, error) {
	// 创建密码块
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, err
	}

	// 生成页面特定的IV
	iv := em.deriveIV(key.IV, pageNo)

	// 解密数据
	plaintext := make([]byte, len(data))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(plaintext, data)

	// 去除填充
	padding := int(plaintext[len(plaintext)-1])
	return plaintext[:len(plaintext)-padding], nil
}

// deriveIV 为每个页面生成唯一的IV
func (em *EncryptionManager) deriveIV(baseIV []byte, pageNo uint32) []byte {
	// 组合基础IV和页号
	data := make([]byte, len(baseIV)+4)
	copy(data, baseIV)
	binary.BigEndian.PutUint32(data[len(baseIV):], pageNo)

	// 计算SHA-256哈希
	hash := sha256.Sum256(data)

	// 返回前16字节作为新的IV
	return hash[:16]
}

// Close 关闭加密管理器
func (em *EncryptionManager) Close() error {
	em.mu.Lock()
	defer em.mu.Unlock()

	// 清理密钥
	for k := range em.spaceKeys {
		delete(em.spaceKeys, k)
	}

	// 清零主密钥
	for i := range em.masterKey {
		em.masterKey[i] = 0
	}

	return nil
}
