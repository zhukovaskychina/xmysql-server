package manager

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
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
	ErrInvalidKey        = errors.New("invalid encryption key")
	ErrInvalidIV         = errors.New("invalid initialization vector")
	ErrInvalidCiphertext = errors.New("invalid ciphertext")
	ErrKeyNotFound       = errors.New("encryption key not found")
	ErrInvalidKeyring    = errors.New("invalid encryption keyring")
	ErrInvalidPageSize   = errors.New("page size must be a multiple of the AES block size")
)

var keyringMagic = []byte("XMYSQL-KEYRING-V1\x00")

// NewEncryptionManager 创建加密管理器
func NewEncryptionManager(masterKey []byte, settings EncryptionSettings) *EncryptionManager {
	return &EncryptionManager{
		masterKey: append([]byte(nil), masterKey...),
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
	key := em.spaceKeys[spaceID]
	if key == nil {
		return nil
	}
	copyKey := *key
	copyKey.Key = append([]byte(nil), key.Key...)
	copyKey.IV = append([]byte(nil), key.IV...)
	return &copyKey
}

// SpaceIDs returns the tablespaces for which this manager has a persisted or
// in-memory key. The result is sorted for deterministic provider setup.
func (em *EncryptionManager) SpaceIDs() []uint32 {
	em.mu.RLock()
	ids := make([]uint32, 0, len(em.spaceKeys))
	for spaceID := range em.spaceKeys {
		ids = append(ids, spaceID)
	}
	em.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
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

func (em *EncryptionManager) restoreKey(key *EncryptionKey) {
	if key == nil {
		return
	}
	copyKey := *key
	copyKey.Key = append([]byte(nil), key.Key...)
	copyKey.IV = append([]byte(nil), key.IV...)
	em.mu.Lock()
	em.spaceKeys[key.SpaceID] = &copyKey
	em.mu.Unlock()
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

// EncryptPageFixed encrypts a page without changing its length. Storage
// providers use this form because a page must occupy the same number of
// bytes on disk before and after encryption. The caller must provide a page
// whose size is a multiple of AES's block size.
func (em *EncryptionManager) EncryptPageFixed(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	key := em.GetKey(spaceID)
	if key == nil {
		return nil, ErrKeyNotFound
	}
	if key.Method == ENCRYPTION_METHOD_NONE {
		return append([]byte(nil), data...), nil
	}
	if key.Method != ENCRYPTION_METHOD_AES {
		return nil, errors.New("unsupported encryption method")
	}
	if len(data)%aes.BlockSize != 0 {
		return nil, ErrInvalidPageSize
	}
	return em.encryptAESFixed(key, pageNo, data)
}

// DecryptPageFixed is the fixed-size counterpart of EncryptPageFixed.
func (em *EncryptionManager) DecryptPageFixed(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	key := em.GetKey(spaceID)
	if key == nil {
		return nil, ErrKeyNotFound
	}
	if key.Method == ENCRYPTION_METHOD_NONE {
		return append([]byte(nil), data...), nil
	}
	if key.Method != ENCRYPTION_METHOD_AES {
		return nil, errors.New("unsupported encryption method")
	}
	if len(data)%aes.BlockSize != 0 {
		return nil, ErrInvalidPageSize
	}
	return em.decryptAESFixed(key, pageNo, data)
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
	if len(data) == 0 || len(data)%aes.BlockSize != 0 {
		return nil, ErrInvalidCiphertext
	}

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
	if padding == 0 || padding > aes.BlockSize || padding > len(plaintext) {
		return nil, ErrInvalidCiphertext
	}
	for _, value := range plaintext[len(plaintext)-padding:] {
		if int(value) != padding {
			return nil, ErrInvalidCiphertext
		}
	}
	return plaintext[:len(plaintext)-padding], nil
}

func (em *EncryptionManager) encryptAESFixed(key *EncryptionKey, pageNo uint32, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, err
	}
	iv := em.deriveIV(key.IV, pageNo)
	ciphertext := make([]byte, len(data))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, data)
	return ciphertext, nil
}

func (em *EncryptionManager) decryptAESFixed(key *EncryptionKey, pageNo uint32, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, err
	}
	iv := em.deriveIV(key.IV, pageNo)
	plaintext := make([]byte, len(data))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, data)
	return plaintext, nil
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

type persistedKeyring struct {
	Version uint32           `json:"version"`
	Keys    []*EncryptionKey `json:"keys"`
}

func (em *EncryptionManager) keyringAEAD() (cipher.AEAD, error) {
	em.mu.RLock()
	masterKey := append([]byte(nil), em.masterKey...)
	em.mu.RUnlock()
	if len(masterKey) == 0 {
		return nil, ErrInvalidKey
	}
	derived := sha256.Sum256(masterKey)
	block, err := aes.NewCipher(derived[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// SaveKeyring persists the in-memory tablespace keys in an authenticated
// envelope encrypted with the manager master key. The publication uses a
// temporary file so a process crash cannot leave a partially written keyring.
func (em *EncryptionManager) SaveKeyring(path string) error {
	aead, err := em.keyringAEAD()
	if err != nil {
		return err
	}

	em.mu.RLock()
	keys := make([]*EncryptionKey, 0, len(em.spaceKeys))
	for _, key := range em.spaceKeys {
		if key == nil {
			continue
		}
		copyKey := *key
		copyKey.Key = append([]byte(nil), key.Key...)
		copyKey.IV = append([]byte(nil), key.IV...)
		keys = append(keys, &copyKey)
	}
	em.mu.RUnlock()

	payload, err := json.Marshal(persistedKeyring{Version: 1, Keys: keys})
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	sealed := aead.Seal(nil, nonce, payload, keyringMagic)
	encoded := append(append(append([]byte(nil), keyringMagic...), nonce...), sealed...)

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".keyring-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if removeErr := os.Remove(path); removeErr != nil {
			return err
		}
		if err := os.Rename(tmpPath, path); err != nil {
			return err
		}
	}
	return nil
}

// LoadKeyring restores tablespace keys from a keyring authenticated and
// encrypted with this manager's master key.
func (em *EncryptionManager) LoadKeyring(path string) error {
	aead, err := em.keyringAEAD()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	minimum := len(keyringMagic) + aead.NonceSize() + aead.Overhead()
	if len(data) < minimum || !bytes.Equal(data[:len(keyringMagic)], keyringMagic) {
		return ErrInvalidKeyring
	}
	nonceStart := len(keyringMagic)
	nonceEnd := nonceStart + aead.NonceSize()
	payload, err := aead.Open(nil, data[nonceStart:nonceEnd], data[nonceEnd:], keyringMagic)
	if err != nil {
		return ErrInvalidKeyring
	}
	var persisted persistedKeyring
	if err := json.Unmarshal(payload, &persisted); err != nil || persisted.Version != 1 {
		return ErrInvalidKeyring
	}
	keys := make(map[uint32]*EncryptionKey, len(persisted.Keys))
	for _, key := range persisted.Keys {
		if key == nil || (key.Method != ENCRYPTION_METHOD_NONE && key.Method != ENCRYPTION_METHOD_AES) || len(key.Key) != 32 || len(key.IV) != aes.BlockSize {
			return ErrInvalidKeyring
		}
		copyKey := *key
		copyKey.Key = append([]byte(nil), key.Key...)
		copyKey.IV = append([]byte(nil), key.IV...)
		keys[key.SpaceID] = &copyKey
	}
	em.mu.Lock()
	em.spaceKeys = keys
	em.mu.Unlock()
	return nil
}
