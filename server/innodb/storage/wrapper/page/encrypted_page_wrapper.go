package page

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrInvalidKeySize    = errors.New("invalid encryption key size")
	ErrInvalidNonceSize  = errors.New("invalid nonce size")
	ErrInvalidCiphertext = errors.New("invalid ciphertext")
	ErrEncryptionFailed  = errors.New("encryption failed")
	ErrDecryptionFailed  = errors.New("decryption failed")
)

// EncryptedPageWrapper 加密页面包装器
type EncryptedPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	mu sync.RWMutex

	// 底层的加密页面实现
	encryptedPage *pages.EncryptedPage

	// 加密数据
	encryptedData []byte
	originalSize  uint32
	nonce         []byte

	// 加密参数
	keyID     uint32 // 密钥ID
	algorithm uint16 // 1:AES-GCM
	key       []byte // 加密密钥
}

// NewEncryptedPageWrapper 创建加密页面
func NewEncryptedPageWrapper(id, spaceID, pageNo uint32, bp *buffer_pool.BufferPool) *EncryptedPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_ENCRYPTED)
	encryptedPage := pages.NewEncryptedPage(spaceID, id, pages.EncryptionAES256CBC, 1, 1)

	return &EncryptedPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		encryptedPage:   encryptedPage,
		algorithm:       1, // 默认使用AES-GCM
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析加密页面
func (epw *EncryptedPageWrapper) ParseFromBytes(data []byte) error {
	epw.Lock()
	defer epw.Unlock()

	if err := epw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析加密页面特有的数据
	if err := epw.encryptedPage.Deserialize(data); err != nil {
		return err
	}

	return nil
}

// ToBytes 序列化加密页面为字节数组
func (epw *EncryptedPageWrapper) ToBytes() ([]byte, error) {
	epw.RLock()
	defer epw.RUnlock()

	// 序列化加密页面
	data := epw.encryptedPage.Serialize()

	// 更新基础包装器的内容
	if len(epw.content) != len(data) {
		epw.content = make([]byte, len(data))
	}
	copy(epw.content, data)

	return data, nil
}

// 加密页面特有的方法

// SetKey 设置加密密钥
func (epw *EncryptedPageWrapper) SetKey(key []byte) error {
	epw.mu.Lock()
	defer epw.mu.Unlock()

	if len(key) != 16 && len(key) != 32 { // AES-128 or AES-256
		return ErrInvalidKeySize
	}

	epw.key = make([]byte, len(key))
	copy(epw.key, key)

	return nil
}

// SetData 设置原始数据并加密
func (epw *EncryptedPageWrapper) SetData(data []byte) error {
	epw.Lock()
	defer epw.Unlock()

	// 使用底层实现
	if epw.encryptedPage != nil {
		if err := epw.encryptedPage.EncryptData(data, epw.key); err != nil {
			return err
		}
	} else {
		// 手动加密
		if err := epw.encryptData(data); err != nil {
			return err
		}
	}

	epw.MarkDirty()
	return nil
}

// GetOriginalData 获取解密后的原始数据
func (epw *EncryptedPageWrapper) GetOriginalData() ([]byte, error) {
	epw.RLock()
	defer epw.RUnlock()

	if epw.encryptedPage != nil {
		return epw.encryptedPage.DecryptData(epw.key)
	}

	// 手动解密
	return epw.decryptData()
}

// GetEncryptedData 获取加密后的数据
func (epw *EncryptedPageWrapper) GetEncryptedData() []byte {
	epw.RLock()
	defer epw.RUnlock()

	if epw.encryptedPage != nil {
		// 从EncryptedPage结构中获取加密数据
		result := make([]byte, len(epw.encryptedPage.EncryptedData))
		copy(result, epw.encryptedPage.EncryptedData)
		return result
	}

	result := make([]byte, len(epw.encryptedData))
	copy(result, epw.encryptedData)
	return result
}

// GetEncryptionAlgorithm 获取加密算法
func (epw *EncryptedPageWrapper) GetEncryptionAlgorithm() pages.EncryptionAlgorithm {
	epw.RLock()
	defer epw.RUnlock()

	if epw.encryptedPage != nil {
		return epw.encryptedPage.GetEncryptionAlgorithm()
	}

	return pages.EncryptionAES256CBC
}

// GetOriginalSize 获取原始数据大小
func (epw *EncryptedPageWrapper) GetOriginalSize() uint32 {
	epw.RLock()
	defer epw.RUnlock()

	// EncryptedPage没有原始大小字段，返回本地记录的大小
	return epw.originalSize
}

// GetEncryptedSize 获取加密后数据大小
func (epw *EncryptedPageWrapper) GetEncryptedSize() uint32 {
	epw.RLock()
	defer epw.RUnlock()

	if epw.encryptedPage != nil {
		return uint32(len(epw.encryptedPage.EncryptedData))
	}

	return uint32(len(epw.encryptedData))
}

// Validate 验证加密页面数据完整性
func (epw *EncryptedPageWrapper) Validate() error {
	epw.RLock()
	defer epw.RUnlock()

	if epw.encryptedPage != nil {
		return epw.encryptedPage.Validate()
	}

	// 简单验证
	if epw.originalSize == 0 && len(epw.encryptedData) > 0 {
		return errors.New("invalid encrypted data: zero original size")
	}

	return nil
}

// GetEncryptedPage 获取底层的加密页面实现
func (epw *EncryptedPageWrapper) GetEncryptedPage() *pages.EncryptedPage {
	return epw.encryptedPage
}

// Read 实现PageWrapper接口
func (ew *EncryptedPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if ew.bufferPool != nil {
		if page, err := ew.bufferPool.GetPage(ew.GetSpaceID(), ew.GetPageID()); err == nil {
			if page != nil {
				ew.content = page.GetContent()
				return ew.ParseFromBytes(ew.content)
			}
		}
	}

	// 2. 从磁盘读取
	content, err := ew.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	if ew.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(ew.GetSpaceID(), ew.GetPageID())
		bufferPage.SetContent(content)
		ew.bufferPool.PutPage(bufferPage)
	}

	// 4. 解析内容
	ew.content = content
	return ew.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (ew *EncryptedPageWrapper) Write() error {
	// 1. 序列化页面内容
	content, err := ew.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	if ew.bufferPool != nil {
		if page, err := ew.bufferPool.GetPage(ew.GetSpaceID(), ew.GetPageID()); err == nil {
			if page != nil {
				page.SetContent(content)
				page.MarkDirty()
			}
		}
	}

	// 3. 写入磁盘
	return ew.writeToDisk(content)
}

// 内部方法：从磁盘读取
func (ew *EncryptedPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取页面的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return make([]byte, common.PageSize), nil
}

// 内部方法：写入磁盘
func (ew *EncryptedPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return nil
}

// 内部方法：手动加密数据
func (epw *EncryptedPageWrapper) encryptData(data []byte) error {
	if len(epw.key) == 0 {
		// 无密钥，直接存储
		epw.encryptedData = make([]byte, len(data))
		copy(epw.encryptedData, data)
		epw.originalSize = uint32(len(data))
		return nil
	}

	// 生成随机nonce
	epw.nonce = make([]byte, 12) // GCM nonce size
	if _, err := io.ReadFull(rand.Reader, epw.nonce); err != nil {
		return ErrEncryptionFailed
	}

	// 创建AES cipher
	block, err := aes.NewCipher(epw.key)
	if err != nil {
		return ErrEncryptionFailed
	}

	// 创建GCM模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return ErrEncryptionFailed
	}

	// 加密数据
	epw.encryptedData = gcm.Seal(nil, epw.nonce, data, nil)
	epw.originalSize = uint32(len(data))

	return nil
}

// 内部方法：手动解密数据
func (epw *EncryptedPageWrapper) decryptData() ([]byte, error) {
	if len(epw.key) == 0 {
		// 无密钥，直接返回
		result := make([]byte, len(epw.encryptedData))
		copy(result, epw.encryptedData)
		return result, nil
	}

	if len(epw.encryptedData) == 0 || len(epw.nonce) == 0 {
		return nil, ErrInvalidCiphertext
	}

	// 创建AES cipher
	block, err := aes.NewCipher(epw.key)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	// 创建GCM模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	// 解密数据
	result, err := gcm.Open(nil, epw.nonce, epw.encryptedData, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return result, nil
}
