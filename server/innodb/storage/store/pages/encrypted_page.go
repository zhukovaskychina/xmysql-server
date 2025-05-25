/*
FIL_PAGE_TYPE_ENCRYPTED页面详细说明

基本属性：
- 页面类型：FIL_PAGE_TYPE_ENCRYPTED
- 类型编码：0x000C（十进制12）
- 所属模块：InnoDB数据加密
- 页面作用：存储经过加密的页面数据

使用场景：
1. 当开启透明数据加密(TDE)功能时，InnoDB会将页面数据进行加密存储
2. 保护敏感数据，防止磁盘文件被非法访问
3. 在内存中自动解密，应用程序透明访问

页面结构：
- File Header (38字节)
- Encryption Header (32字节):
  - Encryption Algorithm: 加密算法类型 (2字节)
  - Key Version: 密钥版本 (4字节)
  - IV: 初始化向量 (16字节)
  - Checksum: 加密数据校验和 (4字节)
  - Key ID: 密钥ID (4字节)
  - Reserved: 保留字段 (2字节)
- Encrypted Data (16306字节): 加密后的数据
- File Trailer (8字节)

加密算法：
- AES-128-CBC: 128位AES CBC模式
- AES-256-CBC: 256位AES CBC模式
- AES-128-CTR: 128位AES CTR模式
- AES-256-CTR: 256位AES CTR模式
*/

package pages

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"xmysql-server/server/common"
)

// 加密页面常量
const (
	EncryptionHeaderSize = 32    // 加密头部大小
	MaxEncryptedDataSize = 16306 // 最大加密数据大小 (16384 - 38 - 32 - 8)
	IVSize               = 16    // 初始化向量大小
)

// 加密算法类型
type EncryptionAlgorithm uint16

const (
	EncryptionNone      EncryptionAlgorithm = 0
	EncryptionAES128CBC EncryptionAlgorithm = 1
	EncryptionAES256CBC EncryptionAlgorithm = 2
	EncryptionAES128CTR EncryptionAlgorithm = 3
	EncryptionAES256CTR EncryptionAlgorithm = 4
)

// 加密页面错误定义
var (
	ErrEncryptionFailed      = errors.New("加密失败")
	ErrDecryptionFailed      = errors.New("解密失败")
	ErrUnsupportedEncryption = errors.New("不支持的加密算法")
	ErrInvalidEncryptedData  = errors.New("无效的加密数据")
	ErrInvalidKey            = errors.New("无效的密钥")
	ErrInvalidIV             = errors.New("无效的初始化向量")
)

// EncryptionHeader 加密页面头部结构
type EncryptionHeader struct {
	Algorithm  EncryptionAlgorithm // 加密算法类型
	KeyVersion uint32              // 密钥版本
	IV         [IVSize]byte        // 初始化向量
	Checksum   uint32              // 加密数据校验和
	KeyID      uint32              // 密钥ID
	Reserved   uint16              // 保留字段
}

// EncryptedPage 加密页面结构
type EncryptedPage struct {
	FileHeader       FileHeader       // 文件头部 (38字节)
	EncryptionHeader EncryptionHeader // 加密头部 (32字节)
	EncryptedData    []byte           // 加密数据 (16306字节)
	FileTrailer      FileTrailer      // 文件尾部 (8字节)
}

// NewEncryptedPage 创建新的加密页面
func NewEncryptedPage(spaceID, pageNo uint32, algorithm EncryptionAlgorithm, keyID uint32, keyVersion uint32) *EncryptedPage {
	page := &EncryptedPage{
		FileHeader: NewFileHeader(),
		EncryptionHeader: EncryptionHeader{
			Algorithm:  algorithm,
			KeyVersion: keyVersion,
			Checksum:   0,
			KeyID:      keyID,
			Reserved:   0,
		},
		EncryptedData: make([]byte, MaxEncryptedDataSize),
		FileTrailer:   NewFileTrailer(),
	}

	// 生成随机IV
	_, err := rand.Read(page.EncryptionHeader.IV[:])
	if err != nil {
		// 如果生成随机IV失败，使用零值IV（不安全，仅用于兼容性）
		for i := range page.EncryptionHeader.IV {
			page.EncryptionHeader.IV[i] = 0
		}
	}

	// 设置页面头部信息
	page.FileHeader.WritePageOffset(pageNo)
	page.FileHeader.WritePageFileType(int16(common.FIL_PAGE_TYPE_ENCRYPTED))
	page.FileHeader.WritePageArch(spaceID)

	return page
}

// EncryptData 加密数据
func (ep *EncryptedPage) EncryptData(originalData []byte, key []byte) error {
	if len(originalData) == 0 {
		return ErrInvalidEncryptedData
	}

	if len(originalData) > MaxEncryptedDataSize {
		return ErrInvalidEncryptedData
	}

	var encryptedData []byte
	var err error

	switch ep.EncryptionHeader.Algorithm {
	case EncryptionAES128CBC:
		if len(key) != 16 {
			return ErrInvalidKey
		}
		encryptedData, err = ep.encryptWithAESCBC(originalData, key)
	case EncryptionAES256CBC:
		if len(key) != 32 {
			return ErrInvalidKey
		}
		encryptedData, err = ep.encryptWithAESCBC(originalData, key)
	case EncryptionAES128CTR:
		if len(key) != 16 {
			return ErrInvalidKey
		}
		encryptedData, err = ep.encryptWithAESCTR(originalData, key)
	case EncryptionAES256CTR:
		if len(key) != 32 {
			return ErrInvalidKey
		}
		encryptedData, err = ep.encryptWithAESCTR(originalData, key)
	default:
		return ErrUnsupportedEncryption
	}

	if err != nil {
		return err
	}

	// 计算校验和
	ep.EncryptionHeader.Checksum = ep.calculateChecksum(encryptedData)

	// 存储加密数据，确保填充到固定大小
	for i := range ep.EncryptedData {
		ep.EncryptedData[i] = 0
	}
	copy(ep.EncryptedData, encryptedData)

	return nil
}

// DecryptData 解密数据
func (ep *EncryptedPage) DecryptData(key []byte) ([]byte, error) {
	if len(ep.EncryptedData) == 0 {
		return nil, ErrInvalidEncryptedData
	}

	// 验证校验和
	if ep.calculateChecksum(ep.EncryptedData) != ep.EncryptionHeader.Checksum {
		return nil, ErrInvalidEncryptedData
	}

	var originalData []byte
	var err error

	switch ep.EncryptionHeader.Algorithm {
	case EncryptionAES128CBC:
		if len(key) != 16 {
			return nil, ErrInvalidKey
		}
		originalData, err = ep.decryptWithAESCBC(ep.EncryptedData, key)
	case EncryptionAES256CBC:
		if len(key) != 32 {
			return nil, ErrInvalidKey
		}
		originalData, err = ep.decryptWithAESCBC(ep.EncryptedData, key)
	case EncryptionAES128CTR:
		if len(key) != 16 {
			return nil, ErrInvalidKey
		}
		originalData, err = ep.decryptWithAESCTR(ep.EncryptedData, key)
	case EncryptionAES256CTR:
		if len(key) != 32 {
			return nil, ErrInvalidKey
		}
		originalData, err = ep.decryptWithAESCTR(ep.EncryptedData, key)
	default:
		return nil, ErrUnsupportedEncryption
	}

	if err != nil {
		return nil, err
	}

	return originalData, nil
}

// encryptWithAESCBC 使用AES CBC模式加密数据
func (ep *EncryptedPage) encryptWithAESCBC(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrEncryptionFailed
	}

	// PKCS7填充
	paddedData := ep.pkcs7Pad(data, block.BlockSize())

	ciphertext := make([]byte, len(paddedData))
	mode := cipher.NewCBCEncrypter(block, ep.EncryptionHeader.IV[:])
	mode.CryptBlocks(ciphertext, paddedData)

	return ciphertext, nil
}

// decryptWithAESCBC 使用AES CBC模式解密数据
func (ep *EncryptedPage) decryptWithAESCBC(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	if len(data)%block.BlockSize() != 0 {
		return nil, ErrDecryptionFailed
	}

	plaintext := make([]byte, len(data))
	mode := cipher.NewCBCDecrypter(block, ep.EncryptionHeader.IV[:])
	mode.CryptBlocks(plaintext, data)

	// 移除PKCS7填充
	unpaddedData, err := ep.pkcs7Unpad(plaintext)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return unpaddedData, nil
}

// encryptWithAESCTR 使用AES CTR模式加密数据
func (ep *EncryptedPage) encryptWithAESCTR(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrEncryptionFailed
	}

	ciphertext := make([]byte, len(data))
	stream := cipher.NewCTR(block, ep.EncryptionHeader.IV[:])
	stream.XORKeyStream(ciphertext, data)

	return ciphertext, nil
}

// decryptWithAESCTR 使用AES CTR模式解密数据
func (ep *EncryptedPage) decryptWithAESCTR(data []byte, key []byte) ([]byte, error) {
	// CTR模式解密和加密使用相同的操作
	return ep.encryptWithAESCTR(data, key)
}

// pkcs7Pad 添加PKCS7填充
func (ep *EncryptedPage) pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	padtext := make([]byte, padding)
	for i := range padtext {
		padtext[i] = byte(padding)
	}
	return append(data, padtext...)
}

// pkcs7Unpad 移除PKCS7填充
func (ep *EncryptedPage) pkcs7Unpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, ErrDecryptionFailed
	}

	padding := int(data[len(data)-1])
	if padding < 1 || padding > len(data) {
		return nil, ErrDecryptionFailed
	}

	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, ErrDecryptionFailed
		}
	}

	return data[:len(data)-padding], nil
}

// calculateChecksum 计算校验和
func (ep *EncryptedPage) calculateChecksum(data []byte) uint32 {
	// 简单的校验和计算，实际应该使用更复杂的算法
	var checksum uint32
	for _, b := range data {
		checksum += uint32(b)
	}
	return checksum
}

// GetEncryptionAlgorithm 获取加密算法
func (ep *EncryptedPage) GetEncryptionAlgorithm() EncryptionAlgorithm {
	return ep.EncryptionHeader.Algorithm
}

// GetKeyID 获取密钥ID
func (ep *EncryptedPage) GetKeyID() uint32 {
	return ep.EncryptionHeader.KeyID
}

// GetKeyVersion 获取密钥版本
func (ep *EncryptedPage) GetKeyVersion() uint32 {
	return ep.EncryptionHeader.KeyVersion
}

// GetIV 获取初始化向量
func (ep *EncryptedPage) GetIV() [IVSize]byte {
	return ep.EncryptionHeader.IV
}

// SetKeyVersion 设置密钥版本
func (ep *EncryptedPage) SetKeyVersion(version uint32) {
	ep.EncryptionHeader.KeyVersion = version
}

// Serialize 序列化页面为字节数组
func (ep *EncryptedPage) Serialize() []byte {
	data := make([]byte, common.PageSize)
	offset := 0

	// 序列化文件头部
	copy(data[offset:], ep.serializeFileHeader())
	offset += FileHeaderSize

	// 序列化加密头部
	copy(data[offset:], ep.serializeEncryptionHeader())
	offset += EncryptionHeaderSize

	// 序列化加密数据
	copy(data[offset:], ep.EncryptedData)
	offset += MaxEncryptedDataSize

	// 序列化文件尾部
	copy(data[offset:], ep.serializeFileTrailer())

	return data
}

// Deserialize 从字节数组反序列化页面
func (ep *EncryptedPage) Deserialize(data []byte) error {
	if len(data) != common.PageSize {
		return ErrInvalidPageSize
	}

	offset := 0

	// 反序列化文件头部
	if err := ep.deserializeFileHeader(data[offset : offset+FileHeaderSize]); err != nil {
		return err
	}
	offset += FileHeaderSize

	// 反序列化加密头部
	if err := ep.deserializeEncryptionHeader(data[offset : offset+EncryptionHeaderSize]); err != nil {
		return err
	}
	offset += EncryptionHeaderSize

	// 反序列化加密数据
	copy(ep.EncryptedData, data[offset:offset+MaxEncryptedDataSize])
	offset += MaxEncryptedDataSize

	// 反序列化文件尾部
	if err := ep.deserializeFileTrailer(data[offset : offset+FileTrailerSize]); err != nil {
		return err
	}

	return nil
}

// serializeFileHeader 序列化文件头部
func (ep *EncryptedPage) serializeFileHeader() []byte {
	// 实现文件头部序列化逻辑
	data := make([]byte, FileHeaderSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// serializeEncryptionHeader 序列化加密头部
func (ep *EncryptedPage) serializeEncryptionHeader() []byte {
	data := make([]byte, EncryptionHeaderSize)

	binary.LittleEndian.PutUint16(data[0:], uint16(ep.EncryptionHeader.Algorithm))
	binary.LittleEndian.PutUint32(data[2:], ep.EncryptionHeader.KeyVersion)
	copy(data[6:22], ep.EncryptionHeader.IV[:])
	binary.LittleEndian.PutUint32(data[22:], ep.EncryptionHeader.Checksum)
	binary.LittleEndian.PutUint32(data[26:], ep.EncryptionHeader.KeyID)
	binary.LittleEndian.PutUint16(data[30:], ep.EncryptionHeader.Reserved)

	return data
}

// serializeFileTrailer 序列化文件尾部
func (ep *EncryptedPage) serializeFileTrailer() []byte {
	// 实现文件尾部序列化逻辑
	data := make([]byte, FileTrailerSize)
	// 这里应该包含具体的序列化逻辑
	return data
}

// deserializeFileHeader 反序列化文件头部
func (ep *EncryptedPage) deserializeFileHeader(data []byte) error {
	// 实现文件头部反序列化逻辑
	return nil
}

// deserializeEncryptionHeader 反序列化加密头部
func (ep *EncryptedPage) deserializeEncryptionHeader(data []byte) error {
	if len(data) < EncryptionHeaderSize {
		return ErrInvalidPageSize
	}

	ep.EncryptionHeader.Algorithm = EncryptionAlgorithm(binary.LittleEndian.Uint16(data[0:]))
	ep.EncryptionHeader.KeyVersion = binary.LittleEndian.Uint32(data[2:])
	copy(ep.EncryptionHeader.IV[:], data[6:22])
	ep.EncryptionHeader.Checksum = binary.LittleEndian.Uint32(data[22:])
	ep.EncryptionHeader.KeyID = binary.LittleEndian.Uint32(data[26:])
	ep.EncryptionHeader.Reserved = binary.LittleEndian.Uint16(data[30:])

	return nil
}

// deserializeFileTrailer 反序列化文件尾部
func (ep *EncryptedPage) deserializeFileTrailer(data []byte) error {
	// 实现文件尾部反序列化逻辑
	return nil
}

// Validate 验证页面数据完整性
func (ep *EncryptedPage) Validate() error {
	// 验证加密算法
	if ep.EncryptionHeader.Algorithm == EncryptionNone {
		return ErrInvalidEncryptedData
	}

	// 验证密钥ID
	if ep.EncryptionHeader.KeyID == 0 {
		return ErrInvalidEncryptedData
	}

	// 验证校验和
	if ep.calculateChecksum(ep.EncryptedData) != ep.EncryptionHeader.Checksum {
		return ErrInvalidEncryptedData
	}

	return nil
}

// GetFileHeader 获取文件头部
func (ep *EncryptedPage) GetFileHeader() FileHeader {
	return ep.FileHeader
}

// GetFileTrailer 获取文件尾部
func (ep *EncryptedPage) GetFileTrailer() FileTrailer {
	return ep.FileTrailer
}

// GetSerializeBytes 获取序列化后的字节数组
func (ep *EncryptedPage) GetSerializeBytes() []byte {
	return ep.Serialize()
}

// LoadFileHeader 加载文件头部
func (ep *EncryptedPage) LoadFileHeader(content []byte) {
	ep.deserializeFileHeader(content)
}

// LoadFileTrailer 加载文件尾部
func (ep *EncryptedPage) LoadFileTrailer(content []byte) {
	ep.deserializeFileTrailer(content)
}
