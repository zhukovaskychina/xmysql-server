package protocol

import (
	"fmt"
	"strings"
)

// MySQL字符集和校对规则管理
// 参考: https://dev.mysql.com/doc/internals/en/character-set.html

// CharsetInfo 字符集信息
type CharsetInfo struct {
	ID          uint8  // 字符集ID
	Name        string // 字符集名称
	Collation   string // 默认校对规则
	MaxLen      uint8  // 最大字节长度
	Description string // 描述
}

// CollationInfo 校对规则信息
type CollationInfo struct {
	ID        uint16 // 校对规则ID
	Name      string // 校对规则名称
	Charset   string // 所属字符集
	IsDefault bool   // 是否为默认校对规则
}

// CharsetManager 字符集管理器
type CharsetManager struct {
	charsets   map[uint8]*CharsetInfo
	collations map[uint16]*CollationInfo
	nameToID   map[string]uint8
}

// NewCharsetManager 创建字符集管理器
func NewCharsetManager() *CharsetManager {
	cm := &CharsetManager{
		charsets:   make(map[uint8]*CharsetInfo),
		collations: make(map[uint16]*CollationInfo),
		nameToID:   make(map[string]uint8),
	}
	cm.initCharsets()
	cm.initCollations()
	return cm
}

// initCharsets 初始化字符集
func (cm *CharsetManager) initCharsets() {
	charsets := []*CharsetInfo{
		{ID: 1, Name: "big5", Collation: "big5_chinese_ci", MaxLen: 2, Description: "Big5 Traditional Chinese"},
		{ID: 2, Name: "latin2", Collation: "latin2_czech_cs", MaxLen: 1, Description: "Central European"},
		{ID: 3, Name: "dec8", Collation: "dec8_swedish_ci", MaxLen: 1, Description: "DEC West European"},
		{ID: 4, Name: "cp850", Collation: "cp850_general_ci", MaxLen: 1, Description: "DOS West European"},
		{ID: 5, Name: "latin1", Collation: "latin1_german1_ci", MaxLen: 1, Description: "West European"},
		{ID: 6, Name: "hp8", Collation: "hp8_english_ci", MaxLen: 1, Description: "HP West European"},
		{ID: 7, Name: "koi8r", Collation: "koi8r_general_ci", MaxLen: 1, Description: "KOI8-R Relcom Russian"},
		{ID: 8, Name: "latin1", Collation: "latin1_swedish_ci", MaxLen: 1, Description: "West European"},
		{ID: 9, Name: "latin2", Collation: "latin2_general_ci", MaxLen: 1, Description: "Central European"},
		{ID: 10, Name: "swe7", Collation: "swe7_swedish_ci", MaxLen: 1, Description: "7bit Swedish"},
		{ID: 11, Name: "ascii", Collation: "ascii_general_ci", MaxLen: 1, Description: "US ASCII"},
		{ID: 12, Name: "ujis", Collation: "ujis_japanese_ci", MaxLen: 3, Description: "EUC-JP Japanese"},
		{ID: 13, Name: "sjis", Collation: "sjis_japanese_ci", MaxLen: 2, Description: "Shift-JIS Japanese"},
		{ID: 16, Name: "hebrew", Collation: "hebrew_general_ci", MaxLen: 1, Description: "ISO 8859-8 Hebrew"},
		{ID: 18, Name: "tis620", Collation: "tis620_thai_ci", MaxLen: 1, Description: "TIS620 Thai"},
		{ID: 19, Name: "euckr", Collation: "euckr_korean_ci", MaxLen: 2, Description: "EUC-KR Korean"},
		{ID: 22, Name: "koi8u", Collation: "koi8u_general_ci", MaxLen: 1, Description: "KOI8-U Ukrainian"},
		{ID: 24, Name: "gb2312", Collation: "gb2312_chinese_ci", MaxLen: 2, Description: "GB2312 Simplified Chinese"},
		{ID: 25, Name: "greek", Collation: "greek_general_ci", MaxLen: 1, Description: "ISO 8859-7 Greek"},
		{ID: 26, Name: "cp1250", Collation: "cp1250_general_ci", MaxLen: 1, Description: "Windows Central European"},
		{ID: 28, Name: "gbk", Collation: "gbk_chinese_ci", MaxLen: 2, Description: "GBK Simplified Chinese"},
		{ID: 30, Name: "latin5", Collation: "latin5_turkish_ci", MaxLen: 1, Description: "ISO 8859-9 Turkish"},
		{ID: 32, Name: "armscii8", Collation: "armscii8_general_ci", MaxLen: 1, Description: "ARMSCII-8 Armenian"},
		{ID: 33, Name: "utf8", Collation: "utf8_general_ci", MaxLen: 3, Description: "UTF-8 Unicode"},
		{ID: 35, Name: "ucs2", Collation: "ucs2_general_ci", MaxLen: 2, Description: "UCS-2 Unicode"},
		{ID: 36, Name: "cp866", Collation: "cp866_general_ci", MaxLen: 1, Description: "DOS Russian"},
		{ID: 37, Name: "keybcs2", Collation: "keybcs2_general_ci", MaxLen: 1, Description: "DOS Kamenicky Czech-Slovak"},
		{ID: 38, Name: "macce", Collation: "macce_general_ci", MaxLen: 1, Description: "Mac Central European"},
		{ID: 39, Name: "macroman", Collation: "macroman_general_ci", MaxLen: 1, Description: "Mac West European"},
		{ID: 40, Name: "cp852", Collation: "cp852_general_ci", MaxLen: 1, Description: "DOS Central European"},
		{ID: 41, Name: "latin7", Collation: "latin7_general_ci", MaxLen: 1, Description: "ISO 8859-13 Baltic"},
		{ID: 45, Name: "utf8mb4", Collation: "utf8mb4_general_ci", MaxLen: 4, Description: "UTF-8 Unicode"},
		{ID: 46, Name: "utf8mb4", Collation: "utf8mb4_bin", MaxLen: 4, Description: "UTF-8 Unicode"},
		{ID: 51, Name: "cp1251", Collation: "cp1251_general_ci", MaxLen: 1, Description: "Windows Cyrillic"},
		{ID: 54, Name: "utf16", Collation: "utf16_general_ci", MaxLen: 4, Description: "UTF-16 Unicode"},
		{ID: 56, Name: "utf16le", Collation: "utf16le_general_ci", MaxLen: 4, Description: "UTF-16LE Unicode"},
		{ID: 57, Name: "cp1256", Collation: "cp1256_general_ci", MaxLen: 1, Description: "Windows Arabic"},
		{ID: 59, Name: "cp1257", Collation: "cp1257_general_ci", MaxLen: 1, Description: "Windows Baltic"},
		{ID: 60, Name: "utf32", Collation: "utf32_general_ci", MaxLen: 4, Description: "UTF-32 Unicode"},
		{ID: 63, Name: "binary", Collation: "binary", MaxLen: 1, Description: "Binary pseudo charset"},
		{ID: 83, Name: "utf8", Collation: "utf8_bin", MaxLen: 3, Description: "UTF-8 Unicode"},
		{ID: 92, Name: "geostd8", Collation: "geostd8_general_ci", MaxLen: 1, Description: "GEOSTD8 Georgian"},
		{ID: 95, Name: "cp932", Collation: "cp932_japanese_ci", MaxLen: 2, Description: "SJIS for Windows Japanese"},
		{ID: 97, Name: "eucjpms", Collation: "eucjpms_japanese_ci", MaxLen: 3, Description: "UJIS for Windows Japanese"},
		{ID: 224, Name: "utf8mb4", Collation: "utf8mb4_unicode_ci", MaxLen: 4, Description: "UTF-8 Unicode"},
		{ID: 255, Name: "utf8mb4", Collation: "utf8mb4_0900_ai_ci", MaxLen: 4, Description: "UTF-8 Unicode"},
	}

	for _, cs := range charsets {
		cm.charsets[cs.ID] = cs
		// 使用第一个出现的ID作为名称映射
		if _, exists := cm.nameToID[cs.Name]; !exists {
			cm.nameToID[cs.Name] = cs.ID
		}
	}
}

// initCollations 初始化校对规则
func (cm *CharsetManager) initCollations() {
	collations := []*CollationInfo{
		{ID: 1, Name: "big5_chinese_ci", Charset: "big5", IsDefault: true},
		{ID: 8, Name: "latin1_swedish_ci", Charset: "latin1", IsDefault: true},
		{ID: 9, Name: "latin2_general_ci", Charset: "latin2", IsDefault: true},
		{ID: 11, Name: "ascii_general_ci", Charset: "ascii", IsDefault: true},
		{ID: 13, Name: "sjis_japanese_ci", Charset: "sjis", IsDefault: true},
		{ID: 19, Name: "euckr_korean_ci", Charset: "euckr", IsDefault: true},
		{ID: 24, Name: "gb2312_chinese_ci", Charset: "gb2312", IsDefault: true},
		{ID: 25, Name: "greek_general_ci", Charset: "greek", IsDefault: true},
		{ID: 26, Name: "cp1250_general_ci", Charset: "cp1250", IsDefault: true},
		{ID: 28, Name: "gbk_chinese_ci", Charset: "gbk", IsDefault: true},
		{ID: 33, Name: "utf8_general_ci", Charset: "utf8", IsDefault: true},
		{ID: 45, Name: "utf8mb4_general_ci", Charset: "utf8mb4", IsDefault: false},
		{ID: 46, Name: "utf8mb4_bin", Charset: "utf8mb4", IsDefault: false},
		{ID: 63, Name: "binary", Charset: "binary", IsDefault: true},
		{ID: 83, Name: "utf8_bin", Charset: "utf8", IsDefault: false},
		{ID: 224, Name: "utf8mb4_unicode_ci", Charset: "utf8mb4", IsDefault: false},
		{ID: 255, Name: "utf8mb4_0900_ai_ci", Charset: "utf8mb4", IsDefault: true},
	}

	for _, col := range collations {
		cm.collations[col.ID] = col
	}
}

// GetCharsetByID 根据ID获取字符集
func (cm *CharsetManager) GetCharsetByID(id uint8) (*CharsetInfo, error) {
	cs, exists := cm.charsets[id]
	if !exists {
		return nil, fmt.Errorf("charset ID %d not found", id)
	}
	return cs, nil
}

// GetCharsetByName 根据名称获取字符集
func (cm *CharsetManager) GetCharsetByName(name string) (*CharsetInfo, error) {
	name = strings.ToLower(name)
	id, exists := cm.nameToID[name]
	if !exists {
		return nil, fmt.Errorf("charset '%s' not found", name)
	}
	return cm.GetCharsetByID(id)
}

// GetCollationByID 根据ID获取校对规则
func (cm *CharsetManager) GetCollationByID(id uint16) (*CollationInfo, error) {
	col, exists := cm.collations[id]
	if !exists {
		return nil, fmt.Errorf("collation ID %d not found", id)
	}
	return col, nil
}

// GetDefaultCollation 获取字符集的默认校对规则
func (cm *CharsetManager) GetDefaultCollation(charsetName string) (*CollationInfo, error) {
	charsetName = strings.ToLower(charsetName)

	for _, col := range cm.collations {
		if strings.ToLower(col.Charset) == charsetName && col.IsDefault {
			return col, nil
		}
	}

	return nil, fmt.Errorf("no default collation found for charset '%s'", charsetName)
}

// GetCharsetIDByName 根据名称获取字符集ID
func (cm *CharsetManager) GetCharsetIDByName(name string) (uint8, error) {
	name = strings.ToLower(name)
	id, exists := cm.nameToID[name]
	if !exists {
		return 0, fmt.Errorf("charset '%s' not found", name)
	}
	return id, nil
}

// IsValidCharset 检查字符集是否有效
func (cm *CharsetManager) IsValidCharset(id uint8) bool {
	_, exists := cm.charsets[id]
	return exists
}

// IsValidCollation 检查校对规则是否有效
func (cm *CharsetManager) IsValidCollation(id uint16) bool {
	_, exists := cm.collations[id]
	return exists
}

// GetAllCharsets 获取所有字符集
func (cm *CharsetManager) GetAllCharsets() []*CharsetInfo {
	var result []*CharsetInfo
	for _, cs := range cm.charsets {
		result = append(result, cs)
	}
	return result
}

// GetAllCollations 获取所有校对规则
func (cm *CharsetManager) GetAllCollations() []*CollationInfo {
	var result []*CollationInfo
	for _, col := range cm.collations {
		result = append(result, col)
	}
	return result
}

// GetCollationsByCharset 获取指定字符集的所有校对规则
func (cm *CharsetManager) GetCollationsByCharset(charsetName string) []*CollationInfo {
	charsetName = strings.ToLower(charsetName)
	var result []*CollationInfo

	for _, col := range cm.collations {
		if strings.ToLower(col.Charset) == charsetName {
			result = append(result, col)
		}
	}

	return result
}

// ConvertCharsetName 转换字符集名称（处理别名）
func (cm *CharsetManager) ConvertCharsetName(name string) string {
	name = strings.ToLower(name)

	// 处理常见别名
	aliases := map[string]string{
		"utf-8":    "utf8",
		"utf8mb3":  "utf8",
		"utf-8mb4": "utf8mb4",
	}

	if alias, exists := aliases[name]; exists {
		return alias
	}

	return name
}

// GetCharsetMaxLength 获取字符集的最大字节长度
func (cm *CharsetManager) GetCharsetMaxLength(id uint8) (uint8, error) {
	cs, err := cm.GetCharsetByID(id)
	if err != nil {
		return 0, err
	}
	return cs.MaxLen, nil
}

// IsMultiByteCharset 判断是否为多字节字符集
func (cm *CharsetManager) IsMultiByteCharset(id uint8) bool {
	cs, err := cm.GetCharsetByID(id)
	if err != nil {
		return false
	}
	return cs.MaxLen > 1
}

// GetUTF8CharsetID 获取UTF-8字符集ID
func (cm *CharsetManager) GetUTF8CharsetID() uint8 {
	return 33 // utf8_general_ci
}

// GetUTF8MB4CharsetID 获取UTF-8MB4字符集ID
func (cm *CharsetManager) GetUTF8MB4CharsetID() uint8 {
	return 255 // utf8mb4_0900_ai_ci (MySQL 8.0默认)
}

// GetBinaryCharsetID 获取Binary字符集ID
func (cm *CharsetManager) GetBinaryCharsetID() uint8 {
	return 63 // binary
}

// GetLatin1CharsetID 获取Latin1字符集ID
func (cm *CharsetManager) GetLatin1CharsetID() uint8 {
	return 8 // latin1_swedish_ci
}

// CharsetConverter 字符集转换器
type CharsetConverter struct {
	manager *CharsetManager
}

// NewCharsetConverter 创建字符集转换器
func NewCharsetConverter(manager *CharsetManager) *CharsetConverter {
	return &CharsetConverter{
		manager: manager,
	}
}

// Convert 转换字符集（简化实现，实际需要使用iconv或类似库）
func (cc *CharsetConverter) Convert(data []byte, fromCharset, toCharset uint8) ([]byte, error) {
	// 简化实现：如果源和目标字符集相同，直接返回
	if fromCharset == toCharset {
		return data, nil
	}

	// 实际实现需要使用字符集转换库
	// 这里只做基本验证
	_, err := cc.manager.GetCharsetByID(fromCharset)
	if err != nil {
		return nil, fmt.Errorf("invalid source charset: %w", err)
	}

	_, err = cc.manager.GetCharsetByID(toCharset)
	if err != nil {
		return nil, fmt.Errorf("invalid target charset: %w", err)
	}

	// TODO: 实现实际的字符集转换
	// 可以使用 golang.org/x/text/encoding 包
	return data, nil
}

// ValidateString 验证字符串是否符合指定字符集
func (cc *CharsetConverter) ValidateString(data []byte, charset uint8) error {
	cs, err := cc.manager.GetCharsetByID(charset)
	if err != nil {
		return err
	}

	// 简化实现：只检查UTF-8
	if cs.Name == "utf8" || cs.Name == "utf8mb4" {
		// TODO: 实现UTF-8验证
		// 可以使用 utf8.Valid(data)
	}

	return nil
}

// 全局字符集管理器实例
var globalCharsetManager *CharsetManager

// GetGlobalCharsetManager 获取全局字符集管理器
func GetGlobalCharsetManager() *CharsetManager {
	if globalCharsetManager == nil {
		globalCharsetManager = NewCharsetManager()
	}
	return globalCharsetManager
}
