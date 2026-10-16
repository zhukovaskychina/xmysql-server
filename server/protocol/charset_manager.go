package protocol

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/common"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/ianaindex"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	unicodeencoding "golang.org/x/text/encoding/unicode"
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
	charsets := make([]*CharsetInfo, 0)
	for _, record := range common.ProtocolManagerCharsets() {
		charsets = append(charsets, &CharsetInfo{ID: record.ID, Name: record.Name, Collation: record.Collation, MaxLen: record.MaxLen, Description: record.Description})
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
	collations := make([]*CollationInfo, 0)
	for _, record := range common.ProtocolManagerCollations() {
		collations = append(collations, &CollationInfo{ID: record.ID, Name: record.Name, Charset: record.Charset, IsDefault: record.IsDefault})
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
	return common.NormalizeProtocolCharsetName(name)
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
	return common.ProtocolPreferredCharsetID(common.UTF8Charset)
}

// GetUTF8MB4CharsetID 获取UTF-8MB4字符集ID
func (cm *CharsetManager) GetUTF8MB4CharsetID() uint8 {
	return common.ProtocolPreferredCharsetID(common.UTF8MB4Charset)
}

// GetBinaryCharsetID 获取Binary字符集ID
func (cm *CharsetManager) GetBinaryCharsetID() uint8 {
	return common.ProtocolPreferredCharsetID("binary")
}

// GetLatin1CharsetID 获取Latin1字符集ID
func (cm *CharsetManager) GetLatin1CharsetID() uint8 {
	return common.ProtocolPreferredCharsetID(common.Latin1Charset)
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

// Convert converts bytes between the supported MySQL character sets.
func (cc *CharsetConverter) Convert(data []byte, fromCharset, toCharset uint8) ([]byte, error) {
	if cc == nil || cc.manager == nil {
		return nil, fmt.Errorf("charset converter is not initialized")
	}
	if fromCharset == toCharset {
		return append([]byte(nil), data...), nil
	}
	from, err := cc.manager.GetCharsetByID(fromCharset)
	if err != nil {
		return nil, fmt.Errorf("invalid source charset: %w", err)
	}
	to, err := cc.manager.GetCharsetByID(toCharset)
	if err != nil {
		return nil, fmt.Errorf("invalid target charset: %w", err)
	}
	if strings.EqualFold(from.Name, "binary") || strings.EqualFold(to.Name, "binary") {
		return append([]byte(nil), data...), nil
	}
	fromEncoding, err := charsetEncoding(from.Name)
	if err != nil {
		return nil, fmt.Errorf("unsupported source charset %q: %w", from.Name, err)
	}
	toEncoding, err := charsetEncoding(to.Name)
	if err != nil {
		return nil, fmt.Errorf("unsupported target charset %q: %w", to.Name, err)
	}
	decoded, err := fromEncoding.NewDecoder().Bytes(data)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", from.Name, err)
	}
	converted, err := toEncoding.NewEncoder().Bytes(decoded)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", to.Name, err)
	}
	return converted, nil
}

// ValidateString 验证字符串是否符合指定字符集
func (cc *CharsetConverter) ValidateString(data []byte, charset uint8) error {
	cs, err := cc.manager.GetCharsetByID(charset)
	if err != nil {
		return err
	}

	if strings.EqualFold(cs.Name, "binary") {
		return nil
	}
	if strings.EqualFold(cs.Name, "utf8") || strings.EqualFold(cs.Name, "utf8mb4") {
		if !utf8.Valid(data) {
			return fmt.Errorf("invalid UTF-8 sequence")
		}
		return nil
	}
	encoder, err := charsetEncoding(cs.Name)
	if err != nil {
		return err
	}
	_, err = encoder.NewDecoder().Bytes(data)
	return err
}

func charsetEncoding(name string) (encoding.Encoding, error) {
	switch strings.ToLower(name) {
	case "utf8", "utf8mb4":
		return unicodeencoding.UTF8, nil
	case "ascii":
		return ianaindex.IANA.Encoding("US-ASCII")
	case "latin1":
		return charmap.ISO8859_1, nil
	case "latin2":
		return charmap.ISO8859_2, nil
	case "cp1250":
		return charmap.Windows1250, nil
	case "cp1251":
		return charmap.Windows1251, nil
	case "cp1256":
		return charmap.Windows1256, nil
	case "cp1257":
		return charmap.Windows1257, nil
	case "cp850":
		return charmap.CodePage850, nil
	case "gbk":
		return simplifiedchinese.GBK, nil
	case "gb2312":
		return simplifiedchinese.HZGB2312, nil
	case "big5":
		return traditionalchinese.Big5, nil
	case "euckr":
		return korean.EUCKR, nil
	case "sjis", "cp932":
		return japanese.ShiftJIS, nil
	default:
		return nil, fmt.Errorf("no Go encoding mapping")
	}
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
