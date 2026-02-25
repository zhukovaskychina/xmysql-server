package protocol

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// MySQL连接属性（CLIENT_CONNECT_ATTRS）
// 参考: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse

// ConnectionAttributes 连接属性
type ConnectionAttributes struct {
	attributes map[string]string
}

// NewConnectionAttributes 创建连接属性
func NewConnectionAttributes() *ConnectionAttributes {
	return &ConnectionAttributes{
		attributes: make(map[string]string),
	}
}

// Set 设置属性
func (ca *ConnectionAttributes) Set(key, value string) {
	ca.attributes[key] = value
}

// Get 获取属性
func (ca *ConnectionAttributes) Get(key string) (string, bool) {
	value, exists := ca.attributes[key]
	return value, exists
}

// GetAll 获取所有属性
func (ca *ConnectionAttributes) GetAll() map[string]string {
	result := make(map[string]string)
	for k, v := range ca.attributes {
		result[k] = v
	}
	return result
}

// Has 检查属性是否存在
func (ca *ConnectionAttributes) Has(key string) bool {
	_, exists := ca.attributes[key]
	return exists
}

// Delete 删除属性
func (ca *ConnectionAttributes) Delete(key string) {
	delete(ca.attributes, key)
}

// Count 获取属性数量
func (ca *ConnectionAttributes) Count() int {
	return len(ca.attributes)
}

// Clear 清空所有属性
func (ca *ConnectionAttributes) Clear() {
	ca.attributes = make(map[string]string)
}

// ParseConnectionAttributes 解析连接属性
// 格式：length-encoded integer (总长度) + 多个 key-value 对
// 每个 key-value 对：length-encoded string (key) + length-encoded string (value)
func ParseConnectionAttributes(data []byte) (*ConnectionAttributes, int, error) {
	if len(data) == 0 {
		return NewConnectionAttributes(), 0, nil
	}

	attrs := NewConnectionAttributes()
	offset := 0

	// 读取总长度
	totalLength, bytesRead := readLengthEncodedInteger(data[offset:])
	if bytesRead == 0 {
		return nil, 0, fmt.Errorf("failed to read attributes length")
	}
	offset += bytesRead

	if totalLength == 0 {
		return attrs, offset, nil
	}

	// 验证数据长度
	if offset+int(totalLength) > len(data) {
		return nil, 0, fmt.Errorf("insufficient data for attributes: need %d, have %d",
			offset+int(totalLength), len(data))
	}

	// 读取所有属性
	endOffset := offset + int(totalLength)
	for offset < endOffset {
		// 读取key
		key, bytesRead := readLengthEncodedString(data[offset:])
		if bytesRead == 0 {
			return nil, 0, fmt.Errorf("failed to read attribute key at offset %d", offset)
		}
		offset += bytesRead

		// 读取value
		value, bytesRead := readLengthEncodedString(data[offset:])
		if bytesRead == 0 {
			return nil, 0, fmt.Errorf("failed to read attribute value at offset %d", offset)
		}
		offset += bytesRead

		attrs.Set(key, value)
	}

	return attrs, offset, nil
}

// EncodeConnectionAttributes 编码连接属性
func EncodeConnectionAttributes(attrs *ConnectionAttributes) []byte {
	if attrs == nil || attrs.Count() == 0 {
		// 返回长度为0的编码
		return []byte{0}
	}

	// 先计算所有属性的总长度
	var attrData []byte
	for key, value := range attrs.attributes {
		attrData = append(attrData, encodeLengthEncodedString(key)...)
		attrData = append(attrData, encodeLengthEncodedString(value)...)
	}

	// 编码总长度
	result := encodeLengthEncodedInteger(uint64(len(attrData)))
	result = append(result, attrData...)

	return result
}

// readLengthEncodedInteger 读取长度编码的整数
func readLengthEncodedInteger(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	first := data[0]
	if first < 0xfb {
		return uint64(first), 1
	}

	switch first {
	case 0xfc:
		if len(data) < 3 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8, 3
	case 0xfd:
		if len(data) < 4 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4
	case 0xfe:
		if len(data) < 9 {
			return 0, 0
		}
		return binary.LittleEndian.Uint64(data[1:9]), 9
	default:
		return 0, 0
	}
}

// readLengthEncodedString 读取长度编码的字符串
func readLengthEncodedString(data []byte) (string, int) {
	length, bytesRead := readLengthEncodedInteger(data)
	if bytesRead == 0 {
		return "", 0
	}

	if int(length) > len(data)-bytesRead {
		return "", 0
	}

	str := string(data[bytesRead : bytesRead+int(length)])
	return str, bytesRead + int(length)
}

// encodeLengthEncodedInteger 编码长度编码的整数
func encodeLengthEncodedInteger(value uint64) []byte {
	if value < 251 {
		return []byte{byte(value)}
	} else if value < 65536 {
		result := make([]byte, 3)
		result[0] = 0xfc
		result[1] = byte(value)
		result[2] = byte(value >> 8)
		return result
	} else if value < 16777216 {
		result := make([]byte, 4)
		result[0] = 0xfd
		result[1] = byte(value)
		result[2] = byte(value >> 8)
		result[3] = byte(value >> 16)
		return result
	} else {
		result := make([]byte, 9)
		result[0] = 0xfe
		binary.LittleEndian.PutUint64(result[1:], value)
		return result
	}
}

// encodeLengthEncodedString 编码长度编码的字符串
func encodeLengthEncodedString(str string) []byte {
	length := encodeLengthEncodedInteger(uint64(len(str)))
	return append(length, []byte(str)...)
}

// StandardConnectionAttributes 标准连接属性键名
const (
	AttrClientName    = "_client_name"    // 客户端名称
	AttrClientVersion = "_client_version" // 客户端版本
	AttrOS            = "_os"             // 操作系统
	AttrPlatform      = "_platform"       // 平台
	AttrPID           = "_pid"            // 进程ID
	AttrConnectionID  = "_connection_id"  // 连接ID
	AttrThreadID      = "_thread"         // 线程ID
	AttrClientLicense = "_client_license" // 客户端许可证
	AttrProgramName   = "program_name"    // 程序名称
)

// GetClientInfo 获取客户端信息
func (ca *ConnectionAttributes) GetClientInfo() ClientInfo {
	return ClientInfo{
		Name:     ca.getOrDefault(AttrClientName, "unknown"),
		Version:  ca.getOrDefault(AttrClientVersion, "unknown"),
		OS:       ca.getOrDefault(AttrOS, "unknown"),
		Platform: ca.getOrDefault(AttrPlatform, "unknown"),
		PID:      ca.getOrDefault(AttrPID, ""),
		Program:  ca.getOrDefault(AttrProgramName, ""),
	}
}

// getOrDefault 获取属性或返回默认值
func (ca *ConnectionAttributes) getOrDefault(key, defaultValue string) string {
	if value, exists := ca.attributes[key]; exists {
		return value
	}
	return defaultValue
}

// ClientInfo 客户端信息
type ClientInfo struct {
	Name     string
	Version  string
	OS       string
	Platform string
	PID      string
	Program  string
}

// String 返回客户端信息的字符串表示
func (ci ClientInfo) String() string {
	var parts []string

	if ci.Name != "unknown" {
		parts = append(parts, fmt.Sprintf("name=%s", ci.Name))
	}
	if ci.Version != "unknown" {
		parts = append(parts, fmt.Sprintf("version=%s", ci.Version))
	}
	if ci.OS != "unknown" {
		parts = append(parts, fmt.Sprintf("os=%s", ci.OS))
	}
	if ci.Platform != "unknown" {
		parts = append(parts, fmt.Sprintf("platform=%s", ci.Platform))
	}
	if ci.PID != "" {
		parts = append(parts, fmt.Sprintf("pid=%s", ci.PID))
	}
	if ci.Program != "" {
		parts = append(parts, fmt.Sprintf("program=%s", ci.Program))
	}

	return strings.Join(parts, ", ")
}

// ConnectionAttributesParser 连接属性解析器
type ConnectionAttributesParser struct{}

// NewConnectionAttributesParser 创建连接属性解析器
func NewConnectionAttributesParser() *ConnectionAttributesParser {
	return &ConnectionAttributesParser{}
}

// Parse 解析连接属性
func (cap *ConnectionAttributesParser) Parse(data []byte, offset int) (*ConnectionAttributes, int, error) {
	if offset >= len(data) {
		return NewConnectionAttributes(), 0, nil
	}

	return ParseConnectionAttributes(data[offset:])
}

// Encode 编码连接属性
func (cap *ConnectionAttributesParser) Encode(attrs *ConnectionAttributes) []byte {
	return EncodeConnectionAttributes(attrs)
}

// ValidateAttributes 验证连接属性
func (cap *ConnectionAttributesParser) ValidateAttributes(attrs *ConnectionAttributes) error {
	if attrs == nil {
		return fmt.Errorf("attributes is nil")
	}

	// 检查属性数量限制（MySQL限制为64KB）
	encoded := EncodeConnectionAttributes(attrs)
	if len(encoded) > 65535 {
		return fmt.Errorf("attributes too large: %d bytes (max 65535)", len(encoded))
	}

	// 检查每个key和value的长度
	for key, value := range attrs.attributes {
		if len(key) > 255 {
			return fmt.Errorf("attribute key too long: %d bytes (max 255)", len(key))
		}
		if len(value) > 65535 {
			return fmt.Errorf("attribute value too long: %d bytes (max 65535)", len(value))
		}
	}

	return nil
}

// FilterAttributes 过滤连接属性（只保留指定的key）
func FilterAttributes(attrs *ConnectionAttributes, keys []string) *ConnectionAttributes {
	filtered := NewConnectionAttributes()

	for _, key := range keys {
		if value, exists := attrs.Get(key); exists {
			filtered.Set(key, value)
		}
	}

	return filtered
}

// MergeAttributes 合并连接属性
func MergeAttributes(attrs1, attrs2 *ConnectionAttributes) *ConnectionAttributes {
	merged := NewConnectionAttributes()

	// 复制第一个属性集
	for key, value := range attrs1.attributes {
		merged.Set(key, value)
	}

	// 合并第二个属性集（覆盖重复的key）
	for key, value := range attrs2.attributes {
		merged.Set(key, value)
	}

	return merged
}

// GetStandardAttributes 获取标准属性列表
func GetStandardAttributes() []string {
	return []string{
		AttrClientName,
		AttrClientVersion,
		AttrOS,
		AttrPlatform,
		AttrPID,
		AttrConnectionID,
		AttrThreadID,
		AttrClientLicense,
		AttrProgramName,
	}
}

// IsStandardAttribute 检查是否为标准属性
func IsStandardAttribute(key string) bool {
	standardAttrs := GetStandardAttributes()
	for _, attr := range standardAttrs {
		if attr == key {
			return true
		}
	}
	return false
}
