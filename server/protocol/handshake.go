package protocol

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
)

// HandshakePacket MySQL握手包
type HandshakePacket struct {
	ProtocolVersion     byte
	ServerVersion       string
	ConnectionID        uint32
	AuthPluginDataPart1 []byte // 8字节
	Filler              byte   // 0x00
	CapabilityFlags1    uint16 // 低16位能力标志
	CharacterSet        byte
	StatusFlags         uint16
	CapabilityFlags2    uint16 // 高16位能力标志
	AuthPluginDataLen   byte
	Reserved            []byte // 10字节保留
	AuthPluginDataPart2 []byte // 12字节或更多
	AuthPluginName      string
}

// NewHandshakePacket 创建新的握手包
func NewHandshakePacket(connectionID uint32) *HandshakePacket {
	// 生成20字节的认证数据
	authData := make([]byte, 20)
	rand.Read(authData)

	// 确保没有null字节
	for i := range authData {
		if authData[i] == 0 {
			authData[i] = 1
		}
	}

	// 设置实际支持的能力标志（与历史版本保持一致，提升 JDBC 兼容性）
	// 低16位能力标志
	capFlags1 := uint16(0)
	capFlags1 |= uint16(common.CLIENT_LONG_PASSWORD)     // 支持新密码
	capFlags1 |= uint16(common.CLIENT_FOUND_ROWS)        // 返回找到的行数
	capFlags1 |= uint16(common.CLIENT_LONG_FLAG)         // 获取所有列标志
	capFlags1 |= uint16(common.CLIENT_CONNECT_WITH_DB)   // 连接时可指定数据库
	capFlags1 |= uint16(common.CLIENT_NO_SCHEMA)         // 不允许 database.table.column
	capFlags1 |= uint16(common.CLIENT_PROTOCOL_41)       // 使用4.1协议
	capFlags1 |= uint16(common.CLIENT_TRANSACTIONS)      // 支持事务
	capFlags1 |= uint16(common.CLIENT_SECURE_CONNECTION) // 支持安全连接

	// 高16位能力标志
	capFlags2 := uint16(0)
	capFlags2 |= uint16(common.CLIENT_MULTI_STATEMENTS >> 16) // 支持多语句
	capFlags2 |= uint16(common.CLIENT_MULTI_RESULTS >> 16)    // 支持多结果集
	capFlags2 |= uint16(common.CLIENT_PLUGIN_AUTH >> 16)      // 支持插件认证
	capFlags2 |= uint16(common.CLIENT_DEPRECATE_EOF >> 16)    // 不再需要EOF包

	return &HandshakePacket{
		ProtocolVersion:     10,
		ServerVersion:       "8.0.0-xmysql-server",
		ConnectionID:        connectionID,
		AuthPluginDataPart1: authData[:8],
		Filler:              0x00,

		CapabilityFlags1: capFlags1,
		CharacterSet:     0x21,   // utf8_general_ci
		StatusFlags:      0x0002, // SERVER_STATUS_AUTOCOMMIT

		CapabilityFlags2:    capFlags2,
		AuthPluginDataLen:   21,
		Reserved:            make([]byte, 10),
		AuthPluginDataPart2: authData[8:],
		AuthPluginName:      "mysql_native_password",
	}
}

// Encode 编码握手包
func (h *HandshakePacket) Encode() []byte {
	buf := make([]byte, 0, 128)

	// 协议版本
	buf = append(buf, h.ProtocolVersion)

	// 服务器版本（以null结尾）
	buf = append(buf, []byte(h.ServerVersion)...)
	buf = append(buf, 0x00)

	// 连接ID（4字节，小端序）
	connIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(connIDBytes, h.ConnectionID)
	buf = append(buf, connIDBytes...)

	// 认证插件数据第一部分（8字节）
	buf = append(buf, h.AuthPluginDataPart1...)

	// 填充字节
	buf = append(buf, h.Filler)

	// 能力标志低16位
	capFlags1Bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(capFlags1Bytes, h.CapabilityFlags1)
	buf = append(buf, capFlags1Bytes...)

	// 字符集
	buf = append(buf, h.CharacterSet)

	// 状态标志
	statusBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(statusBytes, h.StatusFlags)
	buf = append(buf, statusBytes...)

	// 能力标志高16位
	capFlags2Bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(capFlags2Bytes, h.CapabilityFlags2)
	buf = append(buf, capFlags2Bytes...)

	// 认证插件数据长度
	buf = append(buf, h.AuthPluginDataLen)

	// 保留字段（10字节）
	buf = append(buf, h.Reserved...)

	// 认证插件数据第二部分
	buf = append(buf, h.AuthPluginDataPart2...)
	buf = append(buf, 0x00) // null终止符

	// 认证插件名称（以null结尾）
	buf = append(buf, []byte(h.AuthPluginName)...)
	buf = append(buf, 0x00)

	return h.wrapWithPacketHeader(buf)
}

// wrapWithPacketHeader 包装MySQL包头
func (h *HandshakePacket) wrapWithPacketHeader(payload []byte) []byte {
	header := make([]byte, 4)

	// 包长度（3字节，小端序）
	length := len(payload)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 包序号（握手包序号为0）
	header[3] = 0x00

	return append(header, payload...)
}

// GetAuthData 获取完整的认证数据
func (h *HandshakePacket) GetAuthData() []byte {
	authData := make([]byte, 0, 20)
	authData = append(authData, h.AuthPluginDataPart1...)
	authData = append(authData, h.AuthPluginDataPart2...)
	return authData
}

// HandshakeGenerator 握手包生成器
type HandshakeGenerator struct {
	connectionCounter uint32
}

// NewHandshakeGenerator 创建握手包生成器
func NewHandshakeGenerator() *HandshakeGenerator {
	return &HandshakeGenerator{
		connectionCounter: 1,
	}
}

// GenerateHandshake 生成握手包
func (g *HandshakeGenerator) GenerateHandshake() (*HandshakePacket, error) {
	connectionID := g.connectionCounter
	g.connectionCounter++

	handshake := NewHandshakePacket(connectionID)
	return handshake, nil
}

// GenerateHandshakeWithChallenge 生成带指定挑战的握手包
func (g *HandshakeGenerator) GenerateHandshakeWithChallenge(challenge []byte) (*HandshakePacket, error) {
	if len(challenge) != 20 {
		return nil, fmt.Errorf("challenge must be 20 bytes, got %d", len(challenge))
	}

	connectionID := g.connectionCounter
	g.connectionCounter++

	handshake := &HandshakePacket{
		ProtocolVersion:     10,
		ServerVersion:       "8.0.0-xmysql-server",
		ConnectionID:        connectionID,
		AuthPluginDataPart1: challenge[:8],
		Filler:              0x00,
		CapabilityFlags1:    0xFFFF,
		CharacterSet:        0x21,
		StatusFlags:         0x0002,
		CapabilityFlags2:    0x807F,
		AuthPluginDataLen:   21,
		Reserved:            make([]byte, 10),
		AuthPluginDataPart2: challenge[8:],
		AuthPluginName:      "mysql_native_password",
	}

	return handshake, nil
}

// ============================================================================
// 旧版本兼容代码 (从 handshark.go 迁移)
// ============================================================================

const (
	ServerVersion57 = "5.7.32"
	ServerStatus    = 2
	CharSet         = 1
	ProtocolVersion = 10
)

// HandsharkProtocol 旧版本握手协议结构 (已废弃，使用 HandshakePacket 代替)
// Deprecated: 使用 HandshakePacket 代替
type HandsharkProtocol struct {
	MySQLPacket
	ProtocolVersion          byte
	ServerVersion            string
	ServerThreadID           uint32
	Seed                     []byte
	ServerCapabilitiesLow    uint16
	CharSet                  byte
	ServerStatus             uint16
	ServerCapabilitiesHeight uint16
	RestOfScrambleBuff       []byte
	Auth_plugin_name         string
}

// CalHandShakePacketSize 计算握手包大小
// Deprecated: 使用 HandshakePacket 代替
func CalHandShakePacketSize() int {
	size := 1
	size += len(ServerVersion57)
	size += 5
	size += 20
	size += 19
	size += 12
	size += 1
	return size
}

// DecodeHandshake 解码握手包
// Deprecated: 使用 HandshakePacket 代替
func DecodeHandshake(buff []byte) HandsharkProtocol {
	var cursor int
	var tmp []byte
	hs := new(HandsharkProtocol)

	cursor, hs.ProtocolVersion = util.ReadByte(buff, cursor)
	cursor, tmp = util.ReadWithNull(buff, cursor)
	hs.ServerVersion = string(tmp)
	cursor, hs.ServerThreadID = util.ReadUB4(buff, cursor)
	cursor, hs.Seed = util.ReadWithNull(buff, cursor)
	cursor, hs.ServerCapabilitiesLow = util.ReadUB2(buff, cursor)
	cursor, hs.CharSet = util.ReadByte(buff, cursor)
	cursor, hs.ServerStatus = util.ReadUB2(buff, cursor)
	cursor, hs.ServerCapabilitiesHeight = util.ReadUB2(buff, cursor)
	cursor, _ = util.ReadBytes(buff, cursor, 11)
	cursor, hs.RestOfScrambleBuff = util.ReadWithNull(buff, cursor)
	cursor, tmp = util.ReadWithNull(buff, cursor)
	hs.Auth_plugin_name = string(tmp)

	logger.Debugf("DecodeHandshake: %+v\n", hs)

	return *hs
}

// EncodeHandshake 编码握手包 (旧版本)
// Deprecated: 使用 HandshakePacket.Encode() 代替
func EncodeHandshake(buff []byte) []byte {
	ServerCapablities := GetCapabilitiesWithoutParams()
	Filler13 := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	//rand1
	rand1 := util.RandomBytes(8)
	rand2 := util.RandomBytes(12)

	size := CalHandShakePacketSize()
	buff = util.WriteUB3(buff, uint32(size))
	buff = util.WriteByte(buff, 0)
	buff = util.WriteByte(buff, ProtocolVersion)
	buff = util.WriteWithNull(buff, ([]byte)(ServerVersion57))
	buff = util.WriteUB4(buff, uint32(util.Goid()))
	buff = util.WriteWithNull(buff, append(rand1, rand2...))
	buff = util.WriteUB2(buff, uint16(ServerCapablities))
	buff = util.WriteByte(buff, CharSet)
	buff = util.WriteUB2(buff, ServerStatus)
	buff = util.WriteBytes(buff, Filler13)
	buff = util.WriteWithNull(buff, rand2)

	return buff
}
