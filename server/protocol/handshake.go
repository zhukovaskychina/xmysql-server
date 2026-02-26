package protocol

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
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

// NewHandshakePacket 创建符合 MySQL 8.0 协议的握手包
func NewHandshakePacket(connectionID uint32) *HandshakePacket {
	// 生成 20 字节随机 auth data（scramble）
	authData := make([]byte, 20)
	_, err := rand.Read(authData)
	if err != nil {
		// 极端情况下随机失败，退而求其次
		for i := range authData {
			authData[i] = byte(1 + i)
		}
	}

	// 保证里面没有 0 字节（防止客户端提前截断）
	for i := range authData {
		if authData[i] == 0 {
			authData[i] = 1
		}
	}

	// MySQL 8.0 推荐能力组合（简化版，足够支撑 JDBC 8.x）
	var caps uint32 = 0
	caps |= CLIENT_LONG_PASSWORD
	caps |= CLIENT_LONG_FLAG
	caps |= CLIENT_CONNECT_WITH_DB
	caps |= CLIENT_PROTOCOL_41
	caps |= CLIENT_TRANSACTIONS
	caps |= CLIENT_SECURE_CONNECTION
	caps |= CLIENT_MULTI_STATEMENTS
	caps |= CLIENT_MULTI_RESULTS
	caps |= CLIENT_PLUGIN_AUTH
	caps |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	caps |= CLIENT_DEPRECATE_EOF

	capFlags1 := uint16(caps & 0xFFFF)
	capFlags2 := uint16((caps >> 16) & 0xFFFF)

	// 注意：
	// MySQL 8.0 的 auth plugin data 长度通常是 21:
	// 8 (part1) + 12 (part2) + 1 终止符
	authPluginDataLen := byte(21)

	return &HandshakePacket{
		ProtocolVersion:     10,
		ServerVersion:       "8.0.32-xmysql-server", // 随便写，但建议 8.0.x
		ConnectionID:        connectionID,
		AuthPluginDataPart1: authData[:8],
		Filler:              0x00,

		CapabilityFlags1: capFlags1,

		// 这里写一个有效的 collation，跟 utf8/utf8mb4 兼容即可。
		// 0x21 是 utf8_general_ci（MySQL 官方值），Connector/J 完全认可。
		CharacterSet: 0x21,

		// SERVER_STATUS_AUTOCOMMIT
		StatusFlags: 0x0002,

		CapabilityFlags2:    capFlags2,
		AuthPluginDataLen:   authPluginDataLen,
		Reserved:            make([]byte, 10),
		AuthPluginDataPart2: authData[8:], // 剩下 12 字节
		// 你现在 auth 逻辑是 native 的，就继续用 mysql_native_password
		AuthPluginName: "mysql_native_password",
	}
}

// Encode 按照 MySQL 8.0 协议编码握手包
func (h *HandshakePacket) Encode() []byte {
	payload := make([]byte, 0, 128)

	// 1. 协议版本
	payload = append(payload, h.ProtocolVersion)

	// 2. server version + '\0'
	payload = append(payload, []byte(h.ServerVersion)...)
	payload = append(payload, 0x00)

	// 3. connection id (4 bytes, little endian)
	connID := make([]byte, 4)
	binary.LittleEndian.PutUint32(connID, h.ConnectionID)
	payload = append(payload, connID...)

	// 4. auth-plugin-data-part-1 (8 bytes)
	if len(h.AuthPluginDataPart1) != 8 {
		panic(fmt.Sprintf("AuthPluginDataPart1 must be 8 bytes, got %d", len(h.AuthPluginDataPart1)))
	}
	payload = append(payload, h.AuthPluginDataPart1...)

	// 5. filler = 0x00
	payload = append(payload, h.Filler)

	// 6. capability flags (lower 16 bits)
	cap1 := make([]byte, 2)
	binary.LittleEndian.PutUint16(cap1, h.CapabilityFlags1)
	payload = append(payload, cap1...)

	// 7. character set (1 byte)
	payload = append(payload, h.CharacterSet)

	// 8. status flags (2 bytes)
	status := make([]byte, 2)
	binary.LittleEndian.PutUint16(status, h.StatusFlags)
	payload = append(payload, status...)

	// 9. capability flags (upper 16 bits)
	cap2 := make([]byte, 2)
	binary.LittleEndian.PutUint16(cap2, h.CapabilityFlags2)
	payload = append(payload, cap2...)

	// 10. auth-plugin-data length (1 byte)
	// 如果没填，按 21 兜底
	authDataLen := h.AuthPluginDataLen
	if authDataLen == 0 {
		authDataLen = 21
	}
	payload = append(payload, authDataLen)

	// 11. reserved (10 bytes, all 0)
	if len(h.Reserved) != 10 {
		h.Reserved = make([]byte, 10)
	}
	payload = append(payload, h.Reserved...)

	// 12. auth-plugin-data-part-2 (len >= 12 bytes)
	if len(h.AuthPluginDataPart2) < 12 {
		panic(fmt.Sprintf("AuthPluginDataPart2 must be at least 12 bytes, got %d", len(h.AuthPluginDataPart2)))
	}
	payload = append(payload, h.AuthPluginDataPart2...)

	// 13. 终止符 '\0'
	payload = append(payload, 0x00)

	// 14. auth-plugin-name + '\0'
	if h.AuthPluginName == "" {
		h.AuthPluginName = "mysql_native_password"
	}
	payload = append(payload, []byte(h.AuthPluginName)...)
	payload = append(payload, 0x00)

	// 包外面再加 4 字节 header: 3 字节长度 + 1 字节 seqId(0)
	return h.wrapWithPacketHeader(payload)
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
