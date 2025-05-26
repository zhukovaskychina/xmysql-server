package net

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
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

	return &HandshakePacket{
		ProtocolVersion:     10,
		ServerVersion:       "8.0.0-xmysql-server",
		ConnectionID:        connectionID,
		AuthPluginDataPart1: authData[:8],
		Filler:              0x00,
		CapabilityFlags1:    0xFFFF, // 支持所有低16位能力
		CharacterSet:        0x21,   // utf8_general_ci
		StatusFlags:         0x0002, // SERVER_STATUS_AUTOCOMMIT
		CapabilityFlags2:    0x807F, // 支持高16位能力
		AuthPluginDataLen:   21,     // 20字节数据 + 1字节null终止符
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
