package protocol

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// ProtocolParser MySQL协议解析器接口
type ProtocolParser interface {
	ParsePacket(data []byte, sessionID string) (Message, error)
	CanParse(packetType byte) bool
}

// MySQLProtocolParser MySQL协议解析器实现
type MySQLProtocolParser struct {
	parsers map[byte]PacketParser
}

// PacketParser 包解析器接口
type PacketParser interface {
	Parse(data []byte, sessionID string) (Message, error)
}

// NewMySQLProtocolParser 创建MySQL协议解析器
func NewMySQLProtocolParser() *MySQLProtocolParser {
	parser := &MySQLProtocolParser{
		parsers: make(map[byte]PacketParser),
	}

	// 注册各种包解析器
	parser.RegisterParser(common.COM_QUERY, &QueryPacketParser{})
	parser.RegisterParser(common.COM_QUIT, &QuitPacketParser{})
	parser.RegisterParser(common.COM_INIT_DB, &InitDBPacketParser{})
	parser.RegisterParser(common.COM_PING, &PingPacketParser{})
	parser.RegisterParser(common.COM_SLEEP, &SleepPacketParser{})

	return parser
}

// RegisterParser 注册包解析器
func (p *MySQLProtocolParser) RegisterParser(packetType byte, parser PacketParser) {
	p.parsers[packetType] = parser
}

// ParsePacket 解析MySQL包
func (p *MySQLProtocolParser) ParsePacket(data []byte, sessionID string) (Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty packet data")
	}

	packetType := data[0]
	parser, exists := p.parsers[packetType]
	if !exists {
		return nil, fmt.Errorf("unsupported packet type: %d", packetType)
	}

	return parser.Parse(data, sessionID)
}

// CanParse 检查是否能解析指定类型的包
func (p *MySQLProtocolParser) CanParse(packetType byte) bool {
	_, exists := p.parsers[packetType]
	return exists
}

// 具体的包解析器实现

// QueryPacketParser 查询包解析器
type QueryPacketParser struct{}

func (p *QueryPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid query packet")
	}

	sql := string(data[1:])
	return &QueryMessage{
		BaseMessage: NewBaseMessage(MSG_QUERY_REQUEST, sessionID, sql),
		SQL:         sql,
	}, nil
}

// QuitPacketParser 退出包解析器
type QuitPacketParser struct{}

func (p *QuitPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	return NewBaseMessage(MSG_DISCONNECT, sessionID, nil), nil
}

// InitDBPacketParser 初始化数据库包解析器
type InitDBPacketParser struct{}

func (p *InitDBPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid init db packet")
	}

	database := string(data[1:])
	return &UseDBMessage{
		BaseMessage: NewBaseMessage(MSG_USE_DB_REQUEST, sessionID, database),
		Database:    database,
	}, nil
}

// PingPacketParser Ping包解析器
type PingPacketParser struct{}

func (p *PingPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	return NewBaseMessage(MSG_PING, sessionID, nil), nil
}

// SleepPacketParser Sleep包解析器
type SleepPacketParser struct{}

func (p *SleepPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	return NewBaseMessage(MSG_CONNECT, sessionID, nil), nil
}

// AuthPacketParser 认证包解析器
type AuthPacketParser struct{}

func (p *AuthPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	authPacket := &AuthPacket{}
	authResult := authPacket.DecodeAuth(data)
	if authResult == nil {
		return nil, fmt.Errorf("failed to decode auth packet")
	}

	return &AuthMessage{
		BaseMessage: NewBaseMessage(MSG_AUTH_REQUEST, sessionID, authResult),
		User:        authResult.User,
		Password:    string(authResult.Password),
		Database:    authResult.Database,
		Charset:     "", // 暂时设为空字符串
	}, nil
}
