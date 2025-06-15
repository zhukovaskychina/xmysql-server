package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"

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

// Parse 解析认证包
func (p *AuthPacketParser) Parse(data []byte, sessionID string) (Message, error) {
	logger.Debugf(" [AuthPacketParser.Parse] 开始解析认证包\n")
	logger.Debugf(" [AuthPacketParser.Parse] 完整数据包长度: %d\n", len(data))
	logger.Debugf(" [AuthPacketParser.Parse] 完整数据包内容: %v\n", data)

	if len(data) < 4 {
		logger.Debugf(" [AuthPacketParser.Parse] 数据包太短，需要至少4字节，只有%d字节\n", len(data))
		return nil, fmt.Errorf("auth packet too short")
	}

	// 跳过包头（3字节长度 + 1字节序号）
	payload := data[4:]

	logger.Debugf(" [AuthPacketParser.Parse] 包头: %v\n", data[:4])
	logger.Debugf(" [AuthPacketParser.Parse] 载荷长度: %d\n", len(payload))
	logger.Debugf(" [AuthPacketParser.Parse] 载荷内容: %v\n", payload)

	if len(payload) == 0 {
		logger.Debugf(" [AuthPacketParser.Parse] 载荷为空\n")
		return nil, fmt.Errorf("empty auth payload")
	}

	// 解析客户端认证响应包
	return p.parseClientAuthResponse(payload, sessionID)
}

// parseClientAuthResponse 解析客户端认证响应
func (p *AuthPacketParser) parseClientAuthResponse(payload []byte, sessionID string) (Message, error) {
	logger.Debugf(" [AuthPacketParser] 解析认证响应，payload长度: %d\n", len(payload))
	logger.Debugf(" [AuthPacketParser] payload内容(前64字节): %v\n", payload[:min(len(payload), 64)])

	if len(payload) < 4 {
		return nil, fmt.Errorf("auth response too short")
	}

	offset := 0

	// 1. 客户端能力标志 (4字节)
	if len(payload) < offset+4 {
		return nil, fmt.Errorf("insufficient data for client flags")
	}
	clientFlags := binary.LittleEndian.Uint32(payload[offset : offset+4])
	offset += 4

	// 2. 最大包大小 (4字节)
	if len(payload) < offset+4 {
		return nil, fmt.Errorf("insufficient data for max packet size")
	}
	maxPacketSize := binary.LittleEndian.Uint32(payload[offset : offset+4])
	offset += 4

	// 3. 字符集 (1字节)
	if len(payload) < offset+1 {
		return nil, fmt.Errorf("insufficient data for charset")
	}
	charset := payload[offset]
	offset += 1

	// 4. 保留字段 (23字节)
	if len(payload) < offset+23 {
		return nil, fmt.Errorf("insufficient data for reserved field")
	}
	offset += 23

	logger.Debugf("[AuthPacketParser] 客户端能力标志: 0x%08X, 当前偏移: %d\n", clientFlags, offset)

	// 5. 用户名（以null结尾的字符串）
	userEnd := offset
	for userEnd < len(payload) && payload[userEnd] != 0 {
		userEnd++
	}
	if userEnd >= len(payload) {
		return nil, fmt.Errorf("username not null-terminated")
	}
	username := string(payload[offset:userEnd])
	offset = userEnd + 1

	logger.Debugf("[AuthPacketParser] 用户名: %s, 当前偏移: %d\n", username, offset)

	// 6. 认证响应长度和数据
	var authResponse []byte
	var database string

	if offset < len(payload) {
		// 检查是否有认证响应长度字段
		if clientFlags&CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
			// 长度编码的认证数据
			authLen, lenBytes := p.readLengthEncodedInteger(payload[offset:])
			offset += lenBytes
			if offset+int(authLen) <= len(payload) {
				authResponse = payload[offset : offset+int(authLen)]
				offset += int(authLen)
			}
		} else if clientFlags&CLIENT_SECURE_CONNECTION != 0 {
			// 固定长度的认证数据
			if offset < len(payload) {
				authLen := int(payload[offset])
				offset++
				if offset+authLen <= len(payload) {
					authResponse = payload[offset : offset+authLen]
					offset += authLen
				}
			}
		} else {
			// 以null结尾的密码
			passEnd := offset
			for passEnd < len(payload) && payload[passEnd] != 0 {
				passEnd++
			}
			if passEnd < len(payload) {
				authResponse = payload[offset:passEnd]
				offset = passEnd + 1
			}
		}
	}

	logger.Debugf("[AuthPacketParser] 认证响应长度: %d, 数据: %x, 当前偏移: %d\n", len(authResponse), authResponse, offset)

	// 7. 数据库名（如果指定）
	if offset < len(payload) && clientFlags&CLIENT_CONNECT_WITH_DB != 0 {
		dbEnd := offset
		for dbEnd < len(payload) && payload[dbEnd] != 0 {
			dbEnd++
		}
		if dbEnd <= len(payload) {
			database = string(payload[offset:dbEnd])
		}
	}

	logger.Debugf("[AuthPacketParser] 数据库: %s, 最终偏移: %d\n", database, offset)

	// 解析密码（简化处理）
	password := ""
	if len(authResponse) > 0 {
		// 这里应该根据认证插件类型来处理
		// 对于mysql_native_password，这是SHA1(SHA1(password)) XOR challenge
		// 简化处理，直接转换为字符串（实际应该进行密码验证）
		password = string(authResponse)
	}

	return &AuthMessage{
		BaseMessage:   NewBaseMessage(MSG_AUTH_REQUEST, sessionID, nil),
		User:          username,
		Password:      password,
		Database:      database,
		ClientFlags:   clientFlags,
		MaxPacketSize: maxPacketSize,
		Charset:       fmt.Sprintf("%d", charset),
		AuthResponse:  authResponse,
	}, nil
}

// readLengthEncodedInteger 读取长度编码的整数
func (p *AuthPacketParser) readLengthEncodedInteger(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	switch data[0] {
	case 0xfb:
		return 0, 1 // NULL
	case 0xfc:
		if len(data) < 3 {
			return 0, 1
		}
		return uint64(binary.LittleEndian.Uint16(data[1:3])), 3
	case 0xfd:
		if len(data) < 4 {
			return 0, 1
		}
		return uint64(binary.LittleEndian.Uint32(data[1:4]) & 0xffffff), 4
	case 0xfe:
		if len(data) < 9 {
			return 0, 1
		}
		return binary.LittleEndian.Uint64(data[1:9]), 9
	default:
		return uint64(data[0]), 1
	}
}

// MySQL客户端能力标志常量
const (
	CLIENT_LONG_PASSWORD                  = 0x00000001
	CLIENT_FOUND_ROWS                     = 0x00000002
	CLIENT_LONG_FLAG                      = 0x00000004
	CLIENT_CONNECT_WITH_DB                = 0x00000008
	CLIENT_NO_SCHEMA                      = 0x00000010
	CLIENT_COMPRESS                       = 0x00000020
	CLIENT_ODBC                           = 0x00000040
	CLIENT_LOCAL_FILES                    = 0x00000080
	CLIENT_IGNORE_SPACE                   = 0x00000100
	CLIENT_PROTOCOL_41                    = 0x00000200
	CLIENT_INTERACTIVE                    = 0x00000400
	CLIENT_SSL                            = 0x00000800
	CLIENT_IGNORE_SIGPIPE                 = 0x00001000
	CLIENT_TRANSACTIONS                   = 0x00002000
	CLIENT_RESERVED                       = 0x00004000
	CLIENT_SECURE_CONNECTION              = 0x00008000
	CLIENT_MULTI_STATEMENTS               = 0x00010000
	CLIENT_MULTI_RESULTS                  = 0x00020000
	CLIENT_PS_MULTI_RESULTS               = 0x00040000
	CLIENT_PLUGIN_AUTH                    = 0x00080000
	CLIENT_CONNECT_ATTRS                  = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 0x00400000
	CLIENT_SESSION_TRACK                  = 0x00800000
	CLIENT_DEPRECATE_EOF                  = 0x01000000
)

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
