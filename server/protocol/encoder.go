package protocol

import (
	"fmt"
)

// ProtocolEncoder MySQL协议编码器接口
type ProtocolEncoder interface {
	EncodeMessage(msg Message) ([]byte, error)
	CanEncode(msgType MessageType) bool
}

// MySQLProtocolEncoder MySQL协议编码器实现
type MySQLProtocolEncoder struct {
	encoders map[MessageType]MessageEncoder
}

// MessageEncoder 消息编码器接口
type MessageEncoder interface {
	Encode(msg Message) ([]byte, error)
}

// NewMySQLProtocolEncoder 创建MySQL协议编码器
func NewMySQLProtocolEncoder() *MySQLProtocolEncoder {
	encoder := &MySQLProtocolEncoder{
		encoders: make(map[MessageType]MessageEncoder),
	}

	// 注册各种消息编码器
	encoder.RegisterEncoder(MSG_CONNECT, &ConnectResponseEncoder{})
	encoder.RegisterEncoder(MSG_DISCONNECT, &DisconnectResponseEncoder{})
	encoder.RegisterEncoder(MSG_QUERY_RESPONSE, &QueryResponseEncoder{})
	encoder.RegisterEncoder(MSG_AUTH_RESPONSE, &AuthResponseEncoder{})
	encoder.RegisterEncoder(MSG_USE_DB_RESPONSE, &UseDBResponseEncoder{})
	encoder.RegisterEncoder(MSG_ERROR, &ErrorMessageEncoder{})
	encoder.RegisterEncoder(MSG_PING, &PingResponseEncoder{})

	return encoder
}

// RegisterEncoder 注册消息编码器
func (e *MySQLProtocolEncoder) RegisterEncoder(msgType MessageType, encoder MessageEncoder) {
	e.encoders[msgType] = encoder
}

// EncodeMessage 编码消息
func (e *MySQLProtocolEncoder) EncodeMessage(msg Message) ([]byte, error) {
	encoder, exists := e.encoders[msg.Type()]
	if !exists {
		return nil, fmt.Errorf("unsupported message type: %d", msg.Type())
	}

	return encoder.Encode(msg)
}

// CanEncode 检查是否能编码指定类型的消息
func (e *MySQLProtocolEncoder) CanEncode(msgType MessageType) bool {
	_, exists := e.encoders[msgType]
	return exists
}

// 具体的消息编码器实现

// QueryResponseEncoder 查询响应编码器
type QueryResponseEncoder struct{}

func (e *QueryResponseEncoder) Encode(msg Message) ([]byte, error) {
	responseMsg, ok := msg.(*ResponseMessage)
	if !ok {
		return nil, fmt.Errorf("invalid message type for QueryResponseEncoder")
	}

	result := responseMsg.Result
	if result.Error != nil {
		// 编码错误响应
		return EncodeErrorPacket(1064, "42000", result.Error.Error()), nil
	}

	switch result.Type {
	case "select":
		return e.encodeSelectResult(result)
	case "insert", "update", "delete":
		return EncodeOKPacket(nil, 1, 0, nil), nil // 假设影响1行
	case "ddl":
		return EncodeOKPacket(nil, 0, 0, nil), nil
	default:
		return EncodeOKPacket(nil, 0, 0, nil), nil
	}
}

func (e *QueryResponseEncoder) encodeSelectResult(result *MessageQueryResult) ([]byte, error) {
	var response []byte

	// 编码列数量
	if len(result.Columns) > 0 {
		columnCountPacket := e.encodeColumnCount(len(result.Columns))
		response = append(response, columnCountPacket...)

		// 编码列定义
		for i, column := range result.Columns {
			columnPacket := e.encodeColumnDefinition(column, byte(i+1))
			response = append(response, columnPacket...)
		}

		// EOF包（列定义结束）
		eofPacket := EncodeEOFPacket(0, 0)
		response = append(response, eofPacket...)
	}

	// 编码行数据
	if len(result.Rows) > 0 {
		for i, row := range result.Rows {
			rowPacket := e.encodeRowData(row, byte(i+2+len(result.Columns)))
			response = append(response, rowPacket...)
		}
	}

	// EOF包（数据结束）
	eofPacket := EncodeEOFPacket(0, 0)
	response = append(response, eofPacket...)

	return response, nil
}

func (e *QueryResponseEncoder) encodeColumnCount(count int) []byte {
	payload := []byte{byte(count)}
	return addPacketHeader(payload, 1)
}

func (e *QueryResponseEncoder) encodeColumnDefinition(columnName string, sequenceId byte) []byte {
	payload := make([]byte, 0, 64+len(columnName))

	// 简化的列定义
	payload = appendLengthEncodedString(payload, "def")      // catalog
	payload = appendLengthEncodedString(payload, "")         // schema
	payload = appendLengthEncodedString(payload, "")         // table
	payload = appendLengthEncodedString(payload, "")         // org_table
	payload = appendLengthEncodedString(payload, columnName) // name
	payload = appendLengthEncodedString(payload, columnName) // org_name

	// 固定长度字段
	payload = append(payload, 0x0c)                   // length of fixed fields
	payload = append(payload, 0x21, 0x00)             // character set
	payload = append(payload, 0x00, 0x00, 0x00, 0x00) // column length
	payload = append(payload, 0xFD)                   // column type (VAR_STRING)
	payload = append(payload, 0x00, 0x00)             // flags
	payload = append(payload, 0x00)                   // decimals
	payload = append(payload, 0x00, 0x00)             // filler

	return addPacketHeader(payload, sequenceId)
}

func (e *QueryResponseEncoder) encodeRowData(row []interface{}, sequenceId byte) []byte {
	payload := make([]byte, 0, 256)

	for _, value := range row {
		if value == nil {
			payload = append(payload, 0xFB) // NULL
		} else {
			str := fmt.Sprintf("%v", value)
			payload = appendLengthEncodedString(payload, str)
		}
	}

	return addPacketHeader(payload, sequenceId)
}

// AuthResponseEncoder 认证响应编码器
type AuthResponseEncoder struct{}

func (e *AuthResponseEncoder) Encode(msg Message) ([]byte, error) {
	return EncodeOKPacket(nil, 0, 0, nil), nil
}

// UseDBResponseEncoder 切换数据库响应编码器
type UseDBResponseEncoder struct{}

func (e *UseDBResponseEncoder) Encode(msg Message) ([]byte, error) {
	return EncodeOKPacket(nil, 0, 0, nil), nil
}

// ErrorMessageEncoder 错误消息编码器
type ErrorMessageEncoder struct{}

func (e *ErrorMessageEncoder) Encode(msg Message) ([]byte, error) {
	errorMsg, ok := msg.(*ErrorMessage)
	if !ok {
		return nil, fmt.Errorf("invalid message type for ErrorMessageEncoder")
	}

	return EncodeErrorPacket(errorMsg.Code, errorMsg.State, errorMsg.Message), nil
}

// PingResponseEncoder Ping响应编码器
type PingResponseEncoder struct{}

func (e *PingResponseEncoder) Encode(msg Message) ([]byte, error) {
	return EncodeOKPacket(nil, 0, 0, nil), nil
}

// ConnectResponseEncoder 连接响应编码器
type ConnectResponseEncoder struct{}

func (e *ConnectResponseEncoder) Encode(msg Message) ([]byte, error) {
	// 连接成功，返回OK包
	return EncodeOKPacket(nil, 0, 0, nil), nil
}

// DisconnectResponseEncoder 断开连接响应编码器
type DisconnectResponseEncoder struct{}

func (e *DisconnectResponseEncoder) Encode(msg Message) ([]byte, error) {
	// 断开连接成功，返回OK包
	return EncodeOKPacket(nil, 0, 0, nil), nil
}
