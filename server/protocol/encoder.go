package protocol

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
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
		// 编码错误响应（使用错误处理工具）
		return EncodeErrorFromGoError(result.Error), nil
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

		// 编码列定义（使用列类型信息）
		for i, column := range result.Columns {
			var columnType string
			if result.ColumnTypes != nil && i < len(result.ColumnTypes) {
				columnType = result.ColumnTypes[i]
			}
			columnPacket := e.encodeColumnDefinitionWithType(column, columnType, byte(i+1))
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
	// 使用新的方法，支持列类型信息
	return e.encodeColumnDefinitionWithType(columnName, "", sequenceId)
}

// encodeColumnDefinitionWithType 编码列定义（支持类型信息）
func (e *QueryResponseEncoder) encodeColumnDefinitionWithType(columnName string, columnType string, sequenceId byte) []byte {
	payload := make([]byte, 0, 64+len(columnName))

	// 简化的列定义
	payload = appendLengthEncodedString(payload, "def")      // catalog
	payload = appendLengthEncodedString(payload, "")         // schema
	payload = appendLengthEncodedString(payload, "")         // table
	payload = appendLengthEncodedString(payload, "")         // org_table
	payload = appendLengthEncodedString(payload, columnName) // name
	payload = appendLengthEncodedString(payload, columnName) // org_name

	// 根据列类型确定MySQL类型码和长度
	mysqlType, columnLength, flags, decimals := e.getColumnTypeInfo(columnType)

	// 固定长度字段
	payload = append(payload, 0x0c)                                                                                      // length of fixed fields
	payload = append(payload, 0x21, 0x00)                                                                                // character set (utf8_general_ci)
	payload = append(payload, byte(columnLength), byte(columnLength>>8), byte(columnLength>>16), byte(columnLength>>24)) // column length (4 bytes, little-endian)
	payload = append(payload, mysqlType)                                                                                 // column type
	payload = append(payload, byte(flags), byte(flags>>8))                                                               // flags (2 bytes, little-endian)
	payload = append(payload, decimals)                                                                                  // decimals
	payload = append(payload, 0x00, 0x00)                                                                                // filler

	return addPacketHeader(payload, sequenceId)
}

// getColumnTypeInfo 根据列类型字符串返回MySQL类型信息
func (e *QueryResponseEncoder) getColumnTypeInfo(columnType string) (mysqlType byte, columnLength uint32, flags uint16, decimals byte) {
	// 默认值
	mysqlType = common.COLUMN_TYPE_VAR_STRING
	columnLength = 255
	flags = 0
	decimals = 0

	if columnType == "" {
		return
	}

	// 转换为小写进行匹配
	columnType = strings.ToLower(columnType)

	// 根据类型字符串确定MySQL类型码
	switch {
	case strings.HasPrefix(columnType, "tinyint"):
		mysqlType = common.COLUMN_TYPE_TINY
		columnLength = 4
	case strings.HasPrefix(columnType, "smallint"):
		mysqlType = common.COLUMN_TYPE_SHORT
		columnLength = 6
	case strings.HasPrefix(columnType, "mediumint"):
		mysqlType = common.COLUMN_TYPE_INT24
		columnLength = 9
	case strings.HasPrefix(columnType, "int"):
		mysqlType = common.COLUMN_TYPE_LONG
		columnLength = 11
	case strings.HasPrefix(columnType, "bigint"):
		mysqlType = common.COLUMN_TYPE_LONGLONG
		columnLength = 20
	case strings.HasPrefix(columnType, "float"):
		mysqlType = common.COLUMN_TYPE_FLOAT
		columnLength = 12
		decimals = 31
	case strings.HasPrefix(columnType, "double"):
		mysqlType = common.COLUMN_TYPE_DOUBLE
		columnLength = 22
		decimals = 31
	case strings.HasPrefix(columnType, "decimal"):
		mysqlType = common.COLUMN_TYPE_NEWDECIMAL
		columnLength = 10
		decimals = 0
	case strings.HasPrefix(columnType, "datetime"):
		mysqlType = common.COLUMN_TYPE_DATETIME
		columnLength = 19
	case strings.HasPrefix(columnType, "timestamp"):
		mysqlType = common.COLUMN_TYPE_TIMESTAMP
		columnLength = 19
	case strings.HasPrefix(columnType, "date"):
		mysqlType = common.COLUMN_TYPE_DATE
		columnLength = 10
	case strings.HasPrefix(columnType, "time"):
		mysqlType = common.COLUMN_TYPE_TIME
		columnLength = 10
	case strings.HasPrefix(columnType, "year"):
		mysqlType = common.COLUMN_TYPE_YEAR
		columnLength = 4
	case strings.HasPrefix(columnType, "char"):
		mysqlType = common.COLUMN_TYPE_STRING
		columnLength = 255
	case strings.HasPrefix(columnType, "varchar"):
		mysqlType = common.COLUMN_TYPE_VAR_STRING
		columnLength = 255
	case strings.HasPrefix(columnType, "binary"):
		mysqlType = common.COLUMN_TYPE_STRING
		columnLength = 255
		flags = uint16(common.BinaryFlag)
	case strings.HasPrefix(columnType, "varbinary"):
		mysqlType = common.COLUMN_TYPE_VAR_STRING
		columnLength = 255
		flags = uint16(common.BinaryFlag)
	case strings.HasPrefix(columnType, "tinyblob"), strings.HasPrefix(columnType, "tinytext"):
		mysqlType = common.COLUMN_TYPE_TINY_BLOB
		columnLength = 255
	case strings.HasPrefix(columnType, "blob"), strings.HasPrefix(columnType, "text"):
		mysqlType = common.COLUMN_TYPE_BLOB
		columnLength = 65535
	case strings.HasPrefix(columnType, "mediumblob"), strings.HasPrefix(columnType, "mediumtext"):
		mysqlType = common.COLUMN_TYPE_MEDIUM_BLOB
		columnLength = 16777215
	case strings.HasPrefix(columnType, "longblob"), strings.HasPrefix(columnType, "longtext"):
		mysqlType = common.COLUMN_TYPE_LONG_BLOB
		columnLength = 4294967295
	case strings.HasPrefix(columnType, "enum"):
		mysqlType = common.COLUMN_TYPE_ENUM
		columnLength = 1
	case strings.HasPrefix(columnType, "set"):
		mysqlType = common.COLUMN_TYPE_SET
		columnLength = 1
	case strings.HasPrefix(columnType, "json"):
		mysqlType = common.COLUMN_TYPE_JSON
		columnLength = 4294967295
	case strings.HasPrefix(columnType, "bit"):
		mysqlType = common.COLUMN_TYPE_BIT
		columnLength = 1
	case strings.HasPrefix(columnType, "geometry"), strings.HasPrefix(columnType, "point"),
		strings.HasPrefix(columnType, "linestring"), strings.HasPrefix(columnType, "polygon"):
		mysqlType = common.COLUMN_TYPE_GEOMETRY
		columnLength = 4294967295
	default:
		// 默认使用VAR_STRING
		mysqlType = common.COLUMN_TYPE_VAR_STRING
		columnLength = 255
	}

	return
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
	// 检查认证结果
	if authResult, ok := msg.Payload().(*auth.AuthResult); ok {
		if authResult.Success {
			return e.encodeOKResponse(authResult)
		} else {
			return e.encodeErrorResponse(authResult)
		}
	}

	// 默认返回OK包
	return EncodeOKPacket(nil, 0, 0, nil), nil
}

// encodeOKResponse 编码认证成功响应
func (e *AuthResponseEncoder) encodeOKResponse(authResult *auth.AuthResult) ([]byte, error) {
	// MySQL OK包格式:
	// 1字节: 0x00 (OK标识)
	// 长度编码整数: affected_rows (通常为0)
	// 长度编码整数: last_insert_id (通常为0)
	// 2字节: status_flags
	// 2字节: warnings
	// 字符串: info (可选)

	buf := make([]byte, 0, 64)

	// OK标识
	buf = append(buf, 0x00)

	// affected_rows (0)
	buf = append(buf, 0x00)

	// last_insert_id (0)
	buf = append(buf, 0x00)

	// status_flags (SERVER_STATUS_AUTOCOMMIT)
	buf = append(buf, 0x02, 0x00)

	// warnings (0)
	buf = append(buf, 0x00, 0x00)

	// 可选的info字符串
	if authResult.Database != "" {
		info := fmt.Sprintf("Database changed to '%s'", authResult.Database)
		buf = append(buf, []byte(info)...)
	}

	return e.wrapWithHeader(buf), nil
}

// encodeErrorResponse 编码认证错误响应
func (e *AuthResponseEncoder) encodeErrorResponse(authResult *auth.AuthResult) ([]byte, error) {
	// MySQL Error包格式:
	// 1字节: 0xFF (Error标识)
	// 2字节: error_code
	// 1字节: '#' (SQL状态标识符)
	// 5字节: SQL状态
	// 字符串: error_message

	buf := make([]byte, 0, 128)

	// Error标识
	buf = append(buf, 0xFF)

	// Error code (小端序)
	buf = append(buf, byte(authResult.ErrorCode), byte(authResult.ErrorCode>>8))

	// SQL状态标识符
	buf = append(buf, '#')

	// SQL状态 (默认为28000 - 认证失败)
	sqlState := "28000"
	if authResult.ErrorCode == common.ER_BAD_DB_ERROR {
		sqlState = "42000"
	}
	buf = append(buf, []byte(sqlState)...)

	// Error message
	buf = append(buf, []byte(authResult.ErrorMessage)...)

	return e.wrapWithHeader(buf), nil
}

// wrapWithHeader 包装MySQL包头
func (e *AuthResponseEncoder) wrapWithHeader(payload []byte) []byte {
	// MySQL包头格式:
	// 3字节: 包长度 (小端序)
	// 1字节: 包序号

	header := make([]byte, 4)

	// 包长度 (小端序)
	length := len(payload)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 包序号 (认证响应通常是2)
	header[3] = 0x02

	return append(header, payload...)
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
