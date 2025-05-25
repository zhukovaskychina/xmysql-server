package protocol

import (
	"context"
	"time"
)

// MessageType 消息类型枚举
type MessageType uint8

const (
	// 连接管理消息
	MSG_CONNECT    MessageType = 0x01
	MSG_DISCONNECT MessageType = 0x02
	MSG_PING       MessageType = 0x03

	// 认证消息
	MSG_AUTH_REQUEST  MessageType = 0x10
	MSG_AUTH_RESPONSE MessageType = 0x11

	// SQL执行消息
	MSG_QUERY_REQUEST  MessageType = 0x20
	MSG_QUERY_RESPONSE MessageType = 0x21

	// 数据库操作消息
	MSG_USE_DB_REQUEST  MessageType = 0x30
	MSG_USE_DB_RESPONSE MessageType = 0x31

	// 错误消息
	MSG_ERROR MessageType = 0xFF
)

// Message 统一消息接口
type Message interface {
	Type() MessageType
	SessionID() string
	Timestamp() time.Time
	Context() context.Context
	Payload() interface{}
}

// BaseMessage 基础消息实现
type BaseMessage struct {
	msgType   MessageType
	sessionID string
	timestamp time.Time
	ctx       context.Context
	payload   interface{}
}

func NewBaseMessage(msgType MessageType, sessionID string, payload interface{}) *BaseMessage {
	return &BaseMessage{
		msgType:   msgType,
		sessionID: sessionID,
		timestamp: time.Now(),
		ctx:       context.Background(),
		payload:   payload,
	}
}

func (m *BaseMessage) Type() MessageType {
	return m.msgType
}

func (m *BaseMessage) SessionID() string {
	return m.sessionID
}

func (m *BaseMessage) Timestamp() time.Time {
	return m.timestamp
}

func (m *BaseMessage) Context() context.Context {
	return m.ctx
}

func (m *BaseMessage) Payload() interface{} {
	return m.payload
}

// 具体消息类型定义

// ConnectMessage 连接消息
type ConnectMessage struct {
	*BaseMessage
	ClientInfo *ClientInfo
}

type ClientInfo struct {
	Host     string
	Port     int
	User     string
	Database string
	Charset  string
}

// AuthMessage 认证消息
type AuthMessage struct {
	*BaseMessage
	User     string
	Password string
	Database string
	Charset  string
}

// QueryMessage 查询消息
type QueryMessage struct {
	*BaseMessage
	SQL      string
	Database string
}

// MessageQueryResult 查询结果
type MessageQueryResult struct {
	Columns []string
	Rows    [][]interface{}
	Error   error
	Message string
	Type    string // select, insert, update, delete, ddl, etc.
}

// ResponseMessage 响应消息
type ResponseMessage struct {
	*BaseMessage
	Result *MessageQueryResult
}

// ErrorMessage 错误消息
type ErrorMessage struct {
	*BaseMessage
	Code    uint16
	State   string
	Message string
}

// UseDBMessage 切换数据库消息
type UseDBMessage struct {
	*BaseMessage
	Database string
}

// MessageHandler 消息处理器接口
type MessageHandler interface {
	HandleMessage(msg Message) (Message, error)
	CanHandle(msgType MessageType) bool
}

// MessageBus 消息总线接口
type MessageBus interface {
	Subscribe(msgType MessageType, handler MessageHandler)
	Unsubscribe(msgType MessageType, handler MessageHandler)
	Publish(msg Message) error
	PublishAsync(msg Message) <-chan Message
}
