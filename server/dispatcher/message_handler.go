package dispatcher

import (
	"fmt"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

// BusinessMessageHandler 业务消息处理器
type BusinessMessageHandler struct {
	sqlDispatcher *SQLDispatcher
	config        *conf.Cfg
}

// NewBusinessMessageHandler 创建业务消息处理器
func NewBusinessMessageHandler(config *conf.Cfg) *BusinessMessageHandler {
	return &BusinessMessageHandler{
		sqlDispatcher: NewSQLDispatcher(config),
		config:        config,
	}
}

// HandleMessage 处理消息
func (h *BusinessMessageHandler) HandleMessage(msg protocol.Message) (protocol.Message, error) {
	switch msg.Type() {
	case protocol.MSG_CONNECT:
		return h.handleConnectMessage(msg)
	case protocol.MSG_DISCONNECT:
		return h.handleDisconnectMessage(msg)
	case protocol.MSG_QUERY_REQUEST:
		return h.handleQueryMessage(msg)
	case protocol.MSG_AUTH_REQUEST:
		return h.handleAuthMessage(msg)
	case protocol.MSG_USE_DB_REQUEST:
		return h.handleUseDBMessage(msg)
	case protocol.MSG_PING:
		return h.handlePingMessage(msg)
	default:
		return nil, fmt.Errorf("unsupported message type: %d", msg.Type())
	}
}

// CanHandle 检查是否能处理指定类型的消息
func (h *BusinessMessageHandler) CanHandle(msgType protocol.MessageType) bool {
	switch msgType {
	case protocol.MSG_CONNECT,
		protocol.MSG_DISCONNECT,
		protocol.MSG_QUERY_REQUEST,
		protocol.MSG_AUTH_REQUEST,
		protocol.MSG_USE_DB_REQUEST,
		protocol.MSG_PING:
		return true
	default:
		return false
	}
}

// handleQueryMessage 处理查询消息
func (h *BusinessMessageHandler) handleQueryMessage(msg protocol.Message) (protocol.Message, error) {
	queryMsg, ok := msg.(*protocol.QueryMessage)
	if !ok {
		return nil, fmt.Errorf("invalid query message type")
	}

	// 创建一个临时的session（实际应该从消息中获取）
	session := &MockMySQLServerSession{
		sessionID: msg.SessionID(),
		database:  queryMsg.Database,
	}

	// 分发SQL查询
	resultChan := h.sqlDispatcher.Dispatch(session, queryMsg.SQL, queryMsg.Database)

	// 等待结果
	for result := range resultChan {
		// 转换结果格式
		queryResult := &protocol.MessageQueryResult{
			Columns: result.Columns,
			Rows:    result.Rows,
			Error:   result.Err,
			Message: result.Message,
			Type:    result.ResultType,
		}

		// 创建响应消息
		responseMsg := &protocol.ResponseMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, msg.SessionID(), queryResult),
			Result:      queryResult,
		}

		return responseMsg, nil
	}

	return nil, fmt.Errorf("no result received")
}

// handleAuthMessage 处理认证消息
func (h *BusinessMessageHandler) handleAuthMessage(msg protocol.Message) (protocol.Message, error) {
	authMsg, ok := msg.(*protocol.AuthMessage)
	if !ok {
		return nil, fmt.Errorf("invalid auth message type")
	}

	// 简单的认证逻辑（实际应该验证用户名密码）
	if authMsg.User == "" {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        1045,
			State:       "28000",
			Message:     "Access denied for user",
		}, nil
	}

	// 认证成功
	return protocol.NewBaseMessage(protocol.MSG_AUTH_RESPONSE, msg.SessionID(), nil), nil
}

// handleUseDBMessage 处理切换数据库消息
func (h *BusinessMessageHandler) handleUseDBMessage(msg protocol.Message) (protocol.Message, error) {
	useDBMsg, ok := msg.(*protocol.UseDBMessage)
	if !ok {
		return nil, fmt.Errorf("invalid use db message type")
	}

	// 简单的数据库切换逻辑（实际应该验证数据库是否存在）
	if useDBMsg.Database == "" {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        1049,
			State:       "42000",
			Message:     "Unknown database",
		}, nil
	}

	// 切换成功
	return protocol.NewBaseMessage(protocol.MSG_USE_DB_RESPONSE, msg.SessionID(), nil), nil
}

// handlePingMessage 处理Ping消息
func (h *BusinessMessageHandler) handlePingMessage(msg protocol.Message) (protocol.Message, error) {
	// Ping消息直接返回成功
	return protocol.NewBaseMessage(protocol.MSG_PING, msg.SessionID(), nil), nil
}

// handleConnectMessage 处理连接消息
func (h *BusinessMessageHandler) handleConnectMessage(msg protocol.Message) (protocol.Message, error) {
	connectMsg, ok := msg.(*protocol.ConnectMessage)
	if !ok {
		return nil, fmt.Errorf("invalid connect message type")
	}

	// 记录连接信息
	fmt.Printf("Client connected: %s:%d, User: %s, Database: %s\n",
		connectMsg.ClientInfo.Host,
		connectMsg.ClientInfo.Port,
		connectMsg.ClientInfo.User,
		connectMsg.ClientInfo.Database)

	// 连接成功，返回成功响应
	return protocol.NewBaseMessage(protocol.MSG_CONNECT, msg.SessionID(), "Connection established"), nil
}

// handleDisconnectMessage 处理断开连接消息
func (h *BusinessMessageHandler) handleDisconnectMessage(msg protocol.Message) (protocol.Message, error) {
	// 记录断开连接信息
	fmt.Printf("Client disconnected: %s\n", msg.SessionID())

	// 清理会话相关资源（如果需要的话）
	// 这里可以添加清理逻辑，比如关闭数据库连接、清理缓存等

	// 断开连接成功，返回成功响应
	return protocol.NewBaseMessage(protocol.MSG_DISCONNECT, msg.SessionID(), "Connection closed"), nil
}

// MockMySQLServerSession 模拟的MySQL服务器会话
type MockMySQLServerSession struct {
	sessionID      string
	database       string
	params         map[string]interface{}
	lastActiveTime time.Time
}

func (s *MockMySQLServerSession) GetSessionId() string {
	return s.sessionID
}

func (s *MockMySQLServerSession) GetLastActiveTime() time.Time {
	return s.lastActiveTime
}

func (s *MockMySQLServerSession) SetParamByName(name string, value interface{}) {
	if s.params == nil {
		s.params = make(map[string]interface{})
	}
	s.params[name] = value
}

func (s *MockMySQLServerSession) GetParamByName(name string) interface{} {
	if s.params == nil {
		return nil
	}
	return s.params[name]
}

func (s *MockMySQLServerSession) SendOK() {
	// 模拟发送OK响应
}

func (s *MockMySQLServerSession) SendHandleOk() {
	// 模拟发送握手OK响应
}

func (s *MockMySQLServerSession) SendSelectFields() {
	// 模拟发送选择字段
}

func (s *MockMySQLServerSession) SendError(code uint16, message string) {
	// 模拟发送错误响应
}

func (s *MockMySQLServerSession) SendResultSet(columns []string, rows [][]interface{}) {
	// 模拟发送结果集
}

// 确保MockMySQLServerSession实现了server.MySQLServerSession接口
var _ server.MySQLServerSession = (*MockMySQLServerSession)(nil)
