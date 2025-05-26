package dispatcher

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

// EnhancedBusinessMessageHandler 增强的业务消息处理器，集成认证服务
type EnhancedBusinessMessageHandler struct {
	sqlDispatcher *SQLDispatcher
	authService   auth.AuthService
	config        *conf.Cfg
}

// NewEnhancedBusinessMessageHandler 创建增强的业务消息处理器
func NewEnhancedBusinessMessageHandler(config *conf.Cfg, xmysqlEngine *engine.XMySQLEngine) *EnhancedBusinessMessageHandler {
	// 创建引擎访问
	engineAccess := auth.NewInnoDBEngineAccess(config, xmysqlEngine)

	// 创建认证服务
	authService := auth.NewAuthService(config, engineAccess)

	return &EnhancedBusinessMessageHandler{
		sqlDispatcher: NewSQLDispatcher(config),
		authService:   authService,
		config:        config,
	}
}

// HandleMessage 处理消息
func (h *EnhancedBusinessMessageHandler) HandleMessage(msg protocol.Message) (protocol.Message, error) {
	ctx := context.Background()

	switch msg.Type() {
	case protocol.MSG_CONNECT:
		return h.handleConnectMessage(ctx, msg)
	case protocol.MSG_DISCONNECT:
		return h.handleDisconnectMessage(ctx, msg)
	case protocol.MSG_AUTH_REQUEST:
		return h.handleAuthMessage(ctx, msg)
	case protocol.MSG_QUERY_REQUEST:
		return h.handleQueryMessage(ctx, msg)
	case protocol.MSG_USE_DB_REQUEST:
		return h.handleUseDBMessage(ctx, msg)
	case protocol.MSG_PING:
		return h.handlePingMessage(ctx, msg)
	default:
		return nil, fmt.Errorf("unsupported message type: %d", msg.Type())
	}
}

// CanHandle 检查是否能处理指定类型的消息
func (h *EnhancedBusinessMessageHandler) CanHandle(msgType protocol.MessageType) bool {
	switch msgType {
	case protocol.MSG_CONNECT,
		protocol.MSG_DISCONNECT,
		protocol.MSG_AUTH_REQUEST,
		protocol.MSG_QUERY_REQUEST,
		protocol.MSG_USE_DB_REQUEST,
		protocol.MSG_PING:
		return true
	default:
		return false
	}
}

// handleConnectMessage 处理连接消息
func (h *EnhancedBusinessMessageHandler) handleConnectMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	connectMsg, ok := msg.(*protocol.ConnectMessage)
	if !ok {
		return nil, fmt.Errorf("invalid connect message type")
	}

	// 记录连接信息
	fmt.Printf("Client connecting: %s:%d, User: %s, Database: %s\n",
		connectMsg.ClientInfo.Host,
		connectMsg.ClientInfo.Port,
		connectMsg.ClientInfo.User,
		connectMsg.ClientInfo.Database)

	// 连接建立成功，返回成功响应
	return protocol.NewBaseMessage(protocol.MSG_CONNECT, msg.SessionID(), "Connection established"), nil
}

// handleDisconnectMessage 处理断开连接消息
func (h *EnhancedBusinessMessageHandler) handleDisconnectMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	// 记录断开连接信息
	fmt.Printf("Client disconnected: %s\n", msg.SessionID())

	// 清理会话相关资源
	// 这里可以添加清理逻辑，比如关闭数据库连接、清理缓存等

	// 断开连接成功，返回成功响应
	return protocol.NewBaseMessage(protocol.MSG_DISCONNECT, msg.SessionID(), "Connection closed"), nil
}

// handleAuthMessage 处理认证消息
func (h *EnhancedBusinessMessageHandler) handleAuthMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	authMsg, ok := msg.(*protocol.AuthMessage)
	if !ok {
		return nil, fmt.Errorf("invalid auth message type")
	}

	// 从会话ID中提取客户端IP（简化实现）
	host := h.extractHostFromSessionID(msg.SessionID())

	// 使用认证服务验证用户
	authResult, err := h.authService.AuthenticateUser(
		ctx,
		authMsg.User,
		authMsg.Password,
		host,
		authMsg.Database,
	)
	if err != nil {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        common.ER_ACCESS_DENIED_ERROR,
			State:       "HY000",
			Message:     fmt.Sprintf("Authentication error: %v", err),
		}, nil
	}

	// 检查认证结果
	if !authResult.Success {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        authResult.ErrorCode,
			State:       "28000",
			Message:     authResult.ErrorMessage,
		}, nil
	}

	// 认证成功，记录用户信息
	fmt.Printf("User authenticated successfully: %s@%s, Database: %s\n",
		authResult.User, authResult.Host, authResult.Database)

	// 返回认证成功响应
	return protocol.NewBaseMessage(protocol.MSG_AUTH_RESPONSE, msg.SessionID(), authResult), nil
}

// handleQueryMessage 处理查询消息
func (h *EnhancedBusinessMessageHandler) handleQueryMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	queryMsg, ok := msg.(*protocol.QueryMessage)
	if !ok {
		return nil, fmt.Errorf("invalid query message type")
	}

	// 检查权限（简化实现，实际应该从会话中获取用户信息）
	host := h.extractHostFromSessionID(msg.SessionID())
	user := h.extractUserFromSessionID(msg.SessionID()) // 需要从会话中获取

	// 检查查询权限
	if err := h.checkQueryPrivilege(ctx, user, host, queryMsg.Database, queryMsg.SQL); err != nil {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        common.ER_SPECIFIC_ACCESS_DENIED_ERROR,
			State:       "42000",
			Message:     fmt.Sprintf("Access denied: %v", err),
		}, nil
	}

	// 创建一个临时的session（实际应该从消息中获取）
	session := &EnhancedMockMySQLServerSession{
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

// handleUseDBMessage 处理切换数据库消息
func (h *EnhancedBusinessMessageHandler) handleUseDBMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	useDBMsg, ok := msg.(*protocol.UseDBMessage)
	if !ok {
		return nil, fmt.Errorf("invalid use db message type")
	}

	// 验证数据库是否存在
	if err := h.authService.ValidateDatabase(ctx, useDBMsg.Database); err != nil {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        common.ER_BAD_DB_ERROR,
			State:       "42000",
			Message:     fmt.Sprintf("Unknown database '%s'", useDBMsg.Database),
		}, nil
	}

	// 检查数据库访问权限
	host := h.extractHostFromSessionID(msg.SessionID())
	user := h.extractUserFromSessionID(msg.SessionID())

	if err := h.authService.CheckPrivilege(ctx, user, host, useDBMsg.Database, "", common.SelectPriv); err != nil {
		return &protocol.ErrorMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
			Code:        common.ER_SPECIFIC_ACCESS_DENIED_ERROR,
			State:       "42000",
			Message:     fmt.Sprintf("Access denied for user '%s'@'%s' to database '%s'", user, host, useDBMsg.Database),
		}, nil
	}

	// 切换成功
	fmt.Printf("Database switched to '%s' for session %s\n", useDBMsg.Database, msg.SessionID())
	return protocol.NewBaseMessage(protocol.MSG_USE_DB_RESPONSE, msg.SessionID(), nil), nil
}

// handlePingMessage 处理Ping消息
func (h *EnhancedBusinessMessageHandler) handlePingMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	// Ping消息直接返回成功
	return protocol.NewBaseMessage(protocol.MSG_PING, msg.SessionID(), nil), nil
}

// checkQueryPrivilege 检查查询权限
func (h *EnhancedBusinessMessageHandler) checkQueryPrivilege(ctx context.Context, user, host, database, sql string) error {
	// 解析SQL类型
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	var requiredPriv common.PrivilegeType
	switch {
	case strings.HasPrefix(sqlUpper, "SELECT"):
		requiredPriv = common.SelectPriv
	case strings.HasPrefix(sqlUpper, "INSERT"):
		requiredPriv = common.InsertPriv
	case strings.HasPrefix(sqlUpper, "UPDATE"):
		requiredPriv = common.UpdatePriv
	case strings.HasPrefix(sqlUpper, "DELETE"):
		requiredPriv = common.DeletePriv
	case strings.HasPrefix(sqlUpper, "CREATE"):
		requiredPriv = common.CreatePriv
	case strings.HasPrefix(sqlUpper, "DROP"):
		requiredPriv = common.DropPriv
	case strings.HasPrefix(sqlUpper, "ALTER"):
		requiredPriv = common.AlterPriv
	case strings.HasPrefix(sqlUpper, "SHOW"):
		requiredPriv = common.ShowDBPriv
	default:
		// 对于其他类型的SQL，要求SELECT权限
		requiredPriv = common.SelectPriv
	}

	// 检查权限
	return h.authService.CheckPrivilege(ctx, user, host, database, "", requiredPriv)
}

// extractHostFromSessionID 从会话ID中提取主机信息（简化实现）
func (h *EnhancedBusinessMessageHandler) extractHostFromSessionID(sessionID string) string {
	// 实际实现应该从会话管理器中获取真实的客户端IP
	// 这里简化为解析会话ID或返回默认值
	if strings.Contains(sessionID, "@") {
		parts := strings.Split(sessionID, "@")
		if len(parts) > 1 {
			return parts[1]
		}
	}
	return "127.0.0.1" // 默认本地主机
}

// extractUserFromSessionID 从会话ID中提取用户信息（简化实现）
func (h *EnhancedBusinessMessageHandler) extractUserFromSessionID(sessionID string) string {
	// 实际实现应该从会话管理器中获取真实的用户信息
	// 这里简化为解析会话ID或返回默认值
	if strings.Contains(sessionID, "@") {
		parts := strings.Split(sessionID, "@")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	return "root" // 默认用户
}

// EnhancedMockMySQLServerSession 增强的模拟MySQL服务器会话
type EnhancedMockMySQLServerSession struct {
	sessionID      string
	database       string
	params         map[string]interface{}
	lastActiveTime time.Time
}

func (s *EnhancedMockMySQLServerSession) GetSessionId() string {
	return s.sessionID
}

func (s *EnhancedMockMySQLServerSession) GetLastActiveTime() time.Time {
	return s.lastActiveTime
}

func (s *EnhancedMockMySQLServerSession) SetParamByName(name string, value interface{}) {
	if s.params == nil {
		s.params = make(map[string]interface{})
	}
	s.params[name] = value
}

func (s *EnhancedMockMySQLServerSession) GetParamByName(name string) interface{} {
	if s.params == nil {
		return nil
	}
	return s.params[name]
}

func (s *EnhancedMockMySQLServerSession) SendOK() {
	// 模拟发送OK响应
}

func (s *EnhancedMockMySQLServerSession) SendHandleOk() {
	// 模拟发送握手OK响应
}

func (s *EnhancedMockMySQLServerSession) SendSelectFields() {
	// 模拟发送选择字段
}

func (s *EnhancedMockMySQLServerSession) SendError(code uint16, message string) {
	// 模拟发送错误响应
}

func (s *EnhancedMockMySQLServerSession) SendResultSet(columns []string, rows [][]interface{}) {
	// 模拟发送结果集
}
