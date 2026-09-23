package dispatcher

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

// EnhancedBusinessMessageHandler 增强的业务消息处理器
type EnhancedBusinessMessageHandler struct {
	sqlDispatcher *SQLDispatcher
	authService   auth.AuthService
	config        *conf.Cfg
	sessionsMu    sync.RWMutex
	sessions      map[string]authenticatedSession
}

type authenticatedSession struct {
	user        string
	host        string
	database    string
	activeRoles []string
}

// NewEnhancedBusinessMessageHandler 创建增强的业务消息处理器
func NewEnhancedBusinessMessageHandler(config *conf.Cfg, xmysqlEngine *engine.XMySQLEngine) protocol.MessageHandler {
	// 创建引擎访问
	engineAccess := auth.NewInnoDBEngineAccess(config, xmysqlEngine)

	// 创建认证服务
	authService := auth.NewAuthService(config, engineAccess)

	//  获取存储管理器并创建带存储管理器的SQL分发器
	storageManager := xmysqlEngine.GetStorageManager()
	sqlDispatcher := NewSQLDispatcherWithXMySQLEngine(config, xmysqlEngine, storageManager)

	return &EnhancedBusinessMessageHandler{
		sqlDispatcher: sqlDispatcher,
		authService:   authService,
		config:        config,
		sessions:      make(map[string]authenticatedSession),
	}
}

// HandleMessage 处理消息
func (h *EnhancedBusinessMessageHandler) HandleMessage(msg protocol.Message) (protocol.Message, error) {
	ctx := context.Background()
	logger.Debugf("处理消息: 类型=%d, 会话ID=%s", msg.Type(), msg.SessionID())
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
		logger.Errorf(" 未知消息类型: %d", msg.Type())
		return protocol.NewErrorMessage(msg.SessionID(), common.ER_UNKNOWN_ERROR,
			fmt.Sprintf("Unknown message type: %d", msg.Type())), nil
	}
}

// HandleQueryWithRealSession 使用真实会话对象处理查询
func (h *EnhancedBusinessMessageHandler) HandleQueryWithRealSession(realSession server.MySQLServerSession, query string, database string) (protocol.Message, error) {
	logger.Debugf(" 使用真实会话处理查询: SQL=%s, Database=%s", query, database)

	// 从真实会话中获取用户信息
	var user, host string
	if userParam := realSession.GetParamByName("user"); userParam != nil {
		if u, ok := userParam.(string); ok {
			user = u
		}
	}
	hostParam := realSession.GetParamByName("host")
	if hVal, ok := hostParam.(string); ok && hVal != "" {
		host = hVal
	}
	if host == "" {
		host = "127.0.0.1" // 简化处理
	}

	logger.Debugf(" 查询用户: %s@%s", user, host)

	//  特殊处理简单查询
	if isSelectOneQuery(query) {
		logger.Debugf(" 检测到 SELECT 1 查询，返回硬编码响应")
		// 创建临时消息用于响应生成
		tempMsg := &protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "temp", query),
		}
		return h.createSelectOneResponse(tempMsg), nil
	}

	if engine.IsTransactionCommand(query) {
		logger.Debugf(" 检测到事务控制语句，跳过权限解析并分发到SQLDispatcher")
		return h.dispatchQueryResult("temp", realSession, query, database)
	}

	// 检查权限
	ctx := context.Background()
	if roles, ok := realSession.GetParamByName("active_roles").([]string); ok {
		ctx = server.WithActiveRoles(ctx, roles)
	}
	if isSessionRoleCommand(query) {
		return h.dispatchQueryResult("temp", realSession, query, database)
	}
	if err := h.checkQueryPrivilege(ctx, user, host, database, query); err != nil {
		logger.Errorf(" 权限检查失败: %v", err)
		return protocol.NewErrorMessage("temp", common.ER_ACCESS_DENIED_ERROR,
			user, host, "NO"), nil
	}

	logger.Debugf(" 权限检查通过，准备执行SQL查询")
	logger.Debugf(" 分发SQL查询到SQLDispatcher")

	return h.dispatchQueryResult("temp", realSession, query, database)
}

func (h *EnhancedBusinessMessageHandler) dispatchQueryResult(sessionID string, session server.MySQLServerSession, query string, database string) (protocol.Message, error) {
	resultChan := h.sqlDispatcher.Dispatch(session, query, database)

	logger.Debugf(" 等待SQL分发器结果...")

	// 等待结果
	resultCount := 0
	for result := range resultChan {
		resultCount++
		logger.Debugf(" 收到查询结果 #%d: columns=%v, rows=%d, error=%v, type=%v",
			resultCount, result.Columns, len(result.Rows), result.Err, result.ResultType)

		// 如果有错误，记录详细信息
		if result.Err != nil {
			logger.Errorf(" SQL执行错误: %v", result.Err)
			return protocol.NewErrorMessageFromGoError(sessionID, result.Err), nil
		}
		if shouldReloadPrivilegesAfterQuery(query) {
			if err := h.authService.FlushPrivileges(context.Background()); err != nil {
				return protocol.NewErrorMessageFromGoError(sessionID, err), nil
			}
		}

		// 转换结果格式
		queryResult := &protocol.MessageQueryResult{
			Columns:      result.Columns,
			ColumnTypes:  result.ColumnTypes,
			Rows:         result.Rows,
			Error:        result.Err,
			Message:      result.Message,
			Type:         result.ResultType,
			AffectedRows: result.AffectedRows,
			LastInsertID: result.LastInsertID,
			WarningCount: result.WarningCount,
		}

		responseMsg := &protocol.ResponseMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, sessionID, queryResult),
			Result:      queryResult,
		}

		logger.Debugf(" 返回查询响应: columns=%d, rows=%d", len(result.Columns), len(result.Rows))
		return responseMsg, nil
	}

	logger.Errorf(" 未收到查询结果，结果数量: %d", resultCount)
	return protocol.NewErrorMessage(sessionID, common.ER_UNKNOWN_ERROR,
		"No result received from query execution"), nil
}

func shouldReloadPrivilegesAfterQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	for _, prefix := range []string{"create user", "alter user", "drop user", "grant ", "revoke ", "set default role", "flush privileges", "commit", "rollback"} {
		if lower == prefix || strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func isSessionRoleCommand(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return strings.HasPrefix(lower, "set role ")
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
	logger.Debugf("Client connecting: %s:%d, User: %s, Database: %s\n",
		connectMsg.ConnectionInfo.Host,
		connectMsg.ConnectionInfo.Port,
		connectMsg.ConnectionInfo.User,
		connectMsg.ConnectionInfo.Database)

	// 连接建立成功，返回成功响应
	return protocol.NewBaseMessage(protocol.MSG_CONNECT, msg.SessionID(), "Connection established"), nil
}

// handleDisconnectMessage 处理断开连接消息
func (h *EnhancedBusinessMessageHandler) handleDisconnectMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	// 记录断开连接信息
	logger.Debugf("Client disconnected: %s\n", msg.SessionID())
	h.sessionsMu.Lock()
	delete(h.sessions, msg.SessionID())
	h.sessionsMu.Unlock()

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
		return protocol.NewErrorMessageFromGoError(msg.SessionID(), err), nil
	}

	// 检查认证结果
	if !authResult.Success {
		// 使用认证结果中的错误码创建错误消息
		sqlErr := common.NewErr(authResult.ErrorCode, authResult.ErrorMessage)
		return protocol.NewErrorMessageFromGoError(msg.SessionID(), sqlErr), nil
	}

	// 认证成功，记录用户信息
	logger.Debugf("User authenticated successfully: %s@%s, Database: %s\n",
		authResult.User, authResult.Host, authResult.Database)
	h.sessionsMu.Lock()
	if h.sessions == nil {
		h.sessions = make(map[string]authenticatedSession)
	}
	h.sessions[msg.SessionID()] = authenticatedSession{
		user:        authResult.User,
		host:        authResult.Host,
		database:    authResult.Database,
		activeRoles: append([]string(nil), authResult.ActiveRoles...),
	}
	h.sessionsMu.Unlock()

	// 返回认证成功响应
	return protocol.NewBaseMessage(protocol.MSG_AUTH_RESPONSE, msg.SessionID(), authResult), nil
}

// handleQueryMessage 处理查询消息
func (h *EnhancedBusinessMessageHandler) handleQueryMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	queryMsg, ok := msg.(*protocol.QueryMessage)
	if !ok {
		return nil, fmt.Errorf("invalid query message type")
	}

	logger.Debugf(" 处理查询: SQL=%s, Database=%s", queryMsg.SQL, queryMsg.Database)

	// 优先使用认证阶段绑定到 session ID 的身份；只有兼容旧消息/测试
	// 使用带 user@host 的 session ID 时才回退到解析 session ID。
	identity, authenticated := h.authenticatedSession(msg.SessionID())
	user, host := identity.user, identity.host
	if !authenticated {
		user = h.extractUserFromSessionID(msg.SessionID())
		host = h.extractHostFromSessionID(msg.SessionID())
	}

	logger.Debugf(" 查询用户: %s@%s", user, host)

	//  特殊处理简单查询
	if isSelectOneQuery(queryMsg.SQL) {
		logger.Debugf(" 检测到 SELECT 1 查询，返回硬编码响应")
		return h.createSelectOneResponse(msg), nil
	}

	if engine.IsTransactionCommand(queryMsg.SQL) {
		logger.Debugf(" 检测到事务控制语句，跳过权限解析并分发到SQLDispatcher")
		sessionCtx := server.NewSessionContext(msg.SessionID())
		sessionCtx.SetCurrentDB(queryMsg.Database)
		session := &EnhancedMockMySQLServerSession{
			sessionID: msg.SessionID(),
			database:  queryMsg.Database,
			ctx:       sessionCtx,
		}
		return h.dispatchQueryResult(msg.SessionID(), session, queryMsg.SQL, queryMsg.Database)
	}

	if authenticated {
		ctx = server.WithActiveRoles(ctx, identity.activeRoles)
	}

	// 检查权限
	if !isSessionRoleCommand(queryMsg.SQL) {
		if err := h.checkQueryPrivilege(ctx, user, host, queryMsg.Database, queryMsg.SQL); err != nil {
			logger.Errorf(" 权限检查失败: %v", err)
			return protocol.NewErrorMessage(msg.SessionID(), common.ER_ACCESS_DENIED_ERROR,
				user, host, "NO"), nil
		}
	}

	logger.Debugf(" 权限检查通过，准备执行SQL查询")

	// 创建一个临时的session（实际应该从消息中获取）
	sessionCtx := server.NewSessionContext(msg.SessionID())
	sessionCtx.SetCurrentDB(queryMsg.Database)
	session := &EnhancedMockMySQLServerSession{
		sessionID: msg.SessionID(),
		database:  queryMsg.Database,
		ctx:       sessionCtx,
	}

	logger.Debugf(" 分发SQL查询到SQLDispatcher")

	// 分发SQL查询
	resultChan := h.sqlDispatcher.Dispatch(session, queryMsg.SQL, queryMsg.Database)

	logger.Debugf(" 等待SQL分发器结果...")

	// 等待结果
	resultCount := 0
	for result := range resultChan {
		resultCount++
		logger.Debugf(" 收到查询结果 #%d: columns=%v, rows=%d, error=%v, type=%v",
			resultCount, result.Columns, len(result.Rows), result.Err, result.ResultType)

		// 如果有错误，记录详细信息
		if result.Err != nil {
			logger.Errorf(" SQL执行错误: %v", result.Err)
			return protocol.NewErrorMessageFromGoError(msg.SessionID(), result.Err), nil
		}

		// 转换结果格式
		var columnTypes []string
		if len(result.Columns) > 0 && len(result.Rows) > 0 {
			columnTypes = h.inferColumnTypes(result.Rows, len(result.Columns))
		}

		queryResult := &protocol.MessageQueryResult{
			Columns:      result.Columns,
			ColumnTypes:  columnTypes,
			Rows:         result.Rows,
			Error:        result.Err,
			Message:      result.Message,
			Type:         result.ResultType,
			AffectedRows: result.AffectedRows,
			LastInsertID: result.LastInsertID,
		}

		responseMsg := &protocol.ResponseMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, msg.SessionID(), queryResult),
			Result:      queryResult,
		}

		logger.Debugf(" 返回查询响应: columns=%d, rows=%d", len(result.Columns), len(result.Rows))
		return responseMsg, nil
	}

	logger.Errorf(" 未收到查询结果，结果数量: %d", resultCount)
	return &protocol.ErrorMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, msg.SessionID(), nil),
		Code:        common.ER_UNKNOWN_ERROR,
		State:       "42000",
		Message:     "No result received from query execution",
	}, nil
}

// handleUseDBMessage 处理切换数据库消息
func (h *EnhancedBusinessMessageHandler) handleUseDBMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	useDBMsg, ok := msg.(*protocol.UseDBMessage)
	if !ok {
		return nil, fmt.Errorf("invalid use db message type")
	}

	logger.Debugf("  处理切换数据库消息: sessionID=%s, database=%s", msg.SessionID(), useDBMsg.Database)

	// 验证数据库是否存在
	if err := h.authService.ValidateDatabase(ctx, useDBMsg.Database); err != nil {
		logger.Errorf(" 数据库验证失败: database=%s, error=%v", useDBMsg.Database, err)
		return protocol.NewErrorMessage(msg.SessionID(), common.ER_BAD_DB_ERROR,
			useDBMsg.Database), nil
	}

	logger.Debugf(" 数据库验证通过: database=%s", useDBMsg.Database)

	// 检查数据库访问权限
	identity, authenticated := h.authenticatedSession(msg.SessionID())
	host, user := identity.host, identity.user
	if !authenticated {
		host = h.extractHostFromSessionID(msg.SessionID())
		user = h.extractUserFromSessionID(msg.SessionID())
	}

	logger.Debugf(" 提取用户信息: user=%s, host=%s", user, host)

	if authenticated {
		ctx = server.WithActiveRoles(ctx, identity.activeRoles)
	}

	// 检查数据库访问权限
	if err := h.authService.CheckPrivilege(ctx, user, host, useDBMsg.Database, "", common.SelectPriv); err != nil {
		logger.Errorf(" 数据库访问权限检查失败: user=%s, host=%s, database=%s, error=%v",
			user, host, useDBMsg.Database, err)
		return protocol.NewErrorMessage(msg.SessionID(), common.ER_SPECIFIC_ACCESS_DENIED_ERROR,
			user, host, useDBMsg.Database), nil
	}
	logger.Debugf(" 数据库访问权限检查通过")

	// 切换成功
	logger.Debugf("Database switched to '%s' for session %s\n", useDBMsg.Database, msg.SessionID())
	return protocol.NewBaseMessage(protocol.MSG_USE_DB_RESPONSE, msg.SessionID(), nil), nil
}

// handlePingMessage 处理Ping消息
func (h *EnhancedBusinessMessageHandler) handlePingMessage(ctx context.Context, msg protocol.Message) (protocol.Message, error) {
	// Ping消息直接返回成功
	return protocol.NewBaseMessage(protocol.MSG_PING, msg.SessionID(), nil), nil
}

// checkQueryPrivilege 检查查询权限
func (h *EnhancedBusinessMessageHandler) checkQueryPrivilege(ctx context.Context, user, host, database, sql string) error {
	logger.Debugf(" 检查查询权限: user=%s, host=%s, database=%s, sql=%s", user, host, database, sql)
	if isSelfServiceSetPassword(user, host, sql) {
		logger.Debugf(" SET PASSWORD targets the current account; no global privilege required")
		return nil
	}

	// 解析SQL类型
	sqlType := h.parseSQLType(sql)
	logger.Debugf(" SQL类型: %s", sqlType)

	// 确定所需权限
	requiredPrivs := h.getRequiredPrivilegesForSQL(sql, sqlType)
	logger.Debugf(" 所需权限: %v", requiredPrivs)

	// 调用认证服务检查全部所需权限，不能只检查列表中的第一个权限。
	logger.Debugf(" 调用AuthService.CheckPrivilege...")
	tables := extractPrivilegeTables(sql)
	requirements := privilegeRequirementsForQuery(sql, sqlType, tables, requiredPrivs)
	var err error
	for _, requirement := range requirements {
		if err = h.checkRequiredPrivileges(ctx, user, host, database, requirement.table, requirement.privileges); err != nil {
			break
		}
	}

	if err != nil {
		logger.Errorf(" 权限检查失败: %v", err)

		// 连接探测 SELECT 1 保持无表访问权限要求。
		if strings.Contains(strings.ToLower(sql), "select 1") {
			logger.Warnf("  SELECT 1 连接探测权限检查失败，但允许继续执行")
			return nil // 允许继续执行
		}

		return err
	} else {
		logger.Debugf(" 权限检查通过")
		return nil
	}
}

type privilegeRequirement struct {
	table      string
	privileges []common.PrivilegeType
}

func privilegeRequirementsForQuery(sql, sqlType string, tables []string, requiredPrivs []common.PrivilegeType) []privilegeRequirement {
	normalizedSQL := strings.Join(strings.Fields(strings.ToUpper(sql)), " ")
	if (sqlType == "CREATE" && (strings.HasPrefix(normalizedSQL, "CREATE VIEW ") || strings.HasPrefix(normalizedSQL, "CREATE OR REPLACE VIEW "))) ||
		(sqlType == "ALTER" && strings.HasPrefix(normalizedSQL, "ALTER VIEW ")) {
		viewPrivileges := []common.PrivilegeType{common.CreateViewPriv}
		viewName := createViewObjectName(sql)
		if sqlType == "ALTER" {
			viewName = alterViewObjectName(sql)
		}
		if strings.HasPrefix(normalizedSQL, "CREATE OR REPLACE VIEW ") {
			// Replacing an existing view also requires DROP on the view
			// object in MySQL, in addition to CREATE VIEW.
			if viewName == "" {
				viewPrivileges = append(viewPrivileges, common.DropPriv)
			}
		}
		if sqlType == "ALTER" && viewName == "" {
			viewPrivileges = append(viewPrivileges, common.DropPriv)
		}
		requirements := []privilegeRequirement{{table: "", privileges: viewPrivileges}}
		if (strings.HasPrefix(normalizedSQL, "CREATE OR REPLACE VIEW ") || sqlType == "ALTER") && viewName != "" {
			requirements = append(requirements, privilegeRequirement{table: viewName, privileges: []common.PrivilegeType{common.DropPriv}})
		}
		for _, table := range tables {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
		}
		return requirements
	}
	if strings.Contains(normalizedSQL, " REFERENCES ") && (sqlType == "CREATE" || sqlType == "ALTER") {
		if requirements := foreignKeyPrivilegeRequirements(sql, sqlType, tables); len(requirements) > 0 {
			return requirements
		}
	}
	if sqlType == "ALTER" && strings.HasPrefix(normalizedSQL, "ALTER TABLE ") && len(tables) > 0 {
		return []privilegeRequirement{{table: tables[0], privileges: []common.PrivilegeType{common.AlterPriv, common.CreatePriv, common.InsertPriv}}}
	}
	if sqlType == "DROP" && strings.HasPrefix(normalizedSQL, "DROP VIEW ") {
		viewNames := dropViewObjectNames(sql)
		if len(viewNames) > 0 {
			requirements := make([]privilegeRequirement, 0, len(viewNames))
			for _, viewName := range viewNames {
				requirements = append(requirements, privilegeRequirement{table: viewName, privileges: []common.PrivilegeType{common.DropPriv}})
			}
			return requirements
		}
	}
	if sqlType == "DROP" && (strings.HasPrefix(normalizedSQL, "DROP TABLE ") || strings.HasPrefix(normalizedSQL, "DROP TEMPORARY TABLE ")) {
		tableNames := dropTableObjectNames(sql)
		if len(tableNames) > 0 {
			requirements := make([]privilegeRequirement, 0, len(tableNames))
			for _, tableName := range tableNames {
				requirements = append(requirements, privilegeRequirement{table: tableName, privileges: []common.PrivilegeType{common.DropPriv}})
			}
			return requirements
		}
	}
	if sqlType == "RENAME" && strings.HasPrefix(normalizedSQL, "RENAME TABLE ") {
		if requirements := renameTablePrivilegeRequirements(sql); len(requirements) > 0 {
			return requirements
		}
	}
	if len(tables) == 0 {
		return []privilegeRequirement{{table: "", privileges: requiredPrivs}}
	}
	if sqlType == "CREATE" && strings.HasPrefix(normalizedSQL, "CREATE TABLE ") && strings.Contains(normalizedSQL, " SELECT ") {
		requirements := []privilegeRequirement{{table: tables[0], privileges: []common.PrivilegeType{common.CreatePriv}}}
		for _, table := range tables[1:] {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
		}
		return requirements
	}
	if sqlType == "CREATE" && isCreateTableLikeStatement(strings.Fields(normalizedSQL)) {
		requirements := []privilegeRequirement{{table: tables[0], privileges: []common.PrivilegeType{common.CreatePriv}}}
		for _, table := range tables[1:] {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
		}
		return requirements
	}
	if sqlType == "REPLACE" {
		targetPrivileges := []common.PrivilegeType{common.InsertPriv, common.DeletePriv}
		if !strings.Contains(normalizedSQL, " SELECT ") {
			return []privilegeRequirement{{table: tables[0], privileges: targetPrivileges}}
		}
		requirements := []privilegeRequirement{{table: tables[0], privileges: targetPrivileges}}
		for _, table := range tables[1:] {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
		}
		return requirements
	}
	if sqlType == "UPDATE" && strings.Contains(normalizedSQL, " JOIN ") && strings.Contains(normalizedSQL, " SET ") && len(tables) > 1 {
		requirements := []privilegeRequirement{{table: tables[0], privileges: []common.PrivilegeType{common.UpdatePriv}}}
		for _, table := range tables[1:] {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
		}
		return requirements
	}
	if sqlType == "DELETE" && strings.Contains(normalizedSQL, " JOIN ") && len(tables) > 1 {
		requirements := []privilegeRequirement{{table: tables[0], privileges: []common.PrivilegeType{common.DeletePriv}}}
		for _, table := range tables[1:] {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
		}
		return requirements
	}
	targetPrivileges := []common.PrivilegeType{common.InsertPriv}
	if strings.Contains(normalizedSQL, " ON DUPLICATE KEY UPDATE ") {
		targetPrivileges = append(targetPrivileges, common.UpdatePriv)
	}
	if sqlType != "INSERT" || !strings.Contains(normalizedSQL, " SELECT ") {
		requirements := make([]privilegeRequirement, 0, len(tables))
		for _, table := range tables {
			privileges := requiredPrivs
			if sqlType == "INSERT" && strings.Contains(normalizedSQL, " ON DUPLICATE KEY UPDATE ") {
				privileges = targetPrivileges
			}
			requirements = append(requirements, privilegeRequirement{table: table, privileges: privileges})
		}
		return requirements
	}

	// INSERT ... SELECT needs INSERT on the destination and SELECT on every
	// source table. Keep the existing conservative routing for other DML forms.
	requirements := []privilegeRequirement{{table: tables[0], privileges: targetPrivileges}}
	for _, table := range tables[1:] {
		if strings.EqualFold(table, tables[0]) {
			requirements[0].privileges = append(requirements[0].privileges, common.SelectPriv)
			continue
		}
		requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.SelectPriv}})
	}
	return requirements
}

func renameTablePrivilegeRequirements(sql string) []privilegeRequirement {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	marker := "RENAME TABLE"
	markerIndex := strings.Index(upperSQL, marker)
	if markerIndex < 0 {
		return nil
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql[markerIndex+len(marker):]), ";"))
	if body == "" {
		return nil
	}
	requirements := make([]privilegeRequirement, 0)
	for _, clause := range strings.Split(body, ",") {
		fields := strings.Fields(clause)
		if len(fields) < 3 {
			return nil
		}
		toIndex := -1
		for index, field := range fields {
			if strings.EqualFold(field, "TO") {
				toIndex = index
				break
			}
		}
		if toIndex != 1 || toIndex+1 >= len(fields) {
			return nil
		}
		oldName := normalizePrivilegeObjectName(fields[0])
		newName := normalizePrivilegeObjectName(fields[toIndex+1])
		if oldName == "" || newName == "" {
			return nil
		}
		requirements = append(requirements,
			privilegeRequirement{table: oldName, privileges: []common.PrivilegeType{common.AlterPriv, common.DropPriv}},
			privilegeRequirement{table: newName, privileges: []common.PrivilegeType{common.CreatePriv, common.InsertPriv}},
		)
	}
	return requirements
}

func foreignKeyPrivilegeRequirements(sql, sqlType string, tables []string) []privilegeRequirement {
	if len(tables) < 2 {
		return nil
	}
	target := tables[0]
	if sqlType == "CREATE" {
		fields := strings.Fields(sql)
		for index := 0; index+1 < len(fields); index++ {
			if !strings.EqualFold(fields[index], "TABLE") {
				continue
			}
			if index+3 < len(fields) && strings.EqualFold(fields[index+1], "IF") &&
				strings.EqualFold(fields[index+2], "NOT") && strings.EqualFold(fields[index+3], "EXISTS") {
				if index+4 < len(fields) {
					target = normalizePrivilegeObjectName(fields[index+4])
				}
			}
			break
		}
	}
	if target == "" {
		return nil
	}
	privileges := []common.PrivilegeType{common.AlterPriv, common.CreatePriv, common.InsertPriv}
	if sqlType == "CREATE" {
		privileges = []common.PrivilegeType{common.CreatePriv}
	}
	requirements := []privilegeRequirement{{table: target, privileges: privileges}}
	for _, table := range tables[1:] {
		if !strings.EqualFold(table, target) {
			requirements = append(requirements, privilegeRequirement{table: table, privileges: []common.PrivilegeType{common.ReferencesPriv}})
		}
	}
	return requirements
}

func createViewObjectName(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) < 5 || !strings.EqualFold(fields[0], "CREATE") ||
		!strings.EqualFold(fields[1], "OR") || !strings.EqualFold(fields[2], "REPLACE") ||
		!strings.EqualFold(fields[3], "VIEW") {
		return ""
	}
	return normalizePrivilegeObjectName(fields[4])
}

func alterViewObjectName(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) < 4 || !strings.EqualFold(fields[0], "ALTER") || !strings.EqualFold(fields[1], "VIEW") {
		return ""
	}
	return normalizePrivilegeObjectName(fields[2])
}

func dropViewObjectNames(sql string) []string {
	fields := strings.Fields(sql)
	if len(fields) < 3 || !strings.EqualFold(fields[0], "DROP") || !strings.EqualFold(fields[1], "VIEW") {
		return nil
	}
	start := 2
	if start+2 < len(fields) && strings.EqualFold(fields[start], "IF") &&
		strings.EqualFold(fields[start+1], "EXISTS") {
		start += 2
	}
	if start >= len(fields) {
		return nil
	}
	names := make([]string, 0, len(fields)-start)
	seen := make(map[string]struct{})
	for _, field := range fields[start:] {
		for _, rawName := range strings.Split(field, ",") {
			name := normalizePrivilegeObjectName(rawName)
			if name == "" {
				continue
			}
			key := strings.ToLower(name)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			names = append(names, name)
		}
	}
	return names
}

func dropTableObjectNames(sql string) []string {
	fields := strings.Fields(sql)
	if len(fields) < 3 || !strings.EqualFold(fields[0], "DROP") {
		return nil
	}
	start := 1
	if strings.EqualFold(fields[start], "TEMPORARY") {
		start++
	}
	if start >= len(fields) || !strings.EqualFold(fields[start], "TABLE") {
		return nil
	}
	start++
	if start+1 < len(fields) && strings.EqualFold(fields[start], "IF") && strings.EqualFold(fields[start+1], "EXISTS") {
		start += 2
	}
	if start >= len(fields) {
		return nil
	}
	names := make([]string, 0, len(fields)-start)
	seen := make(map[string]struct{})
	for _, field := range fields[start:] {
		for _, rawName := range strings.Split(field, ",") {
			name := normalizePrivilegeObjectName(rawName)
			if name == "" {
				continue
			}
			key := strings.ToLower(name)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			names = append(names, name)
		}
	}
	return names
}

func (h *EnhancedBusinessMessageHandler) checkRequiredPrivileges(
	ctx context.Context,
	user, host, database, table string,
	requiredPrivs []common.PrivilegeType,
) error {
	if len(requiredPrivs) == 0 {
		requiredPrivs = []common.PrivilegeType{common.SelectPriv}
	}
	for _, requiredPriv := range requiredPrivs {
		if err := h.authService.CheckPrivilege(ctx, user, host, database, table, requiredPriv); err != nil {
			return err
		}
	}
	return nil
}

// extractPrivilegeTable extracts the directly referenced table for the common
// statement forms handled by the dispatcher. Passing it to AuthService keeps
// table-scope grants effective; unsupported/complex forms conservatively
// return an empty table and rely on database/global privilege checks.
func extractPrivilegeTable(sql string) string {
	tables := extractPrivilegeTables(sql)
	if len(tables) == 0 {
		return ""
	}
	return tables[0]
}

func extractPrivilegeTables(sql string) []string {
	trimmedSQL := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	fields := strings.Fields(trimmedSQL)
	if len(fields) < 2 {
		return nil
	}
	upperSQL := strings.ToUpper(trimmedSQL)
	if strings.HasPrefix(upperSQL, "SHOW CREATE VIEW ") && len(fields) >= 4 {
		return []string{normalizePrivilegeObjectName(fields[3])}
	}
	if strings.HasPrefix(upperSQL, "CALL ") && len(fields) >= 2 {
		return []string{normalizePrivilegeObjectName(fields[1])}
	}
	keywords := map[string]struct{}{
		"from": {}, "into": {}, "update": {}, "table": {}, "on": {}, "join": {}, "using": {}, "like": {}, "references": {},
	}
	grantStatement := strings.HasPrefix(upperSQL, "GRANT ") || strings.HasPrefix(upperSQL, "REVOKE ")
	createTableLike := isCreateTableLikeStatement(fields)
	tables := make([]string, 0, 2)
	seen := make(map[string]struct{})
	for i := 0; i+1 < len(fields); i++ {
		if _, ok := keywords[strings.ToLower(fields[i])]; !ok {
			continue
		}
		if strings.EqualFold(fields[i], "update") && strings.Contains(upperSQL, "ON DUPLICATE KEY UPDATE") {
			continue
		}
		if strings.EqualFold(fields[i], "like") && !createTableLike {
			continue
		}
		if grantStatement && strings.EqualFold(fields[i], "from") {
			continue
		}
		candidate := strings.Trim(fields[i+1], "`(),")
		if strings.EqualFold(fields[i], "table") && strings.EqualFold(candidate, "if") && i+4 < len(fields) &&
			strings.EqualFold(fields[i+2], "not") && strings.EqualFold(fields[i+3], "exists") {
			candidate = strings.Trim(fields[i+4], "`(),")
		}
		if candidate == "" || strings.HasPrefix(candidate, "(") {
			continue
		}
		// JOIN ... ON a.id=b.id and INSERT ... ON DUPLICATE KEY UPDATE
		// are predicates/control clauses, not table references.
		if strings.EqualFold(fields[i], "on") && (strings.Contains(candidate, "=") || strings.EqualFold(candidate, "duplicate") || (i+2 < len(fields) && strings.ContainsAny(fields[i+2], "=<>"))) {
			continue
		}
		candidate = normalizePrivilegeObjectName(candidate)
		if candidate == "" {
			continue
		}
		key := strings.ToLower(candidate)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, candidate)
	}
	return tables
}

func isCreateTableLikeStatement(fields []string) bool {
	if len(fields) < 4 || !strings.EqualFold(fields[0], "CREATE") || !strings.EqualFold(fields[1], "TABLE") {
		return false
	}
	index := 2
	if index+2 < len(fields) && strings.EqualFold(fields[index], "IF") && strings.EqualFold(fields[index+1], "NOT") && strings.EqualFold(fields[index+2], "EXISTS") {
		index += 3
	}
	return index+1 < len(fields) && strings.EqualFold(fields[index+1], "LIKE")
}

func normalizePrivilegeObjectName(name string) string {
	name = strings.Trim(name, "`(),;")
	if parenthesis := strings.IndexByte(name, '('); parenthesis >= 0 {
		name = name[:parenthesis]
	}
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	return strings.Trim(name, "`")
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
	return "" // 未携带身份时保持未认证，不得提升为 root
}

func (h *EnhancedBusinessMessageHandler) authenticatedSession(sessionID string) (authenticatedSession, bool) {
	h.sessionsMu.RLock()
	identity, ok := h.sessions[sessionID]
	h.sessionsMu.RUnlock()
	return identity, ok
}

// EnhancedMockMySQLServerSession 增强的模拟MySQL服务器会话
type EnhancedMockMySQLServerSession struct {
	sessionID      string
	database       string
	params         map[string]interface{}
	ctx            *server.SessionContext
	lastActiveTime time.Time
	session        Session // 添加session字段用于网络通信
}

// Session 网络会话接口，用于发送数据到客户端
type Session interface {
	WriteBytes([]byte) error
}

func (s *EnhancedMockMySQLServerSession) GetSessionId() string {
	return s.sessionID
}

func (s *EnhancedMockMySQLServerSession) GetLastActiveTime() time.Time {
	return s.lastActiveTime
}

func (s *EnhancedMockMySQLServerSession) SessionContext() *server.SessionContext {
	if s.ctx == nil {
		s.ctx = server.NewSessionContext(s.sessionID)
		s.ctx.SetCurrentDB(s.database)
	}
	return s.ctx
}

func (s *EnhancedMockMySQLServerSession) SetParamByName(name string, value interface{}) {
	if s.params == nil {
		s.params = make(map[string]interface{})
	}
	s.params[name] = value
	// 双写 SessionContext，与真实 session 行为一致
	if s.ctx != nil {
		switch name {
		case "database":
			s.ctx.SetCurrentDB(toString(value))
		case "user":
			s.ctx.SetUsername(toString(value))
		case "autocommit":
			s.ctx.SetAutocommit(toBool(value))
		case "in_transaction":
			s.ctx.SetInTransaction(toBool(value))
		}
	}
}

func (s *EnhancedMockMySQLServerSession) GetParamByName(name string) interface{} {
	if s.params == nil {
		if s.ctx != nil {
			switch name {
			case "database":
				return s.ctx.GetCurrentDB()
			case "user":
				return s.ctx.GetUsername()
			case "in_transaction":
				return s.ctx.GetInTransaction()
			}
		}
		return nil
	}
	return s.params[name]
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toBool(v interface{}) bool {
	if v == nil {
		return true
	}
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val == "1" || val == "ON" || val == "on" || val == "true"
	}
	return true
}

func (s *EnhancedMockMySQLServerSession) SendOK() {
	// 发送OK响应包
	buff := make([]byte, 0)
	buff = protocol.EncodeOK(buff, 0, 0, nil)

	if s.session != nil {
		s.session.WriteBytes(buff)
	}
	logger.Debug("Sent OK packet")
}

func (s *EnhancedMockMySQLServerSession) SendHandleOk() {
	// 发送握手OK响应包
	buff := make([]byte, 0)
	buff = protocol.EncodeHandshake(buff)

	if s.session != nil {
		s.session.WriteBytes(buff)
	}
	logger.Debug("Sent handshake packet")
}

func (s *EnhancedMockMySQLServerSession) SendSelectFields() {
	// 发送SELECT字段定义
	// 这里我们创建一个简单的字段定义示例
	fieldCount := 1
	selectResponse := protocol.NewSelectResponse(fieldCount)

	// 添加一个示例字段
	selectResponse.AddField("id", common.COLUMN_TYPE_LONG)

	// 发送结果集头部
	headerData := selectResponse.Header.EncodeBuff()
	if s.session != nil {
		s.session.WriteBytes(headerData)
	}

	// 发送字段定义
	fieldsData := selectResponse.EncodeFields()
	if s.session != nil {
		s.session.WriteBytes(fieldsData)
	}

	// 发送EOF包结束字段定义
	eofData := selectResponse.EncodeEof()
	if s.session != nil {
		s.session.WriteBytes(eofData)
	}

	logger.Debug("Sent SELECT fields")
}

func (s *EnhancedMockMySQLServerSession) SendError(code uint16, message string) {
	// 发送错误响应包
	errorPacket := &protocol.ErrorPacket{}
	errorPacket.InitErrorPacket()

	errorData := errorPacket.EncodeErrorPacket(message)

	if s.session != nil {
		s.session.WriteBytes(errorData)
	}

	logger.Errorf("Sent error packet: code=%d, message=%s", code, message)
}

func (s *EnhancedMockMySQLServerSession) SendResultSet(columns []string, rows [][]interface{}) {
	// 发送完整的结果集
	fieldCount := len(columns)
	if fieldCount == 0 {
		s.SendError(1054, "No columns specified")
		return
	}

	selectResponse := protocol.NewSelectResponse(fieldCount)

	// 添加字段定义
	for _, columnName := range columns {
		selectResponse.AddField(columnName, common.COLUMN_TYPE_VAR_STRING)
	}

	// 发送结果集头部（字段数量）
	headerData := selectResponse.Header.EncodeBuff()
	if s.session != nil {
		s.session.WriteBytes(headerData)
	}

	// 发送字段定义
	fieldsData := selectResponse.EncodeFields()
	if s.session != nil {
		s.session.WriteBytes(fieldsData)
	}

	// 发送EOF包结束字段定义阶段
	eofData := selectResponse.EncodeEof()
	if s.session != nil {
		s.session.WriteBytes(eofData)
	}

	// 发送数据行
	for _, row := range rows {
		// 将interface{}类型的行数据转换为字符串
		stringRow := make([]string, len(row))
		for i, value := range row {
			if value == nil {
				stringRow[i] = ""
			} else {
				stringRow[i] = fmt.Sprintf("%v", value)
			}
		}

		// 编码并发送行数据
		rowData := selectResponse.WriteStringRows(stringRow)
		if s.session != nil {
			s.session.WriteBytes(rowData)
		}
	}

	// 发送最后的EOF包结束数据传输
	lastEofData := selectResponse.EncodeLastEof()
	if s.session != nil {
		s.session.WriteBytes(lastEofData)
	}

	logger.Infof("Sent result set: %d columns, %d rows", len(columns), len(rows))
}

// NewEnhancedMockMySQLServerSessionWithSession 创建一个带有实际网络会话的MySQLServerSession实例
func NewEnhancedMockMySQLServerSessionWithSession(sessionID string, database string, netSession Session) *EnhancedMockMySQLServerSession {
	ctx := server.NewSessionContext(sessionID)
	ctx.SetCurrentDB(database)
	return &EnhancedMockMySQLServerSession{
		sessionID:      sessionID,
		database:       database,
		params:         make(map[string]interface{}),
		ctx:            ctx,
		lastActiveTime: time.Now(),
		session:        netSession,
	}
}

// NewEnhancedMockMySQLServerSession 创建一个没有网络会话的MySQLServerSession实例（仅用于测试）
func NewEnhancedMockMySQLServerSession(sessionID string, database string) *EnhancedMockMySQLServerSession {
	ctx := server.NewSessionContext(sessionID)
	ctx.SetCurrentDB(database)
	return &EnhancedMockMySQLServerSession{
		sessionID:      sessionID,
		database:       database,
		params:         make(map[string]interface{}),
		ctx:            ctx,
		lastActiveTime: time.Now(),
		session:        nil, // 没有实际的网络会话
	}
}

// parseSQLType 解析SQL类型
func (h *EnhancedBusinessMessageHandler) parseSQLType(sql string) string {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	switch {
	case strings.HasPrefix(sqlUpper, "SELECT"):
		return "SELECT"
	case strings.HasPrefix(sqlUpper, "CALL"):
		return "CALL"
	case strings.HasPrefix(sqlUpper, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(sqlUpper, "REPLACE"):
		return "REPLACE"
	case strings.HasPrefix(sqlUpper, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(sqlUpper, "DELETE"):
		return "DELETE"
	case strings.HasPrefix(sqlUpper, "RENAME TABLE"):
		return "RENAME"
	case strings.HasPrefix(sqlUpper, "TRUNCATE"):
		return "TRUNCATE"
	case strings.HasPrefix(sqlUpper, "CREATE"):
		return "CREATE"
	case strings.HasPrefix(sqlUpper, "DROP"):
		return "DROP"
	case strings.HasPrefix(sqlUpper, "ALTER"):
		return "ALTER"
	case strings.HasPrefix(sqlUpper, "SHOW"):
		return "SHOW"
	default:
		return "OTHER"
	}
}

// getRequiredPrivilegesForSQL refines the coarse statement class with the
// feature-specific privileges MySQL applies to stored objects. Keeping this
// at the dispatcher boundary ensures the executor cannot accidentally turn a
// CREATE VIEW/TRIGGER/EVENT/ROUTINE into a generic CREATE check.
func (h *EnhancedBusinessMessageHandler) getRequiredPrivilegesForSQL(sql, sqlType string) []common.PrivilegeType {
	normalized := strings.Join(strings.Fields(strings.ToUpper(strings.TrimSpace(sql))), " ")
	switch {
	case strings.HasPrefix(normalized, "CREATE OR REPLACE VIEW "):
		return []common.PrivilegeType{common.CreateViewPriv, common.DropPriv}
	case strings.HasPrefix(normalized, "CREATE VIEW "):
		return []common.PrivilegeType{common.CreateViewPriv}
	case strings.HasPrefix(normalized, "ALTER VIEW "):
		return []common.PrivilegeType{common.CreateViewPriv, common.DropPriv}
	case strings.HasPrefix(normalized, "CREATE TEMPORARY TABLE "):
		return []common.PrivilegeType{common.CreateTMPTablePriv}
	case strings.HasPrefix(normalized, "CREATE TABLESPACE "),
		strings.HasPrefix(normalized, "ALTER TABLESPACE "),
		strings.HasPrefix(normalized, "DROP TABLESPACE "):
		return []common.PrivilegeType{common.CreateTablespacePriv}
	case strings.HasPrefix(normalized, "CREATE INDEX "):
		return []common.PrivilegeType{common.IndexPriv}
	case strings.HasPrefix(normalized, "CREATE TRIGGER "), strings.HasPrefix(normalized, "CREATE OR REPLACE TRIGGER "):
		return []common.PrivilegeType{common.TriggerPriv}
	case strings.HasPrefix(normalized, "CREATE EVENT "):
		return []common.PrivilegeType{common.EventPriv}
	case strings.HasPrefix(normalized, "CREATE PROCEDURE "), strings.HasPrefix(normalized, "CREATE FUNCTION "):
		return []common.PrivilegeType{common.CreateRoutinePriv}
	case strings.HasPrefix(normalized, "ALTER EVENT "), strings.HasPrefix(normalized, "DROP EVENT "):
		return []common.PrivilegeType{common.EventPriv}
	case strings.HasPrefix(normalized, "ALTER PROCEDURE "), strings.HasPrefix(normalized, "ALTER FUNCTION "),
		strings.HasPrefix(normalized, "DROP PROCEDURE "), strings.HasPrefix(normalized, "DROP FUNCTION "):
		return []common.PrivilegeType{common.AlterRoutinePriv}
	case strings.HasPrefix(normalized, "DROP TRIGGER "):
		return []common.PrivilegeType{common.TriggerPriv}
	case strings.HasPrefix(normalized, "DROP INDEX "):
		return []common.PrivilegeType{common.IndexPriv}
	case strings.HasPrefix(normalized, "CREATE USER "), strings.HasPrefix(normalized, "ALTER USER "),
		strings.HasPrefix(normalized, "DROP USER "), strings.HasPrefix(normalized, "RENAME USER "):
		return []common.PrivilegeType{common.CreateUserPriv}
	case strings.HasPrefix(normalized, "SET PASSWORD "):
		return []common.PrivilegeType{common.CreateUserPriv}
	case strings.HasPrefix(normalized, "CREATE ROLE "):
		return []common.PrivilegeType{common.CreateRolePriv}
	case strings.HasPrefix(normalized, "DROP ROLE "):
		return []common.PrivilegeType{common.DropRolePriv}
	case strings.HasPrefix(normalized, "GRANT "), strings.HasPrefix(normalized, "REVOKE "):
		return []common.PrivilegeType{common.GrantPriv}
	case strings.HasPrefix(normalized, "FLUSH "):
		return []common.PrivilegeType{common.ReloadPriv}
	case strings.HasPrefix(normalized, "KILL "):
		// KILL ownership and PROCESS/SUPER checks depend on the target thread;
		// the engine performs that contextual check after parsing the id.
		return nil
	case strings.HasPrefix(normalized, "LOCK TABLES "):
		return []common.PrivilegeType{common.LockTablesPriv}
	case strings.HasPrefix(normalized, "SHOW CREATE VIEW "):
		return []common.PrivilegeType{common.ShowViewPriv}
	case sqlType == "CALL":
		return []common.PrivilegeType{common.ExecutePriv}
	default:
		return h.getRequiredPrivileges(sqlType)
	}
}

func isSelfServiceSetPassword(user, host, sql string) bool {
	normalized := strings.Join(strings.Fields(strings.ToUpper(strings.TrimSpace(sql))), " ")
	if !strings.HasPrefix(normalized, "SET PASSWORD") {
		return false
	}
	if !strings.Contains(normalized, " FOR ") || strings.Contains(normalized, " FOR CURRENT_USER") {
		return true
	}
	match := regexp.MustCompile(`(?is)^\s*set\s+password\s+for\s+'([^']*)'\s*@\s*'([^']*)'`).FindStringSubmatch(sql)
	return len(match) == 3 && strings.EqualFold(match[1], user) && strings.EqualFold(match[2], host)
}

// getRequiredPrivileges 获取所需权限
func (h *EnhancedBusinessMessageHandler) getRequiredPrivileges(sqlType string) []common.PrivilegeType {
	switch sqlType {
	case "SELECT":
		return []common.PrivilegeType{common.SelectPriv}
	case "INSERT":
		return []common.PrivilegeType{common.InsertPriv}
	case "REPLACE":
		return []common.PrivilegeType{common.InsertPriv, common.DeletePriv}
	case "UPDATE":
		return []common.PrivilegeType{common.UpdatePriv}
	case "DELETE":
		return []common.PrivilegeType{common.DeletePriv}
	case "TRUNCATE":
		return []common.PrivilegeType{common.DropPriv}
	case "CREATE":
		return []common.PrivilegeType{common.CreatePriv}
	case "DROP":
		return []common.PrivilegeType{common.DropPriv}
	case "ALTER":
		return []common.PrivilegeType{common.AlterPriv}
	case "SHOW":
		return []common.PrivilegeType{common.ShowDBPriv}
	default:
		return []common.PrivilegeType{common.SelectPriv}
	}
}

func (h *EnhancedBusinessMessageHandler) inferColumnTypes(rows [][]interface{}, columnCount int) []string {
	types := make([]string, columnCount)
	for col := 0; col < columnCount; col++ {
		var t string
		for r := 0; r < len(rows); r++ {
			v := rows[r]
			if col >= len(v) {
				continue
			}
			val := v[col]
			if val == nil {
				continue
			}
			switch x := val.(type) {
			case int8, int16, int32, int:
				t = "int"
			case uint8, uint16, uint32, uint:
				t = "int"
			case int64:
				t = "bigint"
			case uint64:
				t = "bigint"
			case float32, float64:
				t = "double"
			case bool:
				t = "tinyint"
			case []byte:
				t = "varbinary"
			case string:
				t = "varchar"
			default:
				// try time types via fmt
				_ = x
				t = "varchar"
			}
			if t != "" {
				break
			}
		}
		if t == "" {
			t = "varchar"
		}
		types[col] = t
	}
	return types
}

// createSelectOneResponse 创建SELECT 1查询的硬编码响应
func (h *EnhancedBusinessMessageHandler) createSelectOneResponse(msg protocol.Message) protocol.Message {
	logger.Debugf("  创建 SELECT 1 硬编码响应")

	queryResult := &protocol.MessageQueryResult{
		Columns:     []string{"1"},
		ColumnTypes: []string{"BIGINT"},
		Rows:        [][]interface{}{{int64(1)}},
		Error:       nil,
		Message:     "Query OK, 1 row in set",
		Type:        "SELECT",
	}

	responseMsg := &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, msg.SessionID(), queryResult),
		Result:      queryResult,
	}

	logger.Debugf(" SELECT 1 硬编码响应创建完成")
	return responseMsg
}

func isSelectOneQuery(query string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(query))
	normalized = strings.TrimSuffix(normalized, ";")
	if strings.Contains(normalized, " FROM ") {
		return false
	}
	return normalized == "SELECT 1" || strings.HasPrefix(normalized, "SELECT 1 AS ")
}
