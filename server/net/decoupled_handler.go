package net

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex" // 临时注释 - 密码验证被跳过时不需要
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

// localMin 返回两个整数中的较小值，避免依赖 Go1.21 内置 min
func localMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// appendLenEncInt 在当前包内实现长度编码整数，避免依赖 protocol 包的非导出函数
func appendLenEncInt(data []byte, value uint64) []byte {
	if value < 251 {
		return append(data, byte(value))
	} else if value < (1 << 16) {
		return append(data, 0xFC,
			byte(value),
			byte(value>>8))
	} else if value < (1 << 24) {
		return append(data, 0xFD,
			byte(value),
			byte(value>>8),
			byte(value>>16))
	}

	return append(data, 0xFE,
		byte(value),
		byte(value>>8),
		byte(value>>16),
		byte(value>>24),
		byte(value>>32),
		byte(value>>40),
		byte(value>>48),
		byte(value>>56))
}

// DecoupledMySQLMessageHandler 解耦的MySQL消息处理器
type DecoupledMySQLMessageHandler struct {
	rwlock     sync.RWMutex
	cfg        *conf.Cfg
	sessionMap map[Session]server.MySQLServerSession

	// 协议层组件
	protocolParser  protocol.ProtocolParser
	protocolEncoder protocol.ProtocolEncoder
	messageBus      protocol.MessageBus

	// 业务层处理器
	businessHandler protocol.MessageHandler

	// 握手生成器
	handshakeGenerator *protocol.HandshakeGenerator

	// 认证服务
	authService  auth.AuthService
	xmysqlEngine *engine.XMySQLEngine

	// ResultSet 编码器（复用实例，避免重复创建）
	resultSetEncoder  *protocol.MySQLResultSetEncoder
	replicationSource *replication.Source
	replicaRegistry   *replication.ReplicaRegistry
}

func recordAuthenticationFailure(user, host string) {
	metrics.DefaultRuntimeRecorder().RecordAuthenticationFailure(user, host)
}

func recordAuthenticationSuccess(user, host string) {
	recorder := metrics.DefaultRuntimeRecorder()
	recorder.RecordAuthenticationSuccess(user, host)
	recorder.RecordConnection(user, host)
}

// NewDecoupledMySQLMessageHandler 创建解耦的MySQL消息处理器
func NewDecoupledMySQLMessageHandler(cfg *conf.Cfg) *DecoupledMySQLMessageHandler {
	// 创建XMySQL引擎
	xmysqlEngine := engine.NewXMySQLEngine(cfg)

	return NewDecoupledMySQLMessageHandlerWithEngine(cfg, xmysqlEngine)
}

// NewDecoupledMySQLMessageHandlerWithEngine 使用已存在的XMySQLEngine创建解耦的MySQL消息处理器
// 推荐使用此方法以避免重复创建XMySQLEngine实例
func NewDecoupledMySQLMessageHandlerWithEngine(cfg *conf.Cfg, xmysqlEngine *engine.XMySQLEngine) *DecoupledMySQLMessageHandler {
	// 创建引擎访问接口
	engineAccess := auth.NewInnoDBEngineAccess(cfg, xmysqlEngine)

	// 创建认证服务
	authService := auth.NewAuthService(cfg, engineAccess)

	handler := &DecoupledMySQLMessageHandler{
		sessionMap:         make(map[Session]server.MySQLServerSession),
		cfg:                cfg,
		protocolParser:     protocol.NewMySQLProtocolParser(),
		protocolEncoder:    protocol.NewMySQLProtocolEncoder(),
		messageBus:         protocol.NewDefaultMessageBus(),
		businessHandler:    dispatcher.NewEnhancedBusinessMessageHandler(cfg, xmysqlEngine),
		handshakeGenerator: protocol.NewHandshakeGenerator(),
		authService:        authService,
		xmysqlEngine:       xmysqlEngine,
		resultSetEncoder:   protocol.NewMySQLResultSetEncoder(), // 初始化 ResultSet 编码器
		replicationSource:  xmysqlEngine.ReplicationSource(),
		replicaRegistry:    xmysqlEngine.ReplicaRegistry(),
	}

	// 注册业务处理器到消息总线
	handler.registerBusinessHandlers()
	if xmysqlEngine != nil && xmysqlEngine.QueryExecutor != nil {
		xmysqlEngine.QueryExecutor.SetSessionKillControl(handler.killSessionByID)
		xmysqlEngine.QueryExecutor.SetSessionQueryKillControl(handler.killQueryByID)
		xmysqlEngine.QueryExecutor.SetProcesslistProvider(handler.snapshotSessions)
	}

	return handler
}

// registerBusinessHandlers 注册业务处理器
func (h *DecoupledMySQLMessageHandler) registerBusinessHandlers() {
	// 注册所有支持的消息类型
	supportedTypes := []protocol.MessageType{
		protocol.MSG_CONNECT,
		protocol.MSG_DISCONNECT,
		protocol.MSG_AUTH_REQUEST,
		protocol.MSG_QUERY_REQUEST,
		protocol.MSG_USE_DB_REQUEST,
		protocol.MSG_PING,
	}

	for _, msgType := range supportedTypes {
		h.messageBus.Subscribe(msgType, h.businessHandler)
	}
}

// OnOpen 连接建立事件
func (h *DecoupledMySQLMessageHandler) OnOpen(session Session) error {
	// 创建MySQL会话对象
	mysqlSession := NewMySQLServerSession(session)
	if mysqlSession != nil && mysqlSession.SessionContext() != nil {
		// MySQL exposes the transport connection id as CONNECTION_ID() and
		// uses the same id for COM_PROCESS_KILL/PROCESSLIST lookup.
		mysqlSession.SessionContext().SetConnectionID(session.ID())
	}

	h.rwlock.Lock()
	h.sessionMap[session] = mysqlSession
	h.rwlock.Unlock()
	h.recordActiveConnections()
	if h.replicationSource != nil {
		session.SetAttribute("replication_source", h.replicationSource)
	}

	logger.Debugf("新连接建立: %s", session.Stat())

	// 生成握手包
	handshakePacket, err := h.handshakeGenerator.GenerateHandshake()
	if err != nil {
		logger.Errorf("生成握手包失败: %v", err)
		return err
	}

	// 保存challenge到session属性（用于后续密码验证）
	challenge := handshakePacket.GetAuthData()
	session.SetAttribute("auth_challenge", challenge)
	session.SetAttribute("server_auth_plugin", handshakePacket.AuthPluginName)
	session.SetAttribute("prepared_stmt_mgr", protocol.NewPreparedStatementManager())
	logger.Debugf("保存challenge到session: %x", challenge)

	// 发送握手包
	handshakeData := handshakePacket.Encode()
	if err := session.WriteBytes(handshakeData); err != nil {
		logger.Errorf("发送握手包失败: %v", err)
		return err
	}

	logger.Debugf("握手包发送成功")
	return nil
}

// sendErrorResponse 发送错误响应（MySQL Error Packet）
func (h *DecoupledMySQLMessageHandler) sendErrorResponse(session Session, code uint16, state, message string) error {
	logger.Debugf("发送错误响应: code=%d, state=%s, message=%s", code, state, message)

	// 使用统一的协议编码函数生成 Error 包，序列号固定为 1
	errorPacket := protocol.EncodeErrorPacketWithSeq(code, state, message, 1)

	return session.WriteBytes(errorPacket)
}

// sendGoErrorResponse keeps prepared-statement and text-protocol execution
// errors on the same MySQL errno/SQLSTATE mapping.  Prepared statements use
// this path directly for cursor execution failures, so hard-coding 1064 here
// would make Connector/J observe a different contract from COM_QUERY.
func (h *DecoupledMySQLMessageHandler) sendGoErrorResponse(session Session, err error) error {
	sqlErr := protocol.ClassifyGoError(err)
	if sqlErr == nil {
		return h.sendErrorResponse(session, common.ER_UNKNOWN_ERROR, common.DefaultMySQLState, "unknown error")
	}
	return h.sendErrorResponse(session, sqlErr.Code, sqlErr.State, sqlErr.Message)
}

// createMySQLPacket 创建带包头的 MySQL 数据包
// MySQL包格式：3字节长度 + 1字节序号 + 载荷
func (h *DecoupledMySQLMessageHandler) createMySQLPacket(payload []byte, seqId byte) []byte {
	packet := make([]byte, 4+len(payload))

	// 长度（3字节，小端序）
	packet[0] = byte(len(payload) & 0xFF)
	packet[1] = byte((len(payload) >> 8) & 0xFF)
	packet[2] = byte((len(payload) >> 16) & 0xFF)

	// 序号
	packet[3] = seqId

	// 载荷
	copy(packet[4:], payload)

	return packet
}

// createOKPacket 创建OK包（包含包头），用于握手阶段认证成功响应
// affectedRows, lastInsertId 用于 DML；sequenceId 为包序号
func (h *DecoupledMySQLMessageHandler) createOKPacket(affectedRows, lastInsertId uint64, sequenceId byte) []byte {
	payload := []byte{}

	// OK包标识符
	payload = append(payload, 0x00)

	// 受影响的行数 (length-encoded integer)
	payload = h.appendLengthEncodedInt(payload, affectedRows)

	// 最后插入的ID (length-encoded integer)
	payload = h.appendLengthEncodedInt(payload, lastInsertId)

	// 服务器状态标志 (SERVER_STATUS_AUTOCOMMIT)
	payload = append(payload, 0x02, 0x00)

	// 警告数量
	payload = append(payload, 0x00, 0x00)

	// 添加包头
	return h.addPacketHeader(payload, sequenceId)
}

// appendLengthEncodedInt 追加长度编码整数（HEAD版本实现）
func (h *DecoupledMySQLMessageHandler) appendLengthEncodedInt(data []byte, value uint64) []byte {
	if value < 251 {
		return append(data, byte(value))
	} else if value < 65536 {
		data = append(data, 0xFC)
		data = append(data, byte(value), byte(value>>8))
		return data
	} else if value < 16777216 {
		data = append(data, 0xFD)
		data = append(data, byte(value), byte(value>>8), byte(value>>16))
		return data
	}

	data = append(data, 0xFE)
	for i := 0; i < 8; i++ {
		data = append(data, byte(value>>(i*8)))
	}
	return data
}

// addPacketHeader 为载荷添加 MySQL 包头（3 字节长度 + 1 字节序号）
func (h *DecoupledMySQLMessageHandler) addPacketHeader(payload []byte, sequenceId byte) []byte {
	length := len(payload)
	header := make([]byte, 4)

	// 包长度 (3字节，小端序)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 序列号
	header[3] = sequenceId

	return append(header, payload...)
}

// sendMySQLOKPacket 发送 MySQL OK Packet（包含包头）
func (h *DecoupledMySQLMessageHandler) sendMySQLOKPacket(session Session, affectedRows, lastInsertId uint64, seqId byte) error {
	logger.Debugf("发送OK包")

	okData := protocol.EncodeOKPacketWithSeq(affectedRows, lastInsertId, protocol.SERVER_STATUS_AUTOCOMMIT, 0, seqId)
	return session.WriteBytes(okData)
}

func (h *DecoupledMySQLMessageHandler) sendMySQLOKPacketWithStatus(session Session, affectedRows, lastInsertId uint64, seqId byte, statusFlags uint16) error {
	return h.sendMySQLOKPacketWithStatusAndWarnings(session, affectedRows, lastInsertId, seqId, statusFlags, 0)
}

func (h *DecoupledMySQLMessageHandler) sendMySQLOKPacketWithStatusAndWarnings(session Session, affectedRows, lastInsertId uint64, seqId byte, statusFlags uint16, warningCount uint16) error {
	logger.Debugf("发送OK包")

	okData := protocol.EncodeOKPacketWithSeq(affectedRows, lastInsertId, statusFlags, warningCount, seqId)
	return session.WriteBytes(okData)
}

func mysqlResponseStatusFlags(session Session, mysqlSession server.MySQLServerSession) uint16 {
	status := mysqlSessionStatusFlags(mysqlSession)
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	if more, ok := session.GetAttribute("__more_results__").(bool); ok && more && capabilities&(protocol.CLIENT_MULTI_RESULTS|protocol.CLIENT_PS_MULTI_RESULTS) != 0 {
		status |= protocol.SERVER_MORE_RESULTS_EXISTS
	}
	return status
}

func mysqlSessionStatusFlags(mysqlSession server.MySQLServerSession) uint16 {
	if mysqlSession == nil {
		return protocol.SERVER_STATUS_AUTOCOMMIT
	}
	var flags uint16
	if sessionAutocommitEnabled(mysqlSession.GetParamByName("autocommit")) {
		flags |= protocol.SERVER_STATUS_AUTOCOMMIT
	}
	if inTxn, ok := mysqlSession.GetParamByName("in_transaction").(bool); ok && inTxn {
		flags |= protocol.SERVER_STATUS_IN_TRANS
	}
	return flags
}

func sessionAutocommitEnabled(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return true
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "0", "off", "false", "disabled", "no":
			return false
		default:
			return true
		}
	case int:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint64:
		return v != 0
	default:
		return true
	}
}

func (h *DecoupledMySQLMessageHandler) recordActiveConnections() {
	if h == nil {
		return
	}
	h.rwlock.RLock()
	count := len(h.sessionMap)
	h.rwlock.RUnlock()
	metrics.DefaultRuntimeRecorder().SetActiveConnections("mysql", count)
}

func (h *DecoupledMySQLMessageHandler) snapshotSessions() []server.MySQLServerSession {
	if h == nil {
		return nil
	}
	h.rwlock.RLock()
	sessions := make([]server.MySQLServerSession, 0, len(h.sessionMap))
	for _, session := range h.sessionMap {
		if session != nil {
			sessions = append(sessions, session)
		}
	}
	h.rwlock.RUnlock()
	return sessions
}

// removeSession atomically takes a session out of the handler registry. The
// close and error callbacks can run concurrently for different connections,
// so both the lookup and delete must share the same lock.
func (h *DecoupledMySQLMessageHandler) removeSession(session Session) (server.MySQLServerSession, bool) {
	h.rwlock.Lock()
	mysqlSession, ok := h.sessionMap[session]
	if ok {
		delete(h.sessionMap, session)
	}
	h.rwlock.Unlock()
	return mysqlSession, ok
}

// OnClose 连接关闭事件
func (h *DecoupledMySQLMessageHandler) OnClose(session Session) {
	logger.Debugf("[OnClose] 连接关闭: SessionID=%s, RemoteAddr=%s", session.Stat(), session.RemoteAddr())
	h.unregisterReplicaSession(session)
	mysqlSession, ok := h.removeSession(session)
	if ok && h.xmysqlEngine != nil {
		if err := h.xmysqlEngine.CleanupTemporaryTables(mysqlSession); err != nil {
			logger.Errorf("temporary table cleanup failed on close: %v", err)
		}
	}

	h.recordActiveConnections()

	logger.Debugf("[OnClose] 会话已从映射中移除")

	// 主动关闭会话，配合单测验证关闭状态
	session.Close()
}

// OnError 连接错误事件
func (h *DecoupledMySQLMessageHandler) OnError(session Session, err error) {
	logger.Errorf("[OnError] 会话错误: SessionID=%s, RemoteAddr=%s, Error=%v",
		session.Stat(), session.RemoteAddr(), err)
	h.unregisterReplicaSession(session)
	mysqlSession, ok := h.removeSession(session)
	if ok && h.xmysqlEngine != nil {
		if cleanupErr := h.xmysqlEngine.CleanupTemporaryTables(mysqlSession); cleanupErr != nil {
			logger.Errorf("temporary table cleanup failed on error: %v", cleanupErr)
		}
	}

	h.recordActiveConnections()

	logger.Debugf("[OnError] 会话已从映射中移除")

	// 错误处理时不强制关闭连接，让上层决定
}

// OnCron 定时检查事件
func (h *DecoupledMySQLMessageHandler) OnCron(session Session) {
	// 定时检查会话状态
}

// OnMessage 消息处理事件
func (h *DecoupledMySQLMessageHandler) OnMessage(session Session, pkg interface{}) {
	logger.Debug(h.formatLogSimple(session, "OnMessage", fmt.Sprintf("收到消息，类型: %T", pkg)))

	recMySQLPkg, ok := pkg.(*MySQLPackage)
	if !ok {
		logger.Error(h.formatLogSimple(session, "OnMessage", fmt.Sprintf("无效的包类型: %T", pkg)))
		return
	}

	// 获取命令信息（用于日志）
	var cmdName, cmdDetail string
	if len(recMySQLPkg.Body) > 0 {
		cmdName = h.getCommandName(recMySQLPkg.Body[0])
		cmdDetail = h.getCommandDetail(recMySQLPkg.Body)
	} else {
		cmdName = "EMPTY"
		cmdDetail = "empty packet"
	}

	logger.Debug(h.formatLog(session, "OnMessage", cmdName, cmdDetail,
		fmt.Sprintf("收到MySQL包: 长度=%v, 序号=%d, Body长度=%d",
			recMySQLPkg.Header.PacketLength, recMySQLPkg.Header.PacketId, len(recMySQLPkg.Body))))
	logger.Debug(h.formatLog(session, "OnMessage", cmdName, cmdDetail,
		fmt.Sprintf("包头信息: PacketLength=%v, PacketId=%d",
			recMySQLPkg.Header.PacketLength, recMySQLPkg.Header.PacketId)))
	logger.Debug(h.formatLog(session, "OnMessage", cmdName, cmdDetail,
		fmt.Sprintf("包体数据: %v", recMySQLPkg.Body)))

	h.rwlock.RLock()
	currentMysqlSession, ok := h.sessionMap[session]
	h.rwlock.RUnlock()
	if !ok {
		logger.Error(h.formatLog(session, "OnMessage", cmdName, cmdDetail, "找不到会话"))
		return
	}

	logger.Debug(h.formatLog(session, "OnMessage", cmdName, cmdDetail, "找到会话，开始处理包"))
	if err := h.handlePacket(session, &currentMysqlSession, recMySQLPkg); err != nil {
		logger.Debug(h.formatLog(session, "OnMessage", cmdName, cmdDetail, fmt.Sprintf("处理包时出错: %v", err)))
		// 不要在这里直接关闭连接，发送错误响应
		h.sendErrorResponse(session, 1064, "42000", err.Error())
	}

	// 检查是否需要关闭连接（COM_QUIT）
	if shouldClose := session.GetAttribute("should_close"); shouldClose != nil {
		if close, ok := shouldClose.(bool); ok && close {
			logger.Debug(h.formatLog(session, "OnMessage", "COM_QUIT", "quit", "检测到关闭标记，准备关闭会话"))
			h.unregisterReplicaSession(session)
			// 清理会话映射
			h.rwlock.Lock()
			delete(h.sessionMap, session)
			h.rwlock.Unlock()
			h.recordActiveConnections()

			// 关闭会话
			session.Close()
			logger.Debug(h.formatLog(session, "OnMessage", "COM_QUIT", "quit", "会话已关闭"))
		}
	}
}

// handlePacket 处理MySQL包
func (h *DecoupledMySQLMessageHandler) handlePacket(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	authStatus := session.GetAttribute("auth_status")

	// 获取命令信息（用于日志）
	var cmdName, cmdDetail string
	if len(recMySQLPkg.Body) > 0 {
		cmdName = h.getCommandName(recMySQLPkg.Body[0])
		cmdDetail = h.getCommandDetail(recMySQLPkg.Body)
	} else {
		cmdName = "EMPTY"
		cmdDetail = "empty packet"
	}

	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "检查认证状态"))
	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("authStatus: %v (类型: %T)", authStatus, authStatus)))
	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("包体长度: %d, 前10字节: %v", len(recMySQLPkg.Body), recMySQLPkg.Body[:localMin(len(recMySQLPkg.Body), 10)])))

	// 处理认证
	if authStatus == nil {
		logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "认证状态为nil，调用handleAuthentication"))
		return h.handleAuthentication(session, currentMysqlSession, recMySQLPkg)
	}
	if pending, ok := session.GetAttribute("auth_switch_pending").(*authSwitchState); ok && pending != nil {
		return h.handleAuthSwitchResponse(session, currentMysqlSession, recMySQLPkg, pending)
	}

	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("认证状态存在: %v，进入已认证流程", authStatus)))

	// 已认证，解析协议包为消息
	if len(recMySQLPkg.Body) == 0 {
		logger.Error(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "包体为空"))
		return fmt.Errorf("empty packet body")
	}

	firstByte := recMySQLPkg.Body[0]

	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("包的第一字节: 0x%02X (%d), 包体长度: %d", firstByte, firstByte, len(recMySQLPkg.Body))))
	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("包体前20字节: %v", recMySQLPkg.Body[:localMin(len(recMySQLPkg.Body), 20)])))

	// 特殊处理 COM_INIT_DB (0x02)：切换当前数据库并更新 session 状态
	if len(recMySQLPkg.Body) >= 2 && firstByte == 0x02 { // COM_INIT_DB
		dbName := string(recMySQLPkg.Body[1:])
		dbName = strings.TrimSpace(dbName)
		logger.Debug(h.formatLog(session, "handlePacket", "COM_INIT_DB", dbName, "切换数据库并更新 session"))

		// 可选：校验数据库是否存在（通过业务层或 authService）
		if h.authService != nil {
			if err := h.authService.ValidateDatabase(context.Background(), dbName); err != nil {
				logger.Warn(h.formatLog(session, "handlePacket", "COM_INIT_DB", dbName, fmt.Sprintf("数据库校验失败(继续): %v", err)))
				// 仍更新 session，由后续 DDL/DML 再报错
			}
		}

		(*currentMysqlSession).SetParamByName("database", dbName)
		logger.Debug(h.formatLog(session, "handlePacket", "COM_INIT_DB", dbName, "session.currentDB 已更新"))

		okPacket := protocol.EncodeOK(nil, 0, 0, nil)
		return session.WriteBytes(okPacket)
	}

	// 特殊处理查询包（绕过协议解析器）
	if len(recMySQLPkg.Body) >= 2 && firstByte == 0x03 { // COM_QUERY
		query := string(recMySQLPkg.Body[1:])
		logger.Debug(h.formatLog(session, "handlePacket", "COM_QUERY", query, "检测到查询包，直接处理"))

		// 创建查询消息
		queryMsg := &protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), query),
			SQL:         query,
		}

		logger.Debug(h.formatLog(session, "handlePacket", "COM_QUERY", query, "查询消息创建成功，调用handleQueryMessageDirect"))

		// 直接处理查询（传入真实 session，保证 USE 等语句能更新 currentDB）
		err := h.handleQueryMessageDirect(session, currentMysqlSession, queryMsg)
		if err != nil {
			logger.Error(h.formatLog(session, "handlePacket", "COM_QUERY", query, fmt.Sprintf("查询处理失败: %v", err)))
			return err
		}

		logger.Debug(h.formatLog(session, "handlePacket", "COM_QUERY", query, "查询处理完成"))
		return nil
	}

	// 预编译语句：走与 COM_QUERY 相同的执行器路径
	if len(recMySQLPkg.Body) >= 1 {
		switch recMySQLPkg.Body[0] {
		case common.COM_REFRESH:
			return h.handleRefresh(session, recMySQLPkg.Body)
		case common.COM_FIELD_LIST:
			return h.handleFieldList(session, currentMysqlSession, recMySQLPkg.Body)
		case common.COM_STATISTICS:
			return h.handleStatistics(session)
		case common.COM_PROCESS_INFO:
			return h.handleProcessInfo(session, currentMysqlSession)
		case common.COM_PROCESS_KILL:
			return h.handleProcessKill(session, currentMysqlSession, recMySQLPkg.Body, recMySQLPkg.Header.PacketId+1)
		case common.COM_CREATE_DB:
			return h.handleDatabaseCommand(session, currentMysqlSession, recMySQLPkg.Body, true)
		case common.COM_DROP_DB:
			return h.handleDatabaseCommand(session, currentMysqlSession, recMySQLPkg.Body, false)
		case common.COM_SET_OPTION:
			return h.handleSetOption(session, recMySQLPkg.Body)
		case common.COM_PING:
			return session.WriteBytes(h.createOKPacket(0, 0, recMySQLPkg.Header.PacketId+1))
		case common.COM_CHANGE_USER:
			return h.handleComChangeUser(session, currentMysqlSession, recMySQLPkg)
		case common.COM_REGISTER_SLAVE:
			return h.handleRegisterSlave(session, recMySQLPkg)
		case common.COM_STMT_PREPARE:
			return h.handleComStmtPrepare(session, currentMysqlSession, recMySQLPkg)
		case common.COM_STMT_EXECUTE:
			return h.handleComStmtExecute(session, currentMysqlSession, recMySQLPkg)
		case common.COM_STMT_CLOSE:
			return h.handleComStmtClose(session, recMySQLPkg)
		case common.COM_STMT_RESET:
			return h.handleComStmtReset(session, recMySQLPkg)
		case common.COM_STMT_SEND_LONG_DATA:
			return h.handleComStmtSendLongData(session, recMySQLPkg)
		case common.COM_STMT_FETCH:
			return h.handleComStmtFetch(session, recMySQLPkg)
		case common.COM_RESET_CONNECTION:
			return h.handleComResetConnection(session, currentMysqlSession, recMySQLPkg)
		}
	}
	if firstByte == common.COM_BINLOG_DUMP || firstByte == common.COM_BINLOG_DUMP_GTID {
		return dumpBinlogEvents(session, recMySQLPkg.Body)
	}

	if !h.protocolParser.CanParse(firstByte) {
		logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "命令暂未支持，返回不支持特性错误"))
		return h.handleUnsupportedCommand(session, firstByte)
	}

	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "非查询包，使用协议解析器处理"))

	// 使用协议解析器解析包
	message, err := h.protocolParser.ParsePacket(recMySQLPkg.Body, session.Stat())
	if err != nil {
		logger.Error(h.formatLog(session, "handlePacket", cmdName, cmdDetail, fmt.Sprintf("协议解析失败: %v", err)))
		return h.sendErrorResponse(session, 1064, "42000", "Protocol parse error")
	}

	logger.Debug(h.formatLog(session, "handlePacket", cmdName, cmdDetail, fmt.Sprintf("包解析成功，消息类型: %d", message.Type())))

	// 直接处理业务消息（同步处理避免会话关闭问题）
	return h.handleBusinessMessageSync(session, message)
}

// handleRefresh acknowledges COM_REFRESH. The current server does not expose
// MySQL's legacy query-cache/table-cache refresh knobs; metadata and privilege
// state are already refreshed through their authoritative engine paths, so a
// valid command is a protocol-level no-op rather than an unsupported error.
func (h *DecoupledMySQLMessageHandler) handleRefresh(session Session, body []byte) error {
	if len(body) < 2 {
		return h.sendErrorResponse(session, 1105, "42000", "Invalid COM_REFRESH packet")
	}
	return session.WriteBytes(protocol.EncodeOKPacketWithSeq(0, 0, protocol.SERVER_STATUS_AUTOCOMMIT, 0, 1))
}

// handleStatistics implements the text response expected by COM_STATISTICS.
// The command is intentionally a lightweight server-level snapshot; detailed
// counters remain exposed through the metrics endpoint.
func (h *DecoupledMySQLMessageHandler) handleStatistics(session Session) error {
	stats := "Uptime: 0  Threads: 1  Questions: 0  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000"
	return session.WriteBytes(h.createMySQLPacket([]byte(stats), 1))
}

// handleProcessInfo implements COM_PROCESS_INFO by routing through the same
// privilege-aware SHOW PROCESSLIST path used by SQL clients.
func (h *DecoupledMySQLMessageHandler) handleProcessInfo(session Session, currentMysqlSession *server.MySQLServerSession) error {
	if h.businessHandler == nil || currentMysqlSession == nil {
		return h.sendErrorResponse(session, 1105, "42000", "COM_PROCESS_INFO is unavailable")
	}
	database := ""
	if value, ok := (*currentMysqlSession).GetParamByName("database").(string); ok {
		database = strings.TrimSpace(value)
	}
	var response protocol.Message
	var err error
	if enhanced, ok := h.businessHandler.(*dispatcher.EnhancedBusinessMessageHandler); ok {
		response, err = enhanced.HandleQueryWithRealSession(*currentMysqlSession, "SHOW PROCESSLIST", database)
	} else {
		response, err = h.businessHandler.HandleMessage(&protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), "SHOW PROCESSLIST"),
			SQL:         "SHOW PROCESSLIST",
			Database:    database,
		})
	}
	if err != nil {
		return h.sendErrorResponse(session, 1105, "42000", err.Error())
	}
	if errorResponse, ok := response.(*protocol.ErrorMessage); ok {
		return h.sendErrorResponse(session, errorResponse.Code, errorResponse.State, errorResponse.Message)
	}
	queryResponse, ok := response.(*protocol.ResponseMessage)
	if !ok || queryResponse.Result == nil {
		return h.sendErrorResponse(session, 1105, "42000", "COM_PROCESS_INFO returned no result")
	}
	return h.sendQueryResultSet(session, queryResponse.Result, 1)
}

// handleProcessKill implements COM_PROCESS_KILL. A session may terminate its
// own thread or another thread owned by the same account. Killing another
// account requires the global PROCESS or SUPER privilege, matching the
// administrative boundary used by the SQL process-list path.
func (h *DecoupledMySQLMessageHandler) handleProcessKill(session Session, currentMysqlSession *server.MySQLServerSession, body []byte, sequence byte) error {
	if len(body) != 5 {
		return h.sendErrorResponse(session, 1105, "42000", "Invalid COM_PROCESS_KILL packet")
	}
	if currentMysqlSession == nil || *currentMysqlSession == nil {
		return h.sendErrorResponse(session, 1095, "HY000", "You are not owner of this thread")
	}

	targetID := binary.LittleEndian.Uint32(body[1:5])
	if err := h.killSessionByID(targetID, valueOrNilMySQLSession(currentMysqlSession)); err != nil {
		code := uint16(1095)
		if strings.HasPrefix(err.Error(), "Unknown thread id:") {
			code = 1094
		}
		return h.sendErrorResponse(session, code, "HY000", err.Error())
	}
	return session.WriteBytes(h.createOKPacket(0, 0, sequence))
}

func valueOrNilMySQLSession(currentMysqlSession *server.MySQLServerSession) server.MySQLServerSession {
	if currentMysqlSession == nil {
		return nil
	}
	return *currentMysqlSession
}

func (h *DecoupledMySQLMessageHandler) killSessionByID(targetID uint32, currentMysqlSession server.MySQLServerSession) error {
	if currentMysqlSession == nil {
		return fmt.Errorf("You are not owner of thread %d", targetID)
	}
	targetSession, targetMysqlSession := h.findSessionByID(targetID)
	if targetSession == nil || targetMysqlSession == nil {
		return fmt.Errorf("Unknown thread id: %d", targetID)
	}
	if err := h.authorizeSessionAction(targetID, currentMysqlSession, targetMysqlSession); err != nil {
		return err
	}

	// Close outside the registry read lock. The network callback may re-enter
	// OnClose and remove the same session from sessionMap.
	targetSession.Close()
	return nil
}

func (h *DecoupledMySQLMessageHandler) killQueryByID(targetID uint32, currentMysqlSession server.MySQLServerSession) error {
	if currentMysqlSession == nil {
		return fmt.Errorf("You are not owner of thread %d", targetID)
	}
	_, targetMysqlSession := h.findSessionByID(targetID)
	if targetMysqlSession == nil {
		return fmt.Errorf("Unknown thread id: %d", targetID)
	}
	if err := h.authorizeSessionAction(targetID, currentMysqlSession, targetMysqlSession); err != nil {
		return err
	}
	if h.xmysqlEngine == nil || h.xmysqlEngine.QueryExecutor == nil {
		return fmt.Errorf("session query kill control is not configured")
	}
	return h.xmysqlEngine.QueryExecutor.CancelActiveQuery(targetID)
}

func (h *DecoupledMySQLMessageHandler) findSessionByID(targetID uint32) (Session, server.MySQLServerSession) {
	var targetSession Session
	var targetMysqlSession server.MySQLServerSession
	h.rwlock.RLock()
	for candidate, candidateMysqlSession := range h.sessionMap {
		if candidateMysqlSession == nil || candidateMysqlSession.SessionContext() == nil {
			continue
		}
		if candidateMysqlSession.SessionContext().GetConnectionID() == targetID {
			targetSession = candidate
			targetMysqlSession = candidateMysqlSession
			break
		}
	}
	h.rwlock.RUnlock()
	return targetSession, targetMysqlSession
}

func (h *DecoupledMySQLMessageHandler) authorizeSessionAction(targetID uint32, currentMysqlSession, targetMysqlSession server.MySQLServerSession) error {
	currentUser, _ := currentMysqlSession.GetParamByName("user").(string)
	targetUser, _ := targetMysqlSession.GetParamByName("user").(string)
	currentID := uint32(0)
	if currentMysqlSession.SessionContext() != nil {
		currentID = currentMysqlSession.SessionContext().GetConnectionID()
	}
	isOwner := targetID == currentID || (currentUser != "" && strings.EqualFold(currentUser, targetUser))
	if !isOwner {
		host, _ := currentMysqlSession.GetParamByName("host").(string)
		processErr := error(nil)
		if h.authService == nil || currentUser == "" {
			processErr = fmt.Errorf("process privilege unavailable")
		} else {
			processErr = h.authService.CheckPrivilege(context.Background(), currentUser, host, "", "", common.ProcessPriv)
			if processErr != nil {
				processErr = h.authService.CheckPrivilege(context.Background(), currentUser, host, "", "", common.SuperPriv)
			}
		}
		if processErr != nil {
			return fmt.Errorf("You are not owner of thread %d", targetID)
		}
	}
	return nil
}

func (h *DecoupledMySQLMessageHandler) handleDatabaseCommand(session Session, currentMysqlSession *server.MySQLServerSession, body []byte, create bool) error {
	if len(body) < 2 || currentMysqlSession == nil || h.businessHandler == nil {
		return h.sendErrorResponse(session, 1105, "42000", "database command is unavailable")
	}
	databaseName := strings.Trim(strings.TrimSpace(string(body[1:])), "`\x00")
	if databaseName == "" || strings.ContainsAny(databaseName, " \t\r\n;'") {
		return h.sendErrorResponse(session, 1105, "42000", "invalid database name")
	}
	action := "DROP"
	if create {
		action = "CREATE"
	}
	query := fmt.Sprintf("%s DATABASE `%s`", action, strings.ReplaceAll(databaseName, "`", "``"))
	var response protocol.Message
	var err error
	if enhanced, ok := h.businessHandler.(*dispatcher.EnhancedBusinessMessageHandler); ok {
		response, err = enhanced.HandleQueryWithRealSession(*currentMysqlSession, query, databaseName)
	} else {
		response, err = h.businessHandler.HandleMessage(&protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), query),
			SQL:         query,
			Database:    databaseName,
		})
	}
	if err != nil {
		return h.sendErrorResponse(session, 1105, "42000", err.Error())
	}
	if errorResponse, ok := response.(*protocol.ErrorMessage); ok {
		return h.sendErrorResponse(session, errorResponse.Code, errorResponse.State, errorResponse.Message)
	}
	queryResponse, ok := response.(*protocol.ResponseMessage)
	if !ok || queryResponse.Result == nil {
		return h.sendErrorResponse(session, 1105, "42000", "database command returned no result")
	}
	statusFlags := uint16(protocol.SERVER_STATUS_AUTOCOMMIT)
	if currentMysqlSession != nil {
		statusFlags = mysqlResponseStatusFlags(session, *currentMysqlSession)
	}
	return h.sendMySQLOKPacketWithStatus(session, queryResponse.Result.AffectedRows, queryResponse.Result.LastInsertID, 1, statusFlags)
}

func (h *DecoupledMySQLMessageHandler) handleSetOption(session Session, body []byte) error {
	if len(body) < 3 {
		return h.sendErrorResponse(session, 1105, "42000", "Invalid COM_SET_OPTION packet")
	}
	option := uint16(body[1]) | uint16(body[2])<<8
	switch option {
	case 0: // MYSQL_OPTION_MULTI_STATEMENTS_ON
		session.SetAttribute("client_multi_statements", true)
	case 1: // MYSQL_OPTION_MULTI_STATEMENTS_OFF
		session.SetAttribute("client_multi_statements", false)
	default:
		return h.sendErrorResponse(session, 1105, "42000", fmt.Sprintf("unsupported COM_SET_OPTION value %d", option))
	}
	return session.WriteBytes(protocol.EncodeOKPacketWithSeq(0, 0, protocol.SERVER_STATUS_AUTOCOMMIT, 0, 1))
}

// handleFieldList implements COM_FIELD_LIST using the engine's authoritative
// SHOW FULL COLUMNS metadata path. COM_FIELD_LIST is not a result set: MySQL
// expects one ColumnDefinition packet per table column followed by EOF, with
// no leading column-count packet.
func (h *DecoupledMySQLMessageHandler) handleFieldList(session Session, currentMysqlSession *server.MySQLServerSession, body []byte) error {
	if len(body) < 2 {
		return h.sendErrorResponse(session, 1105, "42000", "Invalid COM_FIELD_LIST packet")
	}
	table, wildcard := parseFieldListRequest(body[1:])
	if table == "" {
		return h.sendErrorResponse(session, 1105, "42000", "COM_FIELD_LIST requires a table name")
	}
	database := ""
	if currentMysqlSession != nil {
		if value, ok := (*currentMysqlSession).GetParamByName("database").(string); ok {
			database = strings.TrimSpace(value)
		}
	}
	query := fieldListMetadataQuery(table, database, wildcard)
	if h.businessHandler == nil {
		return h.sendErrorResponse(session, 1105, "42000", "COM_FIELD_LIST is unavailable")
	}

	var response protocol.Message
	var err error
	if enhanced, ok := h.businessHandler.(*dispatcher.EnhancedBusinessMessageHandler); ok && currentMysqlSession != nil {
		response, err = enhanced.HandleQueryWithRealSession(*currentMysqlSession, query, database)
	} else {
		response, err = h.businessHandler.HandleMessage(&protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), query),
			SQL:         query,
			Database:    database,
		})
	}
	if err != nil {
		return h.sendErrorResponse(session, 1105, "42000", err.Error())
	}
	if errorResponse, ok := response.(*protocol.ErrorMessage); ok {
		return h.sendErrorResponse(session, errorResponse.Code, errorResponse.State, errorResponse.Message)
	}
	queryResponse, ok := response.(*protocol.ResponseMessage)
	if !ok || queryResponse.Result == nil {
		return h.sendErrorResponse(session, 1105, "42000", "COM_FIELD_LIST metadata query returned no result")
	}

	encoder := h.resultSetEncoder
	if encoder == nil {
		encoder = protocol.NewMySQLResultSetEncoder()
	}
	sequenceID := byte(1)
	for _, row := range queryResponse.Result.Rows {
		if len(row) < 2 || strings.TrimSpace(fmt.Sprint(row[0])) == "" {
			continue
		}
		definition := fieldListColumnDefinition(encoder, table, row)
		if err := session.WriteBytes(encoder.EncodeColumnDefinitionPacket(definition, sequenceID)); err != nil {
			return err
		}
		sequenceID++
	}
	statusFlags := uint16(protocol.SERVER_STATUS_AUTOCOMMIT)
	if currentMysqlSession != nil {
		statusFlags = mysqlResponseStatusFlags(session, *currentMysqlSession)
	}
	return session.WriteBytes(protocol.EncodeEOFPacketWithSeq(0, statusFlags, sequenceID))
}

func parseFieldListRequest(payload []byte) (table, wildcard string) {
	parts := strings.SplitN(string(payload), "\x00", 2)
	table = strings.Trim(strings.TrimSpace(parts[0]), "`")
	if len(parts) == 2 {
		wildcard = strings.Trim(strings.TrimSpace(parts[1]), "\x00")
	}
	return table, wildcard
}

func fieldListMetadataQuery(table, database, wildcard string) string {
	parts := strings.Split(table, ".")
	for index := range parts {
		parts[index] = strings.Trim(strings.TrimSpace(parts[index]), "`")
	}
	var query string
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		query = fmt.Sprintf("SHOW FULL COLUMNS FROM `%s` FROM `%s`", parts[1], parts[0])
	} else {
		query = fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`", table)
	}
	if wildcard != "" {
		query += " LIKE '" + strings.ReplaceAll(wildcard, "'", "''") + "'"
	}
	return query
}

func fieldListColumnDefinition(encoder *protocol.MySQLResultSetEncoder, table string, row []interface{}) *protocol.ColumnDefinition {
	name := strings.Trim(strings.TrimSpace(fmt.Sprint(row[0])), "`")
	typeName := fmt.Sprint(row[1])
	definition := createColumnDefinitionForType(encoder, name, typeName)
	definition.Table = table
	definition.OrgTable = table
	definition.OrgName = name
	if len(row) > 3 && strings.EqualFold(strings.TrimSpace(fmt.Sprint(row[3])), "NO") {
		definition.Flags |= protocol.FLAG_NOT_NULL
	}
	if len(row) > 4 {
		switch strings.ToUpper(strings.TrimSpace(fmt.Sprint(row[4]))) {
		case "PRI":
			definition.Flags |= protocol.FLAG_PRI_KEY
		case "UNI":
			definition.Flags |= protocol.FLAG_UNIQUE_KEY
		case "MUL":
			definition.Flags |= protocol.FLAG_MULTIPLE_KEY
		}
	}
	if len(row) > 6 && strings.Contains(strings.ToLower(fmt.Sprint(row[6])), "auto_increment") {
		definition.Flags |= protocol.FLAG_AUTO_INCREMENT
	}
	return definition
}

func (h *DecoupledMySQLMessageHandler) handleUnsupportedCommand(session Session, cmd byte) error {
	cmdName := h.getCommandName(cmd)
	state := common.MySQLState[common.ErrNotSupportedYet]
	if state == "" {
		state = "42000"
	}
	return h.sendErrorResponse(session, common.ErrNotSupportedYet, state, fmt.Sprintf("%s is not supported", cmdName))
}

// handleBusinessMessageSync 同步处理业务消息
func (h *DecoupledMySQLMessageHandler) handleBusinessMessageSync(session Session, message protocol.Message) error {
	logger.Debugf("[handleBusinessMessageSync] 同步处理业务消息，类型: %d", message.Type())

	if message == nil {
		return nil
	}

	// 查询消息使用专用处理逻辑（传 nil 会从 sessionMap 查找 currentMysqlSession）
	if message.Type() == protocol.MSG_QUERY_REQUEST {
		return h.handleQueryMessageDirect(session, nil, message)
	}

	// COM_QUIT 不发送响应，只标记关闭
	if message.Type() == protocol.MSG_DISCONNECT {
		session.SetAttribute("should_close", true)
		return nil
	}

	if h.businessHandler == nil {
		return nil
	}

	response, err := h.businessHandler.HandleMessage(message)
	if err != nil {
		logger.Errorf("[handleBusinessMessageSync] 业务处理器处理失败: %v", err)
		return h.sendErrorResponse(session, 1064, "42000", err.Error())
	}

	if response == nil {
		return nil
	}

	switch resp := response.(type) {
	case *protocol.ResponseMessage:
		if resp.Result != nil {
			if len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0 {
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			}
			return h.sendQueryResultSet(session, resp.Result, 1)
		}
		return h.sendMySQLOKPacket(session, 0, 0, 1)
	case *protocol.ErrorMessage:
		return h.sendErrorResponse(session, resp.Code, resp.State, resp.Message)
	default:
		return h.sendMySQLOKPacket(session, 0, 0, 1)
	}
}

// handleQueryMessageDirect 直接处理查询消息。传入 currentMysqlSession 以保证 USE/COM_INIT_DB 等能更新同一会话的 currentDB。
func (h *DecoupledMySQLMessageHandler) handleQueryMessageDirect(session Session, currentMysqlSession *server.MySQLServerSession, message protocol.Message) (err error) {
	logger.Debugf("[handleQueryMessageDirect] 开始处理查询消息")

	if session.IsClosed() {
		return fmt.Errorf("session is closed")
	}

	queryMsg, ok := message.(*protocol.QueryMessage)
	if !ok {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid query message")
	}
	statements := splitTopLevelStatements(queryMsg.SQL)
	if len(statements) > 1 {
		if !multiStatementsEnabled(session) {
			session.SetAttribute("__more_results__", false)
			return h.sendErrorResponse(session, 1064, "42000", "multiple statements are disabled")
		}
		for index, statement := range statements {
			session.SetAttribute("__more_results__", index < len(statements)-1)
			next := &protocol.QueryMessage{BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, message.SessionID(), statement), SQL: statement, Database: queryMsg.Database}
			if err := h.handleQueryMessageDirect(session, currentMysqlSession, next); err != nil {
				session.SetAttribute("__more_results__", false)
				return err
			}
		}
		session.SetAttribute("__more_results__", false)
		return nil
	}

	query := queryMsg.SQL
	logger.Debugf("[handleQueryMessageDirect] SQL: %s", query)
	startedAt := time.Now()
	statementType := runtimeStatementType(query)
	database := ""
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		latency := time.Since(startedAt)
		recorder := metrics.DefaultRuntimeRecorder()
		recorder.RecordStatementWithThreadID(0, database, query, statementType, status, latency)
		recorder.RecordQuery(database, statementType, status, latency)
		if err != nil {
			recorder.RecordQueryError(database, "execution", "1064")
		}
		switch statementType {
		case "COMMIT":
			recorder.RecordTransactionCommit("session")
		case "ROLLBACK":
			recorder.RecordTransactionRollback("session", "client")
		}
	}()
	if currentMysqlSession == nil {
		h.rwlock.RLock()
		ms, exists := h.sessionMap[session]
		h.rwlock.RUnlock()
		if !exists {
			return h.sendErrorResponse(session, 1064, "42000", "Session not found")
		}
		currentMysqlSession = &ms
	}

	// 从真实 session 取当前 database，供引擎和 USE 语句更新同一会话
	if p := (*currentMysqlSession).GetParamByName("database"); p != nil {
		if s, ok := p.(string); ok {
			database = s
		}
	}
	logger.Debugf("[handleQueryMessageDirect] 当前 session database: %q", database)

	if h.businessHandler == nil {
		return h.sendMySQLOKPacketWithStatus(session, 0, 0, 1, mysqlResponseStatusFlags(session, *currentMysqlSession))
	}

	var response protocol.Message
	// 优先使用真实 session 执行，这样 USE 等语句会更新 session.currentDB
	if enh, ok := h.businessHandler.(*dispatcher.EnhancedBusinessMessageHandler); ok {
		response, err = enh.HandleQueryWithRealSession(*currentMysqlSession, query, database)
	} else {
		response, err = h.businessHandler.HandleMessage(message)
	}
	if err != nil {
		return h.sendErrorResponse(session, 1064, "42000", err.Error())
	}

	if response == nil {
		return h.sendMySQLOKPacketWithStatus(session, 0, 0, 1, mysqlResponseStatusFlags(session, *currentMysqlSession))
	}
	// COMMIT/ROLLBACK RELEASE is completed by the engine first. Propagate its
	// close-after-response marker to the transport so the client receives the
	// OK packet before the connection is torn down.
	if shouldClose, ok := (*currentMysqlSession).GetParamByName("should_close").(bool); ok && shouldClose {
		session.SetAttribute("should_close", true)
	}

	switch resp := response.(type) {
	case *protocol.ResponseMessage:
		if resp.Result != nil {
			typeStr := strings.ToLower(resp.Result.Type)
			if typeStr == "set" || typeStr == "ddl" || len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0 {
				return h.sendMySQLOKPacketWithStatusAndWarnings(session, resp.Result.AffectedRows, resp.Result.LastInsertID, 1, mysqlResponseStatusFlags(session, *currentMysqlSession), resp.Result.WarningCount)
			}
			return h.sendQueryResultSet(session, resp.Result, 1)
		}
		return h.sendMySQLOKPacketWithStatus(session, 0, 0, 1, mysqlResponseStatusFlags(session, *currentMysqlSession))
	case *protocol.ErrorMessage:
		return h.sendErrorResponse(session, resp.Code, resp.State, resp.Message)
	default:
		return h.sendMySQLOKPacketWithStatus(session, 0, 0, 1, mysqlResponseStatusFlags(session, *currentMysqlSession))
	}
}

func runtimeStatementType(query string) string {
	fields := strings.Fields(strings.TrimSpace(query))
	if len(fields) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(strings.Trim(fields[0], "`"))
}

// multiStatementsEnabled applies COM_SET_OPTION as an explicit per-session
// override and otherwise falls back to the capability negotiated at handshake.
// MySQL clients must advertise CLIENT_MULTI_STATEMENTS before COM_QUERY may
// contain more than one top-level statement.
func multiStatementsEnabled(session Session) bool {
	if enabled, ok := session.GetAttribute("client_multi_statements").(bool); ok {
		return enabled
	}
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	return capabilities&protocol.CLIENT_MULTI_STATEMENTS != 0
}

// handleAuthentication 处理认证
func (h *DecoupledMySQLMessageHandler) handleAuthentication(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	if pending, ok := session.GetAttribute("auth_switch_pending").(*authSwitchState); ok && pending != nil {
		return h.handleAuthSwitchResponse(session, currentMysqlSession, recMySQLPkg, pending)
	}
	logger.Debugf("处理认证包，包长度: %d, 包序号: %d, Body长度: %d",
		len(recMySQLPkg.Header.PacketLength), recMySQLPkg.Header.PacketId, len(recMySQLPkg.Body))

	logger.Debugf("认证包Body内容(前64字节): %v", recMySQLPkg.Body[:localMin(len(recMySQLPkg.Body), 64)])

	// 检查包体长度 - 降低要求到最小必要的长度
	if len(recMySQLPkg.Body) < 4 {
		logger.Errorf("认证包体太短: %d bytes", len(recMySQLPkg.Body))
		return h.sendErrorResponse(session, 1045, "28000", "Authentication packet too short")
	}

	// 解析认证包
	offset := 0
	payload := recMySQLPkg.Body

	logger.Debugf("开始解析认证包，总长度: %d", len(payload))

	// 读取客户端能力标志 (4字节)
	if offset+4 > len(payload) {
		logger.Errorf("无法读取客户端能力标志，需要%d字节，只有%d字节", offset+4, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth packet format")
	}
	clientFlags := binary.LittleEndian.Uint32(payload[offset : offset+4])
	offset += 4

	// 保存客户端能力标志到会话，后续根据 CLIENT_DEPRECATE_EOF 动态选择 EOF/OK
	session.SetAttribute("client_capabilities", clientFlags)
	if currentMysqlSession != nil && *currentMysqlSession != nil {
		(*currentMysqlSession).SetParamByName("client_capabilities", clientFlags)
	}

	// 读取最大包大小 (4字节)
	if offset+4 > len(payload) {
		logger.Errorf("无法读取最大包大小，需要%d字节，只有%d字节", offset+4, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth packet format")
	}
	maxPacketSize := binary.LittleEndian.Uint32(payload[offset : offset+4])
	offset += 4

	// 保存 max_allowed_packet 到会话
	session.SetAttribute("max_allowed_packet", maxPacketSize)
	logger.Debugf("客户端 max_allowed_packet: %d bytes (%d MB)", maxPacketSize, maxPacketSize/(1024*1024))

	// 跳过字符集 (1字节)
	if offset+1 > len(payload) {
		logger.Errorf("无法跳过字符集，需要%d字节，只有%d字节", offset+1, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth packet format")
	}
	offset += 1

	// 跳过保留字节 (23字节)
	if offset+23 > len(payload) {
		logger.Errorf("无法跳过保留字节，需要%d字节，只有%d字节", offset+23, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth packet format")
	}
	offset += 23

	logger.Debugf("客户端能力标志: 0x%08X, 当前偏移: %d", clientFlags, offset)

	// 读取用户名（null结尾字符串）
	userStart := offset
	for offset < len(payload) && payload[offset] != 0 {
		offset++
	}
	if offset >= len(payload) {
		logger.Errorf("用户名没有正确的null终止符，开始位置: %d, 当前位置: %d, 总长度: %d", userStart, offset, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid username format")
	}
	username := string(payload[userStart:offset])
	offset++ // 跳过null终止符

	logger.Debugf("用户名: %s, 当前偏移: %d", username, offset)

	authResponse, offset, err := readClientAuthResponse(payload, offset, clientFlags)
	if err != nil {
		logger.Errorf("读取认证响应失败: %v", err)
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth response format")
	}

	logger.Debugf("认证响应数据: %x, 当前偏移: %d", authResponse, offset)

	// 读取数据库名（如果存在）
	var database string
	if offset < len(payload) {
		dbStart := offset
		for offset < len(payload) && payload[offset] != 0 {
			offset++
		}
		if offset > dbStart {
			database = string(payload[dbStart:offset])
		}
		logger.Debugf("数据库: %s, 最终偏移: %d", database, offset)
	} else {
		logger.Debugf("没有数据库名信息")
	}

	// The connection-attributes block follows the optional database and
	// authentication-plugin fields. Keep the parsed map on the server session
	// so Performance Schema consumers can observe the same handshake data that
	// MySQL exposes through session_connect_attrs.
	if clientFlags&protocol.CLIENT_CONNECT_ATTRS != 0 {
		attributesOffset := offset
		if attributesOffset < len(payload) {
			for attributesOffset < len(payload) && payload[attributesOffset] != 0 {
				attributesOffset++
			}
			if attributesOffset < len(payload) {
				attributesOffset++
			}
			if clientFlags&protocol.CLIENT_PLUGIN_AUTH != 0 {
				for attributesOffset < len(payload) && payload[attributesOffset] != 0 {
					attributesOffset++
				}
				if attributesOffset < len(payload) {
					attributesOffset++
				}
			}
			if attributes, _, attributesErr := protocol.ParseConnectionAttributes(payload[attributesOffset:]); attributesErr == nil && attributes != nil {
				attributeMap := attributes.GetAll()
				session.SetAttribute("connection_attributes", attributeMap)
				if currentMysqlSession != nil && *currentMysqlSession != nil {
					(*currentMysqlSession).SetParamByName("connection_attributes", attributeMap)
				}
			}
		}
	}

	// 验证用户名
	if username == "" {
		logger.Errorf("用户名为空")
		return h.sendErrorResponse(session, 1045, "28000", "Access denied for empty user")
	}

	// 获取保存的challenge
	challengeAttr := session.GetAttribute("auth_challenge")
	if challengeAttr == nil {
		logger.Errorf("未找到challenge数据")
		return h.sendErrorResponse(session, 1045, "28000", "Authentication failed: missing challenge")
	}
	challenge, ok := challengeAttr.([]byte)
	if !ok {
		logger.Errorf("challenge数据类型错误")
		return h.sendErrorResponse(session, 1045, "28000", "Authentication failed: invalid challenge")
	}

	logger.Debugf("开始密码验证，用户: %s, challenge: %x, authResponse: %x", username, challenge, authResponse)

	// 使用AuthService进行密码验证
	ctx := context.Background()
	host := h.resolveAuthHost(session)
	if host == "" {
		logger.Errorf("无法解析客户端主机地址: %s", session.RemoteAddr())
		return h.sendErrorResponse(session, 1045, "28000", "Access denied for user")
	}
	if database == "" {
		database = "mysql" // 默认数据库
	}
	if err := h.enforceAccountTLS(ctx, session, username, host); err != nil {
		logger.Errorf("认证失败: %v", err)
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, 1045, "28000", err.Error())
	}

	// The initial handshake advertises mysql_native_password for compatibility
	// with existing users. If the selected account uses caching_sha2_password,
	// switch the client to that plugin before validating its response. This is
	// the normal MySQL AuthSwitchRequest flow and avoids treating a native
	// 20-byte response as a caching_sha2 response.
	if userInfo, lookupErr := h.authService.GetUserInfo(ctx, username, host); lookupErr == nil {
		if plugin := authSwitchPlugin(userInfo); plugin != "" && (plugin == "sha256_password" || len(authResponse) != 32) {
			state := &authSwitchState{Username: username, Database: database, Host: host, Challenge: append([]byte(nil), challenge...), ResponseSequence: recMySQLPkg.Header.PacketId + 1}
			session.SetAttribute("auth_switch_pending", state)
			return session.WriteBytes(encodeAuthSwitchRequest(plugin, challenge, recMySQLPkg.Header.PacketId+1))
		}
	}

	// 将authResponse转换为十六进制字符串（模拟客户端发送的密码）
	// 注意：这里需要特殊处理，因为authResponse是加密后的数据
	// 我们需要使用AuthService的ValidatePassword方法
	authResult, err := h.authenticateWithChallenge(ctx, username, authResponse, challenge, host, database)
	if err != nil {
		logger.Errorf("认证失败: %v", err)
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, 1045, "28000", fmt.Sprintf("Access denied for user '%s'@'%s'", username, host))
	}

	if !authResult.Success {
		logger.Errorf("认证失败: %s", authResult.ErrorMessage)
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, authResult.ErrorCode, "28000", authResult.ErrorMessage)
	}
	if err := h.applyProxyIdentity(ctx, authResult); err != nil {
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, 1045, "28000", err.Error())
	}

	if err := h.completeAuthentication(session, currentMysqlSession, authResult.User, database, authResult.Host, authResult.ActiveRoles, authResult.Privileges, authResult.DynamicPrivileges); err != nil {
		return err
	}
	recordAuthenticationSuccess(authResult.User, authResult.Host)
	return nil
}

func (h *DecoupledMySQLMessageHandler) handleAuthSwitchResponse(session Session, currentMysqlSession *server.MySQLServerSession, packet *MySQLPackage, pending *authSwitchState) error {
	if pending == nil || len(packet.Body) == 0 {
		if pending != nil {
			recordAuthenticationFailure(pending.Username, pending.Host)
		}
		return h.sendErrorResponse(session, 1045, "28000", "Authentication switch response is empty")
	}
	session.SetAttribute("auth_switch_pending", nil)
	if err := h.enforceAccountTLS(context.Background(), session, pending.Username, pending.Host); err != nil {
		recordAuthenticationFailure(pending.Username, pending.Host)
		return h.sendErrorResponse(session, 1045, "28000", err.Error())
	}
	if handled, requestErr := h.handleCachingSHA2PublicKeyRequest(session, packet); handled {
		if requestErr != nil {
			recordAuthenticationFailure(pending.Username, pending.Host)
			return h.sendErrorResponse(session, 1045, "28000", requestErr.Error())
		}
		return nil
	}
	if authResult, handled, fullErr := h.authenticateSHA256PasswordFullAuth(context.Background(), session, pending, packet.Body); handled {
		if fullErr != nil {
			recordAuthenticationFailure(pending.Username, pending.Host)
			return h.sendErrorResponse(session, 1045, "28000", fullErr.Error())
		}
		if authResult == nil || !authResult.Success {
			recordAuthenticationFailure(pending.Username, pending.Host)
			if authResult == nil {
				return h.sendErrorResponse(session, 1045, "28000", "sha256_password full authentication failed")
			}
			return h.sendErrorResponse(session, authResult.ErrorCode, "28000", authResult.ErrorMessage)
		}
		return h.finishAuthSwitch(session, currentMysqlSession, pending, authResult, packet.Header.PacketId+1)
	}
	if authResult, handled, fullErr := h.authenticateCachingSHA2FullAuth(context.Background(), session, pending, packet.Body); handled {
		if fullErr != nil {
			recordAuthenticationFailure(pending.Username, pending.Host)
			return h.sendErrorResponse(session, 1045, "28000", fullErr.Error())
		}
		if authResult == nil || !authResult.Success {
			recordAuthenticationFailure(pending.Username, pending.Host)
			if authResult == nil {
				return h.sendErrorResponse(session, 1045, "28000", "caching_sha2_password full authentication failed")
			}
			return h.sendErrorResponse(session, authResult.ErrorCode, "28000", authResult.ErrorMessage)
		}
		return h.finishAuthSwitch(session, currentMysqlSession, pending, authResult, packet.Header.PacketId+1)
	}
	authResult, err := h.authenticateWithChallenge(context.Background(), pending.Username, packet.Body, pending.Challenge, pending.Host, pending.Database)
	if err != nil || authResult == nil || !authResult.Success {
		recordAuthenticationFailure(pending.Username, pending.Host)
		if err != nil {
			return h.sendErrorResponse(session, 1045, "28000", err.Error())
		}
		return h.sendErrorResponse(session, authResult.ErrorCode, "28000", authResult.ErrorMessage)
	}
	return h.finishAuthSwitch(session, currentMysqlSession, pending, authResult, packet.Header.PacketId+1)
}

func (h *DecoupledMySQLMessageHandler) finishAuthSwitch(session Session, currentMysqlSession *server.MySQLServerSession, pending *authSwitchState, authResult *auth.AuthResult, sequence byte) error {
	if pending == nil || authResult == nil || !authResult.Success {
		if pending != nil {
			recordAuthenticationFailure(pending.Username, pending.Host)
		}
		return h.sendErrorResponse(session, 1045, "28000", "Authentication switch failed")
	}
	if pending.ChangeUser {
		if err := h.resetConnectionState(session, currentMysqlSession); err != nil {
			recordAuthenticationFailure(pending.Username, pending.Host)
			return h.sendErrorResponse(session, 1105, "HY000", err.Error())
		}
	}
	if err := h.applyProxyIdentity(context.Background(), authResult); err != nil {
		recordAuthenticationFailure(pending.Username, pending.Host)
		return h.sendErrorResponse(session, 1045, "28000", err.Error())
	}
	if err := h.completeAuthenticationWithSequence(session, currentMysqlSession, authResult.User, pending.Database, authResult.Host, authResult.ActiveRoles, authResult.Privileges, authResult.DynamicPrivileges, sequence); err != nil {
		return err
	}
	if pending.ChangeUser {
		applyChangeUserCharset(currentMysqlSession, pending.Charset, pending.Collation)
	}
	recordAuthenticationSuccess(authResult.User, authResult.Host)
	return nil
}

func (h *DecoupledMySQLMessageHandler) completeAuthentication(session Session, currentMysqlSession *server.MySQLServerSession, username, database, host string, activeRoles []string, privileges []common.PrivilegeType, dynamicPrivileges []string) error {
	return h.completeAuthenticationWithSequence(session, currentMysqlSession, username, database, host, activeRoles, privileges, dynamicPrivileges, 2)
}

func (h *DecoupledMySQLMessageHandler) completeAuthenticationWithSequence(session Session, currentMysqlSession *server.MySQLServerSession, username, database, host string, activeRoles []string, privileges []common.PrivilegeType, dynamicPrivileges []string, sequence byte) error {
	session.SetAttribute("auth_status", "success")
	if currentMysqlSession != nil && *currentMysqlSession != nil {
		(*currentMysqlSession).SetParamByName("user", username)
		(*currentMysqlSession).SetParamByName("database", database)
		(*currentMysqlSession).SetParamByName("host", host)
		(*currentMysqlSession).SetParamByName("remote_addr", session.RemoteAddr())
		(*currentMysqlSession).SetParamByName("global_privileges", append([]common.PrivilegeType(nil), privileges...))
		(*currentMysqlSession).SetParamByName("dynamic_privileges", append([]string(nil), dynamicPrivileges...))
		(*currentMysqlSession).SetParamByName("active_roles", append([]string(nil), activeRoles...))
		recordTLSConnectionState(session, currentMysqlSession)
		h.rwlock.Lock()
		h.sessionMap[session] = *currentMysqlSession
		h.rwlock.Unlock()
	}
	logger.Debugf("认证成功，用户: %s, 数据库: %s", username, database)
	okData := h.createOKPacket(0, 0, sequence)
	if err := session.WriteBytes(okData); err != nil {
		logger.Errorf("发送认证响应失败: %v", err)
		return err
	}
	return nil
}

func (h *DecoupledMySQLMessageHandler) resolveAuthHost(session Session) string {
	remoteAddr := strings.TrimSpace(session.RemoteAddr())
	if remoteAddr == "" {
		return ""
	}

	if host, _, err := net.SplitHostPort(remoteAddr); err == nil && host != "" {
		parsedHost := strings.Trim(host, "[]")
		if ip := net.ParseIP(parsedHost); ip != nil && ip.IsLoopback() {
			return "localhost"
		}
		return parsedHost
	}

	if ip := net.ParseIP(remoteAddr); ip != nil {
		if ip.IsLoopback() {
			return "localhost"
		}
		return remoteAddr
	}

	lastColon := strings.LastIndex(remoteAddr, ":")
	if lastColon > 0 {
		candidateHost := strings.Trim(remoteAddr[:lastColon], "[]")
		if ip := net.ParseIP(candidateHost); ip != nil {
			if ip.IsLoopback() {
				return "localhost"
			}
			return candidateHost
		}
	}

	return ""
}

// authenticateWithChallenge 握手阶段认证：mysql_native_password（*HEX40）或 caching_sha2 快速路径（authentication_string 为 64 位十六进制 stage2）。
func (h *DecoupledMySQLMessageHandler) authenticateWithChallenge(
	ctx context.Context,
	username string,
	authResponse []byte,
	challenge []byte,
	host string,
	database string,
) (*auth.AuthResult, error) {
	// 开发环境可配置免密：仅用于本地联调，生产应关闭。
	if h.cfg != nil && h.cfg.DevBypassPasswordAuth {
		logger.Warnf("⚠️ dev_bypass_password_auth=true，已跳过口令校验（user=%s）", username)
		return &auth.AuthResult{Success: true, User: username, Host: host}, nil
	}

	denied := &auth.AuthResult{
		Success:      false,
		ErrorCode:    1045,
		ErrorMessage: fmt.Sprintf("Access denied for user '%s'@'%s' (using password: YES)", username, host),
	}

	userInfo, err := h.authService.GetUserInfo(ctx, username, host)
	if err != nil {
		logger.Errorf("获取用户信息失败: %v", err)
		return &auth.AuthResult{
			Success:      false,
			ErrorCode:    1045,
			ErrorMessage: fmt.Sprintf("Access denied for user '%s'@'%s'", username, host),
		}, err
	}

	if userInfo.AccountLocked {
		return &auth.AuthResult{
			Success:      false,
			ErrorCode:    1045,
			ErrorMessage: fmt.Sprintf("Account '%s'@'%s' is locked", username, host),
		}, nil
	}
	if userInfo.PasswordExpired {
		return &auth.AuthResult{
			Success:      false,
			ErrorCode:    1045,
			ErrorMessage: "Your password has expired. To log in you must change it using a client that supports expired passwords.",
		}, nil
	}

	if userInfo.Password == "" || userInfo.Password == "*" {
		if len(authResponse) == 0 {
			return &auth.AuthResult{Success: true, User: username, Host: host, ActiveRoles: append([]string(nil), userInfo.DefaultRoles...)}, nil
		}
		return denied, nil
	}

	nativeV := &auth.MySQLNativePasswordValidator{}
	sha2V := &auth.CachingSHA2PasswordValidator{}

	if strings.HasPrefix(userInfo.Password, "*") && len(userInfo.Password) == 41 {
		if nativeV.ValidateNativeHandshakeResponse(authResponse, challenge, userInfo.Password) {
			return &auth.AuthResult{Success: true, User: username, Host: host, ActiveRoles: append([]string(nil), userInfo.DefaultRoles...)}, nil
		}
		return denied, nil
	}

	if len(authResponse) == 32 {
		if sha2V.ValidateCachingSHA2FastAuth(authResponse, challenge, userInfo.Password) {
			return &auth.AuthResult{Success: true, User: username, Host: host, ActiveRoles: append([]string(nil), userInfo.DefaultRoles...)}, nil
		}
	}

	logger.Debugf("认证失败: 不支持的 authentication_string 格式或错误口令 (user=%s)", username)
	return denied, nil
}

func (h *DecoupledMySQLMessageHandler) preparedStmtMgrFromSession(session Session) *protocol.PreparedStatementManager {
	v := session.GetAttribute("prepared_stmt_mgr")
	if v == nil {
		m := protocol.NewPreparedStatementManager()
		session.SetAttribute("prepared_stmt_mgr", m)
		return m
	}
	return v.(*protocol.PreparedStatementManager)
}

// bindPreparedStmtMgr exposes the protocol-owned prepared statement inventory
// to the engine's Performance Schema views without making the network Session
// implementation part of the engine package contract.
func bindPreparedStmtMgr(currentMysqlSession *server.MySQLServerSession, mgr *protocol.PreparedStatementManager) {
	if currentMysqlSession != nil && *currentMysqlSession != nil && mgr != nil {
		(*currentMysqlSession).SetParamByName("prepared_stmt_mgr", mgr)
	}
}

func (h *DecoupledMySQLMessageHandler) handleComStmtPrepare(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 2 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_PREPARE")
	}
	sqlText := string(recMySQLPkg.Body[1:])
	mgr := h.preparedStmtMgrFromSession(session)
	bindPreparedStmtMgr(currentMysqlSession, mgr)
	stmt, err := mgr.Prepare(sqlText)
	if err != nil {
		return h.sendGoErrorResponse(session, err)
	}
	// Connector/J decides whether it can open a server-side cursor from the
	// result metadata returned by COM_STMT_PREPARE.  Populate that metadata for
	// read-only statements up front; otherwise it sends a normal execute and
	// subsequently decodes our text rows as binary rows.
	h.populatePreparedResultMetadata(session, currentMysqlSession, stmt)
	seq := recMySQLPkg.Header.PacketId + 1
	for _, pkt := range protocol.EncodePrepareResponse(stmt, seq) {
		if werr := session.WriteBytes(pkt); werr != nil {
			return werr
		}
	}
	return nil
}

func (h *DecoupledMySQLMessageHandler) populatePreparedResultMetadata(session Session, currentMysqlSession *server.MySQLServerSession, stmt *protocol.PreparedStatement) {
	if stmt == nil || !looksLikePreparedResultStatement(stmt.SQL) || h.businessHandler == nil {
		return
	}
	params := make([]interface{}, strings.Count(stmt.SQL, "?"))
	metadataSQL := protocol.BindPreparedSQL(stmt.SQL, params)
	result, err := h.executePreparedQueryResult(session, currentMysqlSession, metadataSQL)
	if err != nil || result == nil || len(result.Columns) == 0 {
		return
	}
	definitions := h.resultColumnDefinitions(result)
	stmt.ColumnCount = uint16(len(definitions))
	stmt.Columns = make([]*protocol.ColumnMetadata, 0, len(definitions))
	for _, definition := range definitions {
		stmt.Columns = append(stmt.Columns, &protocol.ColumnMetadata{
			Catalog:  definition.Catalog,
			Database: definition.Schema,
			Table:    definition.Table,
			OrgTable: definition.OrgTable,
			Name:     definition.Name,
			OrgName:  definition.OrgName,
			Charset:  definition.CharacterSet,
			Length:   definition.ColumnLength,
			Type:     definition.ColumnType,
			Flags:    definition.Flags,
			Decimals: definition.Decimals,
		})
	}
}

func looksLikePreparedResultStatement(sqlText string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(sqlText))
	for _, prefix := range []string{"select", "with", "show", "describe", "desc", "explain"} {
		if strings.HasPrefix(trimmed, prefix+" ") || trimmed == prefix {
			return true
		}
	}
	return false
}

func (h *DecoupledMySQLMessageHandler) handleComStmtExecute(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	body := recMySQLPkg.Body
	if len(body) < 10 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_EXECUTE")
	}
	stmtID := binary.LittleEndian.Uint32(body[1:5])
	mgr := h.preparedStmtMgrFromSession(session)
	bindPreparedStmtMgr(currentMysqlSession, mgr)
	stmt, err := mgr.Get(stmtID)
	if err != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
	}
	if body[5]&0x01 != 0 {
		open, cursorErr := mgr.HasOpenCursor(stmtID)
		if cursorErr != nil {
			return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", cursorErr.Error())
		}
		if open {
			return h.sendErrorResponse(session, common.ErrExecStmtWithOpenCursor, common.MySQLState[common.ErrExecStmtWithOpenCursor], "")
		}
	}
	params, typeBlock, perr := protocol.ParseBinaryStmtExecuteParams(body[10:], stmt.ParamCount, stmt.LastParamTypes)
	if perr != nil {
		return h.sendErrorResponse(session, 1210, "HY000", perr.Error())
	}
	longData, lerr := mgr.ConsumeLongData(stmtID)
	if lerr != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", lerr.Error())
	}
	for paramID, value := range longData {
		if int(paramID) < len(params) {
			params[paramID] = value
		}
	}
	stmt.LastParamTypes = typeBlock
	boundSQL := protocol.BindPreparedSQL(stmt.SQL, params)
	if body[5]&0x01 != 0 {
		result, err := h.executePreparedQueryResult(session, currentMysqlSession, boundSQL)
		if err != nil {
			return h.sendGoErrorResponse(session, err)
		}
		if result == nil || !strings.EqualFold(result.Type, "select") {
			return h.sendErrorResponse(session, common.ErrNotSupportedYet, "0A000", "server-side cursors require a result set")
		}
		if err := mgr.SetCursorResult(stmtID, result); err != nil {
			if open, _ := mgr.HasOpenCursor(stmtID); open {
				return h.sendErrorResponse(session, common.ErrExecStmtWithOpenCursor, common.MySQLState[common.ErrExecStmtWithOpenCursor], "")
			}
			return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
		}
		// A cursor execute returns column metadata and keeps the rows on the
		// server.  Returning a standalone OK packet here makes Connector/J
		// treat the cursor as an ordinary update and causes the subsequent
		// COM_STMT_FETCH response to be decoded with the wrong protocol.
		return h.sendCursorMetadata(session, result, recMySQLPkg.Header.PacketId+1)
	}
	result, err := h.executePreparedQueryResult(session, currentMysqlSession, boundSQL)
	if err != nil {
		return h.sendGoErrorResponse(session, err)
	}
	if result == nil || (len(result.Columns) == 0 && len(result.Rows) == 0) {
		var affectedRows, lastInsertID uint64
		if result != nil {
			affectedRows = result.AffectedRows
			lastInsertID = result.LastInsertID
		}
		return h.sendMySQLOKPacketWithStatus(session, affectedRows, lastInsertID, recMySQLPkg.Header.PacketId+1, mysqlResponseStatusFlags(session, *currentMysqlSession))
	}
	return h.sendBinaryPreparedResultSet(session, result, recMySQLPkg.Header.PacketId+1)
}

func (h *DecoupledMySQLMessageHandler) executePreparedQueryResult(session Session, currentMysqlSession *server.MySQLServerSession, query string) (*protocol.MessageQueryResult, error) {
	if h.businessHandler == nil {
		return nil, fmt.Errorf("business handler is not initialized")
	}
	database := ""
	if currentMysqlSession != nil && *currentMysqlSession != nil {
		if value, ok := (*currentMysqlSession).GetParamByName("database").(string); ok {
			database = value
		}
	}
	message := &protocol.QueryMessage{BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), query), SQL: query, Database: database}
	var response protocol.Message
	var err error
	if enh, ok := h.businessHandler.(*dispatcher.EnhancedBusinessMessageHandler); ok && currentMysqlSession != nil {
		response, err = enh.HandleQueryWithRealSession(*currentMysqlSession, query, database)
	} else {
		response, err = h.businessHandler.HandleMessage(message)
	}
	if err != nil {
		return nil, err
	}
	switch resp := response.(type) {
	case *protocol.ResponseMessage:
		if resp.Result == nil {
			return nil, nil
		}
		if resp.Result.Error != nil {
			return nil, resp.Result.Error
		}
		return resp.Result, nil
	case *protocol.ErrorMessage:
		return nil, &common.SQLError{Code: resp.Code, State: resp.State, Message: resp.Message}
	default:
		return nil, nil
	}
}

func (h *DecoupledMySQLMessageHandler) handleComStmtFetch(session Session, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 9 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_FETCH")
	}
	stmtID := binary.LittleEndian.Uint32(recMySQLPkg.Body[1:5])
	rowCount := binary.LittleEndian.Uint32(recMySQLPkg.Body[5:9])
	mgr := h.preparedStmtMgrFromSession(session)
	result, done, err := mgr.FetchCursor(stmtID, rowCount)
	if err != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
	}
	return h.sendBinaryCursorRows(session, result, done, recMySQLPkg.Header.PacketId+1)
}

func (h *DecoupledMySQLMessageHandler) handleComStmtSendLongData(session Session, recMySQLPkg *MySQLPackage) error {
	body := recMySQLPkg.Body
	if len(body) < 7 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_SEND_LONG_DATA")
	}
	stmtID := binary.LittleEndian.Uint32(body[1:5])
	paramID := binary.LittleEndian.Uint16(body[5:7])
	mgr := h.preparedStmtMgrFromSession(session)
	if err := mgr.AppendLongData(stmtID, paramID, body[7:]); err != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
	}
	return nil
}

func (h *DecoupledMySQLMessageHandler) handleComResetConnection(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	if err := h.resetConnectionState(session, currentMysqlSession); err != nil {
		return h.sendErrorResponse(session, 1105, "HY000", err.Error())
	}
	okData := h.createOKPacket(0, 0, recMySQLPkg.Header.PacketId+1)
	return session.WriteBytes(okData)
}

// resetConnectionState contains the state cleanup shared by COM_RESET_CONNECTION
// and the successful COM_CHANGE_USER path. It intentionally preserves the
// handshake capability flags while clearing command/session-local overrides.
func (h *DecoupledMySQLMessageHandler) resetConnectionState(session Session, currentMysqlSession *server.MySQLServerSession) error {
	resetPreparedStmtMgr := protocol.NewPreparedStatementManager()
	session.SetAttribute("prepared_stmt_mgr", resetPreparedStmtMgr)
	bindPreparedStmtMgr(currentMysqlSession, resetPreparedStmtMgr)
	// COM_SET_OPTION changes a connection-local capability.  Reset the
	// override back to the handshake baseline so pooled connections do not
	// retain a previous user's multi-statement setting.
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	session.SetAttribute("client_multi_statements", capabilities&protocol.CLIENT_MULTI_STATEMENTS != 0)
	session.SetAttribute("__more_results__", false)
	if currentMysqlSession != nil && *currentMysqlSession != nil {
		if h.xmysqlEngine != nil {
			if err := h.xmysqlEngine.ResetSession(*currentMysqlSession); err != nil {
				return err
			}
		} else {
			(*currentMysqlSession).SetParamByName("autocommit", "1")
			(*currentMysqlSession).SetParamByName("in_transaction", false)
			(*currentMysqlSession).SetParamByName("database", "")
			(*currentMysqlSession).SetParamByName("last_insert_id", uint64(0))
			(*currentMysqlSession).SetParamByName("row_count", int64(0))
			(*currentMysqlSession).SetParamByName("locked_tables", map[string]string{})
			(*currentMysqlSession).SetParamByName("warnings", []engine.Warning{})
			(*currentMysqlSession).SetParamByName("user_variables", map[string]interface{}{})
			(*currentMysqlSession).SetParamByName("session_variables", map[string]interface{}{})
		}
	}
	return nil
}

// handleComChangeUser implements the command-phase re-authentication path
// used by connection pools. The packet uses the negotiated secure-connection
// one-byte auth length and carries the new user/database; optional charset and
// plugin fields are accepted and ignored because the existing authentication
// service selects the account policy from the server-side user record.
func (h *DecoupledMySQLMessageHandler) handleComChangeUser(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	body := recMySQLPkg.Body
	if len(body) < 2 {
		return h.sendErrorResponse(session, 1045, "28000", "Invalid COM_CHANGE_USER packet")
	}
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	offset := 1
	username, next, ok := readNulField(body, offset)
	if !ok || username == "" {
		return h.sendErrorResponse(session, 1045, "28000", "Invalid COM_CHANGE_USER user")
	}
	offset = next
	var authResponse []byte
	var authErr error
	authResponse, offset, authErr = readClientAuthResponse(body, offset, capabilities)
	if authErr != nil {
		return h.sendErrorResponse(session, 1045, "28000", "Invalid COM_CHANGE_USER authentication data")
	}
	database, next, ok := readNulField(body, offset)
	if !ok {
		return h.sendErrorResponse(session, 1045, "28000", "Invalid COM_CHANGE_USER database")
	}
	offset = next
	requestedCharset, requestedCollation := "", ""
	if capabilities&protocol.CLIENT_PROTOCOL_41 != 0 && offset < len(body) {
		if offset+2 > len(body) {
			return h.sendErrorResponse(session, 1045, "28000", "Invalid COM_CHANGE_USER character set")
		}
		collationID := binary.LittleEndian.Uint16(body[offset : offset+2])
		var charsetErr error
		requestedCharset, requestedCollation, charsetErr = changeUserCharset(collationID)
		if charsetErr != nil {
			sqlErr := protocol.ClassifyGoError(charsetErr)
			return h.sendErrorResponse(session, sqlErr.Code, sqlErr.State, sqlErr.Message)
		}
		offset += 2
		if offset < len(body) {
			_, _, _ = readNulField(body, offset) // optional authentication plugin name
		}
	}

	challengeValue := session.GetAttribute("auth_challenge")
	challenge, ok := challengeValue.([]byte)
	if !ok || len(challenge) == 0 {
		return h.sendErrorResponse(session, 1045, "28000", "Authentication failed: missing challenge")
	}
	host := h.resolveAuthHost(session)
	if host == "" {
		return h.sendErrorResponse(session, 1045, "28000", "Access denied for user")
	}
	changeUserFailure := func(message string) error {
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, 1045, "28000", message)
	}
	if err := h.enforceAccountTLS(context.Background(), session, username, host); err != nil {
		return changeUserFailure(err.Error())
	}
	if h.authService != nil && (h.cfg == nil || !h.cfg.DevBypassPasswordAuth) {
		if userInfo, lookupErr := h.authService.GetUserInfo(context.Background(), username, host); lookupErr == nil && userInfo != nil {
			if plugin := authSwitchPlugin(userInfo); plugin != "" && (plugin == "sha256_password" || len(authResponse) != 32) {
				pending := &authSwitchState{
					Username:         username,
					Database:         database,
					Host:             host,
					Challenge:        append([]byte(nil), challenge...),
					ChangeUser:       true,
					Charset:          requestedCharset,
					Collation:        requestedCollation,
					ResponseSequence: recMySQLPkg.Header.PacketId + 1,
				}
				session.SetAttribute("auth_switch_pending", pending)
				return session.WriteBytes(encodeAuthSwitchRequest(plugin, challenge, recMySQLPkg.Header.PacketId+1))
			}
		}
	}
	authResult, err := h.authenticateWithChallenge(context.Background(), username, authResponse, challenge, host, database)
	if err != nil {
		return changeUserFailure(err.Error())
	}
	if authResult == nil || !authResult.Success {
		if authResult == nil {
			return changeUserFailure("Authentication failed")
		}
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, authResult.ErrorCode, "28000", authResult.ErrorMessage)
	}
	if err := h.applyProxyIdentity(context.Background(), authResult); err != nil {
		return changeUserFailure(err.Error())
	}
	if err := h.resetConnectionState(session, currentMysqlSession); err != nil {
		recordAuthenticationFailure(username, host)
		return h.sendErrorResponse(session, 1105, "HY000", err.Error())
	}
	if currentMysqlSession != nil && *currentMysqlSession != nil {
		(*currentMysqlSession).SetParamByName("user", authResult.User)
		(*currentMysqlSession).SetParamByName("host", authResult.Host)
		(*currentMysqlSession).SetParamByName("database", database)
		(*currentMysqlSession).SetParamByName("global_privileges", append([]common.PrivilegeType(nil), authResult.Privileges...))
		(*currentMysqlSession).SetParamByName("dynamic_privileges", append([]string(nil), authResult.DynamicPrivileges...))
		(*currentMysqlSession).SetParamByName("active_roles", append([]string(nil), authResult.ActiveRoles...))
		recordTLSConnectionState(session, currentMysqlSession)
		applyChangeUserCharset(currentMysqlSession, requestedCharset, requestedCollation)
		h.rwlock.Lock()
		h.sessionMap[session] = *currentMysqlSession
		h.rwlock.Unlock()
	}
	session.SetAttribute("auth_status", "success")
	recordAuthenticationSuccess(authResult.User, authResult.Host)
	return session.WriteBytes(h.createOKPacket(0, 0, recMySQLPkg.Header.PacketId+1))
}

func (h *DecoupledMySQLMessageHandler) applyProxyIdentity(ctx context.Context, result *auth.AuthResult) error {
	if result == nil || !result.Success || h.authService == nil {
		return nil
	}
	resolver, ok := h.authService.(interface {
		ResolveProxyUser(context.Context, string, string) (string, string, bool, error)
	})
	if !ok {
		return nil
	}
	originalUser, originalHost := result.User, result.Host
	proxiedUser, proxiedHost, found, err := resolver.ResolveProxyUser(ctx, originalUser, originalHost)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	proxiedInfo, err := h.authService.GetUserInfo(ctx, proxiedUser, proxiedHost)
	if err != nil || proxiedInfo == nil {
		if err == nil {
			err = fmt.Errorf("PROXY target '%s'@'%s' does not exist", proxiedUser, proxiedHost)
		}
		return err
	}
	if proxiedInfo.AccountLocked {
		return fmt.Errorf("PROXY target '%s'@'%s' is locked", proxiedUser, proxiedHost)
	}
	result.User = proxiedUser
	result.Host = proxiedHost
	result.Privileges = append([]common.PrivilegeType(nil), proxiedInfo.GlobalPrivileges...)
	result.DynamicPrivileges = append([]string(nil), proxiedInfo.DynamicPrivileges...)
	result.ActiveRoles = append([]string(nil), proxiedInfo.DefaultRoles...)
	return nil
}

func changeUserCharset(collationID uint16) (string, string, error) {
	collation, err := protocol.GetGlobalCharsetManager().GetCollationByID(collationID)
	if err != nil {
		return "", "", common.NewErrf(common.ErrUnknownCollation, "Unknown collation: '%d'", nil, collationID)
	}
	return collation.Charset, collation.Name, nil
}

func applyChangeUserCharset(currentMysqlSession *server.MySQLServerSession, charset, collation string) {
	if currentMysqlSession == nil || *currentMysqlSession == nil || charset == "" {
		return
	}
	(*currentMysqlSession).SetParamByName("character_set_client", charset)
	(*currentMysqlSession).SetParamByName("character_set_connection", charset)
	(*currentMysqlSession).SetParamByName("character_set_results", charset)
	if collation != "" {
		(*currentMysqlSession).SetParamByName("collation_connection", collation)
	}
}

func readNulField(payload []byte, offset int) (string, int, bool) {
	value, next, ok := readNulFieldBytes(payload, offset)
	return string(value), next, ok
}

func readNulFieldBytes(payload []byte, offset int) ([]byte, int, bool) {
	if offset < 0 || offset > len(payload) {
		return nil, offset, false
	}
	end := bytes.IndexByte(payload[offset:], 0)
	if end < 0 {
		return nil, offset, false
	}
	end += offset
	return payload[offset:end], end + 1, true
}

func (h *DecoupledMySQLMessageHandler) handleComStmtClose(session Session, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 5 {
		return nil
	}
	stmtID := binary.LittleEndian.Uint32(recMySQLPkg.Body[1:5])
	mgr := h.preparedStmtMgrFromSession(session)
	_ = mgr.Close(stmtID)
	return nil
}

func (h *DecoupledMySQLMessageHandler) handleComStmtReset(session Session, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 5 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_RESET")
	}
	stmtID := binary.LittleEndian.Uint32(recMySQLPkg.Body[1:5])
	mgr := h.preparedStmtMgrFromSession(session)
	if err := mgr.Reset(stmtID); err != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
	}
	okData := h.createOKPacket(0, 0, recMySQLPkg.Header.PacketId+1)
	return session.WriteBytes(okData)
}

// sendQueryResultSet 发送查询结果集
// 严格按照 MySQL 协议规范实现，兼容 MySQL Connector/J 5.1.x
func (h *DecoupledMySQLMessageHandler) sendQueryResultSet(session Session, result *protocol.MessageQueryResult, seqID byte) error {
	logger.Debugf("[sendQueryResultSet] 开始发送查询结果集（MySQL 协议标准实现）")

	// 检查会话状态
	if session.IsClosed() {
		logger.Errorf("会话已关闭，无法发送结果集")
		return fmt.Errorf("session is closed")
	}

	if result == nil {
		logger.Errorf("结果集为空")
		return fmt.Errorf("result is nil")
	}

	logger.Debugf("结果集信息: 列数=%d, 行数=%d", len(result.Columns), len(result.Rows))

	// 使用复用的协议编码器（避免重复创建，提升性能）
	encoder := h.resultSetEncoder

	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	useDeprecatedEOF := capabilities&protocol.CLIENT_DEPRECATE_EOF == 0
	statusFlags := uint16(protocol.SERVER_STATUS_AUTOCOMMIT)
	if more, ok := session.GetAttribute("__more_results__").(bool); ok && more && capabilities&(protocol.CLIENT_MULTI_RESULTS|protocol.CLIENT_PS_MULTI_RESULTS) != 0 {
		statusFlags |= protocol.SERVER_MORE_RESULTS_EXISTS
	}

	// ========================================================================
	// Step 1: 发送 Column Count Packet
	// ========================================================================
	columnCount := uint64(len(result.Columns))
	columnCountData := encoder.WriteLenEncInt(columnCount)
	columnCountPacket := h.createMySQLPacket(columnCountData, seqID)

	logger.Debugf("[sendQueryResultSet] 发送列数包: %d 列", columnCount)
	err := session.WriteBytes(columnCountPacket)
	if err != nil {
		logger.Errorf("发送列数包失败: %v", err)
		return err
	}
	seqID++

	// ========================================================================
	// Step 2: 发送 Column Definition Packets
	// ========================================================================
	columnDefinitions := h.resultColumnDefinitions(result)
	for colIdx, colDef := range columnDefinitions {
		columnDefPacket := encoder.EncodeColumnDefinitionPacket(colDef, seqID)

		logger.Debugf("[sendQueryResultSet] 发送列定义: name=%s, type=0x%02X, length=%d",
			result.Columns[colIdx], colDef.ColumnType, colDef.ColumnLength)

		err := session.WriteBytes(columnDefPacket)
		if err != nil {
			logger.Errorf("发送列定义包失败: %v", err)
			return err
		}
		seqID++
	}

	// ========================================================================
	// Step 3: 发送列定义结束标记（EOF 或 OK，取决于 CLIENT_DEPRECATE_EOF）
	// 在 CLIENT_DEPRECATE_EOF 下必须发送 OK 包（0x00）作为列定义结束标记。
	// ========================================================================
	if useDeprecatedEOF {
		eofPacket1 := protocol.EncodeEOFPacketWithSeq(0, statusFlags, seqID)

		logger.Debugf("[sendQueryResultSet] 发送第一个 EOF 包（列定义结束）")
		err = session.WriteBytes(eofPacket1)
		if err != nil {
			logger.Errorf("发送第一个 EOF 包失败: %v", err)
			return err
		}
		seqID++
	} else {
		okPacket1 := protocol.EncodeOKPacketWithSeq(0, 0, statusFlags, 0, seqID)

		logger.Debugf("[sendQueryResultSet] 发送列定义结束 OK 包（CLIENT_DEPRECATE_EOF）")
		err = session.WriteBytes(okPacket1)
		if err != nil {
			logger.Errorf("发送列定义结束 OK 包失败: %v", err)
			return err
		}
		seqID++
	}

	// ========================================================================
	// Step 4: 发送 Row Data Packets（文本协议）
	// ========================================================================
	for rowIdx, row := range result.Rows {
		rowPacket := encoder.EncodeRowDataPacket(row, seqID)

		rowPacketHex := hex.EncodeToString(rowPacket)
		logger.Debugf("ROW_PACKET_HEX: %s", rowPacketHex)

		if len(rowPacket) >= 4 {
			payloadLen := int(rowPacket[0]) |
				int(rowPacket[1])<<8 |
				int(rowPacket[2])<<16
			seq := rowPacket[3]
			bodyHex := hex.EncodeToString(rowPacket[4:])

			logger.Debugf("ROW_PACKET_LENGTH: %d", payloadLen)
			logger.Debugf("ROW_PACKET_SEQ: %d", seq)
			logger.Debugf("ROW_PACKET_BODY_HEX: %s", bodyHex)
		} else {
			logger.Errorf("ROW_PACKET_TOO_SHORT: len=%d", len(rowPacket))
		}

		logger.Debugf("[sendQueryResultSet] 发送行数据 %d: %v", rowIdx, row)

		err := session.WriteBytes(rowPacket)
		if err != nil {
			logger.Errorf("发送行数据包失败: %v", err)
			return err
		}
		seqID++

		// 详细记录每列的值和类型
		for colIdx, val := range row {
			if val == nil {
				logger.Debugf("   列 %d (%s): NULL", colIdx, result.Columns[colIdx])
			} else {
				logger.Debugf("   列 %d (%s): %v (类型: %T)", colIdx, result.Columns[colIdx], val, val)
			}
		}
	}

	// ========================================================================
	// Step 5: 发送结果集结束标记（EOF 或 OK，结束行数据）
	// 在 CLIENT_DEPRECATE_EOF 下发送 OK 包（0x00）。
	// ========================================================================
	if useDeprecatedEOF {
		eofPacket2 := protocol.EncodeEOFPacketWithSeq(0, statusFlags, seqID)

		logger.Debugf("[sendQueryResultSet] 发送第二个 EOF 包（行数据结束）")
		err = session.WriteBytes(eofPacket2)
		if err != nil {
			logger.Errorf("发送第二个 EOF 包失败: %v", err)
			return err
		}
	} else {
		okPacket2 := protocol.EncodeOKPacketWithSeq(0, 0, statusFlags, 0, seqID)

		logger.Debugf("[sendQueryResultSet] 发送结果集结束 OK 包（CLIENT_DEPRECATE_EOF）")
		err = session.WriteBytes(okPacket2)
		if err != nil {
			logger.Errorf("发送结果集结束 OK 包失败: %v", err)
			return err
		}
	}

	logger.Debugf("[sendQueryResultSet] ✅ 查询结果集发送完成: %d 列, %d 行", len(result.Columns), len(result.Rows))

	return nil
}

// resultColumnDefinitions keeps text and binary result paths on the same
// metadata contract.  Cursor rows must use exactly the types advertised by
// the column definitions or Connector/J will consume the row bytes with the
// wrong width.
func (h *DecoupledMySQLMessageHandler) resultColumnDefinitions(result *protocol.MessageQueryResult) []*protocol.ColumnDefinition {
	if result == nil {
		return nil
	}
	encoder := h.resultSetEncoder
	definitions := make([]*protocol.ColumnDefinition, 0, len(result.Columns))
	for index, name := range result.Columns {
		var definition *protocol.ColumnDefinition
		if index < len(result.ColumnTypes) && strings.TrimSpace(result.ColumnTypes[index]) != "" {
			definition = createColumnDefinitionForType(encoder, name, result.ColumnTypes[index])
		}
		if definition == nil && len(result.Rows) > 0 && index < len(result.Rows[0]) {
			definition = encoder.CreateColumnDefinitionFromValue(name, result.Rows[0][index])
		}
		if definition == nil {
			definition = encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_VAR_STRING, 0)
		}
		definitions = append(definitions, definition)
	}
	return definitions
}

func (h *DecoupledMySQLMessageHandler) sendCursorMetadata(session Session, result *protocol.MessageQueryResult, seqID byte) error {
	if result == nil {
		return fmt.Errorf("cursor result is nil")
	}
	definitions := h.resultColumnDefinitions(result)
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	statusFlags := uint16(protocol.SERVER_STATUS_AUTOCOMMIT | protocol.SERVER_STATUS_CURSOR_EXISTS)
	if err := session.WriteBytes(h.createMySQLPacket(h.resultSetEncoder.WriteLenEncInt(uint64(len(definitions))), seqID)); err != nil {
		return err
	}
	seqID++
	for _, definition := range definitions {
		if err := session.WriteBytes(h.resultSetEncoder.EncodeColumnDefinitionPacket(definition, seqID)); err != nil {
			return err
		}
		seqID++
	}
	return h.sendResultTerminator(session, seqID, capabilities, statusFlags, result.WarningCount)
}

func (h *DecoupledMySQLMessageHandler) sendBinaryCursorRows(session Session, result *protocol.MessageQueryResult, done bool, seqID byte) error {
	if result == nil {
		return fmt.Errorf("cursor result is nil")
	}
	definitions := h.resultColumnDefinitions(result)
	for _, row := range result.Rows {
		packet := h.resultSetEncoder.EncodeBinaryRowPacket(row, definitions, seqID)
		if err := session.WriteBytes(packet); err != nil {
			return err
		}
		seqID++
	}
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	statusFlags := uint16(protocol.SERVER_STATUS_AUTOCOMMIT)
	if done {
		statusFlags |= protocol.SERVER_STATUS_LAST_ROW_SENT
	} else {
		statusFlags |= protocol.SERVER_STATUS_CURSOR_EXISTS
	}
	return h.sendResultTerminator(session, seqID, capabilities, statusFlags, result.WarningCount)
}

// sendBinaryPreparedResultSet encodes COM_STMT_EXECUTE rows with MySQL's
// binary result-set protocol. Prepared-statement clients decode rows from the
// column metadata and binary null bitmap; text-protocol rows are not
// interchangeable with this response.
func (h *DecoupledMySQLMessageHandler) sendBinaryPreparedResultSet(session Session, result *protocol.MessageQueryResult, seqID byte) error {
	if result == nil {
		return fmt.Errorf("prepared result is nil")
	}
	definitions := h.resultColumnDefinitions(result)
	if err := session.WriteBytes(h.createMySQLPacket(h.resultSetEncoder.WriteLenEncInt(uint64(len(definitions))), seqID)); err != nil {
		return err
	}
	seqID++
	for _, definition := range definitions {
		if err := session.WriteBytes(h.resultSetEncoder.EncodeColumnDefinitionPacket(definition, seqID)); err != nil {
			return err
		}
		seqID++
	}
	capabilities, _ := session.GetAttribute("client_capabilities").(uint32)
	statusFlags := uint16(protocol.SERVER_STATUS_AUTOCOMMIT | protocol.SERVER_STATUS_LAST_ROW_SENT)
	if err := h.sendResultTerminator(session, seqID, capabilities, statusFlags, result.WarningCount); err != nil {
		return err
	}
	seqID++
	for _, row := range result.Rows {
		if err := session.WriteBytes(h.resultSetEncoder.EncodeBinaryRowPacket(row, definitions, seqID)); err != nil {
			return err
		}
		seqID++
	}
	return h.sendResultTerminator(session, seqID, capabilities, statusFlags, result.WarningCount)
}

func (h *DecoupledMySQLMessageHandler) sendResultTerminator(session Session, seqID byte, capabilities uint32, statusFlags uint16, warnings uint16) error {
	if capabilities&protocol.CLIENT_DEPRECATE_EOF == 0 {
		return session.WriteBytes(protocol.EncodeEOFPacketWithSeq(warnings, statusFlags, seqID))
	}
	return session.WriteBytes(protocol.EncodeOKPacketWithSeq(0, 0, statusFlags, warnings, seqID))
}

// splitTopLevelStatements splits COM_QUERY multi-statements without treating
// semicolons inside strings or nested expressions as statement boundaries.
func splitTopLevelStatements(query string) []string {
	statements := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(query); index++ {
		ch := query[index]
		if quote != 0 {
			if ch == quote && (index == 0 || query[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ';':
			if depth == 0 {
				if statement := strings.TrimSpace(query[start:index]); statement != "" {
					statements = append(statements, statement)
				}
				start = index + 1
			}
		}
	}
	if statement := strings.TrimSpace(query[start:]); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

func createColumnDefinitionForType(encoder *protocol.MySQLResultSetEncoder, name string, columnType string) *protocol.ColumnDefinition {
	typeName := strings.ToLower(strings.TrimSpace(columnType))
	if separator := strings.IndexAny(typeName, "( "); separator >= 0 {
		typeName = typeName[:separator]
	}
	switch typeName {
	case "tinyint":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_TINY, 0)
	case "smallint":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_SHORT, 0)
	case "mediumint":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_INT24, 0)
	case "int", "integer":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_LONG, 0)
	case "bigint":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_LONGLONG, 0)
	case "float":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_FLOAT, 0)
	case "double":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_DOUBLE, 0)
	case "decimal":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_NEWDECIMAL, 0)
	case "date":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_DATE, 0)
	case "time":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_TIME, 0)
	case "datetime":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_DATETIME, 0)
	case "timestamp":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_TIMESTAMP, 0)
	case "year":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_YEAR, 0)
	case "bool", "boolean":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_TINY, 0)
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_BLOB, 0)
	default:
		return encoder.CreateColumnDefinition(name, protocol.MYSQL_TYPE_VAR_STRING, 0)
	}
}

// getCommandName 获取命令名称
func (h *DecoupledMySQLMessageHandler) getCommandName(cmd byte) string {
	switch cmd {
	case 0x00:
		return "COM_SLEEP"
	case 0x01:
		return "COM_QUIT"
	case 0x02:
		return "COM_INIT_DB"
	case 0x03:
		return "COM_QUERY"
	case 0x04:
		return "COM_FIELD_LIST"
	case 0x05:
		return "COM_CREATE_DB"
	case 0x06:
		return "COM_DROP_DB"
	case 0x07:
		return "COM_REFRESH"
	case 0x08:
		return "COM_SHUTDOWN"
	case 0x09:
		return "COM_STATISTICS"
	case 0x0a:
		return "COM_PROCESS_INFO"
	case 0x0b:
		return "COM_CONNECT"
	case 0x0c:
		return "COM_PROCESS_KILL"
	case 0x0d:
		return "COM_DEBUG"
	case 0x0e:
		return "COM_PING"
	case 0x0f:
		return "COM_TIME"
	case 0x10:
		return "COM_DELAYED_INSERT"
	case 0x11:
		return "COM_CHANGE_USER"
	case 0x12:
		return "COM_BINLOG_DUMP"
	case 0x13:
		return "COM_TABLE_DUMP"
	case 0x14:
		return "COM_CONNECT_OUT"
	case 0x15:
		return "COM_REGISTER_SLAVE"
	case 0x16:
		return "COM_STMT_PREPARE"
	case 0x17:
		return "COM_STMT_EXECUTE"
	case 0x18:
		return "COM_STMT_SEND_LONG_DATA"
	case 0x19:
		return "COM_STMT_CLOSE"
	case 0x1a:
		return "COM_STMT_RESET"
	case 0x1b:
		return "COM_SET_OPTION"
	case 0x1c:
		return "COM_STMT_FETCH"
	case 0x1d:
		return "COM_DAEMON"
	case 0x1e:
		return "COM_BINLOG_DUMP_GTID"
	case 0x1f:
		return "COM_RESET_CONNECTION"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", cmd)
	}
}

// getCommandDetail 获取命令详情
func (h *DecoupledMySQLMessageHandler) getCommandDetail(body []byte) string {
	if len(body) == 0 {
		return "empty"
	}

	cmd := body[0]
	switch cmd {
	case 0x03: // COM_QUERY
		if len(body) > 1 {
			query := string(body[1:])
			if len(query) > 100 {
				return query[:100] + "..."
			}
			return query
		}
		return "empty query"
	case 0x02: // COM_INIT_DB
		if len(body) > 1 {
			return string(body[1:])
		}
		return "empty db"
	case 0x16: // COM_STMT_PREPARE
		if len(body) > 1 {
			query := string(body[1:])
			if len(query) > 100 {
				return query[:100] + "..."
			}
			return query
		}
		return "empty prepare"
	case 0x17: // COM_STMT_EXECUTE
		if len(body) >= 5 {
			stmtID := uint32(body[1]) | uint32(body[2])<<8 | uint32(body[3])<<16 | uint32(body[4])<<24
			return fmt.Sprintf("stmt_id=%d", stmtID)
		}
		return "invalid execute"
	case 0x19: // COM_STMT_CLOSE
		if len(body) >= 5 {
			stmtID := uint32(body[1]) | uint32(body[2])<<8 | uint32(body[3])<<16 | uint32(body[4])<<24
			return fmt.Sprintf("stmt_id=%d", stmtID)
		}
		return "invalid close"
	case 0x01: // COM_QUIT
		return "quit"
	case 0x0e: // COM_PING
		return "ping"
	default:
		return fmt.Sprintf("cmd=0x%02X", cmd)
	}
}

// formatLog 格式化日志 - 统一格式: [方法名] [命令: 详情] SessionID=xxx, RemoteAddr=xxx 消息内容
func (h *DecoupledMySQLMessageHandler) formatLog(session Session, method, cmdName, cmdDetail, message string) string {
	if session != nil {
		return fmt.Sprintf("[%s] [%s: %s] SessionID=%s, RemoteAddr=%s - %s",
			method, cmdName, cmdDetail, session.Stat(), session.RemoteAddr(), message)
	}
	return fmt.Sprintf("[%s] [%s: %s] %s", method, cmdName, cmdDetail, message)
}

// formatLogSimple 格式化简单日志 - 不包含命令信息
func (h *DecoupledMySQLMessageHandler) formatLogSimple(session Session, method, message string) string {
	if session != nil {
		return fmt.Sprintf("[%s] SessionID=%s, RemoteAddr=%s - %s",
			method, session.Stat(), session.RemoteAddr(), message)
	}
	return fmt.Sprintf("[%s] %s", method, message)
}

// readClientAuthResponse 读取握手响应中的认证数据（支持 CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA 与 CLIENT_SECURE_CONNECTION）。
func readClientAuthResponse(payload []byte, offset int, clientFlags uint32) (auth []byte, next int, err error) {
	if offset > len(payload) {
		return nil, offset, fmt.Errorf("offset past end")
	}
	if clientFlags&common.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
		length, n := readLenEncUintClientAuth(payload[offset:])
		if n <= 0 {
			return nil, offset, fmt.Errorf("invalid length-encoded auth length")
		}
		start := offset + n
		if start+int(length) > len(payload) {
			return nil, offset, fmt.Errorf("auth payload truncated")
		}
		if length == 0 {
			return []byte{}, start + int(length), nil
		}
		return payload[start : start+int(length)], start + int(length), nil
	}
	if clientFlags&common.CLIENT_SECURE_CONNECTION != 0 {
		if offset >= len(payload) {
			return nil, offset, fmt.Errorf("missing secure auth length")
		}
		l := int(payload[offset])
		offset++
		if offset+l > len(payload) {
			return nil, offset, fmt.Errorf("secure auth truncated")
		}
		return payload[offset : offset+l], offset + l, nil
	}
	start := offset
	for offset < len(payload) && payload[offset] != 0 {
		offset++
	}
	if offset > len(payload) {
		return nil, start, fmt.Errorf("unterminated auth string")
	}
	return payload[start:offset], offset + 1, nil
}

func readLenEncUintClientAuth(b []byte) (length uint64, consumed int) {
	if len(b) == 0 {
		return 0, 0
	}
	switch b[0] {
	case 0xfc:
		if len(b) < 3 {
			return 0, 0
		}
		return uint64(b[1]) | uint64(b[2])<<8, 3
	case 0xfd:
		if len(b) < 4 {
			return 0, 0
		}
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4
	case 0xfe:
		if len(b) < 9 {
			return 0, 0
		}
		var v uint64
		for i := 0; i < 8; i++ {
			v |= uint64(b[1+i]) << (8 * i)
		}
		return v, 9
	default:
		if b[0] < 0xfb {
			return uint64(b[0]), 1
		}
		return 0, 0
	}
}
