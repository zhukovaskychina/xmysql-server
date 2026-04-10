package net

import (
	"context"
	"encoding/binary"
	"encoding/hex" // 临时注释 - 密码验证被跳过时不需要
	"fmt"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
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
	authService auth.AuthService

	// ResultSet 编码器（复用实例，避免重复创建）
	resultSetEncoder *protocol.MySQLResultSetEncoder
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
		resultSetEncoder:   protocol.NewMySQLResultSetEncoder(), // 初始化 ResultSet 编码器
	}

	// 注册业务处理器到消息总线
	handler.registerBusinessHandlers()

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

	h.rwlock.Lock()
	h.sessionMap[session] = mysqlSession
	h.rwlock.Unlock()

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

// OnClose 连接关闭事件
func (h *DecoupledMySQLMessageHandler) OnClose(session Session) {
	logger.Debugf("[OnClose] 连接关闭: SessionID=%s, RemoteAddr=%s", session.Stat(), session.RemoteAddr())

	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()

	logger.Debugf("[OnClose] 会话已从映射中移除")

	// 主动关闭会话，配合单测验证关闭状态
	session.Close()
}

// OnError 连接错误事件
func (h *DecoupledMySQLMessageHandler) OnError(session Session, err error) {
	logger.Errorf("[OnError] 会话错误: SessionID=%s, RemoteAddr=%s, Error=%v",
		session.Stat(), session.RemoteAddr(), err)

	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()

	logger.Debugf("[OnError] 会话已从映射中移除")

	// 错误处理时不强制关闭连接，让上层决定
}

// OnCron 定时检查事件
func (h *DecoupledMySQLMessageHandler) OnCron(session Session) {
	// 定时检查会话状态
}

// OnMessage 消息处理事件
func (h *DecoupledMySQLMessageHandler) OnMessage(session Session, pkg interface{}) {
	logger.Debugf(h.formatLogSimple(session, "OnMessage", fmt.Sprintf("收到消息，类型: %T", pkg)))

	recMySQLPkg, ok := pkg.(*MySQLPackage)
	if !ok {
		logger.Errorf(h.formatLogSimple(session, "OnMessage", fmt.Sprintf("无效的包类型: %T", pkg)))
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

	logger.Debugf(h.formatLog(session, "OnMessage", cmdName, cmdDetail,
		fmt.Sprintf("收到MySQL包: 长度=%v, 序号=%d, Body长度=%d",
			recMySQLPkg.Header.PacketLength, recMySQLPkg.Header.PacketId, len(recMySQLPkg.Body))))
	logger.Debugf(h.formatLog(session, "OnMessage", cmdName, cmdDetail,
		fmt.Sprintf("包头信息: PacketLength=%v, PacketId=%d",
			recMySQLPkg.Header.PacketLength, recMySQLPkg.Header.PacketId)))
	logger.Debugf(h.formatLog(session, "OnMessage", cmdName, cmdDetail,
		fmt.Sprintf("包体数据: %v", recMySQLPkg.Body)))

	currentMysqlSession, ok := h.sessionMap[session]
	if !ok {
		logger.Errorf(h.formatLog(session, "OnMessage", cmdName, cmdDetail, "找不到会话"))
		return
	}

	logger.Debugf(h.formatLog(session, "OnMessage", cmdName, cmdDetail, "找到会话，开始处理包"))
	if err := h.handlePacket(session, &currentMysqlSession, recMySQLPkg); err != nil {
		logger.Debugf(h.formatLog(session, "OnMessage", cmdName, cmdDetail, fmt.Sprintf("处理包时出错: %v", err)))
		// 不要在这里直接关闭连接，发送错误响应
		h.sendErrorResponse(session, 1064, "42000", err.Error())
	}

	// 检查是否需要关闭连接（COM_QUIT）
	if shouldClose := session.GetAttribute("should_close"); shouldClose != nil {
		if close, ok := shouldClose.(bool); ok && close {
			logger.Debugf(h.formatLog(session, "OnMessage", "COM_QUIT", "quit", "检测到关闭标记，准备关闭会话"))
			// 清理会话映射
			h.rwlock.Lock()
			delete(h.sessionMap, session)
			h.rwlock.Unlock()

			// 关闭会话
			session.Close()
			logger.Debugf(h.formatLog(session, "OnMessage", "COM_QUIT", "quit", "会话已关闭"))
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

	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "检查认证状态"))
	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("authStatus: %v (类型: %T)", authStatus, authStatus)))
	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("包体长度: %d, 前10字节: %v", len(recMySQLPkg.Body), recMySQLPkg.Body[:localMin(len(recMySQLPkg.Body), 10)])))

	// 处理认证
	if authStatus == nil {
		logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "认证状态为nil，调用handleAuthentication"))
		return h.handleAuthentication(session, currentMysqlSession, recMySQLPkg)
	}

	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("认证状态存在: %v，进入已认证流程", authStatus)))

	// 已认证，解析协议包为消息
	if len(recMySQLPkg.Body) == 0 {
		logger.Errorf(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "包体为空"))
		return fmt.Errorf("empty packet body")
	}

	firstByte := recMySQLPkg.Body[0]

	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("包的第一字节: 0x%02X (%d), 包体长度: %d", firstByte, firstByte, len(recMySQLPkg.Body))))
	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail,
		fmt.Sprintf("包体前20字节: %v", recMySQLPkg.Body[:localMin(len(recMySQLPkg.Body), 20)])))

	// 特殊处理 COM_INIT_DB (0x02)：切换当前数据库并更新 session 状态
	if len(recMySQLPkg.Body) >= 2 && firstByte == 0x02 { // COM_INIT_DB
		dbName := string(recMySQLPkg.Body[1:])
		dbName = strings.TrimSpace(dbName)
		logger.Debugf(h.formatLog(session, "handlePacket", "COM_INIT_DB", dbName, "切换数据库并更新 session"))

		// 可选：校验数据库是否存在（通过业务层或 authService）
		if h.authService != nil {
			if err := h.authService.ValidateDatabase(context.Background(), dbName); err != nil {
				logger.Warnf(h.formatLog(session, "handlePacket", "COM_INIT_DB", dbName, fmt.Sprintf("数据库校验失败(继续): %v", err)))
				// 仍更新 session，由后续 DDL/DML 再报错
			}
		}

		(*currentMysqlSession).SetParamByName("database", dbName)
		logger.Debugf(h.formatLog(session, "handlePacket", "COM_INIT_DB", dbName, "session.currentDB 已更新"))

		okPacket := protocol.EncodeOK(nil, 0, 0, nil)
		return session.WriteBytes(okPacket)
	}

	// 特殊处理查询包（绕过协议解析器）
	if len(recMySQLPkg.Body) >= 2 && firstByte == 0x03 { // COM_QUERY
		query := string(recMySQLPkg.Body[1:])
		logger.Debugf(h.formatLog(session, "handlePacket", "COM_QUERY", query, "检测到查询包，直接处理"))

		// 创建查询消息
		queryMsg := &protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), query),
			SQL:         query,
		}

		logger.Debugf(h.formatLog(session, "handlePacket", "COM_QUERY", query, "查询消息创建成功，调用handleQueryMessageDirect"))

		// 直接处理查询（传入真实 session，保证 USE 等语句能更新 currentDB）
		err := h.handleQueryMessageDirect(session, currentMysqlSession, queryMsg)
		if err != nil {
			logger.Errorf(h.formatLog(session, "handlePacket", "COM_QUERY", query, fmt.Sprintf("查询处理失败: %v", err)))
			return err
		}

		logger.Debugf(h.formatLog(session, "handlePacket", "COM_QUERY", query, "查询处理完成"))
		return nil
	}

	// 预编译语句：走与 COM_QUERY 相同的执行器路径
	if len(recMySQLPkg.Body) >= 1 {
		switch recMySQLPkg.Body[0] {
		case common.COM_STMT_PREPARE:
			return h.handleComStmtPrepare(session, currentMysqlSession, recMySQLPkg)
		case common.COM_STMT_EXECUTE:
			return h.handleComStmtExecute(session, currentMysqlSession, recMySQLPkg)
		case common.COM_STMT_CLOSE:
			return h.handleComStmtClose(session, recMySQLPkg)
		case common.COM_STMT_RESET:
			return h.handleComStmtReset(session, recMySQLPkg)
		}
	}

	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail, "非查询包，使用协议解析器处理"))

	// 使用协议解析器解析包
	message, err := h.protocolParser.ParsePacket(recMySQLPkg.Body, session.Stat())
	if err != nil {
		logger.Errorf(h.formatLog(session, "handlePacket", cmdName, cmdDetail, fmt.Sprintf("协议解析失败: %v", err)))
		return h.sendErrorResponse(session, 1064, "42000", "Protocol parse error")
	}

	logger.Debugf(h.formatLog(session, "handlePacket", cmdName, cmdDetail, fmt.Sprintf("包解析成功，消息类型: %d", message.Type())))

	// 直接处理业务消息（同步处理避免会话关闭问题）
	return h.handleBusinessMessageSync(session, message)
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
func (h *DecoupledMySQLMessageHandler) handleQueryMessageDirect(session Session, currentMysqlSession *server.MySQLServerSession, message protocol.Message) error {
	logger.Debugf("[handleQueryMessageDirect] 开始处理查询消息")

	if session.IsClosed() {
		return fmt.Errorf("session is closed")
	}

	queryMsg, ok := message.(*protocol.QueryMessage)
	if !ok {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid query message")
	}

	query := queryMsg.SQL
	logger.Debugf("[handleQueryMessageDirect] SQL: %s", query)
	session.SetAttribute("__result_sent__", false)

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
	database := ""
	if p := (*currentMysqlSession).GetParamByName("database"); p != nil {
		if s, ok := p.(string); ok {
			database = s
		}
	}
	logger.Debugf("[handleQueryMessageDirect] 当前 session database: %q", database)

	if h.businessHandler == nil {
		return h.sendMySQLOKPacket(session, 0, 0, 1)
	}

	var response protocol.Message
	var err error
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
		return h.sendMySQLOKPacket(session, 0, 0, 1)
	}

	switch resp := response.(type) {
	case *protocol.ResponseMessage:
		if resp.Result != nil {
			typeStr := strings.ToLower(resp.Result.Type)
			if typeStr == "set" || typeStr == "ddl" || (len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0) {
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

// handleAuthentication 处理认证
func (h *DecoupledMySQLMessageHandler) handleAuthentication(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
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
	host := "%" // 暂时使用通配符主机
	if database == "" {
		database = "mysql" // 默认数据库
	}

	// 将authResponse转换为十六进制字符串（模拟客户端发送的密码）
	// 注意：这里需要特殊处理，因为authResponse是加密后的数据
	// 我们需要使用AuthService的ValidatePassword方法
	authResult, err := h.authenticateWithChallenge(ctx, username, authResponse, challenge, host, database)
	if err != nil {
		logger.Errorf("认证失败: %v", err)
		return h.sendErrorResponse(session, 1045, "28000", fmt.Sprintf("Access denied for user '%s'@'%s'", username, host))
	}

	if !authResult.Success {
		logger.Errorf("认证失败: %s", authResult.ErrorMessage)
		return h.sendErrorResponse(session, authResult.ErrorCode, "28000", authResult.ErrorMessage)
	}

	// 设置认证成功状态
	session.SetAttribute("auth_status", "success")
	(*currentMysqlSession).SetParamByName("user", username)
	(*currentMysqlSession).SetParamByName("database", database)

	// 更新会话映射
	h.rwlock.Lock()
	h.sessionMap[session] = *currentMysqlSession
	h.rwlock.Unlock()

	logger.Debugf("认证成功，用户: %s, 数据库: %s", username, database)

	// 发送认证成功响应 (OK包)
	okData := h.createOKPacket(0, 0, 2)

	logger.Debugf("准备发送认证OK包，包长度: %d, 数据: %v", len(okData), okData)

	err = session.WriteBytes(okData)
	if err != nil {
		logger.Errorf("发送认证响应失败: %v", err)
		return err
	}

	logger.Debugf("认证成功响应发送完成")

	// 检查会话状态
	if session.IsClosed() {
		logger.Errorf("警告：认证完成后会话已关闭")
	} else {
		logger.Debugf("认证完成后会话仍然活跃")
	}

	return nil
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
			return &auth.AuthResult{Success: true, User: username, Host: host}, nil
		}
		return denied, nil
	}

	nativeV := &auth.MySQLNativePasswordValidator{}
	sha2V := &auth.CachingSHA2PasswordValidator{}

	if strings.HasPrefix(userInfo.Password, "*") && len(userInfo.Password) == 41 {
		if nativeV.ValidateNativeHandshakeResponse(authResponse, challenge, userInfo.Password) {
			return &auth.AuthResult{Success: true, User: username, Host: host}, nil
		}
		return denied, nil
	}

	if len(authResponse) == 32 {
		if sha2V.ValidateCachingSHA2FastAuth(authResponse, challenge, userInfo.Password) {
			return &auth.AuthResult{Success: true, User: username, Host: host}, nil
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

func (h *DecoupledMySQLMessageHandler) handleComStmtPrepare(session Session, _ *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 2 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_PREPARE")
	}
	sqlText := string(recMySQLPkg.Body[1:])
	mgr := h.preparedStmtMgrFromSession(session)
	stmt, err := mgr.Prepare(sqlText)
	if err != nil {
		return h.sendErrorResponse(session, 1064, "42000", err.Error())
	}
	seq := recMySQLPkg.Header.PacketId + 1
	for _, pkt := range protocol.EncodePrepareResponse(stmt, seq) {
		if werr := session.WriteBytes(pkt); werr != nil {
			return werr
		}
	}
	return nil
}

func (h *DecoupledMySQLMessageHandler) handleComStmtExecute(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	body := recMySQLPkg.Body
	if len(body) < 10 {
		return h.sendErrorResponse(session, 1064, "42000", "Invalid COM_STMT_EXECUTE")
	}
	stmtID := binary.LittleEndian.Uint32(body[1:5])
	mgr := h.preparedStmtMgrFromSession(session)
	stmt, err := mgr.Get(stmtID)
	if err != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
	}
	params, typeBlock, perr := protocol.ParseBinaryStmtExecuteParams(body[10:], stmt.ParamCount, stmt.LastParamTypes)
	if perr != nil {
		return h.sendErrorResponse(session, 1210, "HY000", perr.Error())
	}
	stmt.LastParamTypes = typeBlock
	boundSQL := protocol.BindPreparedSQL(stmt.SQL, params)
	queryMsg := &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), boundSQL),
		SQL:         boundSQL,
	}
	return h.handleQueryMessageDirect(session, currentMysqlSession, queryMsg)
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
	stmt, err := mgr.Peek(stmtID)
	if err != nil {
		return h.sendErrorResponse(session, common.ErrUnknownStmtHandler, "HY000", err.Error())
	}
	stmt.LastParamTypes = nil
	okData := h.createOKPacket(0, 0, recMySQLPkg.Header.PacketId+1)
	return session.WriteBytes(okData)
}

// sendQueryResultSet 发送查询结果集
// 严格按照 MySQL 协议规范实现，兼容 MySQL Connector/J 5.1.x
func (h *DecoupledMySQLMessageHandler) sendQueryResultSet(session Session, result *protocol.MessageQueryResult, seqID byte) error {
	logger.Debugf("[sendQueryResultSet] 开始发送查询结果集（MySQL 协议标准实现）")

	// ✅ 修复：防止重复发送 ResultSet
	if resultSent := session.GetAttribute("__result_sent__"); resultSent != nil {
		if sent, ok := resultSent.(bool); ok && sent {
			logger.Errorf("❌ 协议错误：尝试重复发送 ResultSet，已忽略")
			return fmt.Errorf("result already sent")
		}
	}

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

	// 检查客户端能力标志，确定是否需要使用 OK 包替代 EOF 包（CLIENT_DEPRECATE_EOF）
	useDeprecatedEOF := true
	if capsVal := session.GetAttribute("client_capabilities"); capsVal != nil {
		if caps, ok := capsVal.(uint32); ok {
			if (caps & common.CLIENT_DEPRECATE_EOF) != 0 {
				useDeprecatedEOF = false
			}
		}
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
	for colIdx, colName := range result.Columns {
		var colDef *protocol.ColumnDefinition

		// 从第一行数据推断列类型
		if len(result.Rows) > 0 && colIdx < len(result.Rows[0]) {
			colDef = encoder.CreateColumnDefinitionFromValue(colName, result.Rows[0][colIdx])
		} else {
			// 没有数据行，默认为 VARCHAR
			colDef = encoder.CreateColumnDefinition(colName, protocol.MYSQL_TYPE_VAR_STRING, 0)
		}

		columnDefPacket := encoder.EncodeColumnDefinitionPacket(colDef, seqID)

		logger.Debugf("[sendQueryResultSet] 发送列定义: name=%s, type=0x%02X, length=%d",
			colName, colDef.ColumnType, colDef.ColumnLength)

		err := session.WriteBytes(columnDefPacket)
		if err != nil {
			logger.Errorf("发送列定义包失败: %v", err)
			return err
		}
		seqID++
	}

	// ========================================================================
	// Step 3: 发送列定义结束标记（EOF 或 OK，取决于 CLIENT_DEPRECATE_EOF）
	// ========================================================================
	if useDeprecatedEOF {
		eofPacket1 := protocol.EncodeEOFPacketWithSeq(0, protocol.SERVER_STATUS_AUTOCOMMIT, seqID)

		logger.Debugf("[sendQueryResultSet] 发送第一个 EOF 包（列定义结束）")
		err = session.WriteBytes(eofPacket1)
		if err != nil {
			logger.Errorf("发送第一个 EOF 包失败: %v", err)
			return err
		}
		seqID++
	} else {
		// 如果启用了 CLIENT_DEPRECATE_EOF，则列定义后不发送任何包（既不是 EOF 也不是 OK）
		// 直接进入 Row Data 阶段
		logger.Debugf("[sendQueryResultSet] 跳过第一个 EOF 包（CLIENT_DEPRECATE_EOF 已启用）")
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
	// ========================================================================
	if useDeprecatedEOF {
		eofPacket2 := protocol.EncodeEOFPacketWithSeq(0, protocol.SERVER_STATUS_AUTOCOMMIT, seqID)

		logger.Debugf("[sendQueryResultSet] 发送第二个 EOF 包（行数据结束）")
		err = session.WriteBytes(eofPacket2)
		if err != nil {
			logger.Errorf("发送第二个 EOF 包失败: %v", err)
			return err
		}
	} else {
		// CLIENT_DEPRECATE_EOF 启用时，使用 OK 包替代 EOF 包结束结果集
		// 注意：此处的 OK 包必须以 0xFE 开头（而不是 0x00），以区别于行数据包

		// 手动构建 0xFE 开头的 OK 包
		payload := []byte{0xFE} // OK marker (EOF style)

		// affected_rows (lenenc-int) -> 0
		payload = h.appendLengthEncodedInt(payload, 0)

		// last_insert_id (lenenc-int) -> 0
		payload = h.appendLengthEncodedInt(payload, 0)

		// status_flags (2 bytes, little-endian)
		statusFlags := protocol.SERVER_STATUS_AUTOCOMMIT
		payload = append(payload, byte(statusFlags), byte(statusFlags>>8))

		// warnings (2 bytes, little-endian)
		warnings := uint16(0)
		payload = append(payload, byte(warnings), byte(warnings>>8))

		okPacket2 := h.addPacketHeader(payload, seqID)

		logger.Debugf("[sendQueryResultSet] 发送结果集结束 OK 包（CLIENT_DEPRECATE_EOF, Header=0xFE）")
		err = session.WriteBytes(okPacket2)
		if err != nil {
			logger.Errorf("发送结果集结束 OK 包失败: %v", err)
			return err
		}
	}

	logger.Debugf("[sendQueryResultSet] ✅ 查询结果集发送完成: %d 列, %d 行", len(result.Columns), len(result.Rows))

	// ✅ 修复：标记 ResultSet 已发送
	session.SetAttribute("__result_sent__", true)

	return nil
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
