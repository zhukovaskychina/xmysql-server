package net

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

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
	handshakeGenerator *HandshakeGenerator
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
	handler := &DecoupledMySQLMessageHandler{
		sessionMap:         make(map[Session]server.MySQLServerSession),
		cfg:                cfg,
		protocolParser:     protocol.NewMySQLProtocolParser(),
		protocolEncoder:    protocol.NewMySQLProtocolEncoder(),
		messageBus:         protocol.NewDefaultMessageBus(),
		businessHandler:    dispatcher.NewEnhancedBusinessMessageHandler(cfg, xmysqlEngine),
		handshakeGenerator: NewHandshakeGenerator(),
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

	// 发送握手包
	handshakeData := handshakePacket.Encode()
	if err := session.WriteBytes(handshakeData); err != nil {
		logger.Errorf("发送握手包失败: %v", err)
		return err
	}

	logger.Debugf("握手包发送成功")
	return nil
}

// OnClose 连接关闭事件
func (h *DecoupledMySQLMessageHandler) OnClose(session Session) {
	logger.Debugf("连接关闭: %s", session.Stat())

	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()

	// 不要在这里调用 session.Close()，会导致重复关闭
}

// OnError 连接错误事件
func (h *DecoupledMySQLMessageHandler) OnError(session Session, err error) {
	logger.Errorf("会话错误: %v", err)

	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()

	// 错误处理时不强制关闭连接，让上层决定
}

// OnCron 定时检查事件
func (h *DecoupledMySQLMessageHandler) OnCron(session Session) {
	// 定时检查会话状态
}

// OnMessage 消息处理事件
func (h *DecoupledMySQLMessageHandler) OnMessage(session Session, pkg interface{}) {
	logger.Debugf("[OnMessage] 收到消息，类型: %T", pkg)

	recMySQLPkg, ok := pkg.(*MySQLPackage)
	if !ok {
		logger.Errorf("无效的包类型: %T", pkg)
		return
	}

	logger.Debugf("收到MySQL包: 长度=%v, 序号=%d, Body长度=%d",
		recMySQLPkg.Header.PacketLength, recMySQLPkg.Header.PacketId, len(recMySQLPkg.Body))
	logger.Debugf("包头信息: PacketLength=%v, PacketId=%d",
		recMySQLPkg.Header.PacketLength, recMySQLPkg.Header.PacketId)
	logger.Debugf("包体数据: %v", recMySQLPkg.Body)

	currentMysqlSession, ok := h.sessionMap[session]
	if !ok {
		logger.Errorf("找不到会话: %v", session)
		return
	}

	logger.Debugf("找到会话，开始处理包")
	if err := h.handlePacket(session, &currentMysqlSession, recMySQLPkg); err != nil {
		logger.Debugf("处理包时出错: %v", err)
		// 不要在这里直接关闭连接，发送错误响应
		h.sendErrorResponse(session, 1064, "42000", err.Error())
	}
}

// handlePacket 处理MySQL包
func (h *DecoupledMySQLMessageHandler) handlePacket(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	authStatus := session.GetAttribute("auth_status")

	logger.Debugf("[handlePacket] 检查认证状态\n")
	logger.Debugf("[handlePacket] authStatus: %v (类型: %T)\n", authStatus, authStatus)
	logger.Debugf("[handlePacket] authStatus == nil: %v\n", authStatus == nil)
	logger.Debugf("[handlePacket] 包体长度: %d\n", len(recMySQLPkg.Body))
	logger.Debugf("[handlePacket] 包体前10字节: %v\n", recMySQLPkg.Body[:min(len(recMySQLPkg.Body), 10)])

	// 处理认证
	if authStatus == nil {
		logger.Debugf("[handlePacket] 认证状态为nil，调用handleAuthentication\n")
		logger.Debugf("处理认证包")
		return h.handleAuthentication(session, currentMysqlSession, recMySQLPkg)
	}

	logger.Debugf("[handlePacket] 认证状态存在: %v，进入已认证流程\n", authStatus)

	// 已认证，解析协议包为消息
	if len(recMySQLPkg.Body) == 0 {
		logger.Debugf("[handlePacket] 包体为空\n")
		logger.Errorf("包体为空")
		return fmt.Errorf("empty packet body")
	}

	firstByte := recMySQLPkg.Body[0]
	logger.Debugf("[handlePacket] 包的第一字节: 0x%02X (%d)\n", firstByte, firstByte)

	logger.Debugf("解析已认证的包，包类型: 0x%02X (%d), 包体长度: %d",
		firstByte, firstByte, len(recMySQLPkg.Body))
	logger.Debugf("包体内容: %v", recMySQLPkg.Body)

	logger.Debugf("[handlePacket] 尝试使用协议解析器解析包，第一字节: 0x%02X\n", firstByte)
	logger.Debugf("[handlePacket] 包体前20字节: %v\n", recMySQLPkg.Body[:min(len(recMySQLPkg.Body), 20)])

	//  特殊处理查询包（绕过协议解析器）
	if len(recMySQLPkg.Body) >= 2 && firstByte == 0x03 { // COM_QUERY
		logger.Debugf("[handlePacket] 检测到查询包 (COM_QUERY)，直接处理\n")
		query := string(recMySQLPkg.Body[1:])
		logger.Debugf("[handlePacket] 查询内容: '%s'\n", query)

		// 创建查询消息
		queryMsg := &protocol.QueryMessage{
			BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, session.Stat(), query),
			SQL:         query,
		}

		logger.Debugf("[handlePacket] 查询消息创建成功，调用handleQueryMessageDirect\n")

		// 直接处理查询
		err := h.handleQueryMessageDirect(session, queryMsg)
		if err != nil {
			logger.Debugf("[handlePacket] 查询处理失败: %v\n", err)
			return err
		}

		logger.Debugf("[handlePacket] 查询处理完成\n")
		return nil
	}

	logger.Debugf("[handlePacket] 非查询包，使用协议解析器处理\n")

	// 使用协议解析器解析包
	message, err := h.protocolParser.ParsePacket(recMySQLPkg.Body, session.Stat())
	if err != nil {
		logger.Debugf("[handlePacket] 协议解析失败: %v\n", err)
		logger.Errorf("协议解析失败: %v", err)
		logger.Errorf("包体数据: %v", recMySQLPkg.Body)
		return h.sendErrorResponse(session, 1064, "42000", "Protocol parse error")
	}

	logger.Debugf("包解析成功，消息类型: %d", message.Type())
	logger.Debugf("[handlePacket] 包解析成功，消息类型: %d\n", message.Type())

	// 直接处理业务消息（同步处理避免会话关闭问题）
	return h.handleBusinessMessageSync(session, message)
}

// handleAuthentication 处理认证
func (h *DecoupledMySQLMessageHandler) handleAuthentication(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	logger.Debugf("处理认证包，包长度: %d, 包序号: %d, Body长度: %d",
		len(recMySQLPkg.Header.PacketLength), recMySQLPkg.Header.PacketId, len(recMySQLPkg.Body))

	logger.Debugf("认证包Body内容(前64字节): %v", recMySQLPkg.Body[:min(len(recMySQLPkg.Body), 64)])

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

	// 跳过最大包大小 (4字节)
	if offset+4 > len(payload) {
		logger.Errorf("无法跳过最大包大小，需要%d字节，只有%d字节", offset+4, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth packet format")
	}
	offset += 4

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

	// 读取认证响应长度
	if offset >= len(payload) {
		logger.Errorf("无法读取认证响应长度，当前偏移: %d, 总长度: %d", offset, len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Missing auth response length")
	}
	authResponseLen := payload[offset]
	offset++

	logger.Debugf("认证响应长度: %d, 当前偏移: %d", authResponseLen, offset)

	// 读取认证响应数据
	if offset+int(authResponseLen) > len(payload) {
		logger.Errorf("认证响应数据长度不足，需要%d字节(offset=%d + len=%d)，只有%d字节",
			offset+int(authResponseLen), offset, int(authResponseLen), len(payload))
		return h.sendErrorResponse(session, 1045, "28000", "Invalid auth response length")
	}
	authResponse := payload[offset : offset+int(authResponseLen)]
	offset += int(authResponseLen)

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

	// 简化认证处理 - 直接认证成功（暂时跳过密码验证）
	if username == "" {
		logger.Errorf("用户名为空")
		return h.sendErrorResponse(session, 1045, "28000", "Access denied for empty user")
	}

	// 设置认证成功状态
	session.SetAttribute("auth_status", "success")
	(*currentMysqlSession).SetParamByName("user", username)
	if database != "" {
		(*currentMysqlSession).SetParamByName("database", database)
	} else {
		(*currentMysqlSession).SetParamByName("database", "mysql")
	}

	// 更新会话映射
	h.rwlock.Lock()
	h.sessionMap[session] = *currentMysqlSession
	h.rwlock.Unlock()

	logger.Debugf("认证成功，用户: %s, 数据库: %s", username, database)

	// 发送认证成功响应 (OK包)
	okData := h.createOKPacket(0, 0, 2)

	logger.Debugf("准备发送认证OK包，包长度: %d, 数据: %v", len(okData), okData)

	err := session.WriteBytes(okData)
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

// createOKPacket 创建OK包
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

// appendLengthEncodedInt 追加长度编码整数
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
	} else {
		data = append(data, 0xFE)
		for i := 0; i < 8; i++ {
			data = append(data, byte(value>>(i*8)))
		}
		return data
	}
}

// addPacketHeader 添加MySQL包头
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

// handleBusinessMessageSync 同步处理业务消息
func (h *DecoupledMySQLMessageHandler) handleBusinessMessageSync(session Session, message protocol.Message) error {
	logger.Debugf("同步处理业务消息，类型: %d", message.Type())

	// 对于查询消息，使用我们的直接处理流程
	if message.Type() == protocol.MSG_QUERY_REQUEST {
		logger.Debugf("检测到查询消息，使用直接处理")
		return h.handleQueryMessageDirect(session, message)
	}

	// 对于其他消息，尝试使用业务处理器
	if h.businessHandler != nil {
		response, err := h.businessHandler.HandleMessage(message)
		if err != nil {
			logger.Errorf("业务处理器处理失败: %v", err)
			return h.sendMySQLErrorPacket(session, 1064, "42000", err.Error(), 1)
		}

		if response != nil {
			logger.Debugf("业务处理器返回响应，类型: %d", response.Type())
			// 检查响应类型并发送适当的结果
			switch resp := response.(type) {
			case *protocol.ResponseMessage:
				// 查询结果响应
				if resp.Result != nil {
					//  检查是否有列和行数据，如果没有则发送OK包
					if len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0 {
						logger.Debugf("查询结果无列和行数据，发送OK包: %s", resp.Result.Message)
						return h.sendMySQLOKPacket(session, 0, 0, 1)
					}
					return h.sendQueryResultSet(session, resp.Result, 1)
				}
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			case *protocol.ErrorMessage:
				// 错误响应
				return h.sendMySQLErrorPacket(session, resp.Code, resp.State, resp.Message, 1)
			default:
				// 默认成功响应
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			}
		}
	}

	// 默认返回OK响应
	logger.Debugf("未知消息类型或无业务处理器，返回默认OK响应")
	return h.sendMySQLOKPacket(session, 0, 0, 1)
}

// handleQueryMessageDirect 直接处理查询消息
func (h *DecoupledMySQLMessageHandler) handleQueryMessageDirect(session Session, message protocol.Message) error {
	logger.Debugf("[handleQueryMessageDirect] 开始处理查询消息")
	logger.Debugf("[Session状态] ID: %d, IsClosed: %v", session.ID(), session.IsClosed())

	// 检查会话状态
	if session.IsClosed() {
		logger.Errorf("会话已关闭，无法处理查询")
		return fmt.Errorf("session is closed")
	}

	// 获取查询内容
	queryMsg, ok := message.(*protocol.QueryMessage)
	if !ok {
		logger.Errorf("消息类型转换失败，期望 *protocol.QueryMessage，实际类型: %T", message)
		return h.sendMySQLErrorPacket(session, 1064, "42000", "Invalid query message type", 1)
	}

	query := queryMsg.SQL
	logger.Debugf("[Query内容] SQL: '%s', 长度: %d", query, len(query))

	// 在响应前再次检查会话状态
	defer func() {
		if r := recover(); r != nil {
			logger.Debugf("发生panic: %v\n", r)
		}
		logger.Debugf("查询处理完成\n")
	}()

	// 在发送响应前最后一次检查会话状态
	if session.IsClosed() {
		logger.Errorf("会话在查询处理过程中被关闭")
		return fmt.Errorf("session closed during query processing")
	}

	// 获取真实的MySQL会话对象
	h.rwlock.RLock()
	realMySQLSession, exists := h.sessionMap[session]
	h.rwlock.RUnlock()

	if !exists {
		logger.Errorf("找不到对应的MySQL会话对象")
		return h.sendMySQLErrorPacket(session, 1064, "42000", "Session not found", 1)
	}

	// 从会话中获取数据库名称
	var database string
	if dbParam := realMySQLSession.GetParamByName("database"); dbParam != nil {
		if db, ok := dbParam.(string); ok {
			database = db
		}
	}

	logger.Debugf("使用数据库: %s", database)

	//  使用EnhancedBusinessMessageHandler的HandleQueryWithRealSession方法
	if enhancedHandler, ok := h.businessHandler.(*dispatcher.EnhancedBusinessMessageHandler); ok {
		logger.Debugf("使用增强业务处理器处理查询")
		response, err := enhancedHandler.HandleQueryWithRealSession(realMySQLSession, query, database)
		if err != nil {
			logger.Errorf("增强业务处理器处理失败: %v", err)
			return h.sendMySQLErrorPacket(session, 1064, "42000", err.Error(), 1)
		}

		if response != nil {
			logger.Debugf("增强业务处理器返回响应，类型: %d", response.Type())

			// 检查响应类型并发送适当的结果
			switch resp := response.(type) {
			case *protocol.ResponseMessage:
				// 查询结果响应
				if resp.Result != nil {
					return h.sendQueryResultSet(session, resp.Result, 1)
				}
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			case *protocol.ErrorMessage:
				// 错误响应
				return h.sendMySQLErrorPacket(session, resp.Code, resp.State, resp.Message, 1)
			default:
				// 默认成功响应
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			}
		}
	}

	// 如果不是增强业务处理器，使用原来的逻辑
	logger.Debugf("使用原始SQL解析逻辑")

	//  使用sqlparser安全地解析SQL语句
	stmt, err := h.parseAndClassifySQL(query)
	if err != nil {
		logger.Errorf("SQL解析失败: %v", err)
		// 对于解析失败的SQL，尝试特殊处理
		return h.handleSpecialQueries(session, query, message)
	}

	logger.Debugf("SQL解析成功，语句类型: %T", stmt)

	// 根据解析后的语句类型进行处理
	return h.handleParsedStatement(session, stmt, message)
}

// parseAndClassifySQL 使用sqlparser安全地解析SQL语句
func (h *DecoupledMySQLMessageHandler) parseAndClassifySQL(query string) (interface{}, error) {
	// 导入sqlparser包
	// 注意：这里需要导入正确的sqlparser包路径
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %v", err)
	}
	return stmt, nil
}

// handleParsedStatement 处理已解析的SQL语句
func (h *DecoupledMySQLMessageHandler) handleParsedStatement(session Session, stmt interface{}, message protocol.Message) error {
	switch parsedStmt := stmt.(type) {
	case *sqlparser.Select:
		logger.Debugf("处理SELECT语句")
		return h.handleSelectStatement(session, parsedStmt, message)

	case *sqlparser.Insert:
		logger.Debugf("处理INSERT语句")
		return h.handleDMLStatement(session, message)

	case *sqlparser.Update:
		logger.Debugf("处理UPDATE语句")
		return h.handleDMLStatement(session, message)

	case *sqlparser.Delete:
		logger.Debugf("处理DELETE语句")
		return h.handleDMLStatement(session, message)

	case *sqlparser.DDL:
		logger.Debugf("处理DDL语句: %s", parsedStmt.Action)
		return h.handleDDLStatement(session, parsedStmt, message)

	case *sqlparser.DBDDL:
		logger.Debugf("处理数据库DDL语句: %s", parsedStmt.Action)
		return h.handleDBDDLStatement(session, parsedStmt, message)

	case *sqlparser.Set:
		logger.Debugf("处理SET语句")
		return h.handleSetStatement(session, parsedStmt, message)

	case *sqlparser.Show:
		logger.Debugf("处理SHOW语句: %s", parsedStmt.Type)
		return h.handleShowStatement(session, parsedStmt, message)

	case *sqlparser.Use:
		logger.Debugf("处理USE语句")
		return h.handleUseStatement(session, parsedStmt, message)

	case *sqlparser.Begin:
		logger.Debugf("处理BEGIN语句")
		return h.handleTransactionStatement(session, message)

	case *sqlparser.Commit:
		logger.Debugf("处理COMMIT语句")
		return h.handleTransactionStatement(session, message)

	case *sqlparser.Rollback:
		logger.Debugf("处理ROLLBACK语句")
		return h.handleTransactionStatement(session, message)

	default:
		logger.Debugf("未知语句类型: %T，使用业务处理器", parsedStmt)
		return h.handleUnknownStatement(session, message)
	}
}

// handleSelectStatement 处理SELECT语句
func (h *DecoupledMySQLMessageHandler) handleSelectStatement(session Session, stmt *sqlparser.Select, message protocol.Message) error {
	// 检查是否为简单的SELECT 1查询
	if h.isSelectOne(stmt) {
		logger.Debugf("检测到SELECT 1查询")
		return h.sendMySQLSelectOneResult(session, 1)
	}

	// 其他SELECT查询使用业务处理器
	return h.handleBusinessQuery(session, message)
}

// handleDMLStatement 处理DML语句（INSERT/UPDATE/DELETE）
func (h *DecoupledMySQLMessageHandler) handleDMLStatement(session Session, message protocol.Message) error {
	logger.Debugf("处理DML语句，使用业务处理器")
	return h.handleBusinessQuery(session, message)
}

// handleDDLStatement 处理DDL语句（CREATE TABLE/DROP TABLE等）
func (h *DecoupledMySQLMessageHandler) handleDDLStatement(session Session, stmt *sqlparser.DDL, message protocol.Message) error {
	logger.Debugf("处理DDL语句: %s，使用业务处理器", stmt.Action)
	return h.handleBusinessQuery(session, message)
}

// handleDBDDLStatement 处理数据库DDL语句（CREATE DATABASE/DROP DATABASE）
func (h *DecoupledMySQLMessageHandler) handleDBDDLStatement(session Session, stmt *sqlparser.DBDDL, message protocol.Message) error {
	logger.Debugf("处理数据库DDL语句: %s，使用业务处理器", stmt.Action)
	return h.handleBusinessQuery(session, message)
}

// handleSetStatement 处理SET语句
func (h *DecoupledMySQLMessageHandler) handleSetStatement(session Session, stmt *sqlparser.Set, message protocol.Message) error {
	logger.Debugf("处理SET语句，使用业务处理器")
	return h.handleBusinessQuery(session, message)
}

// handleShowStatement 处理SHOW语句
func (h *DecoupledMySQLMessageHandler) handleShowStatement(session Session, stmt *sqlparser.Show, message protocol.Message) error {
	switch strings.ToLower(stmt.Type) {
	case "databases":
		logger.Debugf("处理SHOW DATABASES查询")
		return h.sendMySQLShowDatabasesResult(session, 1)
	default:
		logger.Debugf("处理其他SHOW语句，使用业务处理器")
		return h.handleBusinessQuery(session, message)
	}
}

// handleUseStatement 处理USE语句
func (h *DecoupledMySQLMessageHandler) handleUseStatement(session Session, stmt *sqlparser.Use, message protocol.Message) error {
	logger.Debugf("处理USE语句，使用业务处理器")
	return h.handleBusinessQuery(session, message)
}

// handleTransactionStatement 处理事务语句
func (h *DecoupledMySQLMessageHandler) handleTransactionStatement(session Session, message protocol.Message) error {
	logger.Debugf("处理事务语句，返回简单OK")
	return h.sendSimpleOK(session, 1)
}

// handleUnknownStatement 处理未知语句类型
func (h *DecoupledMySQLMessageHandler) handleUnknownStatement(session Session, message protocol.Message) error {
	logger.Debugf("处理未知语句，使用业务处理器")
	return h.handleBusinessQuery(session, message)
}

// handleSpecialQueries 处理特殊查询（解析失败的情况）
func (h *DecoupledMySQLMessageHandler) handleSpecialQueries(session Session, query string, message protocol.Message) error {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	logger.Debugf("处理特殊查询: %s", queryUpper)

	// 检查是否为系统变量查询
	if strings.Contains(queryUpper, "@@") ||
		strings.Contains(queryUpper, "SELECT USER()") ||
		strings.Contains(queryUpper, "SELECT DATABASE()") ||
		strings.Contains(queryUpper, "SELECT VERSION()") ||
		strings.Contains(queryUpper, "INFORMATION_SCHEMA") {
		logger.Debugf("检测到系统变量或信息模式查询，使用业务处理器")
		return h.handleBusinessQuery(session, message)
	}

	// 检查是否为mysql.user查询
	if strings.Contains(queryUpper, "MYSQL.USER") {
		logger.Debugf("处理mysql.user查询")
		return h.sendMySQLUserTableResult(session, 1)
	}

	// 其他特殊查询使用业务处理器
	logger.Debugf("其他特殊查询，使用业务处理器")
	return h.handleBusinessQuery(session, message)
}

// handleBusinessQuery 使用业务处理器处理查询
func (h *DecoupledMySQLMessageHandler) handleBusinessQuery(session Session, message protocol.Message) error {
	if h.businessHandler != nil {
		response, err := h.businessHandler.HandleMessage(message)
		if err != nil {
			logger.Errorf("业务处理器处理失败: %v", err)
			return h.sendMySQLErrorPacket(session, 1064, "42000", err.Error(), 1)
		}

		if response != nil {
			logger.Debugf("业务处理器返回响应，类型: %d", response.Type())

			// 检查响应类型并发送适当的结果
			switch resp := response.(type) {
			case *protocol.ResponseMessage:
				// 查询结果响应
				if resp.Result != nil {
					//  检查是否有列和行数据，如果没有则发送OK包
					if len(resp.Result.Columns) == 0 && len(resp.Result.Rows) == 0 {
						logger.Debugf("查询结果无列和行数据，发送OK包: %s", resp.Result.Message)
						return h.sendMySQLOKPacket(session, 0, 0, 1)
					}
					return h.sendQueryResultSet(session, resp.Result, 1)
				}
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			case *protocol.ErrorMessage:
				// 错误响应
				return h.sendMySQLErrorPacket(session, resp.Code, resp.State, resp.Message, 1)
			default:
				// 默认成功响应
				return h.sendMySQLOKPacket(session, 0, 0, 1)
			}
		}
	}

	logger.Debugf("业务处理器未处理查询，返回默认OK")
	return h.sendSimpleOK(session, 1)
}

// isSelectOne 检查是否为SELECT 1查询
func (h *DecoupledMySQLMessageHandler) isSelectOne(stmt *sqlparser.Select) bool {
	if len(stmt.SelectExprs) != 1 {
		return false
	}

	if aliasedExpr, ok := stmt.SelectExprs[0].(*sqlparser.AliasedExpr); ok {
		if sqlVal, ok := aliasedExpr.Expr.(*sqlparser.SQLVal); ok {
			return string(sqlVal.Val) == "1"
		}
	}

	return false
}

// sendSimpleOK 发送最简单的OK包
func (h *DecoupledMySQLMessageHandler) sendSimpleOK(session Session, seqId byte) error {
	logger.Debugf("发送简单OK包，序号: %d", seqId)

	// 发送前检查会话状态
	if session.IsClosed() {
		logger.Errorf("会话已关闭，无法发送OK包")
		return fmt.Errorf("session is closed")
	}

	// 最简单的OK包：标记字节 + 受影响行数 + 插入ID + 状态 + 警告
	okPayload := []byte{
		0x00,       // OK标记
		0x00,       // 受影响行数（0）
		0x00,       // 最后插入ID（0）
		0x02, 0x00, // 状态标志（SERVER_STATUS_AUTOCOMMIT = 0x0002）
		0x00, 0x00, // 警告数量（2字节）
	}

	// 创建完整的MySQL包
	packetLength := len(okPayload)
	packet := make([]byte, 4+packetLength)

	// 包头：长度（3字节) + 序号（1字节）
	packet[0] = byte(packetLength & 0xff)
	packet[1] = byte((packetLength >> 8) & 0xff)
	packet[2] = byte((packetLength >> 16) & 0xff)
	packet[3] = seqId

	// 复制负载
	copy(packet[4:], okPayload)

	logger.Debugf("OK包内容: %v", packet)

	// 发送前最后检查
	if session.IsClosed() {
		logger.Errorf("会话在准备发送时被关闭")
		return fmt.Errorf("session closed before sending")
	}

	// 使用WriteBytes直接发送字节数组
	err := session.WriteBytes(packet)
	if err != nil {
		logger.Errorf("发送OK包失败: %v", err)
		return fmt.Errorf("failed to send OK packet: %v", err)
	}

	logger.Debugf("OK包发送成功，写入 %d 字节", len(packet))
	return nil
}

// sendMySQLSelectOneResult 发送SELECT 1的结果集响应
func (h *DecoupledMySQLMessageHandler) sendMySQLSelectOneResult(session Session, seqID byte) error {
	logger.Debugf("[sendMySQLSelectOneResult] 开始发送SELECT 1结果\n")

	// 检查连接状态
	if session.IsClosed() {
		logger.Debugf("[sendMySQLSelectOneResult] 会话已关闭\n")
		return fmt.Errorf("session is closed")
	}

	defer func() {
		if r := recover(); r != nil {
			logger.Debugf("发生panic: %v\n", r)
		}
	}()

	// 1. 发送列数包 (1列)
	columnCountPacket := []byte{
		0x01, 0x00, 0x00, seqID, // 包头: 长度=1, 序号=seqID
		0x01, // 列数 = 1
	}

	logger.Debugf("发送列数包: %v\n", columnCountPacket)
	err := session.WriteBytes(columnCountPacket)
	if err != nil {
		logger.Debugf("发送失败: %v\n", err)
		return err
	}
	seqID++

	// 2. 发送列定义包
	columnDefPacket := []byte{
		0x17, 0x00, 0x00, seqID, // 包头: 长度=23, 序号=seqID+1
		0x03, 'd', 'e', 'f', // catalog = "def"
		0x00,      // schema = ""
		0x00,      // table = ""
		0x00,      // org_table = ""
		0x01, '1', // name = "1"
		0x01, '1', // org_name = "1"
		0x0c,       // 固定长度标记
		0x3f, 0x00, // charset = 63 (binary)
		0x01, 0x00, 0x00, 0x00, // length = 1
		0x08,       // type = MYSQL_TYPE_LONGLONG
		0x81, 0x00, // flags = 0x81 (BINARY + NOT_NULL)
		0x00,       // decimals = 0
		0x00, 0x00, // 保留字段
	}

	logger.Debugf("发送列定义包: %v\n", columnDefPacket)
	err = session.WriteBytes(columnDefPacket)
	if err != nil {
		logger.Debugf("发送失败: %v\n", err)
		return err
	}
	seqID++

	// 3. 发送EOF包 (结束列定义)
	eofPacket1 := []byte{
		0x05, 0x00, 0x00, seqID, // 包头: 长度=5, 序号=seqID+2
		0xfe,       // EOF标记
		0x00, 0x00, // warnings = 0
		0x02, 0x00, // status flags = 0x0002 (SERVER_STATUS_AUTOCOMMIT)
	}

	logger.Debugf("发送第一个EOF包: %v\n", eofPacket1)
	err = session.WriteBytes(eofPacket1)
	if err != nil {
		logger.Debugf("发送失败: %v\n", err)
		return err
	}
	seqID++

	// 4. 发送行数据包
	rowDataPacket := []byte{
		0x02, 0x00, 0x00, seqID, // 包头: 长度=2, 序号=seqID+3
		0x01, '1', // 数据 = "1"
	}

	logger.Debugf("发送行数据包: %v\n", rowDataPacket)
	err = session.WriteBytes(rowDataPacket)
	if err != nil {
		logger.Debugf("发送失败: %v\n", err)
		return err
	}
	seqID++

	// 5. 发送最终EOF包 (结束结果集)
	eofPacket2 := []byte{
		0x05, 0x00, 0x00, seqID, // 包头: 长度=5, 序号=seqID+4
		0xfe,       // EOF标记
		0x00, 0x00, // warnings = 0
		0x02, 0x00, // status flags = 0x0002 (SERVER_STATUS_AUTOCOMMIT)
	}

	logger.Debugf("发送最终EOF包: %v\n", eofPacket2)
	err = session.WriteBytes(eofPacket2)
	if err != nil {
		logger.Debugf("发送失败: %v\n", err)
		return err
	}

	logger.Debugf("SELECT 1结果发送完成\n")
	return nil
}

// sendMySQLSelectUserResult 发送SELECT USER()的结果
func (h *DecoupledMySQLMessageHandler) sendMySQLSelectUserResult(session Session, seqID byte) error {
	// 简化版本，返回OK包
	return h.sendSimpleOK(session, seqID)
}

// sendMySQLShowDatabasesResult 发送SHOW DATABASES的结果
func (h *DecoupledMySQLMessageHandler) sendMySQLShowDatabasesResult(session Session, seqID byte) error {
	// 简化版本，返回OK包
	return h.sendSimpleOK(session, seqID)
}

// sendMySQLUserTableResult 发送mysql.user表查询的结果
func (h *DecoupledMySQLMessageHandler) sendMySQLUserTableResult(session Session, seqID byte) error {
	// 简化版本，返回OK包
	return h.sendSimpleOK(session, seqID)
}

// sendMySQLOKPacket 发送OK包
func (h *DecoupledMySQLMessageHandler) sendMySQLOKPacket(session Session, affectedRows, lastInsertId uint64, seqId byte) error {
	logger.Debugf("发送OK包")

	okData := h.createOKPacketData(affectedRows, lastInsertId)
	packet := h.createMySQLPacket(okData, seqId)

	return h.sendMySQLPackets(session, [][]byte{packet})
}

// sendMySQLErrorPacket 发送错误包
func (h *DecoupledMySQLMessageHandler) sendMySQLErrorPacket(session Session, code uint16, state, message string, seqId byte) error {
	logger.Debugf("发送错误包: %d - %s", code, message)

	errorData := h.createErrorPacketData(code, state, message)
	packet := h.createMySQLPacket(errorData, seqId)

	return h.sendMySQLPackets(session, [][]byte{packet})
}

// createMySQLPacket 创建MySQL包
func (h *DecoupledMySQLMessageHandler) createMySQLPacket(payload []byte, seqId byte) []byte {
	// MySQL包格式：3字节长度 + 1字节序号 + 载荷
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

// createColumnDefinitionPacket 创建列定义包
func (h *DecoupledMySQLMessageHandler) createColumnDefinitionPacket(name, typeName string, length uint32) []byte {
	var data []byte

	// 简化的列定义包格式
	data = h.appendLengthEncodedString(data, "def") // catalog
	data = h.appendLengthEncodedString(data, "")    // schema
	data = h.appendLengthEncodedString(data, "")    // table
	data = h.appendLengthEncodedString(data, "")    // org_table
	data = h.appendLengthEncodedString(data, name)  // name
	data = h.appendLengthEncodedString(data, name)  // org_name

	data = append(data, 0x0c)       // length of fixed fields
	data = append(data, 0x21, 0x00) // character set (utf8)

	// column length (4 bytes)
	data = append(data, byte(length&0xFF), byte((length>>8)&0xFF),
		byte((length>>16)&0xFF), byte((length>>24)&0xFF))

	// type
	switch typeName {
	case "BIGINT":
		data = append(data, 0x08) // MYSQL_TYPE_LONGLONG
	case "VARCHAR", "TEXT":
		data = append(data, 0xfd) // MYSQL_TYPE_VAR_STRING
	default:
		data = append(data, 0xfd)
	}

	data = append(data, 0x00, 0x00) // flags
	data = append(data, 0x00)       // decimals
	data = append(data, 0x00, 0x00) // filler

	return data
}

// createRowDataPacket 创建行数据包 - 新版本支持interface{}
func (h *DecoupledMySQLMessageHandler) createRowDataPacket(values []interface{}) []byte {
	var data []byte

	for _, value := range values {
		if value == nil {
			data = append(data, 0xfb) // NULL
		} else {
			// 将interface{}转换为字符串
			valueStr := fmt.Sprintf("%v", value)
			data = h.appendLengthEncodedString(data, valueStr)
		}
	}

	return data
}

// createEOFPacket 创建EOF包
func (h *DecoupledMySQLMessageHandler) createEOFPacket() []byte {
	return []byte{0xfe, 0x00, 0x00, 0x02, 0x00} // EOF marker + warnings + status
}

// createOKPacketData 创建OK包数据
func (h *DecoupledMySQLMessageHandler) createOKPacketData(affectedRows, lastInsertId uint64) []byte {
	var data []byte
	data = append(data, 0x00) // OK marker
	data = h.appendLengthEncodedInt(data, affectedRows)
	data = h.appendLengthEncodedInt(data, lastInsertId)
	data = append(data, 0x02, 0x00) // status flags
	data = append(data, 0x00, 0x00) // warnings
	return data
}

// createErrorPacketData 创建错误包数据
func (h *DecoupledMySQLMessageHandler) createErrorPacketData(code uint16, state, message string) []byte {
	var data []byte
	data = append(data, 0xff)                                  // Error marker
	data = append(data, byte(code&0xFF), byte((code>>8)&0xFF)) // error code
	data = append(data, '#')                                   // SQL state marker
	data = append(data, []byte(state)...)                      // SQL state (5 chars)
	data = append(data, []byte(message)...)                    // error message
	return data
}

// sendMySQLPackets 发送MySQL包序列
func (h *DecoupledMySQLMessageHandler) sendMySQLPackets(session Session, packets [][]byte) error {
	logger.Debugf("发送%d个MySQL包", len(packets))

	for i, packet := range packets {
		logger.Debugf("发送包%d，长度：%d，序号：%d", i+1, len(packet)-4, packet[3])

		if err := session.WriteBytes(packet); err != nil {
			logger.Errorf("发送包%d失败: %v", i+1, err)
			return err
		}
	}

	logger.Debugf("所有包发送成功")
	return nil
}

// sendResponse 发送响应
func (h *DecoupledMySQLMessageHandler) sendResponse(session Session, response protocol.Message) error {
	logger.Debugf("发送响应，类型: %d", response.Type())

	// 检查会话状态
	if session == nil {
		logger.Errorf("会话为空，无法发送响应")
		return fmt.Errorf("session is nil")
	}

	// 使用协议编码器编码响应
	data, err := h.protocolEncoder.EncodeMessage(response)
	if err != nil {
		logger.Errorf("响应编码失败: %v", err)
		return h.sendErrorResponse(session, 1064, "42000", "Response encoding error")
	}

	logger.Debugf("响应数据编码完成，长度: %d", len(data))

	// 发送数据
	if err := session.WriteBytes(data); err != nil {
		logger.Errorf("发送响应失败: %v", err)
		// 如果发送失败，不要继续处理特殊响应
		return err
	}

	logger.Debugf("响应发送成功")

	// 处理特殊响应类型
	h.handleSpecialResponse(session, response)
	return nil
}

// handleSpecialResponse 处理特殊响应类型
func (h *DecoupledMySQLMessageHandler) handleSpecialResponse(session Session, response protocol.Message) {
	switch response.Type() {
	case protocol.MSG_AUTH_RESPONSE:
		// 认证成功，设置会话状态
		session.SetAttribute("auth_status", "success")
		logger.Debugf("认证状态已设置")

	case protocol.MSG_USE_DB_RESPONSE:
		// 数据库切换成功，更新会话信息
		if useDBMsg, ok := response.Payload().(*protocol.UseDBMessage); ok {
			h.rwlock.Lock()
			if mysqlSession, exists := h.sessionMap[session]; exists {
				mysqlSession.SetParamByName("database", useDBMsg.Database)
				logger.Debugf("数据库切换成功: %s", useDBMsg.Database)
			}
			h.rwlock.Unlock()
		}

	case protocol.MSG_DISCONNECT:
		// 断开连接响应，标记会话可以关闭
		logger.Debugf("收到断开连接响应")
	}
}

// sendErrorResponse 发送错误响应
func (h *DecoupledMySQLMessageHandler) sendErrorResponse(session Session, code uint16, state, message string) error {
	logger.Debugf("发送错误响应: %d - %s", code, message)

	errorData := protocol.EncodeErrorPacket(code, state, message)
	if err := session.WriteBytes(errorData); err != nil {
		logger.Errorf("发送错误响应失败: %v", err)
		return err
	}

	logger.Debugf("错误响应发送成功")
	return nil
}

// appendLengthEncodedString 追加长度编码字符串
func (h *DecoupledMySQLMessageHandler) appendLengthEncodedString(data []byte, str string) []byte {
	strBytes := []byte(str)
	length := len(strBytes)

	if length < 251 {
		data = append(data, byte(length))
	} else if length < 65536 {
		data = append(data, 0xFC)
		data = append(data, byte(length), byte(length>>8))
	} else if length < 16777216 {
		data = append(data, 0xFD)
		data = append(data, byte(length), byte(length>>8), byte(length>>16))
	} else {
		data = append(data, 0xFE)
		for i := 0; i < 8; i++ {
			data = append(data, byte(length>>(i*8)))
		}
	}

	return append(data, strBytes...)
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// sendQueryResultSet 发送查询结果集
func (h *DecoupledMySQLMessageHandler) sendQueryResultSet(session Session, result *protocol.MessageQueryResult, seqID byte) error {
	logger.Debugf("[sendQueryResultSet] 开始发送查询结果集")

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

	// 1. 发送列数包
	columnCount := len(result.Columns)
	columnCountData := []byte{byte(columnCount)}
	columnCountPacket := h.createMySQLPacket(columnCountData, seqID)

	err := session.WriteBytes(columnCountPacket)
	if err != nil {
		logger.Errorf("发送列数包失败: %v", err)
		return err
	}
	seqID++

	// 2. 发送列定义包
	for _, column := range result.Columns {
		columnDefPacket := h.createColumnDefinitionPacket(column, "VARCHAR", 255)
		packet := h.createMySQLPacket(columnDefPacket, seqID)

		err := session.WriteBytes(packet)
		if err != nil {
			logger.Errorf("发送列定义包失败: %v", err)
			return err
		}
		seqID++
	}

	// 3. 发送第一个EOF包（结束列定义）
	eofPacket1 := h.createEOFPacket()
	packet1 := h.createMySQLPacket(eofPacket1, seqID)

	err = session.WriteBytes(packet1)
	if err != nil {
		logger.Errorf("发送EOF包1失败: %v", err)
		return err
	}
	seqID++

	// 4. 发送行数据包 - 直接处理interface{}而不是转换为字符串
	for _, row := range result.Rows {
		logger.Debugf("处理行: %v", row)

		// 直接传递interface{}类型的行数据
		rowDataPacket := h.createRowDataPacket(row)
		packet := h.createMySQLPacket(rowDataPacket, seqID)

		err := session.WriteBytes(packet)
		if err != nil {
			logger.Errorf("发送行数据包失败: %v", err)
			return err
		}
		seqID++

		// 记录发送的每个值的详细信息
		for i, val := range row {
			if val == nil {
				logger.Debugf("   列 %d: NULL", i)
			} else {
				logger.Debugf("   列 %d: %v (类型: %T)", i, val, val)
			}
		}
	}

	// 5. 发送第二个EOF包（结束行数据）
	eofPacket2 := h.createEOFPacket()
	packet2 := h.createMySQLPacket(eofPacket2, seqID)

	err = session.WriteBytes(packet2)
	if err != nil {
		logger.Errorf("发送EOF包2失败: %v", err)
		return err
	}

	logger.Debugf("查询结果集发送完成: %d 列, %d 行", len(result.Columns), len(result.Rows))
	return nil
}
