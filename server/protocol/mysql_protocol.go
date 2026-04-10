package protocol

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/session"
	"net"
)

// MySQLProtocolHandler 协议处理器
type MySQLProtocolHandler struct {
	sessionManager  session.SessionManager
	queryDispatcher QueryDispatcher
	preparedStmtMgr *PreparedStatementManager
	encoder         *MySQLResultSetEncoder // ResultSet 编码器（复用实例）
}

// NewMySQLProtocolHandler 创建协议处理器
func NewMySQLProtocolHandler(sessionMgr session.SessionManager, dispatcher QueryDispatcher) *MySQLProtocolHandler {
	return &MySQLProtocolHandler{
		sessionManager:  sessionMgr,
		queryDispatcher: dispatcher,
		preparedStmtMgr: NewPreparedStatementManager(),
		encoder:         NewMySQLResultSetEncoder(), // 初始化 ResultSet 编码器
	}
}

// QueryDispatcher 查询分发器接口
type QueryDispatcher interface {
	Dispatch(sess session.Session, query string) <-chan *QueryResult
}

// QueryResult 查询结果
type QueryResult struct {
	Data       interface{}
	Error      error
	ResultType string
	Message    string
	Columns    []string
	Rows       [][]interface{}
}

// MySQLRawPacket MySQL原始数据包
type MySQLRawPacket struct {
	Header PacketHeader
	Body   []byte
}

// PacketHeader 包头
type PacketHeader struct {
	PacketLength []byte
	PacketId     byte
}

// HandlePacket 处理MySQL数据包
func (h *MySQLProtocolHandler) HandlePacket(conn net.Conn, packet *MySQLRawPacket) error {
	if len(packet.Body) == 0 {
		return fmt.Errorf("empty packet body")
	}

	packetType := packet.Body[0]

	switch packetType {
	case common.COM_SLEEP:
		return h.handleSleep(conn, packet)
	case common.COM_QUERY:
		return h.handleQuery(conn, packet)
	case common.COM_QUIT:
		return h.handleQuit(conn, packet)
	case common.COM_INIT_DB:
		return h.handleInitDB(conn, packet)
	case common.COM_PING:
		return h.handlePing(conn, packet)
	case common.COM_STMT_PREPARE:
		return h.handleStmtPrepare(conn, packet)
	case common.COM_STMT_EXECUTE:
		return h.handleStmtExecute(conn, packet)
	case common.COM_STMT_CLOSE:
		return h.handleStmtClose(conn, packet)
	default:
		// 未知命令，返回错误而不是当作认证包处理
		errPacket := EncodeErrorFromCode(common.ER_UNKNOWN_ERROR,
			fmt.Sprintf("Unknown command: 0x%02X", packetType))
		conn.Write(errPacket)
		return fmt.Errorf("unknown command: 0x%02X", packetType)
	}
}

// handleAuth 处理认证
func (h *MySQLProtocolHandler) handleAuth(conn net.Conn, packet *MySQLRawPacket) error {
	authPacket := &AuthPacket{}

	// 构建完整的认证数据
	authData := make([]byte, 0, len(packet.Header.PacketLength)+1+len(packet.Body))
	authData = append(authData, packet.Header.PacketLength...)
	authData = append(authData, packet.Header.PacketId)
	authData = append(authData, packet.Body...)

	authResult := authPacket.DecodeAuth(authData)
	if authResult == nil {
		return fmt.Errorf("failed to decode auth packet")
	}

	// 创建会话
	sess, err := h.sessionManager.CreateSession(conn, authResult.User, authResult.Database)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}

	// 发送认证成功响应
	okPacket := EncodeOKPacket(nil, 0, 0, nil)
	_, err = conn.Write(okPacket)
	if err != nil {
		h.sessionManager.CloseSession(sess.ID())
		return fmt.Errorf("failed to send auth response: %w", err)
	}

	return nil
}

// handleQuery 处理查询
func (h *MySQLProtocolHandler) handleQuery(conn net.Conn, packet *MySQLRawPacket) error {
	if len(packet.Body) < 2 {
		return fmt.Errorf("invalid query packet")
	}

	query := string(packet.Body[1:])

	// 获取会话
	sess, exists := h.sessionManager.GetSessionByConn(conn)
	if !exists {
		return fmt.Errorf("session not found for connection")
	}

	// 更新会话活动时间
	sess.UpdateActivity()

	// 分发查询
	resultChan := h.queryDispatcher.Dispatch(sess, query)

	// 处理结果
	go h.handleQueryResults(conn, resultChan)

	return nil
}

// handleQueryResults 处理查询结果
func (h *MySQLProtocolHandler) handleQueryResults(conn net.Conn, resultChan <-chan *QueryResult) {
	for result := range resultChan {
		if result.Error != nil {
			// 发送错误响应（使用错误处理工具）
			errPacket := EncodeErrorFromGoError(result.Error)
			conn.Write(errPacket)
			continue
		}

		switch result.ResultType {
		case "query":
			// 发送查询结果
			h.sendQueryResult(conn, result)
		case "ddl":
			// 发送DDL成功响应
			okPacket := EncodeOKPacket(nil, 0, 1, nil)
			conn.Write(okPacket)
		default:
			// 发送通用成功响应
			okPacket := EncodeOKPacket(nil, 0, 0, nil)
			conn.Write(okPacket)
		}
	}
}

// sendQueryResult 发送查询结果
func (h *MySQLProtocolHandler) sendQueryResult(conn net.Conn, result *QueryResult) {
	// 使用编码器生成 ResultSet 包
	resultSetData := &ResultSetData{
		Columns: result.Columns,
		Rows:    result.Rows,
	}

	// 生成所有包（不包括 MySQL packet header）
	packets := h.encoder.SendResultSetPackets(resultSetData)

	// 添加 MySQL packet header 并发送
	seqID := byte(1)
	for _, payload := range packets {
		packet := h.addPacketHeader(payload, seqID)
		conn.Write(packet)
		seqID++
	}
}

// addPacketHeader 添加 MySQL 包头
func (h *MySQLProtocolHandler) addPacketHeader(payload []byte, sequenceID byte) []byte {
	length := len(payload)
	header := make([]byte, 4)

	// 包长度 (3字节，小端序)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 序列号
	header[3] = sequenceID

	return append(header, payload...)
}

// handleQuit 处理退出
func (h *MySQLProtocolHandler) handleQuit(conn net.Conn, packet *MySQLRawPacket) error {
	sess, exists := h.sessionManager.GetSessionByConn(conn)
	if exists {
		h.sessionManager.CloseSession(sess.ID())
	}
	return nil
}

// handleInitDB 处理切换数据库
func (h *MySQLProtocolHandler) handleInitDB(conn net.Conn, packet *MySQLRawPacket) error {
	if len(packet.Body) < 2 {
		return fmt.Errorf("invalid init db packet")
	}

	dbName := string(packet.Body[1:])

	sess, exists := h.sessionManager.GetSessionByConn(conn)
	if !exists {
		return fmt.Errorf("session not found")
	}

	sess.SetDatabase(dbName)

	// 发送成功响应
	okPacket := EncodeOKPacket(nil, 0, 0, nil)
	_, err := conn.Write(okPacket)
	return err
}

// handlePing 处理ping
func (h *MySQLProtocolHandler) handlePing(conn net.Conn, packet *MySQLRawPacket) error {
	okPacket := EncodeOKPacket(nil, 0, 0, nil)
	_, err := conn.Write(okPacket)
	return err
}

// handleSleep 处理sleep命令
func (h *MySQLProtocolHandler) handleSleep(conn net.Conn, packet *MySQLRawPacket) error {
	// Sleep命令不需要特殊处理
	return nil
}

// handleStmtPrepare 处理 COM_STMT_PREPARE 命令
func (h *MySQLProtocolHandler) handleStmtPrepare(conn net.Conn, packet *MySQLRawPacket) error {
	if len(packet.Body) < 2 {
		return fmt.Errorf("invalid stmt prepare packet")
	}

	// 提取SQL语句（跳过命令字节）
	sql := string(packet.Body[1:])

	// 准备语句
	stmt, err := h.preparedStmtMgr.Prepare(sql)
	if err != nil {
		errPacket := EncodeErrorFromGoError(err)
		_, writeErr := conn.Write(errPacket)
		if writeErr != nil {
			return fmt.Errorf("failed to send error packet: %w", writeErr)
		}
		return err
	}

	// 编码响应包
	responsePackets := EncodePrepareResponse(stmt, packet.Header.PacketId+1)

	// 发送所有响应包
	for _, respPacket := range responsePackets {
		_, err = conn.Write(respPacket)
		if err != nil {
			return fmt.Errorf("failed to send prepare response: %w", err)
		}
	}

	return nil
}

// handleStmtExecute 处理 COM_STMT_EXECUTE 命令
func (h *MySQLProtocolHandler) handleStmtExecute(conn net.Conn, packet *MySQLRawPacket) error {
	if len(packet.Body) < 10 {
		return fmt.Errorf("invalid stmt execute packet")
	}

	// 解析语句ID（4字节，小端序）
	stmtID := uint32(packet.Body[1]) | uint32(packet.Body[2])<<8 |
		uint32(packet.Body[3])<<16 | uint32(packet.Body[4])<<24

	// 获取预编译语句
	stmt, err := h.preparedStmtMgr.Get(stmtID)
	if err != nil {
		errPacket := EncodeErrorFromCode(common.ErrUnknownStmtHandler, stmtID)
		conn.Write(errPacket)
		return err
	}

	// 解析执行标志（1字节）
	// flags := packet.Body[5]

	// 解析迭代次数（4字节，通常为1）
	// iterationCount := uint32(packet.Body[6]) | uint32(packet.Body[7])<<8 |
	// 	uint32(packet.Body[8])<<16 | uint32(packet.Body[9])<<24

	params, typeBlock, err := ParseBinaryStmtExecuteParams(packet.Body[10:], stmt.ParamCount, stmt.LastParamTypes)
	if err != nil {
		errPacket := EncodeErrorFromGoError(err)
		conn.Write(errPacket)
		return err
	}
	stmt.LastParamTypes = typeBlock

	boundSQL := BindPreparedSQL(stmt.SQL, params)

	// 获取会话
	sess, exists := h.sessionManager.GetSessionByConn(conn)
	if !exists {
		return fmt.Errorf("session not found for connection")
	}

	// 更新会话活动时间
	sess.UpdateActivity()

	// 执行查询
	resultChan := h.queryDispatcher.Dispatch(sess, boundSQL)

	// 处理结果（同步处理，确保顺序）
	for result := range resultChan {
		if result.Error != nil {
			errPacket := EncodeErrorFromGoError(result.Error)
			conn.Write(errPacket)
			continue
		}

		switch result.ResultType {
		case "query":
			h.sendQueryResult(conn, result)
		case "ddl":
			okPacket := EncodeOKPacket(nil, 0, 1, nil)
			conn.Write(okPacket)
		default:
			okPacket := EncodeOKPacket(nil, 0, 0, nil)
			conn.Write(okPacket)
		}
	}

	return nil
}

// handleStmtClose 处理 COM_STMT_CLOSE 命令
func (h *MySQLProtocolHandler) handleStmtClose(conn net.Conn, packet *MySQLRawPacket) error {
	if len(packet.Body) < 5 {
		return fmt.Errorf("invalid stmt close packet")
	}

	// 解析语句ID（4字节，小端序）
	stmtID := uint32(packet.Body[1]) | uint32(packet.Body[2])<<8 |
		uint32(packet.Body[3])<<16 | uint32(packet.Body[4])<<24

	// 关闭语句
	err := h.preparedStmtMgr.Close(stmtID)
	if err != nil {
		// COM_STMT_CLOSE 不返回响应，即使失败也不发送错误包
		return nil
	}

	// COM_STMT_CLOSE 不返回响应
	return nil
}

