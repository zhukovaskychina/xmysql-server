package protocol

import (
	"fmt"
	"net"
	"xmysql-server/server/common"
	"xmysql-server/server/session"
)

// MySQLProtocolHandler 协议处理器
type MySQLProtocolHandler struct {
	sessionManager  session.SessionManager
	queryDispatcher QueryDispatcher
}

// NewMySQLProtocolHandler 创建协议处理器
func NewMySQLProtocolHandler(sessionMgr session.SessionManager, dispatcher QueryDispatcher) *MySQLProtocolHandler {
	return &MySQLProtocolHandler{
		sessionManager:  sessionMgr,
		queryDispatcher: dispatcher,
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
	default:
		return h.handleAuth(conn, packet)
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
			// 发送错误响应
			errPacket := EncodeErrorPacket(1064, "42000", result.Error.Error())
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
	// 发送列定义
	if len(result.Columns) > 0 {
		columnPacket := EncodeColumnsPacket(result.Columns)
		conn.Write(columnPacket)
	}

	// 发送行数据
	if len(result.Rows) > 0 {
		for _, row := range result.Rows {
			rowPacket := EncodeRowPacket(row)
			conn.Write(rowPacket)
		}
	}

	// 发送EOF包
	eofPacket := EncodeEOFPacket(0, 0)
	conn.Write(eofPacket)
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
