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
}

// NewMySQLProtocolHandler 创建协议处理器
func NewMySQLProtocolHandler(sessionMgr session.SessionManager, dispatcher QueryDispatcher) *MySQLProtocolHandler {
	return &MySQLProtocolHandler{
		sessionManager:  sessionMgr,
		queryDispatcher: dispatcher,
		preparedStmtMgr: NewPreparedStatementManager(),
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

	// 解析参数
	params, err := h.parseExecuteParams(packet.Body[10:], stmt.ParamCount)
	if err != nil {
		errPacket := EncodeErrorFromGoError(err)
		conn.Write(errPacket)
		return err
	}

	// 绑定参数到SQL
	boundSQL := h.bindParameters(stmt.SQL, params)

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

// parseExecuteParams 解析执行参数
func (h *MySQLProtocolHandler) parseExecuteParams(data []byte, paramCount uint16) ([]interface{}, error) {
	if paramCount == 0 {
		return nil, nil
	}

	params := make([]interface{}, paramCount)

	// 计算NULL位图大小
	nullBitmapLen := (int(paramCount) + 7) / 8
	if len(data) < nullBitmapLen+1 {
		return nil, fmt.Errorf("invalid execute packet: insufficient data for null bitmap")
	}

	nullBitmap := data[:nullBitmapLen]
	pos := nullBitmapLen

	// 检查 new_params_bound_flag
	if len(data) <= pos {
		return nil, fmt.Errorf("invalid execute packet: missing new_params_bound_flag")
	}

	newParamsBoundFlag := data[pos]
	pos++

	// 如果有新参数绑定，读取参数类型
	var paramTypes []byte
	if newParamsBoundFlag == 1 {
		if len(data) < pos+int(paramCount)*2 {
			return nil, fmt.Errorf("invalid execute packet: insufficient data for param types")
		}
		paramTypes = data[pos : pos+int(paramCount)*2]
		pos += int(paramCount) * 2
	}

	// 解析参数值
	for i := uint16(0); i < paramCount; i++ {
		// 检查是否为NULL
		bytePos := int(i) / 8
		bitPos := int(i) % 8
		if nullBitmap[bytePos]&(1<<bitPos) != 0 {
			params[i] = nil
			continue
		}

		// 获取参数类型
		var paramType byte
		if newParamsBoundFlag == 1 && paramTypes != nil {
			paramType = paramTypes[i*2]
		} else {
			paramType = common.COLUMN_TYPE_VAR_STRING // 默认类型
		}

		// 根据类型解析参数值
		value, bytesRead, err := h.parseParamValue(data[pos:], paramType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse param %d: %w", i, err)
		}

		params[i] = value
		pos += bytesRead
	}

	return params, nil
}

// parseParamValue 解析参数值
func (h *MySQLProtocolHandler) parseParamValue(data []byte, paramType byte) (interface{}, int, error) {
	switch paramType {
	case common.COLUMN_TYPE_TINY:
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for TINY")
		}
		return int8(data[0]), 1, nil

	case common.COLUMN_TYPE_SHORT:
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for SHORT")
		}
		value := int16(data[0]) | int16(data[1])<<8
		return value, 2, nil

	case common.COLUMN_TYPE_LONG, common.COLUMN_TYPE_INT24:
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for LONG")
		}
		value := int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
		return value, 4, nil

	case common.COLUMN_TYPE_LONGLONG:
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for LONGLONG")
		}
		value := int64(data[0]) | int64(data[1])<<8 | int64(data[2])<<16 | int64(data[3])<<24 |
			int64(data[4])<<32 | int64(data[5])<<40 | int64(data[6])<<48 | int64(data[7])<<56
		return value, 8, nil

	case common.COLUMN_TYPE_VAR_STRING, common.COLUMN_TYPE_STRING, common.COLUMN_TYPE_VARCHAR:
		// 长度编码的字符串
		strLen, lenBytes := h.readLengthEncodedInteger(data)
		if strLen < 0 {
			return nil, 0, fmt.Errorf("invalid length-encoded string")
		}
		totalBytes := lenBytes + int(strLen)
		if len(data) < totalBytes {
			return nil, 0, fmt.Errorf("insufficient data for string")
		}
		value := string(data[lenBytes:totalBytes])
		return value, totalBytes, nil

	default:
		// 默认作为字符串处理
		strLen, lenBytes := h.readLengthEncodedInteger(data)
		if strLen < 0 {
			return nil, 0, fmt.Errorf("invalid length-encoded value")
		}
		totalBytes := lenBytes + int(strLen)
		if len(data) < totalBytes {
			return nil, 0, fmt.Errorf("insufficient data for value")
		}
		value := string(data[lenBytes:totalBytes])
		return value, totalBytes, nil
	}
}

// readLengthEncodedInteger 读取长度编码的整数
func (h *MySQLProtocolHandler) readLengthEncodedInteger(data []byte) (int64, int) {
	if len(data) == 0 {
		return -1, 0
	}

	first := data[0]
	if first < 0xfb {
		return int64(first), 1
	}

	switch first {
	case 0xfc:
		if len(data) < 3 {
			return -1, 0
		}
		return int64(data[1]) | int64(data[2])<<8, 3
	case 0xfd:
		if len(data) < 4 {
			return -1, 0
		}
		return int64(data[1]) | int64(data[2])<<8 | int64(data[3])<<16, 4
	case 0xfe:
		if len(data) < 9 {
			return -1, 0
		}
		return int64(data[1]) | int64(data[2])<<8 | int64(data[3])<<16 | int64(data[4])<<24 |
			int64(data[5])<<32 | int64(data[6])<<40 | int64(data[7])<<48 | int64(data[8])<<56, 9
	default:
		return -1, 0
	}
}

// bindParameters 绑定参数到SQL
func (h *MySQLProtocolHandler) bindParameters(sql string, params []interface{}) string {
	if len(params) == 0 {
		return sql
	}

	result := ""
	paramIndex := 0

	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' && paramIndex < len(params) {
			// 替换占位符
			param := params[paramIndex]
			paramIndex++

			if param == nil {
				result += "NULL"
			} else {
				switch v := param.(type) {
				case string:
					// 转义字符串中的单引号
					escaped := ""
					for _, ch := range v {
						if ch == '\'' {
							escaped += "''"
						} else {
							escaped += string(ch)
						}
					}
					result += "'" + escaped + "'"
				case int8, int16, int32, int64, int:
					result += fmt.Sprintf("%d", v)
				default:
					result += fmt.Sprintf("'%v'", v)
				}
			}
		} else {
			result += string(sql[i])
		}
	}

	return result
}
