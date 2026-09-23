package net

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/auth"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

// MockSession 模拟会话用于测试
type MockSession struct {
	id         string
	attributes map[string]interface{}
	closed     bool
	written    [][]byte
	remoteAddr string
}

func NewMockSession(id string) *MockSession {
	return &MockSession{
		id:         id,
		attributes: make(map[string]interface{}),
		closed:     false,
		written:    make([][]byte, 0),
		remoteAddr: "127.0.0.1:12345",
	}
}

func (s *MockSession) Stat() string {
	return s.id
}

func (s *MockSession) SetAttribute(key interface{}, value interface{}) {
	s.attributes[key.(string)] = value
}

func (s *MockSession) GetAttribute(key interface{}) interface{} {
	return s.attributes[key.(string)]
}

func (s *MockSession) WriteBytes(data []byte) error {
	// 模拟写入数据并记录内容，便于测试验证
	if data != nil {
		// 复制一份数据，避免后续修改影响记录
		buf := make([]byte, len(data))
		copy(buf, data)
		s.written = append(s.written, buf)
	}
	return nil
}

func (s *MockSession) Close() {
	s.closed = true
}

// 实现Session接口的其他必要方法（简化实现）
func (s *MockSession) ID() uint32                                { return 1 }
func (s *MockSession) SetCompressType(compressType CompressType) {}
func (s *MockSession) LocalAddr() string                         { return "127.0.0.1:3308" }
func (s *MockSession) RemoteAddr() string {
	if s.remoteAddr != "" {
		return s.remoteAddr
	}
	return "127.0.0.1:12345"
}
func (s *MockSession) incReadPkgNum()                                        {}
func (s *MockSession) incWritePkgNum()                                       {}
func (s *MockSession) UpdateActive()                                         {}
func (s *MockSession) GetActive() time.Time                                  { return time.Now() }
func (s *MockSession) readTimeout() time.Duration                            { return time.Second }
func (s *MockSession) SetReadTimeout(timeout time.Duration)                  {}
func (s *MockSession) writeTimeout() time.Duration                           { return time.Second }
func (s *MockSession) SetWriteTimeout(timeout time.Duration)                 {}
func (s *MockSession) send(interface{}) (int, error)                         { return 0, nil }
func (s *MockSession) close(int)                                             {}
func (s *MockSession) setSession(Session)                                    {}
func (s *MockSession) Reset()                                                {}
func (s *MockSession) Conn() net.Conn                                        { return nil }
func (s *MockSession) IsClosed() bool                                        { return s.closed }
func (s *MockSession) EndPoint() EndPoint                                    { return nil }
func (s *MockSession) SetMaxMsgLen(length int)                               {}
func (s *MockSession) SetName(name string)                                   {}
func (s *MockSession) SetEventListener(listener EventListener)               {}
func (s *MockSession) SetPkgHandler(handler ReadWriter)                      {}
func (s *MockSession) SetReader(Reader)                                      {}
func (s *MockSession) SetWriter(Writer)                                      {}
func (s *MockSession) SetCronPeriod(period int)                              {}
func (s *MockSession) SetWQLen(length int)                                   {}
func (s *MockSession) SetWaitTime(timeout time.Duration)                     {}
func (s *MockSession) RemoveAttribute(interface{})                           {}
func (s *MockSession) WritePkg(pkg interface{}, timeout time.Duration) error { return nil }
func (s *MockSession) WriteBytesArray(...[]byte) error                       { return nil }

func TestMySQLSessionStatusFlagsReflectAutocommitOff(t *testing.T) {
	session := NewMockSession("status_flags_autocommit_off")
	mysqlSession := NewMySQLServerSession(session)
	mysqlSession.SetParamByName("autocommit", "0")

	flags := mysqlSessionStatusFlags(mysqlSession)
	if flags&protocol.SERVER_STATUS_AUTOCOMMIT != 0 {
		t.Fatalf("expected autocommit flag to be cleared, got 0x%04x", flags)
	}
}

func TestHandleFieldListReturnsColumnDefinitionPackets(t *testing.T) {
	response := &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "field-list", nil),
		Result: &protocol.MessageQueryResult{
			Columns: []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"},
			Rows: [][]interface{}{
				{"id", "int", nil, "NO", "PRI", nil, "auto_increment", "select,insert,update,references", ""},
				{"name", "varchar(32)", "utf8mb4_general_ci", "YES", "", nil, "", "select,insert,update,references", ""},
			},
			Type: "query",
		},
	}
	handler := &DecoupledMySQLMessageHandler{
		businessHandler:  fixedQueryBusinessHandler{response: response},
		resultSetEncoder: protocol.NewMySQLResultSetEncoder(),
	}
	session := NewMockSession("field-list")
	mysqlSession := NewMySQLServerSession(session)
	mysqlSession.SetParamByName("database", "app")

	err := handler.handleFieldList(session, &mysqlSession, append([]byte{common.COM_FIELD_LIST}, append([]byte("users\x00%"), 0)...))
	if err != nil {
		t.Fatalf("handleFieldList() error = %v", err)
	}
	if len(session.written) != 3 {
		t.Fatalf("field list wrote %d packets, want 3 column packets plus EOF", len(session.written))
	}
	if got := session.written[0][4]; got == 0x00 || got == 0xFE {
		t.Fatalf("first field packet marker = 0x%02x, want a column-definition payload", got)
	}
	if got := session.written[1][4]; got == 0x00 || got == 0xFE {
		t.Fatalf("second field packet marker = 0x%02x, want a column-definition payload", got)
	}
	if got := session.written[2][4]; got != 0xFE {
		t.Fatalf("field-list terminator = 0x%02x, want EOF 0xfe", got)
	}
}

func TestHandleStatisticsReturnsTextStatisticsPacket(t *testing.T) {
	handler := &DecoupledMySQLMessageHandler{}
	session := NewMockSession("statistics")

	if err := handler.handleStatistics(session); err != nil {
		t.Fatalf("handleStatistics() error = %v", err)
	}
	if len(session.written) != 1 {
		t.Fatalf("statistics wrote %d packets, want 1", len(session.written))
	}
	packet := session.written[0]
	if len(packet) < 5 || packet[3] != 1 {
		t.Fatalf("statistics packet header = %v, want sequence id 1", packet[:localMin(len(packet), 4)])
	}
	text := string(packet[4:])
	for _, field := range []string{"Uptime:", "Threads:", "Questions:", "Slow queries:", "Queries per second avg:"} {
		if !strings.Contains(text, field) {
			t.Fatalf("statistics payload %q does not contain %q", text, field)
		}
	}
}

func TestHandleProcessInfoReturnsProcessListResultSet(t *testing.T) {
	response := &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "process-info", nil),
		Result: &protocol.MessageQueryResult{
			Columns: []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"},
			Rows:    [][]interface{}{{int64(1), "root", "localhost", "app", "Sleep", int64(0), "", nil}},
			Type:    "query",
		},
	}
	handler := &DecoupledMySQLMessageHandler{
		businessHandler:  fixedQueryBusinessHandler{response: response},
		resultSetEncoder: protocol.NewMySQLResultSetEncoder(),
	}
	session := NewMockSession("process-info")
	mysqlSession := NewMySQLServerSession(session)
	mysqlSession.SetParamByName("database", "app")

	if err := handler.handleProcessInfo(session, &mysqlSession); err != nil {
		t.Fatalf("handleProcessInfo() error = %v", err)
	}
	if len(session.written) != 12 {
		t.Fatalf("process info wrote %d packets, want column count, 8 definitions, EOF, row and EOF", len(session.written))
	}
	if got := session.written[0][4]; got != 8 {
		t.Fatalf("process list column count = %d, want 8", got)
	}
}

func TestHandleDatabaseCommandDispatchesCreateAndDrop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create bool
		cmd    byte
		want   string
	}{
		{name: "create", create: true, cmd: common.COM_CREATE_DB, want: "CREATE DATABASE `app`"},
		{name: "drop", create: false, cmd: common.COM_DROP_DB, want: "DROP DATABASE `app`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			business := &capturingQueryBusinessHandler{response: &protocol.ResponseMessage{
				BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "database-command", nil),
				Result:      &protocol.MessageQueryResult{Type: "ddl"},
			}}
			handler := &DecoupledMySQLMessageHandler{businessHandler: business}
			session := NewMockSession("database-command")
			mysqlSession := NewMySQLServerSession(session)
			mysqlSession.SetParamByName("database", "app")

			if err := handler.handleDatabaseCommand(session, &mysqlSession, append([]byte{tc.cmd}, []byte("app\x00")...), tc.create); err != nil {
				t.Fatalf("handleDatabaseCommand() error = %v", err)
			}
			if business.query != tc.want {
				t.Fatalf("dispatched query = %q, want %q", business.query, tc.want)
			}
			if len(session.written) != 1 || session.written[0][4] != 0x00 {
				t.Fatalf("database command response = %v, want OK packet", session.written)
			}
		})
	}
}

func TestHandleSetOptionTracksMultiStatements(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value byte
		want  bool
	}{
		{name: "on", value: 0, want: true},
		{name: "off", value: 1, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := &DecoupledMySQLMessageHandler{}
			session := NewMockSession("set-option")
			if err := handler.handleSetOption(session, []byte{common.COM_SET_OPTION, tc.value, 0}); err != nil {
				t.Fatalf("handleSetOption() error = %v", err)
			}
			if got, ok := session.GetAttribute("client_multi_statements").(bool); !ok || got != tc.want {
				t.Fatalf("client_multi_statements = %#v, want %v", session.GetAttribute("client_multi_statements"), tc.want)
			}
			if len(session.written) != 1 || session.written[0][4] != 0x00 {
				t.Fatalf("COM_SET_OPTION response = %v, want OK packet", session.written)
			}
		})
	}
}

func TestHandleQueryRejectsMultiStatementsWhenDisabled(t *testing.T) {
	business := &capturingQueryBusinessHandler{response: &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "multi-disabled", nil),
		Result:      &protocol.MessageQueryResult{Type: "ddl"},
	}}
	handler := &DecoupledMySQLMessageHandler{businessHandler: business}
	session := NewMockSession("multi-disabled")
	mysqlSession := NewMySQLServerSession(session)
	session.SetAttribute("client_capabilities", uint32(0))

	if err := handler.handleQueryMessageDirect(session, &mysqlSession, &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "multi-disabled", "select 1; select 2"),
		SQL:         "select 1; select 2",
	}); err != nil {
		t.Fatalf("handleQueryMessageDirect() error = %v", err)
	}
	if business.calls != 0 {
		t.Fatalf("business handler calls = %d, want 0", business.calls)
	}
	if len(session.written) != 1 || session.written[0][4] != 0xff {
		t.Fatalf("multi-statement response = %v, want one ERR packet", session.written)
	}
}

func TestHandleQueryAllowsNegotiatedMultiStatementsAndSetOptionOverride(t *testing.T) {
	business := &capturingQueryBusinessHandler{response: &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "multi-enabled", nil),
		Result:      &protocol.MessageQueryResult{Type: "ddl"},
	}}
	handler := &DecoupledMySQLMessageHandler{businessHandler: business}
	session := NewMockSession("multi-enabled")
	mysqlSession := NewMySQLServerSession(session)
	session.SetAttribute("client_capabilities", uint32(protocol.CLIENT_MULTI_STATEMENTS|protocol.CLIENT_MULTI_RESULTS))

	if err := handler.handleQueryMessageDirect(session, &mysqlSession, &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "multi-enabled", "select 1; select 2"),
		SQL:         "select 1; select 2",
	}); err != nil {
		t.Fatalf("negotiated multi-statement query failed: %v", err)
	}
	if business.calls != 2 {
		t.Fatalf("business handler calls = %d, want 2", business.calls)
	}

	session.written = nil
	if err := handler.handleSetOption(session, []byte{common.COM_SET_OPTION, 1, 0}); err != nil {
		t.Fatalf("COM_SET_OPTION OFF failed: %v", err)
	}
	session.written = nil
	if err := handler.handleQueryMessageDirect(session, &mysqlSession, &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "multi-enabled", "select 3; select 4"),
		SQL:         "select 3; select 4",
	}); err != nil {
		t.Fatalf("disabled multi-statement query returned transport error: %v", err)
	}
	if business.calls != 2 {
		t.Fatalf("business handler calls after COM_SET_OPTION OFF = %d, want 2", business.calls)
	}
	if len(session.written) != 1 || session.written[0][4] != 0xff {
		t.Fatalf("disabled multi-statement response = %v, want one ERR packet", session.written)
	}
}

func TestHandleRefreshAcknowledgesValidCommand(t *testing.T) {
	handler := &DecoupledMySQLMessageHandler{}
	session := NewMockSession("refresh-test")

	if err := handler.handleRefresh(session, []byte{common.COM_REFRESH, 0x04}); err != nil {
		t.Fatalf("handleRefresh() error = %v", err)
	}
	if len(session.written) != 1 || session.written[0][4] != 0x00 {
		t.Fatalf("COM_REFRESH response = %v, want OK packet", session.written)
	}

	session.written = nil
	if err := handler.handleRefresh(session, []byte{common.COM_REFRESH}); err != nil {
		t.Fatalf("handleRefresh() truncated command error = %v", err)
	}
	if len(session.written) != 1 || session.written[0][4] != 0xff {
		t.Fatalf("truncated COM_REFRESH response = %v, want ERR packet", session.written)
	}
}

type capturingQueryBusinessHandler struct {
	query    string
	calls    int
	response protocol.Message
}

func (h *capturingQueryBusinessHandler) HandleMessage(message protocol.Message) (protocol.Message, error) {
	if query, ok := message.(*protocol.QueryMessage); ok {
		h.query = query.SQL
		h.calls++
	}
	return h.response, nil
}

func (h *capturingQueryBusinessHandler) CanHandle(protocol.MessageType) bool { return true }

type fixedQueryBusinessHandler struct {
	response protocol.Message
}

func (h fixedQueryBusinessHandler) HandleMessage(protocol.Message) (protocol.Message, error) {
	return h.response, nil
}

func (h fixedQueryBusinessHandler) CanHandle(protocol.MessageType) bool { return true }

func TestHandleComStmtExecuteCursorPreservesClassifiedQueryError(t *testing.T) {
	handler := &DecoupledMySQLMessageHandler{
		businessHandler: fixedQueryBusinessHandler{response: &protocol.ResponseMessage{
			Result: &protocol.MessageQueryResult{Error: fmt.Errorf("table xmysql_missing does not exist")},
		}},
	}
	session := NewMockSession("stmt_execute_cursor_error")
	mgr := handler.preparedStmtMgrFromSession(session)
	stmt, err := mgr.Prepare("select * from xmysql_missing")
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	body := make([]byte, 10)
	body[0] = common.COM_STMT_EXECUTE
	binary.LittleEndian.PutUint32(body[1:5], stmt.ID)
	body[5] = 0x01 // CURSOR_TYPE_READ_ONLY
	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 4}, Body: body}

	if err := handler.handleComStmtExecute(session, nil, pkt); err != nil {
		t.Fatalf("COM_STMT_EXECUTE returned transport error: %v", err)
	}
	if len(session.written) != 1 {
		t.Fatalf("expected one error packet, got %d", len(session.written))
	}
	packet := session.written[0]
	if len(packet) < 13 || packet[4] != 0xff {
		t.Fatalf("expected MySQL error packet, got %v", packet)
	}
	if got := uint16(packet[5]) | uint16(packet[6])<<8; got != common.ErrNoSuchTable {
		t.Fatalf("expected unknown-table errno %d, got %d", common.ErrNoSuchTable, got)
	}
	if got := string(packet[8:13]); got != "42S02" {
		t.Fatalf("expected unknown-table SQLSTATE 42S02, got %s", got)
	}
}

func TestResolveAuthHost(t *testing.T) {
	config := conf.NewCfg()
	handler := NewDecoupledMySQLMessageHandler(config)

	tests := []struct {
		name   string
		remote string
		want   string
	}{
		{
			name:   "ipv4 with port",
			remote: "127.0.0.1:12345",
			want:   "localhost",
		},
		{
			name:   "ipv6 localhost with port",
			remote: "[::1]:3306",
			want:   "localhost",
		},
		{
			name:   "dns host with port",
			remote: "db.internal:3306",
			want:   "db.internal",
		},
		{
			name:   "ipv6 with port",
			remote: "[2001:db8::1]:3306",
			want:   "2001:db8::1",
		},
		{
			name:   "invalid host",
			remote: "bad_host%%",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := NewMockSession("resolve-host-test")
			session.remoteAddr = tt.remote

			got := handler.resolveAuthHost(session)
			if got != tt.want {
				t.Fatalf("resolveAuthHost(%q) = %q, want %q", tt.remote, got, tt.want)
			}
		})
	}
}

// TestDecoupledMySQLMessageHandler 测试解耦的消息处理器
func TestDecoupledMySQLMessageHandler(t *testing.T) {
	// 创建配置
	config := conf.NewCfg()

	// 创建解耦的消息处理器
	handler := NewDecoupledMySQLMessageHandler(config)

	// 创建模拟会话
	session := NewMockSession("test_simple_protocol-session-1")

	// 测试连接打开
	err := handler.OnOpen(session)
	if err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}

	// 验证会话是否被正确添加
	handler.rwlock.RLock()
	_, exists := handler.sessionMap[session]
	handler.rwlock.RUnlock()

	if !exists {
		t.Fatal("Session was not added to sessionMap")
	}
	if output := metrics.DefaultRuntimeRecorder().PrometheusText(); !strings.Contains(output, `xmysql_connections_active{listener="mysql"} 1`) {
		t.Fatalf("active connection metric after OnOpen = %q, want listener=mysql value 1", output)
	}

	// 测试连接关闭
	handler.OnClose(session)

	// 验证会话是否被正确移除
	handler.rwlock.RLock()
	_, exists = handler.sessionMap[session]
	handler.rwlock.RUnlock()

	if exists {
		t.Fatal("Session was not removed from sessionMap")
	}

	if !session.closed {
		t.Fatal("Session was not closed")
	}
	if output := metrics.DefaultRuntimeRecorder().PrometheusText(); !strings.Contains(output, `xmysql_connections_active{listener="mysql"} 0`) {
		t.Fatalf("active connection metric after OnClose = %q, want listener=mysql value 0", output)
	}
}

func TestDecoupledMySQLMessageHandlerConcurrentClose(t *testing.T) {
	handler := &DecoupledMySQLMessageHandler{
		sessionMap: make(map[Session]server.MySQLServerSession),
	}

	const sessionCount = 32
	sessions := make([]*MockSession, sessionCount)
	handler.rwlock.Lock()
	for i := range sessions {
		sessions[i] = NewMockSession(fmt.Sprintf("concurrent-close-%d", i))
		handler.sessionMap[sessions[i]] = NewMySQLServerSession(sessions[i])
	}
	handler.rwlock.Unlock()

	var wg sync.WaitGroup
	for _, session := range sessions {
		wg.Add(1)
		go func(session *MockSession) {
			defer wg.Done()
			handler.OnClose(session)
		}(session)
	}
	wg.Wait()

	handler.rwlock.RLock()
	remaining := len(handler.sessionMap)
	handler.rwlock.RUnlock()
	if remaining != 0 {
		t.Fatalf("session map retained %d sessions after concurrent close", remaining)
	}
}

// TestMessageBusIntegration 测试消息总线集成
func TestMessageBusIntegration(t *testing.T) {
	// 创建配置
	config := conf.NewCfg()

	// 创建解耦的消息处理器
	handler := NewDecoupledMySQLMessageHandler(config)

	// 创建测试消息
	testMsg := &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "test_simple_protocol-session", "SELECT 1"),
		SQL:         "SELECT 1",
		Database:    "test_simple_protocol",
	}

	// 测试消息总线是否能正确处理消息
	responseChan := handler.messageBus.PublishAsync(testMsg)

	// 等待响应
	select {
	case response := <-responseChan:
		if response == nil {
			t.Fatal("No response received")
		}

		// 验证响应类型
		if response.Type() != protocol.MSG_QUERY_RESPONSE && response.Type() != protocol.MSG_ERROR {
			t.Fatalf("Unexpected response type: %d", response.Type())
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for response")
	}
}

// TestProtocolParserIntegration 测试协议解析器集成
func TestProtocolParserIntegration(t *testing.T) {
	// 创建配置
	config := conf.NewCfg()

	// 创建解耦的消息处理器
	handler := NewDecoupledMySQLMessageHandler(config)

	// 测试查询包解析
	queryPacket := []byte{0x03, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}

	message, err := handler.protocolParser.ParsePacket(queryPacket, "test_simple_protocol-session")
	if err != nil {
		t.Fatalf("Failed to parse query packet: %v", err)
	}

	if message.Type() != protocol.MSG_QUERY_REQUEST {
		t.Fatalf("Expected MSG_QUERY_REQUEST, got %d", message.Type())
	}

	queryMsg, ok := message.(*protocol.QueryMessage)
	if !ok {
		t.Fatal("Message is not a QueryMessage")
	}

	if queryMsg.SQL != "SELECT 1" {
		t.Fatalf("Expected 'SELECT 1', got '%s'", queryMsg.SQL)
	}
}

// TestProtocolEncoderIntegration 测试协议编码器集成
func TestProtocolEncoderIntegration(t *testing.T) {
	// 创建配置
	config := conf.NewCfg()

	// 创建解耦的消息处理器
	handler := NewDecoupledMySQLMessageHandler(config)

	// 创建测试响应消息
	result := &protocol.MessageQueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, "test1"},
			{2, "test2"},
		},
		Type: "select",
	}

	responseMsg := &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "test_simple_protocol-session", result),
		Result:      result,
	}

	// 测试编码
	data, err := handler.protocolEncoder.EncodeMessage(responseMsg)
	if err != nil {
		t.Fatalf("Failed to encode response: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Encoded data is empty")
	}
}

// TestSendQueryResultSet_ClientDeprecateEOFUsesOK verifies capability
// negotiation for the modern CLIENT_DEPRECATE_EOF protocol.
func TestSendQueryResultSet_ClientDeprecateEOFStillUsesEOF(t *testing.T) {
	config := conf.NewCfg()
	// 使用真实的处理器，但通过 MockSession 捕获输出
	handler := NewDecoupledMySQLMessageHandler(config)
	session := NewMockSession("test_sendQueryResultSet_deprecateEOF")

	// 模拟客户端能力：客户端开启 CLIENT_DEPRECATE_EOF，服务端发送 OK terminators。
	session.SetAttribute("client_capabilities", common.CLIENT_DEPRECATE_EOF)

	// 构造与 JDBC init 查询等价的 19 列系统变量结果集
	columns := []string{
		"auto_increment_increment",
		"character_set_client",
		"character_set_connection",
		"character_set_results",
		"character_set_server",
		"collation_server",
		"collation_connection",
		"init_connect",
		"interactive_timeout",
		"license",
		"lower_case_table_names",
		"max_allowed_packet",
		"net_write_timeout",
		"performance_schema",
		"sql_mode",
		"system_time_zone",
		"time_zone",
		"transaction_isolation",
		"wait_timeout",
	}

	row := []interface{}{
		int64(1),             // auto_increment_increment
		"utf8mb4",            // character_set_client
		"utf8mb4",            // character_set_connection
		"utf8mb4",            // character_set_results
		"utf8mb4",            // character_set_server
		"utf8mb4_general_ci", // collation_server
		"utf8mb4_general_ci", // collation_connection
		"",                   // init_connect
		int64(28800),         // interactive_timeout
		"GPL",                // license
		int64(0),             // lower_case_table_names
		int64(67108864),      // max_allowed_packet
		int64(60),            // net_write_timeout
		"ON",                 // performance_schema
		"STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", // sql_mode
		"CST",             // system_time_zone
		"SYSTEM",          // time_zone
		"REPEATABLE-READ", // transaction_isolation
		int64(28800),      // wait_timeout
	}

	result := &protocol.MessageQueryResult{
		Columns: columns,
		Rows:    [][]interface{}{row},
		Type:    "select",
	}

	// 调用 sendQueryResultSet，从 seqID=1 开始
	if err := handler.sendQueryResultSet(session, result, 1); err != nil {
		t.Fatalf("sendQueryResultSet failed: %v", err)
	}

	// 对于 19 列、1 行的结果集，预期包数量：
	// 1 (ColumnCount) + 19 (ColumnDefinitions) + 1 (列结束 EOF) + 1 (Row) + 1 (结果集结束 EOF) = 23
	if len(session.written) != 23 {
		t.Fatalf("unexpected packet count: got %d, want 23", len(session.written))
	}

	// 第一个包是列数包，检查长度和序号是否合理
	colCountPkt := session.written[0]
	if len(colCountPkt) < 5 {
		t.Fatalf("column count packet too short: %d bytes", len(colCountPkt))
	}
	// 包头长度应等于 payload 长度
	payloadLen := int(colCountPkt[0]) | int(colCountPkt[1])<<8 | int(colCountPkt[2])<<16
	if payloadLen != len(colCountPkt)-4 {
		t.Fatalf("column count packet header length = %d, want %d", payloadLen, len(colCountPkt)-4)
	}
	// payload 中的列数应为 19
	if colCountPkt[4] != byte(len(columns)) {
		t.Fatalf("column count mismatch in payload: got %d, want %d", colCountPkt[4], len(columns))
	}

	// 列定义结束包位于第 1+len(columns) 个位置
	colTermPkt := session.written[1+len(columns)]
	if len(colTermPkt) < 5 {
		t.Fatalf("column terminator packet too short: %d bytes", len(colTermPkt))
	}
	// payload 第一个字节应该是 OK 标记 0x00。
	if colTermPkt[4] != 0x00 {
		t.Fatalf("expected OK packet (0x00) as column terminator, got 0x%02X", colTermPkt[4])
	}

	// 结果集结束包是最后一个包
	rowTermPkt := session.written[len(session.written)-1]
	if len(rowTermPkt) < 5 {
		t.Fatalf("row terminator packet too short: %d bytes", len(rowTermPkt))
	}
	if rowTermPkt[4] != 0x00 {
		t.Fatalf("expected OK packet (0x00) as row terminator, got 0x%02X", rowTermPkt[4])
	}
}

func TestCursorResultUsesBinaryRowsAndCursorStatus(t *testing.T) {
	handler := NewDecoupledMySQLMessageHandler(conf.NewCfg())
	session := NewMockSession("binary_cursor")
	session.SetAttribute("client_capabilities", uint32(0))
	result := &protocol.MessageQueryResult{
		Columns:     []string{"id", "name"},
		ColumnTypes: []string{"int", "varchar"},
		Rows:        [][]interface{}{{int64(7), "seven"}},
		Type:        "select",
	}

	if err := handler.sendCursorMetadata(session, result, 1); err != nil {
		t.Fatalf("sendCursorMetadata failed: %v", err)
	}
	if len(session.written) != 4 {
		t.Fatalf("metadata packet count = %d, want 4", len(session.written))
	}
	if session.written[0][4] != 2 {
		t.Fatalf("metadata column count = %d, want 2", session.written[0][4])
	}

	session.written = nil
	if err := handler.sendBinaryCursorRows(session, result, false, 1); err != nil {
		t.Fatalf("sendBinaryCursorRows failed: %v", err)
	}
	if len(session.written) != 2 {
		t.Fatalf("cursor fetch packet count = %d, want row + terminator", len(session.written))
	}
	rowPayload := session.written[0][4:]
	if rowPayload[0] != 0x00 || rowPayload[1] != 0x00 {
		t.Fatalf("binary row prefix = %#v, want marker and empty null bitmap", rowPayload[:2])
	}
	if got := binary.LittleEndian.Uint32(rowPayload[2:6]); got != 7 {
		t.Fatalf("binary cursor id = %d, want 7", got)
	}
	status := binary.LittleEndian.Uint16(session.written[1][7:9])
	if status&protocol.SERVER_STATUS_CURSOR_EXISTS == 0 || status&protocol.SERVER_STATUS_LAST_ROW_SENT != 0 {
		t.Fatalf("open cursor status = %#x, want CURSOR_EXISTS only", status)
	}

	session.written = nil
	if err := handler.sendBinaryCursorRows(session, &protocol.MessageQueryResult{Columns: result.Columns, ColumnTypes: result.ColumnTypes, Type: "select"}, true, 1); err != nil {
		t.Fatalf("send final cursor rows failed: %v", err)
	}
	status = binary.LittleEndian.Uint16(session.written[0][7:9])
	if status&protocol.SERVER_STATUS_LAST_ROW_SENT == 0 || status&protocol.SERVER_STATUS_CURSOR_EXISTS != 0 {
		t.Fatalf("closed cursor status = %#x, want LAST_ROW_SENT", status)
	}
}

func TestHandleQueryResponse_QueryTypeWithColumnsSendsResultSet(t *testing.T) {
	config := conf.NewCfg()
	handler := NewDecoupledMySQLMessageHandler(config)
	session := NewMockSession("test_query_type_with_columns")

	result := &protocol.MessageQueryResult{
		Columns: []string{"Database"},
		Rows:    [][]interface{}{{"app_db"}},
		Type:    "query",
	}
	response := &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "test", result),
		Result:      result,
	}
	handler.businessHandler = fixedQueryBusinessHandler{response: response}
	mysqlSession := NewMySQLServerSession(session)
	query := &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "test", "show databases like 'app_%'"),
		SQL:         "show databases like 'app_%'",
	}

	err := handler.handleQueryMessageDirect(session, &mysqlSession, query)

	if err != nil {
		t.Fatalf("handleQueryMessageDirect failed: %v", err)
	}
	if len(session.written) == 0 {
		t.Fatal("expected packets to be written")
	}
	first := session.written[0]
	if len(first) < 5 {
		t.Fatalf("first packet too short: %d", len(first))
	}
	if first[4] != 0x01 {
		t.Fatalf("expected column-count packet for one-column ResultSet, got first payload byte 0x%02X", first[4])
	}
	if len(session.written) <= 1 {
		t.Fatalf("expected ResultSet packet sequence, got %d packet(s)", len(session.written))
	}
}

func TestHandleQueryMessageDirectRecordsRuntimeMetrics(t *testing.T) {
	before := len(metrics.DefaultRuntimeRecorder().StatementHistory())
	handler := &DecoupledMySQLMessageHandler{businessHandler: fixedQueryBusinessHandler{response: &protocol.ResponseMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "metrics", nil),
		Result:      &protocol.MessageQueryResult{Type: "ddl"},
	}}}
	session := NewMockSession("runtime-metrics-query")
	mysqlSession := NewMySQLServerSession(session)
	query := &protocol.QueryMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "metrics", "select 1"),
		SQL:         "select 1",
	}
	require.NoError(t, handler.handleQueryMessageDirect(session, &mysqlSession, query))
	history := metrics.DefaultRuntimeRecorder().StatementHistory()
	require.Greater(t, len(history), before)
	require.Equal(t, "select 1", history[len(history)-1].SQL)
	require.Equal(t, "SELECT", history[len(history)-1].StatementType)
	require.Contains(t, metrics.DefaultRuntimeRecorder().PrometheusText(), `xmysql_queries_total{database="",status="ok"}`)
}

func TestHandlePacketUnsupportedCommandReturnsErrorPacket(t *testing.T) {
	config := conf.NewCfg()
	handler := NewDecoupledMySQLMessageHandler(config)
	session := NewMockSession("test_handlePacket_unsupported")

	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}
	session.SetAttribute("auth_status", "success")

	currentSession, ok := handler.sessionMap[session]
	if !ok {
		t.Fatal("session not found in handler sessionMap")
	}

	pkt := &MySQLPackage{
		Header: MySQLPkgHeader{
			PacketLength: []byte{0x01, 0x00, 0x00},
			PacketId:     0,
		},
		Body: []byte{common.COM_STATISTICS},
	}

	if err := handler.handlePacket(session, &currentSession, pkt); err != nil {
		t.Fatalf("handlePacket failed: %v", err)
	}

	if session.closed {
		t.Fatalf("session should not be closed for unsupported command")
	}
	if len(session.written) == 0 {
		t.Fatalf("expected error response packet to be written")
	}
}

func TestHandlePacketComProcessKillClosesOwnedTarget(t *testing.T) {
	handler := NewDecoupledMySQLMessageHandler(conf.NewCfg())
	killer := NewMockSession("process-kill-owner")
	target := NewMockSession("process-kill-target")
	if err := handler.OnOpen(killer); err != nil {
		t.Fatalf("killer OnOpen failed: %v", err)
	}
	if err := handler.OnOpen(target); err != nil {
		t.Fatalf("target OnOpen failed: %v", err)
	}
	killer.SetAttribute("auth_status", "success")

	currentSession := handler.sessionMap[killer]
	targetSession := handler.sessionMap[target]
	currentSession.SessionContext().SetConnectionID(101)
	targetSession.SessionContext().SetConnectionID(202)
	currentSession.SetParamByName("user", "alice")
	targetSession.SetParamByName("user", "alice")

	body := []byte{common.COM_PROCESS_KILL, 0xca, 0x00, 0x00, 0x00}
	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 0}, Body: body}
	if err := handler.handlePacket(killer, &currentSession, pkt); err != nil {
		t.Fatalf("COM_PROCESS_KILL failed: %v", err)
	}
	if !target.closed {
		t.Fatal("COM_PROCESS_KILL did not close the target session")
	}
	if len(killer.written) == 0 || killer.written[len(killer.written)-1][4] != 0x00 {
		t.Fatal("COM_PROCESS_KILL did not return an OK packet")
	}
}

func TestHandlePacketComProcessKillRejectsUnknownOrMalformedTarget(t *testing.T) {
	handler := NewDecoupledMySQLMessageHandler(conf.NewCfg())
	session := NewMockSession("process-kill-invalid")
	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}
	session.SetAttribute("auth_status", "success")
	currentSession := handler.sessionMap[session]
	currentSession.SessionContext().SetConnectionID(303)
	currentSession.SetParamByName("user", "alice")

	malformed := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 0}, Body: []byte{common.COM_PROCESS_KILL, 1}}
	if err := handler.handlePacket(session, &currentSession, malformed); err != nil {
		t.Fatalf("malformed COM_PROCESS_KILL returned transport error: %v", err)
	}
	if len(session.written) == 0 || session.written[len(session.written)-1][4] != 0xff {
		t.Fatal("malformed COM_PROCESS_KILL did not return an error packet")
	}

	session.written = nil
	unknown := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 0}, Body: []byte{common.COM_PROCESS_KILL, 0xff, 0xff, 0xff, 0x7f}}
	if err := handler.handlePacket(session, &currentSession, unknown); err != nil {
		t.Fatalf("unknown COM_PROCESS_KILL returned transport error: %v", err)
	}
	if len(session.written) == 0 || session.written[len(session.written)-1][4] != 0xff {
		t.Fatal("unknown COM_PROCESS_KILL did not return an error packet")
	}
}

type denyProcessKillPrivilegeService struct {
	authSwitchTestService
}

func (denyProcessKillPrivilegeService) CheckPrivilege(context.Context, string, string, string, string, common.PrivilegeType) error {
	return fmt.Errorf("privilege denied")
}

func TestHandlePacketComProcessKillRejectsOtherAccountWithoutProcessPrivilege(t *testing.T) {
	handler := NewDecoupledMySQLMessageHandler(conf.NewCfg())
	handler.authService = &denyProcessKillPrivilegeService{}
	killer := NewMockSession("process-kill-denied-owner")
	target := NewMockSession("process-kill-denied-target")
	if err := handler.OnOpen(killer); err != nil {
		t.Fatalf("killer OnOpen failed: %v", err)
	}
	if err := handler.OnOpen(target); err != nil {
		t.Fatalf("target OnOpen failed: %v", err)
	}
	killer.SetAttribute("auth_status", "success")
	killerSession := handler.sessionMap[killer]
	targetSession := handler.sessionMap[target]
	killerSession.SessionContext().SetConnectionID(401)
	targetSession.SessionContext().SetConnectionID(402)
	killerSession.SetParamByName("user", "alice")
	killerSession.SetParamByName("host", "localhost")
	targetSession.SetParamByName("user", "bob")

	body := []byte{common.COM_PROCESS_KILL, 0x92, 0x01, 0x00, 0x00}
	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 0}, Body: body}
	if err := handler.handlePacket(killer, &killerSession, pkt); err != nil {
		t.Fatalf("denied COM_PROCESS_KILL returned transport error: %v", err)
	}
	if target.closed {
		t.Fatal("COM_PROCESS_KILL closed another account without PROCESS/SUPER privilege")
	}
	if len(killer.written) == 0 || killer.written[len(killer.written)-1][4] != 0xff {
		t.Fatal("denied COM_PROCESS_KILL did not return an error packet")
	}
}

func TestHandlePacketComStmtSendLongDataBuffersParameter(t *testing.T) {
	config := conf.NewCfg()
	handler := NewDecoupledMySQLMessageHandler(config)
	session := NewMockSession("test_handlePacket_stmt_send_long_data")

	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}
	session.SetAttribute("auth_status", "success")
	initialPackets := len(session.written)
	mgr := handler.preparedStmtMgrFromSession(session)
	stmt, err := mgr.Prepare("insert into blobs (payload) values (?)")
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	currentSession, ok := handler.sessionMap[session]
	if !ok {
		t.Fatal("session not found in handler sessionMap")
	}

	payload := []byte{
		common.COM_STMT_SEND_LONG_DATA,
		byte(stmt.ID), byte(stmt.ID >> 8), byte(stmt.ID >> 16), byte(stmt.ID >> 24), // statement_id
		0x00, 0x00, // param_id
		'h', 'e', 'l', 'l', 'o',
	}
	pkt := &MySQLPackage{
		Header: MySQLPkgHeader{
			PacketLength: []byte{byte(len(payload)), 0x00, 0x00},
			PacketId:     0,
		},
		Body: payload,
	}

	if err := handler.handlePacket(session, &currentSession, pkt); err != nil {
		t.Fatalf("handlePacket failed: %v", err)
	}

	if session.closed {
		t.Fatalf("session should not be closed for unsupported command")
	}
	if len(session.written) != initialPackets {
		t.Fatalf("COM_STMT_SEND_LONG_DATA must not send a response packet")
	}
	data, err := mgr.ConsumeLongData(stmt.ID)
	if err != nil {
		t.Fatalf("consume long data failed: %v", err)
	}
	if string(data[0]) != "hello" {
		t.Fatalf("unexpected long data: %q", data[0])
	}
}

func TestHandlePacketResetConnectionClearsPreparedState(t *testing.T) {
	handler := NewDecoupledMySQLMessageHandler(conf.NewCfg())
	session := NewMockSession("test_handlePacket_reset_connection")
	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}
	session.SetAttribute("auth_status", "success")
	mgr := handler.preparedStmtMgrFromSession(session)
	if _, err := mgr.Prepare("select ?"); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	currentSession, ok := handler.sessionMap[session]
	if !ok {
		t.Fatal("session not found in handler sessionMap")
	}
	currentSession.SetParamByName("autocommit", "0")
	currentSession.SetParamByName("last_insert_id", uint64(42))
	session.SetAttribute("client_capabilities", uint32(protocol.CLIENT_MULTI_STATEMENTS))
	session.SetAttribute("client_multi_statements", false)
	session.SetAttribute("__more_results__", true)
	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketLength: []byte{1, 0, 0}, PacketId: 0}, Body: []byte{common.COM_RESET_CONNECTION}}
	if err := handler.handlePacket(session, &currentSession, pkt); err != nil {
		t.Fatalf("handlePacket failed: %v", err)
	}
	if got := handler.preparedStmtMgrFromSession(session).Count(); got != 0 {
		t.Fatalf("prepared statements survived reset: %d", got)
	}
	if got := currentSession.GetParamByName("autocommit"); got != "1" {
		t.Fatalf("autocommit was not reset: %v", got)
	}
	if got := currentSession.GetParamByName("last_insert_id"); got != uint64(0) {
		t.Fatalf("last insert id was not reset: %v", got)
	}
	if got, ok := session.GetAttribute("client_multi_statements").(bool); !ok || !got {
		t.Fatalf("multi-statement option was not restored from handshake capabilities: %#v", session.GetAttribute("client_multi_statements"))
	}
	if got, ok := session.GetAttribute("__more_results__").(bool); !ok || got {
		t.Fatalf("more-results state was not cleared: %#v", session.GetAttribute("__more_results__"))
	}
	if len(session.written) == 0 {
		t.Fatal("expected OK response packet")
	}
}

func TestHandlePacketPingAcknowledgesAuthenticatedSession(t *testing.T) {
	handler := NewDecoupledMySQLMessageHandler(conf.NewCfg())
	session := NewMockSession("test_handlePacket_ping")
	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}
	session.SetAttribute("auth_status", "success")
	session.written = nil
	currentSession, ok := handler.sessionMap[session]
	if !ok {
		t.Fatal("session not found in handler sessionMap")
	}

	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 7}, Body: []byte{common.COM_PING}}
	if err := handler.handlePacket(session, &currentSession, pkt); err != nil {
		t.Fatalf("handlePacket failed: %v", err)
	}
	if len(session.written) != 1 {
		t.Fatalf("expected one PING response, got %d", len(session.written))
	}
	response := session.written[0]
	if len(response) < 5 || response[3] != 8 || response[4] != 0x00 {
		t.Fatalf("unexpected PING OK packet: %#v", response)
	}
}

func TestHandleComChangeUserReauthenticatesAndResetsSessionState(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DevBypassPasswordAuth = true
	handler := &DecoupledMySQLMessageHandler{cfg: cfg, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("test_change_user")
	session.SetAttribute("auth_status", "success")
	session.SetAttribute("client_capabilities", uint32(protocol.CLIENT_SECURE_CONNECTION|protocol.CLIENT_PROTOCOL_41|protocol.CLIENT_PLUGIN_AUTH|protocol.CLIENT_MULTI_STATEMENTS))
	session.SetAttribute("auth_challenge", []byte("01234567890123456789"))
	currentSession := NewMySQLServerSession(session)
	currentSession.SetParamByName("user", "old_user")
	currentSession.SetParamByName("database", "old_db")
	currentSession.SetParamByName("autocommit", "0")
	if _, err := handler.preparedStmtMgrFromSession(session).Prepare("select 1"); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	body := []byte{common.COM_CHANGE_USER}
	body = append(body, []byte("new_user\x00")...)
	body = append(body, 0) // empty native auth response in dev-bypass mode
	body = append(body, []byte("new_db\x00")...)
	body = append(body, 0x21, 0x00)
	body = append(body, []byte("mysql_native_password\x00")...)
	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketId: 4}, Body: body}

	if err := handler.handleComChangeUser(session, &currentSession, pkt); err != nil {
		t.Fatalf("COM_CHANGE_USER failed: %v", err)
	}
	if got := currentSession.GetParamByName("user"); got != "new_user" {
		t.Fatalf("user after COM_CHANGE_USER = %v, want new_user", got)
	}
	if got := currentSession.GetParamByName("database"); got != "new_db" {
		t.Fatalf("database after COM_CHANGE_USER = %v, want new_db", got)
	}
	if got := currentSession.GetParamByName("autocommit"); got != "1" {
		t.Fatalf("autocommit after COM_CHANGE_USER = %v, want 1", got)
	}
	if got := handler.preparedStmtMgrFromSession(session).Count(); got != 0 {
		t.Fatalf("prepared statements survived COM_CHANGE_USER: %d", got)
	}
	if len(session.written) != 1 || session.written[0][3] != 5 || session.written[0][4] != 0x00 {
		t.Fatalf("COM_CHANGE_USER response = %v, want OK packet with sequence 5", session.written)
	}
}

type proxyResolutionTestService struct {
	authSwitchTestService
	proxiedUser string
	proxiedHost string
}

func (s *proxyResolutionTestService) ResolveProxyUser(context.Context, string, string) (string, string, bool, error) {
	return s.proxiedUser, s.proxiedHost, true, nil
}

func (s *proxyResolutionTestService) GetUserInfo(_ context.Context, user, host string) (*auth.UserInfo, error) {
	if user == s.proxiedUser && host == s.proxiedHost {
		return &auth.UserInfo{
			User:             user,
			Host:             host,
			GlobalPrivileges: []common.PrivilegeType{common.SelectPriv},
			DefaultRoles:     []string{"report_reader@localhost"},
		}, nil
	}
	return s.authSwitchTestService.user, nil
}

func TestProxyIdentityUsesProxiedAccountForSessionPrivileges(t *testing.T) {
	service := &proxyResolutionTestService{
		authSwitchTestService: authSwitchTestService{user: &auth.UserInfo{User: "proxy_user", Host: "localhost"}},
		proxiedUser:           "report_user",
		proxiedHost:           "localhost",
	}
	handler := &DecoupledMySQLMessageHandler{authService: service}
	result := &auth.AuthResult{Success: true, User: "proxy_user", Host: "localhost"}

	require.NoError(t, handler.applyProxyIdentity(context.Background(), result))
	require.Equal(t, "report_user", result.User)
	require.Equal(t, "localhost", result.Host)
	require.Equal(t, []common.PrivilegeType{common.SelectPriv}, result.Privileges)
	require.Equal(t, []string{"report_reader@localhost"}, result.ActiveRoles)
}

func TestHandleComChangeUserAppliesRequestedCollation(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DevBypassPasswordAuth = true
	handler := &DecoupledMySQLMessageHandler{cfg: cfg, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("test_change_user_charset")
	session.SetAttribute("auth_status", "success")
	session.SetAttribute("client_capabilities", uint32(protocol.CLIENT_SECURE_CONNECTION|protocol.CLIENT_PROTOCOL_41|protocol.CLIENT_PLUGIN_AUTH))
	session.SetAttribute("auth_challenge", []byte("01234567890123456789"))
	currentSession := NewMySQLServerSession(session)
	currentSession.SetParamByName("user", "old_user")
	currentSession.SetParamByName("character_set_client", "latin1")
	currentSession.SetParamByName("character_set_connection", "latin1")
	currentSession.SetParamByName("character_set_results", "latin1")

	body := []byte{common.COM_CHANGE_USER}
	body = append(body, []byte("new_user\x00")...)
	body = append(body, 0) // empty native auth response in dev-bypass mode
	body = append(body, []byte("new_db\x00")...)
	body = append(body, 0x21, 0x00) // collation 33: utf8_general_ci
	body = append(body, []byte("mysql_native_password\x00")...)

	if err := handler.handleComChangeUser(session, &currentSession, &MySQLPackage{Body: body}); err != nil {
		t.Fatalf("COM_CHANGE_USER failed: %v", err)
	}
	for _, name := range []string{"character_set_client", "character_set_connection", "character_set_results"} {
		if got := currentSession.GetParamByName(name); got != "utf8" {
			t.Fatalf("%s after COM_CHANGE_USER = %v, want utf8", name, got)
		}
	}
	if got := currentSession.GetParamByName("collation_connection"); got != "utf8_general_ci" {
		t.Fatalf("collation_connection after COM_CHANGE_USER = %v, want utf8_general_ci", got)
	}
}

func TestHandleComChangeUserReadsLengthEncodedAuthResponse(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DevBypassPasswordAuth = true
	handler := &DecoupledMySQLMessageHandler{cfg: cfg, sessionMap: make(map[Session]server.MySQLServerSession)}
	session := NewMockSession("test_change_user_lenenc_auth")
	session.SetAttribute("auth_status", "success")
	session.SetAttribute("client_capabilities", uint32(protocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA|protocol.CLIENT_PROTOCOL_41|protocol.CLIENT_PLUGIN_AUTH))
	session.SetAttribute("auth_challenge", []byte("01234567890123456789"))
	currentSession := NewMySQLServerSession(session)

	body := []byte{common.COM_CHANGE_USER}
	body = append(body, []byte("new_user\x00")...)
	authResponse := make([]byte, 252)
	body = append(body, 0xfc, 0xfc, 0x00) // lenenc length 252
	body = append(body, authResponse...)
	body = append(body, []byte("new_db\x00")...)
	body = append(body, 0x21, 0x00)
	body = append(body, []byte("mysql_native_password\x00")...)

	if err := handler.handleComChangeUser(session, &currentSession, &MySQLPackage{Body: body}); err != nil {
		t.Fatalf("COM_CHANGE_USER failed: %v", err)
	}
	if got := currentSession.GetParamByName("database"); got != "new_db" {
		t.Fatalf("database after length-encoded COM_CHANGE_USER = %v, want new_db", got)
	}
}

func TestHandleComChangeUserRejectsTruncatedPacket(t *testing.T) {
	handler := &DecoupledMySQLMessageHandler{}
	session := NewMockSession("test_change_user_truncated")
	if err := handler.handleComChangeUser(session, nil, &MySQLPackage{Body: []byte{common.COM_CHANGE_USER}}); err != nil {
		t.Fatalf("truncated COM_CHANGE_USER returned transport error: %v", err)
	}
	if len(session.written) != 1 || session.written[0][4] != 0xff {
		t.Fatalf("truncated COM_CHANGE_USER response = %v, want ERR packet", session.written)
	}
}

// BenchmarkDecoupledHandler 性能基准测试
func BenchmarkDecoupledHandler(b *testing.B) {
	config := conf.NewCfg()
	handler := NewDecoupledMySQLMessageHandler(config)

	queryPacket := []byte{0x03, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		message, err := handler.protocolParser.ParsePacket(queryPacket, "test_simple_protocol-session")
		if err != nil {
			b.Fatalf("Parse failed: %v", err)
		}

		// 模拟消息处理
		_ = message
	}
}
