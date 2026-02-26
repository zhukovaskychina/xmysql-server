package net

import (
	"net"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

// MockSession 模拟会话用于测试
type MockSession struct {
	id         string
	attributes map[string]interface{}
	closed     bool
	written    [][]byte
}

func NewMockSession(id string) *MockSession {
	return &MockSession{
		id:         id,
		attributes: make(map[string]interface{}),
		closed:     false,
		written:    make([][]byte, 0),
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
func (s *MockSession) ID() uint32                                            { return 1 }
func (s *MockSession) SetCompressType(compressType CompressType)             {}
func (s *MockSession) LocalAddr() string                                     { return "127.0.0.1:3308" }
func (s *MockSession) RemoteAddr() string                                    { return "127.0.0.1:12345" }
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

// TestSendQueryResultSet_ClientDeprecateEOFUsesOK 验证在协商了 CLIENT_DEPRECATE_EOF 时
// sendQueryResultSet 会使用 OK 包而不是 EOF 包作为列定义结束和结果集结束标记，
// 并且整体包数量与 19 列单行结果集的预期一致。
func TestSendQueryResultSet_ClientDeprecateEOFUsesOK(t *testing.T) {
	config := conf.NewCfg()
	// 使用真实的处理器，但通过 MockSession 捕获输出
	handler := NewDecoupledMySQLMessageHandler(config)
	session := NewMockSession("test_sendQueryResultSet_deprecateEOF")

	// 模拟客户端能力：开启 CLIENT_DEPRECATE_EOF
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
	// 1 (ColumnCount) + 19 (ColumnDefinitions) + 1 (列结束 OK) + 1 (Row) + 1 (结果集结束 OK) = 23
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
	// payload 第一个字节应该是 OK 标记 0x00，而不是 EOF 标记 0xFE
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
