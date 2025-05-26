package net

import (
	"net"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

// MockSession 模拟会话用于测试
type MockSession struct {
	id         string
	attributes map[string]interface{}
	closed     bool
}

func NewMockSession(id string) *MockSession {
	return &MockSession{
		id:         id,
		attributes: make(map[string]interface{}),
		closed:     false,
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
	// 模拟写入数据
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
	session := NewMockSession("test-session-1")

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
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_REQUEST, "test-session", "SELECT 1"),
		SQL:         "SELECT 1",
		Database:    "test",
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

	message, err := handler.protocolParser.ParsePacket(queryPacket, "test-session")
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
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_QUERY_RESPONSE, "test-session", result),
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

// BenchmarkDecoupledHandler 性能基准测试
func BenchmarkDecoupledHandler(b *testing.B) {
	config := conf.NewCfg()
	handler := NewDecoupledMySQLMessageHandler(config)

	queryPacket := []byte{0x03, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		message, err := handler.protocolParser.ParsePacket(queryPacket, "test-session")
		if err != nil {
			b.Fatalf("Parse failed: %v", err)
		}

		// 模拟消息处理
		_ = message
	}
}
