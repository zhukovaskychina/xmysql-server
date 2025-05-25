package net

import (
	"fmt"
	"sync"
	"time"

	log "github.com/AlexStocks/log4go"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
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
}

// NewDecoupledMySQLMessageHandler 创建解耦的MySQL消息处理器
func NewDecoupledMySQLMessageHandler(cfg *conf.Cfg) *DecoupledMySQLMessageHandler {
	handler := &DecoupledMySQLMessageHandler{
		sessionMap:      make(map[Session]server.MySQLServerSession),
		cfg:             cfg,
		protocolParser:  protocol.NewMySQLProtocolParser(),
		protocolEncoder: protocol.NewMySQLProtocolEncoder(),
		messageBus:      protocol.NewDefaultMessageBus(),
		businessHandler: dispatcher.NewBusinessMessageHandler(cfg),
	}

	// 注册业务处理器到消息总线
	handler.registerBusinessHandlers()

	return handler
}

// registerBusinessHandlers 注册业务处理器
func (h *DecoupledMySQLMessageHandler) registerBusinessHandlers() {
	h.messageBus.Subscribe(protocol.MSG_CONNECT, h.businessHandler)
	h.messageBus.Subscribe(protocol.MSG_DISCONNECT, h.businessHandler)
	h.messageBus.Subscribe(protocol.MSG_QUERY_REQUEST, h.businessHandler)
	h.messageBus.Subscribe(protocol.MSG_AUTH_REQUEST, h.businessHandler)
	h.messageBus.Subscribe(protocol.MSG_USE_DB_REQUEST, h.businessHandler)
	h.messageBus.Subscribe(protocol.MSG_PING, h.businessHandler)
}

// OnOpen 连接打开事件
func (h *DecoupledMySQLMessageHandler) OnOpen(session Session) error {
	var err error

	h.rwlock.RLock()
	if h.cfg.SessionNumber <= len(h.sessionMap) {
		err = errTooManySessions
	}
	h.rwlock.RUnlock()

	if err != nil {
		return err
	}

	log.Info("got session:%s", session.Stat())

	h.rwlock.Lock()
	h.sessionMap[session] = NewMySQLServerSession(session)
	h.rwlock.Unlock()

	// 主动与客户端握手
	h.sessionMap[session].SendHandleOk()

	// 发送连接消息到业务层
	connectMsg := &protocol.ConnectMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_CONNECT, session.Stat(), nil),
		ClientInfo: &protocol.ClientInfo{
			Host: "127.0.0.1", // 从session中获取实际信息
			Port: 3306,
		},
	}

	// 异步处理连接消息
	go h.handleBusinessMessage(session, connectMsg)

	return nil
}

// OnClose 连接关闭事件
func (h *DecoupledMySQLMessageHandler) OnClose(session Session) {
	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()

	session.Close()

	// 发送断开连接消息到业务层
	disconnectMsg := protocol.NewBaseMessage(protocol.MSG_DISCONNECT, session.Stat(), nil)
	go h.handleBusinessMessage(session, disconnectMsg)
}

// OnError 连接错误事件
func (h *DecoupledMySQLMessageHandler) OnError(session Session, err error) {
	log.Error("Session error: %v", err)

	h.rwlock.Lock()
	delete(h.sessionMap, session)
	h.rwlock.Unlock()

	session.Close()
}

// OnCron 定时检查事件
func (h *DecoupledMySQLMessageHandler) OnCron(session Session) {
	// 定时检查会话状态
}

// OnMessage 消息处理事件
func (h *DecoupledMySQLMessageHandler) OnMessage(session Session, pkg interface{}) {
	recMySQLPkg, ok := pkg.(*MySQLPackage)
	if !ok {
		log.Error("Invalid package type: %T", pkg)
		return
	}

	currentMysqlSession, ok := h.sessionMap[session]
	if !ok {
		log.Error("Session not found: %v", session)
		return
	}

	if err := h.handlePacket(session, &currentMysqlSession, recMySQLPkg); err != nil {
		log.Error("Error handling packet: %v", err)
		session.Close()
	}
}

// handlePacket 处理MySQL包
func (h *DecoupledMySQLMessageHandler) handlePacket(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	authStatus := session.GetAttribute("auth_status")

	// 处理认证
	if authStatus == nil {
		return h.handleAuthentication(session, currentMysqlSession, recMySQLPkg)
	}

	// 已认证，解析协议包为消息
	if len(recMySQLPkg.Body) == 0 {
		return fmt.Errorf("empty packet body")
	}

	// 使用协议解析器解析包
	message, err := h.protocolParser.ParsePacket(recMySQLPkg.Body, session.Stat())
	if err != nil {
		log.Error("Failed to parse packet: %v", err)
		return h.sendErrorResponse(session, 1064, "42000", "Protocol parse error")
	}

	// 异步处理业务消息
	go h.handleBusinessMessage(session, message)

	return nil
}

// handleAuthentication 处理认证
func (h *DecoupledMySQLMessageHandler) handleAuthentication(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	// 构造认证数据
	authData := make([]byte, 0, len(recMySQLPkg.Header.PacketLength)+1+len(recMySQLPkg.Body))
	authData = append(authData, recMySQLPkg.Header.PacketLength...)
	authData = append(authData, recMySQLPkg.Header.PacketId)
	authData = append(authData, recMySQLPkg.Body...)

	// 使用认证包解析器
	authParser := &protocol.AuthPacketParser{}
	authMessage, err := authParser.Parse(authData, session.Stat())
	if err != nil {
		return fmt.Errorf("failed to parse auth packet: %v", err)
	}

	// 异步处理认证消息
	go h.handleBusinessMessage(session, authMessage)

	return nil
}

// handleBusinessMessage 处理业务消息
func (h *DecoupledMySQLMessageHandler) handleBusinessMessage(session Session, message protocol.Message) {
	// 通过消息总线异步处理
	responseChan := h.messageBus.PublishAsync(message)

	// 等待响应并发送给客户端
	go func() {
		select {
		case response := <-responseChan:
			if response != nil {
				h.sendResponse(session, response)
			}
		case <-time.After(30 * time.Second): // 超时处理
			log.Error("Business message handling timeout for session: %s", session.Stat())
			h.sendErrorResponse(session, 1205, "HY000", "Request timeout")
		}
	}()
}

// sendResponse 发送响应
func (h *DecoupledMySQLMessageHandler) sendResponse(session Session, response protocol.Message) {
	// 使用协议编码器编码响应
	data, err := h.protocolEncoder.EncodeMessage(response)
	if err != nil {
		log.Error("Failed to encode response: %v", err)
		h.sendErrorResponse(session, 1064, "42000", "Response encoding error")
		return
	}

	// 发送数据
	if err := session.WriteBytes(data); err != nil {
		log.Error("Failed to write response: %v", err)
	}

	// 处理特殊响应类型
	h.handleSpecialResponse(session, response)
}

// handleSpecialResponse 处理特殊响应类型
func (h *DecoupledMySQLMessageHandler) handleSpecialResponse(session Session, response protocol.Message) {
	switch response.Type() {
	case protocol.MSG_AUTH_RESPONSE:
		// 认证成功，设置会话状态
		session.SetAttribute("auth_status", "success")
		if authMsg, ok := response.Payload().(*protocol.AuthMessage); ok {
			h.rwlock.Lock()
			if mysqlSession, exists := h.sessionMap[session]; exists {
				mysqlSession.SetParamByName("database", authMsg.Database)
				mysqlSession.SetParamByName("user", authMsg.User)
			}
			h.rwlock.Unlock()
		}

	case protocol.MSG_USE_DB_RESPONSE:
		// 数据库切换成功，更新会话信息
		if useDBMsg, ok := response.Payload().(*protocol.UseDBMessage); ok {
			h.rwlock.Lock()
			if mysqlSession, exists := h.sessionMap[session]; exists {
				mysqlSession.SetParamByName("database", useDBMsg.Database)
			}
			h.rwlock.Unlock()
		}
	}
}

// sendErrorResponse 发送错误响应
func (h *DecoupledMySQLMessageHandler) sendErrorResponse(session Session, code uint16, state, message string) error {
	errorMsg := &protocol.ErrorMessage{
		BaseMessage: protocol.NewBaseMessage(protocol.MSG_ERROR, session.Stat(), nil),
		Code:        code,
		State:       state,
		Message:     message,
	}

	data, err := h.protocolEncoder.EncodeMessage(errorMsg)
	if err != nil {
		log.Error("Failed to encode error response: %v", err)
		return err
	}

	return session.WriteBytes(data)
}
