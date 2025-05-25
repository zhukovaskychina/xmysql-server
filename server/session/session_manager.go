package session

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// SessionManager 会话管理器接口
type SessionManager interface {
	CreateSession(conn net.Conn, user, database string) (Session, error)
	GetSession(sessionID string) (Session, bool)
	GetSessionByConn(conn net.Conn) (Session, bool)
	CloseSession(sessionID string) error
	GetActiveSessions() []Session
	CleanupExpiredSessions()
}

// Session 会话接口
type Session interface {
	ID() string
	User() string
	Database() string
	SetDatabase(db string)
	Connection() net.Conn
	LastActivity() time.Time
	UpdateActivity()
	IsExpired(timeout time.Duration) bool
	GetAttribute(key string) interface{}
	SetAttribute(key string, value interface{})
	Close() error
}

// SessionManagerImpl 会话管理器实现
type SessionManagerImpl struct {
	sessions       map[string]Session
	connSessions   map[net.Conn]Session
	mutex          sync.RWMutex
	maxSessions    int
	sessionTimeout time.Duration
}

// NewSessionManager 创建会话管理器
func NewSessionManager(maxSessions int, sessionTimeout time.Duration) SessionManager {
	mgr := &SessionManagerImpl{
		sessions:       make(map[string]Session),
		connSessions:   make(map[net.Conn]Session),
		maxSessions:    maxSessions,
		sessionTimeout: sessionTimeout,
	}

	// 启动清理协程
	go mgr.cleanupRoutine()

	return mgr
}

// CreateSession 创建新会话
func (sm *SessionManagerImpl) CreateSession(conn net.Conn, user, database string) (Session, error) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// 检查会话数量限制
	if len(sm.sessions) >= sm.maxSessions {
		return nil, fmt.Errorf("too many active sessions")
	}

	// 检查连接是否已有会话
	if _, exists := sm.connSessions[conn]; exists {
		return nil, fmt.Errorf("connection already has a session")
	}

	// 创建新会话
	session := NewSessionImpl(conn, user, database)

	sm.sessions[session.ID()] = session
	sm.connSessions[conn] = session

	return session, nil
}

// GetSession 根据ID获取会话
func (sm *SessionManagerImpl) GetSession(sessionID string) (Session, bool) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	session, exists := sm.sessions[sessionID]
	return session, exists
}

// GetSessionByConn 根据连接获取会话
func (sm *SessionManagerImpl) GetSessionByConn(conn net.Conn) (Session, bool) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	session, exists := sm.connSessions[conn]
	return session, exists
}

// CloseSession 关闭会话
func (sm *SessionManagerImpl) CloseSession(sessionID string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	session, exists := sm.sessions[sessionID]
	if !exists {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	// 关闭会话
	if err := session.Close(); err != nil {
		return fmt.Errorf("failed to close session: %w", err)
	}

	// 从映射中删除
	delete(sm.sessions, sessionID)
	delete(sm.connSessions, session.Connection())

	return nil
}

// GetActiveSessions 获取所有活跃会话
func (sm *SessionManagerImpl) GetActiveSessions() []Session {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	sessions := make([]Session, 0, len(sm.sessions))
	for _, session := range sm.sessions {
		sessions = append(sessions, session)
	}

	return sessions
}

// CleanupExpiredSessions 清理过期会话
func (sm *SessionManagerImpl) CleanupExpiredSessions() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	expiredSessions := make([]string, 0)

	for sessionID, session := range sm.sessions {
		if session.IsExpired(sm.sessionTimeout) {
			expiredSessions = append(expiredSessions, sessionID)
		}
	}

	// 删除过期会话
	for _, sessionID := range expiredSessions {
		if session, exists := sm.sessions[sessionID]; exists {
			session.Close()
			delete(sm.sessions, sessionID)
			delete(sm.connSessions, session.Connection())
		}
	}
}

// cleanupRoutine 清理协程
func (sm *SessionManagerImpl) cleanupRoutine() {
	ticker := time.NewTicker(time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for range ticker.C {
		sm.CleanupExpiredSessions()
	}
}
