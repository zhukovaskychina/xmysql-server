package session

import (
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"
)

// SessionImpl 会话实现
type SessionImpl struct {
	id           string
	user         string
	database     string
	conn         net.Conn
	lastActivity time.Time
	attributes   map[string]interface{}
	mutex        sync.RWMutex
	closed       bool
}

// NewSessionImpl 创建新的会话实例
func NewSessionImpl(conn net.Conn, user, database string) Session {
	return &SessionImpl{
		id:           generateSessionID(),
		user:         user,
		database:     database,
		conn:         conn,
		lastActivity: time.Now(),
		attributes:   make(map[string]interface{}),
		closed:       false,
	}
}

// ID 返回会话ID
func (s *SessionImpl) ID() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.id
}

// User 返回用户名
func (s *SessionImpl) User() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.user
}

// Database 返回当前数据库
func (s *SessionImpl) Database() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.database
}

// SetDatabase 设置当前数据库
func (s *SessionImpl) SetDatabase(db string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.database = db
	s.lastActivity = time.Now()
}

// Connection 返回网络连接
func (s *SessionImpl) Connection() net.Conn {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.conn
}

// LastActivity 返回最后活动时间
func (s *SessionImpl) LastActivity() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastActivity
}

// UpdateActivity 更新活动时间
func (s *SessionImpl) UpdateActivity() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastActivity = time.Now()
}

// IsExpired 检查会话是否过期
func (s *SessionImpl) IsExpired(timeout time.Duration) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return time.Since(s.lastActivity) > timeout
}

// GetAttribute 获取会话属性
func (s *SessionImpl) GetAttribute(key string) interface{} {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.attributes[key]
}

// SetAttribute 设置会话属性
func (s *SessionImpl) SetAttribute(key string, value interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.attributes[key] = value
	s.lastActivity = time.Now()
}

// Close 关闭会话
func (s *SessionImpl) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	// 关闭网络连接
	if s.conn != nil {
		return s.conn.Close()
	}

	return nil
}

// generateSessionID 生成会话ID
func generateSessionID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return fmt.Sprintf("%x", bytes)
}
