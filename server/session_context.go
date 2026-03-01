package server

import (
	"sync"
)

// SessionContext 表示单条 MySQL 连接上的会话级状态，是该连接上所有会话变量与语义状态的显式载体。
// 生命周期与连接一致，由协议层在创建 MySQLServerSession 时创建并持有。
// 多 goroutine 可能访问（net 读包与引擎执行），字段访问通过 RWMutex 保护。
type SessionContext struct {
	mu sync.RWMutex

	// 连接/会话标识（只读，创建时设定）
	ConnectionID uint32
	SessionID    string

	// 当前会话语义状态
	CurrentDB  string
	Username   string
	Host       string
	Autocommit bool

	// 会话变量（与 SET / 系统变量引擎对齐）
	CharacterSet         string
	SQLMode              string
	TimeZone             string
	TransactionIsolation string

	// 事务状态（预留）
	InTransaction bool

	// 未升格为字段的变量，兼容现有 GetParamByName(key)
	Extra map[string]interface{}
}

// NewSessionContext 创建会话上下文，SessionID 通常来自 session.Stat()。
// ConnectionID 可由协议层在握手后通过 SetConnectionID 设置。
func NewSessionContext(sessionID string) *SessionContext {
	return &SessionContext{
		SessionID:    sessionID,
		Autocommit:   true,
		CharacterSet: "utf8mb4",
		Extra:        make(map[string]interface{}),
	}
}

// SetConnectionID 在握手完成后由协议层设置，便于 @@connection_id 等。
func (c *SessionContext) SetConnectionID(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ConnectionID = id
}

func (c *SessionContext) GetCurrentDB() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.CurrentDB
}

func (c *SessionContext) SetCurrentDB(db string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CurrentDB = db
}

func (c *SessionContext) GetUsername() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Username
}

func (c *SessionContext) SetUsername(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Username = s
}

func (c *SessionContext) GetHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Host
}

func (c *SessionContext) SetHost(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Host = s
}

func (c *SessionContext) GetAutocommit() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Autocommit
}

func (c *SessionContext) SetAutocommit(b bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Autocommit = b
}

func (c *SessionContext) GetCharacterSet() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.CharacterSet
}

func (c *SessionContext) SetCharacterSet(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CharacterSet = s
}

func (c *SessionContext) GetSQLMode() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.SQLMode
}

func (c *SessionContext) SetSQLMode(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SQLMode = s
}

func (c *SessionContext) GetTimeZone() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.TimeZone
}

func (c *SessionContext) SetTimeZone(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TimeZone = s
}

func (c *SessionContext) GetTransactionIsolation() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.TransactionIsolation
}

func (c *SessionContext) SetTransactionIsolation(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TransactionIsolation = s
}

func (c *SessionContext) GetInTransaction() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.InTransaction
}

func (c *SessionContext) SetInTransaction(b bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.InTransaction = b
}

// GetExtra 从 Extra 中按 key 取值，用于未升格为字段的会话变量。
func (c *SessionContext) GetExtra(key string) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.Extra == nil {
		return nil
	}
	return c.Extra[key]
}

// SetExtra 写入 Extra，用于兼容 SetParamByName(key, value)。
func (c *SessionContext) SetExtra(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Extra == nil {
		c.Extra = make(map[string]interface{})
	}
	c.Extra[key] = value
}
