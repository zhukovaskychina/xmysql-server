package server

import (
	"time"
)

type MySQLServerSession interface {

	// GetLastActiveTime 获得当前连接最后一次活跃的时间
	GetLastActiveTime() time.Time

	SendOK()
	SendHandleOk()

	GetParamByName(name string) interface{}
	SetParamByName(name string, value interface{})

	SendSelectFields()

	// SessionContext 返回该连接的会话上下文，用于类型安全的读写；实现方必须返回非 nil。
	SessionContext() *SessionContext
}
