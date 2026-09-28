package net

import (
	"testing"

	gxsync "github.com/dubbogo/gost/sync"
	"github.com/stretchr/testify/require"
)

type osThreadTestEndpoint struct{}

func (osThreadTestEndpoint) ID() EndPointID                      { return 1 }
func (osThreadTestEndpoint) EndPointType() EndPointType          { return TCP_SERVER }
func (osThreadTestEndpoint) RunEventLoop(NewSessionCallback)     {}
func (osThreadTestEndpoint) IsClosed() bool                      { return false }
func (osThreadTestEndpoint) Close()                              {}
func (osThreadTestEndpoint) GetTaskPool() gxsync.GenericTaskPool { return nil }

type osThreadTestListener struct{}

func (osThreadTestListener) OnOpen(Session) error           { return nil }
func (osThreadTestListener) OnClose(Session)                {}
func (osThreadTestListener) OnError(Session, error)         {}
func (osThreadTestListener) OnCron(Session)                 {}
func (osThreadTestListener) OnMessage(Session, interface{}) {}

func TestCurrentOSThreadIDIsAvailableAtProtocolBoundary(t *testing.T) {
	if got := currentOSThreadID(); got <= 0 {
		t.Fatalf("currentOSThreadID() = %d, want a positive OS thread id", got)
	}
}

func TestRecordOSThreadIDStoresIDOnMySQLSession(t *testing.T) {
	session := NewMySQLServerSession(NewMockSession("os-thread-id"))
	recordOSThreadID(session)
	require.Greater(t, session.GetParamByName("__thread_os_id"), int64(0))
}

func TestSessionTaskCapturesOSThreadIDBeforeProtocolCallback(t *testing.T) {
	transport := NewMockSession("os-thread-boundary")
	ss := newSession(osThreadTestEndpoint{}, transport)
	ss.listener = osThreadTestListener{}
	mysqlSession := NewMySQLServerSession(transport)
	ss.SetAttribute(mysqlSessionAttribute, mysqlSession)

	ss.addTask("query")
	require.Greater(t, mysqlSession.GetParamByName("__thread_os_id"), int64(0))
}
