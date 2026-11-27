package net

import "github.com/zhukovaskychina/xmysql-server/server"

const mysqlSessionAttribute = "__mysql_server_session"

// recordOSThreadID captures the OS thread executing the protocol callback.
// Go may move a connection's work between goroutines, so the value is
// refreshed at the boundary rather than inferred from the MySQL connection ID.
func recordOSThreadID(session server.MySQLServerSession) {
	if session == nil {
		return
	}
	if id := currentOSThreadID(); id > 0 {
		session.SetParamByName("__thread_os_id", id)
	}
}
