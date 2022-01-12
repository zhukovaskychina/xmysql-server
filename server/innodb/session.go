package innodb

import (
	"time"
	"xmysql-server/server/innodb/ast"
	"xmysql-server/server/innodb/context"
	"xmysql-server/server/mysql"
)

//
type MySQLServerSession interface {

	//获得当前链接最后一次活跃的时间

	GetLastActiveTime() time.Time

	SendOK()

	SendHandleOk()

	SendError(error *mysql.SQLError)

	GetCurrentDataBase() string

	SetCurrentDatabase(databaseName string)

	ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error)

	ParseOneSQL(sql, charset, collation string) (ast.StmtNode, error)

	PrepareTxnCtx()

	Commit()

	context.Context
}
