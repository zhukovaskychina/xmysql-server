package innodb

import (
	"time"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/context"
	"github.com/zhukovaskychina/xmysql-server/server/mysql"
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
