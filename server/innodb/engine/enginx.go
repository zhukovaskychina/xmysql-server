package engine

import (
	"github.com/goioc/di"
	log "github.com/sirupsen/logrus"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	"github.com/zhukovaskychina/xmysql-server/server/mysql"
	"time"
)

//SQL执行引擎
//默认一个实例
type XMySQLEngine struct {
	conf *conf.Cfg
	//定义查询线程
	//	QueryExecutor *XMySQLExecutor
	//定义purge线程
	//定义SchemaManager
	infoSchemaManager schemas.InfoSchema

	pool *buffer_pool.BufferPool
}

func NewXMySQLEngine(conf *conf.Cfg) *XMySQLEngine {
	var mysqlEngine = new(XMySQLEngine)
	mysqlEngine.conf = conf
	var fileSystem = basic.NewFileSystem(conf)
	fileSystem.AddTableSpace(store.NewSysTableSpace(conf, false))
	var bufferPool = buffer_pool.NewBufferPool(256*16384,
		0.75, 0.25,
		1000, fileSystem)
	mysqlEngine.pool = bufferPool
	mysqlEngine.infoSchemaManager = store.NewInfoSchemaManager(conf, bufferPool)
	mysqlEngine.initPurgeThread()

	di.RegisterBeanInstance("buffer_pool", bufferPool)
	di.RegisterBeanInstance("infoSchemanager", mysqlEngine.infoSchemaManager)
	return mysqlEngine
}

func (srv *XMySQLEngine) initPurgeThread() {
	go srv.flushToDisk()
}

func (srv *XMySQLEngine) flushToDisk() {
	//count := 0
	timeTicker := time.NewTicker(1 * time.Second)
	for {
		<-timeTicker.C
		blockBuffer := srv.pool.GetFlushDiskList().GetLastBlock()
		if blockBuffer == nil {
			log.Info("没有页面可以刷新")
		} else {
			log.Info("刷新脏页面")
			purgeThread(srv.pool.FileSystem, blockBuffer.GetSpaceId(), blockBuffer.GetPageNo(), blockBuffer)
		}

	}
}

func purgeThread(system basic.FileSystem, spaceId uint32, pageNo uint32, block *buffer_pool.BufferBlock) {
	ts := system.GetTableSpaceById(spaceId)
	ts.FlushToDisk(pageNo, *(block.GetFrame()))
}

//ast->plan->storebytes->result->net
func (srv *XMySQLEngine) ExecuteQuery(session innodb.MySQLServerSession, query string) {

	stmt, err := session.ParseOneSQL(query, mysql.UTF8Charset, mysql.UTF8DefaultCollation)
	if err != nil {
		session.SendError(mysql.NewErr(mysql.ErrSyntax, err))
		return
	}
	Compile(session, stmt)
	switch stmt.(type) {
	case *ast.SelectStmt:
		{

		}
	case *ast.CreateTableStmt:
		{

		}
	case *ast.CreateDatabaseStmt:
		{

		}
	case *ast.CreateIndexStmt:
		{

		}
	case *ast.InsertStmt:
		{

		}
	case *ast.UpdateStmt:
		{

		}
	case *ast.DeleteStmt:
		{

		}

	}
}
