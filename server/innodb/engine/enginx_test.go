package engine

import (
	"fmt"
	"testing"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/parser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/util/charset"
)

func TestEngine(t *testing.T) {
	t.Parallel()
	t.Run("testEngine", func(t *testing.T) {
		t.Parallel()
		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"

	})
}
func TestSQLParser(t *testing.T) {
	t.Parallel()
	t.Run("primaryExtent", func(t *testing.T) {
		sql := "select * from INNODB_SYS_TABLES where TABLE_ID>30 and TABLE_ID <100 "

		conf := conf.NewCfg()
		conf.DataDir = "/Users/zhukovasky/xmysql/data"
		conf.BaseDir = "/Users/zhukovasky/xmysql"

		var fileSystem = basic.NewFileSystem(conf)
		fileSystem.AddTableSpace(store.NewSysTableSpace(conf, false))
		var bufferPool = buffer_pool.NewBufferPool(256*16384,
			0.75, 0.25,
			1000, fileSystem)
		infos := store.NewInfoSchemaManager(conf, bufferPool)

		currentSession, _ := createSession(infos)
		currentSession.sessionVars.CurrentDB = "INFORMATION_SCHEMAS"
		nodes, _ := currentSession.ParseSingleSQL(sql, charset.CharsetUTF8, charset.CollationUTF8MB4)
		fmt.Println(nodes)
		switch nodes := nodes.(type) {
		case *ast.SelectStmt:
			{
				fmt.Println(nodes)
				fmt.Println(nodes.Where)
				currentPlan, err := Compile(currentSession, nodes)
				fmt.Println(err)
				fmt.Println(currentPlan)
				fmt.Println(currentPlan.ID())
				//logicalPlan,_:=Compile(context.Background(),nodes)
				//fmt.Println(logicalPlan)
			}
		}

	})

	t.Run("createTable", func(t *testing.T) {
		sql := "CREATE  TABLE `INNODB_SYS_TABLES` (\n  `TABLE_ID` bigint(21) unsigned NOT NULL DEFAULT '0',\n  `NAME` varchar(655) NOT NULL DEFAULT '',\n  `FLAG` int(11) NOT NULL DEFAULT '0',\n  `N_COLS` int(11) NOT NULL DEFAULT '0',\n  `SPACE` int(11) NOT NULL DEFAULT '0',\n  `FILE_FORMAT` varchar(10) DEFAULT NULL,\n  `ROW_FORMAT` varchar(12) DEFAULT NULL,\n  `ZIP_PAGE_SIZE` int(11) unsigned NOT NULL DEFAULT '0',\n  `SPACE_TYPE` varchar(10) DEFAULT NULL\n) ENGINE=MEMORY DEFAULT CHARSET=utf8;"
		parser := parser.New()
		nodes, _ := parser.ParseOneStmt(sql, charset.CharsetUTF8, charset.CollationUTF8MB4)
		fmt.Println(nodes)
		switch nodes := nodes.(type) {
		case *ast.CreateTableStmt:
			{
				fmt.Println(nodes)
				//fmt.Println(nodes.Where)
			}
		}

	})
}
