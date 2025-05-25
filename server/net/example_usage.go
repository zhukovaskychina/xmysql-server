package net

import (
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

// ExampleUsage 展示如何使用解耦后的架构
func ExampleUsage() {
	// 1. 创建配置
	config := conf.NewCfg()

	// 2. 创建解耦的消息处理器（替代原来的MySQLMessageHandler）
	decoupledHandler := NewDecoupledMySQLMessageHandler(config)

	// 3. 在MySQL服务器中使用解耦的处理器
	// 原来的代码：
	// mysqlMsgHandler := NewMySQLMessageHandler(conf)

	// 现在的代码：
	mysqlMsgHandler := decoupledHandler

	// 4. 其他代码保持不变，因为DecoupledMySQLMessageHandler实现了相同的接口
	_ = mysqlMsgHandler
}

// 在mysql_server.go中的修改示例：
/*
func (srv *MySQLServer) initServer(conf *conf.Cfg) {
	var (
		addr     string
		portList []string
		server   Server
	)

	// 使用解耦的消息处理器
	mysqlMsgHandler := NewDecoupledMySQLMessageHandler(conf)  // 替换这一行

	portList = append(portList, strconv.Itoa(conf.Port))
	// ... 其他代码保持不变
}
*/
