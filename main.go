package main

import (
	"flag"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/net"
)

const help = `
******************************************************************************************

 __   ____  __        _____  ____  _          _____ ______ _______      ________ _____  
 \ \ / /  \/  |      / ____|/ __ \| |        / ____|  ____|  __ \ \    / /  ____|  __ \ 
  \ V /| \  / |_   _| (___ | |  | | |  _____| (___ | |__  | |__) \ \  / /| |__  | |__) |
   > < | |\/| | | | |\___ \| |  | | | |______\___ \|  __| |  _  / \ \/ / |  __| |  _  / 
  / . \| |  | | |_| |____) | |__| | |____    ____) | |____| | \ \  \  /  | |____| | \ \ 
 /_/ \_\_|  |_|\__, |_____/ \___\_\______|  |_____/|______|_|  \_\  \/   |______|_|  \_\
                __/ |                                                                   
               |___/                                                                    
******************************************************************************************
*帮助: 																					 
*1. -- help																				 
*2. -- configPath   指定my.ini配置文件													 
*3. -- initialize   初始化数据库															 
******************************************************************************************
`

func main() {
	fmt.Println("Starting XMySQL Server...")

	// 解析命令行参数
	fmt.Println("Parsing command line arguments...")
	var configPath string
	flag.StringVar(&configPath, "configPath", "", "配置文件路径")
	flag.Parse()

	args := &conf.CommandLineArgs{
		ConfigPath: configPath,
	}

	config := conf.NewCfg().Load(args)
	logger.Debugf("Config loaded: error_log=%s, info_log=%s\n", config.LogError, config.LogInfos)

	// 初始化日志
	logger.Info("Initializing logger...")
	logConfig := logger.LogConfig{
		ErrorLogPath: config.LogError,
		InfoLogPath:  config.LogInfos,
		LogLevel:     config.LogLevel,
	}

	if err := logger.InitLogger(logConfig); err != nil {
		logger.Debugf("Failed to initialize logger: %s\n", err.Error())
		panic("Failed to initialize logger: " + err.Error())
	}
	logger.Info("Logger initialized successfully with level: %s\n", config.LogLevel)

	logger.Info("XMySQL Server starting...")
	// 使用现有的网络层实现，已经集成了分层架构：
	// 网络层(net) -> 协议层(protocol) -> SQL分发层(dispatcher) -> 引擎层(innodb/engine)
	mysqlServer := net.NewMySQLServer(config)
	logger.Info("Starting MySQL server...")
	mysqlServer.Start()
	logger.Info("success")
	logger.Info("Server started successfully")
}
