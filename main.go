package main

import (
	_ "context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"xmysql-server/initdb"
	"xmysql-server/server/conf"
	"xmysql-server/server/innodb/net"
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
	var (
		configPath = flag.String("configPath", "", "指定配置文件配置路径")
		initialize = flag.Bool("initialize", false, "初始化數據庫")
	)
	flag.Usage = func() {
		fmt.Print(help)
	}

	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if configPath == nil || *configPath == "" {
		fmt.Println(help)
		os.Exit(1)
	}
	var cfg *conf.Cfg
	cfg = conf.NewCfg()
	cfg.Load(&conf.CommandLineArgs{ConfigPath: *configPath})
	if *initialize == true {
		initdb.InitDBDir(cfg)
		os.Exit(1)
	}

	mysqlServer := net.NewMySQLServer(cfg)
	mysqlServer.Start()
}
