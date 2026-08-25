package main

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
)

func main() {
	// 运行持久化演示
	if err := engine.RunPersistenceDemo(); err != nil {
		fmt.Printf("运行持久化演示失败: %v\n", err)
	}
}
