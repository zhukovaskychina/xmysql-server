package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

func main() {
	fmt.Println("🔌 MySQL连接测试工具")
	fmt.Println("======================")

	// 连接到服务器
	conn, err := net.DialTimeout("tcp", "localhost:3309", 5*time.Second)
	if err != nil {
		logger.Debugf(" 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println(" TCP连接建立成功")

	// 读取握手包
	reader := bufio.NewReader(conn)

	// 读取包头（4字节）
	header := make([]byte, 4)
	_, err = reader.Read(header)
	if err != nil {
		logger.Debugf(" 读取包头失败: %v\n", err)
		return
	}

	// 解析包长度
	length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
	seqID := header[3]

	logger.Debugf(" 握手包: Length=%d, Sequence ID=%d\n", length, seqID)

	// 读取包体
	payload := make([]byte, length)
	_, err = reader.Read(payload)
	if err != nil {
		logger.Debugf(" 读取包体失败: %v\n", err)
		return
	}

	logger.Debugf(" 握手包接收成功 (前16字节): %x\n", payload[:min(16, len(payload))])

	// 提取服务器信息
	if len(payload) > 1 {
		protocolVersion := payload[0]
		logger.Debugf(" 协议版本: %d\n", protocolVersion)

		// 查找服务器版本字符串（以null结尾）
		var serverVersion string
		for i := 1; i < len(payload) && i < 50; i++ {
			if payload[i] == 0 {
				serverVersion = string(payload[1:i])
				break
			}
		}
		logger.Debugf(" 服务器版本: %s\n", serverVersion)
	}

	fmt.Println(" 握手成功，服务器运行正常")
	fmt.Println("\n💡 测试结果：")
	fmt.Println("  - TCP连接： 成功")
	fmt.Println("  - 握手包： 接收成功")
	fmt.Println("  - 服务器： 正常运行")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
