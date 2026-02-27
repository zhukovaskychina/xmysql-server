package main

import (
	"fmt"
	"net"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

func testSchemataQueries() {
	fmt.Println(" 测试XMySQL服务器INFORMATION_SCHEMA.SCHEMATA功能")

	// 连接到XMySQL服务器
	conn, err := net.Dial("tcp", "localhost:3309")
	if err != nil {
		logger.Debugf(" 连接失败: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println(" 成功连接到XMySQL服务器")

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// 读取握手包
	handshakeBuffer := make([]byte, 1024)
	n, err := conn.Read(handshakeBuffer)
	if err != nil {
		logger.Debugf(" 读取握手包失败: %v\n", err)
		return
	}

	logger.Debugf(" 收到握手包，长度: %d\n", n)

	// 发送认证包
	authPacket := []byte{
		// 包长度 (3字节) + 序号 (1字节)
		0x20, 0x00, 0x00, 0x01,
		// 客户端能力标志 (4字节)
		0x85, 0xa6, 0x03, 0x00,
		// 最大包大小 (4字节)
		0x00, 0x00, 0x00, 0x01,
		// 字符集 (1字节)
		0x21,
		// 保留字段 (23字节)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// 用户名 "root" + null终止符
		'r', 'o', 'o', 't', 0x00,
	}

	_, err = conn.Write(authPacket)
	if err != nil {
		logger.Debugf(" 发送认证包失败: %v\n", err)
		return
	}

	// 读取认证响应
	authResponse := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = conn.Read(authResponse)
	if err != nil {
		logger.Debugf(" 读取认证响应失败: %v\n", err)
		return
	}

	logger.Debugf(" 收到认证响应，长度: %d\n", n)

	// 检查认证是否成功
	if n > 4 && authResponse[4] == 0x00 {
		fmt.Println(" 认证成功")
	} else {
		logger.Debugf(" 认证失败，响应: %v\n", authResponse[:n])
		return
	}

	// 测试SCHEMATA查询
	testQueries := []string{
		"SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'demo_db'",
		"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA",
	}

	for i, query := range testQueries {
		logger.Debugf("\n 测试查询 %d: %s\n", i+1, query)

		// 构建查询包
		queryLen := len(query) + 1
		queryPacket := []byte{
			byte(queryLen), byte(queryLen >> 8), byte(queryLen >> 16), // 包长度
			0x00, // 序号
			0x03, // COM_QUERY
		}
		queryPacket = append(queryPacket, []byte(query)...)

		// 发送查询
		_, err = conn.Write(queryPacket)
		if err != nil {
			logger.Debugf(" 发送查询失败: %v\n", err)
			continue
		}

		// 读取响应
		responseBuffer := make([]byte, 4096)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))

		n, err := conn.Read(responseBuffer)
		if err != nil {
			logger.Debugf(" 读取查询响应失败: %v\n", err)
			continue
		}

		logger.Debugf(" 收到响应，长度: %d\n", n)

		if n > 4 {
			firstByte := responseBuffer[4]
			logger.Debugf(" 响应类型: 0x%02X\n", firstByte)

			if firstByte == 0x00 {
				fmt.Println(" 收到OK包")
			} else if firstByte == 0xFF {
				fmt.Println(" 收到错误包")
				if n > 7 {
					errorCode := int(responseBuffer[5]) | int(responseBuffer[6])<<8
					logger.Debugf(" 错误代码: %d\n", errorCode)
					if n > 9 {
						errorMsg := string(responseBuffer[9:n])
						logger.Debugf(" 错误信息: %s\n", errorMsg)
					}
				}
			} else {
				logger.Debugf(" 收到数据包，可能包含查询结果\n")

				// 尝试解析列数
				if n > 5 {
					columnCount := responseBuffer[4]
					logger.Debugf(" 列数: %d\n", columnCount)
				}

				// 显示原始响应数据（前100字节）
				displayLen := n
				if displayLen > 100 {
					displayLen = 100
				}
				logger.Debugf("📄 响应数据(前%d字节): %v\n", displayLen, responseBuffer[:displayLen])
			}
		}
	}

	fmt.Println("\n🎉 测试完成")
}

func main() {
	testSchemataQueries()
}
