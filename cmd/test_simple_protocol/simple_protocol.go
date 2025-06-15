package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"
)

func main() {
	fmt.Println(" MySQL协议查询响应调试")

	// 连接到服务器
	conn, err := net.Dial("tcp", "127.0.0.1:3309")
	if err != nil {
		logger.Debugf(" 连接失败: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println(" TCP连接成功")

	// 读取握手包
	handshake := make([]byte, 1024)
	n, err := conn.Read(handshake)
	if err != nil {
		logger.Debugf(" 读取握手包失败: %v\n", err)
		return
	}

	logger.Debugf(" 收到握手包，长度: %d\n", n)

	// 发送认证包
	authPacket := []byte{
		// 包长度 (3字节) + 序号 (1字节)
		0x47, 0x00, 0x00, 0x01,
		// 客户端能力标志 (4字节)
		0x0d, 0xa6, 0x03, 0x00,
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
		// 认证响应长度
		0x14,
		// 认证响应数据 (20字节 SHA1)
		0x81, 0xF5, 0xE2, 0x1E, 0x35, 0x40, 0x7D, 0x88,
		0x4A, 0x6C, 0xD4, 0xA7, 0x31, 0xAE, 0xBF, 0xB6,
		0xAF, 0x20, 0x9E, 0x1B,
		// 数据库名 "mysql" + null终止符
		'm', 'y', 's', 'q', 'l', 0x00,
	}

	_, err = conn.Write(authPacket)
	if err != nil {
		logger.Debugf(" 发送认证包失败: %v\n", err)
		return
	}

	fmt.Println(" 认证包发送成功")

	// 读取认证响应
	authResponse := make([]byte, 1024)
	n, err = conn.Read(authResponse)
	if err != nil {
		logger.Debugf(" 读取认证响应失败: %v\n", err)
		return
	}

	logger.Debugf(" 收到认证响应，长度: %d\n", n)
	logger.Debugf(" 认证响应内容: %v\n", authResponse[:n])

	// 检查是否是OK包 (第5个字节应该是0x00)
	if n >= 5 && authResponse[4] == 0x00 {
		fmt.Println(" 认证成功 (OK包)")
	} else if n >= 5 && authResponse[4] == 0xFF {
		fmt.Println(" 认证失败 (ERROR包)")
		return
	} else {
		logger.Debugf(" 未知的认证响应类型: 0x%02X\n", authResponse[4])
	}

	// 发送查询包：SELECT 1
	querySQL := "SELECT 1"
	queryPayload := append([]byte{0x03}, []byte(querySQL)...) // 0x03 = COM_QUERY

	queryPacket := make([]byte, 4+len(queryPayload))
	queryPacket[0] = byte(len(queryPayload) & 0xff)
	queryPacket[1] = byte((len(queryPayload) >> 8) & 0xff)
	queryPacket[2] = byte((len(queryPayload) >> 16) & 0xff)
	queryPacket[3] = 0x00 // 查询包序号应该从0开始
	copy(queryPacket[4:], queryPayload)

	logger.Debugf(" 发送查询包，长度: %d, 序号: %d\n", len(queryPayload), 0)
	logger.Debugf(" 查询包内容: %v\n", queryPacket)

	n, err = conn.Write(queryPacket)
	if err != nil {
		logger.Debugf(" 发送查询包失败: %v\n", err)
		return
	}
	logger.Debugf(" 查询包发送成功，发送了 %d 字节\n", n)

	// 立即尝试读取响应，不等待
	err = readQueryResponse(conn)
	if err != nil {
		logger.Debugf(" 读取查询响应失败: %v\n", err)
		return
	}

	fmt.Println(" 查询测试完成")
}

// readQueryResponse 读取查询响应
func readQueryResponse(conn net.Conn) error {
	fmt.Println("📖 立即尝试读取响应...")

	// 设置较长的读取超时
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	reader := bufio.NewReader(conn)

	for i := 1; i <= 10; i++ { // 尝试读取最多10个包
		logger.Debugf("📖 尝试读取响应包%d...\n", i)

		// 读取包头
		header := make([]byte, 4)
		n, err := reader.Read(header)
		if err != nil {
			if err == io.EOF {
				logger.Debugf(" 读取响应包%d失败: EOF (可能服务器关闭了连接)\n", i)
			} else {
				logger.Debugf(" 读取响应包%d头部失败: %v (实际读取: %d字节)\n", i, err, n)
			}
			return err
		}

		if n != 4 {
			logger.Debugf(" 响应包%d头部不完整，期望4字节，实际: %d字节\n", i, n)
			return fmt.Errorf("incomplete header")
		}

		length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
		sequence := header[3]

		logger.Debugf(" 响应包%d - 长度: %d, 序号: %d, 头部: %v\n", i, length, sequence, header)

		if length > 16777215 { // MySQL包的最大长度
			logger.Debugf(" 响应包%d长度异常: %d\n", i, length)
			return fmt.Errorf("invalid packet length: %d", length)
		}

		// 读取包体
		payload := make([]byte, length)
		n, err = io.ReadFull(reader, payload)
		if err != nil {
			logger.Debugf(" 读取响应包%d包体失败: %v (期望: %d, 实际: %d)\n", i, err, length, n)
			return err
		}

		logger.Debugf(" 成功读取响应包%d，包体长度: %d\n", i, len(payload))
		logger.Debugf(" 包体内容: %v\n", payload)
		logger.Debugf(" 包体十六进制: %x\n", payload)

		if len(payload) > 0 {
			firstByte := payload[0]
			logger.Debugf(" 首字节: 0x%02x (%d)\n", firstByte, firstByte)

			switch firstByte {
			case 0x00: // OK包
				logger.Debugf(" 响应包%d是OK包\n", i)
				if len(payload) >= 7 {
					affectedRows := payload[1]
					lastInsertId := payload[2]
					statusFlags := uint16(payload[3]) | uint16(payload[4])<<8
					warnings := uint16(payload[5]) | uint16(payload[6])<<8
					logger.Debugf("  - 受影响行数: %d\n", affectedRows)
					logger.Debugf("  - 最后插入ID: %d\n", lastInsertId)
					logger.Debugf("  - 状态标志: 0x%04x\n", statusFlags)
					logger.Debugf("  - 警告数: %d\n", warnings)
				}
				return nil

			case 0xFF: // ERROR包
				logger.Debugf(" 响应包%d是ERROR包\n", i)
				if len(payload) > 3 {
					errorCode := uint16(payload[1]) | uint16(payload[2])<<8
					sqlState := ""
					message := ""
					if len(payload) > 9 {
						sqlState = string(payload[4:9])
						message = string(payload[9:])
					}
					logger.Debugf("  - 错误代码: %d\n", errorCode)
					logger.Debugf("  - SQL状态: %s\n", sqlState)
					logger.Debugf("  - 错误消息: %s\n", message)
				}
				return fmt.Errorf("server error")

			case 0xFE: // EOF包
				logger.Debugf("📄 响应包%d是EOF包\n", i)
				if len(payload) >= 5 {
					warnings := uint16(payload[1]) | uint16(payload[2])<<8
					statusFlags := uint16(payload[3]) | uint16(payload[4])<<8
					logger.Debugf("  - 警告数: %d\n", warnings)
					logger.Debugf("  - 状态标志: 0x%04x\n", statusFlags)
				}
				return nil

			default:
				if firstByte >= 1 && firstByte <= 250 {
					logger.Debugf(" 响应包%d是结果集包（列数: %d）\n", i, firstByte)
					// 继续读取更多包（列定义、行数据等）
					continue
				} else {
					logger.Debugf("❓ 响应包%d类型未知: 0x%02x\n", i, firstByte)
				}
			}
		}

		// 如果包长度为0或者是某些特殊情况，可能还有更多包
		if length == 0 {
			break
		}
	}

	fmt.Println(" 查询响应读取完成")
	return nil
}
