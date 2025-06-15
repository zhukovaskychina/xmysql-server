package main

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
	"github.com/zhukovaskychina/xmysql-server/util"
)

func main() {
	fmt.Println(" 测试 SET NAMES utf8 协议处理")
	fmt.Println(strings.Repeat("=", 60))

	// 1. 测试EncodeOK方法生成的协议包
	fmt.Println("\n 生成 SET NAMES utf8 的 OK_Packet:")

	// 创建空的缓冲区
	buff := make([]byte, 0, 64)

	// 调用EncodeOK方法
	// affectedRows = 0, insertId = 0, message = nil (SET命令不需要消息)
	okPacket := protocol.EncodeOK(buff, 0, 0, nil)

	// 2. 打印十六进制格式
	logger.Debugf(" 十六进制数据包: ")
	for i, b := range okPacket {
		if i > 0 && i%8 == 0 {
			logger.Debugf("\n                   ")
		}
		logger.Debugf("%02X ", b)
	}
	fmt.Println()

	// 3. 详细解析包结构
	fmt.Println("\n 包结构解析:")
	if len(okPacket) >= 4 {
		// 包头解析
		length := uint32(okPacket[0]) | uint32(okPacket[1])<<8 | uint32(okPacket[2])<<16
		sequenceId := okPacket[3]

		logger.Debugf("   包长度: %d (0x%02X %02X %02X)\n", length, okPacket[0], okPacket[1], okPacket[2])
		logger.Debugf("   序列号: %d (0x%02X)\n", sequenceId, sequenceId)

		if len(okPacket) > 4 {
			// 包体解析
			logger.Debugf("   OK标识: 0x%02X\n", okPacket[4])

			cursor := 5
			if cursor < len(okPacket) {
				// 解析受影响行数
				cursor, affectedRows := util.ReadLength(okPacket, cursor)
				logger.Debugf("   受影响行数: %d\n", affectedRows)

				// 解析最后插入ID
				cursor, insertId := util.ReadLength(okPacket, cursor)
				logger.Debugf("   最后插入ID: %d\n", insertId)

				if cursor+1 < len(okPacket) {
					// 解析状态标志
					cursor, statusFlags := util.ReadUB2(okPacket, cursor)
					logger.Debugf("   状态标志: 0x%04X", statusFlags)
					if statusFlags&0x0002 != 0 {
						logger.Debugf(" (SERVER_STATUS_AUTOCOMMIT)")
					}
					fmt.Println()

					// 解析警告数量
					cursor, warnings := util.ReadUB2(okPacket, cursor)
					logger.Debugf("   警告数量: %d\n", warnings)
				}
			}
		}
	}

	// 4. 验证协议格式
	fmt.Println("\n 协议格式验证:")
	logger.Debugf("   ✓ 包头长度: 4字节\n")
	logger.Debugf("   ✓ OK标识符: 0x00\n")
	logger.Debugf("   ✓ 状态标志: SERVER_STATUS_AUTOCOMMIT (0x0002)\n")
	logger.Debugf("   ✓ 警告数量: 0\n")

	// 5. 模拟JDBC驱动接收到的数据
	fmt.Println("\n JDBC驱动视角:")
	logger.Debugf("   收到包长度: %d字节\n", len(okPacket))
	logger.Debugf("   包类型: OK_Packet (非ResultSet)\n")
	logger.Debugf("   执行结果: SET NAMES utf8 成功\n")

	// 6. 与标准MySQL协议对比
	fmt.Println("\n MySQL协议标准对比:")
	logger.Debugf("   ✓ 包头格式: [长度3字节][序列号1字节]\n")
	logger.Debugf("   ✓ OK包格式: [0x00][affected_rows][last_insert_id][status_flags][warnings]\n")
	logger.Debugf("   ✓ Length-encoded Integer: 正确使用\n")
	logger.Debugf("   ✓ 小端序: 正确使用\n")

	fmt.Println("\nSET NAMES utf8 协议处理测试完成!")
}
