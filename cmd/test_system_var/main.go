package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println(" 测试mysql-connector-java系统变量查询")

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
		// 用户名 "root" + null terminator
		'r', 'o', 'o', 't', 0x00,
	}

	// 发送认证包
	_, err = conn.Write(authPacket)
	if err != nil {
		logger.Debugf(" 发送认证包失败: %v\n", err)
		return
	}

	// 读取认证响应
	authResponseBuffer := make([]byte, 1024)
	n, err = conn.Read(authResponseBuffer)
	if err != nil {
		logger.Debugf(" 读取认证响应失败: %v\n", err)
		return
	}

	logger.Debugf(" 收到认证响应，长度: %d\n", n)

	// 检查是否认证成功（简单检查）
	if n >= 7 && authResponseBuffer[4] == 0x00 {
		fmt.Println(" 认证成功")
	} else {
		logger.Debugf(" 认证失败，响应: %v\n", authResponseBuffer[:n])
		return
	}

	// 测试mysql-connector-java的系统变量查询
	systemVariableQuery := `SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_buffer_length AS net_buffer_length, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@tx_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout`

	fmt.Println(" 发送系统变量查询...")

	// 构造查询包
	queryData := []byte{0x03} // COM_QUERY
	queryData = append(queryData, []byte(systemVariableQuery)...)

	// 计算包长度
	payloadLength := len(queryData)
	queryPacket := []byte{
		byte(payloadLength & 0xff),
		byte((payloadLength >> 8) & 0xff),
		byte((payloadLength >> 16) & 0xff),
		0x00, // 序号
	}
	queryPacket = append(queryPacket, queryData...)

	// 发送查询包
	_, err = conn.Write(queryPacket)
	if err != nil {
		logger.Debugf(" 发送查询包失败: %v\n", err)
		return
	}

	fmt.Println(" 查询包已发送，等待响应...")

	// 读取查询响应
	responseBuffer := make([]byte, 8192)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	n, err = conn.Read(responseBuffer)
	if err != nil {
		logger.Debugf(" 读取查询响应失败: %v\n", err)
		return
	}

	logger.Debugf(" 收到查询响应，长度: %d\n", n)
	logger.Debugf(" 响应内容前64字节: %v\n", responseBuffer[:min(64, n)])

	// 分析响应
	if n >= 5 {
		packetLen := int(responseBuffer[0]) | int(responseBuffer[1])<<8 | int(responseBuffer[2])<<16
		seqNum := responseBuffer[3]
		firstByte := responseBuffer[4]

		logger.Debugf(" 包长度: %d, 序号: %d, 第一字节: 0x%02X\n", packetLen, seqNum, firstByte)

		switch firstByte {
		case 0x00:
			fmt.Println(" 收到OK包 - 系统变量查询被简单确认")
		case 0xff:
			fmt.Println(" 收到错误包")
			if n >= 6 {
				errorCode := int(responseBuffer[5]) | int(responseBuffer[6])<<8
				logger.Debugf("错误代码: %d\n", errorCode)
			}
		case 0xfe:
			fmt.Println("📜 收到EOF包")
		default:
			if firstByte >= 0x01 && firstByte <= 0xfb {
				logger.Debugf(" 收到结果集，列数: %d\n", firstByte)
				fmt.Println(" 系统变量查询返回了结果集！")
			} else {
				logger.Debugf("🤔 未知的响应类型: 0x%02X\n", firstByte)
			}
		}
	}

	fmt.Println("🏁 测试完成")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
