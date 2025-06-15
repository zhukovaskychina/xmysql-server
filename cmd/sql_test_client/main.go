package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	fmt.Println(" MySQL SQL查询测试客户端")
	fmt.Println("==========================")

	// 连接到服务器
	conn, err := net.DialTimeout("tcp", "localhost:3309", 5*time.Second)
	if err != nil {
		logger.Debugf(" 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println(" TCP连接建立成功")

	// 1. 读取握手包
	handshakeData, err := readHandshake(conn)
	if err != nil {
		logger.Debugf(" 读取握手包失败: %v\n", err)
		return
	}

	logger.Debugf(" 握手包接收成功，服务器版本: %s\n", handshakeData.ServerVersion)

	// 2. 发送认证包
	err = sendAuth(conn, handshakeData, "root", "123456", "mysql")
	if err != nil {
		logger.Debugf(" 发送认证失败: %v\n", err)
		return
	}

	// 3. 读取认证响应
	authResult, err := readAuthResponse(conn)
	if err != nil {
		logger.Debugf(" 认证失败: %v\n", err)
		return
	}

	if authResult {
		fmt.Println(" 认证成功!")
	} else {
		fmt.Println(" 认证失败")
		return
	}

	// 4. 发送SQL查询（这将触发权限检查）
	queries := []string{
		"SELECT 1",
		"SELECT USER()",
		"SHOW DATABASES",
		"SELECT * FROM mysql.user WHERE User='root' LIMIT 1",
	}

	for i, query := range queries {
		logger.Debugf("\n 执行查询 %d: %s\n", i+1, query)

		err = sendQuery(conn, query)
		if err != nil {
			logger.Debugf(" 发送查询失败: %v\n", err)
			continue
		}

		result, err := readQueryResponse(conn)
		if err != nil {
			logger.Debugf(" 读取查询结果失败: %v\n", err)
		} else {
			logger.Debugf(" 查询成功: %s\n", result)
		}

		// 短暂等待，让服务器处理
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n🎉 测试完成!")
}

// HandshakeData 握手数据
type HandshakeData struct {
	ServerVersion  string
	ConnectionID   uint32
	AuthPluginData []byte
}

// readHandshake 读取握手包
func readHandshake(conn net.Conn) (*HandshakeData, error) {
	reader := bufio.NewReader(conn)

	// 读取包头
	header := make([]byte, 4)
	_, err := reader.Read(header)
	if err != nil {
		return nil, err
	}

	length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16

	// 读取包体
	payload := make([]byte, length)
	_, err = reader.Read(payload)
	if err != nil {
		return nil, err
	}

	// 解析握手包
	if len(payload) < 1 {
		return nil, fmt.Errorf("握手包太短")
	}

	protocolVersion := payload[0]
	if protocolVersion != 10 {
		return nil, fmt.Errorf("不支持的协议版本: %d", protocolVersion)
	}

	// 找到服务器版本字符串
	var serverVersion string
	var pos int
	for i := 1; i < len(payload); i++ {
		if payload[i] == 0 {
			serverVersion = string(payload[1:i])
			pos = i + 1
			break
		}
	}

	if pos+4 > len(payload) {
		return nil, fmt.Errorf("握手包格式错误")
	}

	// 读取连接ID
	connectionID := binary.LittleEndian.Uint32(payload[pos : pos+4])
	pos += 4

	// 读取认证插件数据（前8字节）
	if pos+8 > len(payload) {
		return nil, fmt.Errorf("握手包格式错误")
	}

	authPluginData := make([]byte, 8)
	copy(authPluginData, payload[pos:pos+8])

	return &HandshakeData{
		ServerVersion:  serverVersion,
		ConnectionID:   connectionID,
		AuthPluginData: authPluginData,
	}, nil
}

// sendAuth 发送认证包
func sendAuth(conn net.Conn, handshake *HandshakeData, username, password, database string) error {
	var buf bytes.Buffer

	// 客户端能力标志
	capabilityFlags := uint32(0x000FA205)
	binary.Write(&buf, binary.LittleEndian, capabilityFlags)

	// 最大包大小
	maxPacketSize := uint32(16777215)
	binary.Write(&buf, binary.LittleEndian, maxPacketSize)

	// 字符集
	characterSet := uint8(33) // utf8_general_ci
	binary.Write(&buf, binary.LittleEndian, characterSet)

	// 保留字节（23字节）
	reserved := make([]byte, 23)
	buf.Write(reserved)

	// 用户名
	buf.WriteString(username)
	buf.WriteByte(0)

	// 认证响应
	authResponse := calculateAuthResponse(password, handshake.AuthPluginData)
	buf.WriteByte(byte(len(authResponse)))
	buf.Write(authResponse)

	// 数据库名
	if database != "" {
		buf.WriteString(database)
		buf.WriteByte(0)
	}

	return sendPacket(conn, buf.Bytes(), 1)
}

// calculateAuthResponse 计算认证响应
func calculateAuthResponse(password string, salt []byte) []byte {
	if password == "" {
		return []byte{}
	}

	// SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password))
	hash2 := sha1.Sum(hash1[:])

	// SHA1(salt + SHA1(SHA1(password)))
	saltedHash := sha1.New()
	saltedHash.Write(salt)
	saltedHash.Write(hash2[:])
	hash3 := saltedHash.Sum(nil)

	// SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	result := make([]byte, 20)
	for i := 0; i < 20; i++ {
		result[i] = hash1[i] ^ hash3[i]
	}

	return result
}

// readAuthResponse 读取认证响应
func readAuthResponse(conn net.Conn) (bool, error) {
	reader := bufio.NewReader(conn)

	// 读取包头
	header := make([]byte, 4)
	_, err := reader.Read(header)
	if err != nil {
		return false, err
	}

	length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16

	// 读取包体
	payload := make([]byte, length)
	_, err = reader.Read(payload)
	if err != nil {
		return false, err
	}

	if len(payload) < 1 {
		return false, fmt.Errorf("认证响应包太短")
	}

	// 检查第一个字节
	switch payload[0] {
	case 0x00:
		// OK包
		fmt.Println(" 收到OK包")
		return true, nil
	case 0xFF:
		// ERROR包
		if len(payload) > 3 {
			errorCode := uint16(payload[1]) | uint16(payload[2])<<8
			errorMessage := string(payload[9:])
			return false, fmt.Errorf("认证错误 %d: %s", errorCode, errorMessage)
		}
		return false, fmt.Errorf("认证失败")
	default:
		return false, fmt.Errorf("未知的认证响应: 0x%02x", payload[0])
	}
}

// sendQuery 发送查询
func sendQuery(conn net.Conn, query string) error {
	var buf bytes.Buffer
	buf.WriteByte(0x03) // COM_QUERY
	buf.WriteString(query)

	return sendPacket(conn, buf.Bytes(), 0)
}

// readQueryResponse 读取查询响应
func readQueryResponse(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)

	// 读取包头
	header := make([]byte, 4)
	_, err := reader.Read(header)
	if err != nil {
		return "", err
	}

	length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16

	// 读取包体
	payload := make([]byte, length)
	_, err = reader.Read(payload)
	if err != nil {
		return "", err
	}

	if len(payload) < 1 {
		return "", fmt.Errorf("查询响应包太短")
	}

	switch payload[0] {
	case 0x00:
		return "OK响应", nil
	case 0xFF:
		if len(payload) > 3 {
			errorCode := uint16(payload[1]) | uint16(payload[2])<<8
			errorMessage := string(payload[9:])
			return "", fmt.Errorf("查询错误 %d: %s", errorCode, errorMessage)
		}
		return "", fmt.Errorf("查询失败")
	default:
		return fmt.Sprintf("结果集响应 (字段数: %d)", payload[0]), nil
	}
}

// sendPacket 发送MySQL包
func sendPacket(conn net.Conn, payload []byte, seqID uint8) error {
	length := uint32(len(payload))
	header := make([]byte, 4)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = seqID

	_, err := conn.Write(append(header, payload...))
	return err
}
