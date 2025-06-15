/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net

import (
	"errors"
	"fmt"
	log "github.com/AlexStocks/log4go"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
	"sync"
)

const (
	WritePkgTimeout = 1e8
)

var (
	errTooManySessions = errors.New("Too many MySQL sessions!")
)
var (
	ErrNotEnoughStream = errors.New("packet stream is not enough")
	ErrTooLargePackage = errors.New("package length is exceed the echo package's legal maximum length.")
	ErrIllegalMagic    = errors.New("package magic is not right.")
)

type PackageHandler interface {
	Handle(Session, *MySQLPackage) error
}

type MySQLPackageHandler struct {
}

func (h *MySQLPackageHandler) Handle(session Session, pkg *MySQLPackage) error {
	log.Debug("get echo package{%s}", pkg)
	// write echo message handle logic here.
	return session.WritePkg(nil, WritePkgTimeout)
}

type MySQLMessageHandler struct {
	rwlock        sync.RWMutex
	cfg           *conf.Cfg
	sessionMap    map[Session]server.MySQLServerSession // 内存区，用于存储mysql的session
	sqlDispatcher *dispatcher.SQLDispatcher             // SQL分发器
}

func NewMySQLMessageHandler(cfg *conf.Cfg) *MySQLMessageHandler {
	var mySQLMessageHandler = new(MySQLMessageHandler)
	mySQLMessageHandler.sessionMap = make(map[Session]server.MySQLServerSession)
	mySQLMessageHandler.cfg = cfg
	// 创建SQL分发器，替代直接使用XMySQLEngine
	mySQLMessageHandler.sqlDispatcher = dispatcher.NewSQLDispatcher(cfg)
	return mySQLMessageHandler
}

func (m *MySQLMessageHandler) OnOpen(session Session) error {
	var (
		err error
	)

	m.rwlock.RLock()

	if m.cfg.SessionNumber <= len(m.sessionMap) {
		err = errTooManySessions
	}
	m.rwlock.RUnlock()
	if err != nil {
		return err
	}
	log.Info("got session:%s", session.Stat())
	m.rwlock.Lock()

	m.sessionMap[session] = NewMySQLServerSession(session)
	m.rwlock.Unlock()
	//主动与客户端握手
	m.sessionMap[session].SendHandleOk()
	return nil
}

func (m *MySQLMessageHandler) OnClose(session Session) {
	session.Close()
	delete(m.sessionMap, session)
}

func (m *MySQLMessageHandler) OnError(session Session, err error) {
	fmt.Println("", err)
	session.Close()
	delete(m.sessionMap, session)
}

func (m *MySQLMessageHandler) OnCron(session Session) {
	fmt.Println("session 检查")
}
func (m *MySQLMessageHandler) OnMessage(session Session, pkg interface{}) {
	recMySQLPkg, ok := pkg.(*MySQLPackage)
	if !ok {
		log.Error("Invalid package type: %T", pkg)
		return
	}

	currentMysqlSession, ok := m.sessionMap[session]
	if !ok {
		log.Error("Session not found: %v", session)
		return
	}

	if err := m.handleMessage(session, &currentMysqlSession, recMySQLPkg); err != nil {
		log.Error("Error handling message: %v", err)
		session.Close()
	}
}

func (m *MySQLMessageHandler) handleMessage(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	authStatus := session.GetAttribute("auth_status")
	if authStatus == nil {
		return m.handleAuth(session, currentMysqlSession, recMySQLPkg)
	}

	// 已认证，处理具体的命令
	if len(recMySQLPkg.Body) == 0 {
		return fmt.Errorf("empty packet body")
	}

	packetType := recMySQLPkg.Body[0]
	switch packetType {
	case common.COM_SLEEP:
		return m.handleSleep(session, currentMysqlSession, recMySQLPkg)
	case common.COM_QUERY:
		return m.handleQuery(session, currentMysqlSession, recMySQLPkg)
	case common.COM_QUIT:
		return m.handleQuit(session, currentMysqlSession, recMySQLPkg)
	case common.COM_INIT_DB:
		return m.handleInitDB(session, currentMysqlSession, recMySQLPkg)
	case common.COM_PING:
		return m.handlePing(session, currentMysqlSession, recMySQLPkg)
	default:
		return fmt.Errorf("unsupported packet type: %d", packetType)
	}
}

func (m *MySQLMessageHandler) handleAuth(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	a := new(protocol.AuthPacket)
	authData := make([]byte, 0, len(recMySQLPkg.Header.PacketLength)+1+len(recMySQLPkg.Body))
	authData = append(authData, recMySQLPkg.Header.PacketLength...)
	authData = append(authData, recMySQLPkg.Header.PacketId)
	authData = append(authData, recMySQLPkg.Body...)

	authResult := a.DecodeAuth(authData)
	if authResult == nil {
		return fmt.Errorf("failed to decode auth packet")
	}

	session.SetAttribute("auth_status", "success")
	(*currentMysqlSession).SetParamByName("database", authResult.Database)
	(*currentMysqlSession).SetParamByName("user", authResult.User)
	m.sessionMap[session] = *currentMysqlSession

	buff := protocol.EncodeOK(nil, 0, 0, nil)
	return session.WriteBytes(buff)
}

func (m *MySQLMessageHandler) handleQuery(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 2 {
		return fmt.Errorf("invalid query packet")
	}

	query := string(recMySQLPkg.Body[1:])
	dbName, ok := (*currentMysqlSession).GetParamByName("database").(string)
	if !ok {
		dbName = "test_simple_protocol" // 默认数据库
	}

	// 分发SQL查询到SQL分发器
	resultChan := m.sqlDispatcher.Dispatch(*currentMysqlSession, query, dbName)

	// 同步处理结果，避免连接状态混乱
	return m.handleQueryResults(session, resultChan)
}

func (m *MySQLMessageHandler) handleQueryResults(session Session, resultChan <-chan *dispatcher.SQLResult) error {
	for result := range resultChan {
		if result.Err != nil {
			// 发送错误响应
			errPacket := protocol.EncodeErrorPacket(1064, "42000", result.Err.Error())
			return session.WriteBytes(errPacket)
		}

		switch result.ResultType {
		case "select", "query":
			// 发送查询结果
			return m.sendQueryResult(session, result)
		case "ddl":
			// 发送DDL成功响应
			okPacket := protocol.EncodeOK(nil, 0, 1, nil)
			return session.WriteBytes(okPacket)
		case "insert", "update", "delete":
			// 发送DML成功响应
			okPacket := protocol.EncodeOK(nil, 1, 0, nil) // 假设影响1行
			return session.WriteBytes(okPacket)
		case "set":
			// 发送SET成功响应
			okPacket := protocol.EncodeOK(nil, 0, 0, nil)
			return session.WriteBytes(okPacket)
		default:
			// 发送通用成功响应
			okPacket := protocol.EncodeOK(nil, 0, 0, nil)
			return session.WriteBytes(okPacket)
		}
	}
	return nil
}

func (m *MySQLMessageHandler) sendQueryResult(session Session, result *dispatcher.SQLResult) error {
	// 确保有列信息才发送查询结果
	if len(result.Columns) == 0 {
		// 如果没有列信息，发送OK包而不是结果集
		okPacket := protocol.EncodeOK(nil, 0, 0, nil)
		return session.WriteBytes(okPacket)
	}

	sequenceId := byte(1) // 从1开始的序列号

	// 发送列数量
	columnCountPacket := m.encodeColumnCountWithSeq(len(result.Columns), sequenceId)
	if err := session.WriteBytes(columnCountPacket); err != nil {
		return err
	}
	sequenceId++

	// 发送列定义
	for _, column := range result.Columns {
		columnPacket := m.encodeColumnDefinitionWithSeq(column, sequenceId)
		if err := session.WriteBytes(columnPacket); err != nil {
			return err
		}
		sequenceId++
	}

	// 发送EOF包（列定义结束）
	eofPacket := protocol.EncodeEOFPacketWithSeq(0, 0, sequenceId)
	if err := session.WriteBytes(eofPacket); err != nil {
		return err
	}
	sequenceId++

	// 发送行数据
	if len(result.Rows) > 0 {
		for _, row := range result.Rows {
			rowPacket := m.encodeRowDataWithSeq(row, sequenceId)
			if err := session.WriteBytes(rowPacket); err != nil {
				return err
			}
			sequenceId++
		}
	}

	// 发送EOF包（数据结束）
	eofPacket = protocol.EncodeEOFPacketWithSeq(0, 0, sequenceId)
	return session.WriteBytes(eofPacket)
}

func (m *MySQLMessageHandler) handleQuit(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	log.Info("Client requested quit")
	return nil
}

func (m *MySQLMessageHandler) handleInitDB(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	if len(recMySQLPkg.Body) < 2 {
		return fmt.Errorf("invalid init db packet")
	}

	dbName := string(recMySQLPkg.Body[1:])
	(*currentMysqlSession).SetParamByName("database", dbName)

	// 发送成功响应
	okPacket := protocol.EncodeOK(nil, 0, 0, nil)
	return session.WriteBytes(okPacket)
}

func (m *MySQLMessageHandler) handlePing(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	okPacket := protocol.EncodeOK(nil, 0, 0, nil)
	return session.WriteBytes(okPacket)
}

func (m *MySQLMessageHandler) handleSleep(session Session, currentMysqlSession *server.MySQLServerSession, recMySQLPkg *MySQLPackage) error {
	// Sleep命令不需要特殊处理
	return nil
}

// 协议编码辅助方法

// encodeColumnCountWithSeq 编码列数量包（带序列号）
func (m *MySQLMessageHandler) encodeColumnCountWithSeq(count int, sequenceId byte) []byte {
	// 使用length-encoded integer编码列数量
	payload := m.encodeLengthEncodedInt(uint64(count))
	return m.addPacketHeader(payload, sequenceId)
}

// encodeColumnDefinitionWithSeq 编码列定义包（带序列号）
func (m *MySQLMessageHandler) encodeColumnDefinitionWithSeq(columnName string, sequenceId byte) []byte {
	payload := make([]byte, 0, 64+len(columnName))

	// 简化的列定义
	payload = m.appendLengthEncodedString(payload, "def")      // catalog
	payload = m.appendLengthEncodedString(payload, "")         // schema
	payload = m.appendLengthEncodedString(payload, "")         // table
	payload = m.appendLengthEncodedString(payload, "")         // org_table
	payload = m.appendLengthEncodedString(payload, columnName) // name
	payload = m.appendLengthEncodedString(payload, columnName) // org_name

	// 固定长度字段
	payload = append(payload, 0x0c)                   // length of fixed fields
	payload = append(payload, 0x21, 0x00)             // character set
	payload = append(payload, 0x00, 0x00, 0x00, 0x00) // column length
	payload = append(payload, 0xFD)                   // column type (VAR_STRING)
	payload = append(payload, 0x00, 0x00)             // flags
	payload = append(payload, 0x00)                   // decimals
	payload = append(payload, 0x00, 0x00)             // filler

	return m.addPacketHeader(payload, sequenceId)
}

// encodeRowDataWithSeq 编码行数据包（带序列号）
func (m *MySQLMessageHandler) encodeRowDataWithSeq(row []interface{}, sequenceId byte) []byte {
	payload := make([]byte, 0, 256)

	for _, value := range row {
		if value == nil {
			payload = append(payload, 0xFB) // NULL
		} else {
			str := fmt.Sprintf("%v", value)
			payload = m.appendLengthEncodedString(payload, str)
		}
	}

	return m.addPacketHeader(payload, sequenceId)
}

// encodeLengthEncodedInt 编码length-encoded integer
func (m *MySQLMessageHandler) encodeLengthEncodedInt(value uint64) []byte {
	if value < 251 {
		return []byte{byte(value)}
	} else if value < 65536 {
		result := make([]byte, 3)
		result[0] = 0xFC
		result[1] = byte(value)
		result[2] = byte(value >> 8)
		return result
	} else if value < 16777216 {
		result := make([]byte, 4)
		result[0] = 0xFD
		result[1] = byte(value)
		result[2] = byte(value >> 8)
		result[3] = byte(value >> 16)
		return result
	} else {
		result := make([]byte, 9)
		result[0] = 0xFE
		for i := 1; i < 9; i++ {
			result[i] = byte(value >> ((i - 1) * 8))
		}
		return result
	}
}

// 保留旧方法以兼容其他地方的调用
// encodeColumnCount 编码列数量包
func (m *MySQLMessageHandler) encodeColumnCount(count int) []byte {
	return m.encodeColumnCountWithSeq(count, 1)
}

// encodeColumnDefinition 编码列定义包
func (m *MySQLMessageHandler) encodeColumnDefinition(columnName string) []byte {
	return m.encodeColumnDefinitionWithSeq(columnName, 2)
}

// encodeRowData 编码行数据包
func (m *MySQLMessageHandler) encodeRowData(row []interface{}) []byte {
	return m.encodeRowDataWithSeq(row, 3)
}

// addPacketHeader 添加MySQL包头
func (m *MySQLMessageHandler) addPacketHeader(payload []byte, sequenceId byte) []byte {
	length := len(payload)
	header := make([]byte, 4)

	// 包长度 (3字节，小端序)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 序列号
	header[3] = sequenceId

	return append(header, payload...)
}

// appendLengthEncodedString 追加长度编码字符串
func (m *MySQLMessageHandler) appendLengthEncodedString(data []byte, str string) []byte {
	data = m.appendLengthEncodedInt(data, uint64(len(str)))
	return append(data, []byte(str)...)
}

// appendLengthEncodedInt 追加长度编码整数
func (m *MySQLMessageHandler) appendLengthEncodedInt(data []byte, value uint64) []byte {
	if value < 251 {
		return append(data, byte(value))
	} else if value < 65536 {
		data = append(data, 0xFC)
		data = append(data, byte(value), byte(value>>8))
		return data
	} else if value < 16777216 {
		data = append(data, 0xFD)
		data = append(data, byte(value), byte(value>>8), byte(value>>16))
		return data
	} else {
		data = append(data, 0xFE)
		for i := 0; i < 8; i++ {
			data = append(data, byte(value>>(i*8)))
		}
		return data
	}
}
