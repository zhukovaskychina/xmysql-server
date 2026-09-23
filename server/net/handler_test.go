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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
)

func TestMySQLMessageHandlerUnsupportedCommandWritesErrorPacket(t *testing.T) {
	config := conf.NewCfg()
	handler := NewMySQLMessageHandler(config)
	session := NewMockSession("test_simple_handler_unsupported")

	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}

	session.SetAttribute("auth_status", "success")
	session.SetAttribute("should_close", false)

	commands := []byte{
		common.COM_FIELD_LIST,
		common.COM_STMT_PREPARE,
		common.COM_STMT_EXECUTE,
		common.COM_STMT_CLOSE,
		common.COM_STMT_RESET,
	}

	for i, cmd := range commands {
		before := len(session.written)
		pkt := &MySQLPackage{
			Header: MySQLPkgHeader{
				PacketLength: []byte{0x01, 0x00, 0x00},
				PacketId:     byte(i),
			},
			Body: []byte{cmd},
		}

		handler.OnMessage(session, pkt)

		if session.closed {
			t.Fatalf("session should stay open for unsupported command %s", common.CommandString(cmd))
		}
		if len(session.written) != before+1 {
			t.Fatalf("expected one packet to be written for %s, got %d", common.CommandString(cmd), len(session.written)-before)
		}

		lastPacket := session.written[len(session.written)-1]
		if len(lastPacket) < 13 {
			t.Fatalf("error packet too short for command %s: %d", common.CommandString(cmd), len(lastPacket))
		}
		if lastPacket[4] != 0xFF {
			t.Fatalf("expect error marker for %s, got 0x%02X", common.CommandString(cmd), lastPacket[4])
		}

		errCode := uint16(lastPacket[5]) | uint16(lastPacket[6])<<8
		if errCode != common.ErrNotSupportedYet {
			t.Fatalf("expect %d for %s, got %d", common.ErrNotSupportedYet, common.CommandString(cmd), errCode)
		}

		state := string(lastPacket[8:13])
		if state != "42000" {
			t.Fatalf("expect sql state 42000 for %s, got %s", common.CommandString(cmd), state)
		}

		msg := string(lastPacket[13:])
		if !strings.Contains(msg, common.CommandString(cmd)) {
			t.Fatalf("error message for %s should contain command name, got %q", common.CommandString(cmd), msg)
		}
	}
}

func TestMySQLMessageHandlerPreservesClassifiedQueryError(t *testing.T) {
	handler := &MySQLMessageHandler{}
	session := NewMockSession("test_simple_handler_query_error")
	resultChan := make(chan *dispatcher.SQLResult, 1)
	resultChan <- &dispatcher.SQLResult{Err: fmt.Errorf("table xmysql_missing does not exist")}
	close(resultChan)

	if err := handler.handleQueryResults(session, resultChan); err != nil {
		t.Fatalf("handleQueryResults failed: %v", err)
	}
	if len(session.written) != 1 {
		t.Fatalf("expected one error packet, got %d", len(session.written))
	}
	packet := session.written[0]
	if len(packet) < 13 || packet[4] != 0xFF {
		t.Fatalf("expected MySQL error packet, got %v", packet)
	}
	if got := uint16(packet[5]) | uint16(packet[6])<<8; got != common.ErrNoSuchTable {
		t.Fatalf("expected unknown-table errno %d, got %d", common.ErrNoSuchTable, got)
	}
	if got := string(packet[8:13]); got != "42S02" {
		t.Fatalf("expected unknown-table SQLSTATE 42S02, got %s", got)
	}
}

func TestMySQLMessageHandlerResetConnectionClearsSessionState(t *testing.T) {
	handler := NewMySQLMessageHandler(conf.NewCfg())
	session := NewMockSession("test_simple_handler_reset")
	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}
	session.SetAttribute("auth_status", "success")
	current := handler.sessionMap[session]
	current.SetParamByName("autocommit", "0")
	current.SetParamByName("database", "app")
	before := len(session.written)
	pkt := &MySQLPackage{Header: MySQLPkgHeader{PacketLength: []byte{1, 0, 0}}, Body: []byte{common.COM_RESET_CONNECTION}}
	if err := handler.handleMessage(session, &current, pkt); err != nil {
		t.Fatalf("reset failed: %v", err)
	}
	if len(session.written) != before+1 {
		t.Fatalf("expected one reset response packet")
	}
	if got := current.GetParamByName("autocommit"); got != "1" {
		t.Fatalf("autocommit was not reset: %v", got)
	}
	if got := current.GetParamByName("database"); got != "" {
		t.Fatalf("database was not reset: %v", got)
	}
}

func TestMySQLMessageHandlerQuitClosesSession(t *testing.T) {
	config := conf.NewCfg()
	handler := NewMySQLMessageHandler(config)
	session := NewMockSession("test_simple_handler_quit")

	if err := handler.OnOpen(session); err != nil {
		t.Fatalf("OnOpen failed: %v", err)
	}

	session.SetAttribute("auth_status", "success")

	pkt := &MySQLPackage{
		Header: MySQLPkgHeader{
			PacketLength: []byte{0x01, 0x00, 0x00},
			PacketId:     0,
		},
		Body: []byte{common.COM_QUIT},
	}

	handler.OnMessage(session, pkt)

	if !session.closed {
		t.Fatal("session should be closed after COM_QUIT")
	}
}

func TestMySQLMessageHandlerConcurrentClose(t *testing.T) {
	handler := &MySQLMessageHandler{sessionMap: make(map[Session]server.MySQLServerSession)}

	const sessionCount = 32
	sessions := make([]*MockSession, sessionCount)
	handler.rwlock.Lock()
	for i := range sessions {
		sessions[i] = NewMockSession(fmt.Sprintf("legacy-concurrent-close-%d", i))
		handler.sessionMap[sessions[i]] = NewMySQLServerSession(sessions[i])
	}
	handler.rwlock.Unlock()

	var wg sync.WaitGroup
	for _, session := range sessions {
		wg.Add(1)
		go func(session *MockSession) {
			defer wg.Done()
			handler.OnClose(session)
		}(session)
	}
	wg.Wait()

	handler.rwlock.RLock()
	remaining := len(handler.sessionMap)
	handler.rwlock.RUnlock()
	if remaining != 0 {
		t.Fatalf("session map retained %d sessions after concurrent close", remaining)
	}
}
