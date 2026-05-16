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
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
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
		common.COM_RESET_CONNECTION,
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

