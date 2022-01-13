// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"encoding/json"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/schemas"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/context"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/parser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sessionctx/variable"
	"github.com/zhukovaskychina/xmysql-server/server/mysql"
)

// Session context
type Session interface {
	context.Context
	Status() uint16 // Flag of current status, such as autocommit.
	String() string // For debug
	Close() error
}

var (
	_         Session = (*session)(nil)
	sessionMu sync.Mutex
)

type stmtRecord struct {
	stmtID uint32
	st     ast.Statement
	params []interface{}
}

type stmtHistory struct {
	history []*stmtRecord
}

func (h *stmtHistory) add(stmtID uint32, st ast.Statement, params ...interface{}) {
	s := &stmtRecord{
		stmtID: stmtID,
		st:     st,
		params: append(([]interface{})(nil), params...),
	}
	h.history = append(h.history, s)
}

type session struct {
	Session
	values      map[fmt.Stringer]interface{}
	parser      *parser.Parser
	sessionVars *variable.SessionVars
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"stauts":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}

func (s *session) ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error) {
	return s.parser.Parse(sql, charset, collation)
}
func (s *session) ParseSingleSQL(sql, charset, collation string) (ast.StmtNode, error) {
	return s.parser.ParseOneStmt(sql, charset, collation)
}

// checkArgs makes sure all the arguments' basic are known and can be handled.
// integer basic are converted to int64 and uint64, time.Time is converted to basic.Time.
// time.Duration is converted to basic.Duration, other known basic are leaved as it is.
func checkArgs(args ...interface{}) error {
	for i, v := range args {
		switch x := v.(type) {
		case bool:
			if x {
				args[i] = int64(1)
			} else {
				args[i] = int64(0)
			}
		case int8:
			args[i] = int64(x)
		case int16:
			args[i] = int64(x)
		case int32:
			args[i] = int64(x)
		case int:
			args[i] = int64(x)
		case uint8:
			args[i] = uint64(x)
		case uint16:
			args[i] = uint64(x)
		case uint32:
			args[i] = uint64(x)
		case uint:
			args[i] = uint64(x)
		case int64:
		case uint64:
		case float32:
		case float64:
		case string:
		case []byte:
		case time.Duration:
			args[i] = basic.Duration{Duration: x}
		case time.Time:
			args[i] = basic.Time{Time: basic.FromGoTime(x), Type: mysql.TypeDatetime}
		case nil:
		default:
			return errors.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}
	return nil
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.values[key] = value
}

func (s *session) Value(key fmt.Stringer) interface{} {
	value := s.values[key]
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	delete(s.values, key)
}

// Close function does some clean work when session end.
func (s *session) Close() error {
	return nil
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// Some vars name for debug.
const (
	retryEmptyHistoryList = "RetryEmptyHistoryList"
)

// CreateSession creates a new session environment.
func CreateSession(info schemas.InfoSchema) (Session, error) {
	s, err := createSession(info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}

func createSession(info schemas.InfoSchema) (*session, error) {
	s := &session{
		values:      make(map[fmt.Stringer]interface{}),
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	s.sessionVars.TxnCtx.InfoSchema = info

	return s, nil
}
