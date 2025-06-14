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
	"bytes"
	"errors"
	"time"

	log "github.com/AlexStocks/log4go"
)

type MySQLEchoPkgHandler struct {
}

func NewMySQLEchoPkgHandler() *MySQLEchoPkgHandler {
	return &MySQLEchoPkgHandler{}
}

func (h *MySQLEchoPkgHandler) Read(ss Session, data []byte) (interface{}, int, error) {
	var (
		err       error
		packetLen int
		pkg       MySQLPackage
		buf       *bytes.Buffer
	)

	log.Info("[MySQLEchoPkgHandler.Read] 收到原始数据，长度: %d, 内容: %v", len(data), data)

	buf = bytes.NewBuffer(data)
	packetLen, err = pkg.Unmarshal(buf)
	if err != nil {
		if err == ErrNotEnoughStream {
			log.Info("[MySQLEchoPkgHandler.Read] 数据流不足，等待更多数据")
			return nil, 0, nil
		}

		log.Error("[MySQLEchoPkgHandler.Read] 解析包失败: %v", err)
		return nil, 0, err
	}

	log.Info("[MySQLEchoPkgHandler.Read] 包解析成功:")
	log.Info("   - 包长度: %v", pkg.Header.PacketLength)
	log.Info("   - 包序号: %d", pkg.Header.PacketId)
	log.Info("   - 包体长度: %d", len(pkg.Body))
	log.Info("   - 包体内容: %v", pkg.Body)
	log.Info("   - 返回解析长度: %d", packetLen)

	return &pkg, packetLen, nil
}

func (h *MySQLEchoPkgHandler) Write(ss Session, pkg interface{}) ([]byte, error) {
	var (
		ok        bool
		err       error
		startTime time.Time
		echoPkg   *MySQLPackage
		buf       *bytes.Buffer
	)

	startTime = time.Now()
	if echoPkg, ok = pkg.(*MySQLPackage); !ok {
		log.Error("illegal pkg:%+v", pkg)
		return nil, errors.New("invalid echo package!")
	}

	buf, err = echoPkg.Marshal()
	if err != nil {
		log.Warn("binary.Write(echoPkg{%#v}) = err{%#v}", echoPkg, err)
		return nil, err
	}

	log.Debug("WriteEchoPkgTimeMs = %s", time.Since(startTime).String())

	return buf.Bytes(), nil
}
