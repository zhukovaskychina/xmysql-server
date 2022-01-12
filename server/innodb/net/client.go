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
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

import (
	log "github.com/AlexStocks/log4go"
	"github.com/dubbogo/gost/net"
	gxsync "github.com/dubbogo/gost/sync"
	jerrors "github.com/juju/errors"
)

const (
	reconnectInterval = 3e8 // 300ms
	connectInterval   = 5e8 // 500ms
	connectTimeout    = 3e9
	maxTimes          = 10
)

var (
	sessionClientKey   = "session-client-owner"
	connectPingPackage = []byte("connect-ping")
)

/////////////////////////////////////////
// getty tcp client
/////////////////////////////////////////

var (
	clientID = EndPointID(0)
)

type client struct {
	ClientOptions

	// endpoint ID
	endPointID EndPointID

	// net
	sync.Mutex
	endPointType EndPointType

	newSession NewSessionCallback
	ssMap      map[Session]struct{}

	sync.Once
	done chan struct{}
	wg   sync.WaitGroup
}

func (c *client) init(opts ...ClientOption) {
	for _, opt := range opts {
		opt(&(c.ClientOptions))
	}
}

func newClient(t EndPointType, opts ...ClientOption) *client {
	c := &client{
		endPointID:   atomic.AddInt32(&clientID, 1),
		endPointType: t,
		done:         make(chan struct{}),
	}

	c.init(opts...)

	if c.number <= 0 || c.addr == "" {
		panic(fmt.Sprintf("client type:%s, @connNum:%d, @serverAddr:%s", t, c.number, c.addr))
	}

	c.ssMap = make(map[Session]struct{}, c.number)

	return c
}

// NewTcpClient function builds a tcp client.
func NewTCPClient(opts ...ClientOption) Client {
	return newClient(TCP_CLIENT, opts...)
}

func (c *client) ID() EndPointID {
	return c.endPointID
}

func (c *client) EndPointType() EndPointType {
	return c.endPointType
}

func (c *client) dialTCP() Session {
	var (
		err  error
		conn net.Conn
	)

	for {
		if c.IsClosed() {
			return nil
		}
		if c.sslEnabled {
			if sslConfig, e := c.tlsConfigBuilder.BuildTlsConfig(); e == nil && sslConfig != nil {
				d := &net.Dialer{Timeout: connectTimeout}
				conn, err = tls.DialWithDialer(d, "tcp", c.addr, sslConfig)
			}
		} else {
			conn, err = net.DialTimeout("tcp", c.addr, connectTimeout)
		}
		if err == nil && gxnet.IsSameAddr(conn.RemoteAddr(), conn.LocalAddr()) {
			conn.Close()
			err = errSelfConnect
		}
		if err == nil {
			return newTCPSession(conn, c)
		}

		log.Info("net.DialTimeout(addr:%s, timeout:%v) = error:%+v", c.addr, jerrors.ErrorStack(err))
		<-wheel.After(connectInterval)
	}
}

func (c *client) dial() Session {
	switch c.endPointType {
	case TCP_CLIENT:
		return c.dialTCP()
	}

	return nil
}

func (c *client) GetTaskPool() gxsync.GenericTaskPool {
	return c.tPool
}

func (c *client) sessionNum() int {
	var num int

	c.Lock()
	for s := range c.ssMap {
		if s.IsClosed() {
			delete(c.ssMap, s)
		}
	}
	num = len(c.ssMap)
	c.Unlock()

	return num
}

func (c *client) connect() {
	var (
		err error
		ss  Session
	)

	for {
		ss = c.dial()
		if ss == nil {
			// client has been closed
			break
		}
		err = c.newSession(ss)
		if err == nil {
			ss.(*session).run()
			c.Lock()
			if c.ssMap == nil {
				c.Unlock()
				break
			}
			c.ssMap[ss] = struct{}{}
			c.Unlock()
			ss.SetAttribute(sessionClientKey, c)
			break
		}
		// don't distinguish between tcp connection and websocket connection. Because
		// gorilla/websocket/conn.go:(Conn)Close also invoke net.Conn.Close()
		ss.Conn().Close()
	}
}

// there are two methods to keep connection pool. the first approach is like
// redigo's lazy connection pool(https://github.com/gomodule/redigo/blob/master/redis/pool.go:),
// in which you should apply testOnBorrow to check alive of the connection.
// the second way is as follows. @RunEventLoop detects the aliveness of the connection
// in regular time interval.
// the active method maybe overburden the cpu slightly.
// however, you can get a active tcp connection very quickly.
func (c *client) RunEventLoop(newSession NewSessionCallback) {
	c.Lock()
	c.newSession = newSession
	c.Unlock()
	c.reConnect()
}

// a for-loop connect to make sure the connection pool is valid
func (c *client) reConnect() {
	var num, max, times, interval int

	max = c.number
	interval = c.reconnectInterval
	if interval == 0 {
		interval = reconnectInterval
	}
	for {
		if c.IsClosed() {
			log.Warn("client{peer:%s} goroutine exit now.", c.addr)
			break
		}

		num = c.sessionNum()
		if max <= num {
			break
		}
		c.connect()
		times++
		if maxTimes < times {
			times = maxTimes
		}
		<-wheel.After(time.Duration(int64(times) * int64(interval)))
	}
}

func (c *client) stop() {
	select {
	case <-c.done:
		return
	default:
		c.Once.Do(func() {
			close(c.done)
			c.Lock()
			for s := range c.ssMap {
				s.RemoveAttribute(sessionClientKey)
				s.Close()
			}
			c.ssMap = nil

			c.Unlock()
		})
	}
}

func (c *client) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *client) Close() {
	c.stop()
	c.wg.Wait()
}
