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
	"strings"
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

var (
	errSelfConnect        = jerrors.New("connect self!")
	serverFastFailTimeout = time.Second * 1
	serverID              = EndPointID(0)
)

type serverimpl struct {
	ServerOptions

	// endpoint ID
	endPointID EndPointID

	// net
	pktListener net.PacketConn

	streamListener net.Listener

	endPointType EndPointType

	sync.Once
	done chan struct{}
	wg   sync.WaitGroup
}

func (s *serverimpl) init(opts ...ServerOption) {
	for _, opt := range opts {
		opt(&(s.ServerOptions))
	}
}

func newServer(t EndPointType, opts ...ServerOption) *serverimpl {
	s := &serverimpl{
		endPointID:   atomic.AddInt32(&serverID, 1),
		endPointType: t,
		done:         make(chan struct{}),
	}

	s.init(opts...)

	return s
}

// NewTCPServer builds a tcp serverimpl.
func NewTCPServer(opts ...ServerOption) Server {
	return newServer(TCP_SERVER, opts...)
}

func (s *serverimpl) ID() int32 {
	return s.endPointID
}

func (s *serverimpl) EndPointType() EndPointType {
	return s.endPointType
}

func (s *serverimpl) stop() {
	select {
	case <-s.done:
		return
	default:
		s.Once.Do(func() {
			close(s.done)
			if s.pktListener != nil {
				s.pktListener.Close()
				s.pktListener = nil
			}
		})
	}
}

func (s *serverimpl) GetTaskPool() gxsync.GenericTaskPool {
	return s.tPool
}

func (s *serverimpl) IsClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// net.ipv4.tcp_max_syn_backlog
// net.ipv4.tcp_timestamps
// net.ipv4.tcp_tw_recycle
func (s *serverimpl) listenTCP() error {
	var (
		err            error
		streamListener net.Listener
	)

	if len(s.addr) == 0 || !strings.Contains(s.addr, ":") {
		streamListener, err = gxnet.ListenOnTCPRandomPort(s.addr)
		if err != nil {
			return jerrors.Annotatef(err, "gxnet.ListenOnTCPRandomPort(addr:%s)", s.addr)
		}
	} else {
		if s.sslEnabled {
			if sslConfig, err := s.tlsConfigBuilder.BuildTlsConfig(); err == nil && sslConfig != nil {
				streamListener, err = tls.Listen("tcp", s.addr, sslConfig)
				if err != nil {
					return jerrors.Annotatef(err, "net.Listen(tcp, addr:%s)", s.addr)
				}
			}
		} else {
			streamListener, err = net.Listen("tcp", s.addr)
			if err != nil {
				return jerrors.Annotatef(err, "net.Listen(tcp, addr:%s)", s.addr)
			}
		}
	}

	s.streamListener = streamListener
	s.addr = s.streamListener.Addr().String()

	return nil
}

// Listen announces on the local network address.
func (s *serverimpl) listen() error {
	return jerrors.Trace(s.listenTCP())
}

func (s *serverimpl) accept(newSession NewSessionCallback) (Session, error) {
	conn, err := s.streamListener.Accept()
	if err != nil {
		return nil, jerrors.Trace(err)
	}
	if gxnet.IsSameAddr(conn.RemoteAddr(), conn.LocalAddr()) {
		log.Warn("conn.localAddr{%s} == conn.RemoteAddr", conn.LocalAddr().String(), conn.RemoteAddr().String())
		return nil, jerrors.Trace(errSelfConnect)
	}

	ss := newTCPSession(conn, s)
	err = newSession(ss)
	if err != nil {
		conn.Close()
		return nil, jerrors.Trace(err)
	}

	return ss, nil
}

func (s *serverimpl) runTcpEventLoop(newSession NewSessionCallback) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var (
			err    error
			client Session
			delay  time.Duration
		)
		for {
			if s.IsClosed() {
				log.Warn("serverimpl{%s} stop accepting client connect request.", s.addr)
				return
			}
			if delay != 0 {
				<-wheel.After(delay)
			}
			client, err = s.accept(newSession)
			if err != nil {
				if netErr, ok := jerrors.Cause(err).(net.Error); ok && netErr.Temporary() {
					if delay == 0 {
						delay = 5 * time.Millisecond
					} else {
						delay *= 2
					}
					if max := 1 * time.Second; delay > max {
						delay = max
					}
					continue
				}
				log.Warn("serverimpl{%s}.Accept() = err:%+v", s.addr, jerrors.ErrorStack(err))
				continue
			}
			delay = 0
			client.(*session).run()
		}
	}()
}

// RunEventLoop serves client request.
// @newSession: new connection callback
func (s *serverimpl) RunEventLoop(newSession NewSessionCallback) {
	if err := s.listen(); err != nil {
		panic(fmt.Errorf("serverimpl.listen() = error:%+v", jerrors.ErrorStack(err)))
	}

	s.runTcpEventLoop(newSession)

}

func (s *serverimpl) PacketConn() net.PacketConn {
	return s.pktListener
}

func (s *serverimpl) Close() {
	s.stop()
	s.wg.Wait()
}
