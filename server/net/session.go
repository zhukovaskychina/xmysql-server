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
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"

	gxbytes "github.com/dubbogo/gost/bytes"

	jerrors "github.com/juju/errors"

	log "github.com/AlexStocks/log4go"

	gxcontext "github.com/dubbogo/gost/context"

	gxtime "github.com/dubbogo/gost/time"
)

const (
	maxReadBufLen    = 4 * 1024
	netIOTimeout     = 30e9     // 30s (原来是1s，太短了)
	period           = 60 * 1e9 // 1 minute
	pendingDuration  = 3e9
	defaultQLen      = 1024
	maxIovecNum      = 10
	MaxWheelTimeSpan = 900e9 // 900s, 15 minute

	defaultSessionName    = "session"
	defaultTCPSessionName = "tcp-session"

	outputFormat = "session %s, Read Bytes: %d, Write Bytes: %d, Read Pkgs: %d, Write Pkgs: %d"
)

/////////////////////////////////////////
// session
/////////////////////////////////////////

var (
	wheel *gxtime.Wheel
)

func init() {
	span := 100e6 // 100ms
	buckets := MaxWheelTimeSpan / span
	wheel = gxtime.NewWheel(time.Duration(span), int(buckets)) // wheel longest span is 15 minute
}

func GetTimeWheel() *gxtime.Wheel {
	return wheel
}

// mysql_server base session
type session struct {
	name     string
	endPoint EndPoint

	// net read Write
	Connection
	listener EventListener

	// codec
	reader Reader // @reader should be nil when @conn is a gettyWSConn object.
	writer Writer

	// write
	wQ chan interface{}

	// handle logic
	maxMsgLen int32

	// heartbeat
	period time.Duration

	// done
	wait time.Duration
	once *sync.Once
	done chan struct{}

	// attribute
	attrs *gxcontext.ValuesContext

	// goroutines sync
	grNum int32
	// read goroutines done signal
	rDone chan struct{}
	lock  sync.RWMutex
}

func newSession(endPoint EndPoint, conn Connection) *session {
	ss := &session{
		name:     defaultSessionName,
		endPoint: endPoint,

		Connection: conn,

		maxMsgLen: maxReadBufLen,

		period: period,

		once:  &sync.Once{},
		done:  make(chan struct{}),
		wait:  pendingDuration,
		attrs: gxcontext.NewValuesContext(context.Background()),
		rDone: make(chan struct{}),
	}

	ss.Connection.setSession(ss)
	ss.SetWriteTimeout(netIOTimeout)
	ss.SetReadTimeout(netIOTimeout)

	return ss
}

func newTCPSession(conn net.Conn, endPoint EndPoint) Session {
	c := newMySQLTCPConn(conn)
	session := newSession(endPoint, c)
	session.name = defaultTCPSessionName

	return session
}

func (s *session) Reset() {
	*s = session{
		name:   defaultSessionName,
		once:   &sync.Once{},
		done:   make(chan struct{}),
		period: period,
		wait:   pendingDuration,
		attrs:  gxcontext.NewValuesContext(context.Background()),
		rDone:  make(chan struct{}),
	}
}

// func (s *session) SetConn(conn net.Conn) { s.gettyConn = newGettyConn(conn) }
func (s *session) Conn() net.Conn {
	if tc, ok := s.Connection.(*MysqlTCPConn); ok {
		return tc.conn
	}

	return nil
}

func (s *session) EndPoint() EndPoint {
	return s.endPoint
}

func (s *session) gettyConn() *mysqlConn {
	if tc, ok := s.Connection.(*MysqlTCPConn); ok {
		return &(tc.mysqlConn)
	}

	return nil
}

// return the connect statistic data
func (s *session) Stat() string {
	var conn *mysqlConn
	if conn = s.gettyConn(); conn == nil {
		return ""
	}
	return fmt.Sprintf(
		outputFormat,
		s.sessionToken(),
		atomic.LoadUint32(&(conn.readBytes)),
		atomic.LoadUint32(&(conn.writeBytes)),
		atomic.LoadUint32(&(conn.readPkgNum)),
		atomic.LoadUint32(&(conn.writePkgNum)),
	)
}

// check whether the session has been closed.
func (s *session) IsClosed() bool {
	select {
	case <-s.done:
		return true

	default:
		return false
	}
}

// set maximum package length of every package in (EventListener)OnMessage(@pkgs)
func (s *session) SetMaxMsgLen(length int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.maxMsgLen = int32(length)
}

// set session name
func (s *session) SetName(name string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.name = name
}

// set EventListener
func (s *session) SetEventListener(listener EventListener) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.listener = listener
}

// set package handler
func (s *session) SetPkgHandler(handler ReadWriter) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.reader = handler
	s.writer = handler
}

// set Reader
func (s *session) SetReader(reader Reader) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.reader = reader
}

// set Writer
func (s *session) SetWriter(writer Writer) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.writer = writer
}

// period is in millisecond. Websocket session will send ping frame automatically every peroid.
func (s *session) SetCronPeriod(period int) {
	if period < 1 {
		panic("@period < 1")
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.period = time.Duration(period) * time.Millisecond
}

// set @session's Write queue size
func (s *session) SetWQLen(writeQLen int) {
	if writeQLen < 1 {
		panic("@writeQLen < 1")
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.wQ = make(chan interface{}, writeQLen)
	log.Debug("%s, [session.SetWQLen] wQ{len:%d, cap:%d}", s.Stat(), len(s.wQ), cap(s.wQ))
}

// set maximum wait time when session got error or got exit signal
func (s *session) SetWaitTime(waitTime time.Duration) {
	if waitTime < 1 {
		panic("@wait < 1")
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.wait = waitTime
}

// set attribute of key @session:key
func (s *session) GetAttribute(key interface{}) interface{} {
	s.lock.RLock()
	if s.attrs == nil {
		s.lock.RUnlock()
		return nil
	}
	ret, flag := s.attrs.Get(key)
	s.lock.RUnlock()

	if !flag {
		return nil
	}

	return ret
}

// get attribute of key @session:key
func (s *session) SetAttribute(key interface{}, value interface{}) {
	s.lock.Lock()
	if s.attrs != nil {
		s.attrs.Set(key, value)
	}
	s.lock.Unlock()
}

// delete attribute of key @session:key
func (s *session) RemoveAttribute(key interface{}) {
	s.lock.Lock()
	if s.attrs != nil {
		s.attrs.Delete(key)
	}
	s.lock.Unlock()
}

func (s *session) sessionToken() string {
	if s.IsClosed() || s.Connection == nil {
		return "session-closed"
	}

	return fmt.Sprintf("{%s:%s:%d:%s<->%s}",
		s.name, s.EndPoint().EndPointType(), s.ID(), s.LocalAddr(), s.RemoteAddr())
}

func (s *session) WritePkg(pkg interface{}, timeout time.Duration) error {
	if pkg == nil {
		return fmt.Errorf("@pkg is nil")
	}
	if s.IsClosed() {
		return ErrSessionClosed
	}

	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			rBuf := make([]byte, size)
			rBuf = rBuf[:runtime.Stack(rBuf, false)]
			log.Error("[session.WritePkg] panic session %s: err=%s%s", s.sessionToken(), r, rBuf)
		}
	}()

	if timeout <= 0 {
		pkgBytes, err := s.writer.Write(s, pkg)
		if err != nil {
			log.Warn("%s, [session.WritePkg] session.writer.Write(@pkg:%#v) = error:%+v", s.Stat(), pkg, err)
			return jerrors.Trace(err)
		}

		pkg = pkgBytes

		_, err = s.Connection.send(pkg)
		if err != nil {
			log.Warn("%s, [session.WritePkg] @s.Connection.Write(pkg:%#v) = err:%+v", s.Stat(), pkg, err)
			return jerrors.Trace(err)
		}
		return nil
	}
	select {
	case s.wQ <- pkg:
		break // for possible gen a new pkg

	case <-wheel.After(timeout):
		log.Warn("%s, [session.WritePkg] wQ{len:%d, cap:%d}", s.Stat(), len(s.wQ), cap(s.wQ))
		return ErrSessionBlocked
	}

	return nil
}

// for codecs
func (s *session) WriteBytes(pkg []byte) error {
	if s.IsClosed() {
		return ErrSessionClosed
	}

	// s.conn.SetWriteTimeout(time.Now().Add(s.wTimeout))
	if _, err := s.Connection.send(pkg); err != nil {
		return jerrors.Annotatef(err, "s.Connection.Write(pkg len:%d)", len(pkg))
	}
	return nil
}

// Write multiple packages at once. so we invoke write sys.call just one time.
func (s *session) WriteBytesArray(pkgs ...[]byte) error {
	if s.IsClosed() {
		return ErrSessionClosed
	}
	// s.conn.SetWriteTimeout(time.Now().Add(s.wTimeout))
	if len(pkgs) == 1 {
		// return s.Connection.Write(pkgs[0])
		return s.WriteBytes(pkgs[0])
	}

	// reduce syscall and memcopy for multiple packages
	if _, ok := s.Connection.(*MysqlTCPConn); ok {
		if _, err := s.Connection.send(pkgs); err != nil {
			return jerrors.Annotatef(err, "s.Connection.Write(pkgs num:%d)", len(pkgs))
		}
		return nil
	}

	// get len
	var (
		l      int
		err    error
		length int
		arrp   *[]byte
		arr    []byte
	)
	length = 0
	for i := 0; i < len(pkgs); i++ {
		length += len(pkgs[i])
	}

	// merge the pkgs
	//arr = make([]byte, length)
	arrp = gxbytes.GetBytes(length)
	defer gxbytes.PutBytes(arrp)
	arr = *arrp

	l = 0
	for i := 0; i < len(pkgs); i++ {
		copy(arr[l:], pkgs[i])
		l += len(pkgs[i])
	}

	if err = s.WriteBytes(arr); err != nil {
		return jerrors.Trace(err)
	}

	num := len(pkgs) - 1
	for i := 0; i < num; i++ {
		s.incWritePkgNum()
	}

	return nil
}

// func (s *session) RunEventLoop() {
func (s *session) run() {
	if s.Connection == nil || s.listener == nil || s.writer == nil {
		errStr := fmt.Sprintf("session{name:%s, conn:%#v, listener:%#v, writer:%#v}",
			s.name, s.Connection, s.listener, s.writer)
		log.Error(errStr)
		panic(errStr)
	}

	if s.wQ == nil {
		s.wQ = make(chan interface{}, defaultQLen)
	}

	// call session opened
	s.UpdateActive()
	if err := s.listener.OnOpen(s); err != nil {
		log.Error("[OnOpen] session %s, error: %#v", s.Stat(), err)
		s.Close()
		return
	}

	// start read/write gr
	atomic.AddInt32(&(s.grNum), 2)
	go s.handleLoop()
	go s.handlePackage()
}

func (s *session) handleLoop() {
	var (
		err      error
		ok       bool
		flag     bool
		wsFlag   bool
		udpFlag  bool
		loopFlag bool
		//	wsConn   *gettyWSConn
		counter  gxtime.CountWatch
		outPkg   interface{}
		pkgBytes []byte
		iovec    [][]byte
	)

	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			rBuf := make([]byte, size)
			rBuf = rBuf[:runtime.Stack(rBuf, false)]
			log.Error("[session.handleLoop] panic session %s: err=%s%s", s.sessionToken(), r, rBuf)
		}

		grNum := atomic.AddInt32(&(s.grNum), -1)
		s.listener.OnClose(s)
		log.Info("%s, [session.handleLoop] goroutine exit now, left gr num %d", s.Stat(), grNum)
		s.gc()
	}()

	flag = true // do not do any read/Write/cron operation while got Write error

	iovec = make([][]byte, 0, maxIovecNum)
LOOP:
	for {
		// A select blocks until one of its cases is ready to run.
		// It choose one at random if multiple are ready. Otherwise it choose default branch if none is ready.
		select {
		case <-s.done:
			// this case assure the (session)handleLoop gr will exit before (session)handlePackage gr.
			<-s.rDone

			if len(s.wQ) == 0 {
				log.Info("%s, [session.handleLoop] got done signal. wQ is nil.", s.Stat())
				break LOOP
			}
			counter.Start()
			if counter.Count() > s.wait.Nanoseconds() {
				log.Info("%s, [session.handleLoop] got done signal ", s.Stat())
				break LOOP
			}

		case outPkg, ok = <-s.wQ:
			if !ok {
				continue
			}
			if !flag {
				log.Warn("[session.handleLoop] drop write out package %#v", outPkg)
				continue
			}

			if udpFlag || wsFlag {
				err = s.WritePkg(outPkg, 0)
				if err != nil {
					log.Error("%s, [session.handleLoop] = error:%+v", s.sessionToken(), jerrors.ErrorStack(err))
					s.stop()
					// break LOOP
					flag = false
				}

				continue
			}

			iovec = iovec[:0]
			for idx := 0; idx < maxIovecNum; idx++ {
				pkgBytes, err = s.writer.Write(s, outPkg)
				if err != nil {
					log.Error("%s, [session.handleLoop] = error:%+v", s.sessionToken(), jerrors.ErrorStack(err))
					s.stop()
					// break LOOP
					flag = false
					break
				}
				iovec = append(iovec, pkgBytes)

				if idx < maxIovecNum-1 {
					loopFlag = true
					select {
					case outPkg, ok = <-s.wQ:
						if !ok {
							loopFlag = false
						}

					default:
						loopFlag = false
						break
					}
					if !loopFlag {
						break // break for-idx loop
					}
				}
			}
			err = s.WriteBytesArray(iovec[:]...)
			if err != nil {
				log.Error("%s, [session.handleLoop]s.WriteBytesArray(iovec len:%d) = error:%+v",
					s.sessionToken(), len(iovec), jerrors.ErrorStack(err))
				s.stop()
				// break LOOP
				flag = false
			}

		case <-wheel.After(s.period):
			if flag {
				s.listener.OnCron(s)
			}
		}
	}
}

func (s *session) addTask(pkg interface{}) {
	f := func() {
		s.listener.OnMessage(s, pkg)
		s.incReadPkgNum()
	}

	if taskPool := s.EndPoint().GetTaskPool(); taskPool != nil {
		taskPool.AddTask(f)
		return
	}

	f()
}

func (s *session) handlePackage() {
	var (
		err error
	)

	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			rBuf := make([]byte, size)
			rBuf = rBuf[:runtime.Stack(rBuf, false)]
			log.Error("[session.handlePackage] panic session %s: err=%s%s", s.sessionToken(), r, rBuf)
		}

		close(s.rDone)
		grNum := atomic.AddInt32(&(s.grNum), -1)
		log.Info("%s, [session.handlePackage] gr will exit now, left gr num %d", s.sessionToken(), grNum)
		s.stop()
		if err != nil {
			log.Error("%s, [session.handlePackage] error:%+v", s.sessionToken(), jerrors.ErrorStack(err))
			if s != nil || s.listener != nil {
				s.listener.OnError(s, err)
			}
		}
	}()

	if _, ok := s.Connection.(*MysqlTCPConn); ok {
		if s.reader == nil {
			errStr := fmt.Sprintf("session{name:%s, conn:%#v, reader:%#v}", s.name, s.Connection, s.reader)
			log.Error(errStr)
			panic(errStr)
		}

		err = s.handleTCPPackage()
	} else {
		panic(fmt.Sprintf("unknown type session{%#v}", s))
	}
}

// get package from tcp stream(packet)
func (s *session) handleTCPPackage() error {
	var (
		ok       bool
		err      error
		netError net.Error
		conn     *MysqlTCPConn
		exit     bool
		bufLen   int
		pkgLen   int
		bufp     *[]byte
		buf      []byte
		pktBuf   *bytes.Buffer
		pkg      interface{}
	)

	bufp = gxbytes.GetBytes(maxReadBufLen)
	buf = *bufp

	pktBuf = gxbytes.GetBytesBuffer()

	defer func() {
		gxbytes.PutBytes(bufp)
		gxbytes.PutBytesBuffer(pktBuf)
	}()

	conn = s.Connection.(*MysqlTCPConn)
	for {
		if s.IsClosed() {
			err = nil
			// do not handle the left stream in pktBuf and exit asap.
			// it is impossible packing a package by the left stream.
			break
		}

		bufLen = 0
		for {
			// for clause for the network timeout condition check
			// s.conn.SetReadTimeout(time.Now().Add(s.rTimeout))
			bufLen, err = conn.recv(buf)
			if err != nil {
				if netError, ok = jerrors.Cause(err).(net.Error); ok && netError.Timeout() {
					break
				}
				if jerrors.Cause(err) == io.EOF {
					log.Info("%s, [session.conn.read] = error:%+v", s.sessionToken(), jerrors.ErrorStack(err))
					err = nil
					exit = true
					break
				}
				log.Error("%s, [session.conn.read] = error:%+v", s.sessionToken(), jerrors.ErrorStack(err))
				exit = true
			}
			break
		}
		if exit {
			break
		}
		if 0 == bufLen {
			continue // just continue if session can not read no more stream bytes.
		}

		logger.Printf("[session.handleTCPPackage] 收到数据，长度: %d, 内容: %v\n", bufLen, buf[:bufLen])

		pktBuf.Write(buf[:bufLen])
		for {
			if pktBuf.Len() <= 0 {
				break
			}

			logger.Debugf("[session.handleTCPPackage] 尝试解析包，缓冲区长度: %d\n", pktBuf.Len())

			pkg, pkgLen, err = s.reader.Read(s, pktBuf.Bytes())
			// for case 3/case 4
			if err == nil && s.maxMsgLen > 0 && pkgLen > int(s.maxMsgLen) {
				err = jerrors.Errorf("pkgLen %d > session max message len %d", pkgLen, s.maxMsgLen)
			}
			// handle case 1
			if err != nil {
				log.Warn("%s, [session.handleTCPPackage] = len{%d}, error:%+v",
					s.sessionToken(), pkgLen, jerrors.ErrorStack(err))
				exit = true
				break
			}
			// handle case 2/case 3
			if pkg == nil {
				logger.Debugf("[session.handleTCPPackage] 解析返回nil包，等待更多数据\n")
				break
			}
			// handle case 4
			logger.Debugf("[session.handleTCPPackage] 包解析成功，长度: %d，调用addTask\n", pkgLen)
			s.UpdateActive()
			s.addTask(pkg)
			pktBuf.Next(pkgLen)
			// continue to handle case 5
		}
		if exit {
			break
		}
	}

	return jerrors.Trace(err)
}

func (s *session) stop() {
	logger.Debugf("[session.stop] Session即将停止: %s\n", s.sessionToken())

	select {
	case <-s.done: // s.done is a blocked channel. if it has not been closed, the default branch will be invoked.
		logger.Debugf("[session.stop] Session已经停止: %s\n", s.sessionToken())
		return

	default:
		logger.Debugf("[session.stop] 执行停止操作: %s\n", s.sessionToken())
		s.once.Do(func() {
			// let read/Write timeout asap
			now := time.Now()
			if conn := s.Conn(); conn != nil {
				conn.SetReadDeadline(now.Add(s.readTimeout()))
				conn.SetWriteDeadline(now.Add(s.writeTimeout()))
			}
			close(s.done)
			c := s.GetAttribute(sessionClientKey)
			if clt, ok := c.(*client); ok {
				clt.reConnect()
			}
		})
	}
}

func (s *session) gc() {
	var (
		wQ   chan interface{}
		conn Connection
	)

	s.lock.Lock()
	if s.attrs != nil {
		s.attrs = nil
		if s.wQ != nil {
			wQ = s.wQ
			s.wQ = nil
		}
		conn = s.Connection
	}
	s.lock.Unlock()

	go func() {
		if wQ != nil {
			conn.close((int)((int64)(s.wait)))
			close(wQ)
		}
	}()
}

// Close will be invoked by NewSessionCallback(if return error is not nil)
// or (session)handleLoop automatically. It's thread safe.
func (s *session) Close() {
	logger.Debugf("[session.Close] 关闭Session: %s\n", s.sessionToken())
	s.stop()
	log.Info("%s closed now. its current gr num is %d",
		s.sessionToken(), atomic.LoadInt32(&(s.grNum)))
}

// MySQL serverimpl session 本地存储
type MySQLServerSessionImpl struct {
	server.MySQLServerSession
	session        Session
	reqNum         int32
	lastActiveTime time.Time //最后活动时间
}

func NewMySQLServerSession(session Session) server.MySQLServerSession {
	var mysqlSession = new(MySQLServerSessionImpl)
	mysqlSession.lastActiveTime = time.Now()
	mysqlSession.reqNum = 0
	mysqlSession.session = session
	return mysqlSession
}

func (m *MySQLServerSessionImpl) GetLastActiveTime() time.Time {
	return m.lastActiveTime
}

func (m *MySQLServerSessionImpl) SendOK() {
	buff := make([]byte, 0)
	buff = protocol.EncodeOK(buff, 0, 0, nil)
	m.session.WriteBytes(buff)
}

func (m *MySQLServerSessionImpl) SendHandleOk() {
	buff := make([]byte, 0)
	buff = protocol.EncodeHandshake(buff)
	m.session.WriteBytes(buff)
}

func (m *MySQLServerSessionImpl) GetParamByName(name string) interface{} {
	return m.session.GetAttribute(name)
}

func (m *MySQLServerSessionImpl) SetParamByName(name string, value interface{}) {
	m.session.SetAttribute(name, value)
}
