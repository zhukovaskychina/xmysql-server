package net

import (
	"fmt"
	"github.com/AlexStocks/getty/transport"
	gxlog "github.com/AlexStocks/goext/log"
	gxnet "github.com/AlexStocks/goext/net"
	log "github.com/AlexStocks/log4go"
	"github.com/dubbogo/gost/sync"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	//"github.com/zhukovaskychina/xmysql-server/server/innodb/wrapper/store"
)

const (
	pprofPath = "/debug/pprof/"
)
const logBanner = `
******************************************************************************************

 __   ____  __        _____  ____  _          _____ ______ _______      ________ _____  
 \ \ / /  \/  |      / ____|/ __ \| |        / ____|  ____|  __ \ \    / /  ____|  __ \ 
  \ V /| \  / |_   _| (___ | |  | | |  _____| (___ | |__  | |__) \ \  / /| |__  | |__) |
   > < | |\/| | | | |\___ \| |  | | | |______\___ \|  __| |  _  / \ \/ / |  __| |  _  / 
  / . \| |  | | |_| |____) | |__| | |____    ____) | |____| | \ \  \  /  | |____| | \ \ 
 /_/ \_\_|  |_|\__, |_____/ \___\_\______|  |_____/|______|_|  \_\  \/   |______|_|  \_\
                __/ |                                                                   
               |___/                                                                    
******************************************************************************************
`

var (
	mysqlPkgHandler = NewMySQLEchoPkgHandler() //Marsh Unmarsh
)

type MySQLServer struct {
	conf       *conf.Cfg
	serverList []Server
	taskPool   gxsync.GenericTaskPool
}

func NewMySQLServer(conf *conf.Cfg) *MySQLServer {

	return &MySQLServer{
		conf:       conf,
		serverList: nil,
		taskPool:   gxsync.NewTaskPoolSimple(0),
	}
}

func (srv *MySQLServer) Start() {
	initProfiling(srv.conf)
	srv.taskPool = gxsync.NewTaskPoolSimple(0)
	srv.initServer(srv.conf)

	gxlog.CInfo(logBanner)
	gxlog.CInfo("启动成功")
	gxlog.CInfo("%s starts successfull! its version=%s, its listen ends=%s:%s\n",
		srv.conf.AppName, getty.Version, srv.conf.BindAddress, srv.conf.Port)
	log.Info("%s starts successfull! its version=%s, its listen ends=%s:%s\n",
		srv.conf.AppName, getty.Version, srv.conf.BindAddress, srv.conf.Port)
	//srv.initPurgeThread()

	srv.initSignal()

}

func initProfiling(conf *conf.Cfg) {
	var (
		addr string
	)
	addr = gxnet.HostAddress(conf.BindAddress, conf.ProfilePort)
	log.Info("App Profiling startup on address{%v}", addr+pprofPath)
	go func() {
		log.Info(http.ListenAndServe(addr, nil))
	}()
}

func (srv *MySQLServer) initServer(conf *conf.Cfg) {
	var (
		addr     string
		portList []string
		server   Server
	)
	mysqlMsgHandler := NewMySQLMessageHandler(conf)
	portList = append(portList, strconv.Itoa(conf.Port))
	if len(portList) == 0 {
		panic("portList is nil")
	}
	for _, port := range portList {
		addr = gxnet.HostAddress2(conf.BindAddress, port)
		serverOpts := []ServerOption{WithLocalAddress(addr)}
		//serverOpts = append(serverOpts, getty.WithServerTaskPool(srv.taskPool))
		server = NewTCPServer(serverOpts...)
		// run serverimpl
		server.RunEventLoop(func(session Session) error {
			var (
				ok      bool
				tcpConn *net.TCPConn
			)
			if conf.MySQLSessionParam.CompressEncoding {
				session.SetCompressType(getty.CompressZip)
			}
			if tcpConn, ok = session.Conn().(*net.TCPConn); !ok {
				panic(fmt.Sprintf("%s, session.conn{%#v} is not tcp connection\n", session.Stat(), session.Conn()))
			}
			tcpConn.SetNoDelay(conf.MySQLSessionParam.TcpNoDelay)
			tcpConn.SetKeepAlive(conf.MySQLSessionParam.TcpKeepAlive)
			if conf.MySQLSessionParam.TcpKeepAlive {
				tcpConn.SetKeepAlivePeriod(conf.MySQLSessionParam.KeepAlivePeriodDuration)
			}
			tcpConn.SetReadBuffer(conf.MySQLSessionParam.TcpRBufSize)
			tcpConn.SetWriteBuffer(conf.MySQLSessionParam.TcpWBufSize)

			session.SetName(conf.MySQLSessionParam.SessionName)
			session.SetMaxMsgLen(conf.MySQLSessionParam.MaxMsgLen)
			session.SetPkgHandler(mysqlPkgHandler)
			session.SetEventListener(mysqlMsgHandler)
			session.SetWQLen(conf.MySQLSessionParam.PkgWQSize)
			session.SetReadTimeout(conf.MySQLSessionParam.TcpReadTimeoutDuration)
			session.SetWriteTimeout(conf.MySQLSessionParam.TcpWriteTimeoutDuration)
			session.SetCronPeriod((int)(conf.SessionTimeoutDuration / 1e6))
			session.SetWaitTime(conf.MySQLSessionParam.WaitTimeoutDuration)
			//session.SetTaskPool(taskPool)
			log.Debug("app accepts new session:%s\n", session.Stat())
			return nil
		})
		log.Debug("serverimpl bind addr{%s} ok!", addr)
		srv.serverList = append(srv.serverList, server)
	}
}

func (srv *MySQLServer) uninitServer() {
	for _, server := range srv.serverList {
		server.Close()
	}
	if srv.taskPool != nil {
		srv.taskPool.Close()
	}
}

func (srv *MySQLServer) initSignal() {
	// signal.Notify的ch信道是阻塞的(signal.Notify不会阻塞发送信号), 需要设置缓冲
	signals := make(chan os.Signal, 1)
	// It is not possible to block SIGKILL or syscall.SIGSTOP
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-signals
		log.Info("get signal %s", sig.String())
		switch sig {
		case syscall.SIGHUP:
		// reload()
		default:
			go time.AfterFunc(srv.conf.FailFastTimeoutDuration, func() {
				// log.Warn("app exit now by force...")
				// os.Exit(1)
				log.Exit("app exit now by force...")
				log.Close()
			})

			// 要么fastFailTimeout时间内执行完毕下面的逻辑然后程序退出，要么执行上面的超时函数程序强行退出
			srv.uninitServer()
			// fmt.Println("app exit now...")
			log.Exit("app exit now...")
			log.Close()
			return
		}
	}
}
