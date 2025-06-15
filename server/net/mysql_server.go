package net

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"

	getty "github.com/AlexStocks/getty/transport"
	gxlog "github.com/AlexStocks/goext/log"
	gxnet "github.com/AlexStocks/goext/net"
	log "github.com/AlexStocks/log4go"
	gxsync "github.com/dubbogo/gost/sync"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
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
	conf           *conf.Cfg
	serverList     []Server
	taskPool       gxsync.GenericTaskPool
	messageHandler *DecoupledMySQLMessageHandler // 新增：共享的消息处理器
	xmysqlEngine   *engine.XMySQLEngine          // 新增 xmysqlEngine 字段
}

func NewMySQLServer(conf *conf.Cfg) *MySQLServer {
	// 在服务器创建时就初始化XMySQL引擎，整个服务器生命周期只创建一次
	xmysqlEngine := engine.NewXMySQLEngine(conf)

	// 使用已初始化的引擎创建消息处理器
	messageHandler := NewDecoupledMySQLMessageHandlerWithEngine(conf, xmysqlEngine)

	return &MySQLServer{
		conf:           conf,
		serverList:     nil,
		taskPool:       gxsync.NewTaskPoolSimple(0),
		messageHandler: messageHandler,
		xmysqlEngine:   xmysqlEngine,
	}
}

func (srv *MySQLServer) Start() {
	initProfiling(srv.conf)
	srv.taskPool = gxsync.NewTaskPoolSimple(0)
	srv.initServer(srv.conf)

	gxlog.CInfo(logBanner)
	gxlog.CInfo("启动成功")
	gxlog.CInfo("%s starts successfull! its version=%s, its listen ends=%s:%s",
		srv.conf.AppName, getty.Version, srv.conf.BindAddress, srv.conf.Port)
	log.Info("%s starts successfull! its version=%s, its listen ends=%s:%s",
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
	fmt.Println("MySQL服务器正在初始化...")

	var (
		addr   string
		server Server
	)

	addr = gxnet.HostAddress2(conf.BindAddress, strconv.Itoa(conf.Port))
	server = NewTCPServer(
		WithLocalAddress(addr),
		WithServerTaskPool(gxsync.NewTaskPoolSimple(0)),
	)

	logger.Debugf("TCP服务器创建成功，地址: %s\n", addr)

	server.RunEventLoop(func(session Session) error {
		logger.Debugf("新连接建立: %s\n", session.Stat())

		if conf.MySQLSessionParam.CompressEncoding {
			session.SetCompressType(getty.CompressZip)
		}
		tcpConn, ok := session.Conn().(*net.TCPConn)
		if !ok {
			panic(fmt.Sprintf("%s, session.conn{%#v} is not tcp connection", session.Stat(), session.Conn()))
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
		session.SetEventListener(srv.messageHandler)
		session.SetWQLen(conf.MySQLSessionParam.PkgWQSize)
		session.SetReadTimeout(conf.MySQLSessionParam.TcpReadTimeoutDuration)
		session.SetWriteTimeout(conf.MySQLSessionParam.TcpWriteTimeoutDuration)
		session.SetCronPeriod((int)(conf.SessionTimeoutDuration / 1e6))
		session.SetWaitTime(conf.MySQLSessionParam.WaitTimeoutDuration)

		logger.Debugf("会话配置完成: %s\n", session.Stat())
		return nil
	})

	srv.serverList = append(srv.serverList, server)
	fmt.Println("MySQL服务器初始化完成！")
}

func (srv *MySQLServer) uninitServer() {
	for _, server := range srv.serverList {
		server.Close()
	}
	if srv.taskPool != nil {
		srv.taskPool.Close()
	}

	// 清理XMySQL引擎资源
	if srv.xmysqlEngine != nil {
		// 如果 XMySQLEngine 有 Close 方法，调用它
		// srv.xmysqlEngine.Close()
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
