package conf

import (
	"errors"
	"fmt"
	"gopkg.in/ini.v1"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"
)

var ConfigPath string

type CommandLineArgs struct {
	ConfigPath string
}

/**
user		= mysql
pid-file	= /var/run/mysqld/mysqld.pid
socket		= /var/run/mysqld/mysqld.sock
port		= 3307
basedir		= /usr
datadir		= /var/lib/mysql
tmpdir		= /tmp
lc-messages-dir	= /usr/share/mysql
*/
type Cfg struct {
	Raw         *ini.File
	User        string
	BindAddress string
	Port        int
	BaseDir     string
	DataDir     string
	AppName     string

	ProfilePort int
	// session
	SessionTimeout         string `default:"60s" yaml:"session_timeout" json:"session_timeout,omitempty"`
	SessionTimeoutDuration time.Duration
	SessionNumber          int `default:"1000" yaml:"session_number" json:"session_number,omitempty"`

	// app
	FailFastTimeout         string `default:"5s" yaml:"fail_fast_timeout" json:"fail_fast_timeout,omitempty"`
	FailFastTimeoutDuration time.Duration

	// session tcp parameters
	MySQLSessionParam MySQLSessionParam `required:"true" yaml:"getty_session_param" json:"getty_session_param,omitempty"`
}
type MySQLSessionParam struct {
	CompressEncoding        bool   `default:"false" yaml:"compress_encoding" json:"compress_encoding,omitempty"`
	TcpNoDelay              bool   `default:"true" yaml:"tcp_no_delay" json:"tcp_no_delay,omitempty"`
	TcpKeepAlive            bool   `default:"true" yaml:"tcp_keep_alive" json:"tcp_keep_alive,omitempty"`
	KeepAlivePeriod         string `default:"180s" yaml:"keep_alive_period" json:"keep_alive_period,omitempty"`
	KeepAlivePeriodDuration time.Duration
	TcpRBufSize             int `default:"262144" yaml:"tcp_r_buf_size" json:"tcp_r_buf_size,omitempty"`
	TcpWBufSize             int `default:"65536" yaml:"tcp_w_buf_size" json:"tcp_w_buf_size,omitempty"`
	PkgRQSize               int
	PkgWQSize               int    `default:"1024" yaml:"pkg_wq_size" json:"pkg_wq_size,omitempty"`
	TcpReadTimeout          string `default:"1s" yaml:"tcp_read_timeout" json:"tcp_read_timeout,omitempty"`
	TcpReadTimeoutDuration  time.Duration
	TcpWriteTimeout         string `default:"5s" yaml:"tcp_write_timeout" json:"tcp_write_timeout,omitempty"`
	TcpWriteTimeoutDuration time.Duration
	WaitTimeout             string `default:"7s" yaml:"wait_timeout" json:"wait_timeout,omitempty"`
	WaitTimeoutDuration     time.Duration
	MaxMsgLen               int    `default:"1024" yaml:"max_msg_len" json:"max_msg_len,omitempty"`
	SessionName             string `default:"echo-server" yaml:"session_name" json:"session_name,omitempty"`
}

func NewCfg() *Cfg {
	return &Cfg{
		Raw:         ini.Empty(),
		User:        "mysql",
		BindAddress: "127.0.0.1",
		Port:        3308,
	}
}

func (cfg *Cfg) Load(args *CommandLineArgs) *Cfg {
	setHomePath(args)
	iniFile, err := cfg.loadConfiguration(args)
	if err != nil {
		fmt.Println("加载配置文件时有异常", err)
		os.Exit(1)
	}
	cfg.Raw = iniFile

	cfg.parseMysqldCfg(cfg.Raw.Section("mysqld"))
	cfg.parseMysqlSessionCfg(cfg.Raw.Section("session"))
	return cfg

}

func setHomePath(args *CommandLineArgs) {
	if args.ConfigPath != "" {
		ConfigPath = args.ConfigPath
		return
	}

	ConfigPath, _ = filepath.Abs(".")

}

func (cfg *Cfg) parseMysqlSessionCfg(section *ini.Section) *Cfg {

	var err error
	cfg.MySQLSessionParam = MySQLSessionParam{}

	compressEncoding, err := section.GetKey("compress_encoding")

	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}
	tcpNoDelay, err := section.GetKey("tcp_no_delay")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}

	tcpKeepAlive, err := section.GetKey("tcp_keep_alive")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}

	keepAlivePeriod, err := section.GetKey("keep_alive_period")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}

	tcpRBufSize, err := section.GetKey("tcp_r_buf_size")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}
	tcpWBufSize, err := section.GetKey("tcp_w_buf_size")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}

	pkgRqSize, err := section.GetKey("pkg_rq_size")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}

	pkgWqSize, err := section.GetKey("pkg_wq_size")
	if err != nil {

	}

	tcpReadTimeout, err := section.GetKey("tcp_read_timeout")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}
	tcpWriteTimeout, err := section.GetKey("tcp_write_timeout")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}
	waitTimeout, err := section.GetKey("wait_timeout")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}
	maxMsgLen, err := section.GetKey("max_msg_len")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}
	sessionName, err := section.GetKey("session_name")
	if err != nil {
		fmt.Println("compress_encoding异常", err)
		os.Exit(1)
	}

	cfg.MySQLSessionParam.CompressEncoding = compressEncoding.MustBool(true)
	cfg.MySQLSessionParam.TcpNoDelay = tcpNoDelay.MustBool(true)
	cfg.MySQLSessionParam.TcpKeepAlive = tcpKeepAlive.MustBool(true)
	cfg.MySQLSessionParam.KeepAlivePeriod = keepAlivePeriod.Value()
	cfg.MySQLSessionParam.KeepAlivePeriodDuration, err = time.ParseDuration(keepAlivePeriod.Value())
	if err != nil {
		fmt.Println(fmt.Sprintf("time.ParseDuration(KeepAlivePeriod{%#v}) = error{%v}", cfg.MySQLSessionParam.KeepAlivePeriod, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.TcpRBufSize, err = tcpRBufSize.Int()
	if err != nil {
		fmt.Println(fmt.Sprintf("(TcpRBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpRBufSize, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.TcpWBufSize, err = tcpWBufSize.Int()
	if err != nil {
		fmt.Println(fmt.Sprintf("(TcpWBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpWBufSize, err))
		os.Exit(1)
	}

	cfg.MySQLSessionParam.PkgRQSize, err = pkgRqSize.Int()
	if err != nil {
		fmt.Println(fmt.Sprintf("(TcpRBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpRBufSize, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.PkgWQSize, err = pkgWqSize.Int()
	if err != nil {
		fmt.Println(fmt.Sprintf("(TcpWBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpWBufSize, err))
		os.Exit(1)
	}

	cfg.MySQLSessionParam.TcpReadTimeoutDuration, err = time.ParseDuration(tcpReadTimeout.Value())
	if err != nil {
		panic(fmt.Sprintf("time.ParseDuration(TcpReadTimeout{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpReadTimeout, err))

	}
	cfg.MySQLSessionParam.TcpWriteTimeoutDuration, err = time.ParseDuration(tcpWriteTimeout.Value())
	if err != nil {
		panic(fmt.Sprintf("time.ParseDuration(TcpWriteTimeout{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpWriteTimeout, err))

	}
	cfg.MySQLSessionParam.WaitTimeoutDuration, err = time.ParseDuration(waitTimeout.Value())
	if err != nil {
		fmt.Println(fmt.Sprintf("(WaitTimeout{%#v}) = error{%v}", cfg.MySQLSessionParam.WaitTimeoutDuration, err))
		os.Exit(1)

	}

	cfg.MySQLSessionParam.MaxMsgLen, err = maxMsgLen.Int()
	if err != nil {
		fmt.Println(fmt.Sprintf("(MaxMsgLen{%#v}) = error{%v}", cfg.MySQLSessionParam.MaxMsgLen, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.SessionName = sessionName.Value()
	return cfg
}

func (cfg *Cfg) parseMysqldCfg(section *ini.Section) *Cfg {
	var err error
	bindAdress, err := valueAsString(section, "bind-address", "localhost")
	if err != nil {
		fmt.Println("读取地址异常", err)
		os.Exit(1)
	}
	ip := net.ParseIP(bindAdress)
	if ip == nil {
		fmt.Println("IP地址异常", err)
		os.Exit(1)
	}
	portValue, err := section.GetKey("port")
	if err != nil {
		fmt.Println("IP地址配置异常", err)
		os.Exit(1)
	}
	intPort := portValue.MustInt(3307)

	baseDirValue, err := section.GetKey("basedir")
	if err != nil {
		fmt.Println("IP地址配置异常", err)
		os.Exit(1)
	}
	dataDirValue, err := section.GetKey("datadir")
	if err != nil {
		fmt.Println("IP地址配置异常", err)
		os.Exit(1)
	}

	maxSessionNumber, err := section.GetKey("max_session_number")

	if err != nil {
		fmt.Println("最大数值异常", err)
		os.Exit(1)
	}
	cfg.SessionNumber, err = maxSessionNumber.Int()
	if err != nil {
		fmt.Println("最大数值异常", err)
		os.Exit(1)
	}
	sessionTimeout, err := section.GetKey("session_timeout")
	cfg.SessionTimeoutDuration, err = time.ParseDuration(sessionTimeout.Value())
	if err != nil {
		fmt.Println("超时配置异常")
		panic(fmt.Sprintf("time.ParseDuration(SessionTimeout{%#v}) = error{%v}", cfg.SessionTimeout, err))
	}

	cfg.BindAddress = bindAdress

	cfg.Port = intPort

	cfg.BaseDir = baseDirValue.Value()
	cfg.DataDir = dataDirValue.Value()
	failFastTimeout, err := section.GetKey("fail_fast_timeout")

	cfg.FailFastTimeout = failFastTimeout.Value()
	if err != nil {
		panic(fmt.Sprintf("time.ParseDuration(SessionTimeout{%#v}) = error{%v}", cfg.SessionTimeout, err))

	}
	cfg.FailFastTimeoutDuration, err = time.ParseDuration(cfg.FailFastTimeout)
	if err != nil {
		panic(fmt.Sprintf("time.ParseDuration(FailFastTimeout{%#v}) = error{%v}", cfg.FailFastTimeout, err))

	}
	cfg.SessionTimeout = sessionTimeout.Value()
	cfg.SessionTimeoutDuration, err = time.ParseDuration(sessionTimeout.Value())
	if err != nil {
		panic(fmt.Sprintf("time.ParseDuration(SessionTimeout{%#v}) = error{%v}", cfg.SessionTimeout, err))
	}
	return cfg
}

func (cfg *Cfg) loadConfiguration(args *CommandLineArgs) (*ini.File, error) {
	var err error

	defaultConfigFile := path.Join(args.ConfigPath, "")

	// check if config file exists
	if _, err := os.Stat(defaultConfigFile); os.IsNotExist(err) {
		fmt.Println("xmysql-server加载配置文件失败，请确保文件路径存在")
		os.Exit(1)
	}

	// load defaults
	parsedFile, err := ini.Load(defaultConfigFile)
	if err != nil {
		fmt.Println(fmt.Sprintf("Failed to parse defaults.ini, %v", err))
		os.Exit(1)
		return nil, err
	}
	return parsedFile, err
}

func valueAsString(section *ini.Section, keyName string, defaultValue string) (value string, err error) {
	defer func() {
		if err_ := recover(); err_ != nil {
			err = errors.New("Invalid valueImpl for key '" + keyName + "' in configuration file")
		}
	}()

	return section.Key(keyName).MustString(defaultValue), nil
}
