package conf

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"gopkg.in/ini.v1"
)

var ConfigPath string

type CommandLineArgs struct {
	ConfigPath string
}

/*
*
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

	// logs
	LogError string `default:"/var/log/mysql/error.log" yaml:"log_error" json:"log_error,omitempty"`
	LogInfos string `default:"/var/log/mysql/mysql.log" yaml:"log_infos" json:"log_infos,omitempty"`
	LogLevel string `default:"info" yaml:"log_level" json:"log_level,omitempty"`

	// innodb
	InnodbDataDir             string `default:"data" yaml:"innodb_data_dir" json:"innodb_data_dir,omitempty"`
	InnodbDataFilePath        string `default:"ibdata1:100M:autoextend" yaml:"innodb_data_file_path" json:"innodb_data_file_path,omitempty"`
	InnodbBufferPoolSize      int    `default:"134217728" yaml:"innodb_buffer_pool_size" json:"innodb_buffer_pool_size,omitempty"`
	InnodbPageSize            int    `default:"16384" yaml:"innodb_page_size" json:"innodb_page_size,omitempty"`
	InnodbLogFileSize         int    `default:"50331648" yaml:"innodb_log_file_size" json:"innodb_log_file_size,omitempty"`
	InnodbLogBufferSize       int    `default:"16777216" yaml:"innodb_log_buffer_size" json:"innodb_log_buffer_size,omitempty"`
	InnodbFlushLogAtTrxCommit int    `default:"1" yaml:"innodb_flush_log_at_trx_commit" json:"innodb_flush_log_at_trx_commit,omitempty"`
	InnodbFileFormat          string `default:"Barracuda" yaml:"innodb_file_format" json:"innodb_file_format,omitempty"`
	InnodbDefaultRowFormat    string `default:"DYNAMIC" yaml:"innodb_default_row_format" json:"innodb_default_row_format,omitempty"`
	InnodbDoublewrite         bool   `default:"true" yaml:"innodb_doublewrite" json:"innodb_doublewrite,omitempty"`
	InnodbAdaptiveHashIndex   bool   `default:"true" yaml:"innodb_adaptive_hash_index" json:"innodb_adaptive_hash_index,omitempty"`
	InnodbRedoLogDir          string `default:"redo" yaml:"innodb_redo_log_dir" json:"innodb_redo_log_dir,omitempty"`
	InnodbUndoLogDir          string `default:"undo" yaml:"innodb_undo_log_dir" json:"innodb_undo_log_dir,omitempty"`
	InnodbEncryption          InnodbEncryptionConfig

	// session tcp parameters
	MySQLSessionParam MySQLSessionParam `required:"true" yaml:"getty_session_param" json:"getty_session_param,omitempty"`
}
type InnodbEncryptionConfig struct {
	MasterKey       string `default:"" yaml:"master_key" json:"master_key,omitempty"`
	KeyRotationDays int    `default:"90" yaml:"key_rotation_days" json:"key_rotation_days,omitempty"`
	Threads         int    `default:"4" yaml:"threads" json:"threads,omitempty"`
	BufferSize      int    `default:"8388608" yaml:"buffer_size" json:"buffer_size,omitempty"`
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
		DataDir:     "data",
		// Logs 默认配置
		LogError: "/var/log/mysql/error.log",
		LogInfos: "/var/log/mysql/mysql.log",
		// InnoDB 默认配置
		InnodbDataDir:             "data",
		InnodbDataFilePath:        "ibdata1:100M:autoextend",
		InnodbBufferPoolSize:      134217728, // 128MB
		InnodbPageSize:            16384,     // 16KB
		InnodbLogFileSize:         50331648,  // 48MB
		InnodbLogBufferSize:       16777216,  // 16MB
		InnodbFlushLogAtTrxCommit: 1,
		InnodbFileFormat:          "Barracuda",
		InnodbDefaultRowFormat:    "DYNAMIC",
		InnodbDoublewrite:         true,
		InnodbAdaptiveHashIndex:   true,
		InnodbRedoLogDir:          "redo",
		InnodbUndoLogDir:          "undo",
		InnodbEncryption: InnodbEncryptionConfig{
			KeyRotationDays: 90,
			Threads:         4,
			BufferSize:      8388608,
		},
	}
}

func (cfg *Cfg) Load(args *CommandLineArgs) *Cfg {
	setHomePath(args)
	iniFile, err := cfg.loadConfiguration(args)
	if err != nil {
		logger.Debugf("加载配置文件时有异常: %v\n", err)
		os.Exit(1)
	}
	cfg.Raw = iniFile

	cfg.parseMysqldCfg(cfg.Raw.Section("mysqld"))
	cfg.parseMysqlSessionCfg(cfg.Raw.Section("session"))
	cfg.parseInnodbCfg(cfg.Raw.Section("innodb"))
	cfg.parseLogsCfg(cfg.Raw.Section("logs"))
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
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}
	tcpNoDelay, err := section.GetKey("tcp_no_delay")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}

	tcpKeepAlive, err := section.GetKey("tcp_keep_alive")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}

	keepAlivePeriod, err := section.GetKey("keep_alive_period")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}

	tcpRBufSize, err := section.GetKey("tcp_r_buf_size")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}
	tcpWBufSize, err := section.GetKey("tcp_w_buf_size")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}

	pkgRqSize, err := section.GetKey("pkg_rq_size")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}

	pkgWqSize, err := section.GetKey("pkg_wq_size")
	if err != nil {

	}

	tcpReadTimeout, err := section.GetKey("tcp_read_timeout")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}
	tcpWriteTimeout, err := section.GetKey("tcp_write_timeout")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}
	waitTimeout, err := section.GetKey("wait_timeout")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}
	maxMsgLen, err := section.GetKey("max_msg_len")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}
	sessionName, err := section.GetKey("session_name")
	if err != nil {
		logger.Error("compress_encoding异常", err)
		os.Exit(1)
	}

	cfg.MySQLSessionParam.CompressEncoding = compressEncoding.MustBool(true)
	cfg.MySQLSessionParam.TcpNoDelay = tcpNoDelay.MustBool(true)
	cfg.MySQLSessionParam.TcpKeepAlive = tcpKeepAlive.MustBool(true)
	cfg.MySQLSessionParam.KeepAlivePeriod = keepAlivePeriod.Value()
	cfg.MySQLSessionParam.KeepAlivePeriodDuration, err = time.ParseDuration(keepAlivePeriod.Value())
	if err != nil {
		logger.Error(fmt.Sprintf("time.ParseDuration(KeepAlivePeriod{%#v}) = error{%v}", cfg.MySQLSessionParam.KeepAlivePeriod, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.TcpRBufSize, err = tcpRBufSize.Int()
	if err != nil {
		logger.Error(fmt.Sprintf("(TcpRBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpRBufSize, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.TcpWBufSize, err = tcpWBufSize.Int()
	if err != nil {
		logger.Error(fmt.Sprintf("(TcpWBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpWBufSize, err))
		os.Exit(1)
	}

	cfg.MySQLSessionParam.PkgRQSize, err = pkgRqSize.Int()
	if err != nil {
		logger.Error(fmt.Sprintf("(TcpRBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpRBufSize, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.PkgWQSize, err = pkgWqSize.Int()
	if err != nil {
		logger.Error(fmt.Sprintf("(TcpWBufSize{%#v}) = error{%v}", cfg.MySQLSessionParam.TcpWBufSize, err))
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
		logger.Error(fmt.Sprintf("(WaitTimeout{%#v}) = error{%v}", cfg.MySQLSessionParam.WaitTimeoutDuration, err))
		os.Exit(1)

	}

	cfg.MySQLSessionParam.MaxMsgLen, err = maxMsgLen.Int()
	if err != nil {
		logger.Error(fmt.Sprintf("(MaxMsgLen{%#v}) = error{%v}", cfg.MySQLSessionParam.MaxMsgLen, err))
		os.Exit(1)
	}
	cfg.MySQLSessionParam.SessionName = sessionName.Value()
	return cfg
}

func (cfg *Cfg) parseMysqldCfg(section *ini.Section) *Cfg {
	var err error
	bindAdress, err := valueAsString(section, "bind-address", "localhost")
	if err != nil {
		logger.Error("读取地址异常", err)
		os.Exit(1)
	}
	ip := net.ParseIP(bindAdress)
	if ip == nil {
		logger.Error("IP地址异常", err)
		os.Exit(1)
	}
	portValue, err := section.GetKey("port")
	if err != nil {
		logger.Error("IP地址配置异常", err)
		os.Exit(1)
	}
	intPort := portValue.MustInt(3307)

	baseDirValue, err := section.GetKey("basedir")
	if err != nil {
		logger.Error("IP地址配置异常", err)
		os.Exit(1)
	}
	dataDirValue, err := section.GetKey("datadir")
	if err != nil {
		logger.Error("IP地址配置异常", err)
		os.Exit(1)
	}

	maxSessionNumber, err := section.GetKey("max_session_number")

	if err != nil {
		logger.Error("最大数值异常", err)
		os.Exit(1)
	}
	cfg.SessionNumber, err = maxSessionNumber.Int()
	if err != nil {
		logger.Error("最大数值异常", err)
		os.Exit(1)
	}
	sessionTimeout, err := section.GetKey("session_timeout")
	cfg.SessionTimeoutDuration, err = time.ParseDuration(sessionTimeout.Value())
	if err != nil {
		logger.Error("超时配置异常")
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

	// 如果没有指定配置文件路径，使用默认的conf/my.ini
	configFile := "conf/my.ini"
	if args.ConfigPath != "" {
		configFile = args.ConfigPath
	}

	// check if config file exists
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		logger.Debugf("配置文件不存在: %s，使用默认配置\n", configFile)
		// 如果配置文件不存在，返回一个空的ini文件，使用默认配置
		return ini.Empty(), nil
	}

	// load configuration file
	parsedFile, err := ini.Load(configFile)
	if err != nil {
		logger.Debugf("解析配置文件失败: %v，使用默认配置\n", err)
		// 如果解析失败，返回一个空的ini文件，使用默认配置
		return ini.Empty(), nil
	}

	logger.Debugf("成功加载配置文件: %s\n", configFile)
	return parsedFile, nil
}

func valueAsString(section *ini.Section, keyName string, defaultValue string) (value string, err error) {
	if section == nil {
		return defaultValue, nil
	}
	value = section.Key(keyName).MustString(defaultValue)
	if value == "" {
		value = defaultValue
	}
	return value, nil
}

// GetString 获取配置项的字符串值
func (cfg *Cfg) GetString(key string) string {
	parts := strings.Split(key, ".")
	if len(parts) < 2 {
		return ""
	}

	section := cfg.Raw.Section(parts[0])
	if section == nil {
		return ""
	}

	value, err := valueAsString(section, strings.Join(parts[1:], "."), "")
	if err != nil {
		return ""
	}
	return value
}

// GetInt 获取配置项的整数值
func (cfg *Cfg) GetInt(key string) int {
	parts := strings.Split(key, ".")
	if len(parts) < 2 {
		return 0
	}

	section := cfg.Raw.Section(parts[0])
	if section == nil {
		return 0
	}

	return section.Key(strings.Join(parts[1:], ".")).MustInt(0)
}

func (cfg *Cfg) parseInnodbCfg(section *ini.Section) *Cfg {
	if section == nil {
		return cfg
	}

	// Parse data directory
	dataDir, err := valueAsString(section, "data_dir", cfg.InnodbDataDir)
	if err == nil {
		cfg.InnodbDataDir = dataDir
	}

	// Parse data file path
	dataFilePath, err := valueAsString(section, "data_file_path", cfg.InnodbDataFilePath)
	if err == nil {
		cfg.InnodbDataFilePath = dataFilePath
	}

	// Parse buffer pool size
	bufferPoolSize := section.Key("buffer_pool_size").MustInt(cfg.InnodbBufferPoolSize)
	cfg.InnodbBufferPoolSize = bufferPoolSize

	// Parse page size
	pageSize := section.Key("page_size").MustInt(cfg.InnodbPageSize)
	cfg.InnodbPageSize = pageSize

	// Parse log file size
	logFileSize := section.Key("log_file_size").MustInt(cfg.InnodbLogFileSize)
	cfg.InnodbLogFileSize = logFileSize

	// Parse log buffer size
	logBufferSize := section.Key("log_buffer_size").MustInt(cfg.InnodbLogBufferSize)
	cfg.InnodbLogBufferSize = logBufferSize

	// Parse flush log at trx commit
	flushLogAtTrxCommit := section.Key("flush_log_at_trx_commit").MustInt(cfg.InnodbFlushLogAtTrxCommit)
	cfg.InnodbFlushLogAtTrxCommit = flushLogAtTrxCommit

	// Parse file format
	fileFormat, err := valueAsString(section, "file_format", cfg.InnodbFileFormat)
	if err == nil {
		cfg.InnodbFileFormat = fileFormat
	}

	// Parse default row format
	defaultRowFormat, err := valueAsString(section, "default_row_format", cfg.InnodbDefaultRowFormat)
	if err == nil {
		cfg.InnodbDefaultRowFormat = defaultRowFormat
	}

	// Parse doublewrite
	doublewrite := section.Key("doublewrite").MustBool(cfg.InnodbDoublewrite)
	cfg.InnodbDoublewrite = doublewrite

	// Parse adaptive hash index
	adaptiveHashIndex := section.Key("adaptive_hash_index").MustBool(cfg.InnodbAdaptiveHashIndex)
	cfg.InnodbAdaptiveHashIndex = adaptiveHashIndex

	// Parse redo log directory
	redoDir, err := valueAsString(section, "redo_log_dir", cfg.InnodbRedoLogDir)
	if err == nil {
		cfg.InnodbRedoLogDir = redoDir
	}

	// Parse undo log directory
	undoDir, err := valueAsString(section, "undo_log_dir", cfg.InnodbUndoLogDir)
	if err == nil {
		cfg.InnodbUndoLogDir = undoDir
	}

	// Parse encryption settings
	masterKey, err := valueAsString(section, "encryption.master_key", cfg.InnodbEncryption.MasterKey)
	if err == nil {
		cfg.InnodbEncryption.MasterKey = masterKey
	}

	keyRotationDays := section.Key("encryption.key_rotation_days").MustInt(cfg.InnodbEncryption.KeyRotationDays)
	cfg.InnodbEncryption.KeyRotationDays = keyRotationDays

	threads := section.Key("encryption.threads").MustInt(cfg.InnodbEncryption.Threads)
	cfg.InnodbEncryption.Threads = threads

	bufferSize := section.Key("encryption.buffer_size").MustInt(cfg.InnodbEncryption.BufferSize)
	cfg.InnodbEncryption.BufferSize = bufferSize

	return cfg
}

func (cfg *Cfg) parseLogsCfg(section *ini.Section) *Cfg {
	if section == nil {
		return cfg
	}

	// Parse log error
	logError, err := valueAsString(section, "log_error", cfg.LogError)
	if err == nil {
		cfg.LogError = logError
	}

	// Parse log infos
	logInfos, err := valueAsString(section, "log_infos", cfg.LogInfos)
	if err == nil {
		cfg.LogInfos = logInfos
	}

	// Parse log level
	logLevel, err := valueAsString(section, "log_level", cfg.LogLevel)
	if err == nil {
		cfg.LogLevel = strings.ToLower(logLevel)
		// 验证日志级别是否有效
		validLevels := []string{"debug", "info", "warn", "error", "fatal", "panic"}
		isValid := false
		for _, level := range validLevels {
			if cfg.LogLevel == level {
				isValid = true
				break
			}
		}
		if !isValid {
			logger.Debugf("警告: 无效的日志级别 '%s', 使用默认级别 'info'\n", logLevel)
			cfg.LogLevel = "info"
		}
	}

	return cfg
}
