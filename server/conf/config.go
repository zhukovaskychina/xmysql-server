package conf

import (
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
	// 开发环境认证开关：true=免密（跳过口令校验），false=执行真实口令校验
	DevBypassPasswordAuth bool

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
	// slow query log
	SlowQueryLog     bool   `default:"false" yaml:"slow_query_log" json:"slow_query_log,omitempty"`
	SlowQueryLogFile string `default:"/var/log/mysql/slow_query.log" yaml:"slow_query_log_file" json:"slow_query_log_file,omitempty"`
	LongQueryTimeMs  int    `default:"1000" yaml:"long_query_time_ms" json:"long_query_time_ms,omitempty"`

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
		Raw:           ini.Empty(),
		User:          "mysql",
		BindAddress:   "127.0.0.1",
		Port:          3308,
		DataDir:       "data",
		SessionNumber: 1000,
		// 默认关闭免密；本地联调可显式改为 true 并配合 127.0.0.1 监听
		DevBypassPasswordAuth: false,
		// Logs 默认配置
		LogError: "/var/log/mysql/error.log",
		LogInfos: "/var/log/mysql/mysql.log",
		// 慢查询日志默认配置
		SlowQueryLog:     false,
		SlowQueryLogFile: "/var/log/mysql/slow_query.log",
		LongQueryTimeMs:  1000,
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
		logger.Warnf("配置加载失败，使用默认配置: %v", err)
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
	cfg.MySQLSessionParam = MySQLSessionParam{}
	cfg.MySQLSessionParam.CompressEncoding = parseBool(section, "compress_encoding", true)
	cfg.MySQLSessionParam.TcpNoDelay = parseBool(section, "tcp_no_delay", true)
	cfg.MySQLSessionParam.TcpKeepAlive = parseBool(section, "tcp_keep_alive", true)
	cfg.MySQLSessionParam.KeepAlivePeriod = parseString(section, "keep_alive_period", "180s")
	cfg.MySQLSessionParam.KeepAlivePeriodDuration = parseDurationOrDefault(
		"keep_alive_period",
		cfg.MySQLSessionParam.KeepAlivePeriod,
		180*time.Second,
	)
	cfg.MySQLSessionParam.TcpRBufSize = parseInt(section, "tcp_r_buf_size", 262144)
	cfg.MySQLSessionParam.TcpWBufSize = parseInt(section, "tcp_w_buf_size", 65536)
	cfg.MySQLSessionParam.PkgRQSize = parseInt(section, "pkg_rq_size", 1024)
	cfg.MySQLSessionParam.PkgWQSize = parseInt(section, "pkg_wq_size", 1024)
	cfg.MySQLSessionParam.TcpReadTimeoutDuration = parseDurationOrDefault(
		"tcp_read_timeout",
		parseString(section, "tcp_read_timeout", "1s"),
		time.Second,
	)
	cfg.MySQLSessionParam.TcpWriteTimeoutDuration = parseDurationOrDefault(
		"tcp_write_timeout",
		parseString(section, "tcp_write_timeout", "5s"),
		5*time.Second,
	)
	cfg.MySQLSessionParam.WaitTimeoutDuration = parseDurationOrDefault(
		"wait_timeout",
		parseString(section, "wait_timeout", "7s"),
		7*time.Second,
	)
	cfg.MySQLSessionParam.MaxMsgLen = parseInt(section, "max_msg_len", 1024)
	if cfg.MySQLSessionParam.MaxMsgLen <= 0 {
		logger.Warnf("max_msg_len 非法(%d)，回退到默认 1024", cfg.MySQLSessionParam.MaxMsgLen)
		cfg.MySQLSessionParam.MaxMsgLen = 1024
	}
	cfg.MySQLSessionParam.SessionName = parseString(section, "session_name", "echo-server")
	return cfg
}

func (cfg *Cfg) parseMysqldCfg(section *ini.Section) *Cfg {
	bindAdress := parseString(section, "bind-address", "localhost")
	bindAdress = normalizeBindAddress(bindAdress)
	ip := net.ParseIP(bindAdress)
	if ip == nil {
		logger.Warnf("bind-address 解析失败(%s)，回退到 127.0.0.1", bindAdress)
		bindAdress = "127.0.0.1"
	}

	sessionTimeoutValue := parseString(section, "session_timeout", "60s")
	cfg.SessionTimeout = sessionTimeoutValue
	cfg.SessionTimeoutDuration = parseDurationOrDefault(
		"session_timeout",
		sessionTimeoutValue,
		60*time.Second,
	)

	cfg.BindAddress = bindAdress
	cfg.DevBypassPasswordAuth = parseBool(section, "dev_bypass_password_auth", cfg.DevBypassPasswordAuth)
	if shouldRejectDevBypass(cfg.BindAddress, cfg.DevBypassPasswordAuth) {
		logger.Warnf("安全策略异常: 非本地监听下不允许开启 dev_bypass_password_auth，已关闭该配置")
		cfg.DevBypassPasswordAuth = false
	}

	cfg.Port = parseInt(section, "port", 3307)
	cfg.BaseDir = parseString(section, "basedir", cfg.BaseDir)
	cfg.DataDir = parseString(section, "datadir", cfg.DataDir)
	cfg.SessionNumber = parseInt(section, "max_session_number", cfg.SessionNumber)

	failFastTimeoutValue := parseString(section, "fail_fast_timeout", "5s")
	cfg.FailFastTimeout = failFastTimeoutValue
	cfg.FailFastTimeoutDuration = parseDurationOrDefault(
		"fail_fast_timeout",
		failFastTimeoutValue,
		5*time.Second,
	)
	return cfg
}

func parseString(section *ini.Section, key string, defaultValue string) string {
	value, err := valueAsString(section, key, defaultValue)
	if err != nil || strings.TrimSpace(value) == "" {
		logger.Warnf("%s 配置缺失或为空，使用默认值: %s", key, defaultValue)
		return defaultValue
	}
	return value
}

func parseInt(section *ini.Section, key string, defaultValue int) int {
	if section == nil {
		return defaultValue
	}
	raw, err := section.Key(key).Int()
	if err != nil {
		logger.Warnf("%s 配置解析失败(%v)，使用默认值: %d", key, err, defaultValue)
		return defaultValue
	}
	return raw
}

func parseBool(section *ini.Section, key string, defaultValue bool) bool {
	if section == nil {
		return defaultValue
	}
	raw, err := section.Key(key).Bool()
	if err != nil {
		logger.Warnf("%s 配置解析失败(%v)，使用默认值: %t", key, err, defaultValue)
		return defaultValue
	}
	return raw
}

func parseDurationOrDefault(name, value string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		logger.Warnf("time.ParseDuration(%s{%#v}) = error{%v}, fallback to %v", name, value, err, fallback)
		return fallback
	}
	return d
}

func normalizeBindAddress(bindAddress string) string {
	bindAddress = strings.TrimSpace(bindAddress)
	if bindAddress == "" {
		return bindAddress
	}
	if strings.EqualFold(bindAddress, "localhost") {
		return "127.0.0.1"
	}
	return strings.Trim(bindAddress, "[]")
}

func isLocalBindAddress(bindAddress string) bool {
	ip := net.ParseIP(bindAddress)
	return ip != nil && ip.IsLoopback()
}

func shouldRejectDevBypass(bindAddress string, devBypass bool) bool {
	return devBypass && !isLocalBindAddress(bindAddress)
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
	if cfg == nil || cfg.Raw == nil {
		return ""
	}

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
	if cfg == nil || cfg.Raw == nil {
		return 0
	}

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

	// Parse slow query settings
	cfg.SlowQueryLog = parseBool(section, "slow_query_log", cfg.SlowQueryLog)

	slowQueryLogFile, err := valueAsString(section, "slow_query_log_file", cfg.SlowQueryLogFile)
	if err == nil && strings.TrimSpace(slowQueryLogFile) != "" {
		cfg.SlowQueryLogFile = slowQueryLogFile
	}

	longQueryTimeMs := parseInt(section, "long_query_time_ms", cfg.LongQueryTimeMs)
	if longQueryTimeMs <= 0 {
		logger.Warnf("long_query_time_ms 配置无效(%d)，回退到默认值: %d", longQueryTimeMs, cfg.LongQueryTimeMs)
	}
	cfg.LongQueryTimeMs = longQueryTimeMs
	if cfg.LongQueryTimeMs <= 0 {
		cfg.LongQueryTimeMs = 1000
	}

	return cfg
}
