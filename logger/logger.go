package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

var (
	// Logger 全局日志实例
	Logger *logrus.Logger
	// InfoLogger 信息日志实例
	InfoLogger *logrus.Logger
	// ErrorLogger 错误日志实例
	ErrorLogger *logrus.Logger
)

// LogConfig 日志配置
type LogConfig struct {
	ErrorLogPath string
	InfoLogPath  string
	LogLevel     string // 新增日志级别配置
}

// CustomFormatter 自定义日志格式化器
type CustomFormatter struct {
	TimestampFormat string
}

// Format 实现 logrus.Formatter 接口
func (f *CustomFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// 格式化时间戳
	timestamp := entry.Time.Format("15:04:05 MST 2006/01/02")

	// 获取日志级别
	level := strings.ToUpper(entry.Level.String())
	if len(level) > 4 {
		level = level[:4]
	}

	// 获取调用者信息
	caller := getCaller()

	// 组装日志消息
	logMsg := fmt.Sprintf("[%s] [%s] (%s) %s\n",
		timestamp,
		level,
		caller,
		entry.Message)

	return []byte(logMsg), nil
}

// getCaller 获取调用者信息
func getCaller() string {
	// 跳过日志框架的调用栈，找到实际的调用者
	for i := 2; i < 20; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		// 跳过日志库本身的调用
		if strings.Contains(file, "/logrus/") ||
			strings.Contains(file, "/logger.go") ||
			strings.Contains(file, "util/logger.go") ||
			strings.Contains(file, "logrus") ||
			strings.Contains(file, "sirupsen") ||
			strings.Contains(file, "/entry.go") {
			continue
		}

		// 获取函数名
		funcName := runtime.FuncForPC(pc).Name()

		// 获取文件名（不包含路径）
		fileName := filepath.Base(file)

		// 格式化调用者信息
		// 格式: filename:package.function:line
		return fmt.Sprintf("%s:%s:%d", fileName, funcName, line)
	}

	return "unknown:unknown:0"
}

// parseLogLevel 解析日志级别字符串为logrus级别
func parseLogLevel(level string) logrus.Level {
	switch strings.ToLower(level) {
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	default:
		return logrus.InfoLevel // 默认级别
	}
}

// InitLogger 初始化日志
func InitLogger(config LogConfig) error {
	// 创建自定义格式化器
	customFormatter := &CustomFormatter{
		TimestampFormat: "15:04:05 MST 2006/01/02",
	}

	// 初始化主日志器
	Logger = logrus.New()
	Logger.SetFormatter(customFormatter)
	Logger.SetLevel(parseLogLevel(config.LogLevel))

	// 初始化信息日志器
	InfoLogger = logrus.New()
	InfoLogger.SetLevel(parseLogLevel(config.LogLevel))
	InfoLogger.SetFormatter(customFormatter)

	// 初始化错误日志器
	ErrorLogger = logrus.New()
	ErrorLogger.SetLevel(parseLogLevel(config.LogLevel))
	ErrorLogger.SetFormatter(customFormatter)

	// 设置信息日志输出
	if config.InfoLogPath != "" {
		infoLogFile, err := openLogFile(config.InfoLogPath)
		if err != nil {
			InfoLogger.SetOutput(os.Stdout)
			InfoLogger.Warnf("Failed to open info log file %s, fallback to stdout: %v", config.InfoLogPath, err)
		} else {
			InfoLogger.SetOutput(io.MultiWriter(os.Stdout, infoLogFile))
		}
	} else {
		InfoLogger.SetOutput(os.Stdout)
	}

	// 设置错误日志输出
	if config.ErrorLogPath != "" {
		errorLogFile, err := openLogFile(config.ErrorLogPath)
		if err != nil {
			ErrorLogger.SetOutput(os.Stderr)
			ErrorLogger.Warnf("Failed to open error log file %s, fallback to stderr: %v", config.ErrorLogPath, err)
		} else {
			ErrorLogger.SetOutput(io.MultiWriter(os.Stderr, errorLogFile))
		}
	} else {
		ErrorLogger.SetOutput(os.Stderr)
	}

	// 设置主日志器输出到信息日志
	Logger.SetOutput(InfoLogger.Out)

	return nil
}

// openLogFile 打开日志文件
func openLogFile(logPath string) (*os.File, error) {
	// 确保日志目录存在
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	// 打开或创建日志文件
	return os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
}

// Info 记录信息日志
func Info(args ...interface{}) {
	if InfoLogger != nil {
		InfoLogger.Info(args...)
	}
}

// Infof 记录格式化信息日志
func Infof(format string, args ...interface{}) {
	if InfoLogger != nil {
		InfoLogger.Infof(format, args...)
	}
}

// Debug 记录调试日志
func Debug(args ...interface{}) {
	if Logger != nil {
		Logger.Debug(args...)
	}
}

// Debugf 记录格式化调试日志
func Debugf(format string, args ...interface{}) {
	if Logger != nil {
		Logger.Debugf(format, args...)
	}
}

// Warn 记录警告日志
func Warn(args ...interface{}) {
	if Logger != nil {
		Logger.Warn(args...)
	}
}

// Warnf 记录格式化警告日志
func Warnf(format string, args ...interface{}) {
	if Logger != nil {
		Logger.Warnf(format, args...)
	}
}

// Error 记录错误日志
func Error(args ...interface{}) {
	if ErrorLogger != nil {
		ErrorLogger.Error(args...)
	}
}

// Errorf 记录格式化错误日志
func Errorf(format string, args ...interface{}) {
	if ErrorLogger != nil {
		ErrorLogger.Errorf(format, args...)
	}
}

// Fatal 记录致命错误日志并退出
func Fatal(args ...interface{}) {
	if ErrorLogger != nil {
		ErrorLogger.Fatal(args...)
	}
}

// Fatalf 记录格式化致命错误日志并退出
func Fatalf(format string, args ...interface{}) {
	if ErrorLogger != nil {
		ErrorLogger.Fatalf(format, args...)
	}
}

// Printf 兼容 fmt.Printf，使用 info 级别记录
func Printf(format string, args ...interface{}) {
	if InfoLogger != nil {
		InfoLogger.Infof(format, args...)
	} else {
		// 如果日志未初始化，回退到 fmt.Printf
		fmt.Printf(format, args...)
	}
}

// Print 兼容 fmt.Print，使用 info 级别记录
func Print(args ...interface{}) {
	if InfoLogger != nil {
		InfoLogger.Info(args...)
	} else {
		// 如果日志未初始化，回退到 fmt.Print
		fmt.Print(args...)
	}
}

// Println 兼容 fmt.Println，使用 info 级别记录
func Println(args ...interface{}) {
	if InfoLogger != nil {
		InfoLogger.Info(args...)
	} else {
		// 如果日志未初始化，回退到 fmt.Println
		fmt.Println(args...)
	}
}

// Notice 记录通知日志（兼容原有代码）
func Notice(message string) {
	Info("Notice: ", message)
}

// WriteNoticeLog 写入通知日志（兼容原有代码）
func WriteNoticeLog(log string) {
	Info("Notice: ", log)
}

// WriteErrorLog 写入错误日志（兼容原有代码）
func WriteErrorLog(log string) {
	Error("Error: ", log)
}

// Log 通用日志函数（兼容原有代码）
func Log(v ...interface{}) {
	Info(v...)
}

// LogErr 错误日志函数（兼容原有代码）
func LogErr(v ...interface{}) {
	Error(v...)
}

// LogDebug 调试日志函数（兼容原有代码）
func LogDebug(v ...interface{}) {
	Debug(v...)
}

// CheckError 检查错误（兼容原有代码）
func CheckError(err error) {
	if err != nil {
		Error("Fatal error: ", err.Error())
	}
}
