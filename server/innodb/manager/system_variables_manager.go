package manager

import (
	"fmt"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

// SystemVariableScope 系统变量作用域
type SystemVariableScope string

const (
	SessionScope SystemVariableScope = "session"
	GlobalScope  SystemVariableScope = "global"
	BothScope    SystemVariableScope = "both" // 即可以是 session 也可以是 global
)

// SystemVariable 系统变量定义
type SystemVariable struct {
	Name         string              // 变量名
	DefaultValue interface{}         // 默认值
	Scope        SystemVariableScope // 作用域
	ReadOnly     bool                // 是否只读
	Description  string              // 描述
}

// SystemVariablesManager 系统变量管理器
type SystemVariablesManager struct {
	mu             sync.RWMutex
	globalVars     map[string]interface{}            // 全局变量值
	sessionVars    map[string]map[string]interface{} // 会话变量值，key是sessionID
	varDefinitions map[string]*SystemVariable        // 变量定义
}

// NewSystemVariablesManager 创建系统变量管理器
func NewSystemVariablesManager() *SystemVariablesManager {
	mgr := &SystemVariablesManager{
		globalVars:     make(map[string]interface{}),
		sessionVars:    make(map[string]map[string]interface{}),
		varDefinitions: make(map[string]*SystemVariable),
	}

	// 初始化默认系统变量
	mgr.initializeDefaultVariables()

	return mgr
}

// initializeDefaultVariables 初始化默认系统变量
func (mgr *SystemVariablesManager) initializeDefaultVariables() {
	// 定义系统变量及其默认值
	variables := []*SystemVariable{
		// 字符集和校对相关
		{Name: "character_set_client", DefaultValue: "utf8mb4", Scope: BothScope, ReadOnly: false, Description: "Client character set"},
		{Name: "character_set_connection", DefaultValue: "utf8mb4", Scope: BothScope, ReadOnly: false, Description: "Connection character set"},
		{Name: "character_set_database", DefaultValue: "utf8mb4", Scope: BothScope, ReadOnly: false, Description: "Database character set"},
		{Name: "character_set_results", DefaultValue: "utf8mb4", Scope: BothScope, ReadOnly: false, Description: "Results character set"},
		{Name: "character_set_server", DefaultValue: "utf8mb4", Scope: BothScope, ReadOnly: false, Description: "Server character set"},
		{Name: "character_set_system", DefaultValue: "utf8", Scope: GlobalScope, ReadOnly: true, Description: "System character set"},
		{Name: "collation_connection", DefaultValue: "utf8mb4_general_ci", Scope: BothScope, ReadOnly: false, Description: "Connection collation"},
		{Name: "collation_database", DefaultValue: "utf8mb4_general_ci", Scope: BothScope, ReadOnly: false, Description: "Database collation"},
		{Name: "collation_server", DefaultValue: "utf8mb4_general_ci", Scope: BothScope, ReadOnly: false, Description: "Server collation"},

		// 自增相关
		{Name: "auto_increment_increment", DefaultValue: int64(1), Scope: BothScope, ReadOnly: false, Description: "Auto increment increment"},
		{Name: "auto_increment_offset", DefaultValue: int64(1), Scope: BothScope, ReadOnly: false, Description: "Auto increment offset"},

		// 网络和超时相关
		{Name: "connect_timeout", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: false, Description: "Connect timeout"},
		{Name: "interactive_timeout", DefaultValue: int64(28800), Scope: BothScope, ReadOnly: false, Description: "Interactive timeout"},
		{Name: "net_read_timeout", DefaultValue: int64(30), Scope: BothScope, ReadOnly: false, Description: "Net read timeout"},
		{Name: "net_write_timeout", DefaultValue: int64(60), Scope: BothScope, ReadOnly: false, Description: "Net write timeout"},
		{Name: "wait_timeout", DefaultValue: int64(28800), Scope: BothScope, ReadOnly: false, Description: "Wait timeout"},
		{Name: "max_allowed_packet", DefaultValue: int64(67108864), Scope: BothScope, ReadOnly: false, Description: "Max allowed packet"},
		{Name: "net_buffer_length", DefaultValue: int64(16384), Scope: BothScope, ReadOnly: false, Description: "Net buffer length"},

		// SQL模式和设置
		{Name: "sql_mode", DefaultValue: "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", Scope: BothScope, ReadOnly: false, Description: "SQL mode"},
		{Name: "init_connect", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Init connect"},
		{Name: "tx_isolation", DefaultValue: "REPEATABLE-READ", Scope: BothScope, ReadOnly: false, Description: "Transaction isolation level"},
		{Name: "transaction_isolation", DefaultValue: "REPEATABLE-READ", Scope: BothScope, ReadOnly: false, Description: "Transaction isolation level"},
		{Name: "tx_read_only", DefaultValue: int64(0), Scope: BothScope, ReadOnly: false, Description: "Transaction read only"},
		{Name: "transaction_read_only", DefaultValue: int64(0), Scope: BothScope, ReadOnly: false, Description: "Transaction read only"},
		{Name: "autocommit", DefaultValue: "ON", Scope: BothScope, ReadOnly: false, Description: "Autocommit"},

		// 时区相关
		{Name: "time_zone", DefaultValue: "SYSTEM", Scope: BothScope, ReadOnly: false, Description: "Time zone"},
		{Name: "system_time_zone", DefaultValue: "CST", Scope: GlobalScope, ReadOnly: true, Description: "System time zone"},

		// 查询缓存
		{Name: "query_cache_type", DefaultValue: "OFF", Scope: BothScope, ReadOnly: false, Description: "Query cache type"},
		{Name: "query_cache_size", DefaultValue: int64(0), Scope: GlobalScope, ReadOnly: false, Description: "Query cache size"},

		// 服务器版本和信息
		{Name: "version", DefaultValue: "8.0.32", Scope: GlobalScope, ReadOnly: true, Description: "Server version"},
		{Name: "version_comment", DefaultValue: "XMySQL Server", Scope: GlobalScope, ReadOnly: true, Description: "Version comment"},
		{Name: "version_compile_machine", DefaultValue: "x86_64", Scope: GlobalScope, ReadOnly: true, Description: "Version compile machine"},
		{Name: "version_compile_os", DefaultValue: "Win32", Scope: GlobalScope, ReadOnly: true, Description: "Version compile OS"},
		{Name: "license", DefaultValue: "GPL", Scope: GlobalScope, ReadOnly: true, Description: "Server license"},

		// 表名大小写
		{Name: "lower_case_table_names", DefaultValue: int64(0), Scope: GlobalScope, ReadOnly: true, Description: "Lower case table names"},

		// Performance Schema
		{Name: "performance_schema", DefaultValue: "ON", Scope: GlobalScope, ReadOnly: true, Description: "Performance schema enabled"},

		// InnoDB相关
		{Name: "innodb_version", DefaultValue: "8.0.32", Scope: GlobalScope, ReadOnly: true, Description: "InnoDB version"},
		{Name: "innodb_buffer_pool_size", DefaultValue: int64(134217728), Scope: GlobalScope, ReadOnly: true, Description: "InnoDB buffer pool size"},
		{Name: "innodb_page_size", DefaultValue: int64(16384), Scope: GlobalScope, ReadOnly: true, Description: "InnoDB page size"},
		{Name: "innodb_log_file_size", DefaultValue: int64(50331648), Scope: GlobalScope, ReadOnly: true, Description: "InnoDB log file size"},
		{Name: "innodb_file_per_table", DefaultValue: "ON", Scope: GlobalScope, ReadOnly: false, Description: "InnoDB file per table"},
		{Name: "innodb_flush_log_at_trx_commit", DefaultValue: int64(1), Scope: GlobalScope, ReadOnly: false, Description: "InnoDB flush log at transaction commit"},

		// 服务器状态
		{Name: "hostname", DefaultValue: "localhost", Scope: GlobalScope, ReadOnly: true, Description: "Server hostname"},
		{Name: "port", DefaultValue: int64(3309), Scope: GlobalScope, ReadOnly: true, Description: "Server port"},
		{Name: "socket", DefaultValue: "/tmp/mysql.sock", Scope: GlobalScope, ReadOnly: true, Description: "Server socket"},
		{Name: "datadir", DefaultValue: "data/", Scope: GlobalScope, ReadOnly: true, Description: "Data directory"},
		{Name: "basedir", DefaultValue: "/usr/local/mysql/", Scope: GlobalScope, ReadOnly: true, Description: "Base directory"},

		// 线程相关
		{Name: "thread_stack", DefaultValue: int64(1048576), Scope: GlobalScope, ReadOnly: true, Description: "Thread stack size"},
		{Name: "thread_cache_size", DefaultValue: int64(8), Scope: GlobalScope, ReadOnly: false, Description: "Thread cache size"},
		{Name: "max_connections", DefaultValue: int64(151), Scope: GlobalScope, ReadOnly: false, Description: "Maximum connections"},

		// 其他
		{Name: "read_only", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Read only mode"},
		{Name: "log_bin", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: true, Description: "Binary logging enabled"},
		{Name: "server_id", DefaultValue: int64(1), Scope: GlobalScope, ReadOnly: false, Description: "Server ID"},
		{Name: "log_error", DefaultValue: "/var/log/mysql/error.log", Scope: GlobalScope, ReadOnly: false, Description: "Error log file"},
		{Name: "general_log", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "General log enabled"},
		{Name: "slow_query_log", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Slow query log enabled"},
	}

	// 注册变量定义并设置默认值
	for _, variable := range variables {
		mgr.varDefinitions[variable.Name] = variable
		mgr.globalVars[variable.Name] = variable.DefaultValue
	}

	logger.Debugf(" 初始化了 %d 个系统变量", len(variables))
}

// GetVariable 获取系统变量值
func (mgr *SystemVariablesManager) GetVariable(sessionID, varName string, scope SystemVariableScope) (interface{}, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	// 检查变量是否存在
	varDef, exists := mgr.varDefinitions[varName]
	if !exists {
		return nil, fmt.Errorf("unknown system variable '%s'", varName)
	}

	// 根据作用域返回值
	switch scope {
	case GlobalScope:
		// 对于全局作用域，检查变量是否支持全局
		if varDef.Scope == SessionScope {
			return nil, fmt.Errorf("variable '%s' is not a global variable", varName)
		}
		if value, exists := mgr.globalVars[varName]; exists {
			return value, nil
		}
		return varDef.DefaultValue, nil

	case SessionScope:
		// 对于会话作用域的处理
		if varDef.Scope == BothScope {
			// BothScope变量：先检查会话值，再检查全局值
			if sessionVars, exists := mgr.sessionVars[sessionID]; exists {
				if value, exists := sessionVars[varName]; exists {
					return value, nil
				}
			}
			// 会话值不存在，返回全局值
			if value, exists := mgr.globalVars[varName]; exists {
				return value, nil
			}
			return varDef.DefaultValue, nil
		} else if varDef.Scope == GlobalScope {
			// GlobalScope变量：直接返回全局值（MySQL兼容性）
			if value, exists := mgr.globalVars[varName]; exists {
				return value, nil
			}
			return varDef.DefaultValue, nil
		} else {
			// SessionScope变量：只检查会话值
			if sessionVars, exists := mgr.sessionVars[sessionID]; exists {
				if value, exists := sessionVars[varName]; exists {
					return value, nil
				}
			}
			return varDef.DefaultValue, nil
		}

	default:
		// 自动选择作用域
		if varDef.Scope == GlobalScope {
			return mgr.GetVariable(sessionID, varName, GlobalScope)
		} else {
			return mgr.GetVariable(sessionID, varName, SessionScope)
		}
	}
}

// SetVariable 设置系统变量值
func (mgr *SystemVariablesManager) SetVariable(sessionID, varName string, value interface{}, scope SystemVariableScope) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// 检查变量是否存在
	varDef, exists := mgr.varDefinitions[varName]
	if !exists {
		return fmt.Errorf("unknown system variable '%s'", varName)
	}

	// 检查是否只读
	if varDef.ReadOnly {
		return fmt.Errorf("variable '%s' is read-only", varName)
	}

	// 根据作用域设置值
	switch scope {
	case GlobalScope:
		if varDef.Scope == SessionScope {
			return fmt.Errorf("variable '%s' is not a global variable", varName)
		}
		mgr.globalVars[varName] = value
		logger.Debugf(" 设置全局变量 %s = %v", varName, value)

	case SessionScope:
		if varDef.Scope == GlobalScope {
			return fmt.Errorf("variable '%s' is not a session variable", varName)
		}

		// 确保会话变量映射存在
		if _, exists := mgr.sessionVars[sessionID]; !exists {
			mgr.sessionVars[sessionID] = make(map[string]interface{})
		}
		mgr.sessionVars[sessionID][varName] = value
		logger.Debugf(" 设置会话变量 %s (session: %s) = %v", varName, sessionID, value)

	default:
		return fmt.Errorf("invalid scope '%s'", scope)
	}

	return nil
}

// ListVariables 列出所有变量
func (mgr *SystemVariablesManager) ListVariables(sessionID string, scope SystemVariableScope) map[string]interface{} {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	result := make(map[string]interface{})

	for varName := range mgr.varDefinitions {
		if value, err := mgr.GetVariable(sessionID, varName, scope); err == nil {
			result[varName] = value
		}
	}

	return result
}

// GetVariableDefinition 获取变量定义
func (mgr *SystemVariablesManager) GetVariableDefinition(varName string) (*SystemVariable, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if varDef, exists := mgr.varDefinitions[varName]; exists {
		return varDef, nil
	}

	return nil, fmt.Errorf("unknown system variable '%s'", varName)
}

// CreateSession 为新会话创建变量空间
func (mgr *SystemVariablesManager) CreateSession(sessionID string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if _, exists := mgr.sessionVars[sessionID]; !exists {
		mgr.sessionVars[sessionID] = make(map[string]interface{})
		logger.Debugf(" 为会话 %s 创建系统变量空间", sessionID)
	}
}

// DestroySession 销毁会话变量
func (mgr *SystemVariablesManager) DestroySession(sessionID string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	delete(mgr.sessionVars, sessionID)
	logger.Debugf("🗑️  销毁会话 %s 的系统变量空间", sessionID)
}

// UpdateServerInfo 更新服务器信息（在服务器启动时调用）
func (mgr *SystemVariablesManager) UpdateServerInfo(hostname string, port int64, datadir, basedir string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.globalVars["hostname"] = hostname
	mgr.globalVars["port"] = port
	mgr.globalVars["datadir"] = datadir
	mgr.globalVars["basedir"] = basedir

	logger.Debugf("🖥️  更新服务器信息: hostname=%s, port=%d", hostname, port)
}

// ParseScope 解析变量作用域字符串
func ParseScope(scopeStr string) SystemVariableScope {
	switch scopeStr {
	case "global":
		return GlobalScope
	case "session":
		return SessionScope
	default:
		return BothScope // 默认为两者都支持
	}
}
