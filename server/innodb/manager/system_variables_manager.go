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
	globalSources  map[string]string                 // 全局变量来源
}

// NewSystemVariablesManager 创建系统变量管理器
func NewSystemVariablesManager() *SystemVariablesManager {
	mgr := &SystemVariablesManager{
		globalVars:     make(map[string]interface{}),
		sessionVars:    make(map[string]map[string]interface{}),
		varDefinitions: make(map[string]*SystemVariable),
		globalSources:  make(map[string]string),
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
		{Name: "innodb_lock_wait_timeout", DefaultValue: int64(50), Scope: BothScope, ReadOnly: false, Description: "InnoDB row lock wait timeout in seconds"},
		{Name: "max_allowed_packet", DefaultValue: int64(67108864), Scope: BothScope, ReadOnly: false, Description: "Max allowed packet"},
		{Name: "net_buffer_length", DefaultValue: int64(16384), Scope: BothScope, ReadOnly: false, Description: "Net buffer length"},

		// SQL模式和设置
		{Name: "sql_mode", DefaultValue: "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", Scope: BothScope, ReadOnly: false, Description: "SQL mode"},
		{Name: "sql_quote_show_create", DefaultValue: int64(1), Scope: BothScope, ReadOnly: false, Description: "Quote identifiers in metadata output"},
		{Name: "init_connect", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Init connect"},
		{Name: "tx_isolation", DefaultValue: "REPEATABLE-READ", Scope: BothScope, ReadOnly: false, Description: "Transaction isolation level"},
		{Name: "transaction_isolation", DefaultValue: "REPEATABLE-READ", Scope: BothScope, ReadOnly: false, Description: "Transaction isolation level"},
		{Name: "tx_read_only", DefaultValue: int64(0), Scope: BothScope, ReadOnly: false, Description: "Transaction read only"},
		{Name: "transaction_read_only", DefaultValue: int64(0), Scope: BothScope, ReadOnly: false, Description: "Transaction read only"},
		{Name: "autocommit", DefaultValue: "ON", Scope: BothScope, ReadOnly: false, Description: "Autocommit"},
		{Name: "foreign_key_checks", DefaultValue: int64(1), Scope: BothScope, ReadOnly: false, Description: "Foreign key constraint checks"},
		{Name: "check_constraint_checks", DefaultValue: int64(1), Scope: BothScope, ReadOnly: false, Description: "Check constraint checks"},

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
		{Name: "performance_schema_accounts_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema accounts rows; -1 means autosizing"},
		{Name: "performance_schema_digests_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema digest rows; -1 means autosizing"},
		{Name: "performance_schema_error_size", DefaultValue: int64(5377), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema instrumented server error codes"},
		{Name: "performance_schema_max_digest_length", DefaultValue: int64(1024), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema digest text length in bytes"},
		{Name: "performance_schema_max_cond_classes", DefaultValue: int64(150), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema condition instruments"},
		{Name: "performance_schema_max_cond_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema condition instances; -1 means autosizing"},
		{Name: "performance_schema_max_digest_sample_age", DefaultValue: int64(60), Scope: GlobalScope, ReadOnly: false, Description: "Maximum age of a Performance Schema digest sample in seconds"},
		{Name: "performance_schema_max_file_classes", DefaultValue: int64(80), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema file instruments"},
		{Name: "performance_schema_max_file_handles", DefaultValue: int64(32768), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema opened file objects"},
		{Name: "performance_schema_max_memory_classes", DefaultValue: int64(470), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema memory instruments"},
		{Name: "performance_schema_max_meter_classes", DefaultValue: int64(30), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema meter instruments"},
		{Name: "performance_schema_max_metric_classes", DefaultValue: int64(600), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema metric instruments"},
		{Name: "performance_schema_max_mutex_classes", DefaultValue: int64(350), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema mutex instruments"},
		{Name: "performance_schema_max_mutex_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema mutex instances; -1 means autosizing"},
		{Name: "performance_schema_max_rwlock_classes", DefaultValue: int64(100), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema rwlock instruments"},
		{Name: "performance_schema_max_rwlock_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema rwlock instances; -1 means autosizing"},
		{Name: "performance_schema_max_socket_classes", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema socket instruments"},
		{Name: "performance_schema_max_stage_classes", DefaultValue: int64(175), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema stage instruments"},
		{Name: "performance_schema_max_statement_classes", DefaultValue: int64(220), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema statement instruments"},
		{Name: "performance_schema_max_statement_stack", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema nested statement depth"},
		{Name: "performance_schema_max_table_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema table instances; -1 means autosizing"},
		{Name: "performance_schema_max_thread_classes", DefaultValue: int64(100), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema thread instruments"},
		{Name: "performance_schema_max_file_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema file instances; -1 means autosizing"},
		{Name: "performance_schema_hosts_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema hosts rows; -1 means autosizing"},
		{Name: "performance_schema_max_index_stat", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema index statistics rows; -1 means autosizing"},
		{Name: "performance_schema_max_metadata_locks", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema metadata lock instruments; -1 means autosizing"},
		{Name: "performance_schema_max_prepared_statements_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema prepared statement instances; -1 means autosizing"},
		{Name: "performance_schema_max_program_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema program instances; -1 means autosizing"},
		{Name: "performance_schema_max_sql_text_length", DefaultValue: int64(1024), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema SQL text length in bytes"},
		{Name: "performance_schema_max_table_handles", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema table-handle instruments; -1 means autosizing"},
		{Name: "performance_schema_max_table_lock_stat", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema table lock statistics; -1 means autosizing"},
		{Name: "performance_schema_max_thread_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema thread instances; -1 means autosizing"},
		{Name: "performance_schema_max_socket_instances", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema socket instances; -1 means autosizing"},
		{Name: "performance_schema_session_connect_attrs_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema connection attribute bytes; -1 means autosizing"},
		{Name: "performance_schema_show_processlist", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Use the Performance Schema implementation for SHOW PROCESSLIST"},
		{Name: "performance_schema_setup_actors_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema setup actors rows; -1 means autosizing"},
		{Name: "performance_schema_setup_objects_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema setup objects rows; -1 means autosizing"},
		{Name: "performance_schema_users_size", DefaultValue: int64(-1), Scope: GlobalScope, ReadOnly: true, Description: "Maximum Performance Schema users rows; -1 means autosizing"},
		{Name: "performance_schema_events_statements_history_size", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: false, Description: "Per-thread statement history size"},
		{Name: "performance_schema_events_statements_history_long_size", DefaultValue: int64(10000), Scope: GlobalScope, ReadOnly: false, Description: "Global statement history size"},
		{Name: "performance_schema_events_stages_history_size", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: false, Description: "Per-thread stage history size"},
		{Name: "performance_schema_events_stages_history_long_size", DefaultValue: int64(10000), Scope: GlobalScope, ReadOnly: false, Description: "Global stage history size"},
		{Name: "performance_schema_events_waits_history_size", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: false, Description: "Per-thread wait history size"},
		{Name: "performance_schema_events_waits_history_long_size", DefaultValue: int64(10000), Scope: GlobalScope, ReadOnly: false, Description: "Global wait history size"},
		{Name: "performance_schema_events_transactions_history_size", DefaultValue: int64(10), Scope: GlobalScope, ReadOnly: false, Description: "Per-thread transaction history size"},
		{Name: "performance_schema_events_transactions_history_long_size", DefaultValue: int64(10000), Scope: GlobalScope, ReadOnly: false, Description: "Global transaction history size"},

		// InnoDB相关
		{Name: "innodb_version", DefaultValue: "8.0.32", Scope: GlobalScope, ReadOnly: true, Description: "InnoDB version"},
		{Name: "innodb_buffer_pool_size", DefaultValue: int64(134217728), Scope: GlobalScope, ReadOnly: true, Description: "InnoDB buffer pool size"},
		{Name: "innodb_page_size", DefaultValue: int64(16384), Scope: GlobalScope, ReadOnly: true, Description: "InnoDB page size"},
		{Name: "innodb_log_file_size", DefaultValue: int64(50331648), Scope: GlobalScope, ReadOnly: true, Description: "InnoDB log file size"},
		{Name: "innodb_file_per_table", DefaultValue: "ON", Scope: GlobalScope, ReadOnly: false, Description: "InnoDB file per table"},
		{Name: "innodb_flush_log_at_trx_commit", DefaultValue: int64(1), Scope: GlobalScope, ReadOnly: false, Description: "InnoDB flush log at transaction commit"},
		{Name: "innodb_cmp_per_index_enabled", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Collect per-index InnoDB compression statistics"},
		{Name: "innodb_monitor_enable", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Enable InnoDB metrics counters"},
		{Name: "innodb_monitor_disable", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Disable InnoDB metrics counters"},
		{Name: "innodb_monitor_reset", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Reset InnoDB metrics counters"},
		{Name: "innodb_monitor_reset_all", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Reset all InnoDB metrics counters"},

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
		{Name: "super_read_only", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Super read only mode"},
		{Name: "activate_all_roles_on_login", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Activate all granted roles when users log in"},
		{Name: "mandatory_roles", DefaultValue: "", Scope: GlobalScope, ReadOnly: false, Description: "Roles granted to every account"},
		{Name: "partial_revokes", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Enable schema-level restrictions on global privileges"},
		{Name: "password_history", DefaultValue: int64(0), Scope: GlobalScope, ReadOnly: false, Description: "Minimum password changes before password reuse"},
		{Name: "password_reuse_interval", DefaultValue: int64(0), Scope: GlobalScope, ReadOnly: false, Description: "Days before a password may be reused"},
		{Name: "password_require_current", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Require the current password for password changes"},
		{Name: "default_password_lifetime", DefaultValue: int64(0), Scope: GlobalScope, ReadOnly: false, Description: "Default password lifetime in days"},
		{Name: "log_bin", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: true, Description: "Binary logging enabled"},
		{Name: "gtid_mode", DefaultValue: "ON", Scope: GlobalScope, ReadOnly: true, Description: "Global transaction identifier mode"},
		{Name: "binlog_format", DefaultValue: "ROW", Scope: GlobalScope, ReadOnly: false, Description: "Binary log row format"},
		{Name: "binlog_checksum", DefaultValue: "CRC32", Scope: GlobalScope, ReadOnly: false, Description: "Binary log checksum algorithm"},
		{Name: "server_uuid", DefaultValue: "00000000-0000-0000-0000-000000000000", Scope: GlobalScope, ReadOnly: true, Description: "Replication server UUID"},
		{Name: "server_id", DefaultValue: int64(1), Scope: GlobalScope, ReadOnly: false, Description: "Server ID"},
		{Name: "log_error", DefaultValue: "/var/log/mysql/error.log", Scope: GlobalScope, ReadOnly: false, Description: "Error log file"},
		{Name: "general_log", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "General log enabled"},
		{Name: "slow_query_log", DefaultValue: "OFF", Scope: GlobalScope, ReadOnly: false, Description: "Slow query log enabled"},
	}

	// 注册变量定义并设置默认值
	for _, variable := range variables {
		mgr.varDefinitions[variable.Name] = variable
		mgr.globalVars[variable.Name] = variable.DefaultValue
		mgr.globalSources[variable.Name] = "COMPILED"
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
		mgr.globalSources[varName] = "GLOBAL"
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

// SetStartupVariable applies a startup-only global variable before the server
// accepts client statements. MySQL exposes these values as read-only at
// runtime, but configuration loading must still be able to initialize them.
func (mgr *SystemVariablesManager) SetStartupVariable(varName string, value interface{}) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	varDef, exists := mgr.varDefinitions[varName]
	if !exists {
		return fmt.Errorf("unknown system variable '%s'", varName)
	}
	if varDef.Scope == SessionScope {
		return fmt.Errorf("variable '%s' is not a global variable", varName)
	}
	mgr.globalVars[varName] = value
	mgr.globalSources[varName] = "COMPILED"
	return nil
}

// GetGlobalVariableSource returns the source of the current global value in
// the vocabulary exposed by PERFORMANCE_SCHEMA.variables_info. Persisted
// values are overlaid by the executor because that metadata also carries the
// durable path and SET user/host fields.
func (mgr *SystemVariablesManager) GetGlobalVariableSource(varName string) string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	if source, ok := mgr.globalSources[varName]; ok && source != "" {
		return source
	}
	return "COMPILED"
}

// ListVariables 列出所有变量
func (mgr *SystemVariablesManager) ListVariables(sessionID string, scope SystemVariableScope) map[string]interface{} {
	mgr.mu.RLock()
	names := make([]string, 0, len(mgr.varDefinitions))
	for varName := range mgr.varDefinitions {
		names = append(names, varName)
	}
	mgr.mu.RUnlock()

	result := make(map[string]interface{}, len(names))
	for _, varName := range names {
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
