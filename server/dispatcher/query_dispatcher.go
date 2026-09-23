package dispatcher

import (
	"fmt"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// SQLDispatcher SQL查询分发器，负责将SQL查询路由到合适的引擎
type SQLDispatcher struct {
	engines map[string]SQLEngine
	router  SQLRouter
	config  *conf.Cfg
	mutex   sync.RWMutex
}

// ResetSession delegates COM_RESET_CONNECTION to the registered SQL engine
// without making the network layer depend on a concrete engine instance.
func (d *SQLDispatcher) ResetSession(session server.MySQLServerSession) error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	for _, registered := range d.engines {
		if resetter, ok := registered.(interface {
			ResetSession(server.MySQLServerSession) error
		}); ok {
			return resetter.ResetSession(session)
		}
	}
	return nil
}

// CleanupTemporaryTables delegates connection-close cleanup to the registered
// SQL engine without coupling the network layer to a concrete engine.
func (d *SQLDispatcher) CleanupTemporaryTables(session server.MySQLServerSession) error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	for _, registered := range d.engines {
		if cleaner, ok := registered.(interface {
			CleanupTemporaryTables(server.MySQLServerSession) error
		}); ok {
			return cleaner.CleanupTemporaryTables(session)
		}
	}
	return nil
}

// SQLEngine SQL引擎接口
type SQLEngine interface {
	ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) <-chan *SQLResult
	Name() string
	CanHandle(query string) bool
}

// SQLRouter SQL路由器接口
type SQLRouter interface {
	Route(session server.MySQLServerSession, query string) string
}

// SQLResult SQL执行结果
type SQLResult struct {
	Err          error
	Data         interface{}
	ResultType   string
	Message      string
	Columns      []string
	ColumnTypes  []string
	Rows         [][]interface{}
	AffectedRows uint64
	LastInsertID uint64
	WarningCount uint16
}

// NewSQLDispatcher 创建SQL分发器
func NewSQLDispatcher(config *conf.Cfg) *SQLDispatcher {
	dispatcher := &SQLDispatcher{
		engines: make(map[string]SQLEngine),
		config:  config,
	}

	// 注册默认的InnoDB引擎
	dispatcher.RegisterEngine(NewInnoDBSQLEngine(config))

	// 设置默认路由器
	dispatcher.router = NewDefaultSQLRouter()

	return dispatcher
}

// NewSQLDispatcherWithStorageManager 创建带存储管理器的SQL分发器
func NewSQLDispatcherWithStorageManager(config *conf.Cfg, storageManager interface{}) *SQLDispatcher {
	return newSQLDispatcher(config, NewInnoDBSQLEngine(config), storageManager)
}

// NewSQLDispatcherWithXMySQLEngine creates a dispatcher that reuses the
// server-owned engine. The network server already starts one engine before it
// builds the protocol handler; creating a second engine here leaves duplicate
// tablespace file handles open on Windows and makes DROP DATABASE fail.
func NewSQLDispatcherWithXMySQLEngine(config *conf.Cfg, xmysqlEngine *engine.XMySQLEngine, storageManager interface{}) *SQLDispatcher {
	if xmysqlEngine == nil {
		return NewSQLDispatcherWithStorageManager(config, storageManager)
	}
	return newSQLDispatcher(config, &InnoDBSQLEngine{
		name:         "innodb",
		config:       config,
		xmysqlEngine: xmysqlEngine,
	}, storageManager)
}

func newSQLDispatcher(config *conf.Cfg, innodbEngine SQLEngine, storageManager interface{}) *SQLDispatcher {
	dispatcher := &SQLDispatcher{
		engines: make(map[string]SQLEngine),
		config:  config,
	}

	logger.Debugf(" [NewSQLDispatcherWithStorageManager] 开始创建SQL分发器")
	logger.Debugf(" [NewSQLDispatcherWithStorageManager] storageManager类型: %T", storageManager)
	logger.Debugf(" [NewSQLDispatcherWithStorageManager] storageManager是否为nil: %v", storageManager == nil)

	// 注册默认的InnoDB引擎
	dispatcher.RegisterEngine(innodbEngine)
	logger.Debugf(" [NewSQLDispatcherWithStorageManager] 注册InnoDB引擎: %s", innodbEngine.Name())

	// 如果提供了StorageManager，注册系统变量引擎
	if sm, ok := storageManager.(*manager.StorageManager); ok && sm != nil {
		logger.Debugf(" [NewSQLDispatcherWithStorageManager] 存储管理器类型检查通过")
		sysVarEngine := NewSystemVariableEngine(sm)
		dispatcher.RegisterEngine(sysVarEngine)
		logger.Debugf(" [NewSQLDispatcherWithStorageManager] 注册系统变量查询引擎: %s", sysVarEngine.Name())
	} else {
		logger.Errorf(" [NewSQLDispatcherWithStorageManager] 存储管理器类型检查失败")
		logger.Errorf(" [NewSQLDispatcherWithStorageManager] 类型转换结果: ok=%v, sm=%v", ok, sm != nil)
	}

	// 设置默认路由器
	dispatcher.router = NewDefaultSQLRouter()
	logger.Debugf(" [NewSQLDispatcherWithStorageManager] 设置默认路由器")

	// 列出所有注册的引擎
	logger.Debugf(" [NewSQLDispatcherWithStorageManager] 最终注册的引擎列表:")
	for name := range dispatcher.engines {
		logger.Debugf("   - %s", name)
	}

	return dispatcher
}

// RegisterEngine 注册SQL引擎
func (d *SQLDispatcher) RegisterEngine(engine SQLEngine) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.engines[engine.Name()] = engine
}

// UnregisterEngine 注销SQL引擎
func (d *SQLDispatcher) UnregisterEngine(name string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.engines, name)
}

// SetRouter 设置SQL路由器
func (d *SQLDispatcher) SetRouter(router SQLRouter) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.router = router
}

// Dispatch 分发SQL查询
func (d *SQLDispatcher) Dispatch(session server.MySQLServerSession, query string, databaseName string) <-chan *SQLResult {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	logger.Debugf(" [SQLDispatcher.Dispatch] 开始分发查询: %s", query)

	// 路由到合适的引擎
	engineName := d.router.Route(session, query)
	logger.Debugf(" [SQLDispatcher.Dispatch] 路由器推荐引擎: %s", engineName)

	// 列出所有可用的引擎
	logger.Debugf(" [SQLDispatcher.Dispatch] 可用引擎列表:")
	for name := range d.engines {
		logger.Debugf("   - %s", name)
	}

	//  详细检查推荐的引擎
	logger.Debugf(" [SQLDispatcher.Dispatch] 检查推荐引擎 '%s' 是否存在", engineName)
	engine, exists := d.engines[engineName]
	logger.Debugf(" [SQLDispatcher.Dispatch] 引擎存在检查结果: exists=%v, engine=%v", exists, engine != nil)

	if !exists {
		logger.Warnf(" [SQLDispatcher.Dispatch] 推荐引擎 %s 不存在，尝试查找可处理的引擎", engineName)
		// 如果指定引擎不存在，使用默认引擎
		for name, e := range d.engines {
			logger.Debugf(" [SQLDispatcher.Dispatch] 检查引擎 %s 是否能处理查询", name)
			if e.CanHandle(query) {
				engine = e
				logger.Debugf(" [SQLDispatcher.Dispatch] 找到可处理的引擎: %s", name)
				break
			}
		}
	} else {
		logger.Debugf(" [SQLDispatcher.Dispatch] 找到推荐引擎: %s", engineName)
	}

	if engine == nil {
		logger.Errorf(" [SQLDispatcher.Dispatch] 没有找到合适的引擎处理查询: %s", query)
		// 没有合适的引擎，返回错误
		resultChan := make(chan *SQLResult, 1)
		resultChan <- &SQLResult{
			Err: fmt.Errorf("no suitable engine found for query: %s", query),
		}
		close(resultChan)
		return resultChan
	}

	logger.Debugf("🚀 [SQLDispatcher.Dispatch] 使用引擎 %s 执行查询", engine.Name())

	// 执行查询
	return engine.ExecuteQuery(session, query, databaseName)
}

// GetEngines 获取所有注册的引擎
func (d *SQLDispatcher) GetEngines() map[string]SQLEngine {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	engines := make(map[string]SQLEngine)
	for name, engine := range d.engines {
		engines[name] = engine
	}
	return engines
}

// InnoDBSQLEngine InnoDB SQL引擎实现
type InnoDBSQLEngine struct {
	name         string
	config       *conf.Cfg
	xmysqlEngine *engine.XMySQLEngine
}

// NewInnoDBSQLEngine 创建InnoDB SQL引擎
func NewInnoDBSQLEngine(config *conf.Cfg) SQLEngine {
	return &InnoDBSQLEngine{
		name:         "innodb",
		config:       config,
		xmysqlEngine: engine.NewXMySQLEngine(config),
	}
}

// Name 返回引擎名称
func (e *InnoDBSQLEngine) Name() string {
	return e.name
}

func (e *InnoDBSQLEngine) ResetSession(session server.MySQLServerSession) error {
	if e == nil || e.xmysqlEngine == nil {
		return nil
	}
	return e.xmysqlEngine.ResetSession(session)
}

// CanHandle 检查是否能处理该查询
func (e *InnoDBSQLEngine) CanHandle(query string) bool {
	if engine.IsTransactionCommand(query) {
		return true
	}

	query = strings.TrimSpace(strings.ToUpper(query))

	supportedQueries := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE",
		"CREATE", "DROP", "ALTER", "SHOW",
		"DESCRIBE", "EXPLAIN", "USE", "SET",
	}

	for _, prefix := range supportedQueries {
		if strings.HasPrefix(query, prefix) {
			return true
		}
	}

	return false
}

// ExecuteQuery 执行SQL查询
func (e *InnoDBSQLEngine) ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) <-chan *SQLResult {
	resultChan := make(chan *SQLResult, 1)

	go func() {
		defer close(resultChan)

		// 调用XMySQLEngine执行查询
		xmysqlResultChan := e.xmysqlEngine.ExecuteQuery(session, query, databaseName)

		// 转换结果格式
		for xmysqlResult := range xmysqlResultChan {
			sqlResult := e.convertResult(xmysqlResult)
			persistSessionExecutionState(session, xmysqlResult)
			resultChan <- sqlResult
		}
	}()

	return resultChan
}

func persistSessionLastInsertID(session server.MySQLServerSession, result *engine.Result) {
	if session == nil || result == nil {
		return
	}
	dmlResult, ok := result.Data.(*engine.DMLResult)
	if !ok || dmlResult.LastInsertId == 0 {
		return
	}
	session.SetParamByName("last_insert_id", dmlResult.LastInsertId)
}

func persistSessionExecutionState(session server.MySQLServerSession, result *engine.Result) {
	if session == nil || result == nil || result.Err != nil {
		return
	}
	if dmlResult, ok := result.Data.(*engine.DMLResult); ok {
		session.SetParamByName("row_count", int64(dmlResult.AffectedRows))
		persistSessionLastInsertID(session, result)
		return
	}
	switch result.ResultType {
	case common.RESULT_TYPE_SELECT:
		session.SetParamByName("row_count", int64(-1))
	case common.RESULT_TYPE_DDL, common.RESULT_TYPE_SET, common.RESULT_TYPE_QUERY, "QUERY":
		session.SetParamByName("row_count", int64(0))
	}
}

// convertResult 将XMySQLEngine的结果转换为SQLResult
func (e *InnoDBSQLEngine) convertResult(xmysqlResult *engine.Result) *SQLResult {
	result := &SQLResult{
		Err:          xmysqlResult.Err,
		Data:         xmysqlResult.Data,
		WarningCount: uint16(len(xmysqlResult.Warnings)),
	}

	// 转换结果类型
	switch xmysqlResult.ResultType {
	case common.RESULT_TYPE_SELECT:
		result.ResultType = "select"
		result = e.convertSelectResult(xmysqlResult, result)
	case common.RESULT_TYPE_DDL:
		result.ResultType = "ddl"
		result.Message = "DDL executed successfully"
	case common.RESULT_TYPE_SET:
		result.ResultType = "set"
		result.Message = "Variable set successfully"
	case common.RESULT_TYPE_ERROR:
		result.ResultType = "error"
	case common.RESULT_TYPE_QUERY, "QUERY":
		result.ResultType = "query"
		//  对于QUERY类型，检查是否有Message，如果Message包含"Database changed"，说明是USE语句
		if xmysqlResult.Message != "" && strings.Contains(xmysqlResult.Message, "Database changed") {
			// USE语句，不需要结果集，只需要消息
			result.Message = xmysqlResult.Message
			result.Columns = []string{}     // 确保没有列
			result.Rows = [][]interface{}{} // 确保没有行
		} else {
			if dmlResult, ok := xmysqlResult.Data.(*engine.DMLResult); ok {
				result.Message = dmlResult.Message
				result.AffectedRows = uint64(dmlResult.AffectedRows)
				result.LastInsertID = dmlResult.LastInsertId
				result.WarningCount = uint16(len(dmlResult.Warnings))
				result.Columns = []string{}
				result.Rows = [][]interface{}{}
			} else if xmysqlResult.Data == nil && xmysqlResult.Message != "" {
				result.Message = xmysqlResult.Message
				result.Columns = []string{}
				result.Rows = [][]interface{}{}
			} else {
				// 其他QUERY类型，正常转换
				result = e.convertSelectResult(xmysqlResult, result)
			}
		}
	default:
		result.ResultType = xmysqlResult.ResultType
		result.Message = "Query executed successfully"
	}

	return result
}

// convertSelectResult 转换SELECT查询结果
func (e *InnoDBSQLEngine) convertSelectResult(xmysqlResult *engine.Result, result *SQLResult) *SQLResult {
	if selectResult, ok := xmysqlResult.Data.(*engine.SelectResult); ok {
		result.Columns = selectResult.Columns
		result.ColumnTypes = selectResult.ColumnTypes
		result.Rows = make([][]interface{}, 0, len(selectResult.Records))
		for _, record := range selectResult.Records {
			if record == nil {
				continue
			}
			values := record.GetValues()
			row := make([]interface{}, 0, len(values))
			for _, value := range values {
				if value == nil {
					row = append(row, nil)
				} else {
					row = append(row, basicValueToInterface(value))
				}
			}
			result.Rows = append(result.Rows, normalizeRowValuesByColumnTypes(row, result.ColumnTypes))
		}
		result.Message = selectResult.Message
		return result
	}

	// 这里需要根据XMySQLEngine的实际数据结构来转换
	if data, ok := xmysqlResult.Data.(map[string]interface{}); ok {
		if columns, exists := data["columns"]; exists {
			if colSlice, ok := columns.([]string); ok {
				result.Columns = colSlice
			}
		}

		if rows, exists := data["rows"]; exists {
			if rowSlice, ok := rows.([][]interface{}); ok {
				result.Rows = rowSlice
			}
		}
	}

	// 如果没有具体的数据结构，提供默认的示例数据
	if len(result.Columns) == 0 {
		result.Columns = []string{"id", "name", "value"}
		result.Rows = [][]interface{}{
			{1, "innodb_test1", "innodb_value1"},
			{2, "innodb_test2", "innodb_value2"},
		}
	}

	result.Message = "Query executed successfully"
	return result
}

func basicValueToInterface(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}

	switch value.Type() {
	case basic.ValueTypeTinyInt:
		return int8(value.Int())
	case basic.ValueTypeSmallInt:
		return int16(value.Int())
	case basic.ValueTypeMediumInt, basic.ValueTypeInt:
		return int32(value.Int())
	case basic.ValueTypeBigInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return value.Float64()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		return value.Bool()
	default:
		return value.ToString()
	}
}

func normalizeRowValuesByColumnTypes(row []interface{}, columnTypes []string) []interface{} {
	if len(columnTypes) == 0 {
		return row
	}
	normalized := make([]interface{}, len(row))
	copy(normalized, row)
	for i := range normalized {
		if i >= len(columnTypes) {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(columnTypes[i])) {
		case "bool", "boolean", "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
			switch v := normalized[i].(type) {
			case string:
				switch strings.ToLower(strings.TrimSpace(v)) {
				case "true", "1", "yes", "on":
					normalized[i] = int64(1)
				case "false", "0", "no", "off":
					normalized[i] = int64(0)
				}
			case int64:
				if strings.EqualFold(strings.TrimSpace(columnTypes[i]), "bool") || strings.EqualFold(strings.TrimSpace(columnTypes[i]), "boolean") {
					normalized[i] = v != 0
				}
			case int:
				if strings.EqualFold(strings.TrimSpace(columnTypes[i]), "bool") || strings.EqualFold(strings.TrimSpace(columnTypes[i]), "boolean") {
					normalized[i] = v != 0
				}
			}
		}
	}
	return normalized
}

// DefaultSQLRouter 默认SQL路由器
type DefaultSQLRouter struct{}

// NewDefaultSQLRouter 创建默认SQL路由器
func NewDefaultSQLRouter() SQLRouter {
	return &DefaultSQLRouter{}
}

// Route 路由SQL查询到合适的引擎
func (r *DefaultSQLRouter) Route(session server.MySQLServerSession, query string) string {
	if engine.IsTransactionCommand(query) {
		logger.Debugf(" [DefaultSQLRouter] 识别为事务控制语句，路由到 innodb 引擎")
		return "innodb"
	}

	if isInformationSchemaMetadataQuery(query) {
		logger.Debugf(" [DefaultSQLRouter] information_schema metadata query routes to innodb")
		return "innodb"
	}

	query = strings.TrimSpace(strings.ToUpper(query))

	logger.Debugf(" [DefaultSQLRouter] 路由查询: %s", query)

	//  优先检查系统变量查询（最高优先级）
	if r.isSystemVariableQuery(query) {
		logger.Debugf(" [DefaultSQLRouter] 识别为系统变量查询，路由到 system_variable 引擎")
		return "system_variable"
	}

	// 根据查询类型路由
	switch {
	case strings.HasPrefix(query, "SELECT"):
		// SELECT查询可能包含系统变量，再次检查
		if r.containsSystemVariableExpression(query) {
			logger.Debugf(" [DefaultSQLRouter] SELECT查询包含系统变量，路由到 system_variable 引擎")
			return "system_variable"
		}
		return "innodb"
	case strings.HasPrefix(query, "SHOW"):
		// SHOW查询的系统变量相关检查
		if r.isShowSystemVariable(query) {
			logger.Debugf(" [DefaultSQLRouter] SHOW系统变量查询，路由到 system_variable 引擎")
			return "system_variable"
		}
		return "innodb"
	case strings.HasPrefix(query, "INSERT"):
		return "innodb"
	case strings.HasPrefix(query, "UPDATE"):
		return "innodb"
	case strings.HasPrefix(query, "DELETE"):
		return "innodb"
	case strings.HasPrefix(query, "CREATE"):
		return "innodb"
	case strings.HasPrefix(query, "DROP"):
		return "innodb"
	case strings.HasPrefix(query, "ALTER"):
		return "innodb"
	case strings.HasPrefix(query, "DESCRIBE"):
		return "innodb"
	case strings.HasPrefix(query, "EXPLAIN"):
		return "innodb"
	case strings.HasPrefix(query, "USE"):
		return "innodb"
	case strings.HasPrefix(query, "SET"):
		// SET语句可能设置系统变量
		if r.isSetSystemVariable(query) {
			logger.Debugf(" [DefaultSQLRouter] SET系统变量查询，路由到 system_variable 引擎")
			return "system_variable"
		}
		return "innodb"
	default:
		logger.Debugf("🔄 [DefaultSQLRouter] 未识别的查询类型，使用默认innodb引擎")
		return "innodb" // 默认使用InnoDB引擎
	}
}

// isSystemVariableQuery 检查是否为系统变量查询
func (r *DefaultSQLRouter) isSystemVariableQuery(query string) bool {
	if isInformationSchemaMetadataQuery(query) {
		return false
	}

	// 检查是否包含@@
	if strings.Contains(query, "@@") {
		return true
	}

	// 检查常见的系统函数
	systemFunctions := []string{
		"USER()", "DATABASE()", "VERSION()", "CONNECTION_ID()",
		"CURRENT_USER()", "SESSION_USER()", "SYSTEM_USER()", "CURRENT_ROLE()", "LAST_INSERT_ID(", "ROW_COUNT(",
	}

	for _, function := range systemFunctions {
		if strings.Contains(query, function) {
			return true
		}
	}

	// 检查INFORMATION_SCHEMA查询
	if strings.Contains(query, "INFORMATION_SCHEMA") {
		return true
	}

	// 检查特定的系统变量查询模式
	systemVariablePatterns := []string{
		"SELECT @@",
		"SHOW VARIABLES",
		"SHOW SESSION VARIABLES",
		"SHOW GLOBAL VARIABLES",
		"SHOW STATUS",
		"SHOW SESSION STATUS",
		"SHOW GLOBAL STATUS",
	}

	for _, pattern := range systemVariablePatterns {
		if strings.Contains(query, pattern) {
			return true
		}
	}

	return false
}

// containsSystemVariableExpression 检查SELECT查询是否包含系统变量表达式
func (r *DefaultSQLRouter) containsSystemVariableExpression(query string) bool {
	// 检查@@变量引用
	if strings.Contains(query, "@@") {
		return true
	}

	// 检查系统函数调用
	systemFunctions := []string{
		"USER()", "DATABASE()", "VERSION()", "CONNECTION_ID()",
		"CURRENT_USER()", "SESSION_USER()", "SYSTEM_USER()", "CURRENT_ROLE()", "LAST_INSERT_ID(", "ROW_COUNT(",
	}

	for _, function := range systemFunctions {
		if strings.Contains(query, function) {
			return true
		}
	}

	return false
}

// isShowSystemVariable 检查是否为SHOW系统变量查询
func (r *DefaultSQLRouter) isShowSystemVariable(query string) bool {
	showSystemPatterns := []string{
		"SHOW VARIABLES",
		"SHOW SESSION VARIABLES",
		"SHOW GLOBAL VARIABLES",
		"SHOW STATUS",
		"SHOW SESSION STATUS",
		"SHOW GLOBAL STATUS",
		"SHOW ENGINES",
		"SHOW CHARSET",
		"SHOW CHARACTER SET",
		"SHOW COLLATION",
	}

	for _, pattern := range showSystemPatterns {
		if strings.Contains(query, pattern) {
			return true
		}
	}

	return false
}

// isSetSystemVariable 检查是否为SET系统变量查询
func (r *DefaultSQLRouter) isSetSystemVariable(query string) bool {
	// SET @@variable = value
	if strings.Contains(query, "SET @@") {
		return true
	}

	// SET SESSION variable = value
	if strings.Contains(query, "SET SESSION") {
		return true
	}

	// SET GLOBAL variable = value
	if strings.Contains(query, "SET GLOBAL") {
		return true
	}

	// 检查常见的系统变量设置
	commonSystemVars := []string{
		"AUTOCOMMIT", "SQL_MODE", "TIME_ZONE", "CHARACTER_SET",
		"COLLATION", "FOREIGN_KEY_CHECKS", "CHECK_CONSTRAINT_CHECKS", "UNIQUE_CHECKS",
	}

	for _, sysVar := range commonSystemVars {
		if strings.Contains(query, "SET "+sysVar) {
			return true
		}
	}

	return false
}
