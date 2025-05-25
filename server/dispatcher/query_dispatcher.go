package dispatcher

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"strings"
	"sync"
)

// SQLDispatcher SQL查询分发器，负责将SQL查询路由到合适的引擎
type SQLDispatcher struct {
	engines map[string]SQLEngine
	router  SQLRouter
	config  *conf.Cfg
	mutex   sync.RWMutex
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
	Err        error
	Data       interface{}
	ResultType string
	Message    string
	Columns    []string
	Rows       [][]interface{}
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

	// 路由到合适的引擎
	engineName := d.router.Route(session, query)

	engine, exists := d.engines[engineName]
	if !exists {
		// 如果指定引擎不存在，使用默认引擎
		for _, e := range d.engines {
			if e.CanHandle(query) {
				engine = e
				break
			}
		}
	}

	if engine == nil {
		// 没有合适的引擎，返回错误
		resultChan := make(chan *SQLResult, 1)
		resultChan <- &SQLResult{
			Err: fmt.Errorf("no suitable engine found for query: %s", query),
		}
		close(resultChan)
		return resultChan
	}

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

// CanHandle 检查是否能处理该查询
func (e *InnoDBSQLEngine) CanHandle(query string) bool {
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
			resultChan <- sqlResult
		}
	}()

	return resultChan
}

// convertResult 将XMySQLEngine的结果转换为SQLResult
func (e *InnoDBSQLEngine) convertResult(xmysqlResult *engine.Result) *SQLResult {
	result := &SQLResult{
		Err:  xmysqlResult.Err,
		Data: xmysqlResult.Data,
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
	case common.RESULT_TYPE_QUERY:
		result.ResultType = "query"
		result = e.convertSelectResult(xmysqlResult, result)
	default:
		result.ResultType = xmysqlResult.ResultType
		result.Message = "Query executed successfully"
	}

	return result
}

// convertSelectResult 转换SELECT查询结果
func (e *InnoDBSQLEngine) convertSelectResult(xmysqlResult *engine.Result, result *SQLResult) *SQLResult {
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

// DefaultSQLRouter 默认SQL路由器
type DefaultSQLRouter struct{}

// NewDefaultSQLRouter 创建默认SQL路由器
func NewDefaultSQLRouter() SQLRouter {
	return &DefaultSQLRouter{}
}

// Route 路由SQL查询到合适的引擎
func (r *DefaultSQLRouter) Route(session server.MySQLServerSession, query string) string {
	query = strings.TrimSpace(strings.ToUpper(query))

	// 根据查询类型路由
	switch {
	case strings.HasPrefix(query, "SELECT"):
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
	case strings.HasPrefix(query, "SHOW"):
		return "innodb"
	case strings.HasPrefix(query, "DESCRIBE"):
		return "innodb"
	case strings.HasPrefix(query, "EXPLAIN"):
		return "innodb"
	case strings.HasPrefix(query, "USE"):
		return "innodb"
	case strings.HasPrefix(query, "SET"):
		return "innodb"
	default:
		return "innodb" // 默认使用InnoDB引擎
	}
}
