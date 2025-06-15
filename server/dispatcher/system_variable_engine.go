package dispatcher

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// SystemVariableEngine 系统变量查询引擎 - 使用火山模型执行
type SystemVariableEngine struct {
	name           string
	storageManager *manager.StorageManager
	sysVarManager  *manager.SystemVariablesManager
	sysVarAnalyzer *manager.SystemVariableAnalyzer
}

// NewSystemVariableEngine 创建系统变量查询引擎
func NewSystemVariableEngine(storageManager *manager.StorageManager) SQLEngine {
	return &SystemVariableEngine{
		name:           "system_variable",
		storageManager: storageManager,
		sysVarManager:  storageManager.GetSystemVariablesManager(),
		sysVarAnalyzer: storageManager.GetSystemVariableAnalyzer(),
	}
}

// Name 返回引擎名称
func (e *SystemVariableEngine) Name() string {
	return e.name
}

// CanHandle 检查是否能处理该查询
func (e *SystemVariableEngine) CanHandle(query string) bool {
	logger.Debugf(" [SystemVariableEngine.CanHandle] 开始检查查询: %s", query)
	logger.Debugf(" [SystemVariableEngine.CanHandle] sysVarAnalyzer是否为nil: %v", e.sysVarAnalyzer == nil)
	logger.Debugf(" [SystemVariableEngine.CanHandle] sysVarManager是否为nil: %v", e.sysVarManager == nil)
	logger.Debugf(" [SystemVariableEngine.CanHandle] storageManager是否为nil: %v", e.storageManager == nil)

	if e.sysVarAnalyzer == nil {
		logger.Errorf(" [SystemVariableEngine.CanHandle] sysVarAnalyzer为nil，无法处理查询")
		return false
	}

	logger.Debugf(" [SystemVariableEngine.CanHandle] 检查查询: %s", query)

	// 1. 使用系统变量分析器进行初步判断
	if e.sysVarAnalyzer.IsSystemVariableQuery(query) {
		logger.Debugf(" [SystemVariableEngine.CanHandle] 系统变量分析器确认可处理")
		return true
	}

	// 2. 使用sqlparser进行更精确的分析
	canHandle := e.canHandleWithSQLParser(query)
	logger.Debugf(" [SystemVariableEngine.CanHandle] sqlparser分析结果: %v", canHandle)

	return canHandle
}

// canHandleWithSQLParser 使用sqlparser进行精确的SQL分析
func (e *SystemVariableEngine) canHandleWithSQLParser(query string) bool {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		logger.Debugf(" [SystemVariableEngine.canHandleWithSQLParser] SQL解析失败: %v", err)
		return false
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return e.canHandleSelectStatement(stmt)
	case *sqlparser.Show:
		return e.canHandleShowStatement(stmt)
	case *sqlparser.Set:
		return e.canHandleSetStatement(stmt)
	default:
		logger.Debugf("🔄 [SystemVariableEngine.canHandleWithSQLParser] 不支持的语句类型: %T", stmt)
		return false
	}
}

// canHandleSelectStatement 检查SELECT语句是否包含系统变量
func (e *SystemVariableEngine) canHandleSelectStatement(stmt *sqlparser.Select) bool {
	logger.Debugf(" [SystemVariableEngine.canHandleSelectStatement] 分析SELECT语句")

	// 检查SELECT表达式中是否包含系统变量
	for _, expr := range stmt.SelectExprs {
		if e.containsSystemVariableExpression(expr) {
			logger.Debugf(" [SystemVariableEngine.canHandleSelectStatement] 找到系统变量表达式")
			return true
		}
	}

	// 检查FROM子句是否查询系统表
	if stmt.From != nil {
		for _, tableExpr := range stmt.From {
			if e.isSystemTable(tableExpr) {
				logger.Debugf(" [SystemVariableEngine.canHandleSelectStatement] 查询系统表")
				return true
			}
		}
	}

	logger.Debugf(" [SystemVariableEngine.canHandleSelectStatement] 未找到系统变量或系统表")
	return false
}

// canHandleShowStatement 检查SHOW语句是否为系统变量相关
func (e *SystemVariableEngine) canHandleShowStatement(stmt *sqlparser.Show) bool {
	logger.Debugf(" [SystemVariableEngine.canHandleShowStatement] 分析SHOW语句: %s", stmt.Type)

	systemShowTypes := map[string]bool{
		"variables":         true,
		"session variables": true,
		"global variables":  true,
		"status":            true,
		"session status":    true,
		"global status":     true,
		"engines":           true,
		"charset":           true,
		"collation":         true,
		"character set":     true,
	}

	showType := strings.ToLower(stmt.Type)
	if systemShowTypes[showType] {
		logger.Debugf(" [SystemVariableEngine.canHandleShowStatement] 支持的SHOW类型: %s", showType)
		return true
	}

	logger.Debugf(" [SystemVariableEngine.canHandleShowStatement] 不支持的SHOW类型: %s", showType)
	return false
}

// canHandleSetStatement 检查SET语句是否为系统变量设置
func (e *SystemVariableEngine) canHandleSetStatement(stmt *sqlparser.Set) bool {
	logger.Debugf(" [SystemVariableEngine.canHandleSetStatement] 分析SET语句")

	// 检查SET表达式中是否包含系统变量
	for _, expr := range stmt.Exprs {
		if e.isSystemVariableSetExpression(expr) {
			logger.Debugf(" [SystemVariableEngine.canHandleSetStatement] 找到系统变量设置")
			return true
		}
	}

	logger.Debugf(" [SystemVariableEngine.canHandleSetStatement] 未找到系统变量设置")
	return false
}

// containsSystemVariableExpression 检查表达式是否包含系统变量
func (e *SystemVariableEngine) containsSystemVariableExpression(expr sqlparser.SelectExpr) bool {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		return e.exprContainsSystemVariable(expr.Expr)
	case *sqlparser.StarExpr:
		// SELECT * 不算系统变量查询
		return false
	default:
		return false
	}
}

// exprContainsSystemVariable 检查SQL表达式是否包含系统变量
func (e *SystemVariableEngine) exprContainsSystemVariable(expr sqlparser.Expr) bool {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		// @@variable_name 格式
		colName := expr.Name.String()
		return strings.HasPrefix(colName, "@@")

	case *sqlparser.FuncExpr:
		// 系统函数如 USER(), VERSION()
		funcName := strings.ToUpper(expr.Name.String())
		systemFunctions := map[string]bool{
			"USER":          true,
			"DATABASE":      true,
			"VERSION":       true,
			"CONNECTION_ID": true,
			"CURRENT_USER":  true,
			"SESSION_USER":  true,
			"SYSTEM_USER":   true,
		}
		return systemFunctions[funcName]

	case *sqlparser.BinaryExpr:
		// 递归检查二元表达式的左右操作数
		return e.exprContainsSystemVariable(expr.Left) || e.exprContainsSystemVariable(expr.Right)

	case *sqlparser.ParenExpr:
		// 检查括号内的表达式
		return e.exprContainsSystemVariable(expr.Expr)

	default:
		// 对于其他类型的表达式，转换为字符串检查
		exprStr := strings.ToUpper(sqlparser.String(expr))
		return strings.Contains(exprStr, "@@") ||
			strings.Contains(exprStr, "USER()") ||
			strings.Contains(exprStr, "DATABASE()") ||
			strings.Contains(exprStr, "VERSION()") ||
			strings.Contains(exprStr, "CONNECTION_ID()") ||
			strings.Contains(exprStr, "CURRENT_USER()") ||
			strings.Contains(exprStr, "SESSION_USER()") ||
			strings.Contains(exprStr, "SYSTEM_USER()")
	}
}

// isSystemTable 检查表表达式是否为系统表
func (e *SystemVariableEngine) isSystemTable(tableExpr sqlparser.TableExpr) bool {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := tableExpr.Expr.(sqlparser.TableName); ok {
			tableNameStr := strings.ToUpper(tableName.Name.String())
			qualifierStr := strings.ToUpper(tableName.Qualifier.String())

			// 检查INFORMATION_SCHEMA表
			if qualifierStr == "INFORMATION_SCHEMA" {
				logger.Debugf(" [SystemVariableEngine.isSystemTable] INFORMATION_SCHEMA表: %s.%s", qualifierStr, tableNameStr)
				return true
			}

			// 检查mysql系统库表
			if qualifierStr == "MYSQL" {
				systemTables := map[string]bool{
					"USER": true, "DB": true, "TABLES_PRIV": true, "COLUMNS_PRIV": true,
					"PROCS_PRIV": true, "PROXIES_PRIV": true, "ROLE_EDGES": true,
					"DEFAULT_ROLES": true, "GLOBAL_GRANTS": true,
				}
				if systemTables[tableNameStr] {
					logger.Debugf(" [SystemVariableEngine.isSystemTable] MySQL系统表: %s.%s", qualifierStr, tableNameStr)
					return true
				}
			}
		}
	}

	return false
}

// isSystemVariableSetExpression 检查SET表达式是否为系统变量设置
func (e *SystemVariableEngine) isSystemVariableSetExpression(expr *sqlparser.SetExpr) bool {
	// 检查变量名是否以@@开头或为已知的系统变量
	varName := strings.ToUpper(expr.Name.String())

	// @@variable格式
	if strings.HasPrefix(varName, "@@") {
		return true
	}

	// 检查常见的系统变量名
	systemVariables := map[string]bool{
		"AUTOCOMMIT":               true,
		"SQL_MODE":                 true,
		"TIME_ZONE":                true,
		"CHARACTER_SET_CLIENT":     true,
		"CHARACTER_SET_CONNECTION": true,
		"CHARACTER_SET_RESULTS":    true,
		"COLLATION_CONNECTION":     true,
		"FOREIGN_KEY_CHECKS":       true,
		"UNIQUE_CHECKS":            true,
		"SQL_SAFE_UPDATES":         true,
	}

	return systemVariables[varName]
}

// ExecuteQuery 执行系统变量查询 - 使用火山模型
func (e *SystemVariableEngine) ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) <-chan *SQLResult {
	resultChan := make(chan *SQLResult, 1)

	go func() {
		defer close(resultChan)

		logger.Debugf("🚀 [SystemVariableEngine.ExecuteQuery] 开始执行查询: %s", query)

		sessionID := e.getSessionID(session)

		// 1. 首先尝试解析为系统函数查询
		if result := e.executeSystemFunctionQuery(session, query, databaseName); result != nil {
			resultChan <- result
			return
		}

		// 2. 尝试解析为SHOW语句
		if result := e.executeShowStatement(session, query, databaseName); result != nil {
			resultChan <- result
			return
		}

		// 3. 尝试解析为SET语句
		if result := e.executeSetStatement(session, query, databaseName); result != nil {
			resultChan <- result
			return
		}

		// 4. 尝试解析为系统变量查询
		varQuery, err := e.sysVarAnalyzer.AnalyzeSystemVariableQuery(query)
		if err != nil {
			logger.Debugf(" [SystemVariableEngine.ExecuteQuery] 分析查询失败: %v", err)
			resultChan <- &SQLResult{
				ResultType: "error",
				Err:        fmt.Errorf("failed to analyze query: %v", err),
				Message:    "Query analysis failed",
			}
			return
		}

		if !varQuery.IsValid || len(varQuery.Variables) == 0 {
			logger.Debugf(" [SystemVariableEngine.ExecuteQuery] 无效的系统变量查询")
			resultChan <- &SQLResult{
				ResultType: "error",
				Err:        fmt.Errorf("invalid system variable query"),
				Message:    "No system variables found in query",
			}
			return
		}

		// 5. 解析SQL语句以获取结构信息
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			logger.Debugf(" [SystemVariableEngine.ExecuteQuery] SQL解析失败: %v", err)
			resultChan <- &SQLResult{
				ResultType: "error",
				Err:        fmt.Errorf("SQL parse failed: %v", err),
				Message:    "SQL parsing failed",
			}
			return
		}

		// 6. 构建火山模型执行器
		executor := e.buildSystemVariableExecutor(sessionID, varQuery, stmt)
		if executor == nil {
			logger.Debugf(" [SystemVariableEngine.ExecuteQuery] 构建执行器失败")
			resultChan <- &SQLResult{
				ResultType: "error",
				Err:        fmt.Errorf("failed to build executor"),
				Message:    "Executor build failed",
			}
			return
		}

		// 7. 使用火山模型执行查询
		result, err := e.executeWithVolcanoModel(executor)
		if err != nil {
			logger.Debugf(" [SystemVariableEngine.ExecuteQuery] 火山模型执行失败: %v", err)
			resultChan <- &SQLResult{
				ResultType: "error",
				Err:        err,
				Message:    "Volcano model execution failed",
			}
			return
		}

		logger.Debugf(" [SystemVariableEngine.ExecuteQuery] 查询执行成功，返回 %d 行数据", len(result.Rows))
		resultChan <- result
	}()

	return resultChan
}

// executeSystemFunctionQuery 执行系统函数查询
func (e *SystemVariableEngine) executeSystemFunctionQuery(session server.MySQLServerSession, query string, databaseName string) *SQLResult {
	logger.Debugf(" [executeSystemFunctionQuery] 检查系统函数查询: %s", query)

	// 解析SQL语句
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil
	}

	// 检查是否包含系统函数
	systemFunctions := []SystemFunctionInfo{}
	for _, selectExpr := range selectStmt.SelectExprs {
		if aliasedExpr, ok := selectExpr.(*sqlparser.AliasedExpr); ok {
			if funcInfo := e.parseSystemFunction(aliasedExpr); funcInfo != nil {
				systemFunctions = append(systemFunctions, *funcInfo)
			}
		}
	}

	if len(systemFunctions) == 0 {
		return nil
	}

	logger.Debugf(" [executeSystemFunctionQuery] 找到 %d 个系统函数", len(systemFunctions))

	// 构建列信息
	columns := make([]string, len(systemFunctions))
	for i, funcInfo := range systemFunctions {
		if funcInfo.Alias != "" {
			columns[i] = funcInfo.Alias
		} else {
			columns[i] = funcInfo.FunctionName + "()"
		}
	}

	// 构建行数据
	row := make([]interface{}, len(systemFunctions))
	sessionID := e.getSessionID(session)

	for i, funcInfo := range systemFunctions {
		value := e.evaluateSystemFunction(funcInfo.FunctionName, session, sessionID)
		row[i] = value
	}

	return &SQLResult{
		ResultType: "select",
		Message:    "System function query executed successfully",
		Columns:    columns,
		Rows:       [][]interface{}{row},
	}
}

// SystemFunctionInfo 系统函数信息
type SystemFunctionInfo struct {
	FunctionName string
	Alias        string
	Arguments    []string
}

// parseSystemFunction 解析系统函数
func (e *SystemVariableEngine) parseSystemFunction(expr *sqlparser.AliasedExpr) *SystemFunctionInfo {
	if funcExpr, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
		funcName := strings.ToUpper(funcExpr.Name.String())

		systemFunctions := map[string]bool{
			"USER":          true,
			"DATABASE":      true,
			"VERSION":       true,
			"CONNECTION_ID": true,
			"CURRENT_USER":  true,
			"SESSION_USER":  true,
			"SYSTEM_USER":   true,
		}

		if systemFunctions[funcName] {
			alias := ""
			if !expr.As.IsEmpty() {
				alias = expr.As.String()
			}

			return &SystemFunctionInfo{
				FunctionName: funcName,
				Alias:        alias,
				Arguments:    []string{}, // 暂时不处理参数
			}
		}
	}

	return nil
}

// evaluateSystemFunction 计算系统函数值
func (e *SystemVariableEngine) evaluateSystemFunction(funcName string, session server.MySQLServerSession, sessionID string) interface{} {
	switch strings.ToUpper(funcName) {
	case "USER", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER":
		if userParam := session.GetParamByName("user"); userParam != nil {
			if user, ok := userParam.(string); ok {
				return user + "@localhost"
			}
		}
		return "root@localhost"

	case "DATABASE":
		if dbParam := session.GetParamByName("database"); dbParam != nil {
			if db, ok := dbParam.(string); ok {
				return db
			}
		}
		return "mysql"

	case "VERSION":
		if version, err := e.sysVarManager.GetVariable(sessionID, "version", manager.GlobalScope); err == nil {
			return version
		}
		return "8.0.32-xmysql"

	case "CONNECTION_ID":
		return sessionID

	default:
		return nil
	}
}

// executeShowStatement 执行SHOW语句
func (e *SystemVariableEngine) executeShowStatement(session server.MySQLServerSession, query string, databaseName string) *SQLResult {
	logger.Debugf(" [executeShowStatement] 检查SHOW语句: %s", query)

	// 解析SQL语句
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil
	}

	showStmt, ok := stmt.(*sqlparser.Show)
	if !ok {
		return nil
	}

	showType := strings.ToLower(showStmt.Type)
	logger.Debugf(" [executeShowStatement] SHOW类型: %s", showType)

	sessionID := e.getSessionID(session)

	switch showType {
	case "variables", "session variables":
		return e.executeShowVariables(sessionID, showStmt, manager.SessionScope)
	case "global variables":
		return e.executeShowVariables(sessionID, showStmt, manager.GlobalScope)
	case "status", "session status":
		return e.executeShowStatus(sessionID, showStmt, manager.SessionScope)
	case "global status":
		return e.executeShowStatus(sessionID, showStmt, manager.GlobalScope)
	case "engines":
		return e.executeShowEngines()
	case "charset", "character set":
		return e.executeShowCharset()
	case "collation":
		return e.executeShowCollation()
	default:
		logger.Debugf(" [executeShowStatement] 不支持的SHOW类型: %s", showType)
		return nil
	}
}

// executeShowVariables 执行SHOW VARIABLES
func (e *SystemVariableEngine) executeShowVariables(sessionID string, showStmt *sqlparser.Show, scope manager.SystemVariableScope) *SQLResult {
	logger.Debugf(" [executeShowVariables] 执行SHOW VARIABLES，作用域: %s", scope)

	variables := e.sysVarManager.ListVariables(sessionID, scope)

	columns := []string{"Variable_name", "Value"}
	rows := make([][]interface{}, 0, len(variables))

	// 处理LIKE过滤 - Show结构可能有ShowTablesOpt字段
	pattern := ""
	if showStmt.ShowTablesOpt != nil && showStmt.ShowTablesOpt.Filter != nil && showStmt.ShowTablesOpt.Filter.Like != "" {
		pattern = strings.ToLower(strings.Trim(showStmt.ShowTablesOpt.Filter.Like, "'\""))
		pattern = strings.ReplaceAll(pattern, "%", "*")
	}

	for varName, value := range variables {
		// 应用LIKE过滤
		if pattern != "" {
			matched, _ := filepath.Match(pattern, strings.ToLower(varName))
			if !matched {
				continue
			}
		}

		rows = append(rows, []interface{}{varName, fmt.Sprintf("%v", value)})
	}

	logger.Debugf(" [executeShowVariables] 返回 %d 个变量", len(rows))

	return &SQLResult{
		ResultType: "select",
		Message:    "SHOW VARIABLES executed successfully",
		Columns:    columns,
		Rows:       rows,
	}
}

// executeShowStatus 执行SHOW STATUS
func (e *SystemVariableEngine) executeShowStatus(sessionID string, showStmt *sqlparser.Show, scope manager.SystemVariableScope) *SQLResult {
	logger.Debugf(" [executeShowStatus] 执行SHOW STATUS，作用域: %s", scope)

	// 模拟一些状态变量
	statusVars := map[string]interface{}{
		"Connections":            1000,
		"Uptime":                 86400,
		"Threads_connected":      5,
		"Threads_running":        2,
		"Questions":              50000,
		"Slow_queries":           10,
		"Opens":                  200,
		"Flush_commands":         5,
		"Open_tables":            100,
		"Queries_per_second_avg": 0.58,
	}

	columns := []string{"Variable_name", "Value"}
	rows := make([][]interface{}, 0, len(statusVars))

	// 处理LIKE过滤
	pattern := ""
	if showStmt.ShowTablesOpt != nil && showStmt.ShowTablesOpt.Filter != nil && showStmt.ShowTablesOpt.Filter.Like != "" {
		pattern = strings.ToLower(strings.Trim(showStmt.ShowTablesOpt.Filter.Like, "'\""))
		pattern = strings.ReplaceAll(pattern, "%", "*")
	}

	for varName, value := range statusVars {
		// 应用LIKE过滤
		if pattern != "" {
			matched, _ := filepath.Match(pattern, strings.ToLower(varName))
			if !matched {
				continue
			}
		}

		rows = append(rows, []interface{}{varName, fmt.Sprintf("%v", value)})
	}

	return &SQLResult{
		ResultType: "select",
		Message:    "SHOW STATUS executed successfully",
		Columns:    columns,
		Rows:       rows,
	}
}

// executeShowEngines 执行SHOW ENGINES
func (e *SystemVariableEngine) executeShowEngines() *SQLResult {
	logger.Debugf(" [executeShowEngines] 执行SHOW ENGINES")

	columns := []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	rows := [][]interface{}{
		{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
		{"MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO"},
		{"MEMORY", "YES", "Hash based, stored in memory, useful for temporary tables", "NO", "NO", "NO"},
		{"CSV", "YES", "CSV storage engine", "NO", "NO", "NO"},
		{"ARCHIVE", "YES", "Archive storage engine", "NO", "NO", "NO"},
		{"PERFORMANCE_SCHEMA", "YES", "Performance Schema", "NO", "NO", "NO"},
		{"FEDERATED", "NO", "Federated MySQL storage engine", "NULL", "NULL", "NULL"},
	}

	return &SQLResult{
		ResultType: "select",
		Message:    "SHOW ENGINES executed successfully",
		Columns:    columns,
		Rows:       rows,
	}
}

// executeShowCharset 执行SHOW CHARSET
func (e *SystemVariableEngine) executeShowCharset() *SQLResult {
	logger.Debugf(" [executeShowCharset] 执行SHOW CHARSET")

	columns := []string{"Charset", "Description", "Default collation", "Maxlen"}
	rows := [][]interface{}{
		{"utf8", "UTF-8 Unicode", "utf8_general_ci", 3},
		{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", 4},
		{"latin1", "cp1252 West European", "latin1_swedish_ci", 1},
		{"ascii", "US ASCII", "ascii_general_ci", 1},
		{"binary", "Binary pseudo charset", "binary", 1},
	}

	return &SQLResult{
		ResultType: "select",
		Message:    "SHOW CHARSET executed successfully",
		Columns:    columns,
		Rows:       rows,
	}
}

// executeShowCollation 执行SHOW COLLATION
func (e *SystemVariableEngine) executeShowCollation() *SQLResult {
	logger.Debugf(" [executeShowCollation] 执行SHOW COLLATION")

	columns := []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}
	rows := [][]interface{}{
		{"utf8mb4_0900_ai_ci", "utf8mb4", 255, "Yes", "Yes", 0, "NO PAD"},
		{"utf8mb4_general_ci", "utf8mb4", 45, "", "Yes", 1, "PAD SPACE"},
		{"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1, "PAD SPACE"},
		{"latin1_swedish_ci", "latin1", 8, "Yes", "Yes", 1, "PAD SPACE"},
		{"ascii_general_ci", "ascii", 11, "Yes", "Yes", 1, "PAD SPACE"},
		{"binary", "binary", 63, "Yes", "Yes", 1, "NO PAD"},
	}

	return &SQLResult{
		ResultType: "select",
		Message:    "SHOW COLLATION executed successfully",
		Columns:    columns,
		Rows:       rows,
	}
}

// executeSetStatement 执行SET语句
func (e *SystemVariableEngine) executeSetStatement(session server.MySQLServerSession, query string, databaseName string) *SQLResult {
	logger.Debugf(" [executeSetStatement] 检查SET语句: %s", query)

	// 解析SQL语句
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		logger.Debugf(" [executeSetStatement] SQL解析失败: %v", err)
		return nil
	}

	setStmt, ok := stmt.(*sqlparser.Set)
	if !ok {
		logger.Debugf(" [executeSetStatement] 不是SET语句")
		return nil
	}

	logger.Debugf(" [executeSetStatement] 识别为SET语句，处理 %d 个表达式", len(setStmt.Exprs))

	sessionID := e.getSessionID(session)
	affectedRows := 0

	// 处理每个SET表达式
	for _, expr := range setStmt.Exprs {
		if err := e.processSetExpression(sessionID, expr); err != nil {
			logger.Errorf(" [executeSetStatement] 处理SET表达式失败: %v", err)
			return &SQLResult{
				ResultType: "error",
				Err:        err,
				Message:    fmt.Sprintf("SET statement failed: %v", err),
			}
		}
		affectedRows++
	}

	logger.Debugf(" [executeSetStatement] SET语句执行成功，处理了 %d 个变量", affectedRows)

	return &SQLResult{
		ResultType: "set",
		Message:    fmt.Sprintf("SET statement executed successfully, %d variables set", affectedRows),
		Columns:    []string{},
		Rows:       [][]interface{}{},
	}
}

// processSetExpression 处理单个SET表达式
func (e *SystemVariableEngine) processSetExpression(sessionID string, expr *sqlparser.SetExpr) error {
	// 获取变量名
	varName := expr.Name.String()
	logger.Debugf(" [processSetExpression] 处理变量: %s", varName)

	// 解析作用域和变量名
	scope, cleanVarName := e.parseSetVariableName(varName)
	logger.Debugf(" [processSetExpression] 解析结果: scope=%s, varName=%s", scope, cleanVarName)

	// 获取设置的值
	value, err := e.evaluateSetValue(expr.Expr)
	if err != nil {
		return fmt.Errorf("failed to evaluate SET value: %v", err)
	}

	logger.Debugf(" [processSetExpression] 设置值: %v", value)

	// 设置系统变量
	if err := e.sysVarManager.SetVariable(sessionID, cleanVarName, value, scope); err != nil {
		logger.Warnf(" [processSetExpression] 设置系统变量失败，但继续执行: %v", err)
		// 对于未知的系统变量，我们记录警告但不返回错误，保持MySQL兼容性
		return nil
	}

	logger.Debugf(" [processSetExpression] 变量 %s 设置成功", cleanVarName)
	return nil
}

// parseSetVariableName 解析SET变量名，提取作用域和变量名
func (e *SystemVariableEngine) parseSetVariableName(varName string) (manager.SystemVariableScope, string) {
	varName = strings.TrimSpace(varName)

	// 处理 @@global.variable_name
	if strings.HasPrefix(varName, "@@global.") {
		cleanName := strings.TrimPrefix(varName, "@@global.")
		return manager.GlobalScope, cleanName
	}

	// 处理 @@session.variable_name
	if strings.HasPrefix(varName, "@@session.") {
		cleanName := strings.TrimPrefix(varName, "@@session.")
		return manager.SessionScope, cleanName
	}

	// 处理 @@variable_name (默认会话作用域)
	if strings.HasPrefix(varName, "@@") {
		cleanName := strings.TrimPrefix(varName, "@@")
		return manager.SessionScope, cleanName
	}

	// 处理普通变量名 (默认会话作用域)
	return manager.SessionScope, varName
}

// evaluateSetValue 计算SET表达式的值
func (e *SystemVariableEngine) evaluateSetValue(expr sqlparser.Expr) (interface{}, error) {
	switch expr := expr.(type) {
	case *sqlparser.SQLVal:
		// 字符串、数字等字面值
		switch expr.Type {
		case sqlparser.StrVal:
			return string(expr.Val), nil
		case sqlparser.IntVal:
			return string(expr.Val), nil
		case sqlparser.FloatVal:
			return string(expr.Val), nil
		case sqlparser.HexNum:
			return string(expr.Val), nil
		case sqlparser.HexVal:
			return string(expr.Val), nil
		case sqlparser.ValArg:
			return string(expr.Val), nil
		default:
			return string(expr.Val), nil
		}

	case *sqlparser.NullVal:
		// NULL值
		return nil, nil

	case sqlparser.BoolVal:
		// 布尔值 - BoolVal本身就是bool类型
		return bool(expr), nil

	case *sqlparser.ColName:
		// 列名引用（可能是其他变量）
		return expr.Name.String(), nil

	default:
		// 其他表达式，转换为字符串
		return sqlparser.String(expr), nil
	}
}

// buildSystemVariableExecutor 构建系统变量执行器
func (e *SystemVariableEngine) buildSystemVariableExecutor(sessionID string, varQuery *manager.SystemVariableQuery, stmt sqlparser.Statement) engine.Executor {
	// 创建执行上下文
	ctx := &engine.ExecutionContext{
		Context: context.Background(),
	}

	// 创建系统变量扫描执行器
	scanExecutor := NewSystemVariableScanExecutor(ctx, e.sysVarManager, sessionID, varQuery)

	// 如果是SELECT语句，可能需要投影
	if selectStmt, ok := stmt.(*sqlparser.Select); ok {
		// 分析SELECT表达式，构建投影执行器
		projectionExecutor := e.buildProjectionExecutor(ctx, scanExecutor, selectStmt, varQuery)
		return projectionExecutor
	}

	return scanExecutor
}

// buildProjectionExecutor 构建投影执行器
func (e *SystemVariableEngine) buildProjectionExecutor(ctx *engine.ExecutionContext, child engine.Executor, selectStmt *sqlparser.Select, varQuery *manager.SystemVariableQuery) engine.Executor {
	// 获取要投影的列
	columns := make([]string, len(varQuery.Variables))
	for i, varInfo := range varQuery.Variables {
		columns[i] = varInfo.Alias
	}

	// 创建投影执行器
	return NewSystemVariableProjectionExecutor(ctx, child, columns, varQuery)
}

// executeWithVolcanoModel 使用火山模型执行查询
func (e *SystemVariableEngine) executeWithVolcanoModel(executor engine.Executor) (*SQLResult, error) {
	logger.Debugf("🌋 开始火山模型执行")

	// 1. 初始化执行器
	if err := executor.Init(); err != nil {
		return nil, fmt.Errorf("failed to initialize executor: %v", err)
	}
	defer executor.Close()

	// 2. 获取结果集
	var rows [][]interface{}
	var columns []string

	// 获取schema信息作为列名
	schema := executor.Schema()
	if schema != nil {
		// Schema是指向接口的指针，需要解引用
		if tables := (*schema).GetTables(); len(tables) > 0 && len(tables[0].Columns) > 0 {
			columns = make([]string, len(tables[0].Columns))
			for i, col := range tables[0].Columns {
				columns[i] = col.Name
			}
		}
	}

	// 如果从schema获取不到列信息，尝试从执行器获取
	if len(columns) == 0 {
		if scanExecutor, ok := executor.(*SystemVariableScanExecutor); ok {
			columns = make([]string, len(scanExecutor.varQuery.Variables))
			for i, varInfo := range scanExecutor.varQuery.Variables {
				columns[i] = varInfo.Alias
			}
			logger.Debugf(" 从扫描执行器获取列信息: %v", columns)
		} else if projExecutor, ok := executor.(*SystemVariableProjectionExecutor); ok {
			columns = projExecutor.columns
			logger.Debugf(" 从投影执行器获取列信息: %v", columns)
		}
	}

	// 3. 火山模型迭代执行
	for {
		err := executor.Next()
		if err == io.EOF {
			break // 正常结束
		}
		if err != nil {
			return nil, fmt.Errorf("executor next error: %v", err)
		}

		// 获取当前行数据
		row := executor.GetRow()
		if row != nil {
			rows = append(rows, row)
		}
	}

	logger.Debugf("🌋 火山模型执行完成，获得 %d 行数据，列: %v", len(rows), columns)

	// 4. 构建结果
	result := &SQLResult{
		Err:        nil,
		ResultType: "select",
		Message:    fmt.Sprintf("Query OK, %d rows in set", len(rows)),
		Columns:    columns,
		Rows:       rows,
	}

	return result, nil
}

// SystemVariableSchema 系统变量查询的Schema实现
type SystemVariableSchema struct {
	name   string
	tables []*metadata.Table
}

// GetName 实现Schema接口
func (s *SystemVariableSchema) GetName() string {
	return s.name
}

// GetCharset 实现Schema接口
func (s *SystemVariableSchema) GetCharset() string {
	return "utf8mb4"
}

// GetCollation 实现Schema接口
func (s *SystemVariableSchema) GetCollation() string {
	return "utf8mb4_general_ci"
}

// GetTables 实现Schema接口
func (s *SystemVariableSchema) GetTables() []*metadata.Table {
	return s.tables
}

// NewSystemVariableSchema 创建系统变量Schema
func NewSystemVariableSchema(varQuery *manager.SystemVariableQuery) *SystemVariableSchema {
	// 创建表结构
	columns := make([]*metadata.Column, len(varQuery.Variables))
	for i, varInfo := range varQuery.Variables {
		columns[i] = &metadata.Column{
			Name:          varInfo.Alias,
			DataType:      metadata.TypeVarchar,
			CharMaxLength: 255,
			IsNullable:    true,
		}
	}

	table := &metadata.Table{
		Name:    "system_variables",
		Columns: columns,
	}

	return &SystemVariableSchema{
		name:   "system_variables_schema",
		tables: []*metadata.Table{table},
	}
}

// SystemVariableScanExecutor 系统变量扫描执行器
type SystemVariableScanExecutor struct {
	engine.BaseExecutor
	sysVarManager *manager.SystemVariablesManager
	sessionID     string
	varQuery      *manager.SystemVariableQuery
	currentRow    []interface{}
	finished      bool
	schema        *metadata.Schema // 明确定义schema字段
}

// NewSystemVariableScanExecutor 创建系统变量扫描执行器
func NewSystemVariableScanExecutor(ctx *engine.ExecutionContext, sysVarManager *manager.SystemVariablesManager, sessionID string, varQuery *manager.SystemVariableQuery) *SystemVariableScanExecutor {
	// 创建Schema实现的指针
	schemaImpl := NewSystemVariableSchema(varQuery)
	// 将接口实现转换为指向接口的指针
	var schema metadata.Schema = schemaImpl

	return &SystemVariableScanExecutor{
		BaseExecutor:  engine.BaseExecutor{},
		sysVarManager: sysVarManager,
		sessionID:     sessionID,
		varQuery:      varQuery,
		finished:      false,
		schema:        &schema, // 指向接口的指针
	}
}

// Init 初始化系统变量扫描
func (s *SystemVariableScanExecutor) Init() error {
	logger.Debugf(" 初始化系统变量扫描执行器，会话: %s", s.sessionID)
	s.finished = false
	return nil
}

// Next 获取下一行数据（系统变量查询只返回一行）
func (s *SystemVariableScanExecutor) Next() error {
	if s.finished {
		return io.EOF
	}

	// 从缓存获取系统变量值
	row := make([]interface{}, len(s.varQuery.Variables))
	for i, varInfo := range s.varQuery.Variables {
		value, err := s.sysVarManager.GetVariable(s.sessionID, varInfo.Name, varInfo.Scope)
		if err != nil {
			logger.Warnf(" 获取系统变量 %s 失败: %v", varInfo.Name, err)
			value = nil // 使用 nil 表示未知变量
		}
		row[i] = value
		logger.Debugf(" 从缓存获取变量 %s = %v", varInfo.Name, value)
	}

	s.currentRow = row
	s.finished = true
	return nil
}

// GetRow 获取当前行数据
func (s *SystemVariableScanExecutor) GetRow() []interface{} {
	return s.currentRow
}

// Close 关闭扫描器
func (s *SystemVariableScanExecutor) Close() error {
	logger.Debugf("🔚 关闭系统变量扫描执行器")
	return nil
}

// Schema 返回扫描器的schema
func (s *SystemVariableScanExecutor) Schema() *metadata.Schema {
	return s.schema
}

// Children 返回子执行器（扫描器没有子执行器）
func (s *SystemVariableScanExecutor) Children() []engine.Executor {
	return nil
}

// SetChildren 设置子执行器
func (s *SystemVariableScanExecutor) SetChildren(children []engine.Executor) {
	// 扫描器不需要子执行器
}

// SystemVariableProjectionExecutor 系统变量投影执行器
type SystemVariableProjectionExecutor struct {
	engine.BaseExecutor
	child    engine.Executor
	columns  []string
	varQuery *manager.SystemVariableQuery
	schema   *metadata.Schema // 明确定义schema字段
}

// NewSystemVariableProjectionExecutor 创建系统变量投影执行器
func NewSystemVariableProjectionExecutor(ctx *engine.ExecutionContext, child engine.Executor, columns []string, varQuery *manager.SystemVariableQuery) *SystemVariableProjectionExecutor {
	// 创建Schema实现的指针
	schemaImpl := NewSystemVariableSchema(varQuery)
	// 将接口实现转换为指向接口的指针
	var schema metadata.Schema = schemaImpl

	return &SystemVariableProjectionExecutor{
		BaseExecutor: engine.BaseExecutor{},
		child:        child,
		columns:      columns,
		varQuery:     varQuery,
		schema:       &schema, // 指向接口的指针
	}
}

// Init 初始化投影执行器
func (p *SystemVariableProjectionExecutor) Init() error {
	logger.Debugf(" 初始化系统变量投影执行器")
	if p.child != nil {
		return p.child.Init()
	}
	return nil
}

// Next 获取下一行投影数据
func (p *SystemVariableProjectionExecutor) Next() error {
	if p.child != nil {
		return p.child.Next()
	}
	return io.EOF
}

// GetRow 获取当前投影行数据
func (p *SystemVariableProjectionExecutor) GetRow() []interface{} {
	if p.child != nil {
		return p.child.GetRow()
	}
	return nil
}

// Close 关闭投影执行器
func (p *SystemVariableProjectionExecutor) Close() error {
	logger.Debugf("🔚 关闭系统变量投影执行器")
	if p.child != nil {
		return p.child.Close()
	}
	return nil
}

// Schema 返回投影器的schema
func (p *SystemVariableProjectionExecutor) Schema() *metadata.Schema {
	return p.schema
}

// Children 返回子执行器
func (p *SystemVariableProjectionExecutor) Children() []engine.Executor {
	if p.child != nil {
		return []engine.Executor{p.child}
	}
	return nil
}

// SetChildren 设置子执行器
func (p *SystemVariableProjectionExecutor) SetChildren(children []engine.Executor) {
	if len(children) > 0 {
		p.child = children[0]
	}
}

// getSessionID 从会话中提取会话ID
func (e *SystemVariableEngine) getSessionID(session server.MySQLServerSession) string {
	// 尝试多种方式获取会话ID

	// 1. 尝试从会话参数中获取
	if sessionIDParam := session.GetParamByName("session_id"); sessionIDParam != nil {
		if sessionID, ok := sessionIDParam.(string); ok && sessionID != "" {
			logger.Debugf("🔑 从session_id参数获取会话ID: %s", sessionID)
			return sessionID
		}
	}

	// 2. 尝试从会话的ID()方法获取（如果实现了）
	if sessionIDMethod, ok := session.(interface{ ID() string }); ok {
		sessionID := sessionIDMethod.ID()
		if sessionID != "" {
			logger.Debugf("🔑 从ID()方法获取会话ID: %s", sessionID)
			return sessionID
		}
	}

	// 3. 尝试从GetSessionId方法获取
	if sessionIDGetter, ok := session.(interface{ GetSessionId() string }); ok {
		sessionID := sessionIDGetter.GetSessionId()
		if sessionID != "" {
			logger.Debugf("🔑 从GetSessionId()方法获取会话ID: %s", sessionID)
			return sessionID
		}
	}

	// 4. 如果都失败了，生成一个基于用户和数据库的会话ID
	var user, database string
	if userParam := session.GetParamByName("user"); userParam != nil {
		if u, ok := userParam.(string); ok {
			user = u
		}
	}
	if dbParam := session.GetParamByName("database"); dbParam != nil {
		if db, ok := dbParam.(string); ok {
			database = db
		}
	}

	// 生成一个相对唯一的会话ID
	sessionID := fmt.Sprintf("%s@%s_session", user, database)
	if sessionID == "@_session" {
		sessionID = "system_default_session"
	}

	logger.Debugf("🔑 生成默认会话ID: %s", sessionID)
	return sessionID
}

// CreateSessionVariables 为会话创建系统变量空间
func (e *SystemVariableEngine) CreateSessionVariables(sessionID string) {
	if e.sysVarManager != nil {
		e.sysVarManager.CreateSession(sessionID)
		logger.Debugf(" 为会话 %s 创建系统变量空间", sessionID)
	}
}

// DestroySessionVariables 销毁会话系统变量空间
func (e *SystemVariableEngine) DestroySessionVariables(sessionID string) {
	if e.sysVarManager != nil {
		e.sysVarManager.DestroySession(sessionID)
		logger.Debugf("🗑️  销毁会话 %s 的系统变量空间", sessionID)
	}
}

// SetSystemVariable 设置系统变量
func (e *SystemVariableEngine) SetSystemVariable(sessionID, varName string, value interface{}, scope manager.SystemVariableScope) error {
	if e.sysVarManager == nil {
		return fmt.Errorf("system variable manager not available")
	}

	return e.sysVarManager.SetVariable(sessionID, varName, value, scope)
}

// GetSystemVariable 获取系统变量
func (e *SystemVariableEngine) GetSystemVariable(sessionID, varName string, scope manager.SystemVariableScope) (interface{}, error) {
	if e.sysVarManager == nil {
		return nil, fmt.Errorf("system variable manager not available")
	}

	return e.sysVarManager.GetVariable(sessionID, varName, scope)
}
