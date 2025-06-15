package manager

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// SystemVariableQuery 系统变量查询信息
type SystemVariableQuery struct {
	Variables []SystemVariableInfo
	IsValid   bool
	RawSQL    string
}

// SystemVariableInfo 单个系统变量信息
type SystemVariableInfo struct {
	Name    string              // 变量名
	Scope   SystemVariableScope // 作用域
	Alias   string              // 别名
	RawExpr string              // 原始表达式
}

// SystemVariableAnalyzer 系统变量查询分析器
type SystemVariableAnalyzer struct {
	variableManager *SystemVariablesManager
}

// NewSystemVariableAnalyzer 创建系统变量分析器
func NewSystemVariableAnalyzer(varMgr *SystemVariablesManager) *SystemVariableAnalyzer {
	return &SystemVariableAnalyzer{
		variableManager: varMgr,
	}
}

// IsSystemVariableQuery 判断是否是系统变量查询
func (analyzer *SystemVariableAnalyzer) IsSystemVariableQuery(sql string) bool {
	sql = strings.TrimSpace(sql)

	// 检查SELECT语句中的系统变量
	matched, _ := regexp.MatchString(`(?i)^\s*SELECT\s+.*@@[a-zA-Z_][a-zA-Z0-9_]*`, sql)
	if matched {
		logger.Debugf(" 检测到SELECT系统变量查询: %s", sql)
		return true
	}

	// 检查SET语句中的系统变量
	setMatched, _ := regexp.MatchString(`(?i)^\s*SET\s+`, sql)
	if setMatched {
		logger.Debugf(" 检测到SET语句: %s", sql)
		return true
	}

	return false
}

// AnalyzeSystemVariableQuery 分析系统变量查询
func (analyzer *SystemVariableAnalyzer) AnalyzeSystemVariableQuery(sql string) (*SystemVariableQuery, error) {
	logger.Debugf(" 分析系统变量查询: %s", sql)

	// 先尝试用SQL解析器解析
	query, err := analyzer.parseWithSQLParser(sql)
	if err == nil && query.IsValid {
		logger.Debugf(" SQL解析器成功解析系统变量查询，找到 %d 个变量", len(query.Variables))
		return query, nil
	}

	// 如果SQL解析器失败，使用正则表达式回退方案
	logger.Debugf(" SQL解析器解析失败，使用正则表达式回退: %v", err)
	return analyzer.parseWithRegex(sql)
}

// parseWithSQLParser 使用SQL解析器解析
func (analyzer *SystemVariableAnalyzer) parseWithSQLParser(sql string) (*SystemVariableQuery, error) {
	// 尝试解析SQL
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL解析失败: %v", err)
	}

	query := &SystemVariableQuery{
		Variables: make([]SystemVariableInfo, 0),
		IsValid:   false,
		RawSQL:    sql,
	}

	// 根据语句类型进行处理
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// 处理SELECT语句
		return analyzer.parseSelectStatement(stmt, query)
	case *sqlparser.Set:
		// 处理SET语句
		return analyzer.parseSetStatement(stmt, query)
	default:
		return nil, fmt.Errorf("不支持的语句类型: %T", stmt)
	}
}

// parseSelectStatement 解析SELECT语句
func (analyzer *SystemVariableAnalyzer) parseSelectStatement(stmt *sqlparser.Select, query *SystemVariableQuery) (*SystemVariableQuery, error) {
	// 分析SELECT表达式
	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			varInfo, err := analyzer.analyzeSelectExpression(expr)
			if err == nil {
				query.Variables = append(query.Variables, *varInfo)
			}
		}
	}

	query.IsValid = len(query.Variables) > 0
	return query, nil
}

// parseSetStatement 解析SET语句
func (analyzer *SystemVariableAnalyzer) parseSetStatement(stmt *sqlparser.Set, query *SystemVariableQuery) (*SystemVariableQuery, error) {
	logger.Debugf(" 解析SET语句，包含 %d 个表达式", len(stmt.Exprs))

	// 分析SET表达式
	for _, setExpr := range stmt.Exprs {
		varInfo, err := analyzer.analyzeSetExpression(setExpr)
		if err == nil {
			query.Variables = append(query.Variables, *varInfo)
			logger.Debugf(" 添加SET变量: %s", varInfo.Name)
		} else {
			logger.Debugf(" 解析SET表达式失败: %v", err)
		}
	}

	query.IsValid = len(query.Variables) > 0
	logger.Debugf(" SET语句解析完成，找到 %d 个变量", len(query.Variables))
	return query, nil
}

// analyzeSetExpression 分析SET表达式
func (analyzer *SystemVariableAnalyzer) analyzeSetExpression(expr *sqlparser.SetExpr) (*SystemVariableInfo, error) {
	// 获取变量名
	varName := expr.Name.String()
	logger.Debugf(" 分析SET变量: %s", varName)

	// 解析作用域和变量名
	scope, cleanVarName := analyzer.parseSetVariableName(varName)

	// 对于SET语句，我们总是认为它是有效的系统变量操作
	return &SystemVariableInfo{
		Name:    cleanVarName,
		Scope:   scope,
		Alias:   cleanVarName,
		RawExpr: varName,
	}, nil
}

// parseSetVariableName 解析SET变量名，提取作用域和变量名
func (analyzer *SystemVariableAnalyzer) parseSetVariableName(varName string) (SystemVariableScope, string) {
	varName = strings.TrimSpace(varName)

	// 处理 @@global.variable_name
	if strings.HasPrefix(varName, "@@global.") {
		cleanName := strings.TrimPrefix(varName, "@@global.")
		return GlobalScope, cleanName
	}

	// 处理 @@session.variable_name
	if strings.HasPrefix(varName, "@@session.") {
		cleanName := strings.TrimPrefix(varName, "@@session.")
		return SessionScope, cleanName
	}

	// 处理 @@variable_name (默认会话作用域)
	if strings.HasPrefix(varName, "@@") {
		cleanName := strings.TrimPrefix(varName, "@@")
		return SessionScope, cleanName
	}

	// 处理普通变量名 (默认会话作用域)
	return SessionScope, varName
}

// analyzeSelectExpression 分析SELECT表达式
func (analyzer *SystemVariableAnalyzer) analyzeSelectExpression(expr *sqlparser.AliasedExpr) (*SystemVariableInfo, error) {
	// 获取表达式字符串
	exprStr := sqlparser.String(expr.Expr)

	// 检查是否包含@@
	if !strings.Contains(exprStr, "@@") {
		return nil, fmt.Errorf("不是系统变量表达式")
	}

	// 解析变量名和作用域
	varName, scope, err := analyzer.parseVariableExpression(exprStr)
	if err != nil {
		return nil, err
	}

	// 获取别名
	alias := ""
	if !expr.As.IsEmpty() {
		alias = expr.As.String()
	} else {
		alias = varName // 默认使用变量名作为别名
	}

	return &SystemVariableInfo{
		Name:    varName,
		Scope:   scope,
		Alias:   alias,
		RawExpr: exprStr,
	}, nil
}

// parseVariableExpression 解析变量表达式
func (analyzer *SystemVariableAnalyzer) parseVariableExpression(expr string) (string, SystemVariableScope, error) {
	expr = strings.TrimSpace(expr)

	// 处理 @@global.variable_name
	if strings.HasPrefix(expr, "@@global.") {
		varName := strings.TrimPrefix(expr, "@@global.")
		return varName, GlobalScope, nil
	}

	// 处理 @@session.variable_name
	if strings.HasPrefix(expr, "@@session.") {
		varName := strings.TrimPrefix(expr, "@@session.")
		return varName, SessionScope, nil
	}

	// 处理 @@variable_name (默认会话作用域)
	if strings.HasPrefix(expr, "@@") {
		varName := strings.TrimPrefix(expr, "@@")
		return varName, SessionScope, nil // 默认为会话作用域
	}

	return "", SessionScope, fmt.Errorf("无效的系统变量表达式: %s", expr)
}

// parseWithRegex 使用正则表达式解析
func (analyzer *SystemVariableAnalyzer) parseWithRegex(sql string) (*SystemVariableQuery, error) {
	logger.Debugf(" 使用正则表达式解析系统变量查询")

	query := &SystemVariableQuery{
		Variables: make([]SystemVariableInfo, 0),
		IsValid:   false,
		RawSQL:    sql,
	}

	//  增强的正则表达式模式，支持复杂的多变量查询
	patterns := []struct {
		pattern string
		desc    string
	}{
		// 匹配 @@scope.variable AS alias 格式
		{`@@(global|session)\.([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)`, "scope.var AS alias"},
		// 匹配 @@scope.variable 格式
		{`@@(global|session)\.([a-zA-Z_][a-zA-Z0-9_]*)`, "scope.var"},
		// 匹配 @@variable AS alias 格式
		{`@@([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)`, "var AS alias"},
		// 匹配 @@variable 格式
		{`@@([a-zA-Z_][a-zA-Z0-9_]*)`, "var"},
	}

	processedVars := make(map[string]bool) // 防止重复处理同一个变量

	for _, p := range patterns {
		regex, err := regexp.Compile(`(?i)` + p.pattern)
		if err != nil {
			logger.Debugf(" 编译正则表达式失败: %s", p.pattern)
			continue
		}

		matches := regex.FindAllStringSubmatch(sql, -1)
		logger.Debugf(" 模式 '%s' 找到 %d 个匹配", p.desc, len(matches))

		for _, match := range matches {
			varInfo, err := analyzer.parseRegexMatch(match, p.desc)
			if err == nil && !processedVars[varInfo.Name] {
				query.Variables = append(query.Variables, *varInfo)
				processedVars[varInfo.Name] = true
				logger.Debugf(" 添加变量: %s (scope: %s, alias: %s)", varInfo.Name, varInfo.Scope, varInfo.Alias)
			} else if err != nil {
				logger.Debugf(" 解析匹配失败: %v", err)
			}
		}
	}

	query.IsValid = len(query.Variables) > 0

	if query.IsValid {
		logger.Debugf(" 正则表达式解析成功，找到 %d 个系统变量", len(query.Variables))
	} else {
		logger.Debugf(" 正则表达式解析失败，未找到系统变量")
	}

	return query, nil
}

// parseRegexMatch 解析正则匹配结果
func (analyzer *SystemVariableAnalyzer) parseRegexMatch(match []string, patternDesc string) (*SystemVariableInfo, error) {
	if len(match) < 2 {
		return nil, fmt.Errorf("匹配结果不足: %v", match)
	}

	var varName, scopeStr, alias string

	// 打印匹配结果用于调试
	logger.Debugf(" 正则匹配结果 (%s): %v", patternDesc, match)

	// 根据模式描述和匹配组的数量来判断格式
	switch patternDesc {
	case "scope.var AS alias":
		if len(match) >= 4 {
			scopeStr = match[1]
			varName = match[2]
			alias = match[3]
		}
	case "scope.var":
		if len(match) >= 3 {
			scopeStr = match[1]
			varName = match[2]
			alias = varName // 没有AS别名，使用变量名
		}
	case "var AS alias":
		if len(match) >= 3 {
			varName = match[1]
			alias = match[2]
			scopeStr = "session" // 默认为session
		}
	case "var":
		if len(match) >= 2 {
			varName = match[1]
			alias = varName
			scopeStr = "session" // 默认为session
		}
	default:
		// 通用解析逻辑
		if len(match) >= 4 && match[1] != "" && match[2] != "" {
			// @@scope.var AS alias 或 @@scope.var 模式
			scopeStr = match[1]
			varName = match[2]
			if len(match) > 3 && match[3] != "" {
				alias = match[3] // 有AS别名
			} else {
				alias = varName // 没有AS别名，使用变量名
			}
		} else if len(match) >= 3 && match[1] != "" {
			// @@var AS alias 模式
			varName = match[1]
			scopeStr = "session" // 默认为session
			if match[2] != "" {
				alias = match[2] // 有AS别名
			} else {
				alias = varName // 没有AS别名
			}
		} else if len(match) >= 2 && match[1] != "" {
			// @@var 模式 (最简单的情况)
			varName = match[1]
			scopeStr = "session" // 默认为session
			alias = varName
		} else {
			return nil, fmt.Errorf("无法解析匹配结果: %v", match)
		}
	}

	// 验证变量名不为空
	if varName == "" {
		return nil, fmt.Errorf("变量名为空: %v", match)
	}

	scope := ParseScope(scopeStr)

	logger.Debugf(" 解析变量: name=%s, scope=%s, alias=%s", varName, scopeStr, alias)

	return &SystemVariableInfo{
		Name:    varName,
		Scope:   scope,
		Alias:   alias,
		RawExpr: fmt.Sprintf("@@%s", varName), // 简化RawExpr
	}, nil
}

// GenerateSystemVariableResult 生成系统变量查询结果
func (analyzer *SystemVariableAnalyzer) GenerateSystemVariableResult(sessionID string, query *SystemVariableQuery) ([]string, [][]interface{}, error) {
	if !query.IsValid || len(query.Variables) == 0 {
		return nil, nil, fmt.Errorf("无效的系统变量查询")
	}

	// 准备列名
	columns := make([]string, len(query.Variables))
	for i, varInfo := range query.Variables {
		columns[i] = varInfo.Alias
	}

	// 准备数据行
	row := make([]interface{}, len(query.Variables))
	for i, varInfo := range query.Variables {
		value, err := analyzer.variableManager.GetVariable(sessionID, varInfo.Name, varInfo.Scope)
		if err != nil {
			logger.Errorf(" 获取系统变量 %s 失败: %v", varInfo.Name, err)
			value = nil // 使用 nil 表示未知变量
		}
		row[i] = value
	}

	rows := [][]interface{}{row}

	logger.Debugf(" 生成系统变量查询结果: columns=%v, rows=%v", columns, rows)
	return columns, rows, nil
}

// ValidateSystemVariables 验证系统变量是否存在
func (analyzer *SystemVariableAnalyzer) ValidateSystemVariables(query *SystemVariableQuery) error {
	for _, varInfo := range query.Variables {
		_, err := analyzer.variableManager.GetVariableDefinition(varInfo.Name)
		if err != nil {
			return fmt.Errorf("未知的系统变量: %s", varInfo.Name)
		}
	}
	return nil
}
