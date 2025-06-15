package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
)

// InnoDBEngineAccess InnoDB引擎访问实现
type InnoDBEngineAccess struct {
	config *conf.Cfg
	engine *engine.XMySQLEngine
}

// NewInnoDBEngineAccess 创建InnoDB引擎访问
func NewInnoDBEngineAccess(config *conf.Cfg, xmysqlEngine *engine.XMySQLEngine) EngineAccess {
	return &InnoDBEngineAccess{
		config: config,
		engine: xmysqlEngine,
	}
}

// QueryUser 查询用户信息
func (ea *InnoDBEngineAccess) QueryUser(ctx context.Context, user, host string) (*UserInfo, error) {
	// 构造查询SQL
	sql := fmt.Sprintf(`
		SELECT User, Host, authentication_string, account_locked, password_expired, 
		       max_connections, max_user_connections
		FROM mysql.user 
		WHERE User = '%s' AND Host = '%s'
	`, user, host)

	// 执行查询
	result, err := ea.executeQuery(ctx, sql, "mysql")
	if err != nil {
		return nil, fmt.Errorf("failed to query user: %v", err)
	}

	if len(result.Rows) == 0 {
		// 尝试通配符匹配
		return ea.queryUserWithWildcard(ctx, user, host)
	}

	row := result.Rows[0]
	userInfo := &UserInfo{
		User:               user,
		Host:               host,
		Password:           ea.getString(row, 2),
		AccountLocked:      ea.getBool(row, 3),
		PasswordExpired:    ea.getBool(row, 4),
		MaxConnections:     ea.getInt(row, 5),
		MaxUserConnections: ea.getInt(row, 6),
		DatabasePrivileges: make(map[string][]common.PrivilegeType),
		TablePrivileges:    make(map[string]map[string][]common.PrivilegeType),
	}

	return userInfo, nil
}

// queryUserWithWildcard 使用通配符查询用户
func (ea *InnoDBEngineAccess) queryUserWithWildcard(ctx context.Context, user, host string) (*UserInfo, error) {
	// 查询所有可能匹配的用户
	sql := fmt.Sprintf(`
		SELECT User, Host, authentication_string, account_locked, password_expired, 
		       max_connections, max_user_connections
		FROM mysql.user 
		WHERE User = '%s'
		ORDER BY Host DESC
	`, user)

	result, err := ea.executeQuery(ctx, sql, "mysql")
	if err != nil {
		return nil, fmt.Errorf("failed to query user with wildcard: %v", err)
	}

	// 查找最佳匹配
	for _, row := range result.Rows {
		hostPattern := ea.getString(row, 1)
		if ea.matchHost(host, hostPattern) {
			userInfo := &UserInfo{
				User:               user,
				Host:               hostPattern,
				Password:           ea.getString(row, 2),
				AccountLocked:      ea.getBool(row, 3),
				PasswordExpired:    ea.getBool(row, 4),
				MaxConnections:     ea.getInt(row, 5),
				MaxUserConnections: ea.getInt(row, 6),
				DatabasePrivileges: make(map[string][]common.PrivilegeType),
				TablePrivileges:    make(map[string]map[string][]common.PrivilegeType),
			}
			return userInfo, nil
		}
	}

	return nil, fmt.Errorf("user '%s'@'%s' not found", user, host)
}

// QueryDatabase 查询数据库信息
func (ea *InnoDBEngineAccess) QueryDatabase(ctx context.Context, database string) (*DatabaseInfo, error) {
	// 查询INFORMATION_SCHEMA.SCHEMATA
	sql := fmt.Sprintf(`
		SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
		FROM INFORMATION_SCHEMA.SCHEMATA 
		WHERE SCHEMA_NAME = '%s'
	`, database)

	result, err := ea.executeQuery(ctx, sql, "INFORMATION_SCHEMA")
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %v", err)
	}

	dbInfo := &DatabaseInfo{
		Name:   database,
		Exists: len(result.Rows) > 0,
	}

	if dbInfo.Exists {
		row := result.Rows[0]
		dbInfo.Charset = ea.getString(row, 1)
		dbInfo.Collation = ea.getString(row, 2)
	}

	return dbInfo, nil
}

// QueryUserPrivileges 查询用户全局权限
func (ea *InnoDBEngineAccess) QueryUserPrivileges(ctx context.Context, user, host string) ([]common.PrivilegeType, error) {
	sql := fmt.Sprintf(`
		SELECT Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv, Drop_priv,
		       Reload_priv, Shutdown_priv, Process_priv, File_priv, Grant_priv, References_priv,
		       Index_priv, Alter_priv, Show_db_priv, Super_priv, Create_tmp_table_priv,
		       Lock_tables_priv, Execute_priv, Repl_slave_priv, Repl_client_priv,
		       Create_view_priv, Show_view_priv, Create_routine_priv, Alter_routine_priv,
		       Create_user_priv, Event_priv, Trigger_priv, Create_tablespace_priv
		FROM mysql.user 
		WHERE User = '%s' AND Host = '%s'
	`, user, host)

	result, err := ea.executeQuery(ctx, sql, "mysql")
	if err != nil {
		return nil, fmt.Errorf("failed to query user privileges: %v", err)
	}

	if len(result.Rows) == 0 {
		return []common.PrivilegeType{}, nil
	}

	row := result.Rows[0]
	var privileges []common.PrivilegeType

	// 映射权限列到权限类型
	privMap := map[int]common.PrivilegeType{
		0:  common.SelectPriv,
		1:  common.InsertPriv,
		2:  common.UpdatePriv,
		3:  common.DeletePriv,
		4:  common.CreatePriv,
		5:  common.DropPriv,
		6:  common.ReloadPriv,
		7:  common.ShutdownPriv,
		8:  common.ProcessPriv,
		9:  common.FilePriv,
		10: common.GrantPriv,
		11: common.ReferencesPriv,
		12: common.IndexPriv,
		13: common.AlterPriv,
		14: common.ShowDBPriv,
		15: common.SuperPriv,
		16: common.CreateTMPTablePriv,
		17: common.LockTablesPriv,
		18: common.ExecutePriv,
		19: common.ReplicationSlavePriv,
		20: common.ReplicationClientPriv,
		21: common.CreateViewPriv,
		22: common.ShowViewPriv,
		23: common.CreateRoutinePriv,
		24: common.AlterRoutinePriv,
		25: common.CreateUserPriv,
		26: common.EventPriv,
		27: common.TriggerPriv,
		28: common.CreateTablespacePriv,
	}

	for i, priv := range privMap {
		if i < len(row) && ea.getBool(row, i) {
			privileges = append(privileges, priv)
		}
	}

	return privileges, nil
}

// QueryDatabasePrivileges 查询数据库权限
func (ea *InnoDBEngineAccess) QueryDatabasePrivileges(ctx context.Context, user, host, database string) ([]common.PrivilegeType, error) {
	sql := fmt.Sprintf(`
		SELECT Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv, Drop_priv,
		       Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv,
		       Lock_tables_priv, Create_view_priv, Show_view_priv, Create_routine_priv,
		       Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv
		FROM mysql.db 
		WHERE User = '%s' AND Host = '%s' AND Db = '%s'
	`, user, host, database)

	result, err := ea.executeQuery(ctx, sql, "mysql")
	if err != nil {
		return nil, fmt.Errorf("failed to query database privileges: %v", err)
	}

	if len(result.Rows) == 0 {
		return []common.PrivilegeType{}, nil
	}

	row := result.Rows[0]
	var privileges []common.PrivilegeType

	// 映射数据库权限
	privMap := map[int]common.PrivilegeType{
		0:  common.SelectPriv,
		1:  common.InsertPriv,
		2:  common.UpdatePriv,
		3:  common.DeletePriv,
		4:  common.CreatePriv,
		5:  common.DropPriv,
		6:  common.GrantPriv,
		7:  common.ReferencesPriv,
		8:  common.IndexPriv,
		9:  common.AlterPriv,
		10: common.CreateTMPTablePriv,
		11: common.LockTablesPriv,
		12: common.CreateViewPriv,
		13: common.ShowViewPriv,
		14: common.CreateRoutinePriv,
		15: common.AlterRoutinePriv,
		16: common.ExecutePriv,
		17: common.EventPriv,
		18: common.TriggerPriv,
	}

	for i, priv := range privMap {
		if i < len(row) && ea.getBool(row, i) {
			privileges = append(privileges, priv)
		}
	}

	return privileges, nil
}

// QueryTablePrivileges 查询表权限
func (ea *InnoDBEngineAccess) QueryTablePrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, error) {
	sql := fmt.Sprintf(`
		SELECT Table_priv
		FROM mysql.tables_priv 
		WHERE User = '%s' AND Host = '%s' AND Db = '%s' AND Table_name = '%s'
	`, user, host, database, table)

	result, err := ea.executeQuery(ctx, sql, "mysql")
	if err != nil {
		return nil, fmt.Errorf("failed to query table privileges: %v", err)
	}

	if len(result.Rows) == 0 {
		return []common.PrivilegeType{}, nil
	}

	row := result.Rows[0]
	privStr := ea.getString(row, 0)

	// 解析权限字符串
	var privileges []common.PrivilegeType
	privList := strings.Split(privStr, ",")
	for _, privName := range privList {
		privName = strings.TrimSpace(privName)
		if priv, ok := common.SetStr2Priv[privName]; ok {
			privileges = append(privileges, priv)
		}
	}

	return privileges, nil
}

// executeQuery 执行查询
func (ea *InnoDBEngineAccess) executeQuery(ctx context.Context, sql, database string) (*QueryResult, error) {
	logger.Debugf(" [executeQuery] 开始执行SQL: %s", sql)
	logger.Debugf(" [executeQuery] 目标数据库: %s", database)

	// 创建临时会话
	session := &MockEngineSession{
		sessionID: "auth-query",
		database:  database,
	}

	logger.Debugf(" [executeQuery] 创建临时会话: %s", session.sessionID)

	// 执行查询
	logger.Debugf(" [executeQuery] 调用 engine.ExecuteQuery...")
	resultChan := ea.engine.ExecuteQuery(session, sql, database)

	logger.Debugf(" [executeQuery] 等待查询结果...")

	// 等待结果
	resultCount := 0
	for result := range resultChan {
		resultCount++
		logger.Debugf("[executeQuery] 收到结果 #%d: error=%v, resultType=%s", resultCount, result.Err, result.ResultType)

		if result.Err != nil {
			logger.Errorf(" [executeQuery] SQL执行失败: %v", result.Err)
			return nil, result.Err
		}

		logger.Debugf("[executeQuery] 结果详细信息:")
		logger.Debugf("  - ResultType: %s", result.ResultType)
		logger.Debugf("  - Data: %+v", result.Data)
		if result.Rows != nil {
			logger.Debugf("  - Rows count: %d", len(result.Rows))
		}

		// 转换结果格式
		queryResult := &QueryResult{
			Columns: []string{}, // 暂时为空，需要从Data中提取
			Rows:    [][]interface{}{},
		}

		//  改进的结果处理逻辑
		if result.Data != nil {
			logger.Debugf(" [executeQuery] 处理结果数据...")

			// 尝试从SelectResult中提取数据
			if selectResult, ok := result.Data.(*engine.SelectResult); ok {
				logger.Debugf(" [executeQuery] 识别为 SelectResult")
				logger.Debugf("  - RowCount: %d", selectResult.RowCount)
				logger.Debugf("  - Columns: %v", selectResult.Columns)

				queryResult.Columns = selectResult.Columns

				// 从Records中提取行数据
				for i, record := range selectResult.Records {
					logger.Debugf("  - 处理记录 %d", i)

					rowData := make([]interface{}, len(selectResult.Columns))
					for j, columnName := range selectResult.Columns {
						// 从记录中获取字段值
						if value, err := record.GetValueByName(columnName); err == nil {
							rowData[j] = value.Raw()
							logger.Debugf("    - %s: %v", columnName, value.Raw())
						} else {
							rowData[j] = nil
							logger.Debugf("    - %s: NULL (error: %v)", columnName, err)
						}
					}
					queryResult.Rows = append(queryResult.Rows, rowData)
				}

				logger.Debugf(" [executeQuery] 成功提取 %d 行数据", len(queryResult.Rows))
				return queryResult, nil
			}

			// 尝试从Data map中提取
			if dataMap, ok := result.Data.(map[string]interface{}); ok {
				logger.Debugf(" [executeQuery] 识别为 Data map")

				if columns, exists := dataMap["columns"]; exists {
					if colSlice, ok := columns.([]string); ok {
						queryResult.Columns = colSlice
						logger.Debugf("  - 提取到列: %v", colSlice)
					}
				}

				if rows, exists := dataMap["rows"]; exists {
					if rowSlice, ok := rows.([][]interface{}); ok {
						queryResult.Rows = rowSlice
						logger.Debugf("  - 提取到 %d 行数据", len(rowSlice))
					}
				}
			}
		}

		// 从Rows中提取数据（备用方案）
		if result.Rows != nil && len(queryResult.Rows) == 0 {
			logger.Debugf(" [executeQuery] 从 result.Rows 提取数据...")

			for i, row := range result.Rows {
				logger.Debugf("  - 处理行 %d", i)
				var rowData []interface{}

				// 获取行的字段数量
				fieldCount := row.GetFieldLength()
				logger.Debugf("    - 字段数量: %d", fieldCount)

				for j := 0; j < fieldCount; j++ {
					value := row.ReadValueByIndex(j)
					if value != nil {
						rowData = append(rowData, value.Raw())
						logger.Debugf("    - 字段 %d: %v", j, value.Raw())
					} else {
						rowData = append(rowData, nil)
						logger.Debugf("    - 字段 %d: NULL", j)
					}
				}
				queryResult.Rows = append(queryResult.Rows, rowData)
			}
		}

		// 如果仍然没有列信息，创建默认列
		if len(queryResult.Columns) == 0 && len(queryResult.Rows) > 0 {
			columnCount := len(queryResult.Rows[0])
			queryResult.Columns = make([]string, columnCount)
			for i := 0; i < columnCount; i++ {
				queryResult.Columns[i] = fmt.Sprintf("column_%d", i+1)
			}
			logger.Debugf(" [executeQuery] 创建默认列名: %v", queryResult.Columns)
		}

		logger.Debugf(" [executeQuery] SQL执行成功，返回结果: columns=%v, rows=%d",
			queryResult.Columns, len(queryResult.Rows))
		return queryResult, nil
	}

	logger.Errorf(" [executeQuery] 未收到查询结果，结果数量: %d", resultCount)
	return nil, fmt.Errorf("no result received")
}

// QueryResult 查询结果
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

// 辅助方法
func (ea *InnoDBEngineAccess) getString(row []interface{}, index int) string {
	if index >= len(row) || row[index] == nil {
		return ""
	}
	if str, ok := row[index].(string); ok {
		return str
	}
	return fmt.Sprintf("%v", row[index])
}

func (ea *InnoDBEngineAccess) getBool(row []interface{}, index int) bool {
	if index >= len(row) || row[index] == nil {
		return false
	}

	switch v := row[index].(type) {
	case bool:
		return v
	case string:
		return strings.ToUpper(v) == "Y" || strings.ToUpper(v) == "YES" || v == "1"
	case int, int64:
		return v != 0
	default:
		str := fmt.Sprintf("%v", v)
		return strings.ToUpper(str) == "Y" || strings.ToUpper(str) == "YES" || str == "1"
	}
}

func (ea *InnoDBEngineAccess) getInt(row []interface{}, index int) int {
	if index >= len(row) || row[index] == nil {
		return 0
	}

	switch v := row[index].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case string:
		if i, err := fmt.Sscanf(v, "%d", new(int)); err == nil && i == 1 {
			var result int
			fmt.Sscanf(v, "%d", &result)
			return result
		}
		return 0
	default:
		return 0
	}
}

// matchHost 匹配主机模式
func (ea *InnoDBEngineAccess) matchHost(host, pattern string) bool {
	if pattern == "%" {
		return true
	}
	if pattern == host {
		return true
	}

	// 简单的通配符匹配
	if strings.Contains(pattern, "%") {
		// 将%替换为.*进行正则匹配
		regexPattern := strings.ReplaceAll(pattern, "%", ".*")
		regexPattern = "^" + regexPattern + "$"
		// 这里简化处理，实际应该使用正则表达式
		return strings.Contains(host, strings.ReplaceAll(pattern, "%", ""))
	}

	return false
}

// MockEngineSession 模拟引擎会话
type MockEngineSession struct {
	sessionID string
	database  string
}

func (s *MockEngineSession) GetSessionId() string {
	return s.sessionID
}

func (s *MockEngineSession) GetLastActiveTime() time.Time {
	return time.Now()
}

func (s *MockEngineSession) SetParamByName(name string, value interface{}) {
	// 模拟实现
}

func (s *MockEngineSession) GetParamByName(name string) interface{} {
	return nil
}

func (s *MockEngineSession) SendOK() {
	// 模拟实现
}

func (s *MockEngineSession) SendHandleOk() {
	// 模拟实现
}

func (s *MockEngineSession) SendSelectFields() {
	// 模拟实现
}

func (s *MockEngineSession) SendError(code uint16, message string) {
	// 模拟实现
}

func (s *MockEngineSession) SendResultSet(columns []string, rows [][]interface{}) {
	// 模拟实现
}
