package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/dispatcher"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// MockMySQLServerSession 模拟MySQL服务器会话
type MockMySQLServerSession struct {
	sessionID string
	params    map[string]interface{}
}

func NewMockMySQLServerSession(sessionID string) *MockMySQLServerSession {
	return &MockMySQLServerSession{
		sessionID: sessionID,
		params:    make(map[string]interface{}),
	}
}

func (s *MockMySQLServerSession) GetParamByName(name string) interface{} {
	return s.params[name]
}

func (s *MockMySQLServerSession) SetParamByName(name string, value interface{}) {
	s.params[name] = value
}

func (s *MockMySQLServerSession) SendOK() {}

func (s *MockMySQLServerSession) SendErr(err error) {}

func (s *MockMySQLServerSession) GetSessionId() string {
	return s.sessionID
}

func (s *MockMySQLServerSession) ID() string {
	return s.sessionID
}

func (s *MockMySQLServerSession) GetLastActiveTime() time.Time {
	return time.Now()
}

func (s *MockMySQLServerSession) SessionContext() *server.SessionContext {
	return server.NewSessionContext(s.sessionID)
}

func (s *MockMySQLServerSession) SendHandleOk() {}

func (s *MockMySQLServerSession) SendSelectFields() {}

func main() {
	fmt.Println(" 测试JDBC连接系统变量查询修复")
	fmt.Println(strings.Repeat("=", 60))

	// 创建最小配置
	config := conf.NewCfg()
	config.Port = 3309

	// 创建存储管理器
	storageManager := manager.NewStorageManager(config)

	// 创建系统变量引擎
	sysVarEngine := dispatcher.NewSystemVariableEngine(storageManager)

	// 创建模拟会话
	session := NewMockMySQLServerSession("jdbc-test-session-001")
	session.SetParamByName("user", "root")
	session.SetParamByName("database", "test_db")

	// JDBC连接时的系统变量查询（mysql-connector-java-5.1.49）
	jdbcQuery := `/* mysql-connector-java-5.1.49 ( Revision: ad86f36e100e104cd926c6b81c8cab9565750116 ) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_buffer_length AS net_buffer_length, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@tx_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout`

	fmt.Println(" 测试JDBC连接查询:")
	logger.Debugf("   %s\n\n", jdbcQuery)

	// 检查引擎是否能处理此查询
	if !sysVarEngine.CanHandle(jdbcQuery) {
		fmt.Println(" 系统变量引擎无法处理JDBC查询")
		return
	}

	logger.Debugf(" 路由成功: %s 引擎\n", sysVarEngine.Name())

	// 执行查询
	resultChan := sysVarEngine.ExecuteQuery(session, jdbcQuery, "test_db")

	// 处理结果
	select {
	case result := <-resultChan:
		if result.Err != nil {
			logger.Debugf(" 查询执行失败: %v\n", result.Err)
			return
		}

		logger.Debugf(" 查询执行成功!\n")
		logger.Debugf(" 结果类型: %s\n", result.ResultType)
		logger.Debugf(" 消息: %s\n", result.Message)
		logger.Debugf(" 列数: %d\n", len(result.Columns))
		logger.Debugf("📄 行数: %d\n", len(result.Rows))

		// 验证关键信息
		if len(result.Columns) == 0 {
			fmt.Println(" 错误: 没有列信息 - 这会导致JDBC驱动报错 'ResultSet is from UPDATE. No Data.'")
			return
		}

		if len(result.Rows) == 0 {
			fmt.Println(" 错误: 没有行数据")
			return
		}

		fmt.Println("\n 列信息:")
		for i, col := range result.Columns {
			logger.Debugf("  %d. %s\n", i+1, col)
		}

		fmt.Println("\n📄 系统变量值:")
		if len(result.Rows) > 0 {
			row := result.Rows[0]
			for i, value := range row {
				if i < len(result.Columns) {
					logger.Debugf("  %s = %v\n", result.Columns[i], value)
				}
			}
		}

		// 验证关键的JDBC连接变量
		fmt.Println("\n 验证关键JDBC变量:")
		expectedVars := []string{
			"auto_increment_increment",
			"character_set_client",
			"character_set_connection",
			"character_set_results",
			"max_allowed_packet",
			"sql_mode",
			"time_zone",
			"transaction_isolation",
		}

		foundVars := make(map[string]bool)
		for _, col := range result.Columns {
			foundVars[col] = true
		}

		allFound := true
		for _, expectedVar := range expectedVars {
			if foundVars[expectedVar] {
				logger.Debugf("   %s\n", expectedVar)
			} else {
				logger.Debugf("   %s (缺失)\n", expectedVar)
				allFound = false
			}
		}

		if allFound {
			fmt.Println("\nJDBC连接修复验证成功!")
			fmt.Println("所有必需的系统变量都已正确返回")
			fmt.Println("结果集格式正确，包含列信息和数据行")
			fmt.Println("JDBC驱动应该能够正常连接")
		} else {
			fmt.Println("\n部分系统变量缺失，可能影响JDBC连接")
		}

	case <-time.After(5 * time.Second):
		fmt.Println(" 查询超时")
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("JDBC连接修复测试完成")
}
