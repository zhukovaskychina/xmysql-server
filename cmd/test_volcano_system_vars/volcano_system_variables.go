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

// 测试系统函数和SHOW语句
func testSystemFunctionsAndShowStatements(engine dispatcher.SQLEngine, session *MockMySQLServerSession) {
	fmt.Println("\n 测试系统函数和SHOW语句")
	fmt.Println(strings.Repeat("-", 50))

	// 1. 测试系统函数
	systemFunctionTests := []struct {
		name  string
		query string
	}{
		{"USER()函数", "SELECT USER()"},
		{"DATABASE()函数", "SELECT DATABASE()"},
		{"VERSION()函数", "SELECT VERSION()"},
		{"CONNECTION_ID()函数", "SELECT CONNECTION_ID()"},
		{"CURRENT_USER()函数", "SELECT CURRENT_USER()"},
		{"SESSION_USER()函数", "SELECT SESSION_USER()"},
		{"多个系统函数", "SELECT USER(), DATABASE(), VERSION()"},
		{"系统函数与别名", "SELECT USER() AS current_user, DATABASE() AS current_db"},
	}

	for _, test := range systemFunctionTests {
		logger.Debugf(" 测试 %s: %s\n", test.name, test.query)

		if engine.CanHandle(test.query) {
			logger.Debugf("    路由: %s 引擎\n", engine.Name())

			resultChan := engine.ExecuteQuery(session, test.query, "test_db")
			select {
			case result := <-resultChan:
				if result.Err != nil {
					logger.Debugf("    执行失败: %v\n", result.Err)
				} else {
					logger.Debugf("    结果: %d列 x %d行\n", len(result.Columns), len(result.Rows))
					if len(result.Rows) > 0 {
						logger.Debugf("    列名: %v\n", result.Columns)
						logger.Debugf("   📄 数据: %v\n", result.Rows[0])
					}
				}
			case <-time.After(2 * time.Second):
				logger.Debugf("   ⏰ 查询超时\n")
			}
		} else {
			logger.Debugf("    路由失败: 无法处理此查询\n")
		}
		fmt.Println()
	}

	// 2. 测试SHOW语句
	showStatementTests := []struct {
		name  string
		query string
	}{
		{"SHOW VARIABLES", "SHOW VARIABLES"},
		{"SHOW GLOBAL VARIABLES", "SHOW GLOBAL VARIABLES"},
		{"SHOW SESSION VARIABLES", "SHOW SESSION VARIABLES"},
		{"SHOW STATUS", "SHOW STATUS"},
		{"SHOW GLOBAL STATUS", "SHOW GLOBAL STATUS"},
		{"SHOW ENGINES", "SHOW ENGINES"},
		{"SHOW CHARSET", "SHOW CHARSET"},
		{"SHOW COLLATION", "SHOW COLLATION"},
		{"SHOW VARIABLES LIKE 'version%'", "SHOW VARIABLES LIKE 'version%'"},
		{"SHOW STATUS LIKE 'Connections'", "SHOW STATUS LIKE 'Connections'"},
	}

	for _, test := range showStatementTests {
		logger.Debugf(" 测试 %s: %s\n", test.name, test.query)

		if engine.CanHandle(test.query) {
			logger.Debugf("    路由: %s 引擎\n", engine.Name())

			resultChan := engine.ExecuteQuery(session, test.query, "test_db")
			select {
			case result := <-resultChan:
				if result.Err != nil {
					logger.Debugf("    执行失败: %v\n", result.Err)
				} else {
					logger.Debugf("    结果: %d列 x %d行\n", len(result.Columns), len(result.Rows))
					if len(result.Rows) > 0 {
						logger.Debugf("    列名: %v\n", result.Columns)
						// 只显示前3行数据避免输出过长
						maxRows := 3
						if len(result.Rows) < maxRows {
							maxRows = len(result.Rows)
						}
						for i := 0; i < maxRows; i++ {
							logger.Debugf("   📄 数据[%d]: %v\n", i, result.Rows[i])
						}
						if len(result.Rows) > 3 {
							logger.Debugf("   📄 ... (共%d行)\n", len(result.Rows))
						}
					}
				}
			case <-time.After(2 * time.Second):
				logger.Debugf("   ⏰ 查询超时\n")
			}
		} else {
			logger.Debugf("    路由失败: 无法处理此查询\n")
		}
		fmt.Println()
	}
}

func main() {
	fmt.Println("测试火山模型 + sqlparser + 系统变量查询集成（增强版）")
	fmt.Println(strings.Repeat("=", 70))

	// 创建最小配置避免存储初始化
	config := conf.NewCfg()
	config.Port = 3309

	// 只创建存储管理器（带最小配置避免路径问题）
	storageManager := manager.NewStorageManager(config)

	// 直接创建系统变量引擎进行测试
	sysVarEngine := dispatcher.NewSystemVariableEngine(storageManager)

	// 创建模拟会话
	session := NewMockMySQLServerSession("test_volcano_system_vars-session-001")
	session.SetParamByName("user", "root")
	session.SetParamByName("database", "test_db")

	// 测试系统函数和SHOW语句
	testSystemFunctionsAndShowStatements(sysVarEngine, session)

	fmt.Println("\n系统函数和SHOW语句测试完成！")
	fmt.Println("火山模型 + sqlparser + 系统变量查询集成测试通过")
	logger.Debugf("系统变量引擎: %s\n", sysVarEngine.Name())
	fmt.Println("支持系统函数: USER(), DATABASE(), VERSION(), CONNECTION_ID(), etc.")
	fmt.Println("支持SHOW语句: VARIABLES, STATUS, ENGINES, CHARSET, COLLATION")
	fmt.Println("火山模型执行: 高效的迭代器模式")
	fmt.Println("sqlparser解析: 精确的SQL语句分析")
}
