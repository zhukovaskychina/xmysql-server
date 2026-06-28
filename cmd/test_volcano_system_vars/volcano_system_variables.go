//go:build demo
// +build demo

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

// MockMySQLServerSession 妯℃嫙MySQL鏈嶅姟鍣ㄤ細璇?type MockMySQLServerSession struct {
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

// 娴嬭瘯绯荤粺鍑芥暟鍜孲HOW璇彞
func testSystemFunctionsAndShowStatements(engine dispatcher.SQLEngine, session *MockMySQLServerSession) {
	fmt.Println("\n 娴嬭瘯绯荤粺鍑芥暟鍜孲HOW璇彞")
	fmt.Println(strings.Repeat("-", 50))

	// 1. 娴嬭瘯绯荤粺鍑芥暟
	systemFunctionTests := []struct {
		name  string
		query string
	}{
		{"USER()鍑芥暟", "SELECT USER()"},
		{"DATABASE()鍑芥暟", "SELECT DATABASE()"},
		{"VERSION()鍑芥暟", "SELECT VERSION()"},
		{"CONNECTION_ID()鍑芥暟", "SELECT CONNECTION_ID()"},
		{"CURRENT_USER()鍑芥暟", "SELECT CURRENT_USER()"},
		{"SESSION_USER()鍑芥暟", "SELECT SESSION_USER()"},
		{"澶氫釜绯荤粺鍑芥暟", "SELECT USER(), DATABASE(), VERSION()"},
		{"绯荤粺鍑芥暟涓庡埆鍚?, "SELECT USER() AS current_user, DATABASE() AS current_db"},
	}

	for _, test := range systemFunctionTests {
		logger.Debugf(" 娴嬭瘯 %s: %s\n", test.name, test.query)

		if engine.CanHandle(test.query) {
			logger.Debugf("    璺敱: %s 寮曟搸\n", engine.Name())

			resultChan := engine.ExecuteQuery(session, test.query, "test_db")
			select {
			case result := <-resultChan:
				if result.Err != nil {
					logger.Debugf("    鎵ц澶辫触: %v\n", result.Err)
				} else {
					logger.Debugf("    缁撴灉: %d鍒?x %d琛孿n", len(result.Columns), len(result.Rows))
					if len(result.Rows) > 0 {
						logger.Debugf("    鍒楀悕: %v\n", result.Columns)
						logger.Debugf("   馃搫 鏁版嵁: %v\n", result.Rows[0])
					}
				}
			case <-time.After(2 * time.Second):
				logger.Debugf("   鈴?鏌ヨ瓒呮椂\n")
			}
		} else {
			logger.Debugf("    璺敱澶辫触: 鏃犳硶澶勭悊姝ゆ煡璇n")
		}
		fmt.Println()
	}

	// 2. 娴嬭瘯SHOW璇彞
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
		logger.Debugf(" 娴嬭瘯 %s: %s\n", test.name, test.query)

		if engine.CanHandle(test.query) {
			logger.Debugf("    璺敱: %s 寮曟搸\n", engine.Name())

			resultChan := engine.ExecuteQuery(session, test.query, "test_db")
			select {
			case result := <-resultChan:
				if result.Err != nil {
					logger.Debugf("    鎵ц澶辫触: %v\n", result.Err)
				} else {
					logger.Debugf("    缁撴灉: %d鍒?x %d琛孿n", len(result.Columns), len(result.Rows))
					if len(result.Rows) > 0 {
						logger.Debugf("    鍒楀悕: %v\n", result.Columns)
						// 鍙樉绀哄墠3琛屾暟鎹伩鍏嶈緭鍑鸿繃闀?						maxRows := 3
						if len(result.Rows) < maxRows {
							maxRows = len(result.Rows)
						}
						for i := 0; i < maxRows; i++ {
							logger.Debugf("   馃搫 鏁版嵁[%d]: %v\n", i, result.Rows[i])
						}
						if len(result.Rows) > 3 {
							logger.Debugf("   馃搫 ... (鍏?d琛?\n", len(result.Rows))
						}
					}
				}
			case <-time.After(2 * time.Second):
				logger.Debugf("   鈴?鏌ヨ瓒呮椂\n")
			}
		} else {
			logger.Debugf("    璺敱澶辫触: 鏃犳硶澶勭悊姝ゆ煡璇n")
		}
		fmt.Println()
	}
}

func main() {
	fmt.Println("娴嬭瘯鐏北妯″瀷 + sqlparser + 绯荤粺鍙橀噺鏌ヨ闆嗘垚锛堝寮虹増锛?)
	fmt.Println(strings.Repeat("=", 70))

	// 鍒涘缓鏈€灏忛厤缃伩鍏嶅瓨鍌ㄥ垵濮嬪寲
	config := conf.NewCfg()
	config.Port = 3309

	// 鍙垱寤哄瓨鍌ㄧ鐞嗗櫒锛堝甫鏈€灏忛厤缃伩鍏嶈矾寰勯棶棰橈級
	storageManager := manager.NewStorageManager(config)

	// 鐩存帴鍒涘缓绯荤粺鍙橀噺寮曟搸杩涜娴嬭瘯
	sysVarEngine := dispatcher.NewSystemVariableEngine(storageManager)

	// 鍒涘缓妯℃嫙浼氳瘽
	session := NewMockMySQLServerSession("test_volcano_system_vars-session-001")
	session.SetParamByName("user", "root")
	session.SetParamByName("database", "test_db")

	// 娴嬭瘯绯荤粺鍑芥暟鍜孲HOW璇彞
	testSystemFunctionsAndShowStatements(sysVarEngine, session)

	fmt.Println("\n绯荤粺鍑芥暟鍜孲HOW璇彞娴嬭瘯瀹屾垚锛?)
	fmt.Println("鐏北妯″瀷 + sqlparser + 绯荤粺鍙橀噺鏌ヨ闆嗘垚娴嬭瘯閫氳繃")
	logger.Debugf("绯荤粺鍙橀噺寮曟搸: %s\n", sysVarEngine.Name())
	fmt.Println("鏀寔绯荤粺鍑芥暟: USER(), DATABASE(), VERSION(), CONNECTION_ID(), etc.")
	fmt.Println("鏀寔SHOW璇彞: VARIABLES, STATUS, ENGINES, CHARSET, COLLATION")
	fmt.Println("鐏北妯″瀷鎵ц: 楂樻晥鐨勮凯浠ｅ櫒妯″紡")
	fmt.Println("sqlparser瑙ｆ瀽: 绮剧‘鐨凷QL璇彞鍒嗘瀽")
}
