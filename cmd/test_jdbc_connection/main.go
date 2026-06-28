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

func main() {
	fmt.Println(" 娴嬭瘯JDBC杩炴帴绯荤粺鍙橀噺鏌ヨ淇")
	fmt.Println(strings.Repeat("=", 60))

	// 鍒涘缓鏈€灏忛厤缃?	config := conf.NewCfg()
	config.Port = 3309

	// 鍒涘缓瀛樺偍绠＄悊鍣?	storageManager := manager.NewStorageManager(config)

	// 鍒涘缓绯荤粺鍙橀噺寮曟搸
	sysVarEngine := dispatcher.NewSystemVariableEngine(storageManager)

	// 鍒涘缓妯℃嫙浼氳瘽
	session := NewMockMySQLServerSession("jdbc-test-session-001")
	session.SetParamByName("user", "root")
	session.SetParamByName("database", "test_db")

	// JDBC杩炴帴鏃剁殑绯荤粺鍙橀噺鏌ヨ锛坢ysql-connector-java-5.1.49锛?	jdbcQuery := `/* mysql-connector-java-5.1.49 ( Revision: ad86f36e100e104cd926c6b81c8cab9565750116 ) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_buffer_length AS net_buffer_length, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@tx_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout`

	fmt.Println(" 娴嬭瘯JDBC杩炴帴鏌ヨ:")
	logger.Debugf("   %s\n\n", jdbcQuery)

	// 妫€鏌ュ紩鎿庢槸鍚﹁兘澶勭悊姝ゆ煡璇?	if !sysVarEngine.CanHandle(jdbcQuery) {
		fmt.Println(" 绯荤粺鍙橀噺寮曟搸鏃犳硶澶勭悊JDBC鏌ヨ")
		return
	}

	logger.Debugf(" 璺敱鎴愬姛: %s 寮曟搸\n", sysVarEngine.Name())

	// 鎵ц鏌ヨ
	resultChan := sysVarEngine.ExecuteQuery(session, jdbcQuery, "test_db")

	// 澶勭悊缁撴灉
	select {
	case result := <-resultChan:
		if result.Err != nil {
			logger.Debugf(" 鏌ヨ鎵ц澶辫触: %v\n", result.Err)
			return
		}

		logger.Debugf(" 鏌ヨ鎵ц鎴愬姛!\n")
		logger.Debugf(" 缁撴灉绫诲瀷: %s\n", result.ResultType)
		logger.Debugf(" 娑堟伅: %s\n", result.Message)
		logger.Debugf(" 鍒楁暟: %d\n", len(result.Columns))
		logger.Debugf("馃搫 琛屾暟: %d\n", len(result.Rows))

		// 楠岃瘉鍏抽敭淇℃伅
		if len(result.Columns) == 0 {
			fmt.Println(" 閿欒: 娌℃湁鍒椾俊鎭?- 杩欎細瀵艰嚧JDBC椹卞姩鎶ラ敊 'ResultSet is from UPDATE. No Data.'")
			return
		}

		if len(result.Rows) == 0 {
			fmt.Println(" 閿欒: 娌℃湁琛屾暟鎹?)
			return
		}

		fmt.Println("\n 鍒椾俊鎭?")
		for i, col := range result.Columns {
			logger.Debugf("  %d. %s\n", i+1, col)
		}

		fmt.Println("\n馃搫 绯荤粺鍙橀噺鍊?")
		if len(result.Rows) > 0 {
			row := result.Rows[0]
			for i, value := range row {
				if i < len(result.Columns) {
					logger.Debugf("  %s = %v\n", result.Columns[i], value)
				}
			}
		}

		// 楠岃瘉鍏抽敭鐨凧DBC杩炴帴鍙橀噺
		fmt.Println("\n 楠岃瘉鍏抽敭JDBC鍙橀噺:")
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
				logger.Debugf("   %s (缂哄け)\n", expectedVar)
				allFound = false
			}
		}

		if allFound {
			fmt.Println("\nJDBC杩炴帴淇楠岃瘉鎴愬姛!")
			fmt.Println("鎵€鏈夊繀闇€鐨勭郴缁熷彉閲忛兘宸叉纭繑鍥?)
			fmt.Println("缁撴灉闆嗘牸寮忔纭紝鍖呭惈鍒椾俊鎭拰鏁版嵁琛?)
			fmt.Println("JDBC椹卞姩搴旇鑳藉姝ｅ父杩炴帴")
		} else {
			fmt.Println("\n閮ㄥ垎绯荤粺鍙橀噺缂哄け锛屽彲鑳藉奖鍝岼DBC杩炴帴")
		}

	case <-time.After(5 * time.Second):
		fmt.Println(" 鏌ヨ瓒呮椂")
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("JDBC杩炴帴淇娴嬭瘯瀹屾垚")
}
