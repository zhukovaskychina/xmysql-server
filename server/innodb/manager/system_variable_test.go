package manager

import (
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

func TestSystemVariablesManager(t *testing.T) {
	// 创建系统变量管理器
	mgr := NewSystemVariablesManager()
	if mgr == nil {
		t.Fatal("Failed to create SystemVariablesManager")
	}

	// 测试获取系统变量
	sessionID := "test_session"
	mgr.CreateSession(sessionID)

	// 测试获取自增变量
	value, err := mgr.GetVariable(sessionID, "auto_increment_increment", SessionScope)
	if err != nil {
		t.Errorf("Failed to get auto_increment_increment: %v", err)
	}
	if value != int64(1) {
		t.Errorf("Expected auto_increment_increment to be 1, got %v", value)
	}

	// 测试设置变量
	err = mgr.SetVariable(sessionID, "autocommit", "OFF", SessionScope)
	if err != nil {
		t.Errorf("Failed to set autocommit: %v", err)
	}

	// 验证设置的值
	value, err = mgr.GetVariable(sessionID, "autocommit", SessionScope)
	if err != nil {
		t.Errorf("Failed to get autocommit after set: %v", err)
	}
	if value != "OFF" {
		t.Errorf("Expected autocommit to be OFF, got %v", value)
	}

	// 测试只读变量
	err = mgr.SetVariable(sessionID, "version", "test_simple_protocol", SessionScope)
	if err == nil {
		t.Error("Expected error when setting read-only variable")
	}

	// 清理
	mgr.DestroySession(sessionID)
}

func TestSystemVariablesManagerSupportsInnoDBLockWaitTimeout(t *testing.T) {
	mgr := NewSystemVariablesManager()
	mgr.CreateSession("lock-timeout")
	value, err := mgr.GetVariable("lock-timeout", "innodb_lock_wait_timeout", SessionScope)
	if err != nil {
		t.Fatalf("failed to read innodb_lock_wait_timeout: %v", err)
	}
	if value != int64(50) {
		t.Fatalf("expected default innodb_lock_wait_timeout=50, got %v", value)
	}
	if err := mgr.SetVariable("lock-timeout", "innodb_lock_wait_timeout", int64(2), SessionScope); err != nil {
		t.Fatalf("failed to set innodb_lock_wait_timeout: %v", err)
	}
	value, err = mgr.GetVariable("lock-timeout", "innodb_lock_wait_timeout", SessionScope)
	if err != nil || value != int64(2) {
		t.Fatalf("expected session innodb_lock_wait_timeout=2, got %v (err=%v)", value, err)
	}
}

func TestSystemVariablesManagerListVariablesDoesNotReenterReadLock(t *testing.T) {
	mgr := NewSystemVariablesManager()
	done := make(chan map[string]interface{}, 1)
	go func() { done <- mgr.ListVariables("list-variables", GlobalScope) }()

	select {
	case variables := <-done:
		if variables["read_only"] != "OFF" {
			t.Fatalf("read_only = %v, want OFF", variables["read_only"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListVariables deadlocked while reading variable values")
	}
}

func TestSystemVariableAnalyzer(t *testing.T) {
	// 创建管理器和分析器
	mgr := NewSystemVariablesManager()
	analyzer := NewSystemVariableAnalyzer(mgr)

	testCases := []struct {
		sql      string
		expected bool
		desc     string
	}{
		{
			sql:      "SELECT @@auto_increment_increment",
			expected: true,
			desc:     "Simple system variable query",
		},
		{
			sql:      "SELECT @@session.autocommit",
			expected: true,
			desc:     "Session scope system variable",
		},
		{
			sql:      "SELECT @@global.version",
			expected: true,
			desc:     "Global scope system variable",
		},
		{
			sql:      "SELECT * FROM users",
			expected: false,
			desc:     "Regular table query",
		},
		{
			sql:      "INSERT INTO table VALUES (1)",
			expected: false,
			desc:     "Non-SELECT query",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := analyzer.IsSystemVariableQuery(tc.sql)
			if result != tc.expected {
				t.Errorf("For SQL '%s': expected %v, got %v", tc.sql, tc.expected, result)
			}
		})
	}
}

func TestSystemVariableQueryGeneration(t *testing.T) {
	// 创建管理器和分析器
	mgr := NewSystemVariablesManager()
	analyzer := NewSystemVariableAnalyzer(mgr)
	sessionID := "test_session"
	mgr.CreateSession(sessionID)

	// 测试MySQL客户端初始化查询
	sql := `SELECT @@session.auto_increment_increment AS auto_increment_increment, 
			@@character_set_client AS character_set_client, 
			@@character_set_connection AS character_set_connection, 
			@@character_set_results AS character_set_results, 
			@@character_set_server AS character_set_server`

	// 使用正则表达式分析
	query, err := analyzer.parseWithRegex(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL with regex: %v", err)
	}

	if !query.IsValid {
		t.Fatal("Query should be valid")
	}

	if len(query.Variables) == 0 {
		t.Fatal("Should find variables in query")
	}

	// 生成结果
	columns, rows, err := analyzer.GenerateSystemVariableResult(sessionID, query)
	if err != nil {
		t.Fatalf("Failed to generate result: %v", err)
	}

	if len(columns) == 0 {
		t.Fatal("Should have columns")
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	if len(rows[0]) != len(columns) {
		t.Fatalf("Row length %d doesn't match column length %d", len(rows[0]), len(columns))
	}

	// 打印结果用于调试
	logger.Debugf("Columns: %v\n", columns)
	logger.Debugf("Rows: %v\n", rows)

	// 清理
	mgr.DestroySession(sessionID)
}
