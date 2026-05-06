package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// TestMySQLUserTableProjection 测试 mysql.user 表的投影功能
func TestMySQLUserTableProjection(t *testing.T) {
	// 创建 SelectExecutor
	se := &SelectExecutor{}

	// 解析 SQL
	sql := `SELECT User, Host, authentication_string, account_locked, password_expired, 
	        max_connections, max_user_connections
	        FROM mysql.user 
	        WHERE User = 'root' AND Host = '%'`

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("Expected *sqlparser.Select, got %T", stmt)
	}

	// 解析 SELECT 语句
	err = se.parseSelectStatement(selectStmt, "mysql")
	if err != nil {
		t.Fatalf("Failed to parse SELECT statement: %v", err)
	}

	// 验证解析的列
	expectedColumns := []string{"User", "Host", "authentication_string", "account_locked",
		"password_expired", "max_connections", "max_user_connections"}

	if len(se.selectExprs) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(se.selectExprs))
	}

	// 创建默认用户数据
	err = se.createDefaultUserData()
	if err != nil {
		t.Fatalf("Failed to create default user data: %v", err)
	}

	// 验证创建了2条记录
	if len(se.resultSet) != 2 {
		t.Errorf("Expected 2 records, got %d", len(se.resultSet))
	}

	// 应用投影
	projectedRecords := se.applyProjection(se.resultSet)

	// 验证投影后的记录数
	if len(projectedRecords) != 2 {
		t.Errorf("Expected 2 projected records, got %d", len(projectedRecords))
	}

	// 验证投影后的字段数
	for i, record := range projectedRecords {
		fieldCount := record.GetColumnCount()
		if fieldCount != len(expectedColumns) {
			t.Errorf("Record %d: expected %d fields, got %d", i, len(expectedColumns), fieldCount)
		}

		// 验证字段值
		t.Logf("Record %d:", i)
		for j := 0; j < fieldCount; j++ {
			value := record.GetValueByIndex(j)
			if value != nil {
				t.Logf("  Field %d (%s): %v", j, expectedColumns[j], value.Raw())
			} else {
				t.Logf("  Field %d (%s): NULL", j, expectedColumns[j])
			}
		}
	}

	// 构建最终结果
	result := se.buildSelectResult()

	// 验证结果列名
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns in result, got %d", len(expectedColumns), len(result.Columns))
	}

	t.Logf("Result columns: %v", result.Columns)
	t.Logf("Result row count: %d", result.RowCount)
}

// TestMySQLUserTableSelectAll 测试 SELECT * 查询
func TestMySQLUserTableSelectAll(t *testing.T) {
	se := &SelectExecutor{}

	sql := `SELECT * FROM mysql.user WHERE User = 'root'`

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("Expected *sqlparser.Select, got %T", stmt)
	}

	err = se.parseSelectStatement(selectStmt, "mysql")
	if err != nil {
		t.Fatalf("Failed to parse SELECT statement: %v", err)
	}

	// 创建默认用户数据
	err = se.createDefaultUserData()
	if err != nil {
		t.Fatalf("Failed to create default user data: %v", err)
	}

	// 应用投影（SELECT * 不应该改变字段数）
	projectedRecords := se.applyProjection(se.resultSet)
	expectedFieldCount := len(se.getMySQLUserTableMeta().Columns)

	// 验证字段数（应该与当前 mysql.user 元数据一致）
	for i, record := range projectedRecords {
		fieldCount := record.GetColumnCount()
		if fieldCount != expectedFieldCount {
			t.Errorf("Record %d: expected %d fields for SELECT *, got %d", i, expectedFieldCount, fieldCount)
		}
	}

	// 构建最终结果
	result := se.buildSelectResult()

	// 验证结果列数（应该与当前 mysql.user 元数据一致）
	if len(result.Columns) != expectedFieldCount {
		t.Errorf("Expected %d columns in result for SELECT *, got %d", expectedFieldCount, len(result.Columns))
	}

	t.Logf("SELECT * result columns count: %d", len(result.Columns))
	t.Logf("SELECT * result row count: %d", result.RowCount)
}
