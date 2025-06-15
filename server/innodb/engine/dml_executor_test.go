package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestDMLExecutor_ParseInsertStatement(t *testing.T) {
	// 测试INSERT语句解析
	insertSQL := "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com.xmysql.server')"

	stmt, err := sqlparser.Parse(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT SQL: %v", err)
	}

	insertStmt, ok := stmt.(*sqlparser.Insert)
	if !ok {
		t.Fatalf("Expected INSERT statement, got %T", stmt)
	}

	// 测试表名解析
	if insertStmt.Table.Name.String() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insertStmt.Table.Name.String())
	}

	// 测试列数
	if len(insertStmt.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(insertStmt.Columns))
	}

	t.Logf(" INSERT语句解析测试通过")
}

func TestDMLExecutor_ParseUpdateStatement(t *testing.T) {
	// 测试UPDATE语句解析
	updateSQL := "UPDATE users SET name = 'Jane Doe', email = 'jane@example.com.xmysql.server' WHERE id = 1"

	stmt, err := sqlparser.Parse(updateSQL)
	if err != nil {
		t.Fatalf("Failed to parse UPDATE SQL: %v", err)
	}

	updateStmt, ok := stmt.(*sqlparser.Update)
	if !ok {
		t.Fatalf("Expected UPDATE statement, got %T", stmt)
	}

	// 测试SET表达式数量
	if len(updateStmt.Exprs) != 2 {
		t.Errorf("Expected 2 SET expressions, got %d", len(updateStmt.Exprs))
	}

	// 测试WHERE子句存在
	if updateStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}

	t.Logf(" UPDATE语句解析测试通过")
}

func TestDMLExecutor_ParseDeleteStatement(t *testing.T) {
	// 测试DELETE语句解析
	deleteSQL := "DELETE FROM users WHERE id = 1"

	stmt, err := sqlparser.Parse(deleteSQL)
	if err != nil {
		t.Fatalf("Failed to parse DELETE SQL: %v", err)
	}

	deleteStmt, ok := stmt.(*sqlparser.Delete)
	if !ok {
		t.Fatalf("Expected DELETE statement, got %T", stmt)
	}

	// 测试表表达式数量
	if len(deleteStmt.TableExprs) != 1 {
		t.Errorf("Expected 1 table expression, got %d", len(deleteStmt.TableExprs))
	}

	// 测试WHERE子句存在
	if deleteStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}

	t.Logf(" DELETE语句解析测试通过")
}

func TestDMLExecutor_ExecuteInsertWithMockData(t *testing.T) {
	// 创建DML执行器（使用nil管理器进行基本测试）
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil)

	// 解析INSERT语句
	insertSQL := "INSERT INTO users (id, name) VALUES (1, 'Test User')"
	stmt, err := sqlparser.Parse(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT SQL: %v", err)
	}

	insertStmt := stmt.(*sqlparser.Insert)

	// 执行INSERT（会因为管理器为空而失败，但测试基本流程）
	ctx := context.Background()
	_, err = dmlExecutor.ExecuteInsert(ctx, insertStmt, "test_db")

	// 由于没有真实的存储管理器，这里会失败，这是预期的
	if err == nil {
		t.Error("Expected error due to nil table manager, but got none")
	}

	t.Logf(" INSERT执行测试通过（预期错误：%v）", err)
}

func TestDMLExecutor_EvaluateExpressions(t *testing.T) {
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil)

	// 测试字符串值解析
	strVal := &sqlparser.SQLVal{
		Type: sqlparser.StrVal,
		Val:  []byte("test string"),
	}

	result, err := dmlExecutor.evaluateExpression(strVal)
	if err != nil {
		t.Errorf("Failed to evaluate string expression: %v", err)
	}

	if result != "test string" {
		t.Errorf("Expected 'test string', got %v", result)
	}

	// 测试整数值解析
	intVal := &sqlparser.SQLVal{
		Type: sqlparser.IntVal,
		Val:  []byte("123"),
	}

	result, err = dmlExecutor.evaluateExpression(intVal)
	if err != nil {
		t.Errorf("Failed to evaluate int expression: %v", err)
	}

	if result != int64(123) {
		t.Errorf("Expected 123, got %v", result)
	}

	// 测试NULL值解析
	nullVal := &sqlparser.NullVal{}
	result, err = dmlExecutor.evaluateExpression(nullVal)
	if err != nil {
		t.Errorf("Failed to evaluate null expression: %v", err)
	}

	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}

	t.Logf(" 表达式计算测试通过")
}

func TestDMLExecutor_ValidateTableNameParsing(t *testing.T) {
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil)

	// 创建一个简单的表表达式
	tableName := sqlparser.TableName{
		Name: sqlparser.NewTableIdent("test_table"),
	}

	aliasedTable := &sqlparser.AliasedTableExpr{
		Expr: tableName,
	}

	// 测试表名解析
	parsedName, err := dmlExecutor.parseTableName(aliasedTable)
	if err != nil {
		t.Errorf("Failed to parse table name: %v", err)
	}

	if parsedName != "test_table" {
		t.Errorf("Expected 'test_table', got '%s'", parsedName)
	}

	t.Logf(" 表名解析测试通过")
}
