package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
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
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

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
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

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
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

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

func TestDMLExecutor_ExtractPrimaryKeyFromCondition(t *testing.T) {
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	testCases := []struct {
		condition   string
		expectedKey interface{}
	}{
		{"id = 1", int64(1)},
		{"`user_id` = '789'", "789"},
		{"name = 'test'", nil},
		{"userid = 1", nil},
		{"id = 1 AND name = 'x'", int64(1)},
	}

	for _, tc := range testCases {
		key := dmlExecutor.extractPrimaryKeyFromCondition(tc.condition)
		if key != tc.expectedKey {
			t.Errorf("condition %q: expected %v, got %v", tc.condition, tc.expectedKey, key)
		}
	}

	t.Logf(" 条件解析主键提取测试通过")
}

func TestDMLExecutor_ParseTableSchemaFromUpdateExpr(t *testing.T) {
	dmlExecutor := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	updateSQL := "UPDATE test_db.users SET name = 'Jane Doe' WHERE id = 1"
	stmt, err := sqlparser.Parse(updateSQL)
	if err != nil {
		t.Fatalf("Failed to parse UPDATE SQL: %v", err)
	}

	updateStmt := stmt.(*sqlparser.Update)
	if len(updateStmt.TableExprs) == 0 {
		t.Fatalf("Expected at least one table expr")
	}

	tableSchema, err := dmlExecutor.parseTableSchema(updateStmt.TableExprs[0])
	if err != nil {
		t.Fatalf("Failed to parse table schema: %v", err)
	}
	if tableSchema != "test_db" {
		t.Errorf("Expected table schema 'test_db', got '%s'", tableSchema)
	}

	t.Logf(" table schema parse test passed")
}

func TestDMLExecutor_ParseInsertData_UsesColumnTypeMetadata(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt},
			{Name: "name", Type: metadata.TypeVarchar, Length: 5},
		},
	}

	stmt, err := sqlparser.Parse("INSERT INTO users (id, name) VALUES (1, 'alice')")
	assert.NoError(t, err)
	insertStmt := stmt.(*sqlparser.Insert)

	insertRows, err := dml.parseInsertData(insertStmt, tableMeta)
	assert.NoError(t, err)
	assert.Len(t, insertRows, 1)
	assert.Equal(t, metadata.TypeInt, insertRows[0].ColumnTypes["id"])
	assert.Equal(t, metadata.TypeVarchar, insertRows[0].ColumnTypes["name"])
}

func TestDMLExecutor_ValidateInsertData_RejectTypeMismatch(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt, IsNullable: false},
		},
	}

	rows := []*InsertRowData{
		{
			ColumnValues: map[string]interface{}{
				"id": "abc",
			},
			ColumnTypes: map[string]metadata.DataType{
				"id": metadata.TypeVarchar,
			},
		},
	}

	err := dml.validateInsertData(rows, tableMeta)
	assert.Error(t, err)
}

func TestDMLExecutor_ValidateInsertData_RejectLengthOverflow(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "name", Type: metadata.TypeVarchar, Length: 4, IsNullable: false},
		},
	}

	rows := []*InsertRowData{
		{
			ColumnValues: map[string]interface{}{
				"name": "toolong",
			},
			ColumnTypes: map[string]metadata.DataType{
				"name": metadata.TypeVarchar,
			},
		},
	}

	err := dml.validateInsertData(rows, tableMeta)
	assert.Error(t, err)
}

func TestDMLExecutor_ValidateInsertData_ApplyDefaultValue(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "status", Type: metadata.TypeVarchar, IsNullable: false, DefaultValue: "active"},
		},
	}

	rows := []*InsertRowData{
		{
			ColumnValues: map[string]interface{}{},
			ColumnTypes:  map[string]metadata.DataType{},
		},
	}

	err := dml.validateInsertData(rows, tableMeta)
	assert.NoError(t, err)
	assert.Equal(t, "active", rows[0].ColumnValues["status"])
}

func TestDMLExecutor_ParseUpdateExpressions_UsesMetadataType(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name: "users",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeInt},
			{Name: "name", Type: metadata.TypeVarchar},
		},
	}

	stmt, err := sqlparser.Parse("UPDATE users SET name = 'Jane Doe' WHERE id = 1")
	assert.NoError(t, err)
	updateStmt := stmt.(*sqlparser.Update)

	exprs, err := dml.parseUpdateExpressions(updateStmt.Exprs, tableMeta)
	assert.NoError(t, err)
	assert.Len(t, exprs, 1)
	assert.Equal(t, metadata.TypeVarchar, exprs[0].ColumnType)
}

func TestDMLExecutor_UpdateRow_ValidateLengthByMetadata(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name:       "users",
		PrimaryKey: []string{"id"},
		Columns: []*metadata.ColumnMeta{
			{Name: "name", Type: metadata.TypeVarchar, Length: 3, IsNullable: false},
		},
	}

	rowInfo := &RowUpdateInfo{
		RowId: 1,
		OldValues: map[string]interface{}{
			"id":   int64(1),
			"name": "abc",
		},
	}

	updateExprs := []*UpdateExpression{
		{
			ColumnName: "name",
			NewValue:   "toolong",
			ColumnType: metadata.TypeVarchar,
		},
	}

	err := dml.updateRow(context.Background(), nil, rowInfo, updateExprs, tableMeta)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "长度超限")
}

func TestDMLExecutor_UpdateRow_RejectNullForNotNull(t *testing.T) {
	dml := NewDMLExecutor(nil, nil, nil, nil, nil, nil)

	tableMeta := &metadata.TableMeta{
		Name:       "users",
		PrimaryKey: []string{"id"},
		Columns: []*metadata.ColumnMeta{
			{Name: "status", Type: metadata.TypeVarchar, IsNullable: false},
		},
	}

	rowInfo := &RowUpdateInfo{
		RowId: 1,
		OldValues: map[string]interface{}{
			"id":     int64(1),
			"status": "active",
		},
	}

	updateExprs := []*UpdateExpression{
		{
			ColumnName: "status",
			NewValue:   nil,
			ColumnType: metadata.TypeVarchar,
		},
	}

	err := dml.updateRow(context.Background(), nil, rowInfo, updateExprs, tableMeta)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "不允许为 NULL")
}
