package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStorageIntegratedDMLExecutor_ComprehensiveOperations(t *testing.T) {
	// 创建存储引擎集成的DML执行器（使用nil管理器进行基本测试）
	executor := NewStorageIntegratedDMLExecutor(
		nil, // optimizerManager
		nil, // bufferPoolManager
		nil, // btreeManager
		nil, // tableManager
		nil, // txManager
		nil, // indexManager
		nil, // storageManager
		nil, // tableStorageManager
	)

	if executor == nil {
		t.Fatalf("Failed to create StorageIntegratedDMLExecutor")
	}

	// 验证统计信息初始化
	stats := executor.GetStats()
	if stats == nil {
		t.Error("Statistics should not be nil")
	}

	t.Logf(" 存储引擎集成DML执行器创建成功")
}

func TestStorageIntegratedDMLExecutor_InsertWithSerialization(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 测试INSERT语句解析
	insertSQL := "INSERT INTO users (id, name, email, age) VALUES (1, 'John Doe', 'john@example.com.xmysql.server', 25)"
	stmt, err := sqlparser.Parse(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT SQL: %v", err)
	}

	insertStmt := stmt.(*sqlparser.Insert)

	// 测试表名解析
	if insertStmt.Table.Name.String() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insertStmt.Table.Name.String())
	}

	// 测试列数
	if len(insertStmt.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(insertStmt.Columns))
	}

	// 验证执行器可以解析INSERT数据
	tableMeta := &metadata.TableMeta{
		Name:    "users",
		Columns: []*metadata.ColumnMeta{},
	}

	insertRows, err := executor.parseInsertData(insertStmt, tableMeta)
	if err != nil {
		t.Errorf("Failed to parse insert data: %v", err)
	} else if len(insertRows) == 0 {
		t.Error("Expected at least one insert row")
	}

	t.Logf(" 存储引擎集成INSERT解析测试通过")
}

func TestStorageIntegratedDMLExecutor_UpdateWithConditions(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 测试UPDATE语句解析
	updateSQL := "UPDATE users SET name = 'Jane Doe', age = 26 WHERE id = 1"
	stmt, err := sqlparser.Parse(updateSQL)
	if err != nil {
		t.Fatalf("Failed to parse UPDATE SQL: %v", err)
	}

	updateStmt := stmt.(*sqlparser.Update)

	// 测试SET表达式数量
	if len(updateStmt.Exprs) != 2 {
		t.Errorf("Expected 2 SET expressions, got %d", len(updateStmt.Exprs))
	}

	// 测试WHERE子句存在
	if updateStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}

	// 验证执行器可以解析UPDATE表达式
	tableMeta := &metadata.TableMeta{
		Name:    "users",
		Columns: []*metadata.ColumnMeta{},
	}

	updateExprs, err := executor.parseUpdateExpressions(updateStmt.Exprs, tableMeta)
	if err != nil {
		t.Errorf("Failed to parse update expressions: %v", err)
	} else if len(updateExprs) != 2 {
		t.Errorf("Expected 2 update expressions, got %d", len(updateExprs))
	}

	t.Logf(" 存储引擎集成UPDATE解析测试通过")
}

func TestStorageIntegratedDMLExecutor_DeleteWithWhere(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 测试DELETE语句解析
	deleteSQL := "DELETE FROM users WHERE id = 1 AND status = 'inactive'"
	stmt, err := sqlparser.Parse(deleteSQL)
	if err != nil {
		t.Fatalf("Failed to parse DELETE SQL: %v", err)
	}

	deleteStmt := stmt.(*sqlparser.Delete)

	// 测试表表达式数量
	if len(deleteStmt.TableExprs) != 1 {
		t.Errorf("Expected 1 table expression, got %d", len(deleteStmt.TableExprs))
	}

	// 测试WHERE子句存在
	if deleteStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}

	// 验证执行器可以解析WHERE条件
	whereConditions := executor.parseWhereConditions(deleteStmt.Where)
	if len(whereConditions) == 0 {
		t.Error("Expected at least one WHERE condition")
	}

	t.Logf(" 存储引擎集成DELETE解析测试通过")
}

func TestStorageIntegratedDMLExecutor_DataSerialization(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 创建测试行数据
	testRow := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":     int64(1),
			"name":   "Test User",
			"email":  "test@example.com.xmysql.server",
			"age":    int64(30),
			"active": true,
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	// 测试序列化
	serialized, err := executor.serializeRowData(testRow, nil)
	if err != nil {
		t.Fatalf("Failed to serialize row data: %v", err)
	}

	if len(serialized) == 0 {
		t.Error("Serialized data is empty")
	}

	t.Logf(" 序列化数据大小: %d bytes", len(serialized))

	// 测试反序列化
	deserialized, err := executor.deserializeRowData(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize row data: %v", err)
	}

	// 验证反序列化结果
	if len(deserialized.ColumnValues) != len(testRow.ColumnValues) {
		t.Errorf("Expected %d columns, got %d", len(testRow.ColumnValues), len(deserialized.ColumnValues))
	}

	// 验证具体值
	if deserialized.ColumnValues["name"] != "Test User" {
		t.Errorf("Expected name 'Test User', got %v", deserialized.ColumnValues["name"])
	}

	if deserialized.ColumnValues["id"] != int64(1) {
		t.Errorf("Expected id 1, got %v", deserialized.ColumnValues["id"])
	}

	t.Logf(" 数据序列化/反序列化测试通过")
}

func TestStorageIntegratedDMLExecutor_TransactionContext(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	ctx := context.Background()

	// 测试事务开始
	txn, err := executor.beginStorageTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if txn == nil {
		t.Error("Transaction context is nil")
	}

	// 验证事务上下文
	txnCtx, ok := txn.(*StorageTransactionContext)
	if !ok {
		t.Error("Invalid transaction context type")
	}

	if txnCtx.Status != "ACTIVE" {
		t.Errorf("Expected transaction status 'ACTIVE', got '%s'", txnCtx.Status)
	}

	if txnCtx.TransactionID == 0 {
		t.Error("Transaction ID should not be zero")
	}

	// 测试事务提交
	err = executor.commitStorageTransaction(ctx, txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if txnCtx.Status != "COMMITTED" {
		t.Errorf("Expected transaction status 'COMMITTED', got '%s'", txnCtx.Status)
	}

	t.Logf(" 存储事务管理测试通过")
}

func TestStorageIntegratedDMLExecutor_PrimaryKeyGeneration(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 测试有ID列的情况
	rowWithId := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":   int64(100),
			"name": "Test User",
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	primaryKey, err := executor.generatePrimaryKey(rowWithId, nil)
	if err != nil {
		t.Fatalf("Failed to generate primary key: %v", err)
	}

	if primaryKey != int64(100) {
		t.Errorf("Expected primary key 100, got %v", primaryKey)
	}

	// 测试没有ID列的情况（自动生成）
	rowWithoutId := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"name": "Test User",
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	autoPrimaryKey, err := executor.generatePrimaryKey(rowWithoutId, nil)
	if err != nil {
		t.Fatalf("Failed to generate auto primary key: %v", err)
	}

	if autoPrimaryKey == nil {
		t.Error("Auto-generated primary key should not be nil")
	}

	t.Logf(" 主键生成测试通过，自动生成的主键: %v", autoPrimaryKey)
}

func TestStorageIntegratedDMLExecutor_ConditionParsing(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 测试WHERE条件解析
	testCases := []struct {
		condition   string
		expectedKey interface{}
	}{
		{"id = 1", int64(1)},
		{"id = '123'", "123"},
		{"user_id = 456", int64(456)},
		{"name = 'test'", nil}, // 非ID字段，应该返回nil
	}

	for _, tc := range testCases {
		key := executor.extractPrimaryKeyFromCondition(tc.condition)
		if key != tc.expectedKey {
			t.Errorf("For condition '%s', expected key %v, got %v", tc.condition, tc.expectedKey, key)
		}
	}

	t.Logf(" WHERE条件解析测试通过")
}

func TestStorageIntegratedDMLExecutor_Statistics(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 验证初始统计信息
	stats := executor.GetStats()
	if stats == nil {
		t.Error("Statistics should not be nil")
	}

	if stats.InsertCount != 0 {
		t.Errorf("Expected initial insert count 0, got %d", stats.InsertCount)
	}

	// 模拟一些操作来更新统计信息
	executor.updateInsertStats(5, time.Millisecond*100)
	executor.updateUpdateStats(3, time.Millisecond*80)
	executor.updateDeleteStats(2, time.Millisecond*50)

	updatedStats := executor.GetStats()
	if updatedStats.InsertCount != 5 {
		t.Errorf("Expected insert count 5, got %d", updatedStats.InsertCount)
	}

	if updatedStats.UpdateCount != 3 {
		t.Errorf("Expected update count 3, got %d", updatedStats.UpdateCount)
	}

	if updatedStats.DeleteCount != 2 {
		t.Errorf("Expected delete count 2, got %d", updatedStats.DeleteCount)
	}

	if updatedStats.TotalTime == 0 {
		t.Error("Total time should not be zero")
	}

	t.Logf(" 统计信息测试通过: Insert=%d, Update=%d, Delete=%d, TotalTime=%v",
		updatedStats.InsertCount, updatedStats.UpdateCount, updatedStats.DeleteCount, updatedStats.TotalTime)
}

func TestStorageIntegratedDMLExecutor_UpdateExpressionApplication(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 创建现有数据
	existingData := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":    int64(1),
			"name":  "Old Name",
			"email": "old@example.com.xmysql.server",
			"age":   int64(25),
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	// 创建更新表达式
	updateExprs := []*UpdateExpression{
		{
			ColumnName: "name",
			NewValue:   "New Name",
			ColumnType: metadata.TypeVarchar,
		},
		{
			ColumnName: "age",
			NewValue:   int64(30),
			ColumnType: metadata.TypeVarchar,
		},
	}

	// 应用更新表达式
	updatedData, err := executor.applyUpdateExpressions(existingData, updateExprs, nil)
	if err != nil {
		t.Fatalf("Failed to apply update expressions: %v", err)
	}

	// 验证更新结果
	if updatedData.ColumnValues["name"] != "New Name" {
		t.Errorf("Expected name 'New Name', got %v", updatedData.ColumnValues["name"])
	}

	if updatedData.ColumnValues["age"] != int64(30) {
		t.Errorf("Expected age 30, got %v", updatedData.ColumnValues["age"])
	}

	// 验证未更新的列保持不变
	if updatedData.ColumnValues["email"] != "old@example.com.xmysql.server" {
		t.Errorf("Expected email 'old@example.com.xmysql.server', got %v", updatedData.ColumnValues["email"])
	}

	if updatedData.ColumnValues["id"] != int64(1) {
		t.Errorf("Expected id 1, got %v", updatedData.ColumnValues["id"])
	}

	t.Logf(" 更新表达式应用测试通过")
}

func TestStorageIntegratedDMLExecutor_Performance(t *testing.T) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	// 性能测试：大量数据序列化/反序列化
	iterations := 1000
	startTime := time.Now()

	for i := 0; i < iterations; i++ {
		testRow := &InsertRowData{
			ColumnValues: map[string]interface{}{
				"id":    int64(i),
				"name":  fmt.Sprintf("User %d", i),
				"email": fmt.Sprintf("user%d@example.com.xmysql.server", i),
				"age":   int64(20 + i%50),
			},
			ColumnTypes: make(map[string]metadata.DataType),
		}

		// 序列化
		serialized, err := executor.serializeRowData(testRow, nil)
		if err != nil {
			t.Fatalf("Failed to serialize at iteration %d: %v", i, err)
		}

		// 反序列化
		_, err = executor.deserializeRowData(serialized)
		if err != nil {
			t.Fatalf("Failed to deserialize at iteration %d: %v", i, err)
		}
	}

	elapsed := time.Since(startTime)
	avgTime := elapsed / time.Duration(iterations)

	t.Logf(" 性能测试通过: %d 次操作耗时 %v，平均每次 %v", iterations, elapsed, avgTime)

	if avgTime > time.Millisecond {
		t.Logf("  警告: 平均操作时间较慢: %v", avgTime)
	}
}

// BenchmarkStorageIntegratedDMLExecutor_Serialization 序列化性能基准测试
func BenchmarkStorageIntegratedDMLExecutor_Serialization(b *testing.B) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	testRow := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":    int64(1),
			"name":  "Benchmark User",
			"email": "benchmark@example.com.xmysql.server",
			"age":   int64(25),
			"data":  "Some longer text data for testing serialization performance",
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := executor.serializeRowData(testRow, nil)
		if err != nil {
			b.Fatalf("Serialization failed: %v", err)
		}
	}
}

// BenchmarkStorageIntegratedDMLExecutor_Deserialization 反序列化性能基准测试
func BenchmarkStorageIntegratedDMLExecutor_Deserialization(b *testing.B) {
	executor := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)

	testRow := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":    int64(1),
			"name":  "Benchmark User",
			"email": "benchmark@example.com.xmysql.server",
			"age":   int64(25),
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	// 预先序列化数据
	serialized, err := executor.serializeRowData(testRow, nil)
	if err != nil {
		b.Fatalf("Pre-serialization failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := executor.deserializeRowData(serialized)
		if err != nil {
			b.Fatalf("Deserialization failed: %v", err)
		}
	}
}
