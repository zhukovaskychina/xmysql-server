package engine

import (
	"context"
	"fmt"
	"log"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// StorageIntegratedExample 存储引擎集成示例
type StorageIntegratedExample struct {
	executor *StorageIntegratedDMLExecutor
}

// NewStorageIntegratedExample 创建存储引擎集成示例
func NewStorageIntegratedExample() *StorageIntegratedExample {
	// 创建各种管理器（在实际应用中，这些应该从系统中获取）
	optimizerManager := &manager.OptimizerManager{}
	bufferPoolManager := &manager.OptimizedBufferPoolManager{}

	// 创建B+树管理器（使用接口类型）
	var btreeManager basic.BPlusTreeManager
	// 在实际应用中，这里应该初始化真实的B+树管理器

	tableManager := &manager.TableManager{}
	txManager := &manager.TransactionManager{}
	indexManager := &manager.IndexManager{}
	storageManager := &manager.StorageManager{}
	tableStorageManager := &manager.TableStorageManager{}

	// 创建存储引擎集成的DML执行器
	executor := NewStorageIntegratedDMLExecutor(
		optimizerManager,
		bufferPoolManager,
		btreeManager,
		tableManager,
		txManager,
		indexManager,
		storageManager,
		tableStorageManager,
	)

	return &StorageIntegratedExample{
		executor: executor,
	}
}

// RunInsertExample 运行INSERT示例
func (sie *StorageIntegratedExample) RunInsertExample() error {
	ctx := context.Background()

	// 解析INSERT语句
	insertSQL := `INSERT INTO users (id, name, email, age) VALUES (1, 'John Doe', 'john@example.com.xmysql.server', 25)`
	stmt, err := sqlparser.Parse(insertSQL)
	if err != nil {
		return fmt.Errorf("解析INSERT语句失败: %v", err)
	}

	insertStmt, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return fmt.Errorf("不是有效的INSERT语句")
	}

	// 执行INSERT操作
	result, err := sie.executor.ExecuteInsert(ctx, insertStmt, "testdb")
	if err != nil {
		return fmt.Errorf("执行INSERT失败: %v", err)
	}

	log.Printf("INSERT执行成功: %s", result.Message)
	log.Printf("影响行数: %d", result.AffectedRows)
	log.Printf("LastInsertID: %d", result.LastInsertId)

	return nil
}

// RunUpdateExample 运行UPDATE示例
func (sie *StorageIntegratedExample) RunUpdateExample() error {
	ctx := context.Background()

	// 解析UPDATE语句
	updateSQL := `UPDATE users SET name = 'Jane Doe', age = 26 WHERE id = 1`
	stmt, err := sqlparser.Parse(updateSQL)
	if err != nil {
		return fmt.Errorf("解析UPDATE语句失败: %v", err)
	}

	updateStmt, ok := stmt.(*sqlparser.Update)
	if !ok {
		return fmt.Errorf("不是有效的UPDATE语句")
	}

	// 执行UPDATE操作
	result, err := sie.executor.ExecuteUpdate(ctx, updateStmt, "testdb")
	if err != nil {
		return fmt.Errorf("执行UPDATE失败: %v", err)
	}

	log.Printf("UPDATE执行成功: %s", result.Message)
	log.Printf("影响行数: %d", result.AffectedRows)

	return nil
}

// RunDeleteExample 运行DELETE示例
func (sie *StorageIntegratedExample) RunDeleteExample() error {
	ctx := context.Background()

	// 解析DELETE语句
	deleteSQL := `DELETE FROM users WHERE id = 1`
	stmt, err := sqlparser.Parse(deleteSQL)
	if err != nil {
		return fmt.Errorf("解析DELETE语句失败: %v", err)
	}

	deleteStmt, ok := stmt.(*sqlparser.Delete)
	if !ok {
		return fmt.Errorf("不是有效的DELETE语句")
	}

	// 执行DELETE操作
	result, err := sie.executor.ExecuteDelete(ctx, deleteStmt, "testdb")
	if err != nil {
		return fmt.Errorf("执行DELETE失败: %v", err)
	}

	log.Printf("DELETE执行成功: %s", result.Message)
	log.Printf("影响行数: %d", result.AffectedRows)

	return nil
}

// RunCompleteExample 运行完整示例
func (sie *StorageIntegratedExample) RunCompleteExample() error {
	log.Println("🚀 开始存储引擎集成DML示例")

	// 1. 执行INSERT操作
	log.Println(" 执行INSERT操作...")
	if err := sie.RunInsertExample(); err != nil {
		log.Printf(" INSERT操作失败: %v", err)
		// 在示例中，我们继续执行其他操作
	} else {
		log.Println(" INSERT操作成功")
	}

	// 2. 执行UPDATE操作
	log.Println("🔄 执行UPDATE操作...")
	if err := sie.RunUpdateExample(); err != nil {
		log.Printf(" UPDATE操作失败: %v", err)
	} else {
		log.Println(" UPDATE操作成功")
	}

	// 3. 执行DELETE操作
	log.Println("🗑️ 执行DELETE操作...")
	if err := sie.RunDeleteExample(); err != nil {
		log.Printf(" DELETE操作失败: %v", err)
	} else {
		log.Println(" DELETE操作成功")
	}

	// 4. 显示统计信息
	stats := sie.executor.GetStats()
	log.Printf(" 执行统计信息:")
	log.Printf("   INSERT次数: %d", stats.InsertCount)
	log.Printf("   UPDATE次数: %d", stats.UpdateCount)
	log.Printf("   DELETE次数: %d", stats.DeleteCount)
	log.Printf("   索引更新次数: %d", stats.IndexUpdates)
	log.Printf("   事务总数: %d", stats.TransactionCount)
	log.Printf("   总执行时间: %v", stats.TotalTime)

	log.Println("🎉 存储引擎集成DML示例完成")
	return nil
}

// DemonstrateDataSerialization 演示数据序列化功能
func (sie *StorageIntegratedExample) DemonstrateDataSerialization() error {
	log.Println(" 演示数据序列化功能")

	// 创建测试数据
	testData := &InsertRowData{
		ColumnValues: map[string]interface{}{
			"id":     int64(1),
			"name":   "测试用户",
			"email":  "test@example.com.xmysql.server",
			"age":    int64(25),
			"active": true,
			"score":  float64(95.5),
		},
		ColumnTypes: make(map[string]metadata.DataType),
	}

	// 序列化数据
	serialized, err := sie.executor.serializeRowData(testData, nil)
	if err != nil {
		return fmt.Errorf("序列化失败: %v", err)
	}

	log.Printf("原始数据: %+v", testData.ColumnValues)
	log.Printf("序列化后大小: %d bytes", len(serialized))

	// 反序列化数据
	deserialized, err := sie.executor.deserializeRowData(serialized)
	if err != nil {
		return fmt.Errorf("反序列化失败: %v", err)
	}

	log.Printf("反序列化后数据: %+v", deserialized.ColumnValues)

	// 验证数据一致性
	if len(testData.ColumnValues) != len(deserialized.ColumnValues) {
		return fmt.Errorf("数据不一致: 原始%d列，反序列化%d列",
			len(testData.ColumnValues), len(deserialized.ColumnValues))
	}

	log.Println(" 数据序列化/反序列化验证成功")
	return nil
}

// DemonstrateTransactionManagement 演示事务管理功能
func (sie *StorageIntegratedExample) DemonstrateTransactionManagement() error {
	log.Println("🔄 演示事务管理功能")

	ctx := context.Background()

	// 开始事务
	txn, err := sie.executor.beginStorageTransaction(ctx)
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}

	log.Println(" 事务开始成功")

	// 模拟一些操作
	log.Println(" 模拟事务操作...")

	// 提交事务
	if err := sie.executor.commitStorageTransaction(ctx, txn); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	log.Println(" 事务提交成功")

	// 演示事务回滚
	txn2, err := sie.executor.beginStorageTransaction(ctx)
	if err != nil {
		return fmt.Errorf("开始第二个事务失败: %v", err)
	}

	log.Println("🔄 开始第二个事务（将回滚）")

	// 回滚事务
	if err := sie.executor.rollbackStorageTransaction(ctx, txn2); err != nil {
		return fmt.Errorf("回滚事务失败: %v", err)
	}

	log.Println(" 事务回滚成功")
	return nil
}

// ExampleUsage 示例用法函数
func ExampleUsage() {
	// 创建示例实例
	example := NewStorageIntegratedExample()

	// 运行完整示例
	if err := example.RunCompleteExample(); err != nil {
		log.Printf("示例执行失败: %v", err)
	}

	// 演示数据序列化
	if err := example.DemonstrateDataSerialization(); err != nil {
		log.Printf("数据序列化演示失败: %v", err)
	}

	// 演示事务管理
	if err := example.DemonstrateTransactionManagement(); err != nil {
		log.Printf("事务管理演示失败: %v", err)
	}
}
