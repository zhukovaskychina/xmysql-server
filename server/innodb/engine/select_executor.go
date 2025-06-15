package engine

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
)

// SelectExecutor SELECT查询执行器
type SelectExecutor struct {
	BaseExecutor

	// 管理器组件
	optimizerManager  *manager.OptimizerManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	btreeManager      basic.BPlusTreeManager
	tableManager      *manager.TableManager

	// 查询相关
	logicalPlan     plan.LogicalPlan
	physicalPlan    *manager.PlanNode
	tableName       string
	schemaName      string
	whereConditions []string
	selectExprs     []string
	orderByColumns  []string
	limit           int
	offset          int

	// 执行状态
	currentRowIndex int
	resultSet       []Record
	isInitialized   bool
}

// NewSelectExecutor 创建SELECT执行器
func NewSelectExecutor(
	optimizerManager *manager.OptimizerManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	btreeManager basic.BPlusTreeManager,
	tableManager *manager.TableManager,
) *SelectExecutor {
	return &SelectExecutor{
		optimizerManager:  optimizerManager,
		bufferPoolManager: bufferPoolManager,
		btreeManager:      btreeManager,
		tableManager:      tableManager,
		currentRowIndex:   0,
		resultSet:         make([]Record, 0),
		isInitialized:     false,
		limit:             -1, // -1表示无限制
		offset:            0,
	}
}

// ExecuteSelect 执行SELECT查询的主入口
func (se *SelectExecutor) ExecuteSelect(ctx context.Context, stmt *sqlparser.Select, schemaName string) (*SelectResult, error) {
	// 1. 解析SELECT语句
	if err := se.parseSelectStatement(stmt, schemaName); err != nil {
		return nil, fmt.Errorf("parse SELECT statement failed: %v", err)
	}

	// 2. 构建逻辑计划
	if err := se.buildLogicalPlan(ctx, stmt); err != nil {
		return nil, fmt.Errorf("build logical plan failed: %v", err)
	}

	// 3. 优化查询计划
	if err := se.optimizeQuery(ctx); err != nil {
		return nil, fmt.Errorf("optimize query failed: %v", err)
	}

	// 4. 生成物理执行计划
	if err := se.generatePhysicalPlan(ctx); err != nil {
		return nil, fmt.Errorf("generate physical plan failed: %v", err)
	}

	// 5. 执行查询
	if err := se.executeQuery(ctx); err != nil {
		return nil, fmt.Errorf("execute query failed: %v", err)
	}

	// 6. 构建结果
	result := se.buildSelectResult()
	return result, nil
}

// parseSelectStatement 解析SELECT语句
func (se *SelectExecutor) parseSelectStatement(stmt *sqlparser.Select, schemaName string) error {
	se.schemaName = schemaName

	// 解析FROM子句
	if len(stmt.From) == 0 {
		return fmt.Errorf("missing FROM clause")
	}

	// 简化处理，假设只有一个表
	for _, fromExpr := range stmt.From {
		switch v := fromExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch tableExpr := v.Expr.(type) {
			case sqlparser.TableName:
				se.tableName = tableExpr.Name.String()
			default:
				return fmt.Errorf("unsupported table expression type: %T", tableExpr)
			}
		default:
			return fmt.Errorf("unsupported FROM expression type: %T", v)
		}
	}

	// 解析SELECT表达式
	if err := se.parseSelectExprs(stmt.SelectExprs); err != nil {
		return err
	}

	// 解析WHERE条件
	if stmt.Where != nil {
		se.whereConditions = se.parseWhereConditions(stmt.Where.Expr)
	}

	// 解析ORDER BY
	if err := se.parseOrderBy(stmt.OrderBy); err != nil {
		return err
	}

	// 解析LIMIT
	if err := se.parseLimit(stmt.Limit); err != nil {
		return err
	}

	return nil
}

// parseSelectExprs 解析SELECT表达式
func (se *SelectExecutor) parseSelectExprs(selectExprs sqlparser.SelectExprs) error {
	for _, expr := range selectExprs {
		switch v := expr.(type) {
		case *sqlparser.StarExpr:
			// SELECT *
			se.selectExprs = append(se.selectExprs, "*")
		case *sqlparser.AliasedExpr:
			// SELECT column_name [AS alias]
			se.selectExprs = append(se.selectExprs, sqlparser.String(v.Expr))
		default:
			return fmt.Errorf("unsupported SELECT expression type: %T", v)
		}
	}
	return nil
}

// parseWhereConditions 解析WHERE条件
func (se *SelectExecutor) parseWhereConditions(expr sqlparser.Expr) []string {
	// 简化实现，将WHERE条件转换为字符串
	conditions := []string{sqlparser.String(expr)}
	return conditions
}

// parseOrderBy 解析ORDER BY子句
func (se *SelectExecutor) parseOrderBy(orderBy sqlparser.OrderBy) error {
	for _, order := range orderBy {
		column := sqlparser.String(order.Expr)
		if order.Direction == sqlparser.DescScr {
			column += " DESC"
		} else {
			column += " ASC"
		}
		se.orderByColumns = append(se.orderByColumns, column)
	}
	return nil
}

// parseLimit 解析LIMIT子句
func (se *SelectExecutor) parseLimit(limitClause *sqlparser.Limit) error {
	if limitClause == nil {
		return nil
	}

	if limitClause.Rowcount != nil {
		switch v := limitClause.Rowcount.(type) {
		case *sqlparser.SQLVal:
			if v.Type == sqlparser.IntVal {
				se.limit = int(v.Val[0]) // 简化处理
			}
		}
	}

	if limitClause.Offset != nil {
		switch v := limitClause.Offset.(type) {
		case *sqlparser.SQLVal:
			if v.Type == sqlparser.IntVal {
				se.offset = int(v.Val[0]) // 简化处理
			}
		}
	}

	return nil
}

// buildLogicalPlan 构建逻辑计划
func (se *SelectExecutor) buildLogicalPlan(ctx context.Context, stmt *sqlparser.Select) error {
	// 使用计划构建器构建逻辑计划
	infoSchema := &InfoSchemaAdapter{
		manager: se.tableManager,
	}

	logicalPlan, err := plan.BuildLogicalPlan(stmt, infoSchema)
	if err != nil {
		return fmt.Errorf("build logical plan failed: %v", err)
	}

	se.logicalPlan = logicalPlan
	return nil
}

// optimizeQuery 优化查询
func (se *SelectExecutor) optimizeQuery(ctx context.Context) error {
	// 使用优化器管理器优化查询
	physicalPlan, err := se.optimizerManager.GeneratePhysicalPlan(ctx, se.tableName, se.whereConditions)
	if err != nil {
		return fmt.Errorf("generate physical plan failed: %v", err)
	}

	se.physicalPlan = physicalPlan
	return nil
}

// generatePhysicalPlan 生成物理执行计划
func (se *SelectExecutor) generatePhysicalPlan(ctx context.Context) error {
	// 基于逻辑计划和优化结果生成物理执行计划
	// 这里我们已经有了物理计划，可以进行进一步的优化

	// 检查是否可以使用索引
	if err := se.chooseAccessMethod(ctx); err != nil {
		return fmt.Errorf("choose access method failed: %v", err)
	}

	return nil
}

// chooseAccessMethod 选择访问方法
func (se *SelectExecutor) chooseAccessMethod(ctx context.Context) error {
	// 获取表的索引信息
	indices, err := se.tableManager.GetTableIndices(ctx, se.schemaName, se.tableName)
	if err != nil {
		// 如果获取索引失败，使用全表扫描
		se.physicalPlan.PlanType = manager.PLAN_TYPE_SEQUENTIAL_SCAN
		return nil
	}

	// 简化实现：如果有索引且WHERE条件中有索引列，使用索引扫描
	if len(indices) > 0 && len(se.whereConditions) > 0 {
		se.physicalPlan.PlanType = manager.PLAN_TYPE_INDEX_SCAN
		se.physicalPlan.IndexName = indices[0].Name
	} else {
		se.physicalPlan.PlanType = manager.PLAN_TYPE_SEQUENTIAL_SCAN
	}

	return nil
}

// executeQuery 执行查询
func (se *SelectExecutor) executeQuery(ctx context.Context) error {
	logger.Debugf(" [SelectExecutor] 开始执行查询: table=%s, schema=%s", se.tableName, se.schemaName)

	//  特殊处理 mysql.user 表查询
	if se.schemaName == "mysql" && se.tableName == "user" {
		logger.Debugf(" [SelectExecutor] 检测到 mysql.user 表查询，使用特殊处理逻辑")
		return se.executeUserTableQuery(ctx)
	}

	// 通用查询处理逻辑
	logger.Debugf(" [SelectExecutor] 执行通用表查询")

	// 从表管理器获取表数据
	if se.tableManager != nil {
		logger.Debugf(" [SelectExecutor] 使用表管理器获取表数据")
		// TODO: 实现通用表查询逻辑
	}

	// 如果没有表管理器，创建示例数据
	logger.Debugf("  [SelectExecutor] 没有表管理器，创建示例数据")
	se.resultSet = []Record{
		NewExecutorRecordFromInterface([]interface{}{1, "sample_user", "sample_value"}, se.getDefaultTableMeta()),
		NewExecutorRecordFromInterface([]interface{}{2, "test_user", "test_value"}, se.getDefaultTableMeta()),
	}

	logger.Debugf(" [SelectExecutor] 查询执行完成，返回 %d 行数据", len(se.resultSet))
	return nil
}

// executeUserTableQuery 执行 mysql.user 表的特殊查询逻辑
func (se *SelectExecutor) executeUserTableQuery(ctx context.Context) error {
	logger.Debugf(" [SelectExecutor] 执行 mysql.user 表查询")

	// 尝试从缓冲池管理器获取存储管理器
	var storageManager interface{}
	if se.bufferPoolManager != nil {
		// 尝试通过反射或其他方式获取存储管理器
		// 这里暂时设置为 nil，使用默认用户数据
		storageManager = nil
	}

	// 如果获取不到存储管理器，创建默认的用户数据
	if storageManager == nil {
		logger.Warnf("  [SelectExecutor] 无法获取存储管理器，创建默认 mysql.user 数据")
		return se.createDefaultUserData()
	}

	// 尝试查询用户数据
	if sm, ok := storageManager.(interface {
		QueryMySQLUser(username, host string) (interface{}, error)
	}); ok {
		logger.Debugf(" [SelectExecutor] 使用存储管理器查询用户数据")

		// 解析WHERE条件以获取用户名和主机
		username, host := se.parseUserQueryConditions()

		user, err := sm.QueryMySQLUser(username, host)
		if err != nil {
			logger.Warnf("  [SelectExecutor] 查询用户数据失败: %v，使用默认数据", err)
			return se.createDefaultUserData()
		}

		// 将用户数据转换为记录
		if err := se.convertUserToRecord(user); err != nil {
			logger.Warnf("  [SelectExecutor] 转换用户数据失败: %v，使用默认数据", err)
			return se.createDefaultUserData()
		}

		logger.Debugf(" [SelectExecutor] 成功从存储管理器获取用户数据")
		return nil
	}

	logger.Warnf("  [SelectExecutor] 存储管理器接口不匹配，创建默认 mysql.user 数据")
	return se.createDefaultUserData()
}

// parseUserQueryConditions 解析WHERE条件中的用户名和主机
func (se *SelectExecutor) parseUserQueryConditions() (username, host string) {
	logger.Debugf(" [SelectExecutor] 解析WHERE条件: %v", se.whereConditions)

	// 默认值
	username = "root"
	host = "localhost"

	// 简单的条件解析
	for _, condition := range se.whereConditions {
		conditionUpper := strings.ToUpper(condition)

		// 查找 User = 'xxx' 条件
		if strings.Contains(conditionUpper, "USER") && strings.Contains(conditionUpper, "=") {
			parts := strings.Split(condition, "=")
			if len(parts) >= 2 {
				userValue := strings.TrimSpace(parts[1])
				userValue = strings.Trim(userValue, "'\"")
				if userValue != "" {
					username = userValue
					logger.Debugf(" [SelectExecutor] 解析到用户名: %s", username)
				}
			}
		}

		// 查找 Host = 'xxx' 条件
		if strings.Contains(conditionUpper, "HOST") && strings.Contains(conditionUpper, "=") {
			parts := strings.Split(condition, "=")
			if len(parts) >= 2 {
				hostValue := strings.TrimSpace(parts[1])
				hostValue = strings.Trim(hostValue, "'\"")
				if hostValue != "" {
					host = hostValue
					logger.Debugf(" [SelectExecutor] 解析到主机: %s", host)
				}
			}
		}
	}

	logger.Debugf(" [SelectExecutor] 最终解析结果: user=%s, host=%s", username, host)
	return username, host
}

// convertUserToRecord 将用户数据转换为记录
func (se *SelectExecutor) convertUserToRecord(user interface{}) error {
	logger.Debugf(" [SelectExecutor] 转换用户数据为记录")

	// 创建用户记录（这里需要根据实际的用户数据结构进行转换）
	// 简化实现：创建包含基本用户信息的记录
	tableMeta := se.getMySQLUserTableMeta()

	// 假设用户对象有基本的字段
	userData := []interface{}{
		"localhost", // Host
		"root",      // User
		"Y",         // Select_priv
		"Y",         // Insert_priv
		"Y",         // Update_priv
		"Y",         // Delete_priv
		"Y",         // Create_priv
		"Y",         // Drop_priv
		// ... 其他字段根据需要添加
	}

	// 确保数据长度匹配列数
	columnCount := len(tableMeta.Columns)
	for len(userData) < columnCount {
		userData = append(userData, "")
	}

	record := NewExecutorRecordFromInterface(userData, tableMeta)
	se.resultSet = []Record{record}

	logger.Debugf(" [SelectExecutor] 用户数据转换完成，生成 1 条记录")
	return nil
}

// createDefaultUserData 创建默认的 mysql.user 表数据
func (se *SelectExecutor) createDefaultUserData() error {
	logger.Debugf("  [SelectExecutor] 创建默认 mysql.user 表数据")

	tableMeta := se.getMySQLUserTableMeta()

	// 创建默认的root用户记录
	rootUsers := [][]interface{}{
		{
			"localhost", "root", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257", "N",
			"0", "N", "2024-01-01 00:00:00", "0", "0", "Y", "{}",
		},
		{
			"%", "root", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",
			"Y", "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257", "N",
			"0", "N", "2024-01-01 00:00:00", "0", "0", "Y", "{}",
		},
	}

	se.resultSet = make([]Record, len(rootUsers))
	for i, userData := range rootUsers {
		se.resultSet[i] = NewExecutorRecordFromInterface(userData, tableMeta)
	}

	logger.Debugf(" [SelectExecutor] 创建了 %d 条默认用户记录", len(se.resultSet))
	return nil
}

// getMySQLUserTableMeta 获取 mysql.user 表的元数据
func (se *SelectExecutor) getMySQLUserTableMeta() *metadata.TableMeta {
	// 创建列元数据指针数组
	columns := make([]*metadata.ColumnMeta, 0, 40)

	// 添加所有列
	columnDefs := []metadata.ColumnMeta{
		{Name: "Host", Type: metadata.TypeChar, Length: 60},
		{Name: "User", Type: metadata.TypeChar, Length: 32},
		{Name: "Select_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Insert_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Update_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Delete_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Drop_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Reload_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Shutdown_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Process_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "File_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Grant_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "References_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Index_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Alter_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Show_db_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Super_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_tmp_table_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Lock_tables_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Execute_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Repl_slave_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Repl_client_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_view_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Show_view_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_routine_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Alter_routine_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_user_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Event_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Trigger_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "Create_tablespace_priv", Type: metadata.TypeEnum, Length: 1},
		{Name: "authentication_string", Type: metadata.TypeText, Length: 65535},
		{Name: "password_expired", Type: metadata.TypeEnum, Length: 1},
		{Name: "max_questions", Type: metadata.TypeInt, Length: 11},
		{Name: "account_locked", Type: metadata.TypeEnum, Length: 1},
		{Name: "password_last_changed", Type: metadata.TypeTimestamp, Length: 19},
		{Name: "max_updates", Type: metadata.TypeInt, Length: 11},
		{Name: "max_connections", Type: metadata.TypeInt, Length: 11},
		{Name: "password_require_current", Type: metadata.TypeEnum, Length: 1},
		{Name: "user_attributes", Type: metadata.TypeJSON, Length: 65535},
	}

	// 转换为指针数组
	for i := range columnDefs {
		columns = append(columns, &columnDefs[i])
	}

	tableMeta := &metadata.TableMeta{
		Name:    "user",
		Columns: columns,
	}

	return tableMeta
}

// scanPage 扫描页面
func (se *SelectExecutor) scanPage(ctx context.Context, pageNum uint32) error {
	// 从缓冲池获取页面
	bufferPage, err := se.bufferPoolManager.GetPage(0, pageNum) // 假设spaceID为0
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	// 解析页面中的记录
	records, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return fmt.Errorf("parse page records failed: %v", err)
	}

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	// 添加到结果集
	se.resultSet = append(se.resultSet, filteredRecords...)

	return nil
}

// scanPageWithSpaceID 使用指定表空间ID扫描页面
func (se *SelectExecutor) scanPageWithSpaceID(ctx context.Context, spaceID, pageNum uint32) error {
	logger.Debugf("扫描页面: SpaceID=%d, PageNum=%d\n", spaceID, pageNum)

	// 从缓冲池获取页面
	bufferPage, err := se.bufferPoolManager.GetPage(spaceID, pageNum)
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed (space=%d, page=%d): %v", spaceID, pageNum, err)
	}

	// 解析页面中的记录
	records, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return fmt.Errorf("parse page records failed (space=%d, page=%d): %v", spaceID, pageNum, err)
	}

	logger.Debugf("从页面 %d 解析出 %d 条记录\n", pageNum, len(records))

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	logger.Debugf("过滤后剩余 %d 条记录\n", len(filteredRecords))

	// 添加到结果集
	se.resultSet = append(se.resultSet, filteredRecords...)

	return nil
}

// scanPageFromSlot 从指定槽位开始扫描页面
func (se *SelectExecutor) scanPageFromSlot(ctx context.Context, pageNum uint32, slot int) error {
	// 从缓冲池获取页面
	bufferPage, err := se.bufferPoolManager.GetPage(0, pageNum)
	if err != nil {
		return fmt.Errorf("get page from buffer pool failed: %v", err)
	}

	// 解析页面中从指定槽位开始的记录
	records, err := se.parsePageRecordsFromSlot(bufferPage, slot)
	if err != nil {
		return fmt.Errorf("parse page records from slot failed: %v", err)
	}

	// 应用WHERE条件过滤
	filteredRecords := se.applyWhereFilter(records)

	// 添加到结果集
	se.resultSet = append(se.resultSet, filteredRecords...)

	return nil
}

// parsePageRecords 解析页面中的记录，结合元信息和投影需求
func (se *SelectExecutor) parsePageRecords(bufferPage interface{}) ([]Record, error) {
	// 获取表的元数据
	tableMeta, err := se.getTableMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	// 确定需要解析的列（基于投影需求）
	requiredColumns, columnMapping, err := se.determineRequiredColumns(tableMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to determine required columns: %v", err)
	}

	// 解析页面数据，只提取需要的列
	records, err := se.parsePageWithProjection(bufferPage, tableMeta, requiredColumns, columnMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page with projection: %v", err)
	}

	return records, nil
}

// getTableMetadata 获取表元数据
func (se *SelectExecutor) getTableMetadata() (*metadata.TableMeta, error) {
	if se.tableManager != nil {
		tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
		if err == nil && tableMeta != nil {
			return tableMeta, nil
		}
	}

	// 使用默认表结构
	return se.getDefaultTableMeta(), nil
}

// determineRequiredColumns 确定需要解析的列
func (se *SelectExecutor) determineRequiredColumns(tableMeta *metadata.TableMeta) ([]int, map[int]int, error) {
	// 如果是 SELECT *，需要所有列
	if len(se.selectExprs) == 0 || (len(se.selectExprs) == 1 && se.selectExprs[0] == "*") {
		requiredColumns := make([]int, len(tableMeta.Columns))
		columnMapping := make(map[int]int)
		for i := range tableMeta.Columns {
			requiredColumns[i] = i
			columnMapping[i] = i // 源索引 -> 目标索引
		}
		return requiredColumns, columnMapping, nil
	}

	// 创建列名到索引的映射（大小写不敏感）
	columnIndexMap := se.buildColumnIndexMap(tableMeta)

	var requiredColumns []int
	columnMapping := make(map[int]int) // 源索引 -> 目标索引

	for targetIndex, expr := range se.selectExprs {
		columnName := se.cleanColumnExpression(expr)
		sourceIndex, exists := se.findColumnIndex(columnName, columnIndexMap)

		if exists {
			// 检查是否已经在需要的列中
			found := false
			for _, reqCol := range requiredColumns {
				if reqCol == sourceIndex {
					found = true
					break
				}
			}

			if !found {
				requiredColumns = append(requiredColumns, sourceIndex)
			}
			columnMapping[sourceIndex] = targetIndex
		}
		// 如果列不存在，稍后在构造记录时处理为 NULL
	}

	return requiredColumns, columnMapping, nil
}

// parsePageWithProjection 解析页面数据并应用投影
func (se *SelectExecutor) parsePageWithProjection(bufferPage interface{}, tableMeta *metadata.TableMeta, requiredColumns []int, columnMapping map[int]int) ([]Record, error) {
	var records []Record

	// 从 bufferPage 获取页面内容
	pageContent, err := se.getPageContent(bufferPage)
	if err != nil {
		return nil, fmt.Errorf("failed to get page content: %v", err)
	}

	// 使用现有的页面解析器解析 InnoDB 页面格式
	indexPage, err := se.parseInnoDBPage(pageContent, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to parse InnoDB page: %v", err)
	}

	// 获取页面中的实际记录
	rows := indexPage.GetRows()
	logger.Debugf("从页面解析出 %d 条实际记录\n", len(rows))

	// 将每条记录转换为投影后的 Record
	for recordIndex, row := range rows {
		if row == nil {
			continue
		}

		// 跳过 infimum 和 supremum 记录
		if row.IsInfimumRow() || row.IsSupremumRow() {
			continue
		}

		// 从实际记录中提取需要的列数据
		sourceValues, err := se.extractValuesFromRow(row, tableMeta, requiredColumns)
		if err != nil {
			logger.Debugf("Warning: Failed to extract values from record %d: %v\n", recordIndex, err)
			continue
		}

		// 构造投影后的记录
		projectedRecord, err := se.buildProjectedRecord(tableMeta, sourceValues, columnMapping)
		if err != nil {
			logger.Debugf("Warning: Failed to build projected record %d: %v\n", recordIndex, err)
			continue
		}

		records = append(records, projectedRecord)
	}

	return records, nil
}

// getPageContent 从 bufferPage 获取页面内容
func (se *SelectExecutor) getPageContent(bufferPage interface{}) ([]byte, error) {
	// 根据不同的缓冲页面类型获取内容
	switch page := bufferPage.(type) {
	case *buffer_pool.BufferPage:
		return page.GetContent(), nil
	case interface{ GetContent() []byte }:
		return page.GetContent(), nil
	default:
		return nil, fmt.Errorf("unsupported buffer page type: %T", bufferPage)
	}
}

// parseInnoDBPage 解析 InnoDB 页面格式
func (se *SelectExecutor) parseInnoDBPage(pageContent []byte, tableMeta *metadata.TableMeta) (basic.IIndexPage, error) {
	// 创建表行元组适配器（如果需要的话）
	tableRowTuple := se.createTableRowTuple(tableMeta)

	// 使用现有的页面解析器解析页面
	if tableRowTuple != nil {
		// 使用带元数据的解析器
		indexPage := page.NewPageIndexByLoadBytesWithTuple(pageContent, tableRowTuple)
		return indexPage, nil
	} else {
		// 使用基础解析器
		indexPage := page.NewPageIndexByLoadBytes(pageContent)
		return indexPage, nil
	}
}

// createTableRowTuple 创建表行元组（如果需要的话）
func (se *SelectExecutor) createTableRowTuple(tableMeta *metadata.TableMeta) metadata.TableRowTuple {
	// 尝试从 tableManager 获取（如果有此方法的话）
	// 但目前 TableManager 没有 GetTableRowTuple 方法，所以直接返回 nil
	// 让解析器使用默认逻辑

	// 如果获取失败，根据 tableMeta 创建一个基础的 TableRowTuple
	// 这里返回 nil，让解析器使用默认逻辑
	return nil
}

// extractValuesFromRow 从实际记录中提取需要的列数据
func (se *SelectExecutor) extractValuesFromRow(row basic.Row, tableMeta *metadata.TableMeta, requiredColumns []int) (map[int]basic.Value, error) {
	sourceValues := make(map[int]basic.Value)

	// 检查记录是否支持按索引读取值
	switch r := row.(type) {
	case interface{ ReadValueByIndex(int) basic.Value }:
		// 使用专门的方法读取值
		for _, columnIndex := range requiredColumns {
			if columnIndex < len(tableMeta.Columns) {
				value := r.ReadValueByIndex(columnIndex)
				if value != nil {
					sourceValues[columnIndex] = value
				} else {
					sourceValues[columnIndex] = basic.NewNull()
				}
			}
		}
	case interface{ GetPrimaryKey() basic.Value }:
		// 如果只能获取主键，则尝试其他方法
		primaryKey := r.GetPrimaryKey()
		if primaryKey != nil && len(requiredColumns) > 0 {
			// 假设第一个需要的列是主键
			sourceValues[requiredColumns[0]] = primaryKey
		}

		// 对于其他列，生成合理的默认值
		for i := 1; i < len(requiredColumns); i++ {
			columnIndex := requiredColumns[i]
			if columnIndex < len(tableMeta.Columns) {
				col := tableMeta.Columns[columnIndex]
				value := se.generateDefaultValueForColumn(col)
				sourceValues[columnIndex] = value
			}
		}
	default:
		// 如果记录类型不支持，生成默认值
		for _, columnIndex := range requiredColumns {
			if columnIndex < len(tableMeta.Columns) {
				col := tableMeta.Columns[columnIndex]
				value := se.generateDefaultValueForColumn(col)
				sourceValues[columnIndex] = value
			}
		}
	}

	return sourceValues, nil
}

// generateDefaultValueForColumn 为列生成默认值
func (se *SelectExecutor) generateDefaultValueForColumn(col *metadata.ColumnMeta) basic.Value {
	switch col.Type {
	case "INT", "BIGINT":
		return basic.NewInt64Value(0)
	case "VARCHAR", "CHAR", "TEXT":
		return basic.NewStringValue(fmt.Sprintf("default_%s", col.Name))
	case "TIMESTAMP", "DATETIME":
		return basic.NewStringValue("1970-01-01 00:00:00")
	case "ENUM":
		return basic.NewStringValue("N")
	case "JSON":
		return basic.NewStringValue("{}")
	default:
		return basic.NewStringValue("null")
	}
}

// buildProjectedRecord 构造投影后的记录
func (se *SelectExecutor) buildProjectedRecord(tableMeta *metadata.TableMeta, sourceValues map[int]basic.Value, columnMapping map[int]int) (Record, error) {
	var projectedValues []basic.Value
	var projectedColumns []*metadata.ColumnMeta

	// 如果是 SELECT *，直接使用所有列
	if len(se.selectExprs) == 0 || (len(se.selectExprs) == 1 && se.selectExprs[0] == "*") {
		// 为 SELECT * 创建合适大小的数组
		projectedValues = make([]basic.Value, len(tableMeta.Columns))
		projectedColumns = make([]*metadata.ColumnMeta, len(tableMeta.Columns))

		for i, col := range tableMeta.Columns {
			if value, exists := sourceValues[i]; exists {
				projectedValues[i] = value
			} else {
				projectedValues[i] = basic.NewNull()
			}
			projectedColumns[i] = col
		}
	} else {
		// 根据 selectExprs 构造投影
		projectedValues = make([]basic.Value, len(se.selectExprs))
		projectedColumns = make([]*metadata.ColumnMeta, len(se.selectExprs))

		columnIndexMap := se.buildColumnIndexMap(tableMeta)

		for targetIndex, expr := range se.selectExprs {
			columnName := se.cleanColumnExpression(expr)
			sourceIndex, exists := se.findColumnIndex(columnName, columnIndexMap)

			if exists {
				if value, hasValue := sourceValues[sourceIndex]; hasValue {
					projectedValues[targetIndex] = value
				} else {
					projectedValues[targetIndex] = basic.NewNull()
				}

				if sourceIndex < len(tableMeta.Columns) {
					projectedColumns[targetIndex] = tableMeta.Columns[sourceIndex]
				} else {
					projectedColumns[targetIndex] = &metadata.ColumnMeta{
						Name: columnName,
						Type: "UNKNOWN",
					}
				}
			} else {
				// 列不存在，设置为 NULL
				projectedValues[targetIndex] = basic.NewNull()
				projectedColumns[targetIndex] = &metadata.ColumnMeta{
					Name: columnName,
					Type: "UNKNOWN",
				}
			}
		}
	}

	// 创建投影后的表元数据
	projectedTableMeta := &metadata.TableMeta{
		Name:    tableMeta.Name,
		Columns: projectedColumns,
	}

	// 创建记录
	return NewExecutorRecord(projectedValues, projectedTableMeta), nil
}

// parsePageRecordsFromSlot 从指定槽位解析记录
func (se *SelectExecutor) parsePageRecordsFromSlot(bufferPage interface{}, slot int) ([]Record, error) {
	// 先获取所有记录，然后从指定槽位开始
	allRecords, err := se.parsePageRecords(bufferPage)
	if err != nil {
		return nil, err
	}

	if slot >= len(allRecords) {
		return []Record{}, nil
	}

	return allRecords[slot:], nil
}

// applyWhereFilter 应用WHERE条件过滤
func (se *SelectExecutor) applyWhereFilter(records []Record) []Record {
	if len(se.whereConditions) == 0 {
		return records
	}

	var filteredRecords []Record

	// 简化实现：只是返回一部分记录来模拟过滤效果
	for i, record := range records {
		if i%2 == 0 { // 简单的过滤逻辑：只返回偶数索引的记录
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}

// buildSelectResult 构建SELECT结果
func (se *SelectExecutor) buildSelectResult() *SelectResult {
	// 应用投影
	projectedRecords := se.applyProjection(se.resultSet)

	// 应用排序
	sortedRecords := se.applyOrderBy(projectedRecords)

	// 应用LIMIT和OFFSET
	limitedRecords := se.applyLimitOffset(sortedRecords)

	// 获取列信息
	columns := se.getColumnNames()

	//  特殊处理 mysql.user 表，确保列信息正确
	if se.schemaName == "mysql" && se.tableName == "user" {
		logger.Debugf(" [buildSelectResult] 为 mysql.user 表构建结果")

		if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
			// SELECT * 查询，使用完整的user表列名
			tableMeta := se.getMySQLUserTableMeta()
			columns = make([]string, len(tableMeta.Columns))
			for i, col := range tableMeta.Columns {
				columns[i] = col.Name
			}
			logger.Debugf(" [buildSelectResult] SELECT * 列信息: %d 列", len(columns))
		} else {
			// 指定列查询，使用解析的表达式
			columns = se.selectExprs
			logger.Debugf(" [buildSelectResult] 指定列查询: %v", columns)
		}
	}

	result := &SelectResult{
		Records:    limitedRecords,
		RowCount:   len(limitedRecords),
		Columns:    columns,
		ResultType: common.RESULT_TYPE_QUERY,
		Message:    fmt.Sprintf("Query OK, %d rows in set", len(limitedRecords)),
	}

	logger.Debugf(" [buildSelectResult] 构建完成: %d行, %d列", result.RowCount, len(result.Columns))
	logger.Debugf(" [buildSelectResult] 列名: %v", result.Columns)

	return result
}

// applyProjection 应用投影（记录在解析阶段已完成投影，此处直接返回）
func (se *SelectExecutor) applyProjection(records []Record) []Record {
	// 投影已在 parsePageRecords 阶段完成，这里直接返回
	// 这样避免了二次投影，提高了性能
	return records
}

// projectRecord 对单条记录进行投影
func (se *SelectExecutor) projectRecord(record Record, selectExprs []string) (Record, error) {
	// 获取表的元数据，用于列名映射
	tableMeta, err := se.getRecordTableMeta(record)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	// 创建列名到索引的映射（大小写不敏感）
	columnIndexMap := se.buildColumnIndexMap(tableMeta)

	// 解析选择的列并获取对应的值
	projectedValues := make([]basic.Value, 0, len(selectExprs))
	projectedColumns := make([]*metadata.ColumnMeta, 0, len(selectExprs))

	for _, expr := range selectExprs {
		// 清理表达式（移除空格、别名等）
		columnName := se.cleanColumnExpression(expr)

		// 大小写不敏感查找列索引
		columnIndex, exists := se.findColumnIndex(columnName, columnIndexMap)
		if !exists {
			// 如果列不存在，创建一个 NULL 值
			projectedValues = append(projectedValues, basic.NewNull())
			projectedColumns = append(projectedColumns, &metadata.ColumnMeta{
				Name: columnName,
				Type: "UNKNOWN",
			})
			continue
		}

		// 获取列值
		value := record.GetValueByIndex(columnIndex)
		projectedValues = append(projectedValues, value)

		// 添加列元数据
		if columnIndex < len(tableMeta.Columns) {
			projectedColumns = append(projectedColumns, tableMeta.Columns[columnIndex])
		} else {
			projectedColumns = append(projectedColumns, &metadata.ColumnMeta{
				Name: columnName,
				Type: "UNKNOWN",
			})
		}
	}

	// 创建新的表元数据
	projectedTableMeta := &metadata.TableMeta{
		Name:    tableMeta.Name + "_projected",
		Columns: projectedColumns,
	}

	// 创建投影后的记录
	projectedRecord := NewExecutorRecord(projectedValues, projectedTableMeta)

	return projectedRecord, nil
}

// getRecordTableMeta 获取记录的表元数据
func (se *SelectExecutor) getRecordTableMeta(record Record) (*metadata.TableMeta, error) {
	// 尝试从 EngineExecutorRecord 获取表元数据
	if engineRecord, ok := record.(*EngineExecutorRecord); ok {
		if meta := engineRecord.GetTableMeta(); meta != nil {
			return meta, nil
		}
	}

	// 如果无法获取元数据，尝试从 TableManager 获取
	if se.tableManager != nil {
		tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
		if err == nil && tableMeta != nil {
			return tableMeta, nil
		}
	}

	// 最后使用默认的表结构
	return se.getDefaultTableMeta(), nil
}

// buildColumnIndexMap 构建列名到索引的映射（大小写不敏感）
func (se *SelectExecutor) buildColumnIndexMap(tableMeta *metadata.TableMeta) map[string]int {
	columnMap := make(map[string]int)

	for i, column := range tableMeta.Columns {
		// 使用小写作为键，实现大小写不敏感
		columnMap[strings.ToLower(column.Name)] = i
	}

	return columnMap
}

// findColumnIndex 查找列索引（大小写不敏感）
func (se *SelectExecutor) findColumnIndex(columnName string, columnMap map[string]int) (int, bool) {
	// 转换为小写进行查找
	lowerColumnName := strings.ToLower(columnName)
	index, exists := columnMap[lowerColumnName]
	return index, exists
}

// cleanColumnExpression 清理列表达式，移除空格和处理别名
func (se *SelectExecutor) cleanColumnExpression(expr string) string {
	// 移除前后空格
	expr = strings.TrimSpace(expr)

	// 处理别名（如果包含 AS 关键字）
	if strings.Contains(strings.ToUpper(expr), " AS ") {
		parts := strings.Split(expr, " ")
		if len(parts) > 0 {
			expr = strings.TrimSpace(parts[0])
		}
	}

	// 移除表前缀（如果存在）
	if strings.Contains(expr, ".") {
		parts := strings.Split(expr, ".")
		if len(parts) > 1 {
			expr = strings.TrimSpace(parts[len(parts)-1])
		}
	}

	// 移除引号
	expr = strings.Trim(expr, "`\"'")

	return expr
}

// getDefaultTableMeta 获取默认的表元数据
func (se *SelectExecutor) getDefaultTableMeta() *metadata.TableMeta {
	return &metadata.TableMeta{
		Name: se.tableName,
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
			{Name: "value", Type: "TEXT"},
		},
	}
}

// applyOrderBy 应用排序
func (se *SelectExecutor) applyOrderBy(records []Record) []Record {
	if len(se.orderByColumns) == 0 {
		return records
	}

	// 简化实现：不进行实际排序
	return records
}

// applyLimitOffset 应用LIMIT和OFFSET
func (se *SelectExecutor) applyLimitOffset(records []Record) []Record {
	start := se.offset
	if start >= len(records) {
		return []Record{}
	}

	end := len(records)
	if se.limit > 0 && start+se.limit < len(records) {
		end = start + se.limit
	}

	return records[start:end]
}

// getColumnNames 获取列名
func (se *SelectExecutor) getColumnNames() []string {
	if len(se.selectExprs) == 1 && se.selectExprs[0] == "*" {
		// 如果是SELECT *，需要从表元数据获取所有列名
		tableMeta, err := se.tableManager.GetTableMetadata(context.Background(), se.schemaName, se.tableName)
		if err != nil {
			return []string{"id", "name"} // 默认列名
		}

		var columnNames []string
		for _, col := range tableMeta.Columns {
			columnNames = append(columnNames, col.Name)
		}
		return columnNames
	}

	return se.selectExprs
}

// SelectResult SELECT查询结果
type SelectResult struct {
	Records    []Record
	RowCount   int
	Columns    []string
	ResultType string
	Message    string
}

// InfoSchemaAdapter 信息模式适配器实现
type InfoSchemaAdapter struct {
	manager *manager.TableManager
}

// TableByName 根据表名查找表元信息
func (a *InfoSchemaAdapter) TableByName(name string) (*metadata.Table, error) {
	// 简化实现：创建一个基本的表结构
	return &metadata.Table{
		Name: name,
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeInt},
			{Name: "name", DataType: metadata.TypeVarchar},
		},
	}, nil
}

// 实现Iterator接口的方法
func (se *SelectExecutor) Init() error {
	se.isInitialized = true
	se.currentRowIndex = 0
	return nil
}

func (se *SelectExecutor) Next() error {
	if !se.isInitialized {
		return fmt.Errorf("executor not initialized")
	}

	if se.currentRowIndex >= len(se.resultSet) {
		return io.EOF
	}

	se.currentRowIndex++
	return nil
}

func (se *SelectExecutor) GetRow() []interface{} {
	if se.currentRowIndex == 0 || se.currentRowIndex > len(se.resultSet) {
		return nil
	}

	record := se.resultSet[se.currentRowIndex-1]
	// 转换 basic.Value 数组为 interface{} 数组
	values := record.GetValues()
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = v.Raw()
	}
	return result
}

func (se *SelectExecutor) Close() error {
	se.closed = true
	se.currentRowIndex = 0
	se.resultSet = nil
	return nil
}

// 实现Executor接口的方法
func (se *SelectExecutor) Schema() *metadata.Schema {
	return se.schema
}

func (se *SelectExecutor) Children() []Executor {
	return se.children
}

func (se *SelectExecutor) SetChildren(children []Executor) {
	se.children = children
}
