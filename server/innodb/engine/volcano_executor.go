package engine

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// Note: Record is defined in executor_record.go as: type Record = basic.ExecutorRecord

// Operator 火山模型算子接口
// 每个算子实现 Open-Next-Close 的标准迭代器模式
type Operator interface {
	// Open 初始化算子，分配资源
	Open(ctx context.Context) error
	// Next 获取下一条记录，返回nil表示EOF
	Next(ctx context.Context) (Record, error)
	// Close 关闭算子并释放资源
	Close() error
	// Schema 返回输出的schema信息
	Schema() *metadata.QuerySchema
}

// BaseOperator 基础算子实现，提供公共功能
type BaseOperator struct {
	children []Operator
	schema   *metadata.QuerySchema
	opened   bool
	closed   bool
}

func (b *BaseOperator) Open(ctx context.Context) error {
	if b.opened {
		return fmt.Errorf("operator already opened")
	}
	for _, child := range b.children {
		if err := child.Open(ctx); err != nil {
			return fmt.Errorf("failed to open child operator: %w", err)
		}
	}
	b.opened = true
	return nil
}

func (b *BaseOperator) Close() error {
	if b.closed {
		return nil
	}
	for _, child := range b.children {
		if err := child.Close(); err != nil {
			logger.Warnf("Failed to close child operator: %v", err)
		}
	}
	b.closed = true
	return nil
}

func (b *BaseOperator) Schema() *metadata.QuerySchema {
	return b.schema
}

// ========================================
// 辅助类型和函数
// ========================================

// SimpleExecutorRecord 简单的ExecutorRecord实现
type SimpleExecutorRecord struct {
	values []basic.Value
	schema *metadata.QuerySchema
}

func NewExecutorRecordFromValues(values []basic.Value, schema *metadata.QuerySchema) Record {
	return &SimpleExecutorRecord{
		values: values,
		schema: schema,
	}
}

func (s *SimpleExecutorRecord) GetValues() []basic.Value {
	return s.values
}

func (s *SimpleExecutorRecord) SetValues(values []basic.Value) {
	s.values = values
}

func (s *SimpleExecutorRecord) GetSchema() *metadata.QuerySchema {
	return s.schema
}

func (s *SimpleExecutorRecord) GetColumnCount() int {
	return len(s.values)
}

func (s *SimpleExecutorRecord) GetValueByIndex(index int) basic.Value {
	if index < 0 || index >= len(s.values) {
		return nil
	}
	return s.values[index]
}

func (s *SimpleExecutorRecord) GetValueByName(name string) (basic.Value, error) {
	// Simple implementation - just return nil for now
	// TODO: Implement proper column name lookup
	return nil, nil
}

func (s *SimpleExecutorRecord) SetValueByIndex(index int, value basic.Value) error {
	if index < 0 || index >= len(s.values) {
		return fmt.Errorf("index out of range: %d", index)
	}
	s.values[index] = value
	return nil
}

func (s *SimpleExecutorRecord) SetValueByName(name string, value basic.Value) error {
	// Simple implementation - just return error for now
	// TODO: Implement proper column name lookup
	return fmt.Errorf("SetValueByName not implemented")
}

// ========================================
// TableScanOperator - 表扫描算子
// ========================================

// TableScanOperator 全表扫描算子，顺序读取表中所有记录
type TableScanOperator struct {
	BaseOperator
	schemaName string
	tableName  string

	// 存储适配器
	storageAdapter *StorageAdapter

	// 扫描状态
	iterator   *TablePageIterator
	currentRow Record
}

func NewTableScanOperator(
	schemaName, tableName string,
	storageAdapter *StorageAdapter,
) *TableScanOperator {
	return &TableScanOperator{
		BaseOperator: BaseOperator{
			children: nil,
			schema:   nil, // 将在Open时设置
		},
		schemaName:     schemaName,
		tableName:      tableName,
		storageAdapter: storageAdapter,
	}
}

func (t *TableScanOperator) Open(ctx context.Context) error {
	if err := t.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 获取表的元数据
	tableMeta, err := t.storageAdapter.GetTableMetadata(ctx, t.schemaName, t.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 从Table创建QuerySchema
	t.schema = metadata.FromTable(tableMeta.Schema)

	// 创建表页面迭代器
	t.iterator, err = t.storageAdapter.ScanTable(ctx, tableMeta)
	if err != nil {
		return fmt.Errorf("failed to create table iterator: %w", err)
	}

	logger.Debugf("TableScanOperator opened for table %s.%s, spaceID=%d",
		t.schemaName, t.tableName, tableMeta.SpaceID)
	return nil
}

func (t *TableScanOperator) Next(ctx context.Context) (Record, error) {
	if !t.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 从迭代器获取下一条记录
	record, err := t.iterator.Next()
	if err != nil {
		return nil, err
	}

	// nil表示EOF
	return record, nil
}

// ========================================
// IndexScanOperator - 索引扫描算子
// ========================================

// IndexScanOperator 索引扫描算子，使用索引快速定位记录
type IndexScanOperator struct {
	BaseOperator
	schemaName string
	tableName  string
	indexName  string

	// 适配器
	storageAdapter *StorageAdapter
	indexAdapter   *IndexAdapter

	// 扫描范围
	startKey basic.Value
	endKey   basic.Value

	// 查询需要的列
	requiredColumns []string
	// 是否覆盖索引（不需要回表）
	isCoveringIndex bool

	// 索引元数据
	indexMetadata *IndexMetadata

	// 主键列表（用于回表）
	primaryKeys [][]byte
	keyIndex    int
}

func NewIndexScanOperator(
	schemaName, tableName, indexName string,
	storageAdapter *StorageAdapter,
	indexAdapter *IndexAdapter,
	startKey, endKey basic.Value,
	requiredColumns []string,
) *IndexScanOperator {
	return &IndexScanOperator{
		BaseOperator: BaseOperator{
			children: nil,
		},
		schemaName:      schemaName,
		tableName:       tableName,
		indexName:       indexName,
		storageAdapter:  storageAdapter,
		indexAdapter:    indexAdapter,
		startKey:        startKey,
		endKey:          endKey,
		requiredColumns: requiredColumns,
		isCoveringIndex: false,
		primaryKeys:     [][]byte{},
		keyIndex:        0,
	}
}

func (i *IndexScanOperator) Open(ctx context.Context) error {
	if err := i.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 获取表的schema信息
	tableMeta, err := i.storageAdapter.GetTableMetadata(ctx, i.schemaName, i.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}
	// 从Table创建QuerySchema
	i.schema = metadata.FromTable(tableMeta.Schema)

	// 获取索引元数据
	i.indexMetadata, err = i.indexAdapter.GetIndexMetadata(ctx, i.schemaName, i.tableName, i.indexName)
	if err != nil {
		return fmt.Errorf("failed to get index metadata: %w", err)
	}

	// 检查是否为覆盖索引
	i.isCoveringIndex = i.indexMetadata.IsCoveringIndex(i.requiredColumns)

	logger.Debugf("IndexScanOperator opened for index %s on table %s.%s, isCoveringIndex=%v",
		i.indexName, i.schemaName, i.tableName, i.isCoveringIndex)

	// 如果不是覆盖索引，需要预先扫描索引获取主键列表
	if !i.isCoveringIndex {
		if err := i.fetchPrimaryKeys(ctx); err != nil {
			return fmt.Errorf("failed to fetch primary keys: %w", err)
		}
	}

	return nil
}

func (i *IndexScanOperator) Next(ctx context.Context) (Record, error) {
	if !i.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 如果是覆盖索引，直接从索引返回数据，不需要回表
	if i.isCoveringIndex {
		return i.nextFromIndex(ctx)
	}

	// 非覆盖索引，需要回表获取完整记录
	return i.nextWithLookup(ctx)
}

// fetchPrimaryKeys 预先扫描索引获取所有主键（用于批量回表优化）
func (i *IndexScanOperator) fetchPrimaryKeys(ctx context.Context) error {
	// 将startKey和endKey转换为字节数组
	var startKeyBytes []byte
	var endKeyBytes []byte

	if i.startKey != nil {
		startKeyBytes = i.startKey.Bytes()
	} else {
		startKeyBytes = []byte{} // 空字节数组表示从最小值开始
	}

	if i.endKey != nil {
		endKeyBytes = i.endKey.Bytes()
	} else {
		endKeyBytes = []byte{0xFF, 0xFF, 0xFF, 0xFF} // 最大值
	}

	// 调用索引适配器进行范围扫描
	primaryKeys, err := i.indexAdapter.RangeScan(ctx, i.indexMetadata.IndexID, startKeyBytes, endKeyBytes)
	if err != nil {
		return fmt.Errorf("index range scan failed: %w", err)
	}

	i.primaryKeys = primaryKeys
	i.keyIndex = 0

	logger.Debugf("Fetched %d primary keys from index %s", len(primaryKeys), i.indexName)
	return nil
}

// nextFromIndex 从索引直接读取数据（覆盖索引）
func (i *IndexScanOperator) nextFromIndex(ctx context.Context) (Record, error) {
	logger.Debugf("nextFromIndex: using covering index %s", i.indexName)

	// 检查是否还有主键需要处理
	if i.keyIndex >= len(i.primaryKeys) {
		return nil, nil // EOF
	}

	// 获取当前索引键
	indexKey := i.primaryKeys[i.keyIndex]
	i.keyIndex++

	// 从索引直接读取记录（覆盖索引优化）
	indexRecordData, err := i.indexAdapter.ReadIndexRecord(ctx, i.indexMetadata.IndexID, indexKey)
	if err != nil {
		// 如果索引读取失败，记录日志但继续处理
		logger.Debugf("Failed to read index record: %v, using fallback", err)

		// 降级为回表查询
		return i.nextWithLookup(ctx)
	}

	// 解析索引记录数据为Record
	// 索引记录包含：索引列的值 + 主键值
	// 这里需要根据索引列定义解析索引记录数据

	// 简化实现：将索引记录数据按列解析
	values := make([]basic.Value, len(i.requiredColumns))

	// 如果有索引记录数据，尝试解析
	if len(indexRecordData) > 0 {
		// 简单实现：将数据平均分配给各列
		// 实际应该根据列类型和长度精确解析
		chunkSize := len(indexRecordData) / len(i.requiredColumns)
		if chunkSize == 0 {
			chunkSize = 1
		}

		for idx := range i.requiredColumns {
			start := idx * chunkSize
			end := start + chunkSize
			if end > len(indexRecordData) {
				end = len(indexRecordData)
			}

			// 将字节数据转换为字符串值
			if start < len(indexRecordData) {
				values[idx] = basic.NewString(string(indexRecordData[start:end]))
			} else {
				values[idx] = basic.NewString("")
			}
		}
	} else {
		// 如果没有数据，使用默认值
		for idx := range i.requiredColumns {
			values[idx] = basic.NewString(fmt.Sprintf("index_value_%d", idx))
		}
	}

	return NewExecutorRecordFromValues(values, i.schema), nil
}

// nextWithLookup 通过回表获取完整记录（非覆盖索引）
func (i *IndexScanOperator) nextWithLookup(ctx context.Context) (Record, error) {
	// 检查是否还有主键需要回表
	if i.keyIndex >= len(i.primaryKeys) {
		return nil, nil // EOF
	}

	// 获取当前主键
	primaryKey := i.primaryKeys[i.keyIndex]
	i.keyIndex++

	logger.Debugf("nextWithLookup: lookup primaryKey for index %s, key=%v", i.indexName, primaryKey)

	// 通过主键回表查找完整记录
	// 步骤：
	// 1. 使用primaryKey在聚簇索引（主键索引）中查找
	// 2. 读取完整记录
	// 3. 转换为Record并返回

	// 获取表的存储信息
	tableMeta, err := i.storageAdapter.GetTableMetadata(ctx, i.schemaName, i.tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata for lookup: %w", err)
	}

	// 使用StorageAdapter的GetRecordByPrimaryKey方法回表
	record, err := i.storageAdapter.GetRecordByPrimaryKey(ctx, tableMeta.SpaceID, primaryKey, tableMeta.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup record by primary key: %w", err)
	}

	// 如果需要投影（只返回部分列），进行列过滤
	if len(i.requiredColumns) > 0 && !i.isCoveringIndex {
		record = i.projectRecord(record, i.requiredColumns, tableMeta.Schema)
	}

	return record, nil
}

// projectRecord 对记录进行列投影，只保留需要的列
func (i *IndexScanOperator) projectRecord(record Record, requiredColumns []string, schema *metadata.Table) Record {
	// 如果没有指定列，返回原记录
	if len(requiredColumns) == 0 {
		return record
	}

	// 创建列名到索引的映射
	columnIndexMap := make(map[string]int)
	for idx, col := range schema.Columns {
		columnIndexMap[col.Name] = idx
	}

	// 提取需要的列
	projectedValues := make([]basic.Value, len(requiredColumns))
	for i, colName := range requiredColumns {
		if colIdx, exists := columnIndexMap[colName]; exists {
			// 从原记录中获取对应列的值
			projectedValues[i] = record.GetValueByIndex(colIdx)
		} else {
			// 如果列不存在，使用NULL值
			projectedValues[i] = basic.NewString("")
		}
	}

	return NewExecutorRecordFromValues(projectedValues, i.schema)
}

// fetchBatchFromIndex 批量从索引读取记录
// ========================================
// FilterOperator - 过滤算子
// ========================================

// FilterOperator 过滤算子，根据条件过滤记录
type FilterOperator struct {
	BaseOperator
	child     Operator
	predicate func(Record) bool
}

func NewFilterOperator(child Operator, predicate func(Record) bool) *FilterOperator {
	return &FilterOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child:     child,
		predicate: predicate,
	}
}

func (f *FilterOperator) Open(ctx context.Context) error {
	if err := f.BaseOperator.Open(ctx); err != nil {
		return err
	}
	f.schema = f.child.Schema()
	return nil
}

func (f *FilterOperator) Next(ctx context.Context) (Record, error) {
	if !f.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 循环查找满足条件的记录
	for {
		record, err := f.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		if record == nil {
			return nil, nil // EOF
		}

		// 应用过滤条件
		if f.predicate == nil || f.predicate(record) {
			return record, nil
		}
	}
}

// ========================================
// ProjectionOperator - 投影算子
// ========================================

// ProjectionOperator 投影算子，选择需要的列
type ProjectionOperator struct {
	BaseOperator
	child       Operator
	projections []int             // 投影列的索引
	exprs       []plan.Expression // 投影表达式（支持计算列）
}

func NewProjectionOperator(child Operator, projections []int) *ProjectionOperator {
	return &ProjectionOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child:       child,
		projections: projections,
	}
}

func NewProjectionOperatorWithExprs(child Operator, exprs []plan.Expression) *ProjectionOperator {
	return &ProjectionOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child: child,
		exprs: exprs,
	}
}

func (p *ProjectionOperator) Open(ctx context.Context) error {
	if err := p.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 从子算子获取schema并投影
	childSchema := p.child.Schema()
	if childSchema != nil {
		p.schema = metadata.ProjectSchema(childSchema, p.projections)
	} else {
		p.schema = metadata.NewQuerySchema()
	}

	return nil
}

func (p *ProjectionOperator) Next(ctx context.Context) (Record, error) {
	if !p.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	record, err := p.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil // EOF
	}

	// 如果有表达式，计算表达式
	if len(p.exprs) > 0 {
		newValues := make([]basic.Value, len(p.exprs))

		// 创建表达式求值上下文
		evalCtx, err := p.createEvalContext(record)
		if err != nil {
			return nil, fmt.Errorf("failed to create eval context: %w", err)
		}

		// 计算每个表达式
		for i, expr := range p.exprs {
			result, err := expr.Eval(evalCtx)
			if err != nil {
				logger.Debugf("Failed to evaluate expression %s: %v, using NULL", expr.String(), err)
				newValues[i] = basic.NewNull()
				continue
			}

			// 将结果转换为basic.Value
			newValues[i] = p.convertToValue(result)
		}

		return NewExecutorRecordFromValues(newValues, p.schema), nil
	}

	// 否则只保留投影列
	values := record.GetValues()
	newValues := make([]basic.Value, len(p.projections))
	for i, idx := range p.projections {
		if idx < len(values) {
			newValues[i] = values[idx]
		} else {
			newValues[i] = basic.NewNull()
		}
	}

	return NewExecutorRecordFromValues(newValues, p.schema), nil
}

// createEvalContext 创建表达式求值上下文
// 将Record转换为map[string]interface{}格式
func (p *ProjectionOperator) createEvalContext(record Record) (*plan.EvalContext, error) {
	// 获取子算子的schema
	childSchema := p.child.Schema()
	if childSchema == nil {
		return &plan.EvalContext{Row: make(map[string]interface{})}, nil
	}

	// 创建列名到值的映射
	row := make(map[string]interface{})
	values := record.GetValues()

	// 遍历schema中的列，建立列名到值的映射
	for i := 0; i < childSchema.ColumnCount() && i < len(values); i++ {
		col, ok := childSchema.GetColumnByIndex(i)
		if ok && col != nil {
			// 将basic.Value转换为interface{}
			row[col.Name] = p.valueToInterface(values[i])
		}
	}

	return &plan.EvalContext{Row: row}, nil
}

// valueToInterface 将basic.Value转换为interface{}
func (p *ProjectionOperator) valueToInterface(val basic.Value) interface{} {
	if val == nil || val.IsNull() {
		return nil
	}

	// 根据值的类型进行转换
	valueType := val.Type()
	switch valueType {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt,
		basic.ValueTypeInt, basic.ValueTypeBigInt:
		return val.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		return val.Float64()
	case basic.ValueTypeVarchar, basic.ValueTypeChar, basic.ValueTypeText:
		return val.String()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		return val.Bool()
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeBlob:
		return val.Bytes()
	case basic.ValueTypeDate, basic.ValueTypeTime, basic.ValueTypeDateTime, basic.ValueTypeTimestamp:
		return val.Time()
	default:
		// 默认使用Raw()方法
		return val.Raw()
	}
}

// convertToValue 将interface{}转换为basic.Value
func (p *ProjectionOperator) convertToValue(result interface{}) basic.Value {
	if result == nil {
		return basic.NewNull()
	}

	switch v := result.(type) {
	case int:
		return basic.NewInt64(int64(v))
	case int32:
		return basic.NewInt64(int64(v))
	case int64:
		return basic.NewInt64(v)
	case float32:
		return basic.NewFloat64(float64(v))
	case float64:
		return basic.NewFloat64(v)
	case string:
		return basic.NewString(v)
	case bool:
		return basic.NewBool(v)
	case []byte:
		return basic.NewBytes(v)
	case time.Time:
		return basic.NewTime(v)
	default:
		// 默认转换为字符串
		return basic.NewString(fmt.Sprintf("%v", v))
	}
}

// ========================================
// NestedLoopJoinOperator - 嵌套循环连接算子
// ========================================

// NestedLoopJoinOperator 嵌套循环连接，适用于小表连接
type NestedLoopJoinOperator struct {
	BaseOperator
	left      Operator
	right     Operator
	joinType  string // INNER, LEFT, RIGHT, FULL
	condition func(leftRow, rightRow Record) bool

	// 状态
	leftRow  Record
	rightEOF bool
}

func NewNestedLoopJoinOperator(
	left, right Operator,
	joinType string,
	condition func(leftRow, rightRow Record) bool,
) *NestedLoopJoinOperator {
	return &NestedLoopJoinOperator{
		BaseOperator: BaseOperator{
			children: []Operator{left, right},
		},
		left:      left,
		right:     right,
		joinType:  joinType,
		condition: condition,
	}
}

func (n *NestedLoopJoinOperator) Open(ctx context.Context) error {
	if err := n.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 合并左右子算子的schema
	leftSchema := n.left.Schema()
	rightSchema := n.right.Schema()
	if leftSchema != nil && rightSchema != nil {
		n.schema = metadata.MergeSchemas(leftSchema, rightSchema)
	} else if leftSchema != nil {
		n.schema = leftSchema.Clone()
	} else if rightSchema != nil {
		n.schema = rightSchema.Clone()
	} else {
		n.schema = metadata.NewQuerySchema()
	}

	return nil
}

func (n *NestedLoopJoinOperator) Next(ctx context.Context) (Record, error) {
	if !n.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	for {
		// 如果左表当前行为空，获取下一行
		if n.leftRow == nil {
			leftRow, err := n.left.Next(ctx)
			if err != nil {
				return nil, err
			}
			if leftRow == nil {
				return nil, nil // 左表遍历完，连接结束
			}
			n.leftRow = leftRow
			n.rightEOF = false
		}

		// 获取右表下一行
		rightRow, err := n.right.Next(ctx)
		if err != nil {
			return nil, err
		}

		// 右表遍历完，重置右表，获取左表下一行
		if rightRow == nil {
			n.rightEOF = true
			n.right.Close()
			if err := n.right.Open(ctx); err != nil {
				return nil, fmt.Errorf("failed to reopen right child: %w", err)
			}
			n.leftRow = nil
			continue
		}

		// 检查连接条件
		if n.condition == nil || n.condition(n.leftRow, rightRow) {
			// 合并左右记录
			return n.mergeRecords(n.leftRow, rightRow), nil
		}
	}
}

func (n *NestedLoopJoinOperator) mergeRecords(left, right Record) Record {
	leftValues := left.GetValues()
	rightValues := right.GetValues()

	mergedValues := make([]basic.Value, 0, len(leftValues)+len(rightValues))
	mergedValues = append(mergedValues, leftValues...)
	mergedValues = append(mergedValues, rightValues...)

	return NewExecutorRecordFromValues(mergedValues, n.schema)
}

// ========================================
// HashJoinOperator - 哈希连接算子
// ========================================

// HashJoinOperator 哈希连接，适用于大表连接
type HashJoinOperator struct {
	BaseOperator
	buildSide Operator
	probeSide Operator
	joinType  string

	// 哈希表构建
	buildKey  func(Record) string
	probeKey  func(Record) string
	hashTable map[string][]Record

	// 探测状态
	built       bool
	probeRow    Record
	matchedRows []Record
	matchedIdx  int
}

func NewHashJoinOperator(
	buildSide, probeSide Operator,
	joinType string,
	buildKey, probeKey func(Record) string,
) *HashJoinOperator {
	return &HashJoinOperator{
		BaseOperator: BaseOperator{
			children: []Operator{buildSide, probeSide},
		},
		buildSide: buildSide,
		probeSide: probeSide,
		joinType:  joinType,
		buildKey:  buildKey,
		probeKey:  probeKey,
		hashTable: make(map[string][]Record),
	}
}

func (h *HashJoinOperator) Open(ctx context.Context) error {
	if err := h.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 合并build和probe端的schema
	buildSchema := h.buildSide.Schema()
	probeSchema := h.probeSide.Schema()
	if buildSchema != nil && probeSchema != nil {
		h.schema = metadata.MergeSchemas(buildSchema, probeSchema)
	} else if buildSchema != nil {
		h.schema = buildSchema.Clone()
	} else if probeSchema != nil {
		h.schema = probeSchema.Clone()
	} else {
		h.schema = metadata.NewQuerySchema()
	}

	return nil
}

func (h *HashJoinOperator) Next(ctx context.Context) (Record, error) {
	if !h.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 第一次调用时构建哈希表
	if !h.built {
		if err := h.buildHashTable(ctx); err != nil {
			return nil, fmt.Errorf("failed to build hash table: %w", err)
		}
		h.built = true
		logger.Debugf("HashJoin: built hash table with %d keys", len(h.hashTable))
	}

	// 探测阶段
	for {
		// 如果当前探测行的匹配已经遍历完，获取下一个探测行
		if h.matchedIdx >= len(h.matchedRows) {
			probeRow, err := h.probeSide.Next(ctx)
			if err != nil {
				return nil, err
			}
			if probeRow == nil {
				return nil, nil // EOF
			}

			h.probeRow = probeRow
			key := h.probeKey(probeRow)
			h.matchedRows = h.hashTable[key]
			h.matchedIdx = 0

			// 如果没有匹配，继续下一个探测行
			if len(h.matchedRows) == 0 {
				continue
			}
		}

		// 返回匹配的记录
		buildRow := h.matchedRows[h.matchedIdx]
		h.matchedIdx++

		return h.mergeRecords(buildRow, h.probeRow), nil
	}
}

func (h *HashJoinOperator) buildHashTable(ctx context.Context) error {
	for {
		record, err := h.buildSide.Next(ctx)
		if err != nil {
			return err
		}
		if record == nil {
			break
		}

		key := h.buildKey(record)
		h.hashTable[key] = append(h.hashTable[key], record)
	}
	return nil
}

func (h *HashJoinOperator) mergeRecords(build, probe Record) Record {
	buildValues := build.GetValues()
	probeValues := probe.GetValues()

	mergedValues := make([]basic.Value, 0, len(buildValues)+len(probeValues))
	mergedValues = append(mergedValues, buildValues...)
	mergedValues = append(mergedValues, probeValues...)

	return NewExecutorRecordFromValues(mergedValues, h.schema)
}

// ========================================
// HashAggregateOperator - 哈希聚合算子
// ========================================

// AggregateFunc 聚合函数接口
type AggregateFunc interface {
	Init()
	Update(value basic.Value)
	Result() basic.Value
	Name() string                  // 聚合函数名称
	ResultType() metadata.DataType // 结果类型
}

// CountAgg COUNT聚合
type CountAgg struct {
	count int64
}

func (c *CountAgg) Init()                         { c.count = 0 }
func (c *CountAgg) Update(value basic.Value)      { c.count++ }
func (c *CountAgg) Result() basic.Value           { return basic.NewInt64Value(c.count) }
func (c *CountAgg) Name() string                  { return "COUNT" }
func (c *CountAgg) ResultType() metadata.DataType { return metadata.TypeBigInt }

// SumAgg SUM聚合
type SumAgg struct {
	sum float64
}

func (s *SumAgg) Init() { s.sum = 0 }
func (s *SumAgg) Update(value basic.Value) {
	if !value.IsNull() {
		s.sum += value.Float64()
	}
}
func (s *SumAgg) Result() basic.Value           { return basic.NewFloatValue(s.sum) }
func (s *SumAgg) Name() string                  { return "SUM" }
func (s *SumAgg) ResultType() metadata.DataType { return metadata.TypeDouble }

// AvgAgg AVG聚合
type AvgAgg struct {
	sum   float64
	count int64
}

func (a *AvgAgg) Init() {
	a.sum = 0
	a.count = 0
}

func (a *AvgAgg) Update(value basic.Value) {
	if !value.IsNull() {
		a.sum += value.Float64()
		a.count++
	}
}

func (a *AvgAgg) Result() basic.Value {
	if a.count == 0 {
		return basic.NewNull()
	}
	return basic.NewFloatValue(a.sum / float64(a.count))
}

func (a *AvgAgg) Name() string                  { return "AVG" }
func (a *AvgAgg) ResultType() metadata.DataType { return metadata.TypeDouble }

// MinAgg MIN聚合
type MinAgg struct {
	min         basic.Value
	initialized bool
}

func (m *MinAgg) Init() {
	m.initialized = false
	m.min = basic.NewNull()
}

func (m *MinAgg) Update(value basic.Value) {
	if value.IsNull() {
		return
	}
	if !m.initialized {
		m.min = value
		m.initialized = true
		return
	}
	// 比较大小
	if value.Float64() < m.min.Float64() {
		m.min = value
	}
}

func (m *MinAgg) Result() basic.Value {
	return m.min
}

func (m *MinAgg) Name() string                  { return "MIN" }
func (m *MinAgg) ResultType() metadata.DataType { return metadata.TypeDouble }

// MaxAgg MAX聚合
type MaxAgg struct {
	max         basic.Value
	initialized bool
}

func (m *MaxAgg) Init() {
	m.initialized = false
	m.max = basic.NewNull()
}

func (m *MaxAgg) Update(value basic.Value) {
	if value.IsNull() {
		return
	}
	if !m.initialized {
		m.max = value
		m.initialized = true
		return
	}
	// 比较大小
	if value.Float64() > m.max.Float64() {
		m.max = value
	}
}

func (m *MaxAgg) Result() basic.Value {
	return m.max
}

func (m *MaxAgg) Name() string                  { return "MAX" }
func (m *MaxAgg) ResultType() metadata.DataType { return metadata.TypeDouble }

// HashAggregateOperator 哈希聚合算子
type HashAggregateOperator struct {
	BaseOperator
	child        Operator
	groupByExprs []int // 分组列索引
	aggFuncs     []AggregateFunc

	// 聚合状态
	hashTable map[string][]AggregateFunc
	computed  bool
	results   []Record
	resultIdx int
}

func NewHashAggregateOperator(
	child Operator,
	groupByExprs []int,
	aggFuncs []AggregateFunc,
) *HashAggregateOperator {
	return &HashAggregateOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child:        child,
		groupByExprs: groupByExprs,
		aggFuncs:     aggFuncs,
		hashTable:    make(map[string][]AggregateFunc),
	}
}

func (h *HashAggregateOperator) Open(ctx context.Context) error {
	if err := h.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 构建聚合后的schema
	// 包含GROUP BY列和聚合函数列
	childSchema := h.child.Schema()
	h.schema = metadata.NewQuerySchema()

	if childSchema != nil {
		// 添加GROUP BY列
		for _, idx := range h.groupByExprs {
			if col, ok := childSchema.GetColumnByIndex(idx); ok {
				h.schema.AddColumn(&metadata.QueryColumn{
					Name:       col.Name,
					DataType:   col.DataType,
					IsNullable: col.IsNullable,
					TableName:  col.TableName,
					SchemaName: col.SchemaName,
				})
			}
		}

		// 添加聚合函数列
		for _, aggFunc := range h.aggFuncs {
			h.schema.AddColumn(&metadata.QueryColumn{
				Name:       aggFunc.Name(),
				DataType:   aggFunc.ResultType(),
				IsNullable: true,
			})
		}
	}

	return nil
}

func (h *HashAggregateOperator) Next(ctx context.Context) (Record, error) {
	if !h.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 惰性计算：第一次调用时才开始聚合
	if !h.computed {
		if err := h.computeAggregates(ctx); err != nil {
			return nil, fmt.Errorf("failed to compute aggregates: %w", err)
		}
		h.computed = true
	}

	// 返回下一个聚合结果
	if h.resultIdx >= len(h.results) {
		return nil, nil // EOF
	}

	result := h.results[h.resultIdx]
	h.resultIdx++
	return result, nil
}

func (h *HashAggregateOperator) computeAggregates(ctx context.Context) error {
	// 遍历所有输入行
	for {
		record, err := h.child.Next(ctx)
		if err != nil {
			return err
		}
		if record == nil {
			break
		}

		// 计算分组键
		groupKey := h.computeGroupKey(record)

		// 获取或创建聚合状态
		aggStates, exists := h.hashTable[groupKey]
		if !exists {
			aggStates = make([]AggregateFunc, len(h.aggFuncs))
			for i, fn := range h.aggFuncs {
				// 复制聚合函数
				switch fn.(type) {
				case *CountAgg:
					aggStates[i] = &CountAgg{}
				case *SumAgg:
					aggStates[i] = &SumAgg{}
				case *AvgAgg:
					aggStates[i] = &AvgAgg{}
				case *MinAgg:
					aggStates[i] = &MinAgg{}
				case *MaxAgg:
					aggStates[i] = &MaxAgg{}
				}
				aggStates[i].Init()
			}
			h.hashTable[groupKey] = aggStates
		}

		// 更新聚合状态
		values := record.GetValues()
		for i, aggState := range aggStates {
			if i < len(values) {
				aggState.Update(values[i])
			}
		}
	}

	// 生成结果
	h.results = make([]Record, 0, len(h.hashTable))
	for _, aggStates := range h.hashTable {
		resultValues := make([]basic.Value, len(aggStates))
		for i, aggState := range aggStates {
			resultValues[i] = aggState.Result()
		}
		h.results = append(h.results, NewExecutorRecordFromValues(resultValues, h.schema))
	}

	logger.Debugf("HashAggregate: computed %d groups", len(h.results))
	return nil
}

func (h *HashAggregateOperator) computeGroupKey(record Record) string {
	if len(h.groupByExprs) == 0 {
		return "" // 无分组，所有行聚合为一组
	}

	values := record.GetValues()
	key := ""
	for _, idx := range h.groupByExprs {
		if idx < len(values) {
			key += values[idx].ToString() + "|"
		}
	}
	return key
}

// ========================================
// SortOperator - 排序算子
// ========================================

// SortKey 排序键
type SortKey struct {
	ColumnIdx int
	Ascending bool
}

// SortOperator 排序算子
type SortOperator struct {
	BaseOperator
	child    Operator
	sortKeys []SortKey

	// 排序状态
	sorted    bool
	results   []Record
	resultIdx int
}

func NewSortOperator(child Operator, sortKeys []SortKey) *SortOperator {
	return &SortOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child:    child,
		sortKeys: sortKeys,
	}
}

func (s *SortOperator) Open(ctx context.Context) error {
	if err := s.BaseOperator.Open(ctx); err != nil {
		return err
	}
	s.schema = s.child.Schema()
	return nil
}

func (s *SortOperator) Next(ctx context.Context) (Record, error) {
	if !s.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 惰性排序：第一次调用时才开始排序
	if !s.sorted {
		if err := s.sortRecords(ctx); err != nil {
			return nil, fmt.Errorf("failed to sort records: %w", err)
		}
		s.sorted = true
	}

	// 返回下一个排序后的记录
	if s.resultIdx >= len(s.results) {
		return nil, nil // EOF
	}

	result := s.results[s.resultIdx]
	s.resultIdx++
	return result, nil
}

func (s *SortOperator) sortRecords(ctx context.Context) error {
	// 读取所有记录
	s.results = make([]Record, 0)
	for {
		record, err := s.child.Next(ctx)
		if err != nil {
			return err
		}
		if record == nil {
			break
		}
		s.results = append(s.results, record)
	}

	// 排序
	sort.Slice(s.results, func(i, j int) bool {
		return s.compareRecords(s.results[i], s.results[j]) < 0
	})

	logger.Debugf("Sort: sorted %d records", len(s.results))
	return nil
}

func (s *SortOperator) compareRecords(r1, r2 Record) int {
	v1 := r1.GetValues()
	v2 := r2.GetValues()

	for _, key := range s.sortKeys {
		if key.ColumnIdx >= len(v1) || key.ColumnIdx >= len(v2) {
			continue
		}

		cmp := s.compareValues(v1[key.ColumnIdx], v2[key.ColumnIdx])
		if cmp != 0 {
			if key.Ascending {
				return cmp
			}
			return -cmp
		}
	}
	return 0
}

func (s *SortOperator) compareValues(v1, v2 basic.Value) int {
	// NULL值处理
	if v1.IsNull() && v2.IsNull() {
		return 0
	}
	if v1.IsNull() {
		return -1
	}
	if v2.IsNull() {
		return 1
	}

	// 数值比较
	f1 := v1.Float64()
	f2 := v2.Float64()
	if f1 < f2 {
		return -1
	}
	if f1 > f2 {
		return 1
	}
	return 0
}

// ========================================
// LimitOperator - 限制算子
// ========================================

// LimitOperator 限制返回记录数量
type LimitOperator struct {
	BaseOperator
	child  Operator
	offset int64
	limit  int64

	// 状态
	currentRow int64
}

func NewLimitOperator(child Operator, offset, limit int64) *LimitOperator {
	return &LimitOperator{
		BaseOperator: BaseOperator{
			children: []Operator{child},
		},
		child:  child,
		offset: offset,
		limit:  limit,
	}
}

func (l *LimitOperator) Open(ctx context.Context) error {
	if err := l.BaseOperator.Open(ctx); err != nil {
		return err
	}
	l.schema = l.child.Schema()
	l.currentRow = 0
	return nil
}

func (l *LimitOperator) Next(ctx context.Context) (Record, error) {
	if !l.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 跳过offset行
	for l.currentRow < l.offset {
		record, err := l.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		if record == nil {
			return nil, nil // EOF
		}
		l.currentRow++
	}

	// 检查是否超过limit
	if l.limit > 0 && l.currentRow >= l.offset+l.limit {
		return nil, nil // EOF
	}

	// 返回下一条记录
	record, err := l.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	if record != nil {
		l.currentRow++
	}
	return record, nil
}

// ========================================
// SubqueryOperator - 子查询算子
// ========================================

// SubqueryOperator 子查询算子，执行子查询并返回结果
type SubqueryOperator struct {
	BaseOperator
	subqueryType string      // "SCALAR", "IN", "EXISTS", "ANY", "ALL"
	correlated   bool        // 是否为关联子查询
	outerRefs    []string    // 外部引用的列
	subplan      Operator    // 子查询的执行计划
	outerRow     Record      // 外层记录（用于关联子查询）
	result       interface{} // 子查询结果（标量子查询）
	resultSet    []Record    // 子查询结果集（IN/EXISTS子查询）
}

// NewSubqueryOperator 创建子查询算子
func NewSubqueryOperator(subqueryType string, correlated bool, outerRefs []string, subplan Operator) *SubqueryOperator {
	return &SubqueryOperator{
		subqueryType: subqueryType,
		correlated:   correlated,
		outerRefs:    outerRefs,
		subplan:      subplan,
	}
}

func (s *SubqueryOperator) Open(ctx context.Context) error {
	if err := s.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 如果是非关联子查询，可以在Open阶段执行
	if !s.correlated {
		return s.executeSubquery(ctx, nil)
	}

	return nil
}

func (s *SubqueryOperator) Next(ctx context.Context) (Record, error) {
	if !s.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 子查询算子通常不直接返回记录，而是作为表达式的一部分
	// 这里返回nil表示EOF
	return nil, nil
}

// ExecuteForRow 为指定的外层记录执行子查询（关联子查询）
func (s *SubqueryOperator) ExecuteForRow(ctx context.Context, outerRow Record) error {
	if !s.correlated {
		// 非关联子查询只需执行一次
		if s.result != nil || s.resultSet != nil {
			return nil
		}
	}

	return s.executeSubquery(ctx, outerRow)
}

// executeSubquery 执行子查询
func (s *SubqueryOperator) executeSubquery(ctx context.Context, outerRow Record) error {
	s.outerRow = outerRow

	// 打开子计划
	if err := s.subplan.Open(ctx); err != nil {
		return fmt.Errorf("failed to open subplan: %w", err)
	}
	defer s.subplan.Close()

	// 根据子查询类型执行
	switch s.subqueryType {
	case "SCALAR":
		return s.executeScalarSubquery(ctx)
	case "IN":
		return s.executeInSubquery(ctx)
	case "EXISTS":
		return s.executeExistsSubquery(ctx)
	case "ANY", "ALL":
		return s.executeQuantifiedSubquery(ctx)
	default:
		return fmt.Errorf("unsupported subquery type: %s", s.subqueryType)
	}
}

// executeScalarSubquery 执行标量子查询（返回单个值）
func (s *SubqueryOperator) executeScalarSubquery(ctx context.Context) error {
	// 标量子查询应该只返回一行一列
	record, err := s.subplan.Next(ctx)
	if err != nil {
		return fmt.Errorf("scalar subquery error: %w", err)
	}

	if record == nil {
		// 子查询没有返回结果，返回NULL
		s.result = nil
		return nil
	}

	// 获取第一列的值
	values := record.GetValues()
	if len(values) == 0 {
		s.result = nil
		return nil
	}

	s.result = values[0]

	// 检查是否有多行结果（标量子查询应该只返回一行）
	nextRecord, err := s.subplan.Next(ctx)
	if err != nil {
		return err
	}
	if nextRecord != nil {
		return fmt.Errorf("scalar subquery returned more than one row")
	}

	return nil
}

// executeInSubquery 执行IN子查询
func (s *SubqueryOperator) executeInSubquery(ctx context.Context) error {
	// 收集所有结果
	s.resultSet = make([]Record, 0)

	for {
		record, err := s.subplan.Next(ctx)
		if err != nil {
			return fmt.Errorf("IN subquery error: %w", err)
		}
		if record == nil {
			break // EOF
		}
		s.resultSet = append(s.resultSet, record)
	}

	return nil
}

// executeExistsSubquery 执行EXISTS子查询
func (s *SubqueryOperator) executeExistsSubquery(ctx context.Context) error {
	// EXISTS只需要检查是否有结果，不需要获取所有行
	record, err := s.subplan.Next(ctx)
	if err != nil {
		return fmt.Errorf("EXISTS subquery error: %w", err)
	}

	// 如果有至少一行结果，EXISTS为true
	if record != nil {
		s.result = true
	} else {
		s.result = false
	}

	return nil
}

// executeQuantifiedSubquery 执行量化子查询（ANY/ALL）
func (s *SubqueryOperator) executeQuantifiedSubquery(ctx context.Context) error {
	// 收集所有结果
	s.resultSet = make([]Record, 0)

	for {
		record, err := s.subplan.Next(ctx)
		if err != nil {
			return fmt.Errorf("quantified subquery error: %w", err)
		}
		if record == nil {
			break // EOF
		}
		s.resultSet = append(s.resultSet, record)
	}

	return nil
}

// GetResult 获取子查询结果
func (s *SubqueryOperator) GetResult() interface{} {
	return s.result
}

// GetResultSet 获取子查询结果集
func (s *SubqueryOperator) GetResultSet() []Record {
	return s.resultSet
}

// ========================================
// ApplyOperator - Apply算子（用于关联子查询）
// ========================================

// ApplyOperator Apply算子，为外层每一行执行内层子查询
type ApplyOperator struct {
	BaseOperator
	outer      Operator          // 外层算子
	inner      Operator          // 内层算子（子查询）
	applyType  string            // "INNER", "LEFT", "SEMI", "ANTI"
	correlated bool              // 是否为关联
	joinConds  []plan.Expression // 关联条件
	outerRow   Record            // 当前外层记录
	innerRows  []Record          // 当前内层结果
	innerIndex int               // 内层结果索引
}

// NewApplyOperator 创建Apply算子
func NewApplyOperator(outer, inner Operator, applyType string, correlated bool, joinConds []plan.Expression) *ApplyOperator {
	return &ApplyOperator{
		outer:      outer,
		inner:      inner,
		applyType:  applyType,
		correlated: correlated,
		joinConds:  joinConds,
	}
}

func (a *ApplyOperator) Open(ctx context.Context) error {
	if err := a.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 打开外层算子
	if err := a.outer.Open(ctx); err != nil {
		return fmt.Errorf("failed to open outer operator: %w", err)
	}

	// 合并schema
	outerSchema := a.outer.Schema()
	innerSchema := a.inner.Schema()

	if outerSchema != nil && innerSchema != nil {
		// 创建合并后的schema
		mergedSchema := metadata.NewQuerySchema()

		// 添加外层列
		for i := 0; i < outerSchema.ColumnCount(); i++ {
			if col, ok := outerSchema.GetColumnByIndex(i); ok {
				mergedSchema.AddColumn(col)
			}
		}

		// 根据Apply类型决定是否添加内层列
		if a.applyType == "INNER" || a.applyType == "LEFT" {
			// INNER/LEFT JOIN需要返回内层列
			for i := 0; i < innerSchema.ColumnCount(); i++ {
				if col, ok := innerSchema.GetColumnByIndex(i); ok {
					mergedSchema.AddColumn(col)
				}
			}
		}
		// SEMI/ANTI JOIN只返回外层列，不需要添加内层列

		a.schema = mergedSchema
	}

	return nil
}

func (a *ApplyOperator) Next(ctx context.Context) (Record, error) {
	if !a.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	for {
		// 如果当前外层记录还有内层结果未处理
		if a.outerRow != nil && a.innerIndex < len(a.innerRows) {
			innerRow := a.innerRows[a.innerIndex]
			a.innerIndex++

			// 根据Apply类型返回结果
			switch a.applyType {
			case "INNER", "LEFT":
				// 合并外层和内层记录
				return a.mergeRecords(a.outerRow, innerRow), nil
			case "SEMI":
				// SEMI JOIN只返回外层记录（已经找到匹配）
				a.outerRow = nil // 标记当前外层记录已处理
				a.innerRows = nil
				return a.outerRow, nil
			case "ANTI":
				// ANTI JOIN不应该返回有匹配的记录
				// 继续处理下一个外层记录
				a.outerRow = nil
				a.innerRows = nil
				continue
			}
		}

		// 获取下一个外层记录
		outerRow, err := a.outer.Next(ctx)
		if err != nil {
			return nil, err
		}
		if outerRow == nil {
			return nil, nil // EOF
		}

		a.outerRow = outerRow
		a.innerIndex = 0

		// 为当前外层记录执行内层子查询
		if err := a.executeInnerForOuter(ctx, outerRow); err != nil {
			return nil, fmt.Errorf("failed to execute inner for outer: %w", err)
		}

		// 根据Apply类型处理结果
		switch a.applyType {
		case "INNER":
			// INNER JOIN：如果没有匹配，跳过当前外层记录
			if len(a.innerRows) == 0 {
				continue
			}
		case "LEFT":
			// LEFT JOIN：如果没有匹配，返回外层记录+NULL
			if len(a.innerRows) == 0 {
				return a.mergeRecords(outerRow, nil), nil
			}
		case "SEMI":
			// SEMI JOIN：如果有匹配，返回外层记录
			if len(a.innerRows) > 0 {
				return outerRow, nil
			}
			// 没有匹配，继续下一个外层记录
			continue
		case "ANTI":
			// ANTI JOIN：如果没有匹配，返回外层记录
			if len(a.innerRows) == 0 {
				return outerRow, nil
			}
			// 有匹配，跳过当前外层记录
			continue
		}
	}
}

// executeInnerForOuter 为外层记录执行内层子查询
func (a *ApplyOperator) executeInnerForOuter(ctx context.Context, outerRow Record) error {
	// 重新打开内层算子
	if err := a.inner.Open(ctx); err != nil {
		return fmt.Errorf("failed to open inner operator: %w", err)
	}
	defer a.inner.Close()

	// 收集所有内层结果
	a.innerRows = make([]Record, 0)

	for {
		innerRow, err := a.inner.Next(ctx)
		if err != nil {
			return err
		}
		if innerRow == nil {
			break // EOF
		}

		// 检查关联条件
		if a.evaluateJoinConditions(outerRow, innerRow) {
			a.innerRows = append(a.innerRows, innerRow)

			// SEMI/ANTI JOIN只需要知道是否有匹配，不需要所有结果
			if a.applyType == "SEMI" || a.applyType == "ANTI" {
				break
			}
		}
	}

	return nil
}

// evaluateJoinConditions 评估关联条件
func (a *ApplyOperator) evaluateJoinConditions(outerRow, innerRow Record) bool {
	if len(a.joinConds) == 0 {
		return true // 没有条件，总是匹配
	}

	// TODO: 实现实际的条件评估
	// 这里需要创建EvalContext并评估表达式
	// 简化实现：总是返回true
	return true
}

// mergeRecords 合并外层和内层记录
func (a *ApplyOperator) mergeRecords(outerRow, innerRow Record) Record {
	outerValues := outerRow.GetValues()

	var mergedValues []basic.Value
	if innerRow != nil {
		innerValues := innerRow.GetValues()
		mergedValues = make([]basic.Value, len(outerValues)+len(innerValues))
		copy(mergedValues, outerValues)
		copy(mergedValues[len(outerValues):], innerValues)
	} else {
		// LEFT JOIN with no match: outer + NULLs
		innerSchema := a.inner.Schema()
		innerColCount := 0
		if innerSchema != nil {
			innerColCount = innerSchema.ColumnCount()
		}

		mergedValues = make([]basic.Value, len(outerValues)+innerColCount)
		copy(mergedValues, outerValues)
		for i := len(outerValues); i < len(mergedValues); i++ {
			mergedValues[i] = basic.NewNull()
		}
	}

	return NewExecutorRecordFromValues(mergedValues, a.schema)
}

func (a *ApplyOperator) Close() error {
	if a.outer != nil {
		a.outer.Close()
	}
	if a.inner != nil {
		a.inner.Close()
	}
	return a.BaseOperator.Close()
}

// ========================================
// VolcanoExecutor - 火山模型执行器
// ========================================

// VolcanoExecutor 火山模型执行器，负责构建和执行算子树
type VolcanoExecutor struct {
	root Operator

	// 管理器组件
	tableManager      *manager.TableManager
	bufferPoolManager *manager.OptimizedBufferPoolManager
	storageManager    *manager.StorageManager
	indexManager      *manager.IndexManager
}

func NewVolcanoExecutor(
	tableManager *manager.TableManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	storageManager *manager.StorageManager,
	indexManager *manager.IndexManager,
) *VolcanoExecutor {
	return &VolcanoExecutor{
		tableManager:      tableManager,
		bufferPoolManager: bufferPoolManager,
		storageManager:    storageManager,
		indexManager:      indexManager,
	}
}

// BuildFromPhysicalPlan 从物理计划构建算子树
func (v *VolcanoExecutor) BuildFromPhysicalPlan(ctx context.Context, physicalPlan plan.PhysicalPlan) error {
	operator, err := v.buildOperatorTree(ctx, physicalPlan)
	if err != nil {
		return fmt.Errorf("failed to build operator tree: %w", err)
	}
	v.root = operator
	return nil
}

// buildOperatorTree 递归构建算子树
func (v *VolcanoExecutor) buildOperatorTree(ctx context.Context, physicalPlan plan.PhysicalPlan) (Operator, error) {
	if physicalPlan == nil {
		return nil, fmt.Errorf("physical plan is nil")
	}

	switch p := physicalPlan.(type) {
	case *plan.PhysicalTableScan:
		return v.buildTableScan(p)

	case *plan.PhysicalIndexScan:
		return v.buildIndexScan(p)

	case *plan.PhysicalSelection:
		return v.buildSelection(ctx, p)

	case *plan.PhysicalProjection:
		return v.buildProjection(ctx, p)

	case *plan.PhysicalHashJoin:
		return v.buildHashJoin(ctx, p)

	case *plan.PhysicalMergeJoin:
		return v.buildMergeJoin(ctx, p)

	case *plan.PhysicalHashAgg:
		return v.buildHashAgg(ctx, p)

	case *plan.PhysicalStreamAgg:
		return v.buildStreamAgg(ctx, p)

	case *plan.PhysicalSort:
		return v.buildSort(ctx, p)

	case *plan.PhysicalSubquery:
		return v.buildSubquery(ctx, p)

	case *plan.PhysicalApply:
		return v.buildApply(ctx, p)

	default:
		return nil, fmt.Errorf("unsupported physical plan type: %T", physicalPlan)
	}
}

func (v *VolcanoExecutor) buildTableScan(p *plan.PhysicalTableScan) (Operator, error) {
	if p.Table == nil {
		return nil, fmt.Errorf("table is nil in PhysicalTableScan")
	}

	// 创建StorageAdapter
	storageAdapter := NewStorageAdapter(
		v.tableManager,
		v.bufferPoolManager,
		v.storageManager,
		nil, // tableStorageManager可以为nil，会在需要时创建
	)

	// 提取schema名称（从DatabaseSchema中获取）
	schemaName := ""
	if p.Table.Schema != nil {
		schemaName = p.Table.Schema.Name
	}

	// 创建TableScanOperator
	return NewTableScanOperator(
		schemaName,
		p.Table.Name,
		storageAdapter,
	), nil
}

func (v *VolcanoExecutor) buildIndexScan(p *plan.PhysicalIndexScan) (Operator, error) {
	if p.Table == nil || p.Index == nil {
		return nil, fmt.Errorf("table or index is nil in PhysicalIndexScan")
	}

	// 创建StorageAdapter
	storageAdapter := NewStorageAdapter(
		v.tableManager,
		v.bufferPoolManager,
		v.storageManager,
		nil, // tableStorageManager可以为nil
	)

	// 创建IndexAdapter
	indexAdapter := NewIndexAdapter(
		v.indexManager,
		nil, // btreeManager可以为nil
		storageAdapter,
	)

	// 提取schema名称（从DatabaseSchema中获取）
	schemaName := ""
	if p.Table.Schema != nil {
		schemaName = p.Table.Schema.Name
	}

	// 创建IndexScanOperator
	return NewIndexScanOperator(
		schemaName,
		p.Table.Name,
		p.Index.Name,
		storageAdapter,
		indexAdapter,
		nil,        // startKey - 可以从p中提取
		nil,        // endKey - 可以从p中提取
		[]string{}, // requiredColumns - 可以从p.Schema中提取
	), nil
}

func (v *VolcanoExecutor) buildSelection(ctx context.Context, p *plan.PhysicalSelection) (Operator, error) {
	children := p.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("PhysicalSelection has no children")
	}

	child, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, err
	}

	// 将Conditions转换为predicate函数
	predicate := v.buildPredicate(p.Conditions, child.Schema())

	return NewFilterOperator(child, predicate), nil
}

func (v *VolcanoExecutor) buildProjection(ctx context.Context, p *plan.PhysicalProjection) (Operator, error) {
	children := p.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("PhysicalProjection has no children")
	}

	child, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, err
	}

	return NewProjectionOperatorWithExprs(child, p.Exprs), nil
}

func (v *VolcanoExecutor) buildHashJoin(ctx context.Context, p *plan.PhysicalHashJoin) (Operator, error) {
	children := p.Children()
	if len(children) < 2 {
		return nil, fmt.Errorf("PhysicalHashJoin needs 2 children")
	}

	left, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, err
	}

	right, err := v.buildOperatorTree(ctx, children[1])
	if err != nil {
		return nil, err
	}

	// 从Conditions构建hash key函数
	buildKey, probeKey := v.buildHashKeyFunctions(p.Conditions, p.LeftSchema, p.RightSchema)

	return NewHashJoinOperator(left, right, p.JoinType, buildKey, probeKey), nil
}

func (v *VolcanoExecutor) buildMergeJoin(ctx context.Context, p *plan.PhysicalMergeJoin) (Operator, error) {
	// MergeJoin可以用NestedLoopJoin实现
	children := p.Children()
	if len(children) < 2 {
		return nil, fmt.Errorf("PhysicalMergeJoin needs 2 children")
	}

	left, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, err
	}

	right, err := v.buildOperatorTree(ctx, children[1])
	if err != nil {
		return nil, err
	}

	condition := func(l, r Record) bool { return true }
	return NewNestedLoopJoinOperator(left, right, p.JoinType, condition), nil
}

func (v *VolcanoExecutor) buildHashAgg(ctx context.Context, p *plan.PhysicalHashAgg) (Operator, error) {
	children := p.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("PhysicalHashAgg has no children")
	}

	child, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, err
	}

	// 从GroupByItems和AggFuncs构建聚合函数
	groupByExprs := v.buildGroupByExprs(p.GroupByItems, child.Schema())
	aggFuncs := v.buildAggFuncs(p.AggFuncs)

	return NewHashAggregateOperator(child, groupByExprs, aggFuncs), nil
}

func (v *VolcanoExecutor) buildStreamAgg(ctx context.Context, p *plan.PhysicalStreamAgg) (Operator, error) {
	// StreamAgg可以用HashAgg实现
	return v.buildHashAgg(ctx, (*plan.PhysicalHashAgg)(p))
}

func (v *VolcanoExecutor) buildSort(ctx context.Context, p *plan.PhysicalSort) (Operator, error) {
	children := p.Children()
	if len(children) == 0 {
		return nil, fmt.Errorf("PhysicalSort has no children")
	}

	child, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, err
	}

	// 从ByItems构建排序键
	sortKeys := v.buildSortKeys(p.ByItems, child.Schema())

	return NewSortOperator(child, sortKeys), nil
}

func (v *VolcanoExecutor) buildSubquery(ctx context.Context, p *plan.PhysicalSubquery) (Operator, error) {
	// 构建子查询的执行计划
	var subplan Operator
	var err error

	if p.Subplan != nil {
		subplan, err = v.buildOperatorTree(ctx, p.Subplan)
		if err != nil {
			return nil, fmt.Errorf("failed to build subquery plan: %w", err)
		}
	}

	return NewSubqueryOperator(p.SubqueryType, p.Correlated, p.OuterRefs, subplan), nil
}

func (v *VolcanoExecutor) buildApply(ctx context.Context, p *plan.PhysicalApply) (Operator, error) {
	children := p.Children()
	if len(children) < 2 {
		return nil, fmt.Errorf("PhysicalApply needs 2 children")
	}

	// 构建外层算子
	outer, err := v.buildOperatorTree(ctx, children[0])
	if err != nil {
		return nil, fmt.Errorf("failed to build outer operator: %w", err)
	}

	// 构建内层算子（子查询）
	inner, err := v.buildOperatorTree(ctx, children[1])
	if err != nil {
		return nil, fmt.Errorf("failed to build inner operator: %w", err)
	}

	return NewApplyOperator(outer, inner, p.ApplyType, p.Correlated, p.JoinConds), nil
}

// Execute 执行查询并返回所有结果
func (v *VolcanoExecutor) Execute(ctx context.Context) ([]Record, error) {
	if v.root == nil {
		return nil, fmt.Errorf("root operator is nil")
	}

	// Open阶段
	if err := v.root.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open root operator: %w", err)
	}
	defer v.root.Close()

	// Next阶段：迭代获取所有结果
	var results []Record
	for {
		record, err := v.root.Next(ctx)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error during execution: %w", err)
		}
		if record == nil {
			break // EOF
		}
		results = append(results, record)
	}

	logger.Debugf("VolcanoExecutor: executed query, returned %d rows", len(results))
	return results, nil
}

// ========================================
// 辅助函数：从物理计划构建算子的辅助方法
// ========================================

// buildPredicate 从Conditions构建predicate函数
func (v *VolcanoExecutor) buildPredicate(conditions []plan.Expression, schema *metadata.QuerySchema) func(Record) bool {
	if len(conditions) == 0 {
		return func(r Record) bool { return true }
	}

	return func(record Record) bool {
		// 创建求值上下文
		evalCtx := v.createEvalContextFromRecord(record, schema)

		// 对所有条件进行AND操作
		for _, cond := range conditions {
			result, err := cond.Eval(evalCtx)
			if err != nil {
				logger.Debugf("Failed to evaluate condition %s: %v, treating as false", cond.String(), err)
				return false
			}

			// 将结果转换为bool
			boolResult, ok := result.(bool)
			if !ok {
				logger.Debugf("Condition %s did not return bool, got %T, treating as false", cond.String(), result)
				return false
			}

			if !boolResult {
				return false
			}
		}

		return true
	}
}

// buildHashKeyFunctions 从Conditions构建hash key函数
func (v *VolcanoExecutor) buildHashKeyFunctions(
	conditions []plan.Expression,
	leftSchema *metadata.DatabaseSchema,
	rightSchema *metadata.DatabaseSchema,
) (func(Record) string, func(Record) string) {
	// 默认实现：使用第一个等值条件
	if len(conditions) == 0 {
		// 没有条件，使用默认实现
		return func(r Record) string { return "" }, func(r Record) string { return "" }
	}

	// 查找第一个等值条件
	for _, cond := range conditions {
		if binOp, ok := cond.(*plan.BinaryOperation); ok && binOp.Op == plan.OpEQ {
			// 提取左右列
			leftCol, rightCol := v.extractJoinColumns(binOp)
			if leftCol != "" && rightCol != "" {
				// 构建build key函数（左表）
				buildKey := func(r Record) string {
					values := r.GetValues()
					// 简化实现：使用第一个值
					if len(values) > 0 {
						return fmt.Sprintf("%v", values[0].Raw())
					}
					return ""
				}

				// 构建probe key函数（右表）
				probeKey := func(r Record) string {
					values := r.GetValues()
					// 简化实现：使用第一个值
					if len(values) > 0 {
						return fmt.Sprintf("%v", values[0].Raw())
					}
					return ""
				}

				return buildKey, probeKey
			}
		}
	}

	// 没有找到等值条件，使用默认实现
	return func(r Record) string { return "" }, func(r Record) string { return "" }
}

// extractJoinColumns 从二元操作中提取连接列
func (v *VolcanoExecutor) extractJoinColumns(binOp *plan.BinaryOperation) (string, string) {
	leftCol, leftOk := binOp.Left.(*plan.Column)
	rightCol, rightOk := binOp.Right.(*plan.Column)

	if leftOk && rightOk {
		return leftCol.Name, rightCol.Name
	}

	return "", ""
}

// buildGroupByExprs 从GroupByItems构建分组表达式（列索引）
func (v *VolcanoExecutor) buildGroupByExprs(groupByItems []plan.Expression, schema *metadata.QuerySchema) []int {
	if len(groupByItems) == 0 {
		return []int{}
	}

	var groupByExprs []int
	for _, item := range groupByItems {
		// 如果是列引用，查找列索引
		if col, ok := item.(*plan.Column); ok {
			idx := v.findColumnIndex(col.Name, schema)
			if idx >= 0 {
				groupByExprs = append(groupByExprs, idx)
			}
		}
	}

	return groupByExprs
}

// buildAggFuncs 从AggFuncs构建聚合函数
func (v *VolcanoExecutor) buildAggFuncs(aggFuncs []plan.AggregateFunc) []AggregateFunc {
	if len(aggFuncs) == 0 {
		return []AggregateFunc{&CountAgg{}}
	}

	var funcs []AggregateFunc
	for _, aggFunc := range aggFuncs {
		funcName := aggFunc.Name()
		switch funcName {
		case "COUNT":
			funcs = append(funcs, &CountAgg{})
		case "SUM":
			funcs = append(funcs, &SumAgg{})
		case "AVG":
			funcs = append(funcs, &AvgAgg{})
		case "MIN":
			funcs = append(funcs, &MinAgg{})
		case "MAX":
			funcs = append(funcs, &MaxAgg{})
		default:
			// 默认使用COUNT
			funcs = append(funcs, &CountAgg{})
		}
	}

	return funcs
}

// buildSortKeys 从ByItems构建排序键
func (v *VolcanoExecutor) buildSortKeys(byItems []plan.ByItem, schema *metadata.QuerySchema) []SortKey {
	if len(byItems) == 0 {
		return []SortKey{{ColumnIdx: 0, Ascending: true}}
	}

	var sortKeys []SortKey
	for _, item := range byItems {
		// 如果是列引用，查找列索引
		if col, ok := item.Expr.(*plan.Column); ok {
			idx := v.findColumnIndex(col.Name, schema)
			if idx >= 0 {
				sortKeys = append(sortKeys, SortKey{
					ColumnIdx: idx,
					Ascending: !item.Desc, // Desc=true表示降序，Ascending=false
				})
			}
		}
	}

	if len(sortKeys) == 0 {
		// 如果没有找到任何列，使用默认排序
		return []SortKey{{ColumnIdx: 0, Ascending: true}}
	}

	return sortKeys
}

// findColumnIndex 在schema中查找列索引
func (v *VolcanoExecutor) findColumnIndex(columnName string, schema *metadata.QuerySchema) int {
	if schema == nil {
		return -1
	}

	for i := 0; i < schema.ColumnCount(); i++ {
		col, ok := schema.GetColumnByIndex(i)
		if ok && col != nil && col.Name == columnName {
			return i
		}
	}

	return -1
}

// createEvalContextFromRecord 从Record创建求值上下文
func (v *VolcanoExecutor) createEvalContextFromRecord(record Record, schema *metadata.QuerySchema) *plan.EvalContext {
	row := make(map[string]interface{})
	if schema == nil {
		return &plan.EvalContext{Row: row}
	}

	values := record.GetValues()
	for i := 0; i < schema.ColumnCount() && i < len(values); i++ {
		col, ok := schema.GetColumnByIndex(i)
		if ok && col != nil {
			// 将basic.Value转换为interface{}
			row[col.Name] = v.valueToInterface(values[i])
		}
	}

	return &plan.EvalContext{Row: row}
}

// valueToInterface 将basic.Value转换为interface{}
func (v *VolcanoExecutor) valueToInterface(val basic.Value) interface{} {
	if val == nil || val.IsNull() {
		return nil
	}

	// 根据值的类型进行转换
	valueType := val.Type()
	switch valueType {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt,
		basic.ValueTypeInt, basic.ValueTypeBigInt:
		return val.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		return val.Float64()
	case basic.ValueTypeVarchar, basic.ValueTypeChar, basic.ValueTypeText:
		return val.String()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		return val.Bool()
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeBlob:
		return val.Bytes()
	case basic.ValueTypeDate, basic.ValueTypeTime, basic.ValueTypeDateTime, basic.ValueTypeTimestamp:
		return val.Time()
	default:
		// 默认使用Raw()方法
		return val.Raw()
	}
}

// ExecuteStream 流式执行查询，返回迭代器
func (v *VolcanoExecutor) ExecuteStream(ctx context.Context) (Operator, error) {
	if v.root == nil {
		return nil, fmt.Errorf("root operator is nil")
	}

	if err := v.root.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open root operator: %w", err)
	}

	return v.root, nil
}
