package engine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
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

// BatchOperator is the optional batch execution contract. Operators that
// implement it can pull bounded chunks from their child without falling back
// to one Next call per row. The returned error may be io.EOF together with a
// final non-empty batch.
type BatchOperator interface {
	NextBatch(ctx context.Context, maxRows int) ([]Record, error)
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
	b.closed = false
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
	b.opened = false
	return nil
}

func (b *BaseOperator) Schema() *metadata.QuerySchema {
	return b.schema
}

func nextOperatorBatch(ctx context.Context, child Operator, maxRows int) ([]Record, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator is nil")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	if batchChild, ok := child.(BatchOperator); ok {
		return batchChild.NextBatch(ctx, maxRows)
	}
	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		record, err := child.Next(ctx)
		if err != nil {
			if err == io.EOF && len(rows) > 0 {
				return rows, io.EOF
			}
			return rows, err
		}
		if record == nil {
			if len(rows) == 0 {
				return nil, io.EOF
			}
			return rows, io.EOF
		}
		rows = append(rows, record)
	}
	return rows, nil
}

// ValuesOperator is the in-memory source for SELECT constants and DUAL.
// Keeping it as an operator lets the normal plan builder execute CTE anchors
// without inventing a catalog table named dual.
type ValuesOperator struct {
	BaseOperator
	exprs   []plan.Expression
	emitted bool
}

func NewValuesOperator(exprs []plan.Expression) *ValuesOperator {
	return &ValuesOperator{
		BaseOperator: BaseOperator{children: nil},
		exprs:        exprs,
	}
}

func (v *ValuesOperator) Open(ctx context.Context) error {
	if err := v.BaseOperator.Open(ctx); err != nil {
		return err
	}
	v.schema = metadata.NewQuerySchema()
	for _, expr := range v.exprs {
		v.schema.AddColumn(metadata.NewQueryColumn(expr.String(), valuesExpressionType(expr)))
	}
	v.emitted = false
	return nil
}

func (v *ValuesOperator) Next(ctx context.Context) (Record, error) {
	if !v.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if v.emitted {
		return nil, nil
	}
	v.emitted = true
	values := make([]basic.Value, len(v.exprs))
	converter := &ProjectionOperator{}
	for i, expr := range v.exprs {
		result, err := expr.Eval(&plan.EvalContext{Row: map[string]interface{}{}})
		if err != nil {
			values[i] = basic.NewNull()
			continue
		}
		values[i] = converter.convertToValue(result)
	}
	return NewExecutorRecordFromValues(values, v.schema), nil
}

func (v *ValuesOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	record, err := v.Next(ctx)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, io.EOF
	}
	return []Record{record}, nil
}

func valuesExpressionType(expr plan.Expression) metadata.DataType {
	switch expr.GetType() {
	case plan.TypeInt:
		return metadata.TypeInt
	case plan.TypeFloat:
		return metadata.TypeDouble
	case plan.TypeBoolean:
		return metadata.TypeBoolean
	case plan.TypeDateTime:
		return metadata.TypeDateTime
	default:
		return metadata.TypeVarchar
	}
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
	if s.schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	col, ok := s.schema.GetColumn(name)
	if !ok || col == nil {
		return nil, fmt.Errorf("column %s not found", name)
	}

	index := col.OrdinalPosition - 1
	if index < 0 || index >= len(s.values) {
		return nil, fmt.Errorf("column %s index out of range", name)
	}

	return s.values[index], nil
}

func (s *SimpleExecutorRecord) SetValueByIndex(index int, value basic.Value) error {
	if index < 0 || index >= len(s.values) {
		return fmt.Errorf("index out of range: %d", index)
	}
	s.values[index] = value
	return nil
}

func (s *SimpleExecutorRecord) SetValueByName(name string, value basic.Value) error {
	if s.schema == nil {
		return fmt.Errorf("schema is nil")
	}

	col, ok := s.schema.GetColumn(name)
	if !ok || col == nil {
		return fmt.Errorf("column %s not found", name)
	}

	index := col.OrdinalPosition - 1
	if index < 0 || index >= len(s.values) {
		return fmt.Errorf("column %s index out of range", name)
	}

	s.values[index] = value
	return nil
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

	// requiredColumns carries logical column pruning into the physical scan.
	// Storage still decodes the full row, but the operator emits only the
	// columns required by projection, predicates, joins, or aggregation.
	requiredColumns []string
	projectIndices  []int
}

func NewTableScanOperator(
	schemaName, tableName string,
	storageAdapter *StorageAdapter,
	required ...[]string,
) *TableScanOperator {
	var requiredColumns []string
	if len(required) > 0 {
		requiredColumns = append([]string(nil), required[0]...)
	}
	return &TableScanOperator{
		BaseOperator: BaseOperator{
			children: nil,
			schema:   nil, // 将在Open时设置
		},
		schemaName:      schemaName,
		tableName:       tableName,
		storageAdapter:  storageAdapter,
		requiredColumns: requiredColumns,
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

	// 从Table创建QuerySchema，并将逻辑列裁剪传入物理输出边界。
	fullSchema := metadata.FromTable(tableMeta.Schema)
	t.schema = fullSchema
	if len(t.requiredColumns) > 0 {
		indices := make([]int, 0, len(t.requiredColumns))
		for _, required := range t.requiredColumns {
			for index, column := range fullSchema.Columns {
				if strings.EqualFold(column.Name, required) {
					indices = append(indices, index)
					break
				}
			}
		}
		if len(indices) == len(t.requiredColumns) {
			t.projectIndices = indices
			t.schema = metadata.ProjectSchema(fullSchema, indices)
		}
	}

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
	if record == nil || len(t.projectIndices) == 0 {
		return record, nil
	}
	values := record.GetValues()
	projected := make([]basic.Value, 0, len(t.projectIndices))
	for _, index := range t.projectIndices {
		if index >= 0 && index < len(values) {
			projected = append(projected, values[index])
		}
	}
	return NewExecutorRecordFromValues(projected, t.schema), nil
}

func (t *TableScanOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !t.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		record, err := t.Next(ctx)
		if err != nil {
			if err == io.EOF && len(rows) > 0 {
				return rows, io.EOF
			}
			return rows, err
		}
		if record == nil {
			if len(rows) == 0 {
				return nil, io.EOF
			}
			return rows, io.EOF
		}
		rows = append(rows, record)
	}
	return rows, nil
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
	// 原始二级索引 key 列表（覆盖索引读取使用；不能用主键替代）
	indexKeys [][]byte
	keyIndex  int
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
		indexKeys:       [][]byte{},
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
	// 从Table创建QuerySchema，并保持列裁剪后的输出顺序。
	fullSchema := metadata.FromTable(tableMeta.Schema)
	i.schema = fullSchema
	if len(i.requiredColumns) > 0 {
		indices := make([]int, 0, len(i.requiredColumns))
		for _, required := range i.requiredColumns {
			for index, column := range fullSchema.Columns {
				if strings.EqualFold(column.Name, required) {
					indices = append(indices, index)
					break
				}
			}
		}
		if len(indices) == len(i.requiredColumns) {
			i.schema = metadata.ProjectSchema(fullSchema, indices)
		}
	}

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

func (i *IndexScanOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !i.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		record, err := i.Next(ctx)
		if err != nil {
			if err == io.EOF && len(rows) > 0 {
				return rows, io.EOF
			}
			return rows, err
		}
		if record == nil {
			if len(rows) == 0 {
				return nil, io.EOF
			}
			return rows, io.EOF
		}
		rows = append(rows, record)
	}
	return rows, nil
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

	if i.indexAdapter == nil || i.indexAdapter.indexManager == nil {
		return fmt.Errorf("secondary index manager is unavailable")
	}

	rows, err := i.indexAdapter.indexManager.RangeSearch(i.indexMetadata.IndexID, startKeyBytes, endKeyBytes)
	if err != nil {
		return fmt.Errorf("secondary index range search failed: %w", err)
	}
	primaryKeys := make([][]byte, 0, len(rows))
	indexKeys := make([][]byte, 0, len(rows))
	for _, row := range rows {
		primaryKey, err := manager.DecodeSecondaryIndexValue(row.ToByte())
		if err != nil {
			return fmt.Errorf("decode secondary index primary key: %w", err)
		}
		indexKeys = append(indexKeys, append([]byte(nil), row.GetPrimaryKey().Bytes()...))
		primaryKeys = append(primaryKeys, primaryKey)
	}

	i.primaryKeys = primaryKeys
	i.indexKeys = indexKeys
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

	// 获取当前索引键。覆盖索引必须使用二级索引 key；primaryKeys 只用于
	// 回表，二者在非唯一二级索引中明确不是同一份字节串。
	indexKey := i.primaryKeys[i.keyIndex]
	if i.keyIndex < len(i.indexKeys) {
		indexKey = i.indexKeys[i.keyIndex]
	}
	i.keyIndex++

	// 从索引直接读取记录（覆盖索引优化），保留 key/value 边界以便精确解码。
	indexRecord, err := i.indexAdapter.ReadIndexEntry(ctx, i.indexMetadata.IndexID, indexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read index record for covering index %q: %w", i.indexName, err)
	}
	if len(i.indexMetadata.Columns) == 0 {
		// Legacy callers that never supplied durable index metadata cannot be
		// decoded. Keep their byte-oriented smoke behavior, but never use it
		// for a real covering index (which always has Columns populated).
		return NewExecutorRecordFromValues([]basic.Value{basic.NewString(indexRecord.Key)}, i.schema), nil
	}

	values, err := i.decodeCoveringIndexEntry(indexRecord)
	if err != nil {
		return nil, fmt.Errorf("decode covering index %q: %w", i.indexName, err)
	}

	return NewExecutorRecordFromValues(values, i.schema), nil
}

func (i *IndexScanOperator) decodeCoveringIndexEntry(indexRecord *manager.IndexRecord) ([]basic.Value, error) {
	if indexRecord == nil {
		return nil, fmt.Errorf("index entry is nil")
	}
	_, _, _, indexedColumns, indexedValues, err := manager.DecodeSecondaryIndexKey(indexRecord.Key)
	if err != nil {
		return nil, err
	}
	if len(indexedColumns) != len(indexedValues) {
		return nil, fmt.Errorf("indexed column/value count mismatch: %d/%d", len(indexedColumns), len(indexedValues))
	}
	indexed := make(map[string]string, len(indexedColumns))
	for index, column := range indexedColumns {
		indexed[strings.ToLower(column)] = indexedValues[index]
	}
	primaryKey, err := manager.DecodeSecondaryIndexValue(indexRecord.Value)
	if err != nil {
		return nil, err
	}
	primaryValues := decodeSecondaryPrimaryKeyParts(primaryKey, len(i.indexMetadata.PrimaryKeyColumns))
	primary := make(map[string]string, len(i.indexMetadata.PrimaryKeyColumns))
	for index, column := range i.indexMetadata.PrimaryKeyColumns {
		if index < len(primaryValues) {
			primary[strings.ToLower(column)] = primaryValues[index]
		}
	}

	values := make([]basic.Value, 0, len(i.requiredColumns))
	for _, required := range i.requiredColumns {
		key := strings.ToLower(required)
		raw, ok := indexed[key]
		if !ok {
			raw, ok = primary[key]
		}
		if !ok {
			return nil, fmt.Errorf("column %s is not present in covering index entry", required)
		}
		values = append(values, decodeCoveringIndexValue(raw, i.schema, required))
	}
	return values, nil
}

func decodeSecondaryPrimaryKeyParts(value []byte, columnCount int) []string {
	if columnCount <= 0 {
		return nil
	}
	parts := make([]string, 0, columnCount)
	offset := 0
	for index := 0; index < columnCount; index++ {
		if offset+4 > len(value) {
			if index == 0 && columnCount == 1 {
				return []string{string(value)}
			}
			return nil
		}
		length := int(binary.BigEndian.Uint32(value[offset : offset+4]))
		offset += 4
		if length < 0 || offset+length > len(value) {
			if index == 0 && columnCount == 1 {
				return []string{string(value)}
			}
			return nil
		}
		parts = append(parts, string(value[offset:offset+length]))
		offset += length
	}
	if offset != len(value) {
		return nil
	}
	return parts
}

func decodeCoveringIndexValue(raw string, schema *metadata.QuerySchema, columnName string) basic.Value {
	if raw == "<nil>" {
		return basic.NewNull()
	}
	var dataType metadata.DataType
	if schema != nil {
		if column, ok := schema.GetColumn(columnName); ok && column != nil {
			dataType = column.DataType
		}
	}
	switch dataType {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeYear:
		if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return basic.NewInt64(value)
		}
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		if value, err := strconv.ParseFloat(raw, 64); err == nil {
			return basic.NewFloat64(value)
		}
	case metadata.TypeBinary, metadata.TypeVarBinary, metadata.TypeBlob, metadata.TypeTinyBlob, metadata.TypeMediumBlob, metadata.TypeLongBlob:
		return basic.NewBytes([]byte(raw))
	}
	return basic.NewString(raw)
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

func (f *FilterOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !f.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	for {
		input, err := nextOperatorBatch(ctx, f.child, maxRows)
		if err != nil && err != io.EOF {
			return input, err
		}
		filtered := make([]Record, 0, len(input))
		for _, record := range input {
			if f.predicate == nil || f.predicate(record) {
				filtered = append(filtered, record)
			}
		}
		if len(filtered) > 0 {
			if err == io.EOF {
				return filtered, io.EOF
			}
			return filtered, nil
		}
		if err == io.EOF {
			return nil, io.EOF
		}
	}
}

// ========================================
// ProjectionOperator - 投影算子
// ========================================

// ProjectionOperator 投影算子，选择需要的列
type ProjectionOperator struct {
	BaseOperator
	child         Operator
	projections   []int             // 投影列的索引
	exprs         []plan.Expression // 投影表达式（支持计算列）
	compiledExprs []plan.CompiledExpression
	evalRow       map[string]interface{}
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
		switch {
		case len(p.projections) > 0:
			p.schema = metadata.ProjectSchema(childSchema, p.projections)
		case len(p.exprs) > 0:
			p.schema = p.buildExprSchema(childSchema)
		default:
			p.schema = childSchema.Clone()
		}
	} else {
		p.schema = metadata.NewQuerySchema()
	}
	if childSchema != nil {
		p.evalRow = make(map[string]interface{}, childSchema.ColumnCount())
	}
	p.compiledExprs = make([]plan.CompiledExpression, len(p.exprs))
	for index, expr := range p.exprs {
		if compiled, ok := plan.CompileExpression(expr); ok {
			p.compiledExprs[index] = compiled
		}
	}

	return nil
}

func (p *ProjectionOperator) buildExprSchema(childSchema *metadata.QuerySchema) *metadata.QuerySchema {
	schema := metadata.NewQuerySchema()
	if childSchema != nil {
		schema.TableName = childSchema.TableName
		schema.SchemaName = childSchema.SchemaName
	}

	for _, expr := range p.exprs {
		schema.AddColumn(p.buildExprColumn(expr, childSchema))
	}

	return schema
}

func (p *ProjectionOperator) buildExprColumn(expr plan.Expression, childSchema *metadata.QuerySchema) *metadata.QueryColumn {
	if colExpr, ok := expr.(*plan.Column); ok {
		if childSchema != nil {
			if childCol, found := childSchema.GetColumn(colExpr.Name); found && childCol != nil {
				return &metadata.QueryColumn{
					Name:            childCol.Name,
					DataType:        childCol.DataType,
					IsNullable:      childCol.IsNullable,
					TableName:       childCol.TableName,
					SchemaName:      childCol.SchemaName,
					OrdinalPosition: childCol.OrdinalPosition,
					CharMaxLength:   childCol.CharMaxLength,
					Comment:         childCol.Comment,
				}
			}
		}
		return metadata.NewQueryColumn(colExpr.Name, metadata.TypeVarchar)
	}

	return metadata.NewQueryColumn(expr.String(), convertPlanDataTypeToMetadata(expr.GetType()))
}

func convertPlanDataTypeToMetadata(dataType plan.DataType) metadata.DataType {
	switch dataType {
	case plan.TypeInt:
		return metadata.TypeInt
	case plan.TypeFloat:
		return metadata.TypeDouble
	case plan.TypeDateTime:
		return metadata.TypeDateTime
	case plan.TypeBoolean:
		return metadata.TypeBoolean
	case plan.TypeNull:
		return metadata.TypeVarchar
	case plan.TypeString, plan.TypeUnknown:
		fallthrough
	default:
		return metadata.TypeVarchar
	}
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

	return p.projectRecord(record)
}

func (p *ProjectionOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !p.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}

	input, err := nextOperatorBatch(ctx, p.child, maxRows)
	if err != nil && err != io.EOF {
		return input, err
	}
	output := make([]Record, 0, len(input))
	for _, record := range input {
		projected, projectErr := p.projectRecord(record)
		if projectErr != nil {
			return output, projectErr
		}
		output = append(output, projected)
	}
	if err == io.EOF {
		return output, io.EOF
	}
	return output, nil
}

func (p *ProjectionOperator) projectRecord(record Record) (Record, error) {

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
			var result interface{}
			var err error
			if i < len(p.compiledExprs) && p.compiledExprs[i] != nil {
				result, err = p.compiledExprs[i](evalCtx)
			} else {
				result, err = expr.Eval(evalCtx)
			}
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

	// Reuse the row map for this operator's row-at-a-time hot path. The
	// operator is not shared across concurrent executions, so this avoids one
	// map allocation per projected row without changing expression semantics.
	row := p.evalRow
	if row == nil {
		row = make(map[string]interface{}, childSchema.ColumnCount())
		p.evalRow = row
	}
	values := record.GetValues()

	// 遍历schema中的列，建立列名到值的映射
	for i := 0; i < childSchema.ColumnCount(); i++ {
		col, ok := childSchema.GetColumnByIndex(i)
		if ok && col != nil {
			// 将basic.Value转换为interface{}
			if i < len(values) {
				row[col.Name] = p.valueToInterface(values[i])
			} else {
				row[col.Name] = nil
			}
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
	leftRow       Record
	rightEOF      bool
	hadMatch      bool   // 当前左行/右行是否已有匹配（LEFT/RIGHT 用）
	phase         int    // FULL 时：1=LEFT 阶段，2=输出未匹配的右行
	rightRow      Record // RIGHT/FULL 阶段 2 的当前右行
	leftEOF       bool   // RIGHT 时右表为 outer，左表扫完标记
	leftColCount  int    // 左表列数，用于生成 NULL 行
	rightColCount int    // 右表列数

	// Batch execution state. Nested-loop joins need to rescan one side for
	// every row of the other side, so the batch path materializes the rescan
	// side once and streams the outer side in bounded chunks.
	batchInitialized    bool
	batchDone           bool
	batchPhase          int
	batchLeftRows       []Record
	batchRightRows      []Record
	batchRightMatched   []bool
	batchLeftBatch      []Record
	batchLeftBatchPos   int
	batchLeftRowPos     int
	batchRightRowPos    int
	batchLeftSourceEOF  bool
	batchLeftHadMatch   bool
	batchRightBatch     []Record
	batchRightBatchPos  int
	batchRightScanPos   int
	batchRightSourceEOF bool
	batchRightHadMatch  bool
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
		n.leftColCount = leftSchema.ColumnCount()
		n.rightColCount = rightSchema.ColumnCount()
	} else if leftSchema != nil {
		n.schema = leftSchema.Clone()
		n.leftColCount = leftSchema.ColumnCount()
	} else if rightSchema != nil {
		n.schema = rightSchema.Clone()
		n.rightColCount = rightSchema.ColumnCount()
	} else {
		n.schema = metadata.NewQuerySchema()
	}

	n.batchInitialized = false
	n.batchDone = false
	n.batchPhase = 0
	n.batchLeftRows = nil
	n.batchRightRows = nil
	n.batchRightMatched = nil
	n.batchLeftBatch = nil
	n.batchLeftBatchPos = 0
	n.batchLeftRowPos = 0
	n.batchRightRowPos = 0
	n.batchLeftSourceEOF = false
	n.batchLeftHadMatch = false
	n.batchRightBatch = nil
	n.batchRightBatchPos = 0
	n.batchRightScanPos = 0
	n.batchRightSourceEOF = false
	n.batchRightHadMatch = false

	return nil
}

func (n *NestedLoopJoinOperator) Next(ctx context.Context) (Record, error) {
	if !n.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if n.batchInitialized {
		return nil, fmt.Errorf("cannot use row execution after batch execution started")
	}

	switch normalizedNestedLoopJoinType(n.joinType) {
	case "RIGHT":
		return n.nextRight(ctx)
	case "FULL":
		return n.nextFull(ctx)
	case "LEFT":
		return n.nextLeft(ctx)
	default:
		// INNER 或空
		return n.nextInner(ctx)
	}
}

// NextBatch executes the nested-loop join with bounded result batches. The
// side that must be rescanned is materialized through nextOperatorBatch, so a
// batch-capable child is not silently reduced to one-row Next calls.
func (n *NestedLoopJoinOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !n.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	if !n.batchInitialized {
		if err := n.initializeBatchJoin(ctx); err != nil {
			return nil, err
		}
	}
	if n.batchDone {
		return nil, io.EOF
	}

	rows := make([]Record, 0, maxRows)
	switch n.batchPhase {
	case 1:
		if normalizedNestedLoopJoinType(n.joinType) == "RIGHT" {
			var err error
			rows, err = n.nextBatchRight(ctx, maxRows)
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			rows, err = n.nextBatchLeft(ctx, maxRows)
			if err != nil {
				return nil, err
			}
		}
	case 2:
		for len(rows) < maxRows && n.batchRightRowPos < len(n.batchRightRows) {
			index := n.batchRightRowPos
			n.batchRightRowPos++
			if !n.batchRightMatched[index] {
				rows = append(rows, n.mergeRecordsWithLeftNull(n.batchRightRows[index]))
			}
		}
		if n.batchRightRowPos >= len(n.batchRightRows) {
			n.batchDone = true
		}
	default:
		n.batchDone = true
	}
	// A FULL join can finish its left phase without producing a row (for
	// example, when the final left row matched). Continue directly into the
	// unmatched-right phase instead of returning an empty, non-EOF batch that
	// would make VolcanoExecutor stop early.
	if len(rows) == 0 && n.batchPhase == 2 && !n.batchDone {
		for len(rows) < maxRows && n.batchRightRowPos < len(n.batchRightRows) {
			index := n.batchRightRowPos
			n.batchRightRowPos++
			if !n.batchRightMatched[index] {
				rows = append(rows, n.mergeRecordsWithLeftNull(n.batchRightRows[index]))
			}
		}
		if n.batchRightRowPos >= len(n.batchRightRows) {
			n.batchDone = true
		}
	}

	if len(rows) == 0 && n.batchDone {
		return nil, io.EOF
	}
	return rows, nil
}

func (n *NestedLoopJoinOperator) initializeBatchJoin(ctx context.Context) error {
	n.batchInitialized = true
	n.batchPhase = 1
	joinType := normalizedNestedLoopJoinType(n.joinType)
	switch joinType {
	case "RIGHT":
		rows, err := collectOperatorRowsInBatches(ctx, n.left)
		if err != nil {
			return err
		}
		n.batchLeftRows = rows
		n.batchRightSourceEOF = false
	case "FULL":
		leftRows, err := collectOperatorRowsInBatches(ctx, n.left)
		if err != nil {
			return err
		}
		rightRows, err := collectOperatorRowsInBatches(ctx, n.right)
		if err != nil {
			return err
		}
		n.batchLeftRows = leftRows
		n.batchRightRows = rightRows
		n.batchRightMatched = make([]bool, len(rightRows))
		n.batchLeftBatch = leftRows
		n.batchLeftSourceEOF = true
	default:
		rows, err := collectOperatorRowsInBatches(ctx, n.right)
		if err != nil {
			return err
		}
		n.batchRightRows = rows
	}
	return nil
}

func normalizedNestedLoopJoinType(joinType string) string {
	switch strings.ToUpper(strings.TrimSpace(joinType)) {
	case "RIGHT", "RIGHT OUTER":
		return "RIGHT"
	case "LEFT", "LEFT OUTER":
		return "LEFT"
	case "FULL", "FULL OUTER":
		return "FULL"
	default:
		return "INNER"
	}
}

func collectOperatorRowsInBatches(ctx context.Context, operator Operator) ([]Record, error) {
	rows := make([]Record, 0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := nextOperatorBatch(ctx, operator, 256)
		if err != nil && err != io.EOF {
			return nil, err
		}
		rows = append(rows, batch...)
		if err == io.EOF || len(batch) == 0 {
			return rows, nil
		}
	}
}

func (n *NestedLoopJoinOperator) nextBatchLeft(ctx context.Context, maxRows int) ([]Record, error) {
	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		if n.batchLeftBatchPos >= len(n.batchLeftBatch) {
			if n.batchLeftSourceEOF {
				if normalizedNestedLoopJoinType(n.joinType) == "FULL" {
					n.batchPhase = 2
					n.batchRightRowPos = 0
					return rows, nil
				}
				n.batchDone = true
				return rows, nil
			}
			batch, err := nextOperatorBatch(ctx, n.left, 256)
			if err != nil && err != io.EOF {
				return rows, err
			}
			n.batchLeftBatch = batch
			n.batchLeftBatchPos = 0
			n.batchLeftSourceEOF = err == io.EOF
			if len(batch) == 0 {
				continue
			}
		}

		leftRow := n.batchLeftBatch[n.batchLeftBatchPos]
		if n.batchRightRowPos < len(n.batchRightRows) {
			index := n.batchRightRowPos
			n.batchRightRowPos++
			rightRow := n.batchRightRows[index]
			if n.condition == nil || n.condition(leftRow, rightRow) {
				n.batchLeftHadMatch = true
				if len(n.batchRightMatched) > index {
					n.batchRightMatched[index] = true
				}
				rows = append(rows, n.mergeRecords(leftRow, rightRow))
			}
			continue
		}

		joinType := normalizedNestedLoopJoinType(n.joinType)
		if (joinType == "LEFT" || joinType == "FULL") && !n.batchLeftHadMatch {
			rows = append(rows, n.mergeRecordsWithRightNull(leftRow))
		}
		n.batchLeftBatchPos++
		n.batchRightRowPos = 0
		n.batchLeftHadMatch = false
	}
	return rows, nil
}

func (n *NestedLoopJoinOperator) nextBatchRight(ctx context.Context, maxRows int) ([]Record, error) {
	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		if n.batchRightBatchPos >= len(n.batchRightBatch) {
			if n.batchRightSourceEOF {
				n.batchDone = true
				return rows, nil
			}
			batch, err := nextOperatorBatch(ctx, n.right, 256)
			if err != nil && err != io.EOF {
				return rows, err
			}
			n.batchRightBatch = batch
			n.batchRightBatchPos = 0
			n.batchRightSourceEOF = err == io.EOF
			if len(batch) == 0 {
				continue
			}
		}

		rightRow := n.batchRightBatch[n.batchRightBatchPos]
		if n.batchLeftRowPos < len(n.batchLeftRows) {
			leftRow := n.batchLeftRows[n.batchLeftRowPos]
			n.batchLeftRowPos++
			if n.condition == nil || n.condition(leftRow, rightRow) {
				n.batchRightHadMatch = true
				rows = append(rows, n.mergeRecords(leftRow, rightRow))
			}
			continue
		}

		if !n.batchRightHadMatch {
			rows = append(rows, n.mergeRecordsWithLeftNull(rightRow))
		}
		n.batchRightBatchPos++
		n.batchLeftRowPos = 0
		n.batchRightHadMatch = false
	}
	return rows, nil
}

// nextInner 内连接：仅输出有匹配的行
func (n *NestedLoopJoinOperator) nextInner(ctx context.Context) (Record, error) {
	for {
		if n.leftRow == nil {
			leftRow, err := n.left.Next(ctx)
			if err != nil {
				return nil, err
			}
			if leftRow == nil {
				return nil, nil
			}
			n.leftRow = leftRow
			n.rightEOF = false
		}

		rightRow, err := n.right.Next(ctx)
		if err != nil {
			return nil, err
		}
		if rightRow == nil {
			n.right.Close()
			if err := n.right.Open(ctx); err != nil {
				return nil, fmt.Errorf("failed to reopen right child: %w", err)
			}
			n.leftRow = nil
			continue
		}

		if n.condition == nil || n.condition(n.leftRow, rightRow) {
			return n.mergeRecords(n.leftRow, rightRow), nil
		}
	}
}

// nextLeft 左外连接：左表每行至少输出一行，无匹配时右表填 NULL
func (n *NestedLoopJoinOperator) nextLeft(ctx context.Context) (Record, error) {
	for {
		if n.leftRow == nil {
			leftRow, err := n.left.Next(ctx)
			if err != nil {
				return nil, err
			}
			if leftRow == nil {
				return nil, nil
			}
			n.leftRow = leftRow
			n.rightEOF = false
			n.hadMatch = false
		}

		rightRow, err := n.right.Next(ctx)
		if err != nil {
			return nil, err
		}
		if rightRow == nil {
			n.right.Close()
			if err := n.right.Open(ctx); err != nil {
				return nil, fmt.Errorf("failed to reopen right child: %w", err)
			}
			if !n.hadMatch {
				out := n.mergeRecordsWithRightNull(n.leftRow)
				n.leftRow = nil
				return out, nil
			}
			n.leftRow = nil
			continue
		}

		if n.condition == nil || n.condition(n.leftRow, rightRow) {
			n.hadMatch = true
			return n.mergeRecords(n.leftRow, rightRow), nil
		}
	}
}

// nextRight 右外连接：右表为 outer，无匹配时左表填 NULL
func (n *NestedLoopJoinOperator) nextRight(ctx context.Context) (Record, error) {
	for {
		if n.rightRow == nil {
			rightRow, err := n.right.Next(ctx)
			if err != nil {
				return nil, err
			}
			if rightRow == nil {
				return nil, nil
			}
			n.rightRow = rightRow
			n.leftEOF = false
			n.hadMatch = false
		}

		leftRow, err := n.left.Next(ctx)
		if err != nil {
			return nil, err
		}
		if leftRow == nil {
			n.left.Close()
			if err := n.left.Open(ctx); err != nil {
				return nil, fmt.Errorf("failed to reopen left child: %w", err)
			}
			if !n.hadMatch {
				out := n.mergeRecordsWithLeftNull(n.rightRow)
				n.rightRow = nil
				return out, nil
			}
			n.rightRow = nil
			continue
		}

		if n.condition == nil || n.condition(leftRow, n.rightRow) {
			n.hadMatch = true
			return n.mergeRecords(leftRow, n.rightRow), nil
		}
	}
}

// nextFull 全外连接：LEFT 阶段 + 未匹配的右行补 NULL
func (n *NestedLoopJoinOperator) nextFull(ctx context.Context) (Record, error) {
	if n.phase == 0 {
		n.phase = 1
	}
	if n.phase == 1 {
		rec, err := n.nextLeft(ctx)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			return rec, nil
		}
		// LEFT 阶段结束，进入阶段 2：输出未匹配的右行
		n.phase = 2
		n.right.Close()
		if err := n.right.Open(ctx); err != nil {
			return nil, fmt.Errorf("failed to reopen right for FULL phase 2: %w", err)
		}
	}
	// phase 2: 对每个右行检查是否有左行与之匹配，无则输出 (NULLs, right)
	for {
		rightRow, err := n.right.Next(ctx)
		if err != nil {
			return nil, err
		}
		if rightRow == nil {
			return nil, nil
		}
		// 扫描左表看是否有匹配
		n.left.Close()
		if err := n.left.Open(ctx); err != nil {
			return nil, err
		}
		matched := false
		for {
			leftRow, err := n.left.Next(ctx)
			if err != nil {
				return nil, err
			}
			if leftRow == nil {
				break
			}
			if n.condition != nil && n.condition(leftRow, rightRow) {
				matched = true
				break
			}
		}
		if !matched {
			return n.mergeRecordsWithLeftNull(rightRow), nil
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

// mergeRecordsWithRightNull 左表有值、右表填 NULL（用于 LEFT 无匹配）
func (n *NestedLoopJoinOperator) mergeRecordsWithRightNull(left Record) Record {
	leftValues := left.GetValues()
	mergedValues := make([]basic.Value, len(leftValues)+n.rightColCount)
	copy(mergedValues, leftValues)
	for i := len(leftValues); i < len(mergedValues); i++ {
		mergedValues[i] = basic.NewNull()
	}
	return NewExecutorRecordFromValues(mergedValues, n.schema)
}

// mergeRecordsWithLeftNull 左表填 NULL、右表有值（用于 RIGHT/FULL 无匹配）
func (n *NestedLoopJoinOperator) mergeRecordsWithLeftNull(right Record) Record {
	rightValues := right.GetValues()
	mergedValues := make([]basic.Value, n.leftColCount+len(rightValues))
	for i := 0; i < n.leftColCount; i++ {
		mergedValues[i] = basic.NewNull()
	}
	copy(mergedValues[n.leftColCount:], rightValues)
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

	// 哈希表构建：key -> build 行在 buildRowsList 中的下标
	buildKey      func(Record) string
	probeKey      func(Record) string
	hashTable     map[string][]int
	buildRowsList []Record

	// 探测状态
	built          bool
	probeRow       Record
	matchedRows    []int // 当前 probe 匹配的 build 行下标
	matchedIdx     int
	batchProbeRows []Record
	batchProbePos  int
	batchProbeEOF  bool

	// 外连接：输出顺序及补 NULL
	outputProbeFirst bool // true=LEFT 时输出 (probe, build)
	buildColCount    int
	probeColCount    int
	// FULL 阶段 2
	matchedBuild     []bool // 对应 build 行是否被匹配过
	fullPhase        int    // 1=probe 阶段，2=输出未匹配 build
	fullUnmatchedIdx int
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
		buildSide:     buildSide,
		probeSide:     probeSide,
		joinType:      joinType,
		buildKey:      buildKey,
		probeKey:      probeKey,
		hashTable:     make(map[string][]int),
		buildRowsList: nil,
	}
}

func (h *HashJoinOperator) Open(ctx context.Context) error {
	if err := h.BaseOperator.Open(ctx); err != nil {
		return err
	}

	buildSchema := h.buildSide.Schema()
	probeSchema := h.probeSide.Schema()
	if buildSchema != nil && probeSchema != nil {
		h.schema = metadata.MergeSchemas(buildSchema, probeSchema)
		h.buildColCount = buildSchema.ColumnCount()
		h.probeColCount = probeSchema.ColumnCount()
	} else if buildSchema != nil {
		h.schema = buildSchema.Clone()
		h.buildColCount = buildSchema.ColumnCount()
	} else if probeSchema != nil {
		h.schema = probeSchema.Clone()
		h.probeColCount = probeSchema.ColumnCount()
	} else {
		h.schema = metadata.NewQuerySchema()
	}

	// LEFT/FULL 时 build=右表、probe=左表，输出顺序为 (probe, build) = (左, 右)
	h.outputProbeFirst = (h.joinType == "LEFT" || h.joinType == "LEFT OUTER" ||
		h.joinType == "FULL" || h.joinType == "FULL OUTER")
	h.built = false
	h.hashTable = make(map[string][]int)
	h.buildRowsList = nil
	h.probeRow = nil
	h.matchedRows = nil
	h.matchedIdx = 0
	h.batchProbeRows = nil
	h.batchProbePos = 0
	h.batchProbeEOF = false
	h.fullPhase = 0
	h.fullUnmatchedIdx = 0
	h.matchedBuild = nil

	return nil
}

func (h *HashJoinOperator) Next(ctx context.Context) (Record, error) {
	if !h.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	if !h.built {
		if err := h.buildHashTable(ctx); err != nil {
			return nil, fmt.Errorf("failed to build hash table: %w", err)
		}
		h.built = true
		h.fullPhase = 1
		if h.joinType == "FULL" || h.joinType == "FULL OUTER" {
			h.fullUnmatchedIdx = 0
		}
		logger.Debugf("HashJoin: built hash table with %d keys", len(h.hashTable))
	}

	// FULL 阶段 2：输出未匹配的 build 行 (NULLs, build)
	if (h.joinType == "FULL" || h.joinType == "FULL OUTER") && h.fullPhase == 2 {
		for h.fullUnmatchedIdx < len(h.buildRowsList) {
			if !h.matchedBuild[h.fullUnmatchedIdx] {
				buildRow := h.buildRowsList[h.fullUnmatchedIdx]
				h.fullUnmatchedIdx++
				return h.mergeRecordsWithLeftNull(h.probeColCount, buildRow), nil
			}
			h.fullUnmatchedIdx++
		}
		return nil, nil
	}

	// 探测阶段
	for {
		if h.matchedIdx >= len(h.matchedRows) {
			probeRow, err := h.probeSide.Next(ctx)
			if err != nil {
				return nil, err
			}
			if probeRow == nil {
				if (h.joinType == "FULL" || h.joinType == "FULL OUTER") && h.fullPhase == 1 {
					h.fullPhase = 2
					h.fullUnmatchedIdx = 0
					return h.Next(ctx)
				}
				return nil, nil
			}

			h.probeRow = probeRow
			key := h.probeKey(probeRow)
			h.matchedRows = h.hashTable[key]
			h.matchedIdx = 0

			if len(h.matchedRows) == 0 {
				switch h.joinType {
				case "LEFT", "LEFT OUTER":
					return h.mergeRecordsWithRightNull(probeRow), nil
				case "RIGHT", "RIGHT OUTER":
					return h.mergeRecordsWithLeftNull(h.buildColCount, probeRow), nil
				case "FULL", "FULL OUTER":
					return h.mergeRecordsWithRightNull(probeRow), nil
				default:
					continue
				}
			}
		}

		buildIdx := h.matchedRows[h.matchedIdx]
		h.matchedIdx++
		buildRow := h.buildRowsList[buildIdx]
		if h.joinType == "FULL" || h.joinType == "FULL OUTER" {
			h.matchedBuild[buildIdx] = true
		}

		if h.outputProbeFirst {
			return h.mergeRecords(h.probeRow, buildRow), nil
		}
		return h.mergeRecords(buildRow, h.probeRow), nil
	}
}

func (h *HashJoinOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !h.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	if !h.built {
		if err := h.buildHashTable(ctx); err != nil {
			return nil, fmt.Errorf("failed to build hash table: %w", err)
		}
		h.built = true
		h.fullPhase = 1
		if h.joinType == "FULL" || h.joinType == "FULL OUTER" {
			h.fullUnmatchedIdx = 0
		}
	}

	rows := make([]Record, 0, maxRows)
	for len(rows) < maxRows {
		if (h.joinType == "FULL" || h.joinType == "FULL OUTER") && h.fullPhase == 2 {
			for len(rows) < maxRows && h.fullUnmatchedIdx < len(h.buildRowsList) {
				if !h.matchedBuild[h.fullUnmatchedIdx] {
					rows = append(rows, h.mergeRecordsWithLeftNull(h.probeColCount, h.buildRowsList[h.fullUnmatchedIdx]))
				}
				h.fullUnmatchedIdx++
			}
			if h.fullUnmatchedIdx >= len(h.buildRowsList) {
				if len(rows) == 0 {
					return nil, io.EOF
				}
				return rows, io.EOF
			}
			continue
		}

		if h.matchedIdx < len(h.matchedRows) {
			buildIdx := h.matchedRows[h.matchedIdx]
			h.matchedIdx++
			buildRow := h.buildRowsList[buildIdx]
			if h.joinType == "FULL" || h.joinType == "FULL OUTER" {
				h.matchedBuild[buildIdx] = true
			}
			if h.outputProbeFirst {
				rows = append(rows, h.mergeRecords(h.probeRow, buildRow))
			} else {
				rows = append(rows, h.mergeRecords(buildRow, h.probeRow))
			}
			continue
		}
		h.matchedRows = nil
		h.matchedIdx = 0
		h.probeRow = nil

		if h.batchProbePos >= len(h.batchProbeRows) {
			if h.batchProbeEOF {
				if h.joinType == "FULL" || h.joinType == "FULL OUTER" {
					h.fullPhase = 2
					continue
				}
				if len(rows) == 0 {
					return nil, io.EOF
				}
				return rows, io.EOF
			}
			batch, err := nextOperatorBatch(ctx, h.probeSide, 256)
			if err != nil && err != io.EOF {
				return nil, err
			}
			h.batchProbeRows = batch
			h.batchProbePos = 0
			h.batchProbeEOF = err == io.EOF
			if len(batch) == 0 {
				continue
			}
		}

		h.probeRow = h.batchProbeRows[h.batchProbePos]
		h.batchProbePos++
		h.matchedRows = h.hashTable[h.probeKey(h.probeRow)]
		if len(h.matchedRows) == 0 {
			switch h.joinType {
			case "LEFT", "LEFT OUTER", "FULL", "FULL OUTER":
				rows = append(rows, h.mergeRecordsWithRightNull(h.probeRow))
			case "RIGHT", "RIGHT OUTER":
				rows = append(rows, h.mergeRecordsWithLeftNull(h.buildColCount, h.probeRow))
			}
			h.probeRow = nil
		}
	}
	return rows, nil
}

func (h *HashJoinOperator) buildHashTable(ctx context.Context) error {
	h.buildRowsList = make([]Record, 0)
	for {
		batch, err := nextOperatorBatch(ctx, h.buildSide, 256)
		if err != nil && err != io.EOF {
			return err
		}
		for _, record := range batch {
			idx := len(h.buildRowsList)
			h.buildRowsList = append(h.buildRowsList, record)
			key := h.buildKey(record)
			h.hashTable[key] = append(h.hashTable[key], idx)
		}
		if err == io.EOF {
			break
		}
	}
	if h.joinType == "FULL" || h.joinType == "FULL OUTER" {
		h.matchedBuild = make([]bool, len(h.buildRowsList))
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

// mergeRecordsWithRightNull 左有值、右补 NULL（probe 端 + buildColCount 个 NULL）
func (h *HashJoinOperator) mergeRecordsWithRightNull(probe Record) Record {
	vals := probe.GetValues()
	merged := make([]basic.Value, len(vals)+h.buildColCount)
	copy(merged, vals)
	for i := len(vals); i < len(merged); i++ {
		merged[i] = basic.NewNull()
	}
	return NewExecutorRecordFromValues(merged, h.schema)
}

// mergeRecordsWithLeftNull 左补 NULL、右有值（leftNullCount 个 NULL + record）
func (h *HashJoinOperator) mergeRecordsWithLeftNull(leftNullCount int, record Record) Record {
	vals := record.GetValues()
	merged := make([]basic.Value, leftNullCount+len(vals))
	for i := 0; i < leftNullCount; i++ {
		merged[i] = basic.NewNull()
	}
	copy(merged[leftNullCount:], vals)
	return NewExecutorRecordFromValues(merged, h.schema)
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

type MultiInputAggregateFunc interface {
	AggregateFunc
	UpdateValues(values []basic.Value)
}

func aggregateDistinctKey(value basic.Value) string {
	if value == nil || value.IsNull() {
		return "<NULL>"
	}
	return fmt.Sprintf("%s:%s", value.Type(), value.String())
}

func aggregateDistinctValuesKey(values []basic.Value) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = aggregateDistinctKey(value)
	}
	return strings.Join(parts, "\x00")
}

// CountAgg COUNT聚合
type CountAgg struct {
	count       int64
	distinct    bool
	countColumn bool
	seen        map[string]struct{}
}

func (c *CountAgg) Init() {
	c.count = 0
	if c.distinct {
		c.seen = make(map[string]struct{})
	} else {
		c.seen = nil
	}
}
func (c *CountAgg) Update(value basic.Value) {
	c.UpdateValues([]basic.Value{value})
}
func (c *CountAgg) UpdateValues(values []basic.Value) {
	if len(values) == 0 {
		c.count++
		return
	}
	if c.countColumn {
		for _, value := range values {
			if value == nil || value.IsNull() {
				return
			}
		}
	}
	if c.distinct {
		key := aggregateDistinctValuesKey(values)
		if _, exists := c.seen[key]; exists {
			return
		}
		c.seen[key] = struct{}{}
	}
	c.count++
}
func (c *CountAgg) Result() basic.Value           { return basic.NewInt64Value(c.count) }
func (c *CountAgg) Name() string                  { return "COUNT" }
func (c *CountAgg) ResultType() metadata.DataType { return metadata.TypeBigInt }

// SumAgg SUM聚合
type SumAgg struct {
	sum      float64
	distinct bool
	seen     map[string]struct{}
}

func (s *SumAgg) Init() {
	s.sum = 0
	if s.distinct {
		s.seen = make(map[string]struct{})
	} else {
		s.seen = nil
	}
}
func (s *SumAgg) Update(value basic.Value) {
	if value != nil && !value.IsNull() {
		if s.distinct {
			key := aggregateDistinctKey(value)
			if _, exists := s.seen[key]; exists {
				return
			}
			s.seen[key] = struct{}{}
		}
		s.sum += value.Float64()
	}
}
func (s *SumAgg) Result() basic.Value           { return basic.NewFloatValue(s.sum) }
func (s *SumAgg) Name() string                  { return "SUM" }
func (s *SumAgg) ResultType() metadata.DataType { return metadata.TypeDouble }

// AvgAgg AVG聚合
type AvgAgg struct {
	sum      float64
	count    int64
	distinct bool
	seen     map[string]struct{}
}

func (a *AvgAgg) Init() {
	a.sum = 0
	a.count = 0
	if a.distinct {
		a.seen = make(map[string]struct{})
	} else {
		a.seen = nil
	}
}

func (a *AvgAgg) Update(value basic.Value) {
	if value != nil && !value.IsNull() {
		if a.distinct {
			key := aggregateDistinctKey(value)
			if _, exists := a.seen[key]; exists {
				return
			}
			a.seen[key] = struct{}{}
		}
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
	distinct    bool
	seen        map[string]struct{}
}

func (m *MinAgg) Init() {
	m.initialized = false
	m.min = basic.NewNull()
	if m.distinct {
		m.seen = make(map[string]struct{})
	} else {
		m.seen = nil
	}
}

func (m *MinAgg) Update(value basic.Value) {
	if value == nil || value.IsNull() {
		return
	}
	if m.distinct {
		key := aggregateDistinctKey(value)
		if _, exists := m.seen[key]; exists {
			return
		}
		m.seen[key] = struct{}{}
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
	distinct    bool
	seen        map[string]struct{}
}

func (m *MaxAgg) Init() {
	m.initialized = false
	m.max = basic.NewNull()
	if m.distinct {
		m.seen = make(map[string]struct{})
	} else {
		m.seen = nil
	}
}

func (m *MaxAgg) Update(value basic.Value) {
	if value == nil || value.IsNull() {
		return
	}
	if m.distinct {
		key := aggregateDistinctKey(value)
		if _, exists := m.seen[key]; exists {
			return
		}
		m.seen[key] = struct{}{}
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

// GroupConcatAgg implements the common Volcano GROUP_CONCAT form. Ordering
// is handled by the query's sort plan; this state owns NULL filtering,
// DISTINCT elimination and separator application.
type GroupConcatAgg struct {
	values    []string
	seen      map[string]struct{}
	separator string
	distinct  bool
}

func (g *GroupConcatAgg) Init() {
	g.values = nil
	if g.distinct {
		g.seen = make(map[string]struct{})
	} else {
		g.seen = nil
	}
}

func (g *GroupConcatAgg) Update(value basic.Value) {
	if value == nil || value.IsNull() {
		return
	}
	text := value.String()
	if g.distinct {
		if _, exists := g.seen[text]; exists {
			return
		}
		g.seen[text] = struct{}{}
	}
	g.values = append(g.values, text)
}

func (g *GroupConcatAgg) Result() basic.Value {
	if len(g.values) == 0 {
		return basic.NewNull()
	}
	separator := g.separator
	if separator == "" {
		separator = ","
	}
	return basic.NewString(strings.Join(g.values, separator))
}

func (g *GroupConcatAgg) Name() string                  { return "GROUP_CONCAT" }
func (g *GroupConcatAgg) ResultType() metadata.DataType { return metadata.TypeText }

type JSONArrayAgg struct {
	values []interface{}
}

func (j *JSONArrayAgg) Init() { j.values = nil }

func (j *JSONArrayAgg) Update(value basic.Value) {
	if value == nil || value.IsNull() {
		j.values = append(j.values, nil)
		return
	}
	j.values = append(j.values, jsonAggregateValue(value))
}

func jsonAggregateValue(value basic.Value) interface{} {
	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt, basic.ValueTypeInt, basic.ValueTypeBigInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble, basic.ValueTypeDecimal:
		return value.Float64()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		return value.Bool()
	case basic.ValueTypeJSON:
		text := value.String()
		if json.Valid([]byte(text)) {
			return json.RawMessage(text)
		}
	}
	return value.String()
}

func (j *JSONArrayAgg) Result() basic.Value {
	encoded, err := json.Marshal(j.values)
	if err != nil {
		return basic.NewNull()
	}
	return basic.NewString(string(encoded))
}

func (j *JSONArrayAgg) Name() string                  { return "JSON_ARRAYAGG" }
func (j *JSONArrayAgg) ResultType() metadata.DataType { return metadata.TypeJSON }

type JSONObjectAgg struct {
	keys   []string
	values []interface{}
}

func (j *JSONObjectAgg) Init() {
	j.keys = nil
	j.values = nil
}

func (j *JSONObjectAgg) Update(value basic.Value) {}

func (j *JSONObjectAgg) UpdateValues(values []basic.Value) {
	if len(values) != 2 || values[0] == nil || values[0].IsNull() {
		return
	}
	key := values[0].String()
	var converted interface{}
	if values[1] != nil && !values[1].IsNull() {
		converted = jsonAggregateValue(values[1])
	}
	for index, existing := range j.keys {
		if existing == key {
			j.values[index] = converted
			return
		}
	}
	j.keys = append(j.keys, key)
	j.values = append(j.values, converted)
}

func (j *JSONObjectAgg) Result() basic.Value {
	object := make(map[string]interface{}, len(j.keys))
	for index, key := range j.keys {
		object[key] = j.values[index]
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return basic.NewNull()
	}
	return basic.NewString(string(encoded))
}

func (j *JSONObjectAgg) Name() string                  { return "JSON_OBJECTAGG" }
func (j *JSONObjectAgg) ResultType() metadata.DataType { return metadata.TypeJSON }

type AnyValueAgg struct {
	value basic.Value
	set   bool
}

func (a *AnyValueAgg) Init() {
	a.value = basic.NewNull()
	a.set = false
}

func (a *AnyValueAgg) Update(value basic.Value) {
	if !a.set {
		if value == nil {
			a.value = basic.NewNull()
		} else {
			a.value = value
		}
		a.set = true
	}
}

func (a *AnyValueAgg) Result() basic.Value {
	if !a.set || a.value == nil {
		return basic.NewNull()
	}
	return a.value
}

func (a *AnyValueAgg) Name() string                  { return "ANY_VALUE" }
func (a *AnyValueAgg) ResultType() metadata.DataType { return metadata.TypeVarchar }

type BitAgg struct {
	name        string
	value       uint64
	initialized bool
}

func (b *BitAgg) Init() {
	b.initialized = false
	if b.name == "BIT_AND" {
		b.value = ^uint64(0)
	} else {
		b.value = 0
	}
}

func (b *BitAgg) Update(value basic.Value) {
	if value == nil || value.IsNull() {
		return
	}
	unsigned := uint64(value.Int())
	switch b.name {
	case "BIT_AND":
		b.value &= unsigned
	case "BIT_OR":
		b.value |= unsigned
	case "BIT_XOR":
		b.value ^= unsigned
	}
	b.initialized = true
}

func (b *BitAgg) Result() basic.Value {
	if !b.initialized && b.name != "BIT_AND" {
		return basic.NewInt64Value(0)
	}
	return basic.NewInt64Value(int64(b.value))
}

func (b *BitAgg) Name() string                  { return b.name }
func (b *BitAgg) ResultType() metadata.DataType { return metadata.TypeBigInt }

type VarianceAgg struct {
	name  string
	count int64
	sum   float64
	mean  float64
	m2    float64
}

func (v *VarianceAgg) Init() {
	v.count = 0
	v.sum = 0
	v.mean = 0
	v.m2 = 0
}

func (v *VarianceAgg) Update(value basic.Value) {
	if value == nil || value.IsNull() {
		return
	}
	x := value.Float64()
	v.count++
	v.sum += x
	delta := x - v.mean
	v.mean += delta / float64(v.count)
	v.m2 += delta * (x - v.mean)
}

func (v *VarianceAgg) Result() basic.Value {
	if v.count == 0 || (strings.HasSuffix(v.name, "_SAMP") && v.count < 2) {
		return basic.NewNull()
	}
	denominator := float64(v.count)
	if strings.HasSuffix(v.name, "_SAMP") {
		denominator = float64(v.count - 1)
	}
	variance := v.m2 / denominator
	if strings.HasPrefix(v.name, "STD") || v.name == "STD" {
		return basic.NewFloatValue(math.Sqrt(variance))
	}
	return basic.NewFloatValue(variance)
}

func (v *VarianceAgg) Name() string                  { return v.name }
func (v *VarianceAgg) ResultType() metadata.DataType { return metadata.TypeDouble }

// HashAggregateOperator 哈希聚合算子
type HashAggregateOperator struct {
	BaseOperator
	child        Operator
	groupByExprs []int // 分组列索引
	aggFuncs     []AggregateFunc

	// 聚合状态
	hashTable       map[string][]AggregateFunc
	computed        bool
	results         []Record
	resultIdx       int
	aggregateInputs [][]int
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

func NewHashAggregateOperatorWithExpressions(
	child Operator,
	groupByExprs []int,
	aggFuncs []AggregateFunc,
	planAggFuncs []plan.AggregateFunc,
) *HashAggregateOperator {
	operator := NewHashAggregateOperator(child, groupByExprs, aggFuncs)
	operator.aggregateInputs = make([][]int, len(planAggFuncs))
	if child == nil || child.Schema() == nil {
		return operator
	}
	for index, aggregate := range planAggFuncs {
		function, ok := aggregate.(*plan.Function)
		if !ok {
			continue
		}
		operator.aggregateInputs[index] = make([]int, len(function.FuncArgs))
		for argIndex, expression := range function.FuncArgs {
			column, ok := expression.(*plan.Column)
			if !ok || column == nil {
				operator.aggregateInputs[index][argIndex] = -1
				continue
			}
			operator.aggregateInputs[index][argIndex] = aggregateColumnIndex(child.Schema(), column.Name)
		}
	}
	return operator
}

func aggregateColumnIndex(schema *metadata.QuerySchema, name string) int {
	if schema == nil {
		return -1
	}
	for index := 0; index < schema.ColumnCount(); index++ {
		column, ok := schema.GetColumnByIndex(index)
		if ok && column != nil && strings.EqualFold(column.Name, name) {
			return index
		}
	}
	return -1
}

func (h *HashAggregateOperator) Open(ctx context.Context) error {
	if err := h.BaseOperator.Open(ctx); err != nil {
		return err
	}
	h.hashTable = make(map[string][]AggregateFunc)
	h.computed = false
	h.results = nil
	h.resultIdx = 0

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
	if err := h.ensureComputed(ctx); err != nil {
		return nil, err
	}

	// 返回下一个聚合结果
	if h.resultIdx >= len(h.results) {
		return nil, nil // EOF
	}

	result := h.results[h.resultIdx]
	h.resultIdx++
	return result, nil
}

func (h *HashAggregateOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !h.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	if err := h.ensureComputed(ctx); err != nil {
		return nil, err
	}
	if h.resultIdx >= len(h.results) {
		return nil, io.EOF
	}

	end := h.resultIdx + maxRows
	if end > len(h.results) {
		end = len(h.results)
	}
	batch := h.results[h.resultIdx:end]
	h.resultIdx = end
	if h.resultIdx >= len(h.results) {
		return batch, io.EOF
	}
	return batch, nil
}

func (h *HashAggregateOperator) ensureComputed(ctx context.Context) error {
	if h.computed {
		return nil
	}
	if err := h.computeAggregates(ctx); err != nil {
		return fmt.Errorf("failed to compute aggregates: %w", err)
	}
	h.computed = true
	return nil
}

func (h *HashAggregateOperator) computeAggregates(ctx context.Context) error {
	// 遍历所有输入行
	for {
		rows, err := nextOperatorBatch(ctx, h.child, 256)
		if err != nil && err != io.EOF {
			return err
		}
		for _, record := range rows {
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
						countAgg := *fn.(*CountAgg)
						aggStates[i] = &countAgg
					case *SumAgg:
						sumAgg := *fn.(*SumAgg)
						aggStates[i] = &sumAgg
					case *AvgAgg:
						avgAgg := *fn.(*AvgAgg)
						aggStates[i] = &avgAgg
					case *MinAgg:
						minAgg := *fn.(*MinAgg)
						aggStates[i] = &minAgg
					case *MaxAgg:
						maxAgg := *fn.(*MaxAgg)
						aggStates[i] = &maxAgg
					case *GroupConcatAgg:
						groupConcat := *fn.(*GroupConcatAgg)
						aggStates[i] = &groupConcat
					case *JSONArrayAgg:
						jsonArray := *fn.(*JSONArrayAgg)
						aggStates[i] = &jsonArray
					case *JSONObjectAgg:
						jsonObject := *fn.(*JSONObjectAgg)
						aggStates[i] = &jsonObject
					case *AnyValueAgg:
						anyValue := *fn.(*AnyValueAgg)
						aggStates[i] = &anyValue
					case *BitAgg:
						bitAgg := *fn.(*BitAgg)
						aggStates[i] = &bitAgg
					case *VarianceAgg:
						varianceAgg := *fn.(*VarianceAgg)
						aggStates[i] = &varianceAgg
					}
					aggStates[i].Init()
				}
				h.hashTable[groupKey] = aggStates
			}

			// 更新聚合状态
			values := record.GetValues()
			for i, aggState := range aggStates {
				inputs := h.getAggregateInputValues(values, i)
				if multiInput, ok := aggState.(MultiInputAggregateFunc); ok {
					multiInput.UpdateValues(inputs)
				} else if len(inputs) > 0 {
					aggState.Update(inputs[0])
				}
			}
		}
		if err == io.EOF {
			break
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

func (h *HashAggregateOperator) getAggregateInputValue(values []basic.Value, aggIndex int) basic.Value {
	inputs := h.getAggregateInputValues(values, aggIndex)
	if len(inputs) > 0 {
		return inputs[0]
	}
	return basic.NewNull()
}

func (h *HashAggregateOperator) getAggregateInputValues(values []basic.Value, aggIndex int) []basic.Value {
	if aggIndex >= 0 && aggIndex < len(h.aggregateInputs) && len(h.aggregateInputs[aggIndex]) > 0 {
		inputs := make([]basic.Value, len(h.aggregateInputs[aggIndex]))
		for index, inputIndex := range h.aggregateInputs[aggIndex] {
			if inputIndex >= 0 && inputIndex < len(values) {
				inputs[index] = values[inputIndex]
			} else {
				inputs[index] = basic.NewNull()
			}
		}
		return inputs
	}
	if len(values) == 0 {
		return []basic.Value{basic.NewNull()}
	}

	firstAggColumn := len(h.groupByExprs)
	if firstAggColumn < len(values) {
		candidateIdx := firstAggColumn + aggIndex
		if candidateIdx < len(values) {
			return []basic.Value{values[candidateIdx]}
		}
	}

	// Fallback for the current simplified aggregate API: if aggregate expressions
	// are not explicitly tracked, apply non-grouped aggregates to the last input
	// column so multiple aggregate functions can still consume the same measure.
	return []basic.Value{values[len(values)-1]}
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
	s.sorted = false
	s.results = nil
	s.resultIdx = 0
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

func (s *SortOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !s.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	if !s.sorted {
		if err := s.sortRecords(ctx); err != nil {
			return nil, fmt.Errorf("failed to sort records: %w", err)
		}
		s.sorted = true
	}
	if s.resultIdx >= len(s.results) {
		return nil, io.EOF
	}

	end := s.resultIdx + maxRows
	if end > len(s.results) {
		end = len(s.results)
	}
	batch := s.results[s.resultIdx:end]
	s.resultIdx = end
	if s.resultIdx >= len(s.results) {
		return batch, io.EOF
	}
	return batch, nil
}

func (s *SortOperator) sortRecords(ctx context.Context) error {
	// Read all records through bounded batches when the child supports them.
	s.results = make([]Record, 0)
	for {
		batch, err := nextOperatorBatch(ctx, s.child, 256)
		if err != nil && err != io.EOF {
			return err
		}
		s.results = append(s.results, batch...)
		if err == io.EOF {
			break
		}
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

func (l *LimitOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !l.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}

	if l.currentRow < l.offset {
		toSkip := l.offset - l.currentRow
		if toSkip > int64(maxRows) {
			toSkip = int64(maxRows)
		}
		batch, err := nextOperatorBatch(ctx, l.child, int(toSkip))
		l.currentRow += int64(len(batch))
		if err != nil {
			return nil, err
		}
		if l.currentRow < l.offset {
			return nil, nil
		}
	}

	if l.limit > 0 && l.currentRow >= l.offset+l.limit {
		return nil, io.EOF
	}
	batchSize := maxRows
	if l.limit > 0 {
		remaining := l.offset + l.limit - l.currentRow
		if remaining < int64(batchSize) {
			batchSize = int(remaining)
		}
	}
	batch, err := nextOperatorBatch(ctx, l.child, batchSize)
	l.currentRow += int64(len(batch))
	return batch, err
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
	executed     bool        // 非关联子查询是否已经物化
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
	s.result = nil
	s.resultSet = nil
	s.executed = false
	if s.subplan == nil {
		return fmt.Errorf("subquery plan is nil")
	}

	// 如果是非关联子查询，可以在Open阶段执行
	if !s.correlated {
		if err := s.executeSubquery(ctx, nil); err != nil {
			return err
		}
		s.executed = true
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
	if !s.opened {
		return fmt.Errorf("operator not opened")
	}
	if !s.correlated && s.executed {
		// 非关联子查询只需执行一次；NULL 也是一个有效的已物化结果。
		return nil
	}
	if err := s.executeSubquery(ctx, outerRow); err != nil {
		return err
	}
	if !s.correlated {
		s.executed = true
	}
	return nil
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
	// 收集所有结果。优先走批量接口，避免 IN 子查询在物化边界退化为逐行拉取。
	s.resultSet = make([]Record, 0)

	for {
		records, err := nextOperatorBatch(ctx, s.subplan, 256)
		if err != nil && err != io.EOF {
			return fmt.Errorf("IN subquery error: %w", err)
		}
		s.resultSet = append(s.resultSet, records...)
		if err == io.EOF {
			break
		}
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
	// 收集所有结果。ANY/ALL 与 IN 共用批量物化路径。
	s.resultSet = make([]Record, 0)

	for {
		records, err := nextOperatorBatch(ctx, s.subplan, 256)
		if err != nil && err != io.EOF {
			return fmt.Errorf("quantified subquery error: %w", err)
		}
		s.resultSet = append(s.resultSet, records...)
		if err == io.EOF {
			break
		}
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

	batchStarted   bool
	batchOuterRows []Record
	batchOuterPos  int
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
	a.batchStarted = false
	a.batchOuterRows = nil
	a.batchOuterPos = 0
	a.outerRow = nil
	a.innerRows = nil
	a.innerIndex = 0

	return nil
}

func (a *ApplyOperator) Next(ctx context.Context) (Record, error) {
	if !a.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if a.batchStarted {
		return nil, fmt.Errorf("cannot use row execution after batch execution started")
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
				matchedOuter := a.outerRow
				a.outerRow = nil // 标记当前外层记录已处理
				a.innerRows = nil
				return matchedOuter, nil
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
				a.outerRow = nil
				a.innerRows = nil
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

// NextBatch keeps correlated Apply bounded at the result boundary while
// pulling both the outer stream and each re-opened inner plan through the
// optional batch contract.
func (a *ApplyOperator) NextBatch(ctx context.Context, maxRows int) ([]Record, error) {
	if !a.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	a.batchStarted = true
	rows := make([]Record, 0, maxRows)

	for len(rows) < maxRows {
		if a.outerRow != nil && a.innerIndex < len(a.innerRows) {
			innerRow := a.innerRows[a.innerIndex]
			a.innerIndex++
			switch strings.ToUpper(strings.TrimSpace(a.applyType)) {
			case "INNER", "LEFT":
				rows = append(rows, a.mergeRecords(a.outerRow, innerRow))
			case "SEMI":
				rows = append(rows, a.outerRow)
				a.outerRow = nil
				a.innerRows = nil
			case "ANTI":
				a.outerRow = nil
				a.innerRows = nil
			}
			continue
		}

		// The current outer row has no more inner results. Move to the next
		// outer batch before executing the inner plan again.
		a.outerRow = nil
		a.innerRows = nil
		a.innerIndex = 0
		if a.batchOuterPos >= len(a.batchOuterRows) {
			batch, err := nextOperatorBatch(ctx, a.outer, 256)
			if err != nil && err != io.EOF {
				return nil, err
			}
			a.batchOuterRows = batch
			a.batchOuterPos = 0
			if len(batch) == 0 {
				return rows, io.EOF
			}
		}

		outerRow := a.batchOuterRows[a.batchOuterPos]
		a.batchOuterPos++
		a.outerRow = outerRow
		if err := a.executeInnerForOuter(ctx, outerRow); err != nil {
			return nil, fmt.Errorf("failed to execute inner for outer: %w", err)
		}

		switch strings.ToUpper(strings.TrimSpace(a.applyType)) {
		case "INNER":
			if len(a.innerRows) == 0 {
				a.outerRow = nil
			}
		case "LEFT":
			if len(a.innerRows) == 0 {
				rows = append(rows, a.mergeRecords(outerRow, nil))
				a.outerRow = nil
			}
		case "SEMI":
			if len(a.innerRows) > 0 {
				rows = append(rows, outerRow)
			}
			a.outerRow = nil
			a.innerRows = nil
		case "ANTI":
			if len(a.innerRows) == 0 {
				rows = append(rows, outerRow)
			}
			a.outerRow = nil
			a.innerRows = nil
		}
	}
	return rows, nil
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
		batch, err := nextOperatorBatch(ctx, a.inner, 256)
		if err != nil && err != io.EOF {
			return err
		}
		for _, innerRow := range batch {
			// 检查关联条件
			if a.evaluateJoinConditions(outerRow, innerRow) {
				a.innerRows = append(a.innerRows, innerRow)

				// SEMI/ANTI JOIN只需要知道是否有匹配，不需要所有结果
				if a.applyType == "SEMI" || a.applyType == "ANTI" {
					return nil
				}
			}
		}
		if err == io.EOF || len(batch) == 0 {
			break
		}
	}

	return nil
}

// evaluateJoinConditions 评估关联条件
func (a *ApplyOperator) evaluateJoinConditions(outerRow, innerRow Record) bool {
	if len(a.joinConds) == 0 {
		return true // 没有条件，总是匹配
	}
	if a == nil {
		return false
	}
	if outerRow == nil || innerRow == nil {
		return false
	}

	// 构造表达式求值上下文
	evalCtx := &plan.EvalContext{Row: make(map[string]interface{})}

	// 先放入外层列
	outerValues := outerRow.GetValues()
	if outerSchema := a.rowSchema(outerRow, a.outerSchema); outerSchema != nil {
		for i := 0; i < outerSchema.ColumnCount() && i < len(outerValues); i++ {
			if col, ok := outerSchema.GetColumnByIndex(i); ok && col != nil {
				value := a.valueToInterfaceForJoin(outerValues[i])
				evalCtx.Row[col.Name] = value
				tableName := col.TableName
				if tableName == "" {
					tableName = outerSchema.TableName
				}
				if tableName != "" {
					evalCtx.Row[tableName+"."+col.Name] = value
				}
			}
		}
	}

	// 再放入内层列
	innerValues := innerRow.GetValues()
	if innerSchema := a.rowSchema(innerRow, a.innerSchema); innerSchema != nil {
		for i := 0; i < innerSchema.ColumnCount() && i < len(innerValues); i++ {
			if col, ok := innerSchema.GetColumnByIndex(i); ok && col != nil {
				value := a.valueToInterfaceForJoin(innerValues[i])
				evalCtx.Row[col.Name] = value
				tableName := col.TableName
				if tableName == "" {
					tableName = innerSchema.TableName
				}
				if tableName != "" {
					evalCtx.Row[tableName+"."+col.Name] = value
				}
			}
		}
	}

	for _, cond := range a.joinConds {
		if cond == nil {
			continue
		}

		result, err := cond.Eval(evalCtx)
		if err != nil {
			logger.Debugf("evaluateJoinConditions failed: %v", err)
			return false
		}

		boolResult, ok := result.(bool)
		if !ok {
			logger.Debugf("evaluateJoinConditions non-bool result: %T", result)
			return false
		}
		if !boolResult {
			return false
		}
	}

	return true
}

func (a *ApplyOperator) outerSchema() *metadata.QuerySchema {
	if a == nil || a.outer == nil {
		return nil
	}
	return a.outer.Schema()
}

func (a *ApplyOperator) innerSchema() *metadata.QuerySchema {
	if a == nil || a.inner == nil {
		return nil
	}
	return a.inner.Schema()
}

func (a *ApplyOperator) valueToInterfaceForJoin(value basic.Value) interface{} {
	if value == nil || value.IsNull() {
		return nil
	}

	switch value.Type() {
	case basic.ValueTypeTinyInt, basic.ValueTypeSmallInt, basic.ValueTypeMediumInt,
		basic.ValueTypeInt, basic.ValueTypeBigInt:
		return value.Int()
	case basic.ValueTypeFloat, basic.ValueTypeDouble:
		return value.Float64()
	case basic.ValueTypeVarchar, basic.ValueTypeChar, basic.ValueTypeText:
		return value.String()
	case basic.ValueTypeBool, basic.ValueTypeBoolean:
		return value.Bool()
	case basic.ValueTypeBinary, basic.ValueTypeVarBinary, basic.ValueTypeBlob:
		return value.Bytes()
	case basic.ValueTypeDate, basic.ValueTypeTime, basic.ValueTypeDateTime, basic.ValueTypeTimestamp:
		return value.Time()
	default:
		return value.Raw()
	}
}

func (a *ApplyOperator) rowSchema(record Record, fallback func() *metadata.QuerySchema) *metadata.QuerySchema {
	if record == nil {
		if fallback != nil {
			return fallback()
		}
		return nil
	}

	if typed, ok := any(record).(interface{ GetSchema() *metadata.QuerySchema }); ok {
		if schema := typed.GetSchema(); schema != nil {
			return schema
		}
	}

	if fallback != nil {
		return fallback()
	}
	return nil
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
	cteContext        *CTEContext
	cteSchemas        map[string]*metadata.QuerySchema
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
		cteContext:        NewCTEContext(),
		cteSchemas:        make(map[string]*metadata.QuerySchema),
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
	case *plan.PhysicalCTEStatement:
		return v.buildCTEStatement(ctx, p)

	case *plan.PhysicalValues:
		return v.buildValues(p)

	case *plan.PhysicalCTEScan:
		schema := v.cteSchemas[p.Name]
		if schema == nil {
			schema = metadata.NewQuerySchema()
		}
		return NewCTEScanOperator(p.Name, v.cteContext, schema), nil

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

func (v *VolcanoExecutor) buildValues(p *plan.PhysicalValues) (Operator, error) {
	return NewValuesOperator(p.Exprs), nil
}

func (v *VolcanoExecutor) buildCTEStatement(ctx context.Context, p *plan.PhysicalCTEStatement) (Operator, error) {
	children := p.Children()
	if len(children) <= p.DefinitionCount {
		return nil, fmt.Errorf("PhysicalCTEStatement has no body")
	}

	// Definitions are planned and registered before the body is built, so a
	// CTEScan can bind to the query schema and shared materialization context.
	for i := 0; i < p.DefinitionCount; i++ {
		switch definition := children[i].(type) {
		case *plan.PhysicalCTE:
			definitionChildren := definition.Children()
			if len(definitionChildren) != 1 {
				return nil, fmt.Errorf("PhysicalCTE %s must have one query child", definition.Name)
			}
			query, err := v.buildOperatorTree(ctx, definitionChildren[0])
			if err != nil {
				return nil, fmt.Errorf("build CTE %s query: %w", definition.Name, err)
			}
			querySchema := query.Schema()
			if querySchema == nil || querySchema.ColumnCount() == 0 {
				querySchema = inferPhysicalQuerySchema(definitionChildren[0], v.cteSchemas)
			}
			v.cteSchemas[definition.Name] = renameCTEQuerySchema(querySchema, definition.Columns)
			v.cteContext.AddDefinition(&CTEDefinition{Name: definition.Name, Recursive: definition.Recursive, Operator: query})
		case *plan.PhysicalRecursiveCTE:
			definitionChildren := definition.Children()
			if len(definitionChildren) != 2 {
				return nil, fmt.Errorf("PhysicalRecursiveCTE %s must have anchor and recursive children", definition.Name)
			}
			anchor, err := v.buildOperatorTree(ctx, definitionChildren[0])
			if err != nil {
				return nil, fmt.Errorf("build recursive CTE %s anchor: %w", definition.Name, err)
			}
			anchorSchema := anchor.Schema()
			if anchorSchema == nil || anchorSchema.ColumnCount() == 0 {
				anchorSchema = inferPhysicalQuerySchema(definitionChildren[0], v.cteSchemas)
			}
			v.cteSchemas[definition.Name] = renameCTEQuerySchema(anchorSchema, definition.Columns)
			recursive, err := v.buildOperatorTree(ctx, definitionChildren[1])
			if err != nil {
				return nil, fmt.Errorf("build recursive CTE %s member: %w", definition.Name, err)
			}
			v.cteContext.AddDefinition(&CTEDefinition{Name: definition.Name, Recursive: true, Operator: anchor})
			_ = recursive
		default:
			return nil, fmt.Errorf("unsupported CTE definition type: %T", children[i])
		}
	}

	body, err := v.buildOperatorTree(ctx, children[p.DefinitionCount])
	if err != nil {
		return nil, fmt.Errorf("build CTE statement body: %w", err)
	}

	// Wrap definitions in declaration order. CTEOperator materializes its
	// query before opening the body, matching MySQL's statement scope.
	for i := p.DefinitionCount - 1; i >= 0; i-- {
		switch definition := children[i].(type) {
		case *plan.PhysicalCTE:
			query := v.cteContext.definitions[normalizeCTEName(definition.Name)].Operator
			body = NewCTEOperator(definition.Name, query, body, v.cteContext)
		case *plan.PhysicalRecursiveCTE:
			definitionChildren := definition.Children()
			anchor, err := v.buildOperatorTree(ctx, definitionChildren[0])
			if err != nil {
				return nil, err
			}
			recursive, err := v.buildOperatorTree(ctx, definitionChildren[1])
			if err != nil {
				return nil, err
			}
			body = NewRecursiveCTEOperator(definition.Name, anchor, recursive, body, v.cteContext, 100)
		}
	}
	return body, nil
}

func renameCTEQuerySchema(schema *metadata.QuerySchema, columns []string) *metadata.QuerySchema {
	if schema == nil {
		schema = metadata.NewQuerySchema()
	} else {
		schema = schema.Clone()
	}
	for i, name := range columns {
		if i >= len(schema.Columns) || strings.TrimSpace(name) == "" {
			break
		}
		schema.Columns[i].Name = name
	}
	return schema
}

func inferPhysicalQuerySchema(physicalPlan plan.PhysicalPlan, cteSchemas map[string]*metadata.QuerySchema) *metadata.QuerySchema {
	schema := metadata.NewQuerySchema()
	switch p := physicalPlan.(type) {
	case *plan.PhysicalValues:
		for _, expr := range p.Exprs {
			schema.AddColumn(metadata.NewQueryColumn(expr.String(), valuesExpressionType(expr)))
		}
	case *plan.PhysicalTableScan:
		if p.Table != nil {
			return metadata.FromTable(p.Table)
		}
	case *plan.PhysicalProjection:
		child := inferPhysicalQuerySchema(firstPhysicalChild(p), cteSchemas)
		for _, expr := range p.Exprs {
			column := metadata.NewQueryColumn(expr.String(), valuesExpressionType(expr))
			if col, ok := expr.(*plan.Column); ok {
				if source, found := child.GetColumn(col.Name); found {
					column = source
				}
			}
			schema.AddColumn(column)
		}
	case *plan.PhysicalSelection:
		return inferPhysicalQuerySchema(firstPhysicalChild(p), cteSchemas)
	case *plan.PhysicalCTEScan:
		if source := cteSchemas[p.Name]; source != nil {
			return source.Clone()
		}
	}
	return schema
}

func firstPhysicalChild(physicalPlan plan.PhysicalPlan) plan.PhysicalPlan {
	children := physicalPlan.Children()
	if len(children) == 0 {
		return nil
	}
	return children[0]
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
		physicalScanRequiredColumns(p),
	), nil
}

func physicalScanRequiredColumns(p *plan.PhysicalTableScan) []string {
	if p == nil {
		return nil
	}
	return physicalRequiredColumns(p.Schema(), p.Table)
}

func physicalRequiredColumns(schema *metadata.DatabaseSchema, table *metadata.Table) []string {
	if table == nil || schema == nil {
		return nil
	}
	pruned, ok := schema.GetTable(table.Name)
	if !ok || len(pruned.Columns) == 0 || len(pruned.Columns) >= len(table.Columns) {
		return nil
	}
	columns := make([]string, 0, len(pruned.Columns))
	for _, column := range pruned.Columns {
		if column != nil {
			columns = append(columns, column.Name)
		}
	}
	return columns
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
		nil, // startKey - 可以从p中提取
		nil, // endKey - 可以从p中提取
		physicalRequiredColumns(p.Schema(), p.Table),
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

	if !physicalHashJoinUsesOnlyEquiConditions(p.Conditions) {
		condition := func(l, r Record) bool {
			return evaluatePhysicalJoinConditions(p.Conditions, l, r, left.Schema(), right.Schema())
		}
		return NewNestedLoopJoinOperator(left, right, p.JoinType, condition), nil
	}

	buildKey, probeKey := v.buildHashKeyFunctions(p.Conditions, p.LeftSchema, p.RightSchema)

	// LEFT/FULL 时用右表做 build、左表做 probe，保证左表每行都输出
	joinType := p.JoinType
	if joinType == "LEFT" || joinType == "LEFT OUTER" || joinType == "FULL" || joinType == "FULL OUTER" {
		return NewHashJoinOperator(right, left, joinType, probeKey, buildKey), nil
	}
	return NewHashJoinOperator(left, right, joinType, buildKey, probeKey), nil
}

func physicalHashJoinUsesOnlyEquiConditions(conditions []plan.Expression) bool {
	if len(conditions) == 0 {
		return false
	}
	for _, condition := range conditions {
		binary, ok := condition.(*plan.BinaryOperation)
		if !ok || binary.Op != plan.OpEQ {
			return false
		}
		if _, ok := binary.Left.(*plan.Column); !ok {
			return false
		}
		if _, ok := binary.Right.(*plan.Column); !ok {
			return false
		}
	}
	return true
}

func (v *VolcanoExecutor) buildMergeJoin(ctx context.Context, p *plan.PhysicalMergeJoin) (Operator, error) {
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

	leftKey, rightKey, ok := buildSortMergeKeyFunctions(p.Conditions)
	if !ok {
		// A merge join can only use the key when all predicates are represented
		// by that key. Evaluate the original predicates in the fallback so
		// non-equality and compound conditions retain their SQL meaning.
		condition := func(l, r Record) bool {
			return evaluatePhysicalJoinConditions(p.Conditions, l, r, left.Schema(), right.Schema())
		}
		return NewNestedLoopJoinOperator(left, right, p.JoinType, condition), nil
	}
	return NewSortMergeJoinOperator(left, right, p.JoinType, leftKey, rightKey), nil
}

func evaluatePhysicalJoinConditions(
	conditions []plan.Expression,
	left, right Record,
	leftSchema, rightSchema *metadata.QuerySchema,
) bool {
	if len(conditions) == 0 {
		return true
	}
	if left == nil || right == nil {
		return false
	}

	row := make(map[string]interface{})
	addPhysicalJoinRowBindings(row, left, leftSchema)
	addPhysicalJoinRowBindings(row, right, rightSchema)
	ctx := &plan.EvalContext{Row: row}
	for _, condition := range conditions {
		if condition == nil {
			continue
		}
		result, err := condition.Eval(ctx)
		if err != nil {
			return false
		}
		matched, ok := result.(bool)
		if !ok || !matched {
			return false
		}
	}
	return true
}

func addPhysicalJoinRowBindings(row map[string]interface{}, record Record, fallback *metadata.QuerySchema) {
	if record == nil {
		return
	}
	schema := fallback
	if typed, ok := any(record).(interface{ GetSchema() *metadata.QuerySchema }); ok {
		if recordSchema := typed.GetSchema(); recordSchema != nil {
			schema = recordSchema
		}
	}
	if schema == nil {
		return
	}

	values := record.GetValues()
	converter := &ApplyOperator{}
	for index := 0; index < schema.ColumnCount() && index < len(values); index++ {
		column, ok := schema.GetColumnByIndex(index)
		if !ok || column == nil {
			continue
		}
		value := converter.valueToInterfaceForJoin(values[index])
		row[column.Name] = value
		tableName := column.TableName
		if tableName == "" {
			tableName = schema.TableName
		}
		if tableName != "" {
			row[tableName+"."+column.Name] = value
		}
	}
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

	return NewHashAggregateOperatorWithExpressions(child, groupByExprs, aggFuncs, p.AggFuncs), nil
}

func (v *VolcanoExecutor) buildStreamAgg(ctx context.Context, p *plan.PhysicalStreamAgg) (Operator, error) {
	// StreamAgg可以用HashAgg实现
	// Keep this adapter explicit: PhysicalHashAgg has additional parallel
	// execution metadata, so the two concrete plan structs are no longer
	// layout-convertible.
	hashPlan := &plan.PhysicalHashAgg{
		BasePhysicalPlan: p.BasePhysicalPlan,
		GroupByItems:     p.GroupByItems,
		AggFuncs:         p.AggFuncs,
	}
	return v.buildHashAgg(ctx, hashPlan)
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

// ExecuteBatches streams the result through callback-sized batches. It keeps
// the Open/Next/Close lifecycle identical to Execute while allowing callers
// such as protocol writers and backup jobs to cap result memory.
func (v *VolcanoExecutor) ExecuteBatches(ctx context.Context, batchSize int, consume func([]Record) error) error {
	if v == nil || v.root == nil {
		return fmt.Errorf("root operator is nil")
	}
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if consume == nil {
		return fmt.Errorf("batch consumer is nil")
	}
	if err := v.root.Open(ctx); err != nil {
		return fmt.Errorf("failed to open root operator: %w", err)
	}
	defer v.root.Close()

	if batchRoot, ok := v.root.(BatchOperator); ok {
		for {
			rows, err := batchRoot.NextBatch(ctx, batchSize)
			if err != nil && err != io.EOF {
				return fmt.Errorf("error during batch execution: %w", err)
			}
			for offset := 0; offset < len(rows); {
				end := offset + batchSize
				if end > len(rows) {
					end = len(rows)
				}
				if err := consume(rows[offset:end]); err != nil {
					return err
				}
				offset = end
			}
			if err == io.EOF {
				return nil
			}
			if len(rows) == 0 {
				return nil
			}
		}
	}

	batch := make([]Record, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := consume(batch); err != nil {
			return err
		}
		batch = make([]Record, 0, batchSize)
		return nil
	}
	for {
		record, err := v.root.Next(ctx)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error during execution: %w", err)
		}
		if record == nil {
			return flush()
		}
		batch = append(batch, record)
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// ========================================
// 辅助函数：从物理计划构建算子的辅助方法
// ========================================

// buildPredicate 从Conditions构建predicate函数
func (v *VolcanoExecutor) buildPredicate(conditions []plan.Expression, schema *metadata.QuerySchema) func(Record) bool {
	if len(conditions) == 0 {
		return func(r Record) bool { return true }
	}
	compiledConditions := make([]plan.CompiledExpression, len(conditions))
	for index, condition := range conditions {
		if compiled, ok := plan.CompileExpression(condition); ok {
			compiledConditions[index] = compiled
		}
	}

	return func(record Record) bool {
		// 创建求值上下文
		evalCtx := v.createEvalContextFromRecord(record, schema)

		// 对所有条件进行AND操作
		for index, cond := range conditions {
			var result interface{}
			var err error
			if compiledConditions[index] != nil {
				result, err = compiledConditions[index](evalCtx)
			} else {
				result, err = cond.Eval(evalCtx)
			}
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
	_ = leftSchema
	_ = rightSchema
	leftColumns := make([]string, 0, len(conditions))
	rightColumns := make([]string, 0, len(conditions))
	for _, cond := range conditions {
		if binOp, ok := cond.(*plan.BinaryOperation); ok && binOp.Op == plan.OpEQ {
			leftCol, rightCol := v.extractJoinColumns(binOp)
			if leftCol != "" && rightCol != "" {
				leftColumns = append(leftColumns, leftCol)
				rightColumns = append(rightColumns, rightCol)
			}
		}
	}
	if len(leftColumns) > 0 {
		return func(r Record) string { return v.hashJoinKey(r, leftColumns) },
			func(r Record) string { return v.hashJoinKey(r, rightColumns) }
	}

	return func(r Record) string { return "" }, func(r Record) string { return "" }
}

func (v *VolcanoExecutor) hashJoinKey(record Record, columns []string) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		value, ok := v.hashJoinColumnValue(record, column)
		if !ok || value == nil || value.IsNull() {
			parts = append(parts, "<NULL>")
			continue
		}
		raw := value.Raw()
		parts = append(parts, fmt.Sprintf("%T:%#v", raw, raw))
	}
	return strings.Join(parts, "\x00")
}

func (v *VolcanoExecutor) hashJoinColumnValue(record Record, column string) (basic.Value, bool) {
	if record == nil {
		return nil, false
	}
	if value, err := record.GetValueByName(column); err == nil {
		return value, true
	}
	if dot := strings.LastIndex(column, "."); dot >= 0 && dot+1 < len(column) {
		if value, err := record.GetValueByName(column[dot+1:]); err == nil {
			return value, true
		}
	}
	if typed, ok := any(record).(interface{ GetSchema() *metadata.QuerySchema }); ok {
		if schema := typed.GetSchema(); schema != nil {
			for index := 0; index < schema.ColumnCount(); index++ {
				col, exists := schema.GetColumnByIndex(index)
				if exists && col != nil && strings.EqualFold(col.Name, column) {
					return record.GetValueByIndex(index), true
				}
			}
		}
	}
	return nil, false
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
		funcName := strings.ToUpper(strings.TrimSpace(aggFunc.Name()))
		function, _ := aggFunc.(*plan.Function)
		switch funcName {
		case "COUNT":
			countAgg := &CountAgg{}
			if function != nil {
				countAgg.distinct = function.Distinct
				countAgg.countColumn = len(function.FuncArgs) > 0
			}
			funcs = append(funcs, countAgg)
		case "SUM":
			sumAgg := &SumAgg{}
			if function != nil {
				sumAgg.distinct = function.Distinct
			}
			funcs = append(funcs, sumAgg)
		case "AVG":
			avgAgg := &AvgAgg{}
			if function != nil {
				avgAgg.distinct = function.Distinct
			}
			funcs = append(funcs, avgAgg)
		case "MIN":
			minAgg := &MinAgg{}
			if function != nil {
				minAgg.distinct = function.Distinct
			}
			funcs = append(funcs, minAgg)
		case "MAX":
			maxAgg := &MaxAgg{}
			if function != nil {
				maxAgg.distinct = function.Distinct
			}
			funcs = append(funcs, maxAgg)
		case "GROUP_CONCAT":
			groupConcat := &GroupConcatAgg{separator: ","}
			if function, ok := aggFunc.(*plan.Function); ok {
				groupConcat.distinct = function.Distinct
				if function.Separator != "" {
					groupConcat.separator = function.Separator
				}
			}
			funcs = append(funcs, groupConcat)
		case "JSON_ARRAYAGG":
			funcs = append(funcs, &JSONArrayAgg{})
		case "JSON_OBJECTAGG":
			funcs = append(funcs, &JSONObjectAgg{})
		case "ANY_VALUE":
			funcs = append(funcs, &AnyValueAgg{})
		case "BIT_AND", "BIT_OR", "BIT_XOR":
			funcs = append(funcs, &BitAgg{name: funcName})
		case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
			funcs = append(funcs, &VarianceAgg{name: funcName})
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
	_, requestedColumn := splitQualifiedColumnNameForEngine(columnName)

	for i := 0; i < schema.ColumnCount(); i++ {
		col, ok := schema.GetColumnByIndex(i)
		if ok && col != nil {
			_, schemaColumn := splitQualifiedColumnNameForEngine(col.Name)
			if strings.EqualFold(schemaColumn, requestedColumn) || strings.EqualFold(col.Name, columnName) {
				return i
			}
		}
	}

	return -1
}

func splitQualifiedColumnNameForEngine(name string) (qualifier, column string) {
	name = strings.Trim(strings.TrimSpace(name), "`")
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		qualifier = strings.Trim(strings.TrimSpace(name[:dot]), "`")
		column = strings.Trim(strings.TrimSpace(name[dot+1:]), "`")
		return qualifier, column
	}
	return "", name
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
