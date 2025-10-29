package engine

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// Record 火山模型中的记录接口
type Record interface {
	GetValues() []basic.Value
	SetValues(values []basic.Value)
	GetSchema() *metadata.Schema
}

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
	Schema() *metadata.Schema
}

// BaseOperator 基础算子实现，提供公共功能
type BaseOperator struct {
	children []Operator
	schema   *metadata.Schema
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

func (b *BaseOperator) Schema() *metadata.Schema {
	return b.schema
}

// ========================================
// 辅助类型和函数
// ========================================

// SimpleExecutorRecord 简单的ExecutorRecord实现
type SimpleExecutorRecord struct {
	values []basic.Value
	schema *metadata.Schema
}

func NewExecutorRecordFromValues(values []basic.Value, schema *metadata.Schema) Record {
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

func (s *SimpleExecutorRecord) GetSchema() *metadata.Schema {
	return s.schema
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
	metadata, err := t.storageAdapter.GetTableMetadata(ctx, t.schemaName, t.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 设置schema
	t.schema = &metadata.Schema{
		Columns: metadata.Schema.Columns,
	}

	// 创建表页面迭代器
	t.iterator, err = t.storageAdapter.ScanTable(ctx, metadata)
	if err != nil {
		return fmt.Errorf("failed to create table iterator: %w", err)
	}

	logger.Debugf("TableScanOperator opened for table %s.%s, spaceID=%d",
		t.schemaName, t.tableName, metadata.SpaceID)
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
	tableMetadata, err := i.storageAdapter.GetTableMetadata(ctx, i.schemaName, i.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}
	i.schema = &metadata.Schema{
		Columns: tableMetadata.Schema.Columns,
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

// fetchPrimaryKeys 预先扫描索引获取所有主键（用于批量回表优化）
func (i *IndexScanOperator) fetchPrimaryKeys(ctx context.Context) error {
	// 将startKey和endKey转换为字节数组
	startKeyBytes := []byte{} // TODO: 实现Value到bytes的转换
	endKeyBytes := []byte{}   // TODO: 实现Value到bytes的转换

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
	// TODO: 实现从索引直接读取逻辑
	logger.Debugf("nextFromIndex: using covering index %s", i.indexName)
	return nil, nil // EOF - 临时实现
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

	// 通过主键回表查找完整记录
	// TODO: 实现实际的回表逻辑
	// 这里需要：
	// 1. 使用primaryKey在聚簇索引中查找
	// 2. 读取完整记录
	// 3. 转换为Record并返回

	logger.Debugf("nextWithLookup: lookup primaryKey for index %s", i.indexName)

	// 临时返回模拟数据
	values := []basic.Value{
		basic.NewInt64(int64(i.keyIndex)),
		basic.NewString(fmt.Sprintf("row_%d", i.keyIndex)),
	}
	return NewExecutorRecordFromValues(values, i.schema), nil
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

	// 构建输出schema
	childSchema := p.child.Schema()
	if childSchema != nil && len(p.projections) > 0 {
		projectedColumns := make([]*metadata.Column, len(p.projections))
		for i, idx := range p.projections {
			if idx < len(childSchema.Columns) {
				projectedColumns[i] = childSchema.Columns[idx]
			}
		}
		p.schema = &metadata.Schema{Columns: projectedColumns}
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
		for i, expr := range p.exprs {
			// TODO: 实现表达式求值
			_ = expr
			newValues[i] = basic.NewNull()
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

	// 构建输出schema（左表+右表）
	leftSchema := n.left.Schema()
	rightSchema := n.right.Schema()
	if leftSchema != nil && rightSchema != nil {
		columns := make([]*metadata.Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))
		columns = append(columns, leftSchema.Columns...)
		columns = append(columns, rightSchema.Columns...)
		n.schema = &metadata.Schema{Columns: columns}
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

	// 构建输出schema
	buildSchema := h.buildSide.Schema()
	probeSchema := h.probeSide.Schema()
	if buildSchema != nil && probeSchema != nil {
		columns := make([]*metadata.Column, 0, len(buildSchema.Columns)+len(probeSchema.Columns))
		columns = append(columns, buildSchema.Columns...)
		columns = append(columns, probeSchema.Columns...)
		h.schema = &metadata.Schema{Columns: columns}
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
}

// CountAgg COUNT聚合
type CountAgg struct {
	count int64
}

func (c *CountAgg) Init()                    { c.count = 0 }
func (c *CountAgg) Update(value basic.Value) { c.count++ }
func (c *CountAgg) Result() basic.Value      { return basic.NewInt64(c.count) }

// SumAgg SUM聚合
type SumAgg struct {
	sum float64
}

func (s *SumAgg) Init() { s.sum = 0 }
func (s *SumAgg) Update(value basic.Value) {
	if !value.IsNull() {
		s.sum += value.ToFloat64()
	}
}
func (s *SumAgg) Result() basic.Value { return basic.NewFloat64(s.sum) }

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
		a.sum += value.ToFloat64()
		a.count++
	}
}

func (a *AvgAgg) Result() basic.Value {
	if a.count == 0 {
		return basic.NewNull()
	}
	return basic.NewFloat64(a.sum / float64(a.count))
}

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
	if value.ToFloat64() < m.min.ToFloat64() {
		m.min = value
	}
}

func (m *MinAgg) Result() basic.Value {
	return m.min
}

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
	if value.ToFloat64() > m.max.ToFloat64() {
		m.max = value
	}
}

func (m *MaxAgg) Result() basic.Value {
	return m.max
}

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

	// 构建输出schema：分组列 + 聚合列
	childSchema := h.child.Schema()
	if childSchema != nil {
		columns := make([]*metadata.Column, 0, len(h.groupByExprs)+len(h.aggFuncs))

		// 添加分组列
		for _, idx := range h.groupByExprs {
			if idx < len(childSchema.Columns) {
				columns = append(columns, childSchema.Columns[idx])
			}
		}

		// 添加聚合列（使用默认名称）
		for i := range h.aggFuncs {
			aggCol := &metadata.Column{
				Name:  fmt.Sprintf("agg_%d", i),
				Type:  metadata.TypeDouble,
				Table: "",
			}
			columns = append(columns, aggCol)
		}

		h.schema = &metadata.Schema{Columns: columns}
	} else {
		h.schema = &metadata.Schema{}
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
	f1 := v1.ToFloat64()
	f2 := v2.ToFloat64()
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

	default:
		return nil, fmt.Errorf("unsupported physical plan type: %T", physicalPlan)
	}
}

func (v *VolcanoExecutor) buildTableScan(p *plan.PhysicalTableScan) (Operator, error) {
	if p.Table == nil {
		return nil, fmt.Errorf("table is nil in PhysicalTableScan")
	}

	return NewTableScanOperator(
		p.Table.Schema,
		p.Table.Name,
		v.tableManager,
		v.bufferPoolManager,
		v.storageManager,
	), nil
}

func (v *VolcanoExecutor) buildIndexScan(p *plan.PhysicalIndexScan) (Operator, error) {
	if p.Table == nil || p.Index == nil {
		return nil, fmt.Errorf("table or index is nil in PhysicalIndexScan")
	}

	return NewIndexScanOperator(
		p.Table.Schema,
		p.Table.Name,
		p.Index.Name,
		v.indexManager,
		nil, // startKey
		nil, // endKey
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

	// TODO: 将Conditions转换为predicate函数
	predicate := func(record Record) bool {
		// 简化实现：总是返回true
		return true
	}

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

	// TODO: 从Conditions构建hash key函数
	buildKey := func(r Record) string { return "" }
	probeKey := func(r Record) string { return "" }

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

	// TODO: 从GroupByItems和AggFuncs构建聚合函数
	groupByExprs := []int{}
	aggFuncs := []AggregateFunc{&CountAgg{}}

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

	// TODO: 从ByItems构建排序键
	sortKeys := []SortKey{{ColumnIdx: 0, Ascending: true}}

	return NewSortOperator(child, sortKeys), nil
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
