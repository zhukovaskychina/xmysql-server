package engine

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// ========================================
// CTE (Common Table Expression) Types
// ========================================

// CTEDefinition CTE定义
type CTEDefinition struct {
	Name      string              // CTE名称
	Columns   []string            // 列名列表（可选）
	Query     sqlparser.Statement // CTE查询语句
	Recursive bool                // 是否是递归CTE
	Operator  Operator            // 已构建的算子（用于物化）
}

// CTEContext CTE上下文
type CTEContext struct {
	definitions  map[string]*CTEDefinition // CTE定义映射
	materialized map[string][]Record       // 物化的CTE结果
}

// NewCTEContext 创建CTE上下文
func NewCTEContext() *CTEContext {
	return &CTEContext{
		definitions:  make(map[string]*CTEDefinition),
		materialized: make(map[string][]Record),
	}
}

// AddDefinition 添加CTE定义
func (ctx *CTEContext) AddDefinition(def *CTEDefinition) {
	ctx.definitions[def.Name] = def
}

// GetDefinition 获取CTE定义
func (ctx *CTEContext) GetDefinition(name string) (*CTEDefinition, bool) {
	def, ok := ctx.definitions[name]
	return def, ok
}

// Materialize 物化CTE
func (ctx *CTEContext) Materialize(name string, records []Record) {
	ctx.materialized[name] = records
}

// GetMaterialized 获取物化的CTE结果
func (ctx *CTEContext) GetMaterialized(name string) ([]Record, bool) {
	records, ok := ctx.materialized[name]
	return records, ok
}

// ========================================
// CTEOperator - CTE算子
// ========================================

// CTEOperator CTE算子（非递归）
type CTEOperator struct {
	BaseOperator
	cteName    string
	cteQuery   Operator
	mainQuery  Operator
	cteContext *CTEContext

	// 执行状态
	materialized bool
	currentIndex int
}

// NewCTEOperator 创建CTE算子
func NewCTEOperator(
	cteName string,
	cteQuery Operator,
	mainQuery Operator,
	cteContext *CTEContext,
) *CTEOperator {
	return &CTEOperator{
		BaseOperator: BaseOperator{
			children: []Operator{cteQuery, mainQuery},
		},
		cteName:    cteName,
		cteQuery:   cteQuery,
		mainQuery:  mainQuery,
		cteContext: cteContext,
	}
}

// Open 初始化CTE算子
func (c *CTEOperator) Open(ctx context.Context) error {
	if err := c.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 物化CTE查询
	if err := c.materializeCTE(ctx); err != nil {
		return fmt.Errorf("failed to materialize CTE: %w", err)
	}

	// 设置schema为主查询的schema
	c.schema = c.mainQuery.Schema()

	logger.Debugf("CTEOperator opened for CTE: %s", c.cteName)
	return nil
}

// Next 获取下一条记录
func (c *CTEOperator) Next(ctx context.Context) (Record, error) {
	if !c.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 直接从主查询获取结果
	return c.mainQuery.Next(ctx)
}

// materializeCTE 物化CTE查询
func (c *CTEOperator) materializeCTE(ctx context.Context) error {
	// 检查是否已经物化
	if _, ok := c.cteContext.GetMaterialized(c.cteName); ok {
		return nil // 已经物化
	}

	// 执行CTE查询并收集所有结果
	var results []Record
	for {
		record, err := c.cteQuery.Next(ctx)
		if err != nil {
			return fmt.Errorf("error reading CTE query: %w", err)
		}
		if record == nil {
			break // EOF
		}
		results = append(results, record)
	}

	// 物化结果
	c.cteContext.Materialize(c.cteName, results)
	c.materialized = true

	logger.Debugf("Materialized CTE %s with %d rows", c.cteName, len(results))
	return nil
}

// ========================================
// RecursiveCTEOperator - 递归CTE算子
// ========================================

// RecursiveCTEOperator 递归CTE算子
type RecursiveCTEOperator struct {
	BaseOperator
	cteName        string
	anchorQuery    Operator // 锚点查询（非递归部分）
	recursiveQuery Operator // 递归查询
	mainQuery      Operator // 主查询
	cteContext     *CTEContext

	// 递归控制
	maxRecursionDepth int
	currentDepth      int

	// 执行状态
	materialized bool
}

// NewRecursiveCTEOperator 创建递归CTE算子
func NewRecursiveCTEOperator(
	cteName string,
	anchorQuery Operator,
	recursiveQuery Operator,
	mainQuery Operator,
	cteContext *CTEContext,
	maxRecursionDepth int,
) *RecursiveCTEOperator {
	if maxRecursionDepth <= 0 {
		maxRecursionDepth = 100 // 默认最大递归深度
	}

	return &RecursiveCTEOperator{
		BaseOperator: BaseOperator{
			children: []Operator{anchorQuery, recursiveQuery, mainQuery},
		},
		cteName:           cteName,
		anchorQuery:       anchorQuery,
		recursiveQuery:    recursiveQuery,
		mainQuery:         mainQuery,
		cteContext:        cteContext,
		maxRecursionDepth: maxRecursionDepth,
	}
}

// Open 初始化递归CTE算子
func (r *RecursiveCTEOperator) Open(ctx context.Context) error {
	if err := r.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 执行递归CTE
	if err := r.executeRecursiveCTE(ctx); err != nil {
		return fmt.Errorf("failed to execute recursive CTE: %w", err)
	}

	// 设置schema为主查询的schema
	r.schema = r.mainQuery.Schema()

	logger.Debugf("RecursiveCTEOperator opened for CTE: %s", r.cteName)
	return nil
}

// Next 获取下一条记录
func (r *RecursiveCTEOperator) Next(ctx context.Context) (Record, error) {
	if !r.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// 直接从主查询获取结果
	return r.mainQuery.Next(ctx)
}

// executeRecursiveCTE 执行递归CTE
func (r *RecursiveCTEOperator) executeRecursiveCTE(ctx context.Context) error {
	// 1. 执行锚点查询（非递归部分）
	anchorResults, err := r.executeAnchorQuery(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute anchor query: %w", err)
	}

	// 初始化结果集
	allResults := make([]Record, 0)
	allResults = append(allResults, anchorResults...)

	// 当前迭代的结果（用于下一次递归）
	currentResults := anchorResults

	// 2. 递归执行
	r.currentDepth = 0
	for r.currentDepth < r.maxRecursionDepth {
		r.currentDepth++

		// 将当前结果物化为临时CTE
		r.cteContext.Materialize(r.cteName, currentResults)

		// 执行递归查询
		recursiveResults, err := r.executeRecursiveQuery(ctx)
		if err != nil {
			return fmt.Errorf("failed to execute recursive query at depth %d: %w", r.currentDepth, err)
		}

		// 检查终止条件
		if len(recursiveResults) == 0 {
			logger.Debugf("Recursive CTE %s terminated at depth %d (no more results)", r.cteName, r.currentDepth)
			break
		}

		// 检查循环依赖
		if r.detectCycle(allResults, recursiveResults) {
			return fmt.Errorf("cycle detected in recursive CTE %s at depth %d", r.cteName, r.currentDepth)
		}

		// 合并结果
		allResults = append(allResults, recursiveResults...)
		currentResults = recursiveResults

		logger.Debugf("Recursive CTE %s depth %d: %d new rows, %d total rows",
			r.cteName, r.currentDepth, len(recursiveResults), len(allResults))
	}

	// 检查是否达到最大递归深度
	if r.currentDepth >= r.maxRecursionDepth {
		logger.Warnf("Recursive CTE %s reached maximum recursion depth %d", r.cteName, r.maxRecursionDepth)
	}

	// 3. 物化最终结果
	r.cteContext.Materialize(r.cteName, allResults)
	r.materialized = true

	logger.Debugf("Recursive CTE %s completed with %d total rows", r.cteName, len(allResults))
	return nil
}

// executeAnchorQuery 执行锚点查询
func (r *RecursiveCTEOperator) executeAnchorQuery(ctx context.Context) ([]Record, error) {
	var results []Record
	for {
		record, err := r.anchorQuery.Next(ctx)
		if err != nil {
			return nil, err
		}
		if record == nil {
			break // EOF
		}
		results = append(results, record)
	}
	return results, nil
}

// executeRecursiveQuery 执行递归查询
func (r *RecursiveCTEOperator) executeRecursiveQuery(ctx context.Context) ([]Record, error) {
	// 注意：递归查询需要重新打开，因为它引用了更新的CTE
	// 这里简化实现，假设递归查询已经正确设置

	var results []Record
	for {
		record, err := r.recursiveQuery.Next(ctx)
		if err != nil {
			return nil, err
		}
		if record == nil {
			break // EOF
		}
		results = append(results, record)
	}
	return results, nil
}

// detectCycle 检测循环依赖
func (r *RecursiveCTEOperator) detectCycle(existingResults, newResults []Record) bool {
	// 简化实现：检查新结果是否与现有结果完全相同
	// 实际应该使用更复杂的循环检测算法

	if len(newResults) == 0 {
		return false
	}

	// 创建现有结果的哈希集合
	existingSet := make(map[string]bool)
	for _, record := range existingResults {
		key := r.recordToKey(record)
		existingSet[key] = true
	}

	// 检查新结果是否都在现有结果中
	allExist := true
	for _, record := range newResults {
		key := r.recordToKey(record)
		if !existingSet[key] {
			allExist = false
			break
		}
	}

	return allExist
}

// recordToKey 将记录转换为键（用于循环检测）
func (r *RecursiveCTEOperator) recordToKey(record Record) string {
	values := record.GetValues()
	key := ""
	for _, val := range values {
		key += fmt.Sprintf("%v|", val.Raw())
	}
	return key
}

// ========================================
// CTEScanOperator - CTE扫描算子
// ========================================

// CTEScanOperator CTE扫描算子（从物化的CTE读取数据）
type CTEScanOperator struct {
	BaseOperator
	cteName    string
	cteContext *CTEContext

	// 执行状态
	records      []Record
	currentIndex int
}

// NewCTEScanOperator 创建CTE扫描算子
func NewCTEScanOperator(
	cteName string,
	cteContext *CTEContext,
	schema *metadata.QuerySchema,
) *CTEScanOperator {
	op := &CTEScanOperator{
		BaseOperator: BaseOperator{
			children: nil,
		},
		cteName:    cteName,
		cteContext: cteContext,
	}
	op.schema = schema
	return op
}

// Open 初始化CTE扫描算子
func (c *CTEScanOperator) Open(ctx context.Context) error {
	if err := c.BaseOperator.Open(ctx); err != nil {
		return err
	}

	// 获取物化的CTE结果
	records, ok := c.cteContext.GetMaterialized(c.cteName)
	if !ok {
		return fmt.Errorf("CTE %s not materialized", c.cteName)
	}

	c.records = records
	c.currentIndex = 0

	logger.Debugf("CTEScanOperator opened for CTE: %s with %d rows", c.cteName, len(c.records))
	return nil
}

// Next 获取下一条记录
func (c *CTEScanOperator) Next(ctx context.Context) (Record, error) {
	if !c.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	if c.currentIndex >= len(c.records) {
		return nil, nil // EOF
	}

	record := c.records[c.currentIndex]
	c.currentIndex++
	return record, nil
}

// ========================================
// CTE优化策略
// ========================================

// CTEMaterializationStrategy CTE物化策略
type CTEMaterializationStrategy int

const (
	// MaterializeAlways 总是物化CTE
	MaterializeAlways CTEMaterializationStrategy = iota
	// MaterializeOnce 只物化一次（多次引用时共享）
	MaterializeOnce
	// InlineCTE 内联CTE（不物化，直接展开）
	InlineCTE
)

// CTEOptimizer CTE优化器
type CTEOptimizer struct {
	strategy CTEMaterializationStrategy
}

// NewCTEOptimizer 创建CTE优化器
func NewCTEOptimizer(strategy CTEMaterializationStrategy) *CTEOptimizer {
	return &CTEOptimizer{
		strategy: strategy,
	}
}

// ShouldMaterialize 判断是否应该物化CTE
func (o *CTEOptimizer) ShouldMaterialize(def *CTEDefinition, referenceCount int) bool {
	switch o.strategy {
	case MaterializeAlways:
		return true
	case MaterializeOnce:
		return referenceCount > 1 // 多次引用时物化
	case InlineCTE:
		return false // 总是内联
	default:
		return true
	}
}

// ShouldInline 判断是否应该内联CTE
func (o *CTEOptimizer) ShouldInline(def *CTEDefinition, referenceCount int) bool {
	// 递归CTE不能内联
	if def.Recursive {
		return false
	}

	switch o.strategy {
	case InlineCTE:
		return true
	case MaterializeOnce:
		return referenceCount == 1 // 只引用一次时内联
	default:
		return false
	}
}

// ========================================
// CTE辅助函数
// ========================================

// ValidateCTEDefinition 验证CTE定义
func ValidateCTEDefinition(def *CTEDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("CTE name cannot be empty")
	}

	if def.Query == nil {
		return fmt.Errorf("CTE query cannot be nil")
	}

	// 递归CTE的额外验证
	if def.Recursive {
		// TODO: 验证递归CTE的结构
		// 1. 必须有UNION ALL
		// 2. 必须有锚点查询和递归查询
		// 3. 递归查询必须引用CTE本身
	}

	return nil
}

// BuildCTEContext 从CTE定义列表构建CTE上下文
func BuildCTEContext(definitions []*CTEDefinition) (*CTEContext, error) {
	ctx := NewCTEContext()

	for _, def := range definitions {
		if err := ValidateCTEDefinition(def); err != nil {
			return nil, fmt.Errorf("invalid CTE definition %s: %w", def.Name, err)
		}

		ctx.AddDefinition(def)
	}

	return ctx, nil
}

// ResolveCTEReferences 解析CTE引用
// 检查主查询中引用的CTE是否都已定义
func ResolveCTEReferences(mainQuery sqlparser.Statement, ctx *CTEContext) error {
	// TODO: 遍历主查询的AST，找到所有表引用
	// 检查每个表引用是否是CTE名称
	// 如果是CTE，验证CTE已定义

	// 简化实现：假设所有引用都已正确定义
	return nil
}

// DetectCTECycle 检测CTE定义中的循环依赖
func DetectCTECycle(definitions []*CTEDefinition) error {
	// TODO: 构建CTE依赖图
	// 使用拓扑排序检测循环

	// 简化实现：假设没有循环
	return nil
}
