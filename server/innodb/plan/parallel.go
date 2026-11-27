package plan

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ParallelExecutor 并行执行器
type ParallelExecutor struct {
	workers      int           // 工作线程数
	chunkSize    int           // 数据块大小
	workerPool   chan struct{} // 工作线程池
	rowReader    PlanRowReader
	cteRows      map[string][][]interface{}
	cteColumns   map[string][]string
	outerContext map[string]interface{}
}

// NewParallelExecutor 创建并行执行器
func NewParallelExecutor(workers, chunkSize int) *ParallelExecutor {
	if workers < 1 {
		workers = 1
	}
	if chunkSize < 1 {
		chunkSize = 1
	}
	return &ParallelExecutor{
		workers:    workers,
		chunkSize:  chunkSize,
		workerPool: make(chan struct{}, workers),
		cteRows:    make(map[string][][]interface{}),
		cteColumns: make(map[string][]string),
	}
}

// SetPlanRowReader connects parallel physical operators to the engine's real
// row source. It should be called during plan setup, before Execute.
func (e *ParallelExecutor) SetPlanRowReader(reader PlanRowReader) {
	if e != nil {
		e.rowReader = reader
	}
}

// ParallelizePhysicalPlan 并行化物理计划
func (e *ParallelExecutor) ParallelizePhysicalPlan(plan PhysicalPlan) PhysicalPlan {
	switch p := plan.(type) {
	case *PhysicalTableScan:
		return e.parallelizeTableScan(p)
	case *PhysicalIndexScan:
		return e.parallelizeIndexScan(p)
	case *PhysicalHashJoin:
		return e.parallelizeHashJoin(p)
	case *PhysicalHashAgg:
		return e.parallelizeHashAgg(p)
	case *PhysicalSort:
		return e.parallelizeSort(p)
	case *PhysicalSubquery:
		if p.Subplan != nil {
			p.Subplan = e.ParallelizePhysicalPlan(p.Subplan)
		}
		return p
	default:
		// 递归并行化子计划
		children := p.Children()
		for i, child := range children {
			children[i] = e.ParallelizePhysicalPlan(child)
		}
		p.SetChildren(children)
		return p
	}
}

// parallelizeChildren materializes the same executable parallel shape below a
// parallel operator. When an engine adapter supplied a row reader, preserve
// the original child identities so adapters can continue matching their own
// plan nodes; the adapter remains authoritative in that mode.
func (e *ParallelExecutor) parallelizeChildren(plan PhysicalPlan) []PhysicalPlan {
	if plan == nil {
		return nil
	}
	children := plan.Children()
	if e.rowReader != nil {
		return children
	}
	for i, child := range children {
		children[i] = e.ParallelizePhysicalPlan(child)
	}
	return children
}

// ParallelTableScan 并行表扫描
type ParallelTableScan struct {
	PhysicalTableScan
	chunks   []DataChunk // 数据分片
	executor *ParallelExecutor
}

// DataChunk 数据分片
type DataChunk struct {
	StartRowID int64
	EndRowID   int64
}

func (e *ParallelExecutor) parallelizeTableScan(scan *PhysicalTableScan) PhysicalPlan {
	// 1. 数据分片
	rowCount := scan.Table.Stats.RowCount
	chunkCount := (rowCount + int64(e.chunkSize) - 1) / int64(e.chunkSize)
	chunks := make([]DataChunk, chunkCount)

	for i := int64(0); i < chunkCount; i++ {
		chunks[i] = DataChunk{
			StartRowID: i * int64(e.chunkSize),
			EndRowID:   min((i+1)*int64(e.chunkSize), rowCount),
		}
	}

	return &ParallelTableScan{
		PhysicalTableScan: *scan,
		chunks:            chunks,
		executor:          e,
	}
}

// ParallelIndexScan is the alternate row-source counterpart of
// ParallelTableScan. It preserves the original physical node for a
// PlanRowReader adapter while also supporting a storage-owned chunk reader.
type ParallelIndexScan struct {
	PhysicalIndexScan
	chunks   []DataChunk
	executor *ParallelExecutor
	source   *PhysicalIndexScan
}

func (e *ParallelExecutor) parallelizeIndexScan(scan *PhysicalIndexScan) PhysicalPlan {
	if scan == nil {
		return nil
	}
	rowCount := scan.GetEstimateRows()
	chunkCount := (rowCount + int64(e.chunkSize) - 1) / int64(e.chunkSize)
	if chunkCount < 1 {
		chunkCount = 1
	}
	chunks := make([]DataChunk, chunkCount)
	for i := int64(0); i < chunkCount; i++ {
		chunks[i] = DataChunk{
			StartRowID: i * int64(e.chunkSize),
			EndRowID:   min((i+1)*int64(e.chunkSize), rowCount),
		}
	}
	return &ParallelIndexScan{
		PhysicalIndexScan: *scan,
		chunks:            chunks,
		executor:          e,
		source:            scan,
	}
}

// ParallelHashJoin 并行哈希连接
type ParallelHashJoin struct {
	PhysicalHashJoin
	partitions int // 分区数
	executor   *ParallelExecutor
}

func (e *ParallelExecutor) parallelizeHashJoin(join *PhysicalHashJoin) PhysicalPlan {
	physicalJoin := *join
	physicalJoin.SetChildren(e.parallelizeChildren(join))
	if len(physicalJoin.HashJoinKeys) == 0 {
		physicalJoin.HashJoinKeys = deriveParallelHashJoinKeys(&physicalJoin)
	}
	// 使用工作线程数作为分区数
	return &ParallelHashJoin{
		PhysicalHashJoin: physicalJoin,
		partitions:       e.workers,
		executor:         e,
	}
}

func deriveParallelHashJoinKeys(join *PhysicalHashJoin) []HashJoinKey {
	if join == nil || len(join.Children()) < 2 {
		return nil
	}
	leftPlan, rightPlan := join.Children()[0], join.Children()[1]
	findColumn := func(plan PhysicalPlan, expression Expression) (int, bool) {
		column, ok := expression.(*Column)
		if !ok || column == nil {
			return -1, false
		}
		name := strings.TrimSpace(column.Name)
		if dot := strings.IndexByte(name, '.'); dot >= 0 {
			if tableName := physicalPlanTableName(plan); tableName == "" || !strings.EqualFold(strings.TrimSpace(name[:dot]), strings.TrimSpace(tableName)) {
				return -1, false
			}
			name = strings.TrimSpace(name[dot+1:])
		}
		for index, candidate := range physicalPlanColumnNames(plan) {
			if strings.EqualFold(strings.TrimSpace(candidate), name) {
				return index, true
			}
		}
		return -1, false
	}
	keys := make([]HashJoinKey, 0)
	for _, condition := range join.Conditions {
		binary, ok := condition.(*BinaryOperation)
		if !ok || binary == nil || binary.Op != OpEQ {
			continue
		}
		leftIndex, leftOK := findColumn(leftPlan, binary.Left)
		rightIndex, rightOK := findColumn(rightPlan, binary.Right)
		if leftOK && rightOK {
			keys = append(keys, HashJoinKey{LeftIndex: leftIndex, RightIndex: rightIndex})
			continue
		}
		leftIndex, leftOK = findColumn(leftPlan, binary.Right)
		rightIndex, rightOK = findColumn(rightPlan, binary.Left)
		if leftOK && rightOK {
			keys = append(keys, HashJoinKey{LeftIndex: leftIndex, RightIndex: rightIndex})
		}
	}
	return keys
}

// ParallelHashAgg 并行哈希聚合
type ParallelHashAgg struct {
	PhysicalHashAgg
	partitions int // 分区数
	executor   *ParallelExecutor
}

func (e *ParallelExecutor) parallelizeHashAgg(agg *PhysicalHashAgg) PhysicalPlan {
	physicalAgg := *agg
	physicalAgg.SetChildren(e.parallelizeChildren(agg))
	return &ParallelHashAgg{
		PhysicalHashAgg: physicalAgg,
		partitions:      e.workers,
		executor:        e,
	}
}

// ParallelSort 并行排序
type ParallelSort struct {
	PhysicalSort
	chunks   []DataChunk // 数据分片
	executor *ParallelExecutor
}

func (e *ParallelExecutor) parallelizeSort(sort *PhysicalSort) PhysicalPlan {
	physicalSort := *sort
	physicalSort.SetChildren(e.parallelizeChildren(sort))
	// 1. 数据分片
	var child PhysicalPlan
	if children := physicalSort.Children(); len(children) > 0 {
		child = children[0]
	}
	rowCount := estimateRowCount(child)
	chunkCount := (rowCount + int64(e.chunkSize) - 1) / int64(e.chunkSize)
	chunks := make([]DataChunk, chunkCount)

	for i := int64(0); i < chunkCount; i++ {
		chunks[i] = DataChunk{
			StartRowID: i * int64(e.chunkSize),
			EndRowID:   min((i+1)*int64(e.chunkSize), rowCount),
		}
	}

	return &ParallelSort{
		PhysicalSort: physicalSort,
		chunks:       chunks,
		executor:     e,
	}
}

// Execute 执行并行计划
func (e *ParallelExecutor) Execute(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
	switch p := plan.(type) {
	case *PhysicalValues:
		return e.executePhysicalValues(ctx, p)
	case *PhysicalSelection:
		return e.executePhysicalSelection(ctx, p)
	case *PhysicalProjection:
		return e.executePhysicalProjection(ctx, p)
	case *PhysicalUnion:
		return e.executePhysicalUnion(ctx, p)
	case *PhysicalMergeJoin:
		return e.executePhysicalMergeJoin(ctx, p)
	case *PhysicalStreamAgg:
		return e.executePhysicalStreamAgg(ctx, p)
	case *PhysicalSubquery:
		return e.executePhysicalSubquery(ctx, p)
	case *PhysicalApply:
		return e.executePhysicalApply(ctx, p)
	case *PhysicalCTEStatement:
		return e.executePhysicalCTEStatement(ctx, p)
	case *PhysicalCTE:
		return e.executePhysicalCTE(ctx, p)
	case *PhysicalCTEScan:
		return e.executePhysicalCTEScan(ctx, p)
	case *ParallelTableScan:
		return e.executeParallelTableScan(ctx, p)
	case *ParallelIndexScan:
		return e.executeParallelIndexScan(ctx, p)
	case *ParallelHashJoin:
		return e.executeParallelHashJoin(ctx, p)
	case *ParallelHashAgg:
		return e.executeParallelHashAgg(ctx, p)
	case *ParallelSort:
		return e.executeParallelSort(ctx, p)
	default:
		return nil, fmt.Errorf("unsupported parallel physical plan: %T", plan)
	}
}

func (e *ParallelExecutor) executePhysicalCTEStatement(ctx context.Context, statement *PhysicalCTEStatement) ([][]interface{}, error) {
	if statement == nil {
		return nil, fmt.Errorf("physical CTE statement is nil")
	}
	children := statement.Children()
	if statement.DefinitionCount < 1 || statement.DefinitionCount >= len(children) {
		return nil, fmt.Errorf("physical CTE statement has invalid definition count %d", statement.DefinitionCount)
	}
	if e.cteRows == nil {
		e.cteRows = make(map[string][][]interface{})
	}
	if e.cteColumns == nil {
		e.cteColumns = make(map[string][]string)
	}
	previousRows := e.cteRows
	previousColumns := e.cteColumns
	e.cteRows = clonePhysicalCTERows(previousRows)
	e.cteColumns = clonePhysicalCTEColumns(previousColumns)
	defer func() {
		e.cteRows = previousRows
		e.cteColumns = previousColumns
	}()

	for _, child := range children[:statement.DefinitionCount] {
		switch definition := child.(type) {
		case *PhysicalCTE:
			rows, err := e.executePhysicalCTE(ctx, definition)
			if err != nil {
				return nil, err
			}
			name := strings.ToLower(strings.TrimSpace(definition.Name))
			if name == "" {
				return nil, fmt.Errorf("physical CTE definition has empty name")
			}
			e.cteRows[name] = rows
			e.cteColumns[name] = append([]string(nil), definition.Columns...)
			bindPhysicalCTEColumns(children[statement.DefinitionCount], name, definition.Columns)
		case *PhysicalRecursiveCTE:
			rows, err := e.executePhysicalRecursiveCTE(ctx, definition)
			if err != nil {
				return nil, err
			}
			name := strings.ToLower(strings.TrimSpace(definition.Name))
			if name == "" {
				return nil, fmt.Errorf("physical recursive CTE definition has empty name")
			}
			e.cteRows[name] = rows
			e.cteColumns[name] = append([]string(nil), definition.Columns...)
			bindPhysicalCTEColumns(children[statement.DefinitionCount], name, definition.Columns)
		default:
			return nil, fmt.Errorf("unsupported physical CTE definition: %T", child)
		}
	}
	return e.readPlanRows(ctx, children[statement.DefinitionCount])
}

func (e *ParallelExecutor) executePhysicalCTE(ctx context.Context, definition *PhysicalCTE) ([][]interface{}, error) {
	if definition == nil || len(definition.Children()) == 0 {
		return nil, fmt.Errorf("physical CTE requires a row-producing child")
	}
	return e.readPlanRows(ctx, definition.Children()[0])
}

func (e *ParallelExecutor) executePhysicalRecursiveCTE(ctx context.Context, definition *PhysicalRecursiveCTE) ([][]interface{}, error) {
	if definition == nil || len(definition.Children()) < 2 {
		return nil, fmt.Errorf("physical recursive CTE requires anchor and recursive children")
	}
	anchorRows, err := e.readPlanRows(ctx, definition.Children()[0])
	if err != nil {
		return nil, err
	}
	name := strings.ToLower(strings.TrimSpace(definition.Name))
	if name == "" {
		return nil, fmt.Errorf("physical recursive CTE definition has empty name")
	}
	e.cteColumns[name] = append([]string(nil), definition.Columns...)
	bindPhysicalCTEColumns(definition.Children()[1], name, definition.Columns)
	result := make([][]interface{}, 0, len(anchorRows))
	for _, row := range anchorRows {
		result = append(result, append([]interface{}(nil), row...))
	}
	delta := anchorRows
	const maxRecursiveIterations = 1000
	for iteration := 0; len(delta) > 0; iteration++ {
		if iteration >= maxRecursiveIterations {
			return nil, fmt.Errorf("physical recursive CTE %q exceeded iteration limit", definition.Name)
		}
		e.cteRows[name] = delta
		next, err := e.readPlanRows(ctx, definition.Children()[1])
		if err != nil {
			return nil, err
		}
		if len(next) == 0 {
			break
		}
		delta = next
		for _, row := range next {
			result = append(result, append([]interface{}(nil), row...))
		}
	}
	return result, nil
}

func (e *ParallelExecutor) executePhysicalCTEScan(_ context.Context, scan *PhysicalCTEScan) ([][]interface{}, error) {
	if scan == nil {
		return nil, fmt.Errorf("physical CTE scan is nil")
	}
	name := strings.ToLower(strings.TrimSpace(scan.Name))
	rows, ok := e.cteRows[name]
	if !ok {
		return nil, fmt.Errorf("physical CTE %q is not materialized", scan.Name)
	}
	return rows, nil
}

func clonePhysicalCTERows(source map[string][][]interface{}) map[string][][]interface{} {
	clone := make(map[string][][]interface{}, len(source))
	for name, rows := range source {
		copiedRows := make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			copiedRows = append(copiedRows, append([]interface{}(nil), row...))
		}
		clone[name] = copiedRows
	}
	return clone
}

func clonePhysicalCTEColumns(source map[string][]string) map[string][]string {
	clone := make(map[string][]string, len(source))
	for name, columns := range source {
		clone[name] = append([]string(nil), columns...)
	}
	return clone
}

// bindPhysicalCTEColumns carries the definition's output contract to scans in
// physical plans that were assembled without a schema object. The logical
// planner normally supplies a schema, but adapters and tests can construct a
// physical CTE directly; those plans still need qualified and unqualified
// column lookup to behave identically.
func bindPhysicalCTEColumns(plan PhysicalPlan, name string, columns []string) {
	if plan == nil || strings.TrimSpace(name) == "" || len(columns) == 0 {
		return
	}
	switch node := plan.(type) {
	case *PhysicalCTEScan:
		if strings.EqualFold(strings.TrimSpace(node.Name), strings.TrimSpace(name)) && len(node.Columns) == 0 {
			node.Columns = append([]string(nil), columns...)
		}
	case *PhysicalSubquery:
		bindPhysicalCTEColumns(node.Subplan, name, columns)
	}
	for _, child := range plan.Children() {
		bindPhysicalCTEColumns(child, name, columns)
	}
}

func (e *ParallelExecutor) executePhysicalSubquery(ctx context.Context, subquery *PhysicalSubquery) ([][]interface{}, error) {
	if subquery == nil || subquery.Subplan == nil {
		return nil, fmt.Errorf("physical subquery requires a subplan")
	}
	if subquery.Correlated && len(e.outerContext) == 0 {
		return nil, fmt.Errorf("correlated physical subquery requires an outer row context")
	}
	rows, err := e.readPlanRows(ctx, subquery.Subplan)
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(subquery.SubqueryType) {
	case "SCALAR":
		if len(rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(rows) == 0 || len(rows[0]) == 0 {
			return [][]interface{}{{nil}}, nil
		}
		return [][]interface{}{{rows[0][0]}}, nil
	case "EXISTS":
		return [][]interface{}{{len(rows) > 0}}, nil
	default:
		return rows, nil
	}
}

func (e *ParallelExecutor) executePhysicalApply(ctx context.Context, apply *PhysicalApply) ([][]interface{}, error) {
	if apply == nil || len(apply.Children()) < 2 {
		return nil, fmt.Errorf("physical apply requires two row-producing children")
	}
	applyType := strings.ToUpper(strings.TrimSpace(apply.ApplyType))
	if applyType == "" {
		applyType = "INNER"
	}
	if applyType != "INNER" && applyType != "LEFT" && applyType != "SEMI" && applyType != "ANTI" {
		return nil, fmt.Errorf("unsupported physical apply type %q", apply.ApplyType)
	}
	leftPlan, rightPlan := apply.Children()[0], apply.Children()[1]
	leftRows, err := e.readPlanRows(ctx, leftPlan)
	if err != nil {
		return nil, err
	}
	var rightRows [][]interface{}
	if !apply.Correlated {
		rightRows, err = e.readPlanRows(ctx, rightPlan)
		if err != nil {
			return nil, err
		}
	}
	leftColumns := physicalPlanColumnNames(leftPlan)
	rightColumns := physicalPlanColumnNames(rightPlan)
	leftTable := physicalPlanTableName(leftPlan)
	rightTable := physicalPlanTableName(rightPlan)
	rightNull := make([]interface{}, len(rightColumns))
	result := make([][]interface{}, 0)

	for _, leftRow := range leftRows {
		if apply.Correlated {
			previousOuterContext := e.outerContext
			e.outerContext = mergePhysicalRowContexts(leftTable, leftColumns, leftRow, "", nil, nil)
			rightRows, err = e.readPlanRows(ctx, rightPlan)
			e.outerContext = previousOuterContext
			if err != nil {
				return nil, err
			}
		}
		matched := false
		for _, rightRow := range rightRows {
			rowContext := mergePhysicalRowContexts(leftTable, leftColumns, leftRow, rightTable, rightColumns, rightRow)
			ok, matchErr := evaluatePhysicalConditionsWithContext(apply.JoinConds, rowContext)
			if matchErr != nil {
				return nil, matchErr
			}
			if !ok {
				continue
			}
			matched = true
			switch applyType {
			case "SEMI":
				result = append(result, append([]interface{}(nil), leftRow...))
			case "ANTI":
				// ANTI emits only left rows without a match.
			case "INNER", "LEFT":
				combined := make([]interface{}, 0, len(leftRow)+len(rightRow))
				combined = append(combined, leftRow...)
				combined = append(combined, rightRow...)
				result = append(result, combined)
			}
			if applyType == "SEMI" {
				break
			}
		}
		if !matched {
			switch applyType {
			case "ANTI":
				result = append(result, append([]interface{}(nil), leftRow...))
			case "LEFT":
				combined := make([]interface{}, 0, len(leftRow)+len(rightNull))
				combined = append(combined, leftRow...)
				combined = append(combined, rightNull...)
				result = append(result, combined)
			}
		}
	}
	return result, nil
}

func (e *ParallelExecutor) executePhysicalValues(ctx context.Context, values *PhysicalValues) ([][]interface{}, error) {
	if values == nil {
		return nil, fmt.Errorf("physical values plan is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row := make([]interface{}, 0, len(values.Exprs))
	for _, expression := range values.Exprs {
		if expression == nil {
			row = append(row, nil)
			continue
		}
		value, err := expression.Eval(&EvalContext{Row: e.physicalEvalContext(nil)})
		if err != nil {
			return nil, fmt.Errorf("evaluate physical values expression %s: %w", expression.String(), err)
		}
		row = append(row, value)
	}
	return [][]interface{}{row}, nil
}

func (e *ParallelExecutor) executePhysicalSelection(ctx context.Context, selection *PhysicalSelection) ([][]interface{}, error) {
	if selection == nil || len(selection.Children()) == 0 {
		return nil, fmt.Errorf("physical selection requires a row-producing child")
	}
	rows, err := e.readPlanRows(ctx, selection.Children()[0])
	if err != nil {
		return nil, err
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		matched, err := evaluatePhysicalConditionsWithContext(selection.Conditions, e.physicalEvalContext(physicalPlanRowContext(selection.Children()[0], row)))
		if err != nil {
			return nil, err
		}
		if matched {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func (e *ParallelExecutor) executePhysicalProjection(ctx context.Context, projection *PhysicalProjection) ([][]interface{}, error) {
	if projection == nil || len(projection.Children()) == 0 {
		return nil, fmt.Errorf("physical projection requires a row-producing child")
	}
	rows, err := e.readPlanRows(ctx, projection.Children()[0])
	if err != nil {
		return nil, err
	}
	projected := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, 0, len(projection.Exprs))
		rowContext := e.physicalEvalContext(physicalPlanRowContext(projection.Children()[0], row))
		for _, expression := range projection.Exprs {
			if expression == nil {
				values = append(values, nil)
				continue
			}
			value, evalErr := expression.Eval(&EvalContext{Row: rowContext})
			if evalErr != nil {
				return nil, fmt.Errorf("evaluate physical projection expression %s: %w", expression.String(), evalErr)
			}
			values = append(values, value)
		}
		projected = append(projected, values)
	}
	if projection.Distinct {
		projected = distinctRows(projected)
	}
	return applySortAndLimit(projected, projection.OrderBy, projection.Offset, projection.Limit, projection.HasLimit), nil
}

func distinctRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	distinct := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		key := physicalRowKey(row)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		distinct = append(distinct, row)
	}
	return distinct
}

func (e *ParallelExecutor) executePhysicalUnion(ctx context.Context, union *PhysicalUnion) ([][]interface{}, error) {
	if union == nil || len(union.Children()) == 0 {
		return nil, fmt.Errorf("physical union requires at least one child")
	}
	childRows := make([][][]interface{}, 0, len(union.Children()))
	for _, child := range union.Children() {
		rows, err := e.readPlanRows(ctx, child)
		if err != nil {
			return nil, err
		}
		childRows = append(childRows, rows)
	}
	rows := executePhysicalSetOperation(union.UnionType, childRows)
	return applySortAndLimit(rows, union.OrderBy, union.Offset, union.Limit, union.HasLimit), nil
}

func executePhysicalSetOperation(unionType string, children [][][]interface{}) [][]interface{} {
	if len(children) == 0 {
		return nil
	}
	normalized := strings.ToUpper(strings.TrimSpace(unionType))
	if normalized == "" || normalized == "UNION" || normalized == "DISTINCT" {
		normalized = "UNION DISTINCT"
	} else if normalized == "ALL" {
		normalized = "UNION ALL"
	}
	if normalized == "UNION ALL" {
		rows := make([][]interface{}, 0)
		for _, child := range children {
			for _, row := range child {
				rows = append(rows, append([]interface{}(nil), row...))
			}
		}
		return rows
	}
	if normalized == "UNION DISTINCT" {
		rows := make([][]interface{}, 0)
		seen := make(map[string]struct{})
		for _, child := range children {
			for _, row := range child {
				key := physicalRowKey(row)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				rows = append(rows, append([]interface{}(nil), row...))
			}
		}
		return rows
	}

	first := children[0]
	if strings.HasPrefix(normalized, "INTERSECT") {
		if strings.HasSuffix(normalized, "ALL") {
			minimum := physicalRowCounts(first)
			for _, child := range children[1:] {
				counts := physicalRowCounts(child)
				for key, count := range minimum {
					if counts[key] < count {
						minimum[key] = counts[key]
					}
				}
			}
			return rowsFromCountsInOrder(first, minimum)
		}
		present := make(map[string]struct{}, len(first))
		for _, row := range first {
			present[physicalRowKey(row)] = struct{}{}
		}
		for _, child := range children[1:] {
			childKeys := make(map[string]struct{}, len(child))
			for _, row := range child {
				childKeys[physicalRowKey(row)] = struct{}{}
			}
			for key := range present {
				if _, exists := childKeys[key]; !exists {
					delete(present, key)
				}
			}
		}
		rows := make([][]interface{}, 0, len(present))
		seen := make(map[string]struct{}, len(present))
		for _, row := range first {
			key := physicalRowKey(row)
			if _, exists := present[key]; !exists {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			rows = append(rows, append([]interface{}(nil), row...))
		}
		return rows
	}

	if strings.HasPrefix(normalized, "EXCEPT") {
		if strings.HasSuffix(normalized, "ALL") {
			removed := make(map[string]int)
			for _, child := range children[1:] {
				for key, count := range physicalRowCounts(child) {
					removed[key] += count
				}
			}
			remaining := physicalRowCounts(first)
			for key, count := range removed {
				remaining[key] -= count
				if remaining[key] < 0 {
					remaining[key] = 0
				}
			}
			return rowsFromCountsInOrder(first, remaining)
		}
		removed := make(map[string]struct{})
		for _, child := range children[1:] {
			for _, row := range child {
				removed[physicalRowKey(row)] = struct{}{}
			}
		}
		rows := make([][]interface{}, 0)
		seen := make(map[string]struct{})
		for _, row := range first {
			key := physicalRowKey(row)
			if _, exists := removed[key]; exists {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			rows = append(rows, append([]interface{}(nil), row...))
		}
		return rows
	}
	return nil
}

func physicalRowCounts(rows [][]interface{}) map[string]int {
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[physicalRowKey(row)]++
	}
	return counts
}

func rowsFromCountsInOrder(rows [][]interface{}, counts map[string]int) [][]interface{} {
	result := make([][]interface{}, 0)
	for _, row := range rows {
		key := physicalRowKey(row)
		if counts[key] <= 0 {
			continue
		}
		counts[key]--
		result = append(result, append([]interface{}(nil), row...))
	}
	return result
}

func applySortAndLimit(rows [][]interface{}, orderBy []ByItem, offset, limit int64, hasLimit bool) [][]interface{} {
	if len(orderBy) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			return rowLess(rows[i], rows[j], orderBy)
		})
	}
	if !hasLimit {
		return rows
	}
	start := offset
	if start < 0 {
		start = 0
	}
	if start >= int64(len(rows)) {
		return [][]interface{}{}
	}
	end := int64(len(rows))
	if limit >= 0 && start+limit < end {
		end = start + limit
	}
	return rows[start:end]
}

func (e *ParallelExecutor) executePhysicalMergeJoin(ctx context.Context, join *PhysicalMergeJoin) ([][]interface{}, error) {
	if join == nil || len(join.Children()) < 2 {
		return nil, fmt.Errorf("physical merge join requires two row-producing children")
	}
	leftRows, err := e.readPlanRows(ctx, join.Children()[0])
	if err != nil {
		return nil, err
	}
	rightRows, err := e.readPlanRows(ctx, join.Children()[1])
	if err != nil {
		return nil, err
	}
	leftColumns := physicalPlanColumnNames(join.Children()[0])
	rightColumns := physicalPlanColumnNames(join.Children()[1])
	leftTable := physicalPlanTableName(join.Children()[0])
	rightTable := physicalPlanTableName(join.Children()[1])
	rightNull := make([]interface{}, len(rightColumns))
	leftNull := make([]interface{}, len(leftColumns))
	result := make([][]interface{}, 0)
	joinType := strings.ToUpper(strings.TrimSpace(join.JoinType))
	leftJoin := strings.Contains(joinType, "LEFT")
	rightJoin := strings.Contains(joinType, "RIGHT")
	fullJoin := strings.Contains(joinType, "FULL")
	matchedRight := make([]bool, len(rightRows))

	if rightJoin && !fullJoin {
		for _, rightRow := range rightRows {
			matched := false
			for _, leftRow := range leftRows {
				rowContext := mergePhysicalRowContexts(leftTable, leftColumns, leftRow, rightTable, rightColumns, rightRow)
				ok, matchErr := evaluatePhysicalConditionsWithContext(join.Conditions, rowContext)
				if matchErr != nil {
					return nil, matchErr
				}
				if !ok {
					continue
				}
				result = append(result, append(append([]interface{}{}, leftRow...), rightRow...))
				matched = true
			}
			if !matched {
				result = append(result, append(append([]interface{}{}, leftNull...), rightRow...))
			}
		}
		return result, nil
	}

	for _, leftRow := range leftRows {
		matched := false
		for rightIndex, rightRow := range rightRows {
			rowContext := mergePhysicalRowContexts(leftTable, leftColumns, leftRow, rightTable, rightColumns, rightRow)
			ok, matchErr := evaluatePhysicalConditionsWithContext(join.Conditions, rowContext)
			if matchErr != nil {
				return nil, matchErr
			}
			if !ok {
				continue
			}
			combined := make([]interface{}, 0, len(leftRow)+len(rightRow))
			combined = append(combined, leftRow...)
			combined = append(combined, rightRow...)
			result = append(result, combined)
			matched = true
			matchedRight[rightIndex] = true
		}
		if (leftJoin || fullJoin) && !matched {
			combined := make([]interface{}, 0, len(leftRow)+len(rightNull))
			combined = append(combined, leftRow...)
			combined = append(combined, rightNull...)
			result = append(result, combined)
		}
	}
	if fullJoin {
		for rightIndex, rightRow := range rightRows {
			if !matchedRight[rightIndex] {
				result = append(result, append(append([]interface{}{}, leftNull...), rightRow...))
			}
		}
	}
	return result, nil
}

func (e *ParallelExecutor) executePhysicalStreamAgg(ctx context.Context, agg *PhysicalStreamAgg) ([][]interface{}, error) {
	if agg == nil || len(agg.Children()) == 0 {
		return nil, fmt.Errorf("physical stream aggregate requires a row-producing child")
	}
	parallelAgg := &ParallelHashAgg{
		PhysicalHashAgg: PhysicalHashAgg{
			BasePhysicalPlan: BasePhysicalPlan{children: agg.Children()},
			GroupByItems:     agg.GroupByItems,
			AggFuncs:         agg.AggFuncs,
		},
		partitions: e.workers,
		executor:   e,
	}
	spec, err := deriveParallelHashAggregateSpec(parallelAgg)
	if err != nil {
		return nil, err
	}
	parallelAgg.AggregateSpec = spec
	return e.executeParallelHashAggFromRows(ctx, parallelAgg)
}

func physicalRowKey(row []interface{}) string {
	parts := make([]string, 0, len(row))
	for _, value := range row {
		parts = append(parts, fmt.Sprintf("%T:%#v", value, value))
	}
	return strings.Join(parts, "\x00")
}

func evaluatePhysicalConditions(conditions []Expression, columns []string, row []interface{}) (bool, error) {
	return evaluatePhysicalConditionsWithContext(conditions, physicalRowContext(columns, row))
}

func evaluatePhysicalConditionsWithContext(conditions []Expression, rowContext map[string]interface{}) (bool, error) {
	for _, condition := range conditions {
		if condition == nil {
			continue
		}
		value, err := condition.Eval(&EvalContext{Row: rowContext})
		if err != nil {
			return false, fmt.Errorf("evaluate physical selection condition %s: %w", condition.String(), err)
		}
		if !physicalValueTruthy(value) {
			return false, nil
		}
	}
	return true, nil
}

func physicalPlanTableName(plan PhysicalPlan) string {
	switch node := plan.(type) {
	case *PhysicalTableScan:
		if node.Table != nil {
			return node.Table.Name
		}
	case *PhysicalIndexScan:
		if node.Table != nil {
			return node.Table.Name
		}
	case *ParallelTableScan:
		if node.Table != nil {
			return node.Table.Name
		}
	case *ParallelIndexScan:
		if node.Table != nil {
			return node.Table.Name
		}
	case *PhysicalCTEScan:
		return strings.TrimSpace(node.Name)
	case *PhysicalCTE:
		return strings.TrimSpace(node.Name)
	case *PhysicalRecursiveCTE:
		return strings.TrimSpace(node.Name)
	case *PhysicalSelection, *PhysicalProjection, *PhysicalSort, *PhysicalHashAgg, *PhysicalStreamAgg,
		*ParallelHashAgg, *ParallelSort, *PhysicalSubquery:
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanTableName(children[0])
		}
	}
	return ""
}

func mergePhysicalRowContexts(leftTable string, leftColumns []string, leftRow []interface{}, rightTable string, rightColumns []string, rightRow []interface{}) map[string]interface{} {
	values := make(map[string]interface{}, (len(leftColumns)+len(rightColumns))*4)
	add := func(tableName string, columns []string, row []interface{}) {
		for index, column := range columns {
			if index >= len(row) {
				break
			}
			value := row[index]
			if _, exists := values[column]; !exists {
				values[column] = value
				values[strings.ToLower(column)] = value
			}
			if tableName != "" {
				qualified := tableName + "." + column
				values[qualified] = value
				values[strings.ToLower(qualified)] = value
			}
		}
	}
	add(leftTable, leftColumns, leftRow)
	add(rightTable, rightColumns, rightRow)
	return values
}

func (e *ParallelExecutor) physicalEvalContext(rowContext map[string]interface{}) map[string]interface{} {
	context := make(map[string]interface{}, len(e.outerContext)+len(rowContext))
	for key, value := range e.outerContext {
		context[key] = value
	}
	for key, value := range rowContext {
		context[key] = value
	}
	return context
}

func physicalPlanColumnNames(plan PhysicalPlan) []string {
	if plan == nil {
		return nil
	}
	switch node := plan.(type) {
	case *PhysicalTableScan:
		return physicalScanColumnNames(node.Table, node.RequiredColumns)
	case *PhysicalIndexScan:
		return physicalScanColumnNames(node.Table, node.RequiredColumns)
	case *ParallelTableScan:
		return physicalScanColumnNames(node.Table, node.RequiredColumns)
	case *ParallelIndexScan:
		return physicalScanColumnNames(node.Table, node.RequiredColumns)
	case *PhysicalValues:
		if names := physicalExpressionColumnNames(node.Exprs); len(names) > 0 {
			return names
		}
	case *PhysicalSubquery:
		if node.Subplan != nil {
			names := physicalPlanColumnNames(node.Subplan)
			if strings.EqualFold(strings.TrimSpace(node.SubqueryType), "SCALAR") || strings.EqualFold(strings.TrimSpace(node.SubqueryType), "EXISTS") {
				if len(names) > 1 {
					return names[:1]
				}
			}
			if len(names) > 0 {
				return names
			}
		}
	case *PhysicalCTE:
		if len(node.Columns) > 0 {
			return append([]string(nil), node.Columns...)
		}
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalRecursiveCTE:
		if len(node.Columns) > 0 {
			return append([]string(nil), node.Columns...)
		}
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalCTEScan:
		if len(node.Columns) > 0 {
			return append([]string(nil), node.Columns...)
		}
	case *PhysicalSelection:
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalSort, *ParallelSort:
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalHashJoin, *ParallelHashJoin, *PhysicalMergeJoin:
		children := plan.Children()
		if len(children) >= 2 {
			left := physicalPlanColumnNames(children[0])
			right := physicalPlanColumnNames(children[1])
			return append(append([]string(nil), left...), right...)
		}
	case *PhysicalApply:
		children := plan.Children()
		if len(children) >= 2 {
			left := physicalPlanColumnNames(children[0])
			applyType := strings.ToUpper(strings.TrimSpace(node.ApplyType))
			if applyType == "SEMI" || applyType == "ANTI" {
				return left
			}
			return append(left, physicalPlanColumnNames(children[1])...)
		}
	case *PhysicalHashAgg:
		if names := aggregateOutputColumnNames(node.GroupByItems, node.AggFuncs); len(names) > 0 {
			return names
		}
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *ParallelHashAgg:
		if names := aggregateOutputColumnNames(node.GroupByItems, node.AggFuncs); len(names) > 0 {
			return names
		}
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalStreamAgg:
		if names := aggregateOutputColumnNames(node.GroupByItems, node.AggFuncs); len(names) > 0 {
			return names
		}
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalProjection:
		if len(node.OutputNames) == len(node.Exprs) && len(node.OutputNames) > 0 {
			return append([]string(nil), node.OutputNames...)
		}
		names := make([]string, 0, len(node.Exprs))
		for _, expr := range node.Exprs {
			if expr == nil {
				continue
			}
			if name := strings.TrimSpace(expr.String()); name != "" {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			return names
		}
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalUnion:
		children := plan.Children()
		if len(children) > 0 {
			return physicalPlanColumnNames(children[0])
		}
	case *PhysicalCTEStatement:
		children := plan.Children()
		if node.DefinitionCount >= 0 && node.DefinitionCount < len(children) {
			return physicalPlanColumnNames(children[node.DefinitionCount])
		}
	}
	if schema := plan.Schema(); schema != nil {
		tableNames := make([]string, 0, len(schema.Tables))
		for tableName := range schema.Tables {
			tableNames = append(tableNames, tableName)
		}
		sort.Strings(tableNames)
		if len(tableNames) > 0 {
			return physicalTableColumnNames(schema.Tables[tableNames[0]])
		}
	}
	return nil
}

func aggregateOutputColumnNames(groupBy []Expression, aggregates []AggregateFunc) []string {
	names := make([]string, 0, len(groupBy)+len(aggregates))
	for _, expression := range groupBy {
		if expression != nil && strings.TrimSpace(expression.String()) != "" {
			names = append(names, expression.String())
		}
	}
	for _, aggregate := range aggregates {
		if aggregate == nil {
			continue
		}
		name := aggregate.Name()
		if function, ok := aggregate.(*Function); ok {
			name = function.String()
		}
		if strings.TrimSpace(name) != "" {
			names = append(names, name)
		}
	}
	return names
}

func physicalExpressionColumnNames(expressions []Expression) []string {
	names := make([]string, 0, len(expressions))
	for _, expression := range expressions {
		if expression == nil {
			continue
		}
		if name := strings.TrimSpace(expression.String()); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func physicalTableColumnNames(table *metadata.Table) []string {
	if table == nil {
		return nil
	}
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if column != nil {
			columns = append(columns, column.Name)
		}
	}
	return columns
}

func physicalScanColumnNames(table *metadata.Table, requiredColumns []string) []string {
	if len(requiredColumns) == 0 {
		return physicalTableColumnNames(table)
	}
	if table == nil {
		return append([]string(nil), requiredColumns...)
	}
	names := make([]string, 0, len(requiredColumns))
	for _, required := range requiredColumns {
		for _, column := range table.Columns {
			if column != nil && strings.EqualFold(strings.TrimSpace(column.Name), physicalRequiredColumnName(required)) {
				names = append(names, column.Name)
				break
			}
		}
	}
	return names
}

func physicalRequiredColumnName(required string) string {
	parts := strings.Split(strings.TrimSpace(required), ".")
	return strings.Trim(strings.TrimSpace(parts[len(parts)-1]), "`")
}

func physicalRowContext(columns []string, row []interface{}) map[string]interface{} {
	values := make(map[string]interface{}, len(columns)*2)
	for index, column := range columns {
		if index >= len(row) {
			break
		}
		values[column] = row[index]
		values[strings.ToLower(column)] = row[index]
	}
	return values
}

func physicalPlanRowContext(plan PhysicalPlan, row []interface{}) map[string]interface{} {
	context := physicalRowContext(physicalPlanColumnNames(plan), row)
	addQualifiedChildContext := func(child PhysicalPlan, childRow []interface{}) {
		for key, value := range physicalPlanRowContext(child, childRow) {
			if strings.Contains(key, ".") {
				context[key] = value
			}
		}
	}
	splitChildRow := func(offset, width int) []interface{} {
		if offset < 0 || offset >= len(row) || width <= 0 {
			return nil
		}
		end := offset + width
		if end > len(row) {
			end = len(row)
		}
		return row[offset:end]
	}
	switch node := plan.(type) {
	case *PhysicalHashJoin, *ParallelHashJoin, *PhysicalMergeJoin:
		children := plan.Children()
		if len(children) >= 2 {
			leftWidth := len(physicalPlanColumnNames(children[0]))
			addQualifiedChildContext(children[0], splitChildRow(0, leftWidth))
			addQualifiedChildContext(children[1], splitChildRow(leftWidth, len(physicalPlanColumnNames(children[1]))))
		}
	case *PhysicalApply:
		children := plan.Children()
		if len(children) >= 2 {
			leftWidth := len(physicalPlanColumnNames(children[0]))
			addQualifiedChildContext(children[0], splitChildRow(0, leftWidth))
			applyType := strings.ToUpper(strings.TrimSpace(node.ApplyType))
			if applyType != "SEMI" && applyType != "ANTI" {
				addQualifiedChildContext(children[1], splitChildRow(leftWidth, len(physicalPlanColumnNames(children[1]))))
			}
		}
	case *PhysicalUnion:
		children := plan.Children()
		if len(children) > 0 {
			addQualifiedChildContext(children[0], row)
		}
	case *PhysicalCTEStatement:
		children := plan.Children()
		if node.DefinitionCount >= 0 && node.DefinitionCount < len(children) {
			addQualifiedChildContext(children[node.DefinitionCount], row)
		}
	}
	tableName := physicalPlanTableName(plan)
	if tableName == "" {
		return context
	}
	for index, column := range physicalPlanColumnNames(plan) {
		if index >= len(row) {
			break
		}
		value := row[index]
		qualified := tableName + "." + column
		context[qualified] = value
		context[strings.ToLower(qualified)] = value
	}
	return context
}

func physicalValueTruthy(value interface{}) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case int:
		return typed != 0
	case int8:
		return typed != 0
	case int16:
		return typed != 0
	case int32:
		return typed != 0
	case int64:
		return typed != 0
	case uint:
		return typed != 0
	case uint8:
		return typed != 0
	case uint16:
		return typed != 0
	case uint32:
		return typed != 0
	case uint64:
		return typed != 0
	case float32:
		return typed != 0
	case float64:
		return typed != 0
	case string:
		return typed != ""
	default:
		return true
	}
}

func (e *ParallelExecutor) executeParallelIndexScan(ctx context.Context, scan *ParallelIndexScan) ([][]interface{}, error) {
	if scan == nil {
		return nil, fmt.Errorf("parallel index scan is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scan.ChunkReader == nil {
		if e.rowReader == nil {
			return nil, fmt.Errorf("parallel index scan requires a configured IndexChunkReader or PlanRowReader")
		}
		source := PhysicalPlan(scan)
		if scan.source != nil {
			source = scan.source
		}
		rows, err := e.rowReader(ctx, source)
		if err != nil {
			return nil, err
		}
		return projectRequiredColumns(scan.Table, scan.RequiredColumns, rows, "parallel index scan")
	}

	chunkResults := make([][][]interface{}, len(scan.chunks))
	errs := make(chan error, len(scan.chunks))
	var wg sync.WaitGroup
	for index, chunk := range scan.chunks {
		wg.Add(1)
		go func(index int, chunk DataChunk) {
			defer wg.Done()
			select {
			case e.workerPool <- struct{}{}:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
			defer func() { <-e.workerPool }()
			rows, err := scan.ChunkReader(ctx, chunk)
			if err != nil {
				errs <- err
				return
			}
			rows, err = projectRequiredColumns(scan.Table, scan.RequiredColumns, rows, "parallel index scan")
			if err != nil {
				errs <- err
				return
			}
			chunkResults[index] = rows
		}(index, chunk)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return nil, err
		}
	}
	results := make([][]interface{}, 0)
	for _, rows := range chunkResults {
		results = append(results, rows...)
	}
	return results, nil
}

func (e *ParallelExecutor) executeParallelTableScan(ctx context.Context, scan *ParallelTableScan) ([][]interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	chunkResults := make([][][]interface{}, len(scan.chunks))
	errs := make(chan error, len(scan.chunks))
	var wg sync.WaitGroup

	// 并行扫描每个数据分片
	for index, chunk := range scan.chunks {
		wg.Add(1)
		go func(index int, chunk DataChunk) {
			defer wg.Done()

			if err := ctx.Err(); err != nil {
				errs <- err
				return
			}
			// 获取工作线程
			select {
			case e.workerPool <- struct{}{}:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
			defer func() { <-e.workerPool }()

			// 扫描分片
			rows, err := scanChunk(ctx, scan, chunk)
			if err != nil {
				errs <- err
				return
			}
			chunkResults[index] = rows
		}(index, chunk)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return nil, err
		}
	}

	results := make([][]interface{}, 0)
	for _, rows := range chunkResults {
		results = append(results, rows...)
	}
	return results, nil
}

func (e *ParallelExecutor) executeParallelHashJoin(ctx context.Context, join *ParallelHashJoin) ([][]interface{}, error) {
	if len(join.Children()) < 2 {
		return nil, fmt.Errorf("parallel hash join requires two row-producing children")
	}
	if len(join.HashJoinKeys) == 0 {
		fallback := &PhysicalMergeJoin{
			BasePhysicalPlan: BasePhysicalPlan{children: join.Children()},
			JoinType:         join.JoinType,
			Conditions:       join.Conditions,
		}
		return e.executePhysicalMergeJoin(ctx, fallback)
	}
	return e.executeParallelHashJoinFromRows(ctx, join)
}

func (e *ParallelExecutor) executeParallelHashJoinFromRows(ctx context.Context, join *ParallelHashJoin) ([][]interface{}, error) {
	children := join.Children()
	leftRows, err := e.readPlanRows(ctx, children[0])
	if err != nil {
		return nil, err
	}
	rightRows, err := e.readPlanRows(ctx, children[1])
	if err != nil {
		return nil, err
	}
	// The map stores row positions within the right input so duplicate keys
	// can retain their original positions and participate in outer joins.
	rightByKey := make(map[string][]int, len(rightRows))
	for index, row := range rightRows {
		key, ok := parallelHashJoinKey(row, join.HashJoinKeys, false)
		if ok {
			rightByKey[key] = append(rightByKey[key], index)
		}
	}

	partitions := e.workers
	if partitions < 1 {
		partitions = 1
	}
	leftColumns := physicalPlanColumnNames(children[0])
	rightColumns := physicalPlanColumnNames(children[1])
	leftTable := physicalPlanTableName(children[0])
	rightTable := physicalPlanTableName(children[1])
	partitionResults := make([][][]interface{}, partitions)
	partitionMatchedRight := make([][]bool, partitions)
	joinType := strings.ToUpper(strings.TrimSpace(join.JoinType))
	leftJoin := strings.Contains(joinType, "LEFT")
	rightJoin := strings.Contains(joinType, "RIGHT")
	fullJoin := strings.Contains(joinType, "FULL")
	rightNull := make([]interface{}, len(rightColumns))
	leftNull := make([]interface{}, len(leftColumns))
	errs := make(chan error, partitions)
	var wg sync.WaitGroup
	for partition := 0; partition < partitions; partition++ {
		start := partition * len(leftRows) / partitions
		end := (partition + 1) * len(leftRows) / partitions
		wg.Add(1)
		go func(partition, start, end int) {
			defer wg.Done()
			select {
			case e.workerPool <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-e.workerPool }()
			rows := make([][]interface{}, 0)
			matchedRight := make([]bool, len(rightRows))
			for _, leftRow := range leftRows[start:end] {
				if err := ctx.Err(); err != nil {
					return
				}
				key, ok := parallelHashJoinKey(leftRow, join.HashJoinKeys, true)
				if !ok {
					if leftJoin || fullJoin {
						combined := append(append([]interface{}{}, leftRow...), rightNull...)
						rows = append(rows, combined)
					}
					continue
				}
				leftMatched := false
				for _, rightIndex := range rightByKey[key] {
					rightRow := rightRows[rightIndex]
					rowContext := mergePhysicalRowContexts(leftTable, leftColumns, leftRow, rightTable, rightColumns, rightRow)
					matched, matchErr := evaluatePhysicalConditionsWithContext(join.Conditions, rowContext)
					if matchErr != nil {
						errs <- matchErr
						return
					}
					if !matched {
						continue
					}
					leftMatched = true
					matchedRight[rightIndex] = true
					combined := make([]interface{}, 0, len(leftRow)+len(rightRow))
					combined = append(combined, leftRow...)
					combined = append(combined, rightRow...)
					rows = append(rows, combined)
				}
				if (leftJoin || fullJoin) && !leftMatched {
					combined := append(append([]interface{}{}, leftRow...), rightNull...)
					rows = append(rows, combined)
				}
			}
			partitionResults[partition] = rows
			partitionMatchedRight[partition] = matchedRight
		}(partition, start, end)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results := make([][]interface{}, 0)
	for _, rows := range partitionResults {
		results = append(results, rows...)
	}
	if rightJoin || fullJoin {
		matchedRight := make([]bool, len(rightRows))
		for _, partition := range partitionMatchedRight {
			for index, matched := range partition {
				matchedRight[index] = matchedRight[index] || matched
			}
		}
		for index, rightRow := range rightRows {
			if matchedRight[index] {
				continue
			}
			combined := append(append([]interface{}{}, leftNull...), rightRow...)
			results = append(results, combined)
		}
	}
	return results, nil
}

func parallelHashJoinKey(row []interface{}, keys []HashJoinKey, left bool) (string, bool) {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		index := key.RightIndex
		if left {
			index = key.LeftIndex
		}
		if index < 0 || index >= len(row) || row[index] == nil {
			return "", false
		}
		parts = append(parts, parallelHashJoinValueKey(row[index]))
	}
	return strings.Join(parts, "\x00"), true
}

func parallelHashJoinValueKey(value interface{}) string {
	if numeric, ok := numericComparableValue(value); ok {
		return fmt.Sprintf("numeric:%.17g", numeric)
	}
	switch typed := value.(type) {
	case string:
		return "text:" + typed
	case []byte:
		return "text:" + string(typed)
	default:
		return fmt.Sprintf("%T:%#v", value, value)
	}
}

func (e *ParallelExecutor) executeParallelHashAgg(ctx context.Context, agg *ParallelHashAgg) ([][]interface{}, error) {
	if agg.AggregateSpec == nil {
		spec, err := deriveParallelHashAggregateSpec(agg)
		if err != nil {
			return nil, err
		}
		agg.AggregateSpec = spec
	}
	if len(agg.Children()) == 0 {
		return nil, fmt.Errorf("parallel hash aggregate requires a row-producing child")
	}
	return e.executeParallelHashAggFromRows(ctx, agg)
}

func deriveParallelHashAggregateSpec(agg *ParallelHashAgg) (*HashAggregateSpec, error) {
	if agg == nil || len(agg.Children()) == 0 {
		return nil, fmt.Errorf("parallel hash aggregate requires a row-producing child")
	}
	columns := physicalPlanColumnNames(agg.Children()[0])
	findColumn := func(expression Expression) (int, error) {
		column, ok := expression.(*Column)
		if !ok || column == nil {
			return -1, fmt.Errorf("parallel hash aggregate supports column expressions only, got %T", expression)
		}
		requiredColumn := physicalRequiredColumnName(column.Name)
		for index, name := range columns {
			if strings.EqualFold(strings.TrimSpace(name), requiredColumn) {
				return index, nil
			}
		}
		return -1, fmt.Errorf("parallel hash aggregate column %q is not present in child schema", column.Name)
	}
	spec := &HashAggregateSpec{GroupByIndexes: make([]int, 0, len(agg.GroupByItems)), Functions: make([]HashAggregateFunction, 0, len(agg.AggFuncs))}
	for _, groupBy := range agg.GroupByItems {
		index, err := findColumn(groupBy)
		if err != nil {
			return nil, err
		}
		spec.GroupByIndexes = append(spec.GroupByIndexes, index)
	}
	for _, aggregate := range agg.AggFuncs {
		if aggregate == nil {
			return nil, fmt.Errorf("parallel hash aggregate contains a nil aggregate function")
		}
		function := HashAggregateFunction{Name: aggregate.Name(), InputIndex: -1}
		if planFunction, ok := aggregate.(*Function); ok {
			function.Distinct = planFunction.Distinct
			function.Separator = planFunction.Separator
		}
		args := aggregate.Args()
		if len(args) > 0 {
			function.InputIndexes = make([]int, 0, len(args))
			for _, arg := range args {
				index, err := findColumn(arg)
				if err != nil {
					return nil, err
				}
				function.InputIndexes = append(function.InputIndexes, index)
			}
			function.InputIndex = function.InputIndexes[0]
		}
		spec.Functions = append(spec.Functions, function)
	}
	return spec, nil
}

type parallelAggregateState struct {
	name       string
	count      int64
	sum        float64
	mean       float64
	m2         float64
	hasValue   bool
	value      interface{}
	bitValue   uint64
	bitInit    bool
	values     []string
	jsonValues []interface{}
	jsonObject map[string]interface{}
	seen       map[string]struct{}
	separator  string
	distinct   bool
}

type parallelAggregateBucket struct {
	groupValues []interface{}
	states      []parallelAggregateState
}

func (e *ParallelExecutor) executeParallelHashAggFromRows(ctx context.Context, agg *ParallelHashAgg) ([][]interface{}, error) {
	rows, err := e.readPlanRows(ctx, agg.Children()[0])
	if err != nil {
		return nil, err
	}
	spec := agg.AggregateSpec
	partitions := e.workers
	if partitions < 1 {
		partitions = 1
	}
	buckets := make([]map[string]*parallelAggregateBucket, partitions)
	for index := range buckets {
		buckets[index] = make(map[string]*parallelAggregateBucket)
	}
	errCh := make(chan error, partitions)
	var wg sync.WaitGroup
	for partition := 0; partition < partitions; partition++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			select {
			case e.workerPool <- struct{}{}:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			defer func() { <-e.workerPool }()
			for _, row := range rows {
				if err := ctx.Err(); err != nil {
					errCh <- err
					return
				}
				key, groupValues, ok := parallelGroupKey(row, spec.GroupByIndexes)
				if !ok || parallelGroupPartition(key, partitions) != partition {
					continue
				}
				bucket := buckets[partition][key]
				if bucket == nil {
					bucket = &parallelAggregateBucket{groupValues: groupValues, states: make([]parallelAggregateState, len(spec.Functions))}
					for index, function := range spec.Functions {
						bucket.states[index].name = strings.ToUpper(strings.TrimSpace(function.Name))
						bucket.states[index].separator = function.Separator
						bucket.states[index].distinct = function.Distinct
						if function.Distinct {
							bucket.states[index].seen = make(map[string]struct{})
						}
						if bucket.states[index].name == "JSON_OBJECTAGG" {
							bucket.states[index].jsonObject = make(map[string]interface{})
						}
						if bucket.states[index].name == "BIT_AND" {
							bucket.states[index].bitValue = ^uint64(0)
						}
					}
					buckets[partition][key] = bucket
				}
				if err := updateParallelAggregateStates(bucket, row, spec.Functions); err != nil {
					errCh <- err
					return
				}
			}
		}(partition)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	results := make([][]interface{}, 0)
	allBuckets := make(map[string]*parallelAggregateBucket)
	for partition := range buckets {
		for key, bucket := range buckets[partition] {
			allBuckets[key] = bucket
		}
	}
	keys := make([]string, 0, len(allBuckets))
	for key := range allBuckets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		bucket := allBuckets[key]
		row := append([]interface{}(nil), bucket.groupValues...)
		for index, state := range bucket.states {
			switch state.name {
			case "COUNT":
				row = append(row, state.count)
			case "SUM":
				if !state.hasValue {
					row = append(row, nil)
				} else {
					row = append(row, state.sum)
				}
			case "AVG":
				if state.count == 0 {
					row = append(row, nil)
				} else {
					row = append(row, state.sum/float64(state.count))
				}
			case "MIN", "MAX":
				if !state.hasValue {
					row = append(row, nil)
				} else {
					row = append(row, state.value)
				}
			case "ANY_VALUE":
				if !state.hasValue {
					row = append(row, nil)
				} else {
					row = append(row, state.value)
				}
			case "BIT_AND", "BIT_OR", "BIT_XOR":
				if !state.bitInit && state.name != "BIT_AND" {
					row = append(row, int64(0))
				} else {
					row = append(row, int64(state.bitValue))
				}
			case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
				if state.count == 0 || (strings.HasSuffix(state.name, "_SAMP") && state.count < 2) {
					row = append(row, nil)
					break
				}
				denominator := float64(state.count)
				if strings.HasSuffix(state.name, "_SAMP") {
					denominator = float64(state.count - 1)
				}
				variance := state.m2 / denominator
				if strings.HasPrefix(state.name, "STD") || state.name == "STD" {
					row = append(row, math.Sqrt(variance))
				} else {
					row = append(row, variance)
				}
			case "GROUP_CONCAT":
				if len(state.values) == 0 {
					row = append(row, nil)
					break
				}
				separator := state.separator
				if separator == "" {
					separator = ","
				}
				row = append(row, strings.Join(state.values, separator))
			case "JSON_ARRAYAGG":
				encoded, err := json.Marshal(state.jsonValues)
				if err != nil {
					return nil, fmt.Errorf("encode parallel JSON_ARRAYAGG: %w", err)
				}
				row = append(row, string(encoded))
			case "JSON_OBJECTAGG":
				encoded, err := json.Marshal(state.jsonObject)
				if err != nil {
					return nil, fmt.Errorf("encode parallel JSON_OBJECTAGG: %w", err)
				}
				row = append(row, string(encoded))
			default:
				return nil, fmt.Errorf("unsupported parallel aggregate %q at index %d", state.name, index)
			}
		}
		results = append(results, row)
	}
	return results, nil
}

func updateParallelAggregateStates(bucket *parallelAggregateBucket, row []interface{}, functions []HashAggregateFunction) error {
	for index, function := range functions {
		state := &bucket.states[index]
		name := state.name
		if name == "COUNT" && function.InputIndex < 0 {
			state.count++
			continue
		}
		inputIndexes := function.InputIndexes
		if len(inputIndexes) == 0 && function.InputIndex >= 0 {
			inputIndexes = []int{function.InputIndex}
		}
		if len(inputIndexes) == 0 {
			return fmt.Errorf("parallel aggregate %s has no input columns", name)
		}
		values := make([]interface{}, len(inputIndexes))
		for valueIndex, inputIndex := range inputIndexes {
			if inputIndex < 0 || inputIndex >= len(row) {
				return fmt.Errorf("parallel aggregate %s input index %d is out of range", name, inputIndex)
			}
			values[valueIndex] = row[inputIndex]
		}
		if name == "JSON_OBJECTAGG" {
			if values[0] == nil {
				continue
			}
			if len(values) < 2 {
				return fmt.Errorf("parallel aggregate JSON_OBJECTAGG requires key and value inputs")
			}
			state.jsonObject[fmt.Sprint(values[0])] = values[1]
			continue
		}
		if function.Distinct && len(values) > 1 {
			hasNull := false
			for _, distinctValue := range values {
				if distinctValue == nil {
					hasNull = true
					break
				}
			}
			if hasNull {
				continue
			}
		}
		value := values[0]
		if value == nil {
			if name == "ANY_VALUE" && !state.hasValue {
				state.value = nil
				state.hasValue = true
			}
			if name == "JSON_ARRAYAGG" {
				state.jsonValues = append(state.jsonValues, nil)
			}
			continue
		}
		if function.Distinct && name != "GROUP_CONCAT" {
			keyParts := make([]string, len(values))
			for valueIndex, distinctValue := range values {
				if distinctValue == nil {
					continue
				}
				keyParts[valueIndex] = fmt.Sprintf("%T:%v", distinctValue, distinctValue)
			}
			key := strings.Join(keyParts, "\x00")
			if _, exists := state.seen[key]; exists {
				continue
			}
			state.seen[key] = struct{}{}
		}
		switch name {
		case "ANY_VALUE":
			if !state.hasValue {
				state.value = value
				state.hasValue = true
			}
		case "COUNT":
			state.count++
		case "SUM", "AVG":
			state.sum += toFloat64Value(value)
			state.count++
			state.hasValue = true
		case "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
			x := toFloat64Value(value)
			state.count++
			delta := x - state.mean
			state.mean += delta / float64(state.count)
			state.m2 += delta * (x - state.mean)
		case "BIT_AND", "BIT_OR", "BIT_XOR":
			unsigned := uint64(toFloat64Value(value))
			switch name {
			case "BIT_AND":
				state.bitValue &= unsigned
			case "BIT_OR":
				state.bitValue |= unsigned
			case "BIT_XOR":
				state.bitValue ^= unsigned
			}
			state.bitInit = true
		case "MIN":
			if !state.hasValue || compareRowValues(value, state.value) < 0 {
				state.value = value
			}
			state.hasValue = true
		case "GROUP_CONCAT":
			text := fmt.Sprint(value)
			if state.distinct {
				if _, exists := state.seen[text]; exists {
					continue
				}
				state.seen[text] = struct{}{}
			}
			state.values = append(state.values, text)
		case "JSON_ARRAYAGG":
			state.jsonValues = append(state.jsonValues, value)
		case "MAX":
			if !state.hasValue || compareRowValues(value, state.value) > 0 {
				state.value = value
			}
			state.hasValue = true
		default:
			return fmt.Errorf("unsupported parallel aggregate %q", name)
		}
	}
	return nil
}

func parallelGroupKey(row []interface{}, indexes []int) (string, []interface{}, bool) {
	parts := make([]string, 0, len(indexes))
	values := make([]interface{}, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(row) {
			return "", nil, false
		}
		value := row[index]
		values = append(values, value)
		parts = append(parts, fmt.Sprintf("%T:%#v", value, value))
	}
	return strings.Join(parts, "\x00"), values, true
}

func parallelGroupPartition(key string, partitions int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(partitions))
}

func (e *ParallelExecutor) executeParallelSort(ctx context.Context, sort *ParallelSort) ([][]interface{}, error) {
	if len(sort.Children()) == 0 {
		return nil, fmt.Errorf("parallel sort requires a row-producing child")
	}
	rows, err := e.readPlanRows(ctx, sort.Children()[0])
	if err != nil {
		return nil, err
	}
	return e.executeParallelSortRows(ctx, sort.ByItems, rows)
}

// readPlanRows selects the configured engine adapter when present and
// otherwise executes a child that has already been converted to a supported
// parallel physical operator. This keeps the plan package independent from
// storage while allowing a fully parallelized tree to run without a second
// callback for every child.
func (e *ParallelExecutor) readPlanRows(ctx context.Context, child PhysicalPlan) ([][]interface{}, error) {
	if e.rowReader != nil {
		switch scan := child.(type) {
		case *ParallelTableScan:
			rows, err := e.rowReader(ctx, child)
			if err != nil {
				return nil, err
			}
			return projectRequiredColumns(scan.Table, scan.RequiredColumns, rows, "parallel table scan")
		case *ParallelIndexScan:
			rows, err := e.rowReader(ctx, child)
			if err != nil {
				return nil, err
			}
			return projectRequiredColumns(scan.Table, scan.RequiredColumns, rows, "parallel index scan")
		}
	}
	switch child.(type) {
	case *PhysicalValues, *PhysicalSelection, *PhysicalProjection, *PhysicalUnion, *PhysicalMergeJoin, *PhysicalStreamAgg, *PhysicalSubquery, *PhysicalApply, *PhysicalCTEStatement, *PhysicalCTE, *PhysicalCTEScan,
		*ParallelHashJoin, *ParallelHashAgg, *ParallelSort:
		return e.Execute(ctx, child)
	}
	if e.rowReader != nil {
		return e.rowReader(ctx, child)
	}
	switch child.(type) {
	case *PhysicalValues, *ParallelTableScan, *ParallelIndexScan, *ParallelHashJoin, *ParallelHashAgg, *ParallelSort, *PhysicalCTEScan:
		return e.Execute(ctx, child)
	default:
		return nil, fmt.Errorf("parallel physical child row reader unavailable for %T", child)
	}
}

func (e *ParallelExecutor) executeParallelSortRows(ctx context.Context, byItems []ByItem, rows [][]interface{}) ([][]interface{}, error) {
	partitions := e.workers
	if partitions < 1 {
		partitions = 1
	}
	sortedChunks := make([][][]interface{}, partitions)
	var wg sync.WaitGroup
	for partition := 0; partition < partitions; partition++ {
		start := partition * len(rows) / partitions
		end := (partition + 1) * len(rows) / partitions
		wg.Add(1)
		go func(partition, start, end int) {
			defer wg.Done()
			select {
			case e.workerPool <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-e.workerPool }()
			chunk := make([][]interface{}, 0, end-start)
			for _, row := range rows[start:end] {
				if err := ctx.Err(); err != nil {
					return
				}
				chunk = append(chunk, row)
			}
			sort.SliceStable(chunk, func(i, j int) bool { return rowLess(chunk[i], chunk[j], byItems) })
			sortedChunks[partition] = chunk
		}(partition, start, end)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return mergeSortedChunks(sortedChunks, byItems)
}

// 辅助函数

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func estimateRowCount(plan PhysicalPlan) int64 {
	if plan == nil {
		return 0
	}

	if rows := plan.GetEstimateRows(); rows > 0 {
		return rows
	}

	return 1000
}

func scanChunk(ctx context.Context, scan *ParallelTableScan, chunk DataChunk) ([][]interface{}, error) {
	if chunk.EndRowID < chunk.StartRowID {
		return nil, fmt.Errorf("invalid chunk range: %d-%d", chunk.StartRowID, chunk.EndRowID)
	}
	if scan == nil || scan.ChunkReader == nil {
		return nil, fmt.Errorf("parallel table scan requires a configured TableChunkReader")
	}
	rows, err := scan.ChunkReader(ctx, chunk)
	if err != nil {
		return nil, err
	}
	return projectParallelScanRows(scan, rows)
}

func projectParallelScanRows(scan *ParallelTableScan, rows [][]interface{}) ([][]interface{}, error) {
	if scan == nil {
		return nil, fmt.Errorf("parallel table scan is nil")
	}
	return projectRequiredColumns(scan.Table, scan.RequiredColumns, rows, "parallel table scan")
}

func projectRequiredColumns(table *metadata.Table, requiredColumns []string, rows [][]interface{}, scanType string) ([][]interface{}, error) {
	if len(requiredColumns) == 0 {
		return rows, nil
	}
	if table == nil {
		return nil, fmt.Errorf("%s requires table metadata for required columns", scanType)
	}
	columnIndexes := make([]int, 0, len(requiredColumns))
	for _, required := range requiredColumns {
		index := -1
		for columnIndex, column := range table.Columns {
			if column != nil && strings.EqualFold(strings.TrimSpace(column.Name), physicalRequiredColumnName(required)) {
				index = columnIndex
				break
			}
		}
		if index < 0 {
			return nil, fmt.Errorf("%s required column %q is not present in table %s", scanType, required, table.Name)
		}
		columnIndexes = append(columnIndexes, index)
	}
	projected := make([][]interface{}, 0, len(rows))
	for rowIndex, row := range rows {
		values := make([]interface{}, 0, len(columnIndexes))
		for _, columnIndex := range columnIndexes {
			if columnIndex >= len(row) {
				return nil, fmt.Errorf("%s row %d has %d values, required column index %d is unavailable", scanType, rowIndex, len(row), columnIndex)
			}
			values = append(values, row[columnIndex])
		}
		projected = append(projected, values)
	}
	return projected, nil
}

func mergeSortedChunks(chunks [][][]interface{}, byItems []ByItem) ([][]interface{}, error) {
	merged := make([][]interface{}, 0)
	for _, chunk := range chunks {
		merged = append(merged, chunk...)
	}

	sort.Slice(merged, func(i, j int) bool {
		return rowLess(merged[i], merged[j], byItems)
	})

	return merged, nil
}

func rowLess(left, right []interface{}, byItems []ByItem) bool {
	if len(byItems) == 0 || len(left) == 0 || len(right) == 0 {
		if len(left) == 0 {
			return len(right) != 0
		}
		if len(right) == 0 {
			return false
		}
		return compareRows(left, right) < 0
	}

	for i, byItem := range byItems {
		columnIndex := i
		if byItem.ColumnIndexSet {
			columnIndex = byItem.ColumnIndex
		}
		if columnIndex >= len(left) || columnIndex >= len(right) {
			continue
		}

		cmp := compareRowValues(left[columnIndex], right[columnIndex])
		if cmp != 0 {
			if byItem.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
	}

	return compareRows(left, right) < 0
}

func compareRows(left, right []interface{}) int {
	leftLen := len(left)
	rightLen := len(right)
	for i := 0; i < leftLen && i < rightLen; i++ {
		if cmp := compareRowValues(left[i], right[i]); cmp != 0 {
			return cmp
		}
	}

	if leftLen < rightLen {
		return -1
	}
	if leftLen > rightLen {
		return 1
	}
	return 0
}

func compareRowValues(left, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	// ORDER BY on text columns is lexical. The previous implementation
	// converted every non-numeric string to zero, making two arbitrary text
	// values compare equal and leaving the input order unchanged. Preserve
	// numeric comparison for numeric values, but keep text and byte strings in
	// their natural MySQL-compatible order.
	switch l := left.(type) {
	case string:
		if r, ok := right.(string); ok {
			return strings.Compare(l, r)
		}
	case []byte:
		if r, ok := right.([]byte); ok {
			return strings.Compare(string(l), string(r))
		}
	}
	if lf, leftNumeric := numericComparableValue(left); leftNumeric {
		if rf, rightNumeric := numericComparableValue(right); rightNumeric {
			if lf < rf {
				return -1
			}
			if lf > rf {
				return 1
			}
			return 0
		}
	}
	return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
}

func toFloat64Value(value interface{}) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		var parsed float64
		if n, err := fmt.Sscanf(v, "%f", &parsed); n == 1 && err == nil {
			return parsed
		}
		return 0
	default:
		return 0
	}
}
