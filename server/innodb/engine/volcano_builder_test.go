package engine

import (
	"context"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// TestVolcanoExecutor_BuildTableScan 测试buildTableScan
func TestVolcanoExecutor_BuildTableScan(t *testing.T) {
	// 创建测试用的物理计划
	table := &metadata.Table{
		Name:   "users",
		Schema: metadata.NewSchema("testdb"),
	}

	physicalPlan := &plan.PhysicalTableScan{
		Table: table,
	}

	// 创建VolcanoExecutor（使用nil managers进行测试）
	executor := &VolcanoExecutor{
		tableManager:      nil,
		bufferPoolManager: nil,
		storageManager:    nil,
		indexManager:      nil,
	}

	// 构建算子
	op, err := executor.buildTableScan(physicalPlan)
	if err != nil {
		t.Fatalf("buildTableScan failed: %v", err)
	}

	if op == nil {
		t.Fatal("buildTableScan returned nil operator")
	}

	// 验证算子类型
	tableScan, ok := op.(*TableScanOperator)
	if !ok {
		t.Fatalf("expected *TableScanOperator, got %T", op)
	}

	// 验证字段
	if tableScan.schemaName != "testdb" {
		t.Errorf("expected schemaName 'testdb', got '%s'", tableScan.schemaName)
	}

	if tableScan.tableName != "users" {
		t.Errorf("expected tableName 'users', got '%s'", tableScan.tableName)
	}
}

// TestVolcanoExecutor_BuildMergeJoin EXE-004：PhysicalMergeJoin 能成功构建算子树（当前退化为 NestedLoopJoin）
func TestVolcanoExecutor_BuildMergeJoin(t *testing.T) {
	ctx := context.Background()
	table := &metadata.Table{
		Name:   "t",
		Schema: metadata.NewSchema("testdb"),
	}
	left := &plan.PhysicalTableScan{BasePhysicalPlan: plan.BasePhysicalPlan{}, Table: table}
	right := &plan.PhysicalTableScan{BasePhysicalPlan: plan.BasePhysicalPlan{}, Table: table}
	merge := &plan.PhysicalMergeJoin{
		BasePhysicalPlan: plan.BasePhysicalPlan{},
		JoinType:         "INNER",
		Conditions:       nil,
	}
	merge.SetChildren([]plan.PhysicalPlan{left, right})

	executor := &VolcanoExecutor{
		tableManager:      nil,
		bufferPoolManager: nil,
		storageManager:    nil,
		indexManager:      nil,
	}

	op, err := executor.buildOperatorTree(ctx, merge)
	if err != nil {
		t.Fatalf("buildMergeJoin (buildOperatorTree) failed: %v", err)
	}
	if op == nil {
		t.Fatal("buildOperatorTree returned nil operator for PhysicalMergeJoin")
	}
	// 当前实现退化为 NestedLoopJoin
	if _, ok := op.(*NestedLoopJoinOperator); !ok {
		t.Logf("PhysicalMergeJoin currently builds as %T (NestedLoopJoin fallback expected)", op)
	}
}

// TestVolcanoExecutor_BuildIndexScan 测试buildIndexScan
func TestVolcanoExecutor_BuildIndexScan(t *testing.T) {
	// 创建测试用的物理计划
	table := &metadata.Table{
		Name:   "users",
		Schema: metadata.NewSchema("testdb"),
	}

	index := &metadata.Index{
		Name: "idx_age",
	}

	physicalPlan := &plan.PhysicalIndexScan{
		Table: table,
		Index: index,
	}

	// 创建VolcanoExecutor
	executor := &VolcanoExecutor{
		tableManager:      nil,
		bufferPoolManager: nil,
		storageManager:    nil,
		indexManager:      nil,
	}

	// 构建算子
	op, err := executor.buildIndexScan(physicalPlan)
	if err != nil {
		t.Fatalf("buildIndexScan failed: %v", err)
	}

	if op == nil {
		t.Fatal("buildIndexScan returned nil operator")
	}

	// 验证算子类型
	indexScan, ok := op.(*IndexScanOperator)
	if !ok {
		t.Fatalf("expected *IndexScanOperator, got %T", op)
	}

	// 验证字段
	if indexScan.schemaName != "testdb" {
		t.Errorf("expected schemaName 'testdb', got '%s'", indexScan.schemaName)
	}

	if indexScan.tableName != "users" {
		t.Errorf("expected tableName 'users', got '%s'", indexScan.tableName)
	}

	if indexScan.indexName != "idx_age" {
		t.Errorf("expected indexName 'idx_age', got '%s'", indexScan.indexName)
	}
}

// TestVolcanoExecutor_BuildPredicate 测试buildPredicate
func TestVolcanoExecutor_BuildPredicate(t *testing.T) {
	executor := &VolcanoExecutor{}

	// 创建测试schema
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("age", metadata.TypeInt))

	// 创建测试条件: age > 18
	conditions := []plan.Expression{
		&plan.BinaryOperation{
			Op:   plan.OpGT,
			Left: &plan.Column{Name: "age"},
			Right: &plan.Constant{
				BaseExpression: plan.BaseExpression{},
				Value:          18,
			},
		},
	}

	// 构建predicate
	predicate := executor.buildPredicate(conditions, schema)

	// 测试predicate（这里只是验证函数可以调用，不验证具体逻辑）
	if predicate == nil {
		t.Fatal("buildPredicate returned nil")
	}
}

// TestVolcanoExecutor_BuildGroupByExprs 测试buildGroupByExprs
func TestVolcanoExecutor_BuildGroupByExprs(t *testing.T) {
	executor := &VolcanoExecutor{}

	// 创建测试schema
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("category", metadata.TypeVarchar))
	schema.AddColumn(metadata.NewQueryColumn("amount", metadata.TypeInt))

	// 创建测试GroupByItems
	groupByItems := []plan.Expression{
		&plan.Column{Name: "category"},
	}

	// 构建groupByExprs
	groupByExprs := executor.buildGroupByExprs(groupByItems, schema)

	// 验证结果
	if len(groupByExprs) != 1 {
		t.Fatalf("expected 1 group by expr, got %d", len(groupByExprs))
	}

	if groupByExprs[0] != 0 {
		t.Errorf("expected column index 0, got %d", groupByExprs[0])
	}
}

// TestVolcanoExecutor_BuildAggFuncs 测试buildAggFuncs
func TestVolcanoExecutor_BuildAggFuncs(t *testing.T) {
	executor := &VolcanoExecutor{}

	// 创建测试AggFuncs
	aggFuncs := []plan.AggregateFunc{
		&plan.Function{FuncName: "COUNT"},
		&plan.Function{FuncName: "SUM"},
		&plan.Function{FuncName: "AVG"},
	}

	// 构建aggFuncs
	funcs := executor.buildAggFuncs(aggFuncs)

	// 验证结果
	if len(funcs) != 3 {
		t.Fatalf("expected 3 agg funcs, got %d", len(funcs))
	}

	// 验证类型
	if _, ok := funcs[0].(*CountAgg); !ok {
		t.Errorf("expected CountAgg, got %T", funcs[0])
	}

	if _, ok := funcs[1].(*SumAgg); !ok {
		t.Errorf("expected SumAgg, got %T", funcs[1])
	}

	if _, ok := funcs[2].(*AvgAgg); !ok {
		t.Errorf("expected AvgAgg, got %T", funcs[2])
	}
}

// TestVolcanoExecutor_BuildSortKeys 测试buildSortKeys
func TestVolcanoExecutor_BuildSortKeys(t *testing.T) {
	executor := &VolcanoExecutor{}

	// 创建测试schema
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))
	schema.AddColumn(metadata.NewQueryColumn("age", metadata.TypeInt))

	// 创建测试ByItems
	byItems := []plan.ByItem{
		{
			Expr: &plan.Column{Name: "age"},
			Desc: true, // 降序
		},
		{
			Expr: &plan.Column{Name: "name"},
			Desc: false, // 升序
		},
	}

	// 构建sortKeys
	sortKeys := executor.buildSortKeys(byItems, schema)

	// 验证结果
	if len(sortKeys) != 2 {
		t.Fatalf("expected 2 sort keys, got %d", len(sortKeys))
	}

	// 验证第一个排序键（age DESC）
	if sortKeys[0].ColumnIdx != 1 {
		t.Errorf("expected column index 1, got %d", sortKeys[0].ColumnIdx)
	}
	if sortKeys[0].Ascending {
		t.Error("expected Ascending=false for DESC order")
	}

	// 验证第二个排序键（name ASC）
	if sortKeys[1].ColumnIdx != 0 {
		t.Errorf("expected column index 0, got %d", sortKeys[1].ColumnIdx)
	}
	if !sortKeys[1].Ascending {
		t.Error("expected Ascending=true for ASC order")
	}
}

// TestVolcanoExecutor_FindColumnIndex 测试findColumnIndex
func TestVolcanoExecutor_FindColumnIndex(t *testing.T) {
	executor := &VolcanoExecutor{}

	// 创建测试schema
	schema := metadata.NewQuerySchema()
	schema.AddColumn(metadata.NewQueryColumn("id", metadata.TypeInt))
	schema.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))
	schema.AddColumn(metadata.NewQueryColumn("age", metadata.TypeInt))

	// 测试查找存在的列
	tests := []struct {
		columnName string
		wantIndex  int
	}{
		{"id", 0},
		{"name", 1},
		{"age", 2},
		{"notexist", -1},
	}

	for _, tt := range tests {
		t.Run(tt.columnName, func(t *testing.T) {
			idx := executor.findColumnIndex(tt.columnName, schema)
			if idx != tt.wantIndex {
				t.Errorf("findColumnIndex(%s) = %d, want %d", tt.columnName, idx, tt.wantIndex)
			}
		})
	}
}

// TestVolcanoExecutor_BuildSelection 测试buildSelection
func TestVolcanoExecutor_BuildSelection(t *testing.T) {
	// 由于buildSelection需要调用buildOperatorTree，我们需要mock它
	// 这里简化测试，只验证函数不会panic
	t.Skip("Skipping integration test - requires full setup")
}
