package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestEliminateAggregationSimpleMax(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}

	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		GroupByItems:    nil,
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "MAX",
			FuncArgs: []Expression{&Column{Name: "id"}},
		}},
	}

	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Exprs:           []Expression{&Function{FuncName: "MAX", FuncArgs: []Expression{&Column{Name: "id"}}}},
	}

	optimized := OptimizeLogicalPlan(proj)

	p, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection")
	}

	if _, ok := p.Children()[0].(*LogicalAggregation); ok {
		t.Fatalf("aggregation not eliminated")
	}

	if len(p.Exprs) != 1 {
		t.Fatalf("unexpected projection expr count")
	}

	col, ok := p.Exprs[0].(*Column)
	if !ok || col.Name != "id" {
		t.Fatalf("expected projection on id")
	}
}

func TestEliminateAggregationSimpleMin(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}

	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		GroupByItems:    nil,
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "MIN",
			FuncArgs: []Expression{&Column{Name: "id"}},
		}},
	}

	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Exprs:           []Expression{&Function{FuncName: "MIN", FuncArgs: []Expression{&Column{Name: "id"}}}},
	}

	optimized := OptimizeLogicalPlan(proj)

	p, ok := optimized.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection")
	}

	if _, ok := p.Children()[0].(*LogicalAggregation); ok {
		t.Fatalf("aggregation not eliminated")
	}

	if len(p.Exprs) != 1 {
		t.Fatalf("unexpected projection expr count")
	}

	col, ok := p.Exprs[0].(*Column)
	if !ok || col.Name != "id" {
		t.Fatalf("expected projection on id")
	}
}

// TestPredicatePushdown_SelectionOverTableScan OPT-011：Selection(谓词) over TableScan 优化后谓词在 TableScan 之上的 Selection 中
func TestPredicatePushdown_SelectionOverTableScan(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		},
	}
	optimized := OptimizeLogicalPlan(sel)
	// 谓词下推后应为 Selection(TableScan) 或 Selection(IndexScan)，且条件被保留
	outSel, ok := optimized.(*LogicalSelection)
	if !ok {
		// 可能被优化成 IndexScan 等，则其父节点或自身应带条件
		if _, isIdx := optimized.(*LogicalIndexScan); isIdx {
			return // 索引扫描也视为下推成功
		}
		t.Fatalf("expected LogicalSelection or LogicalIndexScan at top, got %T", optimized)
	}
	if len(outSel.Conditions) == 0 {
		t.Errorf("expected at least one condition after pushdown, got 0")
	}
	child := outSel.Children()[0]
	if _, ok := child.(*LogicalTableScan); !ok {
		if _, ok := child.(*LogicalIndexScan); !ok {
			t.Errorf("expected TableScan or IndexScan under Selection, got %T", child)
		}
	}
}

// TestPredicatePushdown_Join 谓词下推 OPT-001：仅涉及左表/右表的条件应下推到对应子计划
func TestPredicatePushdown_Join(t *testing.T) {
	tableA := metadata.NewTable("a")
	tableA.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableA.AddColumn(&metadata.Column{Name: "a_name", DataType: metadata.TypeVarchar})
	schemaA := metadata.NewSchema("test")
	_ = schemaA.AddTable(tableA)

	tableB := metadata.NewTable("b")
	tableB.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	tableB.AddColumn(&metadata.Column{Name: "b_score", DataType: metadata.TypeInt})
	schemaB := metadata.NewSchema("test")
	_ = schemaB.AddTable(tableB)

	scanA := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: schemaA},
		Table:           tableA,
	}
	scanB := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: schemaB},
		Table:           tableB,
	}
	join := &LogicalJoin{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scanA, scanB}},
		JoinType:        "INNER",
		Conditions:      []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Column{Name: "id"}}},
		LeftSchema:      schemaA,
		RightSchema:     schemaB,
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{join}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "a_name"}, Right: &Constant{Value: "x"}},
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "b_score"}, Right: &Constant{Value: int64(0)}},
		},
	}

	optimized := OptimizeLogicalPlan(sel)

	// 全部条件可下推时，顶层返回 Join
	outJoin, ok := optimized.(*LogicalJoin)
	if !ok {
		t.Fatalf("expected Join at top after pushdown, got %T", optimized)
	}
	left := outJoin.Children()[0]
	right := outJoin.Children()[1]
	leftSel, leftIsSel := left.(*LogicalSelection)
	rightSel, rightIsSel := right.(*LogicalSelection)
	if !leftIsSel || !rightIsSel {
		t.Fatalf("expected Selection under Join on both sides, got left=%T right=%T", left, right)
	}
	if len(leftSel.Conditions) != 1 {
		t.Errorf("expected 1 condition pushed to left, got %d", len(leftSel.Conditions))
	}
	if len(rightSel.Conditions) != 1 {
		t.Errorf("expected 1 condition pushed to right, got %d", len(rightSel.Conditions))
	}
}

// TestPredicatePushdown_SelectionOverAggregation OPT-011：仅含 GROUP BY 列的条件应下推到聚合之下
func TestPredicatePushdown_SelectionOverAggregation(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	agg := &LogicalAggregation{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		GroupByItems:    []Expression{&Column{Name: "id"}},
		AggFuncs:        []AggregateFunc{&Function{FuncName: "COUNT", FuncArgs: []Expression{&Column{Name: "col1"}}}},
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{agg}},
		Conditions:      []Expression{&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}}},
	}
	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{sel}},
		Exprs:           []Expression{&Column{Name: "id"}, &Function{FuncName: "COUNT", FuncArgs: []Expression{&Column{Name: "col1"}}}},
	}
	optimized := OptimizeLogicalPlan(proj)
	// 应存在一层 Selection(id=1) 在 Agg 的子节点上（谓词已下推）
	var foundSelWithIDCond bool
	var visit func(LogicalPlan)
	visit = func(p LogicalPlan) {
		if p == nil {
			return
		}
		if sel, ok := p.(*LogicalSelection); ok && len(sel.Conditions) > 0 {
			cols := collectUsedColumns(sel.Conditions)
			for _, col := range cols {
				if col == "id" {
					foundSelWithIDCond = true
					return
				}
			}
		}
		if _, ok := p.(*LogicalIndexScan); ok {
			foundSelWithIDCond = true
			return
		}
		for _, c := range p.Children() {
			visit(c)
			if foundSelWithIDCond {
				return
			}
		}
	}
	visit(optimized)
	if !foundSelWithIDCond {
		t.Error("expected predicate on id pushed below Aggregation or used as IndexScan (OPT-011)")
	}
}

// TestColumnPruning_ProjectionOverTableScan OPT-012：Proj(仅列 id) over TableScan 后，子计划输出列被裁剪为仅 id
func TestColumnPruning_ProjectionOverTableScan(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Exprs:           []Expression{&Column{Name: "id"}},
	}
	optimized := OptimizeLogicalPlan(proj)
	// 列裁剪后，找到最底层 TableScan，其 schema 应只含 id
	var findTableScan func(LogicalPlan) *LogicalTableScan
	findTableScan = func(p LogicalPlan) *LogicalTableScan {
		if ts, ok := p.(*LogicalTableScan); ok {
			return ts
		}
		for _, c := range p.Children() {
			if ts := findTableScan(c); ts != nil {
				return ts
			}
		}
		return nil
	}
	ts := findTableScan(optimized)
	if ts == nil {
		t.Fatalf("no TableScan in optimized plan")
	}
	sch := ts.Schema()
	if sch == nil {
		t.Fatalf("TableScan has nil schema")
	}
	tbl, ok := sch.GetTable("test_table")
	if !ok {
		// 可能表名在 schema 里是别的 key
		for _, tbl = range sch.Tables {
			break
		}
	}
	if tbl == nil {
		t.Fatalf("no table in schema")
	}
	if len(tbl.Columns) != 1 {
		t.Errorf("column pruning: expected 1 column in pruned schema, got %d: %v", len(tbl.Columns), tbl.Columns)
	}
	if len(tbl.Columns) >= 1 && tbl.Columns[0].Name != "id" {
		t.Errorf("column pruning: expected column id, got %s", tbl.Columns[0].Name)
	}
}

// TestColumnPruning_ProjectionOverSelectionOverTableScan OPT-012：SELECT name WHERE id=1 需同时保留 id 与 name，不能只留谓词列或只留投影列。
func TestColumnPruning_ProjectionOverSelectionOverTableScan(t *testing.T) {
	table := createTestTable()
	dbSchema := metadata.NewSchema("test")
	if err := dbSchema.AddTable(table); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	scan := &LogicalTableScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: table.Schema},
		Table:           table,
	}
	sel := &LogicalSelection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		},
	}
	proj := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{sel}},
		Exprs:           []Expression{&Column{Name: "name"}},
	}
	optimized := OptimizeLogicalPlan(proj)

	var findLeafScan func(LogicalPlan) (LogicalPlan, bool)
	findLeafScan = func(p LogicalPlan) (LogicalPlan, bool) {
		switch n := p.(type) {
		case *LogicalTableScan, *LogicalIndexScan:
			return n, true
		default:
			for _, c := range p.Children() {
				if leaf, ok := findLeafScan(c); ok {
					return leaf, true
				}
			}
		}
		return nil, false
	}
	leaf, ok := findLeafScan(optimized)
	if !ok {
		t.Fatalf("no TableScan/IndexScan in optimized plan")
	}
	var sch *metadata.DatabaseSchema
	switch s := leaf.(type) {
	case *LogicalTableScan:
		sch = s.Schema()
	case *LogicalIndexScan:
		sch = s.Schema()
	default:
		t.Fatalf("unexpected leaf %T", leaf)
	}
	if sch == nil {
		t.Fatalf("leaf scan has nil schema")
	}
	var tbl *metadata.Table
	for _, tdef := range sch.Tables {
		tbl = tdef
		break
	}
	if tbl == nil {
		t.Fatalf("no table in schema")
	}
	if len(tbl.Columns) != 2 {
		t.Fatalf("expected 2 columns (id,name), got %d", len(tbl.Columns))
	}
	names := []string{tbl.Columns[0].Name, tbl.Columns[1].Name}
	if names[0] > names[1] {
		names[0], names[1] = names[1], names[0]
	}
	if names[0] != "id" || names[1] != "name" {
		t.Errorf("expected columns id+name, got %v %v", tbl.Columns[0].Name, tbl.Columns[1].Name)
	}
}
