package plan

import (
	"reflect"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestLogicalOptimizerUsesDeterministicRuleOrder(t *testing.T) {
	plan := &LogicalTableScan{Table: createTestTable()}
	optimized, trace, err := OptimizeLogicalPlanWithTrace(plan)
	if err != nil {
		t.Fatalf("optimize with trace: %v", err)
	}
	if optimized == nil {
		t.Fatal("optimizer returned nil plan")
	}
	want := []string{
		"normalize-expressions",
		"predicate-pushdown",
		"column-pruning",
		"aggregation-elimination",
		"projection-minmax-simplification",
		"subquery-optimization",
		"index-access-optimization",
	}
	if !reflect.DeepEqual(trace, want) {
		t.Fatalf("rule trace = %v, want %v", trace, want)
	}
}

func TestLogicalOptimizerRejectsConflictingRules(t *testing.T) {
	_, err := NewLogicalOptimizationPipeline([]LogicalOptimizationRule{
		{Name: "same", Apply: func(p LogicalPlan) LogicalPlan { return p }},
		{Name: "same", Apply: func(p LogicalPlan) LogicalPlan { return p }},
	})
	if err == nil {
		t.Fatal("expected duplicate rule error")
	}
}

func TestNormalizeExpressionsTraversesValuesAndCTEPlans(t *testing.T) {
	constantExpression := func() Expression {
		return &BinaryOperation{
			Op:    OpAdd,
			Left:  &Constant{Value: int64(1)},
			Right: &Constant{Value: int64(2)},
		}
	}
	assertNormalizedValue := func(t *testing.T, plan LogicalPlan) {
		t.Helper()
		values, ok := plan.(*LogicalValues)
		if !ok {
			t.Fatalf("plan = %T, want *LogicalValues", plan)
		}
		constant, ok := values.Exprs[0].(*Constant)
		if !ok || constant.Value != int64(3) {
			t.Fatalf("normalized VALUES expression = %#v, want int64(3)", values.Exprs[0])
		}
	}

	values := &LogicalValues{Exprs: []Expression{constantExpression()}}
	normalizeExpressions(values)
	assertNormalizedValue(t, values)

	cte := &LogicalCTE{Query: &LogicalValues{Exprs: []Expression{constantExpression()}}}
	normalizeExpressions(cte)
	assertNormalizedValue(t, cte.Query)

	recursive := &LogicalRecursiveCTE{
		Anchor:    &LogicalValues{Exprs: []Expression{constantExpression()}},
		Recursive: &LogicalValues{Exprs: []Expression{constantExpression()}},
	}
	normalizeExpressions(recursive)
	assertNormalizedValue(t, recursive.Anchor)
	assertNormalizedValue(t, recursive.Recursive)

	statement := &LogicalCTEStatement{
		Definitions: []LogicalPlan{&LogicalValues{Exprs: []Expression{constantExpression()}}},
		Body:        &LogicalValues{Exprs: []Expression{constantExpression()}},
	}
	normalizeExpressions(statement)
	assertNormalizedValue(t, statement.Definitions[0])
	assertNormalizedValue(t, statement.Body)
}

func TestColumnPruningTraversesCTEPlanFields(t *testing.T) {
	newProjection := func() *LogicalProjection {
		table := createTestTable()
		schema := metadata.NewSchema("test")
		if err := schema.AddTable(table); err != nil {
			t.Fatalf("add test table to schema: %v", err)
		}
		scan := &LogicalTableScan{
			BaseLogicalPlan: BaseLogicalPlan{schema: schema},
			Table:           table,
		}
		return &LogicalProjection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
			Exprs:           []Expression{&Column{Name: "id"}},
		}
	}
	assertOnlyID := func(t *testing.T, plan LogicalPlan) {
		t.Helper()
		projection, ok := plan.(*LogicalProjection)
		if !ok || len(projection.Children()) != 1 {
			t.Fatalf("plan = %T, want projection with one child", plan)
		}
		table, ok := projection.Children()[0].Schema().GetTable("test_table")
		if !ok || len(table.Columns) != 1 || table.Columns[0].Name != "id" {
			t.Fatalf("pruned child schema = %#v, want only id", table)
		}
	}

	cte := &LogicalCTE{Query: newProjection()}
	columnPruning(cte, nil)
	assertOnlyID(t, cte.Query)

	recursive := &LogicalRecursiveCTE{Anchor: newProjection(), Recursive: newProjection()}
	columnPruning(recursive, nil)
	assertOnlyID(t, recursive.Anchor)
	assertOnlyID(t, recursive.Recursive)

	statement := &LogicalCTEStatement{
		Definitions: []LogicalPlan{&LogicalCTE{Query: newProjection()}},
		Body:        newProjection(),
	}
	columnPruning(statement, nil)
	definition, ok := statement.Definitions[0].(*LogicalCTE)
	if !ok {
		t.Fatalf("definition = %T, want *LogicalCTE", statement.Definitions[0])
	}
	assertOnlyID(t, definition.Query)
	assertOnlyID(t, statement.Body)

	scanTable := createTestTable()
	scanSchema := metadata.NewSchema("test")
	if err := scanSchema.AddTable(scanTable); err != nil {
		t.Fatalf("add CTE scan table to schema: %v", err)
	}
	cteScan := &LogicalCTEScan{
		BaseLogicalPlan: BaseLogicalPlan{schema: scanSchema},
		Name:            "items",
	}
	columnPruning(cteScan, []string{"id"})
	prunedTable, ok := cteScan.Schema().GetTable("test_table")
	if !ok || len(prunedTable.Columns) != 1 || prunedTable.Columns[0].Name != "id" {
		t.Fatalf("pruned CTE scan schema = %#v, want only id", prunedTable)
	}
}

func TestPredicatePushdownTraversesCTEPlanFields(t *testing.T) {
	newSelection := func() *LogicalSelection {
		table := createTestTable()
		schema := metadata.NewSchema("test")
		if err := schema.AddTable(table); err != nil {
			t.Fatalf("add test table to schema: %v", err)
		}
		scan := &LogicalTableScan{
			BaseLogicalPlan: BaseLogicalPlan{schema: schema},
			Table:           table,
		}
		return &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
			Conditions: []Expression{&BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "id"},
				Right: &Constant{Value: int64(1)},
			}},
		}
	}
	assertPushed := func(t *testing.T, plan LogicalPlan) {
		t.Helper()
		selection, ok := plan.(*LogicalSelection)
		if !ok || len(selection.Children()) != 1 {
			t.Fatalf("plan = %T, want selection with one child", plan)
		}
		if _, ok := selection.Children()[0].(*LogicalTableScan); !ok {
			t.Fatalf("selection child = %T, want table scan", selection.Children()[0])
		}
	}

	cte := &LogicalCTE{Query: newSelection()}
	pushDownPredicates(cte)
	assertPushed(t, cte.Query)

	recursive := &LogicalRecursiveCTE{Anchor: newSelection(), Recursive: newSelection()}
	pushDownPredicates(recursive)
	assertPushed(t, recursive.Anchor)
	assertPushed(t, recursive.Recursive)

	statement := &LogicalCTEStatement{
		Definitions: []LogicalPlan{&LogicalCTE{Query: newSelection()}},
		Body:        newSelection(),
	}
	pushDownPredicates(statement)
	definition, ok := statement.Definitions[0].(*LogicalCTE)
	if !ok {
		t.Fatalf("definition = %T, want *LogicalCTE", statement.Definitions[0])
	}
	assertPushed(t, definition.Query)
	assertPushed(t, statement.Body)
}

func TestOptimizerSelectsIndexesInsideCTEPlanFields(t *testing.T) {
	table := createTestTable()
	schema := metadata.NewSchema("test")
	if err := schema.AddTable(table); err != nil {
		t.Fatalf("add test table to schema: %v", err)
	}
	newFilteredPlan := func() LogicalPlan {
		scan := &LogicalTableScan{
			BaseLogicalPlan: BaseLogicalPlan{schema: schema},
			Table:           table,
		}
		return &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
			Conditions: []Expression{&BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "col1"},
				Right: &Constant{Value: int64(1)},
			}},
		}
	}
	assertIndexed := func(t *testing.T, plan LogicalPlan) {
		t.Helper()
		selection, ok := plan.(*LogicalSelection)
		if !ok || len(selection.Children()) != 1 {
			t.Fatalf("plan = %T, want selection with one child", plan)
		}
		indexScan, ok := selection.Children()[0].(*LogicalIndexScan)
		if !ok || indexScan.Index == nil || indexScan.Index.Name != "idx_col1" {
			t.Fatalf("selection child = %#v, want idx_col1 index scan", selection.Children()[0])
		}
	}

	cte := &LogicalCTE{Query: newFilteredPlan()}
	optimized := OptimizeLogicalPlan(cte)
	optimizedCTE, ok := optimized.(*LogicalCTE)
	if !ok {
		t.Fatalf("optimized plan = %T, want *LogicalCTE", optimized)
	}
	assertIndexed(t, optimizedCTE.Query)
}

func TestOptimizerEliminatesAggregatesInsideCTEPlanFields(t *testing.T) {
	newAggregate := func() LogicalPlan {
		table := createTestTable()
		schema := metadata.NewSchema("test")
		if err := schema.AddTable(table); err != nil {
			t.Fatalf("add test table to schema: %v", err)
		}
		scan := &LogicalTableScan{
			BaseLogicalPlan: BaseLogicalPlan{schema: schema},
			Table:           table,
		}
		return &LogicalAggregation{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{scan}},
			AggFuncs: []AggregateFunc{&Function{
				FuncName: "MAX",
				FuncArgs: []Expression{&Column{Name: "col1"}},
			}},
		}
	}
	assertProjection := func(t *testing.T, plan LogicalPlan) {
		t.Helper()
		if _, ok := plan.(*LogicalProjection); !ok {
			t.Fatalf("plan = %T, want projection after aggregate elimination", plan)
		}
	}

	optimizedCTE, ok := OptimizeLogicalPlan(&LogicalCTE{Query: newAggregate()}).(*LogicalCTE)
	if !ok {
		t.Fatal("optimized ordinary CTE has unexpected type")
	}
	assertProjection(t, optimizedCTE.Query)

	optimizedRecursive, ok := OptimizeLogicalPlan(&LogicalRecursiveCTE{
		Anchor:    newAggregate(),
		Recursive: newAggregate(),
	}).(*LogicalRecursiveCTE)
	if !ok {
		t.Fatal("optimized recursive CTE has unexpected type")
	}
	assertProjection(t, optimizedRecursive.Anchor)
	assertProjection(t, optimizedRecursive.Recursive)

	optimizedStatement, ok := OptimizeLogicalPlan(&LogicalCTEStatement{
		Definitions: []LogicalPlan{&LogicalCTE{Query: newAggregate()}},
		Body:        newAggregate(),
	}).(*LogicalCTEStatement)
	if !ok {
		t.Fatal("optimized CTE statement has unexpected type")
	}
	definition, ok := optimizedStatement.Definitions[0].(*LogicalCTE)
	if !ok {
		t.Fatalf("optimized definition = %T, want *LogicalCTE", optimizedStatement.Definitions[0])
	}
	assertProjection(t, definition.Query)
	assertProjection(t, optimizedStatement.Body)
}

func TestColumnPruningMapsUnionRequirementsByOrdinal(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "union_left"
	rightTable := createTestTable()
	rightTable.Name = "union_right"
	leftSchema := metadata.NewSchema("left")
	rightSchema := metadata.NewSchema("right")
	if err := leftSchema.AddTable(leftTable); err != nil {
		t.Fatalf("add left table: %v", err)
	}
	if err := rightSchema.AddTable(rightTable); err != nil {
		t.Fatalf("add right table: %v", err)
	}
	left := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: leftSchema}, Table: leftTable}
	right := &LogicalTableScan{BaseLogicalPlan: BaseLogicalPlan{schema: rightSchema}, Table: rightTable}
	union := &LogicalUnion{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}}}

	if got := getPlanOutputColumnNames(union); !reflect.DeepEqual(got, []string{"id", "col1", "col2", "col3", "name"}) {
		t.Fatalf("UNION output columns = %v, want first-branch columns", got)
	}
	optimized := columnPruning(union, []string{"col2"})
	optimizedUnion, ok := optimized.(*LogicalUnion)
	if !ok {
		t.Fatalf("optimized plan = %T, want *LogicalUnion", optimized)
	}
	for index, child := range optimizedUnion.Children() {
		if got := tableColumnNames(child.Schema()); !reflect.DeepEqual(got, []string{"col2"}) {
			t.Fatalf("UNION child %d columns = %v, want [col2]", index, got)
		}
	}
}

func TestPlanOutputColumnNamesPreserveApplySemantics(t *testing.T) {
	leftTable := createTestTable()
	leftTable.Name = "apply_left"
	rightTable := createTestTable()
	rightTable.Name = "apply_right"
	left := &LogicalTableScan{Table: leftTable}
	right := &LogicalTableScan{Table: rightTable}

	inner := &LogicalApply{
		ApplyType:       "INNER",
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
	}
	if got, want := getPlanOutputColumnNames(inner), []string{"id", "col1", "col2", "col3", "name", "id", "col1", "col2", "col3", "name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("INNER APPLY output columns = %v, want %v", got, want)
	}

	semi := &LogicalApply{
		ApplyType:       "SEMI",
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}},
	}
	if got, want := getPlanOutputColumnNames(semi), []string{"id", "col1", "col2", "col3", "name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SEMI APPLY output columns = %v, want %v", got, want)
	}
}

func TestPlanOutputColumnNamesExposeSubqueryOutput(t *testing.T) {
	innerTable := createTestTable()
	innerTable.Name = "subquery_source"
	subquery := &LogicalSubquery{
		SubqueryType: "IN",
		Subplan:      &LogicalTableScan{Table: innerTable},
	}
	if got, want := getPlanOutputColumnNames(subquery), []string{"id", "col1", "col2", "col3", "name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("subquery output columns = %v, want %v", got, want)
	}
}

func TestPlanOutputColumnNamesCoverValuesAndAggregation(t *testing.T) {
	values := &LogicalValues{Exprs: []Expression{&Constant{Value: int64(1)}}}
	if got, want := getPlanOutputColumnNames(values), []string{"1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("VALUES output columns = %v, want %v", got, want)
	}

	aggregation := &LogicalAggregation{
		GroupByItems: []Expression{&Column{Name: "category"}},
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "COUNT",
			FuncArgs: []Expression{&Column{Name: "id"}},
		}},
	}
	if got, want := getPlanOutputColumnNames(aggregation), []string{"category", "COUNT(id)"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregation output columns = %v, want %v", got, want)
	}
}

func TestPlanOutputColumnNamesPreferExplicitCTEColumnNames(t *testing.T) {
	query := &LogicalValues{Exprs: []Expression{
		&Constant{Value: int64(1)},
		&Constant{Value: "ready"},
	}}
	cte := &LogicalCTE{
		Columns: []string{"cte_id", "cte_status"},
		Query:   query,
	}
	if got, want := getPlanOutputColumnNames(cte), []string{"cte_id", "cte_status"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CTE output columns = %v, want explicit names %v", got, want)
	}

	recursive := &LogicalRecursiveCTE{
		Columns: []string{"node_id", "parent_id"},
		Anchor:  query,
	}
	if got, want := getPlanOutputColumnNames(recursive), []string{"node_id", "parent_id"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("recursive CTE output columns = %v, want explicit names %v", got, want)
	}
}

func TestColumnPruningAcceptsQualifiedRequirements(t *testing.T) {
	table := createTestTable()
	scan := &LogicalTableScan{Table: table}

	columnPruning(scan, []string{"TEST_TABLE.col2"})
	if got := tableColumnNames(scan.Schema()); !reflect.DeepEqual(got, []string{"col2"}) {
		t.Fatalf("qualified table requirements = %v, want [col2]", got)
	}

	leftTable := createTestTable()
	leftTable.Name = "union_left_qualified"
	rightTable := createTestTable()
	rightTable.Name = "union_right_qualified"
	left := &LogicalTableScan{Table: leftTable}
	right := &LogicalTableScan{Table: rightTable}
	union := &LogicalUnion{BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{left, right}}}
	columnPruning(union, []string{"UNION_LEFT_QUALIFIED.col2"})
	for index, child := range union.Children() {
		if got := tableColumnNames(child.Schema()); !reflect.DeepEqual(got, []string{"col2"}) {
			t.Fatalf("qualified UNION child %d columns = %v, want [col2]", index, got)
		}
	}
}

func TestPredicateOwnershipAcceptsQualifiedOutputColumns(t *testing.T) {
	table := createTestTable()
	table.Name = "orders"
	branch := &LogicalTableScan{Table: table}
	condition := []Expression{&BinaryOperation{
		Op:    OpEQ,
		Left:  &Column{Name: "ORDERS.col2"},
		Right: &Constant{Value: int64(1)},
	}}
	if !canPushThroughUnionBranch(condition, branch) {
		t.Fatal("qualified output column should belong to the UNION branch")
	}

	projection := &LogicalProjection{Exprs: []Expression{&Column{Name: "orders.col2"}}}
	if !canPushThroughIdentityProjection(projection, []Expression{&BinaryOperation{
		Op:    OpEQ,
		Left:  &Column{Name: "COL2"},
		Right: &Constant{Value: int64(1)},
	}}) {
		t.Fatal("qualified identity projection should accept an unqualified predicate column")
	}
}

func TestMinMaxProjectionSimplificationTraversesCTEPlanFields(t *testing.T) {
	newNestedProjection := func() LogicalPlan {
		return &LogicalProjection{
			Exprs: []Expression{&Function{FuncName: "MAX", FuncArgs: []Expression{&Column{Name: "col1"}}}},
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{&LogicalProjection{
				Exprs:           []Expression{&Column{Name: "col1"}},
				BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{&LogicalTableScan{Table: createTestTable()}}},
			}}},
		}
	}
	assertSimplified := func(t *testing.T, plan LogicalPlan) {
		t.Helper()
		projection, ok := plan.(*LogicalProjection)
		if !ok || len(projection.Exprs) != 1 {
			t.Fatalf("plan = %T, want single-column projection", plan)
		}
		if _, ok := projection.Exprs[0].(*Column); !ok {
			t.Fatalf("projection expression = %T, want column after MIN/MAX simplification", projection.Exprs[0])
		}
	}

	optimizedCTE, ok := simplifyProjMinMaxRoot(&LogicalCTE{Query: newNestedProjection()}).(*LogicalCTE)
	if !ok {
		t.Fatal("optimized CTE has unexpected type")
	}
	assertSimplified(t, optimizedCTE.Query)

	optimizedRecursive, ok := simplifyProjMinMaxRoot(&LogicalRecursiveCTE{Anchor: newNestedProjection(), Recursive: newNestedProjection()}).(*LogicalRecursiveCTE)
	if !ok {
		t.Fatal("optimized recursive CTE has unexpected type")
	}
	assertSimplified(t, optimizedRecursive.Anchor)
	assertSimplified(t, optimizedRecursive.Recursive)

	optimizedStatement, ok := simplifyProjMinMaxRoot(&LogicalCTEStatement{Definitions: []LogicalPlan{&LogicalCTE{Query: newNestedProjection()}}, Body: newNestedProjection()}).(*LogicalCTEStatement)
	if !ok {
		t.Fatal("optimized CTE statement has unexpected type")
	}
	definition, ok := optimizedStatement.Definitions[0].(*LogicalCTE)
	if !ok {
		t.Fatalf("optimized definition = %T, want *LogicalCTE", optimizedStatement.Definitions[0])
	}
	assertSimplified(t, definition.Query)
	assertSimplified(t, optimizedStatement.Body)
}
