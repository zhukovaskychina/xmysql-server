package plan

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestParallelRequiredColumnsAcceptQualifiedNames(t *testing.T) {
	table := metadata.NewTable("orders")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "customer", DataType: metadata.TypeVarchar})

	rows, err := projectRequiredColumns(table, []string{"ORDERS.customer"}, [][]interface{}{{int64(7), "alice"}}, "parallel table scan")
	if err != nil {
		t.Fatalf("projectRequiredColumns() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[alice]]" {
		t.Fatalf("projectRequiredColumns() = %v, want [[alice]]", rows)
	}
	if got := physicalScanColumnNames(table, []string{"orders.customer"}); fmt.Sprint(got) != "[customer]" {
		t.Fatalf("physicalScanColumnNames() = %v, want [customer]", got)
	}
}

func TestParallelTableScanRequiresInjectedChunkReader(t *testing.T) {
	table := metadata.NewTable("parallel_order")
	table.Stats.RowCount = 25
	scan := &PhysicalTableScan{Table: table}
	executor := NewParallelExecutor(3, 4)

	plan := executor.ParallelizePhysicalPlan(scan)
	if _, err := executor.Execute(context.Background(), plan); err == nil {
		t.Fatal("Execute() without a TableChunkReader should fail")
	}
}

func TestParallelExecutorRejectsUnsupportedPhysicalPlan(t *testing.T) {
	executor := NewParallelExecutor(1, 1)
	if _, err := executor.Execute(context.Background(), &PhysicalCTEScan{Name: "pending_cte"}); err == nil {
		t.Fatal("Execute() should reject unsupported physical plans instead of returning an empty successful result")
	}
}

func TestParallelExecutorExecutesScalarPhysicalSubquery(t *testing.T) {
	subquery := &PhysicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &PhysicalValues{Exprs: []Expression{
			&Constant{Value: int64(7)},
		}},
	}
	executor := NewParallelExecutor(1, 1)

	rows, err := executor.Execute(context.Background(), subquery)
	if err != nil {
		t.Fatalf("scalar physical subquery Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[7]]" {
		t.Fatalf("scalar physical subquery rows = %v, want [[7]]", rows)
	}
}

func TestParallelExecutorRejectsMultiRowScalarPhysicalSubquery(t *testing.T) {
	subquery := &PhysicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &PhysicalUnion{
			UnionType: "ALL",
		},
	}
	subquery.Subplan.SetChildren([]PhysicalPlan{
		&PhysicalValues{Exprs: []Expression{&Constant{Value: int64(7)}}},
		&PhysicalValues{Exprs: []Expression{&Constant{Value: int64(8)}}},
	})
	executor := NewParallelExecutor(1, 1)

	if _, err := executor.Execute(context.Background(), subquery); err == nil {
		t.Fatal("multi-row scalar physical subquery should return an error")
	}
}

func TestParallelExecutorExecutesPhysicalSemiApply(t *testing.T) {
	apply := &PhysicalApply{ApplyType: "SEMI"}
	apply.SetChildren([]PhysicalPlan{
		&PhysicalValues{Exprs: []Expression{&Constant{Value: int64(1)}}},
		&PhysicalValues{Exprs: []Expression{&Constant{Value: int64(2)}}},
	})
	executor := NewParallelExecutor(1, 1)

	rows, err := executor.Execute(context.Background(), apply)
	if err != nil {
		t.Fatalf("physical semi apply Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1]]" {
		t.Fatalf("physical semi apply rows = %v, want [[1]]", rows)
	}
}

func TestParallelExecutorExecutesCorrelatedPhysicalSemiApply(t *testing.T) {
	leftTable := metadata.NewTable("left_rows")
	leftTable.AddColumn(&metadata.Column{Name: "left_id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("right_rows")
	rightTable.AddColumn(&metadata.Column{Name: "right_id", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	rightSelection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{right}},
		Conditions: []Expression{
			&BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "right_rows.right_id"},
				Right: &Column{Name: "left_rows.left_id"},
			},
		},
	}
	apply := &PhysicalApply{ApplyType: "SEMI", Correlated: true}
	apply.SetChildren([]PhysicalPlan{left, rightSelection})

	executor := NewParallelExecutor(1, 1)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch scan := plan.(type) {
		case *PhysicalTableScan:
			if scan.Table.Name == "left_rows" {
				return [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, nil
			}
			return [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, nil
		default:
			return nil, fmt.Errorf("unexpected plan %T", plan)
		}
	})

	rows, err := executor.Execute(context.Background(), apply)
	if err != nil {
		t.Fatalf("correlated physical semi apply Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1] [2] [3]]" {
		t.Fatalf("correlated physical semi apply rows = %v, want [[1] [2] [3]]", rows)
	}
}

func TestParallelExecutorExecutesCorrelatedPhysicalSubqueryInsideApply(t *testing.T) {
	leftTable := metadata.NewTable("left_subquery")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("right_subquery")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	rightSelection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{right}},
		Conditions: []Expression{
			&BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "right_subquery.id"},
				Right: &Column{Name: "left_subquery.id"},
			},
		},
	}
	rightSubquery := &PhysicalSubquery{
		SubqueryType: "IN",
		Correlated:   true,
		Subplan:      rightSelection,
	}
	apply := &PhysicalApply{ApplyType: "INNER", Correlated: true}
	apply.SetChildren([]PhysicalPlan{left, rightSubquery})

	executor := NewParallelExecutor(1, 1)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, nil
	})

	rows, err := executor.Execute(context.Background(), apply)
	if err != nil {
		t.Fatalf("correlated physical subquery Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1 1] [2 2] [3 3]]" {
		t.Fatalf("correlated physical subquery rows = %v, want [[1 1] [2 2] [3 3]]", rows)
	}
}

func TestParallelExecutorMaterializesPhysicalCTE(t *testing.T) {
	definition := &PhysicalCTE{Name: "recent"}
	definition.SetChildren([]PhysicalPlan{
		&PhysicalValues{Exprs: []Expression{&Constant{Value: int64(7)}}},
	})
	statement := &PhysicalCTEStatement{DefinitionCount: 1}
	statement.SetChildren([]PhysicalPlan{definition, &PhysicalCTEScan{Name: "recent"}})
	executor := NewParallelExecutor(1, 1)

	rows, err := executor.Execute(context.Background(), statement)
	if err != nil {
		t.Fatalf("physical CTE Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[7]]" {
		t.Fatalf("physical CTE rows = %v, want [[7]]", rows)
	}
}

func TestPhysicalPlanColumnNamesPreserveSubqueryAndCTEOutputs(t *testing.T) {
	subquery := &PhysicalSubquery{
		SubqueryType: "SCALAR",
		Subplan: &PhysicalProjection{
			Exprs:       []Expression{&Constant{Value: int64(7)}},
			OutputNames: []string{"answer"},
		},
	}
	if got, want := fmt.Sprint(physicalPlanColumnNames(subquery)), "[answer]"; got != want {
		t.Fatalf("subquery output columns = %s, want %s", got, want)
	}

	cte := &PhysicalCTE{Name: "recent", Columns: []string{"id", "name"}}
	if got, want := fmt.Sprint(physicalPlanColumnNames(cte)), "[id name]"; got != want {
		t.Fatalf("CTE output columns = %s, want %s", got, want)
	}
	recursive := &PhysicalRecursiveCTE{Name: "tree", Columns: []string{"node_id", "parent_id"}}
	if got, want := fmt.Sprint(physicalPlanColumnNames(recursive)), "[node_id parent_id]"; got != want {
		t.Fatalf("recursive CTE output columns = %s, want %s", got, want)
	}

	scan := &PhysicalCTEScan{Name: "recent", Columns: []string{"id", "name"}}
	if got, want := fmt.Sprint(physicalPlanColumnNames(scan)), "[id name]"; got != want {
		t.Fatalf("CTE scan output columns = %s, want %s", got, want)
	}
}

func TestParallelExecutorBindsCTEColumnNamesToSchemaLessBody(t *testing.T) {
	definition := &PhysicalCTE{Name: "recent", Columns: []string{"id", "name"}}
	definition.SetChildren([]PhysicalPlan{
		&PhysicalValues{Exprs: []Expression{
			&Constant{Value: int64(7)},
			&Constant{Value: "alice"},
		}},
	})
	bodyScan := &PhysicalCTEScan{Name: "recent"}
	body := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{bodyScan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "name"},
			Right: &Constant{Value: "alice"},
		}},
	}
	statement := &PhysicalCTEStatement{DefinitionCount: 1}
	statement.SetChildren([]PhysicalPlan{definition, body})

	rows, err := NewParallelExecutor(1, 1).Execute(context.Background(), statement)
	if err != nil {
		t.Fatalf("schema-less CTE body Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[7 alice]]"; got != want {
		t.Fatalf("schema-less CTE body rows = %s, want %s", got, want)
	}
}

func TestParallelExecutorExecutesIntersectAndExceptVariants(t *testing.T) {
	values := func(rows ...int64) PhysicalPlan {
		children := make([]PhysicalPlan, 0, len(rows))
		for _, value := range rows {
			children = append(children, &PhysicalValues{Exprs: []Expression{&Constant{Value: value}}})
		}
		union := &PhysicalUnion{UnionType: "ALL"}
		union.SetChildren(children)
		return union
	}
	executor := NewParallelExecutor(1, 1)

	intersect := &PhysicalUnion{UnionType: "INTERSECT"}
	intersect.SetChildren([]PhysicalPlan{values(1, 1, 2, 3), values(1, 2, 2, 4)})
	rows, err := executor.Execute(context.Background(), intersect)
	if err != nil {
		t.Fatalf("INTERSECT Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1] [2]]"; got != want {
		t.Fatalf("INTERSECT rows = %s, want %s", got, want)
	}

	intersectAll := &PhysicalUnion{UnionType: "INTERSECT ALL"}
	intersectAll.SetChildren([]PhysicalPlan{values(1, 1, 2), values(1, 1, 1, 2, 2)})
	rows, err = executor.Execute(context.Background(), intersectAll)
	if err != nil {
		t.Fatalf("INTERSECT ALL Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1] [1] [2]]"; got != want {
		t.Fatalf("INTERSECT ALL rows = %s, want %s", got, want)
	}

	except := &PhysicalUnion{UnionType: "EXCEPT"}
	except.SetChildren([]PhysicalPlan{values(1, 1, 2, 3), values(1, 2)})
	rows, err = executor.Execute(context.Background(), except)
	if err != nil {
		t.Fatalf("EXCEPT Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[3]]"; got != want {
		t.Fatalf("EXCEPT rows = %s, want %s", got, want)
	}

	exceptAll := &PhysicalUnion{UnionType: "EXCEPT ALL"}
	exceptAll.SetChildren([]PhysicalPlan{values(1, 1, 2, 3), values(1, 2)})
	rows, err = executor.Execute(context.Background(), exceptAll)
	if err != nil {
		t.Fatalf("EXCEPT ALL Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1] [3]]"; got != want {
		t.Fatalf("EXCEPT ALL rows = %s, want %s", got, want)
	}
}

func TestParallelExecutorExecutesRecursivePhysicalCTE(t *testing.T) {
	cteSchema := metadata.NewSchema("recursive_cte")
	cteTable := metadata.NewTable("nums")
	cteTable.AddColumn(&metadata.Column{Name: "n", DataType: metadata.TypeBigInt})
	if err := cteSchema.AddTable(cteTable); err != nil {
		t.Fatalf("add CTE schema table: %v", err)
	}

	anchor := &PhysicalValues{Exprs: []Expression{&Constant{Value: int64(1)}}}
	scan := &PhysicalCTEScan{BasePhysicalPlan: BasePhysicalPlan{schema: cteSchema}, Name: "nums"}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{scan}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpLT, Left: &Column{Name: "n"}, Right: &Constant{Value: int64(3)}},
		},
	}
	projection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{selection}},
		Exprs:            []Expression{&BinaryOperation{Op: OpAdd, Left: &Column{Name: "n"}, Right: &Constant{Value: int64(1)}}},
	}
	definition := &PhysicalRecursiveCTE{Name: "nums", Columns: []string{"n"}}
	definition.SetChildren([]PhysicalPlan{anchor, projection})
	body := &PhysicalCTEScan{BasePhysicalPlan: BasePhysicalPlan{schema: cteSchema}, Name: "nums"}
	statement := &PhysicalCTEStatement{DefinitionCount: 1}
	statement.SetChildren([]PhysicalPlan{definition, body})
	executor := NewParallelExecutor(1, 1)

	rows, err := executor.Execute(context.Background(), statement)
	if err != nil {
		t.Fatalf("recursive physical CTE Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1] [2] [3]]" {
		t.Fatalf("recursive physical CTE rows = %v, want [[1] [2] [3]]", rows)
	}
}

func TestParallelExecutorExecutesPhysicalValues(t *testing.T) {
	values := &PhysicalValues{
		Exprs: []Expression{
			&Constant{Value: int64(7)},
			&Constant{Value: "ready"},
		},
	}
	executor := NewParallelExecutor(2, 2)

	rows, err := executor.Execute(context.Background(), values)
	if err != nil {
		t.Fatalf("physical values Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[7 ready]]" {
		t.Fatalf("physical values rows = %v, want [[7 ready]]", rows)
	}
}

func TestParallelExecutorExecutesPhysicalSelectionAndProjection(t *testing.T) {
	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})
	scan := &PhysicalTableScan{Table: table}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{scan}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "id"}, Right: &Constant{Value: int64(1)}},
		},
	}
	projection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{selection}},
		Exprs:            []Expression{&Column{Name: "name"}, &Column{Name: "id"}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{int64(1), "alice"}, {int64(2), "bob"}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(projection))
	if err != nil {
		t.Fatalf("selection/projection Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[bob 2]]" {
		t.Fatalf("selection/projection rows = %v, want [[bob 2]]", rows)
	}
}

func TestParallelExecutorExecutesPhysicalUnionDistinct(t *testing.T) {
	leftTable := metadata.NewTable("left_values")
	rightTable := metadata.NewTable("right_values")
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	union := &PhysicalUnion{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		UnionType:        "DISTINCT",
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var tableName string
		switch scan := plan.(type) {
		case *ParallelTableScan:
			if scan.Table != nil {
				tableName = scan.Table.Name
			}
		case *PhysicalTableScan:
			if scan.Table != nil {
				tableName = scan.Table.Name
			}
		}
		if tableName == "left_values" {
			return [][]interface{}{{int64(1)}, {int64(2)}}, nil
		}
		return [][]interface{}{{int64(2)}, {int64(3)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(union))
	if err != nil {
		t.Fatalf("union Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1] [2] [3]]" {
		t.Fatalf("union rows = %v, want [[1] [2] [3]]", rows)
	}
	union.UnionType = "ALL"
	rows, err = executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(union))
	if err != nil {
		t.Fatalf("union all Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1] [2] [2] [3]]" {
		t.Fatalf("union all rows = %v, want [[1] [2] [2] [3]]", rows)
	}
}

func TestParallelExecutorExecutesPhysicalMergeJoin(t *testing.T) {
	leftTable := metadata.NewTable("left_rows")
	leftTable.AddColumn(&metadata.Column{Name: "left_id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("right_rows")
	rightTable.AddColumn(&metadata.Column{Name: "right_id", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	join := &PhysicalMergeJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		JoinType:         "INNER",
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "left_id"}, Right: &Column{Name: "right_id"}},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if scan, ok := plan.(*ParallelTableScan); ok && scan.Table != nil && scan.Table.Name == "left_rows" {
			return [][]interface{}{{int64(1)}, {int64(2)}}, nil
		}
		return [][]interface{}{{int64(2)}, {int64(3)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("merge join Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[2 2]]" {
		t.Fatalf("merge join rows = %v, want [[2 2]]", rows)
	}
	join.JoinType = "LEFT"
	rows, err = executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("left merge join Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[1 <nil>] [2 2]]" {
		t.Fatalf("left merge join rows = %v, want [[1 <nil>] [2 2]]", rows)
	}
}

func TestParallelExecutorExecutesRightAndFullMergeJoin(t *testing.T) {
	leftTable := metadata.NewTable("parallel_left")
	leftTable.AddColumn(&metadata.Column{Name: "left_id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("parallel_right")
	rightTable.AddColumn(&metadata.Column{Name: "right_id", DataType: metadata.TypeInt})
	join := &PhysicalMergeJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{
			&PhysicalTableScan{Table: leftTable},
			&PhysicalTableScan{Table: rightTable},
		}},
		JoinType: "RIGHT",
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "left_id"}, Right: &Column{Name: "right_id"}},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if scan, ok := plan.(*ParallelTableScan); ok && scan.Table != nil && scan.Table.Name == "parallel_left" {
			return [][]interface{}{{int64(1)}, {int64(2)}}, nil
		}
		return [][]interface{}{{int64(2)}, {int64(3)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("right merge join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[2 2] [<nil> 3]]"; got != want {
		t.Fatalf("right merge join rows = %s, want %s", got, want)
	}

	join.JoinType = "FULL"
	rows, err = executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("full merge join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 <nil>] [2 2] [<nil> 3]]"; got != want {
		t.Fatalf("full merge join rows = %s, want %s", got, want)
	}
}

func TestParallelSelectionResolvesComputedProjectionColumnWithoutAlias(t *testing.T) {
	table := metadata.NewTable("computed_projection")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	projection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
		Exprs: []Expression{&BinaryOperation{
			Op:    OpAdd,
			Left:  &Column{Name: "id"},
			Right: &Constant{Value: int64(1)},
		}},
	}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{projection}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "(id + 1)"},
			Right: &Constant{Value: int64(1)},
		}},
	}
	executor := NewParallelExecutor(1, 1)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{int64(1)}}, nil
	})

	rows, err := executor.Execute(context.Background(), selection)
	if err != nil {
		t.Fatalf("computed projection selection Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[2]]"; got != want {
		t.Fatalf("computed projection selection rows = %s, want %s", got, want)
	}
}

func TestParallelMergeJoinResolvesQualifiedProjectedColumns(t *testing.T) {
	leftTable := metadata.NewTable("projected_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("projected_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	leftProjection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: leftTable}}},
		Exprs:            []Expression{&Column{Name: "id"}},
		OutputNames:      []string{"left_key"},
	}
	rightProjection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: rightTable}}},
		Exprs:            []Expression{&Column{Name: "id"}},
		OutputNames:      []string{"right_key"},
	}
	join := &PhysicalMergeJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{leftProjection, rightProjection}},
		JoinType:         "INNER",
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "projected_left.left_key"},
			Right: &Column{Name: "projected_right.right_key"},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		scan, ok := plan.(*ParallelTableScan)
		if !ok || scan.Table == nil {
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
		if scan.Table.Name == "projected_left" {
			return [][]interface{}{{int64(1)}, {int64(2)}}, nil
		}
		return [][]interface{}{{int64(2)}, {int64(3)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("qualified projected merge join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[2 2]]"; got != want {
		t.Fatalf("qualified projected merge join rows = %s, want %s", got, want)
	}
}

func TestParallelSelectionResolvesUnionOutputColumn(t *testing.T) {
	leftTable := metadata.NewTable("union_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("union_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	projection := func(table *metadata.Table, name string) *PhysicalProjection {
		return &PhysicalProjection{
			BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
			Exprs:            []Expression{&Column{Name: "id"}},
			OutputNames:      []string{name},
		}
	}
	union := &PhysicalUnion{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{
			projection(leftTable, "union_key"),
			projection(rightTable, "union_key"),
		}},
		UnionType: "ALL",
	}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{union}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "union_key"},
			Right: &Constant{Value: int64(1)},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		scan, ok := plan.(*ParallelTableScan)
		if !ok || scan.Table == nil {
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
		if scan.Table.Name == "union_left" {
			return [][]interface{}{{int64(1)}}, nil
		}
		return [][]interface{}{{int64(2)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(selection))
	if err != nil {
		t.Fatalf("union output selection Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[2]]"; got != want {
		t.Fatalf("union output selection rows = %s, want %s", got, want)
	}
}

func TestParallelExecutorDerivesHashAggregateSpecFromExpressions(t *testing.T) {
	table := metadata.NewTable("sales")
	table.AddColumn(&metadata.Column{Name: "category", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "amount", DataType: metadata.TypeInt})
	scan := &PhysicalTableScan{Table: table}
	aggregate := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{scan}},
		GroupByItems:     []Expression{&Column{Name: "category"}},
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "COUNT",
			FuncArgs: []Expression{&Column{Name: "amount"}},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"a", int64(1)}, {"a", nil}, {"b", int64(2)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggregate))
	if err != nil {
		t.Fatalf("hash aggregate Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[a 1] [b 1]]" {
		t.Fatalf("hash aggregate rows = %v, want [[a 1] [b 1]]", rows)
	}
}

func TestParallelExecutorHashAggregateAcceptsQualifiedColumnNames(t *testing.T) {
	table := metadata.NewTable("orders")
	table.AddColumn(&metadata.Column{Name: "category", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "amount", DataType: metadata.TypeInt})
	aggregate := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
		GroupByItems:     []Expression{&Column{Name: "orders.category"}},
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "COUNT",
			FuncArgs: []Expression{&Column{Name: "orders.amount"}},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"a", int64(1)}, {"a", nil}, {"b", int64(2)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggregate))
	if err != nil {
		t.Fatalf("qualified hash aggregate Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[a 1] [b 1]]"; got != want {
		t.Fatalf("qualified hash aggregate rows = %s, want %s", got, want)
	}
}

func TestParallelSelectionEvaluatesHavingAgainstAggregateOutput(t *testing.T) {
	table := metadata.NewTable("parallel_having_source")
	table.AddColumn(&metadata.Column{Name: "category", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "amount", DataType: metadata.TypeInt})
	aggregate := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
		GroupByItems:     []Expression{&Column{Name: "category"}},
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "COUNT",
			FuncArgs: []Expression{&Column{Name: "amount"}},
		}},
	}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{aggregate}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  &Function{FuncName: "COUNT", FuncArgs: []Expression{&Column{Name: "amount"}}},
			Right: &Constant{Value: int64(1)},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"a", 1}, {"a", 2}, {"b", 3}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(selection))
	if err != nil {
		t.Fatalf("HAVING over parallel aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{"a", int64(2)}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("HAVING over parallel aggregate rows = %v, want %v", rows, want)
	}
}

func TestParallelExecutorExecutesPhysicalStreamAggregate(t *testing.T) {
	table := metadata.NewTable("stream_sales")
	table.AddColumn(&metadata.Column{Name: "category", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "amount", DataType: metadata.TypeInt})
	stream := &PhysicalStreamAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
		GroupByItems:     []Expression{&Column{Name: "category"}},
		AggFuncs: []AggregateFunc{&Function{
			FuncName: "SUM",
			FuncArgs: []Expression{&Column{Name: "amount"}},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"a", int64(1)}, {"a", int64(2)}, {"b", int64(4)}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(stream))
	if err != nil {
		t.Fatalf("stream aggregate Execute() error = %v", err)
	}
	if fmt.Sprint(rows) != "[[a 3] [b 4]]" {
		t.Fatalf("stream aggregate rows = %v, want [[a 3] [b 4]]", rows)
	}
}

func TestParallelTableScanUsesInjectedChunkReader(t *testing.T) {
	table := metadata.NewTable("parallel_reader")
	table.Stats.RowCount = 9
	seen := make([]DataChunk, 0)
	var seenMu sync.Mutex
	scan := &PhysicalTableScan{
		Table: table,
		ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			seenMu.Lock()
			seen = append(seen, chunk)
			seenMu.Unlock()
			rows := make([][]interface{}, 0, chunk.EndRowID-chunk.StartRowID)
			for id := chunk.StartRowID; id < chunk.EndRowID; id++ {
				rows = append(rows, []interface{}{fmt.Sprintf("row-%d", id)})
			}
			return rows, nil
		},
	}

	executor := NewParallelExecutor(2, 3)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(scan))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(rows) != 9 {
		t.Fatalf("Execute() returned %d rows, want 9", len(rows))
	}
	for i, row := range rows {
		if len(row) != 1 || row[0] != fmt.Sprintf("row-%d", i) {
			t.Fatalf("row %d = %v, want row-%d", i, row, i)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("chunk reader saw %d chunks, want 3", len(seen))
	}
}

func TestParallelTableScanProjectsRequiredColumnsFromRealRows(t *testing.T) {
	table := metadata.NewTable("parallel_projection")
	table.Stats.RowCount = 2
	table.AddColumn(&metadata.Column{Name: "id"})
	table.AddColumn(&metadata.Column{Name: "name"})
	table.AddColumn(&metadata.Column{Name: "age"})
	scan := &PhysicalTableScan{
		Table:           table,
		RequiredColumns: []string{"name", "age"},
		ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return [][]interface{}{{int64(1), "alice", int64(42)}, {int64(2), "bob", int64(33)}}[chunk.StartRowID:chunk.EndRowID], nil
		},
	}

	executor := NewParallelExecutor(2, 2)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(scan))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := [][]interface{}{{"alice", int64(42)}, {"bob", int64(33)}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("projected rows = %v, want %v", rows, want)
	}
}

func TestParallelIndexScanUsesRealRowsAndProjectsRequiredColumns(t *testing.T) {
	table := metadata.NewTable("parallel_index_projection")
	table.Stats.RowCount = 4
	table.AddColumn(&metadata.Column{Name: "id"})
	table.AddColumn(&metadata.Column{Name: "name"})
	table.AddColumn(&metadata.Column{Name: "age"})
	index := &metadata.Index{Name: "idx_name", Columns: []string{"name"}}
	var readCalls int32
	scan := &PhysicalIndexScan{
		Table:           table,
		Index:           index,
		RequiredColumns: []string{"name", "age"},
		ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			atomic.AddInt32(&readCalls, 1)
			rows := [][]interface{}{
				{int64(1), "alice", int64(42)},
				{int64(2), "bob", int64(33)},
				{int64(3), "carol", int64(29)},
				{int64(4), "dave", int64(31)},
			}
			return rows[chunk.StartRowID:chunk.EndRowID], nil
		},
	}

	executor := NewParallelExecutor(2, 2)
	plan := executor.ParallelizePhysicalPlan(scan)
	if _, ok := plan.(*ParallelIndexScan); !ok {
		t.Fatalf("parallelized index scan = %T, want *ParallelIndexScan", plan)
	}
	rows, err := executor.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := [][]interface{}{{"alice", int64(42)}, {"bob", int64(33)}, {"carol", int64(29)}, {"dave", int64(31)}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("projected rows = %v, want %v", rows, want)
	}
	if got := atomic.LoadInt32(&readCalls); got != 2 {
		t.Fatalf("index chunk reader calls = %d, want 2", got)
	}
}

func TestParallelIndexScanUsesConfiguredPlanRowReader(t *testing.T) {
	table := metadata.NewTable("parallel_index_reader")
	table.Stats.RowCount = 2
	table.AddColumn(&metadata.Column{Name: "id"})
	table.AddColumn(&metadata.Column{Name: "value"})
	index := &metadata.Index{Name: "idx_value", Columns: []string{"value"}}
	scan := &PhysicalIndexScan{Table: table, Index: index, RequiredColumns: []string{"value"}}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if plan != scan {
			t.Fatalf("row reader received %T instead of original index scan", plan)
		}
		return [][]interface{}{{int64(1), "a"}, {int64(2), "b"}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(scan))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := [][]interface{}{{"a"}, {"b"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("projected rows = %v, want %v", rows, want)
	}
}

func TestRequiredColumnNamesPreservesPrunedLogicalSchema(t *testing.T) {
	table := metadata.NewTable("required_columns")
	table.AddColumn(&metadata.Column{Name: "id"})
	table.AddColumn(&metadata.Column{Name: "name"})
	pruned := metadata.NewSchema("test")
	prunedTable := metadata.NewTable(table.Name)
	prunedTable.AddColumn(&metadata.Column{Name: "name"})
	if err := pruned.AddTable(prunedTable); err != nil {
		t.Fatalf("AddTable() error = %v", err)
	}
	got := RequiredColumnNames(table, pruned)
	if fmt.Sprint(got) != "[name]" {
		t.Fatalf("required columns = %v, want [name]", got)
	}
}

func TestParallelExecutorHonorsCancellationAndNormalizesConfig(t *testing.T) {
	table := metadata.NewTable("parallel_cancel")
	table.Stats.RowCount = 2
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	executor := NewParallelExecutor(0, 0)
	plan := executor.ParallelizePhysicalPlan(&PhysicalTableScan{Table: table})
	if _, err := executor.Execute(ctx, plan); err == nil {
		t.Fatal("Execute() with a cancelled context should fail")
	}
}

func TestParallelHashJoinRequiresEquiKeysAndChildren(t *testing.T) {
	executor := NewParallelExecutor(3, 4)
	plan := executor.ParallelizePhysicalPlan(&PhysicalHashJoin{})
	if _, err := executor.Execute(context.Background(), plan); err == nil {
		t.Fatal("hash join without equi keys and row-producing children should fail")
	}
}

func TestParallelHashJoinUsesChildRowReader(t *testing.T) {
	left := &PhysicalTableScan{Table: metadata.NewTable("parallel_join_left")}
	right := &PhysicalTableScan{Table: metadata.NewTable("parallel_join_right")}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		HashJoinKeys:     []HashJoinKey{{LeftIndex: 0, RightIndex: 0}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch plan {
		case left:
			return [][]interface{}{{1, "a"}, {2, "b"}, {1, "c"}}, nil
		case right:
			return [][]interface{}{{1, "x"}, {3, "z"}}, nil
		default:
			return nil, nil
		}
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("hash join Execute() error = %v", err)
	}
	want := [][]interface{}{{1, "a", 1, "x"}, {1, "c", 1, "x"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("hash join rows = %v, want %v", rows, want)
	}
}

func TestParallelHashJoinDerivesKeysFromEqualityConditions(t *testing.T) {
	leftTable := metadata.NewTable("hash_derive_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("hash_derive_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "hash_derive_left.id"},
			Right: &Column{Name: "hash_derive_right.id"},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch plan {
		case left:
			return [][]interface{}{{int64(1)}, {int64(2)}}, nil
		case right:
			return [][]interface{}{{int64(2)}, {int64(3)}}, nil
		default:
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("derived-key hash join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[2 2]]"; got != want {
		t.Fatalf("derived-key hash join rows = %s, want %s", got, want)
	}
}

func TestParallelHashJoinEvaluatesResidualConditions(t *testing.T) {
	leftTable := metadata.NewTable("hash_residual_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("hash_residual_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable.AddColumn(&metadata.Column{Name: "score", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		HashJoinKeys:     []HashJoinKey{{LeftIndex: 0, RightIndex: 0}},
		Conditions: []Expression{
			&BinaryOperation{Op: OpEQ, Left: &Column{Name: "hash_residual_left.id"}, Right: &Column{Name: "hash_residual_right.id"}},
			&BinaryOperation{Op: OpGT, Left: &Column{Name: "hash_residual_right.score"}, Right: &Constant{Value: int64(10)}},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch plan {
		case left:
			return [][]interface{}{{int64(1)}}, nil
		case right:
			return [][]interface{}{{int64(1), int64(5)}, {int64(1), int64(20)}}, nil
		default:
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("residual-condition hash join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 1 20]]"; got != want {
		t.Fatalf("residual-condition hash join rows = %s, want %s", got, want)
	}
}

func TestParallelHashJoinUsesMySQLNumericCoercionForKeys(t *testing.T) {
	leftTable := metadata.NewTable("hash_coercion_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("hash_coercion_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeVarchar})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		HashJoinKeys:     []HashJoinKey{{LeftIndex: 0, RightIndex: 0}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "hash_coercion_left.id"},
			Right: &Column{Name: "hash_coercion_right.id"},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch plan {
		case left:
			return [][]interface{}{{int64(1)}}, nil
		case right:
			return [][]interface{}{{"1"}, {"2"}}, nil
		default:
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("numeric-coercion hash join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 1]]"; got != want {
		t.Fatalf("numeric-coercion hash join rows = %s, want %s", got, want)
	}
}

func TestParallelHashJoinSupportsLeftOuterRows(t *testing.T) {
	leftTable := metadata.NewTable("hash_outer_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("hash_outer_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable.AddColumn(&metadata.Column{Name: "value", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		JoinType:         "LEFT",
		HashJoinKeys:     []HashJoinKey{{LeftIndex: 0, RightIndex: 0}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch plan {
		case left:
			return [][]interface{}{{int64(1)}, {int64(2)}}, nil
		case right:
			return [][]interface{}{{int64(1), int64(20)}, {int64(3), int64(30)}}, nil
		default:
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("left outer hash join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 1 20] [2 <nil> <nil>]]"; got != want {
		t.Fatalf("left outer hash join rows = %s, want %s", got, want)
	}

	join.JoinType = "RIGHT"
	rows, err = executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("right outer hash join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 1 20] [<nil> 3 30]]"; got != want {
		t.Fatalf("right outer hash join rows = %s, want %s", got, want)
	}

	join.JoinType = "FULL"
	rows, err = executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("full outer hash join Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 1 20] [2 <nil> <nil>] [<nil> 3 30]]"; got != want {
		t.Fatalf("full outer hash join rows = %s, want %s", got, want)
	}
}

func TestParallelSelectionResolvesHashJoinOutputColumn(t *testing.T) {
	leftTable := metadata.NewTable("hash_output_left")
	leftTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable := metadata.NewTable("hash_output_right")
	rightTable.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	rightTable.AddColumn(&metadata.Column{Name: "score", DataType: metadata.TypeInt})
	left := &PhysicalTableScan{Table: leftTable}
	right := &PhysicalTableScan{Table: rightTable}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		HashJoinKeys:     []HashJoinKey{{LeftIndex: 0, RightIndex: 0}},
	}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{join}},
		Conditions: []Expression{
			&BinaryOperation{
				Op:    OpGT,
				Left:  &Column{Name: "score"},
				Right: &Constant{Value: int64(10)},
			},
			&BinaryOperation{
				Op:    OpGT,
				Left:  &Column{Name: "hash_output_right.score"},
				Right: &Constant{Value: int64(10)},
			},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch plan {
		case left:
			return [][]interface{}{{int64(1)}}, nil
		case right:
			return [][]interface{}{{int64(1), int64(20)}}, nil
		default:
			return nil, fmt.Errorf("unexpected row source %T", plan)
		}
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(selection))
	if err != nil {
		t.Fatalf("hash join output selection Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1 1 20]]"; got != want {
		t.Fatalf("hash join output selection rows = %s, want %s", got, want)
	}
}

func TestParallelSelectionUsesPrunedScanColumnNames(t *testing.T) {
	table := metadata.NewTable("pruned_scan")
	table.Stats.RowCount = 1
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})
	scan := &PhysicalTableScan{
		Table:           table,
		RequiredColumns: []string{"name"},
		ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return [][]interface{}{{int64(1), "alice"}}[chunk.StartRowID:chunk.EndRowID], nil
		},
	}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{scan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "name"},
			Right: &Constant{Value: "alice"},
		}},
	}
	executor := NewParallelExecutor(1, 1)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(selection))
	if err != nil {
		t.Fatalf("pruned scan selection Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[alice]]"; got != want {
		t.Fatalf("pruned scan selection rows = %s, want %s", got, want)
	}
}

func TestParallelPlanRowReaderHonorsPrunedTableScanColumns(t *testing.T) {
	table := metadata.NewTable("reader_pruned_scan")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})
	scan := &PhysicalTableScan{Table: table, RequiredColumns: []string{"name"}}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{scan}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "name"},
			Right: &Constant{Value: "alice"},
		}},
	}
	executor := NewParallelExecutor(1, 1)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{int64(1), "alice"}}, nil
	})
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(selection))
	if err != nil {
		t.Fatalf("reader pruned scan selection Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[alice]]"; got != want {
		t.Fatalf("reader pruned scan selection rows = %s, want %s", got, want)
	}
}

func TestParallelSelectionResolvesValuesOutputColumn(t *testing.T) {
	values := &PhysicalValues{Exprs: []Expression{&Constant{Value: int64(1)}}}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{values}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "1"},
			Right: &Constant{Value: int64(1)},
		}},
	}
	executor := NewParallelExecutor(1, 1)
	rows, err := executor.Execute(context.Background(), selection)
	if err != nil {
		t.Fatalf("values output selection Execute() error = %v", err)
	}
	if got, want := fmt.Sprint(rows), "[[1]]"; got != want {
		t.Fatalf("values output selection rows = %s, want %s", got, want)
	}
}

func TestParallelHashJoinUsesParallelizedChildScans(t *testing.T) {
	leftTable := metadata.NewTable("parallel_join_tree_left")
	leftTable.Stats.RowCount = 2
	rightTable := metadata.NewTable("parallel_join_tree_right")
	rightTable.Stats.RowCount = 2
	left := &PhysicalTableScan{
		Table: leftTable,
		ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return [][]interface{}{{1, "left-a"}, {2, "left-b"}}[chunk.StartRowID:chunk.EndRowID], nil
		},
	}
	right := &PhysicalTableScan{
		Table: rightTable,
		ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return [][]interface{}{{1, "right-a"}, {3, "right-c"}}[chunk.StartRowID:chunk.EndRowID], nil
		},
	}
	join := &PhysicalHashJoin{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{left, right}},
		HashJoinKeys:     []HashJoinKey{{LeftIndex: 0, RightIndex: 0}},
	}

	executor := NewParallelExecutor(2, 2)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(join))
	if err != nil {
		t.Fatalf("hash join Execute() error = %v", err)
	}
	want := [][]interface{}{{1, "left-a", 1, "right-a"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("hash join rows = %v, want %v", rows, want)
	}
}

func TestParallelSortUsesChildRowReader(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_sort_source")}
	sortPlan := &PhysicalSort{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		ByItems:          []ByItem{{Expr: &Column{Name: "value"}}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{4, "d"}, {1, "a"}, {3, "c"}, {2, "b"}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(sortPlan))
	if err != nil {
		t.Fatalf("sort Execute() error = %v", err)
	}
	want := [][]interface{}{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("sort rows = %v, want %v", rows, want)
	}
}

func TestParallelSortUsesParallelizedChildScan(t *testing.T) {
	table := metadata.NewTable("parallel_sort_tree_source")
	table.Stats.RowCount = 4
	sortPlan := &PhysicalSort{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{
			Table: table,
			ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return [][]interface{}{{4, "d"}, {1, "a"}, {3, "c"}, {2, "b"}}[chunk.StartRowID:chunk.EndRowID], nil
			},
		}}},
		ByItems: []ByItem{{Expr: &Column{Name: "value"}}},
	}

	executor := NewParallelExecutor(2, 2)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(sortPlan))
	if err != nil {
		t.Fatalf("sort Execute() error = %v", err)
	}
	want := [][]interface{}{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("sort rows = %v, want %v", rows, want)
	}
}

func TestPhysicalProjectionAppliesDistinctBeforeLimit(t *testing.T) {
	table := metadata.NewTable("parallel_projection_distinct_source")
	table.Columns = []*metadata.Column{{Name: "value", DataType: metadata.TypeInt}}
	projection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
		Exprs:            []Expression{&Column{Name: "value"}},
		Distinct:         true,
		Limit:            1,
		HasLimit:         true,
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{1}, {1}, {2}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(projection))
	if err != nil {
		t.Fatalf("distinct projection Execute() error = %v", err)
	}
	want := [][]interface{}{{1}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("distinct projection rows = %v, want %v", rows, want)
	}
}

func TestParallelSelectionUsesProjectionOutputAlias(t *testing.T) {
	table := metadata.NewTable("parallel_projection_alias_source")
	table.Columns = []*metadata.Column{{Name: "value", DataType: metadata.TypeInt}}
	projection := &PhysicalProjection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{Table: table}}},
		Exprs:            []Expression{&Column{Name: "value"}},
		OutputNames:      []string{"projected_value"},
	}
	selection := &PhysicalSelection{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{projection}},
		Conditions: []Expression{&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "projected_value"},
			Right: &Constant{Value: int64(1)},
		}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{1}, {2}, {3}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(selection))
	if err != nil {
		t.Fatalf("selection over aliased projection Execute() error = %v", err)
	}
	want := [][]interface{}{{2}, {3}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("selection over aliased projection rows = %v, want %v", rows, want)
	}
}

func TestParallelSortOrdersTextLexicallyAndKeepsNumericOrdering(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_sort_text_source")}
	sortPlan := &PhysicalSort{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		ByItems:          []ByItem{{Expr: &Column{Name: "value"}}},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"z"}, {"a"}, {"m"}, {"b"}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(sortPlan))
	if err != nil {
		t.Fatalf("text sort Execute() error = %v", err)
	}
	want := [][]interface{}{{"a"}, {"b"}, {"m"}, {"z"}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("text sort rows = %v, want %v", rows, want)
	}
}

func TestParallelHashAggUsesChildRowReader(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_agg_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			GroupByIndexes: []int{0},
			Functions: []HashAggregateFunction{
				{Name: "COUNT", InputIndex: -1},
				{Name: "SUM", InputIndex: 1},
				{Name: "AVG", InputIndex: 1},
			},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"a", 1}, {"b", 2}, {"a", 3}, {"b", nil}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{"a", int64(2), float64(4), float64(2)}, {"b", int64(2), float64(2), float64(2)}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("aggregate rows = %v, want %v", rows, want)
	}
}

func TestParallelHashAggSupportsGroupConcatDistinctAndSeparator(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_group_concat_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			GroupByIndexes: []int{0},
			Functions: []HashAggregateFunction{{
				Name:       "GROUP_CONCAT",
				InputIndex: 1,
				Distinct:   true,
				Separator:  ";",
			}},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"a", "red"}, {"a", "blue"}, {"a", "red"}, {"a", nil}, {"b", nil}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("group concat aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{"a", "red;blue"}, {"b", nil}}
	if got := fmt.Sprint(rows); got != fmt.Sprint(want) {
		t.Fatalf("group concat aggregate rows = %s, want %s", got, want)
	}
}

func TestParallelHashAggSupportsBitAndVarianceAggregates(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_bit_aggregate_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			Functions: []HashAggregateFunction{
				{Name: "BIT_AND", InputIndex: 0},
				{Name: "BIT_OR", InputIndex: 0},
				{Name: "BIT_XOR", InputIndex: 0},
				{Name: "VAR_POP", InputIndex: 0},
				{Name: "STDDEV_SAMP", InputIndex: 0},
			},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{1}, {3}, {3}, {nil}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("bit/stat aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{int64(1), int64(3), int64(1), float64(8) / 9, math.Sqrt(float64(4) / 3)}}
	if got := fmt.Sprint(rows); got != fmt.Sprint(want) {
		t.Fatalf("bit/stat aggregate rows = %s, want %s", got, want)
	}
}

func TestParallelHashAggSupportsJSONArrayAgg(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_json_array_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			Functions: []HashAggregateFunction{{Name: "JSON_ARRAYAGG", InputIndex: 0}},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{1}, {nil}, {3}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("JSON_ARRAYAGG Execute() error = %v", err)
	}
	want := [][]interface{}{{`[1,null,3]`}}
	if got := fmt.Sprint(rows); got != fmt.Sprint(want) {
		t.Fatalf("JSON_ARRAYAGG rows = %s, want %s", got, want)
	}
}

func TestParallelHashAggSupportsAnyValue(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_any_value_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			Functions: []HashAggregateFunction{{Name: "ANY_VALUE", InputIndex: 0}},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{"first"}, {"second"}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("ANY_VALUE Execute() error = %v", err)
	}
	want := [][]interface{}{{"first"}}
	if got := fmt.Sprint(rows); got != fmt.Sprint(want) {
		t.Fatalf("ANY_VALUE rows = %s, want %s", got, want)
	}
}

func TestParallelHashAggSupportsDistinctAggregates(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_distinct_aggregate_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			Functions: []HashAggregateFunction{
				{Name: "COUNT", InputIndex: 0, Distinct: true},
				{Name: "SUM", InputIndex: 0, Distinct: true},
				{Name: "AVG", InputIndex: 0, Distinct: true},
			},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{{1}, {1}, {2}, {nil}}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("distinct aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{int64(2), float64(3), float64(1.5)}}
	if got := fmt.Sprint(rows); got != fmt.Sprint(want) {
		t.Fatalf("distinct aggregate rows = %s, want %s", got, want)
	}
}

func TestParallelHashAggSupportsMultiInputAggregates(t *testing.T) {
	child := &PhysicalTableScan{Table: metadata.NewTable("parallel_multi_input_aggregate_source")}
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
		AggregateSpec: &HashAggregateSpec{
			Functions: []HashAggregateFunction{
				{Name: "COUNT", InputIndexes: []int{0, 1}, InputIndex: 0, Distinct: true},
				{Name: "JSON_OBJECTAGG", InputIndexes: []int{0, 1}, InputIndex: 0},
			},
		},
	}
	executor := NewParallelExecutor(2, 2)
	executor.SetPlanRowReader(func(ctx context.Context, plan PhysicalPlan) ([][]interface{}, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return [][]interface{}{
			{"a", 1},
			{"a", 1},
			{"a", 2},
			{"b", nil},
			{nil, 3},
			{"b", 2},
		}, nil
	})

	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("multi-input aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{int64(3), `{"a":2,"b":2}`}}
	if got := fmt.Sprint(rows); got != fmt.Sprint(want) {
		t.Fatalf("multi-input aggregate rows = %s, want %s", got, want)
	}
}

func TestDeriveParallelHashAggregateSpecBindsAllInputs(t *testing.T) {
	table := metadata.NewTable("parallel_derive_multi_input_source")
	table.AddColumn(&metadata.Column{Name: "k", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "v", DataType: metadata.TypeInt})
	child := &PhysicalTableScan{Table: table}
	agg := &ParallelHashAgg{
		PhysicalHashAgg: PhysicalHashAgg{
			BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{child}},
			AggFuncs: []AggregateFunc{
				&Function{FuncName: "COUNT", Distinct: true, FuncArgs: []Expression{
					&Column{Name: "k"}, &Column{Name: "v"},
				}},
				&Function{FuncName: "JSON_OBJECTAGG", FuncArgs: []Expression{
					&Column{Name: "k"}, &Column{Name: "v"},
				}},
			},
		},
	}

	spec, err := deriveParallelHashAggregateSpec(agg)
	if err != nil {
		t.Fatalf("deriveParallelHashAggregateSpec() error = %v", err)
	}
	if got := len(spec.Functions[0].InputIndexes); got != 2 {
		t.Fatalf("COUNT input indexes = %d, want 2", got)
	}
	if got := len(spec.Functions[1].InputIndexes); got != 2 {
		t.Fatalf("JSON_OBJECTAGG input indexes = %d, want 2", got)
	}
}

func TestParallelHashAggUsesParallelizedChildScan(t *testing.T) {
	table := metadata.NewTable("parallel_agg_tree_source")
	table.Stats.RowCount = 4
	aggPlan := &PhysicalHashAgg{
		BasePhysicalPlan: BasePhysicalPlan{children: []PhysicalPlan{&PhysicalTableScan{
			Table: table,
			ChunkReader: func(ctx context.Context, chunk DataChunk) ([][]interface{}, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return [][]interface{}{{"a", 1}, {"b", 2}, {"a", 3}, {"b", nil}}[chunk.StartRowID:chunk.EndRowID], nil
			},
		}}},
		AggregateSpec: &HashAggregateSpec{
			GroupByIndexes: []int{0},
			Functions: []HashAggregateFunction{
				{Name: "COUNT", InputIndex: -1},
				{Name: "SUM", InputIndex: 1},
				{Name: "AVG", InputIndex: 1},
			},
		},
	}

	executor := NewParallelExecutor(2, 2)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(aggPlan))
	if err != nil {
		t.Fatalf("aggregate Execute() error = %v", err)
	}
	want := [][]interface{}{{"a", int64(2), float64(4), float64(2)}, {"b", int64(2), float64(2), float64(2)}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("aggregate rows = %v, want %v", rows, want)
	}
}
