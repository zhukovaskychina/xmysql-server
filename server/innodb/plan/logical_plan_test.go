package plan

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"testing"
)

type mockInfoSchema struct{ tables map[string]*metadata.Table }

func (m *mockInfoSchema) TableByName(name string) (*metadata.Table, error) {
	if t, ok := m.tables[name]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("table %s not found", name)
}

func createTestTable2() *metadata.Table {
	table := &metadata.Table{
		Name: "users",
		Columns: []*metadata.Column{
			{Name: "id", DataType: metadata.TypeBigInt},
			{Name: "name", DataType: metadata.TypeVarchar},
		},
	}
	schema := metadata.NewSchema("test")
	schema.AddTable(table)
	return table
}

func TestBuildLogicalPlanPreservesQualifiedTableSchema(t *testing.T) {
	table := createTestTable2()
	table.Schema.Name = "app"
	info := &mockInfoSchema{tables: map[string]*metadata.Table{"app.users": table}}
	stmt, err := sqlparser.Parse("select * from app.users")
	if err != nil {
		t.Fatalf("parse qualified SELECT failed: %v", err)
	}
	logical, err := BuildLogicalPlan(stmt.(*sqlparser.Select), info)
	if err != nil {
		t.Fatalf("build qualified SELECT plan failed: %v", err)
	}
	scan, ok := findLogicalTableScan(logical)
	if !ok {
		t.Fatalf("expected qualified table scan, got %T", logical)
	}
	if scan.Table.Schema == nil || scan.Table.Schema.Name != "app" {
		t.Fatalf("qualified schema was not preserved: %#v", scan.Table.Schema)
	}
}

func findLogicalTableScan(logical LogicalPlan) (*LogicalTableScan, bool) {
	if scan, ok := logical.(*LogicalTableScan); ok {
		return scan, true
	}
	for _, child := range logical.Children() {
		if scan, ok := findLogicalTableScan(child); ok {
			return scan, true
		}
	}
	return nil, false
}

func TestBuildExpressionConvertsGroupConcatOptions(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT GROUP_CONCAT(DISTINCT name SEPARATOR ';') FROM users")
	if err != nil {
		t.Fatalf("parse GROUP_CONCAT failed: %v", err)
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		t.Fatalf("expected one select expression, got %T", stmt)
	}
	aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		t.Fatalf("expected aliased expression, got %T", selectStmt.SelectExprs[0])
	}
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected GROUP_CONCAT function, got %T", BuildExpression(aliased.Expr))
	}
	if function.FuncName != "GROUP_CONCAT" || !function.Distinct || function.Separator != ";" {
		t.Fatalf("GROUP_CONCAT options = name %q distinct %v separator %q", function.FuncName, function.Distinct, function.Separator)
	}
	if len(function.FuncArgs) != 1 || function.FuncArgs[0].String() != "name" {
		t.Fatalf("GROUP_CONCAT args = %v", function.FuncArgs)
	}
}

func TestBuildExpressionPreservesConvertUsingCharset(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('é' USING latin1) FROM users")
	if err != nil {
		t.Fatalf("parse CONVERT USING failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT USING function, got %T", BuildExpression(aliased.Expr))
	}
	if function.FuncName != "CONVERT" || function.UsingCharset != "latin1" {
		t.Fatalf("CONVERT USING = name %q charset %q", function.FuncName, function.UsingCharset)
	}

	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate CONVERT USING failed: %v", err)
	}
	if got != string([]byte{0xe9}) {
		t.Fatalf("CONVERT('é' USING latin1) = %#v, want latin1 byte 0xe9", got)
	}
}

func TestBuildExpressionPreservesConvertTypeCharset(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('é', CHAR CHARACTER SET latin1) FROM users")
	if err != nil {
		t.Fatalf("parse CONVERT CHAR CHARACTER SET failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT function, got %T", BuildExpression(aliased.Expr))
	}
	if function.FuncName != "CONVERT" || function.UsingCharset != "latin1" {
		t.Fatalf("CONVERT type charset = name %q charset %q", function.FuncName, function.UsingCharset)
	}
	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate CONVERT CHAR CHARACTER SET failed: %v", err)
	}
	if got != string([]byte{0xe9}) {
		t.Fatalf("CONVERT('é', CHAR CHARACTER SET latin1) = %#v, want latin1 byte 0xe9", got)
	}
}

func TestBuildExpressionSupportsBinaryCastTarget(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('é', BINARY) FROM users")
	if err != nil {
		t.Fatalf("parse CONVERT BINARY failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT function, got %T", BuildExpression(aliased.Expr))
	}
	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate CONVERT BINARY failed: %v", err)
	}
	if got != string([]byte{0xc3, 0xa9}) {
		t.Fatalf("CONVERT('é', BINARY) = %#v, want original UTF-8 bytes", got)
	}
}

func TestBuildExpressionHonorsCastCharacterLength(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('abcdef', CHAR(3)) FROM users")
	if err != nil {
		t.Fatalf("parse CONVERT CHAR length failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT function, got %T", BuildExpression(aliased.Expr))
	}
	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate CONVERT CHAR length failed: %v", err)
	}
	if got != "abc" {
		t.Fatalf("CONVERT('abcdef', CHAR(3)) = %#v, want abc", got)
	}
}

func TestBuildExpressionHonorsDecimalScale(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('123.456', DECIMAL(8, 2)) FROM users")
	if err != nil {
		t.Fatalf("parse CONVERT DECIMAL precision failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT function, got %T", BuildExpression(aliased.Expr))
	}
	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate CONVERT DECIMAL(8,2) failed: %v", err)
	}
	if got != float64(123.46) {
		t.Fatalf("CONVERT('123.456', DECIMAL(8,2)) = %#v, want 123.46", got)
	}
}

func TestBuildExpressionTruncatesBeforeCharsetConversion(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('éx', CHAR(1) CHARACTER SET latin1) FROM users")
	if err != nil {
		t.Fatalf("parse charset conversion with length failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT function, got %T", BuildExpression(aliased.Expr))
	}
	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate charset conversion with length failed: %v", err)
	}
	if got != string([]byte{0xe9}) {
		t.Fatalf("CONVERT('éx', CHAR(1) CHARACTER SET latin1) = %#v, want latin1 byte 0xe9", got)
	}
}

func TestBuildExpressionHonorsBinaryLengthInBytes(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT CONVERT('éx', BINARY(2)) FROM users")
	if err != nil {
		t.Fatalf("parse CONVERT BINARY length failed: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	aliased := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	function, ok := BuildExpression(aliased.Expr).(*Function)
	if !ok {
		t.Fatalf("expected CONVERT function, got %T", BuildExpression(aliased.Expr))
	}
	got, err := function.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("evaluate CONVERT BINARY(2) failed: %v", err)
	}
	if got != string([]byte{0xc3, 0xa9}) {
		t.Fatalf("CONVERT('éx', BINARY(2)) = %#v, want first two bytes", got)
	}
}

func TestBuildLogicalPlan_GroupBy(t *testing.T) {
	table := createTestTable2()
	schema := metadata.NewSchema("test")
	_ = schema.AddTable(table)
	_ = schema // avoid unused
	info := &mockInfoSchema{tables: map[string]*metadata.Table{"users": table}}

	stmt, err := sqlparser.Parse("SELECT name, COUNT(id) FROM users GROUP BY name")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	sel := stmt.(*sqlparser.Select)

	planNode, err := BuildLogicalPlan(sel, info)
	if err != nil {
		t.Fatalf("build plan error: %v", err)
	}

	proj, ok := planNode.(*LogicalProjection)
	if !ok {
		t.Fatalf("expected projection")
	}
	agg, ok := proj.Children()[0].(*LogicalAggregation)
	if !ok {
		t.Fatalf("expected aggregation node")
	}
	if len(agg.GroupByItems) != 1 || len(agg.AggFuncs) != 1 {
		t.Fatalf("unexpected agg content")
	}
}

func TestBuildLogicalPlan_PreservesHavingAfterGroupBy(t *testing.T) {
	table := createTestTable2()
	info := &mockInfoSchema{tables: map[string]*metadata.Table{"users": table}}
	stmt, err := sqlparser.Parse("SELECT name, COUNT(id) FROM users GROUP BY name HAVING COUNT(id) > 1")
	if err != nil {
		t.Fatalf("parse HAVING statement: %v", err)
	}

	logical, err := BuildLogicalPlan(stmt.(*sqlparser.Select), info)
	if err != nil {
		t.Fatalf("build HAVING plan: %v", err)
	}
	projection, ok := logical.(*LogicalProjection)
	if !ok || len(projection.Children()) != 1 {
		t.Fatalf("logical plan = %T, want projection with one child", logical)
	}
	if _, ok := projection.Children()[0].(*LogicalSelection); !ok {
		t.Fatalf("projection child = %T, want HAVING selection", projection.Children()[0])
	}
}

func TestBuildLogicalPlanProjectionPreservesSelectedColumnOrder(t *testing.T) {
	table := createTestTable2()
	info := &mockInfoSchema{tables: map[string]*metadata.Table{"users": table}}
	stmt, err := sqlparser.Parse("SELECT name, id FROM users")
	if err != nil {
		t.Fatalf("parse projection statement: %v", err)
	}

	logical, err := BuildLogicalPlan(stmt.(*sqlparser.Select), info)
	if err != nil {
		t.Fatalf("build projection plan: %v", err)
	}
	if got := getPlanOutputColumnNames(logical); fmt.Sprint(got) != "[name id]" {
		t.Fatalf("projection output columns = %v, want [name id]", got)
	}
}

func TestBuildLogicalPlanProjectionPreservesColumnAlias(t *testing.T) {
	table := createTestTable2()
	info := &mockInfoSchema{tables: map[string]*metadata.Table{"users": table}}
	stmt, err := sqlparser.Parse("SELECT name AS username FROM users")
	if err != nil {
		t.Fatalf("parse aliased projection: %v", err)
	}

	logical, err := BuildLogicalPlan(stmt.(*sqlparser.Select), info)
	if err != nil {
		t.Fatalf("build aliased projection plan: %v", err)
	}
	if got := getPlanOutputColumnNames(logical); fmt.Sprint(got) != "[username]" {
		t.Fatalf("aliased projection output columns = %v, want [username]", got)
	}
}

func TestBuildUnionPreservesTopLevelOrderAndLimit(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 2 UNION ALL SELECT 1 ORDER BY 1 LIMIT 1")
	if err != nil {
		t.Fatalf("parse UNION statement: %v", err)
	}

	logical, err := BuildLogicalPlanStatement(stmt, nil)
	if err != nil {
		t.Fatalf("build UNION plan: %v", err)
	}
	union, ok := logical.(*LogicalUnion)
	if !ok {
		t.Fatalf("logical plan = %T, want *LogicalUnion", logical)
	}
	if len(union.OrderBy) != 1 || !union.HasLimit || union.Limit != 1 {
		t.Fatalf("UNION tail = order %v, hasLimit %v, limit %d; want ORDER BY 1 LIMIT 1", union.OrderBy, union.HasLimit, union.Limit)
	}

	executor := NewParallelExecutor(2, 1)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(ConvertToPhysicalPlan(logical)))
	if err != nil {
		t.Fatalf("execute UNION plan: %v", err)
	}
	want := [][]interface{}{{1}}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("UNION rows = %v, want %v", rows, want)
	}
}

func TestBuildUnionResolvesQualifiedOrderColumn(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 2 AS first_value, 3 AS value UNION ALL SELECT 1, 4 ORDER BY test_table.value")
	if err != nil {
		t.Fatalf("parse qualified UNION ORDER BY: %v", err)
	}

	logical, err := BuildLogicalPlanStatement(stmt, nil)
	if err != nil {
		t.Fatalf("build qualified UNION ORDER BY plan: %v", err)
	}
	union, ok := logical.(*LogicalUnion)
	if !ok || len(union.OrderBy) != 1 {
		t.Fatalf("logical plan = %T with order by %v, want one UNION order item", logical, union)
	}
	if union.OrderBy[0].ColumnIndex != 1 {
		t.Fatalf("qualified UNION ORDER BY column index = %d, want 1", union.OrderBy[0].ColumnIndex)
	}
}

func TestBuildSelectPreservesTopLevelLimit(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 2 ORDER BY 1 LIMIT 0")
	if err != nil {
		t.Fatalf("parse SELECT statement: %v", err)
	}

	logical, err := BuildLogicalPlanStatement(stmt, nil)
	if err != nil {
		t.Fatalf("build SELECT plan: %v", err)
	}
	projection, ok := logical.(*LogicalProjection)
	if !ok {
		t.Fatalf("logical plan = %T, want *LogicalProjection", logical)
	}
	if !projection.HasLimit || projection.Limit != 0 {
		t.Fatalf("SELECT tail = hasLimit %v, limit %d; want LIMIT 0", projection.HasLimit, projection.Limit)
	}

	executor := NewParallelExecutor(2, 1)
	rows, err := executor.Execute(context.Background(), executor.ParallelizePhysicalPlan(ConvertToPhysicalPlan(logical)))
	if err != nil {
		t.Fatalf("execute SELECT plan: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("SELECT LIMIT 0 rows = %v, want no rows", rows)
	}
}
