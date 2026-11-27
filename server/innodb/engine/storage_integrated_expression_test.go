package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestStorageIntegratedExpressionEvaluationCoversPredicateAST(t *testing.T) {
	tests := []struct {
		query    string
		expected interface{}
	}{
		{query: "select 1 = 1", expected: true},
		{query: "select 1 < 2", expected: true},
		{query: "select not (1 = 2)", expected: true},
		{query: "select 1 and 0", expected: false},
		{query: "select 1 or null", expected: true},
		{query: "select 2 in (1, null)", expected: nil},
		{query: "select (1, 2)", expected: []interface{}{int64(1), int64(2)}},
		{query: "select 7 % 3", expected: int64(1)},
		{query: "select 7 div 3", expected: int64(2)},
		{query: "select 6 & 3", expected: int64(2)},
		{query: "select 3 << 2", expected: int64(12)},
		{query: "select '2024-01-01' + interval 2 day", expected: "2024-01-03"},
		{query: "select addtime('2024-01-01 23:59:59.500000', '00:00:00.500000')", expected: "2024-01-02 00:00:00"},
		{query: "select subtime('12:00:00', '01:02:03')", expected: "10:57:57"},
		{query: "select timestamp('2024-01-01', '12:34:56')", expected: "2024-01-01 12:34:56"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.query)
			require.NoError(t, err)
			selectStmt, ok := stmt.(*sqlparser.Select)
			require.True(t, ok)
			aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
			require.True(t, ok)

			result, err := evaluateExpressionWithRow(aliased.Expr, map[string]interface{}{})
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStorageIntegratedExpressionResolvesQualifiedColumnsCaseInsensitively(t *testing.T) {
	stmt, err := sqlparser.Parse("select USERS.NAME")
	require.NoError(t, err)
	selectStmt, ok := stmt.(*sqlparser.Select)
	require.True(t, ok)
	aliased, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	require.True(t, ok)

	value, err := evaluateExpressionWithRow(aliased.Expr, map[string]interface{}{
		"Users.Name": "Alice",
	})
	require.NoError(t, err)
	require.Equal(t, "Alice", value)
}

func TestStorageIntegratedExpressionEvaluationSupportsInsertValues(t *testing.T) {
	stmt, err := sqlparser.Parse("insert into products (id, price) values (1, 42) on duplicate key update price = values(price)")
	require.NoError(t, err)
	insertStmt, ok := stmt.(*sqlparser.Insert)
	require.True(t, ok)
	require.Len(t, insertStmt.OnDup, 1)

	valuesExpr, ok := insertStmt.OnDup[0].Expr.(*sqlparser.ValuesFuncExpr)
	require.True(t, ok, "parser should preserve VALUES(col) as ValuesFuncExpr")

	row := map[string]interface{}{
		"price":                int64(10),
		insertValuesContextKey: map[string]interface{}{"price": int64(42)},
	}
	value, err := evaluateExpressionWithRow(valuesExpr, row)
	require.NoError(t, err)
	require.Equal(t, int64(42), value)

	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	updated, err := dml.applyUpdateExpressions(
		&InsertRowData{ColumnValues: map[string]interface{}{"price": int64(10)}},
		[]*UpdateExpression{{ColumnName: "price", Expr: valuesExpr}},
		nil,
		map[string]interface{}{"price": int64(42)},
	)
	require.NoError(t, err)
	require.Equal(t, int64(42), updated.ColumnValues["price"])

	legacyValue, err := (&InsertOperator{}).evaluateOnDupExpr(
		valuesExpr,
		nil,
		map[string]interface{}{"price": int64(42)},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int64(42), legacyValue)
}

func TestLegacyOnDuplicateSupportsMySQLScalarExpressions(t *testing.T) {
	stmt, err := sqlparser.Parse("insert into products (id, price) values (1, 42) on duplicate key update price = coalesce(values(price), price) + 1")
	require.NoError(t, err)
	insertStmt, ok := stmt.(*sqlparser.Insert)
	require.True(t, ok)
	require.Len(t, insertStmt.OnDup, 1)

	schema := &metadata.Table{
		Name:    "products",
		Columns: []*metadata.Column{{Name: "price", DataType: metadata.TypeBigInt}},
	}
	recordMeta := &metadata.TableMeta{
		Name:    "products",
		Columns: []*metadata.ColumnMeta{{Name: "price", Type: metadata.TypeBigInt}},
	}
	existing := NewExecutorRecordFromInterface([]interface{}{int64(10)}, recordMeta)
	value, err := (&InsertOperator{}).evaluateOnDupExpr(
		insertStmt.OnDup[0].Expr,
		existing,
		map[string]interface{}{"price": int64(42)},
		schema,
	)
	require.NoError(t, err)
	require.Equal(t, int64(43), value)
}

func TestLegacyOnDuplicateSupportsExtendedBinaryExpressions(t *testing.T) {
	schema := &metadata.Table{
		Name:    "products",
		Columns: []*metadata.Column{{Name: "price", DataType: metadata.TypeBigInt}},
	}
	recordMeta := &metadata.TableMeta{
		Name:    "products",
		Columns: []*metadata.ColumnMeta{{Name: "price", Type: metadata.TypeBigInt}},
	}
	existing := NewExecutorRecordFromInterface([]interface{}{int64(10)}, recordMeta)

	for _, test := range []struct {
		query    string
		expected interface{}
	}{
		{query: "insert into products (price) values (42) on duplicate key update price = price % 4", expected: int64(2)},
		{query: "insert into products (price) values (42) on duplicate key update price = price div 3", expected: int64(3)},
	} {
		stmt, err := sqlparser.Parse(test.query)
		require.NoError(t, err)
		insertStmt, ok := stmt.(*sqlparser.Insert)
		require.True(t, ok)
		value, err := (&InsertOperator{}).evaluateOnDupExpr(insertStmt.OnDup[0].Expr, existing, map[string]interface{}{"price": int64(42)}, schema)
		require.NoError(t, err, test.query)
		require.Equal(t, test.expected, value, test.query)
	}
}

func TestLegacyOnDuplicateResolvesColumnReferencesCaseInsensitively(t *testing.T) {
	schema := &metadata.Table{
		Name:    "products",
		Columns: []*metadata.Column{{Name: "price", DataType: metadata.TypeBigInt}},
	}
	recordMeta := &metadata.TableMeta{
		Name:    "products",
		Columns: []*metadata.ColumnMeta{{Name: "price", Type: metadata.TypeBigInt}},
	}
	existing := NewExecutorRecordFromInterface([]interface{}{int64(10)}, recordMeta)
	stmt, err := sqlparser.Parse("insert into products (price) values (42) on duplicate key update price = PRICE")
	require.NoError(t, err)
	insertStmt, ok := stmt.(*sqlparser.Insert)
	require.True(t, ok)

	value, err := (&InsertOperator{}).evaluateOnDupExpr(insertStmt.OnDup[0].Expr, existing, map[string]interface{}{"price": int64(42)}, schema)
	require.NoError(t, err)
	require.Equal(t, int64(42), value)
}

func TestLegacyOnDuplicateResolvesLegacyValuesFunctionCaseInsensitively(t *testing.T) {
	valuesExpr := &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("values"),
		Exprs: sqlparser.SelectExprs{&sqlparser.AliasedExpr{
			Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("PRICE")},
		}},
	}
	value, err := (&InsertOperator{}).evaluateOnDupExpr(
		valuesExpr,
		nil,
		map[string]interface{}{"price": int64(42)},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int64(42), value)
}

func TestStorageIntegratedDMLSupportsDefaultExpressions(t *testing.T) {
	stmt, err := sqlparser.Parse("insert into products (id, status) values (1, default)")
	require.NoError(t, err)
	insertStmt, ok := stmt.(*sqlparser.Insert)
	require.True(t, ok)

	meta := &metadata.TableMeta{
		Name: "products",
		Columns: []*metadata.ColumnMeta{
			{Name: "id", Type: metadata.TypeBigInt},
			{Name: "status", Type: metadata.TypeVarchar, DefaultValue: "active"},
		},
	}
	dml := NewStorageIntegratedDMLExecutor(nil, nil, nil, nil, nil, nil, nil, nil)
	rows, err := dml.parseInsertData(context.Background(), insertStmt, meta, "app")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "active", rows[0].ColumnValues["status"])

	updated, err := dml.applyUpdateExpressions(
		&InsertRowData{ColumnValues: map[string]interface{}{"status": "paused"}},
		[]*UpdateExpression{{ColumnName: "status", Expr: &sqlparser.Default{}}},
		meta,
	)
	require.NoError(t, err)
	require.Equal(t, "active", updated.ColumnValues["status"])
}
