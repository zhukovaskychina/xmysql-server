package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestSqlLikeMatch(t *testing.T) {
	assert.True(t, sqlLikeMatch("users", "user%"))
	assert.True(t, sqlLikeMatch("users", "u_ers"))
	assert.True(t, sqlLikeMatch("Users", "user%"))
	assert.False(t, sqlLikeMatch("orders", "user%"))
}

func TestFilterShowRowsByLike(t *testing.T) {
	rows := [][]interface{}{
		{"users"},
		{"orders"},
		{"audit_logs"},
	}

	filtered := filterShowRowsByLike(rows, "u%")

	assert.Len(t, filtered, 1)
	assert.Equal(t, "users", filtered[0][0])
}

func TestFilterShowRowsByWhereEqual(t *testing.T) {
	rows := [][]interface{}{
		{"version", "8.0.0-xmysql"},
		{"version_comment", "XMySQL Server"},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.ComparisonExpr{
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Variable_name")},
		Operator: sqlparser.EqualStr,
		Right:    sqlparser.NewStrVal([]byte("version")),
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "version", filtered[0][0])
}

func TestFilterShowRowsByWhereAndLike(t *testing.T) {
	rows := [][]interface{}{
		{"Threads_connected", "5"},
		{"Threads_running", "2"},
		{"Questions", "50000"},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.AndExpr{
		Left: &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Variable_name")},
			Operator: sqlparser.LikeStr,
			Right:    sqlparser.NewStrVal([]byte("Threads%")),
		},
		Right: &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Value")},
			Operator: sqlparser.GreaterThanStr,
			Right:    sqlparser.NewIntVal([]byte("3")),
		},
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "Threads_connected", filtered[0][0])
}

func TestFilterShowRowsByWhereNotExpr(t *testing.T) {
	rows := [][]interface{}{
		{"users"},
		{"orders"},
	}
	columns := []string{"Tables_in_testdb"}
	where := &sqlparser.NotExpr{
		Expr: &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Tables_in_testdb")},
			Operator: sqlparser.EqualStr,
			Right:    sqlparser.NewStrVal([]byte("users")),
		},
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "orders", filtered[0][0])
}

func TestFilterShowRowsByWhereIsNotNull(t *testing.T) {
	rows := [][]interface{}{
		{"version", "8.0.0-xmysql"},
		{"version_comment", nil},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.IsExpr{
		Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Value")},
		Operator: sqlparser.IsNotNullStr,
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "version", filtered[0][0])
}

func TestFilterShowRowsByWhereIsTrue(t *testing.T) {
	rows := [][]interface{}{
		{"feature_a", true},
		{"feature_b", false},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.IsExpr{
		Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Value")},
		Operator: sqlparser.IsTrueStr,
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "feature_a", filtered[0][0])
}

func TestFilterShowRowsByWhereIsFalse(t *testing.T) {
	rows := [][]interface{}{
		{"feature_a", true},
		{"feature_b", false},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.IsExpr{
		Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Value")},
		Operator: sqlparser.IsFalseStr,
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "feature_b", filtered[0][0])
}

func TestFilterShowRowsByWhereIsNotTrue(t *testing.T) {
	rows := [][]interface{}{
		{"feature_a", true},
		{"feature_b", false},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.IsExpr{
		Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Value")},
		Operator: sqlparser.IsNotTrueStr,
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "feature_b", filtered[0][0])
}

func TestFilterShowRowsByWhereIsNotFalse(t *testing.T) {
	rows := [][]interface{}{
		{"feature_a", true},
		{"feature_b", false},
	}
	columns := []string{"Variable_name", "Value"}
	where := &sqlparser.IsExpr{
		Expr:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Value")},
		Operator: sqlparser.IsNotFalseStr,
	}

	filtered := filterShowRowsByWhere(rows, columns, where)

	assert.Len(t, filtered, 1)
	assert.Equal(t, "feature_a", filtered[0][0])
}
