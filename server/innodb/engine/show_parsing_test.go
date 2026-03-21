package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestExtractShowColumnsTableNameFromQuery(t *testing.T) {
	assert.Equal(t, "users", extractShowColumnsTableNameFromQuery("show columns from users"))
	assert.Equal(t, "users", extractShowColumnsTableNameFromQuery("show fields in `testdb`.`users`;"))
	assert.Equal(t, "users", extractShowColumnsTableNameFromQuery("show full columns from `testdb`.`users`;"))
}

func TestNormalizeShowTargetName(t *testing.T) {
	assert.Equal(t, "users", normalizeShowTargetName("users"))
	assert.Equal(t, "users", normalizeShowTargetName("`users`"))
	assert.Equal(t, "users", normalizeShowTargetName("`testdb`.`users`;"))
}

func TestExtractShowTablesLikePatternFromQuery(t *testing.T) {
	assert.Equal(t, "user%", extractShowTablesLikePatternFromQuery("show tables like 'user%'"))
	assert.Equal(t, "a_%", extractShowTablesLikePatternFromQuery("show tables from testdb like 'a_%';"))
	assert.Equal(t, "", extractShowTablesLikePatternFromQuery("show tables"))
}

func TestExtractShowDatabasesLikePatternFromQuery(t *testing.T) {
	assert.Equal(t, "app%", extractShowDatabasesLikePatternFromQuery("show databases like 'app%'"))
	assert.Equal(t, "a_%", extractShowDatabasesLikePatternFromQuery("show databases like 'a_%';"))
	assert.Equal(t, "", extractShowDatabasesLikePatternFromQuery("show databases"))
}

func TestExtractShowVariablesLikePatternFromQuery(t *testing.T) {
	assert.Equal(t, "version%", extractShowVariablesLikePatternFromQuery("show variables like 'version%'"))
	assert.Equal(t, "char%", extractShowVariablesLikePatternFromQuery("show global variables like 'char%'"))
	assert.Equal(t, "sql_%", extractShowVariablesLikePatternFromQuery("show session variables like 'sql_%';"))
	assert.Equal(t, "", extractShowVariablesLikePatternFromQuery("show variables"))
}

func TestExtractShowStatusLikePatternFromQuery(t *testing.T) {
	assert.Equal(t, "Up%", extractShowStatusLikePatternFromQuery("show status like 'Up%'"))
	assert.Equal(t, "Thr%", extractShowStatusLikePatternFromQuery("show global status like 'Thr%'"))
	assert.Equal(t, "Com_%", extractShowStatusLikePatternFromQuery("show session status like 'Com_%';"))
	assert.Equal(t, "", extractShowStatusLikePatternFromQuery("show status"))
}

func TestExportedShowParsingHelpers(t *testing.T) {
	stmt := &sqlparser.Show{
		Type: "variables",
		Filter: &sqlparser.ShowFilter{
			Like: "ver%",
			Filter: &sqlparser.ComparisonExpr{
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Variable_name")},
				Operator: sqlparser.EqualStr,
				Right:    sqlparser.NewStrVal([]byte("version")),
			},
		},
	}
	assert.Equal(t, "ver%", ExtractShowLikePatternFromStmt(stmt))
	assert.Equal(t, "version%", ExtractShowVariablesLikePatternFromQuery("show variables like 'version%'"))
	assert.Equal(t, "Up%", ExtractShowStatusLikePatternFromQuery("show status like 'Up%'"))
	assert.Equal(t, "Variable_name = 'version'", sqlparser.String(ExtractShowWhereExprFromStmt(stmt)))
}

func TestExtractShowTablesWhereExprFromQuery(t *testing.T) {
	expr := extractShowTablesWhereExprFromQuery("show tables where 1 = 0")
	assert.NotNil(t, expr)
	assert.Equal(t, "1 = 0", sqlparser.String(expr))
}

func TestResolveShowLikePattern(t *testing.T) {
	tableStmt := &sqlparser.Show{
		Type: "tables",
		ShowTablesOpt: &sqlparser.ShowTablesOpt{
			Filter: &sqlparser.ShowFilter{Like: "user%"},
		},
	}
	assert.Equal(t, "user%", ResolveShowLikePattern("tables", tableStmt, ""))
	assert.Equal(t, "app%", ResolveShowLikePattern("databases", nil, "show databases like 'app%'"))
	assert.Equal(t, "version%", ResolveShowLikePattern("variables", nil, "show variables like 'version%'"))
}

func TestResolveShowWhereExpr(t *testing.T) {
	stmt := &sqlparser.Show{
		Type: "variables",
		Filter: &sqlparser.ShowFilter{
			Filter: &sqlparser.ComparisonExpr{
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("Variable_name")},
				Operator: sqlparser.EqualStr,
				Right:    sqlparser.NewStrVal([]byte("version")),
			},
		},
	}
	assert.Equal(t, "Variable_name = 'version'", sqlparser.String(ResolveShowWhereExpr("variables", stmt, "")))
	assert.Equal(t, "1 = 0", sqlparser.String(ResolveShowWhereExpr("tables", nil, "show tables where 1 = 0")))
}
