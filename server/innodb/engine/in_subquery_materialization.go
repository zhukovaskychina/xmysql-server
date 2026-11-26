package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// Replace complete IN comparison nodes. Replacing text from the last token
// of its left operand corrupts arithmetic, quoting and boolean composition.
func (e *XMySQLExecutor) materializeInSubqueryAST(ctx *ExecutionContext, query, databaseName string) (string, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return "", nil
	} // Other compatibility parsers may own this syntax.
	joinPredicates := make(map[*sqlparser.ComparisonExpr]bool)
	joinProjectionPredicates := make(map[*sqlparser.ComparisonExpr]bool)
	joinWherePredicates := make(map[*sqlparser.ComparisonExpr]bool)
	havingPredicates := make(map[*sqlparser.ComparisonExpr]bool)
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if selectStatement, ok := node.(*sqlparser.Select); ok {
			if selectStatement.Having != nil {
				_ = sqlparser.Walk(func(havingNode sqlparser.SQLNode) (bool, error) {
					if comparison, ok := havingNode.(*sqlparser.ComparisonExpr); ok &&
						(comparison.Operator == sqlparser.InStr || comparison.Operator == sqlparser.NotInStr) {
						if _, isSubquery := comparison.Right.(*sqlparser.Subquery); isSubquery {
							havingPredicates[comparison] = true
						}
					}
					return true, nil
				}, selectStatement.Having.Expr)
			}
			if !selectHasJoin(selectStatement) {
				return true, nil
			}
			for _, selectExpr := range selectStatement.SelectExprs {
				projection, ok := selectExpr.(*sqlparser.AliasedExpr)
				if !ok || !booleanSQLExpression(projection.Expr) || !expressionHasSubquery(projection.Expr) {
					continue
				}
				_ = sqlparser.Walk(func(projectionNode sqlparser.SQLNode) (bool, error) {
					comparison, ok := projectionNode.(*sqlparser.ComparisonExpr)
					if ok && (comparison.Operator == sqlparser.InStr || comparison.Operator == sqlparser.NotInStr) {
						if _, isSubquery := comparison.Right.(*sqlparser.Subquery); isSubquery {
							joinProjectionPredicates[comparison] = true
						}
					}
					return true, nil
				}, projection.Expr)
			}
			if selectStatement.Where != nil {
				_ = sqlparser.Walk(func(whereNode sqlparser.SQLNode) (bool, error) {
					comparison, ok := whereNode.(*sqlparser.ComparisonExpr)
					if ok && (comparison.Operator == sqlparser.InStr || comparison.Operator == sqlparser.NotInStr) {
						if _, isSubquery := comparison.Right.(*sqlparser.Subquery); isSubquery {
							joinWherePredicates[comparison] = true
						}
					}
					return true, nil
				}, selectStatement.Where.Expr)
			}
		}
		if join, ok := node.(*sqlparser.JoinTableExpr); ok && join.Condition.On != nil {
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
				if comparison, ok := node.(*sqlparser.ComparisonExpr); ok {
					joinPredicates[comparison] = true
				}
				return true, nil
			}, join.Condition.On)
		}
		return true, nil
	}, statement)
	changed := false
	var materialize func(sqlparser.SQLNode) error
	materialize = func(root sqlparser.SQLNode) error {
		return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			comparison, ok := node.(*sqlparser.ComparisonExpr)
			if !ok || (comparison.Operator != sqlparser.InStr && comparison.Operator != sqlparser.NotInStr) {
				return true, nil
			}
			subquery, hasSubquery := comparison.Right.(*sqlparser.Subquery)
			if !hasSubquery {
				return true, nil
			}
			if joinPredicates[comparison] {
				return false, nil
			} // Evaluated against each joined candidate.
			if joinProjectionPredicates[comparison] {
				return false, nil
			} // Evaluated once per projected outer row.
			if joinWherePredicates[comparison] {
				if _, independent := e.independentINColumns(subquery.Select, databaseName); !independent {
					return false, nil
				}
			} // Correlated candidates are evaluated in the joined-row scope.
			if havingPredicates[comparison] {
				if _, independent := e.independentINColumns(subquery.Select, databaseName); !independent {
					// Keep correlated HAVING predicates in the AST. Grouped SELECT
					// execution binds each completed group and evaluates IN/NOT IN
					// through its set-subquery evaluator, preserving correlation and
					// SQL's NULL/UNKNOWN behavior. Materializing here would execute
					// the inner query without the group row in scope.
					return false, nil
				}
			}
			if _, independent := e.independentINColumns(subquery.Select, databaseName); !independent {
				return false, common.NewErr(common.ErrNotSupportedYet, "correlated or unbound IN subquery materialization in this clause")
			}
			if err := materialize(subquery.Select); err != nil {
				return false, err
			}
			result, err := e.executeDerivedInnerSelect(ctx, subquery.Select, databaseName)
			if err != nil {
				return false, err
			}
			if len(result.Columns) != 1 {
				return false, fmt.Errorf("subquery returns more than one column")
			}
			if len(result.Records) == 0 {
				truth := "0"
				if comparison.Operator == sqlparser.NotInStr {
					truth = "1"
				}
				comparison.Left = sqlparser.NewIntVal([]byte("1"))
				comparison.Operator = sqlparser.EqualStr
				comparison.Right = sqlparser.NewIntVal([]byte(truth))
			} else {
				values := make(sqlparser.ValTuple, 0, len(result.Records))
				for _, record := range result.Records {
					values = append(values, inSubqueryLiteral(record.GetValueByIndex(0), result.ColumnTypes))
				}
				comparison.Right = values
			}
			changed = true
			return true, nil
		}, root)
	}
	if err := materialize(statement); err != nil {
		return "", err
	}
	if !changed {
		return "", nil
	}
	return sqlparser.String(statement), nil
}

func inSubqueryLiteral(value basic.Value, columnTypes []string) sqlparser.Expr {
	if value == nil || value.IsNull() {
		return &sqlparser.NullVal{}
	}
	typ := metadata.DataType(value.Type().String())
	if len(columnTypes) > 0 && strings.TrimSpace(columnTypes[0]) != "" {
		typ = metadata.DataType(strings.ToUpper(columnTypes[0]))
	}
	switch typ {
	case metadata.TypeTinyInt, metadata.TypeSmallInt, metadata.TypeMediumInt, metadata.TypeInt, metadata.TypeBigInt, metadata.TypeYear:
		return sqlparser.NewIntVal([]byte(strconv.FormatInt(value.Int(), 10)))
	case metadata.TypeFloat, metadata.TypeDouble, metadata.TypeDecimal:
		return sqlparser.NewFloatVal([]byte(strconv.FormatFloat(value.Float64(), 'g', -1, 64)))
	default:
		return sqlparser.NewStrVal([]byte(value.String()))
	}
}
