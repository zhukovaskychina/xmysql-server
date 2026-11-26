package engine

import (
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

// evaluateVectorBlockBinding checks the vector, block, and current source row
// together so callers do not have to reconstruct provenance comparisons.
// VECTOR_VERIFIED_BLOCK_DISTANCE adds an exact distance after those checks.
func evaluateVectorBlockBinding(function *sqlparser.FuncExpr, values map[string]interface{}, distance bool) (interface{}, error) {
	minimum, maximum := 3, 3
	if distance {
		minimum, maximum = 4, 5
	}
	if function == nil || len(function.Exprs) < minimum || len(function.Exprs) > maximum {
		if distance {
			return nil, fmt.Errorf("VECTOR_VERIFIED_BLOCK_DISTANCE requires stored and query vectors, a CONTENT_BLOCK column, a source CONTENT column, and an optional metric")
		}
		return nil, fmt.Errorf("VECTOR_MATCHES_BLOCK requires a VECTOR, CONTENT_BLOCK column and source CONTENT column")
	}
	arguments := make([]interface{}, len(function.Exprs))
	for index, expression := range function.Exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("%s arguments must be scalar expressions", function.Name.String())
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, values)
		if err != nil {
			return nil, err
		}
		arguments[index] = value
	}
	if arguments[0] == nil {
		if distance {
			return nil, nil
		}
		return false, nil
	}
	stored, ok := vectorArgument(arguments[0])
	if !ok {
		return nil, fmt.Errorf("%s first argument must be VECTOR; got %T", function.Name.String(), arguments[0])
	}
	blockIndex, sourceIndex := 1, 2
	var query vector.Value
	if distance {
		if arguments[1] == nil {
			return nil, nil
		}
		query, ok = vectorArgument(arguments[1])
		if !ok {
			return nil, fmt.Errorf("VECTOR_VERIFIED_BLOCK_DISTANCE second argument must be VECTOR; got %T", arguments[1])
		}
		blockIndex, sourceIndex = 2, 3
	}
	blockColumn, ok := function.Exprs[blockIndex].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("%s block must be a CONTENT_BLOCK column", function.Name.String())
	}
	if _, ok := blockColumn.Expr.(*sqlparser.ColName); !ok {
		return nil, fmt.Errorf("%s block must be a CONTENT_BLOCK column", function.Name.String())
	}
	block, isNull, err := evaluateContentBlockArgument(&sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent(function.Name.String()), Exprs: sqlparser.SelectExprs{blockColumn},
	}, values)
	if err != nil {
		return nil, err
	}
	if isNull || block.State != content.BlockStateReady {
		if distance {
			return nil, nil
		}
		return false, nil
	}
	sourceColumnExpr, ok := function.Exprs[sourceIndex].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("%s source must be a CONTENT column", function.Name.String())
	}
	sourceColumn, ok := sourceColumnExpr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("%s source must be a CONTENT column", function.Name.String())
	}
	sourceValue := arguments[sourceIndex]
	if sourceValue == nil {
		if distance {
			return nil, nil
		}
		return false, nil
	}
	var typedSource content.Value
	switch typed := sourceValue.(type) {
	case content.Value:
		typedSource = typed
	case *content.Value:
		if typed == nil {
			if distance {
				return nil, nil
			}
			return false, nil
		}
		typedSource = *typed
	default:
		return nil, fmt.Errorf("%s source must be typed CONTENT; got %T", function.Name.String(), sourceValue)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("%s requires stored-row context for the source column", function.Name.String())
	}
	actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, typedSource)
	if err != nil {
		return nil, fmt.Errorf("%s source verification failed: %w", function.Name.String(), err)
	}
	matches := stored.State == vector.StateReady && stored.Source != nil && stored.Source.Matches(actualSource) &&
		stored.SourceBlockID != "" && strings.EqualFold(stored.SourceBlockID, block.ID) && block.Source.Matches(actualSource)
	if !matches {
		if distance {
			return nil, nil
		}
		return false, nil
	}
	if !distance {
		return true, nil
	}
	metric := vector.MetricCosine
	if len(function.Exprs) == 5 {
		metricName, err := vectorStringArgument(arguments[4], "metric")
		if err != nil {
			return nil, err
		}
		metric = vector.Metric(strings.ToUpper(strings.TrimSpace(metricName)))
	}
	return vector.Distance(stored, query, metric)
}
