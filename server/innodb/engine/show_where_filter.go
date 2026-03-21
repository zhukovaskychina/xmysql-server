package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func filterShowRowsByWhere(rows [][]interface{}, columns []string, where sqlparser.Expr) [][]interface{} {
	if where == nil {
		return rows
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		matched, err := evalShowWhereExpr(where, row, columns)
		if err != nil {
			continue
		}
		if matched {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func FilterShowRowsByWhere(rows [][]interface{}, columns []string, where sqlparser.Expr) [][]interface{} {
	return filterShowRowsByWhere(rows, columns, where)
}

func evalShowWhereExpr(expr sqlparser.Expr, row []interface{}, columns []string) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := evalShowWhereExpr(e.Left, row, columns)
		if err != nil {
			return false, err
		}
		right, err := evalShowWhereExpr(e.Right, row, columns)
		if err != nil {
			return false, err
		}
		return left && right, nil
	case *sqlparser.OrExpr:
		left, err := evalShowWhereExpr(e.Left, row, columns)
		if err != nil {
			return false, err
		}
		right, err := evalShowWhereExpr(e.Right, row, columns)
		if err != nil {
			return false, err
		}
		return left || right, nil
	case *sqlparser.ParenExpr:
		return evalShowWhereExpr(e.Expr, row, columns)
	case *sqlparser.NotExpr:
		result, err := evalShowWhereExpr(e.Expr, row, columns)
		if err != nil {
			return false, err
		}
		return !result, nil
	case *sqlparser.ComparisonExpr:
		return evalShowWhereComparison(e, row, columns)
	case *sqlparser.IsExpr:
		value, err := evalShowWhereValue(e.Expr, row, columns)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(strings.TrimSpace(e.Operator)) {
		case sqlparser.IsNullStr:
			return value == nil || toShowString(value) == "", nil
		case sqlparser.IsNotNullStr:
			return value != nil && toShowString(value) != "", nil
		case sqlparser.IsTrueStr:
			b, ok := toBool(value)
			return ok && b, nil
		case sqlparser.IsFalseStr:
			b, ok := toBool(value)
			return ok && !b, nil
		case sqlparser.IsNotTrueStr:
			b, ok := toBool(value)
			return ok && !b, nil
		case sqlparser.IsNotFalseStr:
			b, ok := toBool(value)
			return ok && b, nil
		default:
			return false, fmt.Errorf("unsupported SHOW WHERE is-operator: %s", e.Operator)
		}
	default:
		return false, fmt.Errorf("unsupported SHOW WHERE expression")
	}
}

func evalShowWhereComparison(expr *sqlparser.ComparisonExpr, row []interface{}, columns []string) (bool, error) {
	left, err := evalShowWhereValue(expr.Left, row, columns)
	if err != nil {
		return false, err
	}
	right, err := evalShowWhereValue(expr.Right, row, columns)
	if err != nil {
		return false, err
	}
	op := strings.ToLower(strings.TrimSpace(expr.Operator))
	switch op {
	case sqlparser.EqualStr:
		return compareShowValues(left, right) == 0, nil
	case sqlparser.NotEqualStr:
		return compareShowValues(left, right) != 0, nil
	case sqlparser.LessThanStr:
		return compareShowValues(left, right) < 0, nil
	case sqlparser.LessEqualStr:
		return compareShowValues(left, right) <= 0, nil
	case sqlparser.GreaterThanStr:
		return compareShowValues(left, right) > 0, nil
	case sqlparser.GreaterEqualStr:
		return compareShowValues(left, right) >= 0, nil
	case sqlparser.LikeStr:
		return sqlLikeMatch(toShowString(left), toShowString(right)), nil
	case sqlparser.NotLikeStr:
		return !sqlLikeMatch(toShowString(left), toShowString(right)), nil
	default:
		return false, fmt.Errorf("unsupported SHOW WHERE operator: %s", expr.Operator)
	}
}

func evalShowWhereValue(expr sqlparser.Expr, row []interface{}, columns []string) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return getShowColumnValue(e.Name.String(), row, columns), nil
	case *sqlparser.SQLVal:
		return showSQLValToValue(e), nil
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		return bool(e), nil
	default:
		return nil, fmt.Errorf("unsupported SHOW WHERE value")
	}
}

func getShowColumnValue(name string, row []interface{}, columns []string) interface{} {
	for i, col := range columns {
		if strings.EqualFold(col, name) && i < len(row) {
			return row[i]
		}
	}
	return nil
}

func showSQLValToValue(v *sqlparser.SQLVal) interface{} {
	raw := string(v.Val)
	switch v.Type {
	case sqlparser.IntVal, sqlparser.FloatVal, sqlparser.HexNum:
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return f
		}
		return raw
	default:
		return raw
	}
}

func compareShowValues(left, right interface{}) int {
	lf, lok := toFloat(left)
	rf, rok := toFloat(right)
	if lok && rok {
		switch {
		case lf < rf:
			return -1
		case lf > rf:
			return 1
		default:
			return 0
		}
	}
	ls := strings.ToLower(toShowString(left))
	rs := strings.ToLower(toShowString(right))
	return strings.Compare(ls, rs)
}

func toFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func toShowString(v interface{}) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", v))
}

func toBool(v interface{}) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case int:
		return t != 0, true
	case int64:
		return t != 0, true
	case float64:
		return t != 0, true
	case float32:
		return t != 0, true
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		switch s {
		case "true", "1", "on", "yes":
			return true, true
		case "false", "0", "off", "no", "":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}
