package engine

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

type explainSelectSource struct {
	reference explainTableReference
	info      *persistedTableInfo
}

type explainPredicate struct {
	leftColumn  *sqlparser.ColName
	rightColumn *sqlparser.ColName
	operator    string
}

type explainAccessCondition struct {
	equality bool
	join     bool
	rangeOp  bool
}

func explainSelectSubqueries(statement *sqlparser.Select) []*plan.SubqueryExpression {
	if statement == nil {
		return nil
	}
	logical, err := plan.BuildLogicalPlan(statement, &InfoSchemaAdapter{})
	if err != nil {
		return nil
	}
	return logicalPlanSubqueryExpressions(logical)
}

func explainSelectHasOnlyBaseTables(statement *sqlparser.Select) bool {
	if statement == nil {
		return false
	}
	var supports func(sqlparser.TableExpr) bool
	supports = func(expression sqlparser.TableExpr) bool {
		switch value := expression.(type) {
		case *sqlparser.AliasedTableExpr:
			_, ok := value.Expr.(sqlparser.TableName)
			return ok
		case *sqlparser.JoinTableExpr:
			return supports(value.LeftExpr) && supports(value.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, child := range value.Exprs {
				if !supports(child) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	for _, expression := range statement.From {
		if !supports(expression) {
			return false
		}
	}
	return true
}

func (e *XMySQLExecutor) explainSelectTableRows(ctx *ExecutionContext, statement *sqlparser.Select, defaultSchema string, subqueries []*plan.SubqueryExpression) ([][]interface{}, error) {
	references := explainTableReferences(statement)
	sources := make([]explainSelectSource, 0, len(references))
	for _, reference := range references {
		schemaName := reference.schema
		if schemaName == "" {
			schemaName = defaultSchema
		}
		info, err := e.readPersistedTableInfo(schemaName, reference.name)
		if err != nil {
			return nil, err
		}
		reference.schema = schemaName
		sources = append(sources, explainSelectSource{reference: reference, info: info})
	}
	conditions := explainSelectPredicates(statement)
	access := explainAccessConditions(conditions, sources)

	selectType := "SIMPLE"
	if len(subqueries) > 0 {
		selectType = "PRIMARY"
	}
	if len(sources) == 0 {
		return [][]interface{}{{int64(1), selectType, nil, nil, "NULL", nil, nil, nil, nil, int64(1), float64(100), "No tables used"}}, nil
	}
	rows := make([][]interface{}, 0, len(sources))
	for sourceIndex, source := range sources {
		possibleKeys, selectedKey, accessType, estimate := explainSourceAccess(source, access[sourceIndex], ctx, e)
		var partitions interface{}
		if partitionRows := e.partitionRowsForTable(source.reference.schema, source.reference.name); len(partitionRows) > 0 {
			names := make([]string, 0, len(partitionRows))
			for _, partition := range partitionRows {
				names = append(names, fmt.Sprint(partition["PARTITION_NAME"]))
			}
			partitions = strings.Join(names, ",")
		}
		var selectedName interface{}
		if selectedKey != nil {
			selectedName = selectedKey.name
		}
		var keyLength interface{}
		if selectedKey != nil {
			if length, ok := persistedIndexKeyLength(source.info, selectedKey, access[sourceIndex]); ok {
				keyLength = length
			}
		}
		extra := ""
		if statement.Where != nil {
			extra = "Using where"
		}
		// MySQL reports final sort/materialization work on the last table row.
		if sourceIndex == len(sources)-1 {
			if len(statement.GroupBy) > 0 {
				extra = appendExplainExtra(extra, "Using temporary")
			}
			if len(statement.OrderBy) > 0 {
				extra = appendExplainExtra(extra, "Using filesort")
			}
		}
		rows = append(rows, []interface{}{int64(1), selectType, source.reference.label, partitions, accessType, nullIfEmpty(possibleKeys), selectedName, keyLength, nil, estimate, float64(100), extra})
	}
	return rows, nil
}

type explainIndexChoice struct {
	name       string
	columns    []string
	unique     bool
	uniqueJoin bool
	condition  explainAccessCondition
	indexOrder int
}

func explainSourceAccess(source explainSelectSource, conditions map[string]explainAccessCondition, ctx *ExecutionContext, executor *XMySQLExecutor) (string, *explainIndexChoice, string, int64) {
	possible := make([]string, 0)
	choices := make([]explainIndexChoice, 0)
	for indexOrder, index := range source.info.Indexes {
		name, _ := index["name"].(string)
		columns := explainPersistedIndexColumns(index["columns"])
		if name == "" || len(columns) == 0 {
			continue
		}
		if visible, ok := index["visible"].(bool); ok && !visible {
			continue
		}
		condition, matches := conditions[strings.ToLower(columns[0])]
		if !matches {
			continue
		}
		indexType := strings.ToLower(persistedString(index["type"]))
		if indexType == "fulltext" || indexType == "spatial" || indexType == "rtree" || indexType == "hash" && !condition.equality {
			continue
		}
		possible = append(possible, name)
		unique, _ := index["unique"].(bool)
		primary, _ := index["primary"].(bool)
		completeUniqueEquality := unique || primary
		uniqueJoin := false
		if completeUniqueEquality {
			for _, column := range columns {
				partCondition, constrained := conditions[strings.ToLower(column)]
				if !constrained || !partCondition.equality {
					completeUniqueEquality = false
					break
				}
				uniqueJoin = uniqueJoin || partCondition.join
			}
		}
		choices = append(choices, explainIndexChoice{name: name, columns: columns, unique: completeUniqueEquality, uniqueJoin: uniqueJoin, condition: condition, indexOrder: indexOrder})
	}

	rowCount := int64(0)
	if source.info.Stats != nil {
		rowCount = int64(source.info.Stats.RowCount)
	} else if executor != nil {
		rowCount, _ = executor.tableRowCount(ctx, source.reference.schema, source.reference.name)
	}
	if rowCount < 1 {
		rowCount = 1
	}
	if len(choices) == 0 {
		return strings.Join(possible, ","), nil, "ALL", rowCount
	}
	sort.SliceStable(choices, func(i, j int) bool {
		left, right := choices[i], choices[j]
		if left.condition.equality != right.condition.equality {
			return left.condition.equality
		}
		if left.unique != right.unique {
			return left.unique
		}
		return left.indexOrder < right.indexOrder
	})
	selected := &choices[0]
	accessType := "range"
	if selected.condition.equality {
		if selected.condition.join || selected.uniqueJoin {
			accessType = "ref"
			if selected.unique {
				accessType = "eq_ref"
			}
		} else {
			accessType = "ref"
			if selected.unique {
				accessType = "const"
			}
		}
	}
	if selected.condition.equality && selected.unique {
		rowCount = 1
	} else if selected.condition.equality && source.info.Stats != nil && len(selected.columns) == 1 {
		if stat, ok := source.info.Stats.IndexStats[selected.name]; ok && stat.DistinctCount > 0 {
			rowCount = int64(math.Ceil(float64(rowCount) / float64(stat.DistinctCount)))
			if rowCount < 1 {
				rowCount = 1
			}
		}
	}
	return strings.Join(possible, ","), selected, accessType, rowCount
}

func explainPersistedIndexColumns(value interface{}) []string {
	raw, ok := value.([]interface{})
	if !ok {
		return nil
	}
	columns := make([]string, 0, len(raw))
	for _, column := range raw {
		name := strings.TrimSpace(fmt.Sprint(column))
		if name != "" && name != "<nil>" {
			columns = append(columns, name)
		}
	}
	return columns
}

func persistedIndexKeyLength(info *persistedTableInfo, index *explainIndexChoice, conditions map[string]explainAccessCondition) (int64, bool) {
	if info == nil || index == nil || len(index.columns) == 0 {
		return 0, false
	}
	total := int64(0)
	for _, indexColumn := range index.columns {
		if _, constrained := conditions[strings.ToLower(indexColumn)]; !constrained {
			break
		}
		found := false
		for _, column := range info.Columns {
			name, _ := column["name"].(string)
			if !strings.EqualFold(name, indexColumn) {
				continue
			}
			length := persistedNumber(column["length"])
			scale := persistedNumber(column["scale"])
			columnType, _ := column["type"].(string)
			collation := persistedString(column["collate"])
			if collation == "" && info.Options != nil {
				collation = persistedString(info.Options["collation"])
			}
			if collation == "" {
				charset := persistedString(column["charset"])
				if charset == "" && info.Options != nil {
					charset = persistedString(info.Options["charset"])
				}
				if charset == "" && informationSchemaCharacterSet(columnType) != nil {
					charset = fmt.Sprint(informationSchemaCharacterSet(columnType))
				}
				if charset != "" {
					collation = persistedString(informationSchemaCollationForCharacterSet(charset))
				}
			}
			keyLength, known := mysqlIndexColumnLength(strings.ToLower(columnType), int64(length), int64(scale), collation)
			if !known {
				return 0, false
			}
			if nullable, ok := column["nullable"].(bool); ok && nullable {
				keyLength++
			}
			total += keyLength
			found = true
			break
		}
		if !found {
			return 0, false
		}
	}
	if total == 0 {
		return 0, false
	}
	return total, true
}

func mysqlIndexColumnLength(columnType string, declaredLength, scale int64, collation string) (int64, bool) {
	switch columnType {
	case "tinyint", "bool", "boolean":
		return 1, true
	case "smallint":
		return 2, true
	case "mediumint":
		return 3, true
	case "int", "integer", "float":
		return 4, true
	case "bigint", "double", "real":
		return 8, true
	case "date":
		return 3, true
	case "time", "datetime", "timestamp":
		base := int64(3)
		if columnType == "datetime" {
			base = 5
		} else if columnType == "timestamp" {
			base = 4
		}
		fractionalBytes := int64(0)
		if scale > 0 {
			switch {
			case scale <= 2:
				fractionalBytes = 1
			case scale <= 4:
				fractionalBytes = 2
			default:
				fractionalBytes = 3
			}
		}
		return base + fractionalBytes, true
	case "year":
		return 1, true
	case "char", "varchar", "binary", "varbinary":
		if declaredLength < 1 {
			return 0, false
		}
		width := int64(1)
		if strings.Contains(columnType, "char") && columnType != "binary" && columnType != "varbinary" {
			normalizedCollation := strings.ToLower(collation)
			switch {
			case strings.HasPrefix(normalizedCollation, "utf8mb4_"):
				width = 4
			case strings.HasPrefix(normalizedCollation, "utf8_") || strings.HasPrefix(normalizedCollation, "utf8mb3_"):
				width = 3
			case strings.HasPrefix(normalizedCollation, "ucs2_"):
				width = 2
			case strings.HasPrefix(normalizedCollation, "utf16_") || strings.HasPrefix(normalizedCollation, "utf32_"):
				width = 4
			case strings.HasPrefix(normalizedCollation, "gbk_") || strings.HasPrefix(normalizedCollation, "big5_") || strings.HasPrefix(normalizedCollation, "sjis_") || strings.HasPrefix(normalizedCollation, "ujis_"):
				width = 2
			case normalizedCollation == "binary" || strings.HasPrefix(normalizedCollation, "latin1_") || strings.HasPrefix(normalizedCollation, "ascii_") || strings.HasPrefix(normalizedCollation, "binary_"):
				width = 1
			default:
				return 0, false
			}
		}
		keyLength := declaredLength * width
		if columnType == "varchar" || columnType == "varbinary" {
			if keyLength > 255 {
				keyLength += 2
			} else {
				keyLength++
			}
		}
		return keyLength, true
	default:
		return 0, false
	}
}

func explainSelectPredicates(statement *sqlparser.Select) []explainPredicate {
	var predicates []explainPredicate
	var appendExpression func(sqlparser.Expr)
	appendExpression = func(expression sqlparser.Expr) {
		switch value := expression.(type) {
		case *sqlparser.AndExpr:
			appendExpression(value.Left)
			appendExpression(value.Right)
		case *sqlparser.ParenExpr:
			appendExpression(value.Expr)
		case *sqlparser.ComparisonExpr:
			operator := strings.ToLower(strings.TrimSpace(value.Operator))
			switch operator {
			case sqlparser.EqualStr, sqlparser.NullSafeEqualStr, sqlparser.LessThanStr, sqlparser.LessEqualStr, sqlparser.GreaterThanStr, sqlparser.GreaterEqualStr, sqlparser.InStr:
				left, _ := value.Left.(*sqlparser.ColName)
				right, _ := value.Right.(*sqlparser.ColName)
				if left != nil && right == nil && explainExpressionHasColumn(value.Right) || right != nil && left == nil && explainExpressionHasColumn(value.Left) {
					return
				}
				if left != nil || right != nil {
					predicates = append(predicates, explainPredicate{leftColumn: left, rightColumn: right, operator: operator})
				}
			}
		case *sqlparser.RangeCond:
			if column, ok := value.Left.(*sqlparser.ColName); ok && !strings.EqualFold(value.Operator, sqlparser.NotBetweenStr) && !explainExpressionHasColumn(value.From) && !explainExpressionHasColumn(value.To) {
				predicates = append(predicates, explainPredicate{leftColumn: column, operator: "range"})
			}
		case *sqlparser.IsExpr:
			if column, ok := value.Expr.(*sqlparser.ColName); ok && strings.EqualFold(value.Operator, sqlparser.IsNullStr) {
				predicates = append(predicates, explainPredicate{leftColumn: column, operator: sqlparser.EqualStr})
			}
		}
	}
	if statement != nil {
		if statement.Where != nil {
			appendExpression(statement.Where.Expr)
		}
		var visitTable func(sqlparser.TableExpr)
		visitTable = func(expression sqlparser.TableExpr) {
			switch value := expression.(type) {
			case *sqlparser.JoinTableExpr:
				appendExpression(value.Condition.On)
				visitTable(value.LeftExpr)
				visitTable(value.RightExpr)
			case *sqlparser.ParenTableExpr:
				for _, child := range value.Exprs {
					visitTable(child)
				}
			}
		}
		for _, expression := range statement.From {
			visitTable(expression)
		}
	}
	return predicates
}

func explainExpressionHasColumn(expression sqlparser.Expr) bool {
	if expression == nil {
		return false
	}
	hasColumn := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.ColName); ok {
			hasColumn = true
			return false, nil
		}
		return !hasColumn, nil
	}, expression)
	return hasColumn
}

func explainAccessConditions(predicates []explainPredicate, sources []explainSelectSource) []map[string]explainAccessCondition {
	conditions := make([]map[string]explainAccessCondition, len(sources))
	for index := range conditions {
		conditions[index] = make(map[string]explainAccessCondition)
	}
	for _, predicate := range predicates {
		if predicate.leftColumn == nil && predicate.rightColumn == nil {
			continue
		}
		leftIndex := explainResolveSourceColumn(predicate.leftColumn, sources)
		rightIndex := explainResolveSourceColumn(predicate.rightColumn, sources)
		if leftIndex >= 0 && leftIndex == rightIndex && predicate.leftColumn != nil && predicate.rightColumn != nil {
			continue
		}
		if leftIndex < 0 && rightIndex < 0 {
			continue
		}
		operator := strings.ToLower(predicate.operator)
		equality := operator == sqlparser.EqualStr || operator == sqlparser.NullSafeEqualStr || operator == sqlparser.InStr
		rangeOp := !equality
		if leftIndex >= 0 && (rightIndex < 0 || rightIndex < leftIndex) {
			columnName := strings.ToLower(predicate.leftColumn.Name.String())
			condition := explainAccessCondition{equality: equality, rangeOp: rangeOp, join: rightIndex >= 0 && rightIndex != leftIndex}
			conditions[leftIndex][columnName] = strongerExplainAccessCondition(conditions[leftIndex][columnName], condition)
		}
		if rightIndex >= 0 && (leftIndex < 0 || leftIndex < rightIndex) {
			columnName := strings.ToLower(predicate.rightColumn.Name.String())
			condition := explainAccessCondition{equality: equality, rangeOp: rangeOp, join: leftIndex >= 0 && leftIndex != rightIndex}
			conditions[rightIndex][columnName] = strongerExplainAccessCondition(conditions[rightIndex][columnName], condition)
		}
	}
	return conditions
}

func strongerExplainAccessCondition(current, candidate explainAccessCondition) explainAccessCondition {
	if candidate.equality && !current.equality {
		return candidate
	}
	if candidate.join && !current.join {
		return candidate
	}
	if candidate.rangeOp && !current.rangeOp {
		return candidate
	}
	return current
}

func explainResolveSourceColumn(column *sqlparser.ColName, sources []explainSelectSource) int {
	if column == nil {
		return -1
	}
	columnName := strings.ToLower(column.Name.String())
	qualifier := strings.ToLower(column.Qualifier.Name.String())
	if qualifier != "" {
		for index, source := range sources {
			if qualifier == strings.ToLower(source.reference.label) || qualifier == strings.ToLower(source.reference.name) {
				return index
			}
		}
		return -1
	}
	matched := -1
	for index, source := range sources {
		for _, metadataColumn := range source.info.Columns {
			name, _ := metadataColumn["name"].(string)
			if strings.EqualFold(name, columnName) {
				if matched >= 0 {
					return -1
				}
				matched = index
				break
			}
		}
	}
	return matched
}
