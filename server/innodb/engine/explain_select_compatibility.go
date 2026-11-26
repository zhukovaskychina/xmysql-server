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

func explainSelectHasSupportedSources(statement *sqlparser.Select) bool {
	if statement == nil {
		return false
	}
	var supports func(sqlparser.TableExpr) bool
	supports = func(expression sqlparser.TableExpr) bool {
		switch value := expression.(type) {
		case *sqlparser.AliasedTableExpr:
			switch source := value.Expr.(type) {
			case sqlparser.TableName:
				return true
			case *sqlparser.Subquery:
				return explainSelectStatementSupportsSources(source.Select)
			default:
				return false
			}
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

func explainSelectStatementSupportsSources(statement sqlparser.SelectStatement) bool {
	switch value := statement.(type) {
	case *sqlparser.Select:
		return explainSelectHasSupportedSources(value)
	case *sqlparser.ParenSelect:
		return explainSelectStatementSupportsSources(value.Select)
	case *sqlparser.Union:
		_, ok := explainUnionBranches(value)
		return ok || explainSetOperationPlanShapeSupported(value)
	default:
		return false
	}
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
		if len(statement.From) == 0 {
			return [][]interface{}{{int64(1), selectType, nil, nil, "NULL", nil, nil, nil, nil, int64(1), float64(100), "No tables used"}}, nil
		}
		return nil, nil
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

type explainDerivedTask struct {
	statement sqlparser.SelectStatement
	id        int64
	parentRow []interface{}
}

func (e *XMySQLExecutor) explainSelectPlanRows(ctx *ExecutionContext, statement *sqlparser.Select, defaultSchema string, rootSubqueries []*plan.SubqueryExpression) ([][]interface{}, error) {
	return e.explainSelectPlanRowsAt(ctx, statement, defaultSchema, 1, "", rootSubqueries, 2)
}

func (e *XMySQLExecutor) explainSelectPlanRowsAt(ctx *ExecutionContext, statement *sqlparser.Select, defaultSchema string, rootID int64, rootType string, rootSubqueries []*plan.SubqueryExpression, firstDerivedID int64) ([][]interface{}, error) {
	nextID := firstDerivedID
	return e.explainSelectPlanRowsWithNextID(ctx, statement, defaultSchema, rootID, rootType, rootSubqueries, &nextID)
}

func (e *XMySQLExecutor) explainSelectPlanRowsWithNextID(ctx *ExecutionContext, statement *sqlparser.Select, defaultSchema string, rootID int64, rootType string, rootSubqueries []*plan.SubqueryExpression, nextID *int64) ([][]interface{}, error) {
	tasks := make([]explainDerivedTask, 0)
	allSubqueries := make([]*plan.SubqueryExpression, 0, len(rootSubqueries))
	seenSubqueries := make(map[*plan.SubqueryExpression]struct{}, len(rootSubqueries))
	appendSubqueries := func(subqueries []*plan.SubqueryExpression) {
		for _, subquery := range subqueries {
			if subquery == nil {
				continue
			}
			if _, exists := seenSubqueries[subquery]; exists {
				continue
			}
			seenSubqueries[subquery] = struct{}{}
			allSubqueries = append(allSubqueries, subquery)
		}
	}
	appendSubqueries(rootSubqueries)

	rows, err := e.explainSelectBlockRows(ctx, statement, defaultSchema, rootID, rootType, nextID, &tasks)
	if err != nil {
		return nil, err
	}
	for taskIndex := 0; taskIndex < len(tasks); taskIndex++ {
		task := tasks[taskIndex]
		child := explainSelectStatementAsSelect(task.statement)
		if child == nil {
			if union, ok := explainSelectStatementAsUnion(task.statement); ok {
				childRows, childErr := e.explainUnionPlanRowsAt(ctx, union, defaultSchema, task.id, nextID)
				if childErr != nil {
					return nil, childErr
				}
				rows = append(rows, childRows...)
			}
			continue
		}
		childSubqueries := explainSelectSubqueries(child)
		appendSubqueries(childSubqueries)
		childRows, childErr := e.explainSelectBlockRows(ctx, child, defaultSchema, task.id, "DERIVED", nextID, &tasks)
		if childErr != nil {
			return nil, childErr
		}
		if task.parentRow != nil {
			if estimate, known := explainDerivedRowsEstimate(child, childRows); known {
				task.parentRow[9] = estimate
			}
		}
		rows = append(rows, childRows...)
	}
	if len(allSubqueries) > 0 {
		subqueryRows, subqueryErr := e.explainSubqueryRows(ctx, allSubqueries, defaultSchema, *nextID)
		if subqueryErr != nil {
			return nil, subqueryErr
		}
		rows = append(rows, subqueryRows...)
		*nextID += int64(len(subqueryRows))
	}
	return rows, nil
}

func explainUnionBranches(statement sqlparser.SelectStatement) ([]*sqlparser.Select, bool) {
	if setOperation, ok := statement.(*sqlparser.Union); ok && setOperation != nil {
		operator := explainSetOperationName(setOperation.Type)
		if operator == "INTERSECT" || operator == "EXCEPT" {
			if len(setOperation.OrderBy) > 0 || setOperation.Limit != nil || setOperation.Lock != "" {
				return nil, false
			}
			left, leftOK := setOperation.Left.(*sqlparser.Select)
			right, rightOK := setOperation.Right.(*sqlparser.Select)
			if !leftOK || !rightOK || !explainSelectHasSupportedSources(left) || !explainSelectHasSupportedSources(right) {
				return nil, false
			}
			return []*sqlparser.Select{left, right}, true
		}
		if operator != "UNION" {
			return nil, false
		}
	}
	branches := make([]*sqlparser.Select, 0, 2)
	firstUnion := true
	var appendStatement func(sqlparser.SelectStatement) bool
	appendStatement = func(statement sqlparser.SelectStatement) bool {
		switch value := statement.(type) {
		case *sqlparser.Union:
			if value == nil || explainSetOperationName(value.Type) != "UNION" || !firstUnion && (len(value.OrderBy) > 0 || value.Limit != nil || value.Lock != "") {
				return false
			}
			firstUnion = false
			return appendStatement(value.Left) && appendStatement(value.Right)
		case *sqlparser.Select:
			if value == nil || !explainSelectHasSupportedSources(value) {
				return false
			}
			branches = append(branches, value)
			return true
		case *sqlparser.ParenSelect:
			if value == nil {
				return false
			}
			return appendStatement(value.Select)
		default:
			return false
		}
	}
	if !appendStatement(statement) || len(branches) < 2 {
		return nil, false
	}
	return branches, true
}

func explainSetOperationName(operation string) string {
	fields := strings.Fields(strings.ToUpper(strings.TrimSpace(operation)))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

type explainSetOperationNode struct {
	operator   string
	filesort   bool
	statement  *sqlparser.Select
	left       *explainSetOperationNode
	right      *explainSetOperationNode
	id         int64
	selectType string
	resultID   int64
	rows       [][]interface{}
}

func buildExplainSetOperationTree(statement sqlparser.SelectStatement) (*explainSetOperationNode, bool) {
	switch value := statement.(type) {
	case *sqlparser.Select:
		return &explainSetOperationNode{statement: value}, value != nil
	case *sqlparser.ParenSelect:
		if value == nil {
			return nil, false
		}
		return buildExplainSetOperationTree(value.Select)
	case *sqlparser.Union:
		if value == nil || value.Lock != "" {
			return nil, false
		}
		operator := explainSetOperationName(value.Type)
		if operator != "UNION" && operator != "INTERSECT" && operator != "EXCEPT" {
			return nil, false
		}
		if operator == "UNION" && strings.EqualFold(strings.TrimSpace(value.Type), sqlparser.UnionAllStr) {
			return nil, false
		}
		left, leftOK := buildExplainSetOperationTree(value.Left)
		right, rightOK := buildExplainSetOperationTree(value.Right)
		if !leftOK || !rightOK {
			return nil, false
		}
		return &explainSetOperationNode{operator: operator, filesort: len(value.OrderBy) > 0, left: left, right: right}, true
	default:
		return nil, false
	}
}

func explainSetOperationPlanShapeSupported(statement sqlparser.SelectStatement) bool {
	tree, ok := buildExplainSetOperationTree(statement)
	if !ok || tree == nil || tree.statement != nil {
		return false
	}
	var supported func(*explainSetOperationNode) bool
	supported = func(node *explainSetOperationNode) bool {
		if node == nil {
			return false
		}
		if node.statement != nil {
			return explainSetOperationLeafPlanShapeSupported(node.statement)
		}
		return supported(node.left) && supported(node.right)
	}
	return supported(tree)
}

func explainSetOperationLeafPlanShapeSupported(statement *sqlparser.Select) bool {
	if !explainSelectHasSupportedSources(statement) {
		return false
	}
	if !explainSelectContainsSubquery(statement) {
		return true
	}
	subqueries := explainSelectSubqueries(statement)
	if len(subqueries) == 0 {
		return false
	}
	for _, subquery := range subqueries {
		if subquery == nil {
			return false
		}
		inner, err := parseSelectSQL(subquery.SQL)
		if err != nil || !explainSelectStatementSupportsSources(inner) {
			return false
		}
		innerSelect := explainSelectStatementAsSelect(inner)
		if innerSelect == nil || explainSelectHasDerivedSource(innerSelect) || explainSelectContainsSubquery(innerSelect) || len(explainTableReferences(innerSelect)) == 0 {
			return false
		}
	}
	return true
}

func explainSelectContainsSubquery(statement *sqlparser.Select) bool {
	if statement == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			found = true
			return false, nil
		}
		return !found, nil
	}, statement)
	return found
}

func explainSetOperationLeaves(node *explainSetOperationNode, inheritedType string, leaves *[]*explainSetOperationNode) {
	if node.statement != nil {
		node.selectType = inheritedType
		*leaves = append(*leaves, node)
		return
	}
	leftType := inheritedType
	rightType := node.operator
	explainSetOperationLeaves(node.left, leftType, leaves)
	explainSetOperationLeaves(node.right, rightType, leaves)
}

func assignExplainSetOperationResultIDs(node *explainSetOperationNode, nextID *int64) {
	if node.statement != nil {
		return
	}
	assignExplainSetOperationResultIDs(node.left, nextID)
	assignExplainSetOperationResultIDs(node.right, nextID)
	node.resultID = *nextID
	*nextID++
}

func explainSetOperationOutputID(node *explainSetOperationNode) int64 {
	if node.statement != nil {
		return node.id
	}
	return node.resultID
}

func appendExplainSetOperationLeafRows(node *explainSetOperationNode, rows *[][]interface{}) {
	if node.statement != nil {
		*rows = append(*rows, node.rows...)
		return
	}
	appendExplainSetOperationLeafRows(node.left, rows)
	appendExplainSetOperationLeafRows(node.right, rows)
}

func appendExplainSetOperationResultRows(node *explainSetOperationNode, rows *[][]interface{}) {
	if node.statement != nil {
		return
	}
	appendExplainSetOperationResultRows(node.left, rows)
	appendExplainSetOperationResultRows(node.right, rows)
	leftID, rightID := explainSetOperationOutputID(node.left), explainSetOperationOutputID(node.right)
	extra := "Using temporary"
	if node.filesort {
		extra = appendExplainExtra(extra, "Using filesort")
	}
	*rows = append(*rows, []interface{}{
		node.resultID, node.operator + " RESULT",
		fmt.Sprintf("<%s%d,%d>", strings.ToLower(node.operator), leftID, rightID),
		nil, "ALL", nil, nil, nil, nil, nil, nil, extra,
	})
}

func (e *XMySQLExecutor) explainNestedSetOperationPlanRowsAt(ctx *ExecutionContext, statement sqlparser.SelectStatement, defaultSchema string, firstID int64, nextID *int64) ([][]interface{}, error) {
	tree, ok := buildExplainSetOperationTree(statement)
	if !ok || tree == nil || tree.statement != nil || !explainSetOperationPlanShapeSupported(statement) {
		return nil, fmt.Errorf("EXPLAIN supports nested set operations over base-table SELECT leaves without UNION ALL, derived/subqueries, or locking clauses")
	}
	leaves := make([]*explainSetOperationNode, 0)
	explainSetOperationLeaves(tree, "PRIMARY", &leaves)
	for index, leaf := range leaves {
		if index == 0 {
			leaf.id = firstID
			continue
		}
		leaf.id = *nextID
		*nextID++
	}
	for _, leaf := range leaves {
		rows, err := e.explainSelectPlanRowsWithNextID(ctx, leaf.statement, defaultSchema, leaf.id, leaf.selectType, nil, nextID)
		if err != nil {
			return nil, err
		}
		leaf.rows = rows
	}
	assignExplainSetOperationResultIDs(tree, nextID)
	rows := make([][]interface{}, 0, len(leaves)*2)
	appendExplainSetOperationLeafRows(tree, &rows)
	appendExplainSetOperationResultRows(tree, &rows)
	sort.SliceStable(rows, func(left, right int) bool {
		leftID, leftHasID := rows[left][0].(int64)
		rightID, rightHasID := rows[right][0].(int64)
		if leftHasID != rightHasID {
			return leftHasID
		}
		return leftHasID && leftID < rightID
	})
	return rows, nil
}

func explainUnionRequiresDistinct(statement sqlparser.SelectStatement) bool {
	switch value := statement.(type) {
	case *sqlparser.Union:
		if value == nil {
			return false
		}
		return !strings.EqualFold(strings.TrimSpace(value.Type), sqlparser.UnionAllStr) || explainUnionRequiresDistinct(value.Left) || explainUnionRequiresDistinct(value.Right)
	case *sqlparser.ParenSelect:
		return value != nil && explainUnionRequiresDistinct(value.Select)
	default:
		return false
	}
}

func (e *XMySQLExecutor) explainUnionPlanRows(ctx *ExecutionContext, statement *sqlparser.Union, defaultSchema string) ([][]interface{}, error) {
	nextID := int64(2)
	return e.explainUnionPlanRowsAt(ctx, statement, defaultSchema, 1, &nextID)
}

func (e *XMySQLExecutor) explainUnionPlanRowsAt(ctx *ExecutionContext, statement *sqlparser.Union, defaultSchema string, firstID int64, nextID *int64) ([][]interface{}, error) {
	branches, ok := explainUnionBranches(statement)
	if !ok {
		return e.explainNestedSetOperationPlanRowsAt(ctx, statement, defaultSchema, firstID, nextID)
	}
	operator := explainSetOperationName(statement.Type)
	rows := make([][]interface{}, 0, len(branches)+1)
	tasks := make([]explainDerivedTask, 0)
	branchIDs := make([]int64, 0, len(branches))
	allSubqueries := make([]*plan.SubqueryExpression, 0)
	seenSubqueries := make(map[*plan.SubqueryExpression]struct{})
	appendSubqueries := func(subqueries []*plan.SubqueryExpression) {
		for _, subquery := range subqueries {
			if subquery == nil {
				continue
			}
			if _, exists := seenSubqueries[subquery]; exists {
				continue
			}
			seenSubqueries[subquery] = struct{}{}
			allSubqueries = append(allSubqueries, subquery)
		}
	}
	for index, branch := range branches {
		id := firstID
		if index > 0 {
			id = *nextID
			*nextID++
		}
		branchIDs = append(branchIDs, id)
		selectType := operator
		if index == 0 {
			selectType = "PRIMARY"
		}
		subqueries := explainSelectSubqueries(branch)
		appendSubqueries(subqueries)
		branchRows, err := e.explainSelectBlockRows(ctx, branch, defaultSchema, id, selectType, nextID, &tasks)
		if err != nil {
			return nil, err
		}
		rows = append(rows, branchRows...)
	}
	for taskIndex := 0; taskIndex < len(tasks); taskIndex++ {
		task := tasks[taskIndex]
		child := explainSelectStatementAsSelect(task.statement)
		if child == nil {
			if union, ok := explainSelectStatementAsUnion(task.statement); ok {
				childRows, err := e.explainUnionPlanRowsAt(ctx, union, defaultSchema, task.id, nextID)
				if err != nil {
					return nil, err
				}
				rows = append(rows, childRows...)
			}
			continue
		}
		childSubqueries := explainSelectSubqueries(child)
		appendSubqueries(childSubqueries)
		childRows, err := e.explainSelectBlockRows(ctx, child, defaultSchema, task.id, "DERIVED", nextID, &tasks)
		if err != nil {
			return nil, err
		}
		if task.parentRow != nil {
			if estimate, known := explainDerivedRowsEstimate(child, childRows); known {
				task.parentRow[9] = estimate
			}
		}
		rows = append(rows, childRows...)
	}
	if len(allSubqueries) > 0 {
		subqueryRows, err := e.explainSubqueryRows(ctx, allSubqueries, defaultSchema, *nextID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, subqueryRows...)
		*nextID += int64(len(subqueryRows))
	}
	resultExtra := ""
	resultType := "UNION RESULT"
	resultTable := "union"
	includeSetResult := explainUnionRequiresDistinct(statement) || len(statement.OrderBy) > 0
	if operator == "INTERSECT" || operator == "EXCEPT" {
		resultExtra = "Using temporary"
		resultType = operator + " RESULT"
		resultTable = strings.ToLower(operator)
		includeSetResult = true
	} else if explainUnionRequiresDistinct(statement) {
		resultExtra = "Using temporary"
	}
	if len(statement.OrderBy) > 0 {
		resultExtra = appendExplainExtra(resultExtra, "Using filesort")
	}
	if includeSetResult {
		var resultID interface{}
		var resultFiltered interface{} = float64(100)
		if operator == "INTERSECT" || operator == "EXCEPT" {
			resultID = *nextID
			*nextID++
			resultFiltered = nil
		}
		rows = append(rows, []interface{}{resultID, resultType, fmt.Sprintf("<%s%d,%d>", resultTable, branchIDs[0], branchIDs[len(branchIDs)-1]), nil, "ALL", nil, nil, nil, nil, nil, resultFiltered, resultExtra})
	}
	return rows, nil
}

func explainSelectStatementAsUnion(statement sqlparser.SelectStatement) (*sqlparser.Union, bool) {
	switch value := statement.(type) {
	case *sqlparser.Union:
		return value, value != nil
	case *sqlparser.ParenSelect:
		return explainSelectStatementAsUnion(value.Select)
	default:
		return nil, false
	}
}

func explainDerivedRowsEstimate(statement *sqlparser.Select, rows [][]interface{}) (interface{}, bool) {
	if statement == nil || len(rows) == 0 {
		return nil, false
	}
	if len(statement.From) == 0 {
		return int64(1), true
	}
	if statement.Distinct != "" || len(statement.GroupBy) > 0 || statement.Having != nil || statement.Limit != nil || len(statement.Windows) > 0 || explainSelectHasDerivedSource(statement) || explainSelectHasAggregateOrWindowProjection(statement) {
		return nil, false
	}
	if len(explainTableReferences(statement)) != 1 || rows[0][9] == nil {
		return nil, false
	}
	return rows[0][9], true
}

func explainSelectHasAggregateOrWindowProjection(statement *sqlparser.Select) bool {
	if statement == nil {
		return false
	}
	found := false
	for _, expression := range statement.SelectExprs {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			function, ok := node.(*sqlparser.FuncExpr)
			if !ok {
				return !found, nil
			}
			if function.Over != nil {
				found = true
				return false, nil
			}
			switch strings.ToUpper(function.Name.String()) {
			case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "JSON_ARRAYAGG", "JSON_OBJECTAGG", "ANY_VALUE", "BIT_AND", "BIT_OR", "BIT_XOR", "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
				found = true
				return false, nil
			}
			return true, nil
		}, expression)
		if found {
			return true
		}
	}
	return found
}

func explainSelectStatementAsSelect(statement sqlparser.SelectStatement) *sqlparser.Select {
	switch value := statement.(type) {
	case *sqlparser.Select:
		return value
	case *sqlparser.ParenSelect:
		return explainSelectStatementAsSelect(value.Select)
	default:
		return nil
	}
}

func (e *XMySQLExecutor) explainSelectBlockRows(ctx *ExecutionContext, statement *sqlparser.Select, defaultSchema string, id int64, selectType string, nextID *int64, tasks *[]explainDerivedTask) ([][]interface{}, error) {
	subqueries := explainSelectSubqueries(statement)
	baseRows, err := e.explainSelectTableRows(ctx, statement, defaultSchema, subqueries)
	if err != nil {
		return nil, err
	}
	hasDerived := explainSelectHasDerivedSource(statement)
	if selectType == "" {
		selectType = "SIMPLE"
		if hasDerived || len(subqueries) > 0 {
			selectType = "PRIMARY"
		}
	}
	if len(statement.From) == 0 {
		if len(baseRows) == 0 {
			return nil, nil
		}
		baseRows[0][0], baseRows[0][1] = id, selectType
		return baseRows, nil
	}
	rows := make([][]interface{}, 0, len(baseRows)+len(statement.From))
	baseIndex := 0
	var appendTable func(sqlparser.TableExpr) error
	appendTable = func(expression sqlparser.TableExpr) error {
		switch value := expression.(type) {
		case *sqlparser.AliasedTableExpr:
			switch source := value.Expr.(type) {
			case sqlparser.TableName:
				if baseIndex >= len(baseRows) {
					return fmt.Errorf("EXPLAIN table source metadata is incomplete")
				}
				row := baseRows[baseIndex]
				baseIndex++
				row[0], row[1] = id, selectType
				rows = append(rows, row)
			case *sqlparser.Subquery:
				derivedID := *nextID
				*nextID++
				row := []interface{}{id, selectType, fmt.Sprintf("<derived%d>", derivedID), nil, "ALL", nil, nil, nil, nil, nil, float64(100), ""}
				rows = append(rows, row)
				*tasks = append(*tasks, explainDerivedTask{statement: source.Select, id: derivedID, parentRow: row})
			default:
				return fmt.Errorf("unsupported EXPLAIN table source %T", source)
			}
		case *sqlparser.JoinTableExpr:
			if err := appendTable(value.LeftExpr); err != nil {
				return err
			}
			return appendTable(value.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, child := range value.Exprs {
				if err := appendTable(child); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported EXPLAIN table expression %T", expression)
		}
		return nil
	}
	for _, expression := range statement.From {
		if err := appendTable(expression); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func explainSelectHasDerivedSource(statement *sqlparser.Select) bool {
	var contains func(sqlparser.TableExpr) bool
	contains = func(expression sqlparser.TableExpr) bool {
		switch value := expression.(type) {
		case *sqlparser.AliasedTableExpr:
			_, ok := value.Expr.(*sqlparser.Subquery)
			return ok
		case *sqlparser.JoinTableExpr:
			return contains(value.LeftExpr) || contains(value.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, child := range value.Exprs {
				if contains(child) {
					return true
				}
			}
		}
		return false
	}
	for _, expression := range statement.From {
		if contains(expression) {
			return true
		}
	}
	return false
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
