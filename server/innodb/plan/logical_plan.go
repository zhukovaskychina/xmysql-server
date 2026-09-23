package plan

import (
	"encoding/hex"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"strconv"
	"strings"
)

// AggregateFunc represents an aggregate function
type AggregateFunc interface {
	Name() string
	Args() []Expression
}

// InfoSchemas represents schema information interface
type InfoSchemas interface {
	TableByName(name string) (*metadata.Table, error)
}

// Index represents an index
type Index struct {
	Name    string
	Columns []string
	Unique  bool
}

// LogicalPlan 逻辑计划接口
type LogicalPlan interface {
	// Schema 返回计划的输出模式
	Schema() *metadata.DatabaseSchema
	// Children 返回子计划
	Children() []LogicalPlan
	// SetChildren 设置子计划
	SetChildren(children []LogicalPlan)
	// String 返回计划的字符串表示
	String() string
}

// BaseLogicalPlan 基础逻辑计划实现
type BaseLogicalPlan struct {
	schema   *metadata.DatabaseSchema
	children []LogicalPlan
}

func (p *BaseLogicalPlan) Schema() *metadata.DatabaseSchema {
	return p.schema
}

func (p *BaseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

func (p *BaseLogicalPlan) SetChildren(children []LogicalPlan) {
	p.children = children
}

// LogicalTableScan 表扫描逻辑计划
type LogicalTableScan struct {
	BaseLogicalPlan
	Table *metadata.Table
}

// LogicalIndexScan 索引扫描逻辑计划
type LogicalIndexScan struct {
	BaseLogicalPlan
	Table *metadata.Table
	Index *Index
}

// LogicalProjection 投影逻辑计划
type LogicalProjection struct {
	BaseLogicalPlan
	Exprs       []Expression
	OutputNames []string
	Distinct    bool
	OrderBy     []ByItem
	Offset      int64
	Limit       int64
	HasLimit    bool
}

// LogicalSelection 选择逻辑计划
type LogicalSelection struct {
	BaseLogicalPlan
	Conditions []Expression
}

// LogicalJoin 连接逻辑计划
type LogicalJoin struct {
	BaseLogicalPlan
	JoinType    string
	Conditions  []Expression
	LeftSchema  *metadata.DatabaseSchema
	RightSchema *metadata.DatabaseSchema
}

// LogicalAggregation 聚合逻辑计划
type LogicalAggregation struct {
	BaseLogicalPlan
	GroupByItems []Expression
	AggFuncs     []AggregateFunc
}

// LogicalSubquery 子查询逻辑计划
type LogicalSubquery struct {
	BaseLogicalPlan
	SubqueryType string      // "SCALAR", "IN", "EXISTS", "ANY", "ALL"
	Correlated   bool        // 是否为关联子查询
	OuterRefs    []string    // 外部引用的列
	Subplan      LogicalPlan // 子查询的逻辑计划
}

// LogicalApply Apply算子（用于关联子查询）
type LogicalApply struct {
	BaseLogicalPlan
	ApplyType  string       // "INNER", "LEFT", "SEMI", "ANTI"
	Correlated bool         // 是否为关联
	JoinConds  []Expression // 关联条件
}

// LogicalUnion represents a UNION/UNION ALL set operation in the plan tree.
// Keeping it as a first-class node prevents the executor from having to
// recover set-operation semantics from a flattened SELECT.
type LogicalUnion struct {
	BaseLogicalPlan
	UnionType string
	OrderBy   []ByItem
	Offset    int64
	Limit     int64
	HasLimit  bool
}

// LogicalCTE is a named CTE definition. Its query is planned once and can be
// referenced by LogicalCTEScan nodes in the statement body.
type LogicalCTE struct {
	BaseLogicalPlan
	Name      string
	Columns   []string
	Recursive bool
	Query     LogicalPlan
}

// LogicalRecursiveCTE keeps anchor and recursive members separate so a
// physical executor can bind the recursive member to the materialized scope.
type LogicalRecursiveCTE struct {
	BaseLogicalPlan
	Name      string
	Columns   []string
	Anchor    LogicalPlan
	Recursive LogicalPlan
}

// LogicalCTEScan is a scope-bound scan of a materialized CTE.
type LogicalCTEScan struct {
	BaseLogicalPlan
	Name    string
	Columns []string
}

// LogicalCTEStatement contains definitions followed by the statement body.
type LogicalCTEStatement struct {
	BaseLogicalPlan
	Definitions []LogicalPlan
	Body        LogicalPlan
}

type LogicalValues struct {
	BaseLogicalPlan
	Exprs []Expression
}

func (p *LogicalTableScan) String() string {
	return fmt.Sprintf("TableScan(%s)", p.Table.Name)
}

func (p *LogicalIndexScan) String() string {
	return fmt.Sprintf("IndexScan(%s.%s)", p.Table.Name, p.Index.Name)
}

func (p *LogicalProjection) String() string {
	return "Projection"
}

func (p *LogicalSelection) String() string {
	return "Selection"
}

func (p *LogicalJoin) String() string {
	return fmt.Sprintf("Join(%s)", p.JoinType)
}

func (p *LogicalAggregation) String() string {
	return "Aggregation"
}

func (p *LogicalSubquery) String() string {
	return fmt.Sprintf("Subquery(%s, correlated=%v)", p.SubqueryType, p.Correlated)
}

func (p *LogicalApply) String() string {
	return fmt.Sprintf("Apply(%s, correlated=%v)", p.ApplyType, p.Correlated)
}

func (p *LogicalUnion) String() string { return fmt.Sprintf("Union(%s)", p.UnionType) }

func (p *LogicalCTE) String() string {
	return fmt.Sprintf("CTE(%s, recursive=%v)", p.Name, p.Recursive)
}

func (p *LogicalRecursiveCTE) String() string {
	return fmt.Sprintf("RecursiveCTE(%s)", p.Name)
}

func (p *LogicalCTEScan) String() string { return fmt.Sprintf("CTEScan(%s)", p.Name) }

func (p *LogicalCTEStatement) String() string { return "CTEStatement" }

func (p *LogicalValues) String() string { return "Values" }

// BuildLogicalPlan 构建逻辑计划
func BuildLogicalPlan(stmt *sqlparser.Select, infoSchema InfoSchemas) (LogicalPlan, error) {
	builder := &PlanBuilder{
		ctx:        nil,
		infoSchema: infoSchema,
		cteSources: make(map[string]LogicalPlan),
	}
	return builder.buildSelect(stmt)
}

// BuildLogicalPlanStatement is the statement-level entry point used by
// callers that need WITH/UNION plans. The historical BuildLogicalPlan API is
// kept for SELECT-only callers.
func BuildLogicalPlanStatement(stmt sqlparser.Statement, infoSchema InfoSchemas) (LogicalPlan, error) {
	builder := &PlanBuilder{
		ctx:        nil,
		infoSchema: infoSchema,
		cteSources: make(map[string]LogicalPlan),
	}
	return builder.buildStatement(stmt)
}

// PlanBuilder 计划构建器
type PlanBuilder struct {
	ctx        interface{}
	infoSchema InfoSchemas
	cteSources map[string]LogicalPlan
}

func (b *PlanBuilder) buildStatement(stmt sqlparser.Statement) (LogicalPlan, error) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		return b.buildSelect(node)
	case *sqlparser.ParenSelect:
		return b.buildSelectStatement(node.Select)
	case *sqlparser.Union:
		return b.buildSelectStatement(node)
	case *sqlparser.With:
		return b.buildWith(node)
	default:
		return nil, fmt.Errorf("unsupported statement for logical plan: %T", stmt)
	}
}

func (b *PlanBuilder) buildSelectStatement(stmt sqlparser.SelectStatement) (LogicalPlan, error) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		return b.buildSelect(node)
	case *sqlparser.ParenSelect:
		return b.buildSelectStatement(node.Select)
	case *sqlparser.Union:
		left, err := b.buildSelectStatement(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildSelectStatement(node.Right)
		if err != nil {
			return nil, err
		}
		union := &LogicalUnion{
			BaseLogicalPlan: BaseLogicalPlan{
				schema:   left.Schema(),
				children: []LogicalPlan{left, right},
			},
			UnionType: node.Type,
			Limit:     -1,
		}
		union.OrderBy = b.buildOrderBy(node.OrderBy, getPlanOutputColumnNames(left))
		if node.Limit != nil {
			offset, limit, err := buildLimitValues(node.Limit)
			if err != nil {
				return nil, err
			}
			union.Offset, union.Limit, union.HasLimit = offset, limit, true
		}
		return union, nil
	default:
		return nil, fmt.Errorf("unsupported select statement: %T", stmt)
	}
}

func (b *PlanBuilder) buildOrderBy(orderBy sqlparser.OrderBy, outputNames []string) []ByItem {
	items := make([]ByItem, 0, len(orderBy))
	for position, order := range orderBy {
		if order == nil {
			continue
		}
		item := ByItem{Expr: b.buildExpr(order.Expr), ColumnIndex: position, ColumnIndexSet: true}
		item.Desc = strings.EqualFold(order.Direction, sqlparser.DescScr)
		if constant, ok := item.Expr.(*Constant); ok {
			if ordinal, ok := planInt64Value(constant.Value); ok && ordinal > 0 {
				item.ColumnIndex = int(ordinal - 1)
			}
		} else if column, ok := item.Expr.(*Column); ok {
			for index, name := range outputNames {
				if optimizerColumnKey(column.Name) == optimizerColumnKey(name) {
					item.ColumnIndex = index
					break
				}
			}
		}
		items = append(items, item)
	}
	return items
}

func buildLimitValues(limit *sqlparser.Limit) (int64, int64, error) {
	if limit == nil || limit.Rowcount == nil {
		return 0, -1, fmt.Errorf("UNION LIMIT requires a row count")
	}
	offset := int64(0)
	if limit.Offset != nil {
		value, ok := planInt64Value(sqlparser.String(limit.Offset))
		if !ok {
			return 0, -1, fmt.Errorf("unsupported UNION LIMIT offset: %s", sqlparser.String(limit.Offset))
		}
		offset = value
	}
	rowcount, ok := planInt64Value(sqlparser.String(limit.Rowcount))
	if !ok {
		return 0, -1, fmt.Errorf("unsupported UNION LIMIT row count: %s", sqlparser.String(limit.Rowcount))
	}
	return offset, rowcount, nil
}

func planInt64Value(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), uint64(v) <= uint64(^uint64(0)>>1)
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), v <= uint64(^uint64(0)>>1)
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func (b *PlanBuilder) buildWith(node *sqlparser.With) (LogicalPlan, error) {
	if node == nil || len(node.CTEs) == 0 {
		return nil, fmt.Errorf("WITH statement has no definitions")
	}
	definitions := make([]LogicalPlan, 0, len(node.CTEs))
	for _, cte := range node.CTEs {
		if cte == nil {
			return nil, fmt.Errorf("CTE definition is nil")
		}
		name := strings.ToLower(strings.TrimSpace(cte.Name.String()))
		if _, exists := b.cteSources[name]; exists {
			return nil, fmt.Errorf("duplicate CTE definition: %s", name)
		}

		if node.Recursive {
			if union, ok := cte.Query.(*sqlparser.Union); ok {
				anchor, err := b.buildSelectStatement(union.Left)
				if err != nil {
					return nil, err
				}
				// Register a typed scope before building the recursive member.
				b.cteSources[name] = &LogicalCTEScan{
					BaseLogicalPlan: BaseLogicalPlan{schema: anchor.Schema()},
					Name:            name,
					Columns:         cteColumnNames(cte.Columns),
				}
				recursive, err := b.buildSelectStatement(union.Right)
				if err != nil {
					return nil, err
				}
				definition := &LogicalRecursiveCTE{
					BaseLogicalPlan: BaseLogicalPlan{schema: anchor.Schema(), children: []LogicalPlan{anchor, recursive}},
					Name:            name,
					Columns:         cteColumnNames(cte.Columns),
					Anchor:          anchor,
					Recursive:       recursive,
				}
				definitions = append(definitions, definition)
				b.cteSources[name] = &LogicalCTEScan{
					BaseLogicalPlan: BaseLogicalPlan{schema: definition.Schema()},
					Name:            name,
					Columns:         append([]string(nil), definition.Columns...),
				}
				continue
			}
		}

		query, err := b.buildStatement(cte.Query)
		if err != nil {
			return nil, fmt.Errorf("build CTE %s: %w", name, err)
		}
		definition := &LogicalCTE{
			BaseLogicalPlan: BaseLogicalPlan{schema: query.Schema(), children: []LogicalPlan{query}},
			Name:            name,
			Columns:         cteColumnNames(cte.Columns),
			Recursive:       node.Recursive,
			Query:           query,
		}
		definitions = append(definitions, definition)
		b.cteSources[name] = &LogicalCTEScan{
			BaseLogicalPlan: BaseLogicalPlan{schema: definition.Schema()},
			Name:            name,
			Columns:         append([]string(nil), definition.Columns...),
		}
	}
	body, err := b.buildStatement(node.Body)
	if err != nil {
		return nil, err
	}
	children := append([]LogicalPlan{}, definitions...)
	children = append(children, body)
	return &LogicalCTEStatement{
		BaseLogicalPlan: BaseLogicalPlan{schema: body.Schema(), children: children},
		Definitions:     definitions,
		Body:            body,
	}, nil
}

func cteColumnNames(columns sqlparser.Columns) []string {
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		result = append(result, column.String())
	}
	return result
}

func (b *PlanBuilder) convertComparisonOp(op string) BinaryOp {
	switch strings.ToLower(op) {
	case "=":
		return OpEQ
	case "!=", "<>":
		return OpNE
	case "<":
		return OpLT
	case "<=":
		return OpLE
	case ">":
		return OpGT
	case ">=":
		return OpGE
	case "like":
		return OpLike
	case "not like":
		return OpNotLike
	case "in":
		return OpIn
	case "not in":
		return OpNotIn
	case "<=>":
		return OpNullSafeEQ
	case "regexp":
		return OpRegexp
	case "not regexp":
		return OpNotRegexp
	}
	return OpEQ
}

func (b *PlanBuilder) convertBinaryOp(op string) BinaryOp {
	switch op {
	case "+":
		return OpAdd
	case "-":
		return OpSub
	case "*":
		return OpMul
	case "/":
		return OpDiv
	case "div":
		return OpIntDiv
	case "%", "mod":
		return OpMod
	case "&":
		return OpBitAnd
	case "|":
		return OpBitOr
	case "^":
		return OpBitXor
	case "<<":
		return OpShiftLeft
	case ">>":
		return OpShiftRight
	}
	return OpAdd
}

// buildSelect 构建SELECT语句的逻辑计划
func (b *PlanBuilder) buildSelect(stmt *sqlparser.Select) (LogicalPlan, error) {
	// 1. 构建FROM子句
	var from LogicalPlan
	var err error
	if len(stmt.From) == 0 {
		from = &LogicalValues{
			BaseLogicalPlan: BaseLogicalPlan{schema: metadata.NewSchema("values")},
			Exprs:           b.buildProjectionExprs(stmt.SelectExprs, metadata.NewSchema("values")),
		}
	} else {
		from, err = b.buildTableRefs(stmt.From)
		if err != nil {
			return nil, err
		}
	}

	// 2. 构建WHERE子句
	if stmt.Where != nil {
		where := &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{from},
			},
			Conditions: []Expression{b.buildExpr(stmt.Where.Expr)},
		}
		from = where
	}

	// 3. 构建GROUP BY子句
	if len(stmt.GroupBy) > 0 {
		groupBy := &LogicalAggregation{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{from},
			},
			GroupByItems: b.buildGroupByItems(stmt.GroupBy),
			AggFuncs:     b.buildAggFuncs(stmt.SelectExprs),
		}
		from = groupBy
	}

	// HAVING is evaluated after grouping and before the final projection.
	// Keeping it as a selection node preserves aggregate predicates for both
	// the logical optimizer and physical adapters.
	if stmt.Having != nil {
		condition := b.buildExpr(stmt.Having.Expr)
		if condition == nil {
			return nil, fmt.Errorf("unsupported HAVING expression: %s", sqlparser.String(stmt.Having.Expr))
		}
		from = &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{from}},
			Conditions:      []Expression{condition},
		}
	}

	// 4. 构建投影
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{
			schema:   from.Schema(),
			children: []LogicalPlan{from},
		},
		Exprs:    b.buildProjectionExprs(stmt.SelectExprs, from.Schema()),
		Distinct: strings.TrimSpace(strings.ToLower(stmt.Distinct)) != "",
	}
	projection.OutputNames = b.buildProjectionOutputNames(stmt.SelectExprs, from.Schema(), projection.Exprs)
	projection.OrderBy = b.buildOrderBy(stmt.OrderBy, getPlanOutputColumnNames(from))
	projection.Limit = -1
	if stmt.Limit != nil {
		offset, limit, err := buildLimitValues(stmt.Limit)
		if err != nil {
			return nil, err
		}
		projection.Offset, projection.Limit, projection.HasLimit = offset, limit, true
	}

	return projection, nil
}

// buildTableRefs 构建表引用
func (b *PlanBuilder) buildTableRefs(tableExprs sqlparser.TableExprs) (LogicalPlan, error) {
	if len(tableExprs) == 0 {
		return nil, fmt.Errorf("empty FROM clause")
	}

	var plan LogicalPlan
	for _, expr := range tableExprs {
		nextPlan, err := b.buildTableExpr(expr)
		if err != nil {
			return nil, err
		}
		if plan == nil {
			plan = nextPlan
		} else {
			plan = &LogicalJoin{
				BaseLogicalPlan: BaseLogicalPlan{
					children: []LogicalPlan{plan, nextPlan},
				},
				JoinType: "INNER",
			}
		}
	}
	return plan, nil
}

// buildTableExpr 将 parser 的 TableExpr 转为逻辑计划（支持 JOIN 与单表）
func (b *PlanBuilder) buildTableExpr(expr sqlparser.TableExpr) (LogicalPlan, error) {
	switch v := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return b.buildTableSource(v)
	case *sqlparser.JoinTableExpr:
		leftPlan, err := b.buildTableExpr(v.LeftExpr)
		if err != nil {
			return nil, err
		}
		rightPlan, err := b.buildTableExpr(v.RightExpr)
		if err != nil {
			return nil, err
		}
		joinType := joinStrToJoinType(v.Join)
		var conditions []Expression
		if v.Condition.On != nil {
			conditions = []Expression{b.buildExpr(v.Condition.On)}
		}
		// 设置左右 Schema 供谓词下推（OPT-001）使用
		leftSchema := leftPlan.Schema()
		rightSchema := rightPlan.Schema()
		return &LogicalJoin{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{leftPlan, rightPlan},
			},
			JoinType:    joinType,
			Conditions:  conditions,
			LeftSchema:  leftSchema,
			RightSchema: rightSchema,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported table expr: %T", expr)
	}
}

// joinStrToJoinType 将 parser 的 Join 字符串转为逻辑计划 JoinType
func joinStrToJoinType(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "left join", "left outer join":
		return "LEFT"
	case "right join", "right outer join":
		return "RIGHT"
	case "join", "inner join", "cross join", "straight_join":
		return "INNER"
	default:
		return "INNER"
	}
}

// buildTableSource 构建表数据源
func (b *PlanBuilder) buildTableSource(tableExpr *sqlparser.AliasedTableExpr) (LogicalPlan, error) {
	switch v := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		// 获取表信息
		tableName := v.Name.String()
		if strings.EqualFold(tableName, "dual") {
			return &LogicalValues{
				BaseLogicalPlan: BaseLogicalPlan{schema: metadata.NewSchema("values")},
			}, nil
		}
		if source, ok := b.cteSources[strings.ToLower(tableName)]; ok {
			scan := &LogicalCTEScan{
				BaseLogicalPlan: BaseLogicalPlan{schema: source.Schema()},
				Name:            strings.ToLower(tableName),
			}
			if sourceScan, ok := source.(*LogicalCTEScan); ok {
				scan.Columns = append([]string(nil), sourceScan.Columns...)
			}
			return scan, nil
		}
		lookupName := tableName
		qualifiedSchema := strings.TrimSpace(v.Qualifier.String())
		if qualifiedSchema != "" {
			lookupName = qualifiedSchema + "." + tableName
		}
		table, err := b.infoSchema.TableByName(lookupName)
		if err != nil && lookupName != tableName {
			// Preserve compatibility with older InfoSchemas that key tables only
			// by their unqualified name, while still preferring a schema-aware
			// lookup whenever the provider supports it.
			table, err = b.infoSchema.TableByName(tableName)
		}
		if err != nil {
			return nil, err
		}
		if table != nil && qualifiedSchema != "" && (table.Schema == nil || !strings.EqualFold(table.Schema.Name, qualifiedSchema)) {
			schema := metadata.NewSchema(qualifiedSchema)
			if addErr := schema.AddTable(table); addErr != nil {
				return nil, addErr
			}
		}
		return &LogicalTableScan{
			BaseLogicalPlan: BaseLogicalPlan{
				schema: table.Schema, // 使用table的Schema字段
			},
			Table: table,
		}, nil
	}
	return nil, fmt.Errorf("unsupported table source: %T", tableExpr.Expr)
}

// buildExpr 构建表达式
func (b *PlanBuilder) buildExpr(expr sqlparser.Expr) Expression {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			if n, err := strconv.ParseInt(string(v.Val), 10, 64); err == nil {
				return &Constant{Value: n}
			}
		case sqlparser.FloatVal:
			if f, err := strconv.ParseFloat(string(v.Val), 64); err == nil {
				return &Constant{Value: f}
			}
		case sqlparser.HexNum:
			if n, err := strconv.ParseInt(string(v.Val), 0, 64); err == nil {
				return &Constant{Value: n}
			}
		case sqlparser.HexVal:
			if decoded, err := hex.DecodeString(string(v.Val)); err == nil {
				return &Constant{Value: string(decoded)}
			}
		case sqlparser.BitVal:
			if n, err := strconv.ParseInt(string(v.Val), 2, 64); err == nil {
				return &Constant{Value: n}
			}
		default:
			return &Constant{Value: string(v.Val)}
		}
	case *sqlparser.NullVal:
		return &Constant{Value: nil}
	case sqlparser.BoolVal:
		return &Constant{Value: bool(v)}
	case *sqlparser.ColName:
		return &Column{Name: v.Name.String()}
	case *sqlparser.BinaryExpr:
		left := b.buildExpr(v.Left)
		right := b.buildExpr(v.Right)
		switch v.Operator {
		case sqlparser.JSONExtractOp:
			return &Function{FuncName: "JSON_EXTRACT", FuncArgs: []Expression{left, right}}
		case sqlparser.JSONUnquoteExtractOp:
			extracted := &Function{FuncName: "JSON_EXTRACT", FuncArgs: []Expression{left, right}}
			return &Function{FuncName: "JSON_UNQUOTE", FuncArgs: []Expression{extracted}}
		}
		return &BinaryOperation{
			Op:    b.convertBinaryOp(v.Operator),
			Left:  left,
			Right: right,
		}
	case *sqlparser.ComparisonExpr:
		return &BinaryOperation{
			Op:     b.convertComparisonOp(v.Operator),
			Left:   b.buildExpr(v.Left),
			Right:  b.buildExpr(v.Right),
			Escape: b.buildExpr(v.Escape),
		}
	case *sqlparser.AndExpr:
		return &BinaryOperation{Op: OpAnd, Left: b.buildExpr(v.Left), Right: b.buildExpr(v.Right)}
	case *sqlparser.OrExpr:
		return &BinaryOperation{Op: OpOr, Left: b.buildExpr(v.Left), Right: b.buildExpr(v.Right)}
	case *sqlparser.NotExpr:
		return &NotExpression{Operand: b.buildExpr(v.Expr)}
	case *sqlparser.UnaryExpr:
		return &UnaryOperation{
			Operator: v.Operator,
			Operand:  b.buildExpr(v.Expr),
		}
	case *sqlparser.FuncExpr:
		var args []Expression
		functionName := strings.ToUpper(v.Name.String())
		for index, a := range v.Exprs {
			if ae, ok := a.(*sqlparser.AliasedExpr); ok {
				if interval, ok := ae.Expr.(*sqlparser.IntervalExpr); ok {
					args = append(args, &Constant{Value: sqlparser.String(interval)})
					continue
				}
				if functionName == "TIMESTAMPDIFF" && index == 0 {
					if unit, ok := ae.Expr.(*sqlparser.ColName); ok {
						args = append(args, &Constant{Value: unit.Name.String()})
						continue
					}
				}
				args = append(args, b.buildExpr(ae.Expr))
			}
		}
		return &Function{FuncName: v.Name.String(), FuncArgs: args, Distinct: v.Distinct}
	case *sqlparser.GroupConcatExpr:
		var args []Expression
		for _, item := range v.Exprs {
			aliased, ok := item.(*sqlparser.AliasedExpr)
			if !ok || aliased == nil {
				continue
			}
			arg := b.buildExpr(aliased.Expr)
			if arg == nil {
				return nil
			}
			args = append(args, arg)
		}
		separator := ","
		if raw := strings.TrimSpace(v.Separator); raw != "" {
			const marker = "separator "
			if index := strings.Index(strings.ToLower(raw), marker); index >= 0 {
				value := strings.TrimSpace(raw[index+len(marker):])
				value = strings.Trim(value, "'\"")
				separator = value
			}
		}
		return &Function{
			FuncName:  "GROUP_CONCAT",
			FuncArgs:  args,
			Distinct:  strings.TrimSpace(v.Distinct) != "",
			Separator: separator,
		}
	case *sqlparser.ConvertExpr:
		if v.Type == nil {
			return nil
		}
		functionName := "CAST"
		usingCharset := ""
		castLength, hasCastLength := convertTypeLength(v.Type)
		castScale, hasCastScale := convertTypeScale(v.Type)
		if strings.TrimSpace(v.Type.Charset) != "" {
			functionName = "CONVERT"
			usingCharset = v.Type.Charset
			return &Function{
				FuncName:      functionName,
				UsingCharset:  usingCharset,
				CastType:      v.Type.Type,
				CastLength:    castLength,
				HasCastLength: hasCastLength,
				CastScale:     castScale,
				HasCastScale:  hasCastScale,
				FuncArgs:      []Expression{b.buildExpr(v.Expr)},
			}
		}
		return &Function{
			FuncName:      functionName,
			UsingCharset:  usingCharset,
			CastType:      v.Type.Type,
			CastLength:    castLength,
			HasCastLength: hasCastLength,
			CastScale:     castScale,
			HasCastScale:  hasCastScale,
			FuncArgs:      []Expression{b.buildExpr(v.Expr), &Constant{Value: v.Type.Type}},
		}
	case *sqlparser.ConvertUsingExpr:
		return &Function{FuncName: "CONVERT", UsingCharset: v.Type, FuncArgs: []Expression{b.buildExpr(v.Expr)}}
	case *sqlparser.SubstrExpr:
		args := []Expression{b.buildExpr(v.Name), b.buildExpr(v.From)}
		if v.To != nil {
			args = append(args, b.buildExpr(v.To))
		}
		return &Function{FuncName: "SUBSTRING", FuncArgs: args}
	case *sqlparser.CollateExpr:
		// The storage engine keeps text in UTF-8; preserve the expression value
		// while leaving collation-aware ordering to the comparison layer.
		return b.buildExpr(v.Expr)
	case sqlparser.ValTuple:
		var vals []interface{}
		var exprs []Expression
		allConstants := true
		for _, e := range v {
			built := b.buildExpr(e)
			if built == nil {
				return nil
			}
			exprs = append(exprs, built)
			if c, ok := built.(*Constant); ok {
				vals = append(vals, c.Value)
			} else {
				allConstants = false
			}
		}
		if allConstants {
			return &Constant{Value: vals}
		}
		return &TupleExpression{BaseExpression: BaseExpression{children: exprs}, Exprs: exprs}
	case *sqlparser.ParenExpr:
		return b.buildExpr(v.Expr)
	case *sqlparser.RangeCond:
		column := b.buildExpr(v.Left)
		lower := b.buildExpr(v.From)
		upper := b.buildExpr(v.To)
		lowerConstant, lowerOK := lower.(*Constant)
		upperConstant, upperOK := upper.(*Constant)
		if column == nil || lower == nil || upper == nil {
			return nil
		}
		between := &BetweenExpression{
			Column: column,
			Not:    v.Operator == sqlparser.NotBetweenStr,
		}
		if lowerOK {
			between.Lower = lowerConstant.Value
		} else {
			between.LowerExpr = lower
		}
		if upperOK {
			between.Upper = upperConstant.Value
		} else {
			between.UpperExpr = upper
		}
		between.children = []Expression{column, lower, upper}
		return between
	case *sqlparser.IsExpr:
		switch v.Operator {
		case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
			return &IsNullExpression{
				Column: b.buildExpr(v.Expr),
				IsNull: v.Operator == sqlparser.IsNullStr,
			}
		case sqlparser.IsTrueStr, sqlparser.IsNotTrueStr, sqlparser.IsFalseStr, sqlparser.IsNotFalseStr:
			return &IsTruthExpression{
				Expr:     b.buildExpr(v.Expr),
				Operator: v.Operator,
			}
		}
		return nil
	case *sqlparser.CaseExpr:
		caseExpression := &CaseExpression{}
		if v.Expr != nil {
			caseExpression.Operand = b.buildExpr(v.Expr)
			if caseExpression.Operand == nil {
				return nil
			}
			caseExpression.children = append(caseExpression.children, caseExpression.Operand)
		}
		for _, when := range v.Whens {
			if when == nil {
				continue
			}
			condition := b.buildExpr(when.Cond)
			value := b.buildExpr(when.Val)
			if condition == nil || value == nil {
				return nil
			}
			caseExpression.Whens = append(caseExpression.Whens, CaseWhen{Condition: condition, Value: value})
			caseExpression.children = append(caseExpression.children, condition, value)
		}
		if v.Else != nil {
			caseExpression.Else = b.buildExpr(v.Else)
			if caseExpression.Else == nil {
				return nil
			}
			caseExpression.children = append(caseExpression.children, caseExpression.Else)
		}
		return caseExpression
	}
	return nil
}

func convertTypeLength(convertType *sqlparser.ConvertType) (int64, bool) {
	if convertType == nil || convertType.Length == nil {
		return 0, false
	}
	length, err := strconv.ParseInt(strings.TrimSpace(string(convertType.Length.Val)), 10, 64)
	if err != nil || length < 0 {
		return 0, false
	}
	return length, true
}

func convertTypeScale(convertType *sqlparser.ConvertType) (int64, bool) {
	if convertType == nil || convertType.Scale == nil {
		return 0, false
	}
	scale, err := strconv.ParseInt(strings.TrimSpace(string(convertType.Scale.Val)), 10, 64)
	if err != nil || scale < 0 {
		return 0, false
	}
	return scale, true
}

// BuildExpression exposes the planner's scalar-expression conversion for
// execution paths that need to compile an expression once and evaluate it for
// many rows. Unsupported AST shapes return nil and callers can retain their
// existing interpreter fallback.
func BuildExpression(expr sqlparser.Expr) Expression {
	return (&PlanBuilder{}).buildExpr(expr)
}

// buildGroupByItems 构建GROUP BY项
func (b *PlanBuilder) buildGroupByItems(groupBy sqlparser.GroupBy) []Expression {
	var items []Expression
	for _, expr := range groupBy {
		items = append(items, b.buildExpr(expr))
	}
	return items
}

// buildAggFuncs 构建聚合函数
func (b *PlanBuilder) buildAggFuncs(selectExprs sqlparser.SelectExprs) []AggregateFunc {
	var funcs []AggregateFunc
	for _, se := range selectExprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		fe, isFunction := ae.Expr.(*sqlparser.FuncExpr)
		if isFunction && !fe.IsAggregate() {
			continue
		}
		if !isFunction {
			if _, isGroupConcat := ae.Expr.(*sqlparser.GroupConcatExpr); !isGroupConcat {
				continue
			}
		}
		built, ok := b.buildExpr(ae.Expr).(*Function)
		if !ok || built == nil {
			continue
		}
		var args []Expression
		args = append(args, built.FuncArgs...)
		built.FuncArgs = args
		funcs = append(funcs, built)
	}
	return funcs
}

// buildProjectionExprs 构建投影表达式
func (b *PlanBuilder) buildProjectionExprs(selectExprs sqlparser.SelectExprs, schema *metadata.DatabaseSchema) []Expression {
	var exprs []Expression
	for _, se := range selectExprs {
		switch v := se.(type) {
		case *sqlparser.AliasedExpr:
			exprs = append(exprs, b.buildExpr(v.Expr))
		case *sqlparser.StarExpr:
			if schema == nil {
				continue
			}
			tblName := v.TableName.Name.String()
			if tblName != "" {
				if tbl, ok := schema.Tables[tblName]; ok {
					for _, col := range tbl.Columns {
						exprs = append(exprs, &Column{Name: col.Name})
					}
				}
				continue
			}
			for _, tbl := range schema.Tables {
				for _, col := range tbl.Columns {
					exprs = append(exprs, &Column{Name: col.Name})
				}
			}
		}
	}
	return exprs
}

// buildProjectionOutputNames retains the result-set names independently of
// the child table schema. The expression still evaluates against its source
// column, while an alias is exposed only as output metadata.
func (b *PlanBuilder) buildProjectionOutputNames(selectExprs sqlparser.SelectExprs, schema *metadata.DatabaseSchema, exprs []Expression) []string {
	names := make([]string, len(exprs))
	for index, expr := range exprs {
		if expr != nil {
			names[index] = expr.String()
		}
	}

	index := 0
	for _, selectExpr := range selectExprs {
		switch value := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			if index < len(names) && value.As.String() != "" {
				names[index] = value.As.String()
			}
			index++
		case *sqlparser.StarExpr:
			index += projectionStarWidth(value, schema)
		}
	}
	return names
}

func projectionStarWidth(star *sqlparser.StarExpr, schema *metadata.DatabaseSchema) int {
	if star == nil || schema == nil {
		return 0
	}
	if tableName := star.TableName.Name.String(); tableName != "" {
		if table, ok := schema.Tables[tableName]; ok && table != nil {
			return len(table.Columns)
		}
		return 0
	}
	width := 0
	for _, table := range schema.Tables {
		if table != nil {
			width += len(table.Columns)
		}
	}
	return width
}
