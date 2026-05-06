package plan

import (
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
	Exprs []Expression
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

// BuildLogicalPlan 构建逻辑计划
func BuildLogicalPlan(stmt *sqlparser.Select, infoSchema InfoSchemas) (LogicalPlan, error) {
	builder := &PlanBuilder{
		ctx:        nil,
		infoSchema: infoSchema,
	}
	return builder.buildSelect(stmt)
}

// PlanBuilder 计划构建器
type PlanBuilder struct {
	ctx        interface{}
	infoSchema InfoSchemas
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
	case "in":
		return OpIn
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
	}
	return OpAdd
}

// buildSelect 构建SELECT语句的逻辑计划
func (b *PlanBuilder) buildSelect(stmt *sqlparser.Select) (LogicalPlan, error) {
	// 1. 构建FROM子句
	from, err := b.buildTableRefs(stmt.From)
	if err != nil {
		return nil, err
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

	// 4. 构建投影
	projection := &LogicalProjection{
		BaseLogicalPlan: BaseLogicalPlan{
			children: []LogicalPlan{from},
		},
		Exprs: b.buildProjectionExprs(stmt.SelectExprs, from.Schema()),
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
		table, err := b.infoSchema.TableByName(tableName)
		if err != nil {
			return nil, err
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
		return &BinaryOperation{
			Op:    b.convertBinaryOp(v.Operator),
			Left:  b.buildExpr(v.Left),
			Right: b.buildExpr(v.Right),
		}
	case *sqlparser.ComparisonExpr:
		return &BinaryOperation{
			Op:    b.convertComparisonOp(v.Operator),
			Left:  b.buildExpr(v.Left),
			Right: b.buildExpr(v.Right),
		}
	case *sqlparser.AndExpr:
		return &BinaryOperation{Op: OpAnd, Left: b.buildExpr(v.Left), Right: b.buildExpr(v.Right)}
	case *sqlparser.OrExpr:
		return &BinaryOperation{Op: OpOr, Left: b.buildExpr(v.Left), Right: b.buildExpr(v.Right)}
	case *sqlparser.FuncExpr:
		var args []Expression
		for _, a := range v.Exprs {
			if ae, ok := a.(*sqlparser.AliasedExpr); ok {
				args = append(args, b.buildExpr(ae.Expr))
			}
		}
		return &Function{FuncName: v.Name.String(), FuncArgs: args}
	case sqlparser.ValTuple:
		var vals []interface{}
		for _, e := range v {
			if c, ok := b.buildExpr(e).(*Constant); ok {
				vals = append(vals, c.Value)
			}
		}
		return &Constant{Value: vals}
	case *sqlparser.ParenExpr:
		return b.buildExpr(v.Expr)
	}
	return nil
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
		fe, ok := ae.Expr.(*sqlparser.FuncExpr)
		if !ok || !fe.IsAggregate() {
			continue
		}
		var args []Expression
		for _, a := range fe.Exprs {
			if ae2, ok := a.(*sqlparser.AliasedExpr); ok {
				args = append(args, b.buildExpr(ae2.Expr))
			}
		}
		funcs = append(funcs, &Function{FuncName: fe.Name.String(), FuncArgs: args})
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
