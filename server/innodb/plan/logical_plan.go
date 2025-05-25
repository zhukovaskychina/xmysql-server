package plan

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
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
		Exprs: b.buildProjectionExprs(stmt.SelectExprs),
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
		switch v := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			tablePlan, err := b.buildTableSource(v)
			if err != nil {
				return nil, err
			}
			if plan == nil {
				plan = tablePlan
			} else {
				// 构建JOIN
				plan = &LogicalJoin{
					BaseLogicalPlan: BaseLogicalPlan{
						children: []LogicalPlan{plan, tablePlan},
					},
					JoinType: "INNER",
				}
			}
		}
	}
	return plan, nil
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
	// TODO: 实现表达式构建
	return nil
}

// buildGroupByItems 构建GROUP BY项
func (b *PlanBuilder) buildGroupByItems(groupBy sqlparser.GroupBy) []Expression {
	// TODO: 实现GROUP BY项构建
	return nil
}

// buildAggFuncs 构建聚合函数
func (b *PlanBuilder) buildAggFuncs(selectExprs sqlparser.SelectExprs) []AggregateFunc {
	// TODO: 实现聚合函数构建
	return nil
}

// buildProjectionExprs 构建投影表达式
func (b *PlanBuilder) buildProjectionExprs(selectExprs sqlparser.SelectExprs) []Expression {
	// TODO: 实现投影表达式构建
	return nil
}
