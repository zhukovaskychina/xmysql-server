// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/juju/errors"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/ast"
	types "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/context"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/expression"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/expression/aggregation"
)

// extractCorColumnsBySchema only extracts the correlated columns that match the outer plan's schema.
// e.g. If the correlated columns from inner plan are [t1.a, t2.a, t3.a] and outer plan's schema is [t2.a, t2.b, t2.c],
// only [t2.a] is treated as this apply's correlated column.
func (a *LogicalApply) extractCorColumnsBySchema() {
	schema := a.children[0].Schema()
	corCols := a.children[1].(LogicalPlan).extractCorrelatedCols()
	resultCorCols := make([]*expression.CorrelatedColumn, schema.Len())
	for _, corCol := range corCols {
		idx := schema.ColumnIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema.Columns[idx],
					Data:   new(types.Datum),
				}
			}
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	length := 0
	for _, col := range resultCorCols {
		if col != nil {
			resultCorCols[length] = col
			length++
		}
	}
	a.corCols = resultCorCols[:length]
}

// canPullUpAgg checks if an apply can pull an aggregation up.
func (a *LogicalApply) canPullUpAgg() bool {
	if a.JoinType != InnerJoin && a.JoinType != LeftOuterJoin {
		return false
	}
	if len(a.EqualConditions)+len(a.LeftConditions)+len(a.RightConditions)+len(a.OtherConditions) > 0 {
		return false
	}
	return len(a.children[0].Schema().Keys) > 0
}

// canPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (a *LogicalAggregation) canPullUp() bool {
	if len(a.GroupByItems) > 0 {
		return false
	}
	for _, f := range a.AggFuncs {
		for _, arg := range f.GetArgs() {
			expr, err := expression.EvaluateExprWithNull(a.ctx, a.children[0].Schema(), arg)
			if err != nil {
				return false
			}
			if con, ok := expr.(*expression.Constant); !ok || !con.Value.IsNull() {
				return false
			}
		}
	}
	return true
}

// decorrelateSolver tries to convert apply plan to join plan.
type decorrelateSolver struct{}

// optimize implements logicalOptRule interface.
func (s *decorrelateSolver) optimize(p LogicalPlan, _ context.Context, _ *idAllocator) (LogicalPlan, error) {
	if apply, ok := p.(*LogicalApply); ok {
		outerPlan := apply.children[0]
		innerPlan := apply.children[1].(LogicalPlan)
		apply.extractCorColumnsBySchema()
		if len(apply.corCols) == 0 {
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			innerPlan.SetParents(join)
			outerPlan.SetParents(join)
			join.self = join
			p = join
		} else if sel, ok := innerPlan.(*Selection); ok {
			// If the inner plan is a selection, we add this condition to join predicates.
			// Notice that no matter what kind of join is, it's always right.
			newConds := make([]expression.Expression, 0, len(sel.Conditions))
			for _, cond := range sel.Conditions {
				newConds = append(newConds, cond.Decorrelate(outerPlan.Schema()))
			}
			apply.attachOnConds(newConds)
			innerPlan = sel.children[0].(LogicalPlan)
			setParentAndChildren(apply, outerPlan, innerPlan)
			return s.optimize(p, nil, nil)
		} else if m, ok := innerPlan.(*MaxOneRow); ok {
			if m.children[0].Schema().MaxOneRow {
				innerPlan = m.children[0].(LogicalPlan)
				setParentAndChildren(apply, outerPlan, innerPlan)
				return s.optimize(p, nil, nil)
			}
		} else if proj, ok := innerPlan.(*Projection); ok {
			for i, expr := range proj.Exprs {
				proj.Exprs[i] = expr.Decorrelate(outerPlan.Schema())
			}
			apply.columnSubstitute(proj.Schema(), proj.Exprs)
			innerPlan = proj.children[0].(LogicalPlan)
			setParentAndChildren(apply, outerPlan, innerPlan)
			if apply.JoinType != SemiJoin && apply.JoinType != LeftOuterSemiJoin {
				proj.SetSchema(apply.Schema())
				proj.Exprs = append(expression.Column2Exprs(outerPlan.Schema().Clone().Columns), proj.Exprs...)
				apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
				proj.SetParents(apply.Parents()...)
				np, err := s.optimize(p, nil, nil)
				if err != nil {
					return nil, errors.Trace(err)
				}
				setParentAndChildren(proj, np)
				return proj, nil
			}
			return s.optimize(p, nil, nil)
		} else if agg, ok := innerPlan.(*LogicalAggregation); ok {
			if apply.canPullUpAgg() && agg.canPullUp() {
				innerPlan = agg.children[0].(LogicalPlan)
				apply.JoinType = LeftOuterJoin
				setParentAndChildren(apply, outerPlan, innerPlan)
				agg.SetSchema(apply.Schema())
				agg.GroupByItems = expression.Column2Exprs(outerPlan.Schema().Keys[0])
				newAggFuncs := make([]aggregation.Aggregation, 0, apply.Schema().Len())
				for _, col := range outerPlan.Schema().Columns {
					first := aggregation.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{col}, false)
					newAggFuncs = append(newAggFuncs, first)
				}
				newAggFuncs = append(newAggFuncs, agg.AggFuncs...)
				agg.AggFuncs = newAggFuncs
				apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
				agg.SetParents(apply.Parents()...)
				np, err := s.optimize(p, nil, nil)
				if err != nil {
					return nil, errors.Trace(err)
				}
				setParentAndChildren(agg, np)
				agg.collectGroupByColumns()
				return agg, nil
			}
		}
	}
	newChildren := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, err := s.optimize(child.(LogicalPlan), nil, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		newChildren = append(newChildren, np)
	}
	setParentAndChildren(p, newChildren...)
	return p, nil
}
