package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestSubqueryOptimizer_DecorrelateSubquery 测试子查询去关联
func TestSubqueryOptimizer_DecorrelateSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer()

	// 创建关联子查询
	// SELECT * FROM t1 WHERE t1.id IN (SELECT t2.id FROM t2 WHERE t2.value = t1.value)
	subquery := &LogicalSubquery{
		SubqueryType: "IN",
		Correlated:   true,
		OuterRefs:    []string{"value"},
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{
						Table: &metadata.Table{Name: "t2"},
					},
				},
			},
			Conditions: []Expression{
				&BinaryOperation{
					Op:    OpEQ,
					Left:  &Column{Name: "t2_value"},
					Right: &Column{Name: "t1_value"},
				},
			},
		},
	}

	// 执行去关联
	result := optimizer.decorrelateSubquery(subquery)

	// The helper has no enclosing outer row source, so it must preserve the
	// correlated boundary instead of fabricating a one-child Apply.
	if result != subquery {
		t.Fatalf("correlated standalone subquery changed into %T", result)
	}
	t.Logf("correlated subquery boundary preserved: %s", result.String())
}

// TestSubqueryOptimizer_OptimizeInSubquery 测试IN子查询优化
func TestSubqueryOptimizer_OptimizeInSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer()

	// 创建IN子查询
	// SELECT * FROM t1 WHERE t1.id IN (SELECT t2.id FROM t2)
	subquery := &LogicalSubquery{
		SubqueryType: "IN",
		Correlated:   false,
		OuterRefs:    []string{},
		Subplan: &LogicalProjection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{
						Table: &metadata.Table{Name: "t2"},
					},
				},
			},
			Exprs: []Expression{
				&Column{Name: "t2_id"},
			},
		},
	}

	// 执行优化
	result := optimizer.optimizeInSubquery(subquery)

	// 验证结果
	if result == nil {
		t.Fatal("优化失败，返回nil")
	}

	subqueryResult, ok := result.(*LogicalSubquery)
	if !ok || subqueryResult != subquery {
		t.Fatalf("standalone IN subquery changed into invalid plan %T", result)
	}

	// 验证统计信息
	stats := optimizer.GetStats()
	if stats.InToSemiJoin != 0 {
		t.Errorf("standalone IN subquery should not be counted as a SEMI join, got %d", stats.InToSemiJoin)
	}

	t.Logf("standalone IN subquery boundary preserved: %s", subqueryResult.String())
}

// TestSubqueryOptimizer_OptimizeExistsSubquery 测试EXISTS子查询优化
func TestSubqueryOptimizer_OptimizeExistsSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer()

	// 创建EXISTS子查询
	// SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.id = t2.id)
	subquery := &LogicalSubquery{
		SubqueryType: "EXISTS",
		Correlated:   true,
		OuterRefs:    []string{"id"},
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{
						Table: &metadata.Table{Name: "t2"},
					},
				},
			},
			Conditions: []Expression{
				&BinaryOperation{
					Op:    OpEQ,
					Left:  &Column{Name: "t1_id"},
					Right: &Column{Name: "t2_id"},
				},
			},
		},
	}

	// 执行优化
	result := optimizer.optimizeExistsSubquery(subquery)

	// 验证结果
	if result == nil {
		t.Fatal("优化失败，返回nil")
	}

	subqueryResult, ok := result.(*LogicalSubquery)
	if !ok || subqueryResult != subquery {
		t.Fatalf("standalone EXISTS subquery changed into invalid plan %T", result)
	}

	// 验证统计信息
	stats := optimizer.GetStats()
	if stats.ExistsToSemiJoin != 0 {
		t.Errorf("standalone EXISTS subquery should not be counted as a SEMI join, got %d", stats.ExistsToSemiJoin)
	}

	t.Logf("standalone EXISTS subquery boundary preserved: %s", subqueryResult.String())
}

// TestSubqueryOptimizer_OptimizeScalarSubquery 测试标量子查询优化
func TestSubqueryOptimizer_OptimizeScalarSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer()

	// 创建标量子查询
	// SELECT (SELECT MAX(salary) FROM employees) AS max_salary
	subquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Correlated:   false,
		OuterRefs:    []string{},
		Subplan: &LogicalAggregation{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{
						Table: &metadata.Table{Name: "employees"},
					},
				},
			},
			GroupByItems: []Expression{},
			AggFuncs: []AggregateFunc{
				&Function{
					FuncName: "MAX",
					FuncArgs: []Expression{
						&Column{Name: "salary"},
					},
				},
			},
		},
	}

	// 执行优化
	result := optimizer.optimizeScalarSubquery(subquery)

	// 验证结果
	if result == nil {
		t.Fatal("优化失败，返回nil")
	}

	// 标量子查询通常保持原样
	resultSubquery, ok := result.(*LogicalSubquery)
	if !ok {
		t.Fatalf("期望返回LogicalSubquery，实际返回%T", result)
	}

	if resultSubquery.SubqueryType != "SCALAR" {
		t.Errorf("期望SubqueryType为SCALAR，实际为%s", resultSubquery.SubqueryType)
	}

	t.Logf("标量子查询优化成功: %s", resultSubquery.String())
}

// TestSubqueryOptimizer_OptimizeApplyNode 测试Apply节点优化
func TestSubqueryOptimizer_OptimizeApplyNode(t *testing.T) {
	optimizer := NewSubqueryOptimizer()

	// 创建非关联的Apply节点
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{
			children: []LogicalPlan{
				&LogicalTableScan{Table: &metadata.Table{Name: "t1"}},
				&LogicalTableScan{Table: &metadata.Table{Name: "t2"}},
			},
		},
		ApplyType:  "INNER",
		Correlated: false,
		JoinConds: []Expression{
			&BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "t1_id"},
				Right: &Column{Name: "t2_id"},
			},
		},
	}

	// 执行优化
	result := optimizer.optimizeApplyNode(apply)

	// 验证结果
	if result == nil {
		t.Fatal("优化失败，返回nil")
	}

	// 非关联的Apply应该转换为普通JOIN
	join, ok := result.(*LogicalJoin)
	if !ok {
		t.Fatalf("期望返回LogicalJoin，实际返回%T", result)
	}

	if join.JoinType != "INNER" {
		t.Errorf("期望JoinType为INNER，实际为%s", join.JoinType)
	}

	// 验证统计信息
	stats := optimizer.GetStats()
	if stats.PulledUpSubqueries != 1 {
		t.Errorf("期望PulledUpSubqueries为1，实际为%d", stats.PulledUpSubqueries)
	}

	t.Logf("Apply节点优化成功: %s", join.String())
}

func TestSubqueryOptimizerFlattensUncorrelatedApplyJoinConditions(t *testing.T) {
	optimizer := NewSubqueryOptimizer()
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{
			children: []LogicalPlan{
				&LogicalTableScan{Table: &metadata.Table{Name: "apply_outer"}},
				&LogicalTableScan{Table: &metadata.Table{Name: "apply_inner"}},
			},
		},
		ApplyType: "INNER",
		JoinConds: []Expression{&BinaryOperation{
			Op:    OpAnd,
			Left:  &BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_outer.id"}, Right: &Column{Name: "apply_inner.id"}},
			Right: &BinaryOperation{Op: OpEQ, Left: &Column{Name: "apply_outer.tenant_id"}, Right: &Column{Name: "apply_inner.tenant_id"}},
		}},
	}

	optimized, ok := optimizer.optimizeApplyNode(apply).(*LogicalJoin)
	if !ok {
		t.Fatalf("optimized plan = %T, want LogicalJoin", optimizer.optimizeApplyNode(apply))
	}
	if len(optimized.Conditions) != 2 {
		t.Fatalf("join conditions = %d, want two conjunctive equality conditions", len(optimized.Conditions))
	}
}

func TestSubqueryOptimizerPreservesUncorrelatedSemiAndAntiApply(t *testing.T) {
	for _, applyType := range []string{"SEMI", "ANTI"} {
		t.Run(applyType, func(t *testing.T) {
			optimizer := NewSubqueryOptimizer()
			apply := &LogicalApply{
				BaseLogicalPlan: BaseLogicalPlan{
					children: []LogicalPlan{
						&LogicalTableScan{Table: &metadata.Table{Name: "outer_rows"}},
						&LogicalTableScan{Table: &metadata.Table{Name: "inner_rows"}},
					},
				},
				ApplyType:  applyType,
				Correlated: false,
			}

			optimized := optimizer.optimizeApplyNode(apply)
			preserved, ok := optimized.(*LogicalApply)
			if !ok {
				t.Fatalf("%s Apply changed into %T and lost semi/anti semantics", applyType, optimized)
			}
			if preserved.ApplyType != applyType || len(preserved.Children()) != 2 {
				t.Fatalf("preserved %s Apply = type %q with %d children", applyType, preserved.ApplyType, len(preserved.Children()))
			}
		})
	}
}

func TestSubqueryOptimizerPreservesApplyWithInvalidArity(t *testing.T) {
	optimizer := NewSubqueryOptimizer()
	apply := &LogicalApply{
		BaseLogicalPlan: BaseLogicalPlan{children: []LogicalPlan{
			&LogicalTableScan{Table: &metadata.Table{Name: "outer_only"}},
		}},
		ApplyType:  "INNER",
		Correlated: false,
	}

	optimized := optimizer.optimizeApplyNode(apply)
	preserved, ok := optimized.(*LogicalApply)
	if !ok {
		t.Fatalf("invalid-arity Apply changed into %T, want original LogicalApply", optimized)
	}
	if len(preserved.Children()) != 1 {
		t.Fatalf("preserved Apply children = %d, want 1", len(preserved.Children()))
	}
}

// TestSubqueryOptimizer_ComplexQuery 测试复杂查询优化
func TestSubqueryOptimizer_ComplexQuery(t *testing.T) {
	optimizer := NewSubqueryOptimizer()

	// 创建复杂查询计划
	// SELECT * FROM t1
	// WHERE t1.id IN (SELECT t2.id FROM t2 WHERE t2.value > 100)
	//   AND EXISTS (SELECT 1 FROM t3 WHERE t3.id = t1.id)

	inSubquery := &LogicalSubquery{
		SubqueryType: "IN",
		Correlated:   false,
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{Table: &metadata.Table{Name: "t2"}},
				},
			},
			Conditions: []Expression{
				&BinaryOperation{
					Op:    OpGT,
					Left:  &Column{Name: "t2_value"},
					Right: &Constant{Value: 100},
				},
			},
		},
	}

	existsSubquery := &LogicalSubquery{
		SubqueryType: "EXISTS",
		Correlated:   true,
		OuterRefs:    []string{"id"},
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{Table: &metadata.Table{Name: "t3"}},
				},
			},
			Conditions: []Expression{
				&BinaryOperation{
					Op:    OpEQ,
					Left:  &Column{Name: "t3_id"},
					Right: &Column{Name: "t1_id"},
				},
			},
		},
	}

	// 优化IN子查询
	optimizedIn := optimizer.optimizeSubqueryNode(inSubquery)
	t.Logf("IN子查询优化: %s", optimizedIn.String())

	// 优化EXISTS子查询
	optimizedExists := optimizer.optimizeSubqueryNode(existsSubquery)
	t.Logf("EXISTS子查询优化: %s", optimizedExists.String())

	// 验证优化结果：IN -> SEMI，EXISTS -> SEMI/INNER（直接调用 optimizeSubqueryNode 不经过 Optimize()，故不更新 GetStats）
	if optimizedIn == nil {
		t.Error("IN 子查询优化结果不应为空")
	}
	if optimizedExists == nil {
		t.Error("EXISTS 子查询优化结果不应为空")
	}
	stats := optimizer.GetStats()
	t.Logf("优化统计: 总子查询=%d, IN转SEMI JOIN=%d, EXISTS转SEMI JOIN=%d (注: 直接调用 optimizeSubqueryNode 时统计未更新)",
		stats.TotalSubqueries,
		stats.InToSemiJoin,
		stats.ExistsToSemiJoin)
}

func TestSubqueryOptimizerPreservesStandaloneSubqueryBoundary(t *testing.T) {
	optimizer := NewSubqueryOptimizer()
	subquery := &LogicalSubquery{
		SubqueryType: "IN",
		Subplan: &LogicalSelection{
			BaseLogicalPlan: BaseLogicalPlan{
				children: []LogicalPlan{
					&LogicalTableScan{Table: &metadata.Table{Name: "items"}},
				},
			},
			Conditions: []Expression{
				&BinaryOperation{
					Op:    OpGT,
					Left:  &Column{Name: "price"},
					Right: &Constant{Value: 10},
				},
			},
		},
	}

	optimized := optimizer.Optimize(subquery)
	if _, ok := optimized.(*LogicalSubquery); !ok {
		t.Fatalf("standalone subquery changed into an invalid outer plan %T", optimized)
	}
	if _, ok := subquery.Subplan.(*LogicalSelection); !ok {
		t.Fatalf("subquery inner plan was not preserved, got %T", subquery.Subplan)
	}
}

func TestSubqueryOptimizerDoesNotDecorrelateWithoutOuterRowSource(t *testing.T) {
	optimizer := NewSubqueryOptimizer()
	subquery := &LogicalSubquery{
		SubqueryType: "EXISTS",
		Correlated:   true,
		OuterRefs:    []string{"id"},
		Subplan:      &LogicalTableScan{Table: &metadata.Table{Name: "items"}},
	}

	optimized := optimizer.optimizeSubqueryNode(subquery)
	if _, ok := optimized.(*LogicalApply); ok {
		t.Fatal("correlated standalone subquery became a one-child Apply")
	}
	if optimized != subquery {
		t.Fatalf("correlated standalone subquery changed into %T", optimized)
	}
}

func TestSubqueryOptimizerTraversesCTEQueryField(t *testing.T) {
	optimizer := NewSubqueryOptimizer()
	inner := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan:      &LogicalTableScan{Table: &metadata.Table{Name: "cte_inner"}},
	}
	cte := &LogicalCTE{
		Name:  "source_cte",
		Query: inner,
	}

	optimized := optimizer.Optimize(cte)
	got, ok := optimized.(*LogicalCTE)
	if !ok || got.Query != inner {
		t.Fatalf("optimized CTE query = %#v, want original nested subquery", got.Query)
	}
	if stats := optimizer.GetStats(); stats.TotalSubqueries != 1 {
		t.Fatalf("subquery count = %d, want 1", stats.TotalSubqueries)
	}
}

func TestSubqueryOptimizerTraversesRecursiveCTEFieldsWithoutDoubleCountingMirrors(t *testing.T) {
	optimizer := NewSubqueryOptimizer()
	anchorSubquery := &LogicalSubquery{
		SubqueryType: "SCALAR",
		Subplan:      &LogicalTableScan{Table: &metadata.Table{Name: "anchor_inner"}},
	}
	recursiveSubquery := &LogicalSubquery{
		SubqueryType: "EXISTS",
		Subplan:      &LogicalTableScan{Table: &metadata.Table{Name: "recursive_inner"}},
	}
	cte := &LogicalRecursiveCTE{
		Name:      "tree_cte",
		Anchor:    anchorSubquery,
		Recursive: recursiveSubquery,
	}
	cte.SetChildren([]LogicalPlan{anchorSubquery, recursiveSubquery})

	optimized := optimizer.Optimize(cte)
	if _, ok := optimized.(*LogicalRecursiveCTE); !ok {
		t.Fatalf("optimized recursive CTE = %T, want *LogicalRecursiveCTE", optimized)
	}
	if stats := optimizer.GetStats(); stats.TotalSubqueries != 2 {
		t.Fatalf("subquery count = %d, want 2", stats.TotalSubqueries)
	}
}
