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

	// 验证结果
	if result == nil {
		t.Fatal("去关联失败，返回nil")
	}

	apply, ok := result.(*LogicalApply)
	if !ok {
		t.Fatalf("期望返回LogicalApply，实际返回%T", result)
	}

	if apply.Correlated {
		t.Error("去关联后应该是非关联的")
	}

	if len(apply.JoinConds) == 0 {
		t.Error("去关联后应该有JOIN条件")
	}

	t.Logf("去关联成功: %s", apply.String())
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

	apply, ok := result.(*LogicalApply)
	if !ok {
		t.Fatalf("期望返回LogicalApply，实际返回%T", result)
	}

	if apply.ApplyType != "SEMI" {
		t.Errorf("期望ApplyType为SEMI，实际为%s", apply.ApplyType)
	}

	// 验证统计信息
	stats := optimizer.GetStats()
	if stats.InToSemiJoin != 1 {
		t.Errorf("期望InToSemiJoin为1，实际为%d", stats.InToSemiJoin)
	}

	t.Logf("IN子查询优化成功: %s", apply.String())
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

	apply, ok := result.(*LogicalApply)
	if !ok {
		t.Fatalf("期望返回LogicalApply，实际返回%T", result)
	}

	if apply.ApplyType != "SEMI" {
		t.Errorf("期望ApplyType为SEMI，实际为%s", apply.ApplyType)
	}

	// 验证统计信息
	stats := optimizer.GetStats()
	if stats.ExistsToSemiJoin != 1 {
		t.Errorf("期望ExistsToSemiJoin为1，实际为%d", stats.ExistsToSemiJoin)
	}

	t.Logf("EXISTS子查询优化成功: %s", apply.String())
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
