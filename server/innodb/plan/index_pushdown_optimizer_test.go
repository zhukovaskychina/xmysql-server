package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestSingleColumnEquality 测试单列等值条件
func TestSingleColumnEquality(t *testing.T) {
	// 创建测试表
	table := createTestTable()

	// 创建优化器
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// 创建WHERE条件：col1 = 1
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(1)},
		},
	}

	// 优化索引访问
	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"col1", "col2"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate, got nil")
	}

	// 验证选择了包含 col1 的索引（单列 idx_col1 或复合 idx_col1_col2_col3 均可用于 col1=1）
	if candidate.Index.Name != "idx_col1" && candidate.Index.Name != "idx_col1_col2_col3" {
		t.Errorf("Expected idx_col1 or idx_col1_col2_col3, got %s", candidate.Index.Name)
	}

	// 验证条件被下推
	if len(candidate.Conditions) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(candidate.Conditions))
	}

	// 验证选择性
	if candidate.Selectivity > 0.2 {
		t.Errorf("Expected selectivity < 0.2, got %f", candidate.Selectivity)
	}
}

// TestSelectivity_RangeWithColumnStatsNoMinMax OPT-017：列有 DistinctCount 但无 MinValue/MaxValue 时，范围条件选择性回退为默认 0.3
func TestSelectivity_RangeWithColumnStatsNoMinMax(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	// 仅设置 DistinctCount，不设置 MinValue/MaxValue（模拟尚未收集范围统计）
	opt.SetStatistics(
		map[string]*TableStats{"test_table": {TableName: "test_table", RowCount: 10000}},
		map[string]*IndexStats{"idx_col1": {IndexName: "idx_col1", Cardinality: 100}},
		map[string]*ColumnStats{
			"col1": {ColumnName: "col1", DistinctCount: 100, NotNullCount: 10000},
		},
	)
	conditions := []Expression{
		&BinaryOperation{Op: OpGT, Left: &Column{Name: "col1"}, Right: &Constant{Value: int64(10)}},
	}
	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"col1"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}
	if candidate == nil {
		t.Fatal("Expected index candidate for col1 > 10")
	}
	// 无 Min/Max 时 estimateRangeByMinMax 返回 0.3
	if candidate.Selectivity < 0.25 || candidate.Selectivity > 0.35 {
		t.Errorf("Expected range selectivity ~0.3 when column has no Min/Max, got %f", candidate.Selectivity)
	}
}

// TestMultiColumnPrefix 测试多列前缀匹配
func TestMultiColumnPrefix(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE col1 = 1 AND col2 = 2
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(1)},
		},
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col2"},
			Right: &Constant{Value: int64(2)},
		},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"col1", "col2", "col3"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate, got nil")
	}

	// 应该选择复合索引idx_col1_col2_col3
	if candidate.Index.Name != "idx_col1_col2_col3" {
		t.Errorf("Expected idx_col1_col2_col3, got %s", candidate.Index.Name)
	}

	// 应该使用2个索引列
	if candidate.KeyLength != 2 {
		t.Errorf("Expected KeyLength=2, got %d", candidate.KeyLength)
	}
}

func TestQualifiedColumnUsesCompositeIndexPrefix(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "test_table.col1"},
			Right: &Constant{Value: int64(1)},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}
	if candidate == nil || candidate.KeyLength != 1 {
		t.Fatalf("qualified column candidate = %#v, want one-column index prefix", candidate)
	}
	if candidate.Selectivity != 0.01 {
		t.Fatalf("qualified column selectivity = %v, want 0.01 from col1 NDV", candidate.Selectivity)
	}
}

// TestCoveringIndex 测试覆盖索引
func TestCoveringIndex(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE col1 = 1，SELECT col1, col2
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(1)},
		},
	}

	selectColumns := []string{"col1", "col2"}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, selectColumns)
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate, got nil")
	}

	// 复合索引idx_col1_col2_col3应该能覆盖col1和col2
	if !candidate.CoverIndex {
		t.Error("Expected covering index, got non-covering")
	}

	// 覆盖索引的原因应该包含"覆盖索引"
	if candidate.Reason == "" || !contains(candidate.Reason, "覆盖索引") {
		t.Errorf("Expected reason to contain '覆盖索引', got: %s", candidate.Reason)
	}
}

func TestCoveringIndexAcceptsQualifiedCaseInsensitiveColumns(t *testing.T) {
	table := createTestTable()
	index := table.Indices[2]

	if !IsCoveringIndex(table, index, []string{"TEST_TABLE.COL1", "`test_table`.`COL2`"}) {
		t.Fatal("qualified mixed-case columns should be covered by the composite index")
	}
}

// TestLikePrefixMatch 测试LIKE前缀匹配
func TestLikePrefixMatch(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE name LIKE 'abc%'
	conditions := []Expression{
		&Function{
			FuncName: "LIKE",
			FuncArgs: []Expression{
				&Column{Name: "name"},
				&Constant{Value: "abc%"},
			},
		},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"name"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate for LIKE prefix match")
	}

	// 验证条件可以下推
	if len(candidate.Conditions) == 0 {
		t.Error("Expected LIKE condition to be pushed down")
	}

	if !candidate.Conditions[0].CanPush {
		t.Error("Expected LIKE prefix match to be pushable")
	}
}

// TestLikeFuzzyMatch 测试LIKE模糊匹配（不可下推）
func TestLikeFuzzyMatch(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE name LIKE '%abc%'
	conditions := []Expression{
		&Function{
			FuncName: "LIKE",
			FuncArgs: []Expression{
				&Column{Name: "name"},
				&Constant{Value: "%abc%"},
			},
		},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"name"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	// LIKE模糊匹配不应该选择索引
	if candidate != nil && len(candidate.Conditions) > 0 && candidate.Conditions[0].CanPush {
		t.Error("Expected LIKE fuzzy match NOT to be pushed down")
	}
}

func TestLikeWithWildcardAfterPrefixIsNotPushable(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpLike, Left: &Column{Name: "name"}, Right: &Constant{Value: "abc%def"}},
	}, []string{"name"})
	if err != nil {
		t.Fatalf("LIKE middle-wildcard optimization failed: %v", err)
	}
	if candidate != nil && len(candidate.Conditions) > 0 && candidate.Conditions[0].CanPush {
		t.Fatalf("LIKE middle-wildcard candidate = %#v, want non-pushable residual filtering", candidate.Conditions[0])
	}
}

// TestInCondition 测试IN条件
func TestInCondition(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE col1 IN (1, 2, 3)
	conditions := []Expression{
		&Function{
			FuncName: "IN",
			FuncArgs: []Expression{
				&Column{Name: "col1"},
				&Constant{Value: int64(1)},
				&Constant{Value: int64(2)},
				&Constant{Value: int64(3)},
			},
		},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"col1"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate for IN condition")
	}

	// IN条件应该可以下推
	if !candidate.Conditions[0].CanPush {
		t.Error("Expected IN condition to be pushable")
	}
}

func TestStructuredRangeAndNullPredicatesUseIndexes(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	rangeCandidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BetweenExpression{Column: &Column{Name: "col1"}, Lower: int64(10), Upper: int64(20)},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("BETWEEN optimization failed: %v", err)
	}
	if rangeCandidate == nil || len(rangeCandidate.Conditions) != 2 {
		t.Fatalf("BETWEEN candidate = %#v, want two range conditions", rangeCandidate)
	}

	nullCandidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&IsNullExpression{Column: &Column{Name: "col1"}, IsNull: true},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("IS NULL optimization failed: %v", err)
	}
	if nullCandidate == nil || len(nullCandidate.Conditions) != 1 || nullCandidate.Conditions[0].Operator != "is_null" {
		t.Fatalf("IS NULL candidate = %#v, want one IS NULL condition", nullCandidate)
	}
}

func TestStructuredRangeWithConstantExpressionsUsesIndexes(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BetweenExpression{
			Column: &Column{Name: "col1"},
			LowerExpr: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Constant{Value: int64(2)},
				Right: &Constant{Value: int64(3)},
			},
			UpperExpr: &Function{
				FuncName: "CAST",
				CastType: "SIGNED",
				FuncArgs: []Expression{
					&Constant{Value: "20"},
					&Constant{Value: "SIGNED"},
				},
			},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("expression BETWEEN optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 2 {
		t.Fatalf("candidate = %#v, want two range conditions", candidate)
	}
	if candidate.Conditions[0].Value != int64(5) || candidate.Conditions[1].Value != int64(20) {
		t.Fatalf("conditions = %#v, want bounds 5 and 20", candidate.Conditions)
	}
}

func TestStructuredIsNotNullPredicateKeepsItsMeaning(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&IsNullExpression{Column: &Column{Name: "col1"}, IsNull: false},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("IS NOT NULL optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("IS NOT NULL candidate = %#v, want one condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != "is_not_null" {
		t.Fatalf("IS NOT NULL operator = %q, want is_not_null", condition.Operator)
	}
	if !condition.CanPush {
		t.Fatal("IS NOT NULL condition should be pushable")
	}
}

func TestNullSafeEqualityCanUseAnIndexWithoutChangingPredicateSemantics(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpNullSafeEQ, Left: &Column{Name: "col1"}, Right: &Constant{Value: nil}},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("NULL-safe equality optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("NULL-safe equality candidate = %#v, want one condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != "<=>" || !condition.CanPush {
		t.Fatalf("NULL-safe equality condition = %#v, want pushable <=>", condition)
	}
}

func TestStructuredInAndLikePredicatesUseIndexes(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	inCandidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&InExpression{Column: &Column{Name: "col1"}, Values: []interface{}{int64(1), int64(2)}},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("IN optimization failed: %v", err)
	}
	if inCandidate == nil || len(inCandidate.Conditions) != 1 || inCandidate.Conditions[0].Operator != "IN" {
		t.Fatalf("IN candidate = %#v, want one IN condition", inCandidate)
	}

	likeCandidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&LikeExpression{Column: &Column{Name: "name"}, Pattern: "abc%"},
	}, []string{"name"})
	if err != nil {
		t.Fatalf("LIKE optimization failed: %v", err)
	}
	if likeCandidate == nil || len(likeCandidate.Conditions) != 1 || !likeCandidate.Conditions[0].CanPush {
		t.Fatalf("LIKE candidate = %#v, want one pushable condition", likeCandidate)
	}
}

func TestBinaryLikePredicatesUseIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpLike, Left: &Column{Name: "name"}, Right: &Constant{Value: "abc%"}},
	}, []string{"name"})
	if err != nil {
		t.Fatalf("binary LIKE optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("binary LIKE candidate = %#v, want one condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != "LIKE" || !condition.CanPush {
		t.Fatalf("binary LIKE condition = %#v, want pushable LIKE", condition)
	}
}

func TestBinaryLikeEscapeDoesNotPushLiteralWildcardAsPrefix(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:     OpLike,
			Left:   &Column{Name: "name"},
			Right:  &Constant{Value: "abc#%"},
			Escape: &Constant{Value: "#"},
		},
	}, []string{"name"})
	if err != nil {
		t.Fatalf("binary LIKE ESCAPE optimization failed: %v", err)
	}
	if candidate != nil {
		t.Fatalf("binary LIKE ESCAPE candidate = %#v, want no prefix pushdown for literal wildcard", candidate)
	}
}

func TestBinaryNotInPredicatesUseIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpNotIn, Left: &Column{Name: "col1"}, Right: &Constant{Value: []interface{}{int64(1), int64(2)}}},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("binary NOT IN optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("binary NOT IN candidate = %#v, want one condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != "NOT IN" || !condition.CanPush {
		t.Fatalf("binary NOT IN condition = %#v, want pushable NOT IN", condition)
	}
}

func TestNormalizedNotInPredicatesUseIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)
	normalized := NewExpressionNormalizer().Normalize(&BinaryOperation{
		Op:    OpNotIn,
		Left:  &Column{Name: "col1"},
		Right: &Constant{Value: []interface{}{int64(1), int64(2)}},
	})

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{normalized}, []string{"col1"})
	if err != nil {
		t.Fatalf("normalized NOT IN optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 2 {
		t.Fatalf("normalized NOT IN candidate = %#v, want two conditions", candidate)
	}
	for _, condition := range candidate.Conditions {
		if condition.Operator != "!=" || !condition.CanPush {
			t.Fatalf("normalized NOT IN condition = %#v, want pushable !=", condition)
		}
	}
}

func TestBinaryNotLikePredicatesUseIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpNotLike, Left: &Column{Name: "name"}, Right: &Constant{Value: "abc%"}},
	}, []string{"name"})
	if err != nil {
		t.Fatalf("binary NOT LIKE optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("binary NOT LIKE candidate = %#v, want one condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != "NOT LIKE" || !condition.CanPush {
		t.Fatalf("binary NOT LIKE condition = %#v, want pushable NOT LIKE", condition)
	}
}

func TestBinaryInPredicatesRequireConstantList(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpIn, Left: &Column{Name: "col1"}, Right: &Constant{Value: int64(1)}},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("invalid binary IN optimization failed: %v", err)
	}
	if candidate != nil {
		t.Fatalf("binary IN candidate = %#v, want no candidate for scalar RHS", candidate)
	}
}

func TestConstantLeftComparisonUsesReversedIndexCondition(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{Op: OpLT, Left: &Constant{Value: int64(10)}, Right: &Column{Name: "col1"}},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant-left comparison optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("constant-left candidate = %#v, want one condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Column != "col1" || condition.Operator != ">" || condition.Value != int64(10) || !condition.CanPush {
		t.Fatalf("constant-left condition = %#v, want col1 > 10 pushdown", condition)
	}
}

// TestRangeQueryBoundary 测试范围查询边界
func TestRangeQueryBoundary(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE col1 > 10 AND col2 = 20
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(10)},
		},
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col2"},
			Right: &Constant{Value: int64(20)},
		},
	}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, []string{"col1", "col2"})
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate")
	}

	// ICP：范围条件后仍可使用后续列的等值条件（col1>10 AND col2=20 可同时下推），故 KeyLength 可为 1 或 2 均正确
	if candidate.Index.Name == "idx_col1_col2_col3" && (candidate.KeyLength < 1 || candidate.KeyLength > 2) {
		t.Errorf("Expected key length 1 or 2 for idx_col1_col2_col3, got %d", candidate.KeyLength)
	}
}

// TestSecondaryIndexWithPrimaryKey 测试二级索引隐式包含主键
func TestSecondaryIndexWithPrimaryKey(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	// WHERE col1 = 1，SELECT col1, id（id是主键）
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(1)},
		},
	}

	selectColumns := []string{"col1", "id"}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, selectColumns)
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate")
	}

	// 二级索引应该能覆盖（因为隐式包含主键）
	if !candidate.CoverIndex {
		t.Error("Expected secondary index to cover (with implicit primary key)")
	}
}

// TestSelectStar 测试SELECT *不能被覆盖
func TestSelectStar(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	conditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "col1"},
			Right: &Constant{Value: int64(1)},
		},
	}

	selectColumns := []string{"*"}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, selectColumns)
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate")
	}

	// SELECT *不应该被覆盖
	if candidate.CoverIndex {
		t.Error("Expected SELECT * NOT to be covered")
	}
}

// 辅助函数

func createTestTable() *metadata.Table {
	table := metadata.NewTable("test_table")

	// 添加列
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "col1", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "col2", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "col3", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})

	// 添加主键索引
	pkIndex := &metadata.Index{
		Name:      "PRIMARY",
		Columns:   []string{"id"},
		IsPrimary: true,
		IsUnique:  true,
		Table:     table,
	}
	table.AddIndex(pkIndex)

	// 添加单列索引
	idx1 := &metadata.Index{
		Name:    "idx_col1",
		Columns: []string{"col1"},
		Table:   table,
	}
	table.AddIndex(idx1)

	// 添加复合索引
	idx2 := &metadata.Index{
		Name:    "idx_col1_col2_col3",
		Columns: []string{"col1", "col2", "col3"},
		Table:   table,
	}
	table.AddIndex(idx2)

	// 添加name索引
	idx3 := &metadata.Index{
		Name:    "idx_name",
		Columns: []string{"name"},
		Table:   table,
	}
	table.AddIndex(idx3)

	return table
}

func TestConstantArithmeticExpressionUsesIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpGE,
			Left: &Column{Name: "col1"},
			Right: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Constant{Value: int64(1)},
				Right: &Constant{Value: int64(2)},
			},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant arithmetic index optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one index condition", candidate)
	}
	condition := candidate.Conditions[0]
	numeric, numericOK := numericComparableValue(condition.Value)
	if condition.Operator != ">=" || !numericOK || numeric != 3 || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable col1 >= 3", condition)
	}
}

func TestConstantScalarFunctionExpressionUsesIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpGE,
			Left: &Column{Name: "col1"},
			Right: &Function{FuncName: "ABS", FuncArgs: []Expression{
				&UnaryOperation{Operator: "-", Operand: &Constant{Value: int64(3)}},
			}},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant scalar function index optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one index condition", candidate)
	}
	condition := candidate.Conditions[0]
	numeric, numericOK := numericComparableValue(condition.Value)
	if condition.Operator != ">=" || !numericOK || numeric != 3 || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable col1 >= 3", condition)
	}
}

func TestConstantCastExpressionUsesIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpGE,
			Left: &Column{Name: "col1"},
			Right: &Function{
				FuncName: "CAST",
				CastType: "SIGNED",
				FuncArgs: []Expression{
					&Constant{Value: "42"},
					&Constant{Value: "SIGNED"},
				},
			},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant CAST index optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one index condition", candidate)
	}
	condition := candidate.Conditions[0]
	numeric, numericOK := numericComparableValue(condition.Value)
	if condition.Operator != ">=" || !numericOK || numeric != 42 || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable col1 >= 42", condition)
	}
}

func TestConstantExpressionTupleUsesInIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpIn,
			Left: &Column{Name: "col1"},
			Right: &TupleExpression{Exprs: []Expression{
				&BinaryOperation{
					Op:    OpAdd,
					Left:  &Constant{Value: int64(2)},
					Right: &Constant{Value: int64(3)},
				},
				&Function{
					FuncName: "CAST",
					CastType: "SIGNED",
					FuncArgs: []Expression{
						&Constant{Value: "20"},
						&Constant{Value: "SIGNED"},
					},
				},
			}},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant tuple IN optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one IN condition", candidate)
	}
	condition := candidate.Conditions[0]
	values, valuesOK := condition.Value.([]interface{})
	if condition.Operator != "IN" || !valuesOK || len(values) != 2 || values[0] != int64(5) || values[1] != int64(20) || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable col1 IN (5, 20)", condition)
	}
}

func TestConstantCaseExpressionUsesIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpGE,
			Left: &Column{Name: "col1"},
			Right: &CaseExpression{
				Whens: []CaseWhen{{
					Condition: &BinaryOperation{
						Op:    OpEQ,
						Left:  &Constant{Value: int64(1)},
						Right: &Constant{Value: int64(1)},
					},
					Value: &Constant{Value: int64(7)},
				}},
				Else: &Constant{Value: int64(8)},
			},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant CASE index optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one index condition", candidate)
	}
	condition := candidate.Conditions[0]
	numeric, numericOK := numericComparableValue(condition.Value)
	if condition.Operator != ">=" || !numericOK || numeric != 7 || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable col1 >= 7", condition)
	}
}

func TestConstantDateFunctionExpressionUsesIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpGE,
			Left: &Column{Name: "col1"},
			Right: &Function{FuncName: "DATE_FORMAT", FuncArgs: []Expression{
				&Constant{Value: "2024-01-15"},
				&Constant{Value: "%Y-%m-%d"},
			}},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant DATE_FORMAT index optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one index condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != ">=" || condition.Value != "2024-01-15" || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable col1 >= 2024-01-15", condition)
	}
}

func TestConstantDigestFunctionExpressionUsesIndexPushdown(t *testing.T) {
	table := createTestTable()
	opt := NewIndexPushdownOptimizer()
	setupTestStatistics(opt)

	candidate, err := opt.OptimizeIndexAccess(table, []Expression{
		&BinaryOperation{
			Op:   OpEQ,
			Left: &Column{Name: "col1"},
			Right: &Function{FuncName: "MD5", FuncArgs: []Expression{
				&Constant{Value: "abc"},
			}},
		},
	}, []string{"col1"})
	if err != nil {
		t.Fatalf("constant MD5 index optimization failed: %v", err)
	}
	if candidate == nil || len(candidate.Conditions) != 1 {
		t.Fatalf("candidate = %#v, want one index condition", candidate)
	}
	condition := candidate.Conditions[0]
	if condition.Operator != "=" || condition.Value != "900150983cd24fb0d6963f7d28e17f72" || !condition.CanPush {
		t.Fatalf("condition = %#v, want pushable MD5 equality", condition)
	}
}

func setupTestStatistics(opt *IndexPushdownOptimizer) {
	tableStats := map[string]*TableStats{
		"test_table": {
			TableName: "test_table",
			RowCount:  10000,
		},
	}

	columnStats := map[string]*ColumnStats{
		"col1": {
			ColumnName:    "col1",
			DistinctCount: 100,
			NotNullCount:  10000,
		},
		"col2": {
			ColumnName:    "col2",
			DistinctCount: 50,
			NotNullCount:  10000,
		},
		"col3": {
			ColumnName:    "col3",
			DistinctCount: 200,
			NotNullCount:  10000,
		},
		"name": {
			ColumnName:    "name",
			DistinctCount: 5000,
			NotNullCount:  10000,
		},
	}

	indexStats := map[string]*IndexStats{
		"idx_col1": {
			IndexName:   "idx_col1",
			Cardinality: 100,
		},
		"idx_col1_col2_col3": {
			IndexName:   "idx_col1_col2_col3",
			Cardinality: 5000,
		},
		"idx_name": {
			IndexName:   "idx_name",
			Cardinality: 5000,
		},
	}

	opt.SetStatistics(tableStats, indexStats, columnStats)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
