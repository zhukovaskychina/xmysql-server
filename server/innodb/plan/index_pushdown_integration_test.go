package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestComplexConditionCombination 测试复杂组合条件
func TestComplexConditionCombination(t *testing.T) {
	// 创建users表
	table := createUsersTable()
	opt := NewIndexPushdownOptimizer()
	setupUsersStatistics(opt)

	// 查询：SELECT name, age FROM users WHERE age > 18 AND age < 60 AND city = 'Beijing'
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(18)},
		},
		&BinaryOperation{
			Op:    OpLT,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(60)},
		},
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "city"},
			Right: &Constant{Value: "Beijing"},
		},
	}

	selectColumns := []string{"name", "age"}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, selectColumns)
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate")
	}

	// 应该选择idx_age_city索引
	if candidate.Index.Name != "idx_age_city" {
		t.Errorf("Expected idx_age_city, got %s", candidate.Index.Name)
	}

	// 应该下推age和city的条件（age的两个范围条件算作一个）
	if len(candidate.Conditions) < 2 {
		t.Errorf("Expected at least 2 conditions pushed down, got %d", len(candidate.Conditions))
	}

	// 由于缺少name列，不应该是覆盖索引
	if candidate.CoverIndex {
		t.Error("Expected non-covering index (missing name column)")
	}
}

// TestCoveringIndexOptimization 测试覆盖索引优化
func TestCoveringIndexOptimization(t *testing.T) {
	table := createUsersTable()
	opt := NewIndexPushdownOptimizer()
	setupUsersStatistics(opt)

	// 查询：SELECT age, city FROM users WHERE age > 18 AND city = 'Beijing'
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpGT,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(18)},
		},
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "city"},
			Right: &Constant{Value: "Beijing"},
		},
	}

	selectColumns := []string{"age", "city"}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, selectColumns)
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate")
	}

	// 应该选择idx_age_city索引
	if candidate.Index.Name != "idx_age_city" {
		t.Errorf("Expected idx_age_city, got %s", candidate.Index.Name)
	}

	// 应该是覆盖索引（所有需要的列都在索引中）
	if !candidate.CoverIndex {
		t.Error("Expected covering index (all columns in index)")
	}

	// 原因应该包含"覆盖索引"
	if !contains(candidate.Reason, "覆盖索引") {
		t.Errorf("Expected reason to contain '覆盖索引', got: %s", candidate.Reason)
	}

	// 覆盖索引的成本应该更低（无回表成本）
	if candidate.Cost > 100 {
		t.Errorf("Expected lower cost for covering index, got %f", candidate.Cost)
	}
}

// TestIndexMergeScenario 测试索引合并场景
func TestIndexMergeScenario(t *testing.T) {
	table := createUsersTable()
	opt := NewIndexPushdownOptimizer()
	setupUsersStatistics(opt)

	// 注意：当前实现不支持OR条件的直接解析
	// 这里测试独立条件的合并候选生成
	conditions1 := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "name"},
			Right: &Constant{Value: "Alice"},
		},
	}

	conditions2 := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "age"},
			Right: &Constant{Value: int64(25)},
		},
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "city"},
			Right: &Constant{Value: "Shanghai"},
		},
	}

	// 分别获取候选
	candidate1, _ := opt.OptimizeIndexAccess(table, conditions1, []string{"*"})
	candidate2, _ := opt.OptimizeIndexAccess(table, conditions2, []string{"*"})

	if candidate1 == nil || candidate2 == nil {
		t.Skip("Cannot test merge without both candidates")
	}

	// 测试合并逻辑
	merged := opt.mergeCandidates([]*IndexCandidate{candidate1, candidate2})

	if len(merged) == 0 {
		t.Log("No merge candidates generated (expected if conditions overlap)")
	} else {
		// 如果生成了合并候选
		mergedCandidate := merged[0]
		if !contains(mergedCandidate.Reason, "索引合并") {
			t.Error("Expected merge candidate to have '索引合并' reason")
		}

		// 合并候选的成本应该是两个索引成本之和加上合并成本
		expectedMinCost := candidate1.Cost + candidate2.Cost
		if mergedCandidate.Cost < expectedMinCost {
			t.Errorf("Merge cost too low: expected >= %f, got %f",
				expectedMinCost, mergedCandidate.Cost)
		}
	}
}

// TestAggregationWithCoveringIndex 测试聚合函数与覆盖索引
func TestAggregationWithCoveringIndex(t *testing.T) {
	table := createUsersTable()
	opt := NewIndexPushdownOptimizer()
	setupUsersStatistics(opt)

	// 查询：SELECT COUNT(age) FROM users WHERE city = 'Beijing'
	conditions := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "city"},
			Right: &Constant{Value: "Beijing"},
		},
	}

	// 聚合函数的列
	selectColumns := []string{"COUNT(age)"}

	candidate, err := opt.OptimizeIndexAccess(table, conditions, selectColumns)
	if err != nil {
		t.Fatalf("OptimizeIndexAccess failed: %v", err)
	}

	if candidate == nil {
		t.Fatal("Expected index candidate")
	}

	// 应该能够识别为覆盖索引（idx_age_city包含age）
	// 注意：这取决于extractColumnFromExpression的实现
	t.Logf("CoverIndex: %v, Reason: %s", candidate.CoverIndex, candidate.Reason)
}

// TestPerformanceComparison 测试性能对比
func TestPerformanceComparison(t *testing.T) {
	table := createUsersTable()
	opt := NewIndexPushdownOptimizer()
	setupUsersStatistics(opt)

	// 场景1：点查询，覆盖索引
	conditions1 := []Expression{
		&BinaryOperation{
			Op:    OpEQ,
			Left:  &Column{Name: "name"},
			Right: &Constant{Value: "Alice"},
		},
	}
	selectColumns1 := []string{"name", "id"}

	candidate1, _ := opt.OptimizeIndexAccess(table, conditions1, selectColumns1)

	// 场景2：点查询，非覆盖索引
	selectColumns2 := []string{"name", "age", "city"}

	candidate2, _ := opt.OptimizeIndexAccess(table, conditions1, selectColumns2)

	// 比较成本：覆盖索引应该更低
	if candidate1 != nil && candidate2 != nil {
		if candidate1.CoverIndex && !candidate2.CoverIndex {
			if candidate1.Cost >= candidate2.Cost {
				t.Errorf("Expected covering index cost (%f) < non-covering cost (%f)",
					candidate1.Cost, candidate2.Cost)
			}
			t.Logf("Performance gain from covering index: %.2fx",
				candidate2.Cost/candidate1.Cost)
		}
	}
}

// TestSelectivityEstimation 测试选择性估算
func TestSelectivityEstimation(t *testing.T) {
	table := createUsersTable()
	opt := NewIndexPushdownOptimizer()
	setupUsersStatistics(opt)

	testCases := []struct {
		name                string
		condition           Expression
		expectedSelectivity float64
		tolerance           float64
	}{
		{
			name: "Equality on high cardinality",
			condition: &BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "name"},
				Right: &Constant{Value: "Alice"},
			},
			expectedSelectivity: 0.0002, // 1/5000
			tolerance:           0.001,
		},
		{
			name: "Equality on low cardinality",
			condition: &BinaryOperation{
				Op:    OpEQ,
				Left:  &Column{Name: "city"},
				Right: &Constant{Value: "Beijing"},
			},
			expectedSelectivity: 0.01, // 1/100
			tolerance:           0.01,
		},
		{
			name: "Range query",
			condition: &BinaryOperation{
				Op:    OpGT,
				Left:  &Column{Name: "age"},
				Right: &Constant{Value: int64(50)},
			},
			expectedSelectivity: 0.3,
			tolerance:           0.2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			candidate, err := opt.OptimizeIndexAccess(table, []Expression{tc.condition}, []string{"*"})
			if err != nil {
				t.Fatalf("OptimizeIndexAccess failed: %v", err)
			}

			if candidate == nil {
				t.Fatal("Expected index candidate")
			}

			diff := candidate.Selectivity - tc.expectedSelectivity
			if diff < 0 {
				diff = -diff
			}
			if diff > tc.tolerance {
				t.Errorf("Selectivity mismatch: expected ~%f, got %f",
					tc.expectedSelectivity, candidate.Selectivity)
			}
		})
	}
}

// 辅助函数

func createUsersTable() *metadata.Table {
	table := metadata.NewTable("users")

	// 添加列
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "name", DataType: metadata.TypeVarchar})
	table.AddColumn(&metadata.Column{Name: "age", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "city", DataType: metadata.TypeVarchar})

	// 主键
	pkIndex := &metadata.Index{
		Name:      "PRIMARY",
		Columns:   []string{"id"},
		IsPrimary: true,
		IsUnique:  true,
		Table:     table,
	}
	table.AddIndex(pkIndex)

	// 索引
	idxName := &metadata.Index{
		Name:    "idx_name",
		Columns: []string{"name"},
		Table:   table,
	}
	table.AddIndex(idxName)

	idxAgeCity := &metadata.Index{
		Name:    "idx_age_city",
		Columns: []string{"age", "city"},
		Table:   table,
	}
	table.AddIndex(idxAgeCity)

	return table
}

func setupUsersStatistics(opt *IndexPushdownOptimizer) {
	tableStats := map[string]*TableStats{
		"users": {
			TableName: "users",
			RowCount:  1000000,
		},
	}

	columnStats := map[string]*ColumnStats{
		"name": {
			ColumnName:    "name",
			DistinctCount: 5000,
			NotNullCount:  1000000,
		},
		"age": {
			ColumnName:    "age",
			DistinctCount: 80,
			NotNullCount:  1000000,
			MinValue:      int64(0),
			MaxValue:      int64(100),
		},
		"city": {
			ColumnName:    "city",
			DistinctCount: 100,
			NotNullCount:  1000000,
		},
	}

	indexStats := map[string]*IndexStats{
		"idx_name": {
			IndexName:   "idx_name",
			Cardinality: 5000,
		},
		"idx_age_city": {
			IndexName:   "idx_age_city",
			Cardinality: 8000,
		},
	}

	opt.SetStatistics(tableStats, indexStats, columnStats)
}
