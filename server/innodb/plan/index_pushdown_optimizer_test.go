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

	// 验证选择了idx_col1索引
	if candidate.Index.Name != "idx_col1" {
		t.Errorf("Expected idx_col1, got %s", candidate.Index.Name)
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

	// 由于col1是范围查询，col2不应该被使用（如果选择了idx_col1_col2_col3）
	if candidate.Index.Name == "idx_col1_col2_col3" && candidate.KeyLength > 1 {
		t.Error("Expected only col1 to be used due to range query boundary")
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
