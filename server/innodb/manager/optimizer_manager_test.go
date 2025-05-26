package manager

import (
	"context"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
	"testing"
)

func TestOptimizerManager_GeneratePhysicalPlan(t *testing.T) {
	// 创建模拟的schema管理器
	schemaManager := schemas.NewInfoSchemas()

	// 创建优化器管理器
	optimizer := NewOptimizerManager(schemaManager)

	tests := []struct {
		name         string
		logicalPlan  *plan.LogicalPlan
		wantPlanType PlanType
	}{
		{
			name: "simple table scan",
			logicalPlan: &plan.LogicalPlan{
				TableName:  "test_table",
				Conditions: []string{"id > 10"},
			},
			wantPlanType: PLAN_TYPE_SEQUENTIAL_SCAN,
		},
		{
			name: "index scan",
			logicalPlan: &plan.LogicalPlan{
				TableName:  "test_table",
				Conditions: []string{"id = 1"},
			},
			wantPlanType: PLAN_TYPE_INDEX_SCAN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := optimizer.GeneratePhysicalPlan(ctx, tt.logicalPlan)
			if err != nil {
				t.Errorf("GeneratePhysicalPlan() error = %v", err)
				return
			}
			if got.PlanType != tt.wantPlanType {
				t.Errorf("GeneratePhysicalPlan() planType = %v, want %v", got.PlanType, tt.wantPlanType)
			}
		})
	}
}

func TestOptimizerManager_OptimizeJoinOrder(t *testing.T) {
	// 创建模拟的schema管理器
	schemaManager := schemas.NewInfoSchemas()

	// 创建优化器管理器
	optimizer := NewOptimizerManager(schemaManager)

	tests := []struct {
		name   string
		tables []string
		joins  []JoinCondition
		want   int
	}{
		{
			name:   "two tables join",
			tables: []string{"t1", "t2"},
			joins: []JoinCondition{
				{
					LeftTable:  "t1",
					RightTable: "t2",
					Condition:  "t1.id = t2.id",
				},
			},
			want: 2,
		},
		{
			name:   "three tables join",
			tables: []string{"t1", "t2", "t3"},
			joins: []JoinCondition{
				{
					LeftTable:  "t1",
					RightTable: "t2",
					Condition:  "t1.id = t2.id",
				},
				{
					LeftTable:  "t2",
					RightTable: "t3",
					Condition:  "t2.id = t3.id",
				},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optimizer.OptimizeJoinOrder(tt.tables, tt.joins)
			if len(got) != tt.want {
				t.Errorf("OptimizeJoinOrder() returned order length = %v, want %v", len(got), tt.want)
			}
		})
	}
}

func TestOptimizerManager_CollectTableStats(t *testing.T) {
	// 创建模拟的schema管理器
	schemaManager := schemas.NewInfoSchemas()

	// 创建优化器管理器
	optimizer := NewOptimizerManager(schemaManager)

	tests := []struct {
		name      string
		tableName string
		wantNil   bool
	}{
		{
			name:      "non-existent table",
			tableName: "non_existent",
			wantNil:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optimizer.collectTableStats(tt.tableName)
			if (got == nil) != tt.wantNil {
				t.Errorf("collectTableStats() got = %v, want nil = %v", got, tt.wantNil)
			}
		})
	}
}

func TestOptimizerManager_GenerateAccessPaths(t *testing.T) {
	// 创建模拟的schema管理器
	schemaManager := schemas.NewInfoSchemas()

	// 创建优化器管理器
	optimizer := NewOptimizerManager(schemaManager)

	tests := []struct {
		name        string
		logicalPlan *plan.LogicalPlan
		stats       *TableStats
		wantPaths   int
	}{
		{
			name: "table with no index",
			logicalPlan: &plan.LogicalPlan{
				TableName:  "test_table",
				Conditions: []string{"id > 10"},
			},
			stats: &TableStats{
				RowCount:   1000,
				AvgRowSize: 100,
				IndexStats: make(map[string]*IndexStats),
			},
			wantPaths: 1, // 只有全表扫描路径
		},
		{
			name: "table with one index",
			logicalPlan: &plan.LogicalPlan{
				TableName:  "test_table",
				Conditions: []string{"id = 1"},
			},
			stats: &TableStats{
				RowCount:   1000,
				AvgRowSize: 100,
				IndexStats: map[string]*IndexStats{
					"idx_id": {
						Cardinality: 1000,
						Height:      3,
						IsUnique:    true,
					},
				},
			},
			wantPaths: 2, // 全表扫描 + 索引扫描
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optimizer.generateAccessPaths(tt.logicalPlan, tt.stats)
			if len(got) != tt.wantPaths {
				t.Errorf("generateAccessPaths() returned %v paths, want %v", len(got), tt.wantPaths)
			}
		})
	}
}

func TestOptimizerManager_ChooseBestPath(t *testing.T) {
	// 创建模拟的schema管理器
	schemaManager := schemas.NewInfoSchemas()

	// 创建优化器管理器
	optimizer := NewOptimizerManager(schemaManager)

	tests := []struct {
		name     string
		paths    []AccessPath
		wantCost float64
	}{
		{
			name: "choose lower cost path",
			paths: []AccessPath{
				{
					PlanType: PLAN_TYPE_SEQUENTIAL_SCAN,
					Cost:     1000,
					RowCount: 1000,
				},
				{
					PlanType:  PLAN_TYPE_INDEX_SCAN,
					Cost:      100,
					RowCount:  100,
					IndexName: "idx_id",
				},
			},
			wantCost: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optimizer.chooseBestPath(tt.paths)
			if got.Cost != tt.wantCost {
				t.Errorf("chooseBestPath() returned path with cost = %v, want %v", got.Cost, tt.wantCost)
			}
		})
	}
}
