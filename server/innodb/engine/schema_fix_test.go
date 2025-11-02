package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestQuerySchema_Creation 测试QuerySchema创建
func TestQuerySchema_Creation(t *testing.T) {
	schema := metadata.NewQuerySchema()
	if schema == nil {
		t.Fatal("NewQuerySchema() returned nil")
	}

	if schema.ColumnCount() != 0 {
		t.Errorf("ColumnCount() = %d, want 0", schema.ColumnCount())
	}

	t.Log("✅ QuerySchema created successfully")
}

// TestQuerySchema_AddColumn 测试添加列
func TestQuerySchema_AddColumn(t *testing.T) {
	schema := metadata.NewQuerySchema()

	// 添加第一列
	col1 := metadata.NewQueryColumn("id", metadata.TypeBigInt)
	schema.AddColumn(col1)

	if schema.ColumnCount() != 1 {
		t.Errorf("ColumnCount() = %d, want 1", schema.ColumnCount())
	}

	// 添加第二列
	col2 := metadata.NewQueryColumn("name", metadata.TypeVarchar)
	schema.AddColumn(col2)

	if schema.ColumnCount() != 2 {
		t.Errorf("ColumnCount() = %d, want 2", schema.ColumnCount())
	}

	t.Logf("✅ Added %d columns", schema.ColumnCount())
}

// TestQuerySchema_GetColumn 测试获取列
func TestQuerySchema_GetColumn(t *testing.T) {
	schema := metadata.NewQuerySchema()

	col1 := metadata.NewQueryColumn("id", metadata.TypeBigInt)
	col2 := metadata.NewQueryColumn("name", metadata.TypeVarchar)
	schema.AddColumn(col1)
	schema.AddColumn(col2)

	// 按名称获取
	if col, ok := schema.GetColumn("id"); !ok {
		t.Error("GetColumn('id') failed")
	} else if col.Name != "id" {
		t.Errorf("GetColumn('id').Name = %s, want 'id'", col.Name)
	}

	// 按索引获取
	if col, ok := schema.GetColumnByIndex(0); !ok {
		t.Error("GetColumnByIndex(0) failed")
	} else if col.Name != "id" {
		t.Errorf("GetColumnByIndex(0).Name = %s, want 'id'", col.Name)
	}

	if col, ok := schema.GetColumnByIndex(1); !ok {
		t.Error("GetColumnByIndex(1) failed")
	} else if col.Name != "name" {
		t.Errorf("GetColumnByIndex(1).Name = %s, want 'name'", col.Name)
	}

	t.Log("✅ Column retrieval works correctly")
}

// TestQuerySchema_FromTable 测试从Table创建QuerySchema
func TestQuerySchema_FromTable(t *testing.T) {
	// 创建测试表
	table := metadata.NewTable("users")
	table.AddColumn(&metadata.Column{
		Name:       "id",
		DataType:   metadata.TypeBigInt,
		IsNullable: false,
	})
	table.AddColumn(&metadata.Column{
		Name:       "name",
		DataType:   metadata.TypeVarchar,
		IsNullable: true,
	})

	// 从Table创建QuerySchema
	schema := metadata.FromTable(table)

	if schema == nil {
		t.Fatal("FromTable() returned nil")
	}

	if schema.ColumnCount() != 2 {
		t.Errorf("ColumnCount() = %d, want 2", schema.ColumnCount())
	}

	if schema.TableName != "users" {
		t.Errorf("TableName = %s, want 'users'", schema.TableName)
	}

	// 验证列信息
	if col, ok := schema.GetColumn("id"); !ok {
		t.Error("Column 'id' not found")
	} else {
		if col.DataType != metadata.TypeBigInt {
			t.Errorf("Column 'id' DataType = %s, want %s", col.DataType, metadata.TypeBigInt)
		}
		if col.IsNullable {
			t.Error("Column 'id' should not be nullable")
		}
	}

	t.Logf("✅ FromTable created schema with %d columns", schema.ColumnCount())
}

// TestQuerySchema_MergeSchemas 测试合并schema
func TestQuerySchema_MergeSchemas(t *testing.T) {
	// 创建第一个schema
	schema1 := metadata.NewQuerySchema()
	schema1.AddColumn(metadata.NewQueryColumn("id", metadata.TypeBigInt))
	schema1.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	// 创建第二个schema
	schema2 := metadata.NewQuerySchema()
	schema2.AddColumn(metadata.NewQueryColumn("age", metadata.TypeInt))
	schema2.AddColumn(metadata.NewQueryColumn("email", metadata.TypeVarchar))

	// 合并
	merged := metadata.MergeSchemas(schema1, schema2)

	if merged.ColumnCount() != 4 {
		t.Errorf("Merged ColumnCount() = %d, want 4", merged.ColumnCount())
	}

	// 验证列顺序
	expectedNames := []string{"id", "name", "age", "email"}
	for i, expectedName := range expectedNames {
		if col, ok := merged.GetColumnByIndex(i); !ok {
			t.Errorf("Column at index %d not found", i)
		} else if col.Name != expectedName {
			t.Errorf("Column at index %d: Name = %s, want %s", i, col.Name, expectedName)
		}
	}

	t.Logf("✅ Merged schema has %d columns", merged.ColumnCount())
}

// TestQuerySchema_ProjectSchema 测试投影schema
func TestQuerySchema_ProjectSchema(t *testing.T) {
	// 创建源schema
	source := metadata.NewQuerySchema()
	source.AddColumn(metadata.NewQueryColumn("id", metadata.TypeBigInt))
	source.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))
	source.AddColumn(metadata.NewQueryColumn("age", metadata.TypeInt))
	source.AddColumn(metadata.NewQueryColumn("email", metadata.TypeVarchar))

	// 投影：选择列 0 和 2 (id 和 age)
	projected := metadata.ProjectSchema(source, []int{0, 2})

	if projected.ColumnCount() != 2 {
		t.Errorf("Projected ColumnCount() = %d, want 2", projected.ColumnCount())
	}

	// 验证投影后的列
	if col, ok := projected.GetColumnByIndex(0); !ok {
		t.Error("Column at index 0 not found")
	} else if col.Name != "id" {
		t.Errorf("Column at index 0: Name = %s, want 'id'", col.Name)
	}

	if col, ok := projected.GetColumnByIndex(1); !ok {
		t.Error("Column at index 1 not found")
	} else if col.Name != "age" {
		t.Errorf("Column at index 1: Name = %s, want 'age'", col.Name)
	}

	t.Logf("✅ Projected schema has %d columns", projected.ColumnCount())
}

// TestQuerySchema_Clone 测试克隆schema
func TestQuerySchema_Clone(t *testing.T) {
	// 创建原始schema
	original := metadata.NewQuerySchema()
	original.TableName = "users"
	original.SchemaName = "test_db"
	original.AddColumn(metadata.NewQueryColumn("id", metadata.TypeBigInt))
	original.AddColumn(metadata.NewQueryColumn("name", metadata.TypeVarchar))

	// 克隆
	cloned := original.Clone()

	if cloned.ColumnCount() != original.ColumnCount() {
		t.Errorf("Cloned ColumnCount() = %d, want %d", cloned.ColumnCount(), original.ColumnCount())
	}

	if cloned.TableName != original.TableName {
		t.Errorf("Cloned TableName = %s, want %s", cloned.TableName, original.TableName)
	}

	if cloned.SchemaName != original.SchemaName {
		t.Errorf("Cloned SchemaName = %s, want %s", cloned.SchemaName, original.SchemaName)
	}

	// 修改克隆的schema不应影响原始schema
	cloned.AddColumn(metadata.NewQueryColumn("age", metadata.TypeInt))

	if original.ColumnCount() == cloned.ColumnCount() {
		t.Error("Modifying cloned schema affected original schema")
	}

	t.Logf("✅ Cloned schema is independent: original=%d cols, cloned=%d cols",
		original.ColumnCount(), cloned.ColumnCount())
}

// TestAggregateFunc_Interface 测试聚合函数接口
func TestAggregateFunc_Interface(t *testing.T) {
	// 测试CountAgg
	countAgg := &CountAgg{}
	if countAgg.Name() != "COUNT" {
		t.Errorf("CountAgg.Name() = %s, want 'COUNT'", countAgg.Name())
	}
	if countAgg.ResultType() != metadata.TypeBigInt {
		t.Errorf("CountAgg.ResultType() = %s, want %s", countAgg.ResultType(), metadata.TypeBigInt)
	}

	// 测试SumAgg
	sumAgg := &SumAgg{}
	if sumAgg.Name() != "SUM" {
		t.Errorf("SumAgg.Name() = %s, want 'SUM'", sumAgg.Name())
	}
	if sumAgg.ResultType() != metadata.TypeDouble {
		t.Errorf("SumAgg.ResultType() = %s, want %s", sumAgg.ResultType(), metadata.TypeDouble)
	}

	// 测试AvgAgg
	avgAgg := &AvgAgg{}
	if avgAgg.Name() != "AVG" {
		t.Errorf("AvgAgg.Name() = %s, want 'AVG'", avgAgg.Name())
	}
	if avgAgg.ResultType() != metadata.TypeDouble {
		t.Errorf("AvgAgg.ResultType() = %s, want %s", avgAgg.ResultType(), metadata.TypeDouble)
	}

	// 测试MinAgg
	minAgg := &MinAgg{}
	if minAgg.Name() != "MIN" {
		t.Errorf("MinAgg.Name() = %s, want 'MIN'", minAgg.Name())
	}
	if minAgg.ResultType() != metadata.TypeDouble {
		t.Errorf("MinAgg.ResultType() = %s, want %s", minAgg.ResultType(), metadata.TypeDouble)
	}

	// 测试MaxAgg
	maxAgg := &MaxAgg{}
	if maxAgg.Name() != "MAX" {
		t.Errorf("MaxAgg.Name() = %s, want 'MAX'", maxAgg.Name())
	}
	if maxAgg.ResultType() != metadata.TypeDouble {
		t.Errorf("MaxAgg.ResultType() = %s, want %s", maxAgg.ResultType(), metadata.TypeDouble)
	}

	t.Log("✅ All aggregate functions implement Name() and ResultType()")
}
