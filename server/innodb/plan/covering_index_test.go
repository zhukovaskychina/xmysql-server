package plan

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// TestIsCoveringIndex_AllSelectColumnsInIndex_ReturnsTrue 所有需要的列都在索引中时应为覆盖索引
func TestIsCoveringIndex_AllSelectColumnsInIndex_ReturnsTrue(t *testing.T) {
	table := buildTableWithPrimaryAndSecondary(t)
	idx, _ := table.GetIndex("idx_a_b")

	requiredColumns := []string{"a", "b"}
	got := IsCoveringIndex(table, idx, requiredColumns)
	if !got {
		t.Errorf("IsCoveringIndex(table, idx_a_b, %v) = false, want true", requiredColumns)
	}
}

// TestIsCoveringIndex_SelectColumnNotInIndex_ReturnsFalse 需要的列不在索引中时不应为覆盖索引
func TestIsCoveringIndex_SelectColumnNotInIndex_ReturnsFalse(t *testing.T) {
	table := buildTableWithPrimaryAndSecondary(t)
	idx, _ := table.GetIndex("idx_a_b")

	requiredColumns := []string{"a", "c"}
	got := IsCoveringIndex(table, idx, requiredColumns)
	if got {
		t.Errorf("IsCoveringIndex(table, idx_a_b, %v) = true, want false (c not in index)", requiredColumns)
	}
}

// TestIsCoveringIndex_SecondaryIndexWithPK_ReturnsTrue 二级索引+主键列可覆盖
func TestIsCoveringIndex_SecondaryIndexWithPK_ReturnsTrue(t *testing.T) {
	table := buildTableWithPrimaryAndSecondary(t)
	idx, _ := table.GetIndex("idx_a_b")

	requiredColumns := []string{"a", "b", "id"}
	got := IsCoveringIndex(table, idx, requiredColumns)
	if !got {
		t.Errorf("IsCoveringIndex(table, idx_a_b, %v) = false, want true (index cols + implicit PK)", requiredColumns)
	}
}

// TestIsCoveringIndex_SelectStar_ReturnsFalse SELECT * 不能为覆盖
func TestIsCoveringIndex_SelectStar_ReturnsFalse(t *testing.T) {
	table := buildTableWithPrimaryAndSecondary(t)
	idx, _ := table.GetIndex("idx_a_b")

	requiredColumns := []string{"*"}
	got := IsCoveringIndex(table, idx, requiredColumns)
	if got {
		t.Errorf("IsCoveringIndex(table, idx_a_b, [\"*\"]) = true, want false")
	}
}

// TestIsCoveringIndex_PrimaryIndex_CoversAllColumns 主键索引包含所有列时可为覆盖
func TestIsCoveringIndex_PrimaryIndex_CoversAllColumns(t *testing.T) {
	table := buildTableWithPrimaryAndSecondary(t)
	pk, _ := table.GetIndex("PRIMARY")

	requiredColumns := []string{"id", "a", "b", "c"}
	got := IsCoveringIndex(table, pk, requiredColumns)
	if !got {
		t.Errorf("IsCoveringIndex(table, PRIMARY, all columns) = false, want true")
	}
}

// TestIsCoveringIndex_EmptyRequired_ReturnsFalse 空需求列视为不覆盖
func TestIsCoveringIndex_EmptyRequired_ReturnsFalse(t *testing.T) {
	table := buildTableWithPrimaryAndSecondary(t)
	idx, _ := table.GetIndex("idx_a_b")

	got := IsCoveringIndex(table, idx, nil)
	if got {
		t.Errorf("IsCoveringIndex(table, idx, nil) = true, want false")
	}
	got = IsCoveringIndex(table, idx, []string{})
	if got {
		t.Errorf("IsCoveringIndex(table, idx, []) = true, want false")
	}
}

func buildTableWithPrimaryAndSecondary(t *testing.T) *metadata.Table {
	t.Helper()
	table := metadata.NewTable("t1")
	table.AddColumn(&metadata.Column{Name: "id", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "a", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "b", DataType: metadata.TypeInt})
	table.AddColumn(&metadata.Column{Name: "c", DataType: metadata.TypeVarchar})

	pk := &metadata.Index{
		Name:      "PRIMARY",
		Columns:   []string{"id"},
		IsPrimary: true,
		IsUnique:  true,
		Table:     table,
	}
	if err := table.AddIndex(pk); err != nil {
		t.Fatalf("add primary: %v", err)
	}

	sec := &metadata.Index{
		Name:    "idx_a_b",
		Columns: []string{"a", "b"},
		Table:   table,
	}
	if err := table.AddIndex(sec); err != nil {
		t.Fatalf("add secondary: %v", err)
	}
	return table
}
