package engine

import "testing"

// TestIndexScanOperator_CoveringIndex 测试覆盖索引（不需要回表）
func TestIndexScanOperator_CoveringIndex(t *testing.T) {
	metadata := &IndexMetadata{
		IndexName: "idx_name_email",
		Columns:   []string{"name", "email"},
		IsPrimary: false,
		IsUnique:  false,
	}

	requiredColumns := []string{"name", "email"}
	if !metadata.IsCoveringIndex(requiredColumns) {
		t.Error("Expected covering index, but got false")
	}
}

// TestIndexScanOperator_NonCoveringIndex 测试非覆盖索引（需要回表）
func TestIndexScanOperator_NonCoveringIndex(t *testing.T) {
	metadata := &IndexMetadata{
		IndexName: "idx_name",
		Columns:   []string{"name"},
		IsPrimary: false,
		IsUnique:  false,
	}

	requiredColumns := []string{"name", "email", "age"}
	if metadata.IsCoveringIndex(requiredColumns) {
		t.Error("Expected non-covering index, but got true")
	}
}

// TestIndexScanOperator_PrimaryIndex 测试主键索引（总是覆盖索引）
func TestIndexScanOperator_PrimaryIndex(t *testing.T) {
	metadata := &IndexMetadata{
		IndexName: "PRIMARY",
		Columns:   []string{"id"},
		IsPrimary: true,
		IsUnique:  true,
	}

	if !metadata.IsCoveringIndex([]string{"id", "name", "age"}) {
		t.Error("Expected primary index to be covering index, but got false")
	}
}
