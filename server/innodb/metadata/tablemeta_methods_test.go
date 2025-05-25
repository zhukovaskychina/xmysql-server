package metadata

import (
	"testing"
)

func TestTableMeta_AddColumnAndGetColumn(t *testing.T) {
	table := CreateTableMeta("test_table")
	col := &ColumnMeta{Name: "id", Type: TypeInt, IsPrimary: true}
	table.AddColumn(col)

	got, err := table.GetColumn("id")
	if err != nil {
		t.Fatalf("expected to get column, got error: %v", err)
	}
	if got.Name != "id" {
		t.Errorf("expected column name 'id', got %s", got.Name)
	}
}

func TestTableMeta_AddIndex(t *testing.T) {
	table := CreateTableMeta("test_table")
	table.AddColumn(&ColumnMeta{Name: "id", Type: TypeInt})
	table.AddIndex("idx_id", []string{"id"}, true)
	if len(table.Indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(table.Indices))
	}
	if table.Indices[0].Name != "idx_id" {
		t.Errorf("expected index name 'idx_id', got %s", table.Indices[0].Name)
	}
}

func TestTableMeta_SetPrimaryKey(t *testing.T) {
	table := CreateTableMeta("test_table")
	table.AddColumn(&ColumnMeta{Name: "id", Type: TypeInt})
	err := table.SetPrimaryKey("id")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(table.PrimaryKey) != 1 || table.PrimaryKey[0] != "id" {
		t.Errorf("primary key not set correctly: %v", table.PrimaryKey)
	}
}

func TestTableMeta_Validate(t *testing.T) {
	table := CreateTableMeta("test_table")
	table.AddColumn(&ColumnMeta{Name: "id", Type: TypeInt, IsPrimary: true})
	table.AddColumn(&ColumnMeta{Name: "name", Type: TypeVarchar, Length: 20})
	table.AddIndex("idx_id", []string{"id"}, true)
	err := table.Validate()
	if err != nil {
		t.Errorf("expected table meta to be valid, got error: %v", err)
	}
}

func TestTableMeta_Validate_DuplicateColumn(t *testing.T) {
	table := CreateTableMeta("test_table")
	table.AddColumn(&ColumnMeta{Name: "id", Type: TypeInt})
	table.AddColumn(&ColumnMeta{Name: "id", Type: TypeInt})
	err := table.Validate()
	if err == nil {
		t.Errorf("expected error for duplicate column name, got nil")
	}
}

func TestTableMeta_Validate_MissingPK(t *testing.T) {
	table := CreateTableMeta("test_table")
	table.AddColumn(&ColumnMeta{Name: "name", Type: TypeVarchar, Length: 20})
	table.PrimaryKey = []string{"id"}
	err := table.Validate()
	if err == nil {
		t.Errorf("expected error for missing primary key column, got nil")
	}
}

func TestTableMeta_Validate_IndexUnknownColumn(t *testing.T) {
	table := CreateTableMeta("test_table")
	table.AddColumn(&ColumnMeta{Name: "id", Type: TypeInt})
	table.AddIndex("idx_x", []string{"x"}, false)
	err := table.Validate()
	if err == nil {
		t.Errorf("expected error for index referencing unknown column, got nil")
	}
}
