package sqlparser

import "testing"

func TestParseCreateTableTableLevelForeignKey(t *testing.T) {
	stmt, err := Parse("create table child (id int primary key, parent_id int, constraint fk_parent foreign key (parent_id) references parent (id) on delete cascade)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	ddl, ok := stmt.(*DDL)
	if !ok {
		t.Fatalf("Parse() statement type = %T, want *DDL", stmt)
	}
	if ddl.TableSpec == nil || len(ddl.TableSpec.ForeignKeys) != 1 {
		t.Fatalf("got table foreign keys = %#v, want one foreign key", ddl.TableSpec)
	}

	fk := ddl.TableSpec.ForeignKeys[0]
	if fk.Name.String() != "fk_parent" || fk.ReferencedTable.Name.String() != "parent" {
		t.Fatalf("got foreign key = %#v, want named reference to parent", fk)
	}
	if len(fk.Columns) != 1 || fk.Columns[0].String() != "parent_id" {
		t.Fatalf("got local columns = %#v, want parent_id", fk.Columns)
	}
	if len(fk.ReferencedColumns) != 1 || fk.ReferencedColumns[0].String() != "id" {
		t.Fatalf("got referenced columns = %#v, want id", fk.ReferencedColumns)
	}
	if fk.OnDelete != "cascade" {
		t.Fatalf("got ON DELETE action = %q, want cascade", fk.OnDelete)
	}
}
