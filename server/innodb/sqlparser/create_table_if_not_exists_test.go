package sqlparser

import "testing"

func TestParseCreateTableIfNotExistsSetsDDLFlag(t *testing.T) {
	stmt, err := Parse("create table if not exists users (id int primary key)")
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	ddl, ok := stmt.(*DDL)
	if !ok {
		t.Fatalf("Parse returned %T, want *DDL", stmt)
	}
	if !ddl.IfExists {
		t.Fatalf("DDL.IfExists = false, want true for CREATE TABLE IF NOT EXISTS")
	}
}
