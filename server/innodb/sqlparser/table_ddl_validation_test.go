package sqlparser

import "testing"

func TestTableDDLRejectsInvalidAndAcceptsLike(t *testing.T) {
	for _, query := range []string{"CREATE TABLE t (id int) invalid", "CREATE TABLE t LIKE u", "DROP TABLE t bad", "RENAME TABLE t TO u bad"} {
		parsed, err := ParseForExecution(query)
		if query == "CREATE TABLE t LIKE u" && err != nil {
			t.Fatalf("LIKE: %v", err)
		}
		if query != "CREATE TABLE t LIKE u" && err == nil {
			t.Fatalf("expected invalid for %q, got %#v", query, parsed)
		}
	}
	parsed, err := ParseForExecution("CREATE TABLE copied AS SELECT id FROM source")
	if err != nil || parsed.AST.(*TableDDLStatement).AsSelect == nil {
		t.Fatalf("AS SELECT was not retained: %#v err=%v", parsed, err)
	}
}
