package sqlparser

import "testing"

func TestShowExecutionWherePreservesPredicate(t *testing.T) {
	for _, query := range []string{
		"show tables from app where 1 = 0",
		"show table status from app where 1 = 0",
		"show columns from app.t where 1 = 0",
		"show collation where 1 = 0",
	} {
		t.Run(query, func(t *testing.T) {
			parsed, err := ParseForExecution(query)
			if err != nil {
				t.Fatal(err)
			}
			show := parsed.AST.(*Show)
			if show.Filter == nil || show.Filter.Filter == nil || String(show.Filter.Filter) != "1 = 0" {
				t.Fatalf("WHERE predicate was discarded: %#v", show)
			}
		})
	}
}
