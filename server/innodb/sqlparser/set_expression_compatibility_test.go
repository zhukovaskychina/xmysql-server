package sqlparser

import "testing"

func TestSetExpressionASTSupportsCTEAndPrecedence(t *testing.T) {
	query := "with q as ((select 1 as n union all select 2) intersect select 2) select n from q"
	statement, err := Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	with := statement.(*With)
	set, ok := with.CTEs[0].Query.(*Union)
	if !ok || set.Type != "intersect" {
		t.Fatalf("CTE set = %#v", with.CTEs[0].Query)
	}
	statement, err = Parse("select 1 union select 2 intersect select 3 except all select 4")
	if err != nil {
		t.Fatal(err)
	}
	root := statement.(*Union)
	if root.Type != "except all" {
		t.Fatalf("root = %#v", root)
	}
	union, ok := root.Left.(*Union)
	if !ok || union.Type != "union" {
		t.Fatalf("left = %#v", root.Left)
	}
	intersection, ok := union.Right.(*Union)
	if !ok || intersection.Type != "intersect" {
		t.Fatalf("intersection = %#v", union.Right)
	}
	if _, err := Parse(String(statement)); err != nil {
		t.Fatal(err)
	}
}
