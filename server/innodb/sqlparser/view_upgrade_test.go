package sqlparser

import "testing"

func TestViewUpgradeStructuredDefinition(t *testing.T) {
	parsed, err := ParseForExecution("CREATE OR REPLACE VIEW app.v (id) AS SELECT id FROM app.t")
	if err != nil {
		t.Fatalf("ParseForExecution() error = %v", err)
	}
	view, ok := parsed.AST.(*ViewStatement)
	if !ok || view.Kind != "create" || !view.OrReplace || view.Name.Qualifier.String() != "app" || view.Name.Name.String() != "v" || len(view.Columns) != 1 || view.Definition == nil {
		t.Fatalf("view = %#v", parsed.AST)
	}
}

func TestViewUpgradePreservesDefinition(t *testing.T) {
	parsed, err := ParseForExecution("CREATE OR REPLACE VIEW app.v (id) AS SELECT id FROM app.t")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*ViewStatement)
	if stmt.Kind != "create" || stmt.Definition == nil || len(stmt.Columns) != 1 {
		t.Fatalf("view=%#v", stmt)
	}
}

func TestViewASTDefinerAndScope(t *testing.T) {
	parsed, err := ParseForExecution("CREATE DEFINER='alice'@'localhost' SQL SECURITY INVOKER VIEW app.v AS SELECT text FROM other.t WHERE text = 'app.t'")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*ViewStatement)
	if stmt.Definer.User != "alice" || stmt.Definer.Host != "localhost" || stmt.Security != "invoker" {
		t.Fatalf("view=%#v", stmt)
	}
	refs, err := CollectObjectReferences(stmt.Definition, "app")
	if err != nil || len(refs) != 1 || refs[0].Schema != "other" {
		t.Fatalf("refs=%v err=%v", refs, err)
	}
}

func TestViewUpgradeDropDefinition(t *testing.T) {
	parsed, err := ParseForExecution("DROP VIEW app.v")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*ViewStatement)
	if stmt.Kind != "drop" || stmt.Name.Name.String() != "v" {
		t.Fatalf("view=%#v", stmt)
	}
}

func TestViewUpgradePreservesSecurityAndCheckOption(t *testing.T) {
	parsed, err := ParseForExecution("CREATE ALGORITHM=MERGE DEFINER='alice'@'localhost' SQL SECURITY INVOKER VIEW app.v AS SELECT id FROM app.t WITH LOCAL CHECK OPTION")
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.AST.(*ViewStatement)
	if stmt.Algorithm != "merge" || stmt.Definer.User == "" || stmt.Security != "invoker" || stmt.CheckOption != "LOCAL" {
		t.Fatalf("view=%#v", stmt)
	}
}
