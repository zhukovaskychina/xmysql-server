package sqlparser

import "testing"

func TestParseCreateTableInlineCheckConstraints(t *testing.T) {
	stmt, err := Parse("create table checks (id int primary key, age int check (age >= 0 and age <= 150), salary decimal(10,2) check (salary > 0))")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	ddl, ok := stmt.(*DDL)
	if !ok {
		t.Fatalf("Parse() statement type = %T, want *DDL", stmt)
	}
	if ddl.TableSpec == nil {
		t.Fatal("Parse() returned a nil table specification")
	}
	if len(ddl.TableSpec.Columns) != 3 {
		t.Fatalf("got %d columns, want 3", len(ddl.TableSpec.Columns))
	}
	if len(ddl.TableSpec.Checks) != 2 {
		t.Fatalf("got inline checks %#v, want 2 checks", ddl.TableSpec.Checks)
	}
	if ddl.TableSpec.Checks[0] != "age >= 0 and age <= 150" || ddl.TableSpec.Checks[1] != "salary > 0" {
		t.Fatalf("got inline checks %#v", ddl.TableSpec.Checks)
	}

	namedStmt, err := Parse("create table named_checks (age int constraint chk_age check (age >= 0))")
	if err != nil {
		t.Fatalf("Parse() named inline check error = %v", err)
	}
	namedDDL, ok := namedStmt.(*DDL)
	if !ok || namedDDL.TableSpec == nil || len(namedDDL.TableSpec.Columns) != 1 || len(namedDDL.TableSpec.Checks) != 1 {
		t.Fatalf("named inline check was not preserved: %#v", namedStmt)
	}

	notEnforcedStmt, err := Parse("create table unenforced_checks (age int constraint chk_age check (age >= 0) not enforced)")
	if err != nil {
		t.Fatalf("Parse() NOT ENFORCED check error = %v", err)
	}
	notEnforcedDDL, ok := notEnforcedStmt.(*DDL)
	if !ok || notEnforcedDDL.TableSpec == nil || len(notEnforcedDDL.TableSpec.CheckDefinitions) != 1 {
		t.Fatalf("NOT ENFORCED check metadata was not preserved: %#v", notEnforcedStmt)
	}
	definition := notEnforcedDDL.TableSpec.CheckDefinitions[0]
	if definition.Name != "chk_age" || definition.Expression != "age >= 0" || definition.Enforced {
		t.Fatalf("got NOT ENFORCED definition %#v", definition)
	}

	tableLevelStmt, err := Parse("create table table_checks (age int, constraint chk_age check (age >= 0) not enforced)")
	if err != nil {
		t.Fatalf("Parse() table-level NOT ENFORCED check error = %v", err)
	}
	tableLevelDDL := tableLevelStmt.(*DDL)
	if len(tableLevelDDL.TableSpec.CheckDefinitions) != 1 || tableLevelDDL.TableSpec.CheckDefinitions[0].Name != "chk_age" || tableLevelDDL.TableSpec.CheckDefinitions[0].Enforced {
		t.Fatalf("got table-level NOT ENFORCED definitions %#v", tableLevelDDL.TableSpec.CheckDefinitions)
	}
}
