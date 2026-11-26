package sqlparser

import (
	"strings"
	"testing"
)

func TestCreateTableAllowsContentAsUnquotedColumnName(t *testing.T) {
	statement, err := Parse("create table docs (id int primary key, content text, fulltext index idx_content (content))")
	if err != nil {
		t.Fatalf("parse MySQL CREATE TABLE with unquoted content column: %v", err)
	}
	ddl, ok := statement.(*DDL)
	if !ok || ddl.TableSpec == nil {
		t.Fatalf("got %T, want CREATE TABLE DDL with table spec", statement)
	}
	if len(ddl.TableSpec.Columns) != 2 || ddl.TableSpec.Columns[1].Name.String() != "content" {
		t.Fatalf("content column was not retained: %#v", ddl.TableSpec.Columns)
	}
	if len(ddl.TableSpec.Indexes) != 1 ||
		!strings.Contains(strings.ToUpper(ddl.TableSpec.Indexes[0].Info.Type), "FULLTEXT") ||
		ddl.TableSpec.Indexes[0].Info.Name.String() != "idx_content" {
		t.Fatalf("FULLTEXT index was not retained: %#v", ddl.TableSpec.Indexes)
	}

	contentStatement, err := Parse("create table content_objects (payload CONTENT)")
	if err != nil {
		t.Fatalf("parse native CONTENT column type: %v", err)
	}
	contentDDL := contentStatement.(*DDL)
	if len(contentDDL.TableSpec.Columns) != 1 || !strings.EqualFold(contentDDL.TableSpec.Columns[0].Type.Type, "CONTENT") {
		t.Fatalf("native CONTENT type was not retained: %#v", contentDDL.TableSpec.Columns)
	}
}
