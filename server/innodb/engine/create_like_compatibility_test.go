package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestCreateTableLikePattern(t *testing.T) {
	parsed, err := sqlparser.ParseForExecution("create table copied_users like source_users")
	if err != nil {
		t.Fatal(err)
	}
	stmt, ok := parsed.AST.(*sqlparser.TableDDLStatement)
	if !ok || stmt.Like.Name.String() != "source_users" || stmt.Targets[0].Name.String() != "copied_users" {
		t.Fatalf("unexpected CREATE TABLE LIKE AST: %#v", parsed.AST)
	}
}
