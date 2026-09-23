package engine

import (
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestJSONValueReturningCompatibilityParsing(t *testing.T) {
	query := `select json_value('{"n":"42"}', '$.n' returning unsigned)`
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	open := strings.Index(trimmed, "(")
	close := matchingParenthesis(trimmed, open)
	if close != len(trimmed)-1 {
		t.Fatalf("JSON_VALUE close parenthesis = %d, want %d", close, len(trimmed)-1)
	}
	arguments := splitTopLevelComma(trimmed[open+1 : close])
	if len(arguments) != 2 {
		t.Fatalf("JSON_VALUE arguments = %#v, want document and returning path", arguments)
	}
	returning := jsonValueReturningPattern.FindStringSubmatch(strings.TrimSpace(arguments[1]))
	if len(returning) != 4 || !strings.EqualFold(returning[2], "unsigned") {
		t.Fatalf("JSON_VALUE returning match = %#v", returning)
	}
	rewritten := "select cast(json_value(" + arguments[0] + ", " + returning[1] + ") as " + returning[2] + ")"
	parsed, err := sqlparser.Parse(rewritten)
	if err != nil {
		t.Fatalf("rewritten JSON_VALUE RETURNING query %q failed: %v", rewritten, err)
	}
	if selectStmt, ok := parsed.(*sqlparser.Select); ok && !selectHasNoFrom(selectStmt) {
		t.Fatalf("rewritten JSON_VALUE query parsed with a non-DUAL FROM: %s", sqlparser.String(parsed))
	}
}

func TestJSONValueReturningCompatibilityExecutesStandaloneSelect(t *testing.T) {
	ctx := &ExecutionContext{Results: make(chan *Result, 1)}
	handled, err := (&XMySQLExecutor{}).executeRawJSONValueCompatibility(ctx, `select json_value('{"n":"42"}', '$.n' returning unsigned)`, "app")
	if err != nil {
		t.Fatalf("JSON_VALUE RETURNING execution failed: %v", err)
	}
	if !handled {
		t.Fatal("JSON_VALUE RETURNING query was not handled")
	}
	result := <-ctx.Results
	if result.Err != nil {
		t.Fatalf("JSON_VALUE RETURNING result failed: %v", result.Err)
	}
}

func TestRewriteJSONValueReturningQuerySupportsNormalQueryTails(t *testing.T) {
	query := `select id, json_value(payload, '$.n' returning unsigned default 7 on empty) as n from items where json_value(payload, '$.n' returning unsigned) > 0 order by id`
	rewritten, err := rewriteJSONValueReturningQuery(query)
	if err != nil {
		t.Fatalf("rewrite JSON_VALUE RETURNING query failed: %v", err)
	}
	if rewritten == query {
		t.Fatal("JSON_VALUE RETURNING query was not rewritten")
	}
	if strings.Count(strings.ToLower(rewritten), "json_value_compat(") != 2 {
		t.Fatalf("rewritten query = %q, want two internal JSON_VALUE calls", rewritten)
	}
	if _, err := sqlparser.Parse(rewritten); err != nil {
		t.Fatalf("rewritten query does not parse: %v; query=%q", err, rewritten)
	}
}

func TestJSONValueReturningCompatibilityExecutesWithFromAndPredicate(t *testing.T) {
	dataDir := t.TempDir()
	engine := NewXMySQLEngine(&conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	})
	defer engine.Close()

	for _, query := range []string{
		"create database app",
		"create table json_value_rows (id int primary key, payload varchar(100))",
		`insert into json_value_rows values (1, '{"n":"42"}'), (2, '{}')`,
	} {
		result := <-engine.ExecuteQuery(nil, query, "app")
		if result.Err != nil {
			t.Fatalf("setup query %q failed: %v", query, result.Err)
		}
	}
	result := <-engine.ExecuteQuery(nil, `select id, json_value(payload, '$.n' returning unsigned default 7 on empty) as n from json_value_rows where json_value(payload, '$.n' returning unsigned default 0 on empty) > 0 order by id`, "app")
	if result.Err != nil {
		t.Fatalf("JSON_VALUE RETURNING query failed: %v", result.Err)
	}
	selectResult, ok := result.Data.(*SelectResult)
	if !ok || selectResult == nil || len(selectResult.Records) != 1 {
		t.Fatalf("JSON_VALUE RETURNING rows = %#v, want one row", result.Data)
	}
	values := selectResult.Records[0].GetValues()
	if len(values) != 2 || values[0].Int() != 1 || values[1].String() != "42" {
		t.Fatalf("JSON_VALUE RETURNING row = %#v, want [1 42]", values)
	}
}
