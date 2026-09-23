package engine

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestParseOrderByPreservesDescendingDirection(t *testing.T) {
	statement, err := sqlparser.Parse("select u.id from users u order by u.id desc")
	if err != nil {
		t.Fatalf("parse SELECT: %v", err)
	}
	selectStmt := statement.(*sqlparser.Select)
	se := &SelectExecutor{}
	if err := se.parseOrderBy(selectStmt.OrderBy); err != nil {
		t.Fatalf("parse ORDER BY: %v", err)
	}
	if len(se.orderByColumns) != 1 || se.orderByColumns[0] != "u.id DESC" {
		t.Fatalf("orderByColumns = %#v, want [u.id DESC]", se.orderByColumns)
	}
}

func TestSortJoinedRowsUsesColumnTypeForDecimalOrder(t *testing.T) {
	se := &SelectExecutor{
		orderByColumns: []string{"o.amount DESC"},
		joinOrderTypes: map[string]metadata.DataType{"o.amount": metadata.DataType("decimal")},
	}
	rows := []joinedMapRow{
		{"o.amount": "79.99"},
		{"o.amount": "299.99"},
	}

	se.sortJoinedRows(rows)
	if got := rows[0]["o.amount"]; got != "299.99" {
		t.Fatalf("descending decimal order first value = %#v, want 299.99", got)
	}
}
