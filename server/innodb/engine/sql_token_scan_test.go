package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindPredicateCallWithOperatorWhitespace(t *testing.T) {
	for _, input := range []struct{ sql, left, operator string }{
		{"select id from t where k in (select k from s)", "k", "in"},
		{"k\tNOT\nIN (select k from s)", "k", "NOT\nIN"},
		{"t.k >= ALL (select k from s)", "t.k", ">= ALL"},
		{"k=ANY (select k from s)", "k", "=ANY"},
	} {
		t.Run(input.sql, func(t *testing.T) {
			call, found := findPredicateCall(input.sql, false)
			require.True(t, found)
			require.Equal(t, input.left, call.left)
			require.Equal(t, input.operator, call.operator)
			require.Equal(t, byte('('), input.sql[call.end-1])
		})
	}
}

func TestCorrelatedSQLBindingScanners(t *testing.T) {
	query := "select p.id from parents p where exists (select 1 from children c where c.parent_id = p.id and c.active = 1)"
	at, negated, found := findExistsCall(query)
	require.True(t, found)
	require.False(t, negated)
	require.Equal(t, "exists (select", query[at:at+14])
	start, end, found := findSpacedKeyword(query[:at], "where")
	require.True(t, found)
	require.Equal(t, "select p.id from parents p", strings.TrimSpace(query[:start]))
	require.Equal(t, "", strings.TrimSpace(query[end:at]))
	table, alias, ok := parseLeadingTableRef("parents p", false, false)
	require.True(t, ok)
	require.Equal(t, "parents", table)
	require.Equal(t, "p", alias)
	require.True(t, sqlHasQualifierDot("select 1 from children c where c.parent_id = p.id and c.active = 1", alias))
	require.Equal(t, "select p.id from parents p", correlatedOuterSourceSQL("p.id", "", "select 1 from children c where c.parent_id = p.id", "parents p"))
}

func TestQueryScannersIgnoreLiteralAndCommentContents(t *testing.T) {
	for _, sql := range []string{
		"select 'k in (select x from missing)'",
		"select 1 /* k in (select x from missing) */",
		"select 1 -- k in (select x from missing)\n",
	} {
		_, found := findPredicateCall(sql, false)
		require.False(t, found, sql)
	}
	_, _, found := findExistsCall("select 'exists (select x from missing)'")
	require.False(t, found)
	require.Equal(t, "select 'p.id', 7 /* p.id */", replaceQualifiedName("select 'p.id', p.id /* p.id */", "p", "id", "7"))
}

func TestPredicateScannerPreservesOffsetsAcrossBinaryLiteralBytes(t *testing.T) {
	sql := "select '" + strings.Repeat(string([]byte{0xff, 0xfe, 0x00}), 4096) + "' as payload"
	_, found := findPredicateCall(sql, false)
	require.False(t, found)
}
