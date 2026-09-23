package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandlerOpenReadNextCloseUsesSessionCursor(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(32))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'alice'), (2, 'bob')")
	session := newTestMySQLSession()

	opened := <-executor.ExecuteQuery(session, "handler users open", "app")
	require.NoError(t, opened.Err)
	first := <-executor.ExecuteQuery(session, "handler users read primary first", "app")
	require.NoError(t, first.Err)
	firstResult := first.Data.(*SelectResult)
	require.Len(t, firstResult.Records, 1)
	require.Equal(t, []byte("1"), firstResult.Records[0].GetValues()[0].Raw())

	next := <-executor.ExecuteQuery(session, "handler users read primary next", "app")
	require.NoError(t, next.Err)
	nextResult := next.Data.(*SelectResult)
	require.Len(t, nextResult.Records, 1)
	require.Equal(t, []byte("2"), nextResult.Records[0].GetValues()[0].Raw())

	filtered := <-executor.ExecuteQuery(session, "handler users read primary first where name = 'bob'", "app")
	require.NoError(t, filtered.Err)
	filteredResult := filtered.Data.(*SelectResult)
	require.Len(t, filteredResult.Records, 1)
	require.Equal(t, []byte("bob"), filteredResult.Records[0].GetValues()[1].Raw())

	closed := <-executor.ExecuteQuery(session, "handler users close", "app")
	require.NoError(t, closed.Err)
	missing := <-executor.ExecuteQuery(session, "handler users read primary next", "app")
	require.Error(t, missing.Err)

	reset := executor.QueryExecutor.ResetSession(session)
	require.NoError(t, reset)
	_, exists := session.GetParamByName("handler_cursors").(map[string]*handlerCursorState)
	require.True(t, exists)
}

func TestHandlerOpenAsAliasUsesAliasForReadAndClose(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into users values (7)")
	session := newTestMySQLSession()

	require.NoError(t, (<-executor.ExecuteQuery(session, "handler users open as user_cursor", "app")).Err)
	read := <-executor.ExecuteQuery(session, "handler user_cursor read primary first", "app")
	require.NoError(t, read.Err)
	require.Equal(t, "7", read.Data.(*SelectResult).Records[0].GetValues()[0].ToString())
	require.NoError(t, (<-executor.ExecuteQuery(session, "handler user_cursor close", "app")).Err)
	missing := <-executor.ExecuteQuery(session, "handler user_cursor read primary next", "app")
	require.Error(t, missing.Err)
}

func TestHandlerOpenRejectsDuplicateCursorName(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key)")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "handler users open as user_cursor")
	duplicate := <-executor.ExecuteQuery(session, "handler users open as user_cursor", "app")
	require.Error(t, duplicate.Err)
	require.Contains(t, strings.ToLower(duplicate.Err.Error()), "already open")

	read := <-executor.ExecuteQuery(session, "handler user_cursor read first", "app")
	require.NoError(t, read.Err)
}

func TestHandlerOpenRejectsSecondCursorForSamePhysicalTableAndCloseByTableName(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table users (id int primary key, name varchar(20))")
	mustExecSessionSQL(t, executor, session, "app", "insert into users (id, name) values (1, 'alice')")
	mustExecSessionSQL(t, executor, session, "app", "handler users open as user_cursor")

	duplicate := <-executor.ExecuteQuery(session, "handler users open", "app")
	require.Error(t, duplicate.Err)
	require.Contains(t, strings.ToLower(duplicate.Err.Error()), "already open")

	closed := <-executor.ExecuteQuery(session, "handler users close", "app")
	require.NoError(t, closed.Err)
	afterClose := <-executor.ExecuteQuery(session, "handler user_cursor read first", "app")
	require.Error(t, afterClose.Err)
}

func TestHandlerReadsInIndexOrderAndSupportsSingleColumnLookup(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, name varchar(32), score int, key score_idx (score))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 'late', 30), (2, 'early', 10), (3, 'middle', 20)")
	session := newTestMySQLSession()

	require.NoError(t, (<-executor.ExecuteQuery(session, "handler users open", "app")).Err)
	first := <-executor.ExecuteQuery(session, "handler users read score_idx first", "app")
	require.NoError(t, first.Err)
	require.Equal(t, "early", first.Data.(*SelectResult).Records[0].GetValues()[1].ToString())
	next := <-executor.ExecuteQuery(session, "handler users read score_idx next", "app")
	require.NoError(t, next.Err)
	require.Equal(t, "middle", next.Data.(*SelectResult).Records[0].GetValues()[1].ToString())

	lookup := <-executor.ExecuteQuery(session, "handler users read score_idx = (30)", "app")
	require.NoError(t, lookup.Err)
	require.Equal(t, "late", lookup.Data.(*SelectResult).Records[0].GetValues()[1].ToString())
	missing := <-executor.ExecuteQuery(session, "handler users read score_idx > (30)", "app")
	require.NoError(t, missing.Err)
	require.Empty(t, missing.Data.(*SelectResult).Records)
}

func TestHandlerReadsCompositeIndexLookup(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key, tenant_id int, event_id int, key tenant_event (tenant_id, event_id))")
	mustExecSQL(t, executor, "app", "insert into events values (1, 2, 20), (2, 1, 30), (3, 2, 10), (4, 2, 30)")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "handler events open", "app")).Err)
	lookup := <-executor.ExecuteQuery(session, "handler events read tenant_event = (2, 10)", "app")
	require.NoError(t, lookup.Err)
	require.Equal(t, "3", lookup.Data.(*SelectResult).Records[0].GetValues()[0].ToString())
	next := <-executor.ExecuteQuery(session, "handler events read tenant_event next", "app")
	require.NoError(t, next.Err)
	require.Equal(t, "1", next.Data.(*SelectResult).Records[0].GetValues()[0].ToString())
	nextAgain := <-executor.ExecuteQuery(session, "handler events read tenant_event next", "app")
	require.NoError(t, nextAgain.Err)
	require.Equal(t, "4", nextAgain.Data.(*SelectResult).Records[0].GetValues()[0].ToString())
}

func TestHandlerReadsCompositeIndexPrefixKeyPart(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key, tenant_id int, event_id int, key tenant_event (tenant_id, event_id))")
	mustExecSQL(t, executor, "app", "insert into events values (1, 2, 20), (2, 1, 30), (3, 2, 10), (4, 2, 30)")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "handler events open", "app")).Err)
	lookup := <-executor.ExecuteQuery(session, "handler events read tenant_event = (2)", "app")
	require.NoError(t, lookup.Err)
	if selected := lookup.Data.(*SelectResult); len(selected.Records) == 0 {
		t.Fatalf("prefix lookup returned no rows: %#v", selected)
	}
	require.Equal(t, "3", lookup.Data.(*SelectResult).Records[0].GetValues()[0].ToString())
	next := <-executor.ExecuteQuery(session, "handler events read tenant_event next", "app")
	require.NoError(t, next.Err)
	require.Equal(t, "1", next.Data.(*SelectResult).Records[0].GetValues()[0].ToString())
}

func TestHandlerReadSupportsWhereLimit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table users (id int primary key, score int, key score_idx (score))")
	mustExecSQL(t, executor, "app", "insert into users values (1, 10), (2, 20), (3, 30)")
	session := newTestMySQLSession()
	require.NoError(t, (<-executor.ExecuteQuery(session, "handler users open", "app")).Err)
	read := <-executor.ExecuteQuery(session, "handler users read score_idx first where score >= 10 limit 1", "app")
	require.NoError(t, read.Err)
	selected := read.Data.(*SelectResult)
	require.Len(t, selected.Records, 1)
	require.Equal(t, "1", selected.Records[0].GetValues()[0].ToString())
}

func TestHandlerReadLimitNormalizesOffsetForms(t *testing.T) {
	tests := []struct {
		name   string
		spec   string
		want   string
		remain string
	}{
		{name: "count", spec: "where score >= 10 limit 2", want: "limit 2", remain: "where score >= 10"},
		{name: "comma", spec: "where score >= 10 limit 1,2", want: "limit 1,2", remain: "where score >= 10"},
		{name: "offset", spec: "where score >= 10 limit 2 offset 1", want: "limit 2 offset 1", remain: "where score >= 10"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			remain, limit := handlerReadLimit(test.spec)
			require.Equal(t, test.remain, remain)
			require.Equal(t, test.want, limit)
		})
	}
}
