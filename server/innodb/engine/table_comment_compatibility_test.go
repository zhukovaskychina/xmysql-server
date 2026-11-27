package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableCommentPersistsAcrossCreateAlterAndInformationSchema(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table docs (id int primary key comment 'document identifier') comment='initial docs'")

	rows := mustQuerySQL(t, executor, "app", "select table_comment from information_schema.tables where table_schema='app' and table_name='docs'")
	require.Len(t, rows, 1)
	require.Equal(t, "initial docs", fmt.Sprint(rows[0][0]))

	columnRows := mustQuerySQL(t, executor, "app", "select remarks from information_schema.columns where table_schema='app' and table_name='docs' and column_name='id'")
	require.Len(t, columnRows, 1)
	require.Equal(t, "document identifier", fmt.Sprint(columnRows[0][0]))

	mustExecSQL(t, executor, "app", "alter table docs comment='updated docs'")
	rows = mustQuerySQL(t, executor, "app", "select table_comment from information_schema.tables where table_schema='app' and table_name='docs'")
	require.Len(t, rows, 1)
	require.Equal(t, "updated docs", fmt.Sprint(rows[0][0]))

	showCreateResult := <-executor.ExecuteQuery(nil, "show create table docs", "app")
	require.NoError(t, showCreateResult.Err)
	showCreate, ok := showCreateResult.Data.(map[string]interface{})
	require.True(t, ok, "SHOW CREATE TABLE result = %T", showCreateResult.Data)
	showCreateRows, ok := showCreate["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, showCreateRows, 1)
	require.True(t, strings.Contains(strings.ToLower(fmt.Sprint(showCreateRows[0][1])), "comment='updated docs'"), "SHOW CREATE TABLE = %v", showCreateRows[0][1])
	require.True(t, strings.Contains(strings.ToLower(fmt.Sprint(showCreateRows[0][1])), "comment 'document identifier'"), "SHOW CREATE TABLE = %v", showCreateRows[0][1])

	fullColumnsResult := <-executor.ExecuteQuery(nil, "show full columns from docs", "app")
	require.NoError(t, fullColumnsResult.Err)
	fullColumns, ok := fullColumnsResult.Data.(map[string]interface{})
	require.True(t, ok, "SHOW FULL COLUMNS result = %T", fullColumnsResult.Data)
	fullColumnRows, ok := fullColumns["rows"].([][]interface{})
	require.True(t, ok)
	require.Len(t, fullColumnRows, 1)
	require.Nil(t, fullColumnRows[0][2])
	require.Equal(t, "document identifier", fmt.Sprint(fullColumnRows[0][8]))
}
