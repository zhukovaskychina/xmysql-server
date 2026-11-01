package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableDDLASTPersistence(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("database", "ddl_ast_app")
	mustExecSessionSQL(t, executor, session, "", "CREATE DATABASE ddl_ast_app")
	mustExecSessionSQL(t, executor, session, "ddl_ast_app", "CREATE TABLE source_rows (id INT PRIMARY KEY, value VARCHAR(20))")
	mustExecSessionSQL(t, executor, session, "ddl_ast_app", "INSERT INTO source_rows VALUES (1, 'one')")
	mustExecSessionSQL(t, executor, session, "ddl_ast_app", "CREATE TABLE copied_rows LIKE source_rows")
	mustExecSessionSQL(t, executor, session, "ddl_ast_app", "RENAME TABLE copied_rows TO renamed_rows")
	rows := mustQuerySessionSQL(t, executor, session, "ddl_ast_app", "SELECT id FROM renamed_rows")
	require.Empty(t, rows)
	mustExecSessionSQL(t, executor, session, "ddl_ast_app", "DROP TABLE renamed_rows")
	_, statErr := os.Stat(filepath.Join(executor.QueryExecutor.getDataDir(), "ddl_ast_app", "renamed_rows.frm"))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	first, second := newTestMySQLSession(), newTestMySQLSession()
	first.SetParamByName("database", "ddl_ast_app")
	second.SetParamByName("database", "ddl_ast_app")
	mustExecSessionSQL(t, executor, first, "ddl_ast_app", "CREATE TEMPORARY TABLE session_only (id INT)")
	firstState := getTemporaryTableSessionState(first, false)
	secondState := getTemporaryTableSessionState(second, false)
	require.NotNil(t, firstState)
	require.Nil(t, secondState)
	firstState.mu.RLock()
	_, firstBound := firstState.tables[temporaryTableKey("ddl_ast_app", "session_only")]
	firstState.mu.RUnlock()
	require.True(t, firstBound)
}
