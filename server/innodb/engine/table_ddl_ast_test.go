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

func TestCreateSelectASTExecutesNewlinesAndLiteralKeywords(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "CREATE DATABASE app")
	mustExecSQL(t, engine, "app", "CREATE TABLE ast_copy\nAS\nSELECT concat('AS SELECT ', ')') AS label")
	require.Equal(t, [][]interface{}{{"AS SELECT )"}}, mustQuerySQL(t, engine, "app", "SELECT label FROM ast_copy"))
	ctx := &ExecutionContext{DatabaseName: "app"}
	handled, err := engine.QueryExecutor.executeRawCreateTableAsSelectCompatibility(ctx, "CREATE TABLE invalid_copy AS SELECT n FROM", "app")
	require.True(t, handled)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(engine.QueryExecutor.getDataDir(), "app", "invalid_copy.frm"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCreateIndexVisibilityASTPreservesNamesAndComments(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "CREATE DATABASE app")
	mustExecSQL(t, engine, "app", "CREATE TABLE ast_visibility (visible int, KEY `i,n` (visible) COMMENT 'invisible' VISIBLE, KEY `h,idden` (visible) INVISIBLE)")
	info, err := readTableMetadataMap(filepath.Join(engine.QueryExecutor.getDataDir(), "app", "ast_visibility.frm"))
	require.NoError(t, err)
	visibility := make(map[string]bool)
	for _, raw := range info["indexes"].([]interface{}) {
		index := raw.(map[string]interface{})
		visible := true
		if value, ok := index["visible"].(bool); ok {
			visible = value
		}
		visibility[index["name"].(string)] = visible
	}
	require.Contains(t, visibility, "i,n")
	require.Contains(t, visibility, "h,idden")
	require.True(t, visibility["i,n"])
	require.False(t, visibility["h,idden"])
}
