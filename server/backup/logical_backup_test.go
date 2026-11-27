package backup

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestLogicalBackupCrossDirectoryImportAndPITR(t *testing.T) {
	backupDir, restoreDir := t.TempDir(), t.TempDir()
	path := filepath.Join(backupDir, "app.json")
	file, err := os.Create(path)
	require.NoError(t, err)
	tables := map[string][]replication.RowChange{
		"app.docs": {{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": int64(1)}}},
	}
	require.NoError(t, Export(file, tables))
	require.NoError(t, file.Close())
	input, err := os.Open(path)
	require.NoError(t, err)
	loaded, err := Import(input)
	require.NoError(t, err)
	require.NoError(t, input.Close())
	require.Equal(t, tables, loaded.Tables)

	source, err := replication.NewSource(restoreDir, "source-uuid", 1)
	require.NoError(t, err)
	first, err := source.Append(1, tables["app.docs"])
	require.NoError(t, err)
	second, err := source.Append(2, []replication.RowChange{{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": 2}}})
	require.NoError(t, err)
	require.Len(t, second, 3)
	restored := RestoreUntilPosition(append(first, second...), first[2].Position)
	require.Len(t, restored["app.docs"], 1)
}

func TestLogicalBackupPreservesSchemaStatementsAndReplaysUntilCommitPosition(t *testing.T) {
	backupDir := t.TempDir()
	path := filepath.Join(backupDir, "app-with-schema.json")
	statements := []replication.Statement{{Database: "app", SQL: "CREATE TABLE docs (id INT PRIMARY KEY)"}}
	rows := map[string][]replication.RowChange{
		"app.docs": {{Table: "app.docs", Action: "insert", After: map[string]interface{}{"id": int64(1)}}},
	}
	var encoded bytes.Buffer
	require.NoError(t, ExportWithStatements(&encoded, rows, statements))
	require.NoError(t, os.WriteFile(path, encoded.Bytes(), 0644))

	loaded, err := Import(strings.NewReader(encoded.String()))
	require.NoError(t, err)
	require.Equal(t, 2, loaded.Version)
	require.Equal(t, statements, loaded.Statements)
	require.Equal(t, rows, loaded.Tables)

	source, err := replication.NewSource(t.TempDir(), "schema-source", 1)
	require.NoError(t, err)
	first, err := source.AppendTransaction(1, nil, statements)
	require.NoError(t, err)
	second, err := source.AppendTransaction(2, rows["app.docs"], nil)
	require.NoError(t, err)

	restored := RestoreUntilPositionWithStatements(append(first, second...), first[2].Position)
	require.Equal(t, statements, restored.Statements)
	require.Equal(t, map[string][]replication.RowChange{}, restored.Tables)

	var replayed []replication.Statement
	require.NoError(t, ReplayStatements(restored.Statements, func(statement replication.Statement) error {
		replayed = append(replayed, statement)
		return nil
	}))
	require.Equal(t, statements, replayed)
}

func TestLogicalBackupRejectsInvalidSchemaStatementDuringReplay(t *testing.T) {
	err := ReplayStatements([]replication.Statement{{Database: "app", SQL: "  "}}, func(replication.Statement) error {
		t.Fatal("empty schema statement should not reach the replay callback")
		return nil
	})
	require.ErrorContains(t, err, "empty SQL")
}

func TestRestoreNativeUntilPositionUsesCommittedPhysicalBoundary(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "native-pitr", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, []replication.RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1)},
	}}, nil)
	require.NoError(t, err)
	_, err = source.AppendTransaction(2, []replication.RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(2)},
	}}, nil)
	require.NoError(t, err)

	native, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var firstCommitEnd, firstRowEnd uint64
	for _, event := range native {
		switch event.Type {
		case 30:
			if firstRowEnd == 0 {
				firstRowEnd = event.EndPosition
			}
		case 16:
			if firstCommitEnd == 0 {
				firstCommitEnd = event.EndPosition
			}
		}
	}
	require.NotZero(t, firstRowEnd)
	require.NotZero(t, firstCommitEnd)

	partial, err := RestoreNativeUntilPosition(source, "binlog.000001", firstRowEnd)
	require.NoError(t, err)
	require.Empty(t, partial.Tables, "a physical position before XID must not commit a partial transaction")

	committed, err := RestoreNativeUntilPosition(source, "binlog.000001", firstCommitEnd)
	require.NoError(t, err)
	require.Len(t, committed.Tables["app.docs"], 1)
	require.Equal(t, float64(1), committed.Tables["app.docs"][0].After["id"])
}

func TestRestoreNativeUntilPositionsReplaysAcrossRotatedFiles(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "native-pitr-rotate", 17)
	require.NoError(t, err)
	_, err = source.AppendTransaction(1, []replication.RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1)},
	}}, nil)
	require.NoError(t, err)
	_, err = source.Rotate()
	require.NoError(t, err)
	_, err = source.AppendTransaction(2, []replication.RowChange{{
		Table:  "app.docs",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(2)},
	}}, nil)
	require.NoError(t, err)

	first, err := source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	second, err := source.Writer.NativeEvents("binlog.000002")
	require.NoError(t, err)
	firstCommitEnd := nativeCommitEndPosition(t, first)
	secondCommitEnd := nativeCommitEndPosition(t, second)

	restored, err := RestoreNativeUntilPositions(source, map[string]uint64{
		"binlog.000001": firstCommitEnd,
		"binlog.000002": secondCommitEnd,
	})
	require.NoError(t, err)
	require.Len(t, restored.Tables["app.docs"], 2)
}

func TestRestoreNativeUntilPositionsRejectsMissingBoundaryFile(t *testing.T) {
	source, err := replication.NewSource(t.TempDir(), "native-pitr-missing", 17)
	require.NoError(t, err)
	_, err = RestoreNativeUntilPositions(source, map[string]uint64{"binlog.000999": 4})
	require.ErrorContains(t, err, `native binlog file "binlog.000999" does not exist`)
}

func nativeCommitEndPosition(t *testing.T, events []replication.NativeBinlogEvent) uint64 {
	t.Helper()
	for _, event := range events {
		if event.Type == 16 {
			return event.EndPosition
		}
	}
	t.Fatal("native XID event not found")
	return 0
}
