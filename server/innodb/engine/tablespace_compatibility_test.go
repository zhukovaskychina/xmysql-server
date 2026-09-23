package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTablespaceDDLCompatibilityManagesPhysicalFile(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)

	created := <-executor.ExecuteQuery(nil, "CREATE TABLESPACE ledger ADD DATAFILE 'ignored.ibd' ENGINE=InnoDB", "")
	require.NoError(t, created.Err)
	require.FileExists(t, filepath.Join(dataDir, "ledger.ibd"))

	renamed := <-executor.ExecuteQuery(nil, "ALTER TABLESPACE ledger RENAME TO archive", "")
	require.NoError(t, renamed.Err)
	require.NoFileExists(t, filepath.Join(dataDir, "ledger.ibd"))
	require.FileExists(t, filepath.Join(dataDir, "archive.ibd"))

	dropped := <-executor.ExecuteQuery(nil, "DROP TABLESPACE archive ENGINE=InnoDB", "")
	require.NoError(t, dropped.Err)
	require.NoFileExists(t, filepath.Join(dataDir, "archive.ibd"))
	_, err := executor.GetStorageManager().GetTablespace("archive")
	require.Error(t, err)
}

func TestTablespaceDDLImplicitlyCommitsTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table tx_table (id int primary key)")
	mustExecSessionSQL(t, executor, session, "app", "begin")
	mustExecSessionSQL(t, executor, session, "app", "select * from tx_table")

	mustExecSessionSQL(t, executor, session, "app", "create tablespace boundary add datafile 'boundary.ibd'")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.False(t, sessionHasTransactionTableLocks(session))
	mustExecSessionSQL(t, executor, session, "app", "drop tablespace boundary")
}

func TestExplicitTransactionBlocksAlterTableTablespaceUntilCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	mover := newTestMySQLSession()
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create tablespace archive add datafile 'archive.ibd'")
	mustExecSessionSQL(t, executor, owner, "app", "create table ledger (id int primary key)")
	mustExecSessionSQL(t, executor, owner, "app", "insert into ledger values (1)")
	mustExecSessionSQL(t, executor, owner, "app", "begin")
	mustExecSessionSQL(t, executor, owner, "app", "select * from ledger")

	moveDone := make(chan *Result, 1)
	go func() {
		moveDone <- <-executor.ExecuteQuery(mover, "alter table ledger tablespace archive", "app")
	}()

	select {
	case result := <-moveDone:
		t.Fatalf("ALTER TABLE ... TABLESPACE completed before the transaction boundary: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	mustExecSessionSQL(t, executor, owner, "app", "commit")
	select {
	case result := <-moveDone:
		require.NoError(t, result.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("ALTER TABLE ... TABLESPACE did not complete after COMMIT released the metadata lock")
	}
}

func TestTablespaceDropRejectsTableOwnedSpace(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")

	result := <-executor.ExecuteQuery(nil, "DROP TABLESPACE `app/ledger` ENGINE=InnoDB", "")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "in use by table app.ledger")
	require.FileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "app", "ledger.ibd"))
}

func TestTablespaceCreateRejectsUnknownOptions(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := <-executor.ExecuteQuery(nil, "CREATE TABLESPACE ledger ENCRYPTION='Y'", "")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "unsupported CREATE TABLESPACE options")
	require.NoFileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "ledger.ibd"))
}

func TestTablespaceCreateIfNotExistsMatchesSQLSemantics(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace ledger add datafile 'ledger.ibd'")

	duplicate := <-executor.ExecuteQuery(nil, "create tablespace ledger add datafile 'ledger.ibd'", "")
	require.Error(t, duplicate.Err)
	require.Contains(t, duplicate.Err.Error(), "already exists")

	idempotent := <-executor.ExecuteQuery(nil, "create tablespace if not exists ledger add datafile 'ledger.ibd'", "")
	require.NoError(t, idempotent.Err)
}

func TestTablespaceCanBeMountedByMultipleTables(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace shared add datafile 'shared.ibd'")
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table first (id int primary key) tablespace shared")
	mustExecSQL(t, executor, "app", "create table second (id int primary key) tablespace shared")

	storage := executor.GetStorageManager().GetTableStorageManager()
	first, err := storage.GetTableStorageInfo("app", "first")
	require.NoError(t, err)
	second, err := storage.GetTableStorageInfo("app", "second")
	require.NoError(t, err)
	require.Equal(t, first.SpaceID, second.SpaceID)
	require.False(t, first.OwnsTablespace)
	require.False(t, second.OwnsTablespace)
	mustExecSQL(t, executor, "app", "insert into first values (1)")
	mustExecSQL(t, executor, "app", "insert into second values (2)")
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "select id from first"))
	require.Equal(t, [][]interface{}{{"2"}}, mustQuerySQL(t, executor, "app", "select id from second"))

	mustExecSQL(t, executor, "app", "drop table first")
	require.FileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "shared.ibd"))
	mustExecSQL(t, executor, "app", "drop table second")
	require.FileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "shared.ibd"))
	mustExecSQL(t, executor, "", "drop tablespace shared")
	require.NoFileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "shared.ibd"))
}

func TestAlterTableTablespaceMovesRowsAndKeepsGeneralSpace(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace archive add datafile 'archive.ibd'")
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into ledger values (7), (9)")
	oldInfo, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "ledger")
	require.NoError(t, err)
	oldSpaceID := oldInfo.SpaceID

	mustExecSQL(t, executor, "app", "alter table ledger tablespace archive")
	newInfo, err := executor.GetStorageManager().GetTableStorageManager().GetTableStorageInfo("app", "ledger")
	require.NoError(t, err)
	require.NotEqual(t, oldSpaceID, newInfo.SpaceID)
	require.False(t, newInfo.OwnsTablespace)
	require.Equal(t, [][]interface{}{{"7"}, {"9"}}, mustQuerySQL(t, executor, "app", "select id from ledger order by id"))
	require.NoFileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "app", "ledger.ibd"))
	require.FileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "archive.ibd"))
}

func TestFilePerTableTablespaceDiscardAndImportRoundTrip(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key)")
	mustExecSQL(t, executor, "app", "insert into ledger values (7), (9)")

	dataPath := filepath.Join(executor.GetStorageManager().DataDir(), "app", "ledger.ibd")
	backupPath := filepath.Join(t.TempDir(), "ledger.ibd")
	backup, err := os.ReadFile(dataPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(backupPath, backup, 0644))
	mustExecSQL(t, executor, "app", "alter table ledger discard tablespace")
	require.NoFileExists(t, dataPath)
	discardedQuery := <-executor.ExecuteQuery(nil, "select id from ledger", "app")
	require.Error(t, discardedQuery.Err)
	require.Contains(t, discardedQuery.Err.Error(), "IMPORT TABLESPACE")

	backup, err = os.ReadFile(backupPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dataPath, backup, 0644))
	mustExecSQL(t, executor, "app", "alter table ledger import tablespace")
	require.Equal(t, [][]interface{}{{"7"}, {"9"}}, mustQuerySQL(t, executor, "app", "select id from ledger order by id"))
}

func TestPartitionedFilePerTableTablespaceDiscardAndImportRoundTrip(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into events values (3), (17)")

	dataDir := executor.GetStorageManager().DataDir()
	backupDir := t.TempDir()
	for _, partition := range []string{"p0", "p1"} {
		path := filepath.Join(dataDir, "app", "events#"+partition+".ibd")
		backup, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(backupDir, partition+".ibd"), backup, 0644))
	}
	mustExecSQL(t, executor, "app", "alter table events discard tablespace")
	for _, partition := range []string{"p0", "p1"} {
		require.NoFileExists(t, filepath.Join(dataDir, "app", "events#"+partition+".ibd"))
		backup, err := os.ReadFile(filepath.Join(backupDir, partition+".ibd"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "app", "events#"+partition+".ibd"), backup, 0644))
	}
	mustExecSQL(t, executor, "app", "alter table events import tablespace")
	require.Equal(t, [][]interface{}{{"3"}, {"17"}}, mustQuerySQL(t, executor, "app", "select id from events order by id"))
}

func TestTablespaceDiscardRejectsGeneralTablespaceAttachment(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create tablespace shared add datafile 'shared.ibd'")
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table ledger (id int primary key) tablespace shared")

	result := <-executor.ExecuteQuery(nil, "alter table ledger discard tablespace", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "shared or general tablespace")
	require.FileExists(t, filepath.Join(executor.GetStorageManager().DataDir(), "shared.ibd"))
}

func TestAlterPartitionedTableTablespaceMovesEachPartitionIntoSharedSpace(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create tablespace archive add datafile 'ignored.ibd' engine=innodb")
	mustExecSQL(t, executor, "app", "create table events (id int primary key) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	mustExecSQL(t, executor, "app", "insert into events values (3), (17)")
	oldSpaces, err := executor.GetStorageManager().ListSpaces()
	require.NoError(t, err)
	oldPartitionSpaceNames := map[string]bool{}
	for _, space := range oldSpaces {
		if strings.HasPrefix(space.Name, "app/events#") {
			oldPartitionSpaceNames[space.Name] = true
		}
	}

	mustExecSQL(t, executor, "app", "alter table events tablespace archive")
	storage := executor.GetStorageManager().GetTableStorageManager()
	info, err := storage.GetTableStorageInfo("app", "events")
	require.NoError(t, err)
	require.Len(t, info.Partitions, 2)
	require.Equal(t, info.Partitions[0].SpaceID, info.Partitions[1].SpaceID)
	require.NotZero(t, info.Partitions[0].RootPageNo)
	require.NotZero(t, info.Partitions[1].RootPageNo)
	require.NotEqual(t, info.Partitions[0].RootPageNo, info.Partitions[1].RootPageNo)
	require.Equal(t, [][]interface{}{{"3"}, {"17"}}, mustQuerySQL(t, executor, "app", "select id from events order by id"))
	spaces, err := executor.GetStorageManager().ListSpaces()
	require.NoError(t, err)
	for _, space := range spaces {
		require.False(t, oldPartitionSpaceNames[space.Name], "old partition space %s should be removed", space.Name)
	}

	general, err := executor.GetStorageManager().GetTablespace("archive")
	require.NoError(t, err)
	require.Equal(t, general.SpaceID, info.Partitions[0].SpaceID)
}
