package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestReplicationStorageCommitBindsIdentityInWAL(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	engine := NewXMySQLEngine(cfg)
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table wal_identity (id int primary key)")

	const transactionID = "source-a:42"
	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	session.SetParamByName("replication_transaction_id", transactionID)
	session.SetParamByName("transaction_journal_id", replicationTransactionJournalID(transactionID))
	session.SetParamByName("autocommit", "0")
	require.NoError(t, engine.QueryExecutor.beginReplicationStorageTransaction(session))
	require.NoError(t, executeReplicationQuery(engine, session, "begin", ""))
	require.NoError(t, executeReplicationQuery(engine, session, "insert into wal_identity values (1)", "app"))
	require.NoError(t, executeReplicationQuery(engine, session, "commit", ""))
	require.NoError(t, engine.Close())

	commitData, err := readRedoCommitData(filepath.Join(dataDir, "redo", "redo.log"))
	require.NoError(t, err)
	require.Equal(t, []byte(transactionID), commitData)
}

func TestClientStorageCommitBindsCommitKeyInWAL(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	engine := NewXMySQLEngine(cfg)
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table client_wal_identity (id int primary key)")
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "app", "begin")
	mustExecSessionSQL(t, engine, session, "app", "insert into client_wal_identity values (1)")
	commitKey := strings.TrimSpace(engine.QueryExecutor.sessionTransactionState(session).CommitKey)
	require.NotEmpty(t, commitKey)
	mustExecSessionSQL(t, engine, session, "app", "commit")
	require.NoError(t, engine.Close())

	commitData, err := readRedoCommitData(filepath.Join(dataDir, "redo", "redo.log"))
	require.NoError(t, err)
	require.Equal(t, []byte(commitKey), commitData)
}

func TestXAStorageCommitBindsXIDInWAL(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	engine := NewXMySQLEngine(cfg)
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table xa_wal_identity (id int primary key)")
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "app", "XA START 'wal-xa','branch',17")
	mustExecSessionSQL(t, engine, session, "app", "insert into xa_wal_identity values (1)")
	mustExecSessionSQL(t, engine, session, "app", "XA END 'wal-xa','branch',17")
	mustExecSessionSQL(t, engine, session, "app", "XA COMMIT 'wal-xa','branch',17 ONE PHASE")
	require.NoError(t, engine.Close())

	expected := fmt.Sprintf("%d:%x:%x", 17, []byte("wal-xa"), []byte("branch"))
	commitData, err := readRedoCommitDataMatching(filepath.Join(dataDir, "redo", "redo.log"), []byte(expected))
	require.NoError(t, err)
	require.Equal(t, []byte(expected), commitData)
}

func TestXACommitJournalBindsXIDForRecoveryIdentity(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table xa_journal_identity (id int primary key)")
	session := newTestMySQLSession()
	executor.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		return fmt.Errorf("injected publication failure for %s", transactionID)
	})
	for _, query := range []string{
		"XA START 'journal-xa','branch',23",
		"insert into xa_journal_identity values (1)",
		"XA END 'journal-xa','branch',23",
	} {
		mustExecSessionSQL(t, executor, session, "app", query)
	}
	commitResult := <-executor.ExecuteQuery(session, "XA COMMIT 'journal-xa','branch',23 ONE PHASE", "app")
	require.Error(t, commitResult.Err)

	journalPath := transactionJournalPath(dataDir, executor.QueryExecutor.transactionJournalID(session))
	raw, err := os.ReadFile(journalPath)
	require.NoError(t, err)
	journalIdentity, _, err := journalReplicationCommitRecord(raw)
	require.NoError(t, err)
	require.Equal(t, "23:6a6f75726e616c2d7861:6272616e6368", journalIdentity)
}

func TestXAChangeJournalBindsXIDBeforePhysicalCommit(t *testing.T) {
	dataDir := t.TempDir()
	executor := newTestStorageIntegratedExecutor(t, dataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table xa_precommit_journal_identity (id int primary key)")
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "XA START 'precommit-xa','branch',29")
	mustExecSessionSQL(t, executor, session, "app", "insert into xa_precommit_journal_identity values (1)")

	journalPath := transactionJournalPath(dataDir, executor.QueryExecutor.transactionJournalID(session))
	raw, err := os.ReadFile(journalPath)
	require.NoError(t, err)
	changes, err := decodeDurableJournal(raw)
	require.NoError(t, err)
	journalIdentity, _, _ := durableJournalMetadata(changes)
	require.Equal(t, "29:707265636f6d6d69742d7861:6272616e6368", journalIdentity)
}

func readRedoCommitData(path string) ([]byte, error) {
	return readRedoCommitDataMatching(path, nil)
}

func readRedoCommitDataMatching(path string, expected []byte) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	for {
		var lsn uint64
		if err := binary.Read(file, binary.BigEndian, &lsn); err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		var trxID int64
		var pageID uint64
		var entryType uint8
		var dataLen uint16
		if err := binary.Read(file, binary.BigEndian, &trxID); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.BigEndian, &pageID); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.BigEndian, &entryType); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}
		if entryType == manager.LOG_TYPE_TXN_COMMIT {
			if expected == nil || string(data) == string(expected) {
				return data, nil
			}
		}
	}
}
