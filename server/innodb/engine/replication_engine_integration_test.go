package engine

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestEngineReplicationConfigEnablesQuorumAutoFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceAddress := reserveTCPAddress(t)
	firstAddress := reserveTCPAddress(t)
	secondAddress := reserveTCPAddress(t)
	sourceURL := "http://" + sourceAddress
	firstURL := "http://" + firstAddress
	secondURL := "http://" + secondAddress

	newConfig := func(role, uuid string, serverID uint32, listenAddress, source string, peers []string) *conf.Cfg {
		dataDir := t.TempDir()
		return &conf.Cfg{
			DataDir:                           dataDir,
			InnodbDataDir:                     dataDir,
			InnodbBufferPoolSize:              16 * 1024 * 1024,
			ReplicationRole:                   role,
			ReplicationUUID:                   uuid,
			ReplicationServerID:               serverID,
			ReplicationListenAddress:          listenAddress,
			ReplicationSourceURL:              source,
			ReplicationPeers:                  peers,
			ReplicationAutoFailover:           role == replication.RoleReplica,
			ReplicationFailureTimeoutDuration: 150 * time.Millisecond,
			ReplicationPollIntervalDuration:   20 * time.Millisecond,
		}
	}

	source := NewXMySQLEngine(newConfig(replication.RoleSource, "config-source", 1, sourceAddress, "", nil))
	first := NewXMySQLEngine(newConfig(replication.RoleReplica, "config-first", 10, firstAddress, sourceURL, []string{sourceURL, secondURL}))
	second := NewXMySQLEngine(newConfig(replication.RoleReplica, "config-second", 20, secondAddress, sourceURL, []string{sourceURL, firstURL}))
	sourceClosed := false
	t.Cleanup(func() {
		if !sourceClosed {
			require.NoError(t, source.Close())
		}
		require.NoError(t, first.Close())
		require.NoError(t, second.Close())
	})
	require.NoError(t, source.Start(ctx))
	require.NoError(t, first.Start(ctx))
	require.NoError(t, second.Start(ctx))

	require.Eventually(t, func() bool {
		return source.ReplicationStatus().(replication.StatusSnapshot).Role == replication.RoleSource &&
			first.ReplicationStatus().(replication.StatusSnapshot).Role == replication.RoleReplica &&
			second.ReplicationStatus().(replication.StatusSnapshot).Role == replication.RoleReplica
	}, 3*time.Second, 20*time.Millisecond)

	require.NoError(t, source.Close())
	sourceClosed = true
	require.Eventually(t, func() bool {
		firstStatus := first.ReplicationStatus().(replication.StatusSnapshot)
		secondStatus := second.ReplicationStatus().(replication.StatusSnapshot)
		return firstStatus.Role == replication.RoleSource && secondStatus.Role == replication.RoleReplica
	}, 5*time.Second, 20*time.Millisecond)
}

func TestEngineReplicationConfigRequiresQuorumBeforeAutoFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceAddress := reserveTCPAddress(t)
	replicaAddress := reserveTCPAddress(t)
	unreachablePeerAddress := reserveTCPAddress(t)
	sourceURL := "http://" + sourceAddress
	unreachablePeerURL := "http://" + unreachablePeerAddress

	newConfig := func(role, uuid string, serverID uint32, listenAddress, source string, peers []string) *conf.Cfg {
		dataDir := t.TempDir()
		return &conf.Cfg{
			DataDir:                           dataDir,
			InnodbDataDir:                     dataDir,
			InnodbBufferPoolSize:              16 * 1024 * 1024,
			ReplicationRole:                   role,
			ReplicationUUID:                   uuid,
			ReplicationServerID:               serverID,
			ReplicationListenAddress:          listenAddress,
			ReplicationSourceURL:              source,
			ReplicationPeers:                  peers,
			ReplicationAutoFailover:           role == replication.RoleReplica,
			ReplicationFailureTimeoutDuration: 100 * time.Millisecond,
			ReplicationPollIntervalDuration:   20 * time.Millisecond,
		}
	}

	source := NewXMySQLEngine(newConfig(replication.RoleSource, "quorum-source", 1, sourceAddress, "", nil))
	replica := NewXMySQLEngine(newConfig(replication.RoleReplica, "quorum-replica", 10, replicaAddress, sourceURL, []string{sourceURL, unreachablePeerURL}))
	t.Cleanup(func() {
		require.NoError(t, replica.Close())
		require.NoError(t, source.Close())
	})
	require.NoError(t, source.Start(ctx))
	require.NoError(t, replica.Start(ctx))

	require.Eventually(t, func() bool {
		return source.ReplicationStatus().(replication.StatusSnapshot).Role == replication.RoleSource &&
			replica.ReplicationStatus().(replication.StatusSnapshot).Role == replication.RoleReplica
	}, 3*time.Second, 20*time.Millisecond)

	require.NoError(t, source.Close())
	require.Eventually(t, func() bool {
		return replica.ReplicationStatus().(replication.StatusSnapshot).Role == replication.RoleReplica
	}, 2*time.Second, 20*time.Millisecond,
		"an isolated replica must not self-promote without a majority of configured members")
}

func TestEngineSourceRejectsClientWritesWhenFenced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "fenced-engine-source",
		ReplicationServerID:             109,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table fenced_items (id int primary key)")
	require.NoError(t, engine.Start(ctx))

	payload := strings.NewReader(`{"epoch":1,"candidate_uuid":"promoted-engine-replica"}`)
	request, err := http.NewRequest(http.MethodPost, "http://"+engine.replicationRuntime.Address()+"/replication/fence", payload)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()

	result := <-engine.ExecuteQuery(newTestMySQLSession(), "insert into fenced_items values (1)", "app")
	require.Error(t, result.Err)
	require.ErrorContains(t, result.Err, "fenced")
}

func TestEngineSourceCapturesReplaceAndOnDuplicateKeyTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "replace-odku-source",
		ReplicationServerID:             110,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table items (id int primary key, value varchar(64))")
	require.NoError(t, engine.Start(ctx))

	session := newTestMySQLSession()
	t.Cleanup(func() { engine.QueryExecutor.clearSessionTransactionState(session) })
	mustExecSessionSQL(t, engine, session, "app", "insert into items values (1, 'initial')")
	initialGTIDCount := len(engine.ReplicationSource().Executed["replace-odku-source"])

	mustExecSessionSQL(t, engine, session, "app", "replace into items values (1, 'replaced')")
	replaceGTIDCount := len(engine.ReplicationSource().Executed["replace-odku-source"])
	require.Equal(t, initialGTIDCount+1, replaceGTIDCount,
		"REPLACE must publish one committed native transaction")

	mustExecSessionSQL(t, engine, session, "app", "insert into items values (1, 'updated') on duplicate key update value = values(value)")
	odkuGTIDCount := len(engine.ReplicationSource().Executed["replace-odku-source"])
	require.Equal(t, replaceGTIDCount+1, odkuGTIDCount,
		"ON DUPLICATE KEY UPDATE must publish one committed native transaction")

	rows := mustQuerySQL(t, engine, "app", "select id, value from items")
	require.Equal(t, [][]interface{}{{"1", "updated"}}, rows)
}

func TestCommittedStorageWALPreventsOrphanJournalRollback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table committed_rows (id int primary key, value varchar(64))")
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "app", "begin")
	mustExecSessionSQL(t, engine, session, "app", "insert into committed_rows values (1, 'durable')")
	shared, ok := session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext)
	require.True(t, ok)
	require.NotNil(t, shared)

	// Simulate a process failure after the physical storage/WAL commit but
	// before the higher-level replication commit journal record is appended.
	dml, err := engine.QueryExecutor.newStorageIntegratedDMLExecutor()
	require.NoError(t, err)
	commitContext := transactionContextForSession(context.Background(), session)
	commitContext = context.WithValue(commitContext, clientStorageTransactionContextKey, nil)
	commitContext = context.WithValue(commitContext, storageTransactionSessionContextKey, nil)
	commitContext = context.WithValue(commitContext, storageTransactionForceCommitContextKey, true)
	require.NoError(t, dml.commitStorageTransaction(commitContext, shared))
	require.Equal(t, "COMMITTED", shared.Status)
	require.NoError(t, engine.Close())

	restarted := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	var recoveredTransactionID string
	var recoveredChanges []replication.RowChange
	restarted.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		recoveredTransactionID = transactionID
		recoveredChanges = append([]replication.RowChange(nil), changes...)
		return nil
	})
	require.NoError(t, restarted.Start(ctx))
	require.NotEmpty(t, recoveredTransactionID)
	require.Len(t, recoveredChanges, 1)
	require.Equal(t, []string{"insert"}, []string{recoveredChanges[0].Action})
	require.Equal(t, [][]interface{}{{"1", "durable"}}, mustQuerySQL(t, restarted, "app", "select id, value from committed_rows"))
}

func TestEngineReloadsPersistedDictionaryTablesIntoStorageMappingAfterRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	first := NewXMySQLEngine(cfg)
	mustExecSQL(t, first, "", "create database restart_mapping")
	mustExecSQL(t, first, "restart_mapping", "create table rows (id int primary key, value varchar(64))")
	mustExecSQL(t, first, "restart_mapping", "insert into rows values (1, 'before-restart')")
	require.NoError(t, first.Close())

	second := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.Start(ctx))
	mustExecSQL(t, second, "restart_mapping", "insert into rows values (2, 'after-restart')")
	require.Equal(t, [][]interface{}{{"1", "before-restart"}, {"2", "after-restart"}}, mustQuerySQL(t, second, "restart_mapping", "select id, value from rows order by id"))
}

func TestReplicationStatementApplyIsIdempotentAcrossCommitStateRecovery(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table items (id int primary key, value varchar(64))")

	statement := replication.Statement{Database: "app", SQL: "insert into items (id, value) values (1, 'replayed')"}
	require.NoError(t, engine.applyReplicationStatementsWithID("source-a:1", []replication.Statement{statement}))
	require.NoError(t, engine.Close())
	engine = NewXMySQLEngine(cfg)
	require.NoError(t, engine.applyReplicationStatementsWithID("source-a:1", []replication.Statement{statement}))

	rows := mustQuerySQL(t, engine, "app", "select id, value from items")
	require.Equal(t, [][]interface{}{{"1", "replayed"}}, rows)
}

func TestReplicationStatementApplyCreatesQualifiedDDLVisibleToQueries(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	require.NoError(t, engine.applyReplicationStatementsWithID("source-a:ddl-db", []replication.Statement{{
		SQL: "CREATE DATABASE replication_ddl_visible",
	}}))
	require.NoError(t, engine.applyReplicationStatementsWithID("source-a:ddl-table", []replication.Statement{{
		SQL: "CREATE TABLE replication_ddl_visible.rows (id INT PRIMARY KEY, value VARCHAR(64))",
	}}))

	require.Equal(t, [][]interface{}{}, mustQuerySQL(t, engine, "", "SELECT id, value FROM replication_ddl_visible.rows ORDER BY id"))
}

func TestReplicationReplayUsesSharedStorageTransactionBoundary(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table shared_replay (id int primary key, value varchar(64))")

	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	session.SetParamByName("autocommit", "0")
	require.NoError(t, engine.QueryExecutor.beginReplicationStorageTransaction(session))
	shared, ok := session.GetParamByName(replicationStorageTransactionContextKey).(*StorageTransactionContext)
	require.True(t, ok)
	require.NotNil(t, shared)
	require.Nil(t, session.GetParamByName(clientStorageTransactionContextKey), "replication replay must not be registered as a client storage transaction")

	require.NoError(t, executeReplicationQuery(engine, session, "begin", ""))
	require.NoError(t, executeReplicationQuery(engine, session, "insert into shared_replay values (1, 'one')", "app"))
	require.NoError(t, executeReplicationQuery(engine, session, "insert into shared_replay values (2, 'two')", "app"))
	require.Same(t, shared, session.GetParamByName(replicationStorageTransactionContextKey))
	require.Equal(t, "ACTIVE", shared.Status)

	require.NoError(t, executeReplicationQuery(engine, session, "commit", ""))
	require.Nil(t, session.GetParamByName(replicationStorageTransactionContextKey))
	require.Equal(t, "COMMITTED", shared.Status)
	require.Equal(t, [][]interface{}{{"1", "one"}, {"2", "two"}}, mustQuerySQL(t, engine, "app", "select id, value from shared_replay order by id"))
}

func TestReplicationCommitMarkerFailureLeavesJournalRecoverable(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table items (id int primary key, value varchar(64))")

	transactionID := "source-a:2"
	statement := replication.Statement{Database: "app", SQL: "insert into items (id, value) values (2, 'recoverable')"}
	engine.QueryExecutor.replicationCommitMarkerHook = func(string) error {
		return errors.New("injected replication commit marker failure")
	}
	require.ErrorContains(t, engine.applyReplicationStatementsWithID(transactionID, []replication.Statement{statement}), "injected replication commit marker failure")

	rows := mustQuerySQL(t, engine, "app", "select id from items where id = 2")
	require.Len(t, rows, 1, "the storage commit happened before the marker failure")
	require.ErrorContains(t, engine.QueryExecutor.RecoverOrphanedTransactions(), "injected replication commit marker failure")
	rows = mustQuerySQL(t, engine, "app", "select id from items where id = 2")
	require.Len(t, rows, 1, "the durable commit record must protect a completed storage transaction")
	_, markerErr := os.Stat(replicationCommitMarkerPath(engine.QueryExecutor.getDataDir(), transactionID))
	require.ErrorIs(t, markerErr, os.ErrNotExist, "a failed marker hook must leave the marker pending until recovery can complete it")

	engine.QueryExecutor.replicationCommitMarkerHook = nil
	require.NoError(t, engine.QueryExecutor.RecoverOrphanedTransactions())
	_, markerErr = os.Stat(replicationCommitMarkerPath(engine.QueryExecutor.getDataDir(), transactionID))
	require.NoError(t, markerErr, "recovery must finalize the applied marker after the storage commit is durable")
	require.NoError(t, engine.applyReplicationStatementsWithID(transactionID, []replication.Statement{statement}))
	rows = mustQuerySQL(t, engine, "app", "select id from items where id = 2")
	require.Len(t, rows, 1, "retry must be idempotent after the active journal records commit")

	require.NoError(t, engine.Close())
	engine = NewXMySQLEngine(cfg)
	require.NoError(t, engine.QueryExecutor.RecoverOrphanedTransactions())
	require.NoError(t, engine.applyReplicationStatementsWithID(transactionID, []replication.Statement{statement}))
	rows = mustQuerySQL(t, engine, "app", "select id from items where id = 2")
	require.Len(t, rows, 1, "restart must preserve the committed transaction and suppress duplicate replay")
}

func TestReplicationStorageCommitMarkerFailureCanRetrySameCommittedContext(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir
	executor := newTestStorageIntegratedExecutor(t, cfg.DataDir)
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table retry_marker (id int primary key, value varchar(64))")

	transactionID := "source-a:same-context-retry"
	session := newReplicationSession()
	session.SetParamByName("replication_replay", true)
	session.SetParamByName("replication_transaction_id", transactionID)
	session.SetParamByName("transaction_journal_id", replicationTransactionJournalID(transactionID))
	session.SetParamByName("autocommit", "0")
	require.NoError(t, executor.QueryExecutor.beginReplicationStorageTransaction(session))
	require.NoError(t, executeReplicationQuery(executor, session, "insert into retry_marker values (1, 'committed')", "app"))

	markerFailures := 0
	executor.QueryExecutor.replicationCommitMarkerHook = func(string) error {
		markerFailures++
		if markerFailures == 1 {
			return errors.New("injected transient replication commit marker failure")
		}
		return nil
	}
	firstErr := executor.QueryExecutor.commitReplicationStorageTransaction(session)
	require.ErrorContains(t, firstErr, "injected transient replication commit marker failure")
	shared, ok := session.GetParamByName(replicationStorageTransactionContextKey).(*StorageTransactionContext)
	require.True(t, ok)
	require.Equal(t, "COMMITTED", shared.Status)

	// The storage/WAL commit is already authoritative. Retrying the same
	// context must finish the publication marker without calling Commit again.
	require.NoError(t, executor.QueryExecutor.commitReplicationStorageTransaction(session))
	require.Nil(t, session.GetParamByName(replicationStorageTransactionContextKey))
	require.Equal(t, 2, markerFailures)
	require.True(t, func() bool {
		committed, err := executor.QueryExecutor.replicationTransactionCommitted(transactionID)
		return err == nil && committed
	}())
	require.Equal(t, [][]interface{}{{"1", "committed"}}, mustQuerySQL(t, executor, "app", "select id, value from retry_marker"))
}

func TestReplicationStorageCommitFlushFailureLeavesDurableCommitRecord(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newReplicationSession()
	transactionID := "source-a:flush-failure"
	session.SetParamByName("replication_replay", true)
	session.SetParamByName("replication_transaction_id", transactionID)
	session.SetParamByName("transaction_journal_id", replicationTransactionJournalID(transactionID))
	session.SetParamByName("autocommit", "0")

	require.NoError(t, engine.QueryExecutor.beginReplicationStorageTransaction(session))
	shared, ok := session.GetParamByName(replicationStorageTransactionContextKey).(*StorageTransactionContext)
	require.True(t, ok)
	require.NotNil(t, shared)
	// A malformed dirty page makes the post-commit flush fail after the storage
	// transaction has already reached COMMITTED. The recovery record must be
	// written before that error is returned.
	bpm, ok := engine.QueryExecutor.bufferPoolManager.(*manager.OptimizedBufferPoolManager)
	require.True(t, ok)
	page, err := bpm.GetPage(0, 0)
	require.NoError(t, err)
	page.SetContent([]byte{1})
	page.SetDirty(true)
	shared.ModifiedPages["0:0"] = 0

	err = engine.QueryExecutor.commitReplicationStorageTransaction(session)
	require.Error(t, err)
	_, markerErr := os.Stat(replicationCommitMarkerPath(engine.QueryExecutor.getDataDir(), transactionID))
	require.NoError(t, markerErr, "applied GTID marker must be durable before a later dirty-page flush failure")
	committed, checkErr := engine.QueryExecutor.replicationTransactionCommitted(transactionID)
	require.NoError(t, checkErr)
	require.True(t, committed, "a committed storage transaction must not be recovered as an orphan after flush failure")
}

func TestClientCommitPublishesAfterStorageAndRetriesByStableTransactionKey(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "client-commit-source", 21)
	require.NoError(t, err)

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table client_commit_retry (id int primary key, value varchar(32))")

	postAppendFailure := true
	keys := make([]string, 0, 2)
	executor.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		keys = append(keys, transactionID)
		if _, err := source.AppendCommittedTransactionWithKey(transactionID, changes, statements); err != nil {
			return err
		}
		if postAppendFailure {
			postAppendFailure = false
			return errors.New("injected post-append commit error")
		}
		return nil
	})

	mustExecSessionSQL(t, executor, session, "app", "start transaction")
	mustExecSessionSQL(t, executor, session, "app", "insert into client_commit_retry values (1, 'committed')")
	firstCommit := <-executor.ExecuteQuery(session, "commit", "app")
	require.ErrorContains(t, firstCommit.Err, "injected post-append commit error")
	require.True(t, sessionBoolParam(session, "in_transaction"), "a failed publisher must leave the transaction retryable")
	require.Nil(t, session.GetParamByName(clientStorageTransactionContextKey), "storage must already be committed before publisher failure")
	require.Equal(t, [][]interface{}{{"1", "committed"}}, mustQuerySQL(t, executor, "app", "select id, value from client_commit_retry"))
	require.Len(t, source.Executed["client-commit-source"], 1, "the first attempt must publish one GTID")

	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, keys, 2)
	require.Equal(t, keys[0], keys[1], "commit retry must reuse one transaction key")
	require.Len(t, source.Executed["client-commit-source"], 1, "retry must not allocate a second GTID")
	events, err := source.DecodeNativeDumpFrom("binlog.000001", 4, replication.GTIDIntervals{})
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestClientCommitSyncsRecoveryJournalBeforePhysicalCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database app")
	mustExecSQL(t, executor, "app", "create table journal_sync_order (id int primary key, value varchar(32))")

	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "app", "start transaction")
	mustExecSessionSQL(t, executor, session, "app", "insert into journal_sync_order values (1, 'pending')")
	shared, ok := session.GetParamByName(clientStorageTransactionContextKey).(*StorageTransactionContext)
	require.True(t, ok)
	require.NotNil(t, shared)
	require.NotNil(t, shared.RealTransaction)

	syncCalls := 0
	executor.QueryExecutor.transactionJournalSyncHook = func(string) error {
		syncCalls++
		return errors.New("injected pre-commit journal sync failure")
	}
	commit := <-executor.ExecuteQuery(session, "commit", "app")
	require.ErrorContains(t, commit.Err, "injected pre-commit journal sync failure")
	require.Equal(t, 1, syncCalls)
	require.Same(t, shared, session.GetParamByName(clientStorageTransactionContextKey), "the storage transaction must remain retryable")
	require.Equal(t, "ACTIVE", shared.Status, "physical storage commit must not outrun the recovery journal")
	committed, err := executor.QueryExecutor.txManager.GetRedoLogManager().HasCommittedTransaction(shared.RealTransaction.ID)
	require.NoError(t, err)
	require.False(t, committed)

	executor.QueryExecutor.transactionJournalSyncHook = nil
	mustExecSessionSQL(t, executor, session, "app", "commit")
	require.Equal(t, [][]interface{}{{"1", "pending"}}, mustQuerySQL(t, executor, "app", "select id, value from journal_sync_order"))
}

func TestClientCommitRecoveryPreservesStorageAfterPublisherErrorAndRestart(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir

	engine := NewXMySQLEngine(cfg)
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table client_restart (id int primary key, value varchar(32))")
	source, err := replication.NewSource(t.TempDir(), "client-restart-source", 22)
	require.NoError(t, err)
	postAppendFailure := true
	engine.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		if _, appendErr := source.AppendCommittedTransactionWithKey(transactionID, changes, statements); appendErr != nil {
			return appendErr
		}
		if postAppendFailure {
			postAppendFailure = false
			return errors.New("injected restart-window publisher error")
		}
		return nil
	})

	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	mustExecSessionSQL(t, engine, session, "app", "insert into client_restart values (1, 'committed')")
	commit := <-engine.ExecuteQuery(session, "commit", "app")
	require.ErrorContains(t, commit.Err, "injected restart-window publisher error")
	require.Equal(t, [][]interface{}{{"1", "committed"}}, mustQuerySQL(t, engine, "app", "select id, value from client_restart"))
	require.NoError(t, engine.Close())

	restarted := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	require.NoError(t, restarted.QueryExecutor.RecoverOrphanedTransactions())
	require.Equal(t, [][]interface{}{{"1", "committed"}}, mustQuerySQL(t, restarted, "app", "select id, value from client_restart"), "a committed storage transaction must not be rolled back after publisher failure and restart")
}

func TestClientCommitRecoveryRepublishesPendingJournalAfterRestart(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:              t.TempDir(),
		InnodbDataDir:        t.TempDir(),
		InnodbBufferPoolSize: 16 * 1024 * 1024,
	}
	cfg.InnodbDataDir = cfg.DataDir

	engine := NewXMySQLEngine(cfg)
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table client_restart_publish (id int primary key, value varchar(32))")
	source, err := replication.NewSource(t.TempDir(), "client-restart-publish-source", 23)
	require.NoError(t, err)

	engine.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(string, []replication.RowChange, []replication.Statement) error {
		return errors.New("injected publisher outage before append")
	})
	session := newTestMySQLSession()
	mustExecSessionSQL(t, engine, session, "app", "start transaction")
	mustExecSessionSQL(t, engine, session, "app", "insert into client_restart_publish values (1, 'committed')")
	commit := <-engine.ExecuteQuery(session, "commit", "app")
	require.ErrorContains(t, commit.Err, "injected publisher outage before append")
	require.NoError(t, engine.Close())

	restarted := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	var recoveredStatements []replication.Statement
	restarted.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		recoveredStatements = append([]replication.Statement(nil), statements...)
		_, appendErr := source.AppendCommittedTransactionWithKey(transactionID, changes, statements)
		return appendErr
	})
	require.NoError(t, restarted.QueryExecutor.RecoverOrphanedTransactions())
	require.Len(t, source.Executed["client-restart-publish-source"], 1)
	require.Equal(t, []replication.Statement{{Database: "app", SQL: "insert into client_restart_publish values (1, 'committed')"}}, recoveredStatements)
	require.Equal(t, [][]interface{}{{"1", "committed"}}, mustQuerySQL(t, restarted, "app", "select id, value from client_restart_publish"))
}

func TestAutocommitStatementRecoveryRepublishesPendingJournal(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "autocommit-statement-recovery-source", 24)
	require.NoError(t, err)

	firstAttempt := true
	executor.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		require.Empty(t, changes, "statement-only commit must not invent row images")
		if firstAttempt {
			firstAttempt = false
			return errors.New("injected statement publisher outage")
		}
		_, appendErr := source.AppendCommittedTransactionWithKey(transactionID, changes, statements)
		return appendErr
	})

	session := newTestMySQLSession()
	executor.QueryExecutor.recordReplicationStatement(session, replication.Statement{
		Database: "app",
		SQL:      "create table statement_only_recovery (id int primary key)",
	})
	require.Empty(t, source.Executed, "the first publisher attempt must fail before source append")

	require.NoError(t, executor.QueryExecutor.RecoverOrphanedTransactions())
	require.Len(t, source.Executed["autocommit-statement-recovery-source"], 1)
	require.NoError(t, executor.QueryExecutor.RecoverOrphanedTransactions(), "recovery should be idempotent after journal cleanup")
}

func TestEngineSourceReplicaReplicatesCommittedDML(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceControl := reserveTCPAddress(t)
	sourceCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbRedoLogDir:                filepath.Join(t.TempDir(), "redo"),
		InnodbUndoLogDir:                filepath.Join(t.TempDir(), "undo"),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "engine-source",
		ReplicationServerID:             101,
		ReplicationListenAddress:        sourceControl,
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	sourceCfg.InnodbDataDir = sourceCfg.DataDir
	source := NewXMySQLEngine(sourceCfg)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	mustExecSQL(t, source, "", "create database app")
	mustExecSQL(t, source, "app", "create table items (id int primary key, value varchar(64))")
	require.NoError(t, source.Start(ctx))

	replicaCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbRedoLogDir:                filepath.Join(t.TempDir(), "redo"),
		InnodbUndoLogDir:                filepath.Join(t.TempDir(), "undo"),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "replica",
		ReplicationUUID:                 "engine-replica",
		ReplicationServerID:             102,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationSourceURL:            "http://" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
		ReplicationReadOnly:             false,
	}
	replicaCfg.InnodbDataDir = replicaCfg.DataDir
	replica := NewXMySQLEngine(replicaCfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	mustExecSQL(t, replica, "", "create database app")
	mustExecSQL(t, replica, "app", "create table items (id int primary key, value varchar(64))")
	require.NoError(t, replica.Start(ctx))

	replica2Cfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "replica",
		ReplicationUUID:                 "engine-replica-2",
		ReplicationServerID:             103,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationSourceURL:            "http://" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
		ReplicationReadOnly:             false,
	}
	replica2Cfg.InnodbDataDir = replica2Cfg.DataDir
	replica2 := NewXMySQLEngine(replica2Cfg)
	t.Cleanup(func() { require.NoError(t, replica2.Close()) })
	mustExecSQL(t, replica2, "", "create database app")
	mustExecSQL(t, replica2, "app", "create table items (id int primary key, value varchar(64))")
	require.NoError(t, replica2.Start(ctx))

	session := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(session) })
	mustExecSessionSQL(t, source, session, "app", "insert into items (id, value) values (1, 'from-source')")

	require.Eventually(t, func() bool {
		result := <-replica.ExecuteQuery(nil, "select id, value from items", "app")
		if result == nil || result.Err != nil {
			return false
		}
		selected, ok := result.Data.(*SelectResult)
		if !ok || len(selected.Records) != 1 {
			return false
		}
		secondResult := <-replica2.ExecuteQuery(nil, "select id, value from items", "app")
		if secondResult == nil || secondResult.Err != nil {
			return false
		}
		secondSelected, secondOK := secondResult.Data.(*SelectResult)
		return secondOK && len(secondSelected.Records) == 1
	}, 5*time.Second, 20*time.Millisecond)

	rollbackSession := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(rollbackSession) })
	mustExecSessionSQL(t, source, rollbackSession, "app", "begin")
	mustExecSessionSQL(t, source, rollbackSession, "app", "insert into items (id, value) values (9, 'rolled-back')")
	mustExecSessionSQL(t, source, rollbackSession, "app", "rollback")
	time.Sleep(100 * time.Millisecond)
	result := <-replica.ExecuteQuery(nil, "select id from items where id = 9", "app")
	require.NoError(t, result.Err)
	rolledBack, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Empty(t, rolledBack.Records)

	commitSession := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(commitSession) })
	mustExecSessionSQL(t, source, commitSession, "app", "begin")
	mustExecSessionSQL(t, source, commitSession, "app", "insert into items (id, value) values (2, 'committed')")
	mustExecSessionSQL(t, source, commitSession, "app", "commit")
	require.Eventually(t, func() bool {
		result := <-replica.ExecuteQuery(nil, "select id from items where id = 2", "app")
		if result == nil || result.Err != nil {
			return false
		}
		selected, ok := result.Data.(*SelectResult)
		return ok && len(selected.Records) == 1
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, source.replicationRuntime.Close())
	source.replicationRuntime = nil
	require.NoError(t, replica.PromoteReplication())
	promotedSession := newTestMySQLSession()
	t.Cleanup(func() { replica.QueryExecutor.clearSessionTransactionState(promotedSession) })
	mustExecSessionSQL(t, replica, promotedSession, "app", "insert into items (id, value) values (3, 'after-promote')")
	promotedRows := mustQuerySQL(t, replica, "app", "select id from items where id = 3")
	require.Len(t, promotedRows, 1)
}

func TestEngineSourceReplicaReconnectsAfterSourceRestartWithoutDuplicateTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "restartable-engine-source",
		ReplicationServerID:             111,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	sourceCfg.InnodbDataDir = sourceCfg.DataDir
	var source *XMySQLEngine
	startSource := func() {
		source = NewXMySQLEngine(sourceCfg)
		require.NoError(t, source.Start(ctx))
	}
	startSource()
	t.Cleanup(func() {
		if source != nil {
			require.NoError(t, source.Close())
		}
	})
	mustExecSQL(t, source, "", "create database app")
	mustExecSQL(t, source, "app", "create table items (id int primary key, value varchar(64))")

	replicaCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "replica",
		ReplicationUUID:                 "restartable-engine-replica",
		ReplicationServerID:             112,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationSourceURL:            "http://" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
		ReplicationReadOnly:             false,
	}
	replicaCfg.InnodbDataDir = replicaCfg.DataDir
	replica := NewXMySQLEngine(replicaCfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	mustExecSQL(t, replica, "", "create database app")
	mustExecSQL(t, replica, "app", "create table items (id int primary key, value varchar(64))")
	require.NoError(t, replica.Start(ctx))

	firstSession := newTestMySQLSession()
	t.Cleanup(func() {
		if source != nil {
			source.QueryExecutor.clearSessionTransactionState(firstSession)
		}
	})
	mustExecSessionSQLFully(t, source, firstSession, "app", "insert into items values (1, 'before-restart')")
	require.Eventually(t, func() bool {
		return len(mustQuerySQL(t, replica, "app", "select id from items where id = 1")) == 1
	}, 5*time.Second, 20*time.Millisecond)

	// Keep the replica alive while the source endpoint disappears. The source
	// is restarted with the same data directory and UUID so its next GTID and
	// durable native history continue from the previous process.
	require.NoError(t, source.Close())
	source = nil
	time.Sleep(100 * time.Millisecond)
	startSource()
	secondSession := newTestMySQLSession()
	t.Cleanup(func() {
		if source != nil {
			source.QueryExecutor.clearSessionTransactionState(secondSession)
		}
	})
	mustExecSessionSQLFully(t, source, secondSession, "app", "insert into items values (2, 'after-restart')")
	mustExecSessionSQLFully(t, source, secondSession, "app", "xa start 'restart-xa', 'branch', 1")
	mustExecSessionSQLFully(t, source, secondSession, "app", "insert into items values (3, 'xa-after-restart')")
	mustExecSessionSQLFully(t, source, secondSession, "app", "xa end 'restart-xa', 'branch', 1")
	mustExecSessionSQLFully(t, source, secondSession, "app", "xa prepare 'restart-xa', 'branch', 1")
	mustExecSessionSQLFully(t, source, secondSession, "app", "xa commit 'restart-xa', 'branch', 1")

	require.Eventually(t, func() bool {
		rows := mustQuerySQL(t, replica, "app", "select id, value from items order by id")
		return len(rows) == 3 && rows[0][0] == "1" && rows[1][0] == "2" && rows[2][0] == "3"
	}, 8*time.Second, 20*time.Millisecond)
	rows := mustQuerySQL(t, replica, "app", "select id, value from items order by id")
	require.Equal(t, [][]interface{}{{"1", "before-restart"}, {"2", "after-restart"}, {"3", "xa-after-restart"}}, rows)
}

func TestEngineSourceReplicaReplicatesCommittedDDL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceControl := reserveTCPAddress(t)
	sourceCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "ddl-source",
		ReplicationServerID:             201,
		ReplicationListenAddress:        sourceControl,
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	sourceCfg.InnodbDataDir = sourceCfg.DataDir
	source := NewXMySQLEngine(sourceCfg)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	require.NoError(t, source.Start(ctx))

	replicaCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "replica",
		ReplicationUUID:                 "ddl-replica",
		ReplicationServerID:             202,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationSourceURL:            "http://" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
		ReplicationReadOnly:             false,
	}
	replicaCfg.InnodbDataDir = replicaCfg.DataDir
	replica := NewXMySQLEngine(replicaCfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	require.NoError(t, replica.Start(ctx))

	sourceSession := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(sourceSession) })
	mustExecSessionSQLFully(t, source, sourceSession, "", "create database replicated")
	mustExecSessionSQLFully(t, source, sourceSession, "replicated", "create table events (id int primary key, body varchar(64))")

	replicated := assert.Eventually(t, func() bool {
		result := <-replica.ExecuteQuery(nil, "select table_name from information_schema.tables where table_schema = 'replicated' and table_name = 'events'", "")
		if result == nil || result.Err != nil {
			return false
		}
		selected, ok := result.Data.(*SelectResult)
		return ok && len(selected.Records) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, replicated)
}

func TestEngineSourceReplicaReplicatesStoredObjectDefinition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceCfg := &conf.Cfg{
		DataDir: t.TempDir(), InnodbDataDir: t.TempDir(), InnodbBufferPoolSize: 16 * 1024 * 1024,
		ReplicationRole: "source", ReplicationUUID: "routine-source", ReplicationServerID: 211,
		ReplicationListenAddress: reserveTCPAddress(t), ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	sourceCfg.InnodbDataDir = sourceCfg.DataDir
	source := NewXMySQLEngine(sourceCfg)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	require.NoError(t, source.Start(ctx))

	replicaCfg := &conf.Cfg{
		DataDir: t.TempDir(), InnodbDataDir: t.TempDir(), InnodbBufferPoolSize: 16 * 1024 * 1024,
		ReplicationRole: "replica", ReplicationUUID: "routine-replica", ReplicationServerID: 212,
		ReplicationListenAddress: reserveTCPAddress(t), ReplicationSourceURL: "http://" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond, ReplicationReadOnly: true,
	}
	replicaCfg.InnodbDataDir = replicaCfg.DataDir
	replica := NewXMySQLEngine(replicaCfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	require.NoError(t, replica.Start(ctx))

	session := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(session) })
	mustExecSessionSQLFully(t, source, session, "", "create database routines")
	mustExecSessionSQLFully(t, source, session, "routines", "create procedure report() begin select 1; end")

	replicated := assert.Eventually(t, func() bool {
		result := <-replica.ExecuteQuery(nil, "select routine_name from information_schema.routines where routine_schema = 'routines' and routine_name = 'report'", "")
		if result == nil || result.Err != nil {
			return false
		}
		selected, ok := result.Data.(*SelectResult)
		return ok && len(selected.Records) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, replicated)
}

func TestEngineSourceReplicaReplicatesAlterTableAndTriggerDefinitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceCfg := &conf.Cfg{
		DataDir: t.TempDir(), InnodbDataDir: t.TempDir(), InnodbBufferPoolSize: 16 * 1024 * 1024,
		ReplicationRole: "source", ReplicationUUID: "complex-ddl-source", ReplicationServerID: 221,
		ReplicationListenAddress: reserveTCPAddress(t), ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	sourceCfg.InnodbDataDir = sourceCfg.DataDir
	source := NewXMySQLEngine(sourceCfg)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	require.NoError(t, source.Start(ctx))
	sourceSession := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(sourceSession) })
	mustExecSessionSQLFully(t, source, sourceSession, "", "create database complex_ddl")
	mustExecSessionSQLFully(t, source, sourceSession, "complex_ddl", "create table items (id int primary key, value int)")
	mustExecSessionSQLFully(t, source, sourceSession, "complex_ddl", "alter table items add column note varchar(32) default 'new', add index idx_value (value)")
	mustExecSessionSQLFully(t, source, sourceSession, "complex_ddl", "create trigger items_audit before update on items for each row set new.note = 'updated'")

	replicaCfg := &conf.Cfg{
		DataDir: t.TempDir(), InnodbDataDir: t.TempDir(), InnodbBufferPoolSize: 16 * 1024 * 1024,
		ReplicationRole: "replica", ReplicationUUID: "complex-ddl-replica", ReplicationServerID: 222,
		ReplicationListenAddress: reserveTCPAddress(t), ReplicationSourceURL: "http://" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond, ReplicationReadOnly: true,
	}
	replicaCfg.InnodbDataDir = replicaCfg.DataDir
	replica := NewXMySQLEngine(replicaCfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	require.NoError(t, replica.Start(ctx))

	replicated := assert.Eventually(t, func() bool {
		columns := <-replica.ExecuteQuery(nil, "select column_name from information_schema.columns where table_schema = 'complex_ddl' and table_name = 'items' and column_name = 'note'", "")
		if columns == nil || columns.Err != nil {
			return false
		}
		selected, ok := columns.Data.(*SelectResult)
		if !ok || len(selected.Records) != 1 {
			return false
		}
		triggers := <-replica.ExecuteQuery(nil, "select trigger_name from information_schema.triggers where trigger_schema = 'complex_ddl' and trigger_name = 'items_audit'", "")
		if triggers == nil || triggers.Err != nil {
			return false
		}
		triggerRows, triggerOK := triggers.Data.(*SelectResult)
		return triggerOK && len(triggerRows.Records) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, replicated)
}

func TestEngineSourceCapturesRowImagesAndPartialJSONEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "row-capture-source",
		ReplicationServerID:             111,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	cfg.InnodbDataDir = cfg.DataDir
	engine := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table docs (id int primary key, payload json)")
	require.NoError(t, engine.Start(ctx))
	session := newTestMySQLSession()
	t.Cleanup(func() { engine.QueryExecutor.clearSessionTransactionState(session) })
	mustExecSessionSQL(t, engine, session, "app", "insert into docs values (1, '{\"name\":\"old\",\"description\":\"this is a durable JSON row image used to validate native partial update capture\"}')")
	mustExecSessionSQL(t, engine, session, "app", "update docs set payload = '{\"name\":\"new\",\"description\":\"this is a durable JSON row image used to validate native partial update capture\"}' where id = 1")

	events, err := engine.ReplicationSource().Dump(4)
	require.NoError(t, err)
	var updateFound bool
	for _, event := range events {
		for _, change := range event.Changes {
			if change.Action == "update" && change.Table == "app.docs" {
				updateFound = true
				require.Equal(t, "JSON", change.ColumnTypes["payload"])
				require.NotEmpty(t, change.PartialJSONUpdates["payload"], "logical row event must retain the derived JSON diff")
			}
		}
	}
	require.True(t, updateFound, "source stream must contain a captured update row image")

	nativeEvents, err := engine.ReplicationSource().Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	partialFound := false
	for _, event := range nativeEvents {
		if event.Type == 39 {
			partialFound = true
			break
		}
	}
	require.True(t, partialFound, "native stream must use PARTIAL_UPDATE_ROWS_EVENT for compact JSON update")
}

func TestEngineAppliesReplicationRowImagesWithoutStatementText(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table row_apply (id int primary key, value varchar(64))")

	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:  "app.row_apply",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1), "value": "first"},
	}}))
	// A retry after storage commit but before the replica GTID state replace
	// must recognize the already materialized row image and avoid a duplicate
	// insert. This is the storage-side half of the replication crash window.
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:  "app.row_apply",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1), "value": "first"},
	}}))
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:  "app.row_apply",
		Action: "update",
		Before: map[string]interface{}{"id": int64(1), "value": "first"},
		After:  map[string]interface{}{"id": int64(1), "value": "second"},
	}}))
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:  "app.row_apply",
		Action: "delete",
		Before: map[string]interface{}{"id": int64(1), "value": "second"},
	}}))

	rows := mustQuerySQL(t, engine, "app", "select id, value from row_apply")
	require.Empty(t, rows)
}

func TestEngineBindsNativeSyntheticColumnNamesToPersistedTableOrder(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table native_bind (id int primary key, value varchar(64))")

	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:  "app.native_bind",
		Action: "insert",
		After:  map[string]interface{}{"column_1": int64(7), "column_2": "official"},
	}}))
	require.Equal(t, [][]interface{}{{"7", "official"}}, mustQuerySQL(t, engine, "app", "select id, value from native_bind"))
}

func TestEngineAppliesPartialJSONRowImageWithoutFullAfterValue(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table json_row_apply (id int primary key, payload json)")
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:  "app.json_row_apply",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1), "payload": `{"profile":{"name":"old"}}`},
	}}))
	updates, ok := replication.DeriveJSONPartialUpdates(`{"profile":{"name":"old"}}`, `{"profile":{"name":"new","city":"Austin"}}`)
	require.True(t, ok)
	update := replication.RowChange{
		Table:              "app.json_row_apply",
		Action:             "update",
		Before:             map[string]interface{}{"id": int64(1), "payload": `{"profile":{"name":"old"}}`},
		PartialJSONUpdates: map[string][]replication.JSONPartialUpdate{"payload": updates},
	}
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{update}))
	// The same physical row image can be retried after a replica state-file
	// failure; the post-image check must make the retry a no-op.
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{update}))
	rows := mustQuerySQL(t, engine, "app", "select payload from json_row_apply")
	require.Equal(t, [][]interface{}{{`{"profile":{"city":"Austin","name":"new"}}`}}, rows)
}

func TestEngineSourceReplicaInitialSyncFromEmptyReplica(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "source",
		ReplicationUUID:                 "initial-sync-source",
		ReplicationServerID:             301,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
	}
	sourceCfg.InnodbDataDir = sourceCfg.DataDir
	source := NewXMySQLEngine(sourceCfg)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	sourceSession := newTestMySQLSession()
	t.Cleanup(func() { source.QueryExecutor.clearSessionTransactionState(sourceSession) })

	// The source may already contain data when a replica is provisioned. The
	// replication hook is installed during engine construction, so these
	// committed DDL/DML statements must be available to the new replica from
	// its initial position rather than requiring a pre-created schema.
	mustExecSessionSQL(t, source, sourceSession, "", "create database initial_sync")
	mustExecSessionSQL(t, source, sourceSession, "initial_sync", "create table events (id int primary key, body varchar(64))")
	mustExecSessionSQL(t, source, sourceSession, "initial_sync", "insert into events (id, body) values (1, 'before-replica')")
	require.NotEmpty(t, source.ReplicationSource().Executed)
	source.QueryExecutor.clearSessionTransactionState(sourceSession)
	require.NoError(t, source.Start(ctx))

	replicaCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
		InnodbBufferPoolSize:            16 * 1024 * 1024,
		ReplicationRole:                 "replica",
		ReplicationUUID:                 "initial-sync-replica",
		ReplicationServerID:             302,
		ReplicationListenAddress:        reserveTCPAddress(t),
		ReplicationSourceURL:            "http:" + "//" + source.replicationRuntime.Address(),
		ReplicationPollIntervalDuration: 10 * time.Millisecond,
		ReplicationReadOnly:             true,
	}
	replicaCfg.InnodbDataDir = replicaCfg.DataDir
	replica := NewXMySQLEngine(replicaCfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	require.NoError(t, replica.Start(ctx))

	replicated := assert.Eventually(t, func() bool {
		result := <-replica.ExecuteQuery(nil, "select id, body from events", "initial_sync")
		if result == nil || result.Err != nil {
			return false
		}
		selected, ok := result.Data.(*SelectResult)
		return ok && len(selected.Records) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, replicated)
	rows := mustQuerySQL(t, replica, "initial_sync", "select id, body from events")
	require.Len(t, rows, 1)
}

func TestReplicaRejectsClientWritesWhileReplicationIsActive(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:                 t.TempDir(),
		InnodbDataDir:           t.TempDir(),
		InnodbBufferPoolSize:    16 * 1024 * 1024,
		ReplicationRole:         "replica",
		ReplicationUUID:         "read-only-replica",
		ReplicationServerID:     303,
		ReplicationSourceURL:    "http://127.0.0.1:1",
		ReplicationReadOnly:     true,
		ReplicationPollInterval: "1s",
	}
	cfg.InnodbDataDir = cfg.DataDir
	replica := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })

	session := newTestMySQLSession()
	result := <-replica.ExecuteQuery(session, "insert into events values (1, 'client-write')", "app")
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "replica is read-only")
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func mustExecSessionSQLFully(t *testing.T, executor *XMySQLEngine, session server.MySQLServerSession, databaseName, sql string) {
	t.Helper()
	var first *Result
	for result := range executor.ExecuteQuery(session, sql, databaseName) {
		if first == nil {
			first = result
		}
	}
	require.NotNil(t, first)
	require.NoError(t, first.Err)
}
