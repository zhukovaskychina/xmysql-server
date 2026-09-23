package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestParseXAStatementSupportsBasicXIDForms(t *testing.T) {
	op, xid, onePhase, ok, err := parseXAStatement("XA START 'global-1','branch-1',42")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "START", op)
	require.False(t, onePhase)
	require.Equal(t, "global-1", xid.gtrid)
	require.Equal(t, "branch-1", xid.bqual)
	require.Equal(t, uint32(42), xid.formatID)

	op, xid, onePhase, ok, err = parseXAStatement("XA COMMIT 'global-1','branch-1',42 ONE PHASE")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "COMMIT", op)
	require.True(t, onePhase)
	require.Equal(t, "global-1", xid.gtrid)

	op, xid, onePhase, ok, err = parseXAStatement("XA BEGIN 'global-2','branch-2',43")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "START", op)
	require.False(t, onePhase)
	require.Equal(t, "global-2", xid.gtrid)
	require.Equal(t, "branch-2", xid.bqual)
	require.Equal(t, uint32(43), xid.formatID)
}

func TestXACompatibilitySupportsBeginAlias(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_begin (id int primary key)")
	for _, query := range []string{
		"XA BEGIN 'begin-alias'",
		"INSERT INTO xa_begin VALUES (1)",
		"XA END 'begin-alias'",
		"XA COMMIT 'begin-alias' ONE PHASE",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_begin"))
}

func TestParseXAStatementSupportsSuspendResumeOptions(t *testing.T) {
	op, xid, _, options, ok, err := parseXAStatementOptions("XA END 'g','b',7 SUSPEND FOR MIGRATE")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "END", op)
	require.Equal(t, "g", xid.gtrid)
	require.Equal(t, []string{"SUSPEND", "FOR", "MIGRATE"}, options)

	op, xid, _, options, ok, err = parseXAStatementOptions("XA START 'g','b',7 RESUME")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "START", op)
	require.Equal(t, "g", xid.gtrid)
	require.Equal(t, []string{"RESUME"}, options)

	_, _, _, _, _, err = parseXAStatementOptions("XA COMMIT 'g' JOIN")
	require.ErrorContains(t, err, "does not support options")

	op, _, _, options, ok, err = parseXAStatementOptions("XA RECOVER CONVERT XID")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "RECOVER", op)
	require.Equal(t, []string{"CONVERT", "XID"}, options)
}

func TestXACompatibilitySupportsRecoverConvertXID(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("dynamic_privileges", []string{"XA_RECOVER_ADMIN"})
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_convert (id int primary key)")
	for _, query := range []string{
		"XA START 'abc','def',7",
		"INSERT INTO xa_convert VALUES (1)",
		"XA END 'abc','def',7",
		"XA PREPARE 'abc','def',7",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}

	result := <-executor.ExecuteQuery(session, "XA RECOVER CONVERT XID", "app")
	require.NoError(t, result.Err)
	require.Equal(t, [][]interface{}{{"7", "3", "3", "616263646566"}}, selectResultRows(result.Data.(*SelectResult)))
	require.NoError(t, (<-executor.ExecuteQuery(session, "XA ROLLBACK 'abc','def',7", "app")).Err)
}

func TestXACompatibilitySupportsPrepareRecoverCommitAndRollback(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("dynamic_privileges", []string{"XA_RECOVER_ADMIN"})
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_rows (id int primary key)")

	for _, query := range []string{
		"XA START 'g1','b1',1",
		"INSERT INTO xa_rows VALUES (1)",
		"XA END 'g1','b1',1",
		"XA PREPARE 'g1','b1',1",
	} {
		result := <-executor.ExecuteQuery(session, query, "app")
		require.NoError(t, result.Err, query)
	}

	recoverResult := <-executor.ExecuteQuery(session, "XA RECOVER", "app")
	require.NoError(t, recoverResult.Err)
	require.Equal(t, [][]interface{}{{"1", "2", "2", "g1b1"}}, selectResultRows(recoverResult.Data.(*SelectResult)))

	commitResult := <-executor.ExecuteQuery(session, "XA COMMIT 'g1','b1',1", "app")
	require.NoError(t, commitResult.Err)
	require.False(t, sessionBoolParam(session, "in_transaction"))

	rows := mustQuerySQL(t, executor, "app", "SELECT id FROM xa_rows")
	require.Equal(t, [][]interface{}{{"1"}}, rows)

	for _, query := range []string{
		"XA START 'g2','b2',1",
		"INSERT INTO xa_rows VALUES (2)",
		"XA END 'g2','b2',1",
		"XA PREPARE 'g2','b2',1",
		"XA ROLLBACK 'g2','b2',1",
	} {
		result := <-executor.ExecuteQuery(session, query, "app")
		require.NoError(t, result.Err, query)
	}

	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_rows"))
	recoverResult = <-executor.ExecuteQuery(session, "XA RECOVER", "app")
	require.NoError(t, recoverResult.Err)
	require.Empty(t, selectResultRows(recoverResult.Data.(*SelectResult)))
}

func TestXACommitPublishesOneNativeBinlogTransactionAfterPrepare(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "xa-source", 17)
	require.NoError(t, err)
	executor.QueryExecutor.SetReplicationCommitTransactionHook(func(changes []replication.RowChange, statements []replication.Statement) error {
		_, appendErr := source.AppendCommittedTransaction(changes, statements)
		return appendErr
	})
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_native_rows (id int primary key, label varchar(20))")
	baselineGTIDs := source.Executed.String()
	baselineSet := replication.GTIDSet{}
	for uuid, sequences := range source.Executed {
		baselineSet[uuid] = map[uint64]struct{}{}
		for sequence := range sequences {
			baselineSet[uuid][sequence] = struct{}{}
		}
	}

	for _, query := range []string{
		"XA START 'native-xa','branch',17",
		"INSERT INTO xa_native_rows VALUES (1, 'prepared')",
		"XA END 'native-xa','branch',17",
		"XA PREPARE 'native-xa','branch',17",
	} {
		mustExecSessionSQL(t, executor, session, "app", query)
	}
	require.Equal(t, baselineGTIDs, source.Executed.String(), "XA PREPARE must not publish a committed GTID")
	mustExecSessionSQL(t, executor, session, "app", "XA COMMIT 'native-xa','branch',17")
	committedSequences := source.Executed["xa-source"]
	var committedSequence uint64
	for sequence := range committedSequences {
		if sequence > committedSequence {
			committedSequence = sequence
		}
	}
	require.Greater(t, committedSequence, uint64(0))

	transactions, err := source.DecodeNativeDumpFrom("binlog.000001", 4, replication.GTIDIntervalsFromSet(baselineSet))
	require.NoError(t, err)
	require.Len(t, transactions, 1)
	require.Equal(t, replication.GTID{UUID: "xa-source", Seq: committedSequence}, transactions[0].GTID)
	require.Len(t, transactions[0].Changes, 1)
	require.Equal(t, "prepared", transactions[0].Changes[0].After["label"])

	replica, err := replication.NewReplica(t.TempDir())
	require.NoError(t, err)
	replica.Executed = baselineSet
	applied := make([]replication.RowChange, 0)
	replica.ApplyRows = func(changes []replication.RowChange) error {
		applied = append(applied, changes...)
		return nil
	}
	_, err = replica.ReplicateNativeFrom(source, "binlog.000001", 4)
	require.NoError(t, err)
	require.Len(t, applied, 1)
	require.True(t, replica.Executed.Contains(replication.GTID{UUID: "xa-source", Seq: committedSequence}))
	_, err = replica.ReplicateNativeFrom(source, "binlog.000001", 4)
	require.NoError(t, err)
	require.Len(t, applied, 1, "replaying the same native XA transaction must be idempotent")
}

func TestXACommitRetryAfterBinlogAppendFailureKeepsPreparedTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "xa-retry-source", 18)
	require.NoError(t, err)
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_retry_rows (id int primary key)")
	failAppend := true
	executor.QueryExecutor.SetReplicationCommitTransactionHook(func(changes []replication.RowChange, statements []replication.Statement) error {
		if failAppend {
			return fmt.Errorf("injected binlog append failure")
		}
		_, appendErr := source.AppendCommittedTransaction(changes, statements)
		return appendErr
	})
	for _, query := range []string{
		"XA START 'retry-xa'",
		"INSERT INTO xa_retry_rows VALUES (1)",
		"XA END 'retry-xa'",
		"XA PREPARE 'retry-xa'",
	} {
		mustExecSessionSQL(t, executor, session, "app", query)
	}
	commit := <-executor.ExecuteQuery(session, "XA COMMIT 'retry-xa'", "app")
	require.Error(t, commit.Err)
	require.True(t, sessionBoolParam(session, "in_transaction"))
	require.Equal(t, "PREPARED", session.GetParamByName("xa_state"))
	require.Empty(t, source.Executed)

	failAppend = false
	mustExecSessionSQL(t, executor, session, "app", "XA COMMIT 'retry-xa'")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_retry_rows"))
	require.Len(t, source.Executed["xa-retry-source"], 1)
}

func TestXACommitRetryAfterPostAppendErrorDoesNotDuplicateNativeTransaction(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	source, err := replication.NewSource(t.TempDir(), "xa-post-append-source", 19)
	require.NoError(t, err)
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_post_append (id int primary key)")
	postAppendFailure := true
	executor.QueryExecutor.SetReplicationCommitTransactionHookWithID(func(transactionID string, changes []replication.RowChange, statements []replication.Statement) error {
		if _, err := source.AppendCommittedTransactionWithKey(transactionID, changes, statements); err != nil {
			return err
		}
		if postAppendFailure {
			postAppendFailure = false
			return fmt.Errorf("injected error after durable binlog append")
		}
		return nil
	})
	for _, query := range []string{
		"XA START 'post-append-xa'",
		"INSERT INTO xa_post_append VALUES (1)",
		"XA END 'post-append-xa'",
		"XA PREPARE 'post-append-xa'",
	} {
		mustExecSessionSQL(t, executor, session, "app", query)
	}
	commit := <-executor.ExecuteQuery(session, "XA COMMIT 'post-append-xa'", "app")
	require.Error(t, commit.Err)
	require.True(t, sessionBoolParam(session, "in_transaction"))
	require.Len(t, source.Executed["xa-post-append-source"], 1)

	mustExecSessionSQL(t, executor, session, "app", "XA COMMIT 'post-append-xa'")
	require.False(t, sessionBoolParam(session, "in_transaction"))
	transactions, err := source.DecodeNativeDumpFrom("binlog.000001", 4, replication.GTIDIntervals{})
	require.NoError(t, err)
	require.Len(t, transactions, 1, "retrying after an error returned after append must not publish a second native transaction")
	require.Len(t, transactions[0].Changes, 1)
	require.Equal(t, "1", fmt.Sprint(transactions[0].Changes[0].After["id"]))
}

func TestXACompatibilityAllowsPreparedTransactionToBeCommittedByAnotherSession(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	owner := newTestMySQLSession()
	coordinator := newTestMySQLSession()
	coordinator.SetParamByName("dynamic_privileges", []string{"XA_RECOVER_ADMIN"})
	mustExecSessionSQL(t, executor, owner, "", "create database app")
	mustExecSessionSQL(t, executor, owner, "app", "create table xa_cross_session (id int primary key)")

	for _, query := range []string{
		"XA START 'cross','branch',7",
		"INSERT INTO xa_cross_session VALUES (7)",
		"XA END 'cross','branch',7",
		"XA PREPARE 'cross','branch',7",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(owner, query, "app")).Err, query)
	}

	recoverResult := <-executor.ExecuteQuery(coordinator, "XA RECOVER", "app")
	require.NoError(t, recoverResult.Err)
	require.Equal(t, [][]interface{}{{"7", "5", "6", "crossbranch"}}, selectResultRows(recoverResult.Data.(*SelectResult)))

	commitResult := <-executor.ExecuteQuery(coordinator, "XA COMMIT 'cross','branch',7", "app")
	require.NoError(t, commitResult.Err)
	require.Equal(t, [][]interface{}{{"7"}}, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_cross_session"))
	require.False(t, sessionBoolParam(owner, "in_transaction"))
}

func TestXACompatibilitySupportsOnePhaseCommit(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("dynamic_privileges", []string{"XA_RECOVER_ADMIN"})
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_one_phase (id int primary key)")

	for _, query := range []string{
		"XA START 'one-phase'",
		"INSERT INTO xa_one_phase VALUES (1)",
		"XA END 'one-phase'",
		"XA COMMIT 'one-phase' ONE PHASE",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}
	require.Equal(t, [][]interface{}{{"1"}}, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_one_phase"))
}

func TestXACompatibilitySupportsSuspendAndResumeLifecycle(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_suspend (id int primary key)")

	for _, query := range []string{
		"XA START 'suspend-xa'",
		"INSERT INTO xa_suspend VALUES (1)",
		"XA END 'suspend-xa' SUSPEND FOR MIGRATE",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}
	require.Equal(t, "SUSPENDED", session.GetParamByName("xa_state"))
	require.ErrorContains(t, (<-executor.ExecuteQuery(session, "INSERT INTO xa_suspend VALUES (2)", "app")).Err, "SUSPENDED")

	for _, query := range []string{
		"XA START 'suspend-xa' RESUME",
		"INSERT INTO xa_suspend VALUES (2)",
		"XA END 'suspend-xa'",
		"XA PREPARE 'suspend-xa'",
		"XA ROLLBACK 'suspend-xa'",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}
	require.Empty(t, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_suspend"))

	for _, query := range []string{
		"XA START 'join-xa'",
		"INSERT INTO xa_suspend VALUES (3)",
		"XA END 'join-xa' SUSPEND",
		"XA START 'join-xa' JOIN",
		"XA END 'join-xa'",
		"XA ROLLBACK 'join-xa'",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}

	joiner := newTestMySQLSession()
	for _, query := range []string{
		"XA START 'cross-join'",
		"INSERT INTO xa_suspend VALUES (4)",
		"XA END 'cross-join' SUSPEND",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}
	for _, query := range []string{
		"XA START 'cross-join' JOIN",
		"INSERT INTO xa_suspend VALUES (5)",
		"XA END 'cross-join'",
		"XA PREPARE 'cross-join'",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(joiner, query, "app")).Err, query)
	}
	require.NoError(t, (<-executor.ExecuteQuery(session, "XA COMMIT 'cross-join'", "app")).Err)
	require.Equal(t, [][]interface{}{{"4"}, {"5"}}, mustQuerySQL(t, executor, "app", "SELECT id FROM xa_suspend ORDER BY id"))
	require.False(t, sessionBoolParam(session, "in_transaction"))
	require.False(t, sessionBoolParam(joiner, "in_transaction"))
}

func TestXACompatibilityDoesNotDiscardPreparedTransactionOnSessionReset(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	session.SetParamByName("dynamic_privileges", []string{"XA_RECOVER_ADMIN"})
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_reset (id int primary key)")

	for _, query := range []string{
		"XA START 'reset-xa'",
		"INSERT INTO xa_reset VALUES (9)",
		"XA END 'reset-xa'",
		"XA PREPARE 'reset-xa'",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}

	require.ErrorContains(t, executor.QueryExecutor.ResetSession(session), "prepared XA transaction")
	recoverResult := <-executor.ExecuteQuery(session, "XA RECOVER", "app")
	require.NoError(t, recoverResult.Err)
	require.Len(t, selectResultRows(recoverResult.Data.(*SelectResult)), 1)
	require.NoError(t, (<-executor.ExecuteQuery(session, "XA ROLLBACK 'reset-xa'", "app")).Err)
}

func TestXACompatibilityRejectsRegularSessionMutationWhilePrepared(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	session := newTestMySQLSession()
	mustExecSessionSQL(t, executor, session, "", "create database app")
	mustExecSessionSQL(t, executor, session, "app", "create table xa_protocol (id int primary key)")
	for _, query := range []string{
		"XA START 'protocol-xa'",
		"INSERT INTO xa_protocol VALUES (1)",
		"XA END 'protocol-xa'",
		"XA PREPARE 'protocol-xa'",
	} {
		require.NoError(t, (<-executor.ExecuteQuery(session, query, "app")).Err, query)
	}
	require.ErrorContains(t, (<-executor.ExecuteQuery(session, "INSERT INTO xa_protocol VALUES (2)", "app")).Err, "PREPARED")
	require.ErrorContains(t, (<-executor.ExecuteQuery(session, "ROLLBACK", "app")).Err, "PREPARED")
	require.NoError(t, (<-executor.ExecuteQuery(session, "XA ROLLBACK 'protocol-xa'", "app")).Err)
}

func TestXACompatibilityRecoversPreparedTransactionAcrossEngineRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	first := NewXMySQLEngine(cfg)
	owner := newTestMySQLSession()
	mustExecSessionSQL(t, first, owner, "", "create database app")
	mustExecSessionSQL(t, first, owner, "app", "create table xa_restart (id int primary key)")
	for _, query := range []string{
		"XA START 'restart-rollback','branch',11",
		"INSERT INTO xa_restart VALUES (11)",
		"XA END 'restart-rollback','branch',11",
		"XA PREPARE 'restart-rollback','branch',11",
	} {
		require.NoError(t, (<-first.ExecuteQuery(owner, query, "app")).Err, query)
	}
	require.FileExists(t, preparedXAManifestPath(dataDir, (xaIdentifier{gtrid: "restart-rollback", bqual: "branch", formatID: 11}).key()))
	require.NoError(t, first.Close())

	second := NewXMySQLEngine(cfg)
	require.NoError(t, second.Start(context.Background()))
	coordinator := newTestMySQLSession()
	coordinator.SetParamByName("dynamic_privileges", []string{"XA_RECOVER_ADMIN"})
	recoverResult := <-second.ExecuteQuery(coordinator, "XA RECOVER", "app")
	require.NoError(t, recoverResult.Err)
	require.Equal(t, [][]interface{}{{"11", "16", "6", "restart-rollbackbranch"}}, selectResultRows(recoverResult.Data.(*SelectResult)))
	require.NoError(t, (<-second.ExecuteQuery(coordinator, "XA ROLLBACK 'restart-rollback','branch',11", "app")).Err)
	require.Empty(t, mustQuerySQL(t, second, "app", "SELECT id FROM xa_restart"))
	require.NoError(t, second.Close())

	third := NewXMySQLEngine(cfg)
	require.NoError(t, third.Start(context.Background()))
	owner = newTestMySQLSession()
	mustExecSessionSQL(t, third, owner, "app", "create table xa_restart_commit (id int primary key)")
	for _, query := range []string{
		"XA START 'restart-commit','branch',12",
		"INSERT INTO xa_restart_commit VALUES (12)",
		"XA END 'restart-commit','branch',12",
		"XA PREPARE 'restart-commit','branch',12",
	} {
		require.NoError(t, (<-third.ExecuteQuery(owner, query, "app")).Err, query)
	}
	require.NoError(t, third.Close())

	fourth := NewXMySQLEngine(cfg)
	require.NoError(t, fourth.Start(context.Background()))
	coordinator = newTestMySQLSession()
	require.NoError(t, (<-fourth.ExecuteQuery(coordinator, "XA COMMIT 'restart-commit','branch',12", "app")).Err)
	require.Equal(t, [][]interface{}{{"12"}}, mustQuerySQL(t, fourth, "app", "SELECT id FROM xa_restart_commit"))
	require.NoError(t, fourth.Close())
}

func TestXACompatibilityRecoversSuspendedTransactionAcrossEngineRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &conf.Cfg{
		DataDir:              dataDir,
		InnodbDataDir:        dataDir,
		InnodbBufferPoolSize: 16 * 1024 * 1024,
		InnodbPageSize:       16384,
	}

	first := NewXMySQLEngine(cfg)
	owner := newTestMySQLSession()
	mustExecSessionSQL(t, first, owner, "", "create database app")
	mustExecSessionSQL(t, first, owner, "app", "create table xa_suspend_restart (id int primary key)")
	for _, query := range []string{
		"XA START 'suspend-restart','branch',21",
		"INSERT INTO xa_suspend_restart VALUES (21)",
		"XA END 'suspend-restart','branch',21 SUSPEND FOR MIGRATE",
	} {
		require.NoError(t, (<-first.ExecuteQuery(owner, query, "app")).Err, query)
	}
	key := (xaIdentifier{gtrid: "suspend-restart", bqual: "branch", formatID: 21}).key()
	require.FileExists(t, suspendedXAManifestPath(dataDir, key))
	duplicate := newTestMySQLSession()
	require.ErrorContains(t, (<-first.ExecuteQuery(duplicate, "XA START 'suspend-restart','branch',21", "app")).Err, "XAER_DUPID")
	require.NoError(t, first.Close())

	second := NewXMySQLEngine(cfg)
	require.NoError(t, second.Start(context.Background()))
	coordinator := newTestMySQLSession()
	for _, query := range []string{
		"XA START 'suspend-restart','branch',21 JOIN",
		"XA END 'suspend-restart','branch',21",
		"XA PREPARE 'suspend-restart','branch',21",
		"XA COMMIT 'suspend-restart','branch',21",
	} {
		require.NoError(t, (<-second.ExecuteQuery(coordinator, query, "app")).Err, query)
	}
	require.NoFileExists(t, suspendedXAManifestPath(dataDir, key))
	require.Equal(t, [][]interface{}{{"21"}}, mustQuerySQL(t, second, "app", "SELECT id FROM xa_suspend_restart"))
	require.NoError(t, second.Close())
}
