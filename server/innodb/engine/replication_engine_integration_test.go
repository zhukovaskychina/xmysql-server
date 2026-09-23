package engine

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

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

func TestEngineSourceReplicaReplicatesCommittedDML(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceControl := reserveTCPAddress(t)
	sourceCfg := &conf.Cfg{
		DataDir:                         t.TempDir(),
		InnodbDataDir:                   t.TempDir(),
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
	require.NoError(t, engine.applyReplicationRows([]replication.RowChange{{
		Table:              "app.json_row_apply",
		Action:             "update",
		Before:             map[string]interface{}{"id": int64(1), "payload": `{"profile":{"name":"old"}}`},
		PartialJSONUpdates: map[string][]replication.JSONPartialUpdate{"payload": updates},
	}}))
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
