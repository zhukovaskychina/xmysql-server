package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestShowReplicaStatusExposesReplicationRuntimeState(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:              replication.RoleReplica,
			UUID:              "replica-1",
			SourceURL:         "http://source:8080",
			SourceUUID:        "source-uuid-1",
			ExecutedGTIDs:     "source-1:1-4",
			SourcePosition:    42,
			ReplicationLagSec: 0.25,
		}
	})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replica status"}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, "show replica status")
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Len(t, selectResult.Records, 1)
	require.Contains(t, selectResult.Columns, "Replica_IO_Running")
	require.Contains(t, selectResult.Columns, "Seconds_Behind_Source")
	require.Contains(t, selectResult.Columns, "Executed_Gtid_Set")
	require.Contains(t, selectResult.Columns, "Source_UUID")

	values := selectResult.Records[0].GetValues()
	for i, column := range selectResult.Columns {
		switch column {
		case "Replica_IO_Running":
			require.Equal(t, "Yes", values[i].String())
		case "Seconds_Behind_Source":
			require.Equal(t, "0.25", values[i].String())
		case "Executed_Gtid_Set":
			require.Equal(t, "source-1:1-4", values[i].String())
		case "Source_UUID":
			require.Equal(t, "source-uuid-1", values[i].String())
		}
	}
}

func TestShowReplicaStatusAcceptsDefaultChannelAndRejectsNamedChannel(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{Role: replication.RoleReplica, SourceURL: "mysql://source.example:3306"}
	})

	for _, query := range []string{"show replica status for channel ''", "show slave status for channel \"\""} {
		results := make(chan *Result, 1)
		ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: query}
		executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, query)
		result := <-results
		require.NoError(t, result.Err, query)
	}

	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replica status for channel 'analytics'"}
	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, ctx.RawQuery)
	result := <-results
	require.ErrorContains(t, result.Err, "only the default replication channel")
}

func TestShowReplicaStatusUsesMySQL84ColumnContract(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:                replication.RoleReplica,
			SourceURL:           "http://source:8080",
			SourceUser:          "repl",
			SourceFile:          "binlog.000007",
			SourcePosition:      128,
			ExecutedGTIDs:       "source-1:1-4",
			SourceAutoPosition:  true,
			ReplicaRunning:      true,
			ReplicaRunningKnown: true,
		}
	})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replica status"}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, "show replica status")
	result := <-results
	require.NoError(t, result.Err)
	selectResult, ok := result.Data.(*SelectResult)
	require.True(t, ok)
	require.Equal(t, []string{
		"Replica_IO_State", "Source_Host", "Source_User", "Source_Port", "Connect_Retry",
		"Source_Log_File", "Read_Source_Log_Pos", "Relay_Log_File", "Relay_Log_Pos",
		"Relay_Source_Log_File", "Replica_IO_Running", "Replica_SQL_Running", "Replicate_Do_DB",
		"Replicate_Ignore_DB", "Replicate_Do_Table", "Replicate_Ignore_Table", "Replicate_Wild_Do_Table",
		"Replicate_Wild_Ignore_Table", "Last_Errno", "Last_Error", "Skip_Counter", "Exec_Source_Log_Pos",
		"Relay_Log_Space", "Until_Condition", "Until_Log_File", "Until_Log_Pos", "Source_SSL_Allowed",
		"Source_SSL_CA_File", "Source_SSL_CA_Path", "Source_SSL_Cert", "Source_SSL_Cipher", "Source_SSL_Key",
		"Seconds_Behind_Source", "Source_SSL_Verify_Server_Cert", "Last_IO_Errno", "Last_IO_Error",
		"Last_SQL_Errno", "Last_SQL_Error", "Replicate_Ignore_Server_Ids", "Source_Server_Id", "Source_UUID",
		"Source_Info_File", "SQL_Delay", "SQL_Remaining_Delay", "Replica_SQL_Running_State", "Source_Retry_Count",
		"Source_Bind", "Last_IO_Error_Timestamp", "Last_SQL_Error_Timestamp", "Source_SSL_Crl", "Source_SSL_Crlpath",
		"Retrieved_Gtid_Set", "Executed_Gtid_Set", "Auto_Position", "Replicate_Rewrite_DB", "Channel_Name",
		"Source_TLS_Version", "Source_public_key_path", "Get_Source_public_key", "Network_Namespace",
	}, selectResult.Columns)
	values := selectResult.Records[0].GetValues()
	for index, column := range selectResult.Columns {
		switch column {
		case "Source_User":
			require.Equal(t, "repl", values[index].String())
		case "Auto_Position":
			require.Equal(t, "1", values[index].String())
		}
	}
}

func TestShowReplicaStatusProjectsReplicationRewriteDB(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:      replication.RoleReplica,
			SourceURL: "mysql://source.example:3306",
			ReplicationFilters: []replication.ReplicationFilterStatus{
				{Name: "REPLICATE_REWRITE_DB", Rule: "source_db -> target_db"},
				{Name: "REPLICATE_REWRITE_DB", Rule: "legacy_db -> archive_db"},
			},
		}
	})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replica status"}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, "show replica status")
	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	values := selectResult.Records[0].GetValues()
	for index, column := range selectResult.Columns {
		if column == "Replicate_Rewrite_DB" {
			require.Equal(t, "(source_db,target_db),(legacy_db,archive_db)", values[index].String())
			return
		}
	}
	t.Fatal("SHOW REPLICA STATUS did not return Replicate_Rewrite_DB")
}

func TestShowReplicaStatusRequiresReplicationClientOrSuperPrivilege(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:       replication.RoleReplica,
			SourceURL:  "mysql://source.example:3306",
			SourceUUID: "source-uuid-1",
		}
	})
	mustExecSQL(t, executor, "", "create user 'replica_status_reader'@'localhost' identified by 'secret'")

	session := newTestMySQLSession()
	session.SetParamByName("user", "replica_status_reader")
	session.SetParamByName("host", "localhost")

	denied := <-executor.ExecuteQuery(session, "show replica status", "")
	require.Error(t, denied.Err)
	require.Contains(t, denied.Err.Error(), "REPLICATION CLIENT")

	mustExecSQL(t, executor, "", "grant replication client on *.* to 'replica_status_reader'@'localhost'")
	allowed := <-executor.ExecuteQuery(session, "show replica status", "")
	require.NoError(t, allowed.Err)

	mustExecSQL(t, executor, "", "revoke replication client on *.* from 'replica_status_reader'@'localhost'")
	mustExecSQL(t, executor, "", "grant super on *.* to 'replica_status_reader'@'localhost'")
	legacyAllowed := <-executor.ExecuteQuery(session, "show slave status", "")
	require.NoError(t, legacyAllowed.Err)
}

func TestShowReplicaStatusProjectsClassifiedErrorState(t *testing.T) {
	executor := &XMySQLExecutor{}
	errorAt := time.Date(2026, 9, 27, 13, 14, 15, 0, time.UTC)
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:                replication.RoleReplica,
			SourceURL:           "mysql://source.example:3306",
			ReplicaRunningKnown: true,
			ReplicaRunning:      false,
			LastError:           "ERROR 1064 (42000): apply failed",
			LastErrorNumber:     1064,
			LastErrorAt:         errorAt,
			LastSQLError:        "ERROR 1064 (42000): apply failed",
			LastSQLErrorNumber:  1064,
			LastSQLErrorAt:      errorAt,
		}
	})
	results := make(chan *Result, 1)
	ctx := &ExecutionContext{Context: context.Background(), Results: results, RawQuery: "show replica status"}

	executor.executeShowStatementWithQuery(ctx, &sqlparser.Show{Type: "replica"}, nil, "show replica status")
	result := <-results
	require.NoError(t, result.Err)
	selectResult := result.Data.(*SelectResult)
	values := selectResult.Records[0].GetValues()
	var lastErrno, lastIOErrno, lastSQLErrno int64
	var lastSQLError, lastSQLTimestamp string
	for index, column := range selectResult.Columns {
		switch column {
		case "Last_Errno":
			lastErrno = values[index].Int()
		case "Last_IO_Errno":
			lastIOErrno = values[index].Int()
		case "Last_SQL_Errno":
			lastSQLErrno = values[index].Int()
		case "Last_SQL_Error":
			lastSQLError = values[index].String()
		case "Last_SQL_Error_Timestamp":
			lastSQLTimestamp = values[index].String()
		}
	}
	require.Equal(t, int64(1064), lastErrno)
	require.Equal(t, int64(1064), lastSQLErrno)
	require.Equal(t, "ERROR 1064 (42000): apply failed", lastSQLError)
	require.Equal(t, "2026-09-27 13:14:15", lastSQLTimestamp)
	require.Equal(t, int64(0), lastIOErrno)
}
