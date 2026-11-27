package replication

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplicaReplicationFiltersSkipChangesButAdvanceTransaction(t *testing.T) {
	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	var applied []RowChange
	replica.ApplyRows = func(changes []RowChange) error {
		applied = append(applied, changes...)
		return nil
	}
	require.NoError(t, replica.SetReplicationFilters(ReplicationFilterConfig{
		ReplicateDoDB:        []string{"app"},
		ReplicateIgnoreTable: []string{"app.secret"},
	}, "CHANGE_REPLICATION_FILTER"))

	gtid := GTID{UUID: "source-filter", Seq: 1}
	err = replica.Apply([]BinlogEvent{
		{Type: EventBegin, GTID: gtid},
		{Type: EventRow, GTID: gtid, Changes: []RowChange{
			{Table: "app.visible", Action: "insert"},
			{Table: "app.secret", Action: "insert"},
			{Table: "other.visible", Action: "insert"},
		}},
		{Type: EventCommit, GTID: gtid},
	})
	require.NoError(t, err)
	require.Equal(t, []RowChange{{Table: "app.visible", Action: "insert"}}, applied)
	require.True(t, replica.Executed.Contains(gtid), "filtered transactions must still advance the executed GTID set")
	require.Len(t, replica.AppliedRows, 1)

	restarted, err := NewReplica(filepathForReplicaState(t, replica))
	require.NoError(t, err)
	statuses := restarted.ReplicationFilterStatus()
	require.Len(t, statuses, 2)
	require.Equal(t, "CHANGE_REPLICATION_FILTER", statuses[0].ConfiguredBy)
	require.Equal(t, "app", statuses[0].Rule)
}

func TestReplicationFilterWildcardAndStatementRules(t *testing.T) {
	filters := ReplicationFilterConfig{
		ReplicateWildDoTable:     []string{"app.audit_%"},
		ReplicateWildIgnoreTable: []string{"app.audit_secret"},
		ReplicateIgnoreDB:        []string{"blocked"},
	}
	require.True(t, filters.AllowsRowChange(RowChange{Table: "app.audit_events"}))
	require.False(t, filters.AllowsRowChange(RowChange{Table: "app.audit_secret"}))
	require.False(t, filters.AllowsRowChange(RowChange{Table: "blocked.audit_events"}))
	require.True(t, filters.AllowsStatement(Statement{Database: "app", SQL: "insert into audit_events values (1)"}))
	require.False(t, filters.AllowsStatement(Statement{Database: "blocked", SQL: "insert into audit_events values (1)"}))
}

func TestReplicationFilterStatementTableRulesInspectQualifiedReferences(t *testing.T) {
	doTable := ReplicationFilterConfig{ReplicateDoTable: []string{"app.orders"}}
	require.True(t, doTable.AllowsStatement(Statement{Database: "app", SQL: "insert into orders values (1)"}))
	require.False(t, doTable.AllowsStatement(Statement{Database: "app", SQL: "insert into app.orders select * from app.orders_archive"}),
		"a statement touching a non-matching table must not pass a do-table filter")
	require.False(t, doTable.AllowsStatement(Statement{Database: "app", SQL: "update app.users set id = 1"}))

	ignoreTable := ReplicationFilterConfig{ReplicateIgnoreTable: []string{"app.secret"}}
	require.False(t, ignoreTable.AllowsStatement(Statement{Database: "app", SQL: "delete from app.secret where id = 1"}))
	require.True(t, ignoreTable.AllowsStatement(Statement{
		Database: "app",
		SQL:      "insert into app.orders select 'app.secret', \"app.secret\" /* app.secret */ from app.orders_archive -- app.secret\n",
	}))
}

func TestReplicationRewriteDBRewritesRowAndStatementTargetsBeforeFiltering(t *testing.T) {
	filters := ReplicationFilterConfig{
		ReplicateRewriteDB: []ReplicationDBRewrite{{From: "source_db", To: "target_db"}},
		ReplicateDoTable:   []string{"target_db.orders"},
	}

	row := filters.RewriteRowChange(RowChange{Table: "source_db.orders", Action: "insert"})
	require.Equal(t, "target_db.orders", row.Table)
	require.True(t, filters.AllowsRowChange(RowChange{Table: "source_db.orders", Action: "insert"}),
		"rewrite must happen before table filters are evaluated")
	require.False(t, filters.AllowsRowChange(RowChange{Table: "other_db.orders", Action: "insert"}))

	statement := filters.RewriteStatement(Statement{Database: "source_db", SQL: "insert into orders values (1)"})
	require.Equal(t, "target_db", statement.Database)
	require.True(t, filters.AllowsStatement(Statement{Database: "source_db", SQL: "insert into orders values (1)"}))
}

func TestReplicationRewriteDBRewritesQualifiedStatementNamesOnly(t *testing.T) {
	filters := ReplicationFilterConfig{ReplicateRewriteDB: []ReplicationDBRewrite{{From: "source_db", To: "target_db"}}}
	statement := filters.RewriteStatement(Statement{
		Database: "source_db",
		SQL: "insert into source_db.orders select * from `source_db`.`archive`; -- source_db.orders\n" +
			"select 'source_db.orders', \"source_db.orders\" /* source_db.orders */",
	})
	require.Equal(t, "target_db", statement.Database)
	require.Equal(t,
		"insert into target_db.orders select * from `target_db`.`archive`; -- source_db.orders\n"+
			"select 'source_db.orders', \"source_db.orders\" /* source_db.orders */",
		statement.SQL)
}

func TestReplicaReplicationRewriteDBReachesStorageCallbacks(t *testing.T) {
	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	var appliedRows []RowChange
	var appliedStatements []Statement
	replica.ApplyRows = func(changes []RowChange) error {
		appliedRows = append(appliedRows, changes...)
		return nil
	}
	replica.ApplyStatements = func(statements []Statement) error {
		appliedStatements = append(appliedStatements, statements...)
		return nil
	}
	require.NoError(t, replica.SetReplicationFilters(ReplicationFilterConfig{
		ReplicateRewriteDB: []ReplicationDBRewrite{{From: "source_db", To: "target_db"}},
	}, "CHANGE_REPLICATION_FILTER"))

	rowGTID := GTID{UUID: "source-rewrite", Seq: 1}
	require.NoError(t, replica.Apply([]BinlogEvent{
		{Type: EventBegin, GTID: rowGTID},
		{Type: EventRow, GTID: rowGTID, Changes: []RowChange{{Table: "source_db.orders", Action: "insert"}}},
		{Type: EventCommit, GTID: rowGTID},
	}))
	require.Equal(t, []RowChange{{Table: "target_db.orders", Action: "insert"}}, appliedRows)

	statementGTID := GTID{UUID: "source-rewrite", Seq: 2}
	require.NoError(t, replica.Apply([]BinlogEvent{
		{Type: EventBegin, GTID: statementGTID},
		{Type: EventRow, GTID: statementGTID, Statements: []Statement{{Database: "source_db", SQL: "insert into orders values (1)"}}},
		{Type: EventCommit, GTID: statementGTID},
	}))
	require.Equal(t, []Statement{{Database: "target_db", SQL: "insert into orders values (1)"}}, appliedStatements)
}

func TestReplicaReplicationRewriteDBAppliesToPreparedXACommit(t *testing.T) {
	replica, err := NewReplica(t.TempDir())
	require.NoError(t, err)
	var applied []RowChange
	replica.ApplyRows = func(changes []RowChange) error {
		applied = append(applied, changes...)
		return nil
	}
	require.NoError(t, replica.SetReplicationFilters(ReplicationFilterConfig{
		ReplicateRewriteDB: []ReplicationDBRewrite{{From: "source_db", To: "target_db"}},
	}, "CHANGE_REPLICATION_FILTER"))
	xid := XAIdentity{GTRID: "rewrite-xa", FormatID: 42}
	prepareGTID := GTID{UUID: "source-rewrite-xa", Seq: 1}
	commitGTID := GTID{UUID: "source-rewrite-xa", Seq: 2}
	require.NoError(t, replica.Apply([]BinlogEvent{
		{Type: EventXAPrepare, GTID: prepareGTID, XA: &xid, Changes: []RowChange{{Table: "source_db.orders", Action: "insert"}}},
		{Type: EventXACommit, GTID: prepareGTID, TerminalGTID: &commitGTID, XA: &xid},
	}))
	require.Equal(t, []RowChange{{Table: "target_db.orders", Action: "insert"}}, applied)
}

// filepathForReplicaState returns the data directory used by NewReplica in a
// way that keeps the persistence assertion in this test independent of the
// internal replica state filename.
func filepathForReplicaState(t *testing.T, replica *Replica) string {
	t.Helper()
	if replica == nil || replica.statePath == "" {
		t.Fatal("replica state path is empty")
	}
	return filepath.Dir(filepath.Dir(replica.statePath))
}
