package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestParseChangeReplicationFilter(t *testing.T) {
	filters, err := parseReplicationFilter("CHANGE REPLICATION FILTER REPLICATE_DO_DB = ('app', 'audit'), REPLICATE_IGNORE_TABLE = ('app.secret')")
	require.NoError(t, err)
	require.Equal(t, []string{"app", "audit"}, filters.ReplicateDoDB)
	require.Equal(t, []string{"app.secret"}, filters.ReplicateIgnoreTable)
}

func TestParseChangeReplicationFilterRewriteDB(t *testing.T) {
	filters, err := parseReplicationFilter("CHANGE REPLICATION FILTER REPLICATE_REWRITE_DB = ((source_db, target_db), ('legacy_db', 'archive_db'))")
	require.NoError(t, err)
	require.Equal(t, []replication.ReplicationDBRewrite{
		{From: "source_db", To: "target_db"},
		{From: "legacy_db", To: "archive_db"},
	}, filters.ReplicateRewriteDB)
}

func TestChangeReplicationFilterDispatchesToRuntimeControl(t *testing.T) {
	executor := &XMySQLExecutor{}
	var received replication.ReplicationFilterConfig
	executor.SetReplicationFilterControl(func(config replication.ReplicationFilterConfig) error {
		received = config
		return nil
	})
	handled, err := executor.executeReplicationControl(
		"CHANGE REPLICATION FILTER REPLICATE_DO_DB = ('app'), REPLICATE_WILD_IGNORE_TABLE = ('app.secret%')",
		"change replication filter replicate_do_db = ('app'), replicate_wild_ignore_table = ('app.secret%')",
	)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, []string{"app"}, received.ReplicateDoDB)
	require.Equal(t, []string{"app.secret%"}, received.ReplicateWildIgnoreTable)

	handled, err = executor.executeReplicationControl(
		"CHANGE REPLICATION FILTER REPLICATE_DO_DB = ('app') FOR CHANNEL ''",
		"change replication filter replicate_do_db = ('app') for channel ''",
	)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, []string{"app"}, received.ReplicateDoDB)

	handled, err = executor.executeReplicationControl(
		"CHANGE REPLICATION FILTER REPLICATE_DO_DB = ('app') FOR CHANNEL 'analytics'",
		"change replication filter replicate_do_db = ('app') for channel 'analytics'",
	)
	require.True(t, handled)
	require.ErrorContains(t, err, "only the default replication channel")
}

func TestPerformanceSchemaReplicationFilterViewProjectsRewriteDB(t *testing.T) {
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{ReplicationFilters: []replication.ReplicationFilterStatus{{
			Name: "REPLICATE_REWRITE_DB", Rule: "source_db -> target_db", ConfiguredBy: "CHANGE_REPLICATION_FILTER",
		}}}
	})
	result := executor.executePerformanceSchemaReplicationSelect(
		"select filter_name, filter_rule, configured_by from performance_schema.replication_applier_global_filters",
		"replication_applier_global_filters",
	)
	require.Equal(t, [][]interface{}{{"REPLICATE_REWRITE_DB", "source_db -> target_db", "CHANGE_REPLICATION_FILTER"}}, selectResultRows(result))
}

func TestPerformanceSchemaReplicationFilterViewsProjectConfiguredRules(t *testing.T) {
	activeSince := time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC)
	executor := &XMySQLExecutor{}
	executor.SetReplicationStatusProvider(func() replication.StatusSnapshot {
		return replication.StatusSnapshot{
			Role:      replication.RoleReplica,
			SourceURL: "mysql://source.example:3306",
			ReplicationFilters: []replication.ReplicationFilterStatus{{
				Name: "REPLICATE_DO_DB", Rule: "app", ConfiguredBy: "CHANGE_REPLICATION_FILTER", ActiveSince: activeSince, Counter: 3,
			}},
		}
	})

	rowResult := executor.executePerformanceSchemaReplicationSelect(
		"select channel_name, filter_name, filter_rule, configured_by, active_since, counter from performance_schema.replication_applier_filters",
		"replication_applier_filters",
	)
	require.Equal(t, [][]interface{}{{"", "REPLICATE_DO_DB", "app", "CHANGE_REPLICATION_FILTER", "2026-09-27 14:00:00", "3"}}, selectResultRows(rowResult))

	globalResult := executor.executePerformanceSchemaReplicationSelect(
		"select filter_name, filter_rule, configured_by, active_since from performance_schema.replication_applier_global_filters",
		"replication_applier_global_filters",
	)
	require.Equal(t, [][]interface{}{{"REPLICATE_DO_DB", "app", "CHANGE_REPLICATION_FILTER", "2026-09-27 14:00:00"}}, selectResultRows(globalResult))
}
