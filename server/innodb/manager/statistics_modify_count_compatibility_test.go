package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestInfoSchemaStatsUpdatePersistsThroughConfiguredWriter(t *testing.T) {
	manager := NewInfoSchemaManager(nil, nil, nil)
	var persisted *metadata.InfoTableStats
	manager.SetStatsPersister(func(_ context.Context, schemaName, tableName string, stats *metadata.InfoTableStats) error {
		if schemaName != "app" || tableName != "users" {
			t.Fatalf("persister target = %s.%s, want app.users", schemaName, tableName)
		}
		persisted = stats
		return nil
	})

	stats := &metadata.InfoTableStats{RowCount: 7, ModifyCount: 1}
	require.NoError(t, manager.UpdateTableStats(context.Background(), "app", "users", stats))
	require.Same(t, stats, persisted)
}

func TestInfoSchemaStatsModifyCountSurvivesInvalidationUntilRefresh(t *testing.T) {
	manager := NewInfoSchemaManager(nil, nil, nil)
	key := "app.users"
	manager.modifyCounts[key] = 2

	require.NoError(t, manager.UpdateTableStats(context.Background(), "app", "users", &metadata.InfoTableStats{RowCount: 3, ModifyCount: 0}))
	manager.mu.RLock()
	_, exists := manager.modifyCounts[key]
	manager.mu.RUnlock()
	require.False(t, exists, "ANALYZE refresh should clear the previous modify count")

	manager.modifyCounts[key] = 4
	require.NoError(t, manager.UpdateTableStats(context.Background(), "app", "users", &metadata.InfoTableStats{RowCount: 3, ModifyCount: 4}))
	manager.mu.RLock()
	count := manager.modifyCounts[key]
	manager.mu.RUnlock()
	require.Equal(t, uint64(4), count)
}

func TestInfoSchemaStatsModifyCountSurvivesReloadAfterInvalidation(t *testing.T) {
	manager := NewInfoSchemaManager(nil, nil, nil)
	manager.SetStatsLoader(func(context.Context, string, string) (*metadata.InfoTableStats, error) {
		return &metadata.InfoTableStats{RowCount: 1}, nil
	})

	initial, err := manager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Zero(t, initial.ModifyCount)
	require.NoError(t, manager.InvalidateTableStats(context.Background(), "app", "users"))

	refreshed, err := manager.GetTableStats(context.Background(), "app", "users")
	require.NoError(t, err)
	require.Equal(t, uint64(1), refreshed.ModifyCount)
}
