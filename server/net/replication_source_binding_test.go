package net

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

func TestDecoupledHandlerBindsConfiguredReplicationSource(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:                  t.TempDir(),
		InnodbDataDir:            t.TempDir(),
		InnodbBufferPoolSize:     16 * 1024 * 1024,
		ReplicationRole:          replication.RoleSource,
		ReplicationUUID:          "handler-source",
		ReplicationServerID:      301,
		ReplicationListenAddress: "",
	}
	cfg.InnodbDataDir = cfg.DataDir
	xmysqlEngine := engine.NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, xmysqlEngine.Close()) })
	handler := NewDecoupledMySQLMessageHandlerWithEngine(cfg, xmysqlEngine)
	session := NewMockSession("replication-source-binding")

	require.NoError(t, handler.OnOpen(session))
	source, ok := session.GetAttribute("replication_source").(*replication.Source)
	require.True(t, ok)
	require.NotNil(t, source)
}

func TestDecoupledHandlerRefreshesReplicationSourceAfterRuntimeOwnershipChanges(t *testing.T) {
	cfg := &conf.Cfg{
		DataDir:                  t.TempDir(),
		InnodbDataDir:            t.TempDir(),
		InnodbBufferPoolSize:     16 * 1024 * 1024,
		ReplicationRole:          replication.RoleSource,
		ReplicationUUID:          "handler-live-source",
		ReplicationServerID:      302,
		ReplicationListenAddress: "",
	}
	cfg.InnodbDataDir = cfg.DataDir
	xmysqlEngine := engine.NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, xmysqlEngine.Close()) })
	handler := NewDecoupledMySQLMessageHandlerWithEngine(cfg, xmysqlEngine)
	session := NewMockSession("replication-source-refresh")

	require.NoError(t, handler.OnOpen(session))
	original, ok := session.GetAttribute("replication_source").(*replication.Source)
	require.True(t, ok)
	require.NotNil(t, original)

	// Simulate a stale connection-local cache. The refresh path must consult
	// the engine provider again before a native replication command.
	handler.replicationSource = nil
	handler.refreshReplicationSource(session)
	refreshed, ok := session.GetAttribute("replication_source").(*replication.Source)
	require.True(t, ok)
	require.Same(t, original, refreshed)
}
