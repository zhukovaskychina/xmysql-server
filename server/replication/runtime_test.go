package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errApplyFailed = errors.New("apply failed")

func TestParseMySQLErrorNumber(t *testing.T) {
	require.Equal(t, int64(1236), parseMySQLErrorNumber(errors.New("ERROR 1236 (HY000): Could not find first log file name in binary log index file")))
	require.Equal(t, int64(1045), parseMySQLErrorNumber(errors.New("error 1045: Access denied")))
	require.Zero(t, parseMySQLErrorNumber(errors.New("source unavailable")))
}

func TestRuntimeStatusTracksReplicationErrorNumberAndTimestamp(t *testing.T) {
	runtime := &Runtime{replica: &Replica{}}
	runtime.setError(wrapReplicationError(replicationErrorClassSQL, errors.New("ERROR 1236 (HY000): source stopped")))

	status := runtime.Status()
	require.Equal(t, int64(1236), status.LastErrorNumber)
	require.Equal(t, "ERROR 1236 (HY000): source stopped", status.LastError)
	require.False(t, status.LastErrorAt.IsZero())
	require.Equal(t, int64(1236), status.LastSQLErrorNumber)
	require.False(t, status.LastSQLErrorAt.IsZero())
	require.Zero(t, status.LastIOErrorNumber)
}

func TestNativeRetryDelayUsesConfiguredConnectionRetryInterval(t *testing.T) {
	fallback := 500 * time.Millisecond
	retryErr := errors.New("source unavailable")

	require.Equal(t, 7*time.Second, nativeRetryDelay(MySQLBinlogSourceConfig{ConnectionRetryInterval: 7}, retryErr, fallback))
	require.Equal(t, fallback, nativeRetryDelay(MySQLBinlogSourceConfig{ConnectionRetryInterval: 7}, nil, fallback))
	require.Equal(t, fallback, nativeRetryDelay(MySQLBinlogSourceConfig{}, retryErr, fallback))
}

func TestNativeRuntimeTracksHeartbeatFramesForStatus(t *testing.T) {
	heartbeat := make([]byte, 19)
	heartbeat[4] = 27 // HEARTBEAT_EVENT
	source := &scriptedNativeSource{
		batches: [][]NativeBinlogEvent{{{
			File:        "mysql-bin.000001",
			Position:    4,
			EndPosition: 100,
			Type:        27,
			Raw:         heartbeat,
		}}},
		uuid: "heartbeat-source",
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "heartbeat-replica",
		ServerID:     17,
		SourceURL:    "mysql://repl@127.0.0.1:3306/?binlog_file=mysql-bin.000001&gtid_auto_position=false",
		NativeSource: source,
	})
	require.NoError(t, err)

	position := uint64(4)
	require.NoError(t, runtime.pullNativeOnce(context.Background(), &position))
	status := runtime.Status()
	require.Equal(t, uint64(1), status.ReceivedHeartbeats)
	require.False(t, status.LastHeartbeatAt.IsZero())
	require.Equal(t, uint64(100), status.SourcePosition)
}

type scriptedNativeSource struct {
	mu      sync.Mutex
	batches [][]NativeBinlogEvent
	errors  []error
	index   int
	uuid    string
}

func (source *scriptedNativeSource) Dump(_ context.Context, maxEvents int) ([]NativeBinlogEvent, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.index >= len(source.batches) {
		return nil, nil
	}
	batch := append([]NativeBinlogEvent(nil), source.batches[source.index]...)
	source.index++
	if maxEvents > 0 && len(batch) > maxEvents {
		batch = batch[:maxEvents]
	}
	var err error
	if source.index-1 < len(source.errors) {
		err = source.errors[source.index-1]
	}
	return batch, err
}

func (source *scriptedNativeSource) SourceUUID(context.Context) (string, error) {
	return source.uuid, nil
}

func TestNativeRuntimePullAppliesSplitTransactionAndPersistsPosition(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "native-runtime-upstream", 41)
	require.NoError(t, err)
	_, err = upstream.AppendTransaction(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id", "value"},
		After:   map[string]interface{}{"id": int64(1), "value": "native"},
	}}, nil)
	require.NoError(t, err)
	native, err := upstream.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(native), 2)
	split := len(native) - 1
	source := &scriptedNativeSource{batches: [][]NativeBinlogEvent{native[:split], native[split:]}, uuid: "native-upstream-uuid"}

	var mu sync.Mutex
	var applied []RowChange
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-runtime-replica",
		ServerID:     42,
		SourceURL:    "mysql://root:secret@127.0.0.1:3306/?server_id=42&binlog_file=binlog.000001&binlog_pos=4",
		NativeSource: source,
		PollInterval: 5 * time.Millisecond,
		ApplyRows: func(changes []RowChange) error {
			mu.Lock()
			defer mu.Unlock()
			applied = append(applied, changes...)
			return nil
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, runtime.Start(ctx))
	defer runtime.Close()

	appliedEventually := assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		value, ok := appliedValueBytes(applied, "value")
		return len(applied) == 1 && ok && string(value) == "native"
	}, 2*time.Second, 5*time.Millisecond)
	if !appliedEventually {
		t.Logf("native runtime status after timeout: %+v", runtime.Status())
		mu.Lock()
		t.Logf("applied rows: %#v", applied)
		mu.Unlock()
	}
	require.True(t, appliedEventually)
	require.Equal(t, "mysql://127.0.0.1:3306/?server_id=42&binlog_file=binlog.000001&binlog_pos=4", runtime.Status().SourceURL)
	require.Equal(t, native[len(native)-1].EndPosition, runtime.Status().SourcePosition)
	require.NotEmpty(t, runtime.Status().ExecutedGTIDs)
	require.Equal(t, "native-upstream-uuid", runtime.Status().SourceUUID)

	reloaded, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   runtime.cfg.DataDir,
		UUID:      "native-runtime-replica",
		ServerID:  42,
		SourceURL: "mysql://root:secret@127.0.0.1:3306/?server_id=42&binlog_file=binlog.000001&binlog_pos=4",
	})
	require.NoError(t, err)
	require.Equal(t, runtime.Status().SourcePosition, reloaded.Status().SourcePosition)
	require.Equal(t, runtime.Status().ExecutedGTIDs, reloaded.Status().ExecutedGTIDs)
	require.Equal(t, runtime.Status().SourceUUID, reloaded.Status().SourceUUID,
		"replica restart must retain the authoritative upstream UUID before the next pull")
	identity, err := os.ReadFile(filepath.Join(runtime.cfg.DataDir, "replication", "source_identity.json"))
	require.NoError(t, err)
	require.Contains(t, string(identity), `"source_uuid": "native-upstream-uuid"`)
	require.NotContains(t, string(identity), "secret", "source identity persistence must not copy source credentials")
}

func TestNativeRuntimeRecoversAfterPartialBatchTransportErrorWithoutDuplicateApply(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "native-runtime-network-upstream", 43)
	require.NoError(t, err)
	_, err = upstream.AppendTransaction(1, []RowChange{{
		Table:   "app.docs",
		Action:  "insert",
		Columns: []string{"id", "value"},
		After:   map[string]interface{}{"id": int64(7), "value": "network-retry"},
	}}, nil)
	require.NoError(t, err)
	native, err := upstream.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(native), 2)
	split := len(native) - 1
	transportErr := errors.New("injected native transport reset after partial batch")
	source := &scriptedNativeSource{
		batches: [][]NativeBinlogEvent{native[:split], native[split:]},
		errors:  []error{transportErr, nil},
		uuid:    "native-runtime-network-upstream",
	}

	var mu sync.Mutex
	var applied []RowChange
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-runtime-network-replica",
		ServerID:     44,
		SourceURL:    "mysql://root:secret@127.0.0.1:3306/?server_id=44&binlog_file=binlog.000001&binlog_pos=4",
		NativeSource: source,
		PollInterval: 5 * time.Millisecond,
		ApplyRows: func(changes []RowChange) error {
			mu.Lock()
			defer mu.Unlock()
			applied = append(applied, changes...)
			return nil
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, runtime.Start(ctx))
	defer runtime.Close()

	completed := assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		value, ok := appliedValueBytes(applied, "value")
		return len(applied) == 1 && ok && string(value) == "network-retry" &&
			runtime.Status().SourcePosition > 4
	}, 2*time.Second, 5*time.Millisecond)
	if !completed {
		mu.Lock()
		t.Logf("applied after transport retry: %#v", applied)
		mu.Unlock()
		t.Logf("runtime status after transport retry: %+v", runtime.Status())
	}
	require.True(t, completed)

	mu.Lock()
	require.Len(t, applied, 1, "a transport retry after a partial native batch must not duplicate storage apply")
	mu.Unlock()
	status := runtime.Status()
	require.NotEmpty(t, status.ExecutedGTIDs)
	require.Greater(t, status.SourcePosition, uint64(4))
	require.Contains(t, status.LastIOError, "injected native transport reset")
}

func TestRuntimeNamedChannelsPersistIsolatedSourceAndStatus(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   dataDir,
		UUID:      "named-channel-runtime",
		ServerID:  81,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)

	require.NoError(t, runtime.ChangeSourceForChannel("west", "http://127.0.0.1:2"))
	status, err := runtime.StatusForChannel("west")
	require.NoError(t, err)
	require.Equal(t, "west", status.ChannelName)
	require.Equal(t, "http://127.0.0.1:2", status.SourceURL)
	require.NotEqual(t, runtime.Status().SourceURL, status.SourceURL)
	require.FileExists(t, filepath.Join(dataDir, "replication", "channels.json"))
	require.FileExists(t, filepath.Join(dataDir, "channels", "west", "replication", "source.json"))

	reloaded, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   dataDir,
		UUID:      "named-channel-runtime",
		ServerID:  81,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	reloadedStatus, err := reloaded.StatusForChannel("west")
	require.NoError(t, err)
	require.Equal(t, "west", reloadedStatus.ChannelName)
	require.Equal(t, "http://127.0.0.1:2", reloadedStatus.SourceURL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, reloaded.Start(ctx))
	defer reloaded.Close()
	require.NoError(t, reloaded.StopReplicaForChannel("west"))
	require.NoError(t, reloaded.ResetReplicaAllForChannel("west"))
	resetStatus, err := reloaded.StatusForChannel("west")
	require.NoError(t, err)
	require.Empty(t, resetStatus.SourceURL)
}

func TestRuntimeNamedChannelsInheritClusterFailoverConfiguration(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:           RoleReplica,
		DataDir:        t.TempDir(),
		UUID:           "named-channel-failover-config",
		ServerID:       82,
		SourceURL:      "http://127.0.0.1:1",
		NativeEndpoint: "mysql://127.0.0.1:3306",
		Peers:          []string{"http://127.0.0.1:29001", "http://127.0.0.1:29002"},
		AutoFailover:   true,
		FailureTimeout: 17 * time.Second,
		PollInterval:   23 * time.Millisecond,
	})
	require.NoError(t, err)
	defer runtime.Close()

	channel, err := runtime.channelRuntime("west")
	require.NoError(t, err)
	require.Equal(t, runtime.cfg.Peers, channel.cfg.Peers)
	require.Equal(t, runtime.cfg.AutoFailover, channel.cfg.AutoFailover)
	require.Equal(t, runtime.cfg.FailureTimeout, channel.cfg.FailureTimeout)
	require.Equal(t, runtime.cfg.PollInterval, channel.cfg.PollInterval)
	require.Equal(t, runtime.cfg.NativeEndpoint, channel.cfg.NativeEndpoint)
}

func TestRuntimeNamedNativeChannelsPollIndependently(t *testing.T) {
	westUpstream, err := NewSource(t.TempDir(), "west-native-upstream", 141)
	require.NoError(t, err)
	_, err = westUpstream.AppendTransaction(1, []RowChange{{
		Table:   "app.channel_rows",
		Action:  "insert",
		Columns: []string{"id", "value"},
		After:   map[string]interface{}{"id": int64(1), "value": "west"},
	}}, nil)
	require.NoError(t, err)
	westEvents, err := westUpstream.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)

	eastUpstream, err := NewSource(t.TempDir(), "east-native-upstream", 142)
	require.NoError(t, err)
	_, err = eastUpstream.AppendTransaction(1, []RowChange{{
		Table:   "app.channel_rows",
		Action:  "insert",
		Columns: []string{"id", "value"},
		After:   map[string]interface{}{"id": int64(2), "value": "east"},
	}}, nil)
	require.NoError(t, err)
	eastEvents, err := eastUpstream.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)

	var mu sync.Mutex
	applied := make(map[string][]RowChange)
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "named-native-channel-runtime",
		ServerID:     143,
		SourceURL:    "http://127.0.0.1:1",
		PollInterval: 5 * time.Millisecond,
		ApplyRowsWithID: func(gtid string, changes []RowChange) error {
			mu.Lock()
			defer mu.Unlock()
			applied[gtid] = append(applied[gtid], changes...)
			return nil
		},
	})
	require.NoError(t, err)
	defer runtime.Close()

	require.NoError(t, runtime.ChangeSourceForChannel("west", "mysql://repl@127.0.0.1:3306/?binlog_file=binlog.000001&gtid_auto_position=false"))
	require.NoError(t, runtime.ChangeSourceForChannel("east", "mysql://repl@127.0.0.1:3307/?binlog_file=binlog.000001&gtid_auto_position=false"))

	west, err := runtime.channelRuntime("west")
	require.NoError(t, err)
	east, err := runtime.channelRuntime("east")
	require.NoError(t, err)
	west.mu.Lock()
	west.nativeSource = &scriptedNativeSource{batches: [][]NativeBinlogEvent{westEvents}, uuid: "west-native-source-uuid"}
	west.mu.Unlock()
	east.mu.Lock()
	east.nativeSource = &scriptedNativeSource{batches: [][]NativeBinlogEvent{eastEvents}, uuid: "east-native-source-uuid"}
	east.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, west.Start(ctx))
	require.NoError(t, east.Start(ctx))

	ready := assert.Eventually(t, func() bool {
		westStatus := west.Status()
		eastStatus := east.Status()
		mu.Lock()
		appliedCount := len(applied)
		mu.Unlock()
		return westStatus.SourceUUID == "west-native-source-uuid" &&
			eastStatus.SourceUUID == "east-native-source-uuid" &&
			westStatus.SourcePosition > 4 &&
			eastStatus.SourcePosition > 4 &&
			appliedCount == 2
	}, 2*time.Second, 5*time.Millisecond)
	if !ready {
		mu.Lock()
		t.Logf("applied=%#v", applied)
		mu.Unlock()
		t.Logf("west status=%+v", west.Status())
		t.Logf("east status=%+v", east.Status())
	}
	require.True(t, ready)

	westStatus := west.Status()
	eastStatus := east.Status()
	require.Equal(t, "west", westStatus.ChannelName)
	require.Equal(t, "east", eastStatus.ChannelName)
	require.NotEqual(t, westStatus.SourceUUID, eastStatus.SourceUUID)
	require.NotEqual(t, westStatus.ExecutedGTIDs, eastStatus.ExecutedGTIDs)
	require.Contains(t, westStatus.SourceURL, "127.0.0.1:3306")
	require.Contains(t, eastStatus.SourceURL, "127.0.0.1:3307")

	mu.Lock()
	defer mu.Unlock()
	var values []string
	for _, changes := range applied {
		for _, change := range changes {
			switch value := change.After["value"].(type) {
			case []byte:
				values = append(values, string(value))
			case string:
				values = append(values, value)
			}
		}
	}
	require.ElementsMatch(t, []string{"west", "east"}, values)
}

func TestNativeRuntimeRestoresPersistedGTIDForAutoPosition(t *testing.T) {
	dataDir := t.TempDir()
	stateDir := filepath.Join(dataDir, "replication")
	require.NoError(t, os.MkdirAll(stateDir, 0755))
	executed := GTIDSet{}
	executed.Add(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 7})
	raw, err := json.Marshal(replicaState{Executed: executed})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "replica_gtid.json"), raw, 0644))

	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   dataDir,
		UUID:      "native-gtid-replica",
		ServerID:  12,
		SourceURL: "mysql://repl@127.0.0.1:3306/?gtid_auto_position=true",
	})
	require.NoError(t, err)
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff:7", runtime.nativeConfig.GTIDSet)
	require.True(t, runtime.nativeConfig.GTIDAutoPosition)
}

func TestNativeRecoveryPrefersPersistedFilePositionBeforeGTID(t *testing.T) {
	require.True(t, nativeRecoveryUsesPersistedFilePosition("binlog.000003", 1973, false))
	require.False(t, nativeRecoveryUsesPersistedFilePosition("", 0, false))
	require.False(t, nativeRecoveryUsesPersistedFilePosition("binlog.000003", 1973, true),
		"a pending relay transaction must keep its file-replay path")
}

func TestNativeRuntimePreservesConfiguredGTIDBaselineBeforeFirstPull(t *testing.T) {
	source := &scriptedNativeSource{
		batches: [][]NativeBinlogEvent{{}},
		uuid:    "official-mysql-baseline",
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-initial-gtid-replica",
		ServerID:     13,
		SourceURL:    "mysql://repl@127.0.0.1:3306/?gtid_auto_position=true&gtid_set=00112233-4455-6677-8899-aabbccddeeff:7",
		NativeSource: source,
	})
	require.NoError(t, err)

	position := uint64(4)
	require.NoError(t, runtime.pullNativeOnce(context.Background(), &position))
	require.Equal(t, 1, source.index, "the configured GTID baseline must allow the first native pull")
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff:7", runtime.nativeConfig.GTIDSet)
}

func TestNativeRuntimeAllowsEmptyConfiguredGTIDAutoPositionBeforeFirstPull(t *testing.T) {
	source := &scriptedNativeSource{batches: [][]NativeBinlogEvent{{}}, uuid: "empty-gtid-source"}
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-empty-gtid-replica",
		ServerID:     14,
		SourceURL:    "mysql://repl@127.0.0.1:3306/?gtid_auto_position=true",
		NativeSource: source,
	})
	require.NoError(t, err)

	position := uint64(4)
	require.NoError(t, runtime.pullNativeOnce(context.Background(), &position))
	require.Equal(t, 1, source.index)
}

func appliedValueBytes(changes []RowChange, column string) ([]byte, bool) {
	if len(changes) == 0 {
		return nil, false
	}
	switch value := changes[0].After[column].(type) {
	case []byte:
		return value, true
	case string:
		return []byte(value), true
	default:
		return nil, false
	}
}

func appliedContainsValue(changes []RowChange, want string) bool {
	for _, change := range changes {
		for _, value := range change.After {
			switch value := value.(type) {
			case []byte:
				if string(value) == want {
					return true
				}
			case string:
				if value == want {
					return true
				}
			}
		}
	}
	return false
}

func TestSourceReplicaRuntimeStreamsCommittedStatementsAndPromotes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "source-runtime",
		ServerID:   11,
		ListenAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, source.Start(ctx))
	defer source.Close()

	var mu sync.Mutex
	var applied []Statement
	replicaDir := t.TempDir()
	replica, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      replicaDir,
		UUID:         "replica-runtime",
		ServerID:     12,
		ListenAddr:   "127.0.0.1:0",
		SourceURL:    "http://" + source.Address(),
		PollInterval: 10 * time.Millisecond,
		Apply: func(statements []Statement) error {
			mu.Lock()
			defer mu.Unlock()
			applied = append(applied, statements...)
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, replica.Start(ctx))
	defer replica.Close()

	require.NoError(t, source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}}))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1 && applied[0].SQL == "insert into t values (1)"
	}, 2*time.Second, 10*time.Millisecond)

	statusResp, err := http.Get("http://" + replica.Address() + "/replication/status")
	require.NoError(t, err)
	defer statusResp.Body.Close()
	require.Equal(t, http.StatusOK, statusResp.StatusCode)

	require.NoError(t, replica.Promote())
	require.Equal(t, RoleSource, replica.Status().Role)
	require.Contains(t, replica.Status().ExecutedGTIDs, "source-runtime:1")
	require.NoError(t, replica.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (2)"}}))
	require.Contains(t, replica.Status().ExecutedGTIDs, "replica-runtime")
	reloadedSource, err := NewSource(replicaDir, "replica-runtime", 12)
	require.NoError(t, err)
	require.Contains(t, reloadedSource.Executed.String(), "source-runtime:1")
	require.Contains(t, reloadedSource.Executed.String(), "replica-runtime:1")
}

func TestRuntimePromotionPersistsReplicaGTIDsBeforeNewWrite(t *testing.T) {
	dataDir := t.TempDir()
	oldSource, err := NewSource(dataDir, "old-source", 11)
	require.NoError(t, err)
	_, err = oldSource.Append(1, []RowChange{{Table: "app.docs", Action: "insert"}})
	require.NoError(t, err)

	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   dataDir,
		UUID:      "promoted-source",
		ServerID:  12,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	runtime.replica.mu.Lock()
	runtime.replica.Executed.Add(GTID{UUID: "upstream-source", Seq: 7})
	runtime.replica.mu.Unlock()
	require.NoError(t, runtime.Promote())

	reloaded, err := NewSource(dataDir, "promoted-source", 12)
	require.NoError(t, err)
	require.True(t, reloaded.Executed.Contains(GTID{UUID: "upstream-source", Seq: 7}), "promotion must persist the replica GTID set before the first promoted write")
}

func TestRuntimePromotionPersistsSourceRoleAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   dataDir,
		UUID:      "durable-promoted-source",
		ServerID:  13,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	require.NoError(t, runtime.Promote())
	require.Equal(t, RoleSource, runtime.Status().Role)

	reloaded, err := NewRuntime(RuntimeConfig{
		Role:     RoleReplica,
		DataDir:  dataDir,
		UUID:     "durable-promoted-source",
		ServerID: 13,
	})
	require.NoError(t, err)
	require.Equal(t, RoleSource, reloaded.Status().Role, "a promoted node must restart as the durable source")
	require.NotNil(t, reloaded.Source())
	require.FileExists(t, filepath.Join(dataDir, "replication", "role.json"))
}

func TestRuntimePromotionImportsCommittedRelayHistoryIntoNewSource(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "promotion-history-upstream", 31)
	require.NoError(t, err)
	_, err = upstream.AppendTransaction(1, []RowChange{{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(1)}}}, nil)
	require.NoError(t, err)
	events, err := upstream.Dump(4)
	require.NoError(t, err)

	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "promotion-history-source",
		ServerID:  32,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	require.NoError(t, runtime.replica.Apply(events))

	require.NoError(t, runtime.Promote())
	history, err := runtime.source.Dump(4)
	require.NoError(t, err)
	require.Len(t, history, 3, "promotion must make committed relay history available from the new source binlog")
	require.Equal(t, EventCommit, history[len(history)-1].Type)
	require.Equal(t, GTID{UUID: "promotion-history-upstream", Seq: 1}, history[len(history)-1].GTID)
	native, err := runtime.source.Writer.NativeEvents("binlog.000001")
	require.NoError(t, err)
	var sawImportedGTID bool
	for _, event := range native {
		if event.Type != 33 || len(event.Raw) < 9 {
			continue
		}
		if binary.LittleEndian.Uint32(event.Raw[5:9]) == 32 {
			sawImportedGTID = true
			break
		}
	}
	require.True(t, sawImportedGTID, "promotion must regenerate native history with the promoted server-id")

	err = runtime.AppendCommittedTransaction([]RowChange{{Table: "app.docs", Action: "insert"}}, nil)
	require.NoError(t, err)
	all, err := runtime.source.Dump(4)
	require.NoError(t, err)
	require.Len(t, all, 6, "promoted writes must follow imported relay history")
}

func TestRuntimePullPersistsSourcePositionAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "runtime-pull-source",
		ServerID:   21,
		ListenAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, source.Start(ctx))
	defer source.Close()
	require.NoError(t, source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into docs values (1)"}}))

	replicaDir := t.TempDir()
	replica, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      replicaDir,
		UUID:         "runtime-pull-replica",
		ServerID:     22,
		ListenAddr:   "127.0.0.1:0",
		SourceURL:    "http://" + source.Address(),
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, replica.Start(ctx))
	require.Eventually(t, func() bool {
		return replica.Status().SourcePosition > 4
	}, 2*time.Second, 10*time.Millisecond)
	durablePosition := replica.Status().SourcePosition
	require.Equal(t, "binlog.000001", replica.replica.LastSourceFile)
	require.NoError(t, replica.Close())

	restarted, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   replicaDir,
		UUID:      "runtime-pull-replica",
		ServerID:  22,
		SourceURL: "http://" + source.Address(),
	})
	require.NoError(t, err)
	require.Equal(t, durablePosition, restarted.Status().SourcePosition)
	require.Equal(t, "binlog.000001", restarted.replica.LastSourceFile)
}

func TestRuntimePromotionPreservesPreparedXA(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "upstream-xa", 11)
	require.NoError(t, err)
	xid := XAIdentity{GTRID: "promote-gtrid", BQUAL: "branch", FormatID: 11}
	change := RowChange{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(12)}}
	require.NoError(t, upstream.PrepareXATransaction("promote-xa-key", xid, []RowChange{change}, nil))
	events, err := upstream.Dump(4)
	require.NoError(t, err)

	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "promoted-xa-source",
		ServerID:  12,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	require.NoError(t, runtime.replica.Apply(events))
	require.Contains(t, runtime.replica.preparedXA, xid.Key())

	require.NoError(t, runtime.Promote())
	require.Contains(t, runtime.source.preparedXA, xid.Key())
	require.NoError(t, runtime.CommitXATransaction("promote-xa-key", xid))
	require.True(t, runtime.source.Executed.Contains(GTID{UUID: upstream.UUID, Seq: 1}))

	reloaded, err := NewSource(runtime.cfg.DataDir, "promoted-xa-source", 12)
	require.NoError(t, err)
	require.True(t, reloaded.Executed.Contains(GTID{UUID: upstream.UUID, Seq: 1}))
	require.Empty(t, reloaded.preparedXA)
}

func TestRuntimePromotionPreservesNativePreparedXA(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "native-upstream-xa", 21)
	require.NoError(t, err)
	xid := XAIdentity{GTRID: "native-promote-gtrid", BQUAL: "branch", FormatID: 21}
	change := RowChange{Table: "app.docs", Action: "insert", Columns: []string{"id"}, After: map[string]interface{}{"id": int64(13)}}
	require.NoError(t, upstream.PrepareXATransaction("native-promote-xa-key", xid, []RowChange{change}, nil))

	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "native-promoted-xa-source",
		ServerID:  22,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	position, err := runtime.replica.ReplicateNativeFrom(upstream, "binlog.000001", 4)
	require.NoError(t, err)
	require.Greater(t, position, uint64(4))
	require.Contains(t, runtime.replica.preparedXA, xid.Key())

	require.NoError(t, runtime.Promote())
	require.Contains(t, runtime.source.preparedXA, xid.Key())
	require.NoError(t, runtime.CommitXATransaction("native-promote-xa-key", xid))
	require.True(t, runtime.source.Executed.Contains(GTID{UUID: upstream.UUID, Seq: 1}))
}

func TestRuntimePromotionPreservesCommittedNativeRelayHistory(t *testing.T) {
	upstream, err := NewSource(t.TempDir(), "00112233-4455-6677-8899-aabbccddeeff", 23)
	require.NoError(t, err)
	_, err = upstream.Append(1, []RowChange{{
		Table:       "app.docs",
		Action:      "insert",
		Columns:     []string{"id", "value"},
		ColumnTypes: map[string]string{"id": "INT", "value": "VARCHAR"},
		After:       map[string]interface{}{"id": int32(17), "value": "before-promotion"},
	}})
	require.NoError(t, err)

	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "11223344-5566-7788-99aa-bbccddeeff00",
		ServerID:  24,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	_, err = runtime.replica.ReplicateNativeFrom(upstream, "binlog.000001", 4)
	require.NoError(t, err)
	require.NotEmpty(t, runtime.replica.nativeRelayEvents)

	require.NoError(t, runtime.Promote())
	transactions, err := runtime.source.DecodeNativeDumpFrom("binlog.000001", 4, nil)
	require.NoError(t, err)
	require.Len(t, transactions, 1, "promotion must preserve committed native relay history for downstream consumers")
	require.Equal(t, GTID{UUID: upstream.UUID, Seq: 1}, transactions[0].GTID)
	require.Equal(t, "before-promotion", transactions[0].Changes[0].After["value"])
}

func TestRuntimeFenceStopsSourceWritesAndHidesBinlogSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "fenced-source",
		ServerID:   51,
		ListenAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, source.Start(ctx))
	defer source.Close()

	payload := `{"epoch":1,"candidate_uuid":"promoted-replica"}`
	request, err := http.NewRequest(http.MethodPost, "http://"+source.Address()+"/replication/fence", strings.NewReader(payload))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()

	status := source.Status()
	require.True(t, status.Fenced)
	require.Equal(t, uint64(1), status.FencingEpoch)
	require.Equal(t, "promoted-replica", status.FencedBy)
	require.Nil(t, source.Source())
	require.ErrorContains(t, source.AppendCommitted([]Statement{{SQL: "insert into t values (1)"}}), "fenced")
}

func TestRuntimePromoteFencesReachableSourceBeforePromotion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldSource, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "old-source",
		ServerID:   61,
		ListenAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, oldSource.Start(ctx))
	defer oldSource.Close()

	replica, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "new-source",
		ServerID:  62,
		SourceURL: "http://" + oldSource.Address(),
		Peers:     []string{"http://" + oldSource.Address()},
	})
	require.NoError(t, err)
	require.NoError(t, replica.Promote())
	require.Equal(t, RoleSource, replica.Status().Role)
	require.False(t, replica.Status().Fenced)
	require.True(t, oldSource.Status().Fenced)
	require.ErrorContains(t, oldSource.AppendCommitted([]Statement{{SQL: "insert into t values (1)"}}), "fenced")
}

func TestRuntimeFencingStateSurvivesRestartAndRejectsStaleEpoch(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "persistent-fence", ServerID: 71})
	require.NoError(t, err)
	require.NoError(t, runtime.applyFence(fenceRequest{Epoch: 7, CandidateUUID: "winner-a"}))
	require.Error(t, runtime.applyFence(fenceRequest{Epoch: 6, CandidateUUID: "winner-b"}))
	require.Error(t, runtime.applyFence(fenceRequest{Epoch: 7, CandidateUUID: "winner-b"}))

	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "persistent-fence", ServerID: 71})
	require.NoError(t, err)
	status := reloaded.Status()
	require.True(t, status.Fenced)
	require.Equal(t, uint64(7), status.FencingEpoch)
	require.Equal(t, "winner-a", status.FencedBy)
	require.ErrorContains(t, reloaded.AppendCommitted([]Statement{{SQL: "insert into t values (1)"}}), "fenced")
}

func TestRuntimePromotionCannotClearAConcurrentHigherEpochFence(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "candidate-replica",
		ServerID:  72,
		SourceURL: "http://127.0.0.1:1",
	})
	require.NoError(t, err)

	runtime.mu.Lock()
	runtime.fencingEpoch = 9
	runtime.fenced = true
	runtime.fencedBy = "lower-winner"
	runtime.mu.Unlock()

	require.ErrorContains(t, runtime.acquirePromotionFence(context.Background()), "promotion fencing lost to lower-winner")
	status := runtime.Status()
	require.True(t, status.Fenced)
	require.Equal(t, uint64(9), status.FencingEpoch)
	require.Equal(t, "lower-winner", status.FencedBy)
}

func TestRuntimeConcurrentFencesPersistMonotonicEpoch(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:     RoleSource,
		DataDir:  t.TempDir(),
		UUID:     "fence-order-source",
		ServerID: 73,
	})
	require.NoError(t, err)

	const requests = 24
	errs := make(chan error, requests)
	for epoch := uint64(1); epoch <= requests; epoch++ {
		epoch := epoch
		go func() {
			errs <- runtime.applyFence(fenceRequest{Epoch: epoch, CandidateUUID: fmt.Sprintf("winner-%03d", epoch)})
		}()
	}
	for index := 0; index < requests; index++ {
		err := <-errs
		if err != nil && !strings.Contains(err.Error(), "fencing epoch already owned") && !strings.Contains(err.Error(), "stale fencing epoch") {
			t.Fatalf("unexpected concurrent fencing error: %v", err)
		}
	}

	reloaded, err := NewRuntime(RuntimeConfig{
		Role:     RoleSource,
		DataDir:  runtime.cfg.DataDir,
		UUID:     "fence-order-source",
		ServerID: 73,
	})
	require.NoError(t, err)
	status := reloaded.Status()
	require.Equal(t, uint64(requests), status.FencingEpoch)
	require.True(t, status.Fenced)
	require.Equal(t, "winner-024", status.FencedBy)
}

func TestRuntimeFenceStateRollsBackWhenPersistenceFails(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:     RoleSource,
		DataDir:  t.TempDir(),
		UUID:     "fence-rollback-source",
		ServerID: 74,
	})
	require.NoError(t, err)
	blocker := filepath.Join(t.TempDir(), "fencing-parent")
	require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0644))
	runtime.fencingPath = filepath.Join(blocker, "fencing.json")

	require.Error(t, runtime.applyFence(fenceRequest{Epoch: 3, CandidateUUID: "rollback-winner"}))
	status := runtime.Status()
	require.Zero(t, status.FencingEpoch)
	require.False(t, status.Fenced)
	require.Empty(t, status.FencedBy)
}

func TestReplicaRuntimeDoesNotAdvanceGTIDWhenApplyFails(t *testing.T) {
	source, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, source.Start(ctx))
	defer source.Close()

	replica, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		SourceURL:    "http://" + source.Address(),
		PollInterval: 10 * time.Millisecond,
		Apply:        func([]Statement) error { return errApplyFailed },
	})
	require.NoError(t, err)
	require.NoError(t, replica.Start(ctx))
	defer replica.Close()
	require.NoError(t, source.AppendCommitted([]Statement{{SQL: "insert into t values (1)"}}))
	require.Eventually(t, func() bool { return strings.Contains(replica.Status().LastError, errApplyFailed.Error()) }, 2*time.Second, 10*time.Millisecond)
	require.Empty(t, replica.Status().ExecutedGTIDs)
}

func TestReplicaRuntimeCloseWaitsForPollLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		SourceURL:    "http://127.0.0.1:1",
		ListenAddr:   "127.0.0.1:0",
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, runtime.Start(ctx))
	require.NoError(t, runtime.Close())
	select {
	case <-runtime.pollDone:
	default:
		t.Fatal("runtime close returned before replica poll loop stopped")
	}
}

func TestSourceBinlogFiltersTransactionsAlreadyExecutedByReplica(t *testing.T) {
	source, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "source-filter",
		ServerID:   21,
		ListenAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, source.Start(ctx))
	defer source.Close()
	require.NoError(t, source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}}))
	require.NoError(t, source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (2)"}}))

	filtered, err := source.Source().filteredDump(4, "source-filter:1")
	require.NoError(t, err)
	require.Len(t, filtered, 3)
	require.Equal(t, uint64(2), filtered[0].GTID.Seq)
	require.Equal(t, EventBegin, filtered[0].Type)
}

func TestSourceBinlogResponseAdvancesPastLastEvent(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), UUID: "source-position", ServerID: 21})
	require.NoError(t, err)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/replication/binlog?position=4", nil)
	recorder := httptest.NewRecorder()
	runtime.handleBinlog(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	var response binlogResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Len(t, response.Events, 3)
	require.Equal(t, response.Events[len(response.Events)-1].Position+1, response.NextPosition)
	require.Equal(t, "binlog.000001", response.LogName)
}

func TestSourceBinlogRejectsInvalidPosition(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), UUID: "source-position-invalid", ServerID: 21})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/replication/binlog?position=not-a-number", nil)
	recorder := httptest.NewRecorder()
	runtime.handleBinlog(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestSourceBinlogAdvancesWhenAllEventsAreGTIDFiltered(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), UUID: "source-filter-position", ServerID: 21})
	require.NoError(t, err)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (2)"}})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/replication/binlog?position=4&gtids=source-filter-position:1-2", nil)
	recorder := httptest.NewRecorder()
	runtime.handleBinlog(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	var response binlogResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Empty(t, response.Events)
	require.Equal(t, uint64(11), response.NextPosition)
}

func TestRuntimeAutoFailoverElectsSingleReplicaWithQuorum(t *testing.T) {
	addresses := make([]string, 3)
	for i := range addresses {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[i] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceURL := "http://127.0.0.1:1"
	newReplica := func(index int, uuid string, serverID uint32) *Runtime {
		peers := make([]string, 0, 2)
		for i, address := range addresses {
			if i != index {
				peers = append(peers, "http://"+address)
			}
		}
		runtime, err := NewRuntime(RuntimeConfig{
			Role:           RoleReplica,
			DataDir:        t.TempDir(),
			UUID:           uuid,
			ServerID:       serverID,
			ListenAddr:     addresses[index],
			SourceURL:      sourceURL,
			Peers:          peers,
			AutoFailover:   true,
			FailureTimeout: 150 * time.Millisecond,
			PollInterval:   20 * time.Millisecond,
		})
		require.NoError(t, err)
		return runtime
	}

	first := newReplica(0, "replica-a", 10)
	second := newReplica(1, "replica-b", 20)
	third := newReplica(2, "replica-c", 30)
	require.NoError(t, first.Start(ctx))
	defer first.Close()
	require.NoError(t, second.Start(ctx))
	defer second.Close()
	require.NoError(t, third.Start(ctx))
	defer third.Close()

	require.Eventually(t, func() bool {
		return first.Status().Role == RoleSource && second.Status().Role == RoleReplica && third.Status().Role == RoleReplica
	}, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, RoleSource, first.Status().Role)
	require.Equal(t, RoleReplica, second.Status().Role)
	require.Equal(t, RoleReplica, third.Status().Role)
}

func TestRuntimeAutoFailoverPreservesReplicatedDataWithTwoReplicas(t *testing.T) {
	sourceAddress := reserveRuntimeAddress(t)
	firstAddress := reserveRuntimeAddress(t)
	secondAddress := reserveRuntimeAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceURL := "http://" + sourceAddress
	firstURL := "http://" + firstAddress
	secondURL := "http://" + secondAddress

	source, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "data-topology-source",
		ServerID:   1,
		ListenAddr: sourceAddress,
	})
	require.NoError(t, err)
	_, err = source.source.Append(1, []RowChange{{
		Table:  "app.items",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(1), "value": "before-source-loss"},
	}})
	require.NoError(t, err)

	newReplica := func(dataDir, uuid, listenAddr, peerURL string, serverID uint32) *Runtime {
		runtime, runtimeErr := NewRuntime(RuntimeConfig{
			Role:           RoleReplica,
			DataDir:        dataDir,
			UUID:           uuid,
			ServerID:       serverID,
			ListenAddr:     listenAddr,
			SourceURL:      sourceURL,
			Peers:          []string{sourceURL, peerURL},
			AutoFailover:   true,
			FailureTimeout: 120 * time.Millisecond,
			PollInterval:   20 * time.Millisecond,
		})
		require.NoError(t, runtimeErr)
		return runtime
	}
	first := newReplica(t.TempDir(), "data-topology-first", firstAddress, secondURL, 10)
	second := newReplica(t.TempDir(), "data-topology-second", secondAddress, firstURL, 20)
	require.NoError(t, source.Start(ctx))
	require.NoError(t, first.Start(ctx))
	require.NoError(t, second.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, first.Close())
		require.NoError(t, second.Close())
		require.NoError(t, source.Close())
	})

	require.Eventually(t, func() bool {
		first.replica.mu.Lock()
		firstRows := len(first.replica.AppliedRows)
		first.replica.mu.Unlock()
		second.replica.mu.Lock()
		secondRows := len(second.replica.AppliedRows)
		second.replica.mu.Unlock()
		return firstRows == 1 && secondRows == 1
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, source.Close())
	require.Eventually(t, func() bool {
		return first.Status().Role == RoleSource && second.Status().Role == RoleReplica
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, RoleSource, first.Status().Role)
	require.Equal(t, RoleReplica, second.Status().Role)
	first.replica.mu.Lock()
	firstRows := append([]RowChange(nil), first.replica.AppliedRows...)
	first.replica.mu.Unlock()
	second.replica.mu.Lock()
	secondRows := append([]RowChange(nil), second.replica.AppliedRows...)
	second.replica.mu.Unlock()
	require.Len(t, firstRows, 1)
	require.Len(t, secondRows, 1)
	require.Equal(t, float64(1), firstRows[0].After["id"])
	require.Equal(t, float64(1), secondRows[0].After["id"])
	require.Eventually(t, func() bool {
		return strings.TrimRight(second.Status().SourceURL, "/") == strings.TrimRight(firstURL, "/")
	}, 5*time.Second, 20*time.Millisecond, "the surviving replica must repoint to the promoted source")
	_, err = first.source.Append(1, []RowChange{{
		Table:  "app.items",
		Action: "insert",
		After:  map[string]interface{}{"id": int64(2), "value": "after-repoint"},
	}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		second.replica.mu.Lock()
		defer second.replica.mu.Unlock()
		return len(second.replica.AppliedRows) == 2 && second.replica.AppliedRows[1].After["id"] == float64(2)
	}, 5*time.Second, 20*time.Millisecond, "the repointed replica must apply a transaction from the promoted source")
}

func TestRuntimeNativeSourceRepointUsesPromotedEndpointAndPreservesCredentialsInMemoryOnly(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:           RoleReplica,
		DataDir:        t.TempDir(),
		UUID:           "native-repoint-replica",
		ServerID:       77,
		SourceURL:      "mysql://repl:secret@old-source.example:3306/?gtid_auto_position=true",
		NativeSource:   &scriptedNativeSource{},
		NativeEndpoint: "mysql://replica.example:3307",
	})
	require.NoError(t, err)

	observed := peerStatus{StatusSnapshot: StatusSnapshot{Role: RoleSource, NativeEndpoint: "mysql://promoted.example:3310"}, URL: "http://control-promoted.example:4401"}
	require.NoError(t, runtime.repointSourceDuringPoll(observed))
	require.Equal(t, "mysql://repl@promoted.example:3310?gtid_auto_position=true", runtime.cfg.SourceURL)
	require.Equal(t, "promoted.example", runtime.nativeConfig.Host)
	require.Equal(t, uint16(3310), runtime.nativeConfig.Port)
	require.Equal(t, "repl", runtime.nativeConfig.User)
	require.Equal(t, "secret", runtime.nativeConfig.Password)
	persisted, err := os.ReadFile(runtime.sourcePath)
	require.NoError(t, err)
	require.NotContains(t, string(persisted), "secret")
}

func TestRuntimeNativeSourceRepointPublishesOnlyAfterPersistence(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-repoint-persistence-replica",
		ServerID:     78,
		SourceURL:    "mysql://repl:secret@old-source.example:3306/?gtid_auto_position=true",
		NativeSource: &scriptedNativeSource{},
	})
	require.NoError(t, err)
	oldSourceURL := runtime.cfg.SourceURL
	oldHost := runtime.nativeConfig.Host
	runtime.nativeSourceUUID = "old-source-uuid"
	require.NoError(t, persistReplicationSourceURL(runtime.sourcePath, redactNativeSourcePassword(oldSourceURL)))

	identityDirectory := filepath.Join(runtime.cfg.DataDir, "identity-directory")
	require.NoError(t, os.MkdirAll(identityDirectory, 0755))
	runtime.sourceIdentityPath = identityDirectory

	observed := peerStatus{StatusSnapshot: StatusSnapshot{
		Role:           RoleSource,
		NativeEndpoint: "mysql://promoted.example:3310",
	}}
	require.Error(t, runtime.repointSourceDuringPoll(observed))
	require.Equal(t, oldSourceURL, runtime.cfg.SourceURL,
		"a failed identity persistence must not publish the new endpoint in memory")
	require.Equal(t, oldHost, runtime.nativeConfig.Host)
	require.Equal(t, "old-source-uuid", runtime.nativeSourceUUID)
	persisted, err := os.ReadFile(runtime.sourcePath)
	require.NoError(t, err)
	require.Contains(t, string(persisted), "old-source.example")
	require.NotContains(t, string(persisted), "promoted.example")
}

func TestRuntimeMembersCanBeUpdatedPersistedAndProbed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstAddress := reserveRuntimeAddress(t)
	secondAddress := reserveRuntimeAddress(t)
	first, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "members-first",
		ServerID:   401,
		ListenAddr: firstAddress,
	})
	require.NoError(t, err)
	second, err := NewRuntime(RuntimeConfig{
		Role:       RoleReplica,
		DataDir:    t.TempDir(),
		UUID:       "members-second",
		ServerID:   402,
		ListenAddr: secondAddress,
		SourceURL:  "http://127.0.0.1:1",
	})
	require.NoError(t, err)
	require.NoError(t, first.Start(ctx))
	defer first.Close()
	require.NoError(t, second.Start(ctx))
	defer second.Close()

	peerURL := "http://" + second.Address()
	require.NoError(t, first.UpdatePeers([]string{peerURL, peerURL}))
	members, err := first.Members(ctx)
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.True(t, members[0].Reachable)
	require.True(t, members[1].Reachable)
	require.Equal(t, "members-second", members[1].Status.UUID)

	reloaded, err := NewRuntime(RuntimeConfig{
		Role:     RoleSource,
		DataDir:  first.cfg.DataDir,
		UUID:     "members-first-reloaded",
		ServerID: 403,
	})
	require.NoError(t, err)
	reloaded.mu.RLock()
	require.Equal(t, []string{peerURL}, reloaded.cfg.Peers)
	reloaded.mu.RUnlock()
	require.Error(t, first.UpdatePeers([]string{"not-a-url"}))
}

func TestRuntimeNativeEndpointDiscoveryRepointsFromPeerStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer, err := NewRuntime(RuntimeConfig{
		Role:           RoleSource,
		DataDir:        t.TempDir(),
		UUID:           "native-discovery-source",
		ServerID:       501,
		ListenAddr:     reserveRuntimeAddress(t),
		NativeEndpoint: "mysql://promoted.example:3310",
	})
	require.NoError(t, err)
	require.NoError(t, peer.Start(ctx))
	defer peer.Close()

	replica, err := NewRuntime(RuntimeConfig{
		Role:       RoleReplica,
		DataDir:    t.TempDir(),
		UUID:       "native-discovery-replica",
		ServerID:   502,
		SourceURL:  "mysql://repl:secret@old-source.example:3306/?gtid_auto_position=true",
		NativeSource: &scriptedNativeSource{uuid: "old-source-uuid"},
		Peers:      []string{"http://" + peer.Address()},
	})
	require.NoError(t, err)

	members, err := replica.Members(ctx)
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.Equal(t, "mysql://promoted.example:3310", members[1].Status.NativeEndpoint)
	require.True(t, members[1].Reachable)
	require.Equal(t, "repl", replica.nativeConfig.User)
	require.Equal(t, "secret", replica.nativeConfig.Password)

	require.NoError(t, replica.tryAutoPromote(ctx))
	require.Equal(t, "mysql://repl@promoted.example:3310?gtid_auto_position=true", replica.cfg.SourceURL)
	require.Equal(t, "mysql://promoted.example:3310?gtid_auto_position=true", replica.Status().SourceURL)
	require.Empty(t, replica.nativeSourceUUID, "a discovered endpoint must invalidate the previous source identity")
}

func TestRuntimeRejectsConfiguredSelfPeerBeforeQuorumCalculations(t *testing.T) {
	_, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "self-peer-source",
		ListenAddr: "127.0.0.1:4401",
		Peers:      []string{"http://127.0.0.1:4401/"},
	})
	require.ErrorContains(t, err, "cannot include local replication endpoint")
}

func TestRuntimeRejectsSelfPeerAfterEphemeralListenerStarts(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "ephemeral-self-peer-source",
		ListenAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, runtime.Start(context.Background()))
	defer runtime.Close()

	require.ErrorContains(t, runtime.UpdatePeers([]string{"http://" + runtime.Address()}), "cannot include local replication endpoint")
}

func TestRuntimeRejectsPersistedSelfPeerBeforeQuorumCalculations(t *testing.T) {
	dataDir := t.TempDir()
	membersPath := filepath.Join(dataDir, "replication", "members.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(membersPath), 0755))
	require.NoError(t, os.WriteFile(membersPath, []byte(`["http://127.0.0.1:4402"]`), 0644))

	_, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    dataDir,
		UUID:       "persisted-self-peer-source",
		ListenAddr: "127.0.0.1:4402",
	})
	require.ErrorContains(t, err, "cannot include local replication endpoint")
}

func TestRuntimeUpdatePeersDoesNotPublishWhenPersistenceFails(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:       RoleSource,
		DataDir:    t.TempDir(),
		UUID:       "members-rollback-source",
		ListenAddr: "127.0.0.1:4403",
		Peers:      []string{"http://127.0.0.1:4404"},
	})
	require.NoError(t, err)
	blocker := filepath.Join(t.TempDir(), "members-parent")
	require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0644))
	runtime.membersPath = filepath.Join(blocker, "members.json")

	require.Error(t, runtime.UpdatePeers([]string{"http://127.0.0.1:4405"}))
	runtime.mu.RLock()
	peers := append([]string(nil), runtime.cfg.Peers...)
	runtime.mu.RUnlock()
	require.Equal(t, []string{"http://127.0.0.1:4404"}, peers)
}

func TestRuntimeStartAndStopReplicaControlsPolling(t *testing.T) {
	source, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, source.Start(context.Background()))
	defer source.Close()

	replica, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		SourceURL:    "http://" + source.Address(),
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, replica.Start(context.Background()))
	require.True(t, replica.Status().ReplicaRunning)
	require.NoError(t, replica.StopReplica())
	require.False(t, replica.Status().ReplicaRunning)
	require.NoError(t, replica.StopReplica(), "stopping an already stopped replica is idempotent")
	require.NoError(t, replica.StartReplica())
	require.True(t, replica.Status().ReplicaRunning)
	require.NoError(t, replica.StartReplica(), "starting an already running replica is idempotent")
	require.NoError(t, replica.StopReplica())
	require.False(t, replica.Status().ReplicaRunning)
	require.NoError(t, replica.Close())
}

func TestRuntimeStartStopReplicaRequireStartedReplica(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: t.TempDir(), SourceURL: "http://127.0.0.1:1"})
	require.NoError(t, err)
	require.Error(t, runtime.StartReplica())
	require.Error(t, runtime.StopReplica())
}

func TestRuntimeChangeSourceUpdatesReplicaEndpointAndPersists(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: dataDir, SourceURL: "http://127.0.0.1:1"})
	require.NoError(t, err)
	require.NoError(t, runtime.ChangeSource("http://127.0.0.1:3307"))
	require.Equal(t, "http://127.0.0.1:3307", runtime.cfg.SourceURL)

	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: dataDir})
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:3307", reloaded.cfg.SourceURL)
	require.Error(t, runtime.ChangeSource("127.0.0.1:3307"))
}

func TestRuntimeChangeSourceDoesNotPublishWhenPersistenceFails(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:      RoleReplica,
		DataDir:   t.TempDir(),
		UUID:      "change-source-rollback",
		SourceURL: "http://127.0.0.1:3301",
	})
	require.NoError(t, err)
	blocker := filepath.Join(t.TempDir(), "source-parent")
	require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0644))
	runtime.sourcePath = filepath.Join(blocker, "source.json")

	require.Error(t, runtime.ChangeSource("http://127.0.0.1:3307"))
	require.Equal(t, "http://127.0.0.1:3301", runtime.cfg.SourceURL)
	require.Equal(t, "http://127.0.0.1:3301", runtime.Status().SourceURL)
}

func TestRuntimeResetReplicaRequiresStoppedLoopAndClearsState(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: t.TempDir(), SourceURL: "http://127.0.0.1:1"})
	require.NoError(t, err)
	runtime.replica.mu.Lock()
	runtime.replica.Executed.Add(GTID{UUID: "source", Seq: 1})
	runtime.replica.AppliedRows = []RowChange{{Table: "app.t", Action: "insert"}}
	runtime.replica.LastSourcePosition = 99
	runtime.replica.mu.Unlock()
	require.Error(t, runtime.ResetReplica())

	// A runtime must be started before its replica control state can be
	// changed, but no poll loop is needed for this reset-only check.
	require.NoError(t, runtime.Start(context.Background()))
	require.NoError(t, runtime.StopReplica())
	require.NoError(t, runtime.ResetReplica())
	runtime.replica.mu.Lock()
	require.Empty(t, runtime.replica.Executed)
	require.Empty(t, runtime.replica.AppliedRows)
	require.Zero(t, runtime.replica.LastSourcePosition)
	runtime.replica.mu.Unlock()
	require.NoError(t, runtime.Close())
}

func TestRuntimeResetReplicaAllClearsPersistedSource(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: dataDir, SourceURL: "http://127.0.0.1:1"})
	require.NoError(t, err)
	require.NoError(t, persistSourceIdentity(runtime.sourceIdentityPath, "old-upstream-uuid"))
	require.NoError(t, runtime.Start(context.Background()))
	require.NoError(t, runtime.StopReplica())
	require.NoError(t, runtime.ResetReplicaAll())
	require.Empty(t, runtime.cfg.SourceURL)
	persisted, err := loadPersistedSource(runtime.sourcePath)
	require.NoError(t, err)
	require.Empty(t, persisted)
	identity, err := loadPersistedSourceIdentity(runtime.sourceIdentityPath)
	require.NoError(t, err)
	require.Empty(t, identity, "RESET REPLICA ALL must not retain the old native source identity")
	require.NoError(t, runtime.Close())
}

func TestRuntimeChangeSourceAcceptsNativeMySQLEndpoint(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-change-source",
		ServerID:     902,
		SourceURL:    "mysql://repl@127.0.0.1:3306?binlog_file=binlog.000001&binlog_pos=4",
		PollInterval: 5 * time.Millisecond,
	})
	require.NoError(t, err)

	err = runtime.ChangeSource("mysql://repl@127.0.0.1:3307?binlog_file=binlog.000002&binlog_pos=4")
	require.NoError(t, err)
	require.Equal(t, "mysql://127.0.0.1:3307?binlog_file=binlog.000002&binlog_pos=4", runtime.Status().SourceURL)
	require.Equal(t, "127.0.0.1", runtime.nativeConfig.Host)
	require.Equal(t, uint16(3307), runtime.nativeConfig.Port)
	require.Equal(t, "binlog.000002", runtime.nativeConfig.BinlogFile)
	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: runtime.cfg.DataDir, ServerID: 902})
	require.NoError(t, err)
	require.Equal(t, "mysql://repl@127.0.0.1:3307?binlog_file=binlog.000002&binlog_pos=4", reloaded.cfg.SourceURL)
}

func TestRuntimeChangeSourceKeepsNativePasswordOutOfPersistedSource(t *testing.T) {
	password := t.Name()
	t.Setenv("XMYSQL_REPLICATION_PASSWORD", password)
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "native-password-change-source",
		ServerID:     903,
		SourceURL:    "mysql://repl@127.0.0.1:3306?binlog_file=binlog.000001&binlog_pos=4",
		PollInterval: 5 * time.Millisecond,
	})
	require.NoError(t, err)

	withPassword := (&url.URL{
		Scheme:   "mysql",
		Host:     "127.0.0.1:3307",
		User:     url.UserPassword("repl", password),
		RawQuery: "binlog_file=binlog.000002&binlog_pos=4",
	}).String()
	require.NoError(t, runtime.ChangeSource(withPassword))
	require.Equal(t, password, runtime.nativeConfig.Password)
	persisted, err := os.ReadFile(runtime.sourcePath)
	require.NoError(t, err)
	require.NotContains(t, string(persisted), password)
	require.NotContains(t, runtime.Status().SourceURL, password)

	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleReplica, DataDir: runtime.cfg.DataDir, ServerID: 903})
	require.NoError(t, err)
	require.Equal(t, password, reloaded.nativeConfig.Password)
}

func TestSourceRotateAndResetMasterManageDurableBinlog(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), UUID: "source-admin", ServerID: 7})
	require.NoError(t, err)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	before := runtime.source.NextPosition()
	require.NoError(t, runtime.FlushBinaryLogs())
	require.Greater(t, runtime.source.NextPosition(), before)
	require.NoError(t, runtime.ResetMaster())
	require.Equal(t, uint64(4), runtime.source.NextPosition())
	require.Empty(t, runtime.source.Executed)
	events, err := runtime.source.Dump(4)
	require.NoError(t, err)
	require.Empty(t, events)
	require.NoError(t, runtime.Close())

	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: runtime.cfg.DataDir, UUID: "source-admin", ServerID: 7})
	require.NoError(t, err)
	require.Equal(t, uint64(4), reloaded.source.NextPosition())
	require.Empty(t, reloaded.source.Executed)
}

func TestResetMasterToStartsNativeBinlogAtRequestedIndex(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "source-reset-to", ServerID: 7})
	require.NoError(t, err)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	require.NoError(t, runtime.ResetMasterTo(1234))
	require.Empty(t, runtime.source.Executed)
	require.Equal(t, uint64(4), runtime.source.NextPosition())
	require.Equal(t, []NativeBinlogFile{{Name: "binlog.001234", Size: runtime.source.NativeFiles()[0].Size}}, runtime.source.NativeFiles())
	require.NoError(t, runtime.Close())

	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "source-reset-to", ServerID: 7})
	require.NoError(t, err)
	defer reloaded.Close()
	require.Empty(t, reloaded.source.Executed)
	require.Equal(t, []NativeBinlogFile{{Name: "binlog.001234", Size: reloaded.source.NativeFiles()[0].Size}}, reloaded.source.NativeFiles())
	file, position := reloaded.source.NativeCurrentFilePosition()
	require.Equal(t, "binlog.001234", file)
	require.Equal(t, reloaded.source.NativeFiles()[0].Size, position)
}

func TestRuntimePurgeBinaryLogsToRemovesOnlyOlderNativeFiles(t *testing.T) {
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: t.TempDir(), UUID: "source-purge", ServerID: 7})
	require.NoError(t, err)
	defer runtime.Close()

	first, err := runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	firstGTID := first[0].GTID
	require.NoError(t, runtime.FlushBinaryLogs())
	second, err := runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (2)"}})
	require.NoError(t, err)
	secondGTID := second[0].GTID
	require.NoError(t, runtime.FlushBinaryLogs())
	third, err := runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (3)"}})
	require.NoError(t, err)
	thirdGTID := third[0].GTID

	require.NoError(t, runtime.PurgeBinaryLogsTo("binlog.000003"))
	files := runtime.source.NativeFiles()
	require.Equal(t, []NativeBinlogFile{{Name: "binlog.000003", Size: files[0].Size}}, files)
	_, ok := runtime.source.NativeGTIDPosition(firstGTID.UUID, firstGTID.Seq)
	require.False(t, ok, "purged GTID must not remain in the physical position index")
	_, ok = runtime.source.NativeGTIDPosition(secondGTID.UUID, secondGTID.Seq)
	require.False(t, ok, "purged GTID must not remain in the physical position index")
	_, ok = runtime.source.NativeGTIDPosition(thirdGTID.UUID, thirdGTID.Seq)
	require.True(t, ok, "retained GTID must remain addressable")

	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: runtime.cfg.DataDir, UUID: "source-purge", ServerID: 7})
	require.NoError(t, err)
	defer reloaded.Close()
	require.Equal(t, []NativeBinlogFile{{Name: "binlog.000003", Size: reloaded.source.NativeFiles()[0].Size}}, reloaded.source.NativeFiles())
	_, ok = reloaded.source.NativeGTIDPosition(thirdGTID.UUID, thirdGTID.Seq)
	require.True(t, ok, "retained GTID must survive reload")
}

func TestRuntimePurgeBinaryLogsBeforeUsesEventTimeAndSurvivesRebuild(t *testing.T) {
	dataDir := t.TempDir()
	previousNow := timeNow
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	timeNow = func() time.Time { return now }
	t.Cleanup(func() { timeNow = previousNow })

	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "source-purge-before", ServerID: 7})
	require.NoError(t, err)

	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (1)"}})
	require.NoError(t, err)
	require.NoError(t, runtime.FlushBinaryLogs())
	now = time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (2)"}})
	require.NoError(t, err)
	require.NoError(t, runtime.FlushBinaryLogs())
	now = time.Date(2026, time.January, 3, 12, 0, 0, 0, time.UTC)
	_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: "insert into t values (3)"}})
	require.NoError(t, err)

	cutoff := time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC)
	require.NoError(t, runtime.PurgeBinaryLogsBefore(cutoff))
	files := runtime.source.NativeFiles()
	require.Len(t, files, 1)
	require.Equal(t, "binlog.000003", files[0].Name)
	require.NoError(t, runtime.Close())

	retainedPath := filepath.Join(dataDir, "replication", "binlog.000003")
	require.NoError(t, os.WriteFile(retainedPath, []byte{0xfe, 'b', 'i', 'n'}, 0644))
	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "source-purge-before", ServerID: 7})
	require.NoError(t, err)
	defer reloaded.Close()
	reloadedFiles := reloaded.source.NativeFiles()
	require.Len(t, reloadedFiles, 1, "time purge must remain durable across native rebuild")
	require.Equal(t, "binlog.000003", reloadedFiles[0].Name)
}

func TestPurgeBinaryLogsRetentionSurvivesNativeRebuildAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	runtime, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "source-purge-rebuild", ServerID: 7})
	require.NoError(t, err)
	for index := 1; index <= 3; index++ {
		_, err = runtime.source.AppendCommitted([]Statement{{Database: "app", SQL: fmt.Sprintf("insert into t values (%d)", index)}})
		require.NoError(t, err)
		if index < 3 {
			require.NoError(t, runtime.FlushBinaryLogs())
		}
	}
	require.NoError(t, runtime.PurgeBinaryLogsTo("binlog.000003"))
	require.NoError(t, runtime.Close())

	// Leave a valid native magic header but remove the rest of the retained
	// file, forcing NewBinlogWriter through its logical-stream rebuild path.
	retainedPath := filepath.Join(dataDir, "replication", "binlog.000003")
	require.NoError(t, os.WriteFile(retainedPath, []byte{0xfe, 'b', 'i', 'n'}, 0644))
	reloaded, err := NewRuntime(RuntimeConfig{Role: RoleSource, DataDir: dataDir, UUID: "source-purge-rebuild", ServerID: 7})
	require.NoError(t, err)
	defer reloaded.Close()
	files := reloaded.source.NativeFiles()
	require.Len(t, files, 1, "rebuild must not resurrect purged native files")
	require.Equal(t, "binlog.000003", files[0].Name)
}

func reserveRuntimeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
