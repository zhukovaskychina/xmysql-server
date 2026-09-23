package replication

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errApplyFailed = errors.New("apply failed")

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
	require.NoError(t, runtime.Start(context.Background()))
	require.NoError(t, runtime.StopReplica())
	require.NoError(t, runtime.ResetReplicaAll())
	require.Empty(t, runtime.cfg.SourceURL)
	persisted, err := loadPersistedSource(runtime.sourcePath)
	require.NoError(t, err)
	require.Empty(t, persisted)
	require.NoError(t, runtime.Close())
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

func reserveRuntimeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
