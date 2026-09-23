package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	RoleStandalone = "standalone"
	RoleSource     = "source"
	RoleReplica    = "replica"
)

type RuntimeConfig struct {
	Role       string
	DataDir    string
	UUID       string
	ServerID   uint32
	ListenAddr string
	SourceURL  string
	// Peers contains the control-plane URLs of the other members in the
	// replication group. It is only used by the guarded auto-failover path.
	Peers          []string
	AutoFailover   bool
	FailureTimeout time.Duration
	PollInterval   time.Duration
	ApplyRows      func([]RowChange) error
	Apply          func([]Statement) error
}

type Runtime struct {
	mu                sync.RWMutex
	promotionMu       sync.Mutex
	cfg               RuntimeConfig
	role              string
	source            *Source
	replica           *Replica
	server            *http.Server
	listener          net.Listener
	cancel            context.CancelFunc
	baseCtx           context.Context
	pollCancel        context.CancelFunc
	pollDone          chan struct{}
	lastErr           string
	lastSourceFailure time.Time
	membersPath       string
	sourcePath        string
	fencingPath       string
	fencingEpoch      uint64
	fenced            bool
	fencedBy          string
}

type persistedSourceConfig struct {
	SourceURL string `json:"source_url"`
}

type persistedFencingState struct {
	Epoch  uint64 `json:"epoch"`
	Fenced bool   `json:"fenced"`
	By     string `json:"fenced_by,omitempty"`
}

type fenceRequest struct {
	Epoch         uint64 `json:"epoch"`
	CandidateUUID string `json:"candidate_uuid"`
}

type binlogResponse struct {
	Events       []BinlogEvent `json:"events"`
	NextPosition uint64        `json:"next_position"`
}

// StatusSnapshot is the stable operational view exposed through the HTTP and
// SQL replication status surfaces.
type StatusSnapshot struct {
	Role                string  `json:"role"`
	UUID                string  `json:"uuid,omitempty"`
	ServerID            uint32  `json:"server_id,omitempty"`
	ExecutedGTIDs       string  `json:"executed_gtids,omitempty"`
	SourceURL           string  `json:"source_url,omitempty"`
	SourcePosition      uint64  `json:"source_position,omitempty"`
	ReplicationLagSec   float64 `json:"replication_lag_seconds"`
	LastError           string  `json:"last_error,omitempty"`
	Promotable          bool    `json:"promotable"`
	FencingEpoch        uint64  `json:"fencing_epoch,omitempty"`
	Fenced              bool    `json:"fenced"`
	FencedBy            string  `json:"fenced_by,omitempty"`
	ReplicaRunning      bool    `json:"replica_running"`
	ReplicaRunningKnown bool    `json:"-"`
}

type statusResponse = StatusSnapshot

// MemberSnapshot is the control-plane view of one configured cluster member.
// The local member has an empty URL; peer entries retain the URL used for the
// health probe so operators can map a failure back to its configuration.
type MemberSnapshot struct {
	URL       string         `json:"url,omitempty"`
	Status    StatusSnapshot `json:"status"`
	Reachable bool           `json:"reachable"`
	Error     string         `json:"error,omitempty"`
}

type membersResponse struct {
	Members []MemberSnapshot `json:"members"`
}

type updateMembersRequest struct {
	Peers []string `json:"peers"`
}

func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	cfg.Role = strings.ToLower(strings.TrimSpace(cfg.Role))
	if cfg.Role == "" {
		cfg.Role = RoleStandalone
	}
	if cfg.Role != RoleStandalone && cfg.Role != RoleSource && cfg.Role != RoleReplica {
		return nil, fmt.Errorf("unsupported replication role %q", cfg.Role)
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.FailureTimeout <= 0 {
		cfg.FailureTimeout = 5 * time.Second
	}
	if cfg.UUID == "" {
		cfg.UUID = fmt.Sprintf("xmysql-%d", time.Now().UnixNano())
	}
	if cfg.ServerID == 0 {
		cfg.ServerID = 1
	}
	peers, err := normalizePeers(cfg.Peers)
	if err != nil {
		return nil, err
	}
	cfg.Peers = peers
	r := &Runtime{
		cfg:         cfg,
		role:        cfg.Role,
		membersPath: filepath.Join(cfg.DataDir, "replication", "members.json"),
		sourcePath:  filepath.Join(cfg.DataDir, "replication", "source.json"),
		fencingPath: filepath.Join(cfg.DataDir, "replication", "fencing.json"),
	}
	if fencing, err := loadPersistedFencing(r.fencingPath); err != nil {
		return nil, err
	} else {
		r.fencingEpoch = fencing.Epoch
		r.fenced = fencing.Fenced
		r.fencedBy = fencing.By
	}
	if persistedPeers, err := loadPersistedPeers(r.membersPath); err != nil {
		return nil, err
	} else if len(cfg.Peers) == 0 && len(persistedPeers) > 0 {
		r.cfg.Peers = persistedPeers
	}
	if cfg.Role == RoleReplica && strings.TrimSpace(cfg.SourceURL) == "" {
		if persistedSource, err := loadPersistedSource(r.sourcePath); err != nil {
			return nil, err
		} else if persistedSource != "" {
			r.cfg.SourceURL = persistedSource
		}
	}
	if cfg.Role == RoleSource {
		source, err := NewSource(cfg.DataDir, cfg.UUID, cfg.ServerID)
		if err != nil {
			return nil, err
		}
		r.source = source
	}
	if cfg.Role == RoleReplica {
		replica, err := NewReplica(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		replica.ApplyStatements = cfg.Apply
		replica.ApplyRows = cfg.ApplyRows
		r.replica = replica
		if _, err := url.ParseRequestURI(r.cfg.SourceURL); err != nil || strings.TrimSpace(r.cfg.SourceURL) == "" {
			return nil, fmt.Errorf("replica source_url must be a valid URL: %q", r.cfg.SourceURL)
		}
	}
	return r, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	if r == nil || r.role == RoleStandalone {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.baseCtx = ctx
	r.mu.Unlock()
	if r.cfg.ListenAddr != "" {
		listener, err := net.Listen("tcp", r.cfg.ListenAddr)
		if err != nil {
			return err
		}
		r.listener = listener
		r.server = &http.Server{Handler: r.handler()}
		go func() {
			if err := r.server.Serve(listener); err != nil && err != http.ErrServerClosed {
				r.setError(err)
			}
		}()
	}
	if r.role == RoleReplica {
		if err := r.StartReplica(); err != nil {
			return err
		}
	}
	return nil
}

// StartReplica starts the replica apply loop for an already started runtime.
// Repeated calls are idempotent, matching START REPLICA's operational use in
// connection pools and orchestration retries.
func (r *Runtime) StartReplica() error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != RoleReplica || r.replica == nil {
		return fmt.Errorf("only a replica runtime can start replication")
	}
	if r.baseCtx == nil || r.cancel == nil {
		return fmt.Errorf("replication runtime is not started")
	}
	if r.pollCancel != nil {
		return nil
	}
	pollCtx, pollCancel := context.WithCancel(r.baseCtx)
	done := make(chan struct{})
	r.pollCancel = pollCancel
	r.pollDone = done
	go r.poll(pollCtx, done)
	return nil
}

// StopReplica stops only the replica apply loop. The control-plane HTTP
// server remains available so operators can inspect status or start it again.
// Repeated calls are idempotent.
func (r *Runtime) StopReplica() error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	if r.role != RoleReplica || r.replica == nil {
		r.mu.RUnlock()
		return fmt.Errorf("only a replica runtime can stop replication")
	}
	if r.baseCtx == nil || r.cancel == nil {
		r.mu.RUnlock()
		return fmt.Errorf("replication runtime is not started")
	}
	cancel := r.pollCancel
	done := r.pollDone
	r.mu.RUnlock()
	if cancel == nil || done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-time.After(2 * time.Second):
		return fmt.Errorf("replication poll did not stop before STOP REPLICA timeout")
	}
}

// ChangeSource updates the HTTP source endpoint used by a replica and makes
// the setting survive a runtime restart. Credentials are deliberately not
// accepted here; the internal replication control plane uses the existing
// authenticated deployment boundary rather than persisting secrets.
func (r *Runtime) ChangeSource(sourceURL string) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	sourceURL = strings.TrimRight(strings.TrimSpace(sourceURL), "/")
	parsed, err := url.ParseRequestURI(sourceURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("source must be an absolute http URL: %q", sourceURL)
	}
	r.mu.Lock()
	if r.role != RoleReplica || r.replica == nil {
		r.mu.Unlock()
		return fmt.Errorf("only a replica runtime can change source")
	}
	r.cfg.SourceURL = sourceURL
	r.lastSourceFailure = time.Time{}
	r.lastErr = ""
	path := r.sourcePath
	r.mu.Unlock()
	if path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(persistedSourceConfig{SourceURL: sourceURL}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

// ResetReplica removes the local replica execution state. It is intentionally
// refused while the apply loop is active so a concurrent poll cannot commit a
// transaction into a state that is being reset.
func (r *Runtime) ResetReplica() error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	if r.role != RoleReplica || r.replica == nil {
		r.mu.RUnlock()
		return fmt.Errorf("only a replica runtime can reset replication")
	}
	if r.baseCtx == nil || r.cancel == nil {
		r.mu.RUnlock()
		return fmt.Errorf("replication runtime is not started")
	}
	if r.pollCancel != nil {
		r.mu.RUnlock()
		return fmt.Errorf("stop replica before resetting replication")
	}
	replica := r.replica
	r.mu.RUnlock()

	replica.mu.Lock()
	defer replica.mu.Unlock()
	next := replicaState{Executed: GTIDSet{}, AppliedRows: []RowChange{}, PreparedXA: map[string]BinlogEvent{}, RelayEvents: []BinlogEvent{}}
	raw, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(replica.statePath), 0755); err != nil {
		return err
	}
	if err := writeReplicationFileAtomic(replica.statePath, raw); err != nil {
		return err
	}
	replica.Executed = next.Executed
	replica.AppliedRows = next.AppliedRows
	replica.relayEvents = next.RelayEvents
	replica.preparedXA = next.PreparedXA
	replica.LastSourcePosition = 0
	replica.LastAppliedAt = time.Time{}
	replica.LastError = ""
	return nil
}

// ResetReplicaAll clears both local replica execution state and the persisted
// source connection. A subsequent CHANGE REPLICATION SOURCE/CHANGE MASTER is
// required before the replica can be started again.
func (r *Runtime) ResetReplicaAll() error {
	if err := r.ResetReplica(); err != nil {
		return err
	}
	r.mu.Lock()
	r.cfg.SourceURL = ""
	path := r.sourcePath
	r.mu.Unlock()
	if path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(persistedSourceConfig{}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	cancel := r.cancel
	server := r.server
	done := r.pollDone
	r.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
	var shutdownErr error
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		shutdownErr = server.Shutdown(ctx)
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			if shutdownErr == nil {
				shutdownErr = fmt.Errorf("replication poll did not stop before runtime close timeout")
			}
		}
	}
	return shutdownErr
}

func (r *Runtime) AppendCommitted(statements []Statement) error {
	return r.AppendCommittedTransaction(nil, statements)
}

// AppendCommittedTransaction appends row images and statements as one
// committed GTID. This keeps native row-event consumers and statement-based
// replicas on the same transaction boundary.
func (r *Runtime) AppendCommittedTransaction(changes []RowChange, statements []Statement) error {
	return r.AppendCommittedTransactionWithKey("", changes, statements)
}

// AppendCommittedTransactionWithKey preserves the transaction identity used
// by XA coordinators across a post-append retry.
func (r *Runtime) AppendCommittedTransactionWithKey(key string, changes []RowChange, statements []Statement) error {
	r.mu.RLock()
	source := r.source
	role := r.role
	fenced := r.fenced
	r.mu.RUnlock()
	if role != RoleSource || source == nil {
		return fmt.Errorf("replication runtime is not a source")
	}
	if fenced {
		return fmt.Errorf("replication source is fenced")
	}
	if len(statements) == 0 && len(changes) == 0 {
		return nil
	}
	_, err := source.AppendCommittedTransactionWithKey(key, changes, statements)
	return err
}

// FlushBinaryLogs persists a rotate marker on a source. The logical runtime
// keeps one durable JSONL stream, so rotation is represented as an event
// boundary that native and internal consumers can observe.
func (r *Runtime) FlushBinaryLogs() error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	source := r.source
	role := r.role
	fenced := r.fenced
	r.mu.RUnlock()
	if role != RoleSource || source == nil {
		return fmt.Errorf("replication runtime is not a source")
	}
	if fenced {
		return fmt.Errorf("replication source is fenced")
	}
	_, err := source.Rotate()
	return err
}

// ResetMaster clears a source's durable logical binlog and GTID history.
func (r *Runtime) ResetMaster() error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	source := r.source
	role := r.role
	fenced := r.fenced
	r.mu.RUnlock()
	if role != RoleSource || source == nil {
		return fmt.Errorf("replication runtime is not a source")
	}
	if fenced {
		return fmt.Errorf("replication source is fenced")
	}
	return source.ResetMaster()
}

// Address returns the bound control-plane address. It is useful for tests and
// orchestration when ListenAddr uses port 0.
func (r *Runtime) Address() string {
	if r == nil || r.listener == nil {
		return ""
	}
	return r.listener.Addr().String()
}

func (r *Runtime) IsReplica() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role == RoleReplica
}

// IsFenced reports whether this member has been explicitly fenced by a
// higher-epoch promotion decision. A fenced source must reject client writes
// as well as hiding its replication stream; otherwise a split-brain source
// could continue committing data after it has lost ownership.
func (r *Runtime) IsFenced() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fenced
}

// Source returns the local source stream for protocol adapters. It is nil for
// standalone and replica runtimes.
func (r *Runtime) Source() *Source {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.role != RoleSource {
		return nil
	}
	if r.fenced {
		return nil
	}
	return r.source
}

// Replica returns the local replica state for administrative SQL adapters.
// It is nil for standalone and source runtimes.
func (r *Runtime) Replica() *Replica {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.replica
}

func (r *Runtime) Promote() error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.promotionMu.Lock()
	defer r.promotionMu.Unlock()
	r.mu.RLock()
	role := r.role
	fenced := r.fenced
	r.mu.RUnlock()
	if role == RoleSource {
		return nil
	}
	if role != RoleReplica {
		return fmt.Errorf("only a replica can be promoted")
	}
	if fenced {
		return fmt.Errorf("fenced replica cannot be promoted")
	}
	if err := r.acquirePromotionFence(context.Background()); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role == RoleSource {
		return nil
	}
	if r.role != RoleReplica {
		return fmt.Errorf("only a replica can be promoted")
	}
	if r.fenced {
		return fmt.Errorf("fenced replica cannot be promoted")
	}
	if r.cancel != nil {
		if r.pollCancel != nil {
			r.pollCancel()
		}
	}
	source, err := NewSource(r.cfg.DataDir, r.cfg.UUID, r.cfg.ServerID)
	if err != nil {
		return err
	}
	// Promotion must preserve the replica's executed GTID set. Otherwise the
	// new source reports an empty history after failover and can allocate a
	// position that no longer reflects transactions already applied locally.
	if r.replica != nil {
		r.replica.mu.Lock()
		executed := cloneGTIDSet(r.replica.Executed)
		r.replica.mu.Unlock()
		source.Executed = executed
		source.nextSeq = nextSequence(executed, source.UUID)
		if err := source.persistSet(executed); err != nil {
			return err
		}
	}
	r.source = source
	r.role = RoleSource
	r.cfg.SourceURL = ""
	r.fenced = false
	r.fencedBy = ""
	return persistFencing(r.fencingPath, persistedFencingState{Epoch: r.fencingEpoch})
}

func (r *Runtime) acquirePromotionFence(ctx context.Context) error {
	r.mu.RLock()
	peers := append([]string(nil), r.cfg.Peers...)
	localEpoch := r.fencingEpoch
	localUUID := r.cfg.UUID
	r.mu.RUnlock()
	if len(peers) == 0 {
		if localEpoch == ^uint64(0) {
			return fmt.Errorf("fencing epoch exhausted")
		}
		localEpoch++
		r.mu.Lock()
		r.fencingEpoch = localEpoch
		r.fenced = false
		r.fencedBy = ""
		path := r.fencingPath
		r.mu.Unlock()
		return persistFencing(path, persistedFencingState{Epoch: localEpoch})
	}

	client := &http.Client{Timeout: 750 * time.Millisecond}
	maxEpoch := localEpoch
	for _, peer := range peers {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer, "/")+"/replication/status", nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var status StatusSnapshot
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&status)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || decodeErr != nil {
			continue
		}
		if status.FencingEpoch > maxEpoch {
			maxEpoch = status.FencingEpoch
		}
	}
	if maxEpoch == ^uint64(0) {
		return fmt.Errorf("fencing epoch exhausted")
	}
	epoch := maxEpoch + 1
	payload, err := json.Marshal(fenceRequest{Epoch: epoch, CandidateUUID: localUUID})
	if err != nil {
		return err
	}
	acknowledged := 0
	for _, peer := range peers {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peer, "/")+"/replication/fence", bytes.NewReader(payload))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				acknowledged++
			}
			_ = resp.Body.Close()
		}
	}
	quorum := (len(peers)+1)/2 + 1
	if 1+acknowledged < quorum {
		return fmt.Errorf("fencing quorum not reached: acknowledged %d of %d peers", acknowledged, len(peers))
	}
	r.mu.Lock()
	r.fencingEpoch = epoch
	r.fenced = false
	r.fencedBy = ""
	path := r.fencingPath
	r.mu.Unlock()
	return persistFencing(path, persistedFencingState{Epoch: epoch})
}

func (r *Runtime) applyFence(request fenceRequest) error {
	request.CandidateUUID = strings.TrimSpace(request.CandidateUUID)
	if request.Epoch == 0 || request.CandidateUUID == "" {
		return fmt.Errorf("fence request requires epoch and candidate_uuid")
	}
	r.mu.Lock()
	if request.Epoch < r.fencingEpoch {
		r.mu.Unlock()
		return fmt.Errorf("stale fencing epoch")
	}
	if request.Epoch == r.fencingEpoch && r.fenced {
		if request.CandidateUUID >= r.fencedBy {
			r.mu.Unlock()
			return fmt.Errorf("fencing epoch already owned by %s", r.fencedBy)
		}
	}
	if request.Epoch == r.fencingEpoch && !r.fenced {
		if request.CandidateUUID == r.cfg.UUID {
			r.mu.Unlock()
			return nil
		}
		if request.CandidateUUID >= r.cfg.UUID {
			r.mu.Unlock()
			return fmt.Errorf("fencing epoch already owned by %s", r.cfg.UUID)
		}
	}
	r.fencingEpoch = request.Epoch
	r.fenced = true
	r.fencedBy = request.CandidateUUID
	path := r.fencingPath
	r.mu.Unlock()
	return persistFencing(path, persistedFencingState{Epoch: request.Epoch, Fenced: true, By: request.CandidateUUID})
}

func nextSequence(set GTIDSet, uuid string) uint64 {
	sequences := set[uuid]
	max := uint64(0)
	for sequence := range sequences {
		if sequence > max {
			max = sequence
		}
	}
	return max + 1
}

func (r *Runtime) Status() StatusSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status := statusResponse{
		Role:         r.role,
		UUID:         r.cfg.UUID,
		ServerID:     r.cfg.ServerID,
		SourceURL:    r.cfg.SourceURL,
		LastError:    r.lastErr,
		Promotable:   r.role == RoleReplica && !r.fenced,
		FencingEpoch: r.fencingEpoch,
		Fenced:       r.fenced,
		FencedBy:     r.fencedBy,
	}
	if r.role == RoleSource && r.source != nil {
		status.ExecutedGTIDs = r.source.Executed.String()
	}
	if r.role == RoleReplica && r.replica != nil {
		status.ReplicaRunning = r.pollCancel != nil
		status.ReplicaRunningKnown = true
		r.replica.mu.Lock()
		status.ExecutedGTIDs = r.replica.Executed.String()
		status.SourcePosition = r.replica.LastSourcePosition
		if !r.replica.LastAppliedAt.IsZero() {
			status.ReplicationLagSec = time.Since(r.replica.LastAppliedAt).Seconds()
			if status.ReplicationLagSec < 0 {
				status.ReplicationLagSec = 0
			}
		}
		if r.replica.LastError != "" {
			status.LastError = r.replica.LastError
		}
		r.replica.mu.Unlock()
	}
	return status
}

func (r *Runtime) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/replication/binlog", r.handleBinlog)
	mux.HandleFunc("/replication/status", r.handleStatus)
	mux.HandleFunc("/replication/members", r.handleMembers)
	mux.HandleFunc("/replication/fence", r.handleFence)
	mux.HandleFunc("/replication/promote", r.handlePromote)
	return mux
}

func (r *Runtime) handleBinlog(w http.ResponseWriter, req *http.Request) {
	source := r.Source()
	if source == nil {
		http.Error(w, "not a source", http.StatusServiceUnavailable)
		return
	}
	position := uint64(4)
	if rawPosition := strings.TrimSpace(req.URL.Query().Get("position")); rawPosition != "" {
		parsed, err := strconv.ParseUint(rawPosition, 10, 64)
		if err != nil {
			http.Error(w, "invalid replication position", http.StatusBadRequest)
			return
		}
		if parsed != 0 {
			position = parsed
		}
	}
	events, next, err := source.DumpWithGTIDPosition(position, req.URL.Query().Get("gtids"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, binlogResponse{Events: events, NextPosition: next})
}

func (r *Runtime) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, r.Status())
}

func (r *Runtime) handleFence(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var payload fenceRequest
	if err := json.NewDecoder(io.LimitReader(req.Body, 64*1024)).Decode(&payload); err != nil {
		http.Error(w, "invalid fence payload", http.StatusBadRequest)
		return
	}
	if err := r.applyFence(payload); err != nil {
		status := http.StatusConflict
		if strings.Contains(err.Error(), "requires") {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, r.Status())
}

func (r *Runtime) handleMembers(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		members, err := r.Members(req.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, membersResponse{Members: members})
	case http.MethodPost:
		var payload updateMembersRequest
		if err := json.NewDecoder(io.LimitReader(req.Body, 64*1024)).Decode(&payload); err != nil {
			http.Error(w, "invalid members payload", http.StatusBadRequest)
			return
		}
		if err := r.UpdatePeers(payload.Peers); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, membersResponse{Members: []MemberSnapshot{{Status: r.Status(), Reachable: true}}})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// UpdatePeers replaces the configured peer set and persists it before the
// next poll/election. The operation is intentionally replace-all so an
// operator can remove a failed member without leaving stale quorum metadata.
func (r *Runtime) UpdatePeers(peers []string) error {
	normalized, err := normalizePeers(peers)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.cfg.Peers = normalized
	path := r.membersPath
	r.mu.Unlock()
	if path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

// Members probes the current peer set and returns the same view used by
// guarded auto-failover. A failed peer remains in the result with Reachable
// false so operators can distinguish membership from health.
func (r *Runtime) Members(ctx context.Context) ([]MemberSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	peers := append([]string(nil), r.cfg.Peers...)
	r.mu.RUnlock()
	members := []MemberSnapshot{{Status: r.Status(), Reachable: true}}
	client := &http.Client{Timeout: 750 * time.Millisecond}
	for _, peer := range peers {
		member := MemberSnapshot{URL: peer}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer, "/")+"/replication/status", nil)
		if err != nil {
			member.Error = err.Error()
			members = append(members, member)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			member.Error = err.Error()
			members = append(members, member)
			continue
		}
		member.Reachable = resp.StatusCode == http.StatusOK
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&member.Status)
		_ = resp.Body.Close()
		if decodeErr != nil {
			member.Reachable = false
			member.Error = decodeErr.Error()
		} else if resp.StatusCode != http.StatusOK {
			member.Error = resp.Status
		}
		members = append(members, member)
	}
	return members, nil
}

func normalizePeers(peers []string) ([]string, error) {
	seen := make(map[string]struct{}, len(peers))
	result := make([]string, 0, len(peers))
	for _, raw := range peers {
		peer := strings.TrimRight(strings.TrimSpace(raw), "/")
		if peer == "" {
			continue
		}
		parsed, err := url.ParseRequestURI(peer)
		if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
			return nil, fmt.Errorf("peer must be an absolute http URL: %q", raw)
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		result = append(result, peer)
	}
	return result, nil
}

func loadPersistedPeers(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var peers []string
	if err := json.Unmarshal(raw, &peers); err != nil {
		return nil, fmt.Errorf("decode persisted replication members: %w", err)
	}
	return normalizePeers(peers)
}

func loadPersistedSource(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var config persistedSourceConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return "", fmt.Errorf("decode persisted replication source: %w", err)
	}
	if strings.TrimSpace(config.SourceURL) == "" {
		return "", nil
	}
	parsed, err := url.ParseRequestURI(strings.TrimSpace(config.SourceURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("persisted replication source is invalid: %q", config.SourceURL)
	}
	return strings.TrimRight(strings.TrimSpace(config.SourceURL), "/"), nil
}

func loadPersistedFencing(path string) (persistedFencingState, error) {
	if path == "" {
		return persistedFencingState{}, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return persistedFencingState{}, nil
	}
	if err != nil {
		return persistedFencingState{}, err
	}
	var state persistedFencingState
	if err := json.Unmarshal(raw, &state); err != nil {
		return persistedFencingState{}, fmt.Errorf("decode persisted fencing state: %w", err)
	}
	if state.Fenced && strings.TrimSpace(state.By) == "" {
		return persistedFencingState{}, fmt.Errorf("persisted fencing state is missing fenced_by")
	}
	return state, nil
}

func persistFencing(path string, state persistedFencingState) error {
	if path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

func (r *Runtime) handlePromote(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := r.Promote(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, r.Status())
}

func (r *Runtime) poll(ctx context.Context, done chan struct{}) {
	defer func() {
		r.mu.Lock()
		if r.pollDone == done {
			r.pollCancel = nil
		}
		r.mu.Unlock()
		close(done)
	}()
	position := uint64(4)
	r.replica.mu.Lock()
	if r.replica.LastSourcePosition >= 4 {
		position = r.replica.LastSourcePosition
	}
	r.replica.mu.Unlock()
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		r.mu.RLock()
		role := r.role
		r.mu.RUnlock()
		if role != RoleReplica {
			return
		}
		if err := r.pullOnce(ctx, client, &position); err != nil {
			r.setError(err)
			if r.shouldAutoFailover() {
				if err := r.tryAutoPromote(ctx); err != nil {
					r.setError(err)
				}
			}
		} else {
			r.clearSourceFailure()
		}
		timer := time.NewTimer(r.cfg.PollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (r *Runtime) shouldAutoFailover() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.cfg.AutoFailover || r.role != RoleReplica {
		return false
	}
	if r.lastSourceFailure.IsZero() {
		r.lastSourceFailure = time.Now().UTC()
		return false
	}
	return time.Since(r.lastSourceFailure) >= r.cfg.FailureTimeout
}

type peerStatus struct {
	StatusSnapshot
	Reachable bool
}

// tryAutoPromote implements a deliberately conservative single-winner
// election. A replica needs a quorum of reachable members, must not observe a
// live source, and only the lexicographically smallest server ID/UUID among
// eligible replicas may promote. This does not claim consensus-level fencing,
// but it prevents the common two-replica race and makes the safety boundary
// explicit until an external lease/fencing service is configured.
func (r *Runtime) tryAutoPromote(ctx context.Context) error {
	r.mu.RLock()
	peers := append([]string(nil), r.cfg.Peers...)
	local := StatusSnapshot{Role: r.role, UUID: r.cfg.UUID, ServerID: r.cfg.ServerID, Promotable: r.role == RoleReplica && !r.fenced, Fenced: r.fenced, FencingEpoch: r.fencingEpoch}
	totalMembers := len(peers) + 1
	r.mu.RUnlock()
	if local.Fenced {
		return nil
	}
	if len(peers) == 0 {
		return fmt.Errorf("automatic failover requires configured peers")
	}

	client := &http.Client{Timeout: 750 * time.Millisecond}
	statuses := []peerStatus{{StatusSnapshot: local, Reachable: true}}
	sourceSeen := false
	for _, peer := range peers {
		peer = strings.TrimRight(strings.TrimSpace(peer), "/")
		if peer == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, peer+"/replication/status", nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var status StatusSnapshot
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&status)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || decodeErr != nil {
			continue
		}
		statuses = append(statuses, peerStatus{StatusSnapshot: status, Reachable: true})
		if status.Role == RoleSource && !status.Fenced {
			sourceSeen = true
		}
	}
	quorum := totalMembers/2 + 1
	if len(statuses) < quorum || sourceSeen {
		return nil
	}

	best := local
	for _, observed := range statuses[1:] {
		if observed.Role != RoleReplica || !observed.Promotable {
			continue
		}
		if failoverCandidateLess(observed.StatusSnapshot, best) {
			best = observed.StatusSnapshot
		}
	}
	if best.UUID != local.UUID || best.ServerID != local.ServerID {
		return nil
	}
	return r.Promote()
}

func failoverCandidateLess(left, right StatusSnapshot) bool {
	if left.ServerID != right.ServerID {
		return left.ServerID < right.ServerID
	}
	return left.UUID < right.UUID
}

func (r *Runtime) clearSourceFailure() {
	r.mu.Lock()
	r.lastSourceFailure = time.Time{}
	r.mu.Unlock()
}

func (r *Runtime) pullOnce(ctx context.Context, client *http.Client, position *uint64) error {
	endpoint := strings.TrimRight(r.cfg.SourceURL, "/") + "/replication/binlog?position=" + strconv.FormatUint(*position, 10)
	r.replica.mu.Lock()
	executed := r.replica.Executed.String()
	r.replica.mu.Unlock()
	if executed != "" {
		endpoint += "&gtids=" + url.QueryEscape(executed)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("source returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload binlogResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	if len(payload.Events) > 0 {
		for _, transaction := range splitBinlogTransactions(payload.Events) {
			if err := r.replica.Apply(transaction); err != nil {
				return err
			}
		}
	}
	if payload.NextPosition > *position {
		*position = payload.NextPosition
		r.replica.mu.Lock()
		r.replica.LastSourcePosition = *position
		if len(payload.Events) == 0 {
			r.replica.LastAppliedAt = time.Time{}
		} else {
			r.replica.LastAppliedAt = time.Now().UTC()
		}
		r.replica.LastError = ""
		r.replica.mu.Unlock()
	} else if len(payload.Events) == 0 {
		r.replica.mu.Lock()
		r.replica.LastAppliedAt = time.Time{}
		r.replica.LastError = ""
		r.replica.mu.Unlock()
	}
	return nil
}

func splitBinlogTransactions(events []BinlogEvent) [][]BinlogEvent {
	transactions := make([][]BinlogEvent, 0)
	var current []BinlogEvent
	currentGTID := ""
	flush := func() {
		if len(current) == 0 {
			return
		}
		transactions = append(transactions, current)
		current = nil
		currentGTID = ""
	}
	for _, event := range events {
		gtid := event.GTID.String()
		if event.Type == EventRotate {
			flush()
			transactions = append(transactions, []BinlogEvent{event})
			continue
		}
		if currentGTID != "" && currentGTID != gtid {
			flush()
		}
		currentGTID = gtid
		current = append(current, event)
	}
	flush()
	return transactions
}

func (r *Runtime) setError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.lastErr = err.Error()
	r.mu.Unlock()
	if r.replica != nil {
		r.replica.mu.Lock()
		r.replica.LastError = err.Error()
		r.replica.mu.Unlock()
	}
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
