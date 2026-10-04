package replication

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
	Role    string
	DataDir string
	// ChannelName is empty for the default replication channel. Named
	// channels are owned by a parent runtime and use isolated state.
	ChannelName string
	UUID        string
	ServerID    uint32
	ListenAddr  string
	SourceURL   string
	// NativeEndpoint is the MySQL protocol endpoint advertised to peers after
	// promotion. It is separate from ListenAddr, the replication HTTP endpoint.
	NativeEndpoint string
	// Peers contains the control-plane URLs of the other members in the
	// replication group. It is only used by the guarded auto-failover path.
	Peers           []string
	AutoFailover    bool
	FailureTimeout  time.Duration
	PollInterval    time.Duration
	ApplyRows       func([]RowChange) error
	ApplyRowsWithID func(string, []RowChange) error
	Apply           func([]Statement) error
	ApplyWithID     func(string, []Statement) error
	// NativeSource is an optional injected native source for tests and
	// embedders. Production callers normally leave it nil and provide a
	// mysql:// SourceURL so the runtime constructs MySQLBinlogSource.
	NativeSource NativeBinlogSource
}

type Runtime struct {
	mu                    sync.RWMutex
	promotionMu           sync.Mutex
	cfg                   RuntimeConfig
	role                  string
	channelName           string
	channels              map[string]*Runtime
	source                *Source
	replica               *Replica
	server                *http.Server
	listener              net.Listener
	cancel                context.CancelFunc
	baseCtx               context.Context
	pollCancel            context.CancelFunc
	pollDone              chan struct{}
	lastErr               string
	lastErrorNumber       int64
	lastErrorAt           time.Time
	lastIOError           string
	lastIOErrorNumber     int64
	lastIOErrorAt         time.Time
	lastSQLError          string
	lastSQLErrorNumber    int64
	lastSQLErrorAt        time.Time
	lastSourceFailure     time.Time
	nativeSource          NativeBinlogSource
	nativeConfig          MySQLBinlogSourceConfig
	nativeSourceUUID      string
	nativeHeartbeatCount  uint64
	nativeLastHeartbeatAt time.Time
	nativeReplayFromFile  bool
	membersPath           string
	channelsPath          string
	sourcePath            string
	sourceIdentityPath    string
	fencingPath           string
	rolePath              string
	fencingEpoch          uint64
	fenced                bool
	fencedBy              string
}

type persistedSourceConfig struct {
	SourceURL string `json:"source_url"`
}

type persistedSourceIdentity struct {
	SourceUUID string `json:"source_uuid,omitempty"`
}

type persistedRuntimeRole struct {
	Role string `json:"role"`
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
	LogName      string        `json:"log_name,omitempty"`
}

// StatusSnapshot is the stable operational view exposed through the HTTP and
// SQL replication status surfaces.
type StatusSnapshot struct {
	ChannelName         string                    `json:"channel_name,omitempty"`
	Role                string                    `json:"role"`
	UUID                string                    `json:"uuid,omitempty"`
	ServerID            uint32                    `json:"server_id,omitempty"`
	ExecutedGTIDs       string                    `json:"executed_gtids,omitempty"`
	SourceURL           string                    `json:"source_url,omitempty"`
	NativeEndpoint      string                    `json:"native_endpoint,omitempty"`
	SourceUser          string                    `json:"-"`
	SourceUUID          string                    `json:"source_uuid,omitempty"`
	SourceFile          string                    `json:"source_file,omitempty"`
	SourcePosition      uint64                    `json:"source_position,omitempty"`
	SourceAutoPosition  bool                      `json:"source_auto_position,omitempty"`
	ReceivedHeartbeats  uint64                    `json:"-"`
	LastHeartbeatAt     time.Time                 `json:"-"`
	ReplicationLagSec   float64                   `json:"replication_lag_seconds"`
	LastError           string                    `json:"last_error,omitempty"`
	LastErrorNumber     int64                     `json:"-"`
	LastErrorAt         time.Time                 `json:"-"`
	LastIOError         string                    `json:"-"`
	LastIOErrorNumber   int64                     `json:"-"`
	LastIOErrorAt       time.Time                 `json:"-"`
	LastSQLError        string                    `json:"-"`
	LastSQLErrorNumber  int64                     `json:"-"`
	LastSQLErrorAt      time.Time                 `json:"-"`
	ReplicationFilters  []ReplicationFilterStatus `json:"-"`
	Promotable          bool                      `json:"promotable"`
	FencingEpoch        uint64                    `json:"fencing_epoch,omitempty"`
	Fenced              bool                      `json:"fenced"`
	FencedBy            string                    `json:"fenced_by,omitempty"`
	ReplicaRunning      bool                      `json:"replica_running"`
	ReplicaRunningKnown bool                      `json:"-"`
}

func validateReplicationChannelName(channel string) error {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return fmt.Errorf("replication channel name cannot be empty")
	}
	if len(channel) > 64 || strings.ContainsAny(channel, `/\\`) || strings.ContainsRune(channel, 0) || channel == "." || channel == ".." {
		return fmt.Errorf("invalid replication channel name %q", channel)
	}
	return nil
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

type persistedReplicationChannels struct {
	Channels []string `json:"channels"`
}

func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	cfg.Role = strings.ToLower(strings.TrimSpace(cfg.Role))
	if cfg.Role == "" {
		cfg.Role = RoleStandalone
	}
	rolePath := filepath.Join(cfg.DataDir, "replication", "role.json")
	if persistedRole, err := loadPersistedRuntimeRole(rolePath); err != nil {
		return nil, err
	} else if persistedRole != "" {
		// A durable promotion is authoritative on restart. This lets a node
		// recover as the promoted source even when its original process
		// configuration still says RoleReplica.
		cfg.Role = persistedRole
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
	if err := validatePeersAgainstListenAddress(peers, cfg.ListenAddr); err != nil {
		return nil, err
	}
	cfg.Peers = peers
	r := &Runtime{
		cfg:                cfg,
		role:               cfg.Role,
		channelName:        strings.TrimSpace(cfg.ChannelName),
		channels:           make(map[string]*Runtime),
		membersPath:        filepath.Join(cfg.DataDir, "replication", "members.json"),
		channelsPath:       filepath.Join(cfg.DataDir, "replication", "channels.json"),
		sourcePath:         filepath.Join(cfg.DataDir, "replication", "source.json"),
		sourceIdentityPath: filepath.Join(cfg.DataDir, "replication", "source_identity.json"),
		rolePath:           rolePath,
		fencingPath:        filepath.Join(cfg.DataDir, "replication", "fencing.json"),
	}
	if r.channelName != "" {
		if err := validateReplicationChannelName(r.channelName); err != nil {
			return nil, err
		}
	}
	if sourceUUID, err := loadPersistedSourceIdentity(r.sourceIdentityPath); err != nil {
		return nil, err
	} else {
		r.nativeSourceUUID = sourceUUID
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
		if err := validatePeersAgainstListenAddress(persistedPeers, cfg.ListenAddr); err != nil {
			return nil, err
		}
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
		replica.ApplyRowsWithID = cfg.ApplyRowsWithID
		replica.ApplyStatementsWithID = cfg.ApplyWithID
		r.replica = replica
		if strings.TrimSpace(r.cfg.SourceURL) == "" && r.channelName == "" {
			return nil, fmt.Errorf("replica source_url must be a valid URL: %q", r.cfg.SourceURL)
		}
		if strings.TrimSpace(r.cfg.SourceURL) == "" && r.channelName != "" {
			// A named channel can be created before CHANGE REPLICATION SOURCE
			// configures it. It remains inert until then.
			return r, nil
		}
		parsed, err := url.Parse(r.cfg.SourceURL)
		if err != nil || parsed.Host == "" {
			return nil, fmt.Errorf("replica source_url must be a valid URL: %q", r.cfg.SourceURL)
		}
		switch strings.ToLower(parsed.Scheme) {
		case "http", "https":
			// The existing JSON binlog source remains unchanged.
		case "mysql":
			nativeSourceURL := nativeSourceURLWithEnvironmentPassword(r.cfg.SourceURL)
			nativeConfig, parseErr := ParseMySQLBinlogSourceURL(nativeSourceURL, cfg.ServerID)
			if parseErr != nil {
				return nil, parseErr
			}
			r.cfg.SourceURL = redactNativeSourcePassword(r.cfg.SourceURL)
			if persistedGTIDs := nativeMySQLGTIDSet(replica.Executed); persistedGTIDs != "" && nativeConfig.GTIDAutoPosition {
				// The durable replica GTID set is authoritative after a restart;
				// it must drive COM_BINLOG_DUMP_GTID instead of restarting from a
				// stale file/position pair.
				nativeConfig.GTIDSet = persistedGTIDs
			}
			if replica.LastSourceFile != "" {
				nativeConfig.BinlogFile = replica.LastSourceFile
				if replica.LastSourcePosition >= 4 {
					nativeConfig.BinlogPosition = replica.LastSourcePosition
				}
			}
			if cfg.NativeSource != nil {
				r.nativeSource = cfg.NativeSource
			} else {
				if strings.TrimSpace(nativeConfig.BinlogFile) == "" && !nativeConfig.GTIDAutoPosition {
					return nil, fmt.Errorf("mysql replica source_url requires binlog_file on first start")
				}
				nativeSource, sourceErr := NewMySQLBinlogSource(nativeConfig)
				if sourceErr != nil {
					return nil, sourceErr
				}
				r.nativeSource = nativeSource
			}
			r.nativeConfig = nativeConfig
		default:
			return nil, fmt.Errorf("replica source_url must use http, https, or mysql scheme: %q", r.cfg.SourceURL)
		}
	}
	if r.channelName == "" {
		persistedChannels, err := loadPersistedReplicationChannels(r.channelsPath)
		if err != nil {
			return nil, err
		}
		for _, channel := range persistedChannels {
			child, childErr := r.newChannelRuntime(channel)
			if childErr != nil {
				return nil, childErr
			}
			r.channels[channel] = child
		}
	}
	return r, nil
}

func (r *Runtime) newChannelRuntime(channel string) (*Runtime, error) {
	if err := validateReplicationChannelName(channel); err != nil {
		return nil, err
	}
	return NewRuntime(RuntimeConfig{
		Role:            RoleReplica,
		DataDir:         filepath.Join(r.cfg.DataDir, "channels", channel),
		ChannelName:     channel,
		UUID:            r.cfg.UUID + "/" + channel,
		ServerID:        r.cfg.ServerID,
		NativeEndpoint:  r.cfg.NativeEndpoint,
		Peers:           append([]string(nil), r.cfg.Peers...),
		AutoFailover:    r.cfg.AutoFailover,
		FailureTimeout:  r.cfg.FailureTimeout,
		PollInterval:    r.cfg.PollInterval,
		ApplyRows:       r.cfg.ApplyRows,
		ApplyRowsWithID: r.cfg.ApplyRowsWithID,
		Apply:           r.cfg.Apply,
		ApplyWithID:     r.cfg.ApplyWithID,
	})
}

func loadPersistedReplicationChannels(path string) ([]string, error) {
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
	var persisted persistedReplicationChannels
	if err := json.Unmarshal(raw, &persisted); err != nil {
		return nil, fmt.Errorf("decode persisted replication channels: %w", err)
	}
	seen := make(map[string]struct{}, len(persisted.Channels))
	channels := make([]string, 0, len(persisted.Channels))
	for _, rawChannel := range persisted.Channels {
		channel := strings.TrimSpace(rawChannel)
		if err := validateReplicationChannelName(channel); err != nil {
			return nil, err
		}
		if _, ok := seen[channel]; ok {
			continue
		}
		seen[channel] = struct{}{}
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels, nil
}

func (r *Runtime) persistChannelNamesLocked(channels []string) error {
	if r.channelsPath == "" {
		return nil
	}
	copyChannels := append([]string(nil), channels...)
	sort.Strings(copyChannels)
	raw, err := json.MarshalIndent(persistedReplicationChannels{Channels: copyChannels}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.channelsPath), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(r.channelsPath, raw)
}

func (r *Runtime) channelRuntime(channel string) (*Runtime, error) {
	if r == nil {
		return nil, fmt.Errorf("replication runtime is nil")
	}
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return r, nil
	}
	if err := validateReplicationChannelName(channel); err != nil {
		return nil, err
	}
	r.mu.RLock()
	child := r.channels[channel]
	r.mu.RUnlock()
	if child != nil {
		return child, nil
	}
	child, err := r.newChannelRuntime(channel)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if existing := r.channels[channel]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	channels := make([]string, 0, len(r.channels)+1)
	for name := range r.channels {
		channels = append(channels, name)
	}
	channels = append(channels, channel)
	if err := r.persistChannelNamesLocked(channels); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	r.channels[channel] = child
	baseCtx := r.baseCtx
	r.mu.Unlock()
	if baseCtx != nil {
		// Registering a channel after the parent has started creates its
		// lifecycle context, but does not start the applier until START
		// REPLICA FOR CHANNEL is issued.
		ctx, cancel := context.WithCancel(baseCtx)
		child.mu.Lock()
		child.cancel = cancel
		child.baseCtx = ctx
		child.mu.Unlock()
	}
	return child, nil
}

func (r *Runtime) StatusForChannel(channel string) (StatusSnapshot, error) {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return StatusSnapshot{}, err
	}
	return child.Status(), nil
}

// ChannelStatuses returns the default channel followed by all configured
// named channels. It is used by Performance Schema projections, where MySQL
// exposes one row per replication channel rather than a single selected
// channel. The returned snapshots are independent copies of runtime state.
func (r *Runtime) ChannelStatuses() []StatusSnapshot {
	if r == nil {
		return nil
	}
	statuses := []StatusSnapshot{r.Status()}
	r.mu.RLock()
	names := make([]string, 0, len(r.channels))
	for name := range r.channels {
		names = append(names, name)
	}
	children := make(map[string]*Runtime, len(names))
	for _, name := range names {
		children[name] = r.channels[name]
	}
	r.mu.RUnlock()
	sort.Strings(names)
	for _, name := range names {
		if child := children[name]; child != nil {
			statuses = append(statuses, child.Status())
		}
	}
	return statuses
}

func (r *Runtime) SourceForChannel(channel string) *Source {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return nil
	}
	return child.Source()
}

func (r *Runtime) ReplicaForChannel(channel string) *Replica {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return nil
	}
	return child.Replica()
}

func (r *Runtime) StartReplicaForChannel(channel string) error {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return err
	}
	if child == r {
		return r.StartReplica()
	}
	child.mu.RLock()
	started := child.baseCtx != nil && child.cancel != nil
	child.mu.RUnlock()
	if !started {
		r.mu.RLock()
		baseCtx := r.baseCtx
		r.mu.RUnlock()
		if baseCtx == nil {
			return fmt.Errorf("replication runtime is not started")
		}
		if err := child.Start(baseCtx); err != nil {
			return err
		}
	}
	return child.StartReplica()
}

func (r *Runtime) StopReplicaForChannel(channel string) error {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return err
	}
	return child.StopReplica()
}

func (r *Runtime) ChangeSourceForChannel(channel, sourceURL string) error {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return err
	}
	return child.ChangeSource(sourceURL)
}

func (r *Runtime) ChangeReplicationFilterForChannel(channel string, config ReplicationFilterConfig) error {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return err
	}
	return child.ChangeReplicationFilter(config)
}

func (r *Runtime) ResetReplicaForChannel(channel string) error {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return err
	}
	return child.ResetReplica()
}

func (r *Runtime) ResetReplicaAllForChannel(channel string) error {
	child, err := r.channelRuntime(channel)
	if err != nil {
		return err
	}
	return child.ResetReplicaAll()
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
		if strings.TrimSpace(r.cfg.SourceURL) != "" {
			if err := r.StartReplica(); err != nil {
				return err
			}
		}
	}
	r.mu.RLock()
	children := make([]*Runtime, 0, len(r.channels))
	for _, child := range r.channels {
		children = append(children, child)
	}
	r.mu.RUnlock()
	for _, child := range children {
		if err := child.Start(ctx); err != nil {
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
	if strings.TrimSpace(r.cfg.SourceURL) == "" {
		return fmt.Errorf("replication source is not configured")
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

// ChangeSource updates the source endpoint used by a replica and makes the
// setting survive a runtime restart. HTTP URLs use the internal JSON source;
// mysql:// URLs use the native COM_BINLOG_DUMP source. The change must be made
// while the replica thread is stopped, matching MySQL's CHANGE REPLICATION
// SOURCE lifecycle.
func (r *Runtime) ChangeSource(sourceURL string) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	sourceURL = strings.TrimRight(strings.TrimSpace(sourceURL), "/")
	parsed, err := url.ParseRequestURI(sourceURL)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("source must be an absolute HTTP or MySQL URL: %q", sourceURL)
	}
	scheme := strings.ToLower(parsed.Scheme)
	var native NativeBinlogSource
	var nativeConfig MySQLBinlogSourceConfig
	if scheme == "mysql" {
		nativeConfig, err = ParseMySQLBinlogSourceURL(sourceURL, r.cfg.ServerID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(nativeConfig.BinlogFile) == "" && !nativeConfig.GTIDAutoPosition {
			return fmt.Errorf("mysql replica source_url requires binlog_file on first start")
		}
		native, err = NewMySQLBinlogSource(nativeConfig)
		if err != nil {
			return err
		}
	} else if scheme != "http" && scheme != "https" {
		return fmt.Errorf("source must be an absolute HTTP or MySQL URL: %q", sourceURL)
	}
	persistedSourceURL := redactNativeSourcePassword(sourceURL)
	raw, err := json.MarshalIndent(persistedSourceConfig{SourceURL: persistedSourceURL}, "", "  ")
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != RoleReplica || r.replica == nil {
		return fmt.Errorf("only a replica runtime can change source")
	}
	if r.pollCancel != nil {
		return fmt.Errorf("stop replica before changing replication source")
	}
	oldSourceURL := r.cfg.SourceURL
	path := r.sourcePath
	identityPath := r.sourceIdentityPath
	if path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := writeReplicationFileAtomic(path, raw); err != nil {
			return err
		}
	}
	if err := persistSourceIdentity(identityPath, ""); err != nil {
		if path != "" {
			_ = persistSourceConfig(path, redactNativeSourcePassword(oldSourceURL))
		}
		return err
	}
	r.cfg.SourceURL = persistedSourceURL
	r.lastSourceFailure = time.Time{}
	r.lastErr = ""
	r.lastErrorNumber = 0
	r.lastErrorAt = time.Time{}
	r.lastIOError = ""
	r.lastIOErrorNumber = 0
	r.lastIOErrorAt = time.Time{}
	r.lastSQLError = ""
	r.lastSQLErrorNumber = 0
	r.lastSQLErrorAt = time.Time{}
	r.nativeSourceUUID = ""
	r.nativeHeartbeatCount = 0
	r.nativeLastHeartbeatAt = time.Time{}
	r.nativeSource = native
	r.nativeConfig = nativeConfig
	return nil
}

// ChangeReplicationFilter replaces the replica-side filter set while the
// applier is stopped, matching CHANGE REPLICATION FILTER's lifecycle. The
// filter configuration is persisted independently from relay/GTID state.
func (r *Runtime) ChangeReplicationFilter(config ReplicationFilterConfig) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	if r.role != RoleReplica || r.replica == nil {
		r.mu.RUnlock()
		return fmt.Errorf("only a replica runtime can change replication filters")
	}
	if r.pollCancel != nil {
		r.mu.RUnlock()
		return fmt.Errorf("stop replica before changing replication filters")
	}
	replica := r.replica
	r.mu.RUnlock()
	return replica.SetReplicationFilters(config, "CHANGE_REPLICATION_FILTER")
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
	r.mu.Lock()
	r.lastErr = ""
	r.lastErrorNumber = 0
	r.lastErrorAt = time.Time{}
	r.lastIOError = ""
	r.lastIOErrorNumber = 0
	r.lastIOErrorAt = time.Time{}
	r.lastSQLError = ""
	r.lastSQLErrorNumber = 0
	r.lastSQLErrorAt = time.Time{}
	r.nativeHeartbeatCount = 0
	r.nativeLastHeartbeatAt = time.Time{}
	r.mu.Unlock()

	replica.mu.Lock()
	defer replica.mu.Unlock()
	next := replicaState{Executed: GTIDSet{}, AppliedRows: []RowChange{}, PreparedXA: map[string]BinlogEvent{}, RelayEvents: []BinlogEvent{}, NativeRelayEvents: []NativeBinlogEvent{}, NativeTableMaps: []NativeBinlogEvent{}, AppliedTransactions: map[string]bool{}}
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
	replica.nativeRelayEvents = next.NativeRelayEvents
	replica.nativeTableMaps = next.NativeTableMaps
	replica.preparedXA = next.PreparedXA
	replica.appliedTransactions = next.AppliedTransactions
	replica.LastSourceFile = ""
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
	identityPath := r.sourceIdentityPath
	r.mu.Unlock()
	if path == "" {
		return persistSourceIdentity(identityPath, "")
	}
	raw, err := json.MarshalIndent(persistedSourceConfig{}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := writeReplicationFileAtomic(path, raw); err != nil {
		return err
	}
	return persistSourceIdentity(identityPath, "")
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	cancel := r.cancel
	server := r.server
	done := r.pollDone
	children := make([]*Runtime, 0, len(r.channels))
	for _, child := range r.channels {
		children = append(children, child)
	}
	r.mu.RUnlock()
	var childErr error
	for _, child := range children {
		if err := child.Close(); err != nil && childErr == nil {
			childErr = err
		}
	}
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
	if shutdownErr == nil {
		shutdownErr = childErr
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

// AppendOnePhaseXATransaction publishes XA COMMIT ONE PHASE using the native
// XA_PREPARE_EVENT(one_phase=1) boundary instead of a regular XID_EVENT.
func (r *Runtime) AppendOnePhaseXATransaction(key string, xid XAIdentity, changes []RowChange, statements []Statement) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	source, role, fenced := r.source, r.role, r.fenced
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
	return source.AppendOnePhaseXATransaction(key, xid, changes, statements)
}

// PrepareXATransaction appends an XA PREPARE boundary on a source without
// advancing its executed GTID set.
func (r *Runtime) PrepareXATransaction(key string, xid XAIdentity, changes []RowChange, statements []Statement) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	source, role, fenced := r.source, r.role, r.fenced
	r.mu.RUnlock()
	if role != RoleSource || source == nil {
		return fmt.Errorf("replication runtime is not a source")
	}
	if fenced {
		return fmt.Errorf("replication source is fenced")
	}
	return source.PrepareXATransaction(key, xid, changes, statements)
}

// CommitXATransaction appends XA COMMIT and advances the source executed
// GTID state for a previously prepared XID.
func (r *Runtime) CommitXATransaction(key string, xid XAIdentity) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	source, role, fenced := r.source, r.role, r.fenced
	r.mu.RUnlock()
	if role != RoleSource || source == nil {
		return fmt.Errorf("replication runtime is not a source")
	}
	if fenced {
		return fmt.Errorf("replication source is fenced")
	}
	return source.CommitXATransaction(key, xid)
}

// RollbackXATransaction appends XA ROLLBACK for a prepared XID without
// advancing the source executed GTID state.
func (r *Runtime) RollbackXATransaction(key string, xid XAIdentity) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	source, role, fenced := r.source, r.role, r.fenced
	r.mu.RUnlock()
	if role != RoleSource || source == nil {
		return fmt.Errorf("replication runtime is not a source")
	}
	if fenced {
		return fmt.Errorf("replication source is fenced")
	}
	return source.RollbackXATransaction(key, xid)
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
	return r.ResetMasterTo(1)
}

// ResetMasterTo clears a source's durable logical binlog and GTID history and
// optionally starts the native binlog sequence at the requested file index.
func (r *Runtime) ResetMasterTo(index uint32) error {
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
	return source.ResetMasterTo(index)
}

// PurgeBinaryLogsTo removes source native binlog files older than logName.
func (r *Runtime) PurgeBinaryLogsTo(logName string) error {
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
	return source.PurgeBinaryLogsTo(logName)
}

// PurgeBinaryLogsBefore removes source native binlog files older than cutoff.
func (r *Runtime) PurgeBinaryLogsBefore(cutoff time.Time) error {
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
	return source.PurgeBinaryLogsBefore(cutoff)
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
		preparedXA := cloneNativePreparedXA(r.replica.preparedXA)
		relayEvents := make([]BinlogEvent, len(r.replica.relayEvents))
		for index, event := range r.replica.relayEvents {
			relayEvents[index] = cloneBinlogEventForRelay(event)
		}
		r.replica.mu.Unlock()
		source.Executed = executed
		source.nextSeq = nextSequence(executed, source.UUID)
		if err := source.persistDurableState(); err != nil {
			return err
		}
		if err := source.ImportRelayEvents(relayEvents); err != nil {
			return err
		}
		if err := source.ImportPreparedXA(preparedXA); err != nil {
			return err
		}
	}
	if err := persistRuntimeRole(r.rolePath, RoleSource); err != nil {
		return err
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
		if r.fenced && r.fencedBy != "" && r.fencedBy != localUUID {
			fencedBy := r.fencedBy
			r.mu.Unlock()
			return fmt.Errorf("promotion fencing lost to %s", fencedBy)
		}
		previousEpoch, previousFenced, previousBy := r.fencingEpoch, r.fenced, r.fencedBy
		r.fencingEpoch = localEpoch
		r.fenced = false
		r.fencedBy = ""
		path := r.fencingPath
		if err := persistFencing(path, persistedFencingState{Epoch: localEpoch}); err != nil {
			r.fencingEpoch, r.fenced, r.fencedBy = previousEpoch, previousFenced, previousBy
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()
		return nil
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
	if r.fencingEpoch > epoch || (r.fenced && r.fencedBy != "" && r.fencedBy != localUUID) {
		fencedBy := r.fencedBy
		currentEpoch := r.fencingEpoch
		r.mu.Unlock()
		if fencedBy != "" {
			return fmt.Errorf("promotion fencing lost to %s at epoch %d", fencedBy, currentEpoch)
		}
		return fmt.Errorf("promotion fencing epoch advanced to %d", currentEpoch)
	}
	previousEpoch, previousFenced, previousBy := r.fencingEpoch, r.fenced, r.fencedBy
	r.fencingEpoch = epoch
	r.fenced = false
	r.fencedBy = ""
	path := r.fencingPath
	if err := persistFencing(path, persistedFencingState{Epoch: epoch}); err != nil {
		r.fencingEpoch, r.fenced, r.fencedBy = previousEpoch, previousFenced, previousBy
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()
	return nil
}

func (r *Runtime) applyFence(request fenceRequest) error {
	request.CandidateUUID = strings.TrimSpace(request.CandidateUUID)
	if request.Epoch == 0 || request.CandidateUUID == "" {
		return fmt.Errorf("fence request requires epoch and candidate_uuid")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if request.Epoch < r.fencingEpoch {
		return fmt.Errorf("stale fencing epoch")
	}
	if request.Epoch == r.fencingEpoch && r.fenced {
		if request.CandidateUUID >= r.fencedBy {
			return fmt.Errorf("fencing epoch already owned by %s", r.fencedBy)
		}
	}
	if request.Epoch == r.fencingEpoch && !r.fenced {
		if request.CandidateUUID == r.cfg.UUID {
			return nil
		}
		if request.CandidateUUID >= r.cfg.UUID {
			return fmt.Errorf("fencing epoch already owned by %s", r.cfg.UUID)
		}
	}
	previousEpoch, previousFenced, previousBy := r.fencingEpoch, r.fenced, r.fencedBy
	r.fencingEpoch = request.Epoch
	r.fenced = true
	r.fencedBy = request.CandidateUUID
	if err := persistFencing(r.fencingPath, persistedFencingState{Epoch: request.Epoch, Fenced: true, By: request.CandidateUUID}); err != nil {
		r.fencingEpoch, r.fenced, r.fencedBy = previousEpoch, previousFenced, previousBy
		return err
	}
	return nil
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
		ChannelName:        r.channelName,
		Role:               r.role,
		UUID:               r.cfg.UUID,
		ServerID:           r.cfg.ServerID,
		SourceURL:          publicSourceURL(r.cfg.SourceURL),
		NativeEndpoint:     strings.TrimRight(strings.TrimSpace(r.cfg.NativeEndpoint), "/"),
		SourceUUID:         r.nativeSourceUUID,
		SourceAutoPosition: r.nativeSource != nil && r.nativeConfig.GTIDAutoPosition,
		ReceivedHeartbeats: r.nativeHeartbeatCount,
		LastHeartbeatAt:    r.nativeLastHeartbeatAt,
		LastError:          r.lastErr,
		LastErrorNumber:    r.lastErrorNumber,
		LastErrorAt:        r.lastErrorAt,
		LastIOError:        r.lastIOError,
		LastIOErrorNumber:  r.lastIOErrorNumber,
		LastIOErrorAt:      r.lastIOErrorAt,
		LastSQLError:       r.lastSQLError,
		LastSQLErrorNumber: r.lastSQLErrorNumber,
		LastSQLErrorAt:     r.lastSQLErrorAt,
		Promotable:         r.role == RoleReplica && !r.fenced,
		FencingEpoch:       r.fencingEpoch,
		Fenced:             r.fenced,
		FencedBy:           r.fencedBy,
	}
	if parsed, err := url.Parse(strings.TrimSpace(r.cfg.SourceURL)); err == nil && parsed.User != nil {
		status.SourceUser = parsed.User.Username()
	}
	if r.role == RoleSource && r.source != nil {
		status.ExecutedGTIDs = r.source.Executed.String()
	}
	if r.role == RoleReplica && r.replica != nil {
		status.ReplicaRunning = r.pollCancel != nil
		status.ReplicaRunningKnown = true
		r.replica.mu.Lock()
		status.ExecutedGTIDs = r.replica.Executed.String()
		status.SourceFile = r.replica.LastSourceFile
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
		status.ReplicationFilters = r.replica.replicationFilterStatusLocked()
		r.replica.mu.Unlock()
	}
	return status
}

func publicSourceURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.User == nil || !strings.EqualFold(parsed.Scheme, "mysql") {
		return raw
	}
	// A status endpoint is observable by operators and health probes. Do not
	// expose either the password or the source username; the username is still
	// credential material and can reveal account topology.
	parsed.User = nil
	return parsed.String()
}

// redactNativeSourcePassword removes only the password from a native source
// URL while retaining the username needed by status and P_S projections. The
// persisted source file must never become a credential store.
func redactNativeSourcePassword(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "mysql") || parsed.User == nil {
		return raw
	}
	parsed.User = url.User(parsed.User.Username())
	return strings.TrimRight(parsed.String(), "/")
}

// nativeSourceURLWithEnvironmentPassword supplies a native source password at
// runtime without persisting it. CHANGE REPLICATION SOURCE can provide a
// password for the current process; a restarted process may rehydrate it from
// the protected environment variable used by deployment orchestration.
func nativeSourceURLWithEnvironmentPassword(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "mysql") || parsed.User == nil {
		return raw
	}
	if _, passwordSet := parsed.User.Password(); passwordSet {
		return raw
	}
	password := os.Getenv("XMYSQL_REPLICATION_PASSWORD")
	if password == "" {
		return raw
	}
	parsed.User = url.UserPassword(parsed.User.Username(), password)
	return parsed.String()
}

func nativeMySQLGTIDSet(executed GTIDSet) string {
	if len(executed) == 0 {
		return ""
	}
	intervals := GTIDIntervalsFromSet(executed)
	filtered := GTIDIntervals{}
	for uuid, ranges := range intervals {
		if !validNativeMySQLUUID(uuid) {
			continue
		}
		filtered[uuid] = append([]GTIDInterval(nil), ranges...)
	}
	return filtered.String()
}

func validNativeMySQLUUID(value string) bool {
	if len(value) != 36 || strings.Count(value, "-") != 4 {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16
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
	logName, _ := source.NativeCurrentFilePosition()
	writeJSON(w, binlogResponse{Events: events, NextPosition: next, LogName: logName})
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
	r.mu.RLock()
	listenAddr := r.cfg.ListenAddr
	if r.listener != nil {
		listenAddr = r.listener.Addr().String()
	}
	r.mu.RUnlock()
	if err := validatePeersAgainstListenAddress(normalized, listenAddr); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return err
	}
	r.mu.Lock()
	path := r.membersPath
	if path == "" {
		r.cfg.Peers = normalized
		r.mu.Unlock()
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		r.mu.Unlock()
		return err
	}
	if err := writeReplicationFileAtomic(path, raw); err != nil {
		r.mu.Unlock()
		return err
	}
	r.cfg.Peers = normalized
	r.mu.Unlock()
	return nil
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

// validatePeersAgainstListenAddress prevents a node from counting its own
// control endpoint as a remote member. A self peer would inflate the quorum
// denominator while reporting the local node as reachable, which can make an
// isolated replica appear to have a majority during automatic failover.
// Host aliases cannot be proven equivalent without service discovery, so this
// intentionally rejects exact configured/listener endpoint matches only.
func validatePeersAgainstListenAddress(peers []string, listenAddr string) error {
	listenAddr = strings.TrimRight(strings.TrimSpace(listenAddr), "/")
	if listenAddr == "" {
		return nil
	}
	for _, peer := range peers {
		parsed, err := url.Parse(peer)
		if err != nil {
			continue
		}
		if parsed.Host == listenAddr {
			return fmt.Errorf("peer %q cannot include local replication endpoint %q", peer, listenAddr)
		}
	}
	return nil
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
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "mysql") || parsed.Host == "" {
		return "", fmt.Errorf("persisted replication source is invalid: %q", config.SourceURL)
	}
	return strings.TrimRight(strings.TrimSpace(config.SourceURL), "/"), nil
}

func (r *Runtime) persistNativeSourceIdentity(sourceUUID string) error {
	if r == nil {
		return fmt.Errorf("replication runtime is nil")
	}
	r.mu.RLock()
	path := r.sourceIdentityPath
	r.mu.RUnlock()
	return persistSourceIdentity(path, sourceUUID)
}

func persistSourceConfig(path, sourceURL string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	raw, err := json.MarshalIndent(persistedSourceConfig{SourceURL: strings.TrimRight(strings.TrimSpace(sourceURL), "/")}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

func persistSourceIdentity(path, sourceUUID string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	raw, err := json.MarshalIndent(persistedSourceIdentity{SourceUUID: strings.TrimSpace(sourceUUID)}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
}

func loadPersistedSourceIdentity(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var identity persistedSourceIdentity
	if err := json.Unmarshal(raw, &identity); err != nil {
		return "", fmt.Errorf("decode persisted replication source identity: %w", err)
	}
	return strings.TrimSpace(identity.SourceUUID), nil
}

func loadPersistedRuntimeRole(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var persisted persistedRuntimeRole
	if err := json.Unmarshal(raw, &persisted); err != nil {
		return "", fmt.Errorf("decode persisted replication role: %w", err)
	}
	role := strings.ToLower(strings.TrimSpace(persisted.Role))
	if role == "" {
		return "", nil
	}
	if role != RoleSource {
		return "", fmt.Errorf("persisted replication role is invalid: %q", persisted.Role)
	}
	return role, nil
}

func persistRuntimeRole(path, role string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	role = strings.ToLower(strings.TrimSpace(role))
	if role != RoleSource {
		return fmt.Errorf("cannot persist unsupported replication role %q", role)
	}
	raw, err := json.MarshalIndent(persistedRuntimeRole{Role: role}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(path, raw)
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
	r.mu.RLock()
	if position == 4 && r.nativeSource != nil && r.nativeConfig.BinlogPosition >= 4 {
		position = r.nativeConfig.BinlogPosition
	}
	r.mu.RUnlock()
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		r.mu.RLock()
		role := r.role
		native := r.nativeSource != nil
		r.mu.RUnlock()
		if role != RoleReplica {
			return
		}
		var pullErr error
		if native {
			pullErr = r.pullNativeOnce(ctx, &position)
		} else {
			pullErr = r.pullOnce(ctx, client, &position)
		}
		if pullErr != nil {
			if replicationErrorClassOf(pullErr) == replicationErrorClassUnknown {
				pullErr = wrapReplicationError(replicationErrorClassIO, pullErr)
			}
			r.setError(pullErr)
			if r.shouldAutoFailover() {
				if err := r.tryAutoPromote(ctx); err != nil {
					r.setError(err)
				}
			}
		} else {
			r.clearSourceFailure()
		}
		delay := r.cfg.PollInterval
		if native && pullErr != nil {
			r.mu.RLock()
			nativeConfig := r.nativeConfig
			r.mu.RUnlock()
			delay = nativeRetryDelay(nativeConfig, pullErr, delay)
		}
		timer := time.NewTimer(delay)
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

func nativeRetryDelay(config MySQLBinlogSourceConfig, pullErr error, fallback time.Duration) time.Duration {
	if pullErr == nil || config.ConnectionRetryInterval == 0 {
		return fallback
	}
	const maxSeconds = uint64(1<<63-1) / uint64(time.Second)
	if config.ConnectionRetryInterval > maxSeconds {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(config.ConnectionRetryInterval) * time.Second
}

const nativeRuntimeBatchSize = 256

func (r *Runtime) pullNativeOnce(ctx context.Context, position *uint64) error {
	if r == nil || r.replica == nil || position == nil {
		return fmt.Errorf("native replication runtime is not initialized")
	}
	r.mu.RLock()
	source := r.nativeSource
	nativeConfig := r.nativeConfig
	forceFileReplay := r.nativeReplayFromFile
	r.mu.RUnlock()
	if source == nil {
		return fmt.Errorf("native replication source is not configured")
	}
	if metadataSource, ok := source.(NativeBinlogSourceMetadata); ok {
		r.mu.RLock()
		knownSourceUUID := r.nativeSourceUUID
		r.mu.RUnlock()
		if strings.TrimSpace(knownSourceUUID) == "" {
			metadataCtx, cancel := context.WithTimeout(ctx, nativeMetadataTimeout(nativeConfig.ReadTimeout))
			sourceUUID, metadataErr := metadataSource.SourceUUID(metadataCtx)
			cancel()
			if metadataErr == nil && strings.TrimSpace(sourceUUID) != "" {
				sourceUUID = strings.TrimSpace(sourceUUID)
				if persistErr := r.persistNativeSourceIdentity(sourceUUID); persistErr != nil {
					return persistErr
				}
				r.mu.Lock()
				r.nativeSourceUUID = sourceUUID
				r.mu.Unlock()
			}
		}
	}
	if forceFileReplay {
		if err := r.replica.resetNativeTableMapsForFileReplay(); err != nil {
			return err
		}
	}
	r.replica.mu.Lock()
	sourceFile := r.replica.LastSourceFile
	storedPosition := r.replica.LastSourcePosition
	executed := cloneGTIDSet(r.replica.Executed)
	nativeRelayPending := len(r.replica.nativeRelayEvents) > 0
	r.replica.mu.Unlock()
	if forceFileReplay {
		// A MySQL GTID dump may begin a later row event without replaying the
		// TABLE_MAP needed by a fresh decoder. Rewind the current binlog file
		// once so the decoder can rebuild its table dictionary; executed GTIDs
		// keep already applied transactions idempotent.
		nativeConfig.GTIDSet = ""
		*position = 4
		nativeConfig.BinlogPosition = 4
	} else if !nativeRelayPending {
		persistedGTIDs := nativeMySQLGTIDSet(executed)
		if persistedGTIDs != "" {
			nativeConfig.GTIDSet = persistedGTIDs
			// A persisted source file/position is the precise recovery boundary.
			// Prefer it when available: COM_BINLOG_DUMP_GTID with an empty file
			// name may replay a large non-GTID history before reaching the next
			// transaction, so a bounded pull can reconnect at the same old
			// preamble forever. Keep the durable GTID set for stale-file fallback.
			nativeConfig.GTIDAutoPosition = !nativeRecoveryUsesPersistedFilePosition(sourceFile, storedPosition, false)
		} else if !(nativeConfig.GTIDAutoPosition && strings.TrimSpace(nativeConfig.GTIDSet) != "") {
			// Preserve an explicitly configured GTID baseline on first attach.
			// It is needed when the source has already executed initialization
			// transactions that must not be replayed by a new replica.
			nativeConfig.GTIDSet = ""
		}
	} else {
		// A GTID dump restarts at the beginning of the next transaction. If
		// the previous pull ended mid-transaction, that would duplicate the
		// GTID already present in nativeRelayEvents. Resume by file/position
		// until the relay transaction closes, then switch back to GTID mode.
		nativeConfig.GTIDSet = ""
		nativeConfig.GTIDAutoPosition = false
	}
	if nativeConfig.GTIDAutoPosition && nativeConfig.GTIDSet != "" && !nativeRelayPending {
		r.mu.Lock()
		r.nativeConfig.GTIDAutoPosition = true
		r.mu.Unlock()
	}
	nativeGTIDStream := nativeConfig.GTIDAutoPosition && !forceFileReplay && !nativeRelayPending
	if sourceFile == "" {
		sourceFile = nativeConfig.BinlogFile
	}
	if storedPosition >= 4 {
		*position = storedPosition
	}
	if *position < 4 {
		*position = nativeConfig.BinlogPosition
	}
	if *position < 4 {
		*position = 4
	}
	if forceFileReplay {
		*position = 4
		nativeConfig.BinlogPosition = 4
	}
	replayBoundaryFile := sourceFile
	replayBoundaryPosition := storedPosition
	if strings.TrimSpace(sourceFile) == "" && strings.TrimSpace(nativeConfig.GTIDSet) == "" && !nativeConfig.GTIDAutoPosition {
		return fmt.Errorf("native replication source has no binlog file")
	}

	// Recreate the concrete source at the durable position for every pull. A
	// COM_BINLOG_DUMP stream is stateful; restarting it from the persisted
	// event boundary is what makes reconnect and crash recovery idempotent.
	if _, ok := source.(*MySQLBinlogSource); ok {
		nativeConfig.BinlogFile = sourceFile
		nativeConfig.BinlogPosition = *position
		var err error
		source, err = NewMySQLBinlogSource(nativeConfig)
		if err != nil {
			return err
		}
	}
	readTimeout := nativeConfig.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 15 * time.Second
	}
	for attempt := 0; attempt < 2; attempt++ {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		frames, dumpErr := source.Dump(readCtx, nativeRuntimeBatchSize)
		cancel()
		if heartbeatCount := nativeHeartbeatFrameCount(frames); heartbeatCount > 0 {
			r.mu.Lock()
			r.nativeHeartbeatCount += heartbeatCount
			r.nativeLastHeartbeatAt = time.Now().UTC()
			r.mu.Unlock()
		}
		if len(frames) == 0 {
			// A file/position source can become stale across rotation while the
			// replica is stopped. MySQL reports this as ER_MASTER_FATAL_ERROR_READING_BINLOG
			// (1236), commonly with "requested ... position > file size". If the
			// durable executed set is available, retry through GTID auto-position so
			// recovery does not depend on the old file still being current. This is
			// intentionally limited to the concrete MySQL source and one retry; the
			// in-process source and scripted test sources retain their exact behavior.
			if attempt == 0 && dumpErr != nil && nativeConfig.GTIDSet != "" &&
				!nativeConfig.GTIDAutoPosition && isNativeMySQLStalePositionError(dumpErr) {
				nativeConfig.GTIDAutoPosition = true
				nativeConfig.BinlogFile = ""
				nativeConfig.BinlogPosition = 4
				fallback, fallbackErr := NewMySQLBinlogSource(nativeConfig)
				if fallbackErr != nil {
					return fallbackErr
				}
				source = fallback
				continue
			}
			if attempt == 0 && nativeConfig.GTIDSet != "" &&
				dumpErr != nil && !errors.Is(dumpErr, context.DeadlineExceeded) && !errors.Is(dumpErr, context.Canceled) {
				nativeConfig.GTIDSet = ""
				nativeConfig.BinlogFile = sourceFile
				nativeConfig.BinlogPosition = *position
				fallback, fallbackErr := NewMySQLBinlogSource(nativeConfig)
				if fallbackErr != nil {
					return fallbackErr
				}
				source = fallback
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if dumpErr != nil && !errors.Is(dumpErr, context.DeadlineExceeded) && !errors.Is(dumpErr, context.Canceled) {
				return dumpErr
			}
			r.replica.mu.Lock()
			r.replica.LastAppliedAt = time.Time{}
			r.replica.LastError = ""
			r.replica.mu.Unlock()
			return nil
		}

		initialSourceFile := sourceFile
		if !nativeGTIDStream {
			frames = nativeFramesAtOrAfterSourceBoundary(frames, initialSourceFile, *position)
		}
		if len(frames) == 0 {
			if dumpErr != nil && !errors.Is(dumpErr, context.DeadlineExceeded) && !errors.Is(dumpErr, context.Canceled) {
				return dumpErr
			}
			continue
		}
		nextPosition := *position
		metadataOnly := true
		for _, frame := range frames {
			frameFile := strings.TrimSpace(frame.File)
			frameIsCurrentOrNewer := nativeGTIDStream || frameFile == "" || sourceFile == "" || frameFile >= sourceFile
			switch nativeBinlogFrameType(frame.Raw) {
			case 4, 15, 27: // synthetic ROTATE/FORMAT_DESCRIPTION and HEARTBEAT frames
			default:
				metadataOnly = false
			}
			if frameIsCurrentOrNewer && frameFile != "" && frameFile != sourceFile {
				sourceFile = frameFile
				nextPosition = 4
			}
			if frameIsCurrentOrNewer && frame.EndPosition > nextPosition {
				nextPosition = frame.EndPosition
			}
			if rotateFile, rotatePosition, ok := nativeRotateTargetFile(frame); ok &&
				(nativeGTIDStream || sourceFile == "" || rotateFile >= sourceFile) {
				sourceFile = rotateFile
				nextPosition = rotatePosition
			}
		}
		if metadataOnly && attempt == 0 && nativeConfig.GTIDSet != "" &&
			dumpErr != nil && !errors.Is(dumpErr, context.DeadlineExceeded) && !errors.Is(dumpErr, context.Canceled) {
			nativeConfig.GTIDSet = ""
			sourceFile = initialSourceFile
			nativeConfig.BinlogFile = initialSourceFile
			nativeConfig.BinlogPosition = *position
			fallback, fallbackErr := NewMySQLBinlogSource(nativeConfig)
			if fallbackErr != nil {
				return fallbackErr
			}
			source = fallback
			continue
		}
		if nextPosition <= *position && sourceFile == initialSourceFile {
			if metadataOnly {
				r.replica.mu.Lock()
				r.replica.LastAppliedAt = time.Time{}
				r.replica.LastError = ""
				r.replica.mu.Unlock()
				return nil
			}
			return fmt.Errorf("native replication source returned frames without advancing position")
		}
		framesToApply := frames
		if forceFileReplay {
			framesToApply = nativeFramesForFileReplay(frames, replayBoundaryFile, replayBoundaryPosition)
		}
		framesToApply = nativeFramesWithoutHeartbeats(framesToApply)
		if err := r.replica.ApplyNativeAtSource(framesToApply, sourceFile, nextPosition); err != nil {
			r.mu.Lock()
			if _, ok := source.(*MySQLBinlogSource); ok {
				r.nativeReplayFromFile = true
			}
			r.mu.Unlock()
			return wrapReplicationError(replicationErrorClassSQL, err)
		}
		r.mu.Lock()
		r.nativeReplayFromFile = false
		r.mu.Unlock()
		*position = nextPosition
		if dumpErr != nil && !errors.Is(dumpErr, context.DeadlineExceeded) && !errors.Is(dumpErr, context.Canceled) {
			return dumpErr
		}
		return nil
	}
	return nil
}

func nativeHeartbeatFrameCount(frames []NativeBinlogEvent) uint64 {
	var count uint64
	for _, frame := range frames {
		if nativeBinlogFrameType(frame.Raw) == 27 { // HEARTBEAT_EVENT
			count++
		}
	}
	return count
}

func nativeRecoveryUsesPersistedFilePosition(sourceFile string, storedPosition uint64, nativeRelayPending bool) bool {
	return !nativeRelayPending && strings.TrimSpace(sourceFile) != "" && storedPosition >= 4
}

func nativeFramesWithoutHeartbeats(frames []NativeBinlogEvent) []NativeBinlogEvent {
	filtered := make([]NativeBinlogEvent, 0, len(frames))
	for _, frame := range frames {
		if nativeBinlogFrameType(frame.Raw) == 27 { // HEARTBEAT_EVENT
			continue
		}
		filtered = append(filtered, frame)
	}
	return filtered
}

func nativeMetadataTimeout(configured time.Duration) time.Duration {
	if configured <= 0 || configured > 5*time.Second {
		return 5 * time.Second
	}
	return configured
}

func nativeFramesAtOrAfterSourceBoundary(frames []NativeBinlogEvent, boundaryFile string, boundaryPosition uint64) []NativeBinlogEvent {
	if len(frames) == 0 || strings.TrimSpace(boundaryFile) == "" {
		return frames
	}
	filtered := make([]NativeBinlogEvent, 0, len(frames))
	for _, frame := range frames {
		typeCode := nativeBinlogFrameType(frame.Raw)
		if typeCode == 15 { // FORMAT_DESCRIPTION_EVENT is a valid stream preamble.
			filtered = append(filtered, frame)
			continue
		}
		if typeCode == 4 {
			rotateFile, _, ok := nativeRotateTargetFile(frame)
			if !ok || rotateFile >= boundaryFile {
				filtered = append(filtered, frame)
			}
			continue
		}
		file := strings.TrimSpace(frame.File)
		if file == "" {
			if frame.EndPosition > boundaryPosition {
				filtered = append(filtered, frame)
			}
			continue
		}
		if file > boundaryFile || file == boundaryFile && frame.EndPosition > boundaryPosition {
			filtered = append(filtered, frame)
		}
	}
	return filtered
}

func isNativeMySQLStalePositionError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "error 1236") &&
		(strings.Contains(message, "position") || strings.Contains(message, "binlog") || strings.Contains(message, "log file"))
}

func nativeFramesForFileReplay(frames []NativeBinlogEvent, boundaryFile string, boundaryPosition uint64) []NativeBinlogEvent {
	if len(frames) == 0 || strings.TrimSpace(boundaryFile) == "" || boundaryPosition < 4 {
		return frames
	}
	result := make([]NativeBinlogEvent, 0, len(frames))
	for _, frame := range frames {
		typeCode := nativeBinlogFrameType(frame.Raw)
		if typeCode == 19 || typeCode == 4 || typeCode == 15 || typeCode == 35 {
			result = append(result, frame)
			continue
		}
		file := strings.TrimSpace(frame.File)
		if file == "" {
			if frame.Position >= boundaryPosition {
				result = append(result, frame)
			}
			continue
		}
		if file > boundaryFile || file == boundaryFile && frame.EndPosition > boundaryPosition {
			result = append(result, frame)
		}
	}
	return result
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
	URL       string
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
		statuses = append(statuses, peerStatus{StatusSnapshot: status, Reachable: true, URL: peer})
		if status.Role == RoleSource && !status.Fenced {
			sourceSeen = true
		}
	}
	if sourceSeen {
		for _, observed := range statuses[1:] {
			if observed.Role != RoleSource || observed.Fenced || strings.TrimSpace(observed.URL) == "" {
				continue
			}
			if err := r.repointSourceDuringPoll(observed); err != nil {
				return err
			}
			return nil
		}
	}
	if local.Fenced {
		return nil
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

// repointSourceDuringPoll updates a running replica's source after a peer has
// been promoted. The normal ChangeSource API intentionally requires STOP
// REPLICA first; the poll loop is already the serialization point here, so
// waiting for itself would deadlock. The durable relay/GTID state remains
// untouched and makes the new source resume idempotently.
func (r *Runtime) repointSourceDuringPoll(observed peerStatus) error {
	r.mu.RLock()
	native := r.nativeSource != nil || r.nativeConfig.Host != ""
	currentSourceURL := r.cfg.SourceURL
	nativeConfig := r.nativeConfig
	r.mu.RUnlock()
	if native {
		return r.repointNativeSourceDuringPoll(observed.StatusSnapshot.NativeEndpoint, currentSourceURL, nativeConfig)
	}
	return r.repointHTTPSourceDuringPoll(observed.URL)
}

func (r *Runtime) repointHTTPSourceDuringPoll(sourceURL string) error {
	sourceURL = strings.TrimRight(strings.TrimSpace(sourceURL), "/")
	parsed, err := url.ParseRequestURI(sourceURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("automatic source repoint requires an absolute HTTP URL: %q", sourceURL)
	}
	r.mu.Lock()
	if r.role != RoleReplica || r.replica == nil {
		r.mu.Unlock()
		return fmt.Errorf("automatic source repoint requires a replica runtime")
	}
	if r.nativeSource != nil || r.nativeConfig.Host != "" {
		r.mu.Unlock()
		return fmt.Errorf("automatic source repoint for native MySQL sources requires a native source endpoint")
	}
	if strings.TrimRight(strings.TrimSpace(r.cfg.SourceURL), "/") == sourceURL {
		r.lastSourceFailure = time.Time{}
		r.mu.Unlock()
		return nil
	}
	r.cfg.SourceURL = sourceURL
	r.lastSourceFailure = time.Time{}
	r.lastErr = ""
	r.lastErrorNumber = 0
	r.lastErrorAt = time.Time{}
	r.lastIOError = ""
	r.lastIOErrorNumber = 0
	r.lastIOErrorAt = time.Time{}
	r.lastSQLError = ""
	r.lastSQLErrorNumber = 0
	r.lastSQLErrorAt = time.Time{}
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

func (r *Runtime) repointNativeSourceDuringPoll(endpoint, currentSourceURL string, current MySQLBinlogSourceConfig) error {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	parsedEndpoint, err := url.ParseRequestURI(endpoint)
	if err != nil || parsedEndpoint.Host == "" || !strings.EqualFold(parsedEndpoint.Scheme, "mysql") {
		return fmt.Errorf("automatic native source repoint requires a MySQL endpoint: %q", endpoint)
	}
	parsedCurrent, err := url.Parse(strings.TrimSpace(currentSourceURL))
	if err != nil || !strings.EqualFold(parsedCurrent.Scheme, "mysql") {
		return fmt.Errorf("automatic native source repoint has an invalid current source: %q", currentSourceURL)
	}
	if current.User != "" {
		if current.Password != "" {
			parsedEndpoint.User = url.UserPassword(current.User, current.Password)
		} else {
			parsedEndpoint.User = url.User(current.User)
		}
	}
	parsedEndpoint.RawQuery = parsedCurrent.RawQuery
	sourceURL := strings.TrimRight(parsedEndpoint.String(), "/")
	config, err := ParseMySQLBinlogSourceURL(sourceURL, current.ServerID)
	if err != nil {
		return err
	}
	config.BinlogFile = current.BinlogFile
	config.BinlogPosition = current.BinlogPosition
	config.GTIDSet = current.GTIDSet
	config.GTIDAutoPosition = current.GTIDAutoPosition
	config.TLSConfig = current.TLSConfig
	native, err := NewMySQLBinlogSource(config)
	if err != nil {
		return err
	}

	persistedSourceURL := redactNativeSourcePassword(sourceURL)
	r.mu.RLock()
	if r.role != RoleReplica || r.replica == nil {
		r.mu.RUnlock()
		return fmt.Errorf("automatic native source repoint requires a replica runtime")
	}
	oldSourceURL := r.cfg.SourceURL
	oldSourceUUID := r.nativeSourceUUID
	path := r.sourcePath
	identityPath := r.sourceIdentityPath
	r.mu.RUnlock()

	// Publish neither the endpoint nor the native source until both durable
	// files are updated. The source file is written first, matching
	// CHANGE REPLICATION SOURCE; if identity cleanup fails, restore the old
	// source URL so a failed repoint cannot split current and restart state.
	if err := persistReplicationSourceURL(path, persistedSourceURL); err != nil {
		return err
	}
	if err := persistSourceIdentity(identityPath, ""); err != nil {
		restoreErr := persistReplicationSourceURL(path, redactNativeSourcePassword(oldSourceURL))
		if restoreErr != nil {
			return fmt.Errorf("persist native source identity: %w; restore source URL: %v", err, restoreErr)
		}
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != RoleReplica || r.replica == nil {
		_ = persistReplicationSourceURL(path, redactNativeSourcePassword(oldSourceURL))
		_ = persistSourceIdentity(identityPath, oldSourceUUID)
		return fmt.Errorf("automatic native source repoint requires a replica runtime")
	}
	if r.cfg.SourceURL != oldSourceURL {
		_ = persistReplicationSourceURL(path, redactNativeSourcePassword(r.cfg.SourceURL))
		_ = persistSourceIdentity(identityPath, r.nativeSourceUUID)
		return fmt.Errorf("automatic native source repoint superseded by another source change")
	}
	r.cfg.SourceURL = persistedSourceURL
	r.nativeSource = native
	r.nativeConfig = config
	r.lastSourceFailure = time.Time{}
	r.lastErr = ""
	r.lastErrorNumber = 0
	r.lastErrorAt = time.Time{}
	r.lastIOError = ""
	r.lastIOErrorNumber = 0
	r.lastIOErrorAt = time.Time{}
	r.lastSQLError = ""
	r.lastSQLErrorNumber = 0
	r.lastSQLErrorAt = time.Time{}
	r.nativeSourceUUID = ""
	r.nativeHeartbeatCount = 0
	r.nativeLastHeartbeatAt = time.Time{}
	return nil
}

func persistReplicationSourceURL(path, sourceURL string) error {
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
	nextPosition := *position
	if payload.NextPosition > nextPosition {
		nextPosition = payload.NextPosition
	}
	r.replica.mu.Lock()
	sourceFile := r.replica.LastSourceFile
	r.replica.mu.Unlock()
	if strings.TrimSpace(payload.LogName) != "" {
		sourceFile = payload.LogName
	}
	var sourcePosition *replicaSourcePosition
	if nextPosition > *position {
		sourcePosition = &replicaSourcePosition{File: sourceFile, Position: nextPosition}
	}
	if len(payload.Events) > 0 || sourcePosition != nil {
		if err := r.replica.applyWithPreparedXAAndNativeRelayAtSource(payload.Events, nil, nil, false, sourcePosition); err != nil {
			return wrapReplicationError(replicationErrorClassSQL, err)
		}
	}
	if sourcePosition != nil {
		*position = nextPosition
		r.replica.mu.Lock()
		r.replica.LastSourceFile = sourceFile
		r.replica.LastSourcePosition = nextPosition
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

type replicationErrorClass uint8

const (
	replicationErrorClassUnknown replicationErrorClass = iota
	replicationErrorClassIO
	replicationErrorClassSQL
)

type classifiedReplicationError struct {
	class replicationErrorClass
	err   error
}

func (e *classifiedReplicationError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *classifiedReplicationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func wrapReplicationError(class replicationErrorClass, err error) error {
	if err == nil {
		return nil
	}
	return &classifiedReplicationError{class: class, err: err}
}

func replicationErrorClassOf(err error) replicationErrorClass {
	var classified *classifiedReplicationError
	if errors.As(err, &classified) && classified != nil {
		return classified.class
	}
	return replicationErrorClassUnknown
}

func (r *Runtime) setError(err error) {
	if err == nil {
		return
	}
	errorText := err.Error()
	errorAt := time.Now().UTC()
	errorNumber := parseMySQLErrorNumber(err)
	errorClass := replicationErrorClassOf(err)
	r.mu.Lock()
	r.lastErr = errorText
	r.lastErrorNumber = errorNumber
	r.lastErrorAt = errorAt
	switch errorClass {
	case replicationErrorClassIO:
		r.lastIOError = errorText
		r.lastIOErrorNumber = errorNumber
		r.lastIOErrorAt = errorAt
	case replicationErrorClassSQL:
		r.lastSQLError = errorText
		r.lastSQLErrorNumber = errorNumber
		r.lastSQLErrorAt = errorAt
	}
	r.mu.Unlock()
	if r.replica != nil {
		r.replica.mu.Lock()
		r.replica.LastError = errorText
		r.replica.mu.Unlock()
	}
}

var mysqlErrorNumberPattern = regexp.MustCompile(`(?i)\berror\s+([0-9]{1,6})\b`)

func parseMySQLErrorNumber(err error) int64 {
	if err == nil {
		return 0
	}
	match := mysqlErrorNumberPattern.FindStringSubmatch(err.Error())
	if len(match) != 2 {
		return 0
	}
	number, parseErr := strconv.ParseInt(match[1], 10, 64)
	if parseErr != nil {
		return 0
	}
	return number
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
