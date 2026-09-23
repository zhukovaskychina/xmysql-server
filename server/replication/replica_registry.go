package replication

import (
	"sort"
	"sync"
	"time"
)

// ReplicaRegistration is the non-sensitive identity a native replication
// client reports before it starts a binlog dump. It intentionally excludes
// the registration password.
type ReplicaRegistration struct {
	ServerID     uint32
	ReportHost   string
	ReportUser   string
	ReportPort   uint16
	MasterID     uint32
	RegisteredAt time.Time
}

type replicaRegistrationEntry struct {
	token string
	info  ReplicaRegistration
}

// ReplicaRegistry keeps the current native-replication registrations for one
// MySQL listener. A server ID is unique from the source's perspective; a new
// registration replaces the previous connection using that ID.
type ReplicaRegistry struct {
	mu      sync.RWMutex
	entries map[uint32]replicaRegistrationEntry
}

func NewReplicaRegistry() *ReplicaRegistry {
	return &ReplicaRegistry{entries: make(map[uint32]replicaRegistrationEntry)}
}

// Register adds or replaces a registration. token identifies the connection
// so an old connection cannot remove a newer registration with the same ID.
func (r *ReplicaRegistry) Register(token string, info ReplicaRegistration) {
	if r == nil || info.ServerID == 0 {
		return
	}
	if info.RegisteredAt.IsZero() {
		info.RegisteredAt = time.Now().UTC()
	}
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[uint32]replicaRegistrationEntry)
	}
	r.entries[info.ServerID] = replicaRegistrationEntry{token: token, info: info}
	r.mu.Unlock()
}

// Unregister removes the registration owned by token, if it is still the
// active connection for that server ID.
func (r *ReplicaRegistry) Unregister(token string, serverID uint32) {
	if r == nil || serverID == 0 {
		return
	}
	r.mu.Lock()
	if entry, ok := r.entries[serverID]; ok && entry.token == token {
		delete(r.entries, serverID)
	}
	r.mu.Unlock()
}

// Snapshot returns a stable, deterministic view for SHOW REPLICAS and
// SHOW SLAVE HOSTS. The returned slice is detached from the registry.
func (r *ReplicaRegistry) Snapshot() []ReplicaRegistration {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	result := make([]ReplicaRegistration, 0, len(r.entries))
	for _, entry := range r.entries {
		result = append(result, entry.info)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ServerID < result[j].ServerID })
	return result
}
