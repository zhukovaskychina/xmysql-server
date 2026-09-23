package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const uuidShortSequenceLimit = 1 << 24

// UUIDShortGenerator implements MySQL's UUID_SHORT layout:
// (server_id << 46) + (server_start_time << 24) + increment.
// The state file keeps the last start-time/sequence pair so a fast restart
// cannot reuse values generated in the same second.
type UUIDShortGenerator struct {
	mu        sync.Mutex
	serverID  uint64
	startTime uint64
	next      uint64
	statePath string
}

type uuidShortState struct {
	StartTime uint64 `json:"start_time"`
	Next      uint64 `json:"next"`
}

// NewUUIDShortGenerator creates a generator for one server identity. A zero
// server ID is normalized to MySQL's default server ID of 1.
func NewUUIDShortGenerator(serverID uint32, statePath string) (*UUIDShortGenerator, error) {
	if serverID == 0 {
		serverID = 1
	}
	now := uint64(time.Now().Unix())
	generator := &UUIDShortGenerator{
		serverID:  uint64(serverID),
		startTime: now,
		statePath: stringsTrimSpace(statePath),
	}
	if generator.statePath == "" {
		return generator, nil
	}
	data, err := os.ReadFile(generator.statePath)
	if os.IsNotExist(err) {
		return generator, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read UUID_SHORT state: %w", err)
	}
	var state uuidShortState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode UUID_SHORT state: %w", err)
	}
	if state.StartTime != 0 {
		// Continue the durable tuple across restart even when the wall clock
		// crosses a second between construction and the first call.  This keeps
		// UUID_SHORT monotonic and prevents a restart from introducing a
		// 2^24-sized jump or reusing the previous sequence.
		generator.startTime = state.StartTime
		generator.next = state.Next
	}
	return generator, nil
}

// Next returns the next unique UUID_SHORT value and durably advances the
// sequence before returning it.
func (g *UUIDShortGenerator) Next() (uint64, error) {
	if g == nil {
		return 0, fmt.Errorf("UUID_SHORT generator is nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.next >= uuidShortSequenceLimit {
		return 0, fmt.Errorf("UUID_SHORT sequence exhausted for server start time")
	}
	value := (g.serverID << 46) + (g.startTime << 24) + g.next
	state := uuidShortState{StartTime: g.startTime, Next: g.next + 1}
	if err := g.persist(state); err != nil {
		return 0, err
	}
	g.next++
	return value, nil
}

func (g *UUIDShortGenerator) persist(state uuidShortState) error {
	if g.statePath == "" {
		return nil
	}
	if directory := filepath.Dir(g.statePath); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create UUID_SHORT state directory: %w", err)
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode UUID_SHORT state: %w", err)
	}
	temporary := g.statePath + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return fmt.Errorf("write UUID_SHORT state: %w", err)
	}
	if err := os.Rename(temporary, g.statePath); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("publish UUID_SHORT state: %w", err)
	}
	return nil
}

var defaultUUIDShort struct {
	sync.RWMutex
	generator *UUIDShortGenerator
}

func defaultUUIDShortGenerator() *UUIDShortGenerator {
	defaultUUIDShort.RLock()
	generator := defaultUUIDShort.generator
	defaultUUIDShort.RUnlock()
	if generator != nil {
		return generator
	}
	generator, _ = NewUUIDShortGenerator(1, "")
	defaultUUIDShort.Lock()
	if defaultUUIDShort.generator == nil {
		defaultUUIDShort.generator = generator
	} else {
		generator = defaultUUIDShort.generator
	}
	defaultUUIDShort.Unlock()
	return generator
}

// ConfigureDefaultUUIDShort replaces the process default used by SQL
// expression evaluation. The executor configures it from server_id and its
// data directory during startup.
func ConfigureDefaultUUIDShort(serverID uint32, statePath string) error {
	generator, err := NewUUIDShortGenerator(serverID, statePath)
	if err != nil {
		return err
	}
	defaultUUIDShort.Lock()
	defaultUUIDShort.generator = generator
	defaultUUIDShort.Unlock()
	return nil
}

func stringsTrimSpace(value string) string {
	start, end := 0, len(value)
	for start < end && (value[start] == ' ' || value[start] == '\t' || value[start] == '\r' || value[start] == '\n') {
		start++
	}
	for end > start && (value[end-1] == ' ' || value[end-1] == '\t' || value[end-1] == '\r' || value[end-1] == '\n') {
		end--
	}
	return value[start:end]
}
