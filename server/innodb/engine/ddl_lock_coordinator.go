package engine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	observabilitymetrics "github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

// tableDDLCoordinator provides per-table reader/writer coordination for the
// compatibility DDL path. It deliberately does not claim to be the complete
// MySQL metadata-lock subsystem; it protects the in-process executor while a
// metadata ALTER is being applied.
type tableDDLCoordinator struct {
	mu    sync.Mutex
	locks map[string]*tableDDLTableLock
}

type tableDDLTableLock struct {
	readBarrier  sync.RWMutex
	writeBarrier sync.RWMutex
	stateMu      sync.Mutex
	owners       map[string]metadataLockOwner
	waiters      map[string]metadataLockWaiter
	history      []MetadataLockWaitEdge
}

const metadataLockWaitHistoryLimit = 128

type metadataLockOwner struct {
	mode       tableLockMode
	count      int
	acquiredAt time.Time
}

type metadataLockWaiter struct {
	mode        tableLockMode
	waitStarted time.Time
	blockers    map[string]tableLockMode
}

// MetadataLockSnapshot is the coordinator's real-time owner/waiter view for
// performance_schema.metadata_locks. Only acquisitions made through the
// owner-aware executor paths are exposed; direct test/helper lock calls keep
// the legacy behavior and intentionally have no synthetic owner.
type MetadataLockSnapshot struct {
	Table       string
	Owner       string
	Mode        tableLockMode
	Status      string
	AcquiredAt  time.Time
	WaitStarted time.Time
}

// MetadataLockWaitEdge is the table-level wait-for projection used by the
// Performance Schema compatibility views. It is derived only from real
// owner-aware acquisitions; legacy helper locks do not create synthetic
// edges.
type MetadataLockWaitEdge struct {
	Table         string
	WaitingOwner  string
	BlockingOwner string
	WaitingMode   tableLockMode
	BlockingMode  tableLockMode
	WaitStarted   time.Time
	WaitDuration  time.Duration
}

type tableLockMode string

const (
	tableLockRead   tableLockMode = "read"
	tableLockDML    tableLockMode = "dml"
	tableLockShared tableLockMode = "shared"
	tableLockWrite  tableLockMode = "write"
)

type sessionTableLockRequest struct {
	table string
	mode  string
}

type sessionTableLockLeaseEntry struct {
	table    string
	mode     string
	lockMode tableLockMode
	lock     *tableDDLTableLock
	owner    string
}

// sessionTableLockLease holds coordinator locks for the lifetime of a
// session's LOCK TABLES state. Keeping the lease separate from the legacy
// locked_tables map lets existing status/check code keep its simple shape
// while ensuring UNLOCK TABLES and connection reset really release barriers.
type sessionTableLockLease struct {
	entries []sessionTableLockLeaseEntry
}

func (l *sessionTableLockLease) release() {
	if l == nil {
		return
	}
	for i := len(l.entries) - 1; i >= 0; i-- {
		entry := l.entries[i]
		if entry.lock == nil {
			continue
		}
		mode := entry.lockMode
		if mode == "" {
			// LOCK TABLES stores SQL-level modes such as READ/READ LOCAL;
			// those both map to the shared coordinator lock. Transaction
			// leases set lockMode explicitly because they use read/DML locks.
			mode = tableLockShared
			if strings.EqualFold(strings.TrimSpace(entry.mode), "write") {
				mode = tableLockWrite
			}
		}
		entry.lock.unlockOwned(mode, entry.owner)
	}
	l.entries = nil
}

func newTableDDLCoordinator() *tableDDLCoordinator {
	return &tableDDLCoordinator{locks: make(map[string]*tableDDLTableLock)}
}

func (c *tableDDLCoordinator) lockFor(table string) *tableDDLTableLock {
	if c == nil {
		return &tableDDLTableLock{}
	}
	key := strings.ToLower(strings.TrimSpace(table))
	c.mu.Lock()
	defer c.mu.Unlock()
	if lock := c.locks[key]; lock != nil {
		return lock
	}
	lock := &tableDDLTableLock{owners: make(map[string]metadataLockOwner), waiters: make(map[string]metadataLockWaiter)}
	c.locks[key] = lock
	return lock
}

func (l *tableDDLTableLock) RLock()        { l.readBarrier.RLock() }
func (l *tableDDLTableLock) RUnlock()      { l.readBarrier.RUnlock() }
func (l *tableDDLTableLock) DMLRLock()     { l.writeBarrier.RLock() }
func (l *tableDDLTableLock) DMLRUnlock()   { l.writeBarrier.RUnlock() }
func (l *tableDDLTableLock) LockShared()   { l.writeBarrier.Lock() }
func (l *tableDDLTableLock) UnlockShared() { l.writeBarrier.Unlock() }
func (l *tableDDLTableLock) Lock() {
	l.writeBarrier.Lock()
	l.readBarrier.Lock()
}

func (l *tableDDLTableLock) lockWithContext(ctx context.Context, mode tableLockMode) error {
	return l.lockWithContextOwned(ctx, mode, "")
}

func (l *tableDDLTableLock) lockWithContextOwned(ctx context.Context, mode tableLockMode, owner string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	waitStarted := time.Now()
	upgradeFromShared := false
	ownerCount := 1
	if owner != "" {
		l.stateMu.Lock()
		if existing, exists := l.owners[owner]; exists {
			switch {
			case existing.mode == tableLockWrite || existing.mode == mode || (existing.mode == tableLockShared && mode == tableLockDML):
				if existing.count <= 0 {
					existing.count = 1
				}
				existing.count++
				l.owners[owner] = existing
				l.stateMu.Unlock()
				return nil
			case existing.mode == tableLockShared && mode == tableLockWrite:
				// A shared DDL lock already owns writeBarrier. Upgrade by taking
				// readBarrier while retaining that ownership; releasing and
				// reacquiring would create a race with another shared lock.
				upgradeFromShared = true
				ownerCount = existing.count
				if ownerCount <= 0 {
					ownerCount = 1
				}
				ownerCount++
				if l.waiters == nil {
					l.waiters = make(map[string]metadataLockWaiter)
				}
				l.waiters[owner] = metadataLockWaiter{mode: mode, waitStarted: waitStarted}
			default:
				l.stateMu.Unlock()
				return fmt.Errorf("metadata lock owner %s already holds %s lock and cannot acquire %s lock", owner, existing.mode, mode)
			}
		}
		l.stateMu.Unlock()
	}
	if owner != "" {
		l.stateMu.Lock()
		if l.waiters == nil {
			l.waiters = make(map[string]metadataLockWaiter)
		}
		l.waiters[owner] = metadataLockWaiter{mode: mode, waitStarted: waitStarted}
		l.stateMu.Unlock()
	}
	acquiredAndRecorded := false
	waited := false
	defer func() {
		if owner == "" || acquiredAndRecorded {
			return
		}
		l.stateMu.Lock()
		delete(l.waiters, owner)
		l.stateMu.Unlock()
	}()
	interval := time.NewTicker(time.Millisecond)
	defer interval.Stop()
	for {
		acquired := false
		switch mode {
		case tableLockRead:
			acquired = l.readBarrier.TryRLock()
		case tableLockDML:
			acquired = l.writeBarrier.TryRLock()
		case tableLockShared:
			acquired = l.writeBarrier.TryLock()
		case tableLockWrite:
			if upgradeFromShared {
				acquired = l.readBarrier.TryLock()
			} else if l.writeBarrier.TryLock() {
				if l.readBarrier.TryLock() {
					acquired = true
				} else {
					l.writeBarrier.Unlock()
				}
			}
		}
		if acquired {
			if owner != "" {
				l.stateMu.Lock()
				if waited {
					waitDuration := time.Since(waitStarted)
					l.recordWaitHistoryLocked(owner, l.waiters[owner], waitDuration)
					observabilitymetrics.DefaultRuntimeRecorder().RecordLockWait(string(mode), "metadata", waitDuration)
				}
				delete(l.waiters, owner)
				if l.owners == nil {
					l.owners = make(map[string]metadataLockOwner)
				}
				l.owners[owner] = metadataLockOwner{mode: mode, count: ownerCount, acquiredAt: time.Now()}
				l.stateMu.Unlock()
				acquiredAndRecorded = true
			}
			return nil
		}
		waited = true
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-interval.C:
		}
	}
}

func (l *tableDDLTableLock) recordWaitHistoryLocked(waitingOwner string, waiting metadataLockWaiter, duration time.Duration) {
	if waitingOwner == "" || waiting.waitStarted.IsZero() {
		return
	}
	if duration < 0 {
		duration = 0
	}
	ownerNames := make([]string, 0, len(waiting.blockers))
	for owner := range waiting.blockers {
		ownerNames = append(ownerNames, owner)
	}
	sort.Strings(ownerNames)
	for _, blockingOwner := range ownerNames {
		l.history = append(l.history, MetadataLockWaitEdge{
			WaitingOwner: waitingOwner, BlockingOwner: blockingOwner,
			WaitingMode: waiting.mode, BlockingMode: waiting.blockers[blockingOwner],
			WaitStarted: waiting.waitStarted, WaitDuration: duration,
		})
		if len(l.history) > metadataLockWaitHistoryLimit {
			l.history = append([]MetadataLockWaitEdge(nil), l.history[len(l.history)-metadataLockWaitHistoryLimit:]...)
		}
	}
}

func (l *tableDDLTableLock) recordWaitBlockerLocked(blockingOwner string, blockingMode tableLockMode) {
	if blockingOwner == "" {
		return
	}
	for waitingOwner, waiting := range l.waiters {
		if waitingOwner == blockingOwner || !metadataLockModesConflict(waiting.mode, blockingMode) {
			continue
		}
		if waiting.blockers == nil {
			waiting.blockers = make(map[string]tableLockMode)
		}
		waiting.blockers[blockingOwner] = blockingMode
		l.waiters[waitingOwner] = waiting
	}
}

func (l *tableDDLTableLock) unlockOwned(mode tableLockMode, owner string) {
	physicalMode := mode
	if owner != "" {
		l.stateMu.Lock()
		if existing, exists := l.owners[owner]; exists {
			physicalMode = existing.mode
			if existing.count <= 0 {
				existing.count = 1
			}
			if existing.count > 1 {
				existing.count--
				l.owners[owner] = existing
				l.stateMu.Unlock()
				return
			}
			l.recordWaitBlockerLocked(owner, existing.mode)
			delete(l.owners, owner)
		}
		delete(l.waiters, owner)
		l.stateMu.Unlock()
	}
	if physicalMode == tableLockWrite {
		l.Unlock()
	} else if physicalMode == tableLockShared {
		l.UnlockShared()
	} else if physicalMode == tableLockDML {
		l.DMLRUnlock()
	} else {
		l.RUnlock()
	}
}

func (l *tableDDLTableLock) Unlock() {
	l.readBarrier.Unlock()
	l.writeBarrier.Unlock()
}

func (c *tableDDLCoordinator) MetadataLocks() []MetadataLockSnapshot {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	tables := make([]string, 0, len(c.locks))
	locks := make(map[string]*tableDDLTableLock, len(c.locks))
	for table, lock := range c.locks {
		tables = append(tables, table)
		locks[table] = lock
	}
	c.mu.Unlock()
	sort.Strings(tables)
	result := make([]MetadataLockSnapshot, 0)
	for _, table := range tables {
		lock := locks[table]
		if lock == nil {
			continue
		}
		lock.stateMu.Lock()
		owners := make(map[string]metadataLockOwner, len(lock.owners))
		for owner, state := range lock.owners {
			owners[owner] = state
		}
		waiters := make(map[string]metadataLockWaiter, len(lock.waiters))
		for owner, state := range lock.waiters {
			waiters[owner] = state
		}
		lock.stateMu.Unlock()
		ownerNames := make([]string, 0, len(owners))
		for owner := range owners {
			ownerNames = append(ownerNames, owner)
		}
		sort.Strings(ownerNames)
		for _, owner := range ownerNames {
			state := owners[owner]
			result = append(result, MetadataLockSnapshot{Table: table, Owner: owner, Mode: state.mode, Status: "GRANTED", AcquiredAt: state.acquiredAt})
		}
		waiterNames := make([]string, 0, len(waiters))
		for owner := range waiters {
			waiterNames = append(waiterNames, owner)
		}
		sort.Strings(waiterNames)
		for _, owner := range waiterNames {
			state := waiters[owner]
			result = append(result, MetadataLockSnapshot{Table: table, Owner: owner, Mode: state.mode, Status: "PENDING", WaitStarted: state.waitStarted})
		}
	}
	return result
}

func (c *tableDDLCoordinator) MetadataLockWaitEdges() []MetadataLockWaitEdge {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	tables := make([]string, 0, len(c.locks))
	locks := make(map[string]*tableDDLTableLock, len(c.locks))
	for table, lock := range c.locks {
		tables = append(tables, table)
		locks[table] = lock
	}
	c.mu.Unlock()
	sort.Strings(tables)
	edges := make([]MetadataLockWaitEdge, 0)
	for _, table := range tables {
		lock := locks[table]
		if lock == nil {
			continue
		}
		lock.stateMu.Lock()
		owners := make(map[string]metadataLockOwner, len(lock.owners))
		for owner, state := range lock.owners {
			owners[owner] = state
		}
		waiters := make(map[string]metadataLockWaiter, len(lock.waiters))
		for owner, state := range lock.waiters {
			waiters[owner] = state
		}
		lock.stateMu.Unlock()
		ownerNames := make([]string, 0, len(owners))
		for owner := range owners {
			ownerNames = append(ownerNames, owner)
		}
		sort.Strings(ownerNames)
		waiterNames := make([]string, 0, len(waiters))
		for waiter := range waiters {
			waiterNames = append(waiterNames, waiter)
		}
		sort.Strings(waiterNames)
		for _, waiter := range waiterNames {
			waiting := waiters[waiter]
			for _, owner := range ownerNames {
				blocking := owners[owner]
				if waiter == owner || !metadataLockModesConflict(waiting.mode, blocking.mode) {
					continue
				}
				edges = append(edges, MetadataLockWaitEdge{
					Table: table, WaitingOwner: waiter, BlockingOwner: owner,
					WaitingMode: waiting.mode, BlockingMode: blocking.mode,
					WaitStarted: waiting.waitStarted, WaitDuration: time.Since(waiting.waitStarted),
				})
			}
		}
	}
	return edges
}

// MetadataLockWaitHistory returns completed owner-aware metadata-lock waits.
// It is intentionally bounded and excludes canceled acquisitions, legacy
// helper locks, and compatible owner pairs that never blocked one another.
func (c *tableDDLCoordinator) MetadataLockWaitHistory() []MetadataLockWaitEdge {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	tables := make([]string, 0, len(c.locks))
	locks := make(map[string]*tableDDLTableLock, len(c.locks))
	for table, lock := range c.locks {
		tables = append(tables, table)
		locks[table] = lock
	}
	c.mu.Unlock()
	sort.Strings(tables)
	history := make([]MetadataLockWaitEdge, 0)
	for _, table := range tables {
		lock := locks[table]
		if lock == nil {
			continue
		}
		lock.stateMu.Lock()
		for _, edge := range lock.history {
			edge.Table = table
			history = append(history, edge)
		}
		lock.stateMu.Unlock()
	}
	sort.SliceStable(history, func(i, j int) bool {
		if history[i].WaitStarted.Equal(history[j].WaitStarted) {
			if history[i].Table == history[j].Table {
				if history[i].WaitingOwner == history[j].WaitingOwner {
					return history[i].BlockingOwner < history[j].BlockingOwner
				}
				return history[i].WaitingOwner < history[j].WaitingOwner
			}
			return history[i].Table < history[j].Table
		}
		return history[i].WaitStarted.Before(history[j].WaitStarted)
	})
	return history
}

func metadataLockModesConflict(waiting, blocking tableLockMode) bool {
	switch waiting {
	case tableLockRead:
		return blocking == tableLockWrite
	case tableLockDML:
		return blocking == tableLockShared || blocking == tableLockWrite
	case tableLockShared:
		return blocking == tableLockDML || blocking == tableLockShared || blocking == tableLockWrite
	case tableLockWrite:
		return blocking == tableLockRead || blocking == tableLockDML || blocking == tableLockShared || blocking == tableLockWrite
	default:
		return true
	}
}

func (e *XMySQLExecutor) getDDLCoordinator() *tableDDLCoordinator {
	if e == nil {
		return newTableDDLCoordinator()
	}
	e.ddlCoordinatorMu.Lock()
	defer e.ddlCoordinatorMu.Unlock()
	if e.ddlCoordinator == nil {
		e.ddlCoordinator = newTableDDLCoordinator()
	}
	return e.ddlCoordinator
}

func (e *XMySQLExecutor) releaseSessionTableLocks(session server.MySQLServerSession) {
	if session == nil {
		return
	}
	if lease, ok := session.GetParamByName("__table_lock_lease").(*sessionTableLockLease); ok {
		lease.release()
	}
	session.SetParamByName("__table_lock_lease", nil)
	session.SetParamByName("locked_tables", map[string]string{})
}

func (e *XMySQLExecutor) releaseSessionTransactionTableLocks(session server.MySQLServerSession) {
	if session == nil {
		return
	}
	if lease, ok := session.GetParamByName("__transaction_table_lock_lease").(*sessionTableLockLease); ok {
		lease.release()
	}
	session.SetParamByName("__transaction_table_lock_lease", nil)
}

func ddlMetadataLockOwner(session server.MySQLServerSession) string {
	if id := sessionConnectionID(session); id != 0 {
		return fmt.Sprintf("thread/%d", id)
	}
	return ""
}

func (e *XMySQLExecutor) acquireSessionTableLocks(ctx context.Context, session server.MySQLServerSession, requests []sessionTableLockRequest) error {
	e.releaseSessionTableLocks(session)
	if ctx == nil {
		ctx = context.Background()
	}
	sorted := append([]sessionTableLockRequest(nil), requests...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].table < sorted[j].table })
	lease := &sessionTableLockLease{entries: make([]sessionTableLockLeaseEntry, 0, len(sorted))}
	session.SetParamByName("__table_lock_lease", lease)
	legacy := make(map[string]string, len(sorted))
	owner := ddlMetadataLockOwner(session)
	for _, request := range sorted {
		lock := e.getDDLCoordinator().lockFor(request.table)
		mode := tableLockShared
		if request.mode == "write" {
			mode = tableLockWrite
		}
		if err := lock.lockWithContextOwned(ctx, mode, owner); err != nil {
			e.releaseSessionTableLocks(session)
			return err
		}
		lease.entries = append(lease.entries, sessionTableLockLeaseEntry{table: request.table, mode: request.mode, lockMode: mode, lock: lock, owner: owner})
		_, table := compatibilityQualifiedTable(request.table, "")
		legacy[strings.ToLower(table)] = request.mode
	}
	session.SetParamByName("locked_tables", legacy)
	return nil
}

const defaultInnoDBLockWaitTimeout = 50 * time.Second

func sessionTableLockWaitDuration(session server.MySQLServerSession) time.Duration {
	seconds := int64(defaultInnoDBLockWaitTimeout / time.Second)
	if session != nil {
		switch value := session.GetParamByName("innodb_lock_wait_timeout").(type) {
		case int:
			seconds = int64(value)
		case int64:
			seconds = value
		case uint64:
			if value <= uint64(^uint64(0)>>1) {
				seconds = int64(value)
			}
		case string:
			if parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
				seconds = parsed
			}
		}
	}
	if seconds <= 0 {
		return time.Second
	}
	return time.Duration(seconds) * time.Second
}

func tableLockContext(parent context.Context, session server.MySQLServerSession) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, sessionTableLockWaitDuration(session))
}

func tableLockWaitError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("lock wait timeout exceeded; try restarting transaction: %w", err)
	}
	return err
}

func (e *XMySQLExecutor) acquireStatementTableLocks(ctx *ExecutionContext, session server.MySQLServerSession, query, databaseName string) (func(), error) {
	tables := statementTableAccesses(query, databaseName)
	if len(tables) == 0 {
		return func() {}, nil
	}
	lockContext := context.Background()
	if ctx != nil && ctx.Context != nil {
		lockContext = ctx.Context
	}
	lockContext, cancel := tableLockContext(lockContext, session)
	acquired := make([]sessionTableLockLeaseEntry, 0, len(tables))
	lockMode := tableLockRead
	if statementTableWrites(query) {
		lockMode = tableLockDML
	}
	holdForTransaction := sessionTransactionActive(session)
	owner := ddlMetadataLockOwner(session)
	for _, table := range tables {
		if ddlWriteLockHeld(ctx, table) || sessionHoldsTableLock(session, table) || sessionHoldsTransactionTableLock(session, table) {
			continue
		}
		lock := e.getDDLCoordinator().lockFor(table)
		if err := lock.lockWithContextOwned(lockContext, lockMode, owner); err != nil {
			for i := len(acquired) - 1; i >= 0; i-- {
				acquired[i].lock.unlockOwned(tableLockMode(acquired[i].mode), acquired[i].owner)
			}
			cancel()
			return nil, tableLockWaitError(err)
		}
		acquired = append(acquired, sessionTableLockLeaseEntry{table: table, mode: string(lockMode), lockMode: lockMode, lock: lock, owner: owner})
	}
	if holdForTransaction {
		lease, _ := session.GetParamByName("__transaction_table_lock_lease").(*sessionTableLockLease)
		if lease == nil {
			lease = &sessionTableLockLease{}
			session.SetParamByName("__transaction_table_lock_lease", lease)
		}
		lease.entries = append(lease.entries, acquired...)
		cancel()
		return func() {}, nil
	}
	return func() {
		for i := len(acquired) - 1; i >= 0; i-- {
			acquired[i].lock.unlockOwned(tableLockMode(acquired[i].mode), acquired[i].owner)
		}
		cancel()
	}, nil
}

func sessionTableLockMode(session server.MySQLServerSession, table string) (string, bool) {
	if session == nil {
		return "", false
	}
	value := session.GetParamByName("locked_tables")
	locks, ok := value.(map[string]string)
	if !ok {
		return "", false
	}
	mode, exists := locks[strings.ToLower(strings.Trim(strings.TrimSpace(table), "`"))]
	return mode, exists
}

func sessionHoldsTableLock(session server.MySQLServerSession, table string) bool {
	if session == nil {
		return false
	}
	lease, ok := session.GetParamByName("__table_lock_lease").(*sessionTableLockLease)
	if !ok || lease == nil {
		return false
	}
	target := strings.ToLower(strings.TrimSpace(table))
	for _, entry := range lease.entries {
		if entry.table == target {
			return true
		}
	}
	return false
}

func sessionHoldsTransactionTableLock(session server.MySQLServerSession, table string) bool {
	if session == nil {
		return false
	}
	lease, ok := session.GetParamByName("__transaction_table_lock_lease").(*sessionTableLockLease)
	if !ok || lease == nil {
		return false
	}
	target := strings.ToLower(strings.TrimSpace(table))
	for _, entry := range lease.entries {
		if entry.table == target {
			return true
		}
	}
	return false
}

func sessionHasTransactionTableLocks(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	lease, ok := session.GetParamByName("__transaction_table_lock_lease").(*sessionTableLockLease)
	return ok && lease != nil && len(lease.entries) > 0
}

func (e *XMySQLExecutor) prepareDDLImplicitCommit(session server.MySQLServerSession) error {
	if session == nil || (!sessionBoolParam(session, "in_transaction") && !sessionHasTransactionTableLocks(session)) {
		return nil
	}
	return e.commitAutocommitTransaction(session)
}

func (e *XMySQLExecutor) acquireDDLTableLocks(ctx *ExecutionContext, session server.MySQLServerSession, tables []string) (func(), error) {
	seen := make(map[string]struct{}, len(tables))
	keys := make([]string, 0, len(tables))
	for _, table := range tables {
		key := strings.ToLower(strings.TrimSpace(table))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	releases := make([]func(), 0, len(keys))
	for _, table := range keys {
		if sessionHoldsTableLock(session, table) {
			continue
		}
		release, err := e.acquireDDLWriteLock(ctx, table, false)
		if err != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
			return nil, tableLockWaitError(err)
		}
		releases = append(releases, release)
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}, nil
}

// acquireDatabaseDDLTableLocks serializes database-wide destructive DDL with
// transaction-scoped table metadata locks held by other sessions. DROP
// DATABASE must not remove a table while another connection is still using its
// definition inside an active transaction. The table list is sorted by
// acquireDDLTableLocks so concurrent multi-table/database DDL uses one lock
// order and does not introduce an avoidable lock-order cycle.
func (e *XMySQLExecutor) acquireDatabaseDDLTableLocks(ctx *ExecutionContext, session server.MySQLServerSession, database string) (func(), error) {
	if e == nil || e.tableStorageManager == nil || strings.TrimSpace(database) == "" {
		return func() {}, nil
	}
	tables := make([]string, 0)
	for _, info := range e.tableStorageManager.ListAllTables() {
		if !strings.EqualFold(strings.TrimSpace(info.SchemaName), strings.TrimSpace(database)) {
			continue
		}
		tableName := strings.TrimSpace(info.TableName)
		if tableName == "" {
			continue
		}
		tables = append(tables, strings.ToLower(strings.TrimSpace(database))+"."+strings.ToLower(tableName))
	}
	return e.acquireDDLTableLocks(ctx, session, tables)
}

var statementTablePattern = regexp.MustCompile(`(?is)^\s*(?:select\s+.+?\s+from|insert\s+(?:ignore\s+)?into|replace\s+into|update|delete\s+from)\s+` + "`?" + `([a-zA-Z0-9_$]+)` + "`?")
var alterTablePattern = regexp.MustCompile("(?is)^\\s*alter\\s+table\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)")
var alterLockPattern = regexp.MustCompile(`(?is)\block\s*=\s*(default|none|shared|exclusive)\b`)
var statementTablesPattern = regexp.MustCompile("(?is)\\b(?:from|join|update|into)\\s+((?:`?[a-zA-Z0-9_$]+`?\\s*\\.\\s*)?`?[a-zA-Z0-9_$]+`?)")
var lockingReadPattern = regexp.MustCompile(`(?is)\bfor\s+(?:update|share)\b|\block\s+in\s+share\s+mode\b`)

func statementTableAccess(query, databaseName string) (string, bool) {
	tables := statementTableAccesses(query, databaseName)
	if len(tables) == 0 {
		return "", false
	}
	return tables[0], true
}

func statementTableAccesses(query, databaseName string) []string {
	matches := statementTablesPattern.FindAllStringSubmatch(query, -1)
	seen := make(map[string]struct{}, len(matches))
	tables := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		database, table := compatibilityQualifiedTable(match[1], databaseName)
		if database == "" || table == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(database)) + "." + strings.ToLower(strings.TrimSpace(table))
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, key)
	}
	sort.Strings(tables)
	return tables
}

// viewSourceTableAccesses returns physical table references used by a view
// definition. The generic table-access helper intentionally works from SQL
// text for lock acquisition, but INFORMATION_SCHEMA.VIEW_TABLE_USAGE must not
// expose a CTE name as though it were a persistent table.
func viewSourceTableAccesses(query, databaseName string) []string {
	tables := statementTableAccesses(query, databaseName)
	statement, err := sqlparser.Parse(query)
	if err != nil || statement == nil {
		return tables
	}
	cteNames := make(map[string]struct{})
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		cte, ok := node.(*sqlparser.CTEDefinition)
		if ok && cte != nil {
			cteNames[strings.ToLower(strings.TrimSpace(cte.Name.String()))] = struct{}{}
		}
		return true, nil
	}, statement)
	if len(cteNames) == 0 {
		return tables
	}
	filtered := make([]string, 0, len(tables))
	for _, table := range tables {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) == 2 {
			if _, isCTE := cteNames[strings.ToLower(strings.TrimSpace(parts[1]))]; isCTE {
				continue
			}
		}
		filtered = append(filtered, table)
	}
	return filtered
}

func statementTableWrites(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	for _, prefix := range []string{"insert ", "replace ", "update ", "delete "} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return strings.HasPrefix(lower, "select ") && lockingReadPattern.MatchString(lower)
}

func alterTableAccess(query, databaseName string) (string, bool) {
	match := alterTablePattern.FindStringSubmatch(strings.TrimSpace(query))
	if len(match) == 0 {
		return "", false
	}
	return strings.ToLower(strings.TrimSpace(databaseName)) + "." + strings.ToLower(match[1]), true
}

func alterRequiresWriteLock(query string) bool {
	match := alterLockPattern.FindStringSubmatch(query)
	return len(match) == 0 || !strings.EqualFold(match[1], "none")
}

func alterUsesSharedLock(query string) bool {
	match := alterLockPattern.FindStringSubmatch(query)
	return len(match) > 0 && strings.EqualFold(match[1], "shared")
}

func (e *XMySQLExecutor) acquireDDLWriteLock(ctx *ExecutionContext, table string, shared bool) (func(), error) {
	if ctx != nil {
		ctx.mu.Lock()
		if ctx.ddlWriteLocks == nil {
			ctx.ddlWriteLocks = make(map[string]int)
		}
		if ctx.ddlWriteLocks[table] > 0 {
			ctx.ddlWriteLocks[table]++
			ctx.mu.Unlock()
			return func() {
				ctx.mu.Lock()
				if ctx.ddlWriteLocks[table] <= 1 {
					delete(ctx.ddlWriteLocks, table)
				} else {
					ctx.ddlWriteLocks[table]--
				}
				ctx.mu.Unlock()
			}, nil
		}
		ctx.ddlWriteLocks[table] = 1
		ctx.mu.Unlock()
	}

	lock := e.getDDLCoordinator().lockFor(table)
	lockContext := context.Background()
	var session server.MySQLServerSession
	if ctx != nil && ctx.Context != nil {
		lockContext = ctx.Context
	}
	if ctx != nil {
		session = ctx.Session
	}
	lockContext, cancel := tableLockContext(lockContext, session)
	mode := tableLockWrite
	if shared {
		mode = tableLockShared
	}
	owner := ddlMetadataLockOwner(session)
	if err := lock.lockWithContextOwned(lockContext, mode, owner); err != nil {
		cancel()
		if ctx != nil {
			ctx.mu.Lock()
			delete(ctx.ddlWriteLocks, table)
			ctx.mu.Unlock()
		}
		return nil, err
	}
	return func() {
		lock.unlockOwned(mode, owner)
		cancel()
		if ctx != nil {
			ctx.mu.Lock()
			if ctx.ddlWriteLocks[table] <= 1 {
				delete(ctx.ddlWriteLocks, table)
			} else {
				ctx.ddlWriteLocks[table]--
			}
			ctx.mu.Unlock()
		}
	}, nil
}

func (e *XMySQLExecutor) acquireSimpleDDLTableLock(ctx *ExecutionContext, session server.MySQLServerSession, stmtTable sqlparser.TableName, action, databaseName string) (func(), error) {
	if action != "create" && action != "drop" && action != "truncate" && action != "alter" {
		return func() {}, nil
	}
	if sessionBoolParam(session, "in_transaction") || sessionHasTransactionTableLocks(session) {
		if err := e.commitAutocommitTransaction(session); err != nil {
			return nil, err
		}
	}
	tableName := strings.TrimSpace(stmtTable.Name.String())
	database := strings.TrimSpace(stmtTable.Qualifier.String())
	if database == "" {
		database = strings.TrimSpace(databaseName)
	}
	if database == "" || tableName == "" || sessionHoldsTableLock(session, strings.ToLower(database+"."+tableName)) {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = &ExecutionContext{Context: context.Background(), Session: session}
	} else if ctx.Session == nil {
		ctx.Session = session
	}
	table := strings.ToLower(database + "." + tableName)
	release, err := e.acquireDDLWriteLock(ctx, table, false)
	if err != nil {
		return nil, tableLockWaitError(err)
	}
	return release, nil
}

func ddlWriteLockHeld(ctx *ExecutionContext, table string) bool {
	if ctx == nil {
		return false
	}
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.ddlWriteLocks[table] > 0
}
