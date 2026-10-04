package engine

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server"
)

const performanceSchemaDDLCoordinatorRWLockInstrument = "wait/synch/rwlock/sql/xmysql/table_ddl"
const performanceSchemaGlobalReadLockConditionInstrument = "wait/synch/cond/sql/xmysql/global_read_lock"
const performanceSchemaXAMutexInstrument = "wait/synch/mutex/sql/xmysql/xa"

type performanceSchemaConditionRuntimeInstance struct {
	name                string
	identity            string
	objectInstanceBegin int64
}

// performanceSchemaConditionInstances projects the condition variable used
// by the context-aware global read-lock gate. The gate still keeps its
// close-and-replace notification channel so blocked SQL can honor context
// cancellation; the sync.Cond is the authoritative runtime object used for
// broadcast notification and for instance identity.
func (e *XMySQLExecutor) performanceSchemaConditionInstances() []performanceSchemaConditionRuntimeInstance {
	if e == nil {
		return nil
	}
	e.globalReadLock.ensureInitialized()
	pointer := reflect.ValueOf(e.globalReadLock.cond).Pointer()
	objectInstanceBegin := int64(pointer)
	if objectInstanceBegin == 0 {
		objectInstanceBegin = performanceSchemaStableObjectInstance(performanceSchemaGlobalReadLockConditionInstrument)
	}
	return []performanceSchemaConditionRuntimeInstance{{
		name:                performanceSchemaGlobalReadLockConditionInstrument,
		identity:            fmt.Sprintf("%s/%x", performanceSchemaGlobalReadLockConditionInstrument, pointer),
		objectInstanceBegin: objectInstanceBegin,
	}}
}

// performanceSchemaGlobalReadLockConditionObjectInstanceBegin returns the
// same runtime identity used by cond_instances and the current/history wait
// views. Keeping the summary tables on this identity is important because
// MySQL treats OBJECT_INSTANCE_BEGIN as the identity of the instrumented
// synchronization object, not as a table-local synthetic key.
func (e *XMySQLExecutor) performanceSchemaGlobalReadLockConditionObjectInstanceBegin() int64 {
	for _, instance := range e.performanceSchemaConditionInstances() {
		if instance.name == performanceSchemaGlobalReadLockConditionInstrument {
			return instance.objectInstanceBegin
		}
	}
	return performanceSchemaStableObjectInstance(performanceSchemaGlobalReadLockConditionInstrument)
}

func (e *XMySQLExecutor) performanceSchemaConditionInstancesForQuery() []performanceSchemaConditionRuntimeInstance {
	if e == nil {
		return nil
	}
	all := make([]performanceSchemaConditionRuntimeInstance, 0, len(performanceSchemaConditionInstrumentNames))
	for _, instance := range e.performanceSchemaConditionInstances() {
		enabled, _ := e.performanceSchemaInstrumentSetting(instance.name)
		if enabled {
			all = append(all, instance)
		}
	}
	capacity := e.performanceSchemaCapacity("performance_schema_max_cond_instances")
	if capacity > len(all) {
		capacity = len(all)
	}
	selected := all[:capacity]
	overflow := all[capacity:]
	e.performanceSchemaMu.Lock()
	if e.performanceSchemaCondInstanceLostKeys == nil {
		e.performanceSchemaCondInstanceLostKeys = make(map[string]struct{})
	}
	seen := make(map[string]struct{}, len(overflow))
	for _, instance := range overflow {
		seen[instance.identity] = struct{}{}
		e.performanceSchemaCondInstanceLostKeys[instance.identity] = struct{}{}
	}
	for identity := range e.performanceSchemaCondInstanceLostKeys {
		if _, exists := seen[identity]; !exists {
			delete(e.performanceSchemaCondInstanceLostKeys, identity)
		}
	}
	e.performanceSchemaCondInstancesLost.Store(int64(len(e.performanceSchemaCondInstanceLostKeys)))
	e.performanceSchemaMu.Unlock()
	return selected
}

func (e *XMySQLExecutor) refreshPerformanceSchemaConditionInstancesLost() {
	if e == nil {
		return
	}
	if !e.performanceSchemaAnyConditionInstrumentEnabled() {
		e.performanceSchemaMu.Lock()
		e.performanceSchemaCondInstanceLostKeys = make(map[string]struct{})
		e.performanceSchemaCondInstancesLost.Store(0)
		e.performanceSchemaMu.Unlock()
		return
	}
	_ = e.performanceSchemaConditionInstancesForQuery()
}

func (e *XMySQLExecutor) performanceSchemaAnyConditionInstrumentEnabled() bool {
	for _, name := range performanceSchemaConditionInstrumentNames {
		enabled, _ := e.performanceSchemaInstrumentSetting(name)
		if enabled {
			return true
		}
	}
	return false
}

type performanceSchemaMutexRuntimeInstance struct {
	name                string
	identity            string
	objectInstanceBegin int64
	lockedByThreadID    interface{}
}

// performanceSchemaMutexInstances projects a deliberately bounded set of
// executor-owned sync.Mutex objects. Go's sync.Mutex does not expose an owner
// thread id, so LOCKED_BY_THREAD_ID remains NULL; exposing an invented owner
// would be less compatible than preserving the official nullable shape.
func (e *XMySQLExecutor) performanceSchemaMutexInstances() []performanceSchemaMutexRuntimeInstance {
	if e == nil {
		return nil
	}
	objects := []struct {
		name  string
		mutex *sync.Mutex
	}{
		{name: "wait/synch/mutex/sql/xmysql/account", mutex: &e.accountMu},
		{name: "wait/synch/mutex/sql/xmysql/ddl_coordinator", mutex: &e.ddlCoordinatorMu},
		{name: "wait/synch/mutex/sql/xmysql/global_read_lock_state", mutex: &e.globalReadLockStateMu},
		{name: "wait/synch/mutex/sql/xmysql/active_query", mutex: &e.activeQueryMu},
		{name: "wait/synch/mutex/sql/xmysql/xa", mutex: &e.xaMu},
	}
	instances := make([]performanceSchemaMutexRuntimeInstance, 0, len(objects))
	for _, object := range objects {
		pointer := reflect.ValueOf(object.mutex).Pointer()
		objectInstanceBegin := int64(pointer)
		if objectInstanceBegin == 0 {
			objectInstanceBegin = performanceSchemaStableObjectInstance(object.name)
		}
		instances = append(instances, performanceSchemaMutexRuntimeInstance{
			name:                object.name,
			identity:            fmt.Sprintf("%s/%x", object.name, pointer),
			objectInstanceBegin: objectInstanceBegin,
			lockedByThreadID:    e.performanceSchemaMutexOwner(object.name),
		})
	}
	return instances
}

func (e *XMySQLExecutor) unlockPerformanceSchemaMutex(mutex *sync.Mutex, instrument string) {
	if e != nil {
		e.performanceSchemaMutexOwnerMu.Lock()
		delete(e.performanceSchemaMutexOwners, instrument)
		e.performanceSchemaMutexOwnerMu.Unlock()
	}
	if mutex != nil {
		mutex.Unlock()
	}
}

func (e *XMySQLExecutor) performanceSchemaMutexOwner(instrument string) interface{} {
	if e == nil {
		return nil
	}
	e.performanceSchemaMutexOwnerMu.RLock()
	threadID := e.performanceSchemaMutexOwners[instrument]
	e.performanceSchemaMutexOwnerMu.RUnlock()
	if threadID <= 0 {
		return nil
	}
	return threadID
}

func (e *XMySQLExecutor) lockXAMutex(session server.MySQLServerSession) {
	e.lockPerformanceSchemaMutex(&e.xaMu, performanceSchemaXAMutexInstrument, int64(sessionConnectionID(session)))
}

func (e *XMySQLExecutor) unlockXAMutex() {
	e.unlockPerformanceSchemaMutex(&e.xaMu, performanceSchemaXAMutexInstrument)
}

func (e *XMySQLExecutor) performanceSchemaMutexInstancesForQuery() []performanceSchemaMutexRuntimeInstance {
	if e == nil {
		return nil
	}
	all := make([]performanceSchemaMutexRuntimeInstance, 0, len(performanceSchemaMutexInstrumentNames))
	for _, instance := range e.performanceSchemaMutexInstances() {
		enabled, _ := e.performanceSchemaInstrumentSetting(instance.name)
		if enabled {
			all = append(all, instance)
		}
	}
	capacity := e.performanceSchemaCapacity("performance_schema_max_mutex_instances")
	if capacity > len(all) {
		capacity = len(all)
	}
	selected := all[:capacity]
	overflow := all[capacity:]
	e.performanceSchemaMu.Lock()
	if e.performanceSchemaMutexInstanceLostKeys == nil {
		e.performanceSchemaMutexInstanceLostKeys = make(map[string]struct{})
	}
	seen := make(map[string]struct{}, len(overflow))
	for _, instance := range overflow {
		seen[instance.identity] = struct{}{}
		e.performanceSchemaMutexInstanceLostKeys[instance.identity] = struct{}{}
	}
	for identity := range e.performanceSchemaMutexInstanceLostKeys {
		if _, exists := seen[identity]; !exists {
			delete(e.performanceSchemaMutexInstanceLostKeys, identity)
		}
	}
	e.performanceSchemaMutexInstancesLost.Store(int64(len(e.performanceSchemaMutexInstanceLostKeys)))
	e.performanceSchemaMu.Unlock()
	return selected
}

func (e *XMySQLExecutor) refreshPerformanceSchemaMutexInstancesLost() {
	if e == nil {
		return
	}
	if !e.performanceSchemaAnyMutexInstrumentEnabled() {
		e.performanceSchemaMu.Lock()
		e.performanceSchemaMutexInstanceLostKeys = make(map[string]struct{})
		e.performanceSchemaMutexInstancesLost.Store(0)
		e.performanceSchemaMu.Unlock()
		return
	}
	_ = e.performanceSchemaMutexInstancesForQuery()
}

func (e *XMySQLExecutor) performanceSchemaAnyMutexInstrumentEnabled() bool {
	for _, name := range performanceSchemaMutexInstrumentNames {
		enabled, _ := e.performanceSchemaInstrumentSetting(name)
		if enabled {
			return true
		}
	}
	return false
}

type performanceSchemaRWLockRuntimeInstance struct {
	name                  string
	identity              string
	objectInstanceBegin   int64
	writeLockedByThreadID interface{}
	readLockedByCount     int64
}

// performanceSchemaRWLockInstances projects the real table-level RWMutex
// objects owned by the DDL coordinator. It deliberately does not claim that
// every Go lock in the server is a MySQL instrument; only locks with an
// owner-aware coordinator state are exposed here.
func (c *tableDDLCoordinator) performanceSchemaRWLockInstances() []performanceSchemaRWLockRuntimeInstance {
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
	instances := make([]performanceSchemaRWLockRuntimeInstance, 0, len(tables))
	for _, table := range tables {
		lock := locks[table]
		if lock == nil {
			continue
		}
		writeOwner := interface{}(nil)
		readCount := int64(0)
		lock.stateMu.Lock()
		owners := make(map[string]metadataLockOwner, len(lock.owners))
		for owner, state := range lock.owners {
			owners[owner] = state
		}
		lock.stateMu.Unlock()
		ownerNames := make([]string, 0, len(owners))
		for owner := range owners {
			ownerNames = append(ownerNames, owner)
		}
		sort.Strings(ownerNames)
		for _, owner := range ownerNames {
			state := owners[owner]
			if state.mode == tableLockWrite {
				writeOwner = metadataLockThreadID(owner)
				continue
			}
			count := state.count
			if count < 1 {
				count = 1
			}
			readCount += int64(count)
		}
		identity := performanceSchemaDDLCoordinatorRWLockInstrument + "/" + table
		instances = append(instances, performanceSchemaRWLockRuntimeInstance{
			name:                  performanceSchemaDDLCoordinatorRWLockInstrument,
			identity:              identity,
			objectInstanceBegin:   performanceSchemaStableObjectInstance(identity),
			writeLockedByThreadID: writeOwner,
			readLockedByCount:     readCount,
		})
	}
	return instances
}

func (e *XMySQLExecutor) performanceSchemaRWLockInstancesForQuery() []performanceSchemaRWLockRuntimeInstance {
	if e == nil || e.ddlCoordinator == nil {
		return nil
	}
	all := e.ddlCoordinator.performanceSchemaRWLockInstances()
	capacity := e.performanceSchemaCapacity("performance_schema_max_rwlock_instances")
	if capacity > len(all) {
		capacity = len(all)
	}
	selected := all[:capacity]
	overflow := all[capacity:]
	e.performanceSchemaMu.Lock()
	if e.performanceSchemaRWLockInstanceLostKeys == nil {
		e.performanceSchemaRWLockInstanceLostKeys = make(map[string]struct{})
	}
	seen := make(map[string]struct{}, len(overflow))
	for _, instance := range overflow {
		seen[instance.identity] = struct{}{}
		e.performanceSchemaRWLockInstanceLostKeys[instance.identity] = struct{}{}
	}
	for identity := range e.performanceSchemaRWLockInstanceLostKeys {
		if _, exists := seen[identity]; !exists {
			delete(e.performanceSchemaRWLockInstanceLostKeys, identity)
		}
	}
	e.performanceSchemaRWLockInstancesLost.Store(int64(len(e.performanceSchemaRWLockInstanceLostKeys)))
	e.performanceSchemaMu.Unlock()
	return selected
}

func (e *XMySQLExecutor) refreshPerformanceSchemaRWLockInstancesLost() {
	if e == nil {
		return
	}
	enabled, _ := e.performanceSchemaInstrumentSetting(performanceSchemaDDLCoordinatorRWLockInstrument)
	if !enabled {
		e.performanceSchemaMu.Lock()
		e.performanceSchemaRWLockInstanceLostKeys = make(map[string]struct{})
		e.performanceSchemaRWLockInstancesLost.Store(0)
		e.performanceSchemaMu.Unlock()
		return
	}
	_ = e.performanceSchemaRWLockInstancesForQuery()
}

func performanceSchemaStableObjectInstance(name string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(name))))
	value := int64(hash.Sum64() & 0x7fffffffffffffff)
	if value == 0 {
		return 1
	}
	return value
}
