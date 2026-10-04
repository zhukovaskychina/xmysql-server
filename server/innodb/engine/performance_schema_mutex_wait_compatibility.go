package engine

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// performanceSchemaMutexWait is the runtime wait record for an executor-owned
// sync.Mutex. It is intentionally separate from the owner sidecar: the owner
// describes the thread that currently holds the mutex, while this record
// describes a thread blocked waiting to acquire it.
type performanceSchemaMutexWait struct {
	sequence            uint64
	threadID            int64
	eventName           string
	objectInstanceBegin int64
	started             time.Time
	waitDuration        time.Duration
	instrumented        bool
	timed               bool
}

func (e *XMySQLExecutor) lockPerformanceSchemaMutex(mutex *sync.Mutex, instrument string, threadID int64) {
	if mutex == nil {
		return
	}
	if mutex.TryLock() {
		e.recordPerformanceSchemaMutexOwner(instrument, threadID)
		return
	}
	token := e.beginPerformanceSchemaMutexWait(instrument, threadID)
	mutex.Lock()
	e.finishPerformanceSchemaMutexWait(token)
	e.recordPerformanceSchemaMutexOwner(instrument, threadID)
}

func (e *XMySQLExecutor) recordPerformanceSchemaMutexOwner(instrument string, threadID int64) {
	if e == nil {
		return
	}
	e.performanceSchemaMutexOwnerMu.Lock()
	if e.performanceSchemaMutexOwners == nil {
		e.performanceSchemaMutexOwners = make(map[string]int64)
	}
	if threadID > 0 {
		e.performanceSchemaMutexOwners[instrument] = threadID
	} else {
		delete(e.performanceSchemaMutexOwners, instrument)
	}
	e.performanceSchemaMutexOwnerMu.Unlock()
}

func (e *XMySQLExecutor) beginPerformanceSchemaMutexWait(instrument string, threadID int64) uint64 {
	if e == nil || threadID <= 0 || strings.TrimSpace(instrument) == "" {
		return 0
	}
	instrumented, _ := e.performanceSchemaObservedWait(instrument, 0)
	if !instrumented {
		return 0
	}
	_, timed := e.performanceSchemaInstrumentSetting(instrument)
	token := e.performanceSchemaMutexWaitSequence.Add(1)
	wait := performanceSchemaMutexWait{
		sequence:            token,
		threadID:            threadID,
		eventName:           instrument,
		objectInstanceBegin: e.performanceSchemaMutexObjectInstance(instrument),
		started:             time.Now(),
		instrumented:        true,
		timed:               timed,
	}
	e.performanceSchemaMutexWaitMu.Lock()
	if e.performanceSchemaMutexWaits == nil {
		e.performanceSchemaMutexWaits = make(map[uint64]performanceSchemaMutexWait)
	}
	e.performanceSchemaMutexWaits[token] = wait
	e.performanceSchemaMutexWaitMu.Unlock()
	return token
}

func (e *XMySQLExecutor) finishPerformanceSchemaMutexWait(token uint64) {
	if e == nil || token == 0 {
		return
	}
	e.performanceSchemaMutexWaitMu.Lock()
	wait, ok := e.performanceSchemaMutexWaits[token]
	if !ok {
		e.performanceSchemaMutexWaitMu.Unlock()
		return
	}
	delete(e.performanceSchemaMutexWaits, token)
	wait.waitDuration = time.Since(wait.started)
	shortLimit := e.performanceSchemaHistorySize("performance_schema_events_waits_history_size", metadataLockWaitHistoryLimit)
	longLimit := e.performanceSchemaHistorySize("performance_schema_events_waits_history_long_size", metadataLockWaitHistoryLongLimit)
	if longLimit > 0 {
		e.performanceSchemaMutexWaitHistory = append(e.performanceSchemaMutexWaitHistory, wait)
		if len(e.performanceSchemaMutexWaitHistory) > longLimit {
			e.performanceSchemaMutexWaitHistory = append([]performanceSchemaMutexWait(nil), e.performanceSchemaMutexWaitHistory[len(e.performanceSchemaMutexWaitHistory)-longLimit:]...)
		}
	}
	if shortLimit > 0 {
		if e.performanceSchemaMutexWaitHistoryByThread == nil {
			e.performanceSchemaMutexWaitHistoryByThread = make(map[int64][]performanceSchemaMutexWait)
		}
		history := append(e.performanceSchemaMutexWaitHistoryByThread[wait.threadID], wait)
		if len(history) > shortLimit {
			history = history[len(history)-shortLimit:]
		}
		e.performanceSchemaMutexWaitHistoryByThread[wait.threadID] = append([]performanceSchemaMutexWait(nil), history...)
	} else if e.performanceSchemaMutexWaitHistoryByThread != nil {
		delete(e.performanceSchemaMutexWaitHistoryByThread, wait.threadID)
	}
	e.performanceSchemaMutexWaitMu.Unlock()
}

func (e *XMySQLExecutor) performanceSchemaMutexObjectInstance(instrument string) int64 {
	if e != nil {
		for _, instance := range e.performanceSchemaMutexInstances() {
			if strings.EqualFold(instance.name, instrument) {
				return instance.objectInstanceBegin
			}
		}
	}
	return performanceSchemaStableObjectInstance(instrument)
}

func (e *XMySQLExecutor) performanceSchemaMutexWaitSnapshots(includeHistory, historyLong bool) (current, history []performanceSchemaMutexWait) {
	if e == nil {
		return nil, nil
	}
	e.performanceSchemaMutexWaitMu.RLock()
	current = make([]performanceSchemaMutexWait, 0, len(e.performanceSchemaMutexWaits))
	for _, wait := range e.performanceSchemaMutexWaits {
		current = append(current, wait)
	}
	if includeHistory {
		if historyLong {
			history = append(history, e.performanceSchemaMutexWaitHistory...)
		} else {
			for _, waits := range e.performanceSchemaMutexWaitHistoryByThread {
				history = append(history, waits...)
			}
		}
	}
	e.performanceSchemaMutexWaitMu.RUnlock()
	sort.Slice(current, func(i, j int) bool { return current[i].sequence < current[j].sequence })
	sort.Slice(history, func(i, j int) bool { return history[i].sequence < history[j].sequence })
	return current, history
}

func (e *XMySQLExecutor) resetPerformanceSchemaMutexWaitHistory(long bool) {
	if e == nil {
		return
	}
	e.performanceSchemaMutexWaitMu.Lock()
	if long {
		e.performanceSchemaMutexWaitHistory = nil
	} else {
		e.performanceSchemaMutexWaitHistoryByThread = make(map[int64][]performanceSchemaMutexWait)
	}
	e.performanceSchemaMutexWaitMu.Unlock()
}

func (e *XMySQLExecutor) performanceSchemaMutexWaitSummaryResetCutoff() uint64 {
	if e == nil {
		return 0
	}
	e.performanceSchemaMutexWaitMu.RLock()
	sequence := e.performanceSchemaMutexWaitSummaryResetSequence
	e.performanceSchemaMutexWaitMu.RUnlock()
	return sequence
}

func (e *XMySQLExecutor) trimPerformanceSchemaMutexWaitHistory(shortLimit, longLimit int) {
	if e == nil {
		return
	}
	e.performanceSchemaMutexWaitMu.Lock()
	if longLimit <= 0 {
		e.performanceSchemaMutexWaitHistory = nil
	} else if len(e.performanceSchemaMutexWaitHistory) > longLimit {
		e.performanceSchemaMutexWaitHistory = append([]performanceSchemaMutexWait(nil), e.performanceSchemaMutexWaitHistory[len(e.performanceSchemaMutexWaitHistory)-longLimit:]...)
	}
	if shortLimit <= 0 {
		e.performanceSchemaMutexWaitHistoryByThread = make(map[int64][]performanceSchemaMutexWait)
	} else {
		for threadID, history := range e.performanceSchemaMutexWaitHistoryByThread {
			if len(history) > shortLimit {
				e.performanceSchemaMutexWaitHistoryByThread[threadID] = append([]performanceSchemaMutexWait(nil), history[len(history)-shortLimit:]...)
			}
		}
	}
	e.performanceSchemaMutexWaitMu.Unlock()
}
