package engine

import (
	"sort"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
)

type performanceSchemaGlobalReadLockWait struct {
	threadID     int64
	sequence     uint64
	started      time.Time
	waitDuration time.Duration
	instrumented bool
	timed        bool
}

func (e *XMySQLExecutor) beginPerformanceSchemaGlobalReadLockWait(session server.MySQLServerSession, started time.Time) func() {
	if e == nil || session == nil || !e.performanceSchemaInstrumentationConsumersEnabled() {
		return func() {}
	}
	enabled, timed := e.performanceSchemaInstrumentSetting(performanceSchemaGlobalReadLockConditionInstrument)
	if !enabled {
		return func() {}
	}
	threadID := int64(sessionConnectionID(session))
	if threadID == 0 {
		if identity, _, _ := performanceSchemaSessionIdentity(session); identity != 0 {
			threadID = identity
		}
	}
	wait := performanceSchemaGlobalReadLockWait{
		threadID:     threadID,
		started:      started,
		instrumented: true,
		timed:        timed,
	}
	token := e.performanceSchemaGlobalReadLockWaitSequence.Add(1)
	wait.sequence = token
	e.performanceSchemaGlobalReadLockWaitMu.Lock()
	if e.performanceSchemaGlobalReadLockWaits == nil {
		e.performanceSchemaGlobalReadLockWaits = make(map[uint64]performanceSchemaGlobalReadLockWait)
	}
	e.performanceSchemaGlobalReadLockWaits[token] = wait
	e.performanceSchemaGlobalReadLockWaitMu.Unlock()
	return func() {
		wait.waitDuration = time.Since(wait.started)
		if wait.waitDuration < 0 {
			wait.waitDuration = 0
		}
		e.performanceSchemaGlobalReadLockWaitMu.Lock()
		delete(e.performanceSchemaGlobalReadLockWaits, token)
		shortLimit := e.performanceSchemaHistorySize("performance_schema_events_waits_history_size", metadataLockWaitHistoryLimit)
		if shortLimit > 0 {
			if e.performanceSchemaGlobalReadLockWaitHistoryByThread == nil {
				e.performanceSchemaGlobalReadLockWaitHistoryByThread = make(map[int64][]performanceSchemaGlobalReadLockWait)
			}
			shortHistory := append(e.performanceSchemaGlobalReadLockWaitHistoryByThread[wait.threadID], wait)
			if len(shortHistory) > shortLimit {
				shortHistory = append([]performanceSchemaGlobalReadLockWait(nil), shortHistory[len(shortHistory)-shortLimit:]...)
			}
			e.performanceSchemaGlobalReadLockWaitHistoryByThread[wait.threadID] = shortHistory
		} else {
			delete(e.performanceSchemaGlobalReadLockWaitHistoryByThread, wait.threadID)
		}
		longLimit := e.performanceSchemaHistorySize("performance_schema_events_waits_history_long_size", metadataLockWaitHistoryLongLimit)
		if longLimit > 0 {
			e.performanceSchemaGlobalReadLockWaitHistory = append(e.performanceSchemaGlobalReadLockWaitHistory, wait)
			if len(e.performanceSchemaGlobalReadLockWaitHistory) > longLimit {
				e.performanceSchemaGlobalReadLockWaitHistory = append([]performanceSchemaGlobalReadLockWait(nil), e.performanceSchemaGlobalReadLockWaitHistory[len(e.performanceSchemaGlobalReadLockWaitHistory)-longLimit:]...)
			}
		} else {
			e.performanceSchemaGlobalReadLockWaitHistory = e.performanceSchemaGlobalReadLockWaitHistory[:0]
		}
		e.performanceSchemaGlobalReadLockWaitMu.Unlock()
	}
}

func (e *XMySQLExecutor) performanceSchemaGlobalReadLockWaitSnapshots(includeHistory bool) (current, history []performanceSchemaGlobalReadLockWait) {
	if e == nil {
		return nil, nil
	}
	e.performanceSchemaGlobalReadLockWaitMu.RLock()
	for _, wait := range e.performanceSchemaGlobalReadLockWaits {
		waitCopy := wait
		waitCopy.waitDuration = time.Since(wait.started)
		if waitCopy.waitDuration < 0 {
			waitCopy.waitDuration = 0
		}
		current = append(current, waitCopy)
	}
	if includeHistory {
		history = append(history, e.performanceSchemaGlobalReadLockWaitHistory...)
	}
	e.performanceSchemaGlobalReadLockWaitMu.RUnlock()
	return current, history
}

// performanceSchemaGlobalReadLockWaitHistorySnapshot projects the per-thread
// history view from the independently retained long-history order. The long
// history is the authoritative completion order; the short view keeps only
// the newest configured number of waits for each thread.
func (e *XMySQLExecutor) performanceSchemaGlobalReadLockWaitHistorySnapshot(historyLong bool) []performanceSchemaGlobalReadLockWait {
	if e == nil {
		return nil
	}
	e.performanceSchemaGlobalReadLockWaitMu.RLock()
	result := make([]performanceSchemaGlobalReadLockWait, 0)
	if historyLong {
		result = append(result, e.performanceSchemaGlobalReadLockWaitHistory...)
	} else {
		for _, waits := range e.performanceSchemaGlobalReadLockWaitHistoryByThread {
			result = append(result, waits...)
		}
	}
	e.performanceSchemaGlobalReadLockWaitMu.RUnlock()
	sort.SliceStable(result, func(i, j int) bool { return result[i].sequence < result[j].sequence })
	return result
}

func (e *XMySQLExecutor) performanceSchemaGlobalReadLockWaitSummaryResetCutoff() uint64 {
	if e == nil {
		return 0
	}
	e.performanceSchemaGlobalReadLockWaitMu.RLock()
	sequence := e.performanceSchemaGlobalReadLockWaitSummaryResetSequence
	e.performanceSchemaGlobalReadLockWaitMu.RUnlock()
	return sequence
}

func (e *XMySQLExecutor) performanceSchemaGlobalReadLockWaitSummaryByInstanceResetCutoff() uint64 {
	if e == nil {
		return 0
	}
	e.performanceSchemaGlobalReadLockWaitMu.RLock()
	sequence := e.performanceSchemaGlobalReadLockWaitSummaryByInstanceResetSequence
	e.performanceSchemaGlobalReadLockWaitMu.RUnlock()
	return sequence
}
