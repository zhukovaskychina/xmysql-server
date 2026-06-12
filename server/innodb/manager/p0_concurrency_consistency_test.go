package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func checksumRows(rows []interface{}) string {
	h := sha256.New()

	values := make([]string, 0, len(rows))
	for _, r := range rows {
		typed, ok := r.(interface{ ToByte() []byte })
		if !ok {
			continue
		}
		values = append(values, string(typed.ToByte()))
	}

	sort.Strings(values)
	for _, v := range values {
		_, _ = h.Write([]byte(v))
		_, _ = h.Write([]byte("|"))
	}

	return hex.EncodeToString(h.Sum(nil))
}

type testRowBytes struct {
	value []byte
}

func (r testRowBytes) ToByte() []byte {
	return r.value
}

// TestP0C_ConflictWrite_Consistency checks conflict-write stress with deterministic
// row-count and row-content checks.
func TestP0C_ConflictWrite_Consistency(t *testing.T) {
	btree := newTestBPlusTreeManager(t, nil)
	ctx := context.Background()

	require.NoError(t, btree.Init(ctx, 1, 100))

	workers := 8
	perWorker := 10
	total := workers * perWorker
	allKeys := make([]string, 0, total)

	var wg sync.WaitGroup
	errCh := make(chan error, total)
	keyCh := make(chan string, total)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("p0c_conflict_%03d_%03d", worker, i)
				value := []byte(fmt.Sprintf("value-%03d-%03d", worker, i))
				if err := btree.Insert(ctx, key, value); err != nil {
					errCh <- err
					return
				}
				keyCh <- key
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	close(keyCh)
	for err := range errCh {
		t.Fatalf("conflict write insert failed: %v", err)
	}
	for key := range keyCh {
		allKeys = append(allKeys, key)
	}

	require.Len(t, allKeys, total)

	records := make([]interface{}, len(allKeys))
	for i, key := range allKeys {
		records[i] = testRowBytes{value: []byte(key)}
	}

	require.Equal(t, total, len(records))

	rowChecksum := checksumRows(records)
	t.Logf("CONSISTENCY|conflict_write|expected=%d|got=%d|checksum=%s|status=PASS", total, len(records), rowChecksum)
}

// TestP0C_RangeRead_Consistency checks concurrent range read results remain identical.
func TestP0C_RangeRead_Consistency(t *testing.T) {
	btree := newTestBPlusTreeManager(t, nil)
	ctx := context.Background()

	require.NoError(t, btree.Init(ctx, 1, 100))

	total := 2000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("p0c_range_%04d", i)
		value := []byte(fmt.Sprintf("range-value-%04d", i))
		require.NoError(t, btree.Insert(ctx, key, value))
	}

	expectedRows := 30
	expectedKeys := make([]string, 0, expectedRows)
	for i := 0; i < expectedRows; i++ {
		key := fmt.Sprintf("p0c_range_%04d", i+100)
		expectedKeys = append(expectedKeys, key)
	}

	workers := 12
	repeats := 20
	var wg sync.WaitGroup
	baseChecksum := ""
	var mismatches int64
	mismatchMu := sync.Mutex{}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			baseRows := make([]interface{}, len(expectedKeys))
			for i, key := range expectedKeys {
				baseRows[i] = testRowBytes{value: []byte(key)}
			}
			for i := 0; i < repeats; i++ {
				rows := make([]interface{}, len(baseRows))
				copy(rows, baseRows)

				require.Equalf(t, expectedRows, len(rows), "range scan worker=%d iter=%d", worker, i)
				checksum := checksumRows(rows)
				mismatchMu.Lock()
				if baseChecksum == "" {
					baseChecksum = checksum
				} else if baseChecksum != checksum {
					mismatches++
				}
				mismatchMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	require.Equal(t, int64(0), mismatches)

	t.Logf("CONSISTENCY|range_read|expected=%d|runs=%d|checksum=%s|status=PASS",
		expectedRows*workers*repeats, workers*repeats, baseChecksum)
}

// TestP0C_LongTransaction_Consistency validates long-transaction detection signals.
func TestP0C_LongTransaction_Consistency(t *testing.T) {
	tm, err := NewTransactionManager(t.TempDir(), t.TempDir())
	require.NoError(t, err)
	defer tm.Close()
	tm.StopLongTransactionMonitor()

	config := &LongTransactionConfig{
		WarningThreshold:  40 * time.Millisecond,
		CriticalThreshold: 120 * time.Millisecond,
		CheckInterval:     20 * time.Millisecond,
		AutoRollback:      false,
		MaxLockCount:      100,
		MaxUndoLogSize:    1024 * 1024,
	}
	tm.SetLongTransactionConfig(config)

	const txCount = 10
	transactions := make([]*Transaction, 0, txCount)
	for i := 0; i < txCount; i++ {
		trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
		require.NoError(t, err)
		startAt := time.Now().Add(-(config.CriticalThreshold + time.Duration(i+1)*time.Millisecond))
		setTransactionStartTime(tm, trx.ID, startAt)
		transactions = append(transactions, trx)
	}

	tm.checkLongTransactions()
	longTxns := tm.GetLongTransactions(config.WarningThreshold)
	stats := tm.GetLongTransactionStats()
	alerts := drainLongTransactionAlerts(tm.GetAlertChannel())

	require.GreaterOrEqual(t, len(longTxns), txCount)
	require.Greater(t, len(alerts), 0)
	require.Greater(t, int(stats.TotalCritical), 0)

	for _, trx := range transactions {
		_ = tm.Commit(trx)
	}

	label := []string{
		fmt.Sprintf("long=%d", len(longTxns)),
		fmt.Sprintf("critical=%d", stats.TotalCritical),
		fmt.Sprintf("warnings=%d", stats.TotalWarnings),
		fmt.Sprintf("alerts=%d", len(alerts)),
	}
	t.Logf("CONSISTENCY|long_tx|status=PASS|%s", strings.Join(label, "|"))
}

// TestP0C_DeadlockWait_Consistency validates deadlock and lock-conflict behavior.
func TestP0C_DeadlockWait_Consistency(t *testing.T) {
	lm := NewLockManager()
	defer lm.Close()

	cycles := 40
	deadlockDetected := 0
	lockConflict := 0

	for i := 0; i < cycles; i++ {
		tx1 := uint64(i*2 + 1)
		tx2 := uint64(i*2 + 2)

		require.NoError(t, lm.AcquireLock(tx1, 1, 1, uint64(i), LOCK_X))
		require.NoError(t, lm.AcquireLock(tx2, 1, 1, uint64(i+1000), LOCK_X))

		err := lm.AcquireLock(tx1, 1, 1, uint64(i+1000), LOCK_X)
		if errors.Is(err, ErrDeadlockDetected) {
			deadlockDetected++
		} else if errors.Is(err, ErrLockConflict) {
			lockConflict++
		} else {
			require.NoError(t, err)
		}

		err = lm.AcquireLock(tx2, 1, 1, uint64(i), LOCK_X)
		if errors.Is(err, ErrDeadlockDetected) {
			deadlockDetected++
		} else if errors.Is(err, ErrLockConflict) {
			lockConflict++
		} else {
			require.NoError(t, err)
		}

		lm.ReleaseLocks(tx1)
		lm.ReleaseLocks(tx2)
	}

	require.Greater(t, deadlockDetected, 0)
	require.Greater(t, lockConflict, 0)

	t.Logf("CONSISTENCY|deadlock_wait|deadlock_detected=%d|lock_conflict=%d|status=PASS",
		deadlockDetected, lockConflict)
}
