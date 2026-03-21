package manager

import (
	"sync"
	"testing"
	"time"
)

func setTransactionStartTime(tm *TransactionManager, trxID int64, startTime time.Time) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if trx, ok := tm.activeTransactions[trxID]; ok {
		trx.StartTime = startTime
	}
}

func drainLongTransactionAlerts(alertChan <-chan *LongTransactionAlert) []*LongTransactionAlert {
	alerts := make([]*LongTransactionAlert, 0)

	for {
		select {
		case alert := <-alertChan:
			if alert == nil {
				return alerts
			}
			alerts = append(alerts, alert)
		default:
			return alerts
		}
	}
}

// TestLongTransactionDetection 测试长事务检测
func TestLongTransactionDetection(t *testing.T) {
	// 创建临时目录
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create TransactionManager: %v", err)
	}
	defer tm.Close()
	tm.StopLongTransactionMonitor()

	// 设置较短的阈值用于测试
	config := &LongTransactionConfig{
		WarningThreshold:  100 * time.Millisecond,
		CriticalThreshold: 500 * time.Millisecond,
		CheckInterval:     50 * time.Millisecond,
		AutoRollback:      false,
		MaxLockCount:      100,
		MaxUndoLogSize:    1024 * 1024,
	}
	tm.SetLongTransactionConfig(config)

	// 创建一个长事务
	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	setTransactionStartTime(tm, trx.ID, time.Now().Add(-(config.CriticalThreshold + 100*time.Millisecond)))
	tm.checkLongTransactions()

	// 检查统计信息
	stats := tm.GetLongTransactionStats()
	t.Logf("Stats: Warnings=%d, Critical=%d, CurrentLongTxns=%d",
		stats.TotalWarnings, stats.TotalCritical, stats.CurrentLongTxns)

	if stats.TotalWarnings == 0 && stats.TotalCritical == 0 {
		t.Error("Expected at least one alert, got none")
	}

	alerts := drainLongTransactionAlerts(tm.GetAlertChannel())
	if len(alerts) == 0 {
		t.Fatal("Expected at least one alert to be sent")
	}
	if alerts[0].TrxID != trx.ID {
		t.Fatalf("Expected first alert for transaction %d, got %d", trx.ID, alerts[0].TrxID)
	}

	// 提交事务
	if err := tm.Commit(trx); err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}
}

// TestLongTransactionAutoRollback 测试长事务自动回滚
func TestLongTransactionAutoRollback(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create TransactionManager: %v", err)
	}
	defer tm.Close()
	tm.StopLongTransactionMonitor()

	// 启用自动回滚，设置更短的阈值
	config := &LongTransactionConfig{
		WarningThreshold:  50 * time.Millisecond,
		CriticalThreshold: 200 * time.Millisecond,
		CheckInterval:     30 * time.Millisecond,
		AutoRollback:      true,
		MaxLockCount:      100,
		MaxUndoLogSize:    1024 * 1024,
	}
	tm.SetLongTransactionConfig(config)

	// 验证配置已设置
	verifyConfig := tm.GetLongTransactionConfig()
	t.Logf("Config: AutoRollback=%v, CriticalThreshold=%v", verifyConfig.AutoRollback, verifyConfig.CriticalThreshold)

	if !verifyConfig.AutoRollback {
		t.Fatal("AutoRollback should be true")
	}

	// 创建事务
	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	trxID := trx.ID
	t.Logf("Created transaction ID=%d", trxID)

	setTransactionStartTime(tm, trxID, time.Now().Add(-(config.CriticalThreshold + 100*time.Millisecond)))
	tm.checkLongTransactions()

	// 检查事务是否被自动回滚
	stats := tm.GetLongTransactionStats()
	t.Logf("Auto rollbacks: %d, Critical: %d, Warnings: %d", stats.TotalAutoRollbacks, stats.TotalCritical, stats.TotalWarnings)

	if stats.TotalCritical == 0 {
		t.Error("Expected at least one critical alert")
	}
	if stats.TotalAutoRollbacks != 1 {
		t.Errorf("Expected 1 auto rollback, got %d", stats.TotalAutoRollbacks)
	}
	if tm.GetTransaction(trxID) != nil {
		t.Errorf("Expected transaction %d to be removed after auto rollback", trxID)
	}
	if trx.State != TRX_STATE_ROLLED_BACK {
		t.Errorf("Expected transaction state %d, got %d", TRX_STATE_ROLLED_BACK, trx.State)
	}

	alerts := drainLongTransactionAlerts(tm.GetAlertChannel())
	if len(alerts) == 0 {
		t.Fatal("Expected critical alert to be sent")
	}
	if alerts[0].Level != LONG_TXN_LEVEL_CRITICAL {
		t.Fatalf("Expected critical alert, got %s", alerts[0].Level)
	}
}

// TestGetLongTransactions 测试获取长事务列表
func TestGetLongTransactions(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create TransactionManager: %v", err)
	}
	defer tm.Close()

	// 创建多个事务
	trx1, _ := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	trx2, _ := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	trx3, _ := tm.Begin(false, TRX_ISO_REPEATABLE_READ)

	now := time.Now()
	setTransactionStartTime(tm, trx1.ID, now.Add(-250*time.Millisecond))
	setTransactionStartTime(tm, trx2.ID, now.Add(-180*time.Millisecond))
	setTransactionStartTime(tm, trx3.ID, now.Add(-50*time.Millisecond))

	// 获取运行超过150ms的事务
	longTxns := tm.GetLongTransactions(150 * time.Millisecond)

	// 应该有2个长事务（trx1和trx2）
	if len(longTxns) != 2 {
		t.Errorf("Expected 2 long transactions, got %d", len(longTxns))
	}

	t.Logf("Found %d long transactions", len(longTxns))
	for _, trx := range longTxns {
		duration := time.Since(trx.StartTime)
		t.Logf("Long transaction: ID=%d, Duration=%v", trx.ID, duration)
	}

	// 清理
	tm.Commit(trx1)
	tm.Commit(trx2)
	tm.Commit(trx3)
}

// TestUpdateTransactionMetrics 测试更新事务指标
func TestUpdateTransactionMetrics(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create TransactionManager: %v", err)
	}
	defer tm.Close()
	tm.StopLongTransactionMonitor()

	// 创建事务
	trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 更新锁数量
	tm.UpdateTransactionLockCount(trx.ID, 50)

	// 更新Undo日志大小
	tm.UpdateTransactionUndoLogSize(trx.ID, 1024*1024)

	// 更新活跃时间
	tm.UpdateTransactionActivity(trx.ID)

	// 验证更新
	updatedTrx := tm.GetTransaction(trx.ID)
	if updatedTrx.LockCount != 50 {
		t.Errorf("Expected LockCount=50, got %d", updatedTrx.LockCount)
	}

	if updatedTrx.UndoLogSize != 1024*1024 {
		t.Errorf("Expected UndoLogSize=1048576, got %d", updatedTrx.UndoLogSize)
	}

	// 清理
	tm.Commit(trx)
}

// TestLongTransactionConfig 测试配置管理
func TestLongTransactionConfig(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create TransactionManager: %v", err)
	}
	defer tm.Close()

	// 设置新配置
	newConfig := &LongTransactionConfig{
		WarningThreshold:  1 * time.Minute,
		CriticalThreshold: 10 * time.Minute,
		CheckInterval:     30 * time.Second,
		AutoRollback:      true,
		MaxLockCount:      5000,
		MaxUndoLogSize:    500 * 1024 * 1024,
	}
	tm.SetLongTransactionConfig(newConfig)

	// 获取配置并验证
	config := tm.GetLongTransactionConfig()

	if config.WarningThreshold != 1*time.Minute {
		t.Errorf("Expected WarningThreshold=1m, got %v", config.WarningThreshold)
	}

	if config.CriticalThreshold != 10*time.Minute {
		t.Errorf("Expected CriticalThreshold=10m, got %v", config.CriticalThreshold)
	}

	if config.CheckInterval != 30*time.Second {
		t.Errorf("Expected CheckInterval=30s, got %v", config.CheckInterval)
	}

	if !config.AutoRollback {
		t.Error("Expected AutoRollback=true")
	}

	if config.MaxLockCount != 5000 {
		t.Errorf("Expected MaxLockCount=5000, got %d", config.MaxLockCount)
	}

	if config.MaxUndoLogSize != 500*1024*1024 {
		t.Errorf("Expected MaxUndoLogSize=524288000, got %d", config.MaxUndoLogSize)
	}
}

// TestConcurrentLongTransactionDetection 测试并发长事务检测
func TestConcurrentLongTransactionDetection(t *testing.T) {
	redoDir := t.TempDir()
	undoDir := t.TempDir()

	tm, err := NewTransactionManager(redoDir, undoDir)
	if err != nil {
		t.Fatalf("Failed to create TransactionManager: %v", err)
	}
	defer tm.Close()

	// 设置配置
	config := &LongTransactionConfig{
		WarningThreshold:  100 * time.Millisecond,
		CriticalThreshold: 500 * time.Millisecond,
		CheckInterval:     50 * time.Millisecond,
		AutoRollback:      false,
		MaxLockCount:      100,
		MaxUndoLogSize:    1024 * 1024,
	}
	tm.SetLongTransactionConfig(config)

	// 并发创建多个事务
	const numTxns = 10
	var wg sync.WaitGroup
	wg.Add(numTxns)
	ready := make(chan *Transaction, numTxns)
	release := make(chan struct{})

	for i := 0; i < numTxns; i++ {
		go func(id int) {
			defer wg.Done()

			trx, err := tm.Begin(false, TRX_ISO_REPEATABLE_READ)
			if err != nil {
				t.Errorf("Failed to begin transaction %d: %v", id, err)
				return
			}
			setTransactionStartTime(tm, trx.ID, time.Now().Add(-time.Duration(50+id*20)*time.Millisecond))
			ready <- trx

			<-release

			// 提交事务
			if err := tm.Commit(trx); err != nil {
				t.Errorf("Failed to commit transaction %d: %v", id, err)
			}
		}(i)
	}

	for i := 0; i < numTxns; i++ {
		<-ready
	}

	tm.checkLongTransactions()
	close(release)

	wg.Wait()

	// 检查统计
	stats := tm.GetLongTransactionStats()
	t.Logf("Concurrent test stats: Warnings=%d, Critical=%d",
		stats.TotalWarnings, stats.TotalCritical)
	if stats.TotalWarnings == 0 {
		t.Error("Expected at least one warning during concurrent detection")
	}
}
