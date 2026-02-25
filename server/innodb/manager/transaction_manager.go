package manager

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	formatmvcc "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

var (
	ErrInvalidTrxState = errors.New("invalid transaction state")
)

// 长事务告警级别
const (
	LONG_TXN_LEVEL_WARNING  = "WARNING"  // 警告级别
	LONG_TXN_LEVEL_CRITICAL = "CRITICAL" // 严重级别
)

// 事务状态
const (
	TRX_STATE_NOT_STARTED uint8 = iota
	TRX_STATE_ACTIVE
	TRX_STATE_PREPARED
	TRX_STATE_COMMITTED
	TRX_STATE_ROLLED_BACK
)

// 事务隔离级别
const (
	TRX_ISO_READ_UNCOMMITTED uint8 = iota
	TRX_ISO_READ_COMMITTED
	TRX_ISO_REPEATABLE_READ
	TRX_ISO_SERIALIZABLE
)

// Transaction 表示一个事务
type Transaction struct {
	ID             int64                 // 事务ID
	State          uint8                 // 事务状态
	IsolationLevel uint8                 // 隔离级别
	StartTime      time.Time             // 开始时间
	LastActiveTime time.Time             // 最后活跃时间
	ReadView       *formatmvcc.ReadView  // MVCC读视图
	UndoLogs       []UndoLogEntry        // Undo日志
	RedoLogs       []RedoLogEntry        // Redo日志
	IsReadOnly     bool                  // 是否只读事务
	LockCount      int                   // 持有的锁数量
	UndoLogSize    uint64                // Undo日志大小
	Savepoints     map[string]*Savepoint // 保存点（新增）
}

// Savepoint 保存点
type Savepoint struct {
	Name         string    // 保存点名称
	LSN          uint64    // 保存点对应的LSN
	UndoLogCount int       // 保存点时的Undo日志数量
	RedoLogCount int       // 保存点时的Redo日志数量
	CreatedAt    time.Time // 创建时间
}

// LongTransactionAlert 长事务告警
type LongTransactionAlert struct {
	TrxID          int64         // 事务ID
	Level          string        // 告警级别
	Duration       time.Duration // 运行时长
	LockCount      int           // 持有的锁数量
	UndoLogSize    uint64        // Undo日志大小
	IsolationLevel uint8         // 隔离级别
	IsReadOnly     bool          // 是否只读
	Timestamp      time.Time     // 告警时间
	Message        string        // 告警消息
}

// LongTransactionConfig 长事务检测配置
type LongTransactionConfig struct {
	WarningThreshold  time.Duration // 警告阈值（默认30秒）
	CriticalThreshold time.Duration // 严重阈值（默认5分钟）
	CheckInterval     time.Duration // 检查间隔（默认10秒）
	AutoRollback      bool          // 是否自动回滚超时事务
	MaxLockCount      int           // 最大锁数量阈值
	MaxUndoLogSize    uint64        // 最大Undo日志大小阈值（字节）
}

// LongTransactionStats 长事务统计
type LongTransactionStats struct {
	sync.RWMutex
	TotalWarnings      uint64 // 总警告次数
	TotalCritical      uint64 // 总严重告警次数
	TotalAutoRollbacks uint64 // 总自动回滚次数
	CurrentLongTxns    int    // 当前长事务数量
	MaxDuration        time.Duration
	LastCheckTime      time.Time
}

// TransactionManager 事务管理器
type TransactionManager struct {
	mu                 sync.RWMutex
	nextTrxID          int64                  // 下一个事务ID
	activeTransactions map[int64]*Transaction // 活跃事务

	// 日志管理器
	redoManager *RedoLogManager
	undoManager *UndoLogManager

	// 默认配置
	defaultIsolationLevel uint8
	defaultTimeout        time.Duration

	// 长事务检测
	longTxnConfig  *LongTransactionConfig
	longTxnStats   *LongTransactionStats
	alertChan      chan *LongTransactionAlert
	stopMonitor    chan struct{}
	monitorRunning bool
	monitorWg      sync.WaitGroup
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(redoDir, undoDir string) (*TransactionManager, error) {
	redoManager, err := NewRedoLogManager(redoDir, 1000)
	if err != nil {
		return nil, err
	}

	undoManager, err := NewUndoLogManager(undoDir)
	if err != nil {
		return nil, err
	}

	// 默认长事务检测配置
	longTxnConfig := &LongTransactionConfig{
		WarningThreshold:  30 * time.Second,  // 30秒警告
		CriticalThreshold: 5 * time.Minute,   // 5分钟严重告警
		CheckInterval:     10 * time.Second,  // 10秒检查一次
		AutoRollback:      false,             // 默认不自动回滚
		MaxLockCount:      1000,              // 最大1000个锁
		MaxUndoLogSize:    100 * 1024 * 1024, // 100MB Undo日志
	}

	tm := &TransactionManager{
		nextTrxID:             1,
		activeTransactions:    make(map[int64]*Transaction),
		redoManager:           redoManager,
		undoManager:           undoManager,
		defaultIsolationLevel: TRX_ISO_REPEATABLE_READ,
		defaultTimeout:        time.Hour,
		longTxnConfig:         longTxnConfig,
		longTxnStats: &LongTransactionStats{
			LastCheckTime: time.Now(),
		},
		alertChan:   make(chan *LongTransactionAlert, 100),
		stopMonitor: make(chan struct{}),
	}

	// 启动长事务监控
	tm.StartLongTransactionMonitor()

	return tm, nil
}

// Begin 开始新事务
func (tm *TransactionManager) Begin(isReadOnly bool, isolationLevel uint8) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 分配事务ID
	trxID := atomic.AddInt64(&tm.nextTrxID, 1)

	// 创建事务对象
	trx := &Transaction{
		ID:             trxID,
		State:          TRX_STATE_ACTIVE,
		IsolationLevel: isolationLevel,
		StartTime:      time.Now(),
		LastActiveTime: time.Now(),
		IsReadOnly:     isReadOnly,
		Savepoints:     make(map[string]*Savepoint), // 初始化保存点map
	}

	// 创建ReadView（对于RR和RC隔离级别）
	if isolationLevel >= TRX_ISO_READ_COMMITTED {
		trx.ReadView = tm.createReadView(trxID)
	}

	// 记录活跃事务
	tm.activeTransactions[trxID] = trx

	return trx, nil
}

// Commit 提交事务
func (tm *TransactionManager) Commit(trx *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 写入Redo日志
	for _, redoLog := range trx.RedoLogs {
		if _, err := tm.redoManager.Append(&redoLog); err != nil {
			return err
		}
	}

	// 确保Redo日志持久化
	if err := tm.redoManager.Flush(0); err != nil {
		return err
	}

	// 更新事务状态
	trx.State = TRX_STATE_COMMITTED
	trx.LastActiveTime = time.Now()

	// 清理Undo日志
	tm.undoManager.Cleanup(trx.ID)

	// 移除活跃事务记录
	delete(tm.activeTransactions, trx.ID)

	return nil
}

// Rollback 回滚事务
func (tm *TransactionManager) Rollback(trx *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	return tm.rollbackLocked(trx)
}

// rollbackLocked 回滚事务（调用者必须持有锁）
func (tm *TransactionManager) rollbackLocked(trx *Transaction) error {
	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 执行回滚操作
	if err := tm.undoManager.Rollback(trx.ID); err != nil {
		return err
	}

	// 更新事务状态
	trx.State = TRX_STATE_ROLLED_BACK
	trx.LastActiveTime = time.Now()

	// 清理事务记录
	delete(tm.activeTransactions, trx.ID)

	return nil
}

// Savepoint 创建保存点
func (tm *TransactionManager) Savepoint(trx *Transaction, name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 获取当前LSN
	currentLSN := tm.undoManager.GetCurrentLSN(trx.ID)

	// 创建保存点
	savepoint := &Savepoint{
		Name:         name,
		LSN:          currentLSN,
		UndoLogCount: len(trx.UndoLogs),
		RedoLogCount: len(trx.RedoLogs),
		CreatedAt:    time.Now(),
	}

	// 保存到事务的保存点map中
	trx.Savepoints[name] = savepoint
	trx.LastActiveTime = time.Now()

	logger.Debugf("✅ Created savepoint '%s' for transaction %d at LSN %d", name, trx.ID, currentLSN)
	return nil
}

// RollbackToSavepoint 回滚到指定保存点
func (tm *TransactionManager) RollbackToSavepoint(trx *Transaction, name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 查找保存点
	savepoint, exists := trx.Savepoints[name]
	if !exists {
		return fmt.Errorf("savepoint '%s' not found", name)
	}

	logger.Debugf("🔄 Rolling back transaction %d to savepoint '%s' (LSN %d)", trx.ID, name, savepoint.LSN)

	// 执行部分回滚
	if err := tm.undoManager.PartialRollback(trx.ID, savepoint.LSN); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %v", err)
	}

	// 删除该保存点之后创建的所有保存点
	for spName, sp := range trx.Savepoints {
		if sp.CreatedAt.After(savepoint.CreatedAt) {
			delete(trx.Savepoints, spName)
			logger.Debugf("  🗑️  Removed savepoint '%s' (created after '%s')", spName, name)
		}
	}

	// 截断Undo和Redo日志
	if savepoint.UndoLogCount < len(trx.UndoLogs) {
		trx.UndoLogs = trx.UndoLogs[:savepoint.UndoLogCount]
	}
	if savepoint.RedoLogCount < len(trx.RedoLogs) {
		trx.RedoLogs = trx.RedoLogs[:savepoint.RedoLogCount]
	}

	trx.LastActiveTime = time.Now()

	logger.Debugf("✅ Successfully rolled back to savepoint '%s'", name)
	return nil
}

// ReleaseSavepoint 释放保存点
func (tm *TransactionManager) ReleaseSavepoint(trx *Transaction, name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 检查事务状态
	if trx.State != TRX_STATE_ACTIVE {
		return ErrInvalidTrxState
	}

	// 查找保存点
	if _, exists := trx.Savepoints[name]; !exists {
		return fmt.Errorf("savepoint '%s' not found", name)
	}

	// 删除保存点
	delete(trx.Savepoints, name)
	trx.LastActiveTime = time.Now()

	logger.Debugf("✅ Released savepoint '%s' for transaction %d", name, trx.ID)
	return nil
}

// createReadView 创建MVCC读视图
// 修复MVCC-001: 确保ReadView创建时正确捕获所有活跃事务ID
func (tm *TransactionManager) createReadView(trxID int64) *formatmvcc.ReadView {
	// 获取当前活跃事务列表（排除当前事务）
	capacity := len(tm.activeTransactions)
	if capacity > 0 {
		capacity = capacity - 1
	}
	if capacity < 0 {
		capacity = 0
	}
	activeIDs := make([]uint64, 0, capacity)

	for id, trx := range tm.activeTransactions {
		if trx.State == TRX_STATE_ACTIVE && id != trxID {
			activeIDs = append(activeIDs, uint64(id))
		}
	}

	// format/mvcc的NewReadView会自动计算lowWaterMark和highWaterMark
	return formatmvcc.NewReadView(activeIDs, uint64(trxID), uint64(tm.nextTrxID))
}

// GetTransaction 获取事务对象
func (tm *TransactionManager) GetTransaction(trxID int64) *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeTransactions[trxID]
}

// IsVisible 判断数据版本是否对事务可见
func (tm *TransactionManager) IsVisible(trx *Transaction, version int64) bool {
	// 读未提交：总是可见
	if trx.IsolationLevel == TRX_ISO_READ_UNCOMMITTED {
		return true
	}

	// 读已提交：每次可见性判断时创建新的ReadView（语句级快照）
	if trx.IsolationLevel == TRX_ISO_READ_COMMITTED {
		// 使用读锁快照当前活跃事务，避免与Begin时的写锁冲突
		tm.mu.RLock()
		activeIDs := make([]uint64, 0, len(tm.activeTransactions))
		for id, t := range tm.activeTransactions {
			if t.State == TRX_STATE_ACTIVE && id != trx.ID {
				activeIDs = append(activeIDs, uint64(id))
			}
		}
		nextTrxID := tm.nextTrxID
		tm.mu.RUnlock()

		// 临时ReadView用于本次判断
		rv := formatmvcc.NewReadView(activeIDs, uint64(trx.ID), uint64(nextTrxID))
		return rv.IsVisible(uint64(version))
	}

	// 可重复读/串行化：使用事务开始时创建的ReadView
	if trx.ReadView == nil {
		return true
	}
	return trx.ReadView.IsVisible(uint64(version))
}

// Cleanup 清理超时事务
func (tm *TransactionManager) Cleanup() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	timeout := tm.defaultTimeout
	now := time.Now()

	for id, trx := range tm.activeTransactions {
		if now.Sub(trx.LastActiveTime) > timeout {
			// 回滚超时事务
			tm.rollbackLocked(trx)
			delete(tm.activeTransactions, id)
		}
	}
}

// Close 关闭事务管理器
func (tm *TransactionManager) Close() error {
	// 停止长事务监控
	tm.StopLongTransactionMonitor()

	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 回滚所有未完成事务
	for _, trx := range tm.activeTransactions {
		if trx.State == TRX_STATE_ACTIVE {
			tm.rollbackLocked(trx)
		}
	}

	// 关闭日志管理器
	if err := tm.redoManager.Close(); err != nil {
		return err
	}
	if err := tm.undoManager.Close(); err != nil {
		return err
	}

	return nil
}

// StartLongTransactionMonitor 启动长事务监控
func (tm *TransactionManager) StartLongTransactionMonitor() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.monitorRunning {
		return
	}

	tm.monitorRunning = true
	tm.monitorWg.Add(1)

	go tm.longTransactionMonitor()
}

// StopLongTransactionMonitor 停止长事务监控
func (tm *TransactionManager) StopLongTransactionMonitor() {
	tm.mu.Lock()
	if !tm.monitorRunning {
		tm.mu.Unlock()
		return
	}
	tm.monitorRunning = false
	tm.mu.Unlock()

	close(tm.stopMonitor)
	tm.monitorWg.Wait()
}

// longTransactionMonitor 长事务监控协程
func (tm *TransactionManager) longTransactionMonitor() {
	defer tm.monitorWg.Done()

	ticker := time.NewTicker(tm.longTxnConfig.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.checkLongTransactions()

		case <-tm.stopMonitor:
			return
		}
	}
}

// checkLongTransactions 检查长事务
func (tm *TransactionManager) checkLongTransactions() {
	tm.mu.RLock()
	now := time.Now()
	longTxns := make([]*Transaction, 0)

	for _, trx := range tm.activeTransactions {
		if trx.State == TRX_STATE_ACTIVE {
			duration := now.Sub(trx.StartTime)

			// 检查是否超过阈值
			if duration >= tm.longTxnConfig.WarningThreshold {
				longTxns = append(longTxns, trx)
			}
		}
	}
	tm.mu.RUnlock()

	// 处理长事务
	for _, trx := range longTxns {
		tm.handleLongTransaction(trx, now)
	}

	// 更新统计
	tm.longTxnStats.Lock()
	tm.longTxnStats.CurrentLongTxns = len(longTxns)
	tm.longTxnStats.LastCheckTime = now
	tm.longTxnStats.Unlock()
}

// handleLongTransaction 处理长事务
func (tm *TransactionManager) handleLongTransaction(trx *Transaction, now time.Time) {
	duration := now.Sub(trx.StartTime)

	// 读取配置（需要锁保护）
	tm.mu.RLock()
	criticalThreshold := tm.longTxnConfig.CriticalThreshold
	maxLockCount := tm.longTxnConfig.MaxLockCount
	maxUndoLogSize := tm.longTxnConfig.MaxUndoLogSize
	autoRollback := tm.longTxnConfig.AutoRollback
	tm.mu.RUnlock()

	// 确定告警级别
	level := LONG_TXN_LEVEL_WARNING
	if duration >= criticalThreshold {
		level = LONG_TXN_LEVEL_CRITICAL
	}

	// 检查是否超过锁数量阈值
	lockExceeded := trx.LockCount > maxLockCount

	// 检查是否超过Undo日志大小阈值
	undoExceeded := trx.UndoLogSize > maxUndoLogSize

	// 构建告警消息
	message := fmt.Sprintf("Long transaction detected: ID=%d, Duration=%v, Locks=%d, UndoLogSize=%d bytes",
		trx.ID, duration, trx.LockCount, trx.UndoLogSize)

	if lockExceeded {
		message += fmt.Sprintf(", Lock count exceeds threshold (%d > %d)",
			trx.LockCount, maxLockCount)
	}

	if undoExceeded {
		message += fmt.Sprintf(", Undo log size exceeds threshold (%d > %d)",
			trx.UndoLogSize, maxUndoLogSize)
	}

	// 创建告警
	alert := &LongTransactionAlert{
		TrxID:          trx.ID,
		Level:          level,
		Duration:       duration,
		LockCount:      trx.LockCount,
		UndoLogSize:    trx.UndoLogSize,
		IsolationLevel: trx.IsolationLevel,
		IsReadOnly:     trx.IsReadOnly,
		Timestamp:      now,
		Message:        message,
	}

	// 发送告警
	select {
	case tm.alertChan <- alert:
	default:
		// 告警通道满，丢弃告警
	}

	// 更新统计
	tm.longTxnStats.Lock()
	if level == LONG_TXN_LEVEL_WARNING {
		tm.longTxnStats.TotalWarnings++
	} else {
		tm.longTxnStats.TotalCritical++
	}

	if duration > tm.longTxnStats.MaxDuration {
		tm.longTxnStats.MaxDuration = duration
	}
	tm.longTxnStats.Unlock()

	// 自动回滚（如果配置启用且达到严重级别）
	if autoRollback && level == LONG_TXN_LEVEL_CRITICAL {
		tm.mu.Lock()
		err := tm.rollbackLocked(trx)
		tm.mu.Unlock()

		if err == nil {
			tm.longTxnStats.Lock()
			tm.longTxnStats.TotalAutoRollbacks++
			tm.longTxnStats.Unlock()
		}
	}
}

// SetLongTransactionConfig 设置长事务检测配置
func (tm *TransactionManager) SetLongTransactionConfig(config *LongTransactionConfig) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if config.WarningThreshold > 0 {
		tm.longTxnConfig.WarningThreshold = config.WarningThreshold
	}
	if config.CriticalThreshold > 0 {
		tm.longTxnConfig.CriticalThreshold = config.CriticalThreshold
	}
	if config.CheckInterval > 0 {
		tm.longTxnConfig.CheckInterval = config.CheckInterval
	}
	tm.longTxnConfig.AutoRollback = config.AutoRollback
	if config.MaxLockCount > 0 {
		tm.longTxnConfig.MaxLockCount = config.MaxLockCount
	}
	if config.MaxUndoLogSize > 0 {
		tm.longTxnConfig.MaxUndoLogSize = config.MaxUndoLogSize
	}
}

// GetLongTransactionConfig 获取长事务检测配置
func (tm *TransactionManager) GetLongTransactionConfig() *LongTransactionConfig {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return &LongTransactionConfig{
		WarningThreshold:  tm.longTxnConfig.WarningThreshold,
		CriticalThreshold: tm.longTxnConfig.CriticalThreshold,
		CheckInterval:     tm.longTxnConfig.CheckInterval,
		AutoRollback:      tm.longTxnConfig.AutoRollback,
		MaxLockCount:      tm.longTxnConfig.MaxLockCount,
		MaxUndoLogSize:    tm.longTxnConfig.MaxUndoLogSize,
	}
}

// GetLongTransactionStats 获取长事务统计信息
func (tm *TransactionManager) GetLongTransactionStats() *LongTransactionStats {
	tm.longTxnStats.RLock()
	defer tm.longTxnStats.RUnlock()

	return &LongTransactionStats{
		TotalWarnings:      tm.longTxnStats.TotalWarnings,
		TotalCritical:      tm.longTxnStats.TotalCritical,
		TotalAutoRollbacks: tm.longTxnStats.TotalAutoRollbacks,
		CurrentLongTxns:    tm.longTxnStats.CurrentLongTxns,
		MaxDuration:        tm.longTxnStats.MaxDuration,
		LastCheckTime:      tm.longTxnStats.LastCheckTime,
	}
}

// GetAlertChannel 获取告警通道
func (tm *TransactionManager) GetAlertChannel() <-chan *LongTransactionAlert {
	return tm.alertChan
}

// GetLongTransactions 获取当前所有长事务
func (tm *TransactionManager) GetLongTransactions(threshold time.Duration) []*Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	now := time.Now()
	longTxns := make([]*Transaction, 0)

	for _, trx := range tm.activeTransactions {
		if trx.State == TRX_STATE_ACTIVE {
			duration := now.Sub(trx.StartTime)
			if duration >= threshold {
				longTxns = append(longTxns, trx)
			}
		}
	}

	return longTxns
}

// UpdateTransactionActivity 更新事务活跃时间
func (tm *TransactionManager) UpdateTransactionActivity(trxID int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if trx, ok := tm.activeTransactions[trxID]; ok {
		trx.LastActiveTime = time.Now()
	}
}

// UpdateTransactionLockCount 更新事务锁数量
func (tm *TransactionManager) UpdateTransactionLockCount(trxID int64, lockCount int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if trx, ok := tm.activeTransactions[trxID]; ok {
		trx.LockCount = lockCount
	}
}

// UpdateTransactionUndoLogSize 更新事务Undo日志大小
func (tm *TransactionManager) UpdateTransactionUndoLogSize(trxID int64, size uint64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if trx, ok := tm.activeTransactions[trxID]; ok {
		trx.UndoLogSize = size
	}
}
