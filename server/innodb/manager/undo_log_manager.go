package manager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// UndoLogManager 撤销日志管理器
type UndoLogManager struct {
	mu       sync.RWMutex
	logs     map[int64][]UndoLogEntry // 事务ID -> Undo日志列表
	undoDir  string                   // Undo日志目录
	undoFile *os.File                 // Undo日志文件

	// 事务状态跟踪
	activeTxns    map[int64]bool // 活跃事务集合
	oldestTxnTime time.Time      // 最老事务开始时间

	// Purge相关
	purgeQueue     []int64       // 待清理事务队列
	purgeThreshold time.Duration // Purge阈值（事务提交后多久可清理）
	purgeChan      chan int64    // Purge通知通道
	shutdown       chan struct{} // 关闭信号

	// 回滚执行器
	rollbackExecutor RollbackExecutor // 回滚操作执行器

	// 格式化器
	formatter *UndoLogFormatter // Undo日志格式化器
}

// NewUndoLogManager 创建新的撤销日志管理器
func NewUndoLogManager(undoDir string) (*UndoLogManager, error) {
	if err := os.MkdirAll(undoDir, 0755); err != nil {
		return nil, err
	}

	undoFile, err := os.OpenFile(
		filepath.Join(undoDir, "undo.log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	manager := &UndoLogManager{
		logs:           make(map[int64][]UndoLogEntry),
		activeTxns:     make(map[int64]bool),
		undoDir:        undoDir,
		undoFile:       undoFile,
		purgeQueue:     make([]int64, 0),
		purgeThreshold: 5 * time.Minute, // 默认5分钟后清理
		purgeChan:      make(chan int64, 100),
		shutdown:       make(chan struct{}),
		formatter:      NewUndoLogFormatter(),
	}

	// 启动Purge协程
	go manager.purgeWorker()

	return manager, nil
}

// SetRollbackExecutor 设置回滚执行器
func (u *UndoLogManager) SetRollbackExecutor(executor RollbackExecutor) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.rollbackExecutor = executor
}

// Append 追加一条撤销日志
func (u *UndoLogManager) Append(entry *UndoLogEntry) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// 设置创建时间
	entry.Timestamp = time.Now()

	// 如果是新事务，更新活跃事务集合
	if !u.activeTxns[entry.TrxID] {
		u.activeTxns[entry.TrxID] = true
		if u.oldestTxnTime.IsZero() || entry.Timestamp.Before(u.oldestTxnTime) {
			u.oldestTxnTime = entry.Timestamp
		}
	}

	// 添加到内存中
	u.logs[entry.TrxID] = append(u.logs[entry.TrxID], *entry)

	// 写入文件
	return u.writeEntryToFile(entry)
}

// writeEntryToFile 将Undo日志写入文件
func (u *UndoLogManager) writeEntryToFile(entry *UndoLogEntry) error {
	// 写入LSN
	if err := binary.Write(u.undoFile, binary.BigEndian, entry.LSN); err != nil {
		return err
	}

	// 写入事务ID
	if err := binary.Write(u.undoFile, binary.BigEndian, entry.TrxID); err != nil {
		return err
	}

	// 写入表ID
	if err := binary.Write(u.undoFile, binary.BigEndian, entry.TableID); err != nil {
		return err
	}

	// 写入操作类型
	if err := binary.Write(u.undoFile, binary.BigEndian, entry.Type); err != nil {
		return err
	}

	// 写入数据
	dataLen := uint16(len(entry.Data))
	if err := binary.Write(u.undoFile, binary.BigEndian, dataLen); err != nil {
		return err
	}
	if _, err := u.undoFile.Write(entry.Data); err != nil {
		return err
	}

	return u.undoFile.Sync()
}

// Rollback 回滚指定事务
func (u *UndoLogManager) Rollback(txID int64) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	entries, exists := u.logs[txID]
	if !exists {
		return errors.New("transaction not found")
	}

	if u.rollbackExecutor == nil {
		return errors.New("rollback executor not set")
	}

	// 从后向前回滚
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if err := u.executeRollback(&entry); err != nil {
			return fmt.Errorf("rollback entry %d failed: %v", i, err)
		}
	}

	// 清理事务记录
	u.cleanupLocked(txID)

	return nil
}

// PartialRollback 部分回滚（回滚到指定保存点）
func (u *UndoLogManager) PartialRollback(txID int64, savepointLSN uint64) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	entries, exists := u.logs[txID]
	if !exists {
		return errors.New("transaction not found")
	}

	if u.rollbackExecutor == nil {
		return errors.New("rollback executor not set")
	}

	// 找到保存点位置
	savepointIdx := -1
	for i, entry := range entries {
		if entry.LSN == savepointLSN {
			savepointIdx = i
			break
		}
	}

	if savepointIdx == -1 {
		return fmt.Errorf("savepoint LSN %d not found", savepointLSN)
	}

	// 回滚保存点之后的操作
	for i := len(entries) - 1; i > savepointIdx; i-- {
		entry := entries[i]
		if err := u.executeRollback(&entry); err != nil {
			return fmt.Errorf("rollback entry %d failed: %v", i, err)
		}
	}

	// 截断日志列表
	u.logs[txID] = entries[:savepointIdx+1]

	return nil
}

// executeRollback 执行单条Undo日志的回滚
func (u *UndoLogManager) executeRollback(entry *UndoLogEntry) error {
	switch entry.Type {
	case LOG_TYPE_INSERT:
		// INSERT的回滚：删除记录
		// entry.Data包含主键数据
		return u.rollbackExecutor.DeleteRecord(entry.TableID, 0, entry.Data)

	case LOG_TYPE_UPDATE:
		// UPDATE的回滚：恢复旧值
		bitmap, oldData, err := u.formatter.ParseUpdateUndo(entry.Data)
		if err != nil {
			return err
		}
		return u.rollbackExecutor.UpdateRecord(entry.TableID, 0, oldData, bitmap)

	case LOG_TYPE_DELETE:
		// DELETE的回滚：重新插入
		// entry.Data包含完整记录
		return u.rollbackExecutor.InsertRecord(entry.TableID, 0, entry.Data)

	default:
		return fmt.Errorf("unknown undo log type: %d", entry.Type)
	}
}

// Cleanup 清理事务的Undo日志
func (u *UndoLogManager) Cleanup(txID int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.cleanupLocked(txID)
}

// cleanupLocked 清理事务（已加锁）
func (u *UndoLogManager) cleanupLocked(txID int64) {
	delete(u.logs, txID)
	delete(u.activeTxns, txID)

	// 更新最老事务时间
	if len(u.activeTxns) == 0 {
		u.oldestTxnTime = time.Time{}
	} else {
		oldestTime := time.Now()
		for txID := range u.activeTxns {
			if entries := u.logs[txID]; len(entries) > 0 {
				if entries[0].Timestamp.Before(oldestTime) {
					oldestTime = entries[0].Timestamp
				}
			}
		}
		u.oldestTxnTime = oldestTime
	}
}

// SchedulePurge 计划清理已提交事务的Undo日志
func (u *UndoLogManager) SchedulePurge(txID int64) {
	select {
	case u.purgeChan <- txID:
		// 成功加入队列
	default:
		// 队列满，直接清理
		u.Cleanup(txID)
	}
}

// purgeWorker Purge工作协程
func (u *UndoLogManager) purgeWorker() {
	ticker := time.NewTicker(1 * time.Minute) // 每分钟检查一次
	defer ticker.Stop()

	for {
		select {
		case txID := <-u.purgeChan:
			// 延迟清理
			time.AfterFunc(u.purgeThreshold, func() {
				u.Cleanup(txID)
			})

		case <-ticker.C:
			// 定期清理超时事务
			u.purgeExpiredTransactions()

		case <-u.shutdown:
			return
		}
	}
}

// purgeExpiredTransactions 清理超时事务
func (u *UndoLogManager) purgeExpiredTransactions() {
	u.mu.Lock()
	defer u.mu.Unlock()

	expiredTxns := make([]int64, 0)
	threshold := time.Now().Add(-u.purgeThreshold)

	for txID, entries := range u.logs {
		if len(entries) > 0 && entries[0].Timestamp.Before(threshold) {
			// 检查是否还是活跃事务
			if !u.activeTxns[txID] {
				expiredTxns = append(expiredTxns, txID)
			}
		}
	}

	// 清理超时事务
	for _, txID := range expiredTxns {
		u.cleanupLocked(txID)
	}
}

// SetPurgeThreshold 设置Purge阈值
func (u *UndoLogManager) SetPurgeThreshold(threshold time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.purgeThreshold = threshold
}

// GetPurgeThreshold 获取Purge阈值
func (u *UndoLogManager) GetPurgeThreshold() time.Duration {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.purgeThreshold
}

// GetActiveTxns 获取活跃事务列表
func (u *UndoLogManager) GetActiveTxns() []int64 {
	u.mu.RLock()
	defer u.mu.RUnlock()

	txns := make([]int64, 0, len(u.activeTxns))
	for txID := range u.activeTxns {
		txns = append(txns, txID)
	}
	return txns
}

// GetOldestTxnTime 获取最老事务的开始时间
func (u *UndoLogManager) GetOldestTxnTime() time.Time {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.oldestTxnTime
}

// Recover 从Undo日志文件恢复
func (u *UndoLogManager) Recover() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// 定位到文件开始
	if _, err := u.undoFile.Seek(0, 0); err != nil {
		return err
	}

	// 读取并加载Undo日志
	for {
		var entry UndoLogEntry

		// 读取LSN
		if err := binary.Read(u.undoFile, binary.BigEndian, &entry.LSN); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		// 读取事务ID
		if err := binary.Read(u.undoFile, binary.BigEndian, &entry.TrxID); err != nil {
			return err
		}

		// 读取表ID
		if err := binary.Read(u.undoFile, binary.BigEndian, &entry.TableID); err != nil {
			return err
		}

		// 读取操作类型
		if err := binary.Read(u.undoFile, binary.BigEndian, &entry.Type); err != nil {
			return err
		}

		// 读取数据
		var dataLen uint16
		if err := binary.Read(u.undoFile, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		entry.Data = make([]byte, dataLen)
		if _, err := u.undoFile.Read(entry.Data); err != nil {
			return err
		}

		// 设置时间戳
		entry.Timestamp = time.Now()

		// 添加到内存中
		u.logs[entry.TrxID] = append(u.logs[entry.TrxID], entry)

		// 更新活跃事务集合
		if !u.activeTxns[entry.TrxID] {
			u.activeTxns[entry.TrxID] = true
			if u.oldestTxnTime.IsZero() || entry.Timestamp.Before(u.oldestTxnTime) {
				u.oldestTxnTime = entry.Timestamp
			}
		}
	}

	return nil
}

// Close 关闭Undo日志管理器
func (u *UndoLogManager) Close() error {
	// 发送关闭信号
	close(u.shutdown)

	u.mu.Lock()
	defer u.mu.Unlock()

	return u.undoFile.Close()
}

// GetStats 获取Undo Log统计信息
func (u *UndoLogManager) GetStats() *UndoLogStats {
	u.mu.RLock()
	defer u.mu.RUnlock()

	totalLogs := 0
	for _, entries := range u.logs {
		totalLogs += len(entries)
	}

	return &UndoLogStats{
		ActiveTxns:     len(u.activeTxns),
		TotalLogs:      totalLogs,
		PendingPurge:   len(u.purgeChan),
		PurgeThreshold: u.purgeThreshold,
		OldestTxnTime:  u.oldestTxnTime,
	}
}

// UndoLogStats Undo日志统计信息
type UndoLogStats struct {
	ActiveTxns     int           `json:"active_txns"`
	TotalLogs      int           `json:"total_logs"`
	PendingPurge   int           `json:"pending_purge"`
	PurgeThreshold time.Duration `json:"purge_threshold"`
	OldestTxnTime  time.Time     `json:"oldest_txn_time"`
}
