package manager

import (
	"encoding/binary"
	"errors"
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

	return &UndoLogManager{
		logs:       make(map[int64][]UndoLogEntry),
		activeTxns: make(map[int64]bool),
		undoDir:    undoDir,
		undoFile:   undoFile,
	}, nil
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

	// 从后向前回滚
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		// TODO: 应用回滚操作
		// 这里需要调用缓冲池管理器来恢复旧值
		_ = entry // 临时使用以避免编译器警告
	}

	// 清理事务记录
	u.Cleanup(txID)

	return nil
}

// Cleanup 清理事务的Undo日志
func (u *UndoLogManager) Cleanup(txID int64) {
	u.mu.Lock()
	defer u.mu.Unlock()

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

// Close 关闭Undo日志管理器
func (u *UndoLogManager) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.undoFile.Close()
}
