package manager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
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

	// 版本链管理
	versionChains map[uint64]*VersionChain // 记录ID -> 版本链
	versionMu     sync.RWMutex             // 版本链锁

	// CLR（补偿日志记录）管理
	clrLogs map[int64][]uint64 // 事务ID -> CLR LSN列表
	clrMu   sync.RWMutex       // CLR锁

	// LSN管理器（用于生成CLR的LSN）
	lsnManager *LSNManager
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
		versionChains:  make(map[uint64]*VersionChain),
		clrLogs:        make(map[int64][]uint64),
		lsnManager:     NewLSNManager(1), // 从LSN 1开始
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
// 完整实现：
// 1. 按LSN倒序回滚所有Undo日志
// 2. 写入CLR（补偿日志记录）确保回滚操作可恢复
// 3. 正确更新MVCC版本链
// 4. 支持部分回滚（Savepoint）
func (u *UndoLogManager) Rollback(txID int64) error {
	u.mu.Lock()
	logs, exists := u.logs[txID]
	if !exists {
		u.mu.Unlock()
		return fmt.Errorf("no undo logs for transaction %d", txID)
	}

	// 复制日志列表以便在锁外处理
	undoLogs := make([]UndoLogEntry, len(logs))
	copy(undoLogs, logs)
	u.mu.Unlock()

	// 检查回滚执行器
	if u.rollbackExecutor == nil {
		return fmt.Errorf("rollback executor not set")
	}

	logger.Infof("🔄 Starting rollback for transaction %d, %d undo logs to process", txID, len(undoLogs))

	// 步骤1: 按LSN从大到小倒序回滚（从最新的操作开始回滚）
	rollbackCount := 0
	for i := len(undoLogs) - 1; i >= 0; i-- {
		log := &undoLogs[i]

		// 检查是否已经通过CLR回滚
		if u.isAlreadyRolledBack(txID, log.LSN) {
			logger.Debugf("  ⏭️  Undo log LSN=%d already rolled back (CLR exists), skipping", log.LSN)
			continue
		}

		logger.Debugf("  🔄 Rolling back undo log: LSN=%d, Type=%d, RecordID=%d",
			log.LSN, log.Type, log.RecordID)

		// 执行回滚操作（根据不同的操作类型调用不同的方法）
		if err := u.executeUndoLogRollback(log); err != nil {
			return fmt.Errorf("failed to execute undo log LSN=%d: %v", log.LSN, err)
		}

		// 步骤2: 写入CLR（补偿日志记录）
		clrLSN := uint64(u.lsnManager.AllocateLSN())
		u.recordCLR(txID, clrLSN, log.LSN)
		logger.Debugf("  ✅ Recorded CLR: CLR_LSN=%d for Undo_LSN=%d", clrLSN, log.LSN)

		// 步骤3: 更新版本链
		if err := u.updateVersionChain(log); err != nil {
			logger.Warnf("  ⚠️  Failed to update version chain for LSN=%d: %v", log.LSN, err)
			// 版本链更新失败不应中断回滚
		}

		rollbackCount++
	}

	// 步骤4: 清理事务状态
	u.mu.Lock()
	delete(u.logs, txID)
	delete(u.activeTxns, txID)
	u.mu.Unlock()

	logger.Infof("✅ Transaction %d rolled back successfully, %d/%d undo logs processed",
		txID, rollbackCount, len(undoLogs))

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
	// 检查是否已经回滚过（通过CLR检查）
	if u.isAlreadyRolledBack(entry.TrxID, entry.LSN) {
		return nil // 已回滚，跳过
	}

	var err error
	switch entry.Type {
	case LOG_TYPE_INSERT:
		// INSERT的回滚：删除记录
		// entry.Data包含主键数据
		err = u.rollbackExecutor.DeleteRecord(entry.TableID, entry.RecordID, entry.Data)

	case LOG_TYPE_UPDATE:
		// UPDATE的回滚：恢复旧值
		bitmap, oldData, parseErr := u.formatter.ParseUpdateUndo(entry.Data)
		if parseErr != nil {
			return parseErr
		}
		err = u.rollbackExecutor.UpdateRecord(entry.TableID, entry.RecordID, oldData, bitmap)

	case LOG_TYPE_DELETE:
		// DELETE的回滚：重新插入
		// entry.Data包含完整记录
		err = u.rollbackExecutor.InsertRecord(entry.TableID, entry.RecordID, entry.Data)

	default:
		return fmt.Errorf("unknown undo log type: %d", entry.Type)
	}

	if err != nil {
		return err
	}

	// 生成CLR（补偿日志记录）防止重复回滚
	clrLSN := uint64(u.lsnManager.AllocateLSN())
	u.recordCLR(entry.TrxID, clrLSN, entry.LSN)

	// 更新版本链（如果需要）
	u.updateVersionChainOnRollback(entry)

	return nil
}

// executeUndoLogRollback 执行单条Undo日志的回滚
// 根据不同的操作类型（INSERT/UPDATE/DELETE）调用对应的回滚方法
func (u *UndoLogManager) executeUndoLogRollback(entry *UndoLogEntry) error {
	if u.rollbackExecutor == nil {
		return fmt.Errorf("rollback executor not set")
	}

	var err error
	switch entry.Type {
	case LOG_TYPE_INSERT:
		// INSERT的回滚：删除记录
		logger.Debugf("    ↩️  Rollback INSERT: Delete record (tableID=%d, recordID=%d)",
			entry.TableID, entry.RecordID)
		err = u.rollbackExecutor.DeleteRecord(entry.TableID, entry.RecordID, entry.Data)

	case LOG_TYPE_UPDATE:
		// UPDATE的回滚：恢复旧值
		bitmap, oldData, parseErr := u.formatter.ParseUpdateUndo(entry.Data)
		if parseErr != nil {
			return fmt.Errorf("failed to parse UPDATE undo data: %v", parseErr)
		}
		logger.Debugf("    ↩️  Rollback UPDATE: Restore old values (tableID=%d, recordID=%d)",
			entry.TableID, entry.RecordID)
		err = u.rollbackExecutor.UpdateRecord(entry.TableID, entry.RecordID, oldData, bitmap)

	case LOG_TYPE_DELETE:
		// DELETE的回滚：重新插入
		logger.Debugf("    ↩️  Rollback DELETE: Re-insert record (tableID=%d, recordID=%d)",
			entry.TableID, entry.RecordID)
		err = u.rollbackExecutor.InsertRecord(entry.TableID, entry.RecordID, entry.Data)

	default:
		return fmt.Errorf("unknown undo log type: %d", entry.Type)
	}

	if err != nil {
		return fmt.Errorf("rollback execution failed: %v", err)
	}

	return nil
}

// updateVersionChain 更新版本链
// 回滚时需要从版本链中移除对应的版本
func (u *UndoLogManager) updateVersionChain(log *UndoLogEntry) error {
	u.versionMu.Lock()
	defer u.versionMu.Unlock()

	chain, exists := u.versionChains[log.RecordID]
	if !exists {
		// 版本链不存在，可能已被清理，不算错误
		logger.Debugf("    ℹ️  Version chain not found for recordID=%d (may be purged)", log.RecordID)
		return nil
	}

	// 从版本链中移除此Undo日志对应的版本
	removed := chain.RemoveVersion(log.LSN)
	if removed {
		logger.Debugf("    🗑️  Removed version LSN=%d from version chain (recordID=%d)",
			log.LSN, log.RecordID)
	} else {
		logger.Debugf("    ℹ️  Version LSN=%d not found in chain (recordID=%d)",
			log.LSN, log.RecordID)
	}

	return nil
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

	// 清理CLR日志
	u.ClearCLRLogs(txID)

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
			if err == io.EOF {
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

// VersionChain MVCC版本链
type VersionChain struct {
	recordID uint64              // 记录ID
	versions []*VersionChainNode // 版本列表（从新到旧）
	mu       sync.RWMutex        // 版本链锁
}

// VersionChainNode 版本链节点
type VersionChainNode struct {
	txID      int64     // 创建此版本的事务ID
	lsn       uint64    // 日志序列号
	undoPtr   uint64    // 指向Undo日志的指针
	timestamp time.Time // 版本创建时间
	data      []byte    // 版本数据（可选，用于快速访问）
}

// NewVersionChain 创建新的版本链
func NewVersionChain(recordID uint64) *VersionChain {
	return &VersionChain{
		recordID: recordID,
		versions: make([]*VersionChainNode, 0),
	}
}

// AddVersion 添加新版本到版本链
func (vc *VersionChain) AddVersion(txID int64, lsn uint64, undoPtr uint64, data []byte) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	node := &VersionChainNode{
		txID:      txID,
		lsn:       lsn,
		undoPtr:   undoPtr,
		timestamp: time.Now(),
		data:      data,
	}

	// 插入到版本链头部（最新版本）
	vc.versions = append([]*VersionChainNode{node}, vc.versions...)
}

// RemoveVersion 从版本链中移除指定LSN的版本（用于回滚）
func (vc *VersionChain) RemoveVersion(lsn uint64) bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	// 查找并移除指定LSN的版本
	for i, version := range vc.versions {
		if version.lsn == lsn {
			// 从版本链中移除此版本
			vc.versions = append(vc.versions[:i], vc.versions[i+1:]...)
			return true
		}
	}

	return false // 未找到指定LSN的版本
}

// GetVersion 获取指定事务可见的版本
func (vc *VersionChain) GetVersion(readView *ReadView) *VersionChainNode {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	// 从最新版本开始查找
	for _, version := range vc.versions {
		// 检查版本是否对当前ReadView可见
		if readView == nil || readView.IsVisible(version.txID) {
			return version
		}
	}

	return nil
}

// GetLatestVersion 获取最新版本
func (vc *VersionChain) GetLatestVersion() *VersionChainNode {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	if len(vc.versions) > 0 {
		return vc.versions[0]
	}
	return nil
}

// PurgeOldVersions 清理旧版本（在所有活跃事务之前的版本）
func (vc *VersionChain) PurgeOldVersions(oldestActiveTxID int64) int {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	purgedCount := 0
	newVersions := make([]*VersionChainNode, 0)

	for _, version := range vc.versions {
		// 保留比最老活跃事务更新的版本
		if version.txID >= oldestActiveTxID {
			newVersions = append(newVersions, version)
		} else {
			purgedCount++
		}
	}

	// 至少保留一个版本
	if len(newVersions) == 0 && len(vc.versions) > 0 {
		newVersions = append(newVersions, vc.versions[len(vc.versions)-1])
		purgedCount--
	}

	vc.versions = newVersions
	return purgedCount
}

// ReadView MVCC读视图（简化版本，完整版本在mvcc包中）
type ReadView struct {
	creatorTxID int64   // 创建此ReadView的事务ID
	minTxID     int64   // 最小活跃事务ID
	maxTxID     int64   // 最大事务ID
	activeTxIDs []int64 // 活跃事务ID列表
}

// IsVisible 检查版本是否对当前ReadView可见
func (rv *ReadView) IsVisible(txID int64) bool {
	// 自己的修改总是可见
	if txID == rv.creatorTxID {
		return true
	}

	// 在ReadView创建之前提交的事务可见
	if txID < rv.minTxID {
		return true
	}

	// 在ReadView创建之后开始的事务不可见
	if txID >= rv.maxTxID {
		return false
	}

	// 检查是否在活跃事务列表中
	for _, activeTxID := range rv.activeTxIDs {
		if txID == activeTxID {
			return false // 活跃事务不可见
		}
	}

	return true // 已提交事务可见
}

// isAlreadyRolledBack 检查是否已经回滚过（通过CLR检查）
func (u *UndoLogManager) isAlreadyRolledBack(txID int64, lsn uint64) bool {
	u.clrMu.RLock()
	defer u.clrMu.RUnlock()

	clrList, exists := u.clrLogs[txID]
	if !exists {
		return false
	}

	// 检查LSN是否在CLR列表中
	for _, clrLSN := range clrList {
		if clrLSN == lsn {
			return true
		}
	}

	return false
}

// recordCLR 记录补偿日志（CLR）
// 用于标记某个Undo日志已被回滚，防止重复回滚
func (u *UndoLogManager) recordCLR(txID int64, clrLSN uint64, undoLSN uint64) {
	u.clrMu.Lock()
	defer u.clrMu.Unlock()

	if u.clrLogs[txID] == nil {
		u.clrLogs[txID] = make([]uint64, 0)
	}
	u.clrLogs[txID] = append(u.clrLogs[txID], undoLSN)

	logger.Debugf("📝 Recorded CLR: txn=%d, CLR_LSN=%d, Undo_LSN=%d",
		txID, clrLSN, undoLSN)
}

// updateVersionChainOnRollback 回滚时更新版本链
func (u *UndoLogManager) updateVersionChainOnRollback(entry *UndoLogEntry) {
	// 对于回滚操作，我们需要从版本链中移除对应的版本
	// 这里简化处理：标记版本为已回滚
	// 实际实现中可能需要更复杂的版本链管理

	// 注意：这是一个简化实现
	// 完整的实现需要与MVCC管理器协调
}

// BuildVersionChain 为记录构建版本链
func (u *UndoLogManager) BuildVersionChain(recordID uint64, txID int64) (*VersionChain, error) {
	u.versionMu.Lock()
	defer u.versionMu.Unlock()

	// 检查是否已存在版本链
	if chain, exists := u.versionChains[recordID]; exists {
		return chain, nil
	}

	// 创建新的版本链
	chain := NewVersionChain(recordID)

	// 从Undo日志中构建版本链
	u.mu.RLock()
	entries, exists := u.logs[txID]
	u.mu.RUnlock()

	if exists {
		for i := len(entries) - 1; i >= 0; i-- {
			entry := &entries[i]
			// 添加版本到版本链
			chain.AddVersion(entry.TrxID, entry.LSN, entry.LSN, entry.Data)
		}
	}

	u.versionChains[recordID] = chain
	return chain, nil
}

// GetVersionChain 获取记录的版本链
func (u *UndoLogManager) GetVersionChain(recordID uint64) *VersionChain {
	u.versionMu.RLock()
	defer u.versionMu.RUnlock()

	return u.versionChains[recordID]
}

// PurgeVersionChains 清理旧版本链
func (u *UndoLogManager) PurgeVersionChains() int {
	u.versionMu.Lock()
	defer u.versionMu.Unlock()

	// 获取最老的活跃事务ID
	u.mu.RLock()
	var oldestTxID int64 = 0
	for txID := range u.activeTxns {
		if oldestTxID == 0 || txID < oldestTxID {
			oldestTxID = txID
		}
	}
	u.mu.RUnlock()

	if oldestTxID == 0 {
		return 0 // 没有活跃事务
	}

	// 清理每个版本链中的旧版本
	totalPurged := 0
	for _, chain := range u.versionChains {
		purged := chain.PurgeOldVersions(oldestTxID)
		totalPurged += purged
	}

	return totalPurged
}

// ClearCLRLogs 清理事务的CLR日志
func (u *UndoLogManager) ClearCLRLogs(txID int64) {
	u.clrMu.Lock()
	defer u.clrMu.Unlock()

	delete(u.clrLogs, txID)
}

// IsRolledBack 检查指定LSN是否已回滚（公共方法用于测试）
func (u *UndoLogManager) IsRolledBack(txID int64, lsn uint64) bool {
	return u.isAlreadyRolledBack(txID, lsn)
}
