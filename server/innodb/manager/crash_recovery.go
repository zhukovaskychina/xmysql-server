package manager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CrashRecovery 崩溃恢复管理器
// 实现ARIES算法的三阶段恢复：分析（Analysis）、重做（Redo）、撤销（Undo）
type CrashRecovery struct {
	mu sync.RWMutex

	// 日志管理器
	redoLogManager *RedoLogManager
	undoLogManager *UndoLogManager

	// LSN管理器
	lsnManager *LSNManager

	// 检查点LSN
	checkpointLSN uint64

	// 恢复状态
	recoveryPhase    string    // 当前恢复阶段
	analysisComplete bool      // 分析阶段是否完成
	redoComplete     bool      // Redo阶段是否完成
	undoComplete     bool      // Undo阶段是否完成
	recoveryStart    time.Time // 恢复开始时间
	recoveryEnd      time.Time // 恢复结束时间

	// 恢复结果
	activeTransactions map[int64]*TransactionInfo   // 活跃事务列表
	dirtyPages         map[uint64]*PageRecoveryInfo // 脏页列表
	redoStartLSN       uint64                       // Redo起始LSN
	redoEndLSN         uint64                       // Redo结束LSN
	undoTransactions   []int64                      // 需要回滚的事务列表
}

// TransactionInfo 事务恢复信息
type TransactionInfo struct {
	TrxID       int64     // 事务ID
	State       string    // 事务状态：Active/Committed/Aborted
	FirstLSN    uint64    // 第一条日志的LSN
	LastLSN     uint64    // 最后一条日志的LSN
	UndoNextLSN uint64    // 下一条需要Undo的LSN
	StartTime   time.Time // 事务开始时间
}

// PageRecoveryInfo 页面恢复信息
type PageRecoveryInfo struct {
	PageID      uint64 // 页面ID
	RecLSN      uint64 // 第一次修改的LSN（Recovery LSN）
	PageLSN     uint64 // 页面当前LSN
	NeedRedo    bool   // 是否需要重做
	ModifyCount int    // 修改次数
}

// NewCrashRecovery 创建崩溃恢复管理器
func NewCrashRecovery(
	redoLogManager *RedoLogManager,
	undoLogManager *UndoLogManager,
	checkpointLSN uint64,
) *CrashRecovery {
	return &CrashRecovery{
		redoLogManager:     redoLogManager,
		undoLogManager:     undoLogManager,
		lsnManager:         redoLogManager.GetLSNManager(),
		checkpointLSN:      checkpointLSN,
		activeTransactions: make(map[int64]*TransactionInfo),
		dirtyPages:         make(map[uint64]*PageRecoveryInfo),
		undoTransactions:   make([]int64, 0),
	}
}

// Recover 执行完整的崩溃恢复流程
func (cr *CrashRecovery) Recover() error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	cr.recoveryStart = time.Now()
	defer func() {
		cr.recoveryEnd = time.Now()
	}()

	// 阶段1：分析（Analysis）
	if err := cr.analysisPhase(); err != nil {
		return fmt.Errorf("分析阶段失败: %v", err)
	}

	// 阶段2：重做（Redo）
	if err := cr.redoPhase(); err != nil {
		return fmt.Errorf("Redo阶段失败: %v", err)
	}

	// 阶段3：撤销（Undo）
	if err := cr.undoPhase(); err != nil {
		return fmt.Errorf("Undo阶段失败: %v", err)
	}

	return nil
}

// analysisPhase 分析阶段
// 扫描日志确定：
// 1. 恢复起点（RedoLSN）
// 2. 脏页列表
// 3. 活跃事务列表
func (cr *CrashRecovery) analysisPhase() error {
	cr.recoveryPhase = "Analysis"

	// 从Checkpoint开始扫描
	currentLSN := cr.checkpointLSN

	// 扫描Redo日志
	if err := cr.scanRedoLog(currentLSN); err != nil {
		return fmt.Errorf("扫描Redo日志失败: %v", err)
	}

	// 确定Redo起始LSN（最小的RecLSN）
	cr.redoStartLSN = cr.checkpointLSN
	for _, pageInfo := range cr.dirtyPages {
		if pageInfo.RecLSN < cr.redoStartLSN {
			cr.redoStartLSN = pageInfo.RecLSN
		}
	}

	// 确定Redo结束LSN
	cr.redoEndLSN = uint64(cr.lsnManager.GetCurrentLSN())

	// 标记需要回滚的事务
	for txID, txInfo := range cr.activeTransactions {
		if txInfo.State == "Active" {
			cr.undoTransactions = append(cr.undoTransactions, txID)
		}
	}

	cr.analysisComplete = true
	return nil
}

// scanRedoLog 扫描Redo日志
func (cr *CrashRecovery) scanRedoLog(fromLSN uint64) error {
	// 打开Redo日志文件
	logFile, err := os.Open(filepath.Join(cr.redoLogManager.logDir, "redo.log"))
	if err != nil {
		return err
	}
	defer logFile.Close()

	// 从指定LSN开始扫描
	for {
		var entry RedoLogEntry

		// 读取LSN
		if err := binary.Read(logFile, binary.BigEndian, &entry.LSN); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		// 跳过小于fromLSN的日志
		if entry.LSN < fromLSN {
			// 跳过这条日志的剩余部分
			if err := cr.skipLogEntry(logFile); err != nil {
				return err
			}
			continue
		}

		// 读取事务ID
		if err := binary.Read(logFile, binary.BigEndian, &entry.TrxID); err != nil {
			return err
		}

		// 读取页面信息
		if err := binary.Read(logFile, binary.BigEndian, &entry.PageID); err != nil {
			return err
		}
		if err := binary.Read(logFile, binary.BigEndian, &entry.Type); err != nil {
			return err
		}

		// 读取数据
		var dataLen uint16
		if err := binary.Read(logFile, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		entry.Data = make([]byte, dataLen)
		if _, err := logFile.Read(entry.Data); err != nil {
			return err
		}

		// 处理日志条目
		cr.processLogEntry(&entry)
	}

	return nil
}

// processLogEntry 处理日志条目
func (cr *CrashRecovery) processLogEntry(entry *RedoLogEntry) {
	switch entry.Type {
	case LOG_TYPE_TXN_BEGIN:
		// 添加到活跃事务列表
		cr.activeTransactions[entry.TrxID] = &TransactionInfo{
			TrxID:     entry.TrxID,
			FirstLSN:  entry.LSN,
			State:     "Active",
			StartTime: entry.Timestamp,
		}

	case LOG_TYPE_TXN_COMMIT:
		// 从活跃事务列表移除
		if txInfo, exists := cr.activeTransactions[entry.TrxID]; exists {
			txInfo.State = "Committed"
			txInfo.LastLSN = entry.LSN
			delete(cr.activeTransactions, entry.TrxID)
		}

	case LOG_TYPE_TXN_ROLLBACK:
		// 从活跃事务列表移除
		if txInfo, exists := cr.activeTransactions[entry.TrxID]; exists {
			txInfo.State = "Aborted"
			txInfo.LastLSN = entry.LSN
			delete(cr.activeTransactions, entry.TrxID)
		}

	case LOG_TYPE_INSERT, LOG_TYPE_UPDATE, LOG_TYPE_DELETE:
		// 更新脏页列表
		pageID := entry.PageID
		if _, exists := cr.dirtyPages[pageID]; !exists {
			cr.dirtyPages[pageID] = &PageRecoveryInfo{
				PageID:   pageID,
				RecLSN:   entry.LSN,
				PageLSN:  0,
				NeedRedo: true,
			}
		}
	}
}

// skipLogEntry 跳过日志条目
func (cr *CrashRecovery) skipLogEntry(logFile *os.File) error {
	// 跳过事务ID
	if err := binary.Read(logFile, binary.BigEndian, new(int64)); err != nil {
		return err
	}
	// 跳过页面ID
	if err := binary.Read(logFile, binary.BigEndian, new(uint64)); err != nil {
		return err
	}
	// 跳过类型
	if err := binary.Read(logFile, binary.BigEndian, new(uint8)); err != nil {
		return err
	}
	// 读取数据长度并跳过数据
	var dataLen uint16
	if err := binary.Read(logFile, binary.BigEndian, &dataLen); err != nil {
		return err
	}
	_, err := logFile.Seek(int64(dataLen), 1) // 相对当前位置跳过
	return err
}

// redoPhase 重做阶段
// 重放从RedoStartLSN到日志末尾的所有日志，恢复已提交事务的修改
func (cr *CrashRecovery) redoPhase() error {
	cr.recoveryPhase = "Redo"

	if !cr.analysisComplete {
		return fmt.Errorf("分析阶段未完成")
	}

	// 打开Redo日志文件
	logFile, err := os.Open(filepath.Join(cr.redoLogManager.logDir, "redo.log"))
	if err != nil {
		return err
	}
	defer logFile.Close()

	redoCount := 0

	// 从RedoStartLSN开始顺序扫描日志
	for {
		var entry RedoLogEntry

		// 读取LSN
		if err := binary.Read(logFile, binary.BigEndian, &entry.LSN); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		// 如果超过RedoEndLSN，停止
		if entry.LSN > cr.redoEndLSN {
			break
		}

		// 跳过小于RedoStartLSN的日志
		if entry.LSN < cr.redoStartLSN {
			if err := cr.skipLogEntry(logFile); err != nil {
				return err
			}
			continue
		}

		// 读取事务ID
		if err := binary.Read(logFile, binary.BigEndian, &entry.TrxID); err != nil {
			return err
		}

		// 读取页面信息
		if err := binary.Read(logFile, binary.BigEndian, &entry.PageID); err != nil {
			return err
		}
		if err := binary.Read(logFile, binary.BigEndian, &entry.Type); err != nil {
			return err
		}

		// 读取数据
		var dataLen uint16
		if err := binary.Read(logFile, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		entry.Data = make([]byte, dataLen)
		if _, err := logFile.Read(entry.Data); err != nil {
			return err
		}

		// 执行重做操作
		if err := cr.redoLogEntry(&entry); err != nil {
			return fmt.Errorf("重做日志LSN=%d失败: %v", entry.LSN, err)
		}

		redoCount++
	}

	cr.redoComplete = true
	return nil
}

// redoLogEntry 重做单条日志
func (cr *CrashRecovery) redoLogEntry(entry *RedoLogEntry) error {
	// 根据日志类型执行不同的重做操作
	switch entry.Type {
	case LOG_TYPE_INSERT:
		return cr.redoInsert(entry)
	case LOG_TYPE_UPDATE:
		return cr.redoUpdate(entry)
	case LOG_TYPE_DELETE:
		return cr.redoDelete(entry)
	case LOG_TYPE_PAGE_CREATE:
		return cr.redoPageCreate(entry)
	case LOG_TYPE_PAGE_DELETE:
		return cr.redoPageDelete(entry)
	case LOG_TYPE_PAGE_MODIFY:
		return cr.redoPageModify(entry)
	default:
		// 其他类型的日志不需要重做
		return nil
	}
}

// redoInsert 重做INSERT操作
func (cr *CrashRecovery) redoInsert(entry *RedoLogEntry) error {
	// 注意：实际实现需要缓冲池管理器和B+树管理器的支持
	// 这里只是框架代码
	// 实际应该：
	// 1. 从缓冲池获取页面
	// 2. 检查页面LSN
	// 3. 如果entry.LSN > pageLSN，则应用修改
	// 4. 更新页面LSN
	return nil
}

// redoUpdate 重做UPDATE操作
func (cr *CrashRecovery) redoUpdate(entry *RedoLogEntry) error {
	// 类似redoInsert的实现
	return nil
}

// redoDelete 重做DELETE操作
func (cr *CrashRecovery) redoDelete(entry *RedoLogEntry) error {
	// 类似redoInsert的实现
	return nil
}

// redoPageCreate 重做页面创建操作
func (cr *CrashRecovery) redoPageCreate(entry *RedoLogEntry) error {
	// 页面创建的重做逻辑
	return nil
}

// redoPageDelete 重做页面删除操作
func (cr *CrashRecovery) redoPageDelete(entry *RedoLogEntry) error {
	// 页面删除的重做逻辑
	return nil
}

// redoPageModify 重做页面修改操作
func (cr *CrashRecovery) redoPageModify(entry *RedoLogEntry) error {
	// 页面修改的重做逻辑
	return nil
}

// undoPhase 撤销阶段
// 回滚所有未提交事务的修改
func (cr *CrashRecovery) undoPhase() error {
	cr.recoveryPhase = "Undo"

	if !cr.redoComplete {
		return fmt.Errorf("Redo阶段未完成")
	}

	// 对于每个未提交事务，按LSN从大到小回滚
	for _, txID := range cr.undoTransactions {
		if err := cr.rollbackTransaction(txID); err != nil {
			return fmt.Errorf("回滚事务%d失败: %v", txID, err)
		}
	}

	cr.undoComplete = true
	return nil
}

// rollbackTransaction 回滚单个事务
func (cr *CrashRecovery) rollbackTransaction(txID int64) error {
	// 使用UndoLogManager回滚事务
	if err := cr.undoLogManager.Rollback(txID); err != nil {
		return err
	}

	// 写入CLR（Compensation Log Record）
	// CLR记录回滚操作，确保回滚操作本身也是可恢复的
	clrEntry := &RedoLogEntry{
		LSN:   uint64(cr.lsnManager.AllocateLSN()),
		TrxID: txID,
		Type:  LOG_TYPE_COMPENSATE,
		Data:  []byte(fmt.Sprintf("Rollback transaction %d", txID)),
	}

	// 写入CLR到Redo日志
	if _, err := cr.redoLogManager.Append(clrEntry); err != nil {
		return fmt.Errorf("写入CLR失败: %v", err)
	}

	return nil
}

// GetRecoveryStatus 获取恢复状态
func (cr *CrashRecovery) GetRecoveryStatus() *RecoveryStatus {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return &RecoveryStatus{
		Phase:              cr.recoveryPhase,
		AnalysisComplete:   cr.analysisComplete,
		RedoComplete:       cr.redoComplete,
		UndoComplete:       cr.undoComplete,
		CheckpointLSN:      cr.checkpointLSN,
		RedoStartLSN:       cr.redoStartLSN,
		RedoEndLSN:         cr.redoEndLSN,
		ActiveTransactions: len(cr.activeTransactions),
		DirtyPages:         len(cr.dirtyPages),
		UndoTransactions:   len(cr.undoTransactions),
		RecoveryDuration:   cr.recoveryEnd.Sub(cr.recoveryStart),
	}
}

// RecoveryStatus 恢复状态信息
type RecoveryStatus struct {
	Phase              string        `json:"phase"`               // 当前阶段
	AnalysisComplete   bool          `json:"analysis_complete"`   // 分析完成
	RedoComplete       bool          `json:"redo_complete"`       // Redo完成
	UndoComplete       bool          `json:"undo_complete"`       // Undo完成
	CheckpointLSN      uint64        `json:"checkpoint_lsn"`      // Checkpoint LSN
	RedoStartLSN       uint64        `json:"redo_start_lsn"`      // Redo起始LSN
	RedoEndLSN         uint64        `json:"redo_end_lsn"`        // Redo结束LSN
	ActiveTransactions int           `json:"active_transactions"` // 活跃事务数
	DirtyPages         int           `json:"dirty_pages"`         // 脏页数
	UndoTransactions   int           `json:"undo_transactions"`   // 需回滚事务数
	RecoveryDuration   time.Duration `json:"recovery_duration"`   // 恢复耗时
}

// AnalysisResult 分析阶段结果
type AnalysisResult struct {
	RedoStartLSN       uint64                       `json:"redo_start_lsn"`
	RedoEndLSN         uint64                       `json:"redo_end_lsn"`
	ActiveTransactions map[int64]*TransactionInfo   `json:"active_transactions"`
	DirtyPages         map[uint64]*PageRecoveryInfo `json:"dirty_pages"`
	UndoTransactions   []int64                      `json:"undo_transactions"`
}

// GetAnalysisResult 获取分析结果
func (cr *CrashRecovery) GetAnalysisResult() *AnalysisResult {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return &AnalysisResult{
		RedoStartLSN:       cr.redoStartLSN,
		RedoEndLSN:         cr.redoEndLSN,
		ActiveTransactions: cr.activeTransactions,
		DirtyPages:         cr.dirtyPages,
		UndoTransactions:   cr.undoTransactions,
	}
}

// ValidateRecovery 验证恢复结果
func (cr *CrashRecovery) ValidateRecovery() error {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	if !cr.analysisComplete {
		return fmt.Errorf("分析阶段未完成")
	}

	if !cr.redoComplete {
		return fmt.Errorf("Redo阶段未完成")
	}

	if !cr.undoComplete {
		return fmt.Errorf("Undo阶段未完成")
	}

	// 验证所有未提交事务都已回滚
	if len(cr.activeTransactions) > 0 {
		for txID, txInfo := range cr.activeTransactions {
			if txInfo.State == "Active" {
				return fmt.Errorf("事务%d未回滚", txID)
			}
		}
	}

	return nil
}

// GetRecoveryStatistics 获取恢复统计信息
func (cr *CrashRecovery) GetRecoveryStatistics() *RecoveryStatistics {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	totalModifications := 0
	for _, pageInfo := range cr.dirtyPages {
		totalModifications += pageInfo.ModifyCount
	}

	return &RecoveryStatistics{
		TotalTransactions:  len(cr.activeTransactions),
		CommittedTxns:      len(cr.activeTransactions) - len(cr.undoTransactions),
		AbortedTxns:        len(cr.undoTransactions),
		TotalDirtyPages:    len(cr.dirtyPages),
		TotalModifications: totalModifications,
		RedoLSNRange:       cr.redoEndLSN - cr.redoStartLSN,
		RecoveryTime:       cr.recoveryEnd.Sub(cr.recoveryStart),
	}
}

// RecoveryStatistics 恢复统计信息
type RecoveryStatistics struct {
	TotalTransactions  int           `json:"total_transactions"`  // 总事务数
	CommittedTxns      int           `json:"committed_txns"`      // 已提交事务数
	AbortedTxns        int           `json:"aborted_txns"`        // 已中止事务数
	TotalDirtyPages    int           `json:"total_dirty_pages"`   // 总脏页数
	TotalModifications int           `json:"total_modifications"` // 总修改次数
	RedoLSNRange       uint64        `json:"redo_lsn_range"`      // Redo LSN范围
	RecoveryTime       time.Duration `json:"recovery_time"`       // 恢复耗时
}
