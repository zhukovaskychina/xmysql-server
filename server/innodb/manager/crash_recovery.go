package manager

import (
	"encoding/binary"
	"fmt"
	"io"
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

	// 缓冲池管理器（用于页面操作）
	bufferPoolManager BufferPoolInterface

	// 存储管理器（用于页面读写）
	storageManager StorageInterface

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

// BufferPoolInterface 缓冲池接口（用于解耦）
type BufferPoolInterface interface {
	FetchPage(pageID uint64) (PageInterface, error)
	UnpinPage(pageID uint64, isDirty bool) error
	FlushPage(pageID uint64) error
}

// PageInterface 页面接口
type PageInterface interface {
	GetPageID() uint64
	GetLSN() uint64
	SetLSN(lsn uint64)
	GetData() []byte
	SetData(data []byte)
	IsDirty() bool
	SetDirty(dirty bool)
}

// StorageInterface 存储接口
type StorageInterface interface {
	ReadPage(pageID uint64) ([]byte, error)
	WritePage(pageID uint64, data []byte) error
	CreatePage() (uint64, error)
	DeletePage(pageID uint64) error
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
		bufferPoolManager:  nil, // 需要通过SetBufferPoolManager设置
		storageManager:     nil, // 需要通过SetStorageManager设置
	}
}

// SetBufferPoolManager 设置缓冲池管理器
func (cr *CrashRecovery) SetBufferPoolManager(bpm BufferPoolInterface) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.bufferPoolManager = bpm
}

// SetStorageManager 设置存储管理器
func (cr *CrashRecovery) SetStorageManager(sm StorageInterface) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.storageManager = sm
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
			if err == io.EOF {
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
			if err == io.EOF {
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
	case LOG_TYPE_PAGE_SPLIT:
		return cr.redoPageSplit(entry)
	case LOG_TYPE_PAGE_MERGE:
		return cr.redoPageMerge(entry)
	case LOG_TYPE_INDEX_INSERT:
		return cr.redoIndexInsert(entry)
	case LOG_TYPE_INDEX_DELETE:
		return cr.redoIndexDelete(entry)
	case LOG_TYPE_INDEX_UPDATE:
		return cr.redoIndexUpdate(entry)
	case LOG_TYPE_FILE_EXTEND:
		return cr.redoFileExtend(entry)
	case LOG_TYPE_COMPENSATE:
		// CLR 日志不需要重做，只用于标记已回滚的操作
		return nil
	case LOG_TYPE_TXN_BEGIN, LOG_TYPE_TXN_COMMIT, LOG_TYPE_TXN_ROLLBACK, LOG_TYPE_TXN_SAVEPOINT:
		// 事务控制日志在分析阶段已处理，重做阶段不需要处理
		return nil
	case LOG_TYPE_CHECKPOINT:
		// 检查点日志不需要重做
		return nil
	default:
		// 未知类型的日志，记录警告但不中断恢复
		fmt.Printf("警告: 遇到未知日志类型 %d (LSN=%d)，跳过\n", entry.Type, entry.LSN)
		return nil
	}
}

// redoInsert 重做INSERT操作
func (cr *CrashRecovery) redoInsert(entry *RedoLogEntry) error {
	// 实现物理重做逻辑
	// 1. 从缓冲池获取页面
	// 2. 检查页面LSN（幂等性保证）
	// 3. 如果entry.LSN > pageLSN，则应用修改
	// 4. 更新页面LSN

	if cr.bufferPoolManager == nil {
		// 如果没有缓冲池管理器，使用存储管理器直接操作
		return cr.redoWithStorage(entry)
	}

	// 获取页面
	page, err := cr.bufferPoolManager.FetchPage(entry.PageID)
	if err != nil {
		return fmt.Errorf("获取页面%d失败: %v", entry.PageID, err)
	}
	defer cr.bufferPoolManager.UnpinPage(entry.PageID, true)

	// 检查页面LSN（幂等性保证）
	if page.GetLSN() >= entry.LSN {
		// 页面已经包含此修改，跳过
		return nil
	}

	// 应用修改：将日志数据写入页面
	if len(entry.Data) > 0 {
		page.SetData(entry.Data)
	}

	// 更新页面LSN
	page.SetLSN(entry.LSN)
	page.SetDirty(true)

	return nil
}

// redoUpdate 重做UPDATE操作
func (cr *CrashRecovery) redoUpdate(entry *RedoLogEntry) error {
	// UPDATE操作的重做逻辑与INSERT类似
	return cr.redoInsert(entry)
}

// redoDelete 重做DELETE操作
func (cr *CrashRecovery) redoDelete(entry *RedoLogEntry) error {
	// DELETE操作的重做逻辑与INSERT类似
	return cr.redoInsert(entry)
}

// redoPageCreate 重做页面创建操作
func (cr *CrashRecovery) redoPageCreate(entry *RedoLogEntry) error {
	if cr.storageManager == nil {
		return fmt.Errorf("存储管理器未设置")
	}

	// 创建新页面
	pageID, err := cr.storageManager.CreatePage()
	if err != nil {
		return fmt.Errorf("创建页面失败: %v", err)
	}

	// 验证页面ID是否匹配
	if pageID != entry.PageID {
		return fmt.Errorf("页面ID不匹配: 期望%d, 实际%d", entry.PageID, pageID)
	}

	// 如果有初始数据，写入页面
	if len(entry.Data) > 0 {
		if err := cr.storageManager.WritePage(pageID, entry.Data); err != nil {
			return fmt.Errorf("写入页面数据失败: %v", err)
		}
	}

	return nil
}

// redoPageDelete 重做页面删除操作
func (cr *CrashRecovery) redoPageDelete(entry *RedoLogEntry) error {
	if cr.storageManager == nil {
		return fmt.Errorf("存储管理器未设置")
	}

	// 删除页面
	if err := cr.storageManager.DeletePage(entry.PageID); err != nil {
		return fmt.Errorf("删除页面%d失败: %v", entry.PageID, err)
	}

	return nil
}

// redoPageModify 重做页面修改操作
func (cr *CrashRecovery) redoPageModify(entry *RedoLogEntry) error {
	// 页面修改操作与INSERT类似
	return cr.redoInsert(entry)
}

// redoWithStorage 使用存储管理器直接重做（无缓冲池）
func (cr *CrashRecovery) redoWithStorage(entry *RedoLogEntry) error {
	if cr.storageManager == nil {
		// 既没有缓冲池也没有存储管理器，记录警告并跳过
		fmt.Printf("警告: 无法重做LSN=%d的日志，缺少缓冲池和存储管理器\n", entry.LSN)
		return nil
	}

	// 读取页面数据
	pageData, err := cr.storageManager.ReadPage(entry.PageID)
	if err != nil {
		return fmt.Errorf("读取页面%d失败: %v", entry.PageID, err)
	}

	// 检查页面LSN（假设LSN存储在页面头部的前8字节）
	if len(pageData) >= 8 {
		pageLSN := binary.BigEndian.Uint64(pageData[0:8])
		if pageLSN >= entry.LSN {
			// 页面已经包含此修改，跳过
			return nil
		}
	}

	// 应用修改
	if len(entry.Data) > 0 {
		// 更新页面数据
		copy(pageData, entry.Data)

		// 更新页面LSN（写入前8字节）
		if len(pageData) >= 8 {
			binary.BigEndian.PutUint64(pageData[0:8], entry.LSN)
		}

		// 写回页面
		if err := cr.storageManager.WritePage(entry.PageID, pageData); err != nil {
			return fmt.Errorf("写入页面%d失败: %v", entry.PageID, err)
		}
	}

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

// ============================================================================
// 新增的 Redo 日志类型处理方法
// ============================================================================

// redoPageSplit 重做页面分裂操作
// 页面分裂是 B+树操作中的关键操作，需要确保原子性
func (cr *CrashRecovery) redoPageSplit(entry *RedoLogEntry) error {
	if cr.bufferPoolManager == nil {
		return cr.redoWithStorage(entry)
	}

	// 页面分裂日志包含：原页面ID、新页面ID、分裂点、数据分布
	// 这里简化处理：直接应用日志数据到页面
	page, err := cr.bufferPoolManager.FetchPage(entry.PageID)
	if err != nil {
		return fmt.Errorf("获取页面%d失败: %v", entry.PageID, err)
	}
	defer cr.bufferPoolManager.UnpinPage(entry.PageID, true)

	// 检查幂等性
	if page.GetLSN() >= entry.LSN {
		return nil
	}

	// 应用分裂操作
	if len(entry.Data) > 0 {
		page.SetData(entry.Data)
	}

	page.SetLSN(entry.LSN)
	page.SetDirty(true)

	return nil
}

// redoPageMerge 重做页面合并操作
// 页面合并是 B+树删除操作的一部分，需要确保数据一致性
func (cr *CrashRecovery) redoPageMerge(entry *RedoLogEntry) error {
	if cr.bufferPoolManager == nil {
		return cr.redoWithStorage(entry)
	}

	// 页面合并日志包含：左页面ID、右页面ID、合并后的数据
	page, err := cr.bufferPoolManager.FetchPage(entry.PageID)
	if err != nil {
		return fmt.Errorf("获取页面%d失败: %v", entry.PageID, err)
	}
	defer cr.bufferPoolManager.UnpinPage(entry.PageID, true)

	// 检查幂等性
	if page.GetLSN() >= entry.LSN {
		return nil
	}

	// 应用合并操作
	if len(entry.Data) > 0 {
		page.SetData(entry.Data)
	}

	page.SetLSN(entry.LSN)
	page.SetDirty(true)

	return nil
}

// redoIndexInsert 重做索引插入操作
// 索引插入需要维护索引页面的有序性
func (cr *CrashRecovery) redoIndexInsert(entry *RedoLogEntry) error {
	if cr.bufferPoolManager == nil {
		return cr.redoWithStorage(entry)
	}

	// 索引插入日志包含：索引页面ID、插入位置、索引键值
	page, err := cr.bufferPoolManager.FetchPage(entry.PageID)
	if err != nil {
		return fmt.Errorf("获取索引页面%d失败: %v", entry.PageID, err)
	}
	defer cr.bufferPoolManager.UnpinPage(entry.PageID, true)

	// 检查幂等性
	if page.GetLSN() >= entry.LSN {
		return nil
	}

	// 应用索引插入
	if len(entry.Data) > 0 {
		page.SetData(entry.Data)
	}

	page.SetLSN(entry.LSN)
	page.SetDirty(true)

	return nil
}

// redoIndexDelete 重做索引删除操作
// 索引删除需要维护索引页面的完整性
func (cr *CrashRecovery) redoIndexDelete(entry *RedoLogEntry) error {
	if cr.bufferPoolManager == nil {
		return cr.redoWithStorage(entry)
	}

	// 索引删除日志包含：索引页面ID、删除位置、删除的键值
	page, err := cr.bufferPoolManager.FetchPage(entry.PageID)
	if err != nil {
		return fmt.Errorf("获取索引页面%d失败: %v", entry.PageID, err)
	}
	defer cr.bufferPoolManager.UnpinPage(entry.PageID, true)

	// 检查幂等性
	if page.GetLSN() >= entry.LSN {
		return nil
	}

	// 应用索引删除
	if len(entry.Data) > 0 {
		page.SetData(entry.Data)
	}

	page.SetLSN(entry.LSN)
	page.SetDirty(true)

	return nil
}

// redoIndexUpdate 重做索引更新操作
// 索引更新通常是删除旧索引项 + 插入新索引项
func (cr *CrashRecovery) redoIndexUpdate(entry *RedoLogEntry) error {
	// 索引更新的重做逻辑与索引插入类似
	return cr.redoIndexInsert(entry)
}

// redoFileExtend 重做文件扩展操作
// 文件扩展用于表空间增长，需要确保空间分配的原子性
func (cr *CrashRecovery) redoFileExtend(entry *RedoLogEntry) error {
	if cr.storageManager == nil {
		fmt.Printf("警告: 无法重做文件扩展操作 (LSN=%d)，存储管理器未设置\n", entry.LSN)
		return nil
	}

	// 文件扩展日志包含：扩展的页面数量、新的文件大小
	// 这里简化处理：如果页面不存在则创建
	// 实际实现中应该调用存储管理器的扩展接口

	// 检查页面是否已存在
	_, err := cr.storageManager.ReadPage(entry.PageID)
	if err == nil {
		// 页面已存在，说明扩展已完成
		return nil
	}

	// 页面不存在，创建新页面
	pageID, err := cr.storageManager.CreatePage()
	if err != nil {
		return fmt.Errorf("创建扩展页面失败: %v", err)
	}

	// 验证页面ID
	if pageID != entry.PageID {
		fmt.Printf("警告: 扩展页面ID不匹配 (期望=%d, 实际=%d)\n", entry.PageID, pageID)
	}

	// 如果有初始数据，写入页面
	if len(entry.Data) > 0 {
		if err := cr.storageManager.WritePage(pageID, entry.Data); err != nil {
			return fmt.Errorf("写入扩展页面数据失败: %v", err)
		}
	}

	return nil
}
