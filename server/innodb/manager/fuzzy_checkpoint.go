package manager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ============ LOG-008.1: 模糊检查点(Fuzzy Checkpoint)实现 ============

// FuzzyCheckpoint 模糊检查点
// 与Sharp Checkpoint不同,Fuzzy Checkpoint不需要等待所有脏页刷新完毕
// 只记录当前的LSN和脏页列表,允许系统继续运行
type FuzzyCheckpoint struct {
	mu sync.RWMutex

	// 检查点信息
	checkpointLSN  uint64    // 检查点LSN
	checkpointTime time.Time // 检查点时间
	prevCheckpoint uint64    // 上一个检查点LSN

	// 脏页信息
	dirtyPages     map[uint64]*DirtyPageInfo // 脏页列表
	minRecoveryLSN uint64                    // 最小恢复LSN

	// 活跃事务
	activeTxns map[int64]*ActiveTxnInfo // 活跃事务列表

	// 文件路径
	checkpointDir  string // 检查点目录
	checkpointFile string // 检查点文件

	// LSN管理器
	lsnManager *LSNManager

	// 统计信息
	stats *CheckpointStats
}

// DirtyPageInfo 脏页信息
type DirtyPageInfo struct {
	PageID      uint64    // 页面ID
	SpaceID     uint32    // 表空间ID
	FirstModLSN uint64    // 第一次修改的LSN
	LastModLSN  uint64    // 最后一次修改的LSN
	ModifyCount uint32    // 修改次数
	ModifyTime  time.Time // 最后修改时间
}

// ActiveTxnInfo 活跃事务信息
type ActiveTxnInfo struct {
	TxID      int64     // 事务ID
	FirstLSN  uint64    // 第一条日志LSN
	LastLSN   uint64    // 最后一条日志LSN
	StartTime time.Time // 开始时间
	State     string    // 状态
}

// CheckpointStats 检查点统计信息
type CheckpointStats struct {
	TotalCheckpoints uint64        `json:"total_checkpoints"`
	LastCheckpoint   time.Time     `json:"last_checkpoint"`
	LastDuration     time.Duration `json:"last_duration"`
	AvgDuration      time.Duration `json:"avg_duration"`
	DirtyPageCount   int           `json:"dirty_page_count"`
	ActiveTxnCount   int           `json:"active_txn_count"`
}

// NewFuzzyCheckpoint 创建模糊检查点管理器
func NewFuzzyCheckpoint(checkpointDir string, lsnManager *LSNManager) (*FuzzyCheckpoint, error) {
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return nil, err
	}

	fc := &FuzzyCheckpoint{
		dirtyPages:     make(map[uint64]*DirtyPageInfo),
		activeTxns:     make(map[int64]*ActiveTxnInfo),
		checkpointDir:  checkpointDir,
		checkpointFile: filepath.Join(checkpointDir, "checkpoint.dat"),
		lsnManager:     lsnManager,
		stats:          &CheckpointStats{},
	}

	// 尝试加载上一个检查点
	fc.loadCheckpoint()

	return fc, nil
}

// CreateCheckpoint 创建模糊检查点
func (fc *FuzzyCheckpoint) CreateCheckpoint() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	startTime := time.Now()

	// 1. 记录当前LSN
	currentLSN := uint64(fc.lsnManager.GetCurrentLSN())
	fc.prevCheckpoint = fc.checkpointLSN
	fc.checkpointLSN = currentLSN
	fc.checkpointTime = time.Now()

	// 2. 计算最小恢复LSN
	fc.calculateMinRecoveryLSN()

	// 3. 写入检查点文件
	if err := fc.writeCheckpoint(); err != nil {
		return err
	}

	// 4. 更新统计
	duration := time.Since(startTime)
	fc.stats.TotalCheckpoints++
	fc.stats.LastCheckpoint = time.Now()
	fc.stats.LastDuration = duration
	fc.stats.DirtyPageCount = len(fc.dirtyPages)
	fc.stats.ActiveTxnCount = len(fc.activeTxns)

	// 更新平均时长
	if fc.stats.TotalCheckpoints > 1 {
		fc.stats.AvgDuration = (fc.stats.AvgDuration*time.Duration(fc.stats.TotalCheckpoints-1) + duration) / time.Duration(fc.stats.TotalCheckpoints)
	} else {
		fc.stats.AvgDuration = duration
	}

	return nil
}

// AddDirtyPage 添加脏页信息
func (fc *FuzzyCheckpoint) AddDirtyPage(pageID uint64, spaceID uint32, lsn uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	page, exists := fc.dirtyPages[pageID]
	if !exists {
		page = &DirtyPageInfo{
			PageID:      pageID,
			SpaceID:     spaceID,
			FirstModLSN: lsn,
			LastModLSN:  lsn,
			ModifyCount: 1,
			ModifyTime:  time.Now(),
		}
		fc.dirtyPages[pageID] = page
	} else {
		page.LastModLSN = lsn
		page.ModifyCount++
		page.ModifyTime = time.Now()
	}
}

// RemoveDirtyPage 移除脏页(页面已刷新)
func (fc *FuzzyCheckpoint) RemoveDirtyPage(pageID uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	delete(fc.dirtyPages, pageID)
}

// AddActiveTxn 添加活跃事务
func (fc *FuzzyCheckpoint) AddActiveTxn(txID int64, firstLSN uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.activeTxns[txID] = &ActiveTxnInfo{
		TxID:      txID,
		FirstLSN:  firstLSN,
		LastLSN:   firstLSN,
		StartTime: time.Now(),
		State:     "Active",
	}
}

// UpdateActiveTxn 更新活跃事务
func (fc *FuzzyCheckpoint) UpdateActiveTxn(txID int64, lsn uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if txn, exists := fc.activeTxns[txID]; exists {
		txn.LastLSN = lsn
	}
}

// RemoveActiveTxn 移除活跃事务
func (fc *FuzzyCheckpoint) RemoveActiveTxn(txID int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	delete(fc.activeTxns, txID)
}

// calculateMinRecoveryLSN 计算最小恢复LSN
func (fc *FuzzyCheckpoint) calculateMinRecoveryLSN() {
	// 最小恢复LSN = min(所有脏页的FirstModLSN, 所有活跃事务的FirstLSN)
	minLSN := fc.checkpointLSN

	// 检查脏页
	for _, page := range fc.dirtyPages {
		if page.FirstModLSN < minLSN {
			minLSN = page.FirstModLSN
		}
	}

	// 检查活跃事务
	for _, txn := range fc.activeTxns {
		if txn.FirstLSN < minLSN {
			minLSN = txn.FirstLSN
		}
	}

	fc.minRecoveryLSN = minLSN
}

// writeCheckpoint 写入检查点文件
func (fc *FuzzyCheckpoint) writeCheckpoint() error {
	file, err := os.Create(fc.checkpointFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// 写入检查点头部
	header := CheckpointHeader{
		Magic:          0x43484B50, // "CHKP"
		Version:        1,
		CheckpointLSN:  fc.checkpointLSN,
		PrevCheckpoint: fc.prevCheckpoint,
		MinRecoveryLSN: fc.minRecoveryLSN,
		Timestamp:      uint64(fc.checkpointTime.Unix()),
		DirtyPageCount: uint32(len(fc.dirtyPages)),
		ActiveTxnCount: uint32(len(fc.activeTxns)),
	}

	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return err
	}

	// 写入脏页列表
	for _, page := range fc.dirtyPages {
		pageEntry := DirtyPageEntry{
			PageID:      page.PageID,
			SpaceID:     page.SpaceID,
			FirstModLSN: page.FirstModLSN,
			LastModLSN:  page.LastModLSN,
			ModifyCount: page.ModifyCount,
		}
		if err := binary.Write(file, binary.BigEndian, &pageEntry); err != nil {
			return err
		}
	}

	// 写入活跃事务列表
	for _, txn := range fc.activeTxns {
		txnEntry := ActiveTxnEntry{
			TxID:     txn.TxID,
			FirstLSN: txn.FirstLSN,
			LastLSN:  txn.LastLSN,
		}
		if err := binary.Write(file, binary.BigEndian, &txnEntry); err != nil {
			return err
		}
	}

	return file.Sync()
}

// loadCheckpoint 加载检查点
func (fc *FuzzyCheckpoint) loadCheckpoint() error {
	file, err := os.Open(fc.checkpointFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，这是正常的
		}
		return err
	}
	defer file.Close()

	// 读取头部
	var header CheckpointHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return err
	}

	// 验证魔数
	if header.Magic != 0x43484B50 {
		return fmt.Errorf("invalid checkpoint magic: %x", header.Magic)
	}

	// 恢复检查点信息
	fc.checkpointLSN = header.CheckpointLSN
	fc.prevCheckpoint = header.PrevCheckpoint
	fc.minRecoveryLSN = header.MinRecoveryLSN
	fc.checkpointTime = time.Unix(int64(header.Timestamp), 0)

	// 读取脏页列表
	for i := uint32(0); i < header.DirtyPageCount; i++ {
		var pageEntry DirtyPageEntry
		if err := binary.Read(file, binary.BigEndian, &pageEntry); err != nil {
			return err
		}

		fc.dirtyPages[pageEntry.PageID] = &DirtyPageInfo{
			PageID:      pageEntry.PageID,
			SpaceID:     pageEntry.SpaceID,
			FirstModLSN: pageEntry.FirstModLSN,
			LastModLSN:  pageEntry.LastModLSN,
			ModifyCount: pageEntry.ModifyCount,
		}
	}

	// 读取活跃事务列表
	for i := uint32(0); i < header.ActiveTxnCount; i++ {
		var txnEntry ActiveTxnEntry
		if err := binary.Read(file, binary.BigEndian, &txnEntry); err != nil {
			return err
		}

		fc.activeTxns[txnEntry.TxID] = &ActiveTxnInfo{
			TxID:     txnEntry.TxID,
			FirstLSN: txnEntry.FirstLSN,
			LastLSN:  txnEntry.LastLSN,
			State:    "Active",
		}
	}

	return nil
}

// GetCheckpointLSN 获取检查点LSN
func (fc *FuzzyCheckpoint) GetCheckpointLSN() uint64 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.checkpointLSN
}

// GetMinRecoveryLSN 获取最小恢复LSN
func (fc *FuzzyCheckpoint) GetMinRecoveryLSN() uint64 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.minRecoveryLSN
}

// GetStats 获取统计信息
func (fc *FuzzyCheckpoint) GetStats() *CheckpointStats {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	stats := *fc.stats
	stats.DirtyPageCount = len(fc.dirtyPages)
	stats.ActiveTxnCount = len(fc.activeTxns)

	return &stats
}

// CheckpointHeader 检查点文件头
type CheckpointHeader struct {
	Magic          uint32 // 魔数 (4字节)
	Version        uint32 // 版本 (4字节)
	CheckpointLSN  uint64 // 检查点LSN (8字节)
	PrevCheckpoint uint64 // 上一个检查点LSN (8字节)
	MinRecoveryLSN uint64 // 最小恢复LSN (8字节)
	Timestamp      uint64 // 时间戳 (8字节)
	DirtyPageCount uint32 // 脏页数量 (4字节)
	ActiveTxnCount uint32 // 活跃事务数量 (4字节)
	Reserved       uint64 // 保留 (8字节)
}

// DirtyPageEntry 脏页条目
type DirtyPageEntry struct {
	PageID      uint64 // 页面ID (8字节)
	SpaceID     uint32 // 表空间ID (4字节)
	FirstModLSN uint64 // 第一次修改LSN (8字节)
	LastModLSN  uint64 // 最后修改LSN (8字节)
	ModifyCount uint32 // 修改次数 (4字节)
	Reserved    uint32 // 保留 (4字节)
}

// ActiveTxnEntry 活跃事务条目
type ActiveTxnEntry struct {
	TxID     int64  // 事务ID (8字节)
	FirstLSN uint64 // 第一条日志LSN (8字节)
	LastLSN  uint64 // 最后一条日志LSN (8字节)
	Reserved uint64 // 保留 (8字节)
}
