package manager

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"
)

// ============ LOG-003: Mini-Transaction (MTR) 实现 ============

// MiniTransaction Mini-transaction（小事务）
// MTR是InnoDB中用于保证原子操作的机制，一个MTR包含一组相关的Redo日志
// MTR的特点：
// 1. 原子性：MTR中的所有操作要么全部成功，要么全部失败
// 2. 顺序性：MTR中的日志按顺序写入
// 3. 组提交：多个MTR可以批量提交，减少fsync次数
type MiniTransaction struct {
	mu sync.Mutex

	// MTR标识
	mtrID      uint64    // MTR唯一ID
	startLSN   uint64    // 起始LSN
	endLSN     uint64    // 结束LSN
	createTime time.Time // 创建时间

	// 日志条目
	logEntries     []RedoLogEntry // Redo日志列表
	totalSize      int            // 总大小（字节）
	compressedSize int            // 压缩后大小

	// 状态
	state      MTRState // MTR状态
	committed  bool     // 是否已提交
	compressed bool     // 是否已压缩

	// 上下文信息
	txID      int64  // 关联的事务ID
	threadID  uint32 // 线程ID
	operation string // 操作类型描述

	// 修改的页面
	modifiedPages map[uint64]bool // 修改的页面集合（用于脏页管理）
}

// MTRState MTR状态
type MTRState int

const (
	MTR_STATE_ACTIVE    MTRState = iota // 活跃状态
	MTR_STATE_PREPARING                 // 准备提交
	MTR_STATE_COMMITTED                 // 已提交
	MTR_STATE_ABORTED                   // 已中止
)

// NewMiniTransaction 创建新的Mini-transaction
func NewMiniTransaction(mtrID uint64, txID int64) *MiniTransaction {
	return &MiniTransaction{
		mtrID:         mtrID,
		txID:          txID,
		createTime:    time.Now(),
		state:         MTR_STATE_ACTIVE,
		logEntries:    make([]RedoLogEntry, 0, 8),
		modifiedPages: make(map[uint64]bool),
	}
}

// AddLogEntry 添加Redo日志条目
func (mtr *MiniTransaction) AddLogEntry(entry RedoLogEntry) error {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	if mtr.state != MTR_STATE_ACTIVE {
		return fmt.Errorf("MTR is not active, current state: %d", mtr.state)
	}

	// 记录修改的页面
	mtr.modifiedPages[entry.PageID] = true

	// 添加日志条目
	mtr.logEntries = append(mtr.logEntries, entry)

	// 更新大小
	mtr.totalSize += entry.Size()

	return nil
}

// SetOperation 设置操作描述
func (mtr *MiniTransaction) SetOperation(operation string) {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()
	mtr.operation = operation
}

// GetModifiedPages 获取修改的页面列表
func (mtr *MiniTransaction) GetModifiedPages() []uint64 {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	pages := make([]uint64, 0, len(mtr.modifiedPages))
	for pageID := range mtr.modifiedPages {
		pages = append(pages, pageID)
	}
	return pages
}

// Prepare 准备提交
func (mtr *MiniTransaction) Prepare() error {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	if mtr.state != MTR_STATE_ACTIVE {
		return fmt.Errorf("MTR is not active")
	}

	mtr.state = MTR_STATE_PREPARING
	return nil
}

// Commit 提交MTR
func (mtr *MiniTransaction) Commit(startLSN, endLSN uint64) error {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	if mtr.state != MTR_STATE_PREPARING {
		return fmt.Errorf("MTR is not in preparing state")
	}

	mtr.startLSN = startLSN
	mtr.endLSN = endLSN
	mtr.state = MTR_STATE_COMMITTED
	mtr.committed = true

	return nil
}

// Abort 中止MTR
func (mtr *MiniTransaction) Abort() {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	mtr.state = MTR_STATE_ABORTED
	mtr.logEntries = nil
}

// Serialize 序列化MTR为字节流
// 格式：[MTR Header] + [Log Entries]
func (mtr *MiniTransaction) Serialize() ([]byte, error) {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	buf := new(bytes.Buffer)

	// 写入MTR头部
	header := MTRHeader{
		MtrID:      mtr.mtrID,
		TxID:       mtr.txID,
		StartLSN:   mtr.startLSN,
		EndLSN:     mtr.endLSN,
		LogCount:   uint16(len(mtr.logEntries)),
		TotalSize:  uint32(mtr.totalSize),
		ThreadID:   mtr.threadID,
		CreateTime: uint64(mtr.createTime.Unix()),
		Flags:      mtr.buildFlags(),
	}

	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		return nil, err
	}

	// 写入操作描述长度和内容
	operationBytes := []byte(mtr.operation)
	operationLen := uint16(len(operationBytes))
	if err := binary.Write(buf, binary.BigEndian, operationLen); err != nil {
		return nil, err
	}
	buf.Write(operationBytes)

	// 写入日志条目
	for _, entry := range mtr.logEntries {
		entryBytes, err := entry.Serialize()
		if err != nil {
			return nil, err
		}
		buf.Write(entryBytes)
	}

	// 计算CRC32校验和
	data := buf.Bytes()
	checksum := crc32.ChecksumIEEE(data)
	if err := binary.Write(buf, binary.BigEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Compress 压缩MTR（使用gzip）
func (mtr *MiniTransaction) Compress() ([]byte, error) {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	// 序列化
	data, err := mtr.serializeUnlocked()
	if err != nil {
		return nil, err
	}

	// gzip压缩
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	mtr.compressed = true
	mtr.compressedSize = buf.Len()

	return buf.Bytes(), nil
}

// serializeUnlocked 序列化（不加锁版本）
func (mtr *MiniTransaction) serializeUnlocked() ([]byte, error) {
	mtr.mu.Unlock()
	defer mtr.mu.Lock()
	return mtr.Serialize()
}

// buildFlags 构建标志位
func (mtr *MiniTransaction) buildFlags() uint16 {
	var flags uint16
	if mtr.compressed {
		flags |= MTR_FLAG_COMPRESSED
	}
	if mtr.committed {
		flags |= MTR_FLAG_COMMITTED
	}
	return flags
}

// GetStats 获取MTR统计信息
func (mtr *MiniTransaction) GetStats() *MTRStats {
	mtr.mu.Lock()
	defer mtr.mu.Unlock()

	return &MTRStats{
		MtrID:          mtr.mtrID,
		LogCount:       len(mtr.logEntries),
		TotalSize:      mtr.totalSize,
		CompressedSize: mtr.compressedSize,
		ModifiedPages:  len(mtr.modifiedPages),
		State:          mtr.state,
		Duration:       time.Since(mtr.createTime),
	}
}

// MTRHeader MTR头部
type MTRHeader struct {
	MtrID      uint64 // MTR ID (8字节)
	TxID       int64  // 事务ID (8字节)
	StartLSN   uint64 // 起始LSN (8字节)
	EndLSN     uint64 // 结束LSN (8字节)
	LogCount   uint16 // 日志条目数量 (2字节)
	TotalSize  uint32 // 总大小 (4字节)
	ThreadID   uint32 // 线程ID (4字节)
	CreateTime uint64 // 创建时间 (8字节)
	Flags      uint16 // 标志位 (2字节)
	Reserved   uint16 // 保留 (2字节)
}

// MTR标志位
const (
	MTR_FLAG_COMPRESSED = 1 << 0 // 压缩标志
	MTR_FLAG_COMMITTED  = 1 << 1 // 已提交标志
)

// MTRStats MTR统计信息
type MTRStats struct {
	MtrID          uint64        `json:"mtr_id"`
	LogCount       int           `json:"log_count"`
	TotalSize      int           `json:"total_size"`
	CompressedSize int           `json:"compressed_size"`
	ModifiedPages  int           `json:"modified_pages"`
	State          MTRState      `json:"state"`
	Duration       time.Duration `json:"duration"`
}

// ============ MTR Manager ============

// MTRManager MTR管理器
type MTRManager struct {
	mu sync.RWMutex

	// MTR池
	activeMTRs   map[uint64]*MiniTransaction // 活跃的MTR
	nextMtrID    uint64                      // 下一个MTR ID
	mtrIDCounter uint64                      // MTR ID计数器

	// 统计信息
	stats *MTRManagerStats

	// 配置
	config *MTRConfig
}

// MTRConfig MTR配置
type MTRConfig struct {
	MaxMTRSize        int           // 最大MTR大小
	MaxLogEntries     int           // 最大日志条目数
	CompressionLevel  int           // 压缩级别 (0-9)
	EnableCompression bool          // 是否启用压缩
	MTRTimeout        time.Duration // MTR超时时间
}

// NewMTRManager 创建MTR管理器
func NewMTRManager(config *MTRConfig) *MTRManager {
	if config == nil {
		config = &MTRConfig{
			MaxMTRSize:        1024 * 1024, // 1MB
			MaxLogEntries:     1000,
			CompressionLevel:  6,
			EnableCompression: true,
			MTRTimeout:        30 * time.Second,
		}
	}

	return &MTRManager{
		activeMTRs:   make(map[uint64]*MiniTransaction),
		nextMtrID:    1,
		mtrIDCounter: 0,
		stats:        &MTRManagerStats{},
		config:       config,
	}
}

// BeginMTR 开始一个新的MTR
func (mm *MTRManager) BeginMTR(txID int64) *MiniTransaction {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// 分配MTR ID
	mtrID := mm.nextMtrID
	mm.nextMtrID++
	mm.mtrIDCounter++

	// 创建MTR
	mtr := NewMiniTransaction(mtrID, txID)

	// 加入活跃MTR列表
	mm.activeMTRs[mtrID] = mtr

	// 更新统计
	mm.stats.TotalMTRs++
	mm.stats.ActiveMTRs++

	return mtr
}

// CommitMTR 提交MTR
func (mm *MTRManager) CommitMTR(mtr *MiniTransaction, startLSN, endLSN uint64) error {
	// 准备提交
	if err := mtr.Prepare(); err != nil {
		return err
	}

	// 提交
	if err := mtr.Commit(startLSN, endLSN); err != nil {
		return err
	}

	// 从活跃列表移除
	mm.mu.Lock()
	delete(mm.activeMTRs, mtr.mtrID)
	mm.stats.ActiveMTRs--
	mm.stats.CommittedMTRs++
	mm.mu.Unlock()

	return nil
}

// AbortMTR 中止MTR
func (mm *MTRManager) AbortMTR(mtr *MiniTransaction) {
	mtr.Abort()

	mm.mu.Lock()
	delete(mm.activeMTRs, mtr.mtrID)
	mm.stats.ActiveMTRs--
	mm.stats.AbortedMTRs++
	mm.mu.Unlock()
}

// GetActiveMTRs 获取所有活跃MTR
func (mm *MTRManager) GetActiveMTRs() []*MiniTransaction {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	mtrs := make([]*MiniTransaction, 0, len(mm.activeMTRs))
	for _, mtr := range mm.activeMTRs {
		mtrs = append(mtrs, mtr)
	}
	return mtrs
}

// GetStats 获取统计信息
func (mm *MTRManager) GetStats() *MTRManagerStats {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	stats := *mm.stats
	return &stats
}

// MTRManagerStats MTR管理器统计
type MTRManagerStats struct {
	TotalMTRs     uint64 `json:"total_mtrs"`
	ActiveMTRs    uint64 `json:"active_mtrs"`
	CommittedMTRs uint64 `json:"committed_mtrs"`
	AbortedMTRs   uint64 `json:"aborted_mtrs"`
}

// ============ Redo Log Entry扩展 ============

// Size 计算Redo日志条目的大小
func (e *RedoLogEntry) Size() int {
	// LSN(8) + TrxID(8) + PageID(8) + Type(1) + DataLen(2) + Data(n)
	return 8 + 8 + 8 + 1 + 2 + len(e.Data)
}

// Serialize 序列化Redo日志条目
func (e *RedoLogEntry) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入LSN
	if err := binary.Write(buf, binary.BigEndian, e.LSN); err != nil {
		return nil, err
	}

	// 写入事务ID
	if err := binary.Write(buf, binary.BigEndian, e.TrxID); err != nil {
		return nil, err
	}

	// 写入页面ID
	if err := binary.Write(buf, binary.BigEndian, e.PageID); err != nil {
		return nil, err
	}

	// 写入操作类型
	if err := buf.WriteByte(byte(e.Type)); err != nil {
		return nil, err
	}

	// 写入数据长度
	dataLen := uint16(len(e.Data))
	if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
		return nil, err
	}

	// 写入数据
	if _, err := buf.Write(e.Data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
