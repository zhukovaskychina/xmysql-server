package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"sync"
)

var (
	ErrInvalidTrxSysHeader = errors.New("invalid transaction system page header")
)

const (
	TRX_SYS_HEADER_SIZE     = 38
	TRX_SYS_RSEG_ARRAY_SIZE = 128 * 4 // 128个回滚段,每个4字节

	// 事务系统页面内部偏移量
	TRX_SYS_TRX_ID_STORE      = 0x100
	TRX_SYS_COMMIT_ID_STORE   = 0x108
	TRX_SYS_BINLOG_INFO_LEN   = 0x50
	TRX_SYS_BINLOG_INFO_STORE = 0x110
	TRX_SYS_N_RSEGS           = 128
	TRX_SYS_RSEGS_ARRAY       = 0x160
)

// TrxSysPageWrapper 事务系统页面包装器
type TrxSysPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	mu sync.RWMutex

	// 事务系统信息
	maxTrxID       uint64   // 最大事务ID
	maxCommitID    uint64   // 最大提交ID
	lastBinlogInfo []byte   // 最后的binlog信息
	rsegs          []uint32 // 回滚段列表
}

// NewTrxSysPageWrapper 创建事务系统页面
func NewTrxSysPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *TrxSysPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_TRX_SYS)

	return &TrxSysPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		maxTrxID:        0,
		maxCommitID:     0,
		lastBinlogInfo:  make([]byte, TRX_SYS_BINLOG_INFO_LEN),
		rsegs:           make([]uint32, TRX_SYS_N_RSEGS),
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析事务系统页面
func (tw *TrxSysPageWrapper) ParseFromBytes(data []byte) error {
	tw.Lock()
	defer tw.Unlock()

	if err := tw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	if len(data) < TRX_SYS_RSEGS_ARRAY+TRX_SYS_RSEG_ARRAY_SIZE {
		return ErrInvalidTrxSysHeader
	}

	// 解析事务系统信息
	tw.maxTrxID = binary.LittleEndian.Uint64(data[TRX_SYS_TRX_ID_STORE:])
	tw.maxCommitID = binary.LittleEndian.Uint64(data[TRX_SYS_COMMIT_ID_STORE:])

	// 解析binlog信息
	tw.lastBinlogInfo = make([]byte, TRX_SYS_BINLOG_INFO_LEN)
	copy(tw.lastBinlogInfo, data[TRX_SYS_BINLOG_INFO_STORE:TRX_SYS_BINLOG_INFO_STORE+TRX_SYS_BINLOG_INFO_LEN])

	// 解析回滚段列表
	tw.rsegs = make([]uint32, TRX_SYS_N_RSEGS)
	for i := 0; i < TRX_SYS_N_RSEGS; i++ {
		offset := TRX_SYS_RSEGS_ARRAY + i*4
		tw.rsegs[i] = binary.LittleEndian.Uint32(data[offset:])
	}

	return nil
}

// ToBytes 序列化事务系统页面为字节数组
func (tw *TrxSysPageWrapper) ToBytes() ([]byte, error) {
	tw.RLock()
	defer tw.RUnlock()

	// 创建页面内容
	content := make([]byte, common.PageSize)

	// 写入基础页面头
	base, err := tw.BasePageWrapper.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(content, base[:common.FileHeaderSize])

	// 写入事务系统信息
	binary.LittleEndian.PutUint64(content[TRX_SYS_TRX_ID_STORE:], tw.maxTrxID)
	binary.LittleEndian.PutUint64(content[TRX_SYS_COMMIT_ID_STORE:], tw.maxCommitID)

	// 写入binlog信息
	copy(content[TRX_SYS_BINLOG_INFO_STORE:], tw.lastBinlogInfo)

	// 写入回滚段列表
	for i := 0; i < TRX_SYS_N_RSEGS; i++ {
		offset := TRX_SYS_RSEGS_ARRAY + i*4
		binary.LittleEndian.PutUint32(content[offset:], tw.rsegs[i])
	}

	// 更新基础包装器的内容
	if len(tw.content) != len(content) {
		tw.content = make([]byte, len(content))
	}
	copy(tw.content, content)

	return content, nil
}

// Read 实现PageWrapper接口
func (tw *TrxSysPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if tw.bufferPool != nil {
		if page, err := tw.bufferPool.GetPage(tw.GetSpaceID(), tw.GetPageID()); err == nil {
			if page != nil {
				tw.content = page.GetContent()
				return tw.ParseFromBytes(tw.content)
			}
		}
	}

	// 2. 从磁盘读取
	content, err := tw.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	if tw.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(tw.GetSpaceID(), tw.GetPageID())
		bufferPage.SetContent(content)
		tw.bufferPool.PutPage(bufferPage)
	}

	// 4. 解析内容
	tw.content = content
	return tw.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (tw *TrxSysPageWrapper) Write() error {
	// 1. 序列化页面内容
	content, err := tw.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	if tw.bufferPool != nil {
		if page, err := tw.bufferPool.GetPage(tw.GetSpaceID(), tw.GetPageID()); err == nil {
			if page != nil {
				page.SetContent(content)
				page.MarkDirty()
			}
		}
	}

	// 3. 写入磁盘
	return tw.writeToDisk(content)
}

// 事务系统页面特有的方法

// GetMaxTrxID 获取最大事务ID
func (tw *TrxSysPageWrapper) GetMaxTrxID() uint64 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	return tw.maxTrxID
}

// SetMaxTrxID 设置最大事务ID
func (tw *TrxSysPageWrapper) SetMaxTrxID(id uint64) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.maxTrxID = id
	tw.MarkDirty()
}

// GetMaxCommitID 获取最大提交ID
func (tw *TrxSysPageWrapper) GetMaxCommitID() uint64 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	return tw.maxCommitID
}

// SetMaxCommitID 设置最大提交ID
func (tw *TrxSysPageWrapper) SetMaxCommitID(id uint64) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.maxCommitID = id
	tw.MarkDirty()
}

// GetLastBinlogInfo 获取最后的binlog信息
func (tw *TrxSysPageWrapper) GetLastBinlogInfo() []byte {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	result := make([]byte, len(tw.lastBinlogInfo))
	copy(result, tw.lastBinlogInfo)
	return result
}

// SetLastBinlogInfo 设置最后的binlog信息
func (tw *TrxSysPageWrapper) SetLastBinlogInfo(info []byte) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	maxLen := TRX_SYS_BINLOG_INFO_LEN
	if len(info) > maxLen {
		tw.lastBinlogInfo = make([]byte, maxLen)
		copy(tw.lastBinlogInfo, info[:maxLen])
	} else {
		tw.lastBinlogInfo = make([]byte, maxLen)
		copy(tw.lastBinlogInfo, info)
	}

	tw.MarkDirty()
}

// GetRollbackSegment 获取回滚段
func (tw *TrxSysPageWrapper) GetRollbackSegment(slot int) uint32 {
	if slot < 0 || slot >= TRX_SYS_N_RSEGS {
		return 0
	}
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	return tw.rsegs[slot]
}

// SetRollbackSegment 设置回滚段
func (tw *TrxSysPageWrapper) SetRollbackSegment(slot int, pageNo uint32) {
	if slot < 0 || slot >= TRX_SYS_N_RSEGS {
		return
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.rsegs[slot] = pageNo
	tw.MarkDirty()
}

// GetAllRollbackSegments 获取所有回滚段
func (tw *TrxSysPageWrapper) GetAllRollbackSegments() []uint32 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	result := make([]uint32, len(tw.rsegs))
	copy(result, tw.rsegs)
	return result
}

// AllocateRollbackSegmentSlot 分配一个回滚段槽
func (tw *TrxSysPageWrapper) AllocateRollbackSegmentSlot() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 查找空闲槽
	for i := 0; i < TRX_SYS_N_RSEGS; i++ {
		if tw.rsegs[i] == 0 {
			return i
		}
	}

	return -1 // 没有可用槽
}

// FreeRollbackSegmentSlot 释放回滚段槽
func (tw *TrxSysPageWrapper) FreeRollbackSegmentSlot(slot int) {
	if slot < 0 || slot >= TRX_SYS_N_RSEGS {
		return
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.rsegs[slot] = 0
	tw.MarkDirty()
}

// GetUsedRollbackSegmentCount 获取已使用的回滚段数量
func (tw *TrxSysPageWrapper) GetUsedRollbackSegmentCount() int {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	count := 0
	for _, rseg := range tw.rsegs {
		if rseg != 0 {
			count++
		}
	}
	return count
}

// NextTrxID 获取下一个事务ID并递增
func (tw *TrxSysPageWrapper) NextTrxID() uint64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.maxTrxID++
	tw.MarkDirty()
	return tw.maxTrxID
}

// NextCommitID 获取下一个提交ID并递增
func (tw *TrxSysPageWrapper) NextCommitID() uint64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.maxCommitID++
	tw.MarkDirty()
	return tw.maxCommitID
}

// Validate 验证事务系统页面数据完整性
func (tw *TrxSysPageWrapper) Validate() error {
	tw.RLock()
	defer tw.RUnlock()

	if tw.maxTrxID < tw.maxCommitID {
		return errors.New("max transaction ID less than max commit ID")
	}

	if len(tw.rsegs) != TRX_SYS_N_RSEGS {
		return errors.New("rollback segment array size mismatch")
	}

	if len(tw.lastBinlogInfo) != TRX_SYS_BINLOG_INFO_LEN {
		return errors.New("binlog info size mismatch")
	}

	return nil
}

// 内部方法：从磁盘读取
func (tw *TrxSysPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取页面的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return make([]byte, common.PageSize), nil
}

// 内部方法：写入磁盘
func (tw *TrxSysPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return nil
}
