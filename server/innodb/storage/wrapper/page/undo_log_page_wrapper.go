package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"sync"
)

var (
	ErrInvalidUndoHeader = errors.New("invalid undo log header")
	ErrInvalidUndoRecord = errors.New("invalid undo record")
)

// UndoLogPageWrapper UNDO LOG页面包装器
type UndoLogPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	mu sync.RWMutex

	// UNDO LOG信息
	trxID       uint64       // 事务ID
	undoType    uint16       // UNDO类型
	tableID     uint64       // 表ID
	firstRecPos uint16       // 第一条记录位置
	nextLogPage uint32       // 下一个日志页面
	lastLogPage uint32       // 上一个日志页面
	records     []UndoRecord // UNDO记录列表
}

// UndoRecord UNDO记录
type UndoRecord struct {
	Type      UndoRecordType // 记录类型
	TableID   uint64         // 表ID
	RecordID  uint64         // 记录ID
	Data      []byte         // 记录数据
	NextPos   uint16         // 下一条记录位置
	PrevPos   uint16         // 上一条记录位置
	Timestamp uint64         // 时间戳
}

// UndoRecordType UNDO记录类型
type UndoRecordType uint8

const (
	UndoInsert UndoRecordType = iota
	UndoUpdate
	UndoDelete
	UndoPurge
)

const (
	UNDO_HEADER_SIZE = 38 // 8(trxID) + 2(type) + 8(tableID) + 2(firstRecPos) + 4(nextPage) + 4(lastPage) + 2(recordCount) + 8(reserved)
)

// NewUndoLogPageWrapper 创建UNDO LOG页面
func NewUndoLogPageWrapper(id, spaceID, pageNo uint32, bp *buffer_pool.BufferPool) *UndoLogPageWrapper {
	return &UndoLogPageWrapper{
		BasePageWrapper: NewBasePageWrapper(pageNo, spaceID, common.FIL_PAGE_UNDO_LOG),
		bufferPool:      bp,
		records:         make([]UndoRecord, 0),
	}
}

// Read 实现PageWrapper接口
func (uw *UndoLogPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if page, _ := uw.bufferPool.GetPage(uw.GetSpaceID(), uw.GetPageID()); page != nil {
		uw.content = page.GetContent()
		return uw.ParseFromBytes(uw.content)
	}

	// 2. 从磁盘读取
	content, err := uw.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	bufferPage := buffer_pool.NewBufferPage(uw.GetSpaceID(), uw.GetPageID())
	bufferPage.SetContent(content)
	uw.bufferPool.PutPage(bufferPage)

	// 4. 解析内容
	uw.content = content
	return uw.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (uw *UndoLogPageWrapper) Write() error {
	// 1. 序列化内容
	content, err := uw.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	bufferPage := uw.GetBufferPage()
	if bufferPage == nil {
		bufferPage = buffer_pool.NewBufferPage(uw.GetSpaceID(), uw.GetPageID())
		uw.SetBufferPage(bufferPage)
	}
	bufferPage.SetContent(content)
	bufferPage.MarkDirty()
	uw.bufferPool.PutPage(bufferPage)

	// 3. 根据策略决定是否写入磁盘
	if uw.needFlush() {
		return uw.writeToDisk(content)
	}

	return nil
}

// ParseFromBytes 从字节解析
func (uw *UndoLogPageWrapper) ParseFromBytes(content []byte) error {
	uw.mu.Lock()
	defer uw.mu.Unlock()

	if len(content) < common.FileHeaderSize+UNDO_HEADER_SIZE {
		return ErrInvalidUndoHeader
	}

	// 解析基础页面头
	if err := uw.BasePageWrapper.ParseFromBytes(content); err != nil {
		return err
	}

	// 解析UNDO LOG头
	offset := common.FileHeaderSize
	uw.trxID = binary.LittleEndian.Uint64(content[offset:])
	uw.undoType = binary.LittleEndian.Uint16(content[offset+8:])
	uw.tableID = binary.LittleEndian.Uint64(content[offset+10:])
	uw.firstRecPos = binary.LittleEndian.Uint16(content[offset+18:])
	uw.nextLogPage = binary.LittleEndian.Uint32(content[offset+20:])
	uw.lastLogPage = binary.LittleEndian.Uint32(content[offset+24:])
	recordCount := binary.LittleEndian.Uint16(content[offset+28:])

	// 解析UNDO记录
	uw.records = make([]UndoRecord, 0, recordCount)
	pos := uw.firstRecPos

	for i := uint16(0); i < recordCount && pos > 0; i++ {
		if pos >= uint16(len(content)) {
			return ErrInvalidUndoRecord
		}

		record := UndoRecord{}
		record.Type = UndoRecordType(content[pos])
		record.TableID = binary.LittleEndian.Uint64(content[pos+1:])
		record.RecordID = binary.LittleEndian.Uint64(content[pos+9:])
		record.NextPos = binary.LittleEndian.Uint16(content[pos+17:])
		record.PrevPos = binary.LittleEndian.Uint16(content[pos+19:])
		record.Timestamp = binary.LittleEndian.Uint64(content[pos+21:])
		dataLen := binary.LittleEndian.Uint16(content[pos+29:])

		if pos+31+uint16(dataLen) > uint16(len(content)) {
			return ErrInvalidUndoRecord
		}

		record.Data = make([]byte, dataLen)
		copy(record.Data, content[pos+31:pos+31+uint16(dataLen)])

		uw.records = append(uw.records, record)
		pos = record.NextPos
	}

	return nil
}

// ToBytes 转换为字节
func (uw *UndoLogPageWrapper) ToBytes() ([]byte, error) {
	uw.mu.RLock()
	defer uw.mu.RUnlock()

	// 计算记录总大小
	recordsSize := 0
	for _, rec := range uw.records {
		recordsSize += 31 + len(rec.Data) // 31 = 1(type) + 8(tableID) + 8(recordID) + 2(nextPos) + 2(prevPos) + 8(timestamp) + 2(dataLen)
	}

	// 计算总大小
	totalSize := common.FileHeaderSize + UNDO_HEADER_SIZE + recordsSize + common.FileTrailerSize

	// 分配缓冲区
	content := make([]byte, totalSize)

	// 写入基础页面头
	base, err := uw.BasePageWrapper.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(content, base[:common.FileHeaderSize])

	// 写入UNDO LOG头
	offset := common.FileHeaderSize
	binary.LittleEndian.PutUint64(content[offset:], uw.trxID)
	binary.LittleEndian.PutUint16(content[offset+8:], uw.undoType)
	binary.LittleEndian.PutUint64(content[offset+10:], uw.tableID)
	binary.LittleEndian.PutUint16(content[offset+18:], uw.firstRecPos)
	binary.LittleEndian.PutUint32(content[offset+20:], uw.nextLogPage)
	binary.LittleEndian.PutUint32(content[offset+24:], uw.lastLogPage)
	binary.LittleEndian.PutUint16(content[offset+28:], uint16(len(uw.records)))

	// 写入UNDO记录
	pos := common.FileHeaderSize + UNDO_HEADER_SIZE
	for _, rec := range uw.records {
		content[pos] = byte(rec.Type)
		binary.LittleEndian.PutUint64(content[pos+1:], rec.TableID)
		binary.LittleEndian.PutUint64(content[pos+9:], rec.RecordID)
		binary.LittleEndian.PutUint16(content[pos+17:], rec.NextPos)
		binary.LittleEndian.PutUint16(content[pos+19:], rec.PrevPos)
		binary.LittleEndian.PutUint64(content[pos+21:], rec.Timestamp)
		binary.LittleEndian.PutUint16(content[pos+29:], uint16(len(rec.Data)))
		copy(content[pos+31:], rec.Data)
		pos += 31 + len(rec.Data)
	}

	return content, nil
}

// AddUndoRecord 添加UNDO记录
func (uw *UndoLogPageWrapper) AddUndoRecord(record UndoRecord) error {
	uw.mu.Lock()
	defer uw.mu.Unlock()

	// 设置记录位置
	if len(uw.records) == 0 {
		uw.firstRecPos = uint16(common.FileHeaderSize + UNDO_HEADER_SIZE)
		record.PrevPos = 0
	} else {
		lastRecord := &uw.records[len(uw.records)-1]
		record.PrevPos = lastRecord.NextPos
	}
	record.NextPos = 0

	uw.records = append(uw.records, record)
	uw.MarkDirty()

	return nil
}

// GetUndoRecords 获取UNDO记录
func (uw *UndoLogPageWrapper) GetUndoRecords() []UndoRecord {
	uw.mu.RLock()
	defer uw.mu.RUnlock()

	records := make([]UndoRecord, len(uw.records))
	copy(records, uw.records)
	return records
}

// GetTransactionID 获取事务ID
func (uw *UndoLogPageWrapper) GetTransactionID() uint64 {
	uw.mu.RLock()
	defer uw.mu.RUnlock()
	return uw.trxID
}

// SetTransactionID 设置事务ID
func (uw *UndoLogPageWrapper) SetTransactionID(id uint64) {
	uw.mu.Lock()
	defer uw.mu.Unlock()
	uw.trxID = id
	uw.MarkDirty()
}

// 辅助方法
func (uw *UndoLogPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取逻辑
	return nil, nil
}

func (uw *UndoLogPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘逻辑
	return nil
}

func (uw *UndoLogPageWrapper) needFlush() bool {
	// TODO: 实现刷新策略
	return false
}

func (uw *UndoLogPageWrapper) GetBufferPage() *buffer_pool.BufferPage {
	return nil
}

func (uw *UndoLogPageWrapper) SetBufferPage(page *buffer_pool.BufferPage) {

}
