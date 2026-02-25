package mvcc

import (
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	formatmvcc "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

// 确保MVCCIndexPage实现了IMVCCPage接口
var _ IMVCCPage = (*MVCCIndexPage)(nil)

// MVCCIndexPage MVCC索引页面实现
type MVCCIndexPage struct {
	sync.RWMutex

	// 基础信息
	id       uint32
	spaceID  uint32
	pageType common.PageType
	size     uint32
	lsn      uint64

	// MVCC信息
	version uint64
	txID    uint64
	rollPtr []byte

	// 记录管理
	records   map[uint16]*formatmvcc.RecordVersion
	freeSlots []uint16

	// 锁管理
	locks map[uint64]LockMode

	// 脏页标记
	dirty bool
}

// NewMVCCIndexPage 创建MVCC索引页面
func NewMVCCIndexPage(id, spaceID uint32) *MVCCIndexPage {
	return &MVCCIndexPage{
		id:        id,
		spaceID:   spaceID,
		pageType:  common.FILE_PAGE_INDEX,
		records:   make(map[uint16]*formatmvcc.RecordVersion),
		freeSlots: make([]uint16, 0),
		locks:     make(map[uint64]LockMode),
	}
}

// ID 实现Page接口
func (p *MVCCIndexPage) ID() uint32 {
	return p.id
}

// Type 实现Page接口
func (p *MVCCIndexPage) Type() basic.PageType {
	return p.pageType
}

// SpaceID 实现Page接口
func (p *MVCCIndexPage) SpaceID() uint32 {
	return p.spaceID
}

// Size 实现Page接口
func (p *MVCCIndexPage) Size() uint32 {
	return p.size
}

// LSN 实现Page接口
func (p *MVCCIndexPage) LSN() uint64 {
	return p.lsn
}

// ReadFrom 实现Page接口
func (p *MVCCIndexPage) ReadFrom(r io.Reader) (int64, error) {
	// TODO: 实现页面反序列化
	return 0, nil
}

// WriteTo 实现Page接口
func (p *MVCCIndexPage) WriteTo(w io.Writer) (int64, error) {
	// TODO: 实现页面序列化
	return 0, nil
}

// Init 实现Page接口
func (p *MVCCIndexPage) Init() error {
	p.Lock()
	defer p.Unlock()

	// 初始化空闲槽位
	for i := uint16(0); i < 100; i++ {
		p.freeSlots = append(p.freeSlots, i)
	}

	return nil
}

// Release 实现basic.IPage接口
func (p *MVCCIndexPage) Release() {
	p.Lock()
	defer p.Unlock()

	// 清理资源
	p.records = nil
	p.freeSlots = nil
	p.locks = nil
}

// IsDirty 实现Page接口
func (p *MVCCIndexPage) IsDirty() bool {
	return p.dirty
}

// MarkDirty 实现Page接口
func (p *MVCCIndexPage) MarkDirty() {
	p.dirty = true
}

// ClearDirty 实现Page接口
func (p *MVCCIndexPage) ClearDirty() {
	p.dirty = false
}

// GetVersion 实现MVCCPage接口
func (p *MVCCIndexPage) GetVersion() uint64 {
	return p.version
}

// SetVersion 实现MVCCPage接口
func (p *MVCCIndexPage) SetVersion(version uint64) {
	p.version = version
}

// GetTxID 实现MVCCPage接口
func (p *MVCCIndexPage) GetTxID() uint64 {
	return p.txID
}

// SetTxID 实现MVCCPage接口
func (p *MVCCIndexPage) SetTxID(txID uint64) {
	p.txID = txID
}

// GetRollPtr 实现IMVCCPage接口
func (p *MVCCIndexPage) GetRollPtr() []byte {
	p.RLock()
	defer p.RUnlock()
	if p.rollPtr == nil {
		return nil
	}
	result := make([]byte, len(p.rollPtr))
	copy(result, p.rollPtr)
	return result
}

// SetRollPtr 实现IMVCCPage接口
func (p *MVCCIndexPage) SetRollPtr(ptr []byte) {
	p.Lock()
	defer p.Unlock()
	if ptr == nil {
		p.rollPtr = nil
		return
	}
	p.rollPtr = make([]byte, len(ptr))
	copy(p.rollPtr, ptr)
}

// GetContent 实现basic.IPage接口
func (p *MVCCIndexPage) GetContent() []byte {
	// 返回页面内容的副本
	// 对于MVCC页面，内容包括所有记录的序列化数据
	data, _ := p.ToBytes()
	return data
}

// GetData 实现basic.IPage接口（与GetContent相同）
func (p *MVCCIndexPage) GetData() []byte {
	return p.GetContent()
}

// SetData 实现basic.IPage接口
func (p *MVCCIndexPage) SetData(data []byte) error {
	return p.ParseFromBytes(data)
}

// SetContent 实现basic.IPage接口
func (p *MVCCIndexPage) SetContent(content []byte) {
	_ = p.SetData(content)
}

// GetPageID 实现basic.IPage接口
func (p *MVCCIndexPage) GetPageID() uint32 {
	return p.id
}

// GetPageNo 实现basic.IPage接口
func (p *MVCCIndexPage) GetPageNo() uint32 {
	return p.id
}

// GetSpaceID 实现basic.IPage接口
func (p *MVCCIndexPage) GetSpaceID() uint32 {
	return p.spaceID
}

// GetPageType 实现basic.IPage接口
func (p *MVCCIndexPage) GetPageType() basic.PageType {
	return p.pageType
}

// GetSize 实现basic.IPage接口
func (p *MVCCIndexPage) GetSize() uint32 {
	return p.size
}

// GetLSN 实现basic.IPage接口
func (p *MVCCIndexPage) GetLSN() uint64 {
	return p.lsn
}

// SetLSN 实现basic.IPage接口
func (p *MVCCIndexPage) SetLSN(lsn uint64) {
	p.lsn = lsn
}

// SetDirty 实现basic.IPage接口
func (p *MVCCIndexPage) SetDirty(dirty bool) {
	p.dirty = dirty
}

// GetState 实现basic.IPage接口
func (p *MVCCIndexPage) GetState() basic.PageState {
	// MVCC页面默认为活跃状态
	if p.dirty {
		return basic.PageStateDirty
	}
	return basic.PageStateClean
}

// SetState 实现basic.IPage接口
func (p *MVCCIndexPage) SetState(state basic.PageState) {
	// 根据状态设置dirty标记
	p.dirty = (state == basic.PageStateDirty)
}

// Pin 实现basic.IPage接口
func (p *MVCCIndexPage) Pin() {
	// MVCC页面的pin操作（可以扩展）
}

// Unpin 实现basic.IPage接口
func (p *MVCCIndexPage) Unpin() {
	// MVCC页面的unpin操作（可以扩展）
}

// Read 实现basic.IPage接口
func (p *MVCCIndexPage) Read() error {
	// TODO: 实现从磁盘读取
	return nil
}

// Write 实现basic.IPage接口
func (p *MVCCIndexPage) Write() error {
	// TODO: 实现写入磁盘
	return nil
}

// IsLeafPage 实现basic.IPage接口
func (p *MVCCIndexPage) IsLeafPage() bool {
	// MVCC索引页面默认为叶子页面
	return true
}

// CreateSnapshot 实现MVCCPage接口
func (p *MVCCIndexPage) CreateSnapshot() (*PageSnapshot, error) {
	p.RLock()
	defer p.RUnlock()

	snap := &PageSnapshot{
		PageID:   p.id,
		Version:  p.version,
		TxID:     p.txID,
		CreateTS: time.Now(),
		Records:  make(map[uint16]*formatmvcc.RecordVersion),
	}

	// 复制记录版本
	for slot, rec := range p.records {
		snap.Records[slot] = rec.Clone()
	}

	return snap, nil
}

// RestoreSnapshot 实现MVCCPage接口
func (p *MVCCIndexPage) RestoreSnapshot(snap *PageSnapshot) error {
	p.Lock()
	defer p.Unlock()

	// 恢复页面信息
	p.version = snap.Version
	p.txID = snap.TxID

	// 恢复记录
	p.records = make(map[uint16]*formatmvcc.RecordVersion)
	for slot, rec := range snap.Records {
		p.records[slot] = rec.Clone()
	}

	p.MarkDirty()
	return nil
}

// AcquireLock 实现MVCCPage接口
func (p *MVCCIndexPage) AcquireLock(txID uint64, mode LockMode) error {
	p.Lock()
	defer p.Unlock()

	// 检查锁冲突
	for id, m := range p.locks {
		if id != txID {
			if m == LockModeExclusive || mode == LockModeExclusive {
				return ErrLockConflict
			}
		}
	}

	p.locks[txID] = mode
	return nil
}

// ReleaseLock 实现MVCCPage接口
func (p *MVCCIndexPage) ReleaseLock(txID uint64) error {
	p.Lock()
	defer p.Unlock()

	delete(p.locks, txID)
	return nil
}

// GetRecord 获取记录
func (p *MVCCIndexPage) GetRecord(slot uint16) (basic.Row, error) {
	p.RLock()
	defer p.RUnlock()

	rec, ok := p.records[slot]
	if !ok {
		return nil, ErrRecordNotFound
	}

	return rec.Value, nil
}

// InsertRecord 插入记录
func (p *MVCCIndexPage) InsertRecord(key basic.Value, value basic.Row) error {
	p.Lock()
	defer p.Unlock()

	// 获取空闲槽位
	if len(p.freeSlots) == 0 {
		return ErrPageFull
	}
	slot := p.freeSlots[0]
	p.freeSlots = p.freeSlots[1:]

	// 创建新记录版本
	rec := formatmvcc.NewRecordVersion(p.version, p.txID, 0, key, value)

	p.records[slot] = rec
	p.MarkDirty()

	return nil
}

// DeleteRecord 删除记录
func (p *MVCCIndexPage) DeleteRecord(slot uint16) error {
	p.Lock()
	defer p.Unlock()

	if _, ok := p.records[slot]; !ok {
		return ErrRecordNotFound
	}

	delete(p.records, slot)
	p.freeSlots = append(p.freeSlots, slot)
	p.MarkDirty()

	return nil
}

// UpdateRecord 更新记录
func (p *MVCCIndexPage) UpdateRecord(slot uint16, value basic.Row) error {
	p.Lock()
	defer p.Unlock()

	rec, ok := p.records[slot]
	if !ok {
		return ErrRecordNotFound
	}

	// 创建新版本
	newRec := formatmvcc.NewRecordVersion(p.version, p.txID, 0, rec.GetKey(), value)
	newRec.SetNext(rec)

	p.records[slot] = newRec
	p.MarkDirty()

	return nil
}

// ParseFromBytes 实现IMVCCPage接口
func (p *MVCCIndexPage) ParseFromBytes(data []byte) error {
	if len(data) < 16 { // version(8) + txID(8)
		return nil // 数据不足，使用默认值
	}

	p.Lock()
	defer p.Unlock()

	p.version = binary.BigEndian.Uint64(data[0:8])
	p.txID = binary.BigEndian.Uint64(data[8:16])

	// 解析rollback pointer
	if len(data) > 16 {
		rollPtrSize := binary.BigEndian.Uint16(data[16:18])
		if rollPtrSize > 0 && len(data) >= 18+int(rollPtrSize) {
			p.rollPtr = make([]byte, rollPtrSize)
			copy(p.rollPtr, data[18:18+rollPtrSize])
		}
	}

	return nil
}

// ToBytes 实现IMVCCPage接口
func (p *MVCCIndexPage) ToBytes() ([]byte, error) {
	p.RLock()
	defer p.RUnlock()

	size := 16 // version(8) + txID(8)
	rollPtrSize := 0
	if p.rollPtr != nil {
		rollPtrSize = len(p.rollPtr)
		size += 2 + rollPtrSize // 2 bytes for length
	}

	data := make([]byte, size)
	binary.BigEndian.PutUint64(data[0:8], p.version)
	binary.BigEndian.PutUint64(data[8:16], p.txID)

	if rollPtrSize > 0 {
		binary.BigEndian.PutUint16(data[16:18], uint16(rollPtrSize))
		copy(data[18:], p.rollPtr)
	}

	return data, nil
}
