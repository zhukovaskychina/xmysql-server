package mvcc

import (
	"io"
	"sync"
	"time"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
)

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

	// 记录管理
	records   map[uint16]*RecordVersion
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
		records:   make(map[uint16]*RecordVersion),
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

// Release 实现Page接口
func (p *MVCCIndexPage) Release() error {
	p.Lock()
	defer p.Unlock()

	// 清理资源
	p.records = nil
	p.freeSlots = nil
	p.locks = nil

	return nil
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

// CreateSnapshot 实现MVCCPage接口
func (p *MVCCIndexPage) CreateSnapshot() (*PageSnapshot, error) {
	p.RLock()
	defer p.RUnlock()

	snap := &PageSnapshot{
		PageID:   p.id,
		Version:  p.version,
		TxID:     p.txID,
		CreateTS: time.Now(),
		Records:  make(map[uint16]*RecordVersion),
	}

	// 复制记录版本
	for slot, rec := range p.records {
		snap.Records[slot] = &RecordVersion{
			Version:  rec.Version,
			TxID:     rec.TxID,
			CreateTS: rec.CreateTS,
			Key:      rec.Key,
			Value:    rec.Value,
			Next:     rec.Next,
		}
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
	p.records = make(map[uint16]*RecordVersion)
	for slot, rec := range snap.Records {
		p.records[slot] = &RecordVersion{
			Version:  rec.Version,
			TxID:     rec.TxID,
			CreateTS: rec.CreateTS,
			Key:      rec.Key,
			Value:    rec.Value,
			Next:     rec.Next,
		}
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
	rec := &RecordVersion{
		Version:  p.version,
		TxID:     p.txID,
		CreateTS: time.Now(),
		Key:      key,
		Value:    value,
	}

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
	newRec := &RecordVersion{
		Version:  p.version,
		TxID:     p.txID,
		CreateTS: time.Now(),
		Key:      rec.Key,
		Value:    value,
		Next:     rec,
	}

	p.records[slot] = newRec
	p.MarkDirty()

	return nil
}
