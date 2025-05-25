package page

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/storage/wrapper/types"

	"xmysql-server/server/innodb/basic"
)

const (
	// FileHeaderSize is the size of file header in bytes
	FileHeaderSize = 38

	// PageHeaderSize is the size of page header in bytes
	PageHeaderSize = 56

	// FileTrailerSize is the size of file trailer in bytes
	FileTrailerSize = 8
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrDuplicateKey   = errors.New("duplicate key")
	ErrInvalidKeyType = errors.New("invalid key type")
)

// IndexPage represents an index page in InnoDB
type IndexPage struct {
	sync.RWMutex

	// Page number and space ID
	pageNo  uint32
	spaceId uint32

	// Headers
	fileHeader      FileHeader
	pageHeader      PageHeader
	indexPageHeader *types.IndexPageHeader

	// Page components
	infimumSupermum []byte
	userRecords     []byte
	pageDirectory   []byte
	freeSpace       []byte

	// Entries and key space
	entries  []*types.IndexEntry
	keySpace map[string]uint16 // key bytes -> offset

	// Stats and state
	stats   *basic.PageStats
	content []byte
	lsn     uint64
	dirty   bool
	state   basic.PageState
}

// Lock implements sync.Locker
func (ip *IndexPage) Lock() {
	ip.RWMutex.Lock()
}

// Unlock implements sync.Locker
func (ip *IndexPage) Unlock() {
	ip.RWMutex.Unlock()
}

// RLock implements RLocker
func (ip *IndexPage) RLock() {
	ip.RWMutex.RLock()
}

// RUnlock implements RLocker
func (ip *IndexPage) RUnlock() {
	ip.RWMutex.RUnlock()
}

// NewIndexPage creates a new index page
func NewIndexPage(pageNo uint32, spaceId uint32) *IndexPage {
	ip := &IndexPage{
		pageNo:  pageNo,
		spaceId: spaceId,
		indexPageHeader: &types.IndexPageHeader{
			Level: 0,
		},
		keySpace: make(map[string]uint16),
		stats:    &basic.PageStats{},
		state:    basic.PageStateActive,
	}

	// Initialize headers
	ip.fileHeader = *NewFileHeader()
	ip.pageHeader = *NewPageHeader()
	ip.infimumSupermum = make([]byte, 0)
	ip.userRecords = make([]byte, 0)
	ip.pageDirectory = make([]byte, 0)
	ip.freeSpace = make([]byte, 0)

	// Initialize page content
	ip.infimumSupermum = make([]byte, 26) // 13 bytes each for infimum and supremum
	ip.userRecords = make([]byte, 0)
	ip.pageDirectory = make([]byte, 0)
	ip.freeSpace = make([]byte, 0)

	return ip
}

// Ensure IndexPage implements types.IIndexPage
var _ types.IIndexPage = (*IndexPage)(nil)

// GetFileHeader returns the file header bytes
func (p *IndexPage) GetFileHeader() []byte {
	return p.fileHeader.GetBytes()
}

// GetFileTrailer returns the file trailer bytes
func (p *IndexPage) GetFileTrailer() []byte {
	return make([]byte, 8) // Fixed size file trailer
}

// GetIndexID returns the index ID
func (p *IndexPage) GetIndexID() uint64 {
	return p.indexPageHeader.IndexID
}

// SetIndexID sets the index ID
func (p *IndexPage) SetIndexID(indexID uint64) {
	p.indexPageHeader.IndexID = indexID
}

// GetKeyCount returns the key count
func (p *IndexPage) GetKeyCount() uint16 {
	return p.indexPageHeader.KeyCount
}

// SetKeyCount sets the key count
func (p *IndexPage) SetKeyCount(keyCount uint16) {
	p.indexPageHeader.KeyCount = keyCount
}

// GetPageHeader returns the page header
func (p *IndexPage) GetPageHeader() *PageHeader {
	return &p.pageHeader
}

// GetIndexPageHeader returns the index page header
func (p *IndexPage) GetIndexPageHeader() *types.IndexPageHeader {
	return p.indexPageHeader
}

// GetSpaceID returns the space ID
func (p *IndexPage) GetSpaceID() uint32 {
	return p.spaceId
}

// GetPageNo returns the page number
func (p *IndexPage) GetPageNo() uint32 {
	return p.pageNo
}

// GetPageType implements types.IPageWrapper
func (p *IndexPage) GetPageType() uint16 {
	return uint16(common.FIL_PAGE_INDEX)
}

// GetPageLSN implements types.IPageWrapper
func (p *IndexPage) GetPageLSN() uint64 {
	return p.lsn
}

// GetLSN implements types.IPageWrapper
func (ip *IndexPage) GetLSN() uint64 {
	return ip.lsn
}

// SetLSN implements types.IPageWrapper
func (ip *IndexPage) SetLSN(lsn uint64) {
	ip.lsn = lsn
}

// IsDirty implements types.IPageWrapper
func (ip *IndexPage) IsDirty() bool {
	return ip.dirty
}

// MarkDirty implements types.IPageWrapper
func (ip *IndexPage) MarkDirty() {
	ip.dirty = true
	ip.stats.DirtyCount++
}

// GetState implements types.IPageWrapper
func (ip *IndexPage) GetState() basic.PageState {
	return ip.state
}

// SetState implements types.IPageWrapper
func (ip *IndexPage) SetState(state basic.PageState) {
	ip.state = state
}

// GetStats implements types.IPageWrapper
func (p *IndexPage) GetStats() *basic.PageStats {
	return p.stats
}

// Pin implements types.IPageWrapper
func (p *IndexPage) Pin() {
	p.stats.PinCount++
}

// Unpin implements types.IPageWrapper
func (p *IndexPage) Unpin() {
	if p.stats.PinCount > 0 {
		p.stats.PinCount--
	}
}

// Read implements types.IPageWrapper
func (p *IndexPage) Read() error {
	p.Lock()
	defer p.Unlock()

	// Read file header
	if err := p.readFileHeader(); err != nil {
		return err
	}

	// Read page header
	if err := p.readPageHeader(); err != nil {
		return err
	}

	// Read index entries
	if err := p.readIndexEntries(); err != nil {
		return err
	}

	// Update stats
	p.stats.ReadCount++
	p.stats.LastAccessAt = uint64(time.Now().UnixNano())

	return nil
}

// Write implements types.IPageWrapper
func (p *IndexPage) Write() error {
	// Write page header
	binary.LittleEndian.PutUint16(p.content[38:], p.indexPageHeader.KeyCount)
	binary.LittleEndian.PutUint16(p.content[40:], p.indexPageHeader.Level)
	binary.LittleEndian.PutUint64(p.content[42:], p.indexPageHeader.IndexID)
	binary.LittleEndian.PutUint32(p.content[50:], p.indexPageHeader.LeftPage)
	binary.LittleEndian.PutUint32(p.content[54:], p.indexPageHeader.RightPage)
	binary.LittleEndian.PutUint32(p.content[58:], p.indexPageHeader.ParentPage)
	p.content[62] = boolToByte(p.indexPageHeader.IsLeaf)
	p.content[63] = boolToByte(p.indexPageHeader.IsRoot)

	// Update stats
	p.stats.IncWriteCount()
	p.stats.LastModified = basic.GetCurrentTimestamp()

	return nil
}

// GetSegLeaf implements interfaces.IIndexWrapper
func (ip *IndexPage) GetSegLeaf() []byte {
	return ip.pageHeader.PageBtrSegLeaf
}

// SetSegLeaf implements interfaces.IIndexWrapper
func (ip *IndexPage) SetSegLeaf(segLeaf []byte) {
	ip.pageHeader.PageBtrSegLeaf = segLeaf
}

// GetSegInt implements interfaces.IIndexWrapper
func (ip *IndexPage) GetSegInt() []byte {
	return ip.pageHeader.PageBtrSegLeaf
}

// SetSegInt implements interfaces.IIndexWrapper
func (ip *IndexPage) SetSegInt(segInt []byte) {
	ip.pageHeader.PageBtrSegLeaf = segInt
}

// GetSegTop implements interfaces.IIndexWrapper
func (ip *IndexPage) GetSegTop() []byte {
	return ip.pageHeader.PageBtrSegTop
}

// InsertKey 插入键值
func (ip *IndexPage) InsertKey(key types.Key, pageNo uint32, rowID uint64) error {
	ip.Lock()
	defer ip.Unlock()

	// Check if key already exists
	if _, err := ip.FindKey(key); err == nil {
		return ErrDuplicateKey
	} else if err != ErrKeyNotFound {
		return err
	}

	// Create new entry
	entry := &types.IndexEntry{
		Key:    key,
		PageNo: pageNo,
		RowID:  rowID,
	}

	// Find insertion position
	pos := sort.Search(len(ip.entries), func(i int) bool {
		return ip.entries[i].Key.Compare(key) >= 0
	})

	// Insert entry
	ip.entries = append(ip.entries[:pos], append([]*types.IndexEntry{entry}, ip.entries[pos:]...)...)

	// Update key space
	ip.keySpace[string(key.GetBytes())] = uint16(pos)

	// Update key count
	ip.indexPageHeader.KeyCount++

	// Mark page as dirty
	ip.MarkDirty()

	return nil
}

// DeleteKey deletes a key from the index page
func (ip *IndexPage) DeleteKey(key types.Key) error {
	// Find the key in the entries
	for i, entry := range ip.entries {
		if entry.Key.Compare(key) == 0 {
			// Remove the entry
			ip.entries = append(ip.entries[:i], ip.entries[i+1:]...)

			// Update key space
			delete(ip.keySpace, string(key.GetBytes()))

			// Mark as dirty
			ip.dirty = true

			return nil
		}
	}

	return fmt.Errorf("key not found")
}

// FindKey 查找键值
func (ip *IndexPage) FindKey(key types.Key) (*types.IndexEntry, error) {
	ip.RLock()
	defer ip.RUnlock()

	// 二分查找
	i := sort.Search(len(ip.entries), func(i int) bool {
		return ip.entries[i].Key.Compare(key) >= 0
	})

	if i < len(ip.entries) && ip.entries[i].Key.Compare(key) == 0 {
		return ip.entries[i], nil
	}

	return nil, ErrKeyNotFound
}

// GetKeys 获取所有键值
func (ip *IndexPage) GetKeys() []types.Key {
	ip.RLock()
	defer ip.RUnlock()

	keys := make([]types.Key, 0, len(ip.entries))
	for _, entry := range ip.entries {
		keys = append(keys, entry.Key)
	}
	return keys
}

// GetEntries 获取所有索引项
func (ip *IndexPage) GetEntries() []*types.IndexEntry {
	ip.RLock()
	defer ip.RUnlock()

	entries := make([]*types.IndexEntry, 0, len(ip.entries))
	for _, entry := range ip.entries {
		entries = append(entries, entry)
	}
	return entries
}

// SetParent 设置父页面
func (ip *IndexPage) SetParent(parentPage uint32) {
	ip.Lock()
	defer ip.Unlock()

	ip.indexPageHeader.ParentPage = parentPage
	ip.MarkDirty()
}

// SetLeftRight 设置左右兄弟页面
func (ip *IndexPage) SetLeftRight(leftPage, rightPage uint32) {
	ip.Lock()
	defer ip.Unlock()

	ip.indexPageHeader.LeftPage = leftPage
	ip.indexPageHeader.RightPage = rightPage
	ip.MarkDirty()
}

// IsLeaf 是否是叶子节点
func (ip *IndexPage) IsLeaf() bool {
	return ip.indexPageHeader.IsLeaf
}

// IsRoot 是否是根节点
func (ip *IndexPage) IsRoot() bool {
	return ip.indexPageHeader.IsRoot
}

// SetRoot 设置为根节点
func (ip *IndexPage) SetRoot(isRoot bool) {
	ip.Lock()
	defer ip.Unlock()

	ip.indexPageHeader.IsRoot = isRoot
	ip.MarkDirty()
}

// GetLevel returns the level of the index page
func (ip *IndexPage) GetLevel() uint16 {
	return ip.indexPageHeader.Level
}

// readFileHeader reads the file header from content
func (ip *IndexPage) readFileHeader() error {
	if len(ip.content) < FileHeaderSize {
		return fmt.Errorf("content too short for file header: %d", len(ip.content))
	}
	ip.fileHeader.ParseBytes(ip.content[:FileHeaderSize])
	return nil
}

// readPageHeader reads the page header from content
func (ip *IndexPage) readPageHeader() error {
	offset := FileHeaderSize
	if len(ip.content) < offset+PageHeaderSize {
		return fmt.Errorf("content too short for page header: %d", len(ip.content))
	}
	ip.pageHeader.ParseBytes(ip.content[offset : offset+PageHeaderSize])
	return nil
}

// readIndexEntries reads the index entries from content
func (ip *IndexPage) readIndexEntries() error {
	offset := FileHeaderSize + PageHeaderSize
	for offset < len(ip.content) {
		entry := &types.IndexEntry{}
		if err := entry.ParseBytes(ip.content[offset:]); err != nil {
			return err
		}
		ip.entries = append(ip.entries, entry)
		offset += entry.Size()
	}
	return nil
}

// boolToByte 布尔值转字节
func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
