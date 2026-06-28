package system

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper"
)

var (
	ErrInvalidSystemPage = errors.New("invalid system page")
	ErrCorruptedPage     = errors.New("corrupted system page")
)

// SystemPageHeader 绯荤粺椤甸潰澶撮儴
type SystemPageHeader struct {
	Version      uint32
	Checksum     uint64
	SystemType   SystemPageType
	SystemState  SystemPageState
	LastModified int64
}

// BaseSystemPage 绯荤粺椤甸潰鍩虹被
type BaseSystemPage struct {
	*wrapper.BasePage
	header SystemPageHeader
	stats  SystemPageStats
}

// NewBaseSystemPage 鍒涘缓绯荤粺椤甸潰
func NewBaseSystemPage(spaceID, pageNo uint32, sysType SystemPageType) *BaseSystemPage {
	sp := &BaseSystemPage{
		BasePage: wrapper.NewBasePage(spaceID, pageNo, common.FIL_PAGE_TYPE_SYS),
	}

	// 鍒濆鍖栭〉闈㈠ご
	sp.header = SystemPageHeader{
		Version:      1,
		SystemType:   sysType,
		SystemState:  SystemPageStateNormal,
		LastModified: time.Now().UnixNano(),
	}

	return sp
}

// GetSystemType 鑾峰彇绯荤粺椤甸潰绫诲瀷
func (sp *BaseSystemPage) GetSystemType() SystemPageType {
	return sp.header.SystemType
}

// GetSystemState 鑾峰彇绯荤粺椤甸潰鐘舵€?
func (sp *BaseSystemPage) GetSystemState() SystemPageState {
	return sp.header.SystemState
}

// SetSystemState 璁剧疆绯荤粺椤甸潰鐘舵€?
func (sp *BaseSystemPage) SetSystemState(state SystemPageState) {
	sp.Lock()
	defer sp.Unlock()

	sp.header.SystemState = state
	sp.header.LastModified = time.Now().UnixNano()
	sp.MarkDirty()
}

// GetSystemStats 鑾峰彇绯荤粺椤甸潰缁熻淇℃伅
func (sp *BaseSystemPage) GetSystemStats() *SystemPageStats {
	return &sp.stats
}

// Recover 鎭㈠椤甸潰
func (sp *BaseSystemPage) Recover() error {
	sp.Lock()
	defer sp.Unlock()

	// 楠岃瘉椤甸潰
	if err := sp.Validate(); err != nil {
		return err
	}

	// 璁剧疆鎭㈠鐘舵€?
	sp.header.SystemState = SystemPageStateRecovering
	atomic.AddUint32(&sp.stats.Recoveries, 1)
	sp.stats.LastRecovered = time.Now().UnixNano()

	// 鏍囪椤甸潰涓鸿剰
	sp.MarkDirty()

	return nil
}

// Validate 楠岃瘉椤甸潰
func (sp *BaseSystemPage) Validate() error {
	sp.RLock()
	defer sp.RUnlock()

	// 楠岃瘉椤甸潰绫诲瀷
	if sp.GetPageType() != common.FIL_PAGE_TYPE_SYS {
		return ErrInvalidSystemPage
	}

	// 楠岃瘉鏍￠獙鍜?
	if !sp.validateChecksum() {
		atomic.AddUint32(&sp.stats.Corruptions, 1)
		return ErrCorruptedPage
	}

	return nil
}

func (sp *BaseSystemPage) SetContent(content []byte) {
	sp.normalizeSystemContent(content)
	sp.BasePage.SetContent(content)
}

// Backup 澶囦唤椤甸潰
func (sp *BaseSystemPage) Backup() error {
	sp.RLock()
	defer sp.RUnlock()

	// TODO: 瀹炵幇椤甸潰澶囦唤
	return nil
}

// Restore 鎭㈠椤甸潰
func (sp *BaseSystemPage) Restore() error {
	sp.Lock()
	defer sp.Unlock()

	// TODO: 瀹炵幇椤甸潰鎭㈠
	return nil
}

// validateChecksum 楠岃瘉鏍￠獙鍜?
// 浣跨敤CRC32绠楁硶楠岃瘉椤甸潰瀹屾暣鎬?
func (sp *BaseSystemPage) validateChecksum() bool {
	// 鑾峰彇椤甸潰鏁版嵁锛堜娇鐢℅etContent鏂规硶锛?
	content := sp.GetContent()
	if len(content) < pages.FileHeaderSize+8 {
		return false
	}

	// 浣跨敤PageIntegrityChecker楠岃瘉鏍￠獙鍜?
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	err := checker.ValidateChecksum(content)

	return err == nil
}

// updateChecksum 鏇存柊鏍￠獙鍜?
// 璁＄畻骞舵洿鏂伴〉闈㈢殑CRC32鏍￠獙鍜?
func (sp *BaseSystemPage) updateChecksum() {
	// 鑾峰彇椤甸潰鏁版嵁锛堜娇鐢℅etContent鏂规硶锛?
	content := sp.GetContent()
	if len(content) < pages.FileHeaderSize+8 {
		sp.header.Checksum = 0
		return
	}

	sp.normalizeSystemContent(content)

	// 浣跨敤PageIntegrityChecker璁＄畻鏍￠獙鍜?
	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	checksum32 := checker.CalculateChecksum(content)

	// 鏇存柊header涓殑鏍￠獙鍜?
	sp.header.Checksum = uint64(checksum32)

	// 鏇存柊椤甸潰鏁版嵁涓殑鏍￠獙鍜屽瓧娈碉紙鍓?瀛楄妭锛?
	binary.LittleEndian.PutUint32(content[0:4], checksum32)

	// 鏇存柊椤甸潰鏁版嵁涓殑trailer鏍￠獙鍜岋紙鏈€鍚?瀛楄妭鐨勫墠4瀛楄妭锛?
	trailerOffset := len(content) - 8
	binary.LittleEndian.PutUint32(content[trailerOffset:trailerOffset+4], checksum32)

	// 鏇存柊鍥為〉闈?
	sp.SetContent(content)
}

func (sp *BaseSystemPage) normalizeSystemContent(content []byte) {
	if len(content) < pages.FileHeaderSize {
		return
	}

	binary.BigEndian.PutUint32(content[4:8], sp.GetPageNo())
	binary.BigEndian.PutUint16(content[24:26], uint16(common.FIL_PAGE_TYPE_SYS))
	binary.BigEndian.PutUint32(content[34:38], sp.GetSpaceID())
}

// Read 瀹炵幇Page鎺ュ彛
func (sp *BaseSystemPage) Read() error {
	if err := sp.BasePage.Read(); err != nil {
		return err
	}

	// 璇诲彇绯荤粺椤甸潰澶?
	content := sp.GetContent()
	sp.header.Version = binary.LittleEndian.Uint32(content[64:])
	sp.header.Checksum = binary.LittleEndian.Uint64(content[68:])
	sp.header.SystemType = SystemPageType(binary.LittleEndian.Uint16(content[76:]))
	sp.header.SystemState = SystemPageState(content[78])
	sp.header.LastModified = int64(binary.LittleEndian.Uint64(content[79:]))

	// 鏇存柊缁熻淇℃伅
	atomic.AddUint64(&sp.stats.Reads, 1)
	sp.stats.LastModified = time.Now().UnixNano()

	return nil
}

// Write 瀹炵幇Page鎺ュ彛
func (sp *BaseSystemPage) Write() error {
	// 鏇存柊鏍￠獙鍜?
	sp.updateChecksum()

	// 鍐欏叆绯荤粺椤甸潰澶?
	content := sp.GetContent()
	binary.LittleEndian.PutUint32(content[64:], sp.header.Version)
	binary.LittleEndian.PutUint64(content[68:], sp.header.Checksum)
	binary.LittleEndian.PutUint16(content[76:], uint16(sp.header.SystemType))
	content[78] = byte(sp.header.SystemState)
	binary.LittleEndian.PutUint64(content[79:], uint64(sp.header.LastModified))
	sp.SetContent(content)

	// 鏇存柊缁熻淇℃伅
	atomic.AddUint64(&sp.stats.Writes, 1)
	sp.stats.LastModified = time.Now().UnixNano()

	return sp.BasePage.Write()
}
