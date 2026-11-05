package ibd

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	doublewriteMagic   = "XMDW0001"
	doublewriteSlots   = 32
	doublewritePathLen = 256
	doublewriteHeader  = 32
)

// Doublewrite protects a page write by fsyncing a full copy before the caller
// writes the real tablespace page. A torn page in the .ibd file can be
// replaced from this copy after a crash.
type Doublewrite struct {
	mu   sync.Mutex
	file *os.File
	next uint32
}

var (
	doublewriteMu sync.Mutex
	doublewrites  = map[string]*Doublewrite{}
)

func doublewriteSlotSize() int {
	return 4 + 4 + 4 + 4 + doublewritePathLen + PageSize + 4
}

// EnableDoublewrite opens datadir/xb_doublewrite and registers it for every
// tablespace file under that directory. A failure here means page writes are
// not crash-safe, so the caller must refuse to start.
func EnableDoublewrite(dataDir string) (*Doublewrite, error) {
	dataDir = filepath.Clean(dataDir)
	if dataDir == "" || dataDir == "." {
		return nil, fmt.Errorf("doublewrite requires a data directory")
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "xb_doublewrite")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	need := int64(doublewriteHeader + doublewriteSlots*doublewriteSlotSize())
	if info.Size() < need {
		if err := file.Truncate(need); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	header := make([]byte, doublewriteHeader)
	copy(header, doublewriteMagic)
	binary.LittleEndian.PutUint32(header[8:], doublewriteSlots)
	binary.LittleEndian.PutUint32(header[12:], uint32(PageSize))
	if _, err := file.WriteAt(header, 0); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	dw := &Doublewrite{file: file}
	doublewriteMu.Lock()
	previous := doublewrites[dataDir]
	doublewrites[dataDir] = dw
	doublewriteMu.Unlock()
	if previous != nil && previous != dw {
		_ = previous.Close()
	}
	return dw, nil
}

// ReleaseDoublewrite removes a registered buffer and closes it when it is still
// the buffer for that directory.
func ReleaseDoublewrite(dataDir string, dw *Doublewrite) {
	if dw == nil {
		return
	}
	dataDir = filepath.Clean(dataDir)
	doublewriteMu.Lock()
	current := doublewrites[dataDir]
	if current == dw {
		delete(doublewrites, dataDir)
	}
	doublewriteMu.Unlock()
	if current == dw {
		_ = dw.Close()
	}
}

// LookupDoublewrite finds the buffer registered for the tablespace file or one
// of its parent directories.
func LookupDoublewrite(filePath string) *Doublewrite {
	dir := filepath.Clean(filepath.Dir(filePath))
	doublewriteMu.Lock()
	defer doublewriteMu.Unlock()
	for i := 0; i < 4 && dir != "" && dir != string(filepath.Separator); i++ {
		if dw := doublewrites[dir]; dw != nil {
			return dw
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return nil
}

// Protect fsyncs a full page image before the tablespace write. The copy is
// the recovery source when the following .ibd write is torn.
func (d *Doublewrite) Protect(spaceID, pageNo uint32, filePath string, page []byte) error {
	if d == nil || d.file == nil {
		return fmt.Errorf("doublewrite buffer is not open")
	}
	if len(page) != PageSize {
		return fmt.Errorf("doublewrite page size %d", len(page))
	}
	pathBytes := []byte(filePath)
	if len(pathBytes) > doublewritePathLen {
		return fmt.Errorf("doublewrite path is longer than %d bytes", doublewritePathLen)
	}
	slot := make([]byte, doublewriteSlotSize())
	binary.LittleEndian.PutUint32(slot[0:], 1)
	binary.LittleEndian.PutUint32(slot[4:], spaceID)
	binary.LittleEndian.PutUint32(slot[8:], pageNo)
	binary.LittleEndian.PutUint32(slot[12:], uint32(len(pathBytes)))
	copy(slot[16:16+doublewritePathLen], pathBytes)
	pageAt := 16 + doublewritePathLen
	copy(slot[pageAt:pageAt+PageSize], page)
	binary.LittleEndian.PutUint32(slot[pageAt+PageSize:], crc32.ChecksumIEEE(page))

	d.mu.Lock()
	defer d.mu.Unlock()
	index := d.next % doublewriteSlots
	d.next++
	offset := int64(doublewriteHeader + int(index)*doublewriteSlotSize())
	if _, err := d.file.WriteAt(slot, offset); err != nil {
		return fmt.Errorf("write doublewrite slot: %w", err)
	}
	if err := d.file.Sync(); err != nil {
		return fmt.Errorf("fsync doublewrite slot: %w", err)
	}
	return nil
}

// Restore rewrites tablespace pages whose bytes do not match a checksum-valid
// doublewrite copy. The copy was fsynced before the real write, so it is the
// last complete image of that page.
func Restore(dataDir string) error {
	dw := LookupDoublewrite(dataDir)
	if dw == nil {
		opened, err := EnableDoublewrite(dataDir)
		if err != nil {
			return err
		}
		dw = opened
	}
	dw.mu.Lock()
	defer dw.mu.Unlock()
	slot := make([]byte, doublewriteSlotSize())
	for index := 0; index < doublewriteSlots; index++ {
		offset := int64(doublewriteHeader + index*doublewriteSlotSize())
		if _, err := dw.file.ReadAt(slot, offset); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(slot[0:]) != 1 {
			continue
		}
		pageNo := binary.LittleEndian.Uint32(slot[8:])
		pathLen := binary.LittleEndian.Uint32(slot[12:])
		if pathLen == 0 || pathLen > doublewritePathLen {
			continue
		}
		pageAt := 16 + doublewritePathLen
		page := make([]byte, PageSize)
		copy(page, slot[pageAt:pageAt+PageSize])
		if crc32.ChecksumIEEE(page) != binary.LittleEndian.Uint32(slot[pageAt+PageSize:]) {
			continue
		}
		target := string(slot[16 : 16+pathLen])
		target = strings.TrimRight(target, "\x00")
		if err := restorePageIfTorn(target, pageNo, page); err != nil {
			return err
		}
	}
	return nil
}

func restorePageIfTorn(path string, pageNo uint32, page []byte) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	current := make([]byte, PageSize)
	offset := int64(pageNo) * int64(PageSize)
	n, err := file.ReadAt(current, offset)
	if err != nil || n != PageSize || crc32.ChecksumIEEE(current) != crc32.ChecksumIEEE(page) {
		if _, err := file.WriteAt(page, offset); err != nil {
			return fmt.Errorf("restore page %s:%d: %w", path, pageNo, err)
		}
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the buffer file. Callers that still have the buffer registered
// must drop the registry entry themselves before closing.
func (d *Doublewrite) Close() error {
	if d == nil || d.file == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.file.Close()
	d.file = nil
	return err
}
