/*
IBD文件（.ibd）是InnoDB存储引擎中的独立表空间文件，用于存储表的数据和索引。

文件组成：
1. 表空间头页（FSP_HDR）- 页0
   - 存储表空间的元信息
   - 管理空间分配

2. 区描述页（XDES）- 页1
   - 管理extent的分配信息
   - 跟踪extent的使用状态

3. 段管理页（INODE）
   - 管理segment的inode信息
   - 维护segment的extent链表

4. 数据页（INDEX）
   - B+Tree结构的数据页
   - 包含聚簇索引和二级索引

5. 其他特殊页面
   - UNDO页：事务回滚
   - BLOB页：大字段存储
   - 插入缓冲页：优化插入操作

物理结构：
.ibd文件 -> Segment(段) -> Extent(区,1MB) -> Page(页,16KB)

IBD_File (最底层)
负责直接的文件 I/O 操作
处理页面的读写
管理文件的物理结构
不关心页面的分配状态
*/

/*
IBD_File represents the lowest layer of the storage system, responsible for:
1. Direct file I/O operations
2. Page reading and writing
3. Physical file management
4. Does not care about page allocation state

Physical Structure:
.ibd file -> Pages (16KB each)

Page Types:
0: FSP_HDR - File Space Header
1: IBUF_BITMAP - Insert Buffer Bitmap
2: INODE - Index Node
3: DATA - B+tree Data
4: UNDO - Undo Log
5: BLOB - Binary Large Object
*/

package ibd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Page size constants
const (
	PageSize = 16 * 1024 // 16KB per page

	// File header constants
	HeaderSize  = 38 // Space header size
	TrailerSize = 8  // Page trailer size
)

// IBD_File represents a physical InnoDB tablespace file
type IBD_File struct {
	sync.RWMutex
	// File information
	filePath string   // Full path to the .ibd file
	file     *os.File // File handle
	// Metadata
	spaceID uint32 // Tablespace ID
	name    string // Tablespace name
}

// NewIBDFile creates a new IBD file instance
func NewIBDFile(dataDir string, name string, spaceID uint32) *IBD_File {
	return &IBD_File{
		filePath: filepath.Join(dataDir, name+".ibd"),
		spaceID:  spaceID,
		name:     name,
	}
}

// Open opens an existing IBD file
func (f *IBD_File) Open() error {
	f.Lock()
	defer f.Unlock()

	if f.file != nil {
		return fmt.Errorf("file already open: %s", f.filePath)
	}

	file, err := os.OpenFile(f.filePath, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}

	f.file = file
	return nil
}

// Create creates and initializes a new IBD file
func (f *IBD_File) Create() error {
	f.Lock()
	defer f.Unlock()

	if f.file != nil {
		return fmt.Errorf("file already open: %s", f.filePath)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(f.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Create new file
	file, err := os.OpenFile(f.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	f.file = file

	// Initialize FSP header page (page 0)
	header := make([]byte, PageSize)
	// Write space ID and other metadata
	if err := f.writePageUnsafe(0, header); err != nil {
		f.file.Close()
		f.file = nil
		return fmt.Errorf("failed to write FSP header: %v", err)
	}

	// Initialize IBUF bitmap page (page 1)
	bitmap := make([]byte, PageSize)
	if err := f.writePageUnsafe(1, bitmap); err != nil {
		f.file.Close()
		f.file = nil
		return fmt.Errorf("failed to write IBUF bitmap: %v", err)
	}

	return nil
}

// writePageUnsafe writes a page to disk without acquiring locks (internal use only)
func (f *IBD_File) writePageUnsafe(pageNo uint32, page []byte) error {
	if f.file == nil {
		return fmt.Errorf("file not open")
	}

	if len(page) != PageSize {
		return fmt.Errorf("invalid page size: %d", len(page))
	}

	// Calculate page offset
	offset := int64(pageNo) * int64(PageSize)

	// Write page data
	n, err := f.file.WriteAt(page, offset)
	if err != nil {
		return fmt.Errorf("failed to write page: %v", err)
	}
	if n != PageSize {
		return fmt.Errorf("incomplete page write: %d bytes", n)
	}

	return nil
}

// ReadPage reads a page from disk
func (f *IBD_File) ReadPage(pageNo uint32) ([]byte, error) {
	f.RLock()
	defer f.RUnlock()

	if f.file == nil {
		return nil, fmt.Errorf("file not open")
	}

	// Allocate page buffer
	page := make([]byte, PageSize)

	// Calculate page offset
	offset := int64(pageNo) * int64(PageSize)

	// Read page data
	n, err := f.file.ReadAt(page, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %v", err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("incomplete page read: %d bytes", n)
	}

	return page, nil
}

// WritePage writes a page to disk
func (f *IBD_File) WritePage(pageNo uint32, page []byte) error {
	f.Lock()
	defer f.Unlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}

	if len(page) != PageSize {
		return fmt.Errorf("invalid page size: %d", len(page))
	}

	// Calculate page offset
	offset := int64(pageNo) * int64(PageSize)

	// Write page data
	n, err := f.file.WriteAt(page, offset)
	if err != nil {
		return fmt.Errorf("failed to write page: %v", err)
	}
	if n != PageSize {
		return fmt.Errorf("incomplete page write: %d bytes", n)
	}

	return nil
}

// Sync flushes file buffers to disk
func (f *IBD_File) Sync() error {
	f.RLock()
	defer f.RUnlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}

	return f.file.Sync()
}

// GetSpaceId returns the tablespace ID
func (f *IBD_File) GetSpaceId() uint32 {
	return f.spaceID
}

// GetTableName returns the tablespace name
func (f *IBD_File) GetTableName() string {
	return f.name
}

// GetFilePath returns the file path
func (f *IBD_File) GetFilePath() string {
	return f.filePath
}

// Delete removes the IBD file from disk
func (f *IBD_File) Delete() error {
	f.Lock()
	defer f.Unlock()

	// Close file if open
	if f.file != nil {
		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
	}

	// Delete file
	if err := os.Remove(f.filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %v", err)
	}

	return nil
}

// Close closes the IBD file
func (f *IBD_File) Close() error {
	f.Lock()
	defer f.Unlock()

	if f.file != nil {
		// Sync file before closing
		if err := f.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %v", err)
		}

		// Close file
		if err := f.file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}

		// Clear file handle
		f.file = nil
	}

	return nil
}

// Exists checks if the IBD file exists on disk
func (f *IBD_File) Exists() bool {
	_, err := os.Stat(f.filePath)
	return err == nil
}

// Size returns the current size of the IBD file in bytes
func (f *IBD_File) Size() (int64, error) {
	f.RLock()
	defer f.RUnlock()

	if f.file == nil {
		return 0, fmt.Errorf("file not open")
	}

	info, err := f.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %v", err)
	}

	return info.Size(), nil
}

// LoadPageByPageNumber reads a page from disk by its page number
// This is an alias for ReadPage to maintain compatibility
func (f *IBD_File) LoadPageByPageNumber(no uint32) ([]byte, error) {
	return f.ReadPage(no)
}
