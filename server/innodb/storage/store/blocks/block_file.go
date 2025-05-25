package blocks

import (
	"os"
	"path"
	"sync"
	"xmysql-server/server/innodb/basic"
)

// BlockFile represents a file that can be read and written in blocks/pages
type BlockFile struct {
	mu       sync.RWMutex
	file     *os.File
	filePath string
	size     int64
}

var _ basic.BlockFile = (*BlockFile)(nil)

// NewBlockFile creates a new block file
func NewBlockFile(dirPath string, fileName string, initSize int64) *BlockFile {
	filePath := path.Join(dirPath, fileName)
	return &BlockFile{
		filePath: filePath,
		size:     initSize,
	}
}

// Open opens the block file
func (bf *BlockFile) Open() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	file, err := os.OpenFile(bf.filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	bf.file = file

	// Initialize file size if needed
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() < bf.size {
		err = file.Truncate(bf.size)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close closes the block file
func (bf *BlockFile) Close() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.file != nil {
		err := bf.file.Close()
		bf.file = nil
		return err
	}
	return nil
}

// ReadPage reads a page from the file
func (bf *BlockFile) ReadPage(pageNo uint32) ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.file == nil {
		if err := bf.Open(); err != nil {
			return nil, err
		}
	}

	offset := int64(pageNo) * 16384 // 16KB pages
	buf := make([]byte, 16384)

	n, err := bf.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	return buf[:n], nil
}

// WritePage writes a page to the file
func (bf *BlockFile) WritePage(pageNo uint32, content []byte) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.file == nil {
		if err := bf.Open(); err != nil {
			return err
		}
	}

	offset := int64(pageNo) * 16384 // 16KB pages
	_, err := bf.file.WriteAt(content, offset)
	return err
}

// Sync syncs the file to disk
func (bf *BlockFile) Sync() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.file != nil {
		return bf.file.Sync()
	}
	return nil
}
