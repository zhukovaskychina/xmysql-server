package page

import (
	"encoding/binary"
	"errors"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"sync"
)

var (
	ErrInvalidDictHeader = errors.New("invalid data dictionary header")
	ErrInvalidTableDef   = errors.New("invalid table definition")
	ErrInvalidIndexDef   = errors.New("invalid index definition")
)

// DataDictionaryPageWrapper 数据字典页面包装器
type DataDictionaryPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 并发控制
	mu sync.RWMutex

	// 底层的数据字典页面实现
	dictPage *pages.DataDictionaryHeaderSysPage

	// 数据字典信息
	maxTableID uint64
	maxIndexID uint64
	maxSpaceID uint32
	tables     []TableDef
	indexes    []IndexDef
}

const (
	DICT_HEADER_SIZE = 24 // 8(maxTableID) + 8(maxIndexID) + 4(maxSpaceID) + 4(reserved)
)

// NewDataDictionaryPageWrapper 创建数据字典页面
func NewDataDictionaryPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *DataDictionaryPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_SYS)
	dictPage := pages.NewDataDictHeaderPage()

	return &DataDictionaryPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		dictPage:        dictPage,
		tables:          make([]TableDef, 0),
		indexes:         make([]IndexDef, 0),
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析数据字典页面
func (dw *DataDictionaryPageWrapper) ParseFromBytes(data []byte) error {
	//dw.Lock()
	//defer dw.Unlock()

	if err := dw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析数据字典页面特有的数据
	parsedPage := pages.ParseDataDictHrdPage(data)
	dw.dictPage = parsedPage

	return dw.parseDataDictContent(data)
}

// ToBytes 序列化数据字典页面为字节数组
func (dw *DataDictionaryPageWrapper) ToBytes() ([]byte, error) {
	dw.RLock()
	defer dw.RUnlock()

	// 序列化数据字典页面
	data := dw.dictPage.GetSerializeBytes()

	// 更新基础包装器的内容
	if len(dw.content) != len(data) {
		dw.content = make([]byte, len(data))
	}
	copy(dw.content, data)

	return data, nil
}

// parseDataDictContent 解析数据字典内容
func (dw *DataDictionaryPageWrapper) parseDataDictContent(content []byte) error {
	if len(content) < pages.FileHeaderSize+DICT_HEADER_SIZE {
		return ErrInvalidDictHeader
	}

	// 解析数据字典头
	offset := pages.FileHeaderSize
	dw.maxTableID = binary.LittleEndian.Uint64(content[offset:])
	dw.maxIndexID = binary.LittleEndian.Uint64(content[offset+8:])
	dw.maxSpaceID = binary.LittleEndian.Uint32(content[offset+16:])

	// 解析表定义（简化版本）
	pos := offset + DICT_HEADER_SIZE
	if pos+4 <= len(content) {
		numTables := binary.LittleEndian.Uint32(content[pos:])
		pos += 4

		dw.tables = make([]TableDef, 0, numTables)
		// 这里可以根据需要添加更详细的解析逻辑
	}

	return nil
}

// Read 实现PageWrapper接口
func (dw *DataDictionaryPageWrapper) Read() error {
	// 1. 尝试从buffer pool读取
	if dw.bufferPool != nil {
		if page, err := dw.bufferPool.GetPage(dw.GetSpaceID(), dw.GetPageID()); err == nil {
			if page != nil {
				dw.content = page.GetContent()
				return dw.ParseFromBytes(dw.content)
			}
		}
	}

	// 2. 从磁盘读取
	content, err := dw.readFromDisk()
	if err != nil {
		return err
	}

	// 3. 加入buffer pool
	if dw.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(dw.GetSpaceID(), dw.GetPageID())
		bufferPage.SetContent(content)
		dw.bufferPool.PutPage(bufferPage)
	}

	// 4. 解析内容
	dw.content = content
	return dw.ParseFromBytes(content)
}

// Write 实现PageWrapper接口
func (dw *DataDictionaryPageWrapper) Write() error {
	// 1. 序列化页面内容
	content, err := dw.ToBytes()
	if err != nil {
		return err
	}

	// 2. 写入buffer pool
	if dw.bufferPool != nil {
		if page, err := dw.bufferPool.GetPage(dw.GetSpaceID(), dw.GetPageID()); err == nil {
			if page != nil {
				page.SetContent(content)
				page.MarkDirty()
			}
		}
	}

	// 3. 写入磁盘
	return dw.writeToDisk(content)
}

// GetTable 获取表定义
func (dw *DataDictionaryPageWrapper) GetTable(id uint64) *TableDef {
	dw.mu.RLock()
	defer dw.mu.RUnlock()

	for i := range dw.tables {
		if dw.tables[i].ID == id {
			return &dw.tables[i]
		}
	}
	return nil
}

// GetIndex 获取索引定义
func (dw *DataDictionaryPageWrapper) GetIndex(id uint64) *IndexDef {
	dw.mu.RLock()
	defer dw.mu.RUnlock()

	for i := range dw.indexes {
		if dw.indexes[i].ID == id {
			return &dw.indexes[i]
		}
	}
	return nil
}

// AddTable 添加表定义
func (dw *DataDictionaryPageWrapper) AddTable(table TableDef) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if table.ID > dw.maxTableID {
		dw.maxTableID = table.ID
	}

	dw.tables = append(dw.tables, table)
	dw.MarkDirty()
	return nil
}

// AddIndex 添加索引定义
func (dw *DataDictionaryPageWrapper) AddIndex(index IndexDef) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if index.ID > dw.maxIndexID {
		dw.maxIndexID = index.ID
	}

	dw.indexes = append(dw.indexes, index)
	dw.MarkDirty()
	return nil
}

// GetMaxTableID 获取最大表ID
func (dw *DataDictionaryPageWrapper) GetMaxTableID() uint64 {
	dw.mu.RLock()
	defer dw.mu.RUnlock()
	return dw.maxTableID
}

// GetMaxIndexID 获取最大索引ID
func (dw *DataDictionaryPageWrapper) GetMaxIndexID() uint64 {
	dw.mu.RLock()
	defer dw.mu.RUnlock()
	return dw.maxIndexID
}

// GetMaxSpaceID 获取最大空间ID
func (dw *DataDictionaryPageWrapper) GetMaxSpaceID() uint32 {
	dw.mu.RLock()
	defer dw.mu.RUnlock()
	return dw.maxSpaceID
}

// GetTables 获取所有表定义
func (dw *DataDictionaryPageWrapper) GetTables() []TableDef {
	dw.mu.RLock()
	defer dw.mu.RUnlock()

	result := make([]TableDef, len(dw.tables))
	copy(result, dw.tables)
	return result
}

// GetIndexes 获取所有索引定义
func (dw *DataDictionaryPageWrapper) GetIndexes() []IndexDef {
	dw.mu.RLock()
	defer dw.mu.RUnlock()

	result := make([]IndexDef, len(dw.indexes))
	copy(result, dw.indexes)
	return result
}

// Validate 验证数据字典页面数据完整性
func (dw *DataDictionaryPageWrapper) Validate() error {
	dw.RLock()
	defer dw.RUnlock()

	if dw.dictPage != nil {
		// 验证基本数据
		if dw.dictPage.GetMaxTableId() == 0 && len(dw.tables) > 0 {
			return errors.New("inconsistent table data")
		}
	}

	return nil
}

// GetDataDictPage 获取底层的数据字典页面实现
func (dw *DataDictionaryPageWrapper) GetDataDictPage() *pages.DataDictionaryHeaderSysPage {
	return dw.dictPage
}

// 内部方法：从磁盘读取
func (dw *DataDictionaryPageWrapper) readFromDisk() ([]byte, error) {
	// TODO: 实现从磁盘读取页面的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return make([]byte, common.PageSize), nil
}

// 内部方法：写入磁盘
func (dw *DataDictionaryPageWrapper) writeToDisk(content []byte) error {
	// TODO: 实现写入磁盘的逻辑
	// 这里需要根据实际的磁盘访问层来实现
	return nil
}
