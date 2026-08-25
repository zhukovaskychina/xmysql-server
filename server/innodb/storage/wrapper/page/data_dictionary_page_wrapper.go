package page

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"sync"
)

var (
	ErrInvalidDictHeader = errors.New("invalid data dictionary header")
	ErrInvalidTableDef   = errors.New("invalid table definition")
	ErrInvalidIndexDef   = errors.New("invalid index definition")
	ErrTableNotFound     = errors.New("table not found")
	ErrIndexNotFound     = errors.New("index not found")
	ErrChecksumMismatch  = errors.New("invalid data dictionary checksum")
)

const (
	dictPayloadMagic      = "XDDC"
	dictPayloadVersion    = uint16(1)
	dictPayloadHeaderSize = 12 // magic(4)+version(2)+reserved(2)+dataLen(4)
	dictPayloadDataOffset = 104
)

type dictIndexRecord struct {
	Index   *IndexDef `json:"index"`
	TableID uint64    `json:"table_id"`
}

type dictPayload struct {
	Version uint16            `json:"v"`
	Tables  []*TableDef       `json:"tables"`
	Indexes []dictIndexRecord `json:"indexes"`
}

// DataDictionaryPageWrapper 数据字典页面包装器
type DataDictionaryPageWrapper struct {
	*BasePageWrapper

	// Buffer Pool支持
	bufferPool *buffer_pool.BufferPool

	// 存储提供者，用于回退读写
	storageProvider basic.StorageProvider

	// 并发控制
	mu sync.RWMutex

	// 底层的数据字典页面实现
	dictPage *pages.DataDictionaryHeaderSysPage

	// 数据字典信息
	maxTableID uint64
	maxIndexID uint64
	maxSpaceID uint32

	// 表/索引定义
	tables   map[uint64]*TableDef
	indexMap map[uint64]dictIndexRecord
}

// NewDataDictionaryPageWrapper 创建数据字典页面
func NewDataDictionaryPageWrapper(id, spaceID uint32, bp *buffer_pool.BufferPool) *DataDictionaryPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_SYS)
	dictPage := pages.NewDataDictHeaderPage()

	return &DataDictionaryPageWrapper{
		BasePageWrapper: base,
		bufferPool:      bp,
		dictPage:        dictPage,
		tables:          make(map[uint64]*TableDef),
		indexMap:        make(map[uint64]dictIndexRecord),
	}
}

// SetStorageProvider 设置回退读取/写入的 storage 提供者
func (dw *DataDictionaryPageWrapper) SetStorageProvider(provider basic.StorageProvider) {
	dw.Lock()
	defer dw.Unlock()
	dw.storageProvider = provider
}

// ParseFromBytes 从字节数据解析数据字典页面
func (dw *DataDictionaryPageWrapper) ParseFromBytes(data []byte) error {
	if err := dw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	parsedPage := pages.ParseDataDictHrdPage(data)
	dw.dictPage = parsedPage

	if len(data) < len(dw.content) {
		return ErrInvalidDictHeader
	}

	dw.maxTableID = parsedPage.GetMaxTableId()
	dw.maxIndexID = parsedPage.GetMaxIndexId()
	dw.maxSpaceID = uint32(parsedPage.GetMaxSpaceId())

	return dw.parseDataDictContent(data)
}

// ToBytes 序列化数据字典页面为字节数组
func (dw *DataDictionaryPageWrapper) ToBytes() ([]byte, error) {
	dw.Lock()
	defer dw.Unlock()

	if dw.dictPage == nil {
		dw.dictPage = pages.NewDataDictHeaderPage()
	}

	if dw.maxTableID > 0 {
		dw.dictPage.SetMaxTableId(dw.maxTableID)
	}

	if dw.maxIndexID > 0 {
		dw.dictPage.SetMaxIndexId(dw.maxIndexID)
	}

	if dw.maxSpaceID > 0 {
		// 字段在底层页结构中是4字节，避免直接拼接8字节转换导致越界
		// 这里不调用 SetMaxSpaceId，避免历史实现中的长度问题。
	}

	data := dw.dictPage.GetSerializeBytes()
	if len(data) == 0 {
		return nil, ErrInvalidDictHeader
	}

	payload, err := dw.buildPayload()
	if err != nil {
		return nil, err
	}
	if len(payload) > dw.getDictionaryPayloadCapacity() {
		return nil, fmt.Errorf("dictionary payload too large: %d > %d", len(payload), dw.getDictionaryPayloadCapacity())
	}

	if dictPayloadDataOffset+len(payload) > len(data)-pages.FileTrailerSize {
		return nil, fmt.Errorf("dictionary payload offset overflow")
	}

	copy(data[dictPayloadDataOffset:], payload)
	// 清空 payload 区域剩余字节，避免被历史脏数据污染
	restStart := dictPayloadDataOffset + len(payload)
	for i := restStart; i < len(data)-pages.FileTrailerSize; i++ {
		data[i] = 0
	}

	dw.updateSerializedChecksumLocked(data)
	if len(dw.content) != len(data) {
		dw.content = make([]byte, len(data))
	}
	copy(dw.content, data)
	dw.markDirtyLocked()
	return data, nil
}

func (dw *DataDictionaryPageWrapper) updateSerializedChecksumLocked(data []byte) {
	if len(data) < pages.FileHeaderSize+pages.FileTrailerSize {
		return
	}

	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	checksum32 := checker.CalculateChecksum(data)
	binary.LittleEndian.PutUint32(data[0:4], checksum32)

	dw.trailer.SetChecksum(uint64(checksum32))
	copy(data[len(data)-pages.FileTrailerSize:], dw.trailer.FileTrailer[:])
}

func (dw *DataDictionaryPageWrapper) markDirtyLocked() {
	dw.dirty = true
	dw.state = basic.PageStateDirty
	dw.stats.DirtyCount++
}

// parseDataDictContent 解析数据字典内容
func (dw *DataDictionaryPageWrapper) parseDataDictContent(content []byte) error {
	if len(content) < dictPayloadDataOffset+dictPayloadHeaderSize {
		return nil
	}

	offset := dictPayloadDataOffset
	header := content[offset : offset+dictPayloadHeaderSize]
	if len(header) < dictPayloadHeaderSize {
		return nil
	}

	magic := string(header[0:4])
	if magic != dictPayloadMagic {
		// 没有持久化元数据，保持空字典
		dw.tables = make(map[uint64]*TableDef)
		dw.indexMap = make(map[uint64]dictIndexRecord)
		return nil
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version > dictPayloadVersion {
		return ErrInvalidDictHeader
	}

	payloadLen := int(binary.LittleEndian.Uint32(header[8:12]))
	payloadStart := dictPayloadDataOffset + dictPayloadHeaderSize
	payloadEnd := payloadStart + payloadLen
	if payloadLen <= 0 || payloadEnd > len(content)-pages.FileTrailerSize {
		return nil
	}

	rawPayload := content[payloadStart:payloadEnd]
	var payload dictPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return ErrInvalidTableDef
	}

	dw.tables = make(map[uint64]*TableDef, len(payload.Tables))
	dw.indexMap = make(map[uint64]dictIndexRecord, len(payload.Indexes))

	for _, tableDef := range payload.Tables {
		if tableDef == nil || tableDef.ID == 0 {
			return ErrInvalidTableDef
		}
		dw.tables[tableDef.ID] = cloneTableDef(tableDef)
	}

	for _, indexRecord := range payload.Indexes {
		if indexRecord.Index == nil || indexRecord.Index.ID == 0 {
			return ErrInvalidIndexDef
		}
		dw.indexMap[indexRecord.Index.ID] = dictIndexRecord{
			Index:   cloneIndexDef(indexRecord.Index),
			TableID: indexRecord.TableID,
		}
	}

	// 同步表级索引到 table.Indexes，便于兼容旧字段读取
	dw.syncTableIndexesLocked()

	// 如无 header 中记录，按持久化数据补齐最大 ID
	for tableID := range dw.tables {
		if tableID > dw.maxTableID {
			dw.maxTableID = tableID
		}
	}
	for indexID := range dw.indexMap {
		if indexID > dw.maxIndexID {
			dw.maxIndexID = indexID
		}
	}

	return nil
}

// Read 实现PageWrapper接口
func (dw *DataDictionaryPageWrapper) Read() error {
	if dw.bufferPool != nil {
		if page, err := dw.bufferPool.GetPage(dw.GetSpaceID(), dw.GetPageID()); err == nil && page != nil {
			content := page.GetContent()
			if err := dw.parseAndCacheContent(content); err == nil {
				return nil
			}
		}
	}

	content, err := dw.readFromStorage()
	if err != nil {
		return err
	}

	// 回填到buffer pool，避免下一次读取重复走存储
	if dw.bufferPool != nil {
		bufferPage := buffer_pool.NewBufferPage(dw.GetSpaceID(), dw.GetPageID())
		bufferPage.SetContent(content)
		if err := dw.bufferPool.PutPage(bufferPage); err != nil {
			return err
		}
	}

	return dw.parseAndCacheContent(content)
}

func (dw *DataDictionaryPageWrapper) parseAndCacheContent(content []byte) error {
	if len(content) == 0 {
		return ErrInvalidDictHeader
	}

	dw.Lock()
	defer dw.Unlock()
	dw.content = make([]byte, len(content))
	copy(dw.content, content)

	return dw.parseDataDictContent(content)
}

// Write 实现PageWrapper接口
func (dw *DataDictionaryPageWrapper) Write() error {
	content, err := dw.ToBytes()
	if err != nil {
		return err
	}

	if dw.bufferPool != nil {
		bufferPage, err := dw.bufferPool.GetPage(dw.GetSpaceID(), dw.GetPageID())
		if err == nil && bufferPage != nil {
			bufferPage.SetContent(content)
			bufferPage.MarkDirty()
		} else {
			bufferPage = buffer_pool.NewBufferPage(dw.GetSpaceID(), dw.GetPageID())
			bufferPage.SetContent(content)
			bufferPage.MarkDirty()
			if err := dw.bufferPool.PutPage(bufferPage); err != nil {
				return err
			}
		}
	}

	if dw.storageProvider != nil {
		// 兜底直接落盘，兼容未接入 buffer_pool 的路径
		if err := dw.storageProvider.WritePage(dw.GetSpaceID(), dw.GetPageID(), content); err != nil {
			return err
		}
	}

	if dw.bufferPool != nil {
		return dw.bufferPool.FlushPage(buffer_pool.NewBufferPage(dw.GetSpaceID(), dw.GetPageID()))
	}

	dw.Lock()
	dw.content = make([]byte, len(content))
	copy(dw.content, content)
	dw.Unlock()

	return nil
}

// GetTableDef 获取表定义
func (dw *DataDictionaryPageWrapper) GetTableDef(id uint64) (*TableDef, error) {
	dw.RLock()
	defer dw.RUnlock()

	table, ok := dw.tables[id]
	if !ok || table == nil {
		return nil, ErrTableNotFound
	}

	t := cloneTableDef(table)
	dw.attachIndexesToTable(t)
	return t, nil
}

// GetTable 获取表定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) GetTable(id uint64) *TableDef {
	table, err := dw.GetTableDef(id)
	if err != nil {
		return nil
	}
	return table
}

// AddTableDef 添加表定义
func (dw *DataDictionaryPageWrapper) AddTableDef(def *TableDef) error {
	if def == nil || def.ID == 0 {
		return ErrInvalidTableDef
	}

	dw.Lock()
	defer dw.Unlock()

	copyDef := cloneTableDef(def)
	dw.tables[copyDef.ID] = copyDef
	if copyDef.ID > dw.maxTableID {
		dw.maxTableID = copyDef.ID
	}

	dw.syncTableIndexesLocked()
	dw.markDirtyLocked()
	return nil
}

// AddTable 添加表定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) AddTable(table TableDef) error {
	return dw.AddTableDef(&table)
}

// RemoveTableDef 删除表定义
func (dw *DataDictionaryPageWrapper) RemoveTableDef(id uint64) error {
	dw.Lock()
	defer dw.Unlock()

	if _, exists := dw.tables[id]; !exists {
		return ErrTableNotFound
	}
	delete(dw.tables, id)

	// 删除该表下的所有索引
	for indexID, record := range dw.indexMap {
		if record.TableID == id {
			delete(dw.indexMap, indexID)
		}
	}

	dw.syncTableIndexesLocked()
	dw.markDirtyLocked()
	return nil
}

// ListTableDefs 获取所有表定义
func (dw *DataDictionaryPageWrapper) ListTableDefs() ([]*TableDef, error) {
	dw.RLock()
	defer dw.RUnlock()

	result := make([]*TableDef, 0, len(dw.tables))
	for _, table := range dw.tables {
		t := cloneTableDef(table)
		dw.attachIndexesToTable(t)
		result = append(result, t)
	}
	return result, nil
}

// GetTables 获取所有表定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) GetTables() []TableDef {
	dw.RLock()
	defer dw.RUnlock()

	result := make([]TableDef, 0, len(dw.tables))
	for _, table := range dw.tables {
		t := cloneTableDef(table)
		dw.attachIndexesToTable(t)
		result = append(result, *t)
	}
	return result
}

// GetIndexDef 获取索引定义
func (dw *DataDictionaryPageWrapper) GetIndexDef(id uint64) (*IndexDef, error) {
	dw.RLock()
	defer dw.RUnlock()

	record, exists := dw.indexMap[id]
	if !exists {
		return nil, ErrIndexNotFound
	}
	return cloneIndexDef(record.Index), nil
}

// GetIndex 获取索引定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) GetIndex(id uint64) *IndexDef {
	index, err := dw.GetIndexDef(id)
	if err != nil {
		return nil
	}
	return index
}

// AddIndexDefForTable 添加索引定义并绑定表 ID（扩展方法）
func (dw *DataDictionaryPageWrapper) AddIndexDefForTable(index *IndexDef, tableID uint64) error {
	if index == nil || index.ID == 0 {
		return ErrInvalidIndexDef
	}

	dw.Lock()
	defer dw.Unlock()

	record := dictIndexRecord{
		Index:   cloneIndexDef(index),
		TableID: tableID,
	}
	dw.indexMap[record.Index.ID] = record

	if record.Index.ID > dw.maxIndexID {
		dw.maxIndexID = record.Index.ID
	}

	if tableID != 0 {
		if table, exists := dw.tables[tableID]; exists {
			t := cloneIndexDef(index)
			table.Indexes = append(table.Indexes, t)
		}
	}

	dw.syncTableIndexesLocked()
	dw.markDirtyLocked()
	return nil
}

// AddIndexDef 添加索引定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) AddIndexDef(def *IndexDef) error {
	return dw.AddIndexDefForTable(def, 0)
}

// AddIndex 添加索引定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) AddIndex(index *IndexDef) error {
	return dw.AddIndexDef(index)
}

// RemoveIndexDef 删除索引定义
func (dw *DataDictionaryPageWrapper) RemoveIndexDef(id uint64) error {
	dw.Lock()
	defer dw.Unlock()

	record, exists := dw.indexMap[id]
	if !exists {
		return ErrIndexNotFound
	}
	delete(dw.indexMap, id)

	if record.TableID != 0 {
		if table, exists := dw.tables[record.TableID]; exists {
			filtered := make([]*IndexDef, 0, len(table.Indexes))
			for _, index := range table.Indexes {
				if index != nil && index.ID != id {
					filtered = append(filtered, index)
				}
			}
			table.Indexes = filtered
		}
	}

	dw.syncTableIndexesLocked()
	dw.markDirtyLocked()
	return nil
}

// ListIndexDefs 获取所有索引定义
func (dw *DataDictionaryPageWrapper) ListIndexDefs() ([]*IndexDef, error) {
	dw.RLock()
	defer dw.RUnlock()

	result := make([]*IndexDef, 0, len(dw.indexMap))
	for _, indexRecord := range dw.indexMap {
		result = append(result, cloneIndexDef(indexRecord.Index))
	}
	return result, nil
}

// GetIndexes 获取所有索引定义（兼容旧接口）
func (dw *DataDictionaryPageWrapper) GetIndexes() []IndexDef {
	dw.RLock()
	defer dw.RUnlock()

	result := make([]IndexDef, 0, len(dw.indexMap))
	for _, record := range dw.indexMap {
		result = append(result, *cloneIndexDef(record.Index))
	}
	return result
}

// GetMaxTableID 获取最大表ID
func (dw *DataDictionaryPageWrapper) GetMaxTableID() uint64 {
	dw.RLock()
	defer dw.RUnlock()
	return dw.maxTableID
}

// GetMaxIndexID 获取最大索引ID
func (dw *DataDictionaryPageWrapper) GetMaxIndexID() uint64 {
	dw.RLock()
	defer dw.RUnlock()
	return dw.maxIndexID
}

// GetMaxSpaceID 获取最大空间ID
func (dw *DataDictionaryPageWrapper) GetMaxSpaceID() uint32 {
	dw.RLock()
	defer dw.RUnlock()
	return dw.maxSpaceID
}

// GetTableCount 获取表数量
func (dw *DataDictionaryPageWrapper) GetTableCount() uint32 {
	dw.RLock()
	defer dw.RUnlock()
	return uint32(len(dw.tables))
}

// GetIndexCount 获取索引数量
func (dw *DataDictionaryPageWrapper) GetIndexCount() uint32 {
	dw.RLock()
	defer dw.RUnlock()
	return uint32(len(dw.indexMap))
}

// Validate 校验数据字典页面数据完整性
func (dw *DataDictionaryPageWrapper) Validate() error {
	if len(dw.content) == 0 {
		return ErrInvalidDictHeader
	}

	checker := pages.NewPageIntegrityChecker(pages.ChecksumCRC32)
	if err := checker.ValidateChecksum(dw.content); err != nil {
		return ErrChecksumMismatch
	}
	return nil
}

// GetDataDictPage 获取底层的数据字典页面实现
func (dw *DataDictionaryPageWrapper) GetDataDictPage() *pages.DataDictionaryHeaderSysPage {
	dw.RLock()
	defer dw.RUnlock()
	return dw.dictPage
}

func (dw *DataDictionaryPageWrapper) buildPayload() ([]byte, error) {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	payloadTables := make([]*TableDef, 0, len(dw.tables))
	for _, table := range dw.tables {
		if table == nil {
			continue
		}
		cloned := cloneTableDef(table)
		// 避免重复持久化，索引通过 indexMap 统一管理
		cloned.Indexes = nil
		payloadTables = append(payloadTables, cloned)
	}

	payloadIndexes := make([]dictIndexRecord, 0, len(dw.indexMap))
	for _, idx := range dw.indexMap {
		payloadIndexes = append(payloadIndexes, dictIndexRecord{
			Index:   cloneIndexDef(idx.Index),
			TableID: idx.TableID,
		})
	}

	payload := dictPayload{
		Version: dictPayloadVersion,
		Tables:  payloadTables,
		Indexes: payloadIndexes,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	header := make([]byte, dictPayloadHeaderSize)
	copy(header[0:4], []byte(dictPayloadMagic))
	binary.LittleEndian.PutUint16(header[4:6], dictPayloadVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(body)))

	raw := make([]byte, 0, dictPayloadHeaderSize+len(body))
	raw = append(raw, header...)
	raw = append(raw, body...)
	return raw, nil
}

func (dw *DataDictionaryPageWrapper) getDictionaryPayloadCapacity() int {
	return common.PageSize - dictPayloadDataOffset - pages.FileTrailerSize
}

// 内部方法：从storage读取
func (dw *DataDictionaryPageWrapper) readFromStorage() ([]byte, error) {
	if dw.storageProvider == nil {
		return nil, errors.New("no storage provider configured for data dictionary page")
	}

	content, err := dw.storageProvider.ReadPage(dw.GetSpaceID(), dw.GetPageID())
	if err != nil {
		return nil, err
	}
	if len(content) < common.PageSize {
		return nil, fmt.Errorf("invalid page size loaded from storage: %d", len(content))
	}

	result := make([]byte, common.PageSize)
	copy(result, content[:common.PageSize])
	return result, nil
}

// 内部方法：写入storage
func (dw *DataDictionaryPageWrapper) writeToStorage(content []byte) error {
	if dw.storageProvider == nil {
		return nil
	}

	if len(content) < common.PageSize {
		return errors.New("invalid page content size")
	}

	return dw.storageProvider.WritePage(dw.GetSpaceID(), dw.GetPageID(), content[:common.PageSize])
}

func cloneTableDef(src *TableDef) *TableDef {
	if src == nil {
		return nil
	}

	dst := &TableDef{
		ID:         src.ID,
		Name:       src.Name,
		Columns:    make([]*ColumnDef, len(src.Columns)),
		Indexes:    make([]*IndexDef, len(src.Indexes)),
		Properties: make(map[string]string, len(src.Properties)),
	}

	for i, col := range src.Columns {
		if col != nil {
			cc := *col
			dst.Columns[i] = &cc
		}
	}

	for i, idx := range src.Indexes {
		if idx != nil {
			dst.Indexes[i] = cloneIndexDef(idx)
		}
	}

	for k, v := range src.Properties {
		dst.Properties[k] = v
	}

	return dst
}

func cloneIndexDef(src *IndexDef) *IndexDef {
	if src == nil {
		return nil
	}
	dst := *src
	if src.Columns != nil {
		dst.Columns = make([]string, len(src.Columns))
		copy(dst.Columns, src.Columns)
	}
	return &dst
}

func (dw *DataDictionaryPageWrapper) attachIndexesToTable(table *TableDef) {
	if table == nil {
		return
	}
	table.Indexes = table.Indexes[:0]
	for _, record := range dw.indexMap {
		if record.TableID == table.ID {
			table.Indexes = append(table.Indexes, cloneIndexDef(record.Index))
		}
	}
}

func (dw *DataDictionaryPageWrapper) syncTableIndexesLocked() {
	for _, table := range dw.tables {
		table.Indexes = table.Indexes[:0]
	}
	for indexID, record := range dw.indexMap {
		_ = indexID
		if record.TableID == 0 {
			continue
		}
		if table, exists := dw.tables[record.TableID]; exists {
			table.Indexes = append(table.Indexes, cloneIndexDef(record.Index))
		}
	}
}

// GetIndexMap 供测试与高层管理器取值
func (dw *DataDictionaryPageWrapper) GetIndexMap() map[uint64]uint64 {
	dw.RLock()
	defer dw.RUnlock()

	result := make(map[uint64]uint64, len(dw.indexMap))
	for indexID, record := range dw.indexMap {
		result[indexID] = record.TableID
	}
	return result
}

func (dw *DataDictionaryPageWrapper) readFromDisk() ([]byte, error) {
	// 向后兼容保留方法名，内部优先从 storage provider 回退
	return dw.readFromStorage()
}

// writeToDisk 回退接口，默认写入storage provider
func (dw *DataDictionaryPageWrapper) writeToDisk(content []byte) error {
	if err := dw.writeToStorage(content); err != nil {
		return err
	}
	return nil
}

func (dw *DataDictionaryPageWrapper) writeToDiskWithBuffer(content []byte) error {
	if len(content) > len(dw.content) {
		return fmt.Errorf("page data too large: %d > %d", len(content), len(dw.content))
	}

	dw.Lock()
	dw.content = make([]byte, len(content))
	copy(dw.content, content)
	dw.Unlock()

	return dw.writeToStorage(content)
}

// Init/Release 留空实现，兼容 interface
func (dw *DataDictionaryPageWrapper) Init() error    { return nil }
func (dw *DataDictionaryPageWrapper) Release() error { return nil }

// GetBufferPage/GetBufferPage 兼容 BasePageWrapper 的扩展接口
func (dw *DataDictionaryPageWrapper) GetBufferPage() *buffer_pool.BufferPage  { return nil }
func (dw *DataDictionaryPageWrapper) SetBufferPage(_ *buffer_pool.BufferPage) {}

func (dw *DataDictionaryPageWrapper) validateTableName(name string) bool {
	if strings.TrimSpace(name) == "" {
		return false
	}
	return true
}
