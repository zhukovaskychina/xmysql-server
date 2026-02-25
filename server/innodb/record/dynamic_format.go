package record

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
)

/*
DynamicRowFormat 实现了InnoDB Dynamic行格式

Dynamic行格式是Compact格式的优化版本（MySQL 5.7+默认格式）：
基本结构与Compact相同，但行溢出处理机制不同：

Compact行溢出阈值：
- 大字段前768字节存储在数据页
- 剩余部分存储在BLOB页，数据页保留20字节指针

Dynamic行溢出阈值：
- 大字段完全存储在BLOB页（仅保留20字节指针）
- 优化了大字段的存储和访问性能
- 更适合TEXT/BLOB等大对象存储

行溢出指针格式（20字节）：
[SpaceID 4B][PageNo 4B][Offset 4B][BlobLength 8B]

设计要点：
1. 行溢出判断：单字段超过40字节且行大小超过8KB触发
2. 指针管理：维护到BLOB页面的映射关系
3. 延迟加载：只在需要时加载BLOB数据
4. 性能优化：减少数据页空间占用，提高缓存效率
*/

const (
	// Dynamic格式特有常量
	DynamicOverflowThreshold = 40       // 字段长度超过40字节可能溢出
	DynamicRowSizeThreshold  = 8 * 1024 // 行大小超过8KB触发溢出检查

	// 溢出指针大小
	OverflowPointerSize = 20

	// 溢出指针组件偏移
	OverflowSpaceIDOffset = 0  // 表空间ID（4字节）
	OverflowPageNoOffset  = 4  // 页面号（4字节）
	OverflowOffsetOffset  = 8  // 页内偏移（4字节）
	OverflowLengthOffset  = 12 // BLOB长度（8字节）

	// 每页最大行数据大小（预留头部和尾部）
	MaxRowSizePerPage = 16384 - 200 // 16KB - 预留空间
)

// DynamicRowFormat Dynamic行格式处理器
type DynamicRowFormat struct {
	// 嵌入Compact格式处理器（复用大部分逻辑）
	*CompactRowFormat

	// 溢出字段管理
	overflowCols map[int]bool // 可能溢出的列索引

	// BLOB管理器接口
	blobManager BlobManagerInterface

	// 统计信息
	stats *DynamicFormatStats

	// 配置
	config *DynamicFormatConfig

	mu sync.RWMutex
}

// BlobManagerInterface BLOB管理器接口
type BlobManagerInterface interface {
	// 写入BLOB数据，返回BLOB ID
	WriteBlob(segmentID uint32, data []byte) (uint64, error)

	// 读取BLOB数据
	ReadBlob(blobID uint64) ([]byte, error)

	// 部分读取BLOB
	ReadBlobPartial(blobID uint64, offset, length uint32) ([]byte, error)

	// 删除BLOB
	DeleteBlob(blobID uint64) error

	// 获取BLOB元数据
	GetBlobMetadata(blobID uint64) (*BlobMetadata, error)
}

// BlobMetadata BLOB元数据
type BlobMetadata struct {
	SpaceID uint32
	PageNo  uint32
	Offset  uint32
	Length  uint64
}

// DynamicFormatConfig Dynamic格式配置
type DynamicFormatConfig struct {
	// 溢出阈值
	OverflowThreshold int // 字段长度超过此值考虑溢出
	RowSizeThreshold  int // 行大小超过此值触发溢出检查

	// 性能优化
	EnableLazyLoad    bool // 启用延迟加载
	EnablePrefetch    bool // 启用预取
	CacheOverflowData bool // 缓存溢出数据

	// 并发控制
	MaxConcurrentBlobs int // 最大并发BLOB操作数
}

// DynamicFormatStats Dynamic格式统计
type DynamicFormatStats struct {
	// 溢出统计
	totalRows         uint64 // 总行数
	overflowRows      uint64 // 溢出行数
	overflowFields    uint64 // 溢出字段数
	totalOverflowSize uint64 // 总溢出数据大小

	// 性能统计
	blobReads   uint64 // BLOB读取次数
	blobWrites  uint64 // BLOB写入次数
	cacheHits   uint64 // 缓存命中次数
	cacheMisses uint64 // 缓存未命中次数

	// 错误统计
	blobReadErrors  uint64 // BLOB读取错误
	blobWriteErrors uint64 // BLOB写入错误
}

// OverflowPointer 溢出指针
type OverflowPointer struct {
	SpaceID uint32 // 表空间ID
	PageNo  uint32 // 页面号
	Offset  uint32 // 页内偏移
	Length  uint64 // BLOB数据长度
	BlobID  uint64 // BLOB ID（内部使用）
}

// DynamicRow Dynamic格式的行数据
type DynamicRow struct {
	*CompactRow // 嵌入Compact行结构

	// 溢出信息
	OverflowPointers map[int]*OverflowPointer // 列索引 -> 溢出指针
	OverflowData     map[int][]byte           // 缓存的溢出数据
}

// NewDynamicRowFormat 创建Dynamic行格式处理器
func NewDynamicRowFormat(columns []*ColumnDef, blobManager BlobManagerInterface) *DynamicRowFormat {
	drf := &DynamicRowFormat{
		CompactRowFormat: NewCompactRowFormat(columns),
		overflowCols:     make(map[int]bool),
		blobManager:      blobManager,
		stats:            &DynamicFormatStats{},
		config: &DynamicFormatConfig{
			OverflowThreshold:  DynamicOverflowThreshold,
			RowSizeThreshold:   DynamicRowSizeThreshold,
			EnableLazyLoad:     true,
			EnablePrefetch:     false,
			CacheOverflowData:  true,
			MaxConcurrentBlobs: 10,
		},
	}

	// 识别可能溢出的列（TEXT/BLOB/长VARCHAR）
	for i, col := range columns {
		if col.IsVarLen && (col.Type == TypeText || col.Type == TypeBlob || col.Type == TypeVarchar) {
			drf.overflowCols[i] = true
		}
	}

	return drf
}

// EncodeRow 编码行数据为Dynamic格式
func (drf *DynamicRowFormat) EncodeRow(values []interface{}, trxID, rollPtr uint64, segmentID uint32) ([]byte, error) {
	atomic.AddUint64(&drf.stats.totalRows, 1)

	// 1. 判断是否需要行溢出
	needsOverflow, overflowCandidates := drf.shouldOverflow(values)

	if !needsOverflow {
		// 不需要溢出，使用标准Compact格式
		return drf.CompactRowFormat.EncodeRow(values, trxID, rollPtr)
	}

	// 2. 处理溢出字段
	atomic.AddUint64(&drf.stats.overflowRows, 1)
	processedValues := make([]interface{}, len(values))
	copy(processedValues, values)

	overflowPointers := make(map[int]*OverflowPointer)

	for colIdx := range overflowCandidates {
		if values[colIdx] == nil {
			continue
		}

		// 将大字段数据写入BLOB页
		data := drf.valueToBytes(values[colIdx], drf.columns[colIdx])

		blobID, err := drf.blobManager.WriteBlob(segmentID, data)
		if err != nil {
			atomic.AddUint64(&drf.stats.blobWriteErrors, 1)
			return nil, fmt.Errorf("failed to write blob for column %d: %w", colIdx, err)
		}

		atomic.AddUint64(&drf.stats.blobWrites, 1)
		atomic.AddUint64(&drf.stats.overflowFields, 1)
		atomic.AddUint64(&drf.stats.totalOverflowSize, uint64(len(data)))

		// 获取BLOB元数据
		metadata, err := drf.blobManager.GetBlobMetadata(blobID)
		if err != nil {
			return nil, fmt.Errorf("failed to get blob metadata: %w", err)
		}

		// 创建溢出指针
		pointer := &OverflowPointer{
			SpaceID: metadata.SpaceID,
			PageNo:  metadata.PageNo,
			Offset:  metadata.Offset,
			Length:  metadata.Length,
			BlobID:  blobID,
		}
		overflowPointers[colIdx] = pointer

		// 替换原值为溢出指针（20字节）
		processedValues[colIdx] = drf.encodeOverflowPointer(pointer)
	}

	// 3. 使用Compact格式编码（溢出字段已替换为指针）
	encodedData, err := drf.CompactRowFormat.EncodeRow(processedValues, trxID, rollPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to encode row: %w", err)
	}

	return encodedData, nil
}

// DecodeRow 解码Dynamic格式的行数据
func (drf *DynamicRowFormat) DecodeRow(data []byte, loadOverflow bool) (*DynamicRow, error) {
	// 1. 使用Compact格式解码
	compactRow, err := drf.CompactRowFormat.DecodeRow(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode compact row: %w", err)
	}

	// 2. 创建Dynamic行对象
	dynamicRow := &DynamicRow{
		CompactRow:       compactRow,
		OverflowPointers: make(map[int]*OverflowPointer),
		OverflowData:     make(map[int][]byte),
	}

	// 3. 识别和解析溢出指针
	for colIdx := range drf.overflowCols {
		if compactRow.ColumnValues[colIdx] == nil {
			continue
		}

		// 检查是否为溢出指针（固定20字节）
		if len(compactRow.ColumnValues[colIdx]) == OverflowPointerSize {
			pointer := drf.decodeOverflowPointer(compactRow.ColumnValues[colIdx])
			dynamicRow.OverflowPointers[colIdx] = pointer

			// 如果需要加载溢出数据
			if loadOverflow {
				data, err := drf.loadOverflowData(pointer)
				if err != nil {
					atomic.AddUint64(&drf.stats.blobReadErrors, 1)
					return nil, fmt.Errorf("failed to load overflow data for column %d: %w", colIdx, err)
				}
				dynamicRow.OverflowData[colIdx] = data
				atomic.AddUint64(&drf.stats.blobReads, 1)
			}
		}
	}

	return dynamicRow, nil
}

// shouldOverflow 判断是否需要行溢出
func (drf *DynamicRowFormat) shouldOverflow(values []interface{}) (bool, map[int]bool) {
	// 计算行总大小
	totalSize := drf.CalculateRowSize(values)

	// 如果行大小小于阈值，不需要溢出
	if totalSize < drf.config.RowSizeThreshold {
		return false, nil
	}

	// 识别溢出候选字段
	candidates := make(map[int]bool)

	for colIdx := range drf.overflowCols {
		if values[colIdx] == nil {
			continue
		}

		data := drf.valueToBytes(values[colIdx], drf.columns[colIdx])

		// 字段长度超过阈值，标记为溢出候选
		if len(data) > drf.config.OverflowThreshold {
			candidates[colIdx] = true
		}
	}

	// 如果有溢出候选，需要溢出
	return len(candidates) > 0, candidates
}

// encodeOverflowPointer 编码溢出指针
func (drf *DynamicRowFormat) encodeOverflowPointer(pointer *OverflowPointer) []byte {
	data := make([]byte, OverflowPointerSize)

	// SpaceID (4字节)
	binary.BigEndian.PutUint32(data[OverflowSpaceIDOffset:], pointer.SpaceID)

	// PageNo (4字节)
	binary.BigEndian.PutUint32(data[OverflowPageNoOffset:], pointer.PageNo)

	// Offset (4字节)
	binary.BigEndian.PutUint32(data[OverflowOffsetOffset:], pointer.Offset)

	// Length (8字节)
	binary.BigEndian.PutUint64(data[OverflowLengthOffset:], pointer.Length)

	return data
}

// decodeOverflowPointer 解码溢出指针
func (drf *DynamicRowFormat) decodeOverflowPointer(data []byte) *OverflowPointer {
	if len(data) != OverflowPointerSize {
		return nil
	}

	pointer := &OverflowPointer{
		SpaceID: binary.BigEndian.Uint32(data[OverflowSpaceIDOffset:]),
		PageNo:  binary.BigEndian.Uint32(data[OverflowPageNoOffset:]),
		Offset:  binary.BigEndian.Uint32(data[OverflowOffsetOffset:]),
		Length:  binary.BigEndian.Uint64(data[OverflowLengthOffset:]),
	}

	// 从元数据构造BlobID（简化实现，实际可能需要更复杂的映射）
	pointer.BlobID = (uint64(pointer.SpaceID) << 32) | uint64(pointer.PageNo)

	return pointer
}

// loadOverflowData 加载溢出数据
func (drf *DynamicRowFormat) loadOverflowData(pointer *OverflowPointer) ([]byte, error) {
	if drf.blobManager == nil {
		return nil, fmt.Errorf("blob manager not configured")
	}

	// 从BLOB管理器读取数据
	data, err := drf.blobManager.ReadBlob(pointer.BlobID)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob %d: %w", pointer.BlobID, err)
	}

	// 验证长度
	if uint64(len(data)) != pointer.Length {
		return nil, fmt.Errorf("blob length mismatch: expected %d, got %d", pointer.Length, len(data))
	}

	return data, nil
}

// GetColumnValue 获取列值（自动加载溢出数据）
func (drf *DynamicRowFormat) GetColumnValue(row *DynamicRow, colIdx int) ([]byte, error) {
	// 检查是否有溢出指针
	if pointer, exists := row.OverflowPointers[colIdx]; exists {
		// 检查缓存
		if data, cached := row.OverflowData[colIdx]; cached {
			atomic.AddUint64(&drf.stats.cacheHits, 1)
			return data, nil
		}

		atomic.AddUint64(&drf.stats.cacheMisses, 1)

		// 加载溢出数据
		data, err := drf.loadOverflowData(pointer)
		if err != nil {
			return nil, err
		}

		// 缓存数据
		if drf.config.CacheOverflowData {
			row.OverflowData[colIdx] = data
		}

		return data, nil
	}

	// 普通列，直接返回
	return row.ColumnValues[colIdx], nil
}

// GetColumnValuePartial 部分获取列值（用于大字段的分页读取）
func (drf *DynamicRowFormat) GetColumnValuePartial(row *DynamicRow, colIdx int, offset, length uint32) ([]byte, error) {
	// 检查是否有溢出指针
	pointer, exists := row.OverflowPointers[colIdx]
	if !exists {
		// 普通列，直接返回切片
		data := row.ColumnValues[colIdx]
		if offset >= uint32(len(data)) {
			return nil, fmt.Errorf("offset out of range")
		}
		end := offset + length
		if end > uint32(len(data)) {
			end = uint32(len(data))
		}
		return data[offset:end], nil
	}

	// 验证范围
	if uint64(offset) >= pointer.Length {
		return nil, fmt.Errorf("offset out of range: %d >= %d", offset, pointer.Length)
	}

	// 调整长度
	if uint64(offset+length) > pointer.Length {
		length = uint32(pointer.Length - uint64(offset))
	}

	// 部分读取BLOB
	data, err := drf.blobManager.ReadBlobPartial(pointer.BlobID, offset, length)
	if err != nil {
		atomic.AddUint64(&drf.stats.blobReadErrors, 1)
		return nil, fmt.Errorf("failed to read blob partial: %w", err)
	}

	atomic.AddUint64(&drf.stats.blobReads, 1)
	return data, nil
}

// DeleteRow 删除行（包括溢出数据）
func (drf *DynamicRowFormat) DeleteRow(row *DynamicRow) error {
	// 删除所有溢出BLOB
	for _, pointer := range row.OverflowPointers {
		if err := drf.blobManager.DeleteBlob(pointer.BlobID); err != nil {
			return fmt.Errorf("failed to delete blob %d: %w", pointer.BlobID, err)
		}
	}

	return nil
}

// UpdateConfig 更新配置
func (drf *DynamicRowFormat) UpdateConfig(config *DynamicFormatConfig) {
	drf.mu.Lock()
	defer drf.mu.Unlock()
	drf.config = config
}

// GetStats 获取统计信息
func (drf *DynamicRowFormat) GetStats() *DynamicFormatStats {
	stats := &DynamicFormatStats{}

	// 原子读取统计数据
	stats.totalRows = atomic.LoadUint64(&drf.stats.totalRows)
	stats.overflowRows = atomic.LoadUint64(&drf.stats.overflowRows)
	stats.overflowFields = atomic.LoadUint64(&drf.stats.overflowFields)
	stats.totalOverflowSize = atomic.LoadUint64(&drf.stats.totalOverflowSize)
	stats.blobReads = atomic.LoadUint64(&drf.stats.blobReads)
	stats.blobWrites = atomic.LoadUint64(&drf.stats.blobWrites)
	stats.cacheHits = atomic.LoadUint64(&drf.stats.cacheHits)
	stats.cacheMisses = atomic.LoadUint64(&drf.stats.cacheMisses)
	stats.blobReadErrors = atomic.LoadUint64(&drf.stats.blobReadErrors)
	stats.blobWriteErrors = atomic.LoadUint64(&drf.stats.blobWriteErrors)

	return stats
}

// GetOverflowRate 获取溢出率
func (drf *DynamicRowFormat) GetOverflowRate() float64 {
	totalRows := atomic.LoadUint64(&drf.stats.totalRows)
	if totalRows == 0 {
		return 0.0
	}
	overflowRows := atomic.LoadUint64(&drf.stats.overflowRows)
	return float64(overflowRows) / float64(totalRows) * 100
}

// GetAverageOverflowSize 获取平均溢出大小
func (drf *DynamicRowFormat) GetAverageOverflowSize() uint64 {
	overflowRows := atomic.LoadUint64(&drf.stats.overflowRows)
	if overflowRows == 0 {
		return 0
	}
	totalSize := atomic.LoadUint64(&drf.stats.totalOverflowSize)
	return totalSize / overflowRows
}

// GetCacheHitRate 获取缓存命中率
func (drf *DynamicRowFormat) GetCacheHitRate() float64 {
	hits := atomic.LoadUint64(&drf.stats.cacheHits)
	misses := atomic.LoadUint64(&drf.stats.cacheMisses)
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total) * 100
}

// ResetStats 重置统计信息
func (drf *DynamicRowFormat) ResetStats() {
	atomic.StoreUint64(&drf.stats.totalRows, 0)
	atomic.StoreUint64(&drf.stats.overflowRows, 0)
	atomic.StoreUint64(&drf.stats.overflowFields, 0)
	atomic.StoreUint64(&drf.stats.totalOverflowSize, 0)
	atomic.StoreUint64(&drf.stats.blobReads, 0)
	atomic.StoreUint64(&drf.stats.blobWrites, 0)
	atomic.StoreUint64(&drf.stats.cacheHits, 0)
	atomic.StoreUint64(&drf.stats.cacheMisses, 0)
	atomic.StoreUint64(&drf.stats.blobReadErrors, 0)
	atomic.StoreUint64(&drf.stats.blobWriteErrors, 0)
}
