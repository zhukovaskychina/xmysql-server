package engine

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// StorageAdapter 存储适配器，连接算子与存储引擎
// 提供抽象的存储访问接口，隐藏底层存储细节
type StorageAdapter struct {
	tableManager        *manager.TableManager
	bufferPoolManager   *manager.OptimizedBufferPoolManager
	storageManager      *manager.StorageManager
	tableStorageManager *manager.TableStorageManager
}

// NewStorageAdapter 创建存储适配器
func NewStorageAdapter(
	tableManager *manager.TableManager,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	storageManager *manager.StorageManager,
	tableStorageManager *manager.TableStorageManager,
) *StorageAdapter {
	return &StorageAdapter{
		tableManager:        tableManager,
		bufferPoolManager:   bufferPoolManager,
		storageManager:      storageManager,
		tableStorageManager: tableStorageManager,
	}
}

// GetTableMetadata 获取表的元数据
func (sa *StorageAdapter) GetTableMetadata(ctx context.Context, schemaName, tableName string) (*TableScanMetadata, error) {
	// 1. 获取表的元数据
	table, err := sa.tableManager.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 2. 获取表的存储信息 (包含表空间ID和段信息)
	storageInfo, err := sa.tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table storage info: %w", err)
	}

	return &TableScanMetadata{
		Schema:      table,
		SpaceID:     storageInfo.SpaceID,
		RootPageNo:  storageInfo.RootPageNo,
		FirstPageNo: 3, // InnoDB默认从第3页开始存储数据
	}, nil
}

// ReadPage 读取指定页面
func (sa *StorageAdapter) ReadPage(ctx context.Context, spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	page, err := sa.bufferPoolManager.GetPage(spaceID, pageNo)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d from space %d: %w", pageNo, spaceID, err)
	}
	return page, nil
}

// ParseRecords 解析页面中的记录
// 根据InnoDB页面格式解析记录，返回Record列表
func (sa *StorageAdapter) ParseRecords(ctx context.Context, page *buffer_pool.BufferPage, schema *metadata.Table) ([]Record, error) {
	content := page.GetContent()
	if len(content) < common.PageHeaderSize {
		return nil, fmt.Errorf("invalid page content size: %d", len(content))
	}

	// 解析页面头部信息
	pageType := common.PageType(binary.LittleEndian.Uint16(content[24:26]))
	recordCount := binary.LittleEndian.Uint16(content[40:42])

	// 只处理索引页（数据页）
	if pageType != common.FIL_PAGE_INDEX {
		logger.Debugf("Skip non-index page, type: %d", pageType)
		return []Record{}, nil
	}

	// 解析记录
	records := make([]Record, 0, recordCount)
	offset := common.PageHeaderSize

	for i := uint16(0); i < recordCount && offset < len(content)-8; i++ {
		// 检查是否到达页面结尾
		if offset+20 > len(content) {
			break
		}

		// 解析记录头（简化版本）
		// 记录格式：[记录头|列数据...]
		recordHeader := content[offset : offset+5]
		_ = recordHeader // 暂时不解析记录头

		offset += 5

		// 解析列数据
		values := make([]basic.Value, len(schema.Columns))
		for colIdx, col := range schema.Columns {
			if offset >= len(content)-8 {
				break
			}

			value, bytesRead := sa.parseColumnValue(content[offset:], string(col.DataType))
			values[colIdx] = value
			offset += bytesRead
		}

		// 创建记录
		record := NewExecutorRecordFromValues(values, nil) // TODO: Fix schema parameter
		records = append(records, record)
	}

	return records, nil
}

// parseColumnValue 解析列值
func (sa *StorageAdapter) parseColumnValue(data []byte, colType string) (basic.Value, int) {
	switch colType {
	case "INT", "INTEGER", "BIGINT":
		if len(data) < 8 {
			return basic.NewInt64Value(0), 0
		}
		val := int64(binary.LittleEndian.Uint64(data[:8]))
		return basic.NewInt64Value(val), 8

	case "VARCHAR", "CHAR", "TEXT":
		// 变长字段：前2字节为长度
		if len(data) < 2 {
			return basic.NewString(""), 0
		}
		strLen := binary.LittleEndian.Uint16(data[:2])
		if len(data) < int(2+strLen) {
			return basic.NewString(""), 2
		}
		str := string(data[2 : 2+strLen])
		return basic.NewString(str), int(2 + strLen)

	default:
		// 默认按8字节处理
		if len(data) < 8 {
			return basic.NewString(""), 0
		}
		return basic.NewString(""), 8
	}
}

// ScanTable 扫描表的所有页面
// 返回页面迭代器，支持流式扫描
func (sa *StorageAdapter) ScanTable(ctx context.Context, metadata *TableScanMetadata) (*TablePageIterator, error) {
	return &TablePageIterator{
		adapter:     sa,
		ctx:         ctx,
		spaceID:     metadata.SpaceID,
		currentPage: metadata.FirstPageNo,
		schema:      metadata.Schema,
	}, nil
}

// TableScanMetadata 表扫描元数据
type TableScanMetadata struct {
	Schema      *metadata.Table
	SpaceID     uint32
	RootPageNo  uint32
	FirstPageNo uint32
}

// TablePageIterator 表页面迭代器
type TablePageIterator struct {
	adapter     *StorageAdapter
	ctx         context.Context
	spaceID     uint32
	currentPage uint32
	schema      *metadata.Table
	records     []Record
	recordIndex int
}

// Next 获取下一条记录
func (it *TablePageIterator) Next() (Record, error) {
	// 如果当前页面的记录还没遍历完，直接返回
	if it.recordIndex < len(it.records) {
		record := it.records[it.recordIndex]
		it.recordIndex++
		return record, nil
	}

	// 读取下一页
	page, err := it.adapter.ReadPage(it.ctx, it.spaceID, it.currentPage)
	if err != nil {
		return nil, err // EOF
	}

	// 解析页面记录
	records, err := it.adapter.ParseRecords(it.ctx, page, it.schema)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		// 尝试读取下一页
		it.currentPage++
		if it.currentPage > 1000 { // 防止无限循环
			return nil, nil // EOF
		}
		return it.Next()
	}

	// 保存记录并返回第一条
	it.records = records
	it.recordIndex = 1
	it.currentPage++

	return records[0], nil
}

// HasNext 是否还有更多记录
func (it *TablePageIterator) HasNext() bool {
	return it.recordIndex < len(it.records) || it.currentPage <= 1000
}
