package manager

import (
	"context"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// EnhancedBTreeAdapter 适配器，让 EnhancedBTreeManager 兼容 basic.BPlusTreeManager 接口
type EnhancedBTreeAdapter struct {
	enhancedManager *EnhancedBTreeManager
	defaultIndexID  uint64 // 默认索引ID，用于兼容旧接口
	spaceID         uint32 // 表空间ID
	rootPageNo      uint32 // 根页号
}

// NewEnhancedBTreeAdapter 创建增强版B+树适配器
func NewEnhancedBTreeAdapter(storageManager *StorageManager, config *BTreeConfig) *EnhancedBTreeAdapter {
	enhancedManager := NewEnhancedBTreeManager(storageManager, config)

	return &EnhancedBTreeAdapter{
		enhancedManager: enhancedManager,
		defaultIndexID:  1, // 默认使用索引ID 1
	}
}

// Init 初始化，从rootPage开始建立树结构
func (adapter *EnhancedBTreeAdapter) Init(ctx context.Context, spaceId uint32, rootPage uint32) error {
	adapter.spaceID = spaceId
	adapter.rootPageNo = rootPage

	// 创建默认索引元信息
	metadata := &IndexMetadata{
		IndexID:    adapter.defaultIndexID,
		TableID:    1, // 默认表ID
		SpaceID:    spaceId,
		RootPageNo: rootPage,
		IndexName:  "PRIMARY",
		IndexType:  IndexTypePrimary,
		IndexState: EnhancedIndexStateBuilding,
		Columns: []IndexColumn{
			{
				ColumnName: "id",
				ColumnPos:  0,
				KeyLength:  8,
				IsDesc:     false,
			},
		},
		KeyLength: 8,
	}

	// 检查索引是否已存在
	existingIndex, err := adapter.enhancedManager.GetIndex(adapter.defaultIndexID)
	if err == nil && existingIndex != nil {
		// 索引已存在，直接返回
		return nil
	}

	// 创建新索引
	_, err = adapter.enhancedManager.CreateIndex(ctx, metadata)
	if err != nil {
		return fmt.Errorf("failed to create default index: %v", err)
	}

	return nil
}

// GetAllLeafPages 遍历所有叶子页号
func (adapter *EnhancedBTreeAdapter) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	index, err := adapter.enhancedManager.GetIndex(adapter.defaultIndexID)
	if err != nil {
		return nil, fmt.Errorf("failed to get index: %v", err)
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return nil, fmt.Errorf("invalid index type")
	}

	return enhancedIndex.GetAllLeafPages(ctx)
}

// Search 查找一个主键，返回所在页号和记录槽位
func (adapter *EnhancedBTreeAdapter) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	// 将 interface{} 类型的 key 转换为 []byte
	keyBytes, err := adapter.convertKeyToBytes(key)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to convert key: %v", err)
	}

	record, err := adapter.enhancedManager.Search(ctx, adapter.defaultIndexID, keyBytes)
	if err != nil {
		return 0, 0, err
	}

	return record.PageNo, int(record.SlotNo), nil
}

// Insert 插入一个键值对
func (adapter *EnhancedBTreeAdapter) Insert(ctx context.Context, key interface{}, value []byte) error {
	// 将 interface{} 类型的 key 转换为 []byte
	keyBytes, err := adapter.convertKeyToBytes(key)
	if err != nil {
		return fmt.Errorf("failed to convert key: %v", err)
	}

	return adapter.enhancedManager.Insert(ctx, adapter.defaultIndexID, keyBytes, value)
}

// RangeSearch 范围查询
func (adapter *EnhancedBTreeAdapter) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	// 将 interface{} 类型的 key 转换为 []byte
	startKeyBytes, err := adapter.convertKeyToBytes(startKey)
	if err != nil {
		return nil, fmt.Errorf("failed to convert start key: %v", err)
	}

	endKeyBytes, err := adapter.convertKeyToBytes(endKey)
	if err != nil {
		return nil, fmt.Errorf("failed to convert end key: %v", err)
	}

	records, err := adapter.enhancedManager.RangeSearch(ctx, adapter.defaultIndexID, startKeyBytes, endKeyBytes)
	if err != nil {
		return nil, err
	}

	// 将 IndexRecord 转换为 basic.Row
	var rows []basic.Row
	for _, record := range records {
		row := &IndexRecordRowAdapter{
			record: &record,
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// GetFirstLeafPage 获取第一个叶子节点
func (adapter *EnhancedBTreeAdapter) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	index, err := adapter.enhancedManager.GetIndex(adapter.defaultIndexID)
	if err != nil {
		return 0, fmt.Errorf("failed to get index: %v", err)
	}

	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return 0, fmt.Errorf("invalid index type")
	}

	return enhancedIndex.GetFirstLeafPage(ctx)
}

// convertKeyToBytes 将 interface{} 类型的 key 转换为 []byte
func (adapter *EnhancedBTreeAdapter) convertKeyToBytes(key interface{}) ([]byte, error) {
	switch v := key.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case int:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int32:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int64:
		return []byte(fmt.Sprintf("%d", v)), nil
	case uint32:
		return []byte(fmt.Sprintf("%d", v)), nil
	case uint64:
		return []byte(fmt.Sprintf("%d", v)), nil
	case float32:
		return []byte(fmt.Sprintf("%f", v)), nil
	case float64:
		return []byte(fmt.Sprintf("%f", v)), nil
	default:
		return []byte(fmt.Sprintf("%v", v)), nil
	}
}

// IndexRecordRowAdapter 将 IndexRecord 适配为 basic.Row 接口
type IndexRecordRowAdapter struct {
	record *IndexRecord
}

func (r *IndexRecordRowAdapter) Less(than basic.Row) bool {
	// 简化实现，实际应该根据具体的比较逻辑
	return false
}

func (r *IndexRecordRowAdapter) ToByte() []byte {
	if r.record != nil {
		return r.record.Value
	}
	return nil
}

func (r *IndexRecordRowAdapter) IsInfimumRow() bool {
	return false
}

func (r *IndexRecordRowAdapter) IsSupremumRow() bool {
	return false
}

func (r *IndexRecordRowAdapter) GetPageNumber() uint32 {
	if r.record != nil {
		return r.record.PageNo
	}
	return 0
}

func (r *IndexRecordRowAdapter) WriteWithNull(content []byte) {
	// TODO: 实现
}

func (r *IndexRecordRowAdapter) WriteBytesWithNullWithsPos(content []byte, index byte) {
	// TODO: 实现
}

func (r *IndexRecordRowAdapter) GetRowLength() uint16 {
	if r.record != nil {
		return uint16(len(r.record.Value))
	}
	return 0
}

func (r *IndexRecordRowAdapter) GetHeaderLength() uint16 {
	return 0 // TODO: 实现
}

func (r *IndexRecordRowAdapter) GetPrimaryKey() basic.Value {
	return nil // TODO: 实现
}

func (r *IndexRecordRowAdapter) GetFieldLength() int {
	return 1 // 简化实现
}

func (r *IndexRecordRowAdapter) ReadValueByIndex(index int) basic.Value {
	return nil // TODO: 实现
}

func (r *IndexRecordRowAdapter) SetNOwned(cnt byte) {
	// TODO: 实现
}

func (r *IndexRecordRowAdapter) GetNOwned() byte {
	return 0
}

func (r *IndexRecordRowAdapter) GetNextRowOffset() uint16 {
	return 0
}

func (r *IndexRecordRowAdapter) SetNextRowOffset(offset uint16) {
	// TODO: 实现
}

func (r *IndexRecordRowAdapter) GetHeapNo() uint16 {
	return 0
}

func (r *IndexRecordRowAdapter) SetHeapNo(heapNo uint16) {
	// TODO: 实现
}

func (r *IndexRecordRowAdapter) SetTransactionId(trxId uint64) {
	// TODO: 实现
}

func (r *IndexRecordRowAdapter) GetValueByColName(colName string) basic.Value {
	return nil // TODO: 实现
}

func (r *IndexRecordRowAdapter) ToString() string {
	if r.record != nil {
		return string(r.record.Value)
	}
	return ""
}

// GetEnhancedManager 获取底层的增强版B+树管理器
func (adapter *EnhancedBTreeAdapter) GetEnhancedManager() *EnhancedBTreeManager {
	return adapter.enhancedManager
}

// Close 关闭适配器
func (adapter *EnhancedBTreeAdapter) Close() error {
	return adapter.enhancedManager.Close()
}
