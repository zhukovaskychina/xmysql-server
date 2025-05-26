package record

import (
	"bytes"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// IndexRecord 索引记录
type IndexRecord struct {
	*BaseRecord
	indexKeys [][]byte
}

// NewIndexRecord 创建索引记录
func NewIndexRecord(id uint64, data []byte, header basic.FieldDataHeader, value basic.FieldDataValue, frmMeta metadata.TableRowTuple, indexKeys [][]byte) *IndexRecord {
	return &IndexRecord{
		BaseRecord: NewBaseRecord(id, data, header, value, frmMeta),
		indexKeys:  indexKeys,
	}
}

// GetIndexKeys 获取索引键值
func (r *IndexRecord) GetIndexKeys() [][]byte {
	return r.indexKeys
}

// SetIndexKeys 设置索引键值
func (r *IndexRecord) SetIndexKeys(keys [][]byte) {
	r.indexKeys = keys
}

// Less 重写Less方法，支持多列索引排序
func (r *IndexRecord) Less(than basic.Row) bool {
	otherIndex, ok := than.(*IndexRecord)
	if !ok {
		return r.BaseRecord.Less(than)
	}

	// 比较所有索引键
	for i, key := range r.indexKeys {
		if i >= len(otherIndex.indexKeys) {
			return false
		}
		cmp := bytes.Compare(key, otherIndex.indexKeys[i])
		if cmp != 0 {
			return cmp < 0
		}
	}
	return len(r.indexKeys) < len(otherIndex.indexKeys)
}
