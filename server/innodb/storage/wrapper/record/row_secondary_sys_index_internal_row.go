package record

import (
	"encoding/binary"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/metadata"
)

// SystemSecondaryIndexInternalRow 系统辅助索引内部节点行记录
// 用于系统表的辅助索引B+树内部节点
type SystemSecondaryIndexInternalRow struct {
	*SecondaryIndexInternalRow // 继承普通辅助索引内部节点
	SystemType                 SystemRecordType
	SystemData                 []byte // 系统特定数据
}

// NewSystemSecondaryIndexInternalRow 创建系统辅助索引内部节点行记录
func NewSystemSecondaryIndexInternalRow(meta metadata.TableRowTuple, indexKey basic.Value, pagePointer uint32, sysType SystemRecordType) basic.Row {
	baseRow := NewSecondaryIndexInternalRow(meta, indexKey, pagePointer)
	return &SystemSecondaryIndexInternalRow{
		SecondaryIndexInternalRow: baseRow.(*SecondaryIndexInternalRow),
		SystemType:                sysType,
		SystemData:                make([]byte, 0),
	}
}

// NewSystemSecondaryIndexInternalRowWithContent 从内容创建系统辅助索引内部节点行记录
func NewSystemSecondaryIndexInternalRowWithContent(content []byte, meta metadata.TableRowTuple, sysType SystemRecordType) basic.Row {
	baseRow := NewSecondaryIndexInternalRowWithContent(content, meta)
	return &SystemSecondaryIndexInternalRow{
		SecondaryIndexInternalRow: baseRow.(*SecondaryIndexInternalRow),
		SystemType:                sysType,
		SystemData:                make([]byte, 0),
	}
}

// 实现basic.Row接口的特化方法
func (r *SystemSecondaryIndexInternalRow) Less(than basic.Row) bool {
	// 系统辅助索引内部节点可能有特殊的排序逻辑
	if otherSys, ok := than.(*SystemSecondaryIndexInternalRow); ok {
		// 先按系统类型排序
		if r.SystemType != otherSys.SystemType {
			return r.SystemType < otherSys.SystemType
		}
		// 同类型系统记录按父类逻辑排序
		return r.SecondaryIndexInternalRow.Less(otherSys.SecondaryIndexInternalRow)
	}
	// 与非系统记录比较时使用父类逻辑
	return r.SecondaryIndexInternalRow.Less(than)
}

func (r *SystemSecondaryIndexInternalRow) ToByte() []byte {
	baseBytes := r.SecondaryIndexInternalRow.ToByte()
	// 添加系统特定的序列化逻辑
	result := make([]byte, 0, len(baseBytes)+len(r.SystemData)+1)
	result = append(result, byte(r.SystemType))
	result = append(result, baseBytes...)
	result = append(result, r.SystemData...)
	return result
}

func (r *SystemSecondaryIndexInternalRow) ToString() string {
	return "SystemSecondaryIndexInternalRow{Type: " + string(rune(r.SystemType)) + "}"
}

// 系统记录特有方法
func (r *SystemSecondaryIndexInternalRow) GetSystemType() SystemRecordType {
	return r.SystemType
}

func (r *SystemSecondaryIndexInternalRow) SetSystemType(sysType SystemRecordType) {
	r.SystemType = sysType
}

func (r *SystemSecondaryIndexInternalRow) GetSystemData() []byte {
	return r.SystemData
}

func (r *SystemSecondaryIndexInternalRow) SetSystemData(data []byte) {
	r.SystemData = data
}

// 系统记录分类方法
func (r *SystemSecondaryIndexInternalRow) IsDataDictionaryRecord() bool {
	return r.SystemType == SystemRecordTypeDataDictionary
}

func (r *SystemSecondaryIndexInternalRow) IsTablespaceRecord() bool {
	return r.SystemType == SystemRecordTypeTablespace
}

func (r *SystemSecondaryIndexInternalRow) IsRollbackSegmentRecord() bool {
	return r.SystemType == SystemRecordTypeRollbackSegment
}

// 系统辅助索引内部节点特定的数据访问方法
func (r *SystemSecondaryIndexInternalRow) GetChildPagePointer() uint32 {
	// 获取子页面指针
	if r.SecondaryIndexInternalRow != nil {
		return r.SecondaryIndexInternalRow.GetChildPagePointer()
	}
	return 0
}

func (r *SystemSecondaryIndexInternalRow) SetChildPagePointer(pageNum uint32) {
	// 设置子页面指针
	if r.SecondaryIndexInternalRow != nil {
		r.SecondaryIndexInternalRow.SetChildPagePointer(pageNum)
	}
}

func (r *SystemSecondaryIndexInternalRow) GetIndexKeyPrefix() string {
	// 对于系统辅助索引内部节点，获取索引键前缀
	if r.SecondaryIndexInternalRow != nil && r.SecondaryIndexInternalRow.IndexKey != nil {
		return r.SecondaryIndexInternalRow.IndexKey.ToString()
	}
	return ""
}

func (r *SystemSecondaryIndexInternalRow) SetIndexKeyPrefix(prefix string) {
	// 设置索引键前缀
	if r.SecondaryIndexInternalRow != nil {
		r.SecondaryIndexInternalRow.SetIndexKey(basic.NewStringValue(prefix))
	}
}

// 系统特定的内部节点数据管理方法
func (r *SystemSecondaryIndexInternalRow) GetTableNamePrefix() string {
	// 对于数据字典辅助索引内部节点（按表名），获取表名前缀
	if r.IsDataDictionaryRecord() {
		return r.GetIndexKeyPrefix()
	}
	return ""
}

func (r *SystemSecondaryIndexInternalRow) SetTableNamePrefix(prefix string) {
	// 设置表名前缀
	if r.IsDataDictionaryRecord() {
		r.SetIndexKeyPrefix(prefix)
	}
}

func (r *SystemSecondaryIndexInternalRow) GetTablespaceFileNamePrefix() string {
	// 对于表空间辅助索引内部节点（按文件名），获取文件名前缀
	if r.IsTablespaceRecord() {
		return r.GetIndexKeyPrefix()
	}
	return ""
}

func (r *SystemSecondaryIndexInternalRow) SetTablespaceFileNamePrefix(prefix string) {
	// 设置表空间文件名前缀
	if r.IsTablespaceRecord() {
		r.SetIndexKeyPrefix(prefix)
	}
}

func (r *SystemSecondaryIndexInternalRow) GetRollbackSegmentIdRange() (uint16, uint16) {
	// 对于回滚段辅助索引内部节点，获取回滚段ID范围
	if r.IsRollbackSegmentRecord() && len(r.SystemData) >= 4 {
		minId := binary.LittleEndian.Uint16(r.SystemData[0:2])
		maxId := binary.LittleEndian.Uint16(r.SystemData[2:4])
		return minId, maxId
	}
	return 0, 0
}

func (r *SystemSecondaryIndexInternalRow) SetRollbackSegmentIdRange(minId, maxId uint16) {
	// 设置回滚段ID范围
	if r.IsRollbackSegmentRecord() {
		if len(r.SystemData) < 4 {
			r.SystemData = make([]byte, 4)
		}
		binary.LittleEndian.PutUint16(r.SystemData[0:2], minId)
		binary.LittleEndian.PutUint16(r.SystemData[2:4], maxId)
	}
}
