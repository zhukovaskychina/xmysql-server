package record

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// SystemSecondaryIndexLeafRow 系统辅助索引叶子节点行记录
// 用于系统表的辅助索引，如按表名索引的数据字典记录
type SystemSecondaryIndexLeafRow struct {
	*SecondaryIndexLeafRow // 继承普通辅助索引叶子节点
	SystemType             SystemRecordType
	SystemData             []byte // 系统特定数据
}

// NewSystemSecondaryIndexLeafRow 创建系统辅助索引叶子节点行记录
func NewSystemSecondaryIndexLeafRow(meta metadata.TableRowTuple, indexKeys []basic.Value, primaryKeys []basic.Value, sysType SystemRecordType) basic.Row {
	baseRow := NewSecondaryIndexLeafRow(meta, indexKeys, primaryKeys)
	return &SystemSecondaryIndexLeafRow{
		SecondaryIndexLeafRow: baseRow.(*SecondaryIndexLeafRow),
		SystemType:            sysType,
		SystemData:            make([]byte, 0),
	}
}

// NewSystemSecondaryIndexLeafRowWithContent 从内容创建系统辅助索引叶子节点行记录
func NewSystemSecondaryIndexLeafRowWithContent(content []byte, meta metadata.TableRowTuple, sysType SystemRecordType) basic.Row {
	baseRow := NewSecondaryIndexLeafRowWithContent(content, meta)
	return &SystemSecondaryIndexLeafRow{
		SecondaryIndexLeafRow: baseRow.(*SecondaryIndexLeafRow),
		SystemType:            sysType,
		SystemData:            make([]byte, 0),
	}
}

// 实现basic.Row接口的特化方法
func (r *SystemSecondaryIndexLeafRow) Less(than basic.Row) bool {
	// 系统辅助索引记录可能有特殊的排序逻辑
	if otherSys, ok := than.(*SystemSecondaryIndexLeafRow); ok {
		// 先按系统类型排序
		if r.SystemType != otherSys.SystemType {
			return r.SystemType < otherSys.SystemType
		}
		// 同类型系统记录按父类逻辑排序
		return r.SecondaryIndexLeafRow.Less(otherSys.SecondaryIndexLeafRow)
	}
	// 与非系统记录比较时使用父类逻辑
	return r.SecondaryIndexLeafRow.Less(than)
}

func (r *SystemSecondaryIndexLeafRow) ToByte() []byte {
	baseBytes := r.SecondaryIndexLeafRow.ToByte()
	// 添加系统特定的序列化逻辑
	result := make([]byte, 0, len(baseBytes)+len(r.SystemData)+1)
	result = append(result, byte(r.SystemType))
	result = append(result, baseBytes...)
	result = append(result, r.SystemData...)
	return result
}

func (r *SystemSecondaryIndexLeafRow) ToString() string {
	return "SystemSecondaryIndexLeafRow{Type: " + string(rune(r.SystemType)) + "}"
}

// 系统记录特有方法
func (r *SystemSecondaryIndexLeafRow) GetSystemType() SystemRecordType {
	return r.SystemType
}

func (r *SystemSecondaryIndexLeafRow) SetSystemType(sysType SystemRecordType) {
	r.SystemType = sysType
}

func (r *SystemSecondaryIndexLeafRow) GetSystemData() []byte {
	return r.SystemData
}

func (r *SystemSecondaryIndexLeafRow) SetSystemData(data []byte) {
	r.SystemData = data
}

// 系统记录分类方法
func (r *SystemSecondaryIndexLeafRow) IsDataDictionaryRecord() bool {
	return r.SystemType == SystemRecordTypeDataDictionary
}

func (r *SystemSecondaryIndexLeafRow) IsTablespaceRecord() bool {
	return r.SystemType == SystemRecordTypeTablespace
}

func (r *SystemSecondaryIndexLeafRow) IsRollbackSegmentRecord() bool {
	return r.SystemType == SystemRecordTypeRollbackSegment
}

// 系统辅助索引特定的数据访问方法
func (r *SystemSecondaryIndexLeafRow) GetTableName() string {
	// 对于数据字典辅助索引（按表名），获取表名
	if r.IsDataDictionaryRecord() && len(r.SystemData) > 0 {
		// 假设表名以null结尾的字符串存储
		for i, b := range r.SystemData {
			if b == 0 {
				return string(r.SystemData[:i])
			}
		}
		return string(r.SystemData)
	}
	return ""
}

func (r *SystemSecondaryIndexLeafRow) SetTableName(tableName string) {
	// 设置表名
	if r.IsDataDictionaryRecord() {
		r.SystemData = append([]byte(tableName), 0) // null结尾
	}
}

func (r *SystemSecondaryIndexLeafRow) GetTablespaceFileName() string {
	// 对于表空间辅助索引（按文件名），获取文件名
	if r.IsTablespaceRecord() && len(r.SystemData) > 0 {
		for i, b := range r.SystemData {
			if b == 0 {
				return string(r.SystemData[:i])
			}
		}
		return string(r.SystemData)
	}
	return ""
}

func (r *SystemSecondaryIndexLeafRow) SetTablespaceFileName(fileName string) {
	// 设置表空间文件名
	if r.IsTablespaceRecord() {
		r.SystemData = append([]byte(fileName), 0) // null结尾
	}
}

func (r *SystemSecondaryIndexLeafRow) GetRollbackSegmentStatus() uint8 {
	// 对于回滚段辅助索引，获取状态
	if r.IsRollbackSegmentRecord() && len(r.SystemData) >= 1 {
		return r.SystemData[0]
	}
	return 0
}

func (r *SystemSecondaryIndexLeafRow) SetRollbackSegmentStatus(status uint8) {
	// 设置回滚段状态
	if r.IsRollbackSegmentRecord() {
		if len(r.SystemData) == 0 {
			r.SystemData = make([]byte, 1)
		}
		r.SystemData[0] = status
	}
}
