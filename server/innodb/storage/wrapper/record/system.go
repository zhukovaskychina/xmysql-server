package record

import (
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/metadata"
)

// SystemRecordType 系统记录类型
type SystemRecordType uint8

const (
	SystemRecordTypeUnknown SystemRecordType = iota
	SystemRecordTypeDataDictionary
	SystemRecordTypeTablespace
	SystemRecordTypeRollbackSegment
)

// SystemRecord 系统记录
type SystemRecord struct {
	*BaseRecord
	systemType SystemRecordType
}

// NewSystemRecord 创建系统记录
func NewSystemRecord(id uint64, data []byte, header basic.FieldDataHeader, value basic.FieldDataValue, frmMeta metadata.TableRowTuple, sysType SystemRecordType) *SystemRecord {
	return &SystemRecord{
		BaseRecord: NewBaseRecord(id, data, header, value, frmMeta),
		systemType: sysType,
	}
}

// GetSystemType 获取系统记录类型
func (r *SystemRecord) GetSystemType() SystemRecordType {
	return r.systemType
}

// SetSystemType 设置系统记录类型
func (r *SystemRecord) SetSystemType(sysType SystemRecordType) {
	r.systemType = sysType
}

// Less 重写Less方法，系统记录按ID排序
func (r *SystemRecord) Less(than basic.Row) bool {
	otherSys, ok := than.(*SystemRecord)
	if !ok {
		return r.BaseRecord.Less(than)
	}
	return r.GetID() < otherSys.GetID()
}
