package types

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// IIndexPage defines the interface for index pages
type IIndexPage interface {
	GetIndexID() uint64
	GetSegLeaf() []byte
	GetSegTop() []byte
	InsertKey(key Key, pageNo uint32, rowID uint64) error
	DeleteKey(key Key) error
	FindKey(key Key) (*IndexEntry, error)
	GetKeys() []Key
	GetEntries() []*IndexEntry
	SetParent(parentPage uint32)
	SetLeftRight(leftPage, rightPage uint32)
	IsLeaf() bool
	IsRoot() bool
	SetRoot(isRoot bool)
	GetLevel() uint16
}

// TableTupleMeta 表元数据
type TableTupleMeta struct {
	TableID     uint64
	DatabaseID  uint32
	TableName   string
	ColumnCount uint32
	Columns     []metadata.Column
}

// SysTableTuple 系统表元数据
type SysTableTuple struct {
	*TableTupleMeta
	SpaceID   uint32
	PageNo    uint32
	IndexID   uint64
	IndexName string
}
