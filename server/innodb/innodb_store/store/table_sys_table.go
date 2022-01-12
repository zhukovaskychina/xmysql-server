package store

import (
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/buffer_pool"
	"xmysql-server/server/innodb/model"
	"xmysql-server/server/innodb/schemas"
	"xmysql-server/server/innodb/tuple"
	"xmysql-server/server/mysql"
	"xmysql-server/util"
)

//INNODB_SYS_TABLES
type MemoryInnodbSysTable struct {
	//系统表元祖信息
	TableTuple tuple.TableRowTuple

	internalTuple tuple.TableRowTuple
	//数据字典，参数传入
	DictionarySys    *DictionarySys
	sysTableIterator basic.Iterator
}

func (m *MemoryInnodbSysTable) Cols() []*schemas.Column {
	panic("implement me")
}

func (m *MemoryInnodbSysTable) WritableCols() []*schemas.Column {
	panic("implement me")
}

func (m *MemoryInnodbSysTable) Meta() *model.TableInfo {

	ti := &model.TableInfo{
		Name: model.NewCIStr(common.INNODB_SYS_TABLES),
	}
	ti.Columns = make([]*model.ColumnInfo, m.TableTuple.GetColumnLength())
	for i := 0; i < m.TableTuple.GetColumnLength(); i++ {
		currentColumnInfo := m.TableTuple.GetColumnInfos(byte(i))

		fieldType := basic.NewFieldType(mysql.RefTypeValue[currentColumnInfo.FieldName])
		//fieldType.Flag=currentColumnInfo.NotNull
		fieldType.Flag = uint(util.ConvertBool2Byte(currentColumnInfo.NotNull))
		ti.Columns[i] = &model.ColumnInfo{
			Name:         model.NewCIStr(currentColumnInfo.FieldName),
			Offset:       i,
			DefaultValue: currentColumnInfo.FieldDefaultValue,
			FieldType:    *fieldType,
			Comment:      "",
		}
	}

	ti.Indices = make([]*model.IndexInfo, 0)
	indexColumns := make([]*model.IndexColumn, 0)
	indexColumns = append(indexColumns, &model.IndexColumn{Name: model.NewCIStr("NAME")})
	ti.Indices = append(ti.Indices, &model.IndexInfo{
		Name:    model.NewCIStr("IDX"),
		Table:   model.NewCIStr(common.INNODB_SYS_TABLES),
		Columns: indexColumns,
		Unique:  true,
		Primary: true,
		Comment: "",
		Tp:      model.IndexTypeBtree,
	})
	return ti
}

func (m *MemoryInnodbSysTable) GetIndex(indexName string) basic.Index {
	panic("implement me")
}

func (m MemoryInnodbSysTable) GetTableTupleMeta() tuple.TableTuple {
	panic("implement me")
}

func (m MemoryInnodbSysTable) GetAllColumns() []*tuple.FormColumnsWrapper {
	panic("implement me")
}

func (m MemoryInnodbSysTable) CheckFieldName(fieldName string) bool {
	formColWrappers, _ := m.TableTuple.GetColumnDescInfo(fieldName)
	if formColWrappers != nil {
		return true
	}
	return false
}

func (m MemoryInnodbSysTable) GetBtree(indexName string) basic.Tree {
	return m.DictionarySys.SysTable.BTree
}

func (m MemoryInnodbSysTable) GetTuple() tuple.TableRowTuple {
	panic("implement me")
}

func NewMemoryInnodbSysTable(sys *DictionarySys) schemas.Table {
	var memoryInnodbSysTable = new(MemoryInnodbSysTable)
	memoryInnodbSysTable.DictionarySys = sys
	memoryInnodbSysTable.TableTuple = NewSysTableTuple()
	memoryInnodbSysTable.internalTuple = newSysTableInternalTupleWithParams()
	sysTableIterator, _ := sys.SysTable.BTree.Iterate()
	memoryInnodbSysTable.sysTableIterator = sysTableIterator

	return memoryInnodbSysTable
}

func (m MemoryInnodbSysTable) TableName() string {
	return common.INNODB_SYS_TABLES
}

func (m MemoryInnodbSysTable) TableId() uint64 {
	return 1
}

func (m MemoryInnodbSysTable) SpaceId() uint32 {
	return 0
}

func (m MemoryInnodbSysTable) ColNums() int {
	return m.TableTuple.GetColumnLength()
}

func (m MemoryInnodbSysTable) RowIter() (basic.RowIterator, error) {
	return NewMemoryIterator(m.sysTableIterator), nil
}

/****###############

#####################
*/

type MemoryInnodbSysColumns struct {
	//系统表元祖信息
	TableTuple tuple.TableRowTuple
	//数据字典，参数传入
	DictionarySys    *DictionarySys
	sysTableIterator basic.Iterator
}

func (m MemoryInnodbSysColumns) Cols() []*schemas.Column {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) WritableCols() []*schemas.Column {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) Meta() *model.TableInfo {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) GetIndex(indexName string) basic.Index {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) GetTableTupleMeta() tuple.TableTuple {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) GetAllColumns() []*tuple.FormColumnsWrapper {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) CheckFieldName(fieldName string) bool {
	panic("implement me")
}

func (m MemoryInnodbSysColumns) GetBtree(indexName string) basic.Tree {
	return m.DictionarySys.SysColumns.BTree
}

func (m MemoryInnodbSysColumns) GetTuple() tuple.TableRowTuple {
	panic("implement me")
}

func NewMemoryInnodbSysColumns(sys *DictionarySys) schemas.Table {
	var memoryInnodbSysTable = new(MemoryInnodbSysColumns)
	memoryInnodbSysTable.DictionarySys = sys
	memoryInnodbSysTable.TableTuple = NewSysTableTuple()
	sysTableIterator, _ := sys.SysColumns.BTree.Iterate()
	memoryInnodbSysTable.sysTableIterator = sysTableIterator
	return memoryInnodbSysTable
}

func (m MemoryInnodbSysColumns) TableName() string {
	return common.INNODB_SYS_COLUMNS
}

func (m MemoryInnodbSysColumns) TableId() uint64 {
	return 1
}

func (m MemoryInnodbSysColumns) SpaceId() uint32 {
	return 0
}

func (m MemoryInnodbSysColumns) ColNums() int {
	return m.TableTuple.GetColumnLength()
}

func (m MemoryInnodbSysColumns) RowIter() (basic.RowIterator, error) {
	return NewMemoryIterator(m.sysTableIterator), nil
}

//根据Name做主键索引
//spaceId 由数据字典管理
type MemoryInnodbTableSpaces struct {
	TableTuple    tuple.TableRowTuple
	internalTuple tuple.TableRowTuple
	primaryTree   basic.Tree
}

func (m MemoryInnodbTableSpaces) Cols() []*schemas.Column {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) WritableCols() []*schemas.Column {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) Meta() *model.TableInfo {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) GetIndex(indexName string) basic.Index {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) GetTableTupleMeta() tuple.TableTuple {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) GetAllColumns() []*tuple.FormColumnsWrapper {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) CheckFieldName(fieldName string) bool {
	panic("implement me")
}

func NewMemoryInnodbTableSpaces(rootPage uint32, indexName string, pool *buffer_pool.BufferPool) schemas.Table {

	var memoryInnodbDataFiles = new(MemoryInnodbTableSpaces)
	memoryInnodbDataFiles.TableTuple = NewSysDataFilesTuple()
	memoryInnodbDataFiles.internalTuple = newSysDataFileInternalTuple()
	bufferBlock := pool.GetPageBlock(0, rootPage)
	spaceRootIndexBytes := *bufferBlock.Frame
	//获取Index segBtr
	segInternalBytes := spaceRootIndexBytes[38+46 : 38+56]
	segLeafBytes := spaceRootIndexBytes[38+46 : 38+56]

	internalSegs := NewInternalSegmentWithTupleAndBufferPool(0, util.ReadUB4Byte2UInt32(segInternalBytes[4:8]),
		util.ReadUB2Byte2Int(segInternalBytes[8:]), indexName, memoryInnodbDataFiles.internalTuple, pool)

	dataSegs := NewDataSegmentWithTupleAndBufferPool(0, util.ReadUB4Byte2UInt32(segLeafBytes[4:8]),
		util.ReadUB2Byte2Int(segLeafBytes[8:]), indexName, memoryInnodbDataFiles.TableTuple, pool)

	bufferBlock = pool.GetPageBlock(0, rootPage)
	currentBytes := *bufferBlock.Frame
	var rootIndex *Index
	if GetRootPageLeafOrInternal(currentBytes) == common.PAGE_LEAF {
		rootIndex = NewPageIndexByLoadBytesWithTuple(currentBytes, memoryInnodbDataFiles.TableTuple).(*Index)
	} else {
		rootIndex = NewPageIndexByLoadBytesWithTuple(currentBytes, memoryInnodbDataFiles.internalTuple).(*Index)
	}
	memoryInnodbDataFiles.primaryTree = NewBtreeWithBufferPool(0, rootPage, indexName, internalSegs, dataSegs,
		rootIndex, pool, newSysTableSpaceInternalTuple(), memoryInnodbDataFiles.TableTuple)
	return memoryInnodbDataFiles
}

func (m MemoryInnodbTableSpaces) GetBtree(indexName string) basic.Tree {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) GetTuple() tuple.TableRowTuple {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) TableName() string {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) TableId() uint64 {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) SpaceId() uint32 {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) ColNums() int {
	panic("implement me")
}

func (m MemoryInnodbTableSpaces) RowIter() (basic.RowIterator, error) {
	panic("implement me")
}

//space spaceId作为主键
func NewMemoryInnodbDataFiles(rootPage uint32, indexName string, pool *buffer_pool.BufferPool) schemas.Table {

	var memoryInnodbDataFiles = new(MemoryInnodbDataFiles)
	memoryInnodbDataFiles.TableTuple = NewSysDataFilesTuple()
	memoryInnodbDataFiles.internalTuple = newSysDataFileInternalTuple()
	bufferBlock := pool.GetPageBlock(0, rootPage)
	spaceRootIndexBytes := *bufferBlock.Frame
	//获取Index segBtr
	segInternalBytes := spaceRootIndexBytes[38+46 : 38+56]
	segLeafBytes := spaceRootIndexBytes[38+46 : 38+56]

	internalSegs := NewInternalSegmentWithTupleAndBufferPool(0, util.ReadUB4Byte2UInt32(segInternalBytes[4:8]),
		util.ReadUB2Byte2Int(segInternalBytes[8:]), indexName, memoryInnodbDataFiles.internalTuple, pool)

	dataSegs := NewDataSegmentWithTupleAndBufferPool(0, util.ReadUB4Byte2UInt32(segLeafBytes[4:8]),
		util.ReadUB2Byte2Int(segLeafBytes[8:]), indexName, memoryInnodbDataFiles.TableTuple, pool)

	bufferBlock = pool.GetPageBlock(0, rootPage)
	currentBytes := *bufferBlock.Frame
	var rootIndex *Index
	if GetRootPageLeafOrInternal(currentBytes) == common.PAGE_LEAF {
		rootIndex = NewPageIndexByLoadBytesWithTuple(currentBytes, memoryInnodbDataFiles.TableTuple).(*Index)
	} else {
		rootIndex = NewPageIndexByLoadBytesWithTuple(currentBytes, memoryInnodbDataFiles.internalTuple).(*Index)
	}
	memoryInnodbDataFiles.primaryTree = NewBtreeWithBufferPool(0, rootPage, indexName, internalSegs, dataSegs,
		rootIndex, pool, newSysTableSpaceInternalTuple(), memoryInnodbDataFiles.TableTuple)
	return memoryInnodbDataFiles
}

type MemoryInnodbDataFiles struct {
	TableTuple    tuple.TableRowTuple
	internalTuple tuple.TableRowTuple
	primaryTree   basic.Tree
}

func (m MemoryInnodbDataFiles) Cols() []*schemas.Column {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) WritableCols() []*schemas.Column {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) Meta() *model.TableInfo {
	return &model.TableInfo{
		Name:    model.CIStr{},
		Columns: nil,
		Indices: nil,
		Comment: "",
	}
}

func (m MemoryInnodbDataFiles) GetIndex(indexName string) basic.Index {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) GetTableTupleMeta() tuple.TableTuple {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) GetAllColumns() []*tuple.FormColumnsWrapper {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) CheckFieldName(fieldName string) bool {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) GetTuple() tuple.TableRowTuple {
	return m.TableTuple
}

func (m MemoryInnodbDataFiles) GetBtree(indexName string) basic.Tree {
	panic("implement me")
}

func (m MemoryInnodbDataFiles) TableName() string {
	return common.INNODB_SYS_DATAFILES
}

func (m MemoryInnodbDataFiles) TableId() uint64 {
	return 14
}

func (m MemoryInnodbDataFiles) SpaceId() uint32 {
	return 0
}

func (m MemoryInnodbDataFiles) ColNums() int {
	return m.TableTuple.GetColumnLength()
}

func (m MemoryInnodbDataFiles) RowIter() (basic.RowIterator, error) {
	panic("implement me")
}

type MemoryInnodbFields struct {
	schemas.Table
	TableTuple tuple.TableRowTuple
	//数据字典，参数传入
	DictionarySys    *DictionarySys
	sysTableIterator basic.Iterator
}

func NewMemoryInnodbFields(sys *DictionarySys) schemas.Table {
	var memoryInnodbSysTable = new(MemoryInnodbFields)
	memoryInnodbSysTable.DictionarySys = sys
	memoryInnodbSysTable.TableTuple = NewSysTableTuple()
	sysTableIterator, _ := sys.SysColumns.BTree.Iterate()
	memoryInnodbSysTable.sysTableIterator = sysTableIterator
	return memoryInnodbSysTable
}

func (m MemoryInnodbFields) TableName() string {
	return common.INNODB_SYS_FIELDS
}

func (m MemoryInnodbFields) TableId() uint64 {
	panic("implement me")
}

func (m MemoryInnodbFields) SpaceId() uint32 {
	return 0
}

func (m MemoryInnodbFields) ColNums() int {
	panic("implement me")
}

func (m MemoryInnodbFields) GetTuple() tuple.TableRowTuple {
	return m.TableTuple
}

func (m MemoryInnodbFields) GetBtree(indexName string) basic.Tree {
	return nil
}

type MemoryInnodbSysIndex struct {
	schemas.Table
	TableTuple    tuple.TableRowTuple
	InternalTuple tuple.TableRowTuple
	//数据字典，参数传入
	DictionarySys    *DictionarySys
	sysTableIterator basic.Iterator
}

func (m MemoryInnodbSysIndex) TableName() string {
	return common.INNODB_SYS_INDEXES
}

func (m MemoryInnodbSysIndex) TableId() uint64 {
	return 0
}

func (m MemoryInnodbSysIndex) SpaceId() uint32 {
	return 0
}

func (m MemoryInnodbSysIndex) ColNums() int {
	panic("implement me")
}

func (m MemoryInnodbSysIndex) RowIter() (basic.RowIterator, error) {
	panic("implement me")
}

func (m MemoryInnodbSysIndex) GetTuple() tuple.TableRowTuple {
	return m.TableTuple
}

func (m MemoryInnodbSysIndex) GetBtree(indexName string) basic.Tree {
	return m.DictionarySys.SysIndex.BTree
}

func NewMemorySysIndexTable(sys *DictionarySys) schemas.Table {
	var memoryInnodbSysTable = new(MemoryInnodbSysIndex)
	memoryInnodbSysTable.DictionarySys = sys
	memoryInnodbSysTable.TableTuple = NewSysTableTuple()
	sysTableIterator, _ := sys.SysColumns.BTree.Iterate()
	memoryInnodbSysTable.sysTableIterator = sysTableIterator
	return memoryInnodbSysTable
}

func GetRootPageLeafOrInternal(currentBytes []byte) string {
	var leafOrInternal string
	filePageTypeBytes := currentBytes[24:26]
	filePageType := util.ReadUB2Byte2Int(filePageTypeBytes)
	if filePageType == common.FILE_PAGE_INDEX {
		infimumSupremum := currentBytes[38+56 : 38+56+26]
		flags := util.ConvertByte2BitsString(infimumSupremum[0])[3]
		if flags == common.PAGE_LEAF {
			leafOrInternal = common.PAGE_LEAF
		} else {
			leafOrInternal = common.PAGE_INTERNAL
		}
	}
	return leafOrInternal
}
