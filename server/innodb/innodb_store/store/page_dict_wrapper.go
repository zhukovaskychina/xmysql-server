package store

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/segs"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type DataDictWrapper struct {
	IPageWrapper
	DataHrdPage   *pages.DataDictionaryHeaderSysPage
	SegmentHeader *segs.SegmentHeader
	MaxRowId      uint64 //没有主键的，也不运训null的unique主键的，则分配一个rowid

	MaxTableId uint64 //tableId

	MaxIndexId uint64 //索引ID

	MaxSpaceId uint32 //空间ID

	SysTableClusterRoot   uint32 //SYS_TABLES_CLUSTER 根页面
	SysTableIdsIndexRoot  uint32 //SYS_TABLES_IDS 二级索引的根页面号
	SysColumnsIndexRoot   uint32 //SYS_COLUMNS 根页面号
	SysIndexesClusterRoot uint32 //SYS_INDEXS
	SysFieldsClusterRoot  uint32 //SYS_FIELDS
}

func NewDataDictWrapper() IPageWrapper {
	var fspBinary = pages.NewDataDictHeaderPage()

	return &DataDictWrapper{
		DataHrdPage: fspBinary,
	}
}

func NewDataDictWrapperByContent(content []byte) IPageWrapper {

	var dataDictWrapper = new(DataDictWrapper)
	dataDictWrapper.DataHrdPage = pages.ParseDataDictHrdPage(content)
	dataDictWrapper.MaxRowId = dataDictWrapper.DataHrdPage.GetMaxRowId()

	return dataDictWrapper

}

func (d *DataDictWrapper) GetMaxTableId() uint64 {
	d.MaxTableId++
	return d.MaxTableId
}
func (d *DataDictWrapper) GetMaxIndexId() uint64 {
	d.MaxIndexId++
	return d.MaxIndexId
}
func (d *DataDictWrapper) GetMaxSpaceId() uint32 {
	d.MaxSpaceId++
	return d.MaxSpaceId
}

func (d *DataDictWrapper) SetDataDictSegments() {
	d.SegmentHeader = segs.NewSegmentHeader(0, 2, 50)
	d.DataHrdPage.SegmentHeader = d.SegmentHeader.GetBytes()
}

//
func NewDataDictWrapperByBytes(content []byte) *DataDictWrapper {
	return &DataDictWrapper{
		DataHrdPage: pages.ParseDataDictHrdPage(content),
	}
}

func (d *DataDictWrapper) processDataDicts() {
	d.MaxRowId = util.ReadUB8Byte2Long(d.DataHrdPage.DataDictHeader.MaxRowId)
	d.MaxTableId = util.ReadUB8Byte2Long(d.DataHrdPage.DataDictHeader.MaxTableId)
	d.MaxIndexId = util.ReadUB8Byte2Long(d.DataHrdPage.DataDictHeader.MaxIndexId)
	d.MaxSpaceId = util.ReadUB4Byte2UInt32(d.DataHrdPage.DataDictHeader.MaxSpaceId)

	d.SysTableClusterRoot = util.ReadUB4Byte2UInt32(d.DataHrdPage.DataDictHeader.SysTableRootPage)
	d.SysTableIdsIndexRoot = util.ReadUB4Byte2UInt32(d.DataHrdPage.DataDictHeader.SysTablesIDSRootPage)
	d.SysColumnsIndexRoot = util.ReadUB4Byte2UInt32(d.DataHrdPage.DataDictHeader.SysColumnsRootPage)
	d.SysIndexesClusterRoot = util.ReadUB4Byte2UInt32(d.DataHrdPage.DataDictHeader.SysIndexesRootPage)
	d.SysFieldsClusterRoot = util.ReadUB4Byte2UInt32(d.DataHrdPage.DataDictHeader.SysFieldsRootPage)
}

func (d *DataDictWrapper) GetSerializeBytes() []byte {

	return d.DataHrdPage.GetSerializeBytes()
}

/**
定义数据字典结构
**/

type DictionarySys struct {
	currentRowId uint64

	currentTableId uint64 //tableId

	currentIndexId uint64 //索引ID

	currentSpaceId uint32 //空间ID

	SysTable *DictTable

	SysColumns *DictTable

	SysIndex *DictTable

	SysFields *DictTable

	DataDict *DataDictWrapper //7号页面

	pool *buffer_pool.BufferPool

	sysLeafTableTuple     tuple.TableRowTuple
	sysInternalTableTuple tuple.TableRowTuple

	sysLeafColumnsTuple     tuple.TableRowTuple
	sysInternalColumnsTuple tuple.TableRowTuple

	sysLeafIndexTuple     tuple.TableRowTuple
	sysInternalIndexTuple tuple.TableRowTuple

	sysLeafFieldsTuple     tuple.TableRowTuple
	sysInternalFieldsTuple tuple.TableRowTuple
}

func NewDictionarySys(pool *buffer_pool.BufferPool) *DictionarySys {
	var dictSys = new(DictionarySys)
	dictSys.currentRowId = 0
	dictSys.currentTableId = 0
	dictSys.currentIndexId = 0
	dictSys.currentSpaceId = 0
	dictSys.pool = pool
	dictSys.sysLeafTableTuple = NewSysTableTuple()
	dictSys.sysInternalTableTuple = newSysTableInternalTuple()

	dictSys.sysLeafColumnsTuple = NewSysColumnsTuple()
	dictSys.sysInternalColumnsTuple = newSysTableInternalTuple()

	dictSys.sysLeafFieldsTuple = NewSysFieldsTuple()
	dictSys.sysInternalFieldsTuple = newSysTableInternalTuple()

	dictSys.sysLeafIndexTuple = NewSysIndexTuple()
	dictSys.sysInternalIndexTuple = newSysTableInternalTuple()

	return dictSys
}

/**
用于mysql创建初始化的时候，用于创建和初始化数据字典
**/
func NewDictionarySysAtInit() *DictionarySys {
	var dictSys = new(DictionarySys)
	dictSys.currentRowId = 0
	dictSys.currentTableId = 0
	dictSys.currentIndexId = 0
	dictSys.currentSpaceId = 0
	dictSys.sysLeafTableTuple = NewSysTableTuple()
	dictSys.sysInternalTableTuple = newSysTableInternalTuple()

	dictSys.sysLeafColumnsTuple = NewSysColumnsTuple()
	dictSys.sysInternalColumnsTuple = newSysTableInternalTuple()

	dictSys.sysLeafFieldsTuple = NewSysFieldsTuple()
	dictSys.sysInternalFieldsTuple = newSysTableInternalTuple()

	dictSys.sysLeafIndexTuple = NewSysIndexTuple()
	dictSys.sysInternalIndexTuple = newSysTableInternalTuple()

	return dictSys
}

func NewDictionarySysByWrapper(dt *DataDictWrapper) *DictionarySys {
	var dictSys = new(DictionarySys)
	dictSys.currentRowId = dt.MaxRowId
	dictSys.currentTableId = dt.MaxTableId
	dictSys.currentIndexId = dt.MaxIndexId
	dictSys.currentSpaceId = dt.MaxSpaceId

	dictSys.sysLeafTableTuple = NewSysTableTuple()
	dictSys.sysInternalTableTuple = newSysTableInternalTuple()

	dictSys.sysLeafColumnsTuple = NewSysColumnsTuple()
	dictSys.sysInternalColumnsTuple = newSysTableInternalTuple()

	dictSys.sysLeafFieldsTuple = NewSysFieldsTuple()
	dictSys.sysInternalFieldsTuple = newSysTableInternalTuple()

	dictSys.sysLeafIndexTuple = NewSysIndexTuple()
	dictSys.sysInternalIndexTuple = newSysTableInternalTuple()

	dictSys.DataDict = dt
	return dictSys
}

//加载数据字典表
func (dictSys *DictionarySys) loadDictionary(pool *buffer_pool.BufferPool) {

	bufferblock7 := pool.GetPageBlock(0, 7)

	dictSys.DataDict = NewDataDictWrapperByContent(*bufferblock7.Frame).(*DataDictWrapper)

	bufferblock8 := pool.GetPageBlock(0, 8)

	tuple := NewSysTableTuple()
	sysTablesRootPage := NewPageIndexByLoadBytesWithTuple(*bufferblock8.Frame, tuple).(*Index)

	bufferblock10 := pool.GetPageBlock(0, 10)
	columnTuple := NewSysColumnsTuple()
	sysColumnsRootPage := NewPageIndexByLoadBytesWithTuple(*bufferblock10.Frame, columnTuple).(*Index)

	bufferblock11 := pool.GetPageBlock(0, 11)
	sysIndexTuple := NewSysIndexTuple()
	sysIndexesRootPage := NewPageIndexByLoadBytesWithTuple(*bufferblock11.Frame, sysIndexTuple).(*Index)

	bufferblock12 := pool.GetPageBlock(0, 12)
	sysFieldTuple := NewSysFieldsTuple()
	sysFieldsRootPage := NewPageIndexByLoadBytesWithTuple(*bufferblock12.Frame, sysFieldTuple).(*Index)

	dictSys.SysTable = NewDictTableWithRootIndex(0, "INNODB_SYS_TABLE", "SYS_TABLE", 8, sysTablesRootPage, pool, dictSys.sysLeafTableTuple, dictSys.sysInternalTableTuple)
	dictSys.SysColumns = NewDictTableWithRootIndex(0, "INNODB_SYS_COLUMNS", "SYS_COLUMNS", 10, sysColumnsRootPage, pool, dictSys.sysLeafColumnsTuple, dictSys.sysInternalColumnsTuple)
	dictSys.SysIndex = NewDictTableWithRootIndex(0, "INNODB_SYS_INDEX", "SYS_INDEX", 11, sysIndexesRootPage, pool, dictSys.sysLeafIndexTuple, dictSys.sysInternalIndexTuple)
	dictSys.SysFields = NewDictTableWithRootIndex(0, "INNODB_SYS_FIELDS", "SYS_FIELDS", 12, sysFieldsRootPage, pool, dictSys.sysLeafFieldsTuple, dictSys.sysInternalFieldsTuple)
	dictSys.currentSpaceId = dictSys.DataDict.MaxSpaceId
	dictSys.currentTableId = dictSys.DataDict.MaxTableId
	dictSys.currentRowId = dictSys.DataDict.MaxRowId
	dictSys.currentIndexId = dictSys.DataDict.MaxIndexId
}

//加载数据字典表
//func (dictSys *DictionarySys) loadDictionary(space *SysTableSpace, pool *buffer_pool.BufferPool) {
//	bufferBlock_8 := pool.GetPageBlock(0, 8)
//
//	tuple := NewSysTableTuple()
//	space.SysTables = NewPageIndexByLoadBytesWithTuple(*bufferBlock_8.Frame, tuple).(*Index)
//
//	bufferblock10 := pool.GetPageBlock(0, 10)
//	columnTuple := NewSysColumnsTuple()
//	space.SysColumns = NewPageIndexByLoadBytesWithTuple(*bufferblock10.Frame, columnTuple).(*Index)
//
//	bufferblock11 := pool.GetPageBlock(0, 11)
//	sysIndexTuple := NewSysIndexTuple()
//	space.SysIndexes = NewPageIndexByLoadBytesWithTuple(*bufferblock11.Frame, sysIndexTuple).(*Index)
//
//	bufferblock12 := pool.GetPageBlock(0, 12)
//	sysFieldTuple := NewSysFieldsTuple()
//	space.SysIndexes = NewPageIndexByLoadBytesWithTuple(*bufferblock12.Frame, sysFieldTuple).(*Index)
//
//	dictSys.SysTable = NewDictTableWithRootIndex(0, "INNODB_SYS_TABLE", "SYS_TABLE", 8, space.SysTables, pool, dictSys.sysLeafTableTuple, dictSys.sysInternalTableTuple)
//	dictSys.SysColumns = NewDictTableWithRootIndex(0, "INNODB_SYS_COLUMNS", "SYS_COLUMNS", 10, space.SysColumns, pool, dictSys.sysLeafColumnsTuple, dictSys.sysInternalColumnsTuple)
//	dictSys.SysIndex = NewDictTableWithRootIndex(0, "INNODB_SYS_INDEX", "SYS_INDEX", 11, space.SysIndexes, pool, dictSys.sysLeafIndexTuple, dictSys.sysInternalIndexTuple)
//	//	dictSys.SysFields = NewDictTableWithRootIndex(0, "INNODB_SYS_FIELDS", "SYS_FIELDS", 12, space.SysFields, space, pool, dictSys.sysLeafFieldsTuple, dictSys.sysInternalFieldsTuple)
//	dictSys.currentSpaceId = dictSys.DataDict.MaxSpaceId
//	dictSys.currentTableId = dictSys.DataDict.MaxTableId
//	dictSys.currentRowId = dictSys.DataDict.MaxRowId
//	dictSys.currentIndexId = dictSys.DataDict.MaxIndexId
//}

func (dictSys *DictionarySys) initDictionary(space *SysTableSpace) {

	dictSys.SysTable = NewDictTableWithRootIndexAtInitialize("INNODB_SYS_TABLE", "SYS_TABLE", 8, space.SysTables, space, dictSys.sysLeafTableTuple, dictSys.sysInternalTableTuple)
	dictSys.SysColumns = NewDictTableWithRootIndexAtInitialize("INNODB_SYS_COLUMNS", "SYS_COLUMNS", 10, space.SysColumns, space, dictSys.sysLeafColumnsTuple, dictSys.sysInternalColumnsTuple)
	dictSys.SysIndex = NewDictTableWithRootIndexAtInitialize("INNODB_SYS_INDEX", "SYS_INDEX", 11, space.SysIndexes, space, dictSys.sysLeafIndexTuple, dictSys.sysInternalIndexTuple)
	dictSys.SysFields = NewDictTableWithRootIndexAtInitialize("INNODB_SYS_FIELDS", "SYS_FIELDS", 12, space.SysFields, space, dictSys.sysLeafFieldsTuple, dictSys.sysInternalFieldsTuple)
	dictSys.currentSpaceId = space.DataDict.MaxSpaceId
	dictSys.currentTableId = space.DataDict.MaxTableId
	dictSys.currentRowId = space.DataDict.MaxRowId
	dictSys.currentIndexId = space.DataDict.MaxIndexId
}

func (dictSys *DictionarySys) CreateTable(databaseName string, tuple *TableTupleMeta) (err error) {
	//插入到SYS_TABLE中

	currentSysTableRow := NewClusterSysIndexLeafRow(dictSys.sysLeafTableTuple, false)
	currentSysColumnRow := NewClusterSysIndexLeafRow(dictSys.sysLeafColumnsTuple, false)
	currentSysIndexRow := NewClusterSysIndexLeafRow(dictSys.sysLeafIndexTuple, false)
	currentSysFieldsRow := NewClusterSysIndexLeafRow(dictSys.sysLeafFieldsTuple, false)

	//dictSys.initSysTableRow(databaseName, tuple, currentSysTableRow)
	//dictSys.initSysColumns(databaseName, tuple, currentSysColumnRow)
	////dictSys.initSysIndex(databaseName, tuple, currentSysIndexRow)
	//dictSys.initSysFields(databaseName, tuple, currentSysFieldsRow)

	err = dictSys.SysTable.AddDictRow(currentSysTableRow)
	if err != nil {
		return err
	}
	err = dictSys.SysTable.AddDictRow(currentSysColumnRow)
	if err != nil {
		return err
	}
	err = dictSys.SysTable.AddDictRow(currentSysIndexRow)
	if err != nil {
		return err
	}
	err = dictSys.SysTable.AddDictRow(currentSysFieldsRow)
	if err != nil {
		return err
	}
	return nil
}

//创建系统文件表
func (dictSys *DictionarySys) initializeSysDataFilesTable(databaseName string, tuple tuple.TableRowTuple) (err error) {

	return dictSys.createSystemTable(databaseName, tuple, 13, 0)
}

//创建空间表
func (dictSys *DictionarySys) initializeSysTableSpacesTable(databaseName string, tuple tuple.TableRowTuple) (err error) {

	return dictSys.createSystemTable(databaseName, tuple, 16, 0)

}

/**
创建系统表
@param	 databaseName 数据库名称
@param   tuple 元祖信息

每创建一个表，需要记录Columns
***/
func (dictSys *DictionarySys) createSystemTable(databaseName string, tuple tuple.TableRowTuple, rootNo uint32, spaceId uint32) (err error) {
	//插入到SYS_TABLE中

	currentSysTableRow, err := dictSys.wrapperSysTable(databaseName, tuple, err)
	if err != nil {
		fmt.Println("error" + err.Error())
		return err
	}
	//系统表插入
	fmt.Println("=============系统表插入==============" + currentSysTableRow.GetPrimaryKey().ToString())
	fmt.Println(currentSysTableRow.ToString())

	///插入SysColumns
	err = dictSys.wrapperSysColumns(tuple, err)
	if err != nil {
		return err
	}
	err = dictSys.wrapperIndex(tuple, rootNo, spaceId, err, currentSysTableRow)
	if err != nil {
		return err
	}
	dictSys.currentTableId++

	return nil
}

func (dictSys *DictionarySys) wrapperSysTable(databaseName string, tuple tuple.TableRowTuple, err error) (basic.Row, error) {
	currentSysTableRow := NewClusterSysIndexLeafRow(dictSys.sysLeafTableTuple, false)
	dictSys.initSysTableRow(databaseName, tuple, currentSysTableRow)
	//dictSys.currentRowId++
	err = dictSys.SysTable.AddDictRow(currentSysTableRow)
	if err != nil {
		return nil, err
	}
	return currentSysTableRow, err
}

func (dictSys *DictionarySys) wrapperIndex(tuple tuple.TableRowTuple, rootNo uint32, spaceId uint32, err error, currentSysTableRow basic.Row) error {
	//主键列不为空
	if tuple.GetPrimaryColumn() != nil {

		currentSysIndexRow := NewClusterSysIndexLeafRow(dictSys.sysLeafIndexTuple, false)
		dictSys.initSysIndexPrimary(rootNo, spaceId, tuple, currentSysIndexRow)
		err = dictSys.SysIndex.AddDictRow(currentSysTableRow)
		if err != nil {
			return err
		}
	}
	//处理二级索引
	if tuple.GetSecondaryColumns() != nil {
		for _, v := range tuple.GetSecondaryColumns() {
			currentSysSecondIndexRow := NewClusterSysIndexLeafRow(dictSys.sysLeafIndexTuple, false)
			var indexType uint32 = 0
			if v.IndexType == "NORMAL" {
				indexType = 0
			}
			dictSys.initSysIndexSecondaryRow(rootNo, spaceId, v.IndexName, indexType, uint32(len(v.IndexColumns)), currentSysSecondIndexRow)
			err = dictSys.SysIndex.AddDictRow(currentSysSecondIndexRow)
			if err != nil {
				return err
			}
		}

	}
	return err
}

func (dictSys *DictionarySys) wrapperSysColumns(tuple tuple.TableRowTuple, err error) error {
	columnlength := tuple.GetColumnLength()

	for i := 0; i < columnlength; i++ {

		currentColumn := tuple.GetColumnInfos(byte(i))

		isHidden := currentColumn.IsHidden
		if isHidden {
			continue
		}

		currentColumnTableRow := NewClusterSysIndexLeafRow(dictSys.sysLeafColumnsTuple, false)

		//
		//rowId
		//currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
		//transaction_id
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
		//rowpointer
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 1)
		//tableId
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentTableId), 2)

		//name
		currentColumnTableRow.WriteBytesWithNullWithsPos([]byte(currentColumn.FieldName), 3)

		//pos
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(i)), 4)

		//mtype
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(uint64(uint32(i)))), 5)

		//prtype
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(uint64(uint32(i)))), 6)

		//len
		currentColumnTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(uint64(uint32(i)))), 7)
		fmt.Println("======待插入=====")
		fmt.Println(currentColumnTableRow.ToString() + "插入的IDK" + currentColumnTableRow.GetPrimaryKey().ToString())
		//插入columns表
		err = dictSys.SysColumns.AddDictRow(currentColumnTableRow)
		if err != nil {

			break
		}
		//dictSys.currentRowId++
	}
	return err
}

func (dictSys *DictionarySys) initSysTableRow(databaseName string, tuple tuple.TableRowTuple, currentSysTableRow basic.Row) {
	//rowId
	//currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
	//transaction_id
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
	//rowpointer
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 1)
	//tableId

	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentTableId), 2)
	//tableName
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte(databaseName+"/"+tuple.GetTableName()), 3)
	//flag
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte{0, 0, 0, 0}, 4)
	//N_COLS
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(tuple.GetColumnLength())), 5)

	//space_id
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(dictSys.currentSpaceId), 6)

	//FileFormat
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("Antelope"), 7)
	//RowFormat
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("Redundant"), 8)
	//ZipPageSize
	currentSysTableRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(0), 9)
	//SpaceType
	currentSysTableRow.WriteBytesWithNullWithsPos([]byte("space"), 10)
}

func (dictSys *DictionarySys) initSysColumns(databaseName string, tuple tuple.TableRowTuple, currentSysColumnRow basic.Row) {

	//rowId
	//currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
	//transaction_id
	currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
	//rowpointer
	currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 1)
	//tableId
	currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentTableId), 2)

	//tableName
	currentSysColumnRow.WriteBytesWithNullWithsPos([]byte(databaseName+"/"+tuple.GetTableName()), 3)
	//flag
	currentSysColumnRow.WriteBytesWithNullWithsPos([]byte{0, 0, 0, 0}, 4)
	//N_COLS
	currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(tuple.GetColumnLength())), 5)

	//space_id
	currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(dictSys.currentSpaceId), 6)

	//FileFormat
	currentSysColumnRow.WriteBytesWithNullWithsPos([]byte("Antelope"), 7)
	//RowFormat
	currentSysColumnRow.WriteBytesWithNullWithsPos([]byte("Redundant"), 8)
	//ZipPageSize
	currentSysColumnRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(0), 9)
	//SpaceType
	currentSysColumnRow.WriteBytesWithNullWithsPos([]byte("space"), 10)
}

func (dictSys *DictionarySys) initSysIndexPrimary(rootPageNo uint32, spaceId uint32, tuple tuple.TableRowTuple, currentSysIndexRow basic.Row) {
	//rowId
	//currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId))
	//transaction_id
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
	//rowpointer
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 1)
	//IndexId
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentIndexId), 2)
	//indexName
	currentSysIndexRow.WriteBytesWithNullWithsPos([]byte(tuple.GetPrimaryColumn().IndexName), 3)
	//TABLE_ID
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentTableId), 4)
	//TYPE
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(3), 5)
	//N_FIELDS
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(uint32(len(tuple.GetPrimaryColumn().IndexColumns))), 6)
	//PAGE_NO
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(rootPageNo), 7)

	//SPACE_NO
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(spaceId), 8)

}

func (dictSys *DictionarySys) initSysIndexSecondaryRow(rootPageNo uint32, spaceId uint32, indexName string, indexType uint32, indexLength uint32, currentSysIndexRow basic.Row) {
	//rowId
	//currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId))
	//transaction_id
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 0)
	//rowpointer
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentRowId), 1)
	//IndexId
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentIndexId), 2)
	//indexName
	currentSysIndexRow.WriteBytesWithNullWithsPos([]byte(indexName), 3)
	//TABLE_ID
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertULong8Bytes(dictSys.currentTableId), 4)
	//TYPE
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(indexType), 5)
	//N_FIELDS
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(indexLength), 6)
	//PAGE_NO
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(rootPageNo), 7)

	//SPACE_NO
	currentSysIndexRow.WriteBytesWithNullWithsPos(util.ConvertUInt4Bytes(spaceId), 8)

}

func (dictSys *DictionarySys) initSysFields(databaseName string, tuple tuple.TableRowTuple, currentSysFieldsRow basic.Row) {
	//rowId
	currentSysFieldsRow.WriteWithNull(util.ConvertULong8Bytes(dictSys.currentRowId))
	//transaction_id
	currentSysFieldsRow.WriteWithNull(util.ConvertULong8Bytes(dictSys.currentRowId))
	//rowpointer
	currentSysFieldsRow.WriteWithNull(util.ConvertULong8Bytes(dictSys.currentRowId))
	//tableId
	currentSysFieldsRow.WriteWithNull(util.ConvertULong8Bytes(dictSys.currentTableId))
	//tableName
	currentSysFieldsRow.WriteWithNull([]byte(databaseName + "/" + tuple.GetDatabaseName()))
	//flag
	currentSysFieldsRow.WriteWithNull([]byte{0, 0, 0, 0})
	//N_COLS
	currentSysFieldsRow.WriteWithNull(util.ConvertUInt4Bytes(uint32(tuple.GetColumnLength())))

	//space_id
	currentSysFieldsRow.WriteWithNull(util.ConvertUInt4Bytes(dictSys.currentSpaceId))

	//FileFormat
	currentSysFieldsRow.WriteWithNull([]byte("Antelope"))
	//RowFormat
	currentSysFieldsRow.WriteWithNull([]byte("Redundant"))
	//ZipPageSize
	currentSysFieldsRow.WriteWithNull(util.ConvertUInt4Bytes(0))
	//SpaceType
	currentSysFieldsRow.WriteWithNull([]byte("space"))
}

type DictTable struct {
	TableName     string
	RootPageNo    uint32
	IndexName     string
	BTree         *BTree
	internalTuple tuple.TableRowTuple
	leafTuple     tuple.TableRowTuple
}

func NewDictTableWithRootIndex(spaceId uint32, TableName string, IndexName string, rootPageNo uint32, rootIndex *Index,
	bufferPool *buffer_pool.BufferPool, leafTuple tuple.TableRowTuple, internalTuple tuple.TableRowTuple) *DictTable {

	var dictTable = new(DictTable)

	dictTable.TableName = TableName
	dictTable.RootPageNo = rootPageNo
	dictTable.leafTuple = leafTuple
	dictTable.internalTuple = internalTuple
	currentIndex := rootIndex

	segLeafBytes := currentIndex.GetSegLeaf()

	segInternalBytes := currentIndex.GetSegTop()

	segInternalSpaceId := util.ReadUB4Byte2UInt32(segInternalBytes[0:4])
	segInternalPageNumber := util.ReadUB4Byte2UInt32(segInternalBytes[4:8])
	segInternalOffset := util.ReadUB2Byte2Int(segInternalBytes[8:10])

	segLeafSpaceId := util.ReadUB4Byte2UInt32(segLeafBytes[0:4])
	segLeafPageNumber := util.ReadUB4Byte2UInt32(segLeafBytes[4:8])
	segLeafOffset := util.ReadUB2Byte2Int(segLeafBytes[8:10])

	internalSeg := NewInternalSegmentWithTupleAndBufferPool(segInternalSpaceId, segInternalPageNumber, segInternalOffset, IndexName, dictTable.internalTuple, bufferPool)
	datasegs := NewDataSegmentWithTupleAndBufferPool(segLeafSpaceId, segLeafPageNumber, segLeafOffset, IndexName, dictTable.leafTuple, bufferPool)
	dictTable.BTree = NewBtreeWithBufferPool(spaceId, rootPageNo, IndexName, internalSeg, datasegs, currentIndex, bufferPool, dictTable.internalTuple, dictTable.leafTuple)

	return dictTable
}
func NewDictTableWithRootIndexAtInitialize(TableName string, IndexName string, rootPageNo uint32, rootIndex *Index,
	space *SysTableSpace,
	leafTuple tuple.TableRowTuple, internalTuple tuple.TableRowTuple) *DictTable {

	var dictTable = new(DictTable)

	dictTable.TableName = TableName
	dictTable.RootPageNo = rootPageNo
	dictTable.leafTuple = leafTuple
	dictTable.internalTuple = internalTuple
	currentIndex := rootIndex

	segLeafBytes := currentIndex.GetSegLeaf()

	segInternalBytes := currentIndex.GetSegTop()

	segInternalSpaceId := util.ReadUB4Byte2UInt32(segInternalBytes[0:4])
	segInternalPageNumber := util.ReadUB4Byte2UInt32(segInternalBytes[4:8])
	segInternalOffset := util.ReadUB2Byte2Int(segInternalBytes[8:10])

	segLeafSpaceId := util.ReadUB4Byte2UInt32(segLeafBytes[0:4])
	segLeafPageNumber := util.ReadUB4Byte2UInt32(segLeafBytes[4:8])
	segLeafOffset := util.ReadUB2Byte2Int(segLeafBytes[8:10])

	//TODO 需要完善初始化加载的逻辑
	internalSeg := NewInternalSegmentWithTuple(segInternalSpaceId, segInternalPageNumber, segInternalOffset, IndexName, space, dictTable.internalTuple)
	datasegs := NewLeafSegmentWithTuple(segLeafSpaceId, segLeafPageNumber, segLeafOffset, IndexName, space, dictTable.leafTuple)
	dictTable.BTree = NewBtreeAtInit(rootPageNo, IndexName, internalSeg, datasegs, currentIndex, space.blockFile, dictTable.internalTuple, dictTable.leafTuple)

	return dictTable
}

func (d *DictTable) AddDictRow(rows basic.Row) error {

	err := d.BTree.Add(rows.GetPrimaryKey(), rows)

	return err
}

func (d *DictTable) RemoveRow(rows basic.Row) error {

	//d.BTree._range(func() (a uint32, idx int, err error, bi bpt_iterator) {
	//		return
	//});

	return nil
}
