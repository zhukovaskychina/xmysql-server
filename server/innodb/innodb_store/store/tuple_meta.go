package store

import (
	"strings"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/blocks"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/table"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type TableTupleMeta struct {
	tuple.TableTuple
	DatabaseName   string
	TableName      string
	ColumnsMap     map[string]*tuple.FormColumnsWrapper //列
	IndexesMap     map[string]*tuple.IndexInfoWrapper
	PrimaryKeyMeta *tuple.IndexInfoWrapper
	Columns        []*tuple.FormColumnsWrapper
	Cfg            *conf.Cfg
	blockFile      *blocks.BlockFile

	PrimaryIndexInfos   *tuple.IndexInfoWrapper
	SecondaryIndexInfos []*tuple.IndexInfoWrapper
}

func NewTupleMeta(DatabaseName string, tableName string, Cfg *conf.Cfg) *TableTupleMeta {
	var indexMap = make(map[string]*tuple.IndexInfoWrapper)
	var columnsMap = make(map[string]*tuple.FormColumnsWrapper)
	var Columns = make([]*tuple.FormColumnsWrapper, 0)
	filePath := Cfg.DataDir + "/" + DatabaseName + "/"
	var tableTupleMeta = new(TableTupleMeta)
	var blockFile = blocks.NewBlockFileWithoutFileSize(filePath, tableName+".frm")
	tableTupleMeta.blockFile = blockFile
	tableTupleMeta.IndexesMap = indexMap
	tableTupleMeta.ColumnsMap = columnsMap
	tableTupleMeta.Columns = Columns
	return tableTupleMeta
}

func (m *TableTupleMeta) GetTableName() string {
	return m.TableName
}

func (m *TableTupleMeta) GetDatabaseName() string {
	return m.DatabaseName
}

//
//func (m *TableTupleMeta) WriteTupleColumn(columnDefs *sqlparser.ColumnDefinition) {
//	frmColWrapper := tuple.NewFormColumnWrapper()
//	frmColWrapper.InitializeFormWrapper(
//		false,
//		bool(columnDefs.Type.Autoincrement),
//		bool(columnDefs.Type.NotNull),
//		columnDefs.Type.Type,
//		columnDefs.Name.String(),
//		columnDefs.Type.Default,
//		columnDefs.Type.Comment,
//		int16(util.ReadUB2Byte2Int(columnDefs.Type.Length.Val)),
//	)
//	m.ColumnsMap[columnDefs.Name.String()] = frmColWrapper
//	m.Columns = append(m.Columns, frmColWrapper)
//}
//
//func (m *TableTupleMeta) WriteTupleColumns(columnDefs []*sqlparser.ColumnDefinition) {
//	for _, v := range columnDefs {
//		m.WriteTupleColumn(v)
//	}
//}
//
//func (m *TableTupleMeta) WriteIndexDefinitions(definitions []*sqlparser.IndexDefinition) {
//	for _, v := range definitions {
//
//		var columns = make([]*tuple.FormColumnsWrapper, 0)
//		for _, it := range v.Columns {
//
//			columns = append(columns, m.ColumnsMap[it.Column.String()])
//		}
//
//		var indexMeta = &tuple.IndexInfoWrapper{
//			IndexName:    v.Info.Name.String(),
//			IndexType:    v.Info.Type,
//			Primary:      v.Info.Primary,
//			Spatial:      v.Info.Spatial,
//			Unique:       v.Info.Unique,
//			IndexColumns: columns,
//		}
//		m.IndexesMap[v.Info.Name.String()] = indexMeta
//	}
//}

func (m *TableTupleMeta) ReadFrmBytes(form *table.Form) {
	//读取form
	for _, v := range form.FieldBytes {
		currentFormCols := tuple.NewFormColumnWrapper()
		currentFormCols.ParseContent(v.FieldColumnsContent)
		m.Columns = append(m.Columns, currentFormCols)
		m.ColumnsMap[currentFormCols.FieldName] = currentFormCols
	}

	m.PrimaryKeyMeta = tuple.NewIndexInfoWrapper(form.ClusterIndex, m.ColumnsMap)

	for _, v := range form.SecondaryIndexes {
		currentIndex := tuple.NewIndexInfoWrapper(v.SecondaryIndexes, m.ColumnsMap)
		m.IndexesMap[currentIndex.IndexName] = currentIndex
	}
}

func (m TableTupleMeta) ReadFrmFromDisk() {

	frmContent, _ := m.blockFile.ReadFileBySeekStartWithSize(0, m.blockFile.FileSize)
	frm := table.NewFormWithBytes(frmContent)
	m.ReadFrmBytes(frm)
}

func (m *TableTupleMeta) FlushToDisk() {
	form := table.NewForm(m.DatabaseName, m.TableName)
	form.ColumnsLength = util.ConvertUInt4Bytes(uint32(len(m.Columns)))
	for _, v := range m.Columns {
		currentFields := table.FieldBytes{
			FieldColumnsOffset:  util.ConvertUInt4Bytes(uint32(len(v.ToBytes()))),
			FieldColumnsContent: v.ToBytes(),
		}
		form.FieldBytes = append(form.FieldBytes, currentFields)
	}
	form.SecondaryIndexesCount = byte(len(m.IndexesMap))
	for _, v := range m.IndexesMap {
		currentSecondIndex := table.SecondaryIndexes{
			SecondaryIndexesOffset: util.ConvertUInt4Bytes(uint32(len(v.ToBytes()))),
			SecondaryIndexes:       v.ToBytes(),
		}
		form.SecondaryIndexes = append(form.SecondaryIndexes, currentSecondIndex)
	}
	if m.PrimaryKeyMeta != nil {
		form.ClusterIndex = m.PrimaryKeyMeta.ToBytes()
		form.ClusterIndexOffSet = util.ConvertUInt4Bytes(uint32(len(m.PrimaryKeyMeta.ToBytes())))
	}

	bytes := form.ToBytes()
	m.blockFile.WriteContentToBlockFile(bytes)
}

func (m *TableTupleMeta) GetIndexInfoWrappers(colName string) []*tuple.IndexInfoWrapper {
	var resultTupleInfos = make([]*tuple.IndexInfoWrapper, 0)

	for _, v := range m.PrimaryIndexInfos.IndexColumns {
		if strings.Compare(strings.ToUpper(colName), strings.ToUpper(v.FieldName)) == 0 {
			resultTupleInfos = append(resultTupleInfos, m.PrimaryIndexInfos)
		}
	}

	for _, v := range m.SecondaryIndexInfos {
		for _, z := range v.IndexColumns {
			if strings.Compare(strings.ToUpper(colName), strings.ToUpper(z.FieldName)) == 0 {
				resultTupleInfos = append(resultTupleInfos, v)
			}
		}
	}
	return resultTupleInfos
}

func (m *TableTupleMeta) GetAllIndexInfoWrappers() []*tuple.IndexInfoWrapper {
	var resultTupleInfos = make([]*tuple.IndexInfoWrapper, 0)

	resultTupleInfos = append(resultTupleInfos, m.PrimaryIndexInfos)

	resultTupleInfos = append(resultTupleInfos, m.SecondaryIndexInfos...)
	return resultTupleInfos
}

func (m *TableTupleMeta) GetPrimaryClusterLeafTuple() tuple.TableRowTuple {
	return NewClusterLeafTuple(m)
}

func (m *TableTupleMeta) GetPrimaryClusterInternalTuple() tuple.TableRowTuple {
	return nil
}
func (m *TableTupleMeta) GetSecondaryInternalTuple() tuple.TableRowTuple {
	return nil
}
func (m *TableTupleMeta) GetSecondaryLeaflTuple() tuple.TableRowTuple {
	return nil
}
