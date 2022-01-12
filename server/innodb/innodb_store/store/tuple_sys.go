package store

import (
	"strings"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/tuple"
)

type SysTableTuple struct {
	tuple.TableRowTuple
	TableName           string
	Columns             []*tuple.FormColumnsWrapper
	PrimaryIndexInfos   *tuple.IndexInfoWrapper
	SecondaryIndexInfos []*tuple.IndexInfoWrapper
}

func (s *SysTableTuple) GetTableName() string {
	return s.TableName
}

func (s *SysTableTuple) GetDatabaseName() string {
	return "information_schema"
}

func (s *SysTableTuple) GetColumnLength() int {
	return len(s.Columns)
}

func (s *SysTableTuple) GetUnHiddenColumnsLength() int {
	var result = 0

	for _, column := range s.Columns {
		if !column.IsHidden {
			result = result + 1
		}
	}

	return result
}

func (s *SysTableTuple) GetColumnInfos(index byte) *tuple.FormColumnsWrapper {
	return s.Columns[index]
}

/**
获取可变长度变量列表
**/
func (s *SysTableTuple) GetVarColumns() []*tuple.FormColumnsWrapper {
	var formColumnsWrapperCols = make([]*tuple.FormColumnsWrapper, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].FieldType == "VARCHAR" {
			formColumnsWrapperCols = append(formColumnsWrapperCols, s.Columns[i])
		}
	}
	return formColumnsWrapperCols
}

func (s *SysTableTuple) GetColumnDescInfo(colName string) (form *tuple.FormColumnsWrapper, pos int) {
	for k, v := range s.Columns {
		if v.FieldName == strings.ToUpper(colName) {
			return v, k
		}
	}
	return nil, -1
}
func (s *SysTableTuple) GetPrimaryColumn() *tuple.IndexInfoWrapper {
	return s.PrimaryIndexInfos
}

func (s *SysTableTuple) GetSecondaryColumns() []*tuple.IndexInfoWrapper {
	return s.SecondaryIndexInfos
}

type SysTableInternalTuple struct {
	tuple.TableRowTuple
	TableName           string
	Columns             []*tuple.FormColumnsWrapper
	PrimaryIndexInfos   *tuple.IndexInfoWrapper
	SecondaryIndexInfos []*tuple.IndexInfoWrapper
}

func (s SysTableInternalTuple) GetTableName() string {
	return s.TableName
}

func (s SysTableInternalTuple) GetDatabaseName() string {
	return "information_schema"
}

func (s SysTableInternalTuple) GetColumnLength() int {
	return len(s.Columns)
}

func (s SysTableInternalTuple) GetUnHiddenColumnsLength() int {
	var result = 0

	for _, column := range s.Columns {
		if !column.IsHidden {
			result = result + 1
		}
	}

	return result
}

func (s SysTableInternalTuple) GetColumnInfos(index byte) *tuple.FormColumnsWrapper {
	return s.Columns[index]
}

func (s SysTableInternalTuple) GetVarColumns() []*tuple.FormColumnsWrapper {
	var formColumnsWrapperCols = make([]*tuple.FormColumnsWrapper, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].FieldType == "VARCHAR" {
			formColumnsWrapperCols = append(formColumnsWrapperCols, s.Columns[i])
		}
	}
	return formColumnsWrapperCols
}

func (s SysTableInternalTuple) GetColumnDescInfo(colName string) (form *tuple.FormColumnsWrapper, pos int) {
	for k, v := range s.Columns {
		if v.FieldName == strings.ToUpper(colName) {
			return v, k
		}
	}
	return nil, -1
}

func (s SysTableInternalTuple) GetVarDescribeInfoIndex(index byte) byte {
	panic("implement me")
}

func (s SysTableInternalTuple) GetPrimaryColumn() *tuple.IndexInfoWrapper {
	return s.PrimaryIndexInfos
}

func (s SysTableInternalTuple) GetSecondaryColumns() []*tuple.IndexInfoWrapper {
	return s.SecondaryIndexInfos
}

func NewSysTableTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableTuple)
	sysTableTuple.TableName = common.INNODB_SYS_TABLES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TRX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_POINTER",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TABLE_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "FLAG",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "N_COLS",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "SPACE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "FILE_FORMAT",
		FieldLength:       10,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "ROW_FORMAT",
		FieldLength:       12,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ZIP_PAGE_SIZE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "SPACE_TYPE",
		FieldLength:       10,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})

	var primaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	primaryColumnsWrapper = append(primaryColumnsWrapper, sysTableTuple.Columns[3])
	sysTableTuple.PrimaryIndexInfos = &tuple.IndexInfoWrapper{
		IndexName:    "PK_SYS_TABLE_NAMES",
		IndexType:    "PRIMARY",
		IndexColumns: primaryColumnsWrapper,
	}

	var secondaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	secondaryColumnsWrapper = append(secondaryColumnsWrapper, sysTableTuple.Columns[2])
	var secondaryIndexWrapper = make([]*tuple.IndexInfoWrapper, 0)
	secondaryIndexWrapper = append(secondaryIndexWrapper, &tuple.IndexInfoWrapper{
		IndexName:    "IDX_SYS_TABLE_IDS",
		IndexType:    "NORMAL",
		IndexColumns: secondaryColumnsWrapper,
	})

	sysTableTuple.SecondaryIndexInfos = secondaryIndexWrapper
	return sysTableTuple
}

func NewSysTableTupleWithFlags(typeFlags string) tuple.TableRowTuple {
	if typeFlags == common.PAGE_INTERNAL {
		return newSysTableInternalTupleWithParams()
	} else {
		return NewSysTableTuple()
	}
}

func newSysTableInternalTupleWithParams() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableInternalTuple)
	sysTableTuple.TableName = common.INNODB_SYS_TABLES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_NO",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	var primaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	primaryColumnsWrapper = append(primaryColumnsWrapper, sysTableTuple.Columns[0], sysTableTuple.Columns[1])
	sysTableTuple.PrimaryIndexInfos = &tuple.IndexInfoWrapper{
		IndexName:    "PK_SYS_TABLE_NAME",
		IndexType:    "PRIMARY",
		IndexColumns: primaryColumnsWrapper,
	}

	return sysTableTuple
}

func newSysTableInternalTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableInternalTuple)
	sysTableTuple.TableName = common.INNODB_SYS_TABLES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_NO",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	return sysTableTuple
}

func newSysTableSpaceInternalTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableInternalTuple)
	sysTableTuple.TableName = common.INNODB_SYS_TABLES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_NO",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	return sysTableTuple
}
func newSysDataFileInternalTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableInternalTuple)
	sysTableTuple.TableName = common.INNODB_SYS_DATAFILES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "SPACE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_NO",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	return sysTableTuple
}

func NewSysColumnsTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableTuple)
	sysTableTuple.TableName = common.INNODB_SYS_COLUMNS
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TRX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_POINTER",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TABLE_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "POS",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "MTYPE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PRTYPE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "LEN",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})

	var primaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	primaryColumnsWrapper = append(primaryColumnsWrapper, sysTableTuple.Columns[2], sysTableTuple.Columns[4])
	sysTableTuple.PrimaryIndexInfos = &tuple.IndexInfoWrapper{
		IndexName:    "PK_SYS_COLUMNS_TABLE_ID_POS",
		IndexType:    "PRIMARY",
		IndexColumns: primaryColumnsWrapper,
	}

	return sysTableTuple
}

func NewSysColumnsTupleWithFlags(typeFlags string) tuple.TableRowTuple {
	if typeFlags == common.PAGE_INTERNAL {
		return newSysTableInternalTuple()
	} else {
		return NewSysColumnsTuple()
	}
}

//SysIndex tableId 和IndexId为联合主键
func NewSysIndexTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableTuple)
	sysTableTuple.TableName = common.INNODB_SYS_INDEXES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TRX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_POINTER",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "INDEX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TABLE_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TYPE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "N_FIELDS",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_NO",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "SPACE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "MERGE_THRESHOLD",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	var primaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	primaryColumnsWrapper = append(primaryColumnsWrapper, sysTableTuple.Columns[3], sysTableTuple.Columns[4])
	sysTableTuple.PrimaryIndexInfos = &tuple.IndexInfoWrapper{
		IndexName:    "PK_SYS_INDEX_TABLE_ID_ID",
		IndexType:    "PRIMARY",
		IndexColumns: primaryColumnsWrapper,
	}

	return sysTableTuple
}

func newSysIndexInternalTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableInternalTuple)
	sysTableTuple.TableName = common.INNODB_SYS_INDEXES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TABLE_ID",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "INDEX_ID",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_NO",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	return sysTableTuple
}

func NewSysIndexTupleWithFlags(typeFlags string) tuple.TableRowTuple {
	if typeFlags == common.PAGE_INTERNAL {
		return newSysIndexInternalTuple()
	} else {
		return NewSysIndexTuple()
	}
}

func NewSysFieldsTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableTuple)
	sysTableTuple.TableName = common.INNODB_SYS_INDEXES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TRX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_POINTER",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TABLE_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "FLAG",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "N_COLS",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "SPACE",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "FILE_FORMAT",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "ROW_FORMAT",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ZIP_PAGE_SIZE",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "SPACE_TYPE",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})

	return sysTableTuple
}

func NewSysFieldsTupleWithFlags(typeFlags string) tuple.TableRowTuple {
	if typeFlags == common.PAGE_INTERNAL {
		return newSysTableInternalTuple()
	} else {
		return NewSysFieldsTuple()
	}
}

func NewSysSpacesTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableTuple)
	sysTableTuple.TableName = common.INNODB_SYS_TABLESPACES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TRX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	//sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
	//	IsHidden:          true,
	//	AutoIncrement:     false,
	//	NotNull:           true,
	//	ZeroFill:          false,
	//	FieldType:         "BIGINT",
	//	FieldTypeIntValue: common.COLUMN_TYPE_INT24,
	//	FieldName:         "ROW_ID",
	//	FieldLength:       21,
	//	FieldCommentValue: "",
	//	FieldDefaultValue: nil,
	//})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_POINTER",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "SPACE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "NAME",
		FieldLength:       655,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "FLAG",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "FILE_FORMAT",
		FieldLength:       10,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "ROW_FORMAT",
		FieldLength:       22,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "PAGE_SIZE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ZIP_PAGE_SIZE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "SPACE_TYPE",
		FieldLength:       10,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "FS_BLOCK_SIZE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "FILE_SIZE",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ALLOCATED_SIZE",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})

	var primaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	primaryColumnsWrapper = append(primaryColumnsWrapper, sysTableTuple.Columns[3])
	sysTableTuple.PrimaryIndexInfos = &tuple.IndexInfoWrapper{
		IndexName:    "PK_SYS_SPACE_NAME",
		IndexType:    "PRIMARY",
		IndexColumns: primaryColumnsWrapper,
	}

	return sysTableTuple

}

func NewSysSpacesTupleWithFlags(typeFlags string) tuple.TableRowTuple {
	if typeFlags == common.PAGE_INTERNAL {
		return newSysTableSpaceInternalTuple()
	} else {
		return NewSysSpacesTuple()
	}
}

//datafile文件元祖
func NewSysDataFilesTuple() tuple.TableRowTuple {
	var sysTableTuple = new(SysTableTuple)
	sysTableTuple.TableName = common.INNODB_SYS_DATAFILES
	sysTableTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)

	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "TRX_ID",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	//sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
	//	IsHidden:          true,
	//	AutoIncrement:     false,
	//	NotNull:           true,
	//	ZeroFill:          false,
	//	FieldType:         "BIGINT",
	//	FieldTypeIntValue: common.COLUMN_TYPE_INT24,
	//	FieldName:         "ROW_ID",
	//	FieldLength:       21,
	//	FieldCommentValue: "",
	//	FieldDefaultValue: nil,
	//})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          true,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "BIGINT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "ROW_POINTER",
		FieldLength:       21,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: common.COLUMN_TYPE_INT24,
		FieldName:         "SPACE",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	sysTableTuple.Columns = append(sysTableTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           true,
		ZeroFill:          false,
		FieldType:         "VARCHAR",
		FieldTypeIntValue: common.COLUMN_TYPE_VARCHAR,
		FieldName:         "PATH",
		FieldLength:       4000,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	var primaryColumnsWrapper = make([]*tuple.FormColumnsWrapper, 0)
	primaryColumnsWrapper = append(primaryColumnsWrapper, sysTableTuple.Columns[2])
	sysTableTuple.PrimaryIndexInfos = &tuple.IndexInfoWrapper{
		IndexName:    "PK_SYS_DATAFILES_SPACE",
		IndexType:    "PRIMARY",
		IndexColumns: primaryColumnsWrapper,
	}

	return sysTableTuple

}

func NewSysDataFilesTupleWithFlags(typeFlags string) tuple.TableRowTuple {
	if typeFlags == common.PAGE_INTERNAL {
		return newSysTableInternalTuple()
	} else {
		return NewSysDataFilesTuple()
	}
}

func (s *SysTableTuple) GetPrimaryKeyColumn() *tuple.FormColumnsWrapper {
	return s.Columns[1]
}
