package store

import (
	"strings"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/tuple"
)

//cluster 的tuple,用于反序列化
type ClusterLeafTuple struct {
	tuple.TableRowTuple
	TableName         string
	dataBaseName      string
	Columns           []*tuple.FormColumnsWrapper
	PrimaryIndexInfos *tuple.IndexInfoWrapper
}

func NewClusterLeafTuple(meta *TableTupleMeta) tuple.TableRowTuple {
	var clusterLeafTuple = new(ClusterLeafTuple)
	clusterLeafTuple.TableName = meta.TableName
	clusterLeafTuple.Columns = meta.Columns
	clusterLeafTuple.PrimaryIndexInfos = meta.PrimaryKeyMeta
	return clusterLeafTuple
}

func (c ClusterLeafTuple) GetTableName() string {
	return c.TableName
}

func (c ClusterLeafTuple) GetDatabaseName() string {
	return c.dataBaseName
}

func (c ClusterLeafTuple) GetColumnLength() int {
	return len(c.Columns)
}

func (c ClusterLeafTuple) GetUnHiddenColumnsLength() int {
	var result = 0

	for _, column := range c.Columns {
		if !column.IsHidden {
			result = result + 1
		}
	}

	return result
}

func (c ClusterLeafTuple) GetColumnInfos(index byte) *tuple.FormColumnsWrapper {
	return c.Columns[index]
}

func (c ClusterLeafTuple) GetVarColumns() []*tuple.FormColumnsWrapper {
	var formColumnsWrapperCols = make([]*tuple.FormColumnsWrapper, 0)
	for i := 0; i < len(c.Columns); i++ {
		if c.Columns[i].FieldType == "VARCHAR" {
			formColumnsWrapperCols = append(formColumnsWrapperCols, c.Columns[i])
		}
	}
	return formColumnsWrapperCols
}

func (c ClusterLeafTuple) GetColumnDescInfo(colName string) (form *tuple.FormColumnsWrapper, pos int) {
	for k, v := range c.Columns {
		if v.FieldName == strings.ToUpper(colName) {
			return v, k
		}
	}
	return nil, -1
}

func (c ClusterLeafTuple) GetVarDescribeInfoIndex(index byte) byte {
	panic("implement me")
}

func (c ClusterLeafTuple) GetPrimaryColumn() *tuple.IndexInfoWrapper {
	panic("implement me")
}

func (c ClusterLeafTuple) GetSecondaryColumns() []*tuple.IndexInfoWrapper {
	panic("implement me")
}

/************
###################################################################
#
#
#
###################################################################

***********/

type ClusterInternalTuple struct {
	tuple.TableRowTuple
	TableName         string
	dataBaseName      string
	Columns           []*tuple.FormColumnsWrapper
	PrimaryIndexInfos *tuple.IndexInfoWrapper
}

func NewClusterInternalTuple(meta *TableTupleMeta) tuple.TableRowTuple {
	var clusterInternalTuple = new(ClusterInternalTuple)
	clusterInternalTuple.TableName = meta.TableName
	clusterInternalTuple.Columns = make([]*tuple.FormColumnsWrapper, 0)
	clusterInternalTuple.Columns = append(clusterInternalTuple.Columns, &tuple.FormColumnsWrapper{
		IsHidden:          false,
		AutoIncrement:     false,
		NotNull:           false,
		ZeroFill:          false,
		FieldType:         "INT",
		FieldTypeIntValue: 0,
		FieldName:         "PAGE_NO",
		FieldLength:       11,
		FieldCommentValue: "",
		FieldDefaultValue: nil,
	})
	clusterInternalTuple.Columns = append(clusterInternalTuple.Columns, meta.PrimaryIndexInfos.IndexColumns...)

	return clusterInternalTuple
}

func (c ClusterInternalTuple) GetTableName() string {
	return c.TableName
}

func (c ClusterInternalTuple) GetDatabaseName() string {
	return c.dataBaseName
}

func (c ClusterInternalTuple) GetColumnLength() int {
	return len(c.Columns)
}

func (c ClusterInternalTuple) GetUnHiddenColumnsLength() int {
	var result = 0

	for _, column := range c.Columns {
		if !column.IsHidden {
			result = result + 1
		}
	}

	return result
}

func (c ClusterInternalTuple) GetColumnInfos(index byte) *tuple.FormColumnsWrapper {
	return c.Columns[index]
}

func (c ClusterInternalTuple) GetVarColumns() []*tuple.FormColumnsWrapper {
	var formColumnsWrapperCols = make([]*tuple.FormColumnsWrapper, 0)
	for i := 0; i < len(c.Columns); i++ {
		if c.Columns[i].FieldType == "VARCHAR" {
			formColumnsWrapperCols = append(formColumnsWrapperCols, c.Columns[i])
		}
	}
	return formColumnsWrapperCols
}

func (c ClusterInternalTuple) GetColumnDescInfo(colName string) (form *tuple.FormColumnsWrapper, pos int) {
	for k, v := range c.Columns {
		if v.FieldName == strings.ToUpper(colName) {
			return v, k
		}
	}
	return nil, -1
}

func (c ClusterInternalTuple) GetVarDescribeInfoIndex(index byte) byte {
	panic("implement me")
}

func (c ClusterInternalTuple) GetPrimaryColumn() *tuple.IndexInfoWrapper {
	panic("implement me")
}

func (c ClusterInternalTuple) GetSecondaryColumns() []*tuple.IndexInfoWrapper {
	panic("implement me")
}

type SecondaryLeafTuple struct {
	tuple.TableRowTuple
	TableName         string
	dataBaseName      string
	Columns           []*tuple.FormColumnsWrapper
	PrimaryIndexInfos *tuple.IndexInfoWrapper
}

func (s SecondaryLeafTuple) GetTableName() string {
	panic("implement me")
}

func (s SecondaryLeafTuple) GetDatabaseName() string {
	panic("implement me")
}

func (s SecondaryLeafTuple) GetColumnLength() int {
	panic("implement me")
}

func (s SecondaryLeafTuple) GetUnHiddenColumnsLength() int {
	panic("implement me")
}

func (s SecondaryLeafTuple) GetColumnInfos(index byte) *tuple.FormColumnsWrapper {
	return s.Columns[index]
}

func (s SecondaryLeafTuple) GetVarColumns() []*tuple.FormColumnsWrapper {
	var formColumnsWrapperCols = make([]*tuple.FormColumnsWrapper, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].FieldType == "VARCHAR" {
			formColumnsWrapperCols = append(formColumnsWrapperCols, s.Columns[i])
		}
	}
	return formColumnsWrapperCols
}

func (s SecondaryLeafTuple) GetColumnDescInfo(colName string) (form *tuple.FormColumnsWrapper, pos int) {
	for k, v := range s.Columns {
		if v.FieldName == strings.ToUpper(colName) {
			return v, k
		}
	}
	return nil, -1
}

func (s SecondaryLeafTuple) GetVarDescribeInfoIndex(index byte) byte {
	panic("implement me")
}

func (s SecondaryLeafTuple) GetPrimaryColumn() *tuple.IndexInfoWrapper {
	panic("implement me")
}

func (s SecondaryLeafTuple) GetSecondaryColumns() []*tuple.IndexInfoWrapper {
	panic("implement me")
}

type SecondaryInternalTuple struct {
	tuple.TableRowTuple
	TableName         string
	dataBaseName      string
	Columns           []*tuple.FormColumnsWrapper
	PrimaryIndexInfos *tuple.IndexInfoWrapper
}

func (s SecondaryInternalTuple) GetTableName() string {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetDatabaseName() string {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetColumnLength() int {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetUnHiddenColumnsLength() int {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetColumnInfos(index byte) *tuple.FormColumnsWrapper {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetVarColumns() []*tuple.FormColumnsWrapper {
	var formColumnsWrapperCols = make([]*tuple.FormColumnsWrapper, 0)
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].FieldType == "VARCHAR" {
			formColumnsWrapperCols = append(formColumnsWrapperCols, s.Columns[i])
		}
	}
	return formColumnsWrapperCols
}

func (s SecondaryInternalTuple) GetColumnDescInfo(colName string) (form *tuple.FormColumnsWrapper, pos int) {
	for k, v := range s.Columns {
		if v.FieldName == strings.ToUpper(colName) {
			return v, k
		}
	}
	return nil, -1
}

func (s SecondaryInternalTuple) GetVarDescribeInfoIndex(index byte) byte {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetPrimaryColumn() *tuple.IndexInfoWrapper {
	panic("implement me")
}

func (s SecondaryInternalTuple) GetSecondaryColumns() []*tuple.IndexInfoWrapper {
	panic("implement me")
}
