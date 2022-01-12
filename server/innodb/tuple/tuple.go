package tuple

type TableTuple interface {
	GetIndexInfoWrappers(colName string) []*IndexInfoWrapper

	GetAllIndexInfoWrappers() []*IndexInfoWrapper
}
type TableRowTuple interface {
	GetTableName() string

	GetDatabaseName() string

	GetColumnLength() int

	//获取非隐藏列
	GetUnHiddenColumnsLength() int

	GetColumnInfos(index byte) *FormColumnsWrapper

	//获取可变列链表
	GetVarColumns() []*FormColumnsWrapper

	GetColumnDescInfo(colName string) (form *FormColumnsWrapper, pos int)
	//根据列下标，计算出可变列表的下标
	//
	GetVarDescribeInfoIndex(index byte) byte

	//获取主键列
	GetPrimaryColumn() *IndexInfoWrapper

	//获取索引列
	GetSecondaryColumns() []*IndexInfoWrapper
}
