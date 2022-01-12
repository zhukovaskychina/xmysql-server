package store

//
//type TableRowTuple interface {
//	GetTableName() string
//
//	GetDatabaseName() string
//
//	GetColumnLength() int
//
//	//获取非隐藏列
//	GetUnHiddenColumnsLength() int
//
//	GetColumnInfos(index byte) *innodb.FormColumnsWrapper
//
//	//获取可变列链表
//	GetVarColumns() []*innodb.FormColumnsWrapper
//
//	GetColumnDescInfo(colName string) (form *innodb.FormColumnsWrapper, pos int)
//	//根据列下标，计算出可变列表的下标
//	//
//	GetVarDescribeInfoIndex(index byte) byte
//
//	//获取主键列
//	GetPrimaryColumn() *innodb.IndexInfoWrapper
//
//	//获取索引列
//	GetSecondaryColumns() []*innodb.IndexInfoWrapper
//}
