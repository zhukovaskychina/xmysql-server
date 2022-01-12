package basic

//本index需要实现trx 目前暂时不操作

type Index interface {

	//Range 查询
	Find(transaction XMySQLTransaction, searchValueStart Value) Cursor

	//根据主键获取行记录
	GetRow(transaction XMySQLTransaction, primaryKey Value) Row

	//插入行
	//返回
	AddRow(transaction XMySQLTransaction, row Row) Value

	AddRowByVersion(transaction XMySQLTransaction, row Row) Value

	//预留并发mvcc 控制
	Range(transaction XMySQLTransaction, searchValueStart Value, searchValueEnd Value) Cursor

	//获取当前索引的成本
	GetCost(transaction XMySQLTransaction) float64
}
