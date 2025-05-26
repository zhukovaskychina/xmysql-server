package basic

import (
	"github.com/zhukovaskychina/xmysql-server/server"
)

//本index需要实现trx 目前暂时不操作

type Index interface {

	//Range 查询
	Find(session server.MySQLServerSession, searchValueStart Value, searchValueEnd Value) Cursor

	//根据主键获取行记录
	GetRow(session server.MySQLServerSession, primaryKey Value) Row

	//插入行
	//返回
	AddRow(row Row) Value

	//预留并发mvcc 控制
	Range(session server.MySQLServerSession, searchValueStart Value, searchValueEnd Value) Cursor
	//获取当前索引的成本
	GetCost() float64
}
