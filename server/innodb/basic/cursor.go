package basic

type Cursor interface {

	//打开游标
	Open() error

	//获取当前行
	GetRow() Row

	//获取下一个
	Next() bool

	//关闭游标
	Close() error

	Type() string

	CursorName() string
}
