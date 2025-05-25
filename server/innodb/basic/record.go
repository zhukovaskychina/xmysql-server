package basic

// Record 记录接口
type Record interface {
	Row                // 继承Row接口的所有方法
	GetID() uint64     // 获取记录ID
	GetData() []byte   // 获取记录数据
	GetLength() uint32 // 获取记录长度
}
