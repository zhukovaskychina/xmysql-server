package basic

// Record 记录接口 - 基础存储层接口
type Record interface {
	Row                // 继承Row接口的所有方法
	GetID() uint64     // 获取记录ID
	GetData() []byte   // 获取记录数据
	GetLength() uint32 // 获取记录长度
}

// ExecutorRecord 火山模型执行器使用的记录接口
type ExecutorRecord interface {
	// 火山模型相关方法
	GetValueByName(columnName string) (Value, error)     // 按列名获取字段值
	SetValueByName(columnName string, value Value) error // 按列名设置字段值
	GetValues() []Value                                  // 获取所有字段值
	SetValues(values []Value)                            // 设置所有字段值
	GetColumnCount() int                                 // 获取列数
	GetValueByIndex(index int) Value                     // 按索引获取字段值
	SetValueByIndex(index int, value Value) error        // 按索引设置字段值
}

// UnifiedRecord 统一记录接口，同时支持存储层和执行器
type UnifiedRecord interface {
	Record         // 继承存储层记录接口
	ExecutorRecord // 继承执行器记录接口

	// 统一记录特有方法
	SetID(id uint64)            // 设置记录ID
	GetStorageData() []byte     // 获取存储格式的数据
	SetStorageData(data []byte) // 设置存储格式的数据

	// 类型转换方法
	AsRecord() Record                 // 转换为存储层记录
	AsExecutorRecord() ExecutorRecord // 转换为执行器记录
}

// StorageRecord 存储层扩展记录接口
type StorageRecord interface {
	Record // 继承基础记录接口

	// 存储层特有方法
	GetHeader() FieldDataHeader
	GetFieldValue() FieldDataValue
	GetType() uint8
	SetType(typ uint8)
}
