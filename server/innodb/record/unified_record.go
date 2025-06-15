package record

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// ExecutorRecord 火山模型使用的记录结构
type ExecutorRecord struct {
	Values []interface{}       // 字段值
	Schema *metadata.TableMeta // 表结构定义
}

// NewExecutorRecord 创建执行器记录
func NewExecutorRecord(values []interface{}, schema *metadata.TableMeta) *ExecutorRecord {
	return &ExecutorRecord{
		Values: values,
		Schema: schema,
	}
}

// GetValue 按列名获取字段值
func (r *ExecutorRecord) GetValue(columnName string) (interface{}, error) {
	if r.Schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	for i, col := range r.Schema.Columns {
		if col.Name == columnName {
			if i < len(r.Values) {
				return r.Values[i], nil
			}
			return nil, fmt.Errorf("value index out of range")
		}
	}
	return nil, fmt.Errorf("column %s not found", columnName)
}

// SetValue 按列名设置字段值
func (r *ExecutorRecord) SetValue(columnName string, value interface{}) error {
	if r.Schema == nil {
		return fmt.Errorf("schema is nil")
	}
	for i, col := range r.Schema.Columns {
		if col.Name == columnName {
			if i < len(r.Values) {
				r.Values[i] = value
				return nil
			}
			return fmt.Errorf("value index out of range")
		}
	}
	return fmt.Errorf("column %s not found", columnName)
}

// GetValues 获取所有字段值
func (r *ExecutorRecord) GetValues() []interface{} {
	return r.Values
}

// SetValues 设置所有字段值
func (r *ExecutorRecord) SetValues(values []interface{}) {
	r.Values = values
}

// GetTableMeta 获取表元数据
func (r *ExecutorRecord) GetTableMeta() *metadata.TableMeta {
	return r.Schema
}

// UnifiedRecord 统一的记录接口，同时支持火山模型和存储层
type UnifiedRecord interface {
	// 继承原有的 basic.Record 接口
	basic.Record

	// 火山模型相关方法
	GetValueByName(columnName string) (basic.Value, error)     // 按列名获取字段值
	SetValueByName(columnName string, value basic.Value) error // 按列名设置字段值
	GetValues() []basic.Value                                  // 获取所有字段值
	SetValues(values []basic.Value)                            // 设置所有字段值
	GetColumnCount() int                                       // 获取列数
	GetValueByIndex(index int) basic.Value                     // 按索引获取字段值
	SetValueByIndex(index int, value basic.Value) error        // 按索引设置字段值

	// 存储层相关方法
	GetStorageData() []byte     // 获取存储格式的数据
	SetStorageData(data []byte) // 设置存储格式的数据
	SetID(id uint64)            // 设置记录ID

	// 转换方法
	ToExecutorRecord() *ExecutorRecord // 转换为火山模型记录
	ToStorageRecord() StorageRecord    // 转换为存储层记录
}

// StorageRecord 存储层使用的记录接口
type StorageRecord interface {
	basic.Row // 继承 Row 接口

	// Record 接口的方法
	GetID() uint64
	GetData() []byte
	GetLength() uint32
	GetType() uint8

	// 存储层特有方法
	GetHeader() basic.FieldDataHeader
	GetValue() basic.FieldDataValue
	GetTableRowTuple() metadata.TableRowTuple
}

// UnifiedRecordImpl 统一记录的实现
type UnifiedRecordImpl struct {
	// 执行器相关字段
	values []interface{}
	schema *metadata.TableMeta

	// 存储层相关字段
	id      uint64
	data    []byte
	header  basic.FieldDataHeader
	value   basic.FieldDataValue
	frmMeta metadata.TableRowTuple
}

// NewUnifiedRecord 创建统一记录
func NewUnifiedRecord() UnifiedRecord {
	return &UnifiedRecordImpl{
		values: make([]interface{}, 0),
	}
}

// NewUnifiedRecordFromExecutor 从执行器记录创建统一记录
func NewUnifiedRecordFromExecutor(execRecord *ExecutorRecord) *UnifiedRecordImpl {
	return &UnifiedRecordImpl{
		values: execRecord.Values,
		schema: execRecord.Schema,
	}
}

// NewUnifiedRecordFromStorage 从存储记录创建统一记录
func NewUnifiedRecordFromStorage(storageRecord StorageRecord) *UnifiedRecordImpl {
	unified := &UnifiedRecordImpl{
		id:      storageRecord.GetID(),
		data:    storageRecord.GetData(),
		header:  storageRecord.GetHeader(),
		value:   storageRecord.GetValue(),
		frmMeta: storageRecord.GetTableRowTuple(),
	}

	// 从存储记录解析出执行器需要的值
	unified.parseValuesFromStorage()

	return unified
}

// parseValuesFromStorage 从存储数据解析出执行器需要的值
func (r *UnifiedRecordImpl) parseValuesFromStorage() {
	if r.frmMeta == nil {
		return
	}

	columnCount := r.frmMeta.GetColumnLength()
	r.values = make([]interface{}, columnCount)

	// 从存储格式解析值（简化实现）
	for i := 0; i < columnCount; i++ {
		if r.value != nil {
			value := r.value.ReadValue(i)
			if value != nil {
				r.values[i] = value.Raw()
			}
		}
	}
}

// 实现 UnifiedRecord 接口 - 火山模型方法
func (r *UnifiedRecordImpl) GetValueByName(columnName string) (basic.Value, error) {
	if r.schema == nil {
		return basic.NewNull(), fmt.Errorf("schema is nil")
	}
	for i, col := range r.schema.Columns {
		if col.Name == columnName {
			if i < len(r.values) {
				switch val := r.values[i].(type) {
				case basic.Value:
					return val, nil
				default:
					return basic.NewStringValue(fmt.Sprintf("%v", val)), nil
				}
			}
			return basic.NewNull(), fmt.Errorf("value index out of range")
		}
	}
	return basic.NewNull(), fmt.Errorf("column %s not found", columnName)
}

func (r *UnifiedRecordImpl) SetValueByName(columnName string, value basic.Value) error {
	if r.schema == nil {
		return fmt.Errorf("schema is nil")
	}
	for i, col := range r.schema.Columns {
		if col.Name == columnName {
			if i >= len(r.values) {
				// 扩展 values 数组
				newValues := make([]interface{}, i+1)
				copy(newValues, r.values)
				r.values = newValues
			}
			r.values[i] = value.Raw()
			return nil
		}
	}
	return fmt.Errorf("column %s not found", columnName)
}

func (r *UnifiedRecordImpl) GetColumnCount() int {
	return len(r.values)
}

func (r *UnifiedRecordImpl) GetValueByIndex(index int) basic.Value {
	if index < 0 || index >= len(r.values) {
		return basic.NewNull()
	}
	switch val := r.values[index].(type) {
	case basic.Value:
		return val
	default:
		return basic.NewStringValue(fmt.Sprintf("%v", val))
	}
}

func (r *UnifiedRecordImpl) SetValueByIndex(index int, value basic.Value) error {
	if index < 0 || index >= len(r.values) {
		return fmt.Errorf("index out of range: %d", index)
	}
	r.values[index] = value.Raw()
	return nil
}

func (r *UnifiedRecordImpl) GetValues() []basic.Value {
	// 转换 interface{} 为 basic.Value
	result := make([]basic.Value, len(r.values))
	for i, v := range r.values {
		switch val := v.(type) {
		case basic.Value:
			result[i] = val
		case string:
			result[i] = basic.NewStringValue(val)
		case int:
			result[i] = basic.NewInt64Value(int64(val))
		case int64:
			result[i] = basic.NewInt64Value(val)
		case []byte:
			result[i] = basic.NewValue(val)
		case nil:
			result[i] = basic.NewNull()
		default:
			result[i] = basic.NewStringValue(fmt.Sprintf("%v", val))
		}
	}
	return result
}

func (r *UnifiedRecordImpl) SetValues(values []basic.Value) {
	// 转换 basic.Value 为 interface{}
	r.values = make([]interface{}, len(values))
	for i, v := range values {
		r.values[i] = v.Raw()
	}
}

func (r *UnifiedRecordImpl) GetTableMeta() *metadata.TableMeta {
	return r.schema
}

func (r *UnifiedRecordImpl) SetTableMeta(schema *metadata.TableMeta) {
	r.schema = schema
}

// 实现 Record 接口 - 存储层方法
func (r *UnifiedRecordImpl) GetID() uint64 {
	return r.id
}

func (r *UnifiedRecordImpl) SetID(id uint64) {
	r.id = id
}

func (r *UnifiedRecordImpl) GetData() []byte {
	if r.data != nil {
		return r.data
	}
	// 如果没有存储数据，从执行器值生成
	return r.generateStorageData()
}

func (r *UnifiedRecordImpl) SetStorageData(data []byte) {
	r.data = data
}

func (r *UnifiedRecordImpl) GetStorageData() []byte {
	return r.GetData()
}

func (r *UnifiedRecordImpl) GetLength() uint32 {
	return uint32(len(r.GetData()))
}

func (r *UnifiedRecordImpl) GetType() uint8 {
	if r.header != nil {
		return r.header.GetRecordType()
	}
	return 0 // 普通记录
}

// generateStorageData 从执行器值生成存储数据
func (r *UnifiedRecordImpl) generateStorageData() []byte {
	if len(r.values) == 0 {
		return []byte{}
	}

	// 简化实现：将所有值序列化为字节数组
	var data []byte
	for _, value := range r.values {
		switch v := value.(type) {
		case string:
			data = append(data, []byte(v)...)
		case []byte:
			data = append(data, v...)
		case int:
			data = append(data, byte(v))
		case int64:
			data = append(data, byte(v))
		default:
			data = append(data, []byte(fmt.Sprintf("%v", v))...)
		}
		data = append(data, 0) // 分隔符
	}
	return data
}

// 实现 Row 接口
func (r *UnifiedRecordImpl) Less(than basic.Row) bool {
	// 简化比较：按主键比较
	thisKey := r.GetPrimaryKey()
	otherKey := than.GetPrimaryKey()
	return thisKey.Compare(otherKey) < 0
}

func (r *UnifiedRecordImpl) ToByte() []byte {
	return r.GetData()
}

func (r *UnifiedRecordImpl) IsInfimumRow() bool {
	return r.GetType() == 2
}

func (r *UnifiedRecordImpl) IsSupremumRow() bool {
	return r.GetType() == 3
}

func (r *UnifiedRecordImpl) GetPageNumber() uint32 {
	// 从数据中提取页号（简化实现）
	data := r.GetData()
	if len(data) >= 4 {
		return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	}
	return 0
}

func (r *UnifiedRecordImpl) WriteWithNull(content []byte) {
	r.data = content
}

func (r *UnifiedRecordImpl) WriteBytesWithNullWithsPos(content []byte, index byte) {
	// 简化实现
	r.data = content
}

func (r *UnifiedRecordImpl) GetRowLength() uint16 {
	return uint16(len(r.GetData()))
}

func (r *UnifiedRecordImpl) GetHeaderLength() uint16 {
	if r.header != nil {
		return r.header.GetRowHeaderLength()
	}
	return 5 // 默认头部长度
}

func (r *UnifiedRecordImpl) GetPrimaryKey() basic.Value {
	// 从第一个值创建主键
	if len(r.values) > 0 {
		return basic.NewValue([]byte(fmt.Sprintf("%v", r.values[0])))
	}
	return basic.NewValue([]byte{})
}

func (r *UnifiedRecordImpl) GetFieldLength() int {
	return len(r.values)
}

func (r *UnifiedRecordImpl) ReadValueByIndex(index int) basic.Value {
	if index < len(r.values) {
		return basic.NewValue([]byte(fmt.Sprintf("%v", r.values[index])))
	}
	return basic.NewValue([]byte{})
}

func (r *UnifiedRecordImpl) SetNOwned(cnt byte) {
	if r.header != nil {
		r.header.SetNOwned(cnt)
	}
}

func (r *UnifiedRecordImpl) GetNOwned() byte {
	if r.header != nil {
		return r.header.GetNOwned()
	}
	return 0
}

func (r *UnifiedRecordImpl) GetNextRowOffset() uint16 {
	if r.header != nil {
		return r.header.GetNextRecord()
	}
	return 0
}

func (r *UnifiedRecordImpl) SetNextRowOffset(offset uint16) {
	if r.header != nil {
		r.header.SetNextRecord(offset)
	}
}

func (r *UnifiedRecordImpl) GetHeapNo() uint16 {
	if r.header != nil {
		return r.header.GetHeapNo()
	}
	return 0
}

func (r *UnifiedRecordImpl) SetHeapNo(heapNo uint16) {
	if r.header != nil {
		r.header.SetHeapNo(heapNo)
	}
}

func (r *UnifiedRecordImpl) SetTransactionId(trxId uint64) {
	// TODO: 实现事务ID设置
}

func (r *UnifiedRecordImpl) GetValueByColName(colName string) basic.Value {
	value, err := r.GetValueByName(colName)
	if err != nil {
		return basic.NewValue([]byte{})
	}
	return value
}

func (r *UnifiedRecordImpl) ToString() string {
	return fmt.Sprintf("UnifiedRecord{ID:%d, Values:%v}", r.id, r.values)
}

// 实现 basic.UnifiedRecord 接口的转换方法
func (r *UnifiedRecordImpl) AsRecord() basic.Record {
	return r
}

func (r *UnifiedRecordImpl) AsExecutorRecord() basic.ExecutorRecord {
	return r
}

// 转换方法
func (r *UnifiedRecordImpl) ToExecutorRecord() *ExecutorRecord {
	return &ExecutorRecord{
		Values: r.values,
		Schema: r.schema,
	}
}

func (r *UnifiedRecordImpl) ToStorageRecord() StorageRecord {
	// 返回一个实现了 StorageRecord 接口的对象
	return &StorageRecordImpl{
		unified: r,
	}
}

// StorageRecordImpl 存储记录的实现
type StorageRecordImpl struct {
	unified *UnifiedRecordImpl
}

func (s *StorageRecordImpl) GetID() uint64 {
	return s.unified.GetID()
}

func (s *StorageRecordImpl) GetData() []byte {
	return s.unified.GetData()
}

func (s *StorageRecordImpl) GetLength() uint32 {
	return s.unified.GetLength()
}

func (s *StorageRecordImpl) GetType() uint8 {
	return s.unified.GetType()
}

func (s *StorageRecordImpl) GetHeader() basic.FieldDataHeader {
	return s.unified.header
}

func (s *StorageRecordImpl) GetValue() basic.FieldDataValue {
	return s.unified.value
}

func (s *StorageRecordImpl) GetTableRowTuple() metadata.TableRowTuple {
	return s.unified.frmMeta
}

// 实现 Row 接口
func (s *StorageRecordImpl) Less(than basic.Row) bool {
	return s.unified.Less(than)
}

func (s *StorageRecordImpl) ToByte() []byte {
	return s.unified.ToByte()
}

func (s *StorageRecordImpl) IsInfimumRow() bool {
	return s.unified.IsInfimumRow()
}

func (s *StorageRecordImpl) IsSupremumRow() bool {
	return s.unified.IsSupremumRow()
}

func (s *StorageRecordImpl) GetPageNumber() uint32 {
	return s.unified.GetPageNumber()
}

func (s *StorageRecordImpl) WriteWithNull(content []byte) {
	s.unified.WriteWithNull(content)
}

func (s *StorageRecordImpl) WriteBytesWithNullWithsPos(content []byte, index byte) {
	s.unified.WriteBytesWithNullWithsPos(content, index)
}

func (s *StorageRecordImpl) GetRowLength() uint16 {
	return s.unified.GetRowLength()
}

func (s *StorageRecordImpl) GetHeaderLength() uint16 {
	return s.unified.GetHeaderLength()
}

func (s *StorageRecordImpl) GetPrimaryKey() basic.Value {
	return s.unified.GetPrimaryKey()
}

func (s *StorageRecordImpl) GetFieldLength() int {
	return s.unified.GetFieldLength()
}

func (s *StorageRecordImpl) ReadValueByIndex(index int) basic.Value {
	return s.unified.ReadValueByIndex(index)
}

func (s *StorageRecordImpl) SetNOwned(cnt byte) {
	s.unified.SetNOwned(cnt)
}

func (s *StorageRecordImpl) GetNOwned() byte {
	return s.unified.GetNOwned()
}

func (s *StorageRecordImpl) GetNextRowOffset() uint16 {
	return s.unified.GetNextRowOffset()
}

func (s *StorageRecordImpl) SetNextRowOffset(offset uint16) {
	s.unified.SetNextRowOffset(offset)
}

func (s *StorageRecordImpl) GetHeapNo() uint16 {
	return s.unified.GetHeapNo()
}

func (s *StorageRecordImpl) SetHeapNo(heapNo uint16) {
	s.unified.SetHeapNo(heapNo)
}

func (s *StorageRecordImpl) SetTransactionId(trxId uint64) {
	s.unified.SetTransactionId(trxId)
}

func (s *StorageRecordImpl) GetValueByColName(colName string) basic.Value {
	return s.unified.GetValueByColName(colName)
}

func (s *StorageRecordImpl) ToString() string {
	return s.unified.ToString()
}

// RecordConverter 记录转换器
type RecordConverter struct{}

// NewRecordConverter 创建记录转换器
func NewRecordConverter() *RecordConverter {
	return &RecordConverter{}
}

// ExecutorToUnified 将执行器记录转换为统一记录
func (c *RecordConverter) ExecutorToUnified(execRecord *ExecutorRecord) UnifiedRecord {
	return NewUnifiedRecordFromExecutor(execRecord)
}

// StorageToUnified 将存储记录转换为统一记录
func (c *RecordConverter) StorageToUnified(storageRecord StorageRecord) UnifiedRecord {
	return NewUnifiedRecordFromStorage(storageRecord)
}

// UnifiedToExecutor 将统一记录转换为执行器记录
func (c *RecordConverter) UnifiedToExecutor(unified UnifiedRecord) *ExecutorRecord {
	return unified.ToExecutorRecord()
}

// UnifiedToStorage 将统一记录转换为存储记录
func (c *RecordConverter) UnifiedToStorage(unified UnifiedRecord) StorageRecord {
	return unified.ToStorageRecord()
}
