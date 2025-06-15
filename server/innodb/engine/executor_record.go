package engine

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

// EngineExecutorRecord engine 包中 ExecutorRecord 接口的实现
type EngineExecutorRecord struct {
	values []basic.Value       // 字段值，使用 basic.Value
	schema *metadata.TableMeta // 表结构定义
}

// NewExecutorRecord 创建执行器记录
func NewExecutorRecord(values []basic.Value, schema *metadata.TableMeta) basic.ExecutorRecord {
	return &EngineExecutorRecord{
		values: values,
		schema: schema,
	}
}

// NewExecutorRecordFromInterface 从 interface{} 数组创建执行器记录（兼容性方法）
func NewExecutorRecordFromInterface(values []interface{}, schema *metadata.TableMeta) basic.ExecutorRecord {
	basicValues := make([]basic.Value, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case string:
			basicValues[i] = basic.NewStringValue(val)
		case int:
			basicValues[i] = basic.NewInt64Value(int64(val))
		case int64:
			basicValues[i] = basic.NewInt64Value(val)
		case []byte:
			basicValues[i] = basic.NewValue(val)
		case nil:
			basicValues[i] = basic.NewNull()
		default:
			basicValues[i] = basic.NewStringValue(fmt.Sprintf("%v", val))
		}
	}
	return NewExecutorRecord(basicValues, schema)
}

// 实现 ExecutorRecord 接口
func (r *EngineExecutorRecord) GetValueByName(columnName string) (basic.Value, error) {
	if r.schema == nil {
		return basic.NewNull(), fmt.Errorf("schema is nil")
	}
	for i, col := range r.schema.Columns {
		if col.Name == columnName {
			if i < len(r.values) {
				return r.values[i], nil
			}
			return basic.NewNull(), fmt.Errorf("value index out of range")
		}
	}
	return basic.NewNull(), fmt.Errorf("column %s not found", columnName)
}

func (r *EngineExecutorRecord) SetValueByName(columnName string, value basic.Value) error {
	if r.schema == nil {
		return fmt.Errorf("schema is nil")
	}
	for i, col := range r.schema.Columns {
		if col.Name == columnName {
			if i < len(r.values) {
				r.values[i] = value
				return nil
			}
			return fmt.Errorf("value index out of range")
		}
	}
	return fmt.Errorf("column %s not found", columnName)
}

func (r *EngineExecutorRecord) GetValues() []basic.Value {
	return r.values
}

func (r *EngineExecutorRecord) SetValues(values []basic.Value) {
	r.values = values
}

func (r *EngineExecutorRecord) GetColumnCount() int {
	return len(r.values)
}

func (r *EngineExecutorRecord) GetValueByIndex(index int) basic.Value {
	if index < 0 || index >= len(r.values) {
		return basic.NewNull()
	}
	return r.values[index]
}

func (r *EngineExecutorRecord) SetValueByIndex(index int, value basic.Value) error {
	if index < 0 || index >= len(r.values) {
		return fmt.Errorf("index out of range: %d", index)
	}
	r.values[index] = value
	return nil
}

// 额外的方法，用于获取表元数据（不在接口中，避免循环引用）
func (r *EngineExecutorRecord) GetTableMeta() *metadata.TableMeta {
	return r.schema
}

func (r *EngineExecutorRecord) SetTableMeta(schema *metadata.TableMeta) {
	r.schema = schema
}

// 兼容性方法：转换为 interface{} 数组
func (r *EngineExecutorRecord) ToInterfaceValues() []interface{} {
	result := make([]interface{}, len(r.values))
	for i, v := range r.values {
		result[i] = v.Raw()
	}
	return result
}

// Record 火山模型使用的记录类型别名
type Record = basic.ExecutorRecord
