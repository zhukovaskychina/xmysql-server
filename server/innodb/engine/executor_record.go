package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

// EngineExecutorRecord engine 包中 ExecutorRecord 接口的实现
type EngineExecutorRecord struct {
	values                []basic.Value       // 字段值，使用 basic.Value
	schema                *metadata.TableMeta // 表结构定义
	contentValues         map[int]content.Value
	vectorValues          map[int]vector.Value
	contentSourceContexts map[string]contentSourceRowContext
	contentSourceByColumn map[int]contentSourceRowContext
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
	contentValues := make(map[int]content.Value)
	vectorValues := make(map[int]vector.Value)
	for i, v := range values {
		switch val := v.(type) {
		case string:
			basicValues[i] = basic.NewStringValue(val)
		case int:
			basicValues[i] = basic.NewInt64Value(int64(val))
		case int64:
			basicValues[i] = basic.NewInt64Value(val)
		case bool:
			basicValues[i] = basic.NewStringValue(projectionValueString(val))
		case []byte:
			basicValues[i] = basic.NewValue(val)
		case content.Value:
			basicValues[i] = basic.NewBytes(val.Bytes)
			contentValues[i] = val
		case *content.Value:
			if val == nil {
				basicValues[i] = basic.NewNull()
			} else {
				basicValues[i] = basic.NewBytes(val.Bytes)
				contentValues[i] = *val
			}
		case vector.Value:
			if encoded, err := val.MarshalBinary(); err == nil {
				basicValues[i] = basic.NewBytes(encoded)
				vectorValues[i] = val.Clone()
			} else {
				basicValues[i] = basic.NewNull()
			}
		case *vector.Value:
			if val == nil {
				basicValues[i] = basic.NewNull()
			} else if encoded, err := val.MarshalBinary(); err == nil {
				basicValues[i] = basic.NewBytes(encoded)
				vectorValues[i] = val.Clone()
			} else {
				basicValues[i] = basic.NewNull()
			}
		case time.Time:
			basicValues[i] = basic.NewTime(val)
		case nil:
			basicValues[i] = basic.NewNull()
		default:
			basicValues[i] = basic.NewStringValue(fmt.Sprintf("%v", val))
		}
	}
	record := NewExecutorRecord(basicValues, schema).(*EngineExecutorRecord)
	if len(contentValues) > 0 {
		record.contentValues = contentValues
	}
	if len(vectorValues) > 0 {
		record.vectorValues = vectorValues
	}
	return record
}

// ContentValueByIndex returns the typed CONTENT metadata retained for SQL
// expressions while the ordinary result value remains the original bytes.
func (r *EngineExecutorRecord) ContentValueByIndex(index int) (content.Value, bool) {
	if r == nil {
		return content.Value{}, false
	}
	value, ok := r.contentValues[index]
	return value, ok
}

func (r *EngineExecutorRecord) VectorValueByIndex(index int) (vector.Value, bool) {
	if r == nil {
		return vector.Value{}, false
	}
	value, ok := r.vectorValues[index]
	return value.Clone(), ok
}

func (r *EngineExecutorRecord) ContentSourceContexts() map[string]contentSourceRowContext {
	if r == nil || len(r.contentSourceContexts) == 0 {
		return nil
	}
	contexts := make(map[string]contentSourceRowContext, len(r.contentSourceContexts))
	for key, value := range r.contentSourceContexts {
		value.PrimaryKey = append([]byte(nil), value.PrimaryKey...)
		contexts[key] = value
	}
	return contexts
}

func (r *EngineExecutorRecord) ContentSourceContextByIndex(index int) (contentSourceRowContext, bool) {
	if r == nil || r.contentSourceByColumn == nil {
		return contentSourceRowContext{}, false
	}
	value, ok := r.contentSourceByColumn[index]
	value.PrimaryKey = append([]byte(nil), value.PrimaryKey...)
	return value, ok
}

func cloneContentSourceContextByColumn(values map[int]contentSourceRowContext) map[int]contentSourceRowContext {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[int]contentSourceRowContext, len(values))
	for index, value := range values {
		value.PrimaryKey = append([]byte(nil), value.PrimaryKey...)
		cloned[index] = value
	}
	return cloned
}

// 实现 ExecutorRecord 接口
func (r *EngineExecutorRecord) GetValueByName(columnName string) (basic.Value, error) {
	if r.schema == nil {
		return basic.NewNull(), fmt.Errorf("schema is nil")
	}
	for i, col := range r.schema.Columns {
		if col != nil && strings.EqualFold(col.Name, columnName) {
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
		if col != nil && strings.EqualFold(col.Name, columnName) {
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
	r.contentValues = nil
	r.vectorValues = nil
	r.contentSourceByColumn = nil
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
	delete(r.contentValues, index)
	delete(r.vectorValues, index)
	delete(r.contentSourceByColumn, index)
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
