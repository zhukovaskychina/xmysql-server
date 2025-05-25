package record

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// Record represents a record in a page
type Record struct {
	basic.Row
	Key   basic.Key
	Value basic.Value
}

// NewRecord creates a new Record
func NewRecord(key basic.Key, value basic.Value) *Record {
	return &Record{
		Key:   key,
		Value: value,
	}
}

// GetKey returns the key of the record
func (r *Record) GetKey() basic.Key {
	return r.Key
}

// GetValue returns the value of the record
func (r *Record) GetValue() basic.Value {
	return r.Value
}

// SetKey sets the key of the record
func (r *Record) SetKey(key basic.Key) {
	r.Key = key
}

// SetValue sets the value of the record
func (r *Record) SetValue(value basic.Value) {
	r.Value = value
}
