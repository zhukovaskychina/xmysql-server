package basic

import (
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"math"
)

// ValType specifies the type for SQLVal.
type ValType int

// These are the possible Valtype values.
// HexNum represents a 0x... valueImpl. It cannot
// be treated as a simple valueImpl because it can
// be interpreted differently depending on the
// context.
const (
	UNKVAL   = 0
	IntVal   = common.COLUMN_TYPE_INT24
	StrVal   = common.COLUMN_TYPE_STRING
	FloatVal = common.COLUMN_TYPE_FLOAT
	HexNum   = common.COLUMN_TYPE_VARCHAR
	HexVal   = common.COLUMN_TYPE_SHORT
	ValArg   = common.COLUMN_TYPE_BLOB
	BitVal   = common.COLUMN_TYPE_BIT
	RowIdVal = common.COLUMN_TYPE_LONG
)

type CompareType string

// ComparisonExpr.Operator
const (
	EqualStr             CompareType = "="
	LessThanStr          CompareType = "<"
	GreaterThanStr       CompareType = ">"
	LessEqualStr         CompareType = "<="
	GreaterEqualStr      CompareType = ">="
	NotEqualStr          CompareType = "!="
	NullSafeEqualStr     CompareType = "<=>"
	InStr                CompareType = "in"
	NotInStr             CompareType = "not in"
	LikeStr              CompareType = "like"
	NotLikeStr           CompareType = "not like"
	RegexpStr            CompareType = "regexp"
	NotRegexpStr         CompareType = "not regexp"
	JSONExtractOp        CompareType = "->"
	JSONUnquoteExtractOp CompareType = "->>"
)

// UnaryExpr.Operator
const (
	UPlusStr   = "+"
	UMinusStr  = "-"
	TildaStr   = "~"
	BangStr    = "!"
	BinaryStr  = "binary "
	UBinaryStr = "_binary "
)

// BinaryExpr.Operator
const (
	BitAndStr     = "&"
	BitOrStr      = "|"
	BitXorStr     = "^"
	PlusStr       = "+"
	MinusStr      = "-"
	MultStr       = "*"
	DivStr        = "/"
	IntDivStr     = "div"
	ModStr        = "%"
	ShiftLeftStr  = "<<"
	ShiftRightStr = ">>"
)

// this string is "character set" and this comment is required
const (
	CharacterSetStr = " character set"
)

func (s CompareType) String() string {
	return string(s)
}

// basicValue 基础值实现
type basicValue struct {
	data      []byte
	typ       ValType
	valueType ValueType
}

// Type 实现Value接口的Type方法
func (v *basicValue) Type() ValueType {
	return v.valueType
}

// Value 接口实现
func (v *basicValue) Compare(other Value) int {
	if otherVal, ok := other.(*basicValue); ok {
		// 简单的字节比较，实际应该根据类型进行比较
		if len(v.data) < len(otherVal.data) {
			return -1
		} else if len(v.data) > len(otherVal.data) {
			return 1
		}
		for i, b := range v.data {
			if b < otherVal.data[i] {
				return -1
			} else if b > otherVal.data[i] {
				return 1
			}
		}
		return 0
	}
	return 0
}

func (v *basicValue) Raw() interface{} {
	return v.data
}

func (v *basicValue) ToString() string {
	return string(v.data)
}

func (v *basicValue) Bytes() []byte {
	return v.data
}

// 特殊值类型
type minValue struct{}
type maxValue struct{}

func (v *minValue) Type() ValueType {
	return ValueTypeNull
}

func (v *minValue) Compare(other Value) int {
	if _, ok := other.(*minValue); ok {
		return 0
	}
	return -1 // minValue 总是小于其他值
}

func (v *minValue) Raw() interface{} {
	return nil
}

func (v *minValue) ToString() string {
	return "MIN"
}

func (v *minValue) Bytes() []byte {
	return []byte{}
}

func (v *minValue) IsNull() bool {
	return false
}

func (v *minValue) Int() int64 {
	return math.MinInt64
}

func (v *minValue) Float64() float64 {
	return math.Inf(-1)
}

func (v *minValue) String() string {
	return "MIN"
}

func (v *minValue) Time() interface{} {
	return nil
}

func (v *minValue) Bool() bool {
	return false
}

// LessOrEqual 实现Value接口的LessOrEqual方法
func (v *minValue) LessOrEqual() (interface{}, interface{}) {
	return nil, ValueTypeNull
}

func (v *maxValue) Type() ValueType {
	return ValueTypeNull
}

func (v *maxValue) Compare(other Value) int {
	if _, ok := other.(*maxValue); ok {
		return 0
	}
	return 1 // maxValue 总是大于其他值
}

func (v *maxValue) Raw() interface{} {
	return nil
}

func (v *maxValue) ToString() string {
	return "MAX"
}

func (v *maxValue) Bytes() []byte {
	return []byte{0xFF, 0xFF, 0xFF, 0xFF}
}

func (v *maxValue) IsNull() bool {
	return false
}

func (v *maxValue) Int() int64 {
	return math.MaxInt64
}

func (v *maxValue) Float64() float64 {
	return math.Inf(1)
}

func (v *maxValue) String() string {
	return "MAX"
}

func (v *maxValue) Time() interface{} {
	return nil
}

func (v *maxValue) Bool() bool {
	return true
}

// LessOrEqual 实现Value接口的LessOrEqual方法
func (v *maxValue) LessOrEqual() (interface{}, interface{}) {
	return []byte{0xFF, 0xFF, 0xFF, 0xFF}, ValueTypeNull
}

// 构造函数实现

func NewMaxValue() Value {
	return &maxValue{}
}

func NewMinValue() Value {
	return &minValue{}
}

func NewValue(key []byte) Value {
	return &basicValue{
		data:      key,
		typ:       StrVal,
		valueType: ValueTypeVarchar,
	}
}

func NewRow(value []byte) Row {
	// 创建一个简单的行实现
	return &simpleRow{
		data: value,
	}
}

// 简单行实现
type simpleRow struct {
	data []byte
}

func (r *simpleRow) Less(than Row) bool {
	if otherRow, ok := than.(*simpleRow); ok {
		for i, b := range r.data {
			if i >= len(otherRow.data) {
				return false
			}
			if b < otherRow.data[i] {
				return true
			} else if b > otherRow.data[i] {
				return false
			}
		}
		return len(r.data) < len(otherRow.data)
	}
	return false
}

func (r *simpleRow) ToByte() []byte {
	return r.data
}

func (r *simpleRow) IsInfimumRow() bool {
	return false
}

func (r *simpleRow) IsSupremumRow() bool {
	return false
}

func (r *simpleRow) GetPageNumber() uint32 {
	return 0
}

func (r *simpleRow) WriteWithNull(content []byte) {
	// TODO: 实现
}

func (r *simpleRow) WriteBytesWithNullWithsPos(content []byte, index byte) {
	// TODO: 实现
}

func (r *simpleRow) GetRowLength() uint16 {
	return uint16(len(r.data))
}

func (r *simpleRow) GetHeaderLength() uint16 {
	return 0
}

func (r *simpleRow) GetPrimaryKey() Value {
	return NewValue(r.data)
}

func (r *simpleRow) GetFieldLength() int {
	return 1
}

func (r *simpleRow) ReadValueByIndex(index int) Value {
	if index == 0 {
		return NewValue(r.data)
	}
	return nil
}

func (r *simpleRow) SetNOwned(cnt byte) {
	// TODO: 实现
}

func (r *simpleRow) GetNOwned() byte {
	return 0
}

func (r *simpleRow) GetNextRowOffset() uint16 {
	return 0
}

func (r *simpleRow) SetNextRowOffset(offset uint16) {
	// TODO: 实现
}

func (r *simpleRow) GetHeapNo() uint16 {
	return 0
}

func (r *simpleRow) SetHeapNo(heapNo uint16) {
	// TODO: 实现
}

func (r *simpleRow) SetTransactionId(trxId uint64) {
	// TODO: 实现
}

func (r *simpleRow) GetValueByColName(colName string) Value {
	return NewValue(r.data)
}

func (r *simpleRow) ToString() string {
	return string(r.data)
}

// 添加一些辅助函数
func NewInt64Value(val int64) Value {
	return &basicValue{
		data:      int64ToBytes(val),
		typ:       IntVal,
		valueType: ValueTypeBigInt,
	}
}

func NewStringValue(val string) Value {
	return &basicValue{
		data:      []byte(val),
		typ:       StrVal,
		valueType: ValueTypeVarchar,
	}
}

func NewFloatValue(val float64) Value {
	return &basicValue{
		data:      float64ToBytes(val),
		typ:       FloatVal,
		valueType: ValueTypeDouble,
	}
}

// 辅助函数
func int64ToBytes(val int64) []byte {
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[i] = byte(val >> (8 * (7 - i)))
	}
	return result
}

func float64ToBytes(val float64) []byte {
	bits := math.Float64bits(val)
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[i] = byte(bits >> (8 * (7 - i)))
	}
	return result
}

// NewNull creates a null value
func NewNull() Value {
	return &nullValue{}
}

// NewInt creates an integer value
func NewInt(val interface{}) Value {
	switch v := val.(type) {
	case int:
		return NewInt64Value(int64(v))
	case int32:
		return NewInt64Value(int64(v))
	case int64:
		return NewInt64Value(v)
	case uint32:
		return NewInt64Value(int64(v))
	case uint64:
		return NewInt64Value(int64(v))
	default:
		return NewInt64Value(0)
	}
}

// NewFloat creates a float value
func NewFloat(val interface{}) Value {
	switch v := val.(type) {
	case float32:
		return NewFloatValue(float64(v))
	case float64:
		return NewFloatValue(v)
	default:
		return NewFloatValue(0.0)
	}
}

// NewString creates a string value
func NewString(val interface{}) Value {
	if s, ok := val.(string); ok {
		return NewStringValue(s)
	}
	return NewStringValue("")
}

// NewTime creates a time value
func NewTime(val interface{}) Value {
	// TODO: Implement proper time value
	return NewStringValue(val.(string))
}

// NewBool creates a boolean value
func NewBool(val interface{}) Value {
	if b, ok := val.(bool); ok {
		if b {
			return NewInt64Value(1)
		}
		return NewInt64Value(0)
	}
	return NewInt64Value(0)
}

// NewBytes creates a byte array value
func NewBytes(val interface{}) Value {
	if b, ok := val.([]byte); ok {
		return &basicValue{
			data:      b,
			typ:       StrVal,
			valueType: ValueTypeBinary,
		}
	}
	return &basicValue{
		data:      []byte{},
		typ:       StrVal,
		valueType: ValueTypeBinary,
	}
}

// nullValue represents a null value
type nullValue struct{}

func (v *nullValue) Type() ValueType {
	return ValueTypeNull
}

func (v *nullValue) Compare(other Value) int {
	if _, ok := other.(*nullValue); ok {
		return 0
	}
	return -1 // null is less than any non-null value
}

func (v *nullValue) Raw() interface{} {
	return nil
}

func (v *nullValue) ToString() string {
	return "NULL"
}

func (v *nullValue) Bytes() []byte {
	return nil
}

func (v *nullValue) IsNull() bool {
	return true
}

func (v *nullValue) Int() int64 {
	return 0
}

func (v *nullValue) Float64() float64 {
	return 0.0
}

func (v *nullValue) String() string {
	return ""
}

func (v *nullValue) Time() interface{} {
	return nil
}

func (v *nullValue) Bool() bool {
	return false
}

// LessOrEqual 实现Value接口的LessOrEqual方法
func (v *nullValue) LessOrEqual() (interface{}, interface{}) {
	return nil, ValueTypeNull
}

// Add IsNull and other methods to basicValue
func (v *basicValue) IsNull() bool {
	return false
}

func (v *basicValue) Int() int64 {
	// TODO: Implement proper conversion
	return 0
}

func (v *basicValue) Float64() float64 {
	// TODO: Implement proper conversion
	return 0.0
}

func (v *basicValue) String() string {
	return string(v.data)
}

func (v *basicValue) Time() interface{} {
	// TODO: Implement proper time conversion
	return nil
}

func (v *basicValue) Bool() bool {
	return len(v.data) > 0 && v.data[0] != 0
}

// LessOrEqual 实现Value接口的LessOrEqual方法
func (v *basicValue) LessOrEqual() (interface{}, interface{}) {
	return v.data, v.valueType
}
