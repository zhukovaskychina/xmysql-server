package basic

import (
	"bytes"
	"strconv"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type BigIntValue struct {
	value []byte
}

func (b BigIntValue) ToDatum() Datum {
	panic("implement me")
}

func (b BigIntValue) ToString() string {
	uint := util.ReadUB8Byte2Long(b.value)
	return strconv.FormatUint(uint, 10)
}

func NewBigIntValue(value []byte) Value {

	var bigIntValue = new(BigIntValue)
	bigIntValue.value = value
	return bigIntValue
}

func (b BigIntValue) Raw() interface{} {
	return util.ReadUB8Bytes2Long(b.value)
}

func (b BigIntValue) ToByte() []byte {
	return b.value
}

func (b BigIntValue) DataType() ValType {
	return RowIdVal
}

func (b BigIntValue) Compare(x Value) (CompareType, error) {
	panic("implement me")
}

func (b BigIntValue) UnaryPlus() (Value, error) {
	panic("implement me")
}

func (b BigIntValue) UnaryMinus() (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Add(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Sub(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Mul(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Div(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Pow(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Mod(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Equal(value Value) (Value, error) {

	return NewBoolValue(bytes.Compare(b.value, value.ToByte()) == 0), nil
}

func (b BigIntValue) NotEqual(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) GreaterThan(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) LessThan(value Value) (Value, error) {
	second := value.Raw().(int64)
	first := b.Raw().(int64)
	return NewBoolValue(first < second), nil
}

func (b BigIntValue) GreaterOrEqual(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) LessOrEqual(value Value) (Value, error) {
	second := value.Raw().(int64)
	first := b.Raw().(int64)
	return NewBoolValue(first <= second), nil
}

func (b BigIntValue) And(value Value) (Value, error) {
	panic("implement me")
}

func (b BigIntValue) Or(value Value) (Value, error) {
	panic("implement me")
}
