package basic

import (
	"github.com/zhukovaskychina/xmysql-server/util"
	"strconv"
)

type IntValue struct {
	Value
	value []byte
}

func (i *IntValue) ToString() string {
	return strconv.Itoa(int(util.ReadUB4Byte2UInt32(i.value)))
}

func NewIntValue(value []byte) Value {

	var bigIntValue = new(IntValue)
	bigIntValue.value = value
	return bigIntValue
}

func (i *IntValue) Raw() interface{} {
	panic("implement me")
}

func (i *IntValue) ToByte() []byte {
	return i.value
}

func (i *IntValue) DataType() ValType {
	return IntVal
}

func (i *IntValue) Compare(x Value) (CompareType, error) {
	panic("implement me")
}

func (i *IntValue) UnaryPlus() (Value, error) {
	panic("implement me")
}

func (i *IntValue) UnaryMinus() (Value, error) {
	panic("implement me")
}

func (i *IntValue) Add(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Sub(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Mul(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Div(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Pow(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Mod(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Equal(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) NotEqual(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) GreaterThan(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) LessThan(value Value) (Value, error) {
	return NewBoolValue(util.ReadUB4Byte2UInt32(i.value) < util.ReadUB4Byte2UInt32(value.ToByte())), nil
}

func (i *IntValue) GreaterOrEqual(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) LessOrEqual(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) And(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) Or(value Value) (Value, error) {
	panic("implement me")
}

func (i *IntValue) ToDatum() Datum {
	return Datum{}
}
