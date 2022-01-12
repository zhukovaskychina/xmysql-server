package basic

import (
	"bytes"
	"github.com/piex/transcode"
)

type VarcharVal struct {
	value []byte
}

func (v *VarcharVal) ToDatum() Datum {
	panic("implement me")
}

func (v *VarcharVal) ToString() string {
	return string(v.value)
}

func NewVarcharVal(content []byte) Value {
	var varcharVal = new(VarcharVal)

	varcharVal.value = content
	return varcharVal
}

func (v *VarcharVal) Raw() interface{} {
	return transcode.FromByteArray(v.value).Decode("GBK").ToString()
}

func (v VarcharVal) ToByte() []byte {
	return v.value
}

func (v VarcharVal) DataType() ValType {
	return StrVal
}

func (v VarcharVal) Compare(x Value) (CompareType, error) {
	panic("implement me")
}

func (v VarcharVal) UnaryPlus() (Value, error) {
	panic("implement me")
}

func (v VarcharVal) UnaryMinus() (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Add(value Value) (Value, error) {

	v.value = append(v.value, value.ToByte()...)
	return NewVarcharVal(v.value), nil
}

func (v VarcharVal) Sub(value Value) (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Mul(value Value) (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Div(value Value) (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Pow(value Value) (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Mod(value Value) (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Equal(value Value) (Value, error) {
	//panic("implement me")
	return NewBoolValue(bytes.Compare(v.value, value.ToByte()) == 0), nil
}

func (v VarcharVal) NotEqual(value Value) (Value, error) {
	//panic("implement me")
	return NewBoolValue(bytes.Compare(v.value, value.ToByte()) == 0), nil
}

func (v VarcharVal) GreaterThan(value Value) (Value, error) {
	//panic("implement me")
	return NewBoolValue(v.ToString() > value.ToString()), nil
}

func (v VarcharVal) LessThan(value Value) (Value, error) {
	return NewBoolValue(v.ToString() < value.ToString()), nil
}

func (v VarcharVal) GreaterOrEqual(value Value) (Value, error) {
	return NewBoolValue(v.ToString() >= value.ToString()), nil
}

func (v VarcharVal) LessOrEqual(value Value) (Value, error) {
	return NewBoolValue(v.ToString() <= value.ToString()), nil
}

func (v VarcharVal) And(value Value) (Value, error) {
	panic("implement me")
}

func (v VarcharVal) Or(value Value) (Value, error) {
	panic("implement me")
}
