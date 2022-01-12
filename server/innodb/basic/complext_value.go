package basic

import (
	"bytes"
	"errors"
)

//复杂数字，就按照tuple的顺序，转换成字符进行排序
type ComplexValue struct {
	content string
}

func (c ComplexValue) ToDatum() Datum {
	panic("implement me")
}

func (c ComplexValue) Raw() interface{} {
	return c.content
}

func (c ComplexValue) ToByte() []byte {
	return []byte(c.content)
}

func (c ComplexValue) DataType() ValType {
	return StrVal
}

func (c ComplexValue) Compare(x Value) (CompareType, error) {
	panic("implement me")
}

func (c ComplexValue) UnaryPlus() (Value, error) {
	panic("implement me")
}

func (c ComplexValue) UnaryMinus() (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Add(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {

		return NewBoolValue(c.content < value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) Sub(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Mul(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Div(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Pow(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Mod(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Equal(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {
		return NewBoolValue(c.content == value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) NotEqual(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {
		return NewBoolValue(c.content != value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) GreaterThan(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {
		return NewBoolValue(c.content > value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) LessThan(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {
		return NewBoolValue(c.content < value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) GreaterOrEqual(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {
		return NewBoolValue(c.content >= value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) LessOrEqual(value Value) (Value, error) {
	if int(value.DataType()) == StrVal {
		return NewBoolValue(c.content <= value.ToString()), nil
	}
	return nil, errors.New("类型不匹配")
}

func (c ComplexValue) And(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) Or(value Value) (Value, error) {
	panic("implement me")
}

func (c ComplexValue) ToString() string {
	return c.content
}

func NewComplexValue(value []Value) Value {
	var stringbuffer bytes.Buffer
	for _, v := range value {
		stringbuffer.WriteString(v.ToString())
	}
	var complextValue = new(ComplexValue)
	complextValue.content = stringbuffer.String()
	return complextValue
}
