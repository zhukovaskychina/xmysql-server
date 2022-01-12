package basic

type BoolValue struct {
	value bool
}

func (b *BoolValue) ToDatum() Datum {
	panic("implement me")
}

func (b *BoolValue) ToString() string {
	panic("implement me")
}

func NewBoolValue(value bool) Value {
	var boolValue = new(BoolValue)
	boolValue.value = value
	return boolValue
}

func (b *BoolValue) Raw() interface{} {
	return b.value
}

func (b BoolValue) ToByte() []byte {
	panic("implement me")
}

func (b BoolValue) DataType() ValType {
	panic("implement me")
}

func (b BoolValue) Compare(x Value) (CompareType, error) {
	panic("implement me")
}

func (b BoolValue) UnaryPlus() (Value, error) {
	panic("implement me")
}

func (b BoolValue) UnaryMinus() (Value, error) {
	panic("implement me")
}

func (b BoolValue) Add(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Sub(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Mul(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Div(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Pow(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Mod(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Equal(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) NotEqual(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) GreaterThan(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) LessThan(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) GreaterOrEqual(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) LessOrEqual(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) And(value Value) (Value, error) {
	panic("implement me")
}

func (b BoolValue) Or(value Value) (Value, error) {
	panic("implement me")
}
