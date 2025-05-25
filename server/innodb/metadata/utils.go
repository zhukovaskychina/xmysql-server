package metadata

import (
	"xmysql-server/server/innodb/basic"
)

// ToBasicValue converts a metadata value to a basic.Value
func ToBasicValue(value interface{}, col *Column) (basic.Value, error) {
	// Handle null values
	if value == nil {
		return basic.NewNull(), nil
	}

	switch col.DataType {
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt:
		return basic.NewInt(value), nil
	case TypeFloat, TypeDouble, TypeDecimal:
		return basic.NewFloat(value), nil
	case TypeVarchar, TypeChar, TypeText, TypeTinyText, TypeMediumText, TypeLongText:
		return basic.NewString(value), nil
	case TypeDate, TypeDateTime, TypeTime, TypeYear, TypeTimestamp:
		return basic.NewTime(value), nil
	case TypeBool, TypeBoolean:
		return basic.NewBool(value), nil
	case TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBinary, TypeVarBinary:
		return basic.NewBytes(value), nil
	default:
		return nil, basic.ErrUnsupportedType
	}
}

// FromBasicValue converts a basic.Value to the appropriate Go type based on column metadata
func FromBasicValue(val basic.Value, col *Column) (interface{}, error) {
	if val == nil || val.IsNull() {
		return nil, nil
	}

	switch col.DataType {
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt:
		return val.Int(), nil
	case TypeFloat, TypeDouble, TypeDecimal:
		return val.Float64(), nil
	case TypeVarchar, TypeChar, TypeText, TypeTinyText, TypeMediumText, TypeLongText:
		return val.String(), nil
	case TypeDate, TypeDateTime, TypeTime, TypeYear, TypeTimestamp:
		return val.Time(), nil
	case TypeBool, TypeBoolean:
		return val.Bool(), nil
	case TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBinary, TypeVarBinary:
		return val.Bytes(), nil
	default:
		return nil, basic.ErrUnsupportedType
	}
}
