package metadata

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

// ToBasicValue converts a metadata value to a basic.Value
func ToBasicValue(value interface{}, col *Column) (basic.Value, error) {
	// Handle null values
	if value == nil {
		return basic.NewNull(), nil
	}
	if col != nil && col.DataType == TypeContent {
		var contentValue content.Value
		switch typed := value.(type) {
		case content.Value:
			contentValue = typed
		case []byte:
			var err error
			contentValue, err = content.NewValue("application/octet-stream", 1, typed)
			if err != nil {
				return nil, err
			}
		case string:
			var err error
			contentValue, err = content.NewValue("application/octet-stream", 1, []byte(typed))
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported CONTENT value type %T", value)
		}
		encoded, err := contentValue.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return basic.NewBytes(encoded), nil
	}
	if col != nil && col.DataType == TypeVector {
		typed, ok := value.(vector.Value)
		if !ok {
			if pointer, pointerOK := value.(*vector.Value); pointerOK && pointer != nil {
				typed, ok = *pointer, true
			}
		}
		if !ok {
			return nil, fmt.Errorf("unsupported VECTOR value type %T", value)
		}
		encoded, err := typed.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return basic.NewBytes(encoded), nil
	}

	switch col.DataType {
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeBit:
		return basic.NewInt(value), nil
	case TypeFloat, TypeDouble, TypeDecimal:
		return basic.NewFloat(value), nil
	case TypeVarchar, TypeChar, TypeText, TypeTinyText, TypeMediumText, TypeLongText:
		return basic.NewString(value), nil
	case TypeDate, TypeDateTime, TypeTime, TypeYear, TypeTimestamp:
		return basic.NewTime(value), nil
	case TypeBool, TypeBoolean:
		return basic.NewBool(value), nil
	case TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeContent, TypeBinary, TypeVarBinary, TypeVector:
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
	if col != nil && col.DataType == TypeContent {
		raw, ok := val.Raw().([]byte)
		if !ok {
			return nil, fmt.Errorf("CONTENT value has unsupported storage type %T", val.Raw())
		}
		var value content.Value
		if content.IsEncoded(raw) {
			if err := value.UnmarshalBinary(raw); err != nil {
				return nil, err
			}
			return append([]byte(nil), value.Bytes...), nil
		}
		return append([]byte(nil), raw...), nil
	}
	if col != nil && col.DataType == TypeVector {
		raw, ok := val.Raw().([]byte)
		if !ok {
			return nil, fmt.Errorf("VECTOR value has unsupported storage type %T", val.Raw())
		}
		var value vector.Value
		if err := value.UnmarshalBinary(raw); err != nil {
			return nil, err
		}
		return value, nil
	}

	switch col.DataType {
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeBit:
		return val.Int(), nil
	case TypeFloat, TypeDouble, TypeDecimal:
		return val.Float64(), nil
	case TypeVarchar, TypeChar, TypeText, TypeTinyText, TypeMediumText, TypeLongText:
		return val.String(), nil
	case TypeDate, TypeDateTime, TypeTime, TypeYear, TypeTimestamp:
		return val.Time(), nil
	case TypeBool, TypeBoolean:
		return val.Bool(), nil
	case TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeContent, TypeBinary, TypeVarBinary, TypeVector:
		return val.Bytes(), nil
	default:
		return nil, basic.ErrUnsupportedType
	}
}
