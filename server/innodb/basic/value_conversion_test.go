package basic

import (
	"testing"
)

// TestInt64Conversion 测试int64类型转换
func TestInt64Conversion(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected int64
	}{
		{
			name:     "TinyInt positive",
			value:    NewInt64(127),
			expected: 127,
		},
		{
			name:     "TinyInt negative",
			value:    NewInt64(-128),
			expected: -128,
		},
		{
			name:     "SmallInt",
			value:    NewInt64(32767),
			expected: 32767,
		},
		{
			name:     "Int",
			value:    NewInt64(2147483647),
			expected: 2147483647,
		},
		{
			name:     "BigInt",
			value:    NewInt64(9223372036854775807),
			expected: 9223372036854775807,
		},
		{
			name:     "Zero",
			value:    NewInt64(0),
			expected: 0,
		},
		{
			name:     "Negative",
			value:    NewInt64(-12345),
			expected: -12345,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.value.Int()
			if result != tt.expected {
				t.Errorf("Int() = %d, want %d", result, tt.expected)
			}
		})
	}

	t.Log("✅ Int64 conversion tests passed")
}

// TestFloat64Conversion 测试float64类型转换
func TestFloat64Conversion(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected float64
		delta    float64
	}{
		{
			name:     "Positive float",
			value:    NewFloat64(123.456),
			expected: 123.456,
			delta:    0.001,
		},
		{
			name:     "Negative float",
			value:    NewFloat64(-789.012),
			expected: -789.012,
			delta:    0.001,
		},
		{
			name:     "Zero",
			value:    NewFloat64(0.0),
			expected: 0.0,
			delta:    0.001,
		},
		{
			name:     "Large number",
			value:    NewFloat64(1234567890.123456),
			expected: 1234567890.123456,
			delta:    0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.value.Float64()
			diff := result - tt.expected
			if diff < 0 {
				diff = -diff
			}
			if diff > tt.delta {
				t.Errorf("Float64() = %f, want %f (delta %f)", result, tt.expected, tt.delta)
			}
		})
	}

	t.Log("✅ Float64 conversion tests passed")
}

// TestStringConversion 测试字符串类型转换
func TestStringConversion(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name:     "Simple string",
			value:    NewString("Hello"),
			expected: "Hello",
		},
		{
			name:     "Empty string",
			value:    NewString(""),
			expected: "",
		},
		{
			name:     "String with spaces",
			value:    NewString("Hello World"),
			expected: "Hello World",
		},
		{
			name:     "String with numbers",
			value:    NewString("12345"),
			expected: "12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.value.String()
			if result != tt.expected {
				t.Errorf("String() = %s, want %s", result, tt.expected)
			}
		})
	}

	t.Log("✅ String conversion tests passed")
}

// TestBytesConversion 测试Bytes()方法
func TestBytesConversion(t *testing.T) {
	tests := []struct {
		name  string
		value Value
	}{
		{
			name:  "Int64 value",
			value: NewInt64(12345),
		},
		{
			name:  "Float64 value",
			value: NewFloat64(123.456),
		},
		{
			name:  "String value",
			value: NewString("test"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytes := tt.value.Bytes()
			if bytes == nil {
				t.Error("Bytes() returned nil")
			}
			if len(bytes) == 0 {
				t.Error("Bytes() returned empty array")
			}
		})
	}

	t.Log("✅ Bytes conversion tests passed")
}

// TestCrossTypeConversion 测试跨类型转换
func TestCrossTypeConversion(t *testing.T) {
	t.Run("Int to Float", func(t *testing.T) {
		intVal := NewInt64(100)
		floatResult := intVal.Float64()
		if floatResult != 100.0 {
			t.Errorf("Int to Float: got %f, want 100.0", floatResult)
		}
	})

	t.Run("Float to Int", func(t *testing.T) {
		floatVal := NewFloat64(123.456)
		intResult := floatVal.Int()
		if intResult != 123 {
			t.Errorf("Float to Int: got %d, want 123", intResult)
		}
	})

	t.Run("String number to Int", func(t *testing.T) {
		strVal := NewString("12345")
		intResult := strVal.Int()
		if intResult != 12345 {
			t.Errorf("String to Int: got %d, want 12345", intResult)
		}
	})

	t.Run("String number to Float", func(t *testing.T) {
		strVal := NewString("123.456")
		floatResult := strVal.Float64()
		diff := floatResult - 123.456
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.001 {
			t.Errorf("String to Float: got %f, want 123.456", floatResult)
		}
	})

	t.Run("Negative string number", func(t *testing.T) {
		strVal := NewString("-999")
		intResult := strVal.Int()
		if intResult != -999 {
			t.Errorf("Negative string to Int: got %d, want -999", intResult)
		}
	})

	t.Log("✅ Cross-type conversion tests passed")
}

// TestNullValue 测试NULL值
func TestNullValue(t *testing.T) {
	nullVal := NewNull()

	if !nullVal.IsNull() {
		t.Error("IsNull() should return true for null value")
	}

	if nullVal.Int() != 0 {
		t.Errorf("Null Int() = %d, want 0", nullVal.Int())
	}

	if nullVal.Float64() != 0.0 {
		t.Errorf("Null Float64() = %f, want 0.0", nullVal.Float64())
	}

	if nullVal.String() != "" {
		t.Errorf("Null String() = %s, want empty string", nullVal.String())
	}

	if nullVal.Bool() != false {
		t.Error("Null Bool() should return false")
	}

	t.Log("✅ Null value tests passed")
}

// TestBoolConversion 测试布尔值转换
func TestBoolConversion(t *testing.T) {
	trueVal := NewBool(true)
	falseVal := NewBool(false)

	if trueVal.Int() != 1 {
		t.Errorf("True Bool to Int: got %d, want 1", trueVal.Int())
	}

	if falseVal.Int() != 0 {
		t.Errorf("False Bool to Int: got %d, want 0", falseVal.Int())
	}

	if trueVal.Float64() != 1.0 {
		t.Errorf("True Bool to Float: got %f, want 1.0", trueVal.Float64())
	}

	if falseVal.Float64() != 0.0 {
		t.Errorf("False Bool to Float: got %f, want 0.0", falseVal.Float64())
	}

	t.Log("✅ Bool conversion tests passed")
}
