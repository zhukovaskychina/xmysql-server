package common

import "testing"

var _ func(string) (byte, uint32, uint16, byte) = GetProtocolColumnTypeInfo

func TestProtocolColumnDefaultsRemainSeparateFromDDLAndCast(t *testing.T) {
	tests := []struct {
		name                  string
		tp                    byte
		ddlLength, ddlDecimal int
		wireLength            uint32
		wireDecimal           byte
	}{
		{"tinyint", 1, 4, 0, 4, 0}, {"smallint", 2, 6, 0, 6, 0},
		{"int", 3, 11, 0, 11, 0}, {"bigint", 8, 20, 0, 20, 0},
		{"mediumint", 9, 9, 0, 9, 0}, {"float", 4, 12, -1, 12, 31},
		{"double", 5, 22, -1, 22, 31}, {"decimal", 246, 10, 0, 10, 0},
		{"date", 10, 10, 0, 10, 0}, {"time", 11, 10, 0, 10, 0},
		{"datetime", 12, 19, 0, 19, 0}, {"timestamp", 7, 19, 0, 19, 0},
		{"year", 13, 4, 0, 4, 0}, {"char", 254, 1, 0, 255, 0},
		{"varchar", 253, 5, 0, 255, 0}, {"enum", 247, -1, 0, 1, 0},
		{"set", 248, -1, 0, 1, 0}, {"geometry", 255, -1, -1, 4294967295, 0},
		{"tinyblob", 249, 255, 0, 255, 0}, {"blob", 252, 65535, 0, 65535, 0},
		{"mediumblob", 250, 16777215, 0, 16777215, 0},
		{"longblob", 251, 4294967295, 0, 4294967295, 0},
		{"json", 245, 4294967295, 0, 4294967295, 0}, {"bit", 16, 1, 0, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			length, decimal := GetDefaultFieldLengthAndDecimal(tt.tp)
			if length != tt.ddlLength || decimal != tt.ddlDecimal {
				t.Fatalf("DDL got %d/%d, want %d/%d", length, decimal, tt.ddlLength, tt.ddlDecimal)
			}
			tp, wireLength, flags, wireDecimal := GetProtocolColumnTypeInfo(tt.name)
			if tp != tt.tp || wireLength != tt.wireLength || flags != 0 || wireDecimal != tt.wireDecimal {
				t.Fatalf("protocol got %d/%d/%d/%d, want %d/%d/0/%d", tp, wireLength, flags, wireDecimal, tt.tp, tt.wireLength, tt.wireDecimal)
			}
		})
	}
	for tp, want := range map[byte][2]int{254: {0, -1}, 8: {22, 0}, 245: {4194304, 0}, 253: {-1, -1}} {
		length, decimal := GetDefaultFieldLengthAndDecimalForCast(tp)
		if [2]int{length, decimal} != want {
			t.Fatalf("CAST type %d: got %d/%d, want %v", tp, length, decimal, want)
		}
	}
}
