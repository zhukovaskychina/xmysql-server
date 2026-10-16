package sqltypes

import (
	"testing"

	querypb "github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser/dependency/querypb"
)

// Explicit wire values make this independent of the shared constant definitions.
func TestMySQLTypeConversionWireContract(t *testing.T) {
	cases := []struct {
		typ         querypb.Type
		wire, flags int64
	}{
		{Int8, 1, 0}, {Uint8, 1, 32}, {Int16, 2, 0}, {Uint16, 2, 32},
		{Int32, 3, 0}, {Uint32, 3, 32}, {Float32, 4, 0}, {Float64, 5, 0},
		{Null, 6, 128}, {Timestamp, 7, 0}, {Int64, 8, 0}, {Uint64, 8, 32},
		{Int24, 9, 0}, {Uint24, 9, 32}, {Date, 10, 128}, {Time, 11, 128},
		{Datetime, 12, 128}, {Year, 13, 32}, {Bit, 16, 32}, {TypeJSON, 245, 0},
		{Decimal, 246, 0}, {Text, 252, 0}, {Blob, 252, 128}, {VarChar, 253, 0},
		{VarBinary, 253, 128}, {Char, 254, 0}, {Binary, 254, 128},
		{Enum, 254, 256}, {Set, 254, 2048}, {Geometry, 255, 0},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String(), func(t *testing.T) {
			wire, flags := TypeToMySQL(tc.typ)
			if wire != tc.wire || flags != tc.flags {
				t.Fatalf("got %d/%d, want %d/%d", wire, flags, tc.wire, tc.flags)
			}
			got, err := MySQLToType(tc.wire, tc.flags)
			if err != nil || got != tc.typ {
				t.Fatalf("reverse got %v/%v, want %v", got, err, tc.typ)
			}
		})
	}
	for _, wire := range []int64{249, 250, 251} {
		for flags, want := range map[int64]querypb.Type{0: Text, 128: Blob, 32: Text} {
			got, err := MySQLToType(wire, flags)
			if err != nil || got != want {
				t.Errorf("blob width %d flags %d: got %v/%v, want %v", wire, flags, got, err, want)
			}
		}
	}
	// These wire codes have never been accepted by this conversion table. Do
	// not accidentally substitute a querypb enum value for a MySQL type number.
	for _, wire := range []int64{0, 14, 15, 17, 18, 19, 247, 248, 256} {
		if _, err := MySQLToType(wire, 0); err == nil {
			t.Errorf("unsupported wire code %d accepted", wire)
		}
	}
}
