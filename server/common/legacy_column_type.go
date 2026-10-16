package common

import "strings"

// legacyColumnTypes preserves the original small public conversion API. In
// particular, its "int" means INT24, unlike the SQL parser's ordinary INT.
// Both directions consume this one table rather than maintaining two switches.
var legacyColumnTypes = map[string]byte{
	"tiny":    COLUMN_TYPE_TINY,
	"varchar": COLUMN_TYPE_VARCHAR,
	"float":   COLUMN_TYPE_FLOAT,
	"decimal": COLUMN_TYPE_DECIMAL,
	"blob":    COLUMN_TYPE_BLOB,
	"int":     COLUMN_TYPE_INT24,
}

func ConvertColTypeToEnums(colType string) byte {
	return legacyColumnTypes[strings.ToLower(colType)]
}

func ConvertColTypeToStr(colType byte) string {
	for name, code := range legacyColumnTypes {
		if code == colType {
			return name
		}
	}
	return ""
}
