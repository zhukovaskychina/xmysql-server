package common

import "strings"

// GetProtocolColumnTypeInfo returns the legacy text-result column metadata.
// Prefix matching intentionally ignores display sizes, numeric scale and
// modifiers, and does not trim whitespace. DDL and CAST defaults remain separate.
func GetProtocolColumnTypeInfo(columnType string) (mysqlType byte, columnLength uint32, flags uint16, decimals byte) {
	const stringColumnLength uint32 = 255
	mysqlType = COLUMN_TYPE_VAR_STRING
	columnLength = stringColumnLength
	columnType = strings.ToLower(columnType)
	switch {
	case strings.HasPrefix(columnType, "tinyint"):
		mysqlType = COLUMN_TYPE_TINY
	case strings.HasPrefix(columnType, "smallint"):
		mysqlType = COLUMN_TYPE_SHORT
	case strings.HasPrefix(columnType, "mediumint"):
		mysqlType = COLUMN_TYPE_INT24
	case strings.HasPrefix(columnType, "int"):
		mysqlType = COLUMN_TYPE_LONG
	case strings.HasPrefix(columnType, "bigint"):
		mysqlType = COLUMN_TYPE_LONGLONG
	case strings.HasPrefix(columnType, "float"):
		mysqlType = COLUMN_TYPE_FLOAT
	case strings.HasPrefix(columnType, "double"):
		mysqlType = COLUMN_TYPE_DOUBLE
	case strings.HasPrefix(columnType, "decimal"):
		mysqlType = COLUMN_TYPE_NEWDECIMAL
	case strings.HasPrefix(columnType, "datetime"):
		mysqlType = COLUMN_TYPE_DATETIME
	case strings.HasPrefix(columnType, "timestamp"):
		mysqlType = COLUMN_TYPE_TIMESTAMP
	case strings.HasPrefix(columnType, "date"):
		mysqlType = COLUMN_TYPE_DATE
	case strings.HasPrefix(columnType, "time"):
		mysqlType = COLUMN_TYPE_TIME
	case strings.HasPrefix(columnType, "year"):
		mysqlType = COLUMN_TYPE_YEAR
	case strings.HasPrefix(columnType, "char"):
		mysqlType = COLUMN_TYPE_STRING
	case strings.HasPrefix(columnType, "varchar"):
		mysqlType = COLUMN_TYPE_VAR_STRING
	case strings.HasPrefix(columnType, "binary"):
		mysqlType = COLUMN_TYPE_STRING
		flags = uint16(BinaryFlag)
	case strings.HasPrefix(columnType, "varbinary"):
		mysqlType = COLUMN_TYPE_VAR_STRING
		flags = uint16(BinaryFlag)
	case strings.HasPrefix(columnType, "tinyblob"), strings.HasPrefix(columnType, "tinytext"):
		mysqlType = COLUMN_TYPE_TINY_BLOB
	case strings.HasPrefix(columnType, "blob"), strings.HasPrefix(columnType, "text"):
		mysqlType = COLUMN_TYPE_BLOB
	case strings.HasPrefix(columnType, "mediumblob"), strings.HasPrefix(columnType, "mediumtext"):
		mysqlType = COLUMN_TYPE_MEDIUM_BLOB
	case strings.HasPrefix(columnType, "longblob"), strings.HasPrefix(columnType, "longtext"):
		mysqlType = COLUMN_TYPE_LONG_BLOB
	case strings.HasPrefix(columnType, "enum"):
		mysqlType = COLUMN_TYPE_ENUM
	case strings.HasPrefix(columnType, "set"):
		mysqlType = COLUMN_TYPE_SET
	case strings.HasPrefix(columnType, "json"):
		mysqlType = COLUMN_TYPE_JSON
	case strings.HasPrefix(columnType, "bit"):
		mysqlType = COLUMN_TYPE_BIT
	case strings.HasPrefix(columnType, "geometry"), strings.HasPrefix(columnType, "point"),
		strings.HasPrefix(columnType, "linestring"), strings.HasPrefix(columnType, "polygon"):
		mysqlType = COLUMN_TYPE_GEOMETRY
	}

	// Integer, temporal, blob, JSON and BIT metadata share the DDL base values.
	// Negative values mean unspecified; never narrow them to unsigned wire fields.
	length, decimal := GetDefaultFieldLengthAndDecimal(mysqlType)
	if length >= 0 {
		columnLength = uint32(length)
	}
	if decimal >= 0 {
		decimals = byte(decimal)
	}

	// These are protocol projection rules, not changes to DDL/CAST defaults.
	switch mysqlType {
	case COLUMN_TYPE_STRING, COLUMN_TYPE_VAR_STRING:
		columnLength = stringColumnLength
	case COLUMN_TYPE_ENUM, COLUMN_TYPE_SET:
		columnLength = 1
	case COLUMN_TYPE_GEOMETRY:
		columnLength = ^uint32(0) // maximum length represented by the wire field
	case COLUMN_TYPE_FLOAT, COLUMN_TYPE_DOUBLE:
		decimals = 31 // non-fixed scale in column-definition packets
	}
	return
}
