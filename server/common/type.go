// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

// MySQL type information. Type* names preserve the parser's byte API while
// COLUMN_TYPE_* is the single source for MySQL wire type numbers.
const (
	TypeUnspecified byte = 0
	TypeTiny        byte = COLUMN_TYPE_TINY  // TINYINT
	TypeShort       byte = COLUMN_TYPE_SHORT // SMALLINT
	TypeLong        byte = COLUMN_TYPE_LONG  // INT
	TypeFloat       byte = COLUMN_TYPE_FLOAT
	TypeDouble      byte = COLUMN_TYPE_DOUBLE
	TypeNull        byte = COLUMN_TYPE_NULL
	TypeTimestamp   byte = COLUMN_TYPE_TIMESTAMP
	TypeLonglong    byte = COLUMN_TYPE_LONGLONG // BIGINT
	TypeInt24       byte = COLUMN_TYPE_INT24    // MEDIUMINT
	TypeDate        byte = COLUMN_TYPE_DATE
	/* TypeDuration original name was TypeTime, renamed to TypeDuration to resolve the conflict with Go type Time.*/
	TypeDuration byte = COLUMN_TYPE_TIME
	TypeDatetime byte = COLUMN_TYPE_DATETIME
	TypeYear     byte = COLUMN_TYPE_YEAR
	TypeNewDate  byte = COLUMN_TYPE_NEWDATE
	TypeVarchar  byte = COLUMN_TYPE_VARCHAR
	TypeBit      byte = COLUMN_TYPE_BIT

	TypeJSON       byte = COLUMN_TYPE_JSON
	TypeNewDecimal byte = COLUMN_TYPE_NEWDECIMAL
	TypeEnum       byte = COLUMN_TYPE_ENUM
	TypeSet        byte = COLUMN_TYPE_SET
	TypeTinyBlob   byte = COLUMN_TYPE_TINY_BLOB
	TypeMediumBlob byte = COLUMN_TYPE_MEDIUM_BLOB
	TypeLongBlob   byte = COLUMN_TYPE_LONG_BLOB
	TypeBlob       byte = COLUMN_TYPE_BLOB
	TypeVarString  byte = COLUMN_TYPE_VAR_STRING
	TypeString     byte = COLUMN_TYPE_STRING /* TypeString is char type */
	TypeGeometry   byte = COLUMN_TYPE_GEOMETRY
)

// Flag information.
const (
	NotNullFlag        uint = 1 << 0  /* Field can't be NULL */
	PriKeyFlag         uint = 1 << 1  /* Field is part of a primary key */
	UniqueKeyFlag      uint = 1 << 2  /* Field is part of a unique key */
	MultipleKeyFlag    uint = 1 << 3  /* Field is part of a key */
	BlobFlag           uint = 1 << 4  /* Field is a blob */
	UnsignedFlag       uint = 1 << 5  /* Field is unsigned */
	ZerofillFlag       uint = 1 << 6  /* Field is zerofill */
	BinaryFlag         uint = 1 << 7  /* Field is binary   */
	EnumFlag           uint = 1 << 8  /* Field is an enum */
	AutoIncrementFlag  uint = 1 << 9  /* Field is an auto increment field */
	TimestampFlag      uint = 1 << 10 /* Field is a timestamp */
	SetFlag            uint = 1 << 11 /* Field is a set */
	NoDefaultValueFlag uint = 1 << 12 /* Field doesn't have a default value */
	OnUpdateNowFlag    uint = 1 << 13 /* Field is set to NOW on UPDATE */
	PartKeyFlag        uint = 1 << 14 /* Intern: Part of some keys */
	NumFlag            uint = 1 << 15 /* Field is a num (for clients) */

	GroupFlag             uint = 1 << 15 /* Internal: Group field */
	UniqueFlag            uint = 1 << 16 /* Internal: Used by sql_yacc */
	BinCmpFlag            uint = 1 << 17 /* Internal: Used by sql_yacc */
	ParseToJSONFlag       uint = 1 << 18 /* Internal: Used when we want to parse string to JSON in CAST */
	IsBooleanFlag         uint = 1 << 19 /* Internal: Used for telling boolean literal from integer */
	PreventNullInsertFlag uint = 1 << 20 /* Prevent this Field from inserting NULL values */
	EnumSetAsIntFlag      uint = 1 << 21 /* Internal: Used for inferring enum eval type. */
	DropColumnIndexFlag   uint = 1 << 22 /* Internal: Used for indicate the column is being dropped with index */
)

// TypeInt24 bounds.
const (
	MaxUint24 = 1<<24 - 1
	MaxInt24  = 1<<23 - 1
	MinInt24  = -1 << 23
)

// HasDropColumnWithIndexFlag checks if DropColumnIndexFlag is set.
func HasDropColumnWithIndexFlag(flag uint) bool {
	return (flag & DropColumnIndexFlag) > 0
}

// HasNotNullFlag checks if NotNullFlag is set.
func HasNotNullFlag(flag uint) bool {
	return (flag & NotNullFlag) > 0
}

// HasNoDefaultValueFlag checks if NoDefaultValueFlag is set.
func HasNoDefaultValueFlag(flag uint) bool {
	return (flag & NoDefaultValueFlag) > 0
}

// HasAutoIncrementFlag checks if AutoIncrementFlag is set.
func HasAutoIncrementFlag(flag uint) bool {
	return (flag & AutoIncrementFlag) > 0
}

// HasUnsignedFlag checks if UnsignedFlag is set.
func HasUnsignedFlag(flag uint) bool {
	return (flag & UnsignedFlag) > 0
}

// HasZerofillFlag checks if ZerofillFlag is set.
func HasZerofillFlag(flag uint) bool {
	return (flag & ZerofillFlag) > 0
}

// HasBinaryFlag checks if BinaryFlag is set.
func HasBinaryFlag(flag uint) bool {
	return (flag & BinaryFlag) > 0
}

// HasPriKeyFlag checks if PriKeyFlag is set.
func HasPriKeyFlag(flag uint) bool {
	return (flag & PriKeyFlag) > 0
}

// HasUniKeyFlag checks if UniqueKeyFlag is set.
func HasUniKeyFlag(flag uint) bool {
	return (flag & UniqueKeyFlag) > 0
}

// HasMultipleKeyFlag checks if MultipleKeyFlag is set.
func HasMultipleKeyFlag(flag uint) bool {
	return (flag & MultipleKeyFlag) > 0
}

// HasTimestampFlag checks if HasTimestampFlag is set.
func HasTimestampFlag(flag uint) bool {
	return (flag & TimestampFlag) > 0
}

// HasOnUpdateNowFlag checks if OnUpdateNowFlag is set.
func HasOnUpdateNowFlag(flag uint) bool {
	return (flag & OnUpdateNowFlag) > 0
}

// HasParseToJSONFlag checks if ParseToJSONFlag is set.
func HasParseToJSONFlag(flag uint) bool {
	return (flag & ParseToJSONFlag) > 0
}

// HasIsBooleanFlag checks if IsBooleanFlag is set.
func HasIsBooleanFlag(flag uint) bool {
	return (flag & IsBooleanFlag) > 0
}

// HasPreventNullInsertFlag checks if PreventNullInsertFlag is set.
func HasPreventNullInsertFlag(flag uint) bool {
	return (flag & PreventNullInsertFlag) > 0
}

// HasEnumSetAsIntFlag checks if EnumSetAsIntFlag is set.
func HasEnumSetAsIntFlag(flag uint) bool {
	return (flag & EnumSetAsIntFlag) > 0
}
