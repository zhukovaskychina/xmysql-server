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

package mysql

// MySQL type informations.
const (
	TypeDecimal   byte = 0
	TypeTiny      byte = 1
	TypeShort     byte = 2
	TypeLong      byte = 3
	TypeFloat     byte = 4
	TypeDouble    byte = 5
	TypeNull      byte = 6
	TypeTimestamp byte = 7
	TypeLonglong  byte = 8
	TypeInt24     byte = 9
	TypeDate      byte = 10
	/* Original name was TypeTime, renamed to Duration to resolve the conflict with Go type Time.*/
	TypeDuration   byte = 11
	TypeDatetime   byte = 12
	TypeYear       byte = 13
	TypeNewDate    byte = 14
	TypeVarchar    byte = 15
	TypeBit        byte = 16
	TypeJSON       byte = 0xf5
	TypeNewDecimal byte = 0xf6
	TypeEnum       byte = 0xf7
	TypeSet        byte = 0xf8
	TypeTinyBlob   byte = 0xf9
	TypeMediumBlob byte = 0xfa
	TypeLongBlob   byte = 0xfb
	TypeBlob       byte = 0xfc
	TypeVarString  byte = 0xfd
	TypeString     byte = 0xfe
	TypeGeometry   byte = 0xff
)

var RefTypeName = map[byte]string{
	0:    "DECIMAL",
	1:    "TINY",
	2:    "SHORT",
	3:    "LONG",
	4:    "FLOAT",
	5:    "DOUBLE",
	6:    "NULL",
	7:    "TIMESTAMP",
	8:    "LONGLONG",
	9:    "INT24",
	10:   "DATE",
	11:   "TIME",
	12:   "DATETIME",
	13:   "YEAR",
	14:   "NEWDATE",
	15:   "VARCHAR",
	16:   "BIT",
	0xF6: "NEWDECIMAL",
	0xF7: "ENUM",
	0xF8: "SET",
	0xF9: "TINYBLOB",
	0xFA: "MEDIUMBLOB",
	0xFB: "LONGBLOB",
	0xFC: "BLOB",
	0xFD: "VARSTRING",
	0xFE: "STRING",
	0xFF: "GEOMETRY",
}
var RefTypeValue = map[string]byte{
	"TINY":       1,
	"SHORT":      2,
	"LONG":       3,
	"FLOAT":      4,
	"DOUBLE":     5,
	"NULL":       6,
	"TIMESTAMP":  7,
	"LONGLONG":   8,
	"INT24":      9,
	"DATE":       10,
	"TIME":       11,
	"DATETIME":   12,
	"YEAR":       13,
	"NEWDATE":    14,
	"VARCHAR":    15,
	"BIT":        16,
	"NEWDECIMAL": 0xF6,
	"ENUM":       0xF7,
	"SET":        0xF8,
	"TINYBLOB":   0xF9,
	"MEDIUMBLOB": 0xFA,
	"LONGBLOB":   0xFB,
	"BLOB":       0xFC,
	"VARSTRING":  0xFD,
	"STRING":     0xFE,
	"GEOMETRY":   0xFF,
}

// TypeUnspecified is an uninitialized type. TypeDecimal is not used in MySQL.
var TypeUnspecified = TypeDecimal

// IsUninitializedType check if a type code is uninitialized.
// TypeDecimal is the old type code for decimal and not be used in the new mysql version.
func IsUninitializedType(tp byte) bool {
	return tp == TypeDecimal
}

// Flag informations.
const (
	NotNullFlag     uint = 1   /* Field can't be NULL */
	PriKeyFlag      uint = 2   /* Field is part of a primary key */
	UniqueKeyFlag   uint = 4   /* Field is part of a unique key */
	MultipleKeyFlag uint = 8   /* Field is part of a key */
	BlobFlag        uint = 16  /* Field is a blob */
	UnsignedFlag    uint = 32  /* Field is unsigned */
	ZerofillFlag    uint = 64  /* Field is zerofill */
	BinaryFlag      uint = 128 /* Field is binary   */

	EnumFlag           uint = 256    /* Field is an enum */
	AutoIncrementFlag  uint = 512    /* Field is an auto increment field */
	TimestampFlag      uint = 1024   /* Field is a timestamp */
	SetFlag            uint = 2048   /* Field is a set */
	NoDefaultValueFlag uint = 4096   /* Field doesn't have a default value */
	OnUpdateNowFlag    uint = 8192   /* Field is set to NOW on UPDATE */
	NumFlag            uint = 32768  /* Field is a num (for clients) */
	PartKeyFlag        uint = 16384  /* Intern: Part of some keys */
	GroupFlag               = 32768  /* Intern: Group field */
	UniqueFlag              = 65536  /* Intern: Used by sql_yacc */
	BinCmpFlag              = 131072 /* Intern: Used by sql_yacc */
	ParseToJSONFlag    uint = 262144 /* Intern: Used when we want to parse string to JSON in CAST */
	IsBooleanFlag      uint = 524288 /* Intern: Used for telling boolean literal from integer */
)

// TypeInt24 bounds.
const (
	MaxUint24 = 1<<24 - 1
	MaxInt24  = 1<<23 - 1
	MinInt24  = -1 << 23
)

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
