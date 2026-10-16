/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	querypb "github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser/dependency/querypb"
)

// This file provides wrappers and support
// functions for querypb.Type.

// These bit flags can be used to plan on the
// common properties of types.
const (
	flagIsIntegral = int(querypb.Flag_ISINTEGRAL)
	flagIsUnsigned = int(querypb.Flag_ISUNSIGNED)
	flagIsFloat    = int(querypb.Flag_ISFLOAT)
	flagIsQuoted   = int(querypb.Flag_ISQUOTED)
	flagIsText     = int(querypb.Flag_ISTEXT)
	flagIsBinary   = int(querypb.Flag_ISBINARY)
)

// IsIntegral returns true if querypb.Type is an integral
// (signed/unsigned) that can be represented using
// up to 64 binary bits.
// If you have a Value object, use its member function.
func IsIntegral(t querypb.Type) bool {
	return int(t)&flagIsIntegral == flagIsIntegral
}

// IsSigned returns true if querypb.Type is a signed integral.
// If you have a Value object, use its member function.
func IsSigned(t querypb.Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral
}

// IsUnsigned returns true if querypb.Type is an unsigned integral.
// Caution: this is not the same as !IsSigned.
// If you have a Value object, use its member function.
func IsUnsigned(t querypb.Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral|flagIsUnsigned
}

// IsFloat returns true is querypb.Type is a floating point.
// If you have a Value object, use its member function.
func IsFloat(t querypb.Type) bool {
	return int(t)&flagIsFloat == flagIsFloat
}

// IsQuoted returns true if querypb.Type is a quoted text or binary.
// If you have a Value object, use its member function.
func IsQuoted(t querypb.Type) bool {
	return int(t)&flagIsQuoted == flagIsQuoted
}

// IsText returns true if querypb.Type is a text.
// If you have a Value object, use its member function.
func IsText(t querypb.Type) bool {
	return int(t)&flagIsText == flagIsText
}

// IsBinary returns true if querypb.Type is a binary.
// If you have a Value object, use its member function.
func IsBinary(t querypb.Type) bool {
	return int(t)&flagIsBinary == flagIsBinary
}

// isNumber returns true if the type is any type of number.
func isNumber(t querypb.Type) bool {
	return IsIntegral(t) || IsFloat(t) || t == Decimal
}

// IsTemporal returns true if Value is time type.
func IsTemporal(t querypb.Type) bool {
	switch t {
	case Timestamp, Date, Time, Datetime:
		return true
	}
	return false
}

// Vitess data types. These are idiomatically
// named synonyms for the querypb.Type values.
// Although these constants are interchangeable,
// they should be treated as different from querypb.Type.
// Use the synonyms only to refer to the type in Value.
// For proto variables, use the querypb.Type constants
// instead.
// The following conditions are non-overlapping
// and cover all types: IsSigned(), IsUnsigned(),
// IsFloat(), IsQuoted(), Null, Decimal, Expression.
// Also, IsIntegral() == (IsSigned()||IsUnsigned()).
// TestCategory needs to be updated accordingly if
// you add a new type.
// If IsBinary or IsText is true, then IsQuoted is
// also true. But there are IsQuoted types that are
// neither binary or text.
// querypb.Type_TUPLE is not included in this list
// because it's not a valid Value type.
// TODO(sougou): provide a categorization function
// that returns enums, which will allow for cleaner
// switch statements for those who want to cover types
// by their category.
const (
	Null       = querypb.Type_NULL_TYPE
	Int8       = querypb.Type_INT8
	Uint8      = querypb.Type_UINT8
	Int16      = querypb.Type_INT16
	Uint16     = querypb.Type_UINT16
	Int24      = querypb.Type_INT24
	Uint24     = querypb.Type_UINT24
	Int32      = querypb.Type_INT32
	Uint32     = querypb.Type_UINT32
	Int64      = querypb.Type_INT64
	Uint64     = querypb.Type_UINT64
	Float32    = querypb.Type_FLOAT32
	Float64    = querypb.Type_FLOAT64
	Timestamp  = querypb.Type_TIMESTAMP
	Date       = querypb.Type_DATE
	Time       = querypb.Type_TIME
	Datetime   = querypb.Type_DATETIME
	Year       = querypb.Type_YEAR
	Decimal    = querypb.Type_DECIMAL
	Text       = querypb.Type_TEXT
	Blob       = querypb.Type_BLOB
	VarChar    = querypb.Type_VARCHAR
	VarBinary  = querypb.Type_VARBINARY
	Char       = querypb.Type_CHAR
	Binary     = querypb.Type_BINARY
	Bit        = querypb.Type_BIT
	Enum       = querypb.Type_ENUM
	Set        = querypb.Type_SET
	Geometry   = querypb.Type_GEOMETRY
	TypeJSON   = querypb.Type_JSON
	Tuple      = querypb.Type_TUPLE
	Expression = querypb.Type_EXPRESSION
)

// If you add to this map, make sure you add a test_simple_protocol case
// in tabletserver/endtoend.
var mysqlToType = map[int64]querypb.Type{
	common.COLUMN_TYPE_TINY:        Int8,
	common.COLUMN_TYPE_SHORT:       Int16,
	common.COLUMN_TYPE_LONG:        Int32,
	common.COLUMN_TYPE_FLOAT:       Float32,
	common.COLUMN_TYPE_DOUBLE:      Float64,
	common.COLUMN_TYPE_NULL:        Null,
	common.COLUMN_TYPE_TIMESTAMP:   Timestamp,
	common.COLUMN_TYPE_LONGLONG:    Int64,
	common.COLUMN_TYPE_INT24:       Int24,
	common.COLUMN_TYPE_DATE:        Date,
	common.COLUMN_TYPE_TIME:        Time,
	common.COLUMN_TYPE_DATETIME:    Datetime,
	common.COLUMN_TYPE_YEAR:        Year,
	common.COLUMN_TYPE_BIT:         Bit,
	common.COLUMN_TYPE_JSON:        TypeJSON,
	common.COLUMN_TYPE_NEWDECIMAL:  Decimal,
	common.COLUMN_TYPE_TINY_BLOB:   Text,
	common.COLUMN_TYPE_MEDIUM_BLOB: Text,
	common.COLUMN_TYPE_LONG_BLOB:   Text,
	common.COLUMN_TYPE_BLOB:        Text,
	common.COLUMN_TYPE_VAR_STRING:  VarChar,
	common.COLUMN_TYPE_STRING:      Char,
	common.COLUMN_TYPE_GEOMETRY:    Geometry,
}

// modifyType modifies the vitess type based on the
// mysql flag. The function checks specific flags based
// on the type. This allows us to ignore stray flags
// that MySQL occasionally sets.
func modifyType(typ querypb.Type, flags int64) querypb.Type {
	switch typ {
	case Int8:
		if flags&int64(common.UnsignedFlag) != 0 {
			return Uint8
		}
		return Int8
	case Int16:
		if flags&int64(common.UnsignedFlag) != 0 {
			return Uint16
		}
		return Int16
	case Int32:
		if flags&int64(common.UnsignedFlag) != 0 {
			return Uint32
		}
		return Int32
	case Int64:
		if flags&int64(common.UnsignedFlag) != 0 {
			return Uint64
		}
		return Int64
	case Int24:
		if flags&int64(common.UnsignedFlag) != 0 {
			return Uint24
		}
		return Int24
	case Text:
		if flags&int64(common.BinaryFlag) != 0 {
			return Blob
		}
		return Text
	case VarChar:
		if flags&int64(common.BinaryFlag) != 0 {
			return VarBinary
		}
		return VarChar
	case Char:
		if flags&int64(common.BinaryFlag) != 0 {
			return Binary
		}
		if flags&int64(common.EnumFlag) != 0 {
			return Enum
		}
		if flags&int64(common.SetFlag) != 0 {
			return Set
		}
		return Char
	}
	return typ
}

// MySQLToType computes the vitess type from mysql type and flags.
func MySQLToType(mysqlType, flags int64) (typ querypb.Type, err error) {
	result, ok := mysqlToType[mysqlType]
	if !ok {
		return 0, fmt.Errorf("unsupported type: %d", mysqlType)
	}
	return modifyType(result, flags), nil
}

// typeToMySQL is the reverse of mysqlToType.
var typeToMySQL = map[querypb.Type]struct {
	typ   int64
	flags int64
}{
	Int8:      {typ: common.COLUMN_TYPE_TINY},
	Uint8:     {typ: common.COLUMN_TYPE_TINY, flags: int64(common.UnsignedFlag)},
	Int16:     {typ: common.COLUMN_TYPE_SHORT},
	Uint16:    {typ: common.COLUMN_TYPE_SHORT, flags: int64(common.UnsignedFlag)},
	Int32:     {typ: common.COLUMN_TYPE_LONG},
	Uint32:    {typ: common.COLUMN_TYPE_LONG, flags: int64(common.UnsignedFlag)},
	Float32:   {typ: common.COLUMN_TYPE_FLOAT},
	Float64:   {typ: common.COLUMN_TYPE_DOUBLE},
	Null:      {typ: common.COLUMN_TYPE_NULL, flags: int64(common.BinaryFlag)},
	Timestamp: {typ: common.COLUMN_TYPE_TIMESTAMP},
	Int64:     {typ: common.COLUMN_TYPE_LONGLONG},
	Uint64:    {typ: common.COLUMN_TYPE_LONGLONG, flags: int64(common.UnsignedFlag)},
	Int24:     {typ: common.COLUMN_TYPE_INT24},
	Uint24:    {typ: common.COLUMN_TYPE_INT24, flags: int64(common.UnsignedFlag)},
	Date:      {typ: common.COLUMN_TYPE_DATE, flags: int64(common.BinaryFlag)},
	Time:      {typ: common.COLUMN_TYPE_TIME, flags: int64(common.BinaryFlag)},
	Datetime:  {typ: common.COLUMN_TYPE_DATETIME, flags: int64(common.BinaryFlag)},
	Year:      {typ: common.COLUMN_TYPE_YEAR, flags: int64(common.UnsignedFlag)},
	Bit:       {typ: common.COLUMN_TYPE_BIT, flags: int64(common.UnsignedFlag)},
	TypeJSON:  {typ: common.COLUMN_TYPE_JSON},
	Decimal:   {typ: common.COLUMN_TYPE_NEWDECIMAL},
	Text:      {typ: common.COLUMN_TYPE_BLOB},
	Blob:      {typ: common.COLUMN_TYPE_BLOB, flags: int64(common.BinaryFlag)},
	VarChar:   {typ: common.COLUMN_TYPE_VAR_STRING},
	VarBinary: {typ: common.COLUMN_TYPE_VAR_STRING, flags: int64(common.BinaryFlag)},
	Char:      {typ: common.COLUMN_TYPE_STRING},
	Binary:    {typ: common.COLUMN_TYPE_STRING, flags: int64(common.BinaryFlag)},
	Enum:      {typ: common.COLUMN_TYPE_STRING, flags: int64(common.EnumFlag)},
	Set:       {typ: common.COLUMN_TYPE_STRING, flags: int64(common.SetFlag)},
	Geometry:  {typ: common.COLUMN_TYPE_GEOMETRY},
}

// TypeToMySQL returns the equivalent mysql type and flag for a vitess type.
func TypeToMySQL(typ querypb.Type) (mysqlType, flags int64) {
	val := typeToMySQL[typ]
	return val.typ, val.flags
}
