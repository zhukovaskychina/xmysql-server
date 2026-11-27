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

// fieldDefault owns DDL defaults and CAST applicability/overrides.
// Shared CAST defaults refer to ddl instead of repeating values in a second table.
type fieldDefault struct {
	length, decimal int
	integer, cast   bool
	castOverride    *[2]int
}

var fieldDefaults = map[byte]fieldDefault{
	TypeBit:        {length: 1, decimal: 0},
	TypeTiny:       {length: 4, decimal: 0, integer: true},
	TypeShort:      {length: 6, decimal: 0, integer: true},
	TypeInt24:      {length: 9, decimal: 0, integer: true},
	TypeLong:       {length: 11, decimal: 0, integer: true},
	TypeLonglong:   {length: 20, decimal: 0, integer: true, cast: true, castOverride: &[2]int{22, 0}},
	TypeDouble:     {length: 22, decimal: -1, cast: true},
	TypeFloat:      {length: 12, decimal: -1, cast: true},
	TypeNewDecimal: {length: 10, decimal: 0, cast: true},
	TypeDuration:   {length: 10, decimal: 0, cast: true},
	TypeDate:       {length: 10, decimal: 0, cast: true},
	TypeTimestamp:  {length: 19, decimal: 0},
	TypeDatetime:   {length: 19, decimal: 0, cast: true},
	TypeYear:       {length: 4, decimal: 0},
	TypeString:     {length: 1, decimal: 0, cast: true, castOverride: &[2]int{0, -1}},
	TypeVarchar:    {length: 5, decimal: 0},
	TypeVarString:  {length: 5, decimal: 0},
	TypeTinyBlob:   {length: 255, decimal: 0},
	TypeBlob:       {length: 65535, decimal: 0},
	TypeMediumBlob: {length: 16777215, decimal: 0},
	TypeLongBlob:   {length: 4294967295, decimal: 0},
	TypeJSON:       {length: 4294967295, decimal: 0, cast: true, castOverride: &[2]int{4194304, 0}},
	TypeNull:       {length: 0, decimal: 0},
	TypeSet:        {length: -1, decimal: 0},
	TypeEnum:       {length: -1, decimal: 0},
}

// IsIntegerType reports whether tp is an integer field type.
func IsIntegerType(tp byte) bool { return fieldDefaults[tp].integer }

// GetDefaultFieldLengthAndDecimal preserves the public DDL default API.
func GetDefaultFieldLengthAndDecimal(tp byte) (flen int, decimal int) {
	if defaults, ok := fieldDefaults[tp]; ok {
		return defaults.length, defaults.decimal
	}
	return -1, -1
}

// GetDefaultFieldLengthAndDecimalForCast preserves the public CAST default API.
func GetDefaultFieldLengthAndDecimalForCast(tp byte) (flen int, decimal int) {
	defaults, ok := fieldDefaults[tp]
	if !ok || !defaults.cast {
		return -1, -1
	}
	if defaults.castOverride != nil {
		return defaults.castOverride[0], defaults.castOverride[1]
	}
	return defaults.length, defaults.decimal
}
