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

import "unicode"

// CharsetNameToID maps charset name to its default collation ID.
func CharsetNameToID(charset string) uint8 {
	// Use quick path for TiDB to avoid access CharsetIDs map
	// "SHOW CHARACTER SET;" to see all the supported character sets.
	if charset == "utf8mb4" {
		return UTF8MB4DefaultCollationID
	} else if charset == "binary" {
		return BinaryDefaultCollationID
	} else if charset == "utf8" {
		return UTF8DefaultCollationID
	} else if charset == "ascii" {
		return ASCIIDefaultCollationID
	} else if charset == "latin1" {
		return Latin1DefaultCollationID
	} else {
		return CharsetIDs[charset]
	}
}

// CharsetIDs maps charset name to its default collation ID.
var CharsetIDs = legacyCharsetIDs()

// Collations maps MySQL collation ID to its name.
var Collations = legacyCollations()

// CollationNames maps MySQL collation name to its ID
var CollationNames = legacyCollationNames()

// MySQL collation information.
const (
	UTF8Charset              = "utf8"
	UTF8MB4Charset           = "utf8mb4"
	Latin1Charset            = "latin1"
	DefaultCharset           = UTF8MB4Charset
	CollationUTF8GeneralCIID = 33
	CollationLatin1BinID     = 47
	CollationASCIIBinID      = 65
	CollationUTF8BinID       = 83
	CollationUTF8MB4BinID    = 46
	CollationBinaryID        = 63
	// These exported aliases preserve the legacy binary-default policy. They
	// do not select the connection or metadata defaults in the shared catalog.
	DefaultCollationID        = CollationUTF8MB4BinID
	Latin1DefaultCollationID  = CollationLatin1BinID
	ASCIIDefaultCollationID   = CollationASCIIBinID
	UTF8DefaultCollationID    = CollationUTF8BinID
	UTF8MB4DefaultCollationID = CollationUTF8MB4BinID
	BinaryDefaultCollationID  = CollationBinaryID
	CollationUTF8BinName      = "utf8_bin"
	CollationUTF8MB4BinName   = "utf8mb4_bin"
	UTF8DefaultCollation      = CollationUTF8BinName
	UTF8MB4DefaultCollation   = CollationUTF8MB4BinName
	DefaultCollationName      = UTF8MB4DefaultCollation

	// MaxBytesOfCharacter, is the max bytes length of a character,
	// refer to RFC3629, in UTF-8, characters from the U+0000..U+10FFFF range
	// (the UTF-16 accessible range) are encoded using sequences of 1 to 4 octets.
	MaxBytesOfCharacter = 4
)

// IsUTF8Charset checks if charset is utf8, utf8mb4.
func IsUTF8Charset(charset string) bool {
	return charset == UTF8Charset || charset == UTF8MB4Charset
}

// RangeGraph defines valid unicode characters to use in column names. It strictly follows MySQL's definition.
// See #3994.
var RangeGraph = []*unicode.RangeTable{
	// _MY_PNT
	unicode.No,
	unicode.Mn,
	unicode.Me,
	unicode.Pc,
	unicode.Pd,
	unicode.Pd,
	unicode.Ps,
	unicode.Pe,
	unicode.Pi,
	unicode.Pf,
	unicode.Po,
	unicode.Sm,
	unicode.Sc,
	unicode.Sk,
	unicode.So,
	// _MY_U
	unicode.Lu,
	unicode.Lt,
	unicode.Nl,
	// _MY_L
	unicode.Ll,
	unicode.Lm,
	unicode.Lo,
	unicode.Nl,
	unicode.Mn,
	unicode.Mc,
	unicode.Me,
	// _MY_NMR
	unicode.Nd,
	unicode.Nl,
	unicode.No,
}
