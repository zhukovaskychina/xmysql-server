package common

import "strings"

const utf8MB3CharsetAlias = "utf8mb3"

// CharsetMetadata defines the fixed charset facts. Protocol and legacy binary
// defaults are separate policies because their historical values differ.
// Zero binary/metadata IDs inherit ProtocolCollationID.
type CharsetMetadata struct {
	Name, Description                                           string
	ProtocolDescription                                         string
	MaxLen                                                      int64
	ProtocolCollationID, BinaryCollationID, MetadataCollationID uint8
}

// CollationMetadata is a fixed MySQL collation record. View projections below
// explicitly preserve the historical metadata subset, aliases, and ID 255 values.
type CollationMetadata struct {
	ID            uint8
	Name, Charset string
	SortLen       int64
	PadAttribute  string
	IsDefault     bool
}

var charsetCatalog = []CharsetMetadata{
	{"big5", "Big5 Traditional Chinese", "", 2, 1, 0, 0},
	{"dec8", "DEC West European", "", 1, 3, 0, 0},
	{"cp850", "DOS West European", "", 1, 4, 0, 0},
	{"hp8", "HP West European", "", 1, 6, 0, 0},
	{"koi8r", "KOI8-R Relcom Russian", "", 1, 7, 0, 0},
	{"latin1", "cp1252 West European", "West European", 1, 8, CollationLatin1BinID, 0},
	{"latin2", "Central European", "", 1, 9, 0, 0},
	{"swe7", "7bit Swedish", "", 1, 10, 0, 0},
	{"ascii", "US ASCII", "", 1, 11, CollationASCIIBinID, 0},
	{"ujis", "EUC-JP Japanese", "", 3, 12, 0, 0},
	{"sjis", "Shift-JIS Japanese", "", 2, 13, 0, 0},
	{"hebrew", "ISO 8859-8 Hebrew", "", 1, 16, 0, 0},
	{"tis620", "TIS620 Thai", "", 1, 18, 0, 0},
	{"euckr", "EUC-KR Korean", "", 2, 19, 0, 0},
	{"koi8u", "KOI8-U Ukrainian", "", 1, 22, 0, 0},
	{"gb2312", "GB2312 Simplified Chinese", "", 2, 24, 0, 0},
	{"greek", "ISO 8859-7 Greek", "", 1, 25, 0, 0},
	{"cp1250", "Windows Central European", "", 1, 26, 0, 0},
	{"gbk", "GBK Simplified Chinese", "", 2, 28, 0, 0},
	{"latin5", "ISO 8859-9 Turkish", "", 1, 30, 0, 0},
	{"armscii8", "ARMSCII-8 Armenian", "", 1, 32, 0, 0},
	{"utf8", "UTF-8 Unicode", "", 3, CollationUTF8GeneralCIID, CollationUTF8BinID, 0},
	{"ucs2", "UCS-2 Unicode", "", 2, 35, 0, 0},
	{"cp866", "DOS Russian", "", 1, 36, 0, 0},
	{"keybcs2", "DOS Kamenicky Czech-Slovak", "", 1, 37, 0, 0},
	{"macce", "Mac Central European", "", 1, 38, 0, 0},
	{"macroman", "Mac West European", "", 1, 39, 0, 0},
	{"cp852", "DOS Central European", "", 1, 40, 0, 0},
	{"latin7", "ISO 8859-13 Baltic", "", 1, 41, 0, 0},
	{"utf8mb4", "UTF-8 Unicode", "", 4, 45, CollationUTF8MB4BinID, 255},
	{"cp1251", "Windows Cyrillic", "", 1, 51, 0, 0},
	{"utf16", "UTF-16 Unicode", "", 4, 54, 0, 0},
	{"utf16le", "UTF-16LE Unicode", "", 4, 56, 0, 0},
	{"cp1256", "Windows Arabic", "", 1, 57, 0, 0},
	{"cp1257", "Windows Baltic", "", 1, 59, 0, 0},
	{"utf32", "UTF-32 Unicode", "", 4, 60, 0, 0},
	{"binary", "Binary pseudo charset", "", 1, CollationBinaryID, 0, 0},
	{"geostd8", "GEOSTD8 Georgian", "", 1, 92, 0, 0},
	{"cp932", "SJIS for Windows Japanese", "", 2, 95, 0, 0},
	{"eucjpms", "UJIS for Windows Japanese", "", 3, 97, 0, 0},
}

var collationCatalog = []CollationMetadata{
	{1, "big5_chinese_ci", "big5", 1, "PAD SPACE", false},
	{2, "latin2_czech_cs", "latin2", 1, "PAD SPACE", false},
	{3, "dec8_swedish_ci", "dec8", 1, "PAD SPACE", false},
	{4, "cp850_general_ci", "cp850", 1, "PAD SPACE", false},
	{5, "latin1_german1_ci", "latin1", 1, "PAD SPACE", false},
	{6, "hp8_english_ci", "hp8", 1, "PAD SPACE", false},
	{7, "koi8r_general_ci", "koi8r", 1, "PAD SPACE", false},
	{8, "latin1_swedish_ci", "latin1", 1, "PAD SPACE", true},
	{9, "latin2_general_ci", "latin2", 1, "PAD SPACE", false},
	{10, "swe7_swedish_ci", "swe7", 1, "PAD SPACE", false},
	{11, "ascii_general_ci", "ascii", 1, "PAD SPACE", false},
	{12, "ujis_japanese_ci", "ujis", 1, "PAD SPACE", false},
	{13, "sjis_japanese_ci", "sjis", 1, "PAD SPACE", false},
	{14, "cp1251_bulgarian_ci", "cp1251", 1, "PAD SPACE", false},
	{15, "latin1_danish_ci", "latin1", 1, "PAD SPACE", false},
	{16, "hebrew_general_ci", "hebrew", 1, "PAD SPACE", false},
	{18, "tis620_thai_ci", "tis620", 1, "PAD SPACE", false},
	{19, "euckr_korean_ci", "euckr", 1, "PAD SPACE", false},
	{20, "latin7_estonian_cs", "latin7", 1, "PAD SPACE", false},
	{21, "latin2_hungarian_ci", "latin2", 1, "PAD SPACE", false},
	{22, "koi8u_general_ci", "koi8u", 1, "PAD SPACE", false},
	{23, "cp1251_ukrainian_ci", "cp1251", 1, "PAD SPACE", false},
	{24, "gb2312_chinese_ci", "gb2312", 1, "PAD SPACE", false},
	{25, "greek_general_ci", "greek", 1, "PAD SPACE", false},
	{26, "cp1250_general_ci", "cp1250", 1, "PAD SPACE", false},
	{27, "latin2_croatian_ci", "latin2", 1, "PAD SPACE", false},
	{28, "gbk_chinese_ci", "gbk", 1, "PAD SPACE", false},
	{29, "cp1257_lithuanian_ci", "cp1257", 1, "PAD SPACE", false},
	{30, "latin5_turkish_ci", "latin5", 1, "PAD SPACE", false},
	{31, "latin1_german2_ci", "latin1", 1, "PAD SPACE", false},
	{32, "armscii8_general_ci", "armscii8", 1, "PAD SPACE", false},
	{CollationUTF8GeneralCIID, "utf8_general_ci", "utf8", 1, "PAD SPACE", true},
	{34, "cp1250_czech_cs", "cp1250", 1, "PAD SPACE", false},
	{35, "ucs2_general_ci", "ucs2", 1, "PAD SPACE", false},
	{36, "cp866_general_ci", "cp866", 1, "PAD SPACE", false},
	{37, "keybcs2_general_ci", "keybcs2", 1, "PAD SPACE", false},
	{38, "macce_general_ci", "macce", 1, "PAD SPACE", false},
	{39, "macroman_general_ci", "macroman", 1, "PAD SPACE", false},
	{40, "cp852_general_ci", "cp852", 1, "PAD SPACE", false},
	{41, "latin7_general_ci", "latin7", 1, "PAD SPACE", false},
	{42, "latin7_general_cs", "latin7", 1, "PAD SPACE", false},
	{43, "macce_bin", "macce", 1, "PAD SPACE", false},
	{44, "cp1250_croatian_ci", "cp1250", 1, "PAD SPACE", false},
	{45, "utf8mb4_general_ci", "utf8mb4", 1, "PAD SPACE", false},
	{CollationUTF8MB4BinID, CollationUTF8MB4BinName, "utf8mb4", 1, "PAD SPACE", false},
	{CollationLatin1BinID, "latin1_bin", "latin1", 1, "PAD SPACE", false},
	{48, "latin1_general_ci", "latin1", 1, "PAD SPACE", false},
	{49, "latin1_general_cs", "latin1", 1, "PAD SPACE", false},
	{50, "cp1251_bin", "cp1251", 1, "PAD SPACE", false},
	{51, "cp1251_general_ci", "cp1251", 1, "PAD SPACE", false},
	{52, "cp1251_general_cs", "cp1251", 1, "PAD SPACE", false},
	{53, "macroman_bin", "macroman", 1, "PAD SPACE", false},
	{54, "utf16_general_ci", "utf16", 1, "PAD SPACE", false},
	{55, "utf16_bin", "utf16", 1, "PAD SPACE", false},
	{56, "utf16le_general_ci", "utf16le", 1, "PAD SPACE", false},
	{57, "cp1256_general_ci", "cp1256", 1, "PAD SPACE", false},
	{58, "cp1257_bin", "cp1257", 1, "PAD SPACE", false},
	{59, "cp1257_general_ci", "cp1257", 1, "PAD SPACE", false},
	{60, "utf32_general_ci", "utf32", 1, "PAD SPACE", false},
	{61, "utf32_bin", "utf32", 1, "PAD SPACE", false},
	{62, "utf16le_bin", "utf16le", 1, "PAD SPACE", false},
	{CollationBinaryID, "binary", "binary", 1, "PAD SPACE", true},
	{64, "armscii8_bin", "armscii8", 1, "PAD SPACE", false},
	{CollationASCIIBinID, "ascii_bin", "ascii", 1, "PAD SPACE", false},
	{66, "cp1250_bin", "cp1250", 1, "PAD SPACE", false},
	{67, "cp1256_bin", "cp1256", 1, "PAD SPACE", false},
	{68, "cp866_bin", "cp866", 1, "PAD SPACE", false},
	{69, "dec8_bin", "dec8", 1, "PAD SPACE", false},
	{70, "greek_bin", "greek", 1, "PAD SPACE", false},
	{71, "hebrew_bin", "hebrew", 1, "PAD SPACE", false},
	{72, "hp8_bin", "hp8", 1, "PAD SPACE", false},
	{73, "keybcs2_bin", "keybcs2", 1, "PAD SPACE", false},
	{74, "koi8r_bin", "koi8r", 1, "PAD SPACE", false},
	{75, "koi8u_bin", "koi8u", 1, "PAD SPACE", false},
	{77, "latin2_bin", "latin2", 1, "PAD SPACE", false},
	{78, "latin5_bin", "latin5", 1, "PAD SPACE", false},
	{79, "latin7_bin", "latin7", 1, "PAD SPACE", false},
	{80, "cp850_bin", "cp850", 1, "PAD SPACE", false},
	{81, "cp852_bin", "cp852", 1, "PAD SPACE", false},
	{82, "swe7_bin", "swe7", 1, "PAD SPACE", false},
	{CollationUTF8BinID, CollationUTF8BinName, "utf8", 1, "PAD SPACE", false},
	{84, "big5_bin", "big5", 1, "PAD SPACE", false},
	{85, "euckr_bin", "euckr", 1, "PAD SPACE", false},
	{86, "gb2312_bin", "gb2312", 1, "PAD SPACE", false},
	{87, "gbk_bin", "gbk", 1, "PAD SPACE", false},
	{88, "sjis_bin", "sjis", 1, "PAD SPACE", false},
	{89, "tis620_bin", "tis620", 1, "PAD SPACE", false},
	{90, "ucs2_bin", "ucs2", 1, "PAD SPACE", false},
	{91, "ujis_bin", "ujis", 1, "PAD SPACE", false},
	{92, "geostd8_general_ci", "geostd8", 1, "PAD SPACE", false},
	{93, "geostd8_bin", "geostd8", 1, "PAD SPACE", false},
	{94, "latin1_spanish_ci", "latin1", 1, "PAD SPACE", false},
	{95, "cp932_japanese_ci", "cp932", 1, "PAD SPACE", false},
	{96, "cp932_bin", "cp932", 1, "PAD SPACE", false},
	{97, "eucjpms_japanese_ci", "eucjpms", 1, "PAD SPACE", false},
	{98, "eucjpms_bin", "eucjpms", 1, "PAD SPACE", false},
	{99, "cp1250_polish_ci", "cp1250", 1, "PAD SPACE", false},
	{101, "utf16_unicode_ci", "utf16", 1, "PAD SPACE", false},
	{102, "utf16_icelandic_ci", "utf16", 1, "PAD SPACE", false},
	{103, "utf16_latvian_ci", "utf16", 1, "PAD SPACE", false},
	{104, "utf16_romanian_ci", "utf16", 1, "PAD SPACE", false},
	{105, "utf16_slovenian_ci", "utf16", 1, "PAD SPACE", false},
	{106, "utf16_polish_ci", "utf16", 1, "PAD SPACE", false},
	{107, "utf16_estonian_ci", "utf16", 1, "PAD SPACE", false},
	{108, "utf16_spanish_ci", "utf16", 1, "PAD SPACE", false},
	{109, "utf16_swedish_ci", "utf16", 1, "PAD SPACE", false},
	{110, "utf16_turkish_ci", "utf16", 1, "PAD SPACE", false},
	{111, "utf16_czech_ci", "utf16", 1, "PAD SPACE", false},
	{112, "utf16_danish_ci", "utf16", 1, "PAD SPACE", false},
	{113, "utf16_lithuanian_ci", "utf16", 1, "PAD SPACE", false},
	{114, "utf16_slovak_ci", "utf16", 1, "PAD SPACE", false},
	{115, "utf16_spanish2_ci", "utf16", 1, "PAD SPACE", false},
	{116, "utf16_roman_ci", "utf16", 1, "PAD SPACE", false},
	{117, "utf16_persian_ci", "utf16", 1, "PAD SPACE", false},
	{118, "utf16_esperanto_ci", "utf16", 1, "PAD SPACE", false},
	{119, "utf16_hungarian_ci", "utf16", 1, "PAD SPACE", false},
	{120, "utf16_sinhala_ci", "utf16", 1, "PAD SPACE", false},
	{121, "utf16_german2_ci", "utf16", 1, "PAD SPACE", false},
	{122, "utf16_croatian_ci", "utf16", 1, "PAD SPACE", false},
	{123, "utf16_unicode_520_ci", "utf16", 1, "PAD SPACE", false},
	{124, "utf16_vietnamese_ci", "utf16", 1, "PAD SPACE", false},
	{128, "ucs2_unicode_ci", "ucs2", 1, "PAD SPACE", false},
	{129, "ucs2_icelandic_ci", "ucs2", 1, "PAD SPACE", false},
	{130, "ucs2_latvian_ci", "ucs2", 1, "PAD SPACE", false},
	{131, "ucs2_romanian_ci", "ucs2", 1, "PAD SPACE", false},
	{132, "ucs2_slovenian_ci", "ucs2", 1, "PAD SPACE", false},
	{133, "ucs2_polish_ci", "ucs2", 1, "PAD SPACE", false},
	{134, "ucs2_estonian_ci", "ucs2", 1, "PAD SPACE", false},
	{135, "ucs2_spanish_ci", "ucs2", 1, "PAD SPACE", false},
	{136, "ucs2_swedish_ci", "ucs2", 1, "PAD SPACE", false},
	{137, "ucs2_turkish_ci", "ucs2", 1, "PAD SPACE", false},
	{138, "ucs2_czech_ci", "ucs2", 1, "PAD SPACE", false},
	{139, "ucs2_danish_ci", "ucs2", 1, "PAD SPACE", false},
	{140, "ucs2_lithuanian_ci", "ucs2", 1, "PAD SPACE", false},
	{141, "ucs2_slovak_ci", "ucs2", 1, "PAD SPACE", false},
	{142, "ucs2_spanish2_ci", "ucs2", 1, "PAD SPACE", false},
	{143, "ucs2_roman_ci", "ucs2", 1, "PAD SPACE", false},
	{144, "ucs2_persian_ci", "ucs2", 1, "PAD SPACE", false},
	{145, "ucs2_esperanto_ci", "ucs2", 1, "PAD SPACE", false},
	{146, "ucs2_hungarian_ci", "ucs2", 1, "PAD SPACE", false},
	{147, "ucs2_sinhala_ci", "ucs2", 1, "PAD SPACE", false},
	{148, "ucs2_german2_ci", "ucs2", 1, "PAD SPACE", false},
	{149, "ucs2_croatian_ci", "ucs2", 1, "PAD SPACE", false},
	{150, "ucs2_unicode_520_ci", "ucs2", 1, "PAD SPACE", false},
	{151, "ucs2_vietnamese_ci", "ucs2", 1, "PAD SPACE", false},
	{159, "ucs2_general_mysql500_ci", "ucs2", 1, "PAD SPACE", false},
	{160, "utf32_unicode_ci", "utf32", 1, "PAD SPACE", false},
	{161, "utf32_icelandic_ci", "utf32", 1, "PAD SPACE", false},
	{162, "utf32_latvian_ci", "utf32", 1, "PAD SPACE", false},
	{163, "utf32_romanian_ci", "utf32", 1, "PAD SPACE", false},
	{164, "utf32_slovenian_ci", "utf32", 1, "PAD SPACE", false},
	{165, "utf32_polish_ci", "utf32", 1, "PAD SPACE", false},
	{166, "utf32_estonian_ci", "utf32", 1, "PAD SPACE", false},
	{167, "utf32_spanish_ci", "utf32", 1, "PAD SPACE", false},
	{168, "utf32_swedish_ci", "utf32", 1, "PAD SPACE", false},
	{169, "utf32_turkish_ci", "utf32", 1, "PAD SPACE", false},
	{170, "utf32_czech_ci", "utf32", 1, "PAD SPACE", false},
	{171, "utf32_danish_ci", "utf32", 1, "PAD SPACE", false},
	{172, "utf32_lithuanian_ci", "utf32", 1, "PAD SPACE", false},
	{173, "utf32_slovak_ci", "utf32", 1, "PAD SPACE", false},
	{174, "utf32_spanish2_ci", "utf32", 1, "PAD SPACE", false},
	{175, "utf32_roman_ci", "utf32", 1, "PAD SPACE", false},
	{176, "utf32_persian_ci", "utf32", 1, "PAD SPACE", false},
	{177, "utf32_esperanto_ci", "utf32", 1, "PAD SPACE", false},
	{178, "utf32_hungarian_ci", "utf32", 1, "PAD SPACE", false},
	{179, "utf32_sinhala_ci", "utf32", 1, "PAD SPACE", false},
	{180, "utf32_german2_ci", "utf32", 1, "PAD SPACE", false},
	{181, "utf32_croatian_ci", "utf32", 1, "PAD SPACE", false},
	{182, "utf32_unicode_520_ci", "utf32", 1, "PAD SPACE", false},
	{183, "utf32_vietnamese_ci", "utf32", 1, "PAD SPACE", false},
	{192, "utf8_unicode_ci", "utf8", 1, "PAD SPACE", false},
	{193, "utf8_icelandic_ci", "utf8", 1, "PAD SPACE", false},
	{194, "utf8_latvian_ci", "utf8", 1, "PAD SPACE", false},
	{195, "utf8_romanian_ci", "utf8", 1, "PAD SPACE", false},
	{196, "utf8_slovenian_ci", "utf8", 1, "PAD SPACE", false},
	{197, "utf8_polish_ci", "utf8", 1, "PAD SPACE", false},
	{198, "utf8_estonian_ci", "utf8", 1, "PAD SPACE", false},
	{199, "utf8_spanish_ci", "utf8", 1, "PAD SPACE", false},
	{200, "utf8_swedish_ci", "utf8", 1, "PAD SPACE", false},
	{201, "utf8_turkish_ci", "utf8", 1, "PAD SPACE", false},
	{202, "utf8_czech_ci", "utf8", 1, "PAD SPACE", false},
	{203, "utf8_danish_ci", "utf8", 1, "PAD SPACE", false},
	{204, "utf8_lithuanian_ci", "utf8", 1, "PAD SPACE", false},
	{205, "utf8_slovak_ci", "utf8", 1, "PAD SPACE", false},
	{206, "utf8_spanish2_ci", "utf8", 1, "PAD SPACE", false},
	{207, "utf8_roman_ci", "utf8", 1, "PAD SPACE", false},
	{208, "utf8_persian_ci", "utf8", 1, "PAD SPACE", false},
	{209, "utf8_esperanto_ci", "utf8", 1, "PAD SPACE", false},
	{210, "utf8_hungarian_ci", "utf8", 1, "PAD SPACE", false},
	{211, "utf8_sinhala_ci", "utf8", 1, "PAD SPACE", false},
	{212, "utf8_german2_ci", "utf8", 1, "PAD SPACE", false},
	{213, "utf8_croatian_ci", "utf8", 1, "PAD SPACE", false},
	{214, "utf8_unicode_520_ci", "utf8", 1, "PAD SPACE", false},
	{215, "utf8_vietnamese_ci", "utf8", 1, "PAD SPACE", false},
	{223, "utf8_general_mysql500_ci", "utf8", 1, "PAD SPACE", false},
	{224, "utf8mb4_unicode_ci", "utf8mb4", 1, "PAD SPACE", false},
	{225, "utf8mb4_icelandic_ci", "utf8mb4", 1, "PAD SPACE", false},
	{226, "utf8mb4_latvian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{227, "utf8mb4_romanian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{228, "utf8mb4_slovenian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{229, "utf8mb4_polish_ci", "utf8mb4", 1, "PAD SPACE", false},
	{230, "utf8mb4_estonian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{231, "utf8mb4_spanish_ci", "utf8mb4", 1, "PAD SPACE", false},
	{232, "utf8mb4_swedish_ci", "utf8mb4", 1, "PAD SPACE", false},
	{233, "utf8mb4_turkish_ci", "utf8mb4", 1, "PAD SPACE", false},
	{234, "utf8mb4_czech_ci", "utf8mb4", 1, "PAD SPACE", false},
	{235, "utf8mb4_danish_ci", "utf8mb4", 1, "PAD SPACE", false},
	{236, "utf8mb4_lithuanian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{237, "utf8mb4_slovak_ci", "utf8mb4", 1, "PAD SPACE", false},
	{238, "utf8mb4_spanish2_ci", "utf8mb4", 1, "PAD SPACE", false},
	{239, "utf8mb4_roman_ci", "utf8mb4", 1, "PAD SPACE", false},
	{240, "utf8mb4_persian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{241, "utf8mb4_esperanto_ci", "utf8mb4", 1, "PAD SPACE", false},
	{242, "utf8mb4_hungarian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{243, "utf8mb4_sinhala_ci", "utf8mb4", 1, "PAD SPACE", false},
	{244, "utf8mb4_german2_ci", "utf8mb4", 1, "PAD SPACE", false},
	{245, "utf8mb4_croatian_ci", "utf8mb4", 1, "PAD SPACE", false},
	{246, "utf8mb4_unicode_520_ci", "utf8mb4", 1, "PAD SPACE", false},
	{247, "utf8mb4_vietnamese_ci", "utf8mb4", 1, "PAD SPACE", false},
	{255, "utf8mb4_0900_ai_ci", "utf8mb4", 0, "NO PAD", true},
}

// ProtocolCharsetIDs returns a fresh compatibility map for connection charsets.
func ProtocolCharsetIDs() map[string]uint8 {
	result := make(map[string]uint8, len(charsetCatalog))
	for _, charset := range charsetCatalog {
		result[charset.Name] = charset.ProtocolCollationID
	}
	return result
}

func legacyCharsetIDs() map[string]uint8 {
	result := make(map[string]uint8, len(charsetCatalog))
	for _, charset := range charsetCatalog {
		id := charset.BinaryCollationID
		if id == 0 {
			id = charset.ProtocolCollationID
		}
		result[charset.Name] = id
	}
	return result
}

func legacyCollations() map[uint8]string {
	result := make(map[uint8]string, len(collationCatalog))
	for _, collation := range collationCatalog {
		result[collation.ID] = collation.Name
	}
	return result
}

func legacyCollationNames() map[string]uint8 {
	result := make(map[string]uint8, len(collationCatalog))
	for _, collation := range collationCatalog {
		result[collation.Name] = collation.ID
	}
	return result
}

func collationByID(id uint8) CollationMetadata {
	for _, collation := range collationCatalog {
		if collation.ID == id {
			return collation
		}
	}
	panic("unknown fixed collation ID")
}

func charsetByName(name string) CharsetMetadata {
	for _, charset := range charsetCatalog {
		if charset.Name == name {
			return charset
		}
	}
	panic("unknown fixed charset")
}

// InformationSchemaCollations preserves the original four-row compatibility
// view. ID 255 intentionally projects PAD SPACE / sortlen 1 here only.
func InformationSchemaCollations() []CollationMetadata {
	result := make([]CollationMetadata, 0, 4)
	for _, id := range []uint8{255, 45, CollationUTF8MB4BinID, 8} {
		collation := collationByID(id)
		if id == 255 {
			collation.SortLen = 1
			collation.PadAttribute = "PAD SPACE"
		}
		result = append(result, collation)
	}
	return result
}

// ShowCollations preserves SHOW's subset and ordered utf8mb3 alias for ID CollationUTF8GeneralCIID.
func ShowCollations() []CollationMetadata {
	result := make([]CollationMetadata, 0, 5)
	for _, id := range []uint8{255, 45, CollationUTF8GeneralCIID, CollationUTF8GeneralCIID, CollationBinaryID} {
		collation := collationByID(id)
		if id == CollationUTF8GeneralCIID && len(result) == 2 {
			collation.Name = strings.Replace(collation.Name, collation.Charset+"_", utf8MB3CharsetAlias+"_", 1)
			collation.Charset = utf8MB3CharsetAlias
		}
		result = append(result, collation)
	}
	return result
}

// InformationSchemaCharsets preserves the original three-row view.
func InformationSchemaCharsets() []CharsetMetadata {
	return []CharsetMetadata{charsetByName("utf8mb4"), charsetByName("utf8"), charsetByName("latin1")}
}

// ShowCharsets retains the utf8mb3 alias without changing the protocol map.
func ShowCharsets() []CharsetMetadata {
	alias := charsetByName("utf8")
	alias.Name = utf8MB3CharsetAlias
	return []CharsetMetadata{charsetByName("utf8mb4"), alias, charsetByName("utf8"), charsetByName("binary")}
}

// MetadataCollationName applies only the SHOW CHARACTER SET alias projection.
func (charset CharsetMetadata) MetadataCollationName() string {
	id := charset.MetadataCollationID
	if id == 0 {
		id = charset.ProtocolCollationID
	}
	name := collationByID(id).Name
	if charset.Name == utf8MB3CharsetAlias {
		return strings.Replace(name, UTF8Charset+"_", utf8MB3CharsetAlias+"_", 1)
	}
	return name
}

// ProtocolCharsetMetadata is the charset manager's wire-facing projection.
// Its ID is a collation ID, and MaxLen intentionally retains uint8.
type ProtocolCharsetMetadata struct {
	ID              uint8
	Name, Collation string
	MaxLen          uint8
	Description     string
}

// ProtocolCollationMetadata preserves the manager's uint16 ID API even though
// the historical support set contains only IDs representable in one byte.
type ProtocolCollationMetadata struct {
	ID            uint16
	Name, Charset string
	IsDefault     bool
}

// ProtocolManagerCharsets preserves the original ordered support selection.
// First-occurrence name lookup deliberately picks latin1=5, latin2=2 and
// utf8mb4=45; that differs from both connection defaults and metadata defaults.
func ProtocolManagerCharsets() []ProtocolCharsetMetadata {
	ids := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 18, 19, 22, 24, 25, 26, 28, 30, 32, 33, 35, 36, 37, 38, 39, 40, 41, 45, 46, 51, 54, 56, 57, 59, 60, 63, 83, 92, 95, 97, 224, 255}
	result := make([]ProtocolCharsetMetadata, 0, len(ids))
	for _, id := range ids {
		collation := collationByID(id)
		charset := charsetByName(collation.Charset)
		description := charset.Description
		if charset.ProtocolDescription != "" {
			description = charset.ProtocolDescription
		}
		result = append(result, ProtocolCharsetMetadata{id, charset.Name, collation.Name, uint8(charset.MaxLen), description})
	}
	return result
}

// ProtocolManagerCollations retains its narrower 17-row support set and
// explicit default flags rather than borrowing information_schema defaults.
func ProtocolManagerCollations() []ProtocolCollationMetadata {
	selection := []struct {
		id        uint8
		isDefault bool
	}{
		{1, true},
		{8, true},
		{9, true},
		{11, true},
		{13, true},
		{19, true},
		{24, true},
		{25, true},
		{26, true},
		{28, true},
		{33, true},
		{45, false},
		{46, false},
		{63, true},
		{83, false},
		{224, false},
		{255, true},
	}
	result := make([]ProtocolCollationMetadata, 0, len(selection))
	for _, projection := range selection {
		collation := collationByID(projection.id)
		result = append(result, ProtocolCollationMetadata{uint16(collation.ID), collation.Name, collation.Charset, projection.isDefault})
	}
	return result
}

// ProtocolPreferredCharsetID serves the four fixed convenience getters. This
// policy intentionally uses metadata defaults (utf8mb4=255), whereas name
// lookup in the manager uses the first supported collation (utf8mb4=45).
func ProtocolPreferredCharsetID(name string) uint8 {
	charset := charsetByName(name)
	if charset.MetadataCollationID != 0 {
		return charset.MetadataCollationID
	}
	return charset.ProtocolCollationID
}

// NormalizeProtocolCharsetName preserves the manager's exact alias behavior.
func NormalizeProtocolCharsetName(name string) string {
	name = strings.ToLower(name)
	switch name {
	case "utf-8", utf8MB3CharsetAlias:
		return UTF8Charset
	case "utf-8mb4":
		return UTF8MB4Charset
	default:
		return name
	}
}

// DispatcherCharsets preserves the dispatcher's five-row SHOW CHARSET view.
// The dispatcher converts MaxLen to int while the engine uses int64/string.
func DispatcherCharsets() []CharsetMetadata {
	result := make([]CharsetMetadata, 0, 5)
	for _, name := range []string{UTF8Charset, UTF8MB4Charset, Latin1Charset, "ascii", "binary"} {
		result = append(result, charsetByName(name))
	}
	return result
}

// DispatcherCollations explicitly retains binary NO PAD and ascii's default
// flag. These differ from the engine view and are compatibility projections.
func DispatcherCollations() []CollationMetadata {
	result := make([]CollationMetadata, 0, 6)
	for _, id := range []uint8{255, 45, CollationUTF8GeneralCIID, 8, 11, CollationBinaryID} {
		collation := collationByID(id)
		collation.IsDefault = id != 45
		if id == CollationBinaryID {
			collation.PadAttribute = "NO PAD"
		}
		result = append(result, collation)
	}
	return result
}

// SchemaCharsetCollations is the schema manager's intentionally limited DDL
// support policy. Selecting IDs from the catalog does not accept every known
// collation or change case/alias handling at this validation boundary.
func SchemaCharsetCollations(name string) ([]string, bool) {
	selection := map[string][]uint8{
		UTF8MB4Charset: {45, 224, CollationUTF8MB4BinID, 255},
		UTF8Charset:    {CollationUTF8GeneralCIID, 192, CollationUTF8BinID},
		Latin1Charset:  {8, 48, CollationLatin1BinID},
		"ascii":        {11, CollationASCIIBinID},
		"binary":       {CollationBinaryID},
	}
	ids, ok := selection[name]
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, collationByID(id).Name)
	}
	return result, true
}
