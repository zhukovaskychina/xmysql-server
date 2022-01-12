package tuple

import (
	"github.com/zhukovaskychina/xmysql-server/util"
)

var RefTypeName = map[int32]string{
	0:     "NULL_TYPE",
	257:   "INT8",
	770:   "UINT8",
	259:   "INT16",
	772:   "UINT16",
	261:   "INT24",
	774:   "UINT24",
	263:   "INT32",
	776:   "UINT32",
	265:   "INT64",
	778:   "UINT64",
	1035:  "FLOAT32",
	1036:  "FLOAT64",
	2061:  "TIMESTAMP",
	2062:  "DATE",
	2063:  "TIME",
	2064:  "DATETIME",
	785:   "YEAR",
	18:    "DECIMAL",
	6163:  "TEXT",
	10260: "BLOB",
	6165:  "VARCHAR",
	10262: "VARBINARY",
	6167:  "CHAR",
	10264: "BINARY",
	2073:  "BIT",
	2074:  "ENUM",
	2075:  "SET",
	28:    "TUPLE",
	2077:  "GEOMETRY",
	2078:  "JSON",
	31:    "EXPRESSION",
}
var RefTypeValue = map[string]int32{
	"NULL_TYPE":  0,
	"INT8":       257,
	"UINT8":      770,
	"INT16":      259,
	"UINT16":     772,
	"INT24":      261,
	"UINT24":     774,
	"INT32":      263,
	"UINT32":     776,
	"INT64":      265,
	"UINT64":     778,
	"FLOAT32":    1035,
	"FLOAT64":    1036,
	"TIMESTAMP":  2061,
	"DATE":       2062,
	"TIME":       2063,
	"DATETIME":   2064,
	"YEAR":       785,
	"DECIMAL":    18,
	"TEXT":       6163,
	"BLOB":       10260,
	"VARCHAR":    6165,
	"VARBINARY":  10262,
	"CHAR":       6167,
	"BINARY":     10264,
	"BIT":        2073,
	"ENUM":       2074,
	"SET":        2075,
	"TUPLE":      28,
	"GEOMETRY":   2077,
	"JSON":       2078,
	"EXPRESSION": 31,
}

type FormColumnsWrapper struct {
	IsHidden      bool
	AutoIncrement bool
	NotNull       bool
	ZeroFill      bool
	//AutoIncrementVal  bool
	FieldType         string //4 byte
	FieldTypeIntValue int
	FieldName         string
	FieldLength       int16
	FieldCommentValue string
	FieldDefaultValue interface{}
}

func (formColumnsWrapper *FormColumnsWrapper) ToBytes() []byte {
	var buff = make([]byte, 0)

	buff = append(buff, convertBoolToByte(formColumnsWrapper.IsHidden))
	buff = append(buff, convertBoolToByte(formColumnsWrapper.AutoIncrement))
	buff = append(buff, convertBoolToByte(formColumnsWrapper.NotNull))
	buff = append(buff, convertBoolToByte(formColumnsWrapper.ZeroFill))
	//buff = append(buff, convertBoolToByte(formColumnsWrapper.AutoIncrementVal))
	buff = append(buff, util.ConvertUInt4Bytes(uint32(RefTypeValue[formColumnsWrapper.FieldType]))...)
	buff = append(buff, util.ConvertInt4Bytes(int32(formColumnsWrapper.FieldTypeIntValue))...)
	buff = append(buff, []byte(formColumnsWrapper.FieldName)...)
	buff = append(buff, util.ConvertUInt2Bytes(uint16(formColumnsWrapper.FieldLength))...)
	buff = append(buff, util.ConvertUInt4Bytes(uint32(len([]byte(formColumnsWrapper.FieldCommentValue))))...)
	buff = append(buff, []byte(formColumnsWrapper.FieldCommentValue)...)
	fieldDefaultValueBytes, _ := util.GetBytes(formColumnsWrapper.FieldDefaultValue)
	buff = append(buff, fieldDefaultValueBytes...)

	buff = append(buff)
	return buff
}

func NewFormColumnWrapper() *FormColumnsWrapper {
	var formColumnWrapper = new(FormColumnsWrapper)
	return formColumnWrapper
}

func (formColumnsWrapper *FormColumnsWrapper) InitializeFormWrapper(Ishidden bool, AutoIncrement bool,
	NotNull bool, FieldType string, FieldName string, FieldDefaultValue interface{}, FieldCommentValue interface{}, FieldLength int16) {
	formColumnsWrapper.IsHidden = Ishidden
	formColumnsWrapper.AutoIncrement = AutoIncrement
	formColumnsWrapper.NotNull = NotNull
	formColumnsWrapper.FieldType = FieldType
	formColumnsWrapper.FieldTypeIntValue = int(RefTypeValue[FieldType])
	formColumnsWrapper.FieldName = FieldName
	formColumnsWrapper.FieldDefaultValue = FieldDefaultValue
	if FieldDefaultValue == nil {
		formColumnsWrapper.FieldCommentValue = ""
	} else {
		formColumnsWrapper.FieldCommentValue = ""
	}

	formColumnsWrapper.FieldLength = FieldLength
}

func (formColumnsWrapper *FormColumnsWrapper) ParseContent(content []byte) {
	formColumnsWrapper.IsHidden = convertByteToBool(content[0])
	formColumnsWrapper.AutoIncrement = convertByteToBool(content[1])
	formColumnsWrapper.NotNull = convertByteToBool(content[2])
	formColumnsWrapper.ZeroFill = convertByteToBool(content[3])
	var cursor = 0
	cursor, fieldType := util.ReadStringWithNull(content[3:], cursor)
	formColumnsWrapper.FieldType = fieldType
	formColumnsWrapper.FieldTypeIntValue = int(RefTypeValue[fieldType])
	cursor, fieldName := util.ReadStringWithNull(content[3:], cursor)
	formColumnsWrapper.FieldName = fieldName
	cursor, fieldLength := util.ReadUB2(content[3:], cursor)
	formColumnsWrapper.FieldLength = int16(fieldLength)
	cursor, fieldComment := util.ReadStringWithNull(content[3:], cursor)
	formColumnsWrapper.FieldCommentValue = fieldComment
	cursor, defaultBytes := util.ReadBytesWithNull(content[3:], cursor)
	formColumnsWrapper.FieldDefaultValue = defaultBytes
}

func convertBoolToByte(val bool) byte {
	if val {
		return 0
	} else {
		return 1
	}
}
func convertByteToBool(val byte) bool {
	if val == 0 {
		return true
	} else {
		return false
	}
}

type IndexInfoWrapper struct {
	IndexName    string
	IndexType    string
	Primary      bool
	Spatial      bool
	Unique       bool
	IndexColumns []*FormColumnsWrapper
}

func NewIndexInfoWrapper(content []byte, colNamesMapper map[string]*FormColumnsWrapper) *IndexInfoWrapper {
	var cursor = 0
	cursor, types := util.ReadStringWithNull(content, cursor)
	cursor, indexName := util.ReadStringWithNull(content, cursor)
	cursor, primaryValue := util.ReadByte(content, cursor)
	cursor, Spatial := util.ReadByte(content, cursor)
	cursor, unique := util.ReadByte(content, cursor)
	indexInfo := IndexInfoWrapper{
		IndexType: types,
		IndexName: indexName,
		Primary:   convertByteToBool(primaryValue),
		Spatial:   convertByteToBool(Spatial),
		Unique:    convertByteToBool(unique),
	}
	var indexInfoArrays = make([]*FormColumnsWrapper, 0)
	for {
		if cursor == len(content) {
			break
		}
		cursors, colName := util.ReadStringWithNull(content, cursor)
		indexInfoArrays = append(indexInfoArrays, colNamesMapper[colName])
		cursor = cursors

	}
	indexInfo.IndexColumns = indexInfoArrays
	return &indexInfo
}

func (i *IndexInfoWrapper) ToBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, []byte(i.IndexType)...)
	buff = append(buff, []byte(i.IndexName)...)
	buff = append(buff, convertBoolToByte(i.Primary))
	buff = append(buff, convertBoolToByte(i.Spatial))
	buff = append(buff, convertBoolToByte(i.Unique))
	for _, v := range i.IndexColumns {
		buff = append(buff, []byte(v.FieldName)...)
		buff = append(buff, '0')
	}
	return buff
}
