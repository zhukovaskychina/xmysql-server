package table

import "github.com/zhukovaskychina/xmysql-server/util"

var RefTypeName = map[int]string{
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
var RefTypeValue = map[string]int{
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

//table 二进制文件

//用于持久化frm

type Form struct {
	FormMagicHeader    byte   //0xFE           1
	MySQLVersionId     []byte //50732			 4
	RowType            byte   //row_type_compact 1
	TableNameOffSet    []byte //				 4
	TableName          []byte //		不定长
	DataBaseNameOffSet []byte //		4
	DataBaseName       []byte //     不定长

	ColumnsLength         []byte //列长度		2byte
	FieldBytes            FieldBytesArray
	ClusterIndexOffSet    []byte //4
	ClusterIndex          []byte //不定长
	SecondaryIndexesCount byte
	SecondaryIndexes      SecondaryIndexesArray
}

type FieldBytes struct {
	FieldColumnsOffset  []byte //列存储偏移量
	FieldColumnsContent []byte //实质内容	//FormColumns 数组
}

type FieldBytesArray []FieldBytes

func (i FieldBytesArray) ToBytes() []byte {
	var buff = make([]byte, 0)
	for _, v := range i {
		buff = append(buff, v.ToBytes()...)
	}
	return buff
}
func (i FieldBytes) ToBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, i.FieldColumnsOffset...)
	buff = append(buff, i.FieldColumnsContent...)
	return buff
}

type SecondaryIndexesArray []SecondaryIndexes

func (i SecondaryIndexesArray) ToBytes() []byte {
	var buff = make([]byte, 0)
	for _, v := range i {
		buff = append(buff, v.ToBytes()...)
	}
	return buff
}

type SecondaryIndexes struct {
	SecondaryIndexesOffset []byte //4
	SecondaryIndexes       []byte
}

func (i SecondaryIndexes) ToBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, i.SecondaryIndexesOffset...)
	buff = append(buff, i.SecondaryIndexes...)
	return buff
}

func NewFormWithBytes(content []byte) *Form {
	var frm = new(Form)
	frm.FormMagicHeader = content[0]
	frm.MySQLVersionId = content[1:5]
	frm.RowType = content[5:6][0]
	frm.TableNameOffSet = content[6:10]
	var tableNameOffset = frm.calValueOffset(frm.TableNameOffSet)

	//计算不定长
	frm.TableName = content[10 : tableNameOffset+10]
	tableNameOffset = tableNameOffset + 10
	frm.DataBaseNameOffSet = content[tableNameOffset : tableNameOffset+4]
	tableNameOffset = tableNameOffset + 4

	var databaseNameOffset = frm.calValueOffset(frm.DataBaseNameOffSet)
	frm.DataBaseName = content[tableNameOffset : databaseNameOffset+tableNameOffset]

	tableNameOffset = tableNameOffset + databaseNameOffset
	//计算列宽，最大256个

	frm.ColumnsLength = content[tableNameOffset : tableNameOffset+2]
	tableNameOffset = tableNameOffset + 2

	var frmFieldArrays = make([]FieldBytes, 0)

	for i := 0; i < frm.calValueOffset(frm.ColumnsLength); i++ {
		currentFieldLengthOffset := content[tableNameOffset : tableNameOffset+4]
		currentFieldLength := frm.calValueOffset(currentFieldLengthOffset)
		tableNameOffset = tableNameOffset + 4
		fieldBytes := FieldBytes{
			FieldColumnsOffset:  currentFieldLengthOffset,
			FieldColumnsContent: content[tableNameOffset : tableNameOffset+currentFieldLength],
		}
		frmFieldArrays = append(frmFieldArrays, fieldBytes)
		tableNameOffset = tableNameOffset + currentFieldLength
	}
	frm.FieldBytes = frmFieldArrays

	//计算

	frm.SecondaryIndexesCount = content[tableNameOffset]
	var frmSecondaryIndexes = make([]SecondaryIndexes, 0)
	for i := 0; i < int(frm.SecondaryIndexesCount); i++ {
		currentFieldLengthOffset := content[tableNameOffset : tableNameOffset+4]
		currentFieldLength := frm.calValueOffset(currentFieldLengthOffset)
		tableNameOffset = tableNameOffset + 4
		secondaryIndexes := SecondaryIndexes{
			SecondaryIndexesOffset: currentFieldLengthOffset,
			SecondaryIndexes:       content[tableNameOffset : tableNameOffset+currentFieldLength],
		}
		frmSecondaryIndexes = append(frmSecondaryIndexes, secondaryIndexes)
		tableNameOffset = tableNameOffset + currentFieldLength
	}
	return frm
}

func (frm *Form) calValueOffset(bytes []byte) int {
	return util.ReadUB4Byte2Int(bytes)
}

func (frm *Form) ToBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, frm.FormMagicHeader)
	buff = append(buff, frm.MySQLVersionId...)
	buff = append(buff, frm.RowType)
	buff = append(buff, frm.TableNameOffSet...)
	buff = append(buff, frm.TableName...)
	buff = append(buff, frm.DataBaseNameOffSet...)
	buff = append(buff, frm.DataBaseName...)
	buff = append(buff, frm.ColumnsLength...)
	buff = append(buff, frm.FieldBytes.ToBytes()...)
	buff = append(buff, frm.ClusterIndexOffSet...)
	buff = append(buff, frm.ClusterIndex...)
	buff = append(buff, frm.SecondaryIndexesCount)
	buff = append(buff, frm.SecondaryIndexes.ToBytes()...)
	return buff
}
