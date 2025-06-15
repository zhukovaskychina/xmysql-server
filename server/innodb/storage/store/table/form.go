package table

import (
	_ "github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/util"
	"strings"
)

const (
	CompactRowType           = 0
	MySQLFrmMagicHeader      = 0xFE
	MySQLVersionId           = 50732
	colKeyNone          byte = iota
	colKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
)

/*
*
*  0x0100
auto_increment_offset表示自增长字段从那个数开始，他的取值范围是1 .. 65535
auto_increment_increment表示自增长字段每次递增的量，其默认值是1，取值范围是1 .. 65535
*/
type FormColumns struct {
	IsHidden                byte   //是否是隐藏列 0true,1false
	Type                    []byte //列类型4
	KeyType                 byte   // 0,1,2,3,4,5,6
	AutoIncrement           byte   //0,1 0 true,1 false
	NotNull                 byte   //0,1 0 true,1 false
	ZeroFill                byte
	AutoIncrementVal        []byte //2 auto_increment_offset 65535
	FieldNameOffSet         []byte //长度为4 列名偏移量
	FieldNameContent        []byte //不定长
	FieldEnumValuesOffSet   []byte //长度为4	枚举值偏移量
	FieldEnumValue          []byte //不定长
	FieldDefaultValueOffSet []byte //长度为4 默认值不定长
	FieldDefaultValueType   byte   //默认值类型 4
	FieldDefaultValue       []byte //默认值不定长
	FieldCommentValueOffSet []byte //长度为4
	FieldCommentValue       []byte //comment不定长
	FieldLength             []byte //字段长度，长度为2
}

//func ConvertBytes2Columns(buff []byte, colLength int) []FormColumns {
//	i := 0
//	formColsArrays := make([]FormColumns, colLength)
//	cursorI := 0
//	cursor := 0
//	for i = 0; i < colLength; i++ {
//		cursorI = cursor
//		cursorI, formColsArrays[i].IsHidden = util.ReadByte(buff, cursorI)
//		cursorI, formColsArrays[i].Type = util.ReadByte(buff, cursorI)
//		cursorI, formColsArrays[i].KeyType = util.ReadByte(buff, cursorI)
//		cursorI, formColsArrays[i].AutoIncrement = util.ReadByte(buff, cursorI)
//		cursorI, formColsArrays[i].NotNull = util.ReadByte(buff, cursorI)
//		cursorI, formColsArrays[i].ZeroFill = util.ReadByte(buff, cursorI)
//		_, FieldNameOffSet := util.ReadUB4(buff, cursorI)
//		cursorI = cursorI + 4
//		cursorI, fieldNameValues := util.ReadBytes(buff, cursorI, int(FieldNameOffSet))
//		formColsArrays[i].FieldName = string(fieldNameValues)
//		formColsArrays[i].FieldNameContent = fieldNameValues
//		_, fieldEnumCnt := util.ReadUB4(buff, cursorI)
//		cursorI = cursorI + 4
//		cursorI, formColsArrays[i].FieldEnumValue = util.ReadBytes(buff, cursorI, int(fieldEnumCnt))
//
//		_, DefaultOffSet := util.ReadUB4(buff, cursorI)
//		cursorI = cursorI + 4
//		cursorI, DefaultValues := util.ReadBytes(buff, cursorI, int(DefaultOffSet))
//		formColsArrays[i].FieldEnumValue = DefaultValues
//		_, commentValueOffset := util.ReadUB4(buff, cursorI)
//		cursorI = cursorI + 4
//		cursorI, formColsArrays[i].FieldCommentValue = util.ReadBytes(buff, cursorI, int(commentValueOffset))
//		fmt.Println(cursorI)
//		formColsArrays[i].FieldCommentValues = string(formColsArrays[i].FieldCommentValue)
//
//		//解析长度
//		cursorI, fieldLength := util.ReadUB4(buff, cursorI)
//		//解析FieldEnumValues
//
//		fieldEnumValueBytes := formColsArrays[i].FieldEnumValue
//		formColsArrays[i].FieldLengthVal = int(fieldLength)
//		if fieldEnumValueBytes != nil {
//			rs := make([]string, 0)
//			cursorJ := 0
//			for {
//				if cursorJ == len(fieldEnumValueBytes) {
//					break
//				}
//				_, curEls := util.ReadStringWithNull(fieldEnumValueBytes, cursorJ)
//				rs = append(rs, curEls)
//
//			}
//			formColsArrays[i].FieldEnumValues = rs
//		}
//		cursorI = cursorI + 0
//		cursor = cursorI
//	}
//	return formColsArrays
//}

func NewForm(dataBaseName string, tableName string) *Form {
	frm := new(Form)
	frm.TableName = []byte(tableName)
	frm.DataBaseName = []byte(dataBaseName)
	frm.FormMagicHeader = MySQLFrmMagicHeader
	frm.MySQLVersionId = util.ConvertInt4Bytes(MySQLVersionId)
	frm.RowType = CompactRowType
	frm.DataBaseNameOffSet = util.ConvertUInt4Bytes(uint32(len(frm.DataBaseName)))
	frm.TableNameOffSet = util.ConvertUInt4Bytes(uint32(len(frm.TableName)))
	frm.SecondaryIndexes = make([]SecondaryIndexes, 0)
	frm.FieldBytes = make([]FieldBytes, 0)

	return frm
}

func (frm *Form) InitForm() {
	frm.FormMagicHeader = MySQLFrmMagicHeader
	frm.MySQLVersionId = util.ConvertInt4Bytes(MySQLVersionId)
	frm.RowType = CompactRowType
}

//
////根据索引信息获取Index
//func (frm *Form) GetIndexByName(indexName string) *IndexDefinition {
//	for _, v := range frm.IndexDefinitions {
//		if v.Info.Name == indexName {
//			return &v
//		}
//	}
//	return nil
//}

//func convertIndexDefinitions2Bytes(indexDefinitions []IndexDefinition) []byte {
//	result := make([]byte, 0)
//	for _, v := range indexDefinitions {
//		result = append(result, convertIndexInfo2Bytes(v.Info)...)
//		for _, strV := range v.Columns {
//			result = util.WriteWithNull(result, []byte(strV))
//		}
//	}
//	return result
//}
//func convertFormColumns2Bytes(columns FormColumns) []byte {
//	rs := make([]byte, 0)
//	rs = append(rs, columns.IsHidden, columns.Type, columns.KeyType, columns.AutoIncrement, columns.NotNull, columns.ZeroFill)
//	rs = append(rs, columns.FieldNameOffSet...)
//	rs = append(rs, columns.FieldNameContent...)
//	rs = append(rs, columns.FieldEnumValuesOffSet...)
//	rs = append(rs, columns.FieldEnumValue...)
//	rs = append(rs, columns.FieldDefaultValueOffSet...)
//	rs = append(rs, columns.FieldDefaultValue...)
//	rs = append(rs, columns.FieldCommentValueOffSet...)
//	rs = append(rs, columns.FieldCommentValue...)
//	rs = append(rs, columns.FieldLength...)
//	return rs
//}

//func checkPrimaryKeyExist(columns []*sqlparser.ColumnDefinition) (bool, *sqlparser.ColumnDefinition) {
//	i := 0
//
//	for i = 0; i < len(columns); i++ {
//		keyType := byte(columns[i].Type.KeyOpt)
//		switch keyType {
//		case colKeyNone:
//			{
//
//			}
//		case colKeyPrimary:
//			{
//				return true, columns[i]
//			}
//
//		}
//	}
//	return false, nil
//}
//func (frm *Form) checkPrimaryKeyIsExists() bool {
//	return true
//}

func calFieldLength(length *sqlparser.SQLVal) int {
	if length == nil {
		return 0
	}
	//
	if len(length.Val) == 1 {
		length.Val = append(length.Val, 0, 0, 0)
	}
	if len(length.Val) == 2 {
		length.Val = append(length.Val, 0, 0)
	}
	if len(length.Val) == 3 {
		length.Val = append(length.Val, 0)
	}

	_, rs := util.ReadUB4(length.Val, 0)
	return int(rs)
}
func calEnumsNameOffset(enumValue []string) (byte, []byte) {
	if enumValue == nil {
		return 0, nil
	}
	size := len(enumValue)
	i := 0
	rs := make([]byte, 0)
	for i = 0; i < size; i++ {
		iter := []byte(strings.TrimSpace(enumValue[i]))
		rs = append(rs, iter...)
		rs = append(rs, 0x00)
	}
	return byte(size), rs
}
func calStringNameOffset(fieldName string) (int, []byte) {
	if len(strings.TrimSpace(fieldName)) == 0 {
		return 0, nil
	}
	return int(len(strings.TrimSpace(fieldName))), []byte(strings.TrimSpace(fieldName))
}

//
////
//func convertIndexInfo2Bytes(info IndexInfo) []byte {
//	result := make([]byte, 0)
//	if info.Unique {
//		result = append(result, colKeyUnique)
//	}
//	if info.Type == "index" {
//		result = append(result, colKey)
//	}
//	if info.Primary {
//		result = append(result, colKeyPrimary)
//	}
//	result = util.WriteWithNull(result, []byte(info.Name))
//	result = append(result, convertBoolToByte(info.Primary))
//	result = append(result, convertBoolToByte(info.Spatial))
//	result = append(result, convertBoolToByte(info.Unique))
//	return result
//}
//
//func ConvertByte2IndexInfo(byteData []byte) IndexInfo {
//
//	cursor, infoNameBytes := util.ReadWithNull(byteData, 1)
//
//	cursor, isPrimary := util.ReadByte(byteData, cursor)
//
//	cursor, isSpatial := util.ReadByte(byteData, cursor)
//
//	cursor, isUnique := util.ReadByte(byteData, cursor)
//
//	indexInfo := IndexInfo{
//		Type:    "",
//		Name:    string(infoNameBytes),
//		Primary: isPrimary == 0,
//		Spatial: isSpatial == 0,
//		Unique:  isUnique == 0,
//	}
//	return indexInfo
//}
