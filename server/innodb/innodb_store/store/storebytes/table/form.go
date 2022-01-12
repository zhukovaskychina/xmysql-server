package table

import (
	_ "github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
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

/**
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
