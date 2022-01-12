package basic

type Row interface {
	//根据Row的主键值，或者是比较值做排序
	//TODO 查询下多列组成的key 如何排序，string 如何排序
	Less(than Row) bool

	//持久化成byte数组
	ToByte() []byte

	IsInfimumRow() bool

	IsSupremumRow() bool

	//获取页面号
	GetPageNumber() uint32

	WriteWithNull(content []byte)

	WriteBytesWithNullWithsPos(content []byte, index byte)

	GetRowLength() uint16 //获取行的实际长度，行的头里面会计算，header长度+value的长度

	//获取文件头长度
	GetHeaderLength() uint16

	GetPrimaryKey() Value

	GetFieldLength() int //获取当前列的cells

	ReadValueByIndex(index int) Value

	//给最大值行设置拥有的行数
	SetNOwned(cnt byte)
	//	注意最小和最大记录的头信息中的 n_owned 属性
	//最小记录的 n_owned 值为 1 ，这就代表着以最小记录结尾的这个分组中只有 1 条记录，也就是最小记录
	//本身。
	//最大记录的 n_owned 值为 5 ，这就代表着以最大记录结尾的这个分组中只有 5 条记录，包括最大记录本
	//身还有我们自己插入的 4 条记录。
	GetNOwned() byte

	//获取下一条记录的偏移量
	GetNextRowOffset() uint16

	//设置下一条记录偏移量

	SetNextRowOffset(offset uint16)

	//获取当前记录在本页面中的相对位置
	GetHeapNo() uint16

	//设置该记录在
	SetHeapNo(heapNo uint16)

	//设置row TransactionId
	SetTransactionId(trxId uint64)

	GetValueByColName(colName string) Value

	ToString() string

	ToDatum() []Datum
}

type Rows []Row

func (a Rows) Len() int {
	return len(a)
}
func (a Rows) Less(i, j int) bool {
	return (a[i]).Less(a[j])
}
func (a Rows) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a Rows) AddRow(row Row) {
	a = append(a, row)
}

//定义二进制类型
type FiledDataBytes []byte

type FieldDataHeader interface {
	SetDeleteFlag(delete bool)
	GetDeleteFlag() bool
	GetRecMinFlag() bool
	SetRecMinFlag(flag bool)
	SetNOwned(size byte)
	GetNOwned() byte
	GetHeapNo() uint16
	SetHeapNo(heapNo uint16)
	GetRecordType() uint8
	SetRecordType(recordType uint8)
	GetNextRecord() uint16
	SetNextRecord(nextRecord uint16)

	//Null值列表  有null的时候，需要记录，用bit来表示
	//@param nullValue 是否需要
	//@param index     u 小表
	SetValueNull(nullValue byte, index byte)

	//长度列表
	GetRowHeaderLength() uint16

	ToByte() []byte

	//设置长度
	SetValueLengthByIndex(realLength int, index byte)

	GetVarValueLengthByIndex(index byte) int

	//获取该记录的实际长度
	GetRecordBytesRealLength() int

	//根据数组下标，判断当前记录实际存储
	IsValueNullByIdx(index byte) bool

	//根据数组下标，获取当前VAR类型的实际长度

	GetVarRealLength(currerntIndex byte) uint16
}
type FieldDataValue interface {
	ToByte() []byte
	//content已经根据字符串实现了存储
	WriteBytesWithNull(content []byte)

	ReadBytesWithNullWithPosition(index int) []byte

	ReadValue(index int) Value

	GetRowDataLength() uint16
}
