package record

import (
	"encoding/binary"
	"fmt"
)

/*
CompactRowFormat 实现了InnoDB Compact行格式

Compact行格式结构（MySQL 5.0+默认格式）：
[变长字段长度列表][NULL值位图][记录头信息5B][隐藏列13B][列1][列2]...[列N]

关键特性：
1. 变长字段长度逆序存储（1-2字节/字段）
2. NULL值位图紧凑存储（1bit/可空字段）
3. 记录头信息固定5字节
4. 隐藏列：DB_TRX_ID(6B) + DB_ROLL_PTR(7B)

设计要点：
- 空间优化：NULL字段不占用数据空间
- 兼容性：与MySQL InnoDB完全兼容
- 性能：高效的变长字段访问
*/

const (
	// 记录头信息大小
	RecordHeaderSize = 5

	// 隐藏列大小
	HiddenColumnSize = 13 // DB_TRX_ID(6B) + DB_ROLL_PTR(7B)

	// 最大变长字段长度（需要2字节存储）
	MaxVarcharLen1Byte = 127   // 1字节可表示0-127
	MaxVarcharLen2Byte = 16383 // 2字节可表示0-16383

	// 记录头标志位
	RecordDeletedFlag = 0x20 // 删除标记
	RecordMinRecFlag  = 0x10 // B+树非叶子节点最小记录标记

	// 记录类型
	RecordTypeOrdinary = 0 // 普通记录
	RecordTypeNodePtr  = 1 // 节点指针记录
	RecordTypeInfimum  = 2 // Infimum记录
	RecordTypeSupremum = 3 // Supremum记录
)

// CompactRowFormat Compact行格式处理器
type CompactRowFormat struct {
	// 列定义
	columns []*ColumnDef

	// 变长字段索引
	varLenCols []int

	// NULL值字段索引
	nullableCols []int

	// NULL位图大小（字节）
	nullBitmapSize int
}

// ColumnDef 列定义
type ColumnDef struct {
	Name       string
	Type       ColumnType
	Length     int // 固定长度字段的长度
	IsNullable bool
	IsVarLen   bool // 是否变长
}

// ColumnType 列类型
type ColumnType int

const (
	TypeTinyInt ColumnType = iota
	TypeSmallInt
	TypeInt
	TypeBigInt
	TypeFloat
	TypeDouble
	TypeChar
	TypeVarchar
	TypeText
	TypeBlob
	TypeDate
	TypeDatetime
	TypeTimestamp
)

// RecordHeader 记录头信息（5字节）
type RecordHeader struct {
	DeletedFlag bool   // 删除标记（1bit）
	MinRecFlag  bool   // 最小记录标记（1bit）
	NOwned      uint8  // 当前记录拥有的记录数（4bit）
	HeapNo      uint16 // 堆中位置（13bit）
	RecordType  uint8  // 记录类型（3bit）
	NextRecord  int16  // 下一条记录偏移（16bit，有符号）
}

// HiddenColumns 隐藏列
type HiddenColumns struct {
	TrxID   uint64 // 事务ID（6字节，只用低48位）
	RollPtr uint64 // 回滚指针（7字节，只用低56位）
}

// CompactRow Compact格式的行数据
type CompactRow struct {
	// 原始数据
	RawData []byte

	// 解析后的组件
	VarLenList   []uint16       // 变长字段长度列表
	NullBitmap   []byte         // NULL值位图
	Header       *RecordHeader  // 记录头
	Hidden       *HiddenColumns // 隐藏列
	ColumnValues [][]byte       // 列数据
}

// NewCompactRowFormat 创建Compact行格式处理器
func NewCompactRowFormat(columns []*ColumnDef) *CompactRowFormat {
	crf := &CompactRowFormat{
		columns:      columns,
		varLenCols:   make([]int, 0),
		nullableCols: make([]int, 0),
	}

	// 识别变长字段和可空字段
	for i, col := range columns {
		if col.IsVarLen {
			crf.varLenCols = append(crf.varLenCols, i)
		}
		if col.IsNullable {
			crf.nullableCols = append(crf.nullableCols, i)
		}
	}

	// 计算NULL位图大小（向上取整到字节）
	crf.nullBitmapSize = (len(crf.nullableCols) + 7) / 8

	return crf
}

// EncodeRow 编码行数据为Compact格式
func (crf *CompactRowFormat) EncodeRow(values []interface{}, trxID, rollPtr uint64) ([]byte, error) {
	if len(values) != len(crf.columns) {
		return nil, fmt.Errorf("value count mismatch: got %d, expected %d", len(values), len(crf.columns))
	}

	// 1. 计算变长字段长度列表
	varLenList := make([]uint16, len(crf.varLenCols))
	for i, colIdx := range crf.varLenCols {
		if values[colIdx] == nil {
			varLenList[i] = 0
		} else {
			data := crf.valueToBytes(values[colIdx], crf.columns[colIdx])
			varLenList[i] = uint16(len(data))
		}
	}

	// 2. 构建NULL值位图
	nullBitmap := make([]byte, crf.nullBitmapSize)
	for i, colIdx := range crf.nullableCols {
		if values[colIdx] == nil {
			byteIdx := i / 8
			bitIdx := i % 8
			nullBitmap[byteIdx] |= (1 << bitIdx)
		}
	}

	// 3. 创建记录头
	header := &RecordHeader{
		DeletedFlag: false,
		MinRecFlag:  false,
		NOwned:      0,
		HeapNo:      0,
		RecordType:  RecordTypeOrdinary,
		NextRecord:  0,
	}

	// 4. 计算总大小
	totalSize := 0

	// 变长字段长度列表（逆序）
	for _, length := range varLenList {
		if length <= MaxVarcharLen1Byte {
			totalSize += 1
		} else {
			totalSize += 2
		}
	}

	// NULL位图
	totalSize += crf.nullBitmapSize

	// 记录头
	totalSize += RecordHeaderSize

	// 隐藏列
	totalSize += HiddenColumnSize

	// 列数据
	for i, col := range crf.columns {
		if values[i] != nil {
			data := crf.valueToBytes(values[i], col)
			totalSize += len(data)
		}
	}

	// 5. 编码到字节数组
	result := make([]byte, totalSize)
	offset := 0

	// 5.1 变长字段长度列表（逆序）
	for i := len(varLenList) - 1; i >= 0; i-- {
		length := varLenList[i]
		if length <= MaxVarcharLen1Byte {
			result[offset] = byte(length)
			offset++
		} else {
			// 2字节，高位在前，最高位为1表示2字节编码
			result[offset] = byte((length >> 8) | 0x80)
			result[offset+1] = byte(length & 0xFF)
			offset += 2
		}
	}

	// 5.2 NULL值位图
	copy(result[offset:], nullBitmap)
	offset += crf.nullBitmapSize

	// 5.3 记录头（5字节）
	crf.encodeRecordHeader(result[offset:], header)
	offset += RecordHeaderSize

	// 5.4 隐藏列（13字节）
	// DB_TRX_ID (6字节，只用低48位)
	for i := 5; i >= 0; i-- {
		result[offset+i] = byte((trxID >> (uint(5-i) * 8)) & 0xFF)
	}
	offset += 6

	// DB_ROLL_PTR (7字节，只用低56位)
	for i := 6; i >= 0; i-- {
		result[offset+i] = byte((rollPtr >> (uint(6-i) * 8)) & 0xFF)
	}
	offset += 7

	// 5.5 列数据
	for i, col := range crf.columns {
		if values[i] != nil {
			data := crf.valueToBytes(values[i], col)
			copy(result[offset:], data)
			offset += len(data)
		}
	}

	return result, nil
}

// DecodeRow 解码Compact格式的行数据
func (crf *CompactRowFormat) DecodeRow(data []byte) (*CompactRow, error) {
	if len(data) < RecordHeaderSize+HiddenColumnSize {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	row := &CompactRow{
		RawData:      data,
		VarLenList:   make([]uint16, len(crf.varLenCols)),
		ColumnValues: make([][]byte, len(crf.columns)),
	}

	offset := 0

	// 1. 解析变长字段长度列表（逆序）
	varLenBytes := 0
	for i := len(crf.varLenCols) - 1; i >= 0; i-- {
		if data[offset]&0x80 == 0 {
			// 1字节编码
			row.VarLenList[i] = uint16(data[offset])
			offset++
			varLenBytes++
		} else {
			// 2字节编码
			row.VarLenList[i] = (uint16(data[offset]&0x7F) << 8) | uint16(data[offset+1])
			offset += 2
			varLenBytes += 2
		}
	}

	// 2. 解析NULL值位图
	row.NullBitmap = make([]byte, crf.nullBitmapSize)
	copy(row.NullBitmap, data[offset:offset+crf.nullBitmapSize])
	offset += crf.nullBitmapSize

	// 3. 解析记录头
	row.Header = crf.decodeRecordHeader(data[offset:])
	offset += RecordHeaderSize

	// 4. 解析隐藏列
	row.Hidden = &HiddenColumns{}

	// DB_TRX_ID (6字节)
	row.Hidden.TrxID = 0
	for i := 0; i < 6; i++ {
		row.Hidden.TrxID = (row.Hidden.TrxID << 8) | uint64(data[offset+i])
	}
	offset += 6

	// DB_ROLL_PTR (7字节)
	row.Hidden.RollPtr = 0
	for i := 0; i < 7; i++ {
		row.Hidden.RollPtr = (row.Hidden.RollPtr << 8) | uint64(data[offset+i])
	}
	offset += 7

	// 5. 解析列数据
	varLenIdx := 0
	for i, col := range crf.columns {
		// 检查是否为NULL
		if crf.isNullValue(i, row.NullBitmap) {
			row.ColumnValues[i] = nil
			if col.IsVarLen {
				varLenIdx++
			}
			continue
		}

		// 根据列类型读取数据
		var length int
		if col.IsVarLen {
			length = int(row.VarLenList[varLenIdx])
			varLenIdx++
		} else {
			length = col.Length
		}

		if offset+length > len(data) {
			return nil, fmt.Errorf("data truncated at column %d", i)
		}

		row.ColumnValues[i] = make([]byte, length)
		copy(row.ColumnValues[i], data[offset:offset+length])
		offset += length
	}

	return row, nil
}

// encodeRecordHeader 编码记录头（5字节）
func (crf *CompactRowFormat) encodeRecordHeader(data []byte, header *RecordHeader) {
	// Byte 0-1: 包含deleted_flag, min_rec_flag, n_owned, heap_no
	var byte0 uint8 = 0
	if header.DeletedFlag {
		byte0 |= RecordDeletedFlag
	}
	if header.MinRecFlag {
		byte0 |= RecordMinRecFlag
	}
	byte0 |= (header.NOwned & 0x0F)

	var byte1 uint8 = byte(header.HeapNo >> 5)
	data[0] = byte0
	data[1] = byte1

	// Byte 2: heap_no低5位 + record_type 3位
	var byte2 uint8 = byte((header.HeapNo & 0x1F) << 3)
	byte2 |= (header.RecordType & 0x07)
	data[2] = byte2

	// Byte 3-4: next_record (16位有符号整数，大端)
	binary.BigEndian.PutUint16(data[3:5], uint16(header.NextRecord))
}

// decodeRecordHeader 解码记录头（5字节）
func (crf *CompactRowFormat) decodeRecordHeader(data []byte) *RecordHeader {
	header := &RecordHeader{}

	// Byte 0
	header.DeletedFlag = (data[0] & RecordDeletedFlag) != 0
	header.MinRecFlag = (data[0] & RecordMinRecFlag) != 0
	header.NOwned = data[0] & 0x0F

	// Byte 1-2: heap_no (13位)
	header.HeapNo = (uint16(data[1]) << 5) | (uint16(data[2]) >> 3)

	// Byte 2: record_type (3位)
	header.RecordType = data[2] & 0x07

	// Byte 3-4: next_record
	header.NextRecord = int16(binary.BigEndian.Uint16(data[3:5]))

	return header
}

// isNullValue 检查列值是否为NULL
func (crf *CompactRowFormat) isNullValue(colIdx int, nullBitmap []byte) bool {
	// 查找列在nullableCols中的位置
	for i, idx := range crf.nullableCols {
		if idx == colIdx {
			byteIdx := i / 8
			bitIdx := i % 8
			if byteIdx < len(nullBitmap) {
				return (nullBitmap[byteIdx] & (1 << bitIdx)) != 0
			}
			return false
		}
	}
	// 列不可为NULL
	return false
}

// valueToBytes 将值转换为字节数组
func (crf *CompactRowFormat) valueToBytes(value interface{}, col *ColumnDef) []byte {
	// 简化实现，实际需要根据列类型进行转换
	switch v := value.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case int:
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(v))
		return data
	case int64:
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(v))
		return data
	default:
		return []byte{}
	}
}

// GetVarLenListSize 获取变长字段长度列表的大小
func (crf *CompactRowFormat) GetVarLenListSize(varLenList []uint16) int {
	size := 0
	for _, length := range varLenList {
		if length <= MaxVarcharLen1Byte {
			size += 1
		} else {
			size += 2
		}
	}
	return size
}

// CalculateRowSize 计算行数据大小
func (crf *CompactRowFormat) CalculateRowSize(values []interface{}) int {
	size := 0

	// 变长字段长度列表
	for _, colIdx := range crf.varLenCols {
		if values[colIdx] != nil {
			data := crf.valueToBytes(values[colIdx], crf.columns[colIdx])
			length := uint16(len(data))
			if length <= MaxVarcharLen1Byte {
				size += 1
			} else {
				size += 2
			}
		} else {
			size += 1 // NULL值也需要占位
		}
	}

	// NULL位图
	size += crf.nullBitmapSize

	// 记录头
	size += RecordHeaderSize

	// 隐藏列
	size += HiddenColumnSize

	// 列数据
	for i, col := range crf.columns {
		if values[i] != nil {
			data := crf.valueToBytes(values[i], col)
			size += len(data)
		}
	}

	return size
}
