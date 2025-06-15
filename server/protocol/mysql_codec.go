package protocol

import (
	"encoding/binary"
	"fmt"
)

// EncodeOKPacket 编码OK包
func EncodeOKPacket(info []byte, affectedRows, lastInsertId uint64, warnings []byte) []byte {
	payload := make([]byte, 0, 64)

	// OK包标识符
	payload = append(payload, 0x00)

	// 受影响的行数 (length-encoded integer)
	payload = appendLengthEncodedInt(payload, affectedRows)

	// 最后插入的ID (length-encoded integer)
	payload = appendLengthEncodedInt(payload, lastInsertId)

	// 服务器状态标志 (2字节，小端序)
	// SERVER_STATUS_AUTOCOMMIT = 0x0002
	payload = append(payload, 0x02, 0x00)

	// 警告数量 (2字节，小端序)
	warningCount := uint16(0)
	if warnings != nil && len(warnings) > 0 {
		warningCount = uint16(len(warnings))
	}
	payload = append(payload, byte(warningCount), byte(warningCount>>8))

	// 可选的info字符串
	if info != nil && len(info) > 0 {
		payload = append(payload, info...)
	}

	// 添加包头，序列号为1（响应客户端的查询）
	return addPacketHeader(payload, 1)
}

// EncodeErrorPacket 编码错误包
func EncodeErrorPacket(errorCode uint16, sqlState, message string) []byte {
	payload := make([]byte, 0, 64+len(message))

	// 错误包标识符
	payload = append(payload, 0xFF)

	// 错误代码
	payload = append(payload, byte(errorCode), byte(errorCode>>8))

	// SQL状态标记
	payload = append(payload, '#')

	// SQL状态
	payload = append(payload, []byte(sqlState)...)

	// 错误消息
	payload = append(payload, []byte(message)...)

	// 添加包头
	return addPacketHeader(payload, 0)
}

// EncodeColumnsPacket 编码列定义包
func EncodeColumnsPacket(columns []string) []byte {
	// 列数量包
	countPayload := make([]byte, 0, 8)
	countPayload = appendLengthEncodedInt(countPayload, uint64(len(columns)))
	result := addPacketHeader(countPayload, 0)

	// 每个列的定义包
	for i, column := range columns {
		columnPayload := make([]byte, 0, 64+len(column))

		// 简化的列定义
		columnPayload = appendLengthEncodedString(columnPayload, "def")  // catalog
		columnPayload = appendLengthEncodedString(columnPayload, "")     // schema
		columnPayload = appendLengthEncodedString(columnPayload, "")     // table
		columnPayload = appendLengthEncodedString(columnPayload, "")     // org_table
		columnPayload = appendLengthEncodedString(columnPayload, column) // name
		columnPayload = appendLengthEncodedString(columnPayload, column) // org_name

		// 固定长度字段
		columnPayload = append(columnPayload, 0x0c)                   // length of fixed fields
		columnPayload = append(columnPayload, 0x21, 0x00)             // character set
		columnPayload = append(columnPayload, 0x00, 0x00, 0x00, 0x00) // column length
		columnPayload = append(columnPayload, 0xFD)                   // column type (VAR_STRING)
		columnPayload = append(columnPayload, 0x00, 0x00)             // flags
		columnPayload = append(columnPayload, 0x00)                   // decimals
		columnPayload = append(columnPayload, 0x00, 0x00)             // filler

		result = append(result, addPacketHeader(columnPayload, byte(i+1))...)
	}

	// EOF包
	result = append(result, EncodeEOFPacket(0, 0)...)

	return result
}

// EncodeRowPacket 编码行数据包
func EncodeRowPacket(row []interface{}) []byte {
	payload := make([]byte, 0, 256)

	for _, value := range row {
		if value == nil {
			payload = append(payload, 0xFB) // NULL
		} else {
			str := fmt.Sprintf("%v", value)
			payload = appendLengthEncodedString(payload, str)
		}
	}

	return addPacketHeader(payload, 0)
}

// EncodeEOFPacket 编码EOF包
func EncodeEOFPacket(warnings, statusFlags uint16) []byte {
	payload := make([]byte, 5)
	payload[0] = 0xFE // EOF标识符
	binary.LittleEndian.PutUint16(payload[1:3], warnings)
	binary.LittleEndian.PutUint16(payload[3:5], statusFlags)

	return addPacketHeader(payload, 0)
}

// EncodeEOFPacketWithSeq 编码EOF包（带序列号）
func EncodeEOFPacketWithSeq(warnings, statusFlags uint16, sequenceId byte) []byte {
	payload := make([]byte, 5)
	payload[0] = 0xFE // EOF标识符
	binary.LittleEndian.PutUint16(payload[1:3], warnings)
	binary.LittleEndian.PutUint16(payload[3:5], statusFlags)

	return addPacketHeader(payload, sequenceId)
}

// addPacketHeader 添加MySQL包头
func addPacketHeader(payload []byte, sequenceId byte) []byte {
	length := len(payload)
	header := make([]byte, 4)

	// 包长度 (3字节，小端序)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 序列号
	header[3] = sequenceId

	return append(header, payload...)
}

// appendLengthEncodedInt 追加长度编码整数
func appendLengthEncodedInt(data []byte, value uint64) []byte {
	if value < 251 {
		return append(data, byte(value))
	} else if value < 65536 {
		data = append(data, 0xFC)
		data = append(data, byte(value), byte(value>>8))
		return data
	} else if value < 16777216 {
		data = append(data, 0xFD)
		data = append(data, byte(value), byte(value>>8), byte(value>>16))
		return data
	} else {
		data = append(data, 0xFE)
		for i := 0; i < 8; i++ {
			data = append(data, byte(value>>(i*8)))
		}
		return data
	}
}

// appendLengthEncodedString 追加长度编码字符串
func appendLengthEncodedString(data []byte, str string) []byte {
	data = appendLengthEncodedInt(data, uint64(len(str)))
	return append(data, []byte(str)...)
}
