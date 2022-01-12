package util

func WriteByte(buf []byte, b byte) []byte {
	buf = append(buf, b)
	return buf
}

func WriteBytes(buf []byte, from []byte) []byte {
	for _, v := range from {
		buf = append(buf, v)
	}
	return buf
}

func WriteUB2(buf []byte, i uint16) []byte {
	buf = append(buf, byte(i&0xFF))
	buf = append(buf, byte((i>>8)&0xFF))
	return buf
}

func WriteUB3(buf []byte, i uint32) []byte {
	buf = append(buf, byte(i&0xFF))
	buf = append(buf, byte((i>>8)&0xFF))
	buf = append(buf, byte((i>>16)&0xFF))
	return buf
}

func WriteUB4(buf []byte, i uint32) []byte {
	buf = append(buf, byte(i&0xFF))
	buf = append(buf, byte((i>>8)&0xFF))
	buf = append(buf, byte((i>>16)&0xFF))
	buf = append(buf, byte((i>>24)&0xFF))
	return buf
}

func WriteUB6(buf []byte, i uint64) []byte {
	buf = append(buf, byte(i&0xFF))
	buf = append(buf, byte((i>>8)&0xFF))
	buf = append(buf, byte((i>>16)&0xFF))
	buf = append(buf, byte((i>>24)&0xFF))
	buf = append(buf, byte((i>>32)&0xFF))
	buf = append(buf, byte((i>>40)&0xFF))
	return buf
}

func WriteUB8(buf []byte, i uint64) []byte {
	buf = append(buf, byte(i&0xFF))
	buf = append(buf, byte((i>>8)&0xFF))
	buf = append(buf, byte((i>>16)&0xFF))
	buf = append(buf, byte((i>>24)&0xFF))
	buf = append(buf, byte((i>>32)&0xFF))
	buf = append(buf, byte((i>>40)&0xFF))
	buf = append(buf, byte((i>>48)&0xFF))
	buf = append(buf, byte((i>>56)&0xFF))
	return buf
}

func WriteLength(buf []byte, length int64) []byte {
	if length <= 251 {
		buf = WriteByte(buf, byte(length))
	} else if length < 0x10000 {
		buf = WriteByte(buf, 252)
		buf = WriteUB2(buf, uint16(length))
	} else if length < 0x1000000 {
		buf = WriteByte(buf, 253)
		buf = WriteUB3(buf, uint32(length))
	} else {
		buf = WriteByte(buf, 254)
		buf = WriteUB8(buf, uint64(length))
	}
	return buf
}

func WriteWithNull(buf []byte, from []byte) []byte {
	buf = WriteBytes(buf, from)
	buf = append(buf, byte(0))
	return buf
}

func WriteWithLength(buf []byte, from []byte) []byte {
	length := len(from)
	buf = WriteLength(buf, int64(length))
	buf = WriteBytes(buf, from)
	return buf
}

func WriteWithLengthWithNullValue(buf []byte, from []byte, nullValue byte) []byte {
	if from == nil {
		buf = WriteByte(buf, nullValue)
	} else {
		buf = WriteWithLength(buf, from)
	}
	return buf
}

func WriteInt32WithNull(buf []byte, data uint32) []byte {
	buf = WriteBytes(buf, ConvertUInt4Bytes(data))
	buf = append(buf, byte(0))
	return buf
}

func ConvertInt2Bytes(i int32) []byte {
	buff := make([]byte, 0)
	rs := WriteUB2(buff, uint16(i))
	return rs
}
func ConvertInt3Bytes(i int32) []byte {
	buff := make([]byte, 0)
	rs := WriteUB3(buff, uint32(i))
	return rs
}

func ConvertInt4Bytes(i int32) []byte {
	buff := make([]byte, 0)
	rs := WriteUB4(buff, uint32(i))
	return rs
}

func ConvertLong8Bytes(i int64) []byte {
	buff := make([]byte, 0)
	rs := WriteUB8(buff, uint64(i))
	return rs
}

func ConvertULong8Bytes(i uint64) []byte {
	buff := make([]byte, 0)
	rs := WriteUB8(buff, i)
	return rs
}

func ConvertBool2Byte(boolValue bool) byte {
	if boolValue {
		return 1
	}
	return 0
}

func ConvertUInt4Bytes(i uint32) []byte {
	buff := make([]byte, 0)
	rs := WriteUB4(buff, i)
	return rs
}

func ConvertUInt2Bytes(i uint16) []byte {
	buff := make([]byte, 0)
	rs := WriteUB2(buff, i)
	return rs
}
