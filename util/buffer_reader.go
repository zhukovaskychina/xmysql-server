package util

func ReadBytes(buff []byte, cursor int, offset int) (int, []byte) {
	if offset <= 0 {
		return cursor, nil
	}
	return cursor + offset, buff[cursor : cursor+offset]
}

func ReadByte(buff []byte, cursor int) (int, byte) {
	return cursor + 1, buff[cursor]
}

func ReadUB2(buff []byte, cursor int) (int, uint16) {
	if len(buff) == 1 {
		buff = append(buff, 0)
	}
	i := uint16(buff[cursor])
	i |= uint16(buff[cursor+1]) << 8
	return cursor + 2, i
}

func ReadUB3(buff []byte, cursor int) (int, uint32) {
	i := uint32(buff[cursor])
	i |= uint32(buff[cursor+1]) << 8
	i |= uint32(buff[cursor+2]) << 16
	return cursor + 3, i
}

func ReadUB4(buff []byte, cursor int) (int, uint32) {
	i := uint32(buff[cursor])
	i |= uint32(buff[cursor+1]) << 8
	i |= uint32(buff[cursor+2]) << 16
	i |= uint32(buff[cursor+3]) << 24
	return cursor + 4, i
}

func ReadUB6(buff []byte, cursor int) (int, uint64) {
	i := uint64(buff[cursor])
	i |= uint64(buff[cursor+1]) << 8
	i |= uint64(buff[cursor+2]) << 16
	i |= uint64(buff[cursor+3]) << 24
	i |= uint64(buff[cursor+4]) << 32
	i |= uint64(buff[cursor+5]) << 40
	return cursor + 6, i
}

func ReadUB8(buff []byte, cursor int) (int, uint64) {
	i := uint64(buff[cursor])
	i |= uint64(buff[cursor+1]) << 8
	i |= uint64(buff[cursor+2]) << 16
	i |= uint64(buff[cursor+3]) << 24
	i |= uint64(buff[cursor+4]) << 32
	i |= uint64(buff[cursor+5]) << 40
	i |= uint64(buff[cursor+6]) << 48
	i |= uint64(buff[cursor+7]) << 56
	return cursor + 8, i
}

func ReadUB8Long(buff []byte, cursor int) (int, int64) {
	i := int64(buff[cursor])
	i |= int64(buff[cursor+1]) << 8
	i |= int64(buff[cursor+2]) << 16
	i |= int64(buff[cursor+3]) << 24
	i |= int64(buff[cursor+4]) << 32
	i |= int64(buff[cursor+5]) << 40
	i |= int64(buff[cursor+6]) << 48
	i |= int64(buff[cursor+7]) << 56
	return cursor + 8, i
}

func ReadLength(buff []byte, cursor int) (int, uint64) {
	length := buff[cursor]
	cursor++
	switch length {
	case 251:
		return cursor, 0
	case 252:
		cursor, u16 := ReadUB2(buff, cursor)
		return cursor, uint64(u16)
	case 253:
		cursor, u24 := ReadUB3(buff, cursor)
		return cursor, uint64(u24)
	case 254:
		cursor, u64 := ReadUB8(buff, cursor)
		return cursor, u64
	default:
		return cursor, uint64(length)

	}
}

func ReadString(buff []byte, cursor int) (int, string) {
	cursor, tmp := ReadBytes(buff, cursor, len(buff)-cursor)
	return cursor, string(tmp)
}

func ReadStringWithNull(buff []byte, cursor int) (int, string) {
	cursor, tmp := ReadWithNull(buff, cursor)
	return cursor, string(tmp)
}

func ReadBytesWithNull(buff []byte, cursor int) (int, []byte) {
	cursor, length := ReadLength(buff, cursor)

	if length <= 0 {
		result := make([]byte, 0)
		return cursor, result
	}
	return int(int64(cursor) + int64(length)), buff[cursor:int(int64(cursor)+int64(length))]
}

func ReadLengthString(buff []byte, cursor int) (int, string) {
	cursor, strLen := ReadLength(buff, cursor)
	cursor, tmp := ReadBytes(buff, cursor, int(strLen))
	return cursor, string(tmp)
}

func ReadWithNull(buff []byte, cursor int) (int, []byte) {
	ret := []byte{}
	for {
		if buff[cursor] != 0 {
			ret = append(ret, buff[cursor])
			cursor++
		} else {
			cursor++
			break
		}
	}
	return cursor, ret
}

func ReadBitSet(buff []byte, cursor int, length int, bigEndian bool) (int, []int) {
	var tmp []byte

	length = (length + 7) >> 3
	cursor, tmp = ReadBytes(buff, cursor, length)
	if bigEndian == false {
		tmp = ByteReverse(tmp)
	}

	ret := []int{}
	for i := 0; i < length; i++ {
		if (tmp[i>>3] & (1 << uint(i%8))) != 0 {
			ret = append(ret, i)
		}
	}
	return cursor, ret
}

func ByteReverse(buff []byte) []byte {
	length := len(buff)
	if length <= 1 {
		return buff
	}
	for i := 0; i < length/2; i++ {
		buff[i], buff[length-i] = buff[length-i], buff[i]
	}
	return buff
}

func GetLength(length int64) int {
	if length < 251 {
		return 1
	} else if length < 0x10000 {
		return 3
	} else if length < 0x1000000 {
		return 4
	} else {
		return 9
	}
}

func GetLengthBytes(buff []byte) int {
	length := len(buff)
	if length < 251 {
		return 1 + length
	} else if length < 0x10000 {
		return 3 + length
	} else if length < 0x1000000 {
		return 4 + length
	} else {
		return 9 + length
	}
}

func ReadUB2Byte2Int(buff []byte) uint16 {
	_, rs := ReadUB2(buff, 0)
	return rs
}

func ReadUB4ByteInt(buff [4]byte) int {
	tb := make([]byte, 0)
	tb = append(tb, buff[0], buff[1], buff[2], buff[3])
	rs, _ := ReadUB4(tb, 0)
	return rs
}

func ReadUB4Byte2Int(buff []byte) int {
	if len(buff) == 1 {
		buff = append(buff, 0, 0, 0)
	}
	if len(buff) == 2 {
		buff = append(buff, 0, 0)
	}
	if len(buff) == 3 {
		buff = append(buff, 0)
	}
	_, rs := ReadUB4(buff, 0)
	return int(rs)
}

func ReadUB4Byte2UInt32(buff []byte) uint32 {
	if len(buff) == 2 {
		buff = append(buff, 0, 0)
	}
	_, rs := ReadUB4(buff, 0)
	return rs
}

func ReadUB8Byte2Long(buff []byte) uint64 {
	if len(buff) == 6 {
		buff = append(buff, 0, 0)
	}
	_, rs := ReadUB8(buff, 0)
	return rs
}

func ReadUB8Bytes2Long(buff []byte) int64 {
	if len(buff) == 6 {
		buff = append(buff, 0, 0)
	}
	_, rs := ReadUB8Long(buff, 0)
	return rs
}
