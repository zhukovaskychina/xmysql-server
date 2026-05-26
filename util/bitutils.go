package util

import (
	"bytes"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"math"
	"strconv"
	"strings"
)

type bit string

func (b bit) Value() string {
	return string(b)
}

func ToBinaryString(data byte) string {
	result := make([]string, 0)
	for i := 0; i < 8; i++ {
		move := uint(7 - i)
		result = append(result, string(strconv.Itoa(int((data>>move)&1))))
	}
	return strings.Join(result, "")
}

func ConvertByte2Bits(data byte) string {
	result := make([]string, 0)
	for i := 0; i < 8; i++ {
		move := uint(7 - i)
		result = append(result, string(strconv.Itoa(int((data>>move)&1))))
	}

	return strings.Join(result, "")
}
func ConvertByte2BitsString(data byte) []string {
	result := make([]string, 0)
	for i := 0; i < 8; i++ {
		move := uint(7 - i)
		result = append(result, string(strconv.Itoa(int((data>>move)&1))))
	}
	return result
}

func ConvertStringArrays2Bytes(data []string) []byte {

	var result = make([]byte, 0)

	for i := 0; i < len(data); i++ {
		result = append(result, ConvertString2Byte(data[i]))
	}
	return result
}

func ConvertStringArrays2BytesArrays(bitString []string) []byte {
	var buff = make([]byte, 0)
	size := len(bitString) >> 3

	for i := 0; i < size; i++ {
		currentByte := (ConvertString2Byte(strings.Join(bitString[i*8:i*8+8], "")))
		buff = append(buff, currentByte)
	}
	return buff
}

func ConvertString2Byte(data string) byte {
	var result = byte(0)

	if len(data) != 8 {
		logger.Warnf("ConvertString2Byte 输入长度非法: %s, return 0", data)
		return 0
	}
	value := strings.SplitAfter(data, "")
	for i := 0; i < 8; i++ {
		if i >= len(value) {
			logger.Warnf("ConvertString2Byte 输入格式异常: %s, return 0", data)
			return 0
		}
		intValue, err := strconv.Atoi(value[i])
		if err != nil {
			logger.Warnf("ConvertString2Byte 内容转换失败: %s, index=%d, err=%v", data, i, err)
			return 0
		}
		if intValue != 0 && intValue != 1 {
			logger.Warnf("ConvertString2Byte 非法位字符: %s, index=%d", data, i)
			return 0
		}
		res := byte(float64(intValue) * math.Pow(2, float64(8-i-1)))
		result = result + res
	}
	return result
}

func ConvertBits2Byte(data string) byte {

	var result = byte(0)

	if len(data) != 8 {
		logger.Warnf("ConvertBits2Byte 输入长度非法: %s, return 0", data)
		return 0
	}
	value := strings.SplitAfter(data, "")
	for i := 0; i < 8; i++ {
		if i >= len(value) {
			logger.Warnf("ConvertBits2Byte 输入格式异常: %s, return 0", data)
			return 0
		}
		intValue, err := strconv.Atoi(value[i])
		if err != nil {
			logger.Warnf("ConvertBits2Byte 内容转换失败: %s, index=%d, err=%v", data, i, err)
			return 0
		}
		if intValue != 0 && intValue != 1 {
			logger.Warnf("ConvertBits2Byte 非法位字符: %s, index=%d", data, i)
			return 0
		}
		res := byte(float64(intValue) * math.Pow(2, float64(8-i-1)))
		result = result + res
	}
	return result
}
func ConvertBits2Bytes(data string) []byte {
	if len(data)%8 != 0 {
		logger.Warnf("ConvertBits2Bytes 输入长度非法: %s, return empty", data)
		return []byte{}
	}
	var buff = make([]byte, 0)
	var size = int(len(data) / 8)
	for i := 0; i < size; i++ {
		iterData := Substr(data, i*8, i*8+8)
		buff = append(buff, ConvertBits2Byte(iterData))
	}

	return buff
}
func Substr(str string, start int, end int) string {
	rs := []rune(str)
	length := len(rs)

	if start < 0 || start > length {
		return ""
	}

	if end < 0 || end > length {
		return ""
	}
	return string(rs[start:end])
}

// 将某个字节的第几位bit置换，value0，1
func ConvertValueOfBitsInBytes(byte byte, index int, value int) byte {
	binaryString := ConvertByte2BitsString(byte)
	binaryString[index] = strconv.Itoa(value)
	resultString := strings.Join(binaryString, "")
	return ConvertBits2Byte(resultString)
}

func ReadBytesByIndexBit(byte byte, index int) string {
	return ConvertByte2BitsString(byte)[index]
}

func ReadBytesByIndexBitByStart(byte byte, start int, end int) string {
	return strings.Join(ConvertByte2BitsString(byte)[start:end], "")
}

func WriteBitsByStart(byte byte, bitsString []string, start int, end int) string {
	data := ConvertByte2BitsString(byte)
	for i := start; i < end; i++ {
		data[i] = bitsString[i-start]
	}
	return strings.Join(data, "")
}
func LeftPaddleBitString(bitstring string, leftPaddleSize int) byte {
	var bufferString = bytes.NewBufferString("")
	for i := 0; i < leftPaddleSize; i++ {
		bufferString.WriteString("0")
	}
	bufferString.WriteString(bitstring)
	return ConvertBits2Byte(bufferString.String())
}

func TrimLeftPaddleBitString(bitData byte, leftPaddleStart int) []string {
	bitStringArray := ConvertByte2BitsString(bitData)
	var result = make([]string, 0)
	for i := leftPaddleStart; i < 8; i++ {
		result = append(result, bitStringArray[i])
	}
	return result
}

// 注意BigEndian和littleEndian
func ConvertBytes2BitStrings(data []byte) []string {
	var stringResultArray = make([]string, 0)
	for i := len(data) - 1; i >= 0; i-- {
		stringResultArray = append(stringResultArray, ConvertByte2BitsString(data[i])...)
	}

	return stringResultArray
}

func ConvertAsciiToByte(ascii uint8) byte {
	res, _ := strconv.Atoi(string(rune(ascii)))
	return byte(res)
}
