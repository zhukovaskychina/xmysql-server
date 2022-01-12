package util

import (
	"fmt"
	"math"
	"testing"
)

func TestBufferUtils(t *testing.T) {
	fmt.Println(math.Mod(11, 3))
}

func TestBufferWrite(t *testing.T) {
	var buff = ConvertUInt4Bytes(2)
	fmt.Println(buff)
	fmt.Println(ReadUB4Byte2UInt32(buff))

	var bb = -127
	fmt.Println(bb)
	fmt.Println(byte(bb))
	fmt.Println(int8(bb))
	fmt.Println(bb >> 8)
}
