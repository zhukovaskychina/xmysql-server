package basic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

//just like(c)signed long long
//anyone can help me pls!
//How can I using like this?
// -9223372036854775808 +9223372036854775807
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return int(tmp)
}

func TestBigInt(t *testing.T) {

	//fmt.Println(int64(18446744073709551615))
	//constant 18446744073709551615 overflows int64
	var x uint64 = 18446744073709551615
	var y int64 = int64(x)

	var m int64 = -1
	var n uint64 = uint64(m)
	fmt.Println(y) //-1

	fmt.Println(n)
}
