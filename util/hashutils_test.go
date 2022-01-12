package util

import (
	"fmt"
	"testing"
)
import (
	"github.com/piex/transcode"
)

func TestHash(t *testing.T) {
	fmt.Println(HashCode([]byte("788788")))

	a := ConvertInt4Bytes(2)
	b := ConvertInt4Bytes(1)
	fmt.Println(0 < 1)
	fmt.Println(HashCode(a) < HashCode(b))
}

func TestCode(t *testing.T) {
	gbkString := "1"
	s := transcode.FromString(gbkString).Decode("GBK").ToString()
	transcode.FromByteArray(nil).Decode("")
	fmt.Println(s)
}
