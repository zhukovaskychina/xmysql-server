package util

import (
	"fmt"
	"testing"
)

func TestName(t *testing.T) {

	var intarrays = make([]int, 0)
	intarrays = append(intarrays, 1)
	intarrays = append(intarrays, 2)
	intarrays = append(intarrays, 3, 4, 5, 6)

	intarrays = append(intarrays[0:1], intarrays[2:]...)
	intarrays = append(intarrays[0:1], 2)

}

func Test_ConvertBytes2BitStrings(t *testing.T) {

	content := ConvertInt4Bytes(128)

	fmt.Println(content)

	fmt.Println(ConvertBytes2BitStrings(content))
}
