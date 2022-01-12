package store

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestTupleTest(t *testing.T) {
	var val interface{} = nil
	fmt.Println(reflect.ValueOf(val))
	if IsNil(val) {
		fmt.Println("val is nil")
	} else {
		fmt.Println("val is not nil")
	}
}
func IsNil(i interface{}) bool {
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	if vi.Kind() == reflect.Invalid {
		return true
	}
	return false
}

func TestName(t *testing.T) {
	fmt.Println(math.Mod(8, 8))
	fmt.Println(19 >> 3)

	fmt.Println(19 & 7)
}
