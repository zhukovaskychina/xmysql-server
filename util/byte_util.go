package util

import (
	"bytes"
	"encoding/gob"
	"reflect"
)

func AppendByte(size int) []byte {

	var buff = make([]byte, 0)

	for i := 0; i < size; i++ {
		buff = append(buff, 0)
	}

	return buff
}

func StringArrays2Bytes(values []string) []byte {

	var buff = make([]byte, 0)

	for _, v := range values {
		buff = append(buff, []byte(v)...)
	}

	return buff
}

func GetBytes(key interface{}) ([]byte, error) {
	if IsNil(key) {
		return nil, nil
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
