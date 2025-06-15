/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesToString(t *testing.T) {
	{
		bs := []byte{0x61, 0x62}
		want := "ab"
		got := BytesToString(bs)
		assert.Equal(t, want, got)
	}

	{
		bs := []byte{}
		want := ""
		got := BytesToString(bs)
		assert.Equal(t, want, got)
	}
}

func TestSting(t *testing.T) {
	{
		want := []byte{0x61, 0x62}
		got := StringToBytes("ab")
		assert.Equal(t, want, got)
	}

	{
		want := []byte{}
		got := StringToBytes("")
		assert.Equal(t, want, got)
	}
}

func TestStingToBytes(t *testing.T) {
	{
		want := []byte{0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x2a, 0x20, 0x46, 0x52, 0x4f, 0x4d, 0x20, 0x74, 0x32}
		got := StringToBytes("SELECT * FROM t2")
		assert.Equal(t, want, got)
	}
}

func TestToJSONString(t *testing.T) {
	type input struct {
		A string
		B int
	}

	testcases := []struct {
		in  *input
		out string
	}{
		{
			in:  &input{A: "a>1", B: 1},
			out: "{\t\"A\": \"a>1\",\t\"B\": 1}",
		},
	}

	for _, testcase := range testcases {
		got, err := ToJSONString(testcase.in, false, "", "\t")
		assert.Nil(t, err)
		assert.Equal(t, testcase.out, got)
	}

}

func TestCRC32(t *testing.T) {
	result := CRC32("HELLO")
	fmt.Println(result)

	a, b := 10, 0
	if a < 0 && a/b > 0 {
		fmt.Println("OK")
	}
}

func TestName(t *testing.T) {
	var i = 16 & 7
	fmt.Println(i)

	var m = 16 >> 3

	fmt.Println(m)
}
