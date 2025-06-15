package util

import (
	"fmt"
	"github.com/smartystreets/assertions"
	"io/ioutil"
	"os"
	"testing"
)

func TestWriteFileBySeekStart(t *testing.T) {

	buff := []byte{'A', 'B'}
	WriteFileBySeekStart("/home/zhukovasky/xmysql/test_simple_protocol.ibd", 38, buff)
	result := ReadFileBySeekStartWithSize("/home/zhukovasky/xmysql/test_simple_protocol.ibd", 38, 2)
	assertions.ShouldEqual(buff, result)
}

func TestWriteByte(t *testing.T) {
	err := ioutil.WriteFile(filename, []byte(start_data), 0644)
	if err != nil {
		panic(err)
	}

	printContents()

	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err := f.Seek(20, 0); err != nil {
		panic(err)
	}

	if _, err := f.WriteAt([]byte("A"), 15); err != nil {
		panic(err)
	}

	printContents()
}

const (
	filename   = "/home/zhukovasky/xmysql/sample.txt"
	start_data = "1234567890123456789012345678901234567890"
)

func printContents() {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}
