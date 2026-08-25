package util

import (
	"fmt"
	"github.com/smartystreets/assertions"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileBySeekStart(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_simple_protocol.ibd")

	buff := []byte{'A', 'B'}
	WriteFileBySeekStart(testFile, 38, buff)
	result := ReadFileBySeekStartWithSize(testFile, 38, 2)
	assertions.ShouldEqual(buff, result)
}

func TestWriteByte(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "sample.txt")

	err := ioutil.WriteFile(filename, []byte(start_data), 0644)
	if err != nil {
		panic(err)
	}

	printContents(filename)

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

	printContents(filename)
}

const start_data = "1234567890123456789012345678901234567890"

func printContents(file string) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}
