package util

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"io"
	_ "io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
)

func ListFileDirByPath(path string) map[string]string {
	resultMap := make(map[string]string)
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if f.IsDir() {
			dbName := f.Name()
			resultMap[dbName] = dbName
		}
	}
	return resultMap
}

func CreateDataBaseDir(Path string, folderName string) bool {
	folderPath := filepath.Join(Path, folderName)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		// 必须分成两步：先创建文件夹、再修改权限
		os.Mkdir(folderPath, 0777) //0777也可以os.ModePerm
		os.Chmod(folderPath, 0777)
		fmt.Println(err)
	}
	return true
}
func CreateFile(filePath string, fileName string) error {

	f, err := os.Create(path.Join(filePath, fileName))
	defer f.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func CreateFileWithPath(filePath string) error {
	f, err := os.Create(filePath)
	defer f.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func CreateFileBySize(filePath string, fileName string, size int64) error {
	f, err := os.Create(path.Join(filePath, fileName))
	defer f.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	if err := f.Truncate(size); err != nil {
		log.Fatal(err)
	}
	return nil
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
func WriteToFileByAppendBytes(filepath string, fileName string, content []byte) {
	f, err := os.OpenFile(path.Join(filepath, fileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModeAppend)
	defer f.Close()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		_, err = f.Write(content)
		logger.LogErr(err)
	}
}

func ReadFileContent(filepath string, fileName string) ([]byte, error) {
	data, err := ioutil.ReadFile(path.Join(filepath, fileName))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return data, err

}

func ReadFileBySeekStart(filePath string, offset uint64) []byte {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// 都是含前不含后的概念
	// offset是从0开始的, 可以比当前的文件内容长度大，多出的部分会用空(0)来代替
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	b := make([]byte, 16384)
	_, err = f.ReadAt(b, int64(offset))
	if err != nil {
		log.Fatal(err)
	}

	return b
}
func ReadFileBySeekStartWithSize(filePath string, offset uint64, size int) []byte {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// 都是含前不含后的概念
	// offset是从0开始的, 可以比当前的文件内容长度大，多出的部分会用空(0)来代替
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	b := make([]byte, size)
	_, err = f.ReadAt(b, int64(offset))
	if err != nil {
		log.Fatal(err)
	}

	return b
}

func WriteFileBySeekStart(filePath string, offset uint64, data []byte) {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.WriteAt(data, int64(offset))

	if err != nil {
		log.Fatal(err)
	}
}
