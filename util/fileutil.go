package util

import (
	"github.com/zhukovaskychina/xmysql-server/logger"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

func ListFileDirByPath(path string) map[string]string {
	resultMap := make(map[string]string)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		logger.Warnf("ListFileDirByPath 读取目录失败: %s, err=%v", path, err)
		return resultMap
	}
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
	if _, err := os.Stat(folderPath); err != nil {
		if !os.IsNotExist(err) {
			logger.Warnf("CreateDataBaseDir Stat失败: %s, err=%v", folderPath, err)
			return false
		}
		if err := os.Mkdir(folderPath, 0777); err != nil {
			logger.Warnf("CreateDataBaseDir Mkdir失败: %s, err=%v", folderPath, err)
			return false
		}
		if err := os.Chmod(folderPath, 0777); err != nil {
			logger.Warnf("CreateDataBaseDir Chmod失败: %s, err=%v", folderPath, err)
			return false
		}
	}
	return true
}
func CreateFile(filePath string, fileName string) error {

	f, err := os.Create(path.Join(filePath, fileName))
	if err != nil {
		logger.Warnf("CreateFile 失败: %s, err=%v", path.Join(filePath, fileName), err)
		return err
	}
	defer f.Close()
	return nil
}

func CreateFileWithPath(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		logger.Warnf("CreateFileWithPath 失败: %s, err=%v", filePath, err)
		return err
	}
	defer f.Close()
	return nil
}

func CreateFileBySize(filePath string, fileName string, size int64) error {
	f, err := os.Create(path.Join(filePath, fileName))
	if err != nil {
		logger.Warnf("CreateFileBySize 创建文件失败: %s, err=%v", path.Join(filePath, fileName), err)
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		logger.Warnf("CreateFileBySize Truncate 失败: %s, err=%v", path.Join(filePath, fileName), err)
		return err
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
	f, err := os.OpenFile(path.Join(filepath, fileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Warnf("WriteToFileByAppendBytes 打开文件失败: %s, err=%v", path.Join(filepath, fileName), err)
		return
	}
	defer f.Close()
	_, err = f.Write(content)
	logger.LogErr(err)
}

func ReadFileContent(filepath string, fileName string) ([]byte, error) {
	data, err := ioutil.ReadFile(path.Join(filepath, fileName))
	if err != nil {
		logger.Warnf("ReadFileContent 读取失败: %s, err=%v", path.Join(filepath, fileName), err)
		return nil, err
	}
	return data, nil

}

func ReadFileBySeekStart(filePath string, offset uint64) []byte {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		logger.Warnf("ReadFileBySeekStart 打开文件失败: %s, err=%v", filePath, err)
		return nil
	}
	defer f.Close()

	// 都是含前不含后的概念
	// offset是从0开始的, 可以比当前的文件内容长度大，多出的部分会用空(0)来代替
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		logger.Warnf("ReadFileBySeekStart Seek 失败: %s, err=%v", filePath, err)
		return nil
	}
	b := make([]byte, 16384)
	_, err = f.ReadAt(b, int64(offset))
	if err != nil {
		logger.Warnf("ReadFileBySeekStart 读取失败: %s, err=%v", filePath, err)
		return nil
	}

	return b
}
func ReadFileBySeekStartWithSize(filePath string, offset uint64, size int) []byte {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		logger.Warnf("ReadFileBySeekStartWithSize 打开文件失败: %s, err=%v", filePath, err)
		return nil
	}
	defer f.Close()

	// 都是含前不含后的概念
	// offset是从0开始的, 可以比当前的文件内容长度大，多出的部分会用空(0)来代替
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		logger.Warnf("ReadFileBySeekStartWithSize Seek 失败: %s, err=%v", filePath, err)
		return nil
	}
	b := make([]byte, size)
	_, err = f.ReadAt(b, int64(offset))
	if err != nil {
		logger.Warnf("ReadFileBySeekStartWithSize 读取失败: %s, err=%v", filePath, err)
		return nil
	}

	return b
}

func WriteFileBySeekStart(filePath string, offset uint64, data []byte) {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		logger.Warnf("WriteFileBySeekStart 打开文件失败: %s, err=%v", filePath, err)
		return
	}
	defer f.Close()
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		logger.Warnf("WriteFileBySeekStart Seek 失败: %s, err=%v", filePath, err)
		return
	}

	_, err = f.WriteAt(data, int64(offset))

	if err != nil {
		logger.Warnf("WriteFileBySeekStart 写入失败: %s, err=%v", filePath, err)
	}
}
