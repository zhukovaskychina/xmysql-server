package blocks

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/util"
	"io"
	"log"
	"os"
	"path"
)

//存储中间层
type BlockFile struct {
	StorageFile *os.File
	FilePath    string
	FileName    string
	FileSize    int64
	OpenState   int
	ReadNumber  int //读数量
	WriteNumber int //写数量
}

//os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModeAppend
func NewBlockFile(filePath string, fileName string, fileSize int64) *BlockFile {
	blockFile := new(BlockFile)

	blockFile.FilePath = filePath
	blockFile.FileName = fileName
	blockFile.FileSize = int64(fileSize)
	blockFile.OpenState = 2
	blockFile.ReadNumber = 0
	blockFile.WriteNumber = 0
	return blockFile
}

func (blockFile *BlockFile) CreateFile() {
	fileName := blockFile.FileName
	filePath := blockFile.FilePath
	fileSize := blockFile.FileSize
	fileFlag, _ := util.PathExists(path.Join(filePath, fileName))
	blockFile.ReadNumber = 0
	blockFile.WriteNumber = 0
	if fileFlag {
		blockFile.FilePath = path.Join(filePath, fileName)
		blockFile.FileName = fileName
		osfiles, _ := os.OpenFile(path.Join(filePath, fileName), os.O_RDWR, os.ModePerm)
		blockFile.StorageFile = osfiles
		blockFile.OpenState = 1

	} else {
		f, err := os.Create(path.Join(filePath, fileName))
		defer f.Close()
		if err != nil {
			log.Fatal(err.Error())
			panic(err)
		}
		if err := f.Truncate(int64(fileSize)); err != nil {
			log.Fatal(err)
		}
		osfiles, _ := os.OpenFile(path.Join(filePath, fileName), os.O_RDWR, os.ModePerm)
		blockFile.StorageFile = osfiles
		blockFile.FilePath = path.Join(filePath, fileName)
		blockFile.FileName = fileName
		blockFile.OpenState = 1
	}
}

func NewBlockFileWithoutFileSize(filePath string, fileName string) *BlockFile {
	blockFile := new(BlockFile)
	fileFlag, _ := util.PathExists(path.Join(filePath, fileName))
	blockFile.ReadNumber = 0
	blockFile.WriteNumber = 0
	if fileFlag {
		blockFile.FilePath = path.Join(filePath, fileName)
		blockFile.FileName = fileName
		osfiles, _ := os.OpenFile(path.Join(filePath, fileName), os.O_RDWR, os.ModePerm)
		blockFile.StorageFile = osfiles
		blockFile.OpenState = 1

	} else {
		f, err := os.Create(path.Join(filePath, fileName))
		defer f.Close()
		if err != nil {
			log.Fatal(err.Error())
			panic(err)
			return nil
		}
		osfiles, _ := os.OpenFile(path.Join(filePath, fileName), os.O_RDWR, os.ModePerm)
		blockFile.StorageFile = osfiles
		blockFile.FilePath = path.Join(filePath, fileName)
		blockFile.FileName = fileName
		blockFile.OpenState = 1
	}
	return blockFile
}

func (blockFile *BlockFile) Exists() bool {
	fileFlag, _ := util.PathExists(path.Join(blockFile.FilePath, blockFile.FileName))
	//fileFlag, _ := util.PathExists(blockFile.FilePath)
	return fileFlag
}

func (blockFile *BlockFile) Close() {
	blockFile.OpenState = 2
	blockFile.StorageFile.Close()
}
func (blockFile *BlockFile) GetFileName() string {
	return blockFile.FileName
}

/***
*向文件中写入内容
*
***/
func (blockFile *BlockFile) WriteContentToBlockFile(content []byte) {
	blockFile.StorageFile.Write(content)
}

func (blockFile *BlockFile) OpenFile() {
	if blockFile.OpenState == 2 {
		file, _ := os.OpenFile(path.Join(blockFile.FilePath, blockFile.FileName), os.O_RDWR, os.ModePerm)
		blockFile.StorageFile = file
		blockFile.OpenState = 1
	}
}

func (blockFile *BlockFile) ReadPageByNumber(pageNumber uint32) ([]byte, error) {
	blockFile.OpenFile()

	// 都是含前不含后的概念
	// offset是从0开始的, 可以比当前的文件内容长度大，多出的部分会用空(0)来代替
	_, err := blockFile.StorageFile.Seek(int64(pageNumber)*common.PAGE_SIZE, io.SeekStart)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	b := make([]byte, common.PAGE_SIZE)
	_, err = blockFile.StorageFile.ReadAt(b, int64(pageNumber*(common.PAGE_SIZE)))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return b, nil
}

func (blockFile *BlockFile) ReadFileBySeekStartWithSize(offset uint64, size int64) ([]byte, error) {
	blockFile.OpenFile()
	// 都是含前不含后的概念
	// offset是从0开始的, 可以比当前的文件内容长度大，多出的部分会用空(0)来代替
	_, err := blockFile.StorageFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	b := make([]byte, size)
	_, err = blockFile.StorageFile.ReadAt(b, int64(offset))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return b, nil
}

/***
*向文件中写入内容
*@offset 起始位置
@data 内容
***/

func (blockFile *BlockFile) WriteFileBySeekStart(offset uint64, data []byte) {
	blockFile.OpenFile()
	_, err := blockFile.StorageFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	_, err = blockFile.StorageFile.WriteAt(data, int64(offset))

	if err != nil {
		log.Fatal(err)
	}

}

//****
//根据
//***//
func (blockFile *BlockFile) WriteContentByPage(pageNum int64, data []byte) error {
	blockFile.OpenFile()
	_, err := blockFile.StorageFile.Seek(int64(pageNum)*common.PAGE_SIZE, io.SeekStart)
	blockFile.AddRead()
	if err != nil {
		log.Fatal(err)
		blockFile.RealeaseRead()
		return err
	}
	blockFile.RealeaseRead()
	blockFile.AddWrite()
	_, err = blockFile.StorageFile.WriteAt(data, int64(pageNum)*common.PAGE_SIZE)

	if err != nil {
		blockFile.RealeaseWrite()
		log.Fatal(err)
		return err
	}
	blockFile.RealeaseWrite()
	return nil
}

func (blockFile *BlockFile) Size() int64 {
	if blockFile.OpenState == 2 {
		blockFile.OpenFile()
	}
	fmt.Println(blockFile.FilePath)
	fd, _ := os.Stat(blockFile.FilePath)
	return fd.Size()
}

/**
根据页面号写入页面
**/
func (blockFile *BlockFile) WritePageContentFileBySeekStart(pageOffset uint64, data []byte) error {
	blockFile.OpenFile()
	blockFile.AddRead()
	_, err := blockFile.StorageFile.Seek(int64(pageOffset)*16384, io.SeekStart)
	if err != nil {
		log.Fatal(err)
		blockFile.RealeaseRead()
		return err
	}
	blockFile.AddWrite()
	_, err = blockFile.StorageFile.WriteAt(data, int64(pageOffset)*16384)

	if err != nil {
		blockFile.RealeaseWrite()
		log.Fatal(err)
		return err
	}
	blockFile.RealeaseWrite()
	return nil
}

//
func (blockFile *BlockFile) Do(offset uint32, do func([]byte) error) error {
	bytes, err := blockFile.ReadPageByNumber(uint32(offset))
	if err != nil {
		return err
	}
	err = do(bytes)
	return err
}

//通常加载64个页面
func (blockFile BlockFile) DoRange(startOffset uint32, endOffset uint32, do func([]byte, uint32, uint32) error) error {
	bytes, err := blockFile.ReadFileBySeekStartWithSize(uint64(startOffset*common.PAGE_SIZE), int64((endOffset-startOffset)*(common.PAGE_SIZE)))
	if err != nil {
		return err
	}
	do(bytes, startOffset, endOffset)
	return nil
}

func (blockFile *BlockFile) RealeaseRead() {
	blockFile.ReadNumber--
}

func (blockFile *BlockFile) RealeaseWrite() {
	blockFile.WriteNumber--
}
func (blockFile *BlockFile) AddRead() {
	blockFile.ReadNumber++
}

func (blockFile *BlockFile) AddWrite() {
	blockFile.WriteNumber++
}
