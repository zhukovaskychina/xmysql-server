package blocks

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/innodb_store/store/storebytes/pages"
	"xmysql-server/util"
)

func TestNewBlockFile(t *testing.T) {

	filePath := "/tmp/"

	blockFile := NewBlockFile(filePath, "myData", 16384)

	fmt.Println(blockFile.OpenState)
	fmt.Println(blockFile.Size())
	blockFile.Close()
	fmt.Println(blockFile.OpenState)
}

func TestBlockFile_GetFileName(t *testing.T) {

	filePath := "/tmp/"

	blockFile := NewBlockFile(filePath, "myData", 16384)

	assert.Equal(t, "myData", blockFile.GetFileName())
}

func TestBlockFile_Close(t *testing.T) {
}

func TestBlockFile_ReadPageByNumber(t *testing.T) {
	filePath := "/Users/zhukovasky/xmysql/tmp/"

	blockFile := NewBlockFile(filePath, "myIbData1"+time.Now().String(), 16384)
	blockFile.CreateFile()
	index := pages.NewIndexPage(0, 1)
	assert.Equal(t, int(util.ReadUB2Byte2Int(index.FileHeader.FilePageType)), common.FILE_PAGE_INDEX)
	blockFile.WriteFileBySeekStart(0, index.GetSerializeBytes())
	//blockFile.Close()
	blockFile.Do(0, func(bytes []byte) error {
		fmt.Println(bytes[24:26])
		assert.Equal(t, int(util.ReadUB2Byte2Int(bytes[24:26])), common.FILE_PAGE_INDEX)
		return nil
	})

	content, _ := blockFile.ReadPageByNumber(0)
	fmt.Println(content)

}
