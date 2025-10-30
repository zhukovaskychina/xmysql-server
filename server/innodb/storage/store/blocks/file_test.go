package blocks

import (
	"github.com/stretchr/testify/assert"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/util"
	"os"
	"testing"
	"time"
)

func TestNewBlockFile(t *testing.T) {
	filePath := "/tmp/"
	fileName := "test_block_file_" + time.Now().Format("20060102150405")

	blockFile := NewBlockFile(filePath, fileName, 16384)
	defer os.Remove(filePath + fileName)

	// Test opening the file
	err := blockFile.Open()
	assert.NoError(t, err)

	// Test closing the file
	err = blockFile.Close()
	assert.NoError(t, err)
}

func TestBlockFile_ReadWrite(t *testing.T) {
	filePath := "/tmp/"
	fileName := "test_rw_" + time.Now().Format("20060102150405")

	blockFile := NewBlockFile(filePath, fileName, 16384*10)
	defer os.Remove(filePath + fileName)

	err := blockFile.Open()
	assert.NoError(t, err)
	defer blockFile.Close()

	// Create an index page
	index := pages.NewIndexPage(0, 1)
	assert.Equal(t, util.ReadUB2Byte2Int(index.FileHeader.FilePageType[:]), uint16(common.FILE_PAGE_INDEX))

	// Write the page
	err = blockFile.WritePage(0, index.GetSerializeBytes())
	assert.NoError(t, err)

	// Read the page back
	content, err := blockFile.ReadPage(0)
	assert.NoError(t, err)
	assert.NotNil(t, content)

	// Verify the page type
	assert.Equal(t, util.ReadUB2Byte2Int(content[24:26]), uint16(common.FILE_PAGE_INDEX))
}

func TestBlockFile_Close(t *testing.T) {
	filePath := "/tmp/"
	fileName := "test_close_" + time.Now().Format("20060102150405")

	blockFile := NewBlockFile(filePath, fileName, 16384)
	defer os.Remove(filePath + fileName)

	err := blockFile.Open()
	assert.NoError(t, err)

	err = blockFile.Close()
	assert.NoError(t, err)

	// Closing again should not error
	err = blockFile.Close()
	assert.NoError(t, err)
}
