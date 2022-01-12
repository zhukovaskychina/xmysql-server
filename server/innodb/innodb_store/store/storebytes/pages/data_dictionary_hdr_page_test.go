package pages

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"xmysql-server/server/common"
	"xmysql-server/util"
)

func TestNewDataDictHeaderPage(t *testing.T) {
	dataDict := NewDataDictHeaderPage()
	fmt.Println(dataDict.FileHeader.FilePageType)
	assert.Equal(t, util.ReadUB2Byte2Int(dataDict.FileHeader.FilePageType), common.FILE_PAGE_TYPE_SYS)

	content := dataDict.GetSerializeBytes()

	dataDictWrapper := ParseDataDictHrdPage(content)

	assert.Equal(t, util.ReadUB2Byte2Int(dataDictWrapper.FileHeader.FilePageType), common.FILE_PAGE_TYPE_SYS)
}
