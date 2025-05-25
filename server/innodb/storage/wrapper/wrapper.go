package wrapper

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"github.com/zhukovaskychina/xmysql-server/util"
)

type IPageWrapper interface {
	GetFileHeader() *pages.FileHeader

	GetFileTrailer() *pages.FileTrailer

	ToByte() []byte
}

type CommonNodeInfo struct {
	NodeInfoLength     uint32 //节点数量
	PreNodePageNumber  uint32 //上一个节点的页面号
	PreNodeOffset      uint16 //上一个节点的偏移量
	NextNodePageNumber uint32 //下一个节点的页面号
	NextNodeOffset     uint16 //下一个节点的偏移量
}

func (ci *CommonNodeInfo) ToBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, util.ConvertUInt4Bytes(ci.NodeInfoLength)...)
	buff = append(buff, util.ConvertUInt4Bytes(ci.PreNodePageNumber)...)
	buff = append(buff, util.ConvertUInt2Bytes(ci.PreNodeOffset)...)
	buff = append(buff, util.ConvertUInt4Bytes(ci.NextNodePageNumber)...)
	buff = append(buff, util.ConvertUInt2Bytes(ci.NextNodeOffset)...)
	return buff
}
