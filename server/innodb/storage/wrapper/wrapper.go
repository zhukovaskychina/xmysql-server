package wrapper

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
	"github.com/zhukovaskychina/xmysql-server/util"
)

// IPageWrapper 使用统一的页面包装器接口
// 此类型别名用于向后兼容，新代码应直接使用 types.IPageWrapper
type IPageWrapper = types.IPageWrapper

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
