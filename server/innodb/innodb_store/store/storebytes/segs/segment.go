package segs

import "github.com/zhukovaskychina/xmysql-server/util"

type SegmentHeader struct {
	INodeEntrySpaceId    []byte // 4 byte  INode Entry 结构所在的表空间
	PageNumberINodeEntry []byte // 4byte	INode Entry 所在的页面编号
	ByteOffsetINodeEntry []byte //2 byte 	INode Entry 在该页面中的偏移量
}

func (s *SegmentHeader) WriteINodeSpaceId(spaceId int) {
	s.INodeEntrySpaceId = util.ConvertInt4Bytes(int32(spaceId))
}

func NewSegmentHeader(spaceId uint32, pageNumber uint32, offset uint16) *SegmentHeader {
	return &SegmentHeader{
		INodeEntrySpaceId:    util.ConvertUInt4Bytes(spaceId),
		PageNumberINodeEntry: util.ConvertUInt4Bytes(pageNumber),
		ByteOffsetINodeEntry: util.ConvertUInt2Bytes(offset*192 + 50),
	}

}

func (s *SegmentHeader) GetBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, s.INodeEntrySpaceId...)
	buff = append(buff, s.PageNumberINodeEntry...)
	buff = append(buff, s.ByteOffsetINodeEntry...)
	return buff
}
