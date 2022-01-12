package buffer_pool

/**
这个就是数据页的控制体，用来描述数据页部分的信息(大部分信息在buf_page_t中)。buf_block_t中第一字段就是buf_page_t，这个不是随意放的，
是必须放在第一字段，因为只有这样buf_block_t和buf_page_t两种类型的指针可以相互转换。第二个字段是frame字段，指向真正存数据的数据页。
buf_block_t还存储了Unzip LRU List链表的根节点。另外一个比较重要的字段就是block级别的mutex。

**/
type BufferBlock struct {
	BufferPage *BufferPage

	Frame *[]byte
}

func NewBufferBlock(frame *[]byte, spaceId, pageNo uint32) *BufferBlock {
	var bufferBlock = new(BufferBlock)
	bufferBlock.Frame = frame
	var bufferPage = NewBufferPage(spaceId, pageNo)

	bufferPage.spaceId = spaceId
	bufferPage.pageNo = pageNo
	bufferBlock.BufferPage = bufferPage
	return bufferBlock
}

func (bb BufferBlock) GetFrame() *[]byte {
	return bb.Frame
}

func (bb BufferBlock) GetSpaceId() uint32 {
	return bb.BufferPage.spaceId
}

func (bb BufferBlock) GetPageNo() uint32 {
	return bb.BufferPage.pageNo
}
