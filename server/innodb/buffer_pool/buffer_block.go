package buffer_pool

/**
这个就是数据页的控制体，用来描述数据页部分的信息(大部分信息在buf_page_t中)。buf_block_t中第一字段就是buf_page_t，这个不是随意放的，
是必须放在第一字段，因为只有这样buf_block_t和buf_page_t两种类型的指针可以相互转换。第二个字段是frame字段，指向真正存数据的数据页。
buf_block_t还存储了Unzip LRU List链表的根节点。另外一个比较重要的字段就是block级别的mutex。

**/
// BufferBlock represents a buffer block in the buffer pool
type BufferBlock struct {
	BufferPage *BufferPage // 页面控制信息
}

// NewBufferBlock creates a new buffer block
func NewBufferBlock(page *BufferPage) *BufferBlock {
	if page == nil {
		page = NewBufferPage(0, 0)
	}

	return &BufferBlock{
		BufferPage: page,
	}
}

// GetContent returns the page content
func (bb *BufferBlock) GetContent() []byte {
	return bb.BufferPage.GetContent()
}

// GetSpaceID returns the space ID
func (bb *BufferBlock) GetSpaceID() uint32 {
	return bb.BufferPage.GetSpaceID()
}

// GetPageNo returns the page number
func (bb *BufferBlock) GetPageNo() uint32 {
	return bb.BufferPage.GetPageNo()
}

// IsDirty returns whether the page is dirty
func (bb *BufferBlock) IsDirty() bool {
	return bb.BufferPage.IsDirty()
}

// SetDirty sets the dirty flag
func (bb *BufferBlock) SetDirty(dirty bool) {

	if dirty {
		bb.BufferPage.MarkDirty()
	} else {
		bb.BufferPage.ClearDirty()
	}
}

func (bb *BufferBlock) MarkDirty() {

}
