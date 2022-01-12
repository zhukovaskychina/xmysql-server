package buffer_pool

import "xmysql-server/server/common"

//TODO 用来实现bufferpool
/**
这个可以理解为另外一个数据页的控制体，大部分的数据页信息存在其中，例如space_id, page_no, page state, newest_modification，
oldest_modification，access_time以及压缩页的所有信息等。压缩页的信息包括压缩页的大小，压缩页的数据指针(真正的压缩页数据是存储在由伙伴
系统分配的数据页上)。这里需要注意一点，如果某个压缩页被解压了，解压页的数据指针是存储在buf_block_t的frame字段里。

**/
type BufferPage struct {
	spaceId uint32

	pageNo uint32

	pageState BufferPageState

	flushType BufferFlushType

	iofix buffer_io_fix

	newestModification common.LSNT

	oldestModification common.LSNT

	accessTime uint64
}

func NewBufferPage(spaceId uint32, pageNo uint32) *BufferPage {
	var bufferPage = new(BufferPage)
	bufferPage.spaceId = spaceId
	bufferPage.pageNo = pageNo
	bufferPage.pageState = BUF_BLOCK_NOT_USED
	return bufferPage
}
