package pages

// FSPPage 表空间头页面
type FSPPage struct {
	AbstractPage
	SpaceID         []byte // 4 bytes 表空间ID
	Size            []byte // 4 bytes 当前表空间大小(页面数)
	FreeLimit       []byte // 4 bytes 最小未使用页号
	Flags           []byte // 4 bytes 表空间标志位
	FreeFragNPages  []byte // 4 bytes 空闲碎片中的页面数
	NextSegmentID   []byte // 8 bytes 下一个段ID
	ListBaseNodes   []byte // 192 bytes 空闲区/页面链表基节点
	SegmentInodeMap []byte // 16320 bytes 段描述符位图
}

// NewFSPPage 创建新的FSP页面
func NewFSPPage() *FSPPage {
	return &FSPPage{
		SpaceID:         make([]byte, 4),
		Size:            make([]byte, 4),
		FreeLimit:       make([]byte, 4),
		Flags:           make([]byte, 4),
		FreeFragNPages:  make([]byte, 4),
		NextSegmentID:   make([]byte, 8),
		ListBaseNodes:   make([]byte, 192),
		SegmentInodeMap: make([]byte, 16320),
	}
}
