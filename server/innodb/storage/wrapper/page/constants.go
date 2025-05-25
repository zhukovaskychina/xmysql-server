package page

// Page constants
const (
	// XDES page constants
	XDES_HEADER_SIZE   = 16 // 4(spaceID) + 4(pageCount) + 4(extentSize) + 4(reserved)
	EXTENT_DESC_SIZE   = 40 // 4(ID) + 1(state) + 3(reserved) + 32(pageBits)
	XDES_BITS_PER_PAGE = 2  // 每个页面使用2位表示状态

	// IBuf bitmap page constants
	IBUF_BITMAP_HEADER_SIZE = 16 // 4(spaceID) + 4(pageCount) + 8(reserved)
	IBUF_BITS_PER_PAGE      = 4  // 每个页面使用4位
)
