package basic

// FileHeaderFields 文件头字段偏移量
const (
	FHeaderChecksum = 0  // 4字节
	FHeaderPageNo   = 4  // 4字节
	FHeaderPrevPage = 8  // 4字节
	FHeaderNextPage = 12 // 4字节
	FHeaderLSN      = 16 // 8字节
	FHeaderPageType = 24 // 2字节
	FHeaderFlushLSN = 26 // 8字节
	FHeaderSpaceID  = 34 // 4字节
	FileHeaderSize  = 38
)

// FileTrailerFields 文件尾字段偏移量
const (
	FTrailerChecksum = 0 // 4字节
	FTrailerLSNLow   = 4 // 4字节
	FileTrailerSize  = 8
)
