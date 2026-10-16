package basic

import "github.com/zhukovaskychina/xmysql-server/server/common"

// FileHeaderFields 文件头字段偏移量
const (
	FHeaderChecksum = common.InnoDBFileHeaderChecksumOffset // 4字节
	FHeaderPageNo   = common.InnoDBFileHeaderPageNoOffset   // 4字节
	FHeaderPrevPage = common.InnoDBFileHeaderPrevOffset     // 4字节
	FHeaderNextPage = common.InnoDBFileHeaderNextOffset     // 4字节
	FHeaderLSN      = common.InnoDBFileHeaderLSNOffset      // 8字节
	FHeaderPageType = common.InnoDBFileHeaderPageTypeOffset // 2字节
	FHeaderFlushLSN = common.InnoDBFileHeaderFlushLSNOffset // 8字节
	FHeaderSpaceID  = common.InnoDBFileHeaderSpaceIDOffset  // 4字节
	FileHeaderSize  = common.FileHeaderSize
)

// FileTrailerFields 文件尾字段偏移量
const (
	FTrailerChecksum = common.InnoDBFileTrailerChecksumOffset // 4字节
	FTrailerLSNLow   = common.InnoDBFileTrailerLSNLowOffset   // 4字节
	FileTrailerSize  = common.FileTrailerSize
)
