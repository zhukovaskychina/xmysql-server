package common

// Fixed storage page capacities. These do not replace configurable page sizes.
const (
	BlobHeaderSize         = 20
	BlobDataSize           = PageSize - FileHeaderSize - BlobHeaderSize - FileTrailerSize
	CompressionHeaderSize  = 16
	MaxCompressedDataSize  = PageSize - FileHeaderSize - CompressionHeaderSize - FileTrailerSize
	EncryptionHeaderSize   = 32
	MaxEncryptedDataSize   = PageSize - FileHeaderSize - EncryptionHeaderSize - FileTrailerSize
	SystemPageHeaderSize   = 32
	SystemPageDataSize     = PageSize - FileHeaderSize - SystemPageHeaderSize - FileTrailerSize
	IBufFreeListHeaderSize = 32
	IBufFreeListDataSize   = PageSize - FileHeaderSize - IBufFreeListHeaderSize - FileTrailerSize
	// The legacy blob wrapper counts the file header within its available space.
	LegacyBlobMaxSize = PageSize - BlobHeaderSize - FileTrailerSize
)
