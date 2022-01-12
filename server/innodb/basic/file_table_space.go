package basic

type FileTableSpace interface {
	FlushToDisk(pageNo uint32, content []byte)

	LoadPageByPageNumber(pageNo uint32) ([]byte, error)

	GetSpaceId() uint32
}
