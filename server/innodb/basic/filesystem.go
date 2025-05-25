package basic

// FileSystem represents a file system that manages tablespaces
type FileSystem interface {
	AddTableSpace(ts FileTableSpace)
	GetTableSpaceById(spaceId uint32) FileTableSpace
}
