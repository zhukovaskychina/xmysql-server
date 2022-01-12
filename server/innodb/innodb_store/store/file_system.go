package store

//
////用于缓存TableSpace
//type FileSystemSpace struct {
//	innodb.FileSystem
//	cfg    *conf.Cfg
//	Spaces map[uint32]innodb.FileTableSpace
//	NOpen  int //	ibd文件打开数量
//}
//
//
//
//func NewFileSystem(cfg *conf.Cfg) innodb.FileSystem {
//	var fileSystem = new(FileSystemSpace)
//	fileSystem.Spaces = make(map[uint32]innodb.FileTableSpace)
//	fileSystem.NOpen = 0
//	fileSystem.cfg = cfg
//	return fileSystem
//}
//
////func (fs *FileSystemSpace) Initialize() {
////	fs.Spaces[0] = NewSysTableSpace(fs.cfg)
////}
//
//func (fs *FileSystemSpace) AddTableSpace(ts innodb.FileTableSpace) {
//	fs.Spaces[ts.GetSpaceId()] = ts
//}
//
//func (fs *FileSystemSpace) GetTableSpaceById(spaceId uint32) innodb.FileTableSpace {
//	return fs.Spaces[spaceId]
//}
