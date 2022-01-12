package basic

import (
	"xmysql-server/server/conf"
)

type FileSystem interface {
	AddTableSpace(ts FileTableSpace)

	GetTableSpaceById(spaceId uint32) FileTableSpace
}

//用于缓存TableSpace
type FileSystemSpace struct {
	FileSystem
	cfg    *conf.Cfg
	Spaces map[uint32]FileTableSpace
	NOpen  int //	ibd文件打开数量
}

func NewFileSystem(cfg *conf.Cfg) FileSystem {
	var fileSystem = new(FileSystemSpace)
	fileSystem.Spaces = make(map[uint32]FileTableSpace)
	fileSystem.NOpen = 0
	fileSystem.cfg = cfg
	return fileSystem
}

func (fs *FileSystemSpace) Initialize() {
	//fs.Spaces[0] = storebytes.NewSysTableSpace(fs.cfg)
}

func (fs *FileSystemSpace) AddTableSpace(ts FileTableSpace) {
	fs.Spaces[ts.GetSpaceId()] = ts
}

func (fs *FileSystemSpace) GetTableSpaceById(spaceId uint32) FileTableSpace {
	return fs.Spaces[spaceId]
}
