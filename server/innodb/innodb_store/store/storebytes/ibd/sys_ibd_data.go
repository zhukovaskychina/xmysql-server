package ibd

import "github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/pages"

type SysIBData struct {
	FspHrdPage *pages.FspHrdBinaryPage //0

	IBufBitMapPage *pages.IBufBitMapPage //1

	FilePageINode *pages.INodePage //2

}
