package store

import "github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/segs"

//128个回滚段，每个段1024个slot
type RollBackSegs struct {
}

func (r RollBackSegs) GetStatsCost(startPageNo, endPageNo uint32) map[string]int64 {
	panic("implement me")
}

func (r RollBackSegs) AllocatePage() *Index {
	panic("implement me")
}

func (r RollBackSegs) AllocateLeafPage() *Index {
	panic("implement me")
}

func (r RollBackSegs) AllocateInternalPage() *Index {
	panic("implement me")
}

func (r RollBackSegs) AllocateNewExtent() Extent {
	panic("implement me")
}

func (r RollBackSegs) GetNotFullNUsedSize() uint32 {
	panic("implement me")
}

func (r RollBackSegs) GetFreeExtentList() *ExtentList {
	panic("implement me")
}

func (r RollBackSegs) GetFullExtentList() *ExtentList {
	panic("implement me")
}

func (r RollBackSegs) GetNotFullExtentList() *ExtentList {
	panic("implement me")
}

func (r RollBackSegs) GetSegmentHeader() *segs.SegmentHeader {
	panic("implement me")
}
