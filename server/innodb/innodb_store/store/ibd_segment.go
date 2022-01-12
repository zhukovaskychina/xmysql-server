package store

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/innodb_store/store/storebytes/segs"
)

//段是一个逻辑概念，管理非连续的区和离散页面
//
//段的创建过程，根据root,分为叶子段和非叶子段，以及回滚段
//查找到INODE的所在页面，然后根据页面偏移量找到INODE Entry结构记录，从而获取到具体的段ID
//

//初始化该段
//为了管理Inode Page，在文件头存储了两个Inode Page链表，
//一个链接已经用满的inode page，一个链接尚未用满的inode page。
//如果当前Inode Page的空间使用完了，就需要再分配一个inode page，
//并加入到FSP_SEG_INODES_FREE链表上(fsp_alloc_seg_inode_page)。
//对于独立表空间，通常一个inode page就足够了
//考虑到实际，目前以单独表空间为例，改写其逻辑，
//取0页面以及2号页面的值，
type Segment interface {
	basic.XMySQLSegment
	AllocatePage() *Index

	AllocateLeafPage() *Index

	AllocateInternalPage() *Index

	//申请新的区来拓展页面
	//如果空间不够，则需要自动拓展表空间大小
	//
	AllocateNewExtent() Extent

	//获取FsegNotFull链表被使用的Page数量
	GetNotFullNUsedSize() uint32

	//获取Free 即完全没有使用并分配该seg的Extent链表
	GetFreeExtentList() *ExtentList
	//获取Full 即完全使用并分配该seg的Extent链表
	GetFullExtentList() *ExtentList
	//获取Not 即完全使用并分配该seg的Extent链表
	GetNotFullExtentList() *ExtentList

	GetSegmentHeader() *segs.SegmentHeader
}
