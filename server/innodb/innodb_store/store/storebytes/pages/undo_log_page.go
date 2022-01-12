package pages

// 18 byte
type UndoLogPageHeader struct {
	TrxUndoPageType  []byte //2 byte 本页面存储什么类型的日志，可以分为 trx_insert and trx_update
	TrxUndoPageStart []byte //2 byte	第一条undo log日志在该页面中的起始偏移量
	TrxUndoPageFree  []byte //2 byte			与上面的trx_undo_page_start 对应，表示当前页面中存储的最后一条undo 日志的结束时候的偏移量
	TrxUndoPageNode  []byte //12 byte	代表一个链表节点结构
}

// 链表节点的结构
type PageNode struct {
	ListLength          []byte //4 byte
	FirstNodePageNumber []byte //4 byte 指向链表头节点的指针
	PrevNodeOffset      []byte //2 byte

	NextNodePageNumber []byte //4 byte 指向链表节点尾节点的指针
	NextNodeOffset     []byte //2 byte
}

//30 byte
type UndoLogSegmentHeader struct {
	TrxUndoState      []byte //2 byte
	TrxUndoLastLog    []byte //2 byte   本undo页面链表的中最后的一个undo log header 的位置
	TrxUndoFsegHeader []byte //10 byte 本undo页面链表对应的段的segment header信息，通过这个信息可以找到该段对应的INODEentry
	TrxUndoPageList   []byte //16 byte  undo页面链表的基节点
}

//***
// TrxUndoState 有如下状态
// TRX_UNDO_ACTIVE 活跃状态，也就是一个活跃的事务正在向这个undo页面链表写入undo日志
// TRX_UNDO_CACHED 被缓存的状态，处于该状态的undo页面链表等待之后被其他事务重用
// TRX_UNDO_TO_FREE 等待被释放的状态，对于Insert undo链表来说，如果在它对应的事务提交之后，该链表不能被重用，那么就会处于这种状态
// TRX_UNDO_TO_PURGE  等待被purge的状态。对于update undo链表来说，如果在它对应的事务提交之后，该链表不能被重用，那么就会处于该状态
// TRX_UNDO_PREPARED 处于此状态的undo 页面链表用于存储处于prepare阶段的事务产生的日志
//*/

//186 byte
type UndoLogHeader struct {
	TrxUndoTrxId       []byte //8 byte 生成本组undo log日志的事务ID
	TrxUndoTrxNo       []byte //8 bye 事务提交后生成的第一个序号，此序号用来标记事务的提交顺序，先提交的顺序小，后面的顺序大
	TrxUndoDelMarks    []byte //2 byte 标记本组undo日志中是否包含由delete mark操作产生的undo日志
	TrxUndoLogStart    []byte //2 byte 表示本组undo日志中第一条undo日志在页面中的偏移量
	TrxUndoXIDExists   []byte //1 byte 表示本组日志是否包含XID信息
	TrxUndoDictTrans   []byte //1 byte 标记本组undo日志是不是由DDL语句产生的。
	TrxUndoTableId     []byte //8byte 如果Trx_undo_dict_trans 为真，那么本属性表示DDL语句操作的表的table id
	TrxUndoNextLog     []byte //2 byte 下一组undo日志在页面中开始的偏移量
	TrxUndoPrevLog     []byte //2 byte 上一组undo日志在页面中的开始偏移量
	TrxUndoHistoryNode []byte //12 byte 一个12字节的链表节点结构，代表一个名为History 链表的节点。
	XIDINFO            []byte //140 byte
}

//其他undolog page
type UndoLogPage struct {
	AbstractPage
	UndoLogPageHeader *UndoLogPageHeader
}

//********
//   第一个UndoLogPage 结构如下
//	  UndoLogPageHeader
//	  UndoLogSegmentHeader
//	  UndoLogHeader
//    Real undo logs
//**** /
type UndoFirstLogPage struct {
}
