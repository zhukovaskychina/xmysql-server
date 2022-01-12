package mvcc

//***
//在Innodb中，每次开启一个事务时，都会为该session分配一个事务对象。而为了对全局所有的事务进行控制和协调，
//有一个全局对象trx_sys，对trx_sys相关成员的操作需要trx_sys->mutex锁。
// 若访问记录trx_id 等于readview的creator_trx_id,则访问的是自己修改过自己的记录，当前记录可以被访问
// 若访问记录trx_id 小于readview的min_trx_id ,表明生成的该版本的事务生成readview已经提交，该版本可以被访问
// 如果被访问版本的trx_id 属性值，大于或等于read_view 中的max_trx_id 值，表明生成该版本的事务是在当前事务readview之后，该版本不可以被访问
// 如果被访问该版本的trx_id属性值在readView的min 和max之间，则需要判断trx_id是否在m_ids 列表中。如果在，说明创建readview生成的该版本的事务还是活跃的，该版本不可以被
// 如果不在，说明创建readview生成该版本的事务已经被提交，该版本可以被访问

// 如果不能访问该版本，则访问它的下一个版本的数据。

//**//

type TrxId struct {
	id [6]byte
}

/****
所有回滚段都记录在trx_sys->rseg_array，数组大小为128，分别对应不同的回滚段；
rseg_array数组类型为trx_rseg_t，用于维护回滚段相关信息；
每个回滚段对象trx_rseg_t还要管理undo log信息，对应结构体为trx_undo_t，使用多个链表来维护trx_undo_t信息;
事务开启时，会专门给他指定一个回滚段，以后该事务用到的undo log页，就从该回滚段上分配;
事务提交后，需要purge的回滚段会被放到purge队列上(purge_sys->purge_queue)。


Valid state transitions are:

	Regular transactions:
	* NOT_STARTED -> ACTIVE -> COMMITTED -> NOT_STARTED

	Auto-commit non-locking read-only:
	* NOT_STARTED -> ACTIVE -> NOT_STARTED

	XA (2PC):
	* NOT_STARTED -> ACTIVE -> PREPARED -> COMMITTED -> NOT_STARTED

	Recovered XA:
	* NOT_STARTED -> PREPARED -> COMMITTED -> (freed)

	XA (2PC) (shutdown before ROLLBACK or COMMIT):
	* NOT_STARTED -> PREPARED -> (freed)

***/
type GlobalTrxSys struct {
	currentTrxId TrxId //6 byte 当前事务ID 最大值2^(48)-1 281474976710655

	lsn uint64 // 8 byte 日志sequenceId

}
