package buffer_pool

//enum buf_page_state {
//BUF_BLOCK_POOL_WATCH,		/*!< a sentinel for the buffer_pool pool
//watch, element of buf_pool->watch[] */
//BUF_BLOCK_ZIP_PAGE,		/*!< contains a clean
//compressed page */
//BUF_BLOCK_ZIP_DIRTY,		/*!< contains a compressed
//page that is in the
//buf_pool->flush_list */
//
//BUF_BLOCK_NOT_USED,		/*!< is in the free list;
//must be after the BUF_BLOCK_ZIP_
//constants for compressed-only pages
//@see buf_block_state_valid() */
//BUF_BLOCK_READY_FOR_USE,	/*!< when buf_LRU_get_free_block
//returns a block, it is in this state */
//BUF_BLOCK_FILE_PAGE,		/*!< contains a buffered file page */
//BUF_BLOCK_MEMORY,		/*!< contains some main memory
//object */
//BUF_BLOCK_REMOVE_HASH		/*!< hash index should be removed
//before putting to the free list */
//};
//

//enum buf_page_state {
//BUF_BLOCK_POOL_WATCH,		/*!< a sentinel for the buffer_pool pool
//watch, element of buf_pool->watch[] */
//BUF_BLOCK_ZIP_PAGE,		/*!< contains a clean
//compressed page */
//BUF_BLOCK_ZIP_DIRTY,		/*!< contains a compressed
//page that is in the
//buf_pool->flush_list */
//
//BUF_BLOCK_NOT_USED,		/*!< is in the free list;
//must be after the BUF_BLOCK_ZIP_
//constants for compressed-only pages
//@see buf_block_state_valid() */
//BUF_BLOCK_READY_FOR_USE,	/*!< when buf_LRU_get_free_block
//returns a block, it is in this state */
//BUF_BLOCK_FILE_PAGE,		/*!< contains a buffered file page */
//BUF_BLOCK_MEMORY,		/*!< contains some main memory
//object */
//BUF_BLOCK_REMOVE_HASH		/*!< hash index should be removed
//before putting to the free list */
//};

type BufferPageState uint8

//这种类型的page是提供给purge线程用的。InnoDB为了实现多版本，需要把之前的数据记录在undo log中，如果没有读请求再需要它，
//就可以通过purge线程删除。换句话说，purge线程需要知道某些数据页是否被读取，现在解法就是首先查看page hash，
//看看这个数据页是否已经被读入，如果没有读入，则获取(启动时候通过malloc分配，不在Buffer Chunks中)一个BUF_BLOCK_POOL_WATCH类型的
//哨兵数据页控制体，同时加入page_hash但是没有真正的数据(buf_blokc_t::frame为空)并把其类型置为BUF_BLOCK_ZIP_PAGE
//(表示已经被使用了，其他purge线程就不会用到这个控制体了)，相关函数buf_pool_watch_set，如果查看page hash后发现有这个数据页，
//只需要判断控制体在内存中的地址是否属于Buffer Chunks即可，如果是表示对应数据页已经被其他线程读入了，
//相关函数buf_pool_watch_occurred。另一方面，如果用户线程需要这个数据页，先查看page hash看看是否是BUF_BLOCK_POOL_WATCH类型的
//数据页，如果是则回收这个BUF_BLOCK_POOL_WATCH类型的数据页，从Free List中(即在Buffer Chunks中)分配一个空闲的控制体，填入数据。
//这里的核心思想就是通过控制体在内存中的地址来确定数据页是否还在被使用。
const BUF_BLOCK_POOL_WATCH BufferPageState = 1

//当链表处于Free List中，状态就为此状态。是一个能长期存在的状态。
const BUF_BLOCK_NOT_USED BufferPageState = 2

/*当从Free List中，获取一个空闲的数据页时，状态会从BUF_BLOCK_NOT_USED
变为BUF_BLOCK_READY_FOR_USE(buf_LRU_get_free_block)，

也是一个比较短暂的状态。处于这个状态的数据页不处于任何逻辑链表中。
*/
const BUF_BLOCK_READY_FOR_USE BufferPageState = 3

//正常被使用的数据页都是这种状态。LRU List中，大部分数据页都是这种状态。压缩页被解压后，状态也会变成BUF_BLOCK_FILE_PAGE。
const BUF_BLOCK_FILE_PAGE BufferPageState = 4

//当加入Free List之前，需要先把page hash移除。因此这种状态就表示此页面page hash已经被移除，但是还没被加入到Free List中，
//是一个比较短暂的状态。 总体来说，大部分数据页都处于BUF_BLOCK_NOT_USED(全部在Free List中)和BUF_BLOCK_FILE_PAGE
//(大部分处于LRU List中，LRU List中还包含除被purge线程标记的BUF_BLOCK_ZIP_PAGE状态的数据页)状态，少部分处于BUF_BLOCK_MEMORY状态，
//极少处于其他状态。前三种状态的数据页都不在Buffer Chunks上，对应的控制体都是临时分配的，
//InnoDB把他们列为invalid state(buf_block_state_valid)。 如果理解了这八种状态以及其之间的转换关系
//，那么阅读Buffer pool的代码细节就会更加游刃有余。
const BUF_BLOCK_REMOVE_HASH BufferPageState = 5

//Buffer Pool中的数据页不仅可以存储用户数据，也可以存储一些系统信息，例如InnoDB行锁，
//自适应哈希索引以及压缩页的数据等，这些数据页被标记为BUF_BLOCK_MEMORY。处于这个状态的数据页不处于任何逻辑链表中
const BUF_BLOCK_MEMORY BufferPageState = 6

///** Flags for flush basic */
//enum buf_flush_t {
//BUF_FLUSH_LRU = 0,		/*!< flush via the LRU list */
//BUF_FLUSH_LIST,			/*!< flush via the flush list
//of dirty blocks */
//BUF_FLUSH_SINGLE_PAGE,		/*!< flush via the LRU list
//but only a single page */
//BUF_FLUSH_N_TYPES		/*!< index of last element + 1  */
//};
//
///** Algorithm to remove the pages for a tablespace from the buffer_pool pool.
//See buf_LRU_flush_or_remove_pages(). */
//enum buf_remove_t {
//BUF_REMOVE_ALL_NO_WRITE,	/*!< Remove all pages from the buffer_pool
//pool, don't write or sync to disk */
//BUF_REMOVE_FLUSH_NO_WRITE,	/*!< Remove only, from the flush list,
//don't write or sync to disk */
//BUF_REMOVE_FLUSH_WRITE		/*!< Flush dirty pages to disk only
//don't remove from the buffer_pool pool */
//};
//

type BufferFlushType uint8

const BUF_FLUSH_LRU BufferFlushType = 1

const BUF_FLUSH_LIST BufferFlushType = 2

const BUF_FLUSH_SINGLE_PAGE BufferFlushType = 3

const BUF_FLUSH_N_TYPES BufferFlushType = 4

//enum buf_io_fix {
//BUF_IO_NONE = 0,		/**< no pending I/O */
//BUF_IO_READ,			/**< read pending */
//BUF_IO_WRITE,			/**< write pending */
//BUF_IO_PIN			/**< disallow relocation of
//block and its removal of from
//the flush_list */
//};

type buffer_io_fix uint8

const (
	BUF_IO_NONE buffer_io_fix = iota
	BUF_IO_READ
	BUF_IO_WRITE
	BUF_IO_PIN
)
