package common

const PAGE_SIZE = 16384
const PAGE_FILE_HEADER_SIZE = 38
const PAGE_PAGE_HEADER_SIZE = 56
const PAGE_INFIMUMSUPERUM_SIZE = 26
const PAGE_FILE_TRAILER_SIZE = 8

//最新分配，还未使用
const FILE_PAGE_TYPE_ALLOCATED = 0x0000

//undo 日志页面
const FILE_PAGE_UNDO_LOG = 0x0002

const FILE_PAGE_INODE = 0x0003

const FILE_PAGE_BUF_FREE_LIST = 0x0004

const FILE_PAGE_IBUF_BITMAP = 0x0005

const FILE_PAGE_TYPE_SYS = 0x0006

const FILE_PAGE_TYPE_TRX_SYS = 0x0007

const FILE_PAGE_TYPE_FSP_HDR = 0x0008

const FILE_PAGE_TYPE_XDES = 0x0009

const FILE_PAGE_TYPE_BLOB = 0x000A

//索引页，也就是数据页面
const FILE_PAGE_INDEX = 0x45BF

//页面插入类型
const (
	PAGE_LEFT         = 0x01
	PAGE_RIGHT        = 0x02
	PAGE_SAME_REC     = 0x03
	PAGE_SAME_PAGE    = 0x04
	PAGE_NO_DIRECTION = 0x05
)

//记录类型
//也就是叶子节点
const ORDINARY_RECORD = 0

//非叶子节点
const NOLEAF_RECORD = 1
const INFIMUM_RECORD = 2
const SUPREMUM_RECORD = 3

//MinRecFlag
const LEAF_RECORD_TYPE = 1 //叶子节点

const NO_LEAF_RECORD_TYPE = 0 //枝干

const COMMON_TRUE = 1

const COMMON_FALSE = 0

const DELETE_OFFSET = 2

const MIN_REC_OFFSET = 3

const N_OWNED_OFFSET = 4

// 叶子页面
const PAGE_LEAF = "0"

//非叶子节点
const PAGE_INTERNAL = "1"
