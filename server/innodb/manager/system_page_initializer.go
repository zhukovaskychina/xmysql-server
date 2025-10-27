package manager

import (
	"encoding/binary"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

/*
SystemPageInitializer 系统页面初始化器

负责初始化系统表空间(ibdata1)的固定系统页面：
- Page 0: FSP Header (File Space Header)
- Page 1: IBUF Bitmap (Insert Buffer Bitmap)
- Page 2: INODE (Segment Inode Page)
- Page 3: SYS (System Page)
- Page 4: INDEX (Index Page)
- Page 5: DICT_ROOT (Data Dictionary Root)
- Page 6: TRX_SYS (Transaction System)
- Page 7: FIRST_RSEG (First Rollback Segment)

设计要点：
1. 确保页面位置固定不变
2. 页面初始化幂等性
3. 页面格式与MySQL兼容
*/

const (
	// 页面大小常量
	PAGE_SIZE = 16384 // 16KB

	// 文件头大小
	FIL_HEADER_SIZE  = 38
	FIL_TRAILER_SIZE = 8

	// FSP Header偏移量
	FSP_HEADER_OFFSET   = FIL_HEADER_SIZE
	FSP_SPACE_ID        = 0
	FSP_NOT_USED        = 4
	FSP_SIZE            = 8
	FSP_FREE_LIMIT      = 12
	FSP_SPACE_FLAGS     = 16
	FSP_FRAG_N_USED     = 20
	FSP_FREE            = 24
	FSP_FREE_FRAG       = 32
	FSP_FULL_FRAG       = 40
	FSP_SEG_ID          = 48
	FSP_SEG_INODES_FULL = 56
	FSP_SEG_INODES_FREE = 64

	// 事务系统页面偏移量
	TRX_SYS_TRX_ID_STORE          = FIL_HEADER_SIZE + 0
	TRX_SYS_FSEG_HEADER           = FIL_HEADER_SIZE + 8
	TRX_SYS_RSEGS                 = FIL_HEADER_SIZE + 18
	TRX_SYS_MYSQL_LOG_MAGIC_N_FLD = FIL_HEADER_SIZE + 2050
	TRX_SYS_MYSQL_LOG_OFFSET_HIGH = FIL_HEADER_SIZE + 2054
	TRX_SYS_MYSQL_LOG_OFFSET_LOW  = FIL_HEADER_SIZE + 2058
	TRX_SYS_DOUBLEWRITE           = FIL_HEADER_SIZE + 2078

	// 数据字典根页面偏移量
	DICT_HDR_ROW_ID       = FIL_HEADER_SIZE + 0
	DICT_HDR_TABLE_ID     = FIL_HEADER_SIZE + 8
	DICT_HDR_INDEX_ID     = FIL_HEADER_SIZE + 16
	DICT_HDR_MAX_SPACE_ID = FIL_HEADER_SIZE + 24
	DICT_HDR_MIX_ID_LOW   = FIL_HEADER_SIZE + 28
	DICT_HDR_TABLES       = FIL_HEADER_SIZE + 32
	DICT_HDR_TABLE_IDS    = FIL_HEADER_SIZE + 42
	DICT_HDR_COLUMNS      = FIL_HEADER_SIZE + 52
	DICT_HDR_INDEXES      = FIL_HEADER_SIZE + 62
	DICT_HDR_FIELDS       = FIL_HEADER_SIZE + 72
)

// SystemPageInitializer 系统页面初始化器
type SystemPageInitializer struct {
	bufferPool *buffer_pool.BufferPool
	spaceID    uint32
}

// NewSystemPageInitializer 创建系统页面初始化器
func NewSystemPageInitializer(bufferPool *buffer_pool.BufferPool, spaceID uint32) *SystemPageInitializer {
	return &SystemPageInitializer{
		bufferPool: bufferPool,
		spaceID:    spaceID,
	}
}

// InitializeSystemPages 初始化所有系统页面
func (spi *SystemPageInitializer) InitializeSystemPages() error {
	// Page 0: FSP Header
	if err := spi.initFSPHeaderPage(); err != nil {
		return fmt.Errorf("failed to initialize FSP header page: %v", err)
	}

	// Page 1: IBUF Bitmap
	if err := spi.initIBufBitmapPage(); err != nil {
		return fmt.Errorf("failed to initialize IBUF bitmap page: %v", err)
	}

	// Page 2: INODE
	if err := spi.initInodePage(); err != nil {
		return fmt.Errorf("failed to initialize INODE page: %v", err)
	}

	// Page 3: SYS
	if err := spi.initSysPage(); err != nil {
		return fmt.Errorf("failed to initialize SYS page: %v", err)
	}

	// Page 5: Data Dictionary Root
	if err := spi.initDictRootPage(); err != nil {
		return fmt.Errorf("failed to initialize data dictionary root page: %v", err)
	}

	// Page 6: Transaction System
	if err := spi.initTrxSysPage(); err != nil {
		return fmt.Errorf("failed to initialize transaction system page: %v", err)
	}

	// Page 7: First Rollback Segment
	if err := spi.initFirstRsegPage(); err != nil {
		return fmt.Errorf("failed to initialize first rollback segment page: %v", err)
	}

	return nil
}

// initFSPHeaderPage 初始化FSP Header页面（Page 0）
func (spi *SystemPageInitializer) initFSPHeaderPage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 0, SYS_PAGE_TYPE_FSP_HDR)

	// 初始化FSP Header
	offset := FSP_HEADER_OFFSET

	// Space ID
	binary.BigEndian.PutUint32(pageData[offset+FSP_SPACE_ID:], spi.spaceID)

	// Not Used (填充0)
	binary.BigEndian.PutUint32(pageData[offset+FSP_NOT_USED:], 0)

	// Size (初始大小：64页，即1个Extent)
	binary.BigEndian.PutUint32(pageData[offset+FSP_SIZE:], 64)

	// Free Limit (初始为64)
	binary.BigEndian.PutUint32(pageData[offset+FSP_FREE_LIMIT:], 64)

	// Space Flags
	binary.BigEndian.PutUint32(pageData[offset+FSP_SPACE_FLAGS:], 0)

	// Frag N Used (Fragment已使用数量)
	binary.BigEndian.PutUint32(pageData[offset+FSP_FRAG_N_USED:], 0)

	// 初始化链表（Free, Free Frag, Full Frag）
	// 这些是双向链表头，初始为空
	spi.initListNode(pageData, offset+FSP_FREE)
	spi.initListNode(pageData, offset+FSP_FREE_FRAG)
	spi.initListNode(pageData, offset+FSP_FULL_FRAG)

	// Next Segment ID
	binary.BigEndian.PutUint64(pageData[offset+FSP_SEG_ID:], 1)

	// Segment Inode链表
	spi.initListNode(pageData, offset+FSP_SEG_INODES_FULL)
	spi.initListNode(pageData, offset+FSP_SEG_INODES_FREE)

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	// 写入缓冲池
	return spi.writePageToBufferPool(0, pageData)
}

// initIBufBitmapPage 初始化Insert Buffer Bitmap页面（Page 1）
func (spi *SystemPageInitializer) initIBufBitmapPage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 1, SYS_PAGE_TYPE_IBUF_BITMAP)

	// IBUF Bitmap页面体：全部初始化为0
	// 每个bit表示一个页面的IBUF状态

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	return spi.writePageToBufferPool(1, pageData)
}

// initInodePage 初始化INODE页面（Page 2）
func (spi *SystemPageInitializer) initInodePage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 2, SYS_PAGE_TYPE_INODE)

	// INODE页面包含段信息节点数组
	// 简化实现：初始化为空的INODE列表

	offset := FIL_HEADER_SIZE

	// List Node for Free List
	spi.initListNode(pageData, offset)
	offset += 12

	// Magic Number
	binary.BigEndian.PutUint32(pageData[offset:], 0x12345678)

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	return spi.writePageToBufferPool(2, pageData)
}

// initSysPage 初始化系统页面（Page 3）
func (spi *SystemPageInitializer) initSysPage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 3, SYS_PAGE_TYPE_SYS)

	// 系统页面可以用于存储额外的系统级信息
	// 简化实现：初始化为空

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	return spi.writePageToBufferPool(3, pageData)
}

// initDictRootPage 初始化数据字典根页面（Page 5）
func (spi *SystemPageInitializer) initDictRootPage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 5, SYS_PAGE_TYPE_DICT_ROOT)

	offset := FIL_HEADER_SIZE

	// Row ID（初始值：256）
	binary.BigEndian.PutUint64(pageData[offset+DICT_HDR_ROW_ID-FIL_HEADER_SIZE:], 256)

	// Table ID（初始值：14，系统表占用1-13）
	binary.BigEndian.PutUint64(pageData[offset+DICT_HDR_TABLE_ID-FIL_HEADER_SIZE:], 14)

	// Index ID（初始值：14）
	binary.BigEndian.PutUint64(pageData[offset+DICT_HDR_INDEX_ID-FIL_HEADER_SIZE:], 14)

	// Max Space ID（初始值：0，系统表空间）
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_MAX_SPACE_ID-FIL_HEADER_SIZE:], 0)

	// Mix ID Low (用于兼容)
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_MIX_ID_LOW-FIL_HEADER_SIZE:], 0)

	// 系统表的根页面指针（这些将在后续创建系统表时设置）
	// SYS_TABLES root page
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_TABLES-FIL_HEADER_SIZE:], 0)

	// SYS_TABLE_IDS root page
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_TABLE_IDS-FIL_HEADER_SIZE:], 0)

	// SYS_COLUMNS root page
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_COLUMNS-FIL_HEADER_SIZE:], 0)

	// SYS_INDEXES root page
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_INDEXES-FIL_HEADER_SIZE:], 0)

	// SYS_FIELDS root page
	binary.BigEndian.PutUint32(pageData[offset+DICT_HDR_FIELDS-FIL_HEADER_SIZE:], 0)

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	return spi.writePageToBufferPool(5, pageData)
}

// initTrxSysPage 初始化事务系统页面（Page 6）
func (spi *SystemPageInitializer) initTrxSysPage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 6, SYS_PAGE_TYPE_TRX_SYS)

	offset := FIL_HEADER_SIZE

	// Transaction ID Store（初始值：256）
	binary.BigEndian.PutUint64(pageData[offset+TRX_SYS_TRX_ID_STORE-FIL_HEADER_SIZE:], 256)

	// FSEG Header (文件段头)
	// Space ID
	binary.BigEndian.PutUint32(pageData[offset+TRX_SYS_FSEG_HEADER-FIL_HEADER_SIZE:], spi.spaceID)
	// Page No
	binary.BigEndian.PutUint32(pageData[offset+TRX_SYS_FSEG_HEADER-FIL_HEADER_SIZE+4:], 6)

	// Rollback Segment Array（128个回滚段槽位）
	rsegOffset := offset + TRX_SYS_RSEGS - FIL_HEADER_SIZE
	for i := 0; i < 128; i++ {
		// 第一个回滚段指向Page 7
		if i == 0 {
			binary.BigEndian.PutUint32(pageData[rsegOffset+i*8:], 7)
			binary.BigEndian.PutUint32(pageData[rsegOffset+i*8+4:], spi.spaceID)
		} else {
			// 其他回滚段未分配（0xFFFFFFFF表示未使用）
			binary.BigEndian.PutUint32(pageData[rsegOffset+i*8:], 0xFFFFFFFF)
			binary.BigEndian.PutUint32(pageData[rsegOffset+i*8+4:], 0xFFFFFFFF)
		}
	}

	// MySQL Log Magic Number
	binary.BigEndian.PutUint32(pageData[offset+TRX_SYS_MYSQL_LOG_MAGIC_N_FLD-FIL_HEADER_SIZE:], 873422344)

	// Doublewrite Buffer信息
	// Block 1和Block 2的起始页号（这里简化处理）
	binary.BigEndian.PutUint32(pageData[offset+TRX_SYS_DOUBLEWRITE-FIL_HEADER_SIZE:], 64)
	binary.BigEndian.PutUint32(pageData[offset+TRX_SYS_DOUBLEWRITE-FIL_HEADER_SIZE+4:], 128)

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	return spi.writePageToBufferPool(6, pageData)
}

// initFirstRsegPage 初始化第一个回滚段页面（Page 7）
func (spi *SystemPageInitializer) initFirstRsegPage() error {
	pageData := make([]byte, PAGE_SIZE)

	// 初始化File Header
	spi.initFileHeader(pageData, 7, 5) // 回滚段页面类型

	offset := FIL_HEADER_SIZE

	// Max Transaction ID
	binary.BigEndian.PutUint64(pageData[offset:], 256)

	// History List Length
	binary.BigEndian.PutUint32(pageData[offset+8:], 0)

	// History List Node
	spi.initListNode(pageData, offset+12)

	// FSEG Header
	binary.BigEndian.PutUint32(pageData[offset+24:], spi.spaceID)
	binary.BigEndian.PutUint32(pageData[offset+28:], 7)

	// Undo Slot Array（1024个undo slot）
	slotOffset := offset + 32
	for i := 0; i < 1024; i++ {
		// 未使用的slot设为0xFFFFFFFF
		binary.BigEndian.PutUint32(pageData[slotOffset+i*4:], 0xFFFFFFFF)
	}

	// 初始化File Trailer
	spi.initFileTrailer(pageData)

	return spi.writePageToBufferPool(7, pageData)
}

// initFileHeader 初始化文件头（38字节）
func (spi *SystemPageInitializer) initFileHeader(pageData []byte, pageNo uint32, pageType uint16) {
	// Checksum (0-3)
	binary.BigEndian.PutUint32(pageData[0:], 0)

	// Page Number (4-7)
	binary.BigEndian.PutUint32(pageData[4:], pageNo)

	// Previous Page (8-11)
	binary.BigEndian.PutUint32(pageData[8:], 0xFFFFFFFF)

	// Next Page (12-15)
	binary.BigEndian.PutUint32(pageData[12:], 0xFFFFFFFF)

	// LSN (16-23)
	binary.BigEndian.PutUint64(pageData[16:], 0)

	// Page Type (24-25)
	binary.BigEndian.PutUint16(pageData[24:], pageType)

	// Flush LSN (26-33) - 仅在Page 0有效
	binary.BigEndian.PutUint64(pageData[26:], 0)

	// Space ID (34-37)
	binary.BigEndian.PutUint32(pageData[34:], spi.spaceID)
}

// initFileTrailer 初始化文件尾（8字节）
func (spi *SystemPageInitializer) initFileTrailer(pageData []byte) {
	// Old-style checksum (最后8字节的前4字节)
	binary.BigEndian.PutUint32(pageData[PAGE_SIZE-8:], 0)

	// Low 32 bits of LSN (最后4字节)
	binary.BigEndian.PutUint32(pageData[PAGE_SIZE-4:], 0)
}

// initListNode 初始化链表节点（12字节）
func (spi *SystemPageInitializer) initListNode(pageData []byte, offset int) {
	// Prev Node Page Number
	binary.BigEndian.PutUint32(pageData[offset:], 0xFFFFFFFF)

	// Prev Node Offset
	binary.BigEndian.PutUint16(pageData[offset+4:], 0xFFFF)

	// Next Node Page Number
	binary.BigEndian.PutUint32(pageData[offset+6:], 0xFFFFFFFF)

	// Next Node Offset
	binary.BigEndian.PutUint16(pageData[offset+10:], 0xFFFF)
}

// writePageToBufferPool 将页面数据写入缓冲池
func (spi *SystemPageInitializer) writePageToBufferPool(pageNo uint32, pageData []byte) error {
	if spi.bufferPool == nil {
		return fmt.Errorf("buffer pool is nil")
	}

	// 获取或创建页面块
	block, err := spi.bufferPool.GetPageBlock(spi.spaceID, pageNo)
	if err != nil || block == nil {
		// 如果获取失败，可能需要创建新块（这里简化处理）
		return fmt.Errorf("failed to get page block: %v", err)
	}

	// 复制数据到块
	copy(block.GetContent(), pageData)

	// 标记为脏页
	block.SetDirty(true)

	// 更新块到缓冲池
	spi.bufferPool.UpdateBlock(spi.spaceID, pageNo, block)

	return nil
}

// GetMaxTableID 从数据字典根页面读取最大表ID
func (spi *SystemPageInitializer) GetMaxTableID() (uint64, error) {
	block, err := spi.bufferPool.GetPageBlock(spi.spaceID, SYS_DICT_ROOT_PAGE)
	if err != nil || block == nil {
		return 0, fmt.Errorf("failed to get dict root page: %v", err)
	}

	pageData := block.GetContent()
	offset := DICT_HDR_TABLE_ID

	maxTableID := binary.BigEndian.Uint64(pageData[offset : offset+8])
	return maxTableID, nil
}

// IncrementMaxTableID 递增最大表ID
func (spi *SystemPageInitializer) IncrementMaxTableID() (uint64, error) {
	block, err := spi.bufferPool.GetPageBlock(spi.spaceID, SYS_DICT_ROOT_PAGE)
	if err != nil || block == nil {
		return 0, fmt.Errorf("failed to get dict root page: %v", err)
	}

	pageData := block.GetContent()
	offset := DICT_HDR_TABLE_ID

	maxTableID := binary.BigEndian.Uint64(pageData[offset : offset+8])
	newTableID := maxTableID + 1

	binary.BigEndian.PutUint64(pageData[offset:offset+8], newTableID)

	// 标记为脏页
	block.SetDirty(true)
	spi.bufferPool.UpdateBlock(spi.spaceID, SYS_DICT_ROOT_PAGE, block)

	return newTableID, nil
}

// GetMaxTrxID 从事务系统页面读取最大事务ID
func (spi *SystemPageInitializer) GetMaxTrxID() (uint64, error) {
	block, err := spi.bufferPool.GetPageBlock(spi.spaceID, SYS_TRX_SYS_PAGE)
	if err != nil || block == nil {
		return 0, fmt.Errorf("failed to get trx sys page: %v", err)
	}

	pageData := block.GetContent()
	offset := TRX_SYS_TRX_ID_STORE

	maxTrxID := binary.BigEndian.Uint64(pageData[offset : offset+8])
	return maxTrxID, nil
}

// AllocateTrxID 分配新的事务ID
func (spi *SystemPageInitializer) AllocateTrxID() (uint64, error) {
	block, err := spi.bufferPool.GetPageBlock(spi.spaceID, SYS_TRX_SYS_PAGE)
	if err != nil || block == nil {
		return 0, fmt.Errorf("failed to get trx sys page: %v", err)
	}

	pageData := block.GetContent()
	offset := TRX_SYS_TRX_ID_STORE

	maxTrxID := binary.BigEndian.Uint64(pageData[offset : offset+8])
	newTrxID := maxTrxID + 1

	binary.BigEndian.PutUint64(pageData[offset:offset+8], newTrxID)

	// 标记为脏页
	block.SetDirty(true)
	spi.bufferPool.UpdateBlock(spi.spaceID, SYS_TRX_SYS_PAGE, block)

	return newTrxID, nil
}
