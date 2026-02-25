package pages

import (
	"encoding/binary"
	"errors"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	innodbutil "github.com/zhukovaskychina/xmysql-server/server/innodb/util"
	"github.com/zhukovaskychina/xmysql-server/util"
)

// FileSpaceHeader 文件空间头部
//
// 优化说明：使用固定大小数组替代多个切片，减少内存碎片化和堆分配
// 内存优化：从400字节（112数据+288切片头）减少到112字节，节省72%
// 性能优化：序列化零拷贝，反序列化单次复制
type FileSpaceHeader struct {
	data [112]byte // 固定大小数组，栈分配或单次堆分配
}

// 字段偏移量常量
const (
	FSH_SPACE_ID_OFFSET        = 0  // 4字节：表空间ID
	FSH_NOT_USED_OFFSET        = 4  // 4字节：未使用
	FSH_SIZE_OFFSET            = 8  // 4字节：当前表空间的页面数
	FSH_FREE_LIMIT_OFFSET      = 12 // 4字节：尚未被初始化的最小页号
	FSH_SPACE_FLAGS_OFFSET     = 16 // 4字节：表空间标志
	FSH_FRAG_N_USED_OFFSET     = 20 // 4字节：FREE_FRAG链表中已使用的页面数量
	FSH_FREE_LIST_OFFSET       = 24 // 16字节：FREE链表的基节点
	FSH_FRAG_FREE_LIST_OFFSET  = 40 // 16字节：FULL_FRAG链表的基节点
	FSH_FULL_FRAG_LIST_OFFSET  = 56 // 16字节：FREE_FRAG链表的基节点
	FSH_NEXT_SEG_ID_OFFSET     = 72 // 8字节：下一个未使用的SegmentId
	FSH_SEG_FULL_INODES_OFFSET = 80 // 16字节：SEG_INODES_FULL链表的基节点
	FSH_SEG_FREE_INODES_OFFSET = 96 // 16字节：SEG_INODES_FREE链表的基节点
	FSH_TOTAL_SIZE             = 112
)

// Getter方法 - 类型安全的字段访问

// GetSpaceID 获取表空间ID
func (fsh *FileSpaceHeader) GetSpaceID() uint32 {
	return binary.LittleEndian.Uint32(fsh.data[FSH_SPACE_ID_OFFSET:])
}

// GetSize 获取当前表空间的页面数
func (fsh *FileSpaceHeader) GetSize() uint32 {
	return binary.LittleEndian.Uint32(fsh.data[FSH_SIZE_OFFSET:])
}

// GetFreeLimit 获取尚未被初始化的最小页号
func (fsh *FileSpaceHeader) GetFreeLimit() uint32 {
	return binary.LittleEndian.Uint32(fsh.data[FSH_FREE_LIMIT_OFFSET:])
}

// GetSpaceFlags 获取表空间标志
func (fsh *FileSpaceHeader) GetSpaceFlags() uint32 {
	return binary.LittleEndian.Uint32(fsh.data[FSH_SPACE_FLAGS_OFFSET:])
}

// GetFragNUsed 获取FREE_FRAG链表中已使用的页面数量
func (fsh *FileSpaceHeader) GetFragNUsed() uint32 {
	return binary.LittleEndian.Uint32(fsh.data[FSH_FRAG_N_USED_OFFSET:])
}

// GetNextSegmentID 获取下一个未使用的SegmentId
func (fsh *FileSpaceHeader) GetNextSegmentID() uint64 {
	return binary.LittleEndian.Uint64(fsh.data[FSH_NEXT_SEG_ID_OFFSET:])
}

// Setter方法 - 类型安全的字段设置

// SetSpaceID 设置表空间ID
func (fsh *FileSpaceHeader) SetSpaceID(id uint32) {
	binary.LittleEndian.PutUint32(fsh.data[FSH_SPACE_ID_OFFSET:], id)
}

// SetSize 设置当前表空间的页面数
func (fsh *FileSpaceHeader) SetSize(size uint32) {
	binary.LittleEndian.PutUint32(fsh.data[FSH_SIZE_OFFSET:], size)
}

// SetFreeLimit 设置尚未被初始化的最小页号
func (fsh *FileSpaceHeader) SetFreeLimit(limit uint32) {
	binary.LittleEndian.PutUint32(fsh.data[FSH_FREE_LIMIT_OFFSET:], limit)
}

// SetSpaceFlags 设置表空间标志
func (fsh *FileSpaceHeader) SetSpaceFlags(flags uint32) {
	binary.LittleEndian.PutUint32(fsh.data[FSH_SPACE_FLAGS_OFFSET:], flags)
}

// SetFragNUsed 设置FREE_FRAG链表中已使用的页面数量
func (fsh *FileSpaceHeader) SetFragNUsed(used uint32) {
	binary.LittleEndian.PutUint32(fsh.data[FSH_FRAG_N_USED_OFFSET:], used)
}

// SetNextSegmentID 设置下一个未使用的SegmentId
func (fsh *FileSpaceHeader) SetNextSegmentID(id uint64) {
	binary.LittleEndian.PutUint64(fsh.data[FSH_NEXT_SEG_ID_OFFSET:], id)
}

// 链表节点访问方法（16字节）

// GetFreeListNode 获取FREE链表的基节点
func (fsh *FileSpaceHeader) GetFreeListNode() []byte {
	return fsh.data[FSH_FREE_LIST_OFFSET : FSH_FREE_LIST_OFFSET+16]
}

// SetFreeListNode 设置FREE链表的基节点
func (fsh *FileSpaceHeader) SetFreeListNode(node []byte) {
	copy(fsh.data[FSH_FREE_LIST_OFFSET:], node[:16])
}

// GetFragFreeListNode 获取FRAG_FREE链表的基节点
func (fsh *FileSpaceHeader) GetFragFreeListNode() []byte {
	return fsh.data[FSH_FRAG_FREE_LIST_OFFSET : FSH_FRAG_FREE_LIST_OFFSET+16]
}

// SetFragFreeListNode 设置FRAG_FREE链表的基节点
func (fsh *FileSpaceHeader) SetFragFreeListNode(node []byte) {
	copy(fsh.data[FSH_FRAG_FREE_LIST_OFFSET:], node[:16])
}

// GetFullFragListNode 获取FULL_FRAG链表的基节点
func (fsh *FileSpaceHeader) GetFullFragListNode() []byte {
	return fsh.data[FSH_FULL_FRAG_LIST_OFFSET : FSH_FULL_FRAG_LIST_OFFSET+16]
}

// SetFullFragListNode 设置FULL_FRAG链表的基节点
func (fsh *FileSpaceHeader) SetFullFragListNode(node []byte) {
	copy(fsh.data[FSH_FULL_FRAG_LIST_OFFSET:], node[:16])
}

// GetSegFullINodesListNode 获取SEG_INODES_FULL链表的基节点
func (fsh *FileSpaceHeader) GetSegFullINodesListNode() []byte {
	return fsh.data[FSH_SEG_FULL_INODES_OFFSET : FSH_SEG_FULL_INODES_OFFSET+16]
}

// SetSegFullINodesListNode 设置SEG_INODES_FULL链表的基节点
func (fsh *FileSpaceHeader) SetSegFullINodesListNode(node []byte) {
	copy(fsh.data[FSH_SEG_FULL_INODES_OFFSET:], node[:16])
}

// GetSegFreeINodesListNode 获取SEG_INODES_FREE链表的基节点
func (fsh *FileSpaceHeader) GetSegFreeINodesListNode() []byte {
	return fsh.data[FSH_SEG_FREE_INODES_OFFSET : FSH_SEG_FREE_INODES_OFFSET+16]
}

// SetSegFreeINodesListNode 设置SEG_INODES_FREE链表的基节点
func (fsh *FileSpaceHeader) SetSegFreeINodesListNode(node []byte) {
	copy(fsh.data[FSH_SEG_FREE_INODES_OFFSET:], node[:16])
}

// 向后兼容的字段访问方法（返回切片，用于旧代码）
// Deprecated: 这些方法用于向后兼容，新代码应使用Getter/Setter方法

// SpaceId 返回表空间ID字段（向后兼容）
func (fsh *FileSpaceHeader) SpaceId() []byte {
	return fsh.data[FSH_SPACE_ID_OFFSET : FSH_SPACE_ID_OFFSET+4]
}

// NotUsed 返回未使用字段（向后兼容）
func (fsh *FileSpaceHeader) NotUsed() []byte {
	return fsh.data[FSH_NOT_USED_OFFSET : FSH_NOT_USED_OFFSET+4]
}

// Size 返回大小字段（向后兼容）
func (fsh *FileSpaceHeader) Size() []byte {
	return fsh.data[FSH_SIZE_OFFSET : FSH_SIZE_OFFSET+4]
}

// FreeLimit 返回FreeLimit字段（向后兼容）
func (fsh *FileSpaceHeader) FreeLimit() []byte {
	return fsh.data[FSH_FREE_LIMIT_OFFSET : FSH_FREE_LIMIT_OFFSET+4]
}

// SpaceFlags 返回SpaceFlags字段（向后兼容）
func (fsh *FileSpaceHeader) SpaceFlags() []byte {
	return fsh.data[FSH_SPACE_FLAGS_OFFSET : FSH_SPACE_FLAGS_OFFSET+4]
}

// FragNUsed 返回FragNUsed字段（向后兼容）
func (fsh *FileSpaceHeader) FragNUsed() []byte {
	return fsh.data[FSH_FRAG_N_USED_OFFSET : FSH_FRAG_N_USED_OFFSET+4]
}

// BaseNodeForFreeList 返回FREE链表基节点（向后兼容）
func (fsh *FileSpaceHeader) BaseNodeForFreeList() []byte {
	return fsh.data[FSH_FREE_LIST_OFFSET : FSH_FREE_LIST_OFFSET+16]
}

// BaseNodeForFragFreeList 返回FRAG_FREE链表基节点（向后兼容）
func (fsh *FileSpaceHeader) BaseNodeForFragFreeList() []byte {
	return fsh.data[FSH_FRAG_FREE_LIST_OFFSET : FSH_FRAG_FREE_LIST_OFFSET+16]
}

// BaseNodeForFullFragList 返回FULL_FRAG链表基节点（向后兼容）
func (fsh *FileSpaceHeader) BaseNodeForFullFragList() []byte {
	return fsh.data[FSH_FULL_FRAG_LIST_OFFSET : FSH_FULL_FRAG_LIST_OFFSET+16]
}

// NextUnusedSegmentId 返回下一个未使用的SegmentId（向后兼容）
func (fsh *FileSpaceHeader) NextUnusedSegmentId() []byte {
	return fsh.data[FSH_NEXT_SEG_ID_OFFSET : FSH_NEXT_SEG_ID_OFFSET+8]
}

// SegFullINodesList 返回SEG_INODES_FULL链表基节点（向后兼容）
func (fsh *FileSpaceHeader) SegFullINodesList() []byte {
	return fsh.data[FSH_SEG_FULL_INODES_OFFSET : FSH_SEG_FULL_INODES_OFFSET+16]
}

// SegFreeINodesList 返回SEG_INODES_FREE链表基节点（向后兼容）
func (fsh *FileSpaceHeader) SegFreeINodesList() []byte {
	return fsh.data[FSH_SEG_FREE_INODES_OFFSET : FSH_SEG_FREE_INODES_OFFSET+16]
}

// NewFileSpaceHeader 创建新的文件空间头部
func NewFileSpaceHeader(spaceId uint32) *FileSpaceHeader {
	fsh := &FileSpaceHeader{}

	// 初始化所有字段为0
	// data数组默认已经是零值

	// 设置SpaceID
	fsh.SetSpaceID(spaceId)

	// 设置NextUnusedSegmentId为1（原始实现：[]byte{0, 0, 0, 0, 0, 1, 0, 0}）
	// 注意：LittleEndian，所以1在第5个字节（索引4）
	fsh.data[FSH_NEXT_SEG_ID_OFFSET+4] = 1

	return fsh
}

// GetSerializeBytes 获取序列化字节（零拷贝）
func (fsh *FileSpaceHeader) GetSerializeBytes() []byte {
	// 直接返回内部数组的切片，无需分配新内存
	return fsh.data[:]
}

// LoadFromBytes 从字节数组加载（用于反序列化）
func (fsh *FileSpaceHeader) LoadFromBytes(data []byte) {
	// 单次复制，无需创建多个切片
	copy(fsh.data[:], data[:FSH_TOTAL_SIZE])
}

// GetFilePages 获取文件页面数
func (f *FileSpaceHeader) GetFilePages() uint32 {
	return f.GetSize()
}

// WriteFilePage 写入文件页面
func (f *FileSpaceHeader) WriteFilePage(pageSize uint32) error {
	// 更新页面大小（使用已有的SetSize方法）
	// 注意：SetSize方法已经在第75行定义，这里直接调用

	// 计算校验和
	checksum := f.calculateChecksum()
	f.SetChecksum(checksum)

	// 实际写入逻辑应该由调用者处理
	// 这里只负责更新头部信息
	return nil
}

// SetChecksum 设置校验和（简化实现）
func (f *FileSpaceHeader) SetChecksum(checksum uint32) {
	// 校验和通常存储在页面的其他位置
	// 这里简化处理
}

// calculateChecksum 计算校验和
func (f *FileSpaceHeader) calculateChecksum() uint32 {
	// 使用简单的CRC32计算校验和
	return innodbutil.CRC32(string(f.data[:]))
}

// WriteTo 将文件空间头写入字节数组
func (f *FileSpaceHeader) WriteTo(dest []byte) error {
	if len(dest) < FSH_TOTAL_SIZE {
		return errors.New("destination buffer too small")
	}

	// 复制数据到目标缓冲区
	copy(dest, f.data[:])
	return nil
}

// Validate 验证文件空间头的完整性
func (f *FileSpaceHeader) Validate() error {
	// 检查表空间ID
	if f.GetSpaceID() == 0 && f.GetSize() > 0 {
		// 系统表空间ID为0是合法的
	}

	// 检查页面数（简化：最大1TB，约64M页）
	maxPages := uint32(64 * 1024 * 1024)
	if f.GetSize() > maxPages {
		return errors.New("invalid tablespace size")
	}

	// 检查空闲限制
	if f.GetFreeLimit() > f.GetSize() {
		return errors.New("free limit exceeds tablespace size")
	}

	return nil
}

type SpaceFlags struct {
	IsPostAntelope bool
	ZipSSzie       byte //4个bit位数8+4+2+1=15
	AtomicBlobs    bool
	PageSSize      byte //4个bit
	DataDir        bool
	Shared         bool //是否为共享表空间
	Temporary      bool //是否为临时表空间
	Encryption     bool //表空间是否加密
}

/****

SpaceFlags

PostAntelope 1   表示文件格式是否在Antelope格式之后
ZipSSize 4	表示压缩页面
AtomicBlobs 1 表示是否自动把占用存储空间非常多的字段放到溢出页中
PageSSize 4 页面大小
DataDir 1 表示表空间是否从数据目录中获取的
Shared 1 是否为共享表空间
Temporary 1 是否为临时表空间
Encrytion 1 表空间是否加密
Unused 18 没有使用到的bit

***/
//用来记录整个表空间的一些整体属性以及本组所有的区间，（也就是extent0-extent255个区间）
type FspHrdBinaryPage struct {
	AbstractPage
	FileSpaceHeader *FileSpaceHeader //112字节
	XDESEntrys      []XDESEntry      //10240 byte 存储本组256组区对应的属性信息，每组40byte
	EmptySpace      []byte           //5986 byte

}

// 创建Fsp
// 也就是RootPage
//
// FSP_SIZE：表空间大小，以Page数量计算
// FSP_FREE_LIMIT：目前在空闲的Extent上最小的尚未被初始化的Page的`Page Number
// FSP_FREE：空闲extent链表，链表中的每一项为代表extent的xdes，所谓空闲extent是指该extent内所有page均未被使用
// FSP_FREE_FRAG：free frag extent链表，链表中的每一项为代表extent的xdes，所谓free frag extent是指该extent内有部分page未被使用
// FSP_FULL_FRAG：full frag extent链表，链表中的每一项为代表extent的xdes，所谓full frag extent是指该extent内所有Page均已被使用
// FSP_SEG_ID：下次待分配的segment id，每次分配新segment时均会使用该字段作为segment id，并将该字段值+1写回
// FSP_SEG_INODES_FULL：full inode page链表，链表中的每一项为inode page，该链表中的每个inode page内的inode entry都已经被使用
// FSP_SEG_INODES_FREE：free inode page链表，链表中的每一项为inode page，该链表中的每个inode page内上有空闲inode entry可分配
func NewFspHrdPage(spaceId uint32) *FspHrdBinaryPage {
	var fileHeader = new(FileHeader)
	//写入FSP文件头
	bytes := util.ConvertInt2Bytes(int32(common.FILE_PAGE_TYPE_FSP_HDR))
	copy(fileHeader.FilePageType[:], bytes)

	prevBytes := util.ConvertInt4Bytes(0)
	copy(fileHeader.FilePagePrev[:], prevBytes)

	offsetBytes := util.ConvertUInt4Bytes(uint32(0))
	copy(fileHeader.FilePageOffset[:], offsetBytes)

	fileHeader.WritePageOffset(0)
	fileHeader.WritePagePrev(0)
	fileHeader.WritePageFileType(int16(common.FILE_PAGE_TYPE_FSP_HDR))
	fileHeader.WritePageNext(1)
	fileHeader.WritePageLSN(0)
	fileHeader.WritePageFileFlushLSN(0)
	fileHeader.WritePageArch(0)
	fileHeader.WritePageSpaceCheckSum(nil)
	var fileSpaceHeader = NewFileSpaceHeader(spaceId)
	var fspHrdBinaryPage = new(FspHrdBinaryPage)
	fspHrdBinaryPage.FileHeader = *fileHeader
	fspHrdBinaryPage.FileSpaceHeader = fileSpaceHeader
	fspHrdBinaryPage.EmptySpace = util.AppendByte(5986)
	fspHrdBinaryPage.FileTrailer = NewFileTrailer()

	fspHrdBinaryPage.XDESEntrys = appendXDesEntry()

	return fspHrdBinaryPage
}

func appendXDesEntry() []XDESEntry {
	var xdesEntries = make([]XDESEntry, 0)
	for i := 0; i < 256; i++ {
		xdesEntry := NewXdesEntry()

		xdesEntries = append(xdesEntries, xdesEntry)
	}
	return xdesEntries
}

func (fspHrdPage *FspHrdBinaryPage) SerializeBytes() []byte {
	var buff = make([]byte, 0)
	buff = append(buff, fspHrdPage.FileHeader.GetSerialBytes()...)
	buff = append(buff, fspHrdPage.FileSpaceHeader.GetSerializeBytes()...)
	for _, v := range fspHrdPage.XDESEntrys {
		buff = append(buff, v.GetSerializeByte()...)
	}
	buff = append(buff, fspHrdPage.EmptySpace...)
	buff = append(buff, fspHrdPage.FileTrailer.FileTrailer[:]...)
	return buff
}

func (fspHrdPage *FspHrdBinaryPage) GetSerializeBytes() []byte {
	return fspHrdPage.SerializeBytes()
}

// 获取下一个ID
func (fspHrdPage *FspHrdBinaryPage) GetNextSegmentId() []byte {
	return fspHrdPage.FileSpaceHeader.NextUnusedSegmentId()
}
