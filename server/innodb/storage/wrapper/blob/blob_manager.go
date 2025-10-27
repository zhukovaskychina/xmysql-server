package blob

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
	"sync"
)

/*
BlobManager BLOB页面管理器

核心功能：
1. BLOB页面分配
   - 从BLOB段分配页面
   - 链式页面管理
   - 页面复用机制

2. BLOB读写操作
   - 流式写入大字段
   - 部分读取支持
   - 并发访问控制

3. BLOB删除
   - 级联删除链式页面
   - 页面空间回收
   - 碎片整理

设计要点：
- 支持单页和多页BLOB
- 链式结构管理大BLOB
- 高效的空间回收
- 并发安全
*/

const (
	// BLOB分配阈值
	BlobInlineThreshold = 8000  // 小于8KB内联存储
	BlobPageThreshold   = 16318 // 单页BLOB阈值

	// BLOB页面数据大小
	BlobPageDataSize = 16318 // 每页可存储的BLOB数据大小
)

// BlobManager BLOB管理器
type BlobManager struct {
	sync.RWMutex

	// 段管理器引用
	segmentManager basic.SegmentManager

	// 空间管理器引用
	spaceManager basic.SpaceManager

	// BLOB索引：BlobID -> FirstPageNo
	blobIndex map[uint64]uint32

	// BLOB元数据
	blobMeta map[uint64]*BlobMetadata

	// 统计信息
	stats *BlobStats
}

// BlobMetadata BLOB元数据
type BlobMetadata struct {
	BlobID    uint64 // BLOB ID
	TotalSize uint32 // 总大小
	PageCount uint32 // 页面数量
	FirstPage uint32 // 第一个页面号
	LastPage  uint32 // 最后一个页面号
	SegmentID uint32 // 所属段ID
	Created   int64  // 创建时间
}

// BlobStats BLOB统计信息
type BlobStats struct {
	sync.RWMutex

	TotalBlobs      uint64 // 总BLOB数
	TotalPages      uint64 // 总页面数
	TotalSize       uint64 // 总大小（字节）
	InlineBlobs     uint64 // 内联BLOB数
	SinglePageBlobs uint64 // 单页BLOB数
	MultiPageBlobs  uint64 // 多页BLOB数

	AllocatedPages uint64 // 已分配页面数
	FreedPages     uint64 // 已释放页面数
	ReclaimedSpace uint64 // 回收的空间（字节）
}

// BlobChain BLOB页面链
type BlobChain struct {
	Pages    []*pages.BlobPage // 页面列表
	PageNos  []uint32          // 页面号列表
	TotalLen uint32            // 总长度
}

// NewBlobManager 创建BLOB管理器
func NewBlobManager(segMgr basic.SegmentManager, spaceMgr basic.SpaceManager) *BlobManager {
	return &BlobManager{
		segmentManager: segMgr,
		spaceManager:   spaceMgr,
		blobIndex:      make(map[uint64]uint32),
		blobMeta:       make(map[uint64]*BlobMetadata),
		stats:          &BlobStats{},
	}
}

// AllocateBlob 分配BLOB存储
func (bm *BlobManager) AllocateBlob(segmentID uint32, data []byte) (uint64, uint32, error) {
	bm.Lock()
	defer bm.Unlock()

	dataSize := uint32(len(data))

	// 检查是否需要BLOB存储
	if dataSize < BlobInlineThreshold {
		// 小于阈值，建议内联存储（返回特殊标识）
		bm.stats.InlineBlobs++
		return 0, 0, fmt.Errorf("data size %d below threshold, use inline storage", dataSize)
	}

	// 生成BLOB ID
	blobID := bm.generateBlobID()

	// 计算需要的页面数
	pageCount := (dataSize + BlobPageDataSize - 1) / BlobPageDataSize

	// 分配页面链
	chain, err := bm.allocateBlobChain(segmentID, dataSize, pageCount)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate blob chain: %v", err)
	}

	// 写入数据到页面链
	if err := bm.writeBlobData(chain, data); err != nil {
		// 回滚：释放已分配的页面
		bm.freeBlobChain(chain)
		return 0, 0, fmt.Errorf("failed to write blob data: %v", err)
	}

	// 保存元数据
	meta := &BlobMetadata{
		BlobID:    blobID,
		TotalSize: dataSize,
		PageCount: pageCount,
		FirstPage: chain.PageNos[0],
		LastPage:  chain.PageNos[len(chain.PageNos)-1],
		SegmentID: segmentID,
		Created:   getCurrentTimestamp(),
	}

	bm.blobIndex[blobID] = chain.PageNos[0]
	bm.blobMeta[blobID] = meta

	// 更新统计
	bm.stats.TotalBlobs++
	bm.stats.TotalPages += uint64(pageCount)
	bm.stats.TotalSize += uint64(dataSize)
	bm.stats.AllocatedPages += uint64(pageCount)

	if pageCount == 1 {
		bm.stats.SinglePageBlobs++
	} else {
		bm.stats.MultiPageBlobs++
	}

	return blobID, chain.PageNos[0], nil
}

// ReadBlob 读取完整BLOB数据
func (bm *BlobManager) ReadBlob(blobID uint64) ([]byte, error) {
	bm.RLock()
	defer bm.RUnlock()

	// 获取元数据
	meta, exists := bm.blobMeta[blobID]
	if !exists {
		return nil, fmt.Errorf("blob %d not found", blobID)
	}

	// 读取页面链
	chain, err := bm.readBlobChain(meta.FirstPage, meta.TotalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob chain: %v", err)
	}

	// 提取数据
	data := make([]byte, 0, meta.TotalSize)
	for _, page := range chain.Pages {
		data = append(data, page.GetBlobData()...)
	}

	return data[:meta.TotalSize], nil
}

// ReadBlobPartial 部分读取BLOB数据
func (bm *BlobManager) ReadBlobPartial(blobID uint64, offset, length uint32) ([]byte, error) {
	bm.RLock()
	defer bm.RUnlock()

	// 获取元数据
	meta, exists := bm.blobMeta[blobID]
	if !exists {
		return nil, fmt.Errorf("blob %d not found", blobID)
	}

	// 验证范围
	if offset >= meta.TotalSize {
		return nil, fmt.Errorf("offset %d exceeds blob size %d", offset, meta.TotalSize)
	}

	// 调整长度
	if offset+length > meta.TotalSize {
		length = meta.TotalSize - offset
	}

	// 计算起始和结束页面
	startPageIdx := offset / BlobPageDataSize
	endPageIdx := (offset + length - 1) / BlobPageDataSize

	// 读取需要的页面
	data := make([]byte, 0, length)
	currentPage := meta.FirstPage

	for i := uint32(0); i <= endPageIdx && currentPage != 0; i++ {
		page, err := bm.readBlobPage(currentPage)
		if err != nil {
			return nil, err
		}

		if i >= startPageIdx {
			pageData := page.GetBlobData()

			// 计算当前页面的读取范围
			pageOffset := uint32(0)
			pageLength := uint32(len(pageData))

			if i == startPageIdx {
				pageOffset = offset % BlobPageDataSize
			}

			if i == endPageIdx {
				pageLength = ((offset + length - 1) % BlobPageDataSize) + 1 - pageOffset
			} else {
				pageLength = pageLength - pageOffset
			}

			data = append(data, pageData[pageOffset:pageOffset+pageLength]...)
		}

		currentPage = page.GetNextPageNo()
	}

	return data, nil
}

// DeleteBlob 删除BLOB（级联删除所有页面）
func (bm *BlobManager) DeleteBlob(blobID uint64) error {
	bm.Lock()
	defer bm.Unlock()

	// 获取元数据
	meta, exists := bm.blobMeta[blobID]
	if !exists {
		return fmt.Errorf("blob %d not found", blobID)
	}

	// 读取页面链以获取所有页面号
	chain, err := bm.readBlobChain(meta.FirstPage, meta.TotalSize)
	if err != nil {
		return fmt.Errorf("failed to read blob chain for deletion: %v", err)
	}

	// 释放所有页面
	if err := bm.freeBlobChain(chain); err != nil {
		return fmt.Errorf("failed to free blob chain: %v", err)
	}

	// 删除元数据
	delete(bm.blobIndex, blobID)
	delete(bm.blobMeta, blobID)

	// 更新统计
	bm.stats.TotalBlobs--
	bm.stats.TotalPages -= uint64(meta.PageCount)
	bm.stats.TotalSize -= uint64(meta.TotalSize)
	bm.stats.FreedPages += uint64(meta.PageCount)
	bm.stats.ReclaimedSpace += uint64(meta.TotalSize)

	return nil
}

// allocateBlobChain 分配BLOB页面链
func (bm *BlobManager) allocateBlobChain(segmentID uint32, totalSize, pageCount uint32) (*BlobChain, error) {
	chain := &BlobChain{
		Pages:    make([]*pages.BlobPage, 0, pageCount),
		PageNos:  make([]uint32, 0, pageCount),
		TotalLen: totalSize,
	}

	for i := uint32(0); i < pageCount; i++ {
		// 从段分配页面
		pageNo, err := bm.segmentManager.AllocatePage(segmentID)
		if err != nil {
			// 分配失败，回滚已分配的页面
			bm.freeBlobChain(chain)
			return nil, fmt.Errorf("failed to allocate page %d: %v", i, err)
		}

		// 创建BLOB页面
		page := pages.NewBlobPage(0, pageNo, uint64(segmentID))

		// 设置链接
		if i < pageCount-1 {
			// 不是最后一页，设置下一页指针（将在实际分配下一页后更新）
			page.BlobHeader.NextPage = 0 // 临时设为0
		} else {
			// 最后一页
			page.BlobHeader.NextPage = 0
		}

		chain.Pages = append(chain.Pages, page)
		chain.PageNos = append(chain.PageNos, pageNo)
	}

	// 更新页面链接
	for i := 0; i < len(chain.Pages)-1; i++ {
		chain.Pages[i].BlobHeader.NextPage = chain.PageNos[i+1]
	}

	return chain, nil
}

// writeBlobData 写入BLOB数据到页面链
func (bm *BlobManager) writeBlobData(chain *BlobChain, data []byte) error {
	offset := uint32(0)

	for i, page := range chain.Pages {
		// 计算当前页面要写入的数据量
		remaining := chain.TotalLen - offset
		pageDataSize := BlobPageDataSize
		if remaining < BlobPageDataSize {
			pageDataSize = int(remaining)
		}

		// 设置BLOB数据
		pageData := data[offset : offset+uint32(pageDataSize)]
		err := page.SetBlobData(pageData, chain.TotalLen, offset, page.BlobHeader.NextPage)
		if err != nil {
			return fmt.Errorf("failed to set blob data on page %d: %v", i, err)
		}

		// 持久化页面（这里需要与存储层集成）
		// TODO: 调用spaceManager.FlushPage()

		offset += uint32(pageDataSize)
	}

	return nil
}

// readBlobChain 读取BLOB页面链
func (bm *BlobManager) readBlobChain(firstPageNo, totalSize uint32) (*BlobChain, error) {
	chain := &BlobChain{
		Pages:    make([]*pages.BlobPage, 0),
		PageNos:  make([]uint32, 0),
		TotalLen: totalSize,
	}

	currentPageNo := firstPageNo

	for currentPageNo != 0 {
		page, err := bm.readBlobPage(currentPageNo)
		if err != nil {
			return nil, fmt.Errorf("failed to read blob page %d: %v", currentPageNo, err)
		}

		chain.Pages = append(chain.Pages, page)
		chain.PageNos = append(chain.PageNos, currentPageNo)

		currentPageNo = page.GetNextPageNo()

		// 防止无限循环
		if len(chain.Pages) > 10000 {
			return nil, fmt.Errorf("blob chain too long, possible corruption")
		}
	}

	return chain, nil
}

// readBlobPage 读取单个BLOB页面
func (bm *BlobManager) readBlobPage(pageNo uint32) (*pages.BlobPage, error) {
	// TODO: 从spaceManager读取页面数据
	// 这里简化实现
	page := &pages.BlobPage{}
	return page, nil
}

// freeBlobChain 释放BLOB页面链
func (bm *BlobManager) freeBlobChain(chain *BlobChain) error {
	for i, pageNo := range chain.PageNos {
		// 从段释放页面
		page := chain.Pages[i]
		err := bm.segmentManager.FreePage(uint32(page.BlobHeader.SegmentID), pageNo)
		if err != nil {
			// 记录错误但继续释放其他页面
			fmt.Printf("Warning: failed to free blob page %d: %v\n", pageNo, err)
		}
	}
	return nil
}

// generateBlobID 生成BLOB ID
func (bm *BlobManager) generateBlobID() uint64 {
	// 简化实现：使用当前BLOB数+1
	// 实际应该使用更健壮的ID生成策略
	return bm.stats.TotalBlobs + 1
}

// getCurrentTimestamp 获取当前时间戳
func getCurrentTimestamp() int64 {
	// 简化实现
	return 0
}

// GetStats 获取统计信息
func (bm *BlobManager) GetStats() *BlobStats {
	bm.stats.RLock()
	defer bm.stats.RUnlock()

	statsCopy := *bm.stats
	return &statsCopy
}

// GetBlobMetadata 获取BLOB元数据
func (bm *BlobManager) GetBlobMetadata(blobID uint64) (*BlobMetadata, error) {
	bm.RLock()
	defer bm.RUnlock()

	meta, exists := bm.blobMeta[blobID]
	if !exists {
		return nil, fmt.Errorf("blob %d not found", blobID)
	}

	// 返回副本
	metaCopy := *meta
	return &metaCopy, nil
}
