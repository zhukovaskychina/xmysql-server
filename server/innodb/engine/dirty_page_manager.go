package engine

import (
	"sort"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// DirtyPageManager 脏页管理器
// 负责跟踪和管理缓冲池中的脏页
type DirtyPageManager struct {
	mu sync.RWMutex

	// 脏页列表（按页面ID索引）
	dirtyPages map[uint64]*DirtyPageEntry

	// 按LSN排序的脏页列表（用于快速选择最老的脏页）
	lsnOrderedPages []*DirtyPageEntry

	// 按修改时间排序的脏页列表
	timeOrderedPages []*DirtyPageEntry

	// 统计信息
	stats *DirtyPageStats

	// 配置
	config *DirtyPageConfig
}

// DirtyPageEntry 脏页条目
type DirtyPageEntry struct {
	PageID      uint64                  // 页面ID (spaceID << 32 | pageNo)
	SpaceID     uint32                  // 表空间ID
	PageNo      uint32                  // 页面号
	Page        *buffer_pool.BufferPage // 页面引用
	FirstModLSN uint64                  // 首次修改LSN
	LastModLSN  uint64                  // 最后修改LSN
	ModifyCount uint32                  // 修改次数
	ModifyTime  time.Time               // 最后修改时间
	FlushTime   time.Time               // 最后刷新时间
	Priority    int                     // 刷新优先级
}

// DirtyPageStats 脏页统计信息
type DirtyPageStats struct {
	TotalDirtyPages uint64    // 总脏页数
	TotalFlushes    uint64    // 总刷新次数
	TotalModifies   uint64    // 总修改次数
	AvgModifyCount  float64   // 平均修改次数
	OldestLSN       uint64    // 最老的LSN
	NewestLSN       uint64    // 最新的LSN
	LastFlushTime   time.Time // 最后刷新时间
	LastFlushCount  int       // 最后刷新数量
}

// DirtyPageConfig 脏页配置
type DirtyPageConfig struct {
	MaxDirtyPages  int           // 最大脏页数
	MaxDirtyRatio  float64       // 最大脏页比例
	FlushBatchSize int           // 批量刷新大小
	FlushInterval  time.Duration // 刷新间隔
	EnableAdaptive bool          // 启用自适应刷新
}

// NewDirtyPageManager 创建脏页管理器
func NewDirtyPageManager(config *DirtyPageConfig) *DirtyPageManager {
	if config == nil {
		config = &DirtyPageConfig{
			MaxDirtyPages:  10000,
			MaxDirtyRatio:  0.75,
			FlushBatchSize: 100,
			FlushInterval:  1 * time.Second,
			EnableAdaptive: true,
		}
	}

	return &DirtyPageManager{
		dirtyPages:       make(map[uint64]*DirtyPageEntry),
		lsnOrderedPages:  make([]*DirtyPageEntry, 0),
		timeOrderedPages: make([]*DirtyPageEntry, 0),
		stats:            &DirtyPageStats{},
		config:           config,
	}
}

// AddDirtyPage 添加脏页
func (dpm *DirtyPageManager) AddDirtyPage(page *buffer_pool.BufferPage, lsn uint64) {
	dpm.mu.Lock()
	defer dpm.mu.Unlock()

	pageID := makePageID(page.GetSpaceID(), page.GetPageNo())

	entry, exists := dpm.dirtyPages[pageID]
	if !exists {
		// 新脏页
		entry = &DirtyPageEntry{
			PageID:      pageID,
			SpaceID:     page.GetSpaceID(),
			PageNo:      page.GetPageNo(),
			Page:        page,
			FirstModLSN: lsn,
			LastModLSN:  lsn,
			ModifyCount: 1,
			ModifyTime:  time.Now(),
			Priority:    0,
		}
		dpm.dirtyPages[pageID] = entry
		dpm.stats.TotalDirtyPages++

		logger.Debugf("📝 添加脏页: [%d:%d] LSN=%d", entry.SpaceID, entry.PageNo, lsn)
	} else {
		// 更新已有脏页
		entry.LastModLSN = lsn
		entry.ModifyCount++
		entry.ModifyTime = time.Now()
		entry.Page = page
	}

	dpm.stats.TotalModifies++

	// 更新LSN范围
	if dpm.stats.OldestLSN == 0 || lsn < dpm.stats.OldestLSN {
		dpm.stats.OldestLSN = lsn
	}
	if lsn > dpm.stats.NewestLSN {
		dpm.stats.NewestLSN = lsn
	}

	// 标记需要重新排序
	dpm.lsnOrderedPages = nil
	dpm.timeOrderedPages = nil
}

// RemoveDirtyPage 移除脏页（页面已刷新）
func (dpm *DirtyPageManager) RemoveDirtyPage(spaceID, pageNo uint32) {
	dpm.mu.Lock()
	defer dpm.mu.Unlock()

	pageID := makePageID(spaceID, pageNo)

	if entry, exists := dpm.dirtyPages[pageID]; exists {
		delete(dpm.dirtyPages, pageID)
		dpm.stats.TotalDirtyPages--
		dpm.stats.TotalFlushes++

		entry.FlushTime = time.Now()

		logger.Debugf("💧 移除脏页: [%d:%d] LSN=%d, 修改次数=%d",
			entry.SpaceID, entry.PageNo, entry.LastModLSN, entry.ModifyCount)

		// 标记需要重新排序
		dpm.lsnOrderedPages = nil
		dpm.timeOrderedPages = nil
	}
}

// GetDirtyPages 获取所有脏页
func (dpm *DirtyPageManager) GetDirtyPages() []*buffer_pool.BufferPage {
	dpm.mu.RLock()
	defer dpm.mu.RUnlock()

	pages := make([]*buffer_pool.BufferPage, 0, len(dpm.dirtyPages))
	for _, entry := range dpm.dirtyPages {
		if entry.Page != nil {
			pages = append(pages, entry.Page)
		}
	}

	return pages
}

// GetDirtyPageCount 获取脏页数量
func (dpm *DirtyPageManager) GetDirtyPageCount() int {
	dpm.mu.RLock()
	defer dpm.mu.RUnlock()

	return len(dpm.dirtyPages)
}

// GetDirtyPagesByLSN 按LSN排序获取脏页（LSN越小越靠前）
func (dpm *DirtyPageManager) GetDirtyPagesByLSN() []*DirtyPageEntry {
	dpm.mu.Lock()
	defer dpm.mu.Unlock()

	// 如果已排序，直接返回
	if dpm.lsnOrderedPages != nil {
		return dpm.lsnOrderedPages
	}

	// 重新排序
	pages := make([]*DirtyPageEntry, 0, len(dpm.dirtyPages))
	for _, entry := range dpm.dirtyPages {
		pages = append(pages, entry)
	}

	sort.Slice(pages, func(i, j int) bool {
		return pages[i].FirstModLSN < pages[j].FirstModLSN
	})

	dpm.lsnOrderedPages = pages
	return pages
}

// GetDirtyPagesByTime 按修改时间排序获取脏页（时间越早越靠前）
func (dpm *DirtyPageManager) GetDirtyPagesByTime() []*DirtyPageEntry {
	dpm.mu.Lock()
	defer dpm.mu.Unlock()

	// 如果已排序，直接返回
	if dpm.timeOrderedPages != nil {
		return dpm.timeOrderedPages
	}

	// 重新排序
	pages := make([]*DirtyPageEntry, 0, len(dpm.dirtyPages))
	for _, entry := range dpm.dirtyPages {
		pages = append(pages, entry)
	}

	sort.Slice(pages, func(i, j int) bool {
		return pages[i].ModifyTime.Before(pages[j].ModifyTime)
	})

	dpm.timeOrderedPages = pages
	return pages
}

// GetStats 获取统计信息
func (dpm *DirtyPageManager) GetStats() *DirtyPageStats {
	dpm.mu.RLock()
	defer dpm.mu.RUnlock()

	stats := *dpm.stats

	// 计算平均修改次数
	if stats.TotalDirtyPages > 0 {
		totalModifyCount := uint64(0)
		for _, entry := range dpm.dirtyPages {
			totalModifyCount += uint64(entry.ModifyCount)
		}
		stats.AvgModifyCount = float64(totalModifyCount) / float64(stats.TotalDirtyPages)
	}

	return &stats
}

// ShouldFlush 判断是否应该刷新脏页
func (dpm *DirtyPageManager) ShouldFlush(totalPages int) bool {
	dpm.mu.RLock()
	defer dpm.mu.RUnlock()

	dirtyCount := len(dpm.dirtyPages)

	// 检查脏页数量
	if dirtyCount >= dpm.config.MaxDirtyPages {
		return true
	}

	// 检查脏页比例
	if totalPages > 0 {
		dirtyRatio := float64(dirtyCount) / float64(totalPages)
		if dirtyRatio >= dpm.config.MaxDirtyRatio {
			return true
		}
	}

	return false
}

// GetFlushBatchSize 获取刷新批量大小
func (dpm *DirtyPageManager) GetFlushBatchSize(totalPages int) int {
	dpm.mu.RLock()
	defer dpm.mu.RUnlock()

	if !dpm.config.EnableAdaptive {
		return dpm.config.FlushBatchSize
	}

	// 自适应批量大小
	dirtyCount := len(dpm.dirtyPages)
	dirtyRatio := float64(dirtyCount) / float64(totalPages)

	batchSize := dpm.config.FlushBatchSize

	// 根据脏页比例调整批量大小
	if dirtyRatio >= 0.75 {
		batchSize = dpm.config.FlushBatchSize * 4 // 激进刷新
	} else if dirtyRatio >= 0.50 {
		batchSize = dpm.config.FlushBatchSize * 2 // 中等刷新
	} else if dirtyRatio >= 0.25 {
		batchSize = dpm.config.FlushBatchSize // 正常刷新
	} else {
		batchSize = dpm.config.FlushBatchSize / 2 // 轻度刷新
	}

	return batchSize
}

// makePageID 生成页面ID
func makePageID(spaceID, pageNo uint32) uint64 {
	return (uint64(spaceID) << 32) | uint64(pageNo)
}
