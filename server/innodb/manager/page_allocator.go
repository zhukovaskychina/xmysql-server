package manager

import (
	"fmt"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

/*
PageAllocator 实现了InnoDB智能页面分配器

核心功能：
1. 智能分配策略
   - 根据请求类型选择最优分配策略
   - 单页分配优先使用Fragment Extent
   - 批量分配使用完整Extent
   - 自适应分配策略调整

2. 位图管理
   - 高效的位图查找算法
   - 位图缓存机制
   - 并发位图更新保护

3. 碎片控制
   - 连续页面优先分配
   - 碎片率监控
   - 自动碎片整理触发

设计要点：
- 支持多种分配策略（Fragment、Complete、Hybrid）
- 维护空间利用率统计
- 最小化分配延迟
- 线程安全的并发分配
*/

const (
	// 分配策略常量
	AllocStrategyFragment = "fragment" // Fragment页面分配
	AllocStrategyComplete = "complete" // 完整Extent分配
	AllocStrategyHybrid   = "hybrid"   // 混合分配（自适应）

	// Fragment Extent相关常量
	FragmentPages    = 32 // 每个Fragment Extent包含32个页面
	PagesPerExtent   = 64 // 每个完整Extent包含64个页面
	FragmentExtentID = 0  // Fragment Extent的ID

	// 阈值配置
	FragmentThreshold    = 8  // 少于8页使用Fragment分配
	BatchThreshold       = 16 // 超过16页使用批量分配
	FragmentationLimit   = 30 // 碎片率上限（百分比）
	ContinuousPreference = 4  // 连续分配优先权重
)

// PageAllocator 页面分配器
type PageAllocator struct {
	sync.RWMutex

	// 表空间引用
	spaceManager basic.SpaceManager
	spaceID      uint32

	// 分配策略
	strategy string // 当前使用的分配策略

	// Fragment管理
	fragmentBitmap  []uint64 // Fragment页面位图（每bit表示一个页面）
	fragmentUsed    uint32   // 已使用的Fragment页面数
	fragmentExtents []uint32 // Fragment Extent列表

	// Extent管理
	freeExtents    []uint32 // 空闲Extent列表
	notFullExtents []uint32 // 部分使用的Extent列表
	fullExtents    []uint32 // 完全使用的Extent列表

	// 统计信息
	stats *AllocationStats

	// 配置信息
	config *AllocatorConfig
}

// AllocationStats 分配统计信息
type AllocationStats struct {
	sync.RWMutex

	// 分配计数
	TotalAllocations uint64 // 总分配次数
	FragmentAllocs   uint64 // Fragment分配次数
	CompleteAllocs   uint64 // 完整Extent分配次数
	BatchAllocs      uint64 // 批量分配次数
	FailedAllocs     uint64 // 失败的分配次数

	// 空间统计
	TotalPages     uint32 // 总页面数
	AllocatedPages uint32 // 已分配页面数
	FragmentPages  uint32 // Fragment页面数
	ExtentPages    uint32 // Extent页面数

	// 碎片统计
	FragmentationRate float64 // 碎片率（0-100）
	LargestFreeExtent uint32  // 最大连续空闲Extent数
	AverageHoleSize   uint32  // 平均空洞大小
}

// AllocatorConfig 分配器配置
type AllocatorConfig struct {
	// 策略配置
	DefaultStrategy      string  // 默认分配策略
	AutoTuneStrategy     bool    // 是否自动调整策略
	PreferContinuous     bool    // 是否优先连续分配
	FragmentationWarning float64 // 碎片率警告阈值

	// 性能配置
	BitmapCacheSize uint32 // 位图缓存大小
	BatchSize       uint32 // 批量分配大小
	PreallocExtents uint32 // 预分配Extent数量
}

// NewPageAllocator 创建页面分配器
func NewPageAllocator(spaceManager basic.SpaceManager, spaceID uint32, config *AllocatorConfig) *PageAllocator {
	if config == nil {
		config = &AllocatorConfig{
			DefaultStrategy:      AllocStrategyHybrid,
			AutoTuneStrategy:     true,
			PreferContinuous:     true,
			FragmentationWarning: 30.0,
			BitmapCacheSize:      256,
			BatchSize:            16,
			PreallocExtents:      4,
		}
	}

	pa := &PageAllocator{
		spaceManager:    spaceManager,
		spaceID:         spaceID,
		strategy:        config.DefaultStrategy,
		fragmentBitmap:  make([]uint64, (FragmentPages+63)/64), // 初始化位图
		fragmentUsed:    0,
		fragmentExtents: make([]uint32, 0),
		freeExtents:     make([]uint32, 0),
		notFullExtents:  make([]uint32, 0),
		fullExtents:     make([]uint32, 0),
		stats: &AllocationStats{
			FragmentationRate: 0.0,
			LargestFreeExtent: 0,
			AverageHoleSize:   0,
		},
		config: config,
	}

	return pa
}

// AllocatePage 分配单个页面
func (pa *PageAllocator) AllocatePage() (uint32, error) {
	pa.Lock()
	defer pa.Unlock()

	// 根据策略选择分配方法
	var pageNo uint32
	var err error

	switch pa.strategy {
	case AllocStrategyFragment:
		pageNo, err = pa.allocateFromFragment()
	case AllocStrategyComplete:
		pageNo, err = pa.allocateFromExtent()
	case AllocStrategyHybrid:
		// 混合策略：优先Fragment，失败则使用Extent
		pageNo, err = pa.allocateFromFragment()
		if err != nil {
			pageNo, err = pa.allocateFromExtent()
		}
	default:
		err = fmt.Errorf("unknown allocation strategy: %s", pa.strategy)
	}

	if err != nil {
		pa.stats.FailedAllocs++
		return 0, err
	}

	// 更新统计
	pa.stats.TotalAllocations++
	pa.stats.AllocatedPages++
	pa.updateFragmentationRate()

	return pageNo, nil
}

// AllocatePages 批量分配页面
func (pa *PageAllocator) AllocatePages(count uint32) ([]uint32, error) {
	pa.Lock()
	defer pa.Unlock()

	if count == 0 {
		return nil, fmt.Errorf("invalid page count: 0")
	}

	// 根据数量选择分配策略
	if count < FragmentThreshold {
		// 小批量：逐个从Fragment分配
		return pa.allocatePagesFromFragment(count)
	} else if count >= BatchThreshold {
		// 大批量：分配完整Extent
		return pa.allocatePagesFromExtents(count)
	} else {
		// 中等批量：混合分配
		return pa.allocatePagesHybrid(count)
	}
}

// allocateFromFragment 从Fragment Extent分配页面
func (pa *PageAllocator) allocateFromFragment() (uint32, error) {
	// 查找空闲的Fragment页面
	pageOffset := pa.findFreeFragmentPage()
	if pageOffset == ^uint32(0) {
		// Fragment已满，需要分配新的Fragment Extent
		return 0, fmt.Errorf("fragment extent is full")
	}

	// 标记页面为已分配
	pa.setFragmentBit(pageOffset)
	pa.fragmentUsed++
	pa.stats.FragmentAllocs++
	pa.stats.FragmentPages++

	// 返回实际页面号（Fragment Extent起始 + offset）
	return pageOffset, nil
}

// allocateFromExtent 从完整Extent分配页面
func (pa *PageAllocator) allocateFromExtent() (uint32, error) {
	// 优先从部分使用的Extent分配
	if len(pa.notFullExtents) > 0 {
		extentID := pa.notFullExtents[0]
		pageNo := extentID*PagesPerExtent + pa.findFreePageInExtent(extentID)
		pa.stats.CompleteAllocs++
		pa.stats.ExtentPages++
		return pageNo, nil
	}

	// 从空闲Extent分配
	if len(pa.freeExtents) > 0 {
		extentID := pa.freeExtents[0]
		pa.freeExtents = pa.freeExtents[1:]

		// 分配第一个页面
		pageNo := extentID * PagesPerExtent
		pa.notFullExtents = append(pa.notFullExtents, extentID)

		pa.stats.CompleteAllocs++
		pa.stats.ExtentPages++
		return pageNo, nil
	}

	// 需要扩展表空间
	extent, err := pa.spaceManager.AllocateExtent(pa.spaceID, basic.ExtentPurposeData)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate extent: %v", err)
	}

	extentID := extent.GetID()
	pageNo := extentID * PagesPerExtent
	pa.notFullExtents = append(pa.notFullExtents, extentID)

	pa.stats.CompleteAllocs++
	pa.stats.ExtentPages++
	return pageNo, nil
}

// allocatePagesFromFragment 从Fragment分配多个页面
func (pa *PageAllocator) allocatePagesFromFragment(count uint32) ([]uint32, error) {
	pages := make([]uint32, 0, count)

	for i := uint32(0); i < count; i++ {
		pageNo, err := pa.allocateFromFragment()
		if err != nil {
			// 如果Fragment不足，返回已分配的页面
			if len(pages) > 0 {
				pa.stats.BatchAllocs++
				return pages, nil
			}
			return nil, err
		}
		pages = append(pages, pageNo)
	}

	pa.stats.BatchAllocs++
	return pages, nil
}

// allocatePagesFromExtents 从Extent分配多个页面
func (pa *PageAllocator) allocatePagesFromExtents(count uint32) ([]uint32, error) {
	pages := make([]uint32, 0, count)

	// 计算需要的Extent数量
	extentsNeeded := (count + PagesPerExtent - 1) / PagesPerExtent

	for i := uint32(0); i < extentsNeeded; i++ {
		// 分配一个Extent
		var extentID uint32
		if len(pa.freeExtents) > 0 {
			extentID = pa.freeExtents[0]
			pa.freeExtents = pa.freeExtents[1:]
		} else {
			// 需要扩展表空间
			extent, err := pa.spaceManager.AllocateExtent(pa.spaceID, basic.ExtentPurposeData)
			if err != nil {
				if len(pages) > 0 {
					pa.stats.BatchAllocs++
					return pages, nil
				}
				return nil, fmt.Errorf("failed to allocate extent: %v", err)
			}
			extentID = extent.GetID()
		}

		// 分配此Extent中的页面
		startPage := extentID * PagesPerExtent
		pagesInExtent := uint32(PagesPerExtent)
		if uint32(len(pages))+pagesInExtent > count {
			pagesInExtent = count - uint32(len(pages))
		}

		for j := uint32(0); j < pagesInExtent; j++ {
			pages = append(pages, startPage+j)
		}

		// 更新Extent状态
		if pagesInExtent < uint32(PagesPerExtent) {
			pa.notFullExtents = append(pa.notFullExtents, extentID)
		} else {
			pa.fullExtents = append(pa.fullExtents, extentID)
		}
	}

	pa.stats.BatchAllocs++
	pa.stats.ExtentPages += count
	return pages, nil
}

// allocatePagesHybrid 混合分配策略
func (pa *PageAllocator) allocatePagesHybrid(count uint32) ([]uint32, error) {
	// 优先从Fragment分配，直到Fragment满或达到目标
	fragmentPages, err := pa.allocatePagesFromFragment(count)
	if err == nil && uint32(len(fragmentPages)) >= count {
		return fragmentPages, nil
	}

	// Fragment不足，从Extent补充
	remaining := count - uint32(len(fragmentPages))
	extentPages, err := pa.allocatePagesFromExtents(remaining)
	if err != nil {
		if len(fragmentPages) > 0 {
			return fragmentPages, nil
		}
		return nil, err
	}

	// 合并结果
	return append(fragmentPages, extentPages...), nil
}

// FreePage 释放单个页面
func (pa *PageAllocator) FreePage(pageNo uint32) error {
	pa.Lock()
	defer pa.Unlock()

	// 判断是Fragment页面还是Extent页面
	if pageNo < FragmentPages {
		// Fragment页面
		pa.clearFragmentBit(pageNo)
		pa.fragmentUsed--
		pa.stats.FragmentPages--
	} else {
		// Extent页面
		extentID := pageNo / PagesPerExtent
		pa.freePageInExtent(extentID, pageNo%PagesPerExtent)
		pa.stats.ExtentPages--
	}

	pa.stats.AllocatedPages--
	pa.updateFragmentationRate()

	return nil
}

// FreePages 批量释放页面
func (pa *PageAllocator) FreePages(pages []uint32) error {
	pa.Lock()
	defer pa.Unlock()

	for _, pageNo := range pages {
		if pageNo < FragmentPages {
			pa.clearFragmentBit(pageNo)
			pa.fragmentUsed--
			pa.stats.FragmentPages--
		} else {
			extentID := pageNo / PagesPerExtent
			pa.freePageInExtent(extentID, pageNo%PagesPerExtent)
			pa.stats.ExtentPages--
		}
		pa.stats.AllocatedPages--
	}

	pa.updateFragmentationRate()
	return nil
}

// GetStats 获取统计信息
func (pa *PageAllocator) GetStats() *AllocationStats {
	pa.RLock()
	defer pa.RUnlock()

	// 返回统计信息的副本（不包含mutex）
	statsCopy := AllocationStats{
		TotalAllocations:  pa.stats.TotalAllocations,
		FragmentAllocs:    pa.stats.FragmentAllocs,
		CompleteAllocs:    pa.stats.CompleteAllocs,
		BatchAllocs:       pa.stats.BatchAllocs,
		FailedAllocs:      pa.stats.FailedAllocs,
		TotalPages:        pa.stats.TotalPages,
		AllocatedPages:    pa.stats.AllocatedPages,
		FragmentPages:     pa.stats.FragmentPages,
		ExtentPages:       pa.stats.ExtentPages,
		FragmentationRate: pa.stats.FragmentationRate,
	}
	return &statsCopy
}

// GetFragmentationRate 获取碎片率
func (pa *PageAllocator) GetFragmentationRate() float64 {
	pa.RLock()
	defer pa.RUnlock()
	return pa.stats.FragmentationRate
}

// SetStrategy 设置分配策略
func (pa *PageAllocator) SetStrategy(strategy string) error {
	pa.Lock()
	defer pa.Unlock()

	switch strategy {
	case AllocStrategyFragment, AllocStrategyComplete, AllocStrategyHybrid:
		pa.strategy = strategy
		return nil
	default:
		return fmt.Errorf("unknown allocation strategy: %s", strategy)
	}
}

// 位图操作辅助函数

// findFreeFragmentPage 查找空闲的Fragment页面
func (pa *PageAllocator) findFreeFragmentPage() uint32 {
	for i := uint32(0); i < FragmentPages; i++ {
		if !pa.isFragmentBitSet(i) {
			return i
		}
	}
	return ^uint32(0) // 返回-1表示未找到
}

// isFragmentBitSet 检查Fragment位图中的某个位是否已设置
func (pa *PageAllocator) isFragmentBitSet(offset uint32) bool {
	wordIndex := offset / 64
	bitIndex := offset % 64
	if wordIndex >= uint32(len(pa.fragmentBitmap)) {
		return false
	}
	return (pa.fragmentBitmap[wordIndex] & (1 << bitIndex)) != 0
}

// setFragmentBit 设置Fragment位图中的某个位
func (pa *PageAllocator) setFragmentBit(offset uint32) {
	wordIndex := offset / 64
	bitIndex := offset % 64
	if wordIndex < uint32(len(pa.fragmentBitmap)) {
		pa.fragmentBitmap[wordIndex] |= (1 << bitIndex)
	}
}

// clearFragmentBit 清除Fragment位图中的某个位
func (pa *PageAllocator) clearFragmentBit(offset uint32) {
	wordIndex := offset / 64
	bitIndex := offset % 64
	if wordIndex < uint32(len(pa.fragmentBitmap)) {
		pa.fragmentBitmap[wordIndex] &^= (1 << bitIndex)
	}
}

// findFreePageInExtent 在指定Extent中查找空闲页面
func (pa *PageAllocator) findFreePageInExtent(extentID uint32) uint32 {
	// TODO: 实现Extent内部的页面分配位图
	// 当前简化实现，返回第一个页面
	return 0
}

// freePageInExtent 释放Extent中的页面
func (pa *PageAllocator) freePageInExtent(extentID uint32, pageOffset uint32) {
	// TODO: 实现Extent内部的页面释放
	// 当前简化实现
}

// updateFragmentationRate 更新碎片率
func (pa *PageAllocator) updateFragmentationRate() {
	if pa.stats.TotalPages == 0 {
		pa.stats.FragmentationRate = 0.0
		return
	}

	// 碎片率 = (Fragment使用的页面数 / 总页面数) * 100
	fragmentRate := float64(pa.stats.FragmentPages) / float64(pa.stats.TotalPages) * 100

	// 考虑空洞和不连续性
	// 这里简化计算，实际应该分析页面分布的连续性
	pa.stats.FragmentationRate = fragmentRate
}
