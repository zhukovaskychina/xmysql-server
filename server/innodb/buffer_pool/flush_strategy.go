package buffer_pool

import (
	"sort"
	"time"
)

// FlushStrategy 刷新策略接口
type FlushStrategy interface {
	SelectPagesToFlush(dirtyPages []*BufferPage, maxPages int) []*BufferPage
	GetFlushPriority(page *BufferPage) int
}

// LSNBasedFlushStrategy 基于LSN的刷新策略
type LSNBasedFlushStrategy struct{}

// NewLSNBasedFlushStrategy 创建基于LSN的刷新策略
func NewLSNBasedFlushStrategy() *LSNBasedFlushStrategy {
	return &LSNBasedFlushStrategy{}
}

// SelectPagesToFlush 选择要刷新的页面
func (s *LSNBasedFlushStrategy) SelectPagesToFlush(dirtyPages []*BufferPage, maxPages int) []*BufferPage {
	if len(dirtyPages) == 0 {
		return nil
	}

	// 按LSN排序，优先刷新LSN较小的页面
	sortedPages := make([]*BufferPage, len(dirtyPages))
	copy(sortedPages, dirtyPages)

	sort.Slice(sortedPages, func(i, j int) bool {
		return sortedPages[i].GetLSN() < sortedPages[j].GetLSN()
	})

	// 选择前maxPages个页面
	if len(sortedPages) > maxPages {
		return sortedPages[:maxPages]
	}

	return sortedPages
}

// GetFlushPriority 获取刷新优先级
func (s *LSNBasedFlushStrategy) GetFlushPriority(page *BufferPage) int {
	// LSN越小，优先级越高
	lsn := page.GetLSN()
	if lsn < 1000 {
		return 10 // 最高优先级
	} else if lsn < 10000 {
		return 7
	} else if lsn < 100000 {
		return 5
	}
	return 1 // 最低优先级
}

// AgeBasedFlushStrategy 基于年龄的刷新策略
type AgeBasedFlushStrategy struct{}

// NewAgeBasedFlushStrategy 创建基于年龄的刷新策略
func NewAgeBasedFlushStrategy() *AgeBasedFlushStrategy {
	return &AgeBasedFlushStrategy{}
}

// SelectPagesToFlush 选择要刷新的页面
func (s *AgeBasedFlushStrategy) SelectPagesToFlush(dirtyPages []*BufferPage, maxPages int) []*BufferPage {
	if len(dirtyPages) == 0 {
		return nil
	}

	// 按访问时间排序，优先刷新较老的页面
	sortedPages := make([]*BufferPage, len(dirtyPages))
	copy(sortedPages, dirtyPages)

	sort.Slice(sortedPages, func(i, j int) bool {
		return sortedPages[i].accessTime < sortedPages[j].accessTime
	})

	if len(sortedPages) > maxPages {
		return sortedPages[:maxPages]
	}

	return sortedPages
}

// GetFlushPriority 获取刷新优先级
func (s *AgeBasedFlushStrategy) GetFlushPriority(page *BufferPage) int {
	now := uint64(time.Now().UnixNano())
	age := now - page.accessTime

	// 年龄越大，优先级越高
	if age > uint64(time.Hour.Nanoseconds()) {
		return 10 // 超过1小时，最高优先级
	} else if age > uint64(time.Minute.Nanoseconds()*30) {
		return 7 // 超过30分钟
	} else if age > uint64(time.Minute.Nanoseconds()*10) {
		return 5 // 超过10分钟
	}
	return 1 // 最低优先级
}

// SizeBasedFlushStrategy 基于大小的刷新策略
type SizeBasedFlushStrategy struct{}

// NewSizeBasedFlushStrategy 创建基于大小的刷新策略
func NewSizeBasedFlushStrategy() *SizeBasedFlushStrategy {
	return &SizeBasedFlushStrategy{}
}

// SelectPagesToFlush 选择要刷新的页面
func (s *SizeBasedFlushStrategy) SelectPagesToFlush(dirtyPages []*BufferPage, maxPages int) []*BufferPage {
	if len(dirtyPages) == 0 {
		return nil
	}

	// 按页面大小排序，优先刷新较大的页面
	sortedPages := make([]*BufferPage, len(dirtyPages))
	copy(sortedPages, dirtyPages)

	sort.Slice(sortedPages, func(i, j int) bool {
		return len(sortedPages[i].GetContent()) > len(sortedPages[j].GetContent())
	})

	if len(sortedPages) > maxPages {
		return sortedPages[:maxPages]
	}

	return sortedPages
}

// GetFlushPriority 获取刷新优先级
func (s *SizeBasedFlushStrategy) GetFlushPriority(page *BufferPage) int {
	size := len(page.GetContent())

	// 页面越大，优先级越高
	if size > 32*1024 {
		return 10 // 超过32KB，最高优先级
	} else if size > 16*1024 {
		return 7 // 超过16KB
	} else if size > 8*1024 {
		return 5 // 超过8KB
	}
	return 1 // 最低优先级
}

// CompositeFlushStrategy 组合刷新策略
type CompositeFlushStrategy struct {
	strategies []FlushStrategy
	weights    []float64
}

// NewCompositeFlushStrategy 创建组合刷新策略
func NewCompositeFlushStrategy(strategies []FlushStrategy, weights []float64) *CompositeFlushStrategy {
	if len(strategies) != len(weights) {
		panic("strategies and weights length mismatch")
	}

	return &CompositeFlushStrategy{
		strategies: strategies,
		weights:    weights,
	}
}

// SelectPagesToFlush 选择要刷新的页面
func (s *CompositeFlushStrategy) SelectPagesToFlush(dirtyPages []*BufferPage, maxPages int) []*BufferPage {
	if len(dirtyPages) == 0 {
		return nil
	}

	// 计算每个页面的综合优先级
	type pageWithPriority struct {
		page     *BufferPage
		priority float64
	}

	pagesWithPriority := make([]pageWithPriority, len(dirtyPages))

	for i, page := range dirtyPages {
		totalPriority := 0.0
		for j, strategy := range s.strategies {
			priority := float64(strategy.GetFlushPriority(page))
			totalPriority += priority * s.weights[j]
		}
		pagesWithPriority[i] = pageWithPriority{page: page, priority: totalPriority}
	}

	// 按综合优先级排序
	sort.Slice(pagesWithPriority, func(i, j int) bool {
		return pagesWithPriority[i].priority > pagesWithPriority[j].priority
	})

	// 选择前maxPages个页面
	result := make([]*BufferPage, 0, maxPages)
	for i := 0; i < len(pagesWithPriority) && i < maxPages; i++ {
		result = append(result, pagesWithPriority[i].page)
	}

	return result
}

// GetFlushPriority 获取刷新优先级
func (s *CompositeFlushStrategy) GetFlushPriority(page *BufferPage) int {
	totalPriority := 0.0
	for i, strategy := range s.strategies {
		priority := float64(strategy.GetFlushPriority(page))
		totalPriority += priority * s.weights[i]
	}
	return int(totalPriority)
}
