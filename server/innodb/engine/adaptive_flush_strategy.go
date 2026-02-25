package engine

import (
	"math"
	"sort"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// AdaptiveFlushStrategy 自适应刷新策略
// 根据系统负载、脏页比例、LSN增长速度等因素动态调整刷新策略
type AdaptiveFlushStrategy struct {
	// 历史统计
	history *FlushHistory

	// 配置
	config *AdaptiveFlushConfig
}

// FlushHistory 刷新历史
type FlushHistory struct {
	// LSN增长速度（LSN/秒）
	lsnGrowthRate float64

	// 脏页增长速度（页/秒）
	dirtyPageGrowthRate float64

	// 最近刷新速度（页/秒）
	recentFlushRate float64

	// 最后更新时间
	lastUpdateTime time.Time

	// 最后LSN
	lastLSN uint64

	// 最后脏页数
	lastDirtyPageCount int
}

// AdaptiveFlushConfig 自适应刷新配置
type AdaptiveFlushConfig struct {
	// 目标脏页比例
	TargetDirtyRatio float64

	// 最大脏页比例
	MaxDirtyRatio float64

	// LSN增长阈值（LSN/秒）
	LSNGrowthThreshold float64

	// 刷新速率限制（页/秒）
	MaxFlushRate int

	// 最小刷新批量
	MinFlushBatch int

	// 最大刷新批量
	MaxFlushBatch int
}

// NewAdaptiveFlushStrategy 创建自适应刷新策略
func NewAdaptiveFlushStrategy(config *AdaptiveFlushConfig) *AdaptiveFlushStrategy {
	if config == nil {
		config = &AdaptiveFlushConfig{
			TargetDirtyRatio:   0.50,
			MaxDirtyRatio:      0.75,
			LSNGrowthThreshold: 10000.0,
			MaxFlushRate:       1000,
			MinFlushBatch:      10,
			MaxFlushBatch:      500,
		}
	}

	return &AdaptiveFlushStrategy{
		history: &FlushHistory{
			lastUpdateTime: time.Now(),
		},
		config: config,
	}
}

// SelectPagesToFlush 选择要刷新的页面
func (afs *AdaptiveFlushStrategy) SelectPagesToFlush(
	dirtyPages []*buffer_pool.BufferPage,
	maxPages int,
	currentLSN uint64,
) []*buffer_pool.BufferPage {
	if len(dirtyPages) == 0 {
		return nil
	}

	// 更新历史统计
	afs.updateHistory(currentLSN, len(dirtyPages))

	// 计算刷新批量大小
	batchSize := afs.calculateFlushBatchSize(len(dirtyPages), maxPages)

	logger.Debugf("🎯 自适应刷新策略: 脏页=%d, 批量=%d, LSN增长=%.2f/s",
		len(dirtyPages), batchSize, afs.history.lsnGrowthRate)

	// 选择页面
	return afs.selectPages(dirtyPages, batchSize)
}

// updateHistory 更新历史统计
func (afs *AdaptiveFlushStrategy) updateHistory(currentLSN uint64, dirtyPageCount int) {
	now := time.Now()
	elapsed := now.Sub(afs.history.lastUpdateTime).Seconds()

	if elapsed > 0 {
		// 计算LSN增长速度
		if afs.history.lastLSN > 0 {
			lsnDelta := float64(currentLSN - afs.history.lastLSN)
			afs.history.lsnGrowthRate = lsnDelta / elapsed
		}

		// 计算脏页增长速度
		if afs.history.lastDirtyPageCount > 0 {
			dirtyPageDelta := float64(dirtyPageCount - afs.history.lastDirtyPageCount)
			afs.history.dirtyPageGrowthRate = dirtyPageDelta / elapsed
		}

		// 更新历史
		afs.history.lastUpdateTime = now
		afs.history.lastLSN = currentLSN
		afs.history.lastDirtyPageCount = dirtyPageCount
	}
}

// calculateFlushBatchSize 计算刷新批量大小
func (afs *AdaptiveFlushStrategy) calculateFlushBatchSize(dirtyPageCount, maxPages int) int {
	// 基础批量大小
	batchSize := afs.config.MinFlushBatch

	// 因素1: 脏页比例
	// 脏页比例越高，批量越大
	dirtyRatio := float64(dirtyPageCount) / float64(maxPages)
	if dirtyRatio > afs.config.TargetDirtyRatio {
		ratioFactor := (dirtyRatio - afs.config.TargetDirtyRatio) / (afs.config.MaxDirtyRatio - afs.config.TargetDirtyRatio)
		batchSize += int(float64(afs.config.MaxFlushBatch-afs.config.MinFlushBatch) * ratioFactor)
	}

	// 因素2: LSN增长速度
	// LSN增长越快，批量越大
	if afs.history.lsnGrowthRate > afs.config.LSNGrowthThreshold {
		lsnFactor := math.Min(afs.history.lsnGrowthRate/afs.config.LSNGrowthThreshold, 2.0)
		batchSize = int(float64(batchSize) * lsnFactor)
	}

	// 因素3: 脏页增长速度
	// 脏页增长越快，批量越大
	if afs.history.dirtyPageGrowthRate > 0 {
		growthFactor := 1.0 + (afs.history.dirtyPageGrowthRate / 100.0)
		batchSize = int(float64(batchSize) * growthFactor)
	}

	// 限制批量大小
	if batchSize < afs.config.MinFlushBatch {
		batchSize = afs.config.MinFlushBatch
	}
	if batchSize > afs.config.MaxFlushBatch {
		batchSize = afs.config.MaxFlushBatch
	}
	if batchSize > dirtyPageCount {
		batchSize = dirtyPageCount
	}

	return batchSize
}

// selectPages 选择页面
func (afs *AdaptiveFlushStrategy) selectPages(
	dirtyPages []*buffer_pool.BufferPage,
	batchSize int,
) []*buffer_pool.BufferPage {
	if len(dirtyPages) <= batchSize {
		return dirtyPages
	}

	// 使用多因素评分选择页面
	type pageScore struct {
		page  *buffer_pool.BufferPage
		score float64
	}

	scores := make([]pageScore, len(dirtyPages))

	for i, page := range dirtyPages {
		score := afs.calculatePageScore(page)
		scores[i] = pageScore{page: page, score: score}
	}

	// 按得分排序（得分越高，越优先刷新）
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	// 选择前batchSize个页面
	selected := make([]*buffer_pool.BufferPage, batchSize)
	for i := 0; i < batchSize; i++ {
		selected[i] = scores[i].page
	}

	return selected
}

// calculatePageScore 计算页面得分
func (afs *AdaptiveFlushStrategy) calculatePageScore(page *buffer_pool.BufferPage) float64 {
	score := 0.0

	// 因素1: LSN（权重50%）
	// LSN越小，得分越高（优先刷新老页面，推进checkpoint LSN）
	lsn := page.GetLSN()
	if lsn > 0 && afs.history.lastLSN > 0 {
		lsnScore := 1.0 - (float64(lsn) / float64(afs.history.lastLSN))
		if lsnScore < 0 {
			lsnScore = 0
		}
		score += lsnScore * 0.5
	}

	// 因素2: 访问频率（权重30%）
	// 访问频率越低，得分越高（减少对热点页面的影响）
	// 简化实现：使用固定得分
	accessScore := 0.5
	score += accessScore * 0.3

	// 因素3: 脏标记（权重20%）
	// 脏页优先刷新
	if page.IsDirty() {
		score += 0.2
	}

	return score
}

// GetFlushRate 获取建议的刷新速率（页/秒）
func (afs *AdaptiveFlushStrategy) GetFlushRate(dirtyPageCount, totalPages int) int {
	dirtyRatio := float64(dirtyPageCount) / float64(totalPages)

	// 基础刷新速率
	baseRate := afs.config.MaxFlushRate / 10

	// 根据脏页比例调整
	if dirtyRatio > afs.config.MaxDirtyRatio {
		// 激进刷新
		return afs.config.MaxFlushRate
	} else if dirtyRatio > afs.config.TargetDirtyRatio {
		// 中等刷新
		factor := (dirtyRatio - afs.config.TargetDirtyRatio) / (afs.config.MaxDirtyRatio - afs.config.TargetDirtyRatio)
		return baseRate + int(float64(afs.config.MaxFlushRate-baseRate)*factor)
	} else {
		// 轻度刷新
		return baseRate
	}
}

// GetFlushInterval 获取建议的刷新间隔
func (afs *AdaptiveFlushStrategy) GetFlushInterval(dirtyPageCount, totalPages int) time.Duration {
	dirtyRatio := float64(dirtyPageCount) / float64(totalPages)

	// 根据脏页比例调整刷新间隔
	if dirtyRatio > afs.config.MaxDirtyRatio {
		return 100 * time.Millisecond // 激进刷新
	} else if dirtyRatio > afs.config.TargetDirtyRatio {
		return 500 * time.Millisecond // 中等刷新
	} else if dirtyRatio > 0.25 {
		return 1 * time.Second // 正常刷新
	} else {
		return 5 * time.Second // 轻度刷新
	}
}

// ShouldFlush 判断是否应该刷新
func (afs *AdaptiveFlushStrategy) ShouldFlush(dirtyPageCount, totalPages int) bool {
	dirtyRatio := float64(dirtyPageCount) / float64(totalPages)

	// 检查脏页比例
	if dirtyRatio >= afs.config.MaxDirtyRatio {
		return true
	}

	// 检查LSN增长速度
	if afs.history.lsnGrowthRate > afs.config.LSNGrowthThreshold*2 {
		return true
	}

	// 检查脏页增长速度
	if afs.history.dirtyPageGrowthRate > 100 {
		return true
	}

	return false
}
