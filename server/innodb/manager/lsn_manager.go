package manager

import (
	"sync"
	"sync/atomic"
	"time"
)

// LSN (Log Sequence Number) 日志序列号
// LSN是一个单调递增的64位整数，用于标识日志记录的顺序位置
type LSN uint64

// LSNManager LSN管理器
// 负责分配和管理LSN，确保LSN的全局唯一性和单调递增性
type LSNManager struct {
	// 当前LSN，使用原子操作保证并发安全
	currentLSN uint64

	// LSN分配统计
	mu                sync.RWMutex
	totalAllocated    uint64    // 总分配次数
	allocationsPerSec float64   // 每秒分配次数
	lastStatsUpdate   time.Time // 上次统计更新时间
	allocCounter      uint64    // 分配计数器

	// LSN范围管理
	minLSN uint64 // 最小LSN（系统启动时的LSN）
	maxLSN uint64 // 当前最大LSN

	// 最近一次设置的检查点 LSN（供恢复/监控读回）
	checkpointLSN uint64
}

// NewLSNManager 创建新的LSN管理器
func NewLSNManager(initialLSN uint64) *LSNManager {
	if initialLSN == 0 {
		initialLSN = 1 // LSN从1开始，0表示无效LSN
	}

	return &LSNManager{
		currentLSN:      initialLSN,
		minLSN:          initialLSN,
		maxLSN:          initialLSN,
		lastStatsUpdate: time.Now(),
	}
}

// AllocateLSN 分配一个新的LSN
// 返回值：新分配的LSN
func (lm *LSNManager) AllocateLSN() LSN {
	// 使用原子操作递增LSN
	newLSN := atomic.AddUint64(&lm.currentLSN, 1)

	// 更新统计信息
	atomic.AddUint64(&lm.allocCounter, 1)
	atomic.StoreUint64(&lm.maxLSN, newLSN)

	return LSN(newLSN)
}

// AllocateLSNRange 批量分配LSN范围
// count: 需要分配的LSN数量
// 返回值：起始LSN和结束LSN（包含）
func (lm *LSNManager) AllocateLSNRange(count uint64) (startLSN, endLSN LSN) {
	if count == 0 {
		return 0, 0
	}

	// 原子地分配一段LSN
	start := atomic.AddUint64(&lm.currentLSN, count)
	startLSN = LSN(start - count + 1)
	endLSN = LSN(start)

	// 更新统计信息
	atomic.AddUint64(&lm.allocCounter, count)
	atomic.StoreUint64(&lm.maxLSN, uint64(endLSN))

	return startLSN, endLSN
}

// GetCurrentLSN 获取当前LSN（已分配的最大LSN）
func (lm *LSNManager) GetCurrentLSN() LSN {
	return LSN(atomic.LoadUint64(&lm.currentLSN))
}

// GetMinLSN 获取最小LSN
func (lm *LSNManager) GetMinLSN() LSN {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return LSN(lm.minLSN)
}

// GetMaxLSN 获取最大LSN
func (lm *LSNManager) GetMaxLSN() LSN {
	return LSN(atomic.LoadUint64(&lm.maxLSN))
}

// SetCheckpointLSN 设置检查点LSN
// 允许在恢复时重置LSN起点
func (lm *LSNManager) SetCheckpointLSN(lsn LSN) {
	if lsn > 0 {
		atomic.StoreUint64(&lm.currentLSN, uint64(lsn))
		atomic.StoreUint64(&lm.maxLSN, uint64(lsn))
		atomic.StoreUint64(&lm.checkpointLSN, uint64(lsn))

		lm.mu.Lock()
		if uint64(lsn) < lm.minLSN {
			lm.minLSN = uint64(lsn)
		}
		lm.mu.Unlock()
	}
}

// GetCheckpointLSN 返回最近一次设置的检查点 LSN；未设置过时返回 0
func (lm *LSNManager) GetCheckpointLSN() LSN {
	return LSN(atomic.LoadUint64(&lm.checkpointLSN))
}

// GetStats 获取LSN分配统计信息
func (lm *LSNManager) GetStats() *LSNStats {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(lm.lastStatsUpdate).Seconds()

	// 计算每秒分配速率
	if elapsed > 0 {
		currentCounter := atomic.LoadUint64(&lm.allocCounter)
		lm.allocationsPerSec = float64(currentCounter-lm.totalAllocated) / elapsed
		lm.totalAllocated = currentCounter
		lm.lastStatsUpdate = now
	}

	return &LSNStats{
		CurrentLSN:        LSN(atomic.LoadUint64(&lm.currentLSN)),
		MinLSN:            LSN(lm.minLSN),
		MaxLSN:            LSN(atomic.LoadUint64(&lm.maxLSN)),
		TotalAllocated:    lm.totalAllocated,
		AllocationsPerSec: lm.allocationsPerSec,
	}
}

// ResetStats 重置统计信息
func (lm *LSNManager) ResetStats() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.totalAllocated = 0
	atomic.StoreUint64(&lm.allocCounter, 0)
	lm.allocationsPerSec = 0
	lm.lastStatsUpdate = time.Now()
}

// LSNStats LSN统计信息
type LSNStats struct {
	CurrentLSN        LSN     `json:"current_lsn"`         // 当前LSN
	MinLSN            LSN     `json:"min_lsn"`             // 最小LSN
	MaxLSN            LSN     `json:"max_lsn"`             // 最大LSN
	TotalAllocated    uint64  `json:"total_allocated"`     // 总分配次数
	AllocationsPerSec float64 `json:"allocations_per_sec"` // 每秒分配次数
}

// LSNRange 表示一个LSN范围
type LSNRange struct {
	Start LSN // 起始LSN（包含）
	End   LSN // 结束LSN（包含）
}

// Contains 判断LSN是否在范围内
func (r *LSNRange) Contains(lsn LSN) bool {
	return lsn >= r.Start && lsn <= r.End
}

// IsValid 判断LSN范围是否有效
func (r *LSNRange) IsValid() bool {
	return r.Start > 0 && r.End >= r.Start
}

// Size 返回LSN范围大小
func (r *LSNRange) Size() uint64 {
	if !r.IsValid() {
		return 0
	}
	return uint64(r.End - r.Start + 1)
}

// Overlaps 判断两个LSN范围是否重叠
func (r *LSNRange) Overlaps(other *LSNRange) bool {
	if !r.IsValid() || !other.IsValid() {
		return false
	}
	return r.Start <= other.End && other.Start <= r.End
}

// Merge 合并两个LSN范围
func (r *LSNRange) Merge(other *LSNRange) *LSNRange {
	if !r.IsValid() || !other.IsValid() {
		return nil
	}

	minStart := r.Start
	if other.Start < minStart {
		minStart = other.Start
	}

	maxEnd := r.End
	if other.End > maxEnd {
		maxEnd = other.End
	}

	return &LSNRange{
		Start: minStart,
		End:   maxEnd,
	}
}
