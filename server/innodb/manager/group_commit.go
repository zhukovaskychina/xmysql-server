package manager

import (
	"sync"
	"time"
)

// GroupCommit 组提交管理器
// 将多个事务的提交请求合并成一次fsync操作，提高I/O效率
type GroupCommit struct {
	mu sync.RWMutex

	// 配置
	windowDuration time.Duration // 组提交窗口期
	maxBatchSize   int           // 最大批次大小

	// 统计
	totalCommits     uint64        // 总提交次数
	totalBatches     uint64        // 总批次数
	totalFsyncs      uint64        // 总fsync次数
	avgBatchSize     float64       // 平均批次大小
	avgCommitLatency time.Duration // 平均提交延迟
	maxCommitLatency time.Duration // 最大提交延迟
	lastStatsUpdate  time.Time     // 上次统计更新时间
}

// NewGroupCommit 创建新的组提交管理器
func NewGroupCommit(windowDuration time.Duration, maxBatchSize int) *GroupCommit {
	if windowDuration == 0 {
		windowDuration = 10 * time.Millisecond // 默认10ms窗口
	}
	if maxBatchSize == 0 {
		maxBatchSize = 100 // 默认100个请求
	}

	return &GroupCommit{
		windowDuration:  windowDuration,
		maxBatchSize:    maxBatchSize,
		lastStatsUpdate: time.Now(),
	}
}

// RecordCommit 记录一次提交
func (gc *GroupCommit) RecordCommit(batchSize int, latency time.Duration) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.totalCommits += uint64(batchSize)
	gc.totalBatches++
	gc.totalFsyncs++

	// 更新平均批次大小
	gc.avgBatchSize = float64(gc.totalCommits) / float64(gc.totalBatches)

	// 更新延迟统计
	if latency > gc.maxCommitLatency {
		gc.maxCommitLatency = latency
	}

	// 计算平均延迟
	totalLatency := time.Duration(gc.avgCommitLatency.Nanoseconds()*int64(gc.totalBatches-1)) + latency
	gc.avgCommitLatency = totalLatency / time.Duration(gc.totalBatches)
}

// GetStats 获取统计信息
func (gc *GroupCommit) GetStats() *GroupCommitStats {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	return &GroupCommitStats{
		TotalCommits:     gc.totalCommits,
		TotalBatches:     gc.totalBatches,
		TotalFsyncs:      gc.totalFsyncs,
		AvgBatchSize:     gc.avgBatchSize,
		AvgCommitLatency: gc.avgCommitLatency,
		MaxCommitLatency: gc.maxCommitLatency,
		WindowDuration:   gc.windowDuration,
		MaxBatchSize:     gc.maxBatchSize,
	}
}

// ResetStats 重置统计信息
func (gc *GroupCommit) ResetStats() {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.totalCommits = 0
	gc.totalBatches = 0
	gc.totalFsyncs = 0
	gc.avgBatchSize = 0
	gc.avgCommitLatency = 0
	gc.maxCommitLatency = 0
	gc.lastStatsUpdate = time.Now()
}

// GetWindowDuration 获取窗口期
func (gc *GroupCommit) GetWindowDuration() time.Duration {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.windowDuration
}

// SetWindowDuration 设置窗口期
func (gc *GroupCommit) SetWindowDuration(duration time.Duration) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.windowDuration = duration
}

// GetMaxBatchSize 获取最大批次大小
func (gc *GroupCommit) GetMaxBatchSize() int {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.maxBatchSize
}

// SetMaxBatchSize 设置最大批次大小
func (gc *GroupCommit) SetMaxBatchSize(size int) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.maxBatchSize = size
}

// GroupCommitStats 组提交统计信息
type GroupCommitStats struct {
	TotalCommits     uint64        `json:"total_commits"`      // 总提交次数
	TotalBatches     uint64        `json:"total_batches"`      // 总批次数
	TotalFsyncs      uint64        `json:"total_fsyncs"`       // 总fsync次数
	AvgBatchSize     float64       `json:"avg_batch_size"`     // 平均批次大小
	AvgCommitLatency time.Duration `json:"avg_commit_latency"` // 平均提交延迟
	MaxCommitLatency time.Duration `json:"max_commit_latency"` // 最大提交延迟
	WindowDuration   time.Duration `json:"window_duration"`    // 窗口期
	MaxBatchSize     int           `json:"max_batch_size"`     // 最大批次大小
}

// BatchCommitRequest 批量提交请求
type BatchCommitRequest struct {
	Requests  []*CommitRequest // 提交请求列表
	StartTime time.Time        // 批次开始时间
}

// NewBatchCommitRequest 创建新的批量提交请求
func NewBatchCommitRequest() *BatchCommitRequest {
	return &BatchCommitRequest{
		Requests:  make([]*CommitRequest, 0),
		StartTime: time.Now(),
	}
}

// Add 添加提交请求
func (bcr *BatchCommitRequest) Add(req *CommitRequest) {
	bcr.Requests = append(bcr.Requests, req)
}

// Size 返回批次大小
func (bcr *BatchCommitRequest) Size() int {
	return len(bcr.Requests)
}

// MaxLSN 返回批次中最大的LSN
func (bcr *BatchCommitRequest) MaxLSN() uint64 {
	var maxLSN uint64
	for _, req := range bcr.Requests {
		if req.LSN > maxLSN {
			maxLSN = req.LSN
		}
	}
	return maxLSN
}

// NotifyAll 通知所有请求完成
func (bcr *BatchCommitRequest) NotifyAll(err error) {
	for _, req := range bcr.Requests {
		if req.Callback != nil {
			req.Callback(err)
		}
		select {
		case req.Done <- err:
		default:
		}
	}
}

// Elapsed 返回批次持续时间
func (bcr *BatchCommitRequest) Elapsed() time.Duration {
	return time.Since(bcr.StartTime)
}
