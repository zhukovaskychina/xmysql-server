package manager

import (
	"sync"
	"time"
)

// ============ LOG-008.3: 检查点性能监控和统计 ============

// CheckpointMonitor 检查点性能监控器
type CheckpointMonitor struct {
	mu sync.RWMutex

	// 检查点管理器
	fuzzyCheckpoint *FuzzyCheckpoint
	logArchiver     *LogArchiver

	// 性能指标
	metrics *CheckpointMetrics

	// 历史记录
	history    []*CheckpointRecord // 最近N次检查点记录
	maxHistory int                 // 最大历史记录数

	// 告警配置
	alertThresholds *AlertThresholds
	alertCallbacks  []AlertCallback

	// 统计周期
	statsInterval time.Duration
	lastStatsTime time.Time
}

// CheckpointMetrics 检查点性能指标
type CheckpointMetrics struct {
	// 基本指标
	TotalCheckpoints      uint64 `json:"total_checkpoints"`
	SuccessfulCheckpoints uint64 `json:"successful_checkpoints"`
	FailedCheckpoints     uint64 `json:"failed_checkpoints"`

	// 时间指标
	AvgCheckpointTime  time.Duration `json:"avg_checkpoint_time"`
	MinCheckpointTime  time.Duration `json:"min_checkpoint_time"`
	MaxCheckpointTime  time.Duration `json:"max_checkpoint_time"`
	LastCheckpointTime time.Time     `json:"last_checkpoint_time"`

	// 数据指标
	AvgDirtyPages float64 `json:"avg_dirty_pages"`
	MaxDirtyPages int     `json:"max_dirty_pages"`
	AvgActiveTxns float64 `json:"avg_active_txns"`
	MaxActiveTxns int     `json:"max_active_txns"`

	// LSN指标
	CurrentLSN     uint64 `json:"current_lsn"`
	CheckpointLSN  uint64 `json:"checkpoint_lsn"`
	MinRecoveryLSN uint64 `json:"min_recovery_lsn"`
	LSNGap         uint64 `json:"lsn_gap"` // CheckpointLSN和MinRecoveryLSN的差距

	// 归档指标
	TotalArchivedFiles uint64 `json:"total_archived_files"`
	TotalArchiveSize   int64  `json:"total_archive_size"`
	ArchivedFilesCount int    `json:"archived_files_count"`
}

// CheckpointRecord 检查点记录
type CheckpointRecord struct {
	Timestamp      time.Time     `json:"timestamp"`
	Duration       time.Duration `json:"duration"`
	CheckpointLSN  uint64        `json:"checkpoint_lsn"`
	MinRecoveryLSN uint64        `json:"min_recovery_lsn"`
	DirtyPages     int           `json:"dirty_pages"`
	ActiveTxns     int           `json:"active_txns"`
	Success        bool          `json:"success"`
	Error          string        `json:"error,omitempty"`
}

// AlertThresholds 告警阈值
type AlertThresholds struct {
	MaxCheckpointTime time.Duration // 最大检查点时间
	MaxDirtyPages     int           // 最大脏页数
	MaxActiveTxns     int           // 最大活跃事务数
	MaxLSNGap         uint64        // 最大LSN差距
}

// AlertCallback 告警回调函数
type AlertCallback func(alert *Alert)

// Alert 告警信息
type Alert struct {
	Level     string      `json:"level"`     // 告警级别: INFO, WARNING, ERROR
	Type      string      `json:"type"`      // 告警类型
	Message   string      `json:"message"`   // 告警消息
	Timestamp time.Time   `json:"timestamp"` // 告警时间
	Value     interface{} `json:"value"`     // 相关值
	Threshold interface{} `json:"threshold"` // 阈值
}

// NewCheckpointMonitor 创建检查点监控器
func NewCheckpointMonitor(fuzzyCheckpoint *FuzzyCheckpoint, logArchiver *LogArchiver) *CheckpointMonitor {
	return &CheckpointMonitor{
		fuzzyCheckpoint: fuzzyCheckpoint,
		logArchiver:     logArchiver,
		metrics:         &CheckpointMetrics{},
		history:         make([]*CheckpointRecord, 0, 100),
		maxHistory:      100,
		alertThresholds: &AlertThresholds{
			MaxCheckpointTime: 5 * time.Second,
			MaxDirtyPages:     10000,
			MaxActiveTxns:     1000,
			MaxLSNGap:         1000000,
		},
		alertCallbacks: make([]AlertCallback, 0),
		statsInterval:  1 * time.Minute,
		lastStatsTime:  time.Now(),
	}
}

// RecordCheckpoint 记录检查点执行情况
func (cm *CheckpointMonitor) RecordCheckpoint(record *CheckpointRecord) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 添加到历史记录
	cm.history = append(cm.history, record)
	if len(cm.history) > cm.maxHistory {
		cm.history = cm.history[1:]
	}

	// 更新指标
	cm.metrics.TotalCheckpoints++
	if record.Success {
		cm.metrics.SuccessfulCheckpoints++
	} else {
		cm.metrics.FailedCheckpoints++
	}

	// 更新时间指标
	if cm.metrics.MinCheckpointTime == 0 || record.Duration < cm.metrics.MinCheckpointTime {
		cm.metrics.MinCheckpointTime = record.Duration
	}
	if record.Duration > cm.metrics.MaxCheckpointTime {
		cm.metrics.MaxCheckpointTime = record.Duration
	}

	// 计算平均检查点时间
	totalDuration := cm.metrics.AvgCheckpointTime * time.Duration(cm.metrics.TotalCheckpoints-1)
	cm.metrics.AvgCheckpointTime = (totalDuration + record.Duration) / time.Duration(cm.metrics.TotalCheckpoints)

	// 更新脏页指标
	if record.DirtyPages > cm.metrics.MaxDirtyPages {
		cm.metrics.MaxDirtyPages = record.DirtyPages
	}
	totalDirtyPages := cm.metrics.AvgDirtyPages * float64(cm.metrics.TotalCheckpoints-1)
	cm.metrics.AvgDirtyPages = (totalDirtyPages + float64(record.DirtyPages)) / float64(cm.metrics.TotalCheckpoints)

	// 更新活跃事务指标
	if record.ActiveTxns > cm.metrics.MaxActiveTxns {
		cm.metrics.MaxActiveTxns = record.ActiveTxns
	}
	totalActiveTxns := cm.metrics.AvgActiveTxns * float64(cm.metrics.TotalCheckpoints-1)
	cm.metrics.AvgActiveTxns = (totalActiveTxns + float64(record.ActiveTxns)) / float64(cm.metrics.TotalCheckpoints)

	// 更新LSN指标
	cm.metrics.CheckpointLSN = record.CheckpointLSN
	cm.metrics.MinRecoveryLSN = record.MinRecoveryLSN
	cm.metrics.LSNGap = record.CheckpointLSN - record.MinRecoveryLSN

	cm.metrics.LastCheckpointTime = record.Timestamp

	// 检查告警
	cm.checkAlerts(record)
}

// checkAlerts 检查告警条件
func (cm *CheckpointMonitor) checkAlerts(record *CheckpointRecord) {
	// 检查检查点时间
	if record.Duration > cm.alertThresholds.MaxCheckpointTime {
		cm.triggerAlert(&Alert{
			Level:     "WARNING",
			Type:      "SLOW_CHECKPOINT",
			Message:   "Checkpoint duration exceeds threshold",
			Timestamp: time.Now(),
			Value:     record.Duration,
			Threshold: cm.alertThresholds.MaxCheckpointTime,
		})
	}

	// 检查脏页数量
	if record.DirtyPages > cm.alertThresholds.MaxDirtyPages {
		cm.triggerAlert(&Alert{
			Level:     "WARNING",
			Type:      "TOO_MANY_DIRTY_PAGES",
			Message:   "Dirty pages count exceeds threshold",
			Timestamp: time.Now(),
			Value:     record.DirtyPages,
			Threshold: cm.alertThresholds.MaxDirtyPages,
		})
	}

	// 检查活跃事务数量
	if record.ActiveTxns > cm.alertThresholds.MaxActiveTxns {
		cm.triggerAlert(&Alert{
			Level:     "WARNING",
			Type:      "TOO_MANY_ACTIVE_TXNS",
			Message:   "Active transactions count exceeds threshold",
			Timestamp: time.Now(),
			Value:     record.ActiveTxns,
			Threshold: cm.alertThresholds.MaxActiveTxns,
		})
	}

	// 检查LSN差距
	lsnGap := record.CheckpointLSN - record.MinRecoveryLSN
	if lsnGap > cm.alertThresholds.MaxLSNGap {
		cm.triggerAlert(&Alert{
			Level:     "WARNING",
			Type:      "LARGE_LSN_GAP",
			Message:   "LSN gap is too large",
			Timestamp: time.Now(),
			Value:     lsnGap,
			Threshold: cm.alertThresholds.MaxLSNGap,
		})
	}

	// 检查失败
	if !record.Success {
		cm.triggerAlert(&Alert{
			Level:     "ERROR",
			Type:      "CHECKPOINT_FAILED",
			Message:   "Checkpoint execution failed: " + record.Error,
			Timestamp: time.Now(),
		})
	}
}

// triggerAlert 触发告警
func (cm *CheckpointMonitor) triggerAlert(alert *Alert) {
	for _, callback := range cm.alertCallbacks {
		go callback(alert)
	}
}

// RegisterAlertCallback 注册告警回调
func (cm *CheckpointMonitor) RegisterAlertCallback(callback AlertCallback) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.alertCallbacks = append(cm.alertCallbacks, callback)
}

// UpdateMetrics 更新所有指标
func (cm *CheckpointMonitor) UpdateMetrics() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 更新检查点指标
	if cm.fuzzyCheckpoint != nil {
		stats := cm.fuzzyCheckpoint.GetStats()
		cm.metrics.CheckpointLSN = cm.fuzzyCheckpoint.GetCheckpointLSN()
		cm.metrics.MinRecoveryLSN = cm.fuzzyCheckpoint.GetMinRecoveryLSN()
		cm.metrics.LSNGap = cm.metrics.CheckpointLSN - cm.metrics.MinRecoveryLSN
		cm.metrics.LastCheckpointTime = stats.LastCheckpoint
	}

	// 更新归档指标
	if cm.logArchiver != nil {
		archiveStats := cm.logArchiver.GetStats()
		cm.metrics.TotalArchivedFiles = archiveStats.TotalArchived
		cm.metrics.TotalArchiveSize = archiveStats.TotalArchiveSize
		cm.metrics.ArchivedFilesCount = archiveStats.ArchivedFiles
	}

	cm.lastStatsTime = time.Now()
}

// GetMetrics 获取性能指标
func (cm *CheckpointMonitor) GetMetrics() *CheckpointMetrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metrics := *cm.metrics
	return &metrics
}

// GetHistory 获取历史记录
func (cm *CheckpointMonitor) GetHistory(limit int) []*CheckpointRecord {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if limit <= 0 || limit > len(cm.history) {
		limit = len(cm.history)
	}

	start := len(cm.history) - limit
	history := make([]*CheckpointRecord, limit)
	copy(history, cm.history[start:])

	return history
}

// GetSummary 获取性能摘要
func (cm *CheckpointMonitor) GetSummary() *PerformanceSummary {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	summary := &PerformanceSummary{
		TotalCheckpoints: cm.metrics.TotalCheckpoints,
		SuccessRate:      0,
		AvgDuration:      cm.metrics.AvgCheckpointTime,
		AvgDirtyPages:    cm.metrics.AvgDirtyPages,
		AvgActiveTxns:    cm.metrics.AvgActiveTxns,
		CurrentLSNGap:    cm.metrics.LSNGap,
		RecentRecords:    len(cm.history),
	}

	if cm.metrics.TotalCheckpoints > 0 {
		summary.SuccessRate = float64(cm.metrics.SuccessfulCheckpoints) / float64(cm.metrics.TotalCheckpoints)
	}

	return summary
}

// PerformanceSummary 性能摘要
type PerformanceSummary struct {
	TotalCheckpoints uint64        `json:"total_checkpoints"`
	SuccessRate      float64       `json:"success_rate"`
	AvgDuration      time.Duration `json:"avg_duration"`
	AvgDirtyPages    float64       `json:"avg_dirty_pages"`
	AvgActiveTxns    float64       `json:"avg_active_txns"`
	CurrentLSNGap    uint64        `json:"current_lsn_gap"`
	RecentRecords    int           `json:"recent_records"`
}

// SetAlertThresholds 设置告警阈值
func (cm *CheckpointMonitor) SetAlertThresholds(thresholds *AlertThresholds) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.alertThresholds = thresholds
}

// GetAlertThresholds 获取告警阈值
func (cm *CheckpointMonitor) GetAlertThresholds() *AlertThresholds {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	thresholds := *cm.alertThresholds
	return &thresholds
}
