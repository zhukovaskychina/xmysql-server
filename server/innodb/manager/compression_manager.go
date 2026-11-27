package manager

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// CompressionManager 管理页面压缩
type CompressionManager struct {
	mu sync.RWMutex

	// 压缩设置映射: space_id -> compression_settings
	spaceSettings map[uint32]*CompressionSettings

	// 压缩统计
	stats                     CompressionStats
	uncompressedCurrent       uint64
	spaceStats                map[uint32]CompressionStats
	perIndexEnabled           bool
	metricStats               CompressionStats
	metricCompressedEnabled   bool
	metricDecompressedEnabled bool
	metricRuntime             map[string]*CompressionMetricRuntime
	pendingDecompress         uint64
	// decompressStartedHook is test-only fault injection used to hold a real
	// decompression operation open while the live pending count is inspected.
	decompressStartedHook func()

	// 压缩缓冲池
	bufferPool sync.Pool
}

// CompressionSettings 表示压缩设置
type CompressionSettings struct {
	SpaceID     uint32  // 表空间ID
	Method      uint8   // 压缩方法
	Level       uint8   // 压缩级别
	BlockSize   uint32  // 压缩块大小
	MinSavings  float64 // 最小压缩率
	MaxFailures uint32  // 最大失败次数
}

// CompressionStats 表示压缩统计信息
type CompressionStats struct {
	TotalPages         uint64        // 总页面数
	CompressedPages    uint64        // 压缩页面数
	UncompressedPages  uint64        // 成功解压页面数
	TotalSize          uint64        // 总大小
	CompressedSize     uint64        // 压缩后大小
	FailureCount       uint64        // 压缩失败次数
	AvgSavings         float64       // 平均压缩率
	CompressDuration   time.Duration // 累计压缩耗时
	UncompressDuration time.Duration // 累计解压耗时
}

// CompressionMetricRuntime contains the lifecycle state exposed through the
// official INNODB_METRICS compression rows. TimeElapsed is expressed in
// seconds, matching MySQL's INNODB_METRICS TIME_ELAPSED column.
type CompressionMetricRuntime struct {
	Count         uint64
	MaxCount      uint64
	MinCount      uint64
	AvgCount      float64
	CountReset    uint64
	MaxCountReset uint64
	MinCountReset uint64
	AvgCountReset float64
	HasCount      bool
	HasCountReset bool
	Enabled       bool
	TimeEnabled   time.Time
	TimeDisabled  time.Time
	TimeElapsed   int64
	TimeReset     time.Time
	enabledSince  time.Time
	elapsed       time.Duration
}

// 压缩方法常量
const (
	COMPRESSION_NONE uint8 = iota // 不压缩
	COMPRESSION_ZLIB              // zlib压缩
)

// 压缩级别常量
const (
	COMPRESSION_LEVEL_NONE    uint8 = 0 // 不压缩
	COMPRESSION_LEVEL_FASTEST uint8 = 1 // 最快压缩
	COMPRESSION_LEVEL_DEFAULT uint8 = 6 // 默认压缩
	COMPRESSION_LEVEL_BEST    uint8 = 9 // 最佳压缩
)

// 页面头部魔数
var compressedPageMagic = []byte{0xC0, 0x4D, 0x50, 0x52} // "CMPR"

// NewCompressionManager 创建压缩管理器
func NewCompressionManager() *CompressionManager {
	return &CompressionManager{
		spaceSettings: make(map[uint32]*CompressionSettings),
		spaceStats:    make(map[uint32]CompressionStats),
		metricRuntime: map[string]*CompressionMetricRuntime{
			"compress_pages_compressed":   &CompressionMetricRuntime{},
			"compress_pages_decompressed": &CompressionMetricRuntime{},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// SetCompressionSettings 设置表空间的压缩设置
func (cm *CompressionManager) SetCompressionSettings(spaceID uint32, settings *CompressionSettings) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.spaceSettings[spaceID] = settings
}

// GetCompressionSettings 获取表空间的压缩设置
func (cm *CompressionManager) GetCompressionSettings(spaceID uint32) *CompressionSettings {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.spaceSettings[spaceID]
}

// CompressPage 压缩页面内容
func (cm *CompressionManager) CompressPage(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	settings := cm.GetCompressionSettings(spaceID)
	if settings == nil || settings.Method == COMPRESSION_NONE {
		return data, nil
	}

	// 获取缓冲区
	buf := cm.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer cm.bufferPool.Put(buf)

	// 写入魔数
	buf.Write(compressedPageMagic)

	// 写入原始大小
	binary.Write(buf, binary.BigEndian, uint32(len(data)))

	var compressed []byte
	var err error
	compressionStarted := time.Now()

	switch settings.Method {
	case COMPRESSION_ZLIB:
		compressed, err = cm.compressZlib(data, settings.Level)
	default:
		return nil, errors.New("unsupported compression method")
	}
	cm.recordCompressionDuration(spaceID, time.Since(compressionStarted))

	if err != nil {
		cm.recordFailure(spaceID)
		return nil, err
	}

	// 检查压缩效果
	savings := 1 - float64(len(compressed))/float64(len(data))
	if savings < settings.MinSavings {
		return data, nil
	}

	// 写入压缩数据
	buf.Write(compressed)

	// 更新统计信息
	cm.updateStats(spaceID, len(data), len(compressed))

	// The buffer is returned to the pool by the deferred cleanup above. Copy
	// the serialized page before returning so a later compression cannot
	// overwrite bytes still owned by the caller.
	result := append([]byte(nil), buf.Bytes()...)
	return result, nil
}

// DecompressPage 解压页面内容
func (cm *CompressionManager) DecompressPage(spaceID uint32, pageNo uint32, data []byte) ([]byte, error) {
	settings := cm.GetCompressionSettings(spaceID)
	if settings == nil || settings.Method == COMPRESSION_NONE {
		return data, nil
	}

	// 检查魔数。A matching magic value is not enough to read the original
	// size; the complete header must be present before slicing it.
	if len(data) < len(compressedPageMagic) || !bytes.Equal(data[:len(compressedPageMagic)], compressedPageMagic) {
		return data, nil // 未压缩的页面
	}
	if len(data) < len(compressedPageMagic)+4 {
		return nil, errors.New("truncated compressed page header")
	}

	atomic.AddUint64(&cm.pendingDecompress, 1)
	defer atomic.AddUint64(&cm.pendingDecompress, ^uint64(0))
	if cm.decompressStartedHook != nil {
		cm.decompressStartedHook()
	}

	// 读取原始大小
	originalSize := binary.BigEndian.Uint32(data[len(compressedPageMagic) : len(compressedPageMagic)+4])
	compressedData := data[len(compressedPageMagic)+4:]

	var result []byte
	uncompressionStarted := time.Now()
	switch settings.Method {
	case COMPRESSION_ZLIB:
		var err error
		result, err = cm.decompressZlib(compressedData, int(originalSize))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported compression method")
	}
	cm.recordUncompress(spaceID, time.Since(uncompressionStarted))
	return result, nil
}

// GetPendingDecompress returns the number of compressed-page decompressions
// currently in flight. The count is independent from completed decompression
// totals and is suitable for the live PENDING_DECOMPRESS projection.
func (cm *CompressionManager) GetPendingDecompress() uint64 {
	if cm == nil {
		return 0
	}
	return atomic.LoadUint64(&cm.pendingDecompress)
}

// SetDecompressStartedHookForTest installs the test-only decompression gate
// used by engine compatibility tests. Production callers should leave it nil.
func (cm *CompressionManager) SetDecompressStartedHookForTest(hook func()) {
	if cm == nil {
		return
	}
	cm.decompressStartedHook = hook
}

// compressZlib 使用zlib压缩数据
func (cm *CompressionManager) compressZlib(data []byte, level uint8) ([]byte, error) {
	buf := cm.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer cm.bufferPool.Put(buf)

	writer, err := zlib.NewWriterLevel(buf, int(level))
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// decompressZlib 使用zlib解压数据
func (cm *CompressionManager) decompressZlib(data []byte, originalSize int) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	result := make([]byte, originalSize)
	if _, err := io.ReadFull(reader, result); err != nil {
		return nil, err
	}

	return result, nil
}

// recordFailure updates both the aggregate and per-space failure counters.
func (cm *CompressionManager) recordFailure(spaceID uint32) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.stats.FailureCount++
	if !cm.perIndexEnabled {
		return
	}
	space := cm.spaceStats[spaceID]
	space.FailureCount++
	cm.spaceStats[spaceID] = space
}

// recordUncompress records only successful decompression operations. A page
// with no compression header is returned as-is and is intentionally not
// counted because no decompression work occurred.
func (cm *CompressionManager) recordUncompress(spaceID uint32, elapsed time.Duration) {
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.stats.UncompressedPages++
	cm.stats.UncompressDuration += elapsed
	cm.uncompressedCurrent++
	if cm.metricDecompressedEnabled {
		cm.metricStats.UncompressedPages++
		cm.recordMetricEventLocked("compress_pages_decompressed")
	}
	if !cm.perIndexEnabled {
		return
	}
	space := cm.spaceStats[spaceID]
	space.UncompressedPages++
	space.UncompressDuration += elapsed
	cm.spaceStats[spaceID] = space
}

// recordCompressionDuration records time spent attempting compression. The
// operation count and success count remain separate so a failed or
// insufficiently-saving attempt still contributes to COMPRESS_TIME.
func (cm *CompressionManager) recordCompressionDuration(spaceID uint32, elapsed time.Duration) {
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.stats.CompressDuration += elapsed
	if !cm.perIndexEnabled {
		return
	}
	space := cm.spaceStats[spaceID]
	space.CompressDuration += elapsed
	cm.spaceStats[spaceID] = space
}

// updateStats 更新压缩统计信息
func (cm *CompressionManager) updateStats(spaceID uint32, originalSize, compressedSize int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	update := func(stats *CompressionStats) {
		stats.TotalPages++
		stats.CompressedPages++
		stats.TotalSize += uint64(originalSize)
		stats.CompressedSize += uint64(compressedSize)
		if stats.TotalSize != 0 {
			stats.AvgSavings = 1 - float64(stats.CompressedSize)/float64(stats.TotalSize)
		}
	}
	update(&cm.stats)
	if cm.metricCompressedEnabled {
		cm.metricStats.CompressedPages++
		cm.recordMetricEventLocked("compress_pages_compressed")
	}
	if !cm.perIndexEnabled {
		return
	}
	space := cm.spaceStats[spaceID]
	update(&space)
	cm.spaceStats[spaceID] = space
}

// GetStats 获取压缩统计信息
func (cm *CompressionManager) GetStats() CompressionStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.stats
}

// GetStatsAndReset returns the aggregate compression counters and atomically
// clears them. The reset views in INFORMATION_SCHEMA use this snapshot form
// so a concurrent compressor cannot be lost between reading and clearing.
func (cm *CompressionManager) GetStatsAndReset() CompressionStats {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	stats := cm.stats
	cm.stats = CompressionStats{}
	cm.uncompressedCurrent = 0
	return stats
}

// GetUncompressedCurrentAndReset returns successful decompressions observed
// since the previous buffer-pool statistics snapshot. The current-window
// value is independent from the cumulative CompressionStats counters so
// reading the INFORMATION_SCHEMA view does not erase UNCOMPRESS_TOTAL.
func (cm *CompressionManager) GetUncompressedCurrentAndReset() uint64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	current := cm.uncompressedCurrent
	cm.uncompressedCurrent = 0
	return current
}

// GetStatsForSpace returns the source-backed compression counters for one
// InnoDB tablespace. A zero-value result means that no successful compression
// operation or failure has been observed for the space yet.
func (cm *CompressionManager) GetStatsForSpace(spaceID uint32) CompressionStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.spaceStats[spaceID]
}

// SetPerIndexEnabled controls collection of the tablespace statistics used by
// INNODB_CMP_PER_INDEX. Aggregate compression statistics remain enabled
// regardless of this switch. Changing the setting starts a fresh collection
// window, matching MySQL's requirement that collection be enabled before the
// operations being measured.
func (cm *CompressionManager) SetPerIndexEnabled(enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.perIndexEnabled == enabled {
		return
	}
	cm.perIndexEnabled = enabled
	cm.spaceStats = make(map[uint32]CompressionStats)
}

// IsPerIndexEnabled reports whether per-index compression collection is active.
func (cm *CompressionManager) IsPerIndexEnabled() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.perIndexEnabled
}

// SetCompressionMetricEnabled controls one official INNODB_METRICS compression
// counter. The aggregate INNODB_CMP counters are intentionally independent
// from this monitoring switch.
func (cm *CompressionManager) SetCompressionMetricEnabled(name string, enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.setCompressionMetricEnabledLocked(name, enabled)
}

// SetCompressionMetricsEnabled enables or disables both compression metrics.
func (cm *CompressionManager) SetCompressionMetricsEnabled(enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.setCompressionMetricEnabledLocked("compress_pages_compressed", enabled)
	cm.setCompressionMetricEnabledLocked("compress_pages_decompressed", enabled)
}

func (cm *CompressionManager) setCompressionMetricEnabledLocked(name string, enabled bool) {
	runtime, ok := cm.metricRuntime[name]
	if !ok {
		return
	}
	if runtime.Enabled == enabled {
		return
	}
	now := time.Now()
	if runtime.Enabled && !runtime.enabledSince.IsZero() {
		runtime.elapsed += now.Sub(runtime.enabledSince)
		runtime.enabledSince = time.Time{}
	}
	runtime.Enabled = enabled
	if enabled {
		runtime.TimeEnabled = now
		runtime.enabledSince = now
	} else {
		runtime.TimeDisabled = now
	}
	switch name {
	case "compress_pages_compressed":
		cm.metricCompressedEnabled = enabled
	case "compress_pages_decompressed":
		cm.metricDecompressedEnabled = enabled
	}
}

func (cm *CompressionManager) recordMetricEventLocked(name string) {
	runtime, ok := cm.metricRuntime[name]
	if !ok || !runtime.Enabled {
		return
	}
	runtime.Count++
	runtime.CountReset++
	runtime.HasCount = true
	runtime.HasCountReset = true
	runtime.MaxCount = runtime.Count
	runtime.MinCount = runtime.Count
	runtime.AvgCount = float64(runtime.Count)
	runtime.MaxCountReset = runtime.CountReset
	runtime.MinCountReset = runtime.CountReset
	runtime.AvgCountReset = float64(runtime.CountReset)
}

// IsCompressionMetricEnabled reports the current monitor state for one
// official compression metric.
func (cm *CompressionManager) IsCompressionMetricEnabled(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	switch name {
	case "compress_pages_compressed":
		return cm.metricCompressedEnabled
	case "compress_pages_decompressed":
		return cm.metricDecompressedEnabled
	default:
		return false
	}
}

// GetCompressionMetricStats returns the source-backed counters collected by
// the enabled INNODB_METRICS compression counters.
func (cm *CompressionManager) GetCompressionMetricStats() CompressionStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.metricStats
}

// GetCompressionMetricRuntime returns a consistent snapshot of one official
// compression metric, including its lifecycle timestamps and reset window.
func (cm *CompressionManager) GetCompressionMetricRuntime(name string) CompressionMetricRuntime {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	runtime := cm.metricRuntime[name]
	if runtime == nil {
		return CompressionMetricRuntime{}
	}
	snapshot := *runtime
	if snapshot.Enabled && !snapshot.enabledSince.IsZero() {
		snapshot.elapsed += time.Since(snapshot.enabledSince)
	}
	snapshot.TimeElapsed = int64(snapshot.elapsed / time.Second)
	snapshot.enabledSince = time.Time{}
	snapshot.elapsed = 0
	return snapshot
}

// ResetCompressionMetric clears one or both official compression counters.
func (cm *CompressionManager) ResetCompressionMetric(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	now := time.Now()
	resetRuntime := func(metricName string) {
		if runtime := cm.metricRuntime[metricName]; runtime != nil {
			runtime.CountReset = 0
			runtime.MaxCountReset = 0
			runtime.MinCountReset = 0
			runtime.AvgCountReset = 0
			runtime.HasCountReset = false
			runtime.TimeReset = now
		}
	}
	switch name {
	case "compress_pages_compressed":
		resetRuntime("compress_pages_compressed")
	case "compress_pages_decompressed":
		resetRuntime("compress_pages_decompressed")
	case "all", "compression":
		resetRuntime("compress_pages_compressed")
		resetRuntime("compress_pages_decompressed")
	}
}

// GetStatsForSpaceAndReset returns and clears one tablespace's compression
// counters atomically. Other tablespaces remain untouched.
func (cm *CompressionManager) GetStatsForSpaceAndReset(spaceID uint32) CompressionStats {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	stats := cm.spaceStats[spaceID]
	cm.spaceStats[spaceID] = CompressionStats{}
	return stats
}

// Close 关闭压缩管理器
func (cm *CompressionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 清理资源
	cm.spaceSettings = nil
	cm.spaceStats = nil
	return nil
}
