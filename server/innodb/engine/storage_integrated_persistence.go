package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// PersistenceManager 页面持久化管理器
type PersistenceManager struct {
	// 核心组件
	bufferPoolManager *manager.OptimizedBufferPoolManager
	storageManager    *manager.StorageManager

	// 持久化配置
	dataDir            string
	walDir             string
	syncMode           PersistenceSyncMode
	flushInterval      time.Duration
	checkpointInterval time.Duration

	// WAL (Write-Ahead Logging) 组件
	walWriter *WALWriter
	walReader *WALReader

	// 检查点管理
	checkpointManager *CheckpointManager

	// 持久化状态
	isRunning          bool
	lastFlushTime      time.Time
	lastCheckpointTime time.Time

	// 同步控制
	flushMutex       sync.RWMutex
	shutdownChan     chan struct{}
	flushTicker      *time.Ticker
	checkpointTicker *time.Ticker

	// 统计信息
	stats *PersistenceStats
}

// PersistenceSyncMode 持久化同步模式
type PersistenceSyncMode int

const (
	SyncModeImmediate   PersistenceSyncMode = iota // 立即同步
	SyncModeGroupCommit                            // 组提交
	SyncModeAsync                                  // 异步刷新
)

// PersistenceStats 持久化统计信息
type PersistenceStats struct {
	TotalFlushes       uint64
	TotalWALWrites     uint64
	TotalCheckpoints   uint64
	FlushLatency       time.Duration
	WALWriteLatency    time.Duration
	CheckpointLatency  time.Duration
	DirtyPagesCount    uint64
	FlushedPagesCount  uint64
	WALSize            uint64
	LastFlushTime      time.Time
	LastCheckpointTime time.Time
}

// NewPersistenceManager 创建持久化管理器
func NewPersistenceManager(
	bufferPoolManager *manager.OptimizedBufferPoolManager,
	storageManager *manager.StorageManager,
	dataDir string,
) *PersistenceManager {
	walDir := filepath.Join(dataDir, "wal")

	// 确保目录存在
	os.MkdirAll(dataDir, 0755)
	os.MkdirAll(walDir, 0755)

	pm := &PersistenceManager{
		bufferPoolManager:  bufferPoolManager,
		storageManager:     storageManager,
		dataDir:            dataDir,
		walDir:             walDir,
		syncMode:           SyncModeImmediate, // 默认立即同步
		flushInterval:      time.Second * 5,   // 5秒刷新间隔
		checkpointInterval: time.Minute * 1,   // 1分钟检查点间隔
		isRunning:          false,
		shutdownChan:       make(chan struct{}),
		stats: &PersistenceStats{
			TotalFlushes:      0,
			TotalWALWrites:    0,
			TotalCheckpoints:  0,
			FlushLatency:      0,
			WALWriteLatency:   0,
			CheckpointLatency: 0,
			DirtyPagesCount:   0,
			FlushedPagesCount: 0,
			WALSize:           0,
		},
	}

	// 初始化WAL组件
	pm.walWriter = NewWALWriter(walDir)
	pm.walReader = NewWALReader(walDir)

	// 初始化检查点管理器
	pm.checkpointManager = NewCheckpointManager(dataDir, pm.bufferPoolManager)

	return pm
}

// Start 启动持久化管理器
func (pm *PersistenceManager) Start(ctx context.Context) error {
	pm.flushMutex.Lock()
	defer pm.flushMutex.Unlock()

	if pm.isRunning {
		return fmt.Errorf("持久化管理器已经在运行")
	}

	logger.Infof("🚀 启动页面持久化管理器")

	// 启动WAL写入器
	if err := pm.walWriter.Start(); err != nil {
		return fmt.Errorf("启动WAL写入器失败: %v", err)
	}

	// 启动检查点管理器
	if err := pm.checkpointManager.Start(ctx); err != nil {
		return fmt.Errorf("启动检查点管理器失败: %v", err)
	}

	// 启动定时刷新
	pm.flushTicker = time.NewTicker(pm.flushInterval)
	pm.checkpointTicker = time.NewTicker(pm.checkpointInterval)

	pm.isRunning = true

	// 启动后台刷新协程
	go pm.backgroundFlushRoutine()
	go pm.backgroundCheckpointRoutine()

	logger.Infof(" 页面持久化管理器启动成功")
	return nil
}

// Stop 停止持久化管理器
func (pm *PersistenceManager) Stop() error {
	pm.flushMutex.Lock()
	defer pm.flushMutex.Unlock()

	if !pm.isRunning {
		return nil
	}

	logger.Infof("🛑 停止页面持久化管理器")

	// 发送停止信号
	close(pm.shutdownChan)

	// 停止定时器
	if pm.flushTicker != nil {
		pm.flushTicker.Stop()
	}
	if pm.checkpointTicker != nil {
		pm.checkpointTicker.Stop()
	}

	// 执行最后一次完整刷新
	if err := pm.FlushAllDirtyPages(context.Background()); err != nil {
		logger.Errorf(" 最终刷新失败: %v", err)
	}

	// 执行最后一次检查点
	if err := pm.CreateCheckpoint(context.Background()); err != nil {
		logger.Errorf(" 最终检查点失败: %v", err)
	}

	// 停止WAL写入器
	if err := pm.walWriter.Stop(); err != nil {
		logger.Errorf(" 停止WAL写入器失败: %v", err)
	}

	// 停止检查点管理器
	if err := pm.checkpointManager.Stop(); err != nil {
		logger.Errorf(" 停止检查点管理器失败: %v", err)
	}

	pm.isRunning = false

	logger.Infof(" 页面持久化管理器停止成功")
	return nil
}

// FlushPage 刷新单个页面到磁盘
func (pm *PersistenceManager) FlushPage(ctx context.Context, spaceID uint32, pageNo uint32) error {
	startTime := time.Now()

	logger.Debugf("💾 刷新页面到磁盘: SpaceID=%d, PageNo=%d", spaceID, pageNo)

	// 1. 从缓冲池获取页面
	page, err := pm.bufferPoolManager.GetPage(spaceID, pageNo)
	if err != nil {
		return fmt.Errorf("获取页面失败: %v", err)
	}

	// 2. 检查页面是否为脏页
	if !page.IsDirty() {
		logger.Debugf("📄 页面不是脏页，跳过刷新: SpaceID=%d, PageNo=%d", spaceID, pageNo)
		return nil
	}

	// 3. 写入WAL日志（Write-Ahead Logging）
	walEntry := &WALEntry{
		LSN:       pm.generateLSN(),
		SpaceID:   spaceID,
		PageNo:    pageNo,
		Operation: WALOpPageFlush,
		Data:      page.GetContent(),
		Timestamp: time.Now(),
	}

	if err := pm.walWriter.WriteEntry(walEntry); err != nil {
		return fmt.Errorf("写入WAL失败: %v", err)
	}

	// 4. 构建页面文件路径
	pageFilePath := pm.getPageFilePath(spaceID, pageNo)

	// 5. 确保目录存在
	if err := os.MkdirAll(filepath.Dir(pageFilePath), 0755); err != nil {
		return fmt.Errorf("创建页面目录失败: %v", err)
	}

	// 6. 写入页面数据到磁盘
	if err := pm.writePageToDisk(pageFilePath, page.GetContent()); err != nil {
		return fmt.Errorf("写入页面到磁盘失败: %v", err)
	}

	// 7. 根据同步模式决定是否立即同步
	if pm.syncMode == SyncModeImmediate {
		if err := pm.syncPageFile(pageFilePath); err != nil {
			return fmt.Errorf("同步页面文件失败: %v", err)
		}
	}

	// 8. 标记页面为干净
	page.ClearDirty()

	// 9. 更新统计信息
	flushLatency := time.Since(startTime)
	pm.updateFlushStats(flushLatency)

	logger.Debugf(" 页面刷新完成: SpaceID=%d, PageNo=%d, 耗时=%v", spaceID, pageNo, flushLatency)
	return nil
}

// FlushAllDirtyPages 刷新所有脏页
func (pm *PersistenceManager) FlushAllDirtyPages(ctx context.Context) error {
	startTime := time.Now()

	logger.Infof("💾 开始刷新所有脏页")

	// 获取所有脏页 - 使用正确的方法名
	dirtyPages := pm.bufferPoolManager.GetDirtyPages()
	if len(dirtyPages) == 0 {
		logger.Infof("📄 没有脏页需要刷新")
		return nil
	}

	logger.Infof(" 发现 %d 个脏页需要刷新", len(dirtyPages))

	flushedCount := 0
	errorCount := 0

	// 批量刷新脏页
	for _, page := range dirtyPages {
		select {
		case <-ctx.Done():
			logger.Infof("  刷新被取消，已刷新 %d 个页面", flushedCount)
			return ctx.Err()
		default:
			if err := pm.FlushPage(ctx, page.GetSpaceID(), page.GetPageNo()); err != nil {
				logger.Errorf(" 刷新页面失败: SpaceID=%d, PageNo=%d, Error=%v",
					page.GetSpaceID(), page.GetPageNo(), err)
				errorCount++
			} else {
				flushedCount++
			}
		}
	}

	totalTime := time.Since(startTime)

	if errorCount > 0 {
		logger.Errorf("  刷新完成，成功: %d, 失败: %d, 总耗时: %v",
			flushedCount, errorCount, totalTime)
		return fmt.Errorf("刷新过程中有 %d 个页面失败", errorCount)
	}

	logger.Infof(" 所有脏页刷新完成，共刷新 %d 个页面，耗时: %v", flushedCount, totalTime)
	return nil
}

// CreateCheckpoint 创建检查点
func (pm *PersistenceManager) CreateCheckpoint(ctx context.Context) error {
	startTime := time.Now()

	logger.Infof("🔄 开始创建检查点")

	// 1. 刷新所有脏页
	if err := pm.FlushAllDirtyPages(ctx); err != nil {
		return fmt.Errorf("刷新脏页失败: %v", err)
	}

	// 2. 创建检查点记录
	checkpoint := &CheckpointRecord{
		LSN:             pm.generateLSN(),
		Timestamp:       time.Now(),
		FlushedPages:    pm.stats.FlushedPagesCount,
		WALSize:         pm.stats.WALSize,
		BufferPoolStats: pm.bufferPoolManager.GetStats(),
	}

	// 3. 写入检查点
	if err := pm.checkpointManager.WriteCheckpoint(checkpoint); err != nil {
		return fmt.Errorf("写入检查点失败: %v", err)
	}

	// 4. 截断WAL日志（可选）
	if err := pm.walWriter.Truncate(checkpoint.LSN); err != nil {
		logger.Errorf("  截断WAL失败: %v", err)
	}

	// 5. 更新统计信息
	checkpointLatency := time.Since(startTime)
	pm.updateCheckpointStats(checkpointLatency)
	pm.lastCheckpointTime = time.Now()

	logger.Infof(" 检查点创建完成，LSN=%d, 耗时=%v", checkpoint.LSN, checkpointLatency)
	return nil
}

// RecoverFromCheckpoint 从检查点恢复
func (pm *PersistenceManager) RecoverFromCheckpoint(ctx context.Context) error {
	logger.Infof("🔄 开始从检查点恢复")

	// 1. 读取最新检查点
	checkpoint, err := pm.checkpointManager.ReadLatestCheckpoint()
	if err != nil {
		logger.Infof("📄 没有找到检查点，执行完整恢复")
		return pm.recoverFromWAL(ctx, 0)
	}

	logger.Infof(" 找到检查点: LSN=%d, 时间=%v", checkpoint.LSN, checkpoint.Timestamp)

	// 2. 从检查点LSN开始恢复WAL
	if err := pm.recoverFromWAL(ctx, checkpoint.LSN); err != nil {
		return fmt.Errorf("从WAL恢复失败: %v", err)
	}

	logger.Infof(" 从检查点恢复完成")
	return nil
}

// 私有方法实现

// backgroundFlushRoutine 后台刷新协程
func (pm *PersistenceManager) backgroundFlushRoutine() {
	logger.Infof("🔄 启动后台页面刷新协程")

	for {
		select {
		case <-pm.shutdownChan:
			logger.Infof("🛑 后台刷新协程收到停止信号")
			return
		case <-pm.flushTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), pm.flushInterval)
			if err := pm.FlushAllDirtyPages(ctx); err != nil {
				logger.Errorf(" 后台刷新失败: %v", err)
			}
			cancel()
		}
	}
}

// backgroundCheckpointRoutine 后台检查点协程
func (pm *PersistenceManager) backgroundCheckpointRoutine() {
	logger.Infof("🔄 启动后台检查点协程")

	for {
		select {
		case <-pm.shutdownChan:
			logger.Infof("🛑 后台检查点协程收到停止信号")
			return
		case <-pm.checkpointTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), pm.checkpointInterval)
			if err := pm.CreateCheckpoint(ctx); err != nil {
				logger.Errorf(" 后台检查点失败: %v", err)
			}
			cancel()
		}
	}
}

// getPageFilePath 获取页面文件路径
func (pm *PersistenceManager) getPageFilePath(spaceID uint32, pageNo uint32) string {
	return filepath.Join(pm.dataDir, fmt.Sprintf("space_%d", spaceID), fmt.Sprintf("page_%d.dat", pageNo))
}

// writePageToDisk 写入页面数据到磁盘
func (pm *PersistenceManager) writePageToDisk(filePath string, data []byte) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("打开页面文件失败: %v", err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("写入页面数据失败: %v", err)
	}

	return nil
}

// syncPageFile 同步页面文件到磁盘
func (pm *PersistenceManager) syncPageFile(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("打开页面文件失败: %v", err)
	}
	defer file.Close()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("同步页面文件失败: %v", err)
	}

	return nil
}

// recoverFromWAL 从WAL恢复
func (pm *PersistenceManager) recoverFromWAL(ctx context.Context, fromLSN uint64) error {
	logger.Infof("🔄 从WAL恢复，起始LSN=%d", fromLSN)

	entries, err := pm.walReader.ReadEntriesFrom(fromLSN)
	if err != nil {
		return fmt.Errorf("读取WAL条目失败: %v", err)
	}

	logger.Infof(" 找到 %d 个WAL条目需要恢复", len(entries))

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := pm.applyWALEntry(entry); err != nil {
				logger.Errorf(" 应用WAL条目失败: LSN=%d, Error=%v", entry.LSN, err)
				continue
			}
		}
	}

	logger.Infof(" WAL恢复完成")
	return nil
}

// applyWALEntry 应用WAL条目
func (pm *PersistenceManager) applyWALEntry(entry *WALEntry) error {
	switch entry.Operation {
	case WALOpPageFlush:
		// 恢复页面数据
		pageFilePath := pm.getPageFilePath(entry.SpaceID, entry.PageNo)
		return pm.writePageToDisk(pageFilePath, entry.Data)
	default:
		logger.Debugf("  未知的WAL操作类型: %d", entry.Operation)
		return nil
	}
}

// generateLSN 生成日志序列号
func (pm *PersistenceManager) generateLSN() uint64 {
	return uint64(time.Now().UnixNano())
}

// updateFlushStats 更新刷新统计信息
func (pm *PersistenceManager) updateFlushStats(latency time.Duration) {
	pm.stats.TotalFlushes++
	pm.stats.FlushLatency = latency
	pm.stats.FlushedPagesCount++
	pm.stats.LastFlushTime = time.Now()
}

// updateCheckpointStats 更新检查点统计信息
func (pm *PersistenceManager) updateCheckpointStats(latency time.Duration) {
	pm.stats.TotalCheckpoints++
	pm.stats.CheckpointLatency = latency
	pm.stats.LastCheckpointTime = time.Now()
}

// GetStats 获取持久化统计信息
func (pm *PersistenceManager) GetStats() *PersistenceStats {
	return pm.stats
}

// SetSyncMode 设置同步模式
func (pm *PersistenceManager) SetSyncMode(mode PersistenceSyncMode) {
	pm.syncMode = mode
	logger.Infof(" 设置持久化同步模式: %d", mode)
}

// SetFlushInterval 设置刷新间隔
func (pm *PersistenceManager) SetFlushInterval(interval time.Duration) {
	pm.flushInterval = interval
	if pm.flushTicker != nil {
		pm.flushTicker.Reset(interval)
	}
	logger.Infof(" 设置刷新间隔: %v", interval)
}

// SetCheckpointInterval 设置检查点间隔
func (pm *PersistenceManager) SetCheckpointInterval(interval time.Duration) {
	pm.checkpointInterval = interval
	if pm.checkpointTicker != nil {
		pm.checkpointTicker.Reset(interval)
	}
	logger.Infof(" 设置检查点间隔: %v", interval)
}
