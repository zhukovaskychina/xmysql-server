package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// CheckpointRecord 检查点记录
type CheckpointRecord struct {
	LSN             uint64                 `json:"lsn"`               // 检查点LSN
	Timestamp       time.Time              `json:"timestamp"`         // 创建时间
	FlushedPages    uint64                 `json:"flushed_pages"`     // 已刷新页面数
	WALSize         uint64                 `json:"wal_size"`          // WAL大小
	BufferPoolStats interface{}            `json:"buffer_pool_stats"` // 缓冲池统计
	TableSpaces     []TableSpaceCheckpoint `json:"table_spaces"`      // 表空间信息
	ActiveTxns      []uint64               `json:"active_txns"`       // 活跃事务
	Checksum        uint32                 `json:"checksum"`          // 校验和
}

// TableSpaceCheckpoint 表空间检查点信息
type TableSpaceCheckpoint struct {
	SpaceID    uint32 `json:"space_id"`     // 表空间ID
	PageCount  uint32 `json:"page_count"`   // 页面数量
	LastPageNo uint32 `json:"last_page_no"` // 最后页面号
	FlushLSN   uint64 `json:"flush_lsn"`    // 刷新LSN
}

// CheckpointManager 检查点管理器
type CheckpointManager struct {
	dataDir           string
	checkpointDir     string
	bufferPoolManager *manager.OptimizedBufferPoolManager

	// 检查点状态
	isRunning       bool
	lastCheckpoint  *CheckpointRecord
	checkpointIndex int

	// 同步控制
	mutex sync.RWMutex

	// 配置
	maxCheckpoints   int    // 最大保留检查点数
	checkpointPrefix string // 检查点文件前缀
}

// NewCheckpointManager 创建检查点管理器
func NewCheckpointManager(
	dataDir string,
	bufferPoolManager *manager.OptimizedBufferPoolManager,
) *CheckpointManager {
	checkpointDir := filepath.Join(dataDir, "checkpoints")

	return &CheckpointManager{
		dataDir:           dataDir,
		checkpointDir:     checkpointDir,
		bufferPoolManager: bufferPoolManager,
		isRunning:         false,
		lastCheckpoint:    nil,
		checkpointIndex:   0,
		maxCheckpoints:    10, // 保留最近10个检查点
		checkpointPrefix:  "checkpoint",
	}
}

// Start 启动检查点管理器
func (cm *CheckpointManager) Start(ctx context.Context) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.isRunning {
		return fmt.Errorf("检查点管理器已经在运行")
	}

	logger.Infof("🚀 启动检查点管理器")

	// 确保检查点目录存在
	if err := os.MkdirAll(cm.checkpointDir, 0755); err != nil {
		return fmt.Errorf("创建检查点目录失败: %v", err)
	}

	// 查找最新的检查点索引
	if err := cm.findLatestCheckpointIndex(); err != nil {
		return fmt.Errorf("查找最新检查点失败: %v", err)
	}

	// 加载最新检查点
	if err := cm.loadLatestCheckpoint(); err != nil {
		logger.Infof("📄 没有找到有效的检查点，将从头开始")
	}

	cm.isRunning = true

	logger.Infof(" 检查点管理器启动成功")
	return nil
}

// Stop 停止检查点管理器
func (cm *CheckpointManager) Stop() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.isRunning {
		return nil
	}

	logger.Infof("🛑 停止检查点管理器")

	cm.isRunning = false

	logger.Infof(" 检查点管理器停止成功")
	return nil
}

// WriteCheckpoint 写入检查点
func (cm *CheckpointManager) WriteCheckpoint(checkpoint *CheckpointRecord) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.isRunning {
		return fmt.Errorf("检查点管理器未运行")
	}

	logger.Infof(" 写入检查点: LSN=%d", checkpoint.LSN)

	// 收集表空间信息
	checkpoint.TableSpaces = cm.collectTableSpaceInfo()

	// 收集活跃事务信息
	checkpoint.ActiveTxns = cm.collectActiveTxns()

	// 计算校验和
	checkpoint.Checksum = cm.calculateChecksum(checkpoint)

	// 序列化检查点
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化检查点失败: %v", err)
	}

	// 生成检查点文件路径
	cm.checkpointIndex++
	checkpointPath := cm.getCheckpointFilePath(cm.checkpointIndex)

	// 写入检查点文件
	if err := cm.writeCheckpointFile(checkpointPath, data); err != nil {
		return fmt.Errorf("写入检查点文件失败: %v", err)
	}

	// 更新最新检查点
	cm.lastCheckpoint = checkpoint

	// 清理旧检查点
	if err := cm.cleanupOldCheckpoints(); err != nil {
		logger.Errorf("  清理旧检查点失败: %v", err)
	}

	logger.Infof(" 检查点写入成功: %s", checkpointPath)
	return nil
}

// ReadLatestCheckpoint 读取最新检查点
func (cm *CheckpointManager) ReadLatestCheckpoint() (*CheckpointRecord, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if cm.lastCheckpoint != nil {
		return cm.lastCheckpoint, nil
	}

	// 查找最新检查点文件
	checkpointFiles, err := cm.getCheckpointFiles()
	if err != nil {
		return nil, fmt.Errorf("获取检查点文件列表失败: %v", err)
	}

	if len(checkpointFiles) == 0 {
		return nil, fmt.Errorf("没有找到检查点文件")
	}

	// 读取最新检查点
	latestFile := checkpointFiles[len(checkpointFiles)-1]
	latestPath := filepath.Join(cm.checkpointDir, latestFile)

	checkpoint, err := cm.readCheckpointFile(latestPath)
	if err != nil {
		return nil, fmt.Errorf("读取检查点文件失败: %v", err)
	}

	// 验证校验和
	expectedChecksum := cm.calculateChecksum(checkpoint)
	if checkpoint.Checksum != expectedChecksum {
		return nil, fmt.Errorf("检查点校验和不匹配")
	}

	logger.Infof("📖 读取最新检查点成功: LSN=%d, 时间=%v",
		checkpoint.LSN, checkpoint.Timestamp)

	return checkpoint, nil
}

// ReadCheckpointByLSN 根据LSN读取检查点
func (cm *CheckpointManager) ReadCheckpointByLSN(lsn uint64) (*CheckpointRecord, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	checkpointFiles, err := cm.getCheckpointFiles()
	if err != nil {
		return nil, fmt.Errorf("获取检查点文件列表失败: %v", err)
	}

	// 查找匹配的检查点
	for i := len(checkpointFiles) - 1; i >= 0; i-- {
		filePath := filepath.Join(cm.checkpointDir, checkpointFiles[i])
		checkpoint, err := cm.readCheckpointFile(filePath)
		if err != nil {
			logger.Errorf(" 读取检查点文件失败: %s, Error: %v", filePath, err)
			continue
		}

		if checkpoint.LSN <= lsn {
			logger.Infof("📖 找到匹配的检查点: LSN=%d (查找LSN=%d)", checkpoint.LSN, lsn)
			return checkpoint, nil
		}
	}

	return nil, fmt.Errorf("没有找到LSN <= %d 的检查点", lsn)
}

// ListCheckpoints 列出所有检查点
func (cm *CheckpointManager) ListCheckpoints() ([]*CheckpointRecord, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	checkpointFiles, err := cm.getCheckpointFiles()
	if err != nil {
		return nil, fmt.Errorf("获取检查点文件列表失败: %v", err)
	}

	var checkpoints []*CheckpointRecord

	for _, file := range checkpointFiles {
		filePath := filepath.Join(cm.checkpointDir, file)
		checkpoint, err := cm.readCheckpointFile(filePath)
		if err != nil {
			logger.Errorf(" 读取检查点文件失败: %s, Error: %v", filePath, err)
			continue
		}

		checkpoints = append(checkpoints, checkpoint)
	}

	// 按LSN排序
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].LSN < checkpoints[j].LSN
	})

	return checkpoints, nil
}

// 私有方法实现

// findLatestCheckpointIndex 查找最新的检查点索引
func (cm *CheckpointManager) findLatestCheckpointIndex() error {
	files, err := cm.getCheckpointFiles()
	if err != nil {
		return err
	}

	maxIndex := 0
	for _, file := range files {
		var index int
		pattern := fmt.Sprintf("%s_%%d.json", cm.checkpointPrefix)
		if n, err := fmt.Sscanf(file, pattern, &index); n == 1 && err == nil {
			if index > maxIndex {
				maxIndex = index
			}
		}
	}

	cm.checkpointIndex = maxIndex
	return nil
}

// loadLatestCheckpoint 加载最新检查点
func (cm *CheckpointManager) loadLatestCheckpoint() error {
	// 直接实现逻辑，避免调用ReadLatestCheckpoint造成死锁
	checkpointFiles, err := cm.getCheckpointFiles()
	if err != nil {
		return fmt.Errorf("获取检查点文件列表失败: %v", err)
	}

	if len(checkpointFiles) == 0 {
		return fmt.Errorf("没有找到检查点文件")
	}

	// 读取最新检查点
	latestFile := checkpointFiles[len(checkpointFiles)-1]
	latestPath := filepath.Join(cm.checkpointDir, latestFile)

	checkpoint, err := cm.readCheckpointFile(latestPath)
	if err != nil {
		return fmt.Errorf("读取检查点文件失败: %v", err)
	}

	// 验证校验和
	expectedChecksum := cm.calculateChecksum(checkpoint)
	if checkpoint.Checksum != expectedChecksum {
		return fmt.Errorf("检查点校验和不匹配")
	}

	logger.Infof("📖 加载最新检查点成功: LSN=%d, 时间=%v",
		checkpoint.LSN, checkpoint.Timestamp)

	cm.lastCheckpoint = checkpoint
	return nil
}

// getCheckpointFilePath 获取检查点文件路径
func (cm *CheckpointManager) getCheckpointFilePath(index int) string {
	filename := fmt.Sprintf("%s_%d.json", cm.checkpointPrefix, index)
	return filepath.Join(cm.checkpointDir, filename)
}

// writeCheckpointFile 写入检查点文件
func (cm *CheckpointManager) writeCheckpointFile(filePath string, data []byte) error {
	// 先写入临时文件
	tempPath := filePath + ".tmp"

	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("创建临时检查点文件失败: %v", err)
	}

	// 写入数据
	if _, err := file.Write(data); err != nil {
		file.Close() // 确保关闭文件
		return fmt.Errorf("写入检查点数据失败: %v", err)
	}

	// 同步文件
	if err := file.Sync(); err != nil {
		file.Close() // 确保关闭文件
		return fmt.Errorf("同步检查点文件失败: %v", err)
	}

	// 显式关闭文件
	if err := file.Close(); err != nil {
		return fmt.Errorf("关闭临时检查点文件失败: %v", err)
	}

	// 在Windows上，如果目标文件存在，需要先删除
	if _, err := os.Stat(filePath); err == nil {
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("删除现有检查点文件失败: %v", err)
		}
	}

	// 原子性重命名
	if err := os.Rename(tempPath, filePath); err != nil {
		return fmt.Errorf("重命名检查点文件失败: %v", err)
	}

	return nil
}

// readCheckpointFile 读取检查点文件
func (cm *CheckpointManager) readCheckpointFile(filePath string) (*CheckpointRecord, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取检查点文件失败: %v", err)
	}

	var checkpoint CheckpointRecord
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("反序列化检查点失败: %v", err)
	}

	return &checkpoint, nil
}

// getCheckpointFiles 获取检查点文件列表
func (cm *CheckpointManager) getCheckpointFiles() ([]string, error) {
	files, err := os.ReadDir(cm.checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("读取检查点目录失败: %v", err)
	}

	var checkpointFiles []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			checkpointFiles = append(checkpointFiles, file.Name())
		}
	}

	// 按文件名排序
	sort.Strings(checkpointFiles)
	return checkpointFiles, nil
}

// collectTableSpaceInfo 收集表空间信息
func (cm *CheckpointManager) collectTableSpaceInfo() []TableSpaceCheckpoint {
	// 简化实现：返回空列表
	// 在实际实现中，应该从存储管理器获取表空间信息
	return []TableSpaceCheckpoint{}
}

// collectActiveTxns 收集活跃事务信息
func (cm *CheckpointManager) collectActiveTxns() []uint64 {
	// 简化实现：返回空列表
	// 在实际实现中，应该从事务管理器获取活跃事务
	return []uint64{}
}

// calculateChecksum 计算检查点校验和
func (cm *CheckpointManager) calculateChecksum(checkpoint *CheckpointRecord) uint32 {
	// 简化实现：使用LSN作为校验和
	return uint32(checkpoint.LSN & 0xFFFFFFFF)
}

// cleanupOldCheckpoints 清理旧检查点
func (cm *CheckpointManager) cleanupOldCheckpoints() error {
	files, err := cm.getCheckpointFiles()
	if err != nil {
		return err
	}

	if len(files) <= cm.maxCheckpoints {
		return nil
	}

	// 删除最老的检查点
	filesToDelete := files[:len(files)-cm.maxCheckpoints]
	deletedCount := 0

	for _, file := range filesToDelete {
		filePath := filepath.Join(cm.checkpointDir, file)
		if err := os.Remove(filePath); err != nil {
			logger.Errorf(" 删除旧检查点失败: %s, Error: %v", filePath, err)
		} else {
			deletedCount++
			logger.Debugf("🗑️ 删除旧检查点: %s", filePath)
		}
	}

	if deletedCount > 0 {
		logger.Infof("🧹 清理了 %d 个旧检查点", deletedCount)
	}

	return nil
}

// GetStats 获取检查点统计信息
func (cm *CheckpointManager) GetStats() *CheckpointStats {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	files, _ := cm.getCheckpointFiles()

	var lastCheckpointTime time.Time
	var lastCheckpointLSN uint64

	if cm.lastCheckpoint != nil {
		lastCheckpointTime = cm.lastCheckpoint.Timestamp
		lastCheckpointLSN = cm.lastCheckpoint.LSN
	}

	return &CheckpointStats{
		TotalCheckpoints:   len(files),
		LastCheckpointTime: lastCheckpointTime,
		LastCheckpointLSN:  lastCheckpointLSN,
		CheckpointDirSize:  cm.calculateDirSize(),
		MaxCheckpoints:     cm.maxCheckpoints,
	}
}

// CheckpointStats 检查点统计信息
type CheckpointStats struct {
	TotalCheckpoints   int       `json:"total_checkpoints"`
	LastCheckpointTime time.Time `json:"last_checkpoint_time"`
	LastCheckpointLSN  uint64    `json:"last_checkpoint_lsn"`
	CheckpointDirSize  int64     `json:"checkpoint_dir_size"`
	MaxCheckpoints     int       `json:"max_checkpoints"`
}

// calculateDirSize 计算目录大小
func (cm *CheckpointManager) calculateDirSize() int64 {
	var totalSize int64

	files, err := os.ReadDir(cm.checkpointDir)
	if err != nil {
		return 0
	}

	for _, file := range files {
		if !file.IsDir() {
			if info, err := file.Info(); err == nil {
				totalSize += info.Size()
			}
		}
	}

	return totalSize
}

// SetMaxCheckpoints 设置最大检查点数量
func (cm *CheckpointManager) SetMaxCheckpoints(max int) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.maxCheckpoints = max
	logger.Infof(" 设置最大检查点数量: %d", max)
}
