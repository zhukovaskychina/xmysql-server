package manager

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ============ LOG-008.2: 日志文件轮转和归档 ============

// LogArchiver 日志归档器
// 负责管理日志文件的轮转、归档和清理
type LogArchiver struct {
	mu sync.RWMutex

	// 日志文件配置
	logDir        string // 日志目录
	archiveDir    string // 归档目录
	currentLogNum int    // 当前日志文件编号
	maxLogSize    int64  // 单个日志文件最大大小
	maxLogFiles   int    // 最大保留日志文件数

	// 归档配置
	enableArchive     bool          // 是否启用归档
	archiveInterval   time.Duration // 归档间隔
	archiveAge        time.Duration // 归档年龄阈值
	enableCompression bool          // 是否压缩归档文件

	// 当前日志文件
	currentLogFile *os.File // 当前日志文件
	currentLogPath string   // 当前日志文件路径
	currentLogSize int64    // 当前日志文件大小

	// 运行控制
	running  bool
	stopChan chan struct{}

	// 统计信息
	stats *ArchiverStats
}

// ArchiverStats 归档器统计
type ArchiverStats struct {
	TotalRotations   uint64    `json:"total_rotations"`
	TotalArchived    uint64    `json:"total_archived"`
	TotalDeleted     uint64    `json:"total_deleted"`
	LastRotation     time.Time `json:"last_rotation"`
	LastArchive      time.Time `json:"last_archive"`
	CurrentLogNum    int       `json:"current_log_num"`
	CurrentLogSize   int64     `json:"current_log_size"`
	ArchivedFiles    int       `json:"archived_files"`
	TotalArchiveSize int64     `json:"total_archive_size"`
}

// NewLogArchiver 创建日志归档器
func NewLogArchiver(logDir, archiveDir string) (*LogArchiver, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		return nil, err
	}

	la := &LogArchiver{
		logDir:            logDir,
		archiveDir:        archiveDir,
		currentLogNum:     1,
		maxLogSize:        100 * 1024 * 1024, // 100MB
		maxLogFiles:       10,
		enableArchive:     true,
		archiveInterval:   1 * time.Hour,
		archiveAge:        24 * time.Hour,
		enableCompression: true,
		stopChan:          make(chan struct{}),
		stats:             &ArchiverStats{},
	}

	// 打开当前日志文件
	if err := la.openCurrentLogFile(); err != nil {
		return nil, err
	}

	return la, nil
}

// Start 启动归档器
func (la *LogArchiver) Start() {
	la.mu.Lock()
	if la.running {
		la.mu.Unlock()
		return
	}
	la.running = true
	la.mu.Unlock()

	go la.archiveWorker()
}

// Stop 停止归档器
func (la *LogArchiver) Stop() {
	la.mu.Lock()
	if !la.running {
		la.mu.Unlock()
		return
	}
	la.running = false
	la.mu.Unlock()

	close(la.stopChan)

	// 关闭当前日志文件
	if la.currentLogFile != nil {
		la.currentLogFile.Close()
	}
}

// Write 写入日志数据
func (la *LogArchiver) Write(data []byte) (int, error) {
	la.mu.Lock()
	defer la.mu.Unlock()

	// 检查是否需要轮转
	if la.currentLogSize+int64(len(data)) > la.maxLogSize {
		if err := la.rotateLog(); err != nil {
			return 0, err
		}
	}

	// 写入数据
	n, err := la.currentLogFile.Write(data)
	if err != nil {
		return n, err
	}

	la.currentLogSize += int64(n)
	return n, nil
}

// rotateLog 轮转日志文件
func (la *LogArchiver) rotateLog() error {
	// 关闭当前文件
	if la.currentLogFile != nil {
		la.currentLogFile.Close()
	}

	// 递增日志编号
	la.currentLogNum++

	// 打开新日志文件
	if err := la.openCurrentLogFile(); err != nil {
		return err
	}

	// 更新统计
	la.stats.TotalRotations++
	la.stats.LastRotation = time.Now()

	return nil
}

// openCurrentLogFile 打开当前日志文件
func (la *LogArchiver) openCurrentLogFile() error {
	la.currentLogPath = filepath.Join(la.logDir, fmt.Sprintf("redo.log.%d", la.currentLogNum))

	file, err := os.OpenFile(la.currentLogPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// 获取文件大小
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	la.currentLogFile = file
	la.currentLogSize = stat.Size()

	return nil
}

// archiveWorker 归档工作协程
func (la *LogArchiver) archiveWorker() {
	ticker := time.NewTicker(la.archiveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			la.archiveOldLogs()
			la.cleanupOldArchives()

		case <-la.stopChan:
			return
		}
	}
}

// archiveOldLogs 归档旧日志文件
func (la *LogArchiver) archiveOldLogs() {
	la.mu.Lock()
	defer la.mu.Unlock()

	if !la.enableArchive {
		return
	}

	// 扫描日志目录
	files, err := filepath.Glob(filepath.Join(la.logDir, "redo.log.*"))
	if err != nil {
		return
	}

	archiveThreshold := time.Now().Add(-la.archiveAge)

	for _, logFile := range files {
		// 跳过当前日志文件
		if logFile == la.currentLogPath {
			continue
		}

		// 检查文件修改时间
		stat, err := os.Stat(logFile)
		if err != nil {
			continue
		}

		if stat.ModTime().Before(archiveThreshold) {
			// 归档文件
			if err := la.archiveFile(logFile); err == nil {
				// 删除原文件
				os.Remove(logFile)
				la.stats.TotalArchived++
			}
		}
	}

	la.stats.LastArchive = time.Now()
}

// archiveFile 归档单个文件
func (la *LogArchiver) archiveFile(logFile string) error {
	// 生成归档文件名
	baseName := filepath.Base(logFile)
	archiveName := baseName + ".gz"
	archivePath := filepath.Join(la.archiveDir, archiveName)

	// 打开源文件
	src, err := os.Open(logFile)
	if err != nil {
		return err
	}
	defer src.Close()

	// 创建归档文件
	dst, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if la.enableCompression {
		// gzip压缩
		gzWriter := gzip.NewWriter(dst)
		defer gzWriter.Close()

		if _, err := io.Copy(gzWriter, src); err != nil {
			return err
		}
	} else {
		// 直接复制
		if _, err := io.Copy(dst, src); err != nil {
			return err
		}
	}

	return nil
}

// cleanupOldArchives 清理旧归档文件
func (la *LogArchiver) cleanupOldArchives() {
	la.mu.Lock()
	defer la.mu.Unlock()

	// 获取所有归档文件
	files, err := filepath.Glob(filepath.Join(la.archiveDir, "redo.log.*.gz"))
	if err != nil {
		return
	}

	// 如果归档文件数超过限制，删除最老的
	if len(files) > la.maxLogFiles {
		// 按修改时间排序
		type fileInfo struct {
			path    string
			modTime time.Time
		}

		fileInfos := make([]fileInfo, 0, len(files))
		for _, file := range files {
			stat, err := os.Stat(file)
			if err != nil {
				continue
			}
			fileInfos = append(fileInfos, fileInfo{
				path:    file,
				modTime: stat.ModTime(),
			})
		}

		// 简单冒泡排序（文件数量不多）
		for i := 0; i < len(fileInfos)-1; i++ {
			for j := 0; j < len(fileInfos)-1-i; j++ {
				if fileInfos[j].modTime.After(fileInfos[j+1].modTime) {
					fileInfos[j], fileInfos[j+1] = fileInfos[j+1], fileInfos[j]
				}
			}
		}

		// 删除最老的文件
		deleteCount := len(fileInfos) - la.maxLogFiles
		for i := 0; i < deleteCount; i++ {
			os.Remove(fileInfos[i].path)
			la.stats.TotalDeleted++
		}
	}
}

// GetStats 获取统计信息
func (la *LogArchiver) GetStats() *ArchiverStats {
	la.mu.RLock()
	defer la.mu.RUnlock()

	stats := *la.stats
	stats.CurrentLogNum = la.currentLogNum
	stats.CurrentLogSize = la.currentLogSize

	// 统计归档文件数和大小
	files, _ := filepath.Glob(filepath.Join(la.archiveDir, "redo.log.*.gz"))
	stats.ArchivedFiles = len(files)

	var totalSize int64
	for _, file := range files {
		if stat, err := os.Stat(file); err == nil {
			totalSize += stat.Size()
		}
	}
	stats.TotalArchiveSize = totalSize

	return &stats
}

// SetMaxLogSize 设置最大日志文件大小
func (la *LogArchiver) SetMaxLogSize(size int64) {
	la.mu.Lock()
	defer la.mu.Unlock()
	la.maxLogSize = size
}

// SetMaxLogFiles 设置最大保留日志文件数
func (la *LogArchiver) SetMaxLogFiles(count int) {
	la.mu.Lock()
	defer la.mu.Unlock()
	la.maxLogFiles = count
}
