package engine

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
)

// WALOperation WAL操作类型
type WALOperation int

const (
	WALOpPageFlush WALOperation = iota // 页面刷新
	WALOpInsert                        // 插入操作
	WALOpUpdate                        // 更新操作
	WALOpDelete                        // 删除操作
	WALOpCommit                        // 事务提交
	WALOpRollback                      // 事务回滚
)

// WALEntry WAL日志条目
type WALEntry struct {
	LSN       uint64       `json:"lsn"`       // 日志序列号
	SpaceID   uint32       `json:"space_id"`  // 表空间ID
	PageNo    uint32       `json:"page_no"`   // 页面号
	Operation WALOperation `json:"operation"` // 操作类型
	Data      []byte       `json:"data"`      // 数据内容
	Timestamp time.Time    `json:"timestamp"` // 时间戳
	TxnID     uint64       `json:"txn_id"`    // 事务ID
	Checksum  uint32       `json:"checksum"`  // 校验和
}

// WALWriter WAL写入器
type WALWriter struct {
	walDir       string
	currentFile  *os.File
	currentPath  string
	fileSize     int64
	maxFileSize  int64
	fileIndex    int
	mutex        sync.Mutex
	isRunning    bool
	writeBuffer  *bufio.Writer
	syncInterval time.Duration
	lastSync     time.Time
}

// WALReader WAL读取器
type WALReader struct {
	walDir string
	mutex  sync.RWMutex
}

// NewWALWriter 创建WAL写入器
func NewWALWriter(walDir string) *WALWriter {
	return &WALWriter{
		walDir:       walDir,
		maxFileSize:  100 * 1024 * 1024, // 100MB
		fileIndex:    0,
		isRunning:    false,
		syncInterval: time.Millisecond * 100, // 100ms同步间隔
	}
}

// NewWALReader 创建WAL读取器
func NewWALReader(walDir string) *WALReader {
	return &WALReader{
		walDir: walDir,
	}
}

// Start 启动WAL写入器
func (w *WALWriter) Start() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.isRunning {
		return fmt.Errorf("WAL写入器已经在运行")
	}

	logger.Infof("🚀 启动WAL写入器")

	// 确保WAL目录存在
	if err := os.MkdirAll(w.walDir, 0755); err != nil {
		return fmt.Errorf("创建WAL目录失败: %v", err)
	}

	// 查找最新的WAL文件索引
	if err := w.findLatestFileIndex(); err != nil {
		return fmt.Errorf("查找最新WAL文件失败: %v", err)
	}

	// 打开或创建WAL文件
	if err := w.openCurrentFile(); err != nil {
		return fmt.Errorf("打开WAL文件失败: %v", err)
	}

	w.isRunning = true
	w.lastSync = time.Now()

	logger.Infof(" WAL写入器启动成功，文件: %s", w.currentPath)
	return nil
}

// Stop 停止WAL写入器
func (w *WALWriter) Stop() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.isRunning {
		return nil
	}

	logger.Infof("🛑 停止WAL写入器")

	// 刷新缓冲区
	if w.writeBuffer != nil {
		if err := w.writeBuffer.Flush(); err != nil {
			logger.Errorf(" 刷新WAL缓冲区失败: %v", err)
		}
	}

	// 同步文件
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			logger.Errorf(" 同步WAL文件失败: %v", err)
		}
		w.currentFile.Close()
	}

	w.isRunning = false

	logger.Infof(" WAL写入器停止成功")
	return nil
}

// WriteEntry 写入WAL条目
func (w *WALWriter) WriteEntry(entry *WALEntry) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.isRunning {
		return fmt.Errorf("WAL写入器未运行")
	}

	// 计算校验和
	entry.Checksum = w.calculateChecksum(entry)

	// 序列化条目
	data, err := w.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("序列化WAL条目失败: %v", err)
	}

	// 检查是否需要轮转文件
	if w.fileSize+int64(len(data)) > w.maxFileSize {
		if err := w.rotateFile(); err != nil {
			return fmt.Errorf("轮转WAL文件失败: %v", err)
		}
	}

	// 写入数据长度
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(len(data)))
	if _, err := w.writeBuffer.Write(lengthBytes); err != nil {
		return fmt.Errorf("写入数据长度失败: %v", err)
	}

	// 写入数据
	if _, err := w.writeBuffer.Write(data); err != nil {
		return fmt.Errorf("写入WAL数据失败: %v", err)
	}

	w.fileSize += int64(len(data)) + 4

	// 根据同步间隔决定是否立即同步
	if time.Since(w.lastSync) >= w.syncInterval {
		if err := w.sync(); err != nil {
			return fmt.Errorf("同步WAL失败: %v", err)
		}
	}

	logger.Debugf(" WAL条目写入成功: LSN=%d, Operation=%d, Size=%d",
		entry.LSN, entry.Operation, len(data))
	return nil
}

// Truncate 截断WAL日志
func (w *WALWriter) Truncate(beforeLSN uint64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	logger.Infof("✂️  截断WAL日志，LSN < %d", beforeLSN)

	// 获取所有WAL文件
	files, err := w.getWALFiles()
	if err != nil {
		return fmt.Errorf("获取WAL文件列表失败: %v", err)
	}

	// 删除旧的WAL文件
	deletedCount := 0
	for _, file := range files {
		// 简化实现：删除除当前文件外的所有文件
		if file != filepath.Base(w.currentPath) {
			filePath := filepath.Join(w.walDir, file)
			if err := os.Remove(filePath); err != nil {
				logger.Errorf(" 删除WAL文件失败: %s, Error: %v", filePath, err)
			} else {
				deletedCount++
				logger.Debugf("🗑️ 删除WAL文件: %s", filePath)
			}
		}
	}

	logger.Infof(" WAL截断完成，删除了 %d 个文件", deletedCount)
	return nil
}

// ReadEntriesFrom 从指定LSN开始读取WAL条目
func (r *WALReader) ReadEntriesFrom(fromLSN uint64) ([]*WALEntry, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	logger.Infof("📖 从LSN %d 开始读取WAL条目", fromLSN)

	// 获取所有WAL文件
	files, err := r.getWALFiles()
	if err != nil {
		return nil, fmt.Errorf("获取WAL文件列表失败: %v", err)
	}

	var allEntries []*WALEntry

	// 读取所有WAL文件
	for _, file := range files {
		filePath := filepath.Join(r.walDir, file)
		entries, err := r.readEntriesFromFile(filePath)
		if err != nil {
			logger.Errorf(" 读取WAL文件失败: %s, Error: %v", filePath, err)
			continue
		}
		allEntries = append(allEntries, entries...)
	}

	// 过滤LSN
	var filteredEntries []*WALEntry
	for _, entry := range allEntries {
		if entry.LSN >= fromLSN {
			filteredEntries = append(filteredEntries, entry)
		}
	}

	// 按LSN排序
	sort.Slice(filteredEntries, func(i, j int) bool {
		return filteredEntries[i].LSN < filteredEntries[j].LSN
	})

	logger.Infof(" 读取到 %d 个WAL条目", len(filteredEntries))
	return filteredEntries, nil
}

// 私有方法实现

// findLatestFileIndex 查找最新的文件索引
func (w *WALWriter) findLatestFileIndex() error {
	files, err := w.getWALFiles()
	if err != nil {
		return err
	}

	maxIndex := -1
	for _, file := range files {
		var index int
		if n, err := fmt.Sscanf(file, "wal_%d.log", &index); n == 1 && err == nil {
			if index > maxIndex {
				maxIndex = index
			}
		}
	}

	if maxIndex >= 0 {
		w.fileIndex = maxIndex
		// 检查当前文件大小
		currentPath := filepath.Join(w.walDir, fmt.Sprintf("wal_%d.log", w.fileIndex))
		if stat, err := os.Stat(currentPath); err == nil {
			w.fileSize = stat.Size()
		}
	}

	return nil
}

// openCurrentFile 打开当前WAL文件
func (w *WALWriter) openCurrentFile() error {
	w.currentPath = filepath.Join(w.walDir, fmt.Sprintf("wal_%d.log", w.fileIndex))

	file, err := os.OpenFile(w.currentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("打开WAL文件失败: %v", err)
	}

	w.currentFile = file
	w.writeBuffer = bufio.NewWriter(file)

	// 获取文件大小
	if stat, err := file.Stat(); err == nil {
		w.fileSize = stat.Size()
	}

	return nil
}

// rotateFile 轮转WAL文件
func (w *WALWriter) rotateFile() error {
	logger.Infof("🔄 轮转WAL文件")

	// 刷新并关闭当前文件
	if w.writeBuffer != nil {
		if err := w.writeBuffer.Flush(); err != nil {
			return fmt.Errorf("刷新缓冲区失败: %v", err)
		}
	}

	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("同步文件失败: %v", err)
		}
		w.currentFile.Close()
	}

	// 创建新文件
	w.fileIndex++
	w.fileSize = 0

	return w.openCurrentFile()
}

// sync 同步WAL文件
func (w *WALWriter) sync() error {
	if w.writeBuffer != nil {
		if err := w.writeBuffer.Flush(); err != nil {
			return fmt.Errorf("刷新缓冲区失败: %v", err)
		}
	}

	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("同步文件失败: %v", err)
		}
	}

	w.lastSync = time.Now()
	return nil
}

// serializeEntry 序列化WAL条目
func (w *WALWriter) serializeEntry(entry *WALEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// calculateChecksum 计算校验和
func (w *WALWriter) calculateChecksum(entry *WALEntry) uint32 {
	// 简化实现：使用数据长度作为校验和
	return uint32(len(entry.Data))
}

// getWALFiles 获取WAL文件列表
func (w *WALWriter) getWALFiles() ([]string, error) {
	files, err := os.ReadDir(w.walDir)
	if err != nil {
		return nil, fmt.Errorf("读取WAL目录失败: %v", err)
	}

	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			walFiles = append(walFiles, file.Name())
		}
	}

	// 按文件名排序
	sort.Strings(walFiles)
	return walFiles, nil
}

// getWALFiles 获取WAL文件列表（读取器版本）
func (r *WALReader) getWALFiles() ([]string, error) {
	files, err := os.ReadDir(r.walDir)
	if err != nil {
		return nil, fmt.Errorf("读取WAL目录失败: %v", err)
	}

	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			walFiles = append(walFiles, file.Name())
		}
	}

	// 按文件名排序
	sort.Strings(walFiles)
	return walFiles, nil
}

// readEntriesFromFile 从文件读取WAL条目
func (r *WALReader) readEntriesFromFile(filePath string) ([]*WALEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开WAL文件失败: %v", err)
	}
	defer file.Close()

	var entries []*WALEntry
	reader := bufio.NewReader(file)

	for {
		// 读取数据长度
		lengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, lengthBytes); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("读取数据长度失败: %v", err)
		}

		dataLength := binary.LittleEndian.Uint32(lengthBytes)

		// 读取数据
		data := make([]byte, dataLength)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, fmt.Errorf("读取WAL数据失败: %v", err)
		}

		// 反序列化条目
		var entry WALEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			logger.Errorf(" 反序列化WAL条目失败: %v", err)
			continue
		}

		// 验证校验和（简化实现）
		expectedChecksum := uint32(len(entry.Data))
		if entry.Checksum != expectedChecksum {
			logger.Errorf(" WAL条目校验和不匹配: LSN=%d", entry.LSN)
			continue
		}

		entries = append(entries, &entry)
	}

	logger.Debugf("📖 从文件 %s 读取到 %d 个WAL条目", filePath, len(entries))
	return entries, nil
}

// WALStats WAL统计信息
type WALStats struct {
	TotalEntries  uint64
	TotalSize     uint64
	FileCount     int
	LastWriteTime time.Time
	WriteLatency  time.Duration
	SyncLatency   time.Duration
}

// GetStats 获取WAL统计信息
func (w *WALWriter) GetStats() *WALStats {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	files, _ := w.getWALFiles()

	return &WALStats{
		TotalEntries:  0, // 需要实际统计
		TotalSize:     uint64(w.fileSize),
		FileCount:     len(files),
		LastWriteTime: w.lastSync,
		WriteLatency:  0, // 需要实际测量
		SyncLatency:   0, // 需要实际测量
	}
}
