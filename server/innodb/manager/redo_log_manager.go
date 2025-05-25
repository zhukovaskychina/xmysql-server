package manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RedoLogManager 重做日志管理器
type RedoLogManager struct {
	mu            sync.RWMutex
	logFile       *os.File       // 日志文件
	nextLSN       int64          // 下一个LSN
	logBufferSize int            // 日志缓冲区大小
	logBuffer     []RedoLogEntry // 日志缓冲区
	logDir        string         // 日志目录
	flushInterval time.Duration  // 刷新间隔

	// 检查点相关
	lastCheckpoint int64     // 最后一次检查点LSN
	checkpointTime time.Time // 最后一次检查点时间
}

// NewRedoLogManager 创建新的重做日志管理器
func NewRedoLogManager(logDir string, bufferSize int) (*RedoLogManager, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	logFile, err := os.OpenFile(
		filepath.Join(logDir, "redo.log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	manager := &RedoLogManager{
		logFile:       logFile,
		nextLSN:       1,
		logBufferSize: bufferSize,
		logBuffer:     make([]RedoLogEntry, 0, bufferSize),
		logDir:        logDir,
		flushInterval: 1 * time.Second,
	}

	// 启动异步刷新协程
	go manager.backgroundFlush()

	return manager, nil
}

// Append 追加一条重做日志
func (r *RedoLogManager) Append(entry *RedoLogEntry) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 分配LSN
	entry.LSN = uint64(r.nextLSN)
	r.nextLSN++
	entry.Timestamp = time.Now()

	// 添加到缓冲区
	r.logBuffer = append(r.logBuffer, *entry)

	// 如果缓冲区满了，触发刷新
	if len(r.logBuffer) >= r.logBufferSize {
		if err := r.flushBuffer(); err != nil {
			return 0, err
		}
	}

	return int64(entry.LSN), nil
}

// Flush 将日志刷新到磁盘
func (r *RedoLogManager) Flush(untilLSN int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.flushBuffer()
}

// flushBuffer 将缓冲区中的日志写入文件
func (r *RedoLogManager) flushBuffer() error {
	if len(r.logBuffer) == 0 {
		return nil
	}

	// 序列化日志条目
	for _, entry := range r.logBuffer {
		// 写入LSN
		if err := binary.Write(r.logFile, binary.BigEndian, entry.LSN); err != nil {
			return err
		}

		// 写入事务ID
		if err := binary.Write(r.logFile, binary.BigEndian, entry.TrxID); err != nil {
			return err
		}

		// 写入页面信息
		if err := binary.Write(r.logFile, binary.BigEndian, entry.PageID); err != nil {
			return err
		}

		// 写入操作类型
		if err := binary.Write(r.logFile, binary.BigEndian, entry.Type); err != nil {
			return err
		}

		// 写入数据长度和数据
		dataLen := uint16(len(entry.Data))
		if err := binary.Write(r.logFile, binary.BigEndian, dataLen); err != nil {
			return err
		}
		if _, err := r.logFile.Write(entry.Data); err != nil {
			return err
		}
	}

	// 清空缓冲区
	r.logBuffer = r.logBuffer[:0]

	// 同步到磁盘
	return r.logFile.Sync()
}

// backgroundFlush 后台定期刷新
func (r *RedoLogManager) backgroundFlush() {
	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.Flush(r.nextLSN)
	}
}

// Recover 从日志文件恢复
func (r *RedoLogManager) Recover() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 定位到文件开始
	if _, err := r.logFile.Seek(0, 0); err != nil {
		return err
	}

	// 读取并重放日志
	for {
		var entry RedoLogEntry

		// 读取LSN
		if err := binary.Read(r.logFile, binary.BigEndian, &entry.LSN); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		// 读取事务ID
		if err := binary.Read(r.logFile, binary.BigEndian, &entry.TrxID); err != nil {
			return err
		}

		// 读取页面信息
		if err := binary.Read(r.logFile, binary.BigEndian, &entry.PageID); err != nil {
			return err
		}
		if err := binary.Read(r.logFile, binary.BigEndian, &entry.Type); err != nil {
			return err
		}

		// 读取数据
		var dataLen uint16
		if err := binary.Read(r.logFile, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		entry.Data = make([]byte, dataLen)
		if _, err := r.logFile.Read(entry.Data); err != nil {
			return err
		}

		// TODO: 重放日志操作
		// 这里需要调用缓冲池管理器来应用修改
	}

	return nil
}

// Checkpoint 创建检查点
func (r *RedoLogManager) Checkpoint() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 确保所有日志都已刷新
	if err := r.flushBuffer(); err != nil {
		return err
	}

	// 更新检查点信息
	r.lastCheckpoint = r.nextLSN - 1
	r.checkpointTime = time.Now()

	// 写入检查点文件
	checkpointFile := filepath.Join(r.logDir, "redo_checkpoint")
	file, err := os.Create(checkpointFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// 写入检查点LSN
	if err := binary.Write(file, binary.BigEndian, r.lastCheckpoint); err != nil {
		return err
	}

	return file.Sync()
}

// Close 关闭日志管理器
func (r *RedoLogManager) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 刷新所有缓冲的日志
	if err := r.flushBuffer(); err != nil {
		return err
	}

	// 关闭文件
	return r.logFile.Close()
}
