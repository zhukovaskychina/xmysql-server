package manager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RedoLogManager 重做日志管理器
type RedoLogManager struct {
	mu            sync.RWMutex
	logFile       *os.File       // 日志文件
	lsnManager    *LSNManager    // LSN管理器
	logBufferSize int            // 日志缓冲区大小
	logBuffer     []RedoLogEntry // 日志缓冲区
	logDir        string         // 日志目录
	flushInterval time.Duration  // 刷新间隔

	// 检查点相关
	lastCheckpoint uint64    // 最后一次检查点LSN
	checkpointTime time.Time // 最后一次检查点时间

	// 组提交相关
	groupCommit       *GroupCommit        // 组提交管理器
	groupCommitWindow time.Duration       // 组提交窗口期
	pendingCommits    chan *CommitRequest // 待提交请求队列
	shutdown          chan struct{}       // 关闭信号
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
		logFile:           logFile,
		lsnManager:        NewLSNManager(1),
		logBufferSize:     bufferSize,
		logBuffer:         make([]RedoLogEntry, 0, bufferSize),
		logDir:            logDir,
		flushInterval:     1 * time.Second,
		groupCommitWindow: 10 * time.Millisecond, // 10ms组提交窗口
		pendingCommits:    make(chan *CommitRequest, 1000),
		shutdown:          make(chan struct{}),
	}

	// 创建组提交管理器
	manager.groupCommit = NewGroupCommit(manager.groupCommitWindow, 100)

	// 启动异步刷新协程
	go manager.backgroundFlush()

	// 启动组提交协程
	go manager.groupCommitWorker()

	return manager, nil
}

// Append 追加一条重做日志
func (r *RedoLogManager) Append(entry *RedoLogEntry) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 使用LSN管理器分配LSN
	entry.LSN = uint64(r.lsnManager.AllocateLSN())
	entry.Timestamp = time.Now()

	// 添加到缓冲区
	r.logBuffer = append(r.logBuffer, *entry)

	// 如果缓冲区满了，触发刷新
	if len(r.logBuffer) >= r.logBufferSize {
		if err := r.flushBuffer(); err != nil {
			return 0, err
		}
	}

	return entry.LSN, nil
}

// Flush 将日志刷新到磁盘
func (r *RedoLogManager) Flush(untilLSN uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.flushBuffer()
}

// FlushAsync 异步刷新日志（使用组提交）
func (r *RedoLogManager) FlushAsync(untilLSN uint64, callback func(error)) {
	req := &CommitRequest{
		LSN:      untilLSN,
		Callback: callback,
		Done:     make(chan error, 1),
	}

	select {
	case r.pendingCommits <- req:
		// 请求已加入队列
	default:
		// 队列满，同步刷新
		err := r.Flush(untilLSN)
		if callback != nil {
			callback(err)
		}
	}
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

	for {
		select {
		case <-ticker.C:
			r.Flush(uint64(r.lsnManager.GetCurrentLSN()))
		case <-r.shutdown:
			return
		}
	}
}

// groupCommitWorker 组提交工作协程
func (r *RedoLogManager) groupCommitWorker() {
	for {
		select {
		case req := <-r.pendingCommits:
			// 收集一批请求
			batch := []*CommitRequest{req}
			timeout := time.After(r.groupCommitWindow)

			// 收集更多请求或超时
			collecting := true
			for collecting {
				select {
				case req := <-r.pendingCommits:
					batch = append(batch, req)
					if len(batch) >= 100 { // 批次大小限制
						collecting = false
					}
				case <-timeout:
					collecting = false
				}
			}

			// 执行组提交
			r.executeGroupCommit(batch)

		case <-r.shutdown:
			return
		}
	}
}

// executeGroupCommit 执行组提交
func (r *RedoLogManager) executeGroupCommit(batch []*CommitRequest) {
	if len(batch) == 0 {
		return
	}

	// 找到最大LSN
	var maxLSN uint64
	for _, req := range batch {
		if req.LSN > maxLSN {
			maxLSN = req.LSN
		}
	}

	// 一次性刷新到最大LSN
	err := r.Flush(maxLSN)

	// 通知所有请求
	for _, req := range batch {
		if req.Callback != nil {
			req.Callback(err)
		}
		select {
		case req.Done <- err:
		default:
		}
	}
}

// Recover 从日志文件恢复
// 【修复TXN-001】此方法已废弃，应使用CrashRecovery进行完整的三阶段恢复
// 保留此方法仅用于向后兼容，实际恢复应使用RecoverWithCrashRecovery
func (r *RedoLogManager) Recover() error {
	// 读取最后的检查点LSN
	checkpointLSN, err := r.readCheckpointLSN()
	if err != nil {
		// 如果没有检查点，从LSN 0开始
		checkpointLSN = 0
	}

	// 创建CrashRecovery实例（不带缓冲池和存储管理器）
	// 这是简化版本，仅用于基本恢复
	crashRecovery := NewCrashRecovery(r, nil, checkpointLSN)

	// 执行完整的三阶段恢复
	return crashRecovery.Recover()
}

// RecoverWithCrashRecovery 使用CrashRecovery进行完整恢复
// 【修复TXN-001】推荐使用此方法进行崩溃恢复
func (r *RedoLogManager) RecoverWithCrashRecovery(
	undoLogManager *UndoLogManager,
	bufferPoolManager BufferPoolInterface,
	storageManager StorageInterface,
) error {
	// 读取最后的检查点LSN
	checkpointLSN, err := r.readCheckpointLSN()
	if err != nil {
		// 如果没有检查点，从LSN 0开始
		checkpointLSN = 0
	}

	// 创建CrashRecovery实例
	crashRecovery := NewCrashRecovery(r, undoLogManager, checkpointLSN)
	crashRecovery.SetBufferPoolManager(bufferPoolManager)
	crashRecovery.SetStorageManager(storageManager)

	// 执行完整的三阶段恢复
	if err := crashRecovery.Recover(); err != nil {
		return fmt.Errorf("崩溃恢复失败: %v", err)
	}

	// 验证恢复结果
	if err := crashRecovery.ValidateRecovery(); err != nil {
		return fmt.Errorf("恢复验证失败: %v", err)
	}

	return nil
}

// readCheckpointLSN 读取检查点LSN
func (r *RedoLogManager) readCheckpointLSN() (uint64, error) {
	checkpointFile := filepath.Join(r.logDir, "redo_checkpoint")
	file, err := os.Open(checkpointFile)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var checkpointLSN uint64
	if err := binary.Read(file, binary.BigEndian, &checkpointLSN); err != nil {
		return 0, err
	}

	return checkpointLSN, nil
}

// replayLogEntry 重放单条日志
// 【已废弃】此方法已废弃，实际重放逻辑在CrashRecovery中实现
// 保留此方法仅用于向后兼容
func (r *RedoLogManager) replayLogEntry(entry *RedoLogEntry) error {
	// 根据日志类型执行不同的重放操作
	switch entry.Type {
	case LOG_TYPE_INSERT, LOG_TYPE_UPDATE, LOG_TYPE_DELETE:
		// 数据修改操作：需要应用到页面
		return r.replayDataModification(entry)

	case LOG_TYPE_PAGE_CREATE, LOG_TYPE_PAGE_DELETE, LOG_TYPE_PAGE_MODIFY:
		// 页面操作：需要应用到页面管理器
		return r.replayPageOperation(entry)

	case LOG_TYPE_TXN_BEGIN, LOG_TYPE_TXN_COMMIT, LOG_TYPE_TXN_ROLLBACK:
		// 事务操作：记录事务状态
		return r.replayTransactionOperation(entry)

	case LOG_TYPE_CHECKPOINT:
		// 检查点：更新检查点信息
		r.lastCheckpoint = entry.LSN
		r.checkpointTime = entry.Timestamp
		return nil

	default:
		// 未知类型：记录警告但继续
		return nil
	}
}

// replayDataModification 重放数据修改操作
// 【已废弃】实际重放逻辑在CrashRecovery.redoInsert/redoUpdate/redoDelete中实现
func (r *RedoLogManager) replayDataModification(entry *RedoLogEntry) error {
	// 注意：这里需要缓冲池管理器的支持
	// 由于当前架构中RedoLogManager不直接持有BufferPoolManager引用
	// 实际的重放逻辑应该在CrashRecovery中完成
	// 这里只是记录需要重放的日志
	return nil
}

// replayPageOperation 重放页面操作
// 【已废弃】实际重放逻辑在CrashRecovery.redoPageCreate/redoPageDelete/redoPageModify中实现
func (r *RedoLogManager) replayPageOperation(entry *RedoLogEntry) error {
	// 页面操作的重放逻辑
	// 实际实现需要页面管理器的支持
	return nil
}

// replayTransactionOperation 重放事务操作
// 【已废弃】实际事务状态跟踪在CrashRecovery.analysisPhase中实现
func (r *RedoLogManager) replayTransactionOperation(entry *RedoLogEntry) error {
	// 事务操作的重放逻辑
	// 主要用于跟踪事务状态
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
	r.lastCheckpoint = uint64(r.lsnManager.GetCurrentLSN())
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
	// 发送关闭信号
	close(r.shutdown)

	r.mu.Lock()
	defer r.mu.Unlock()

	// 刷新所有缓冲的日志
	if err := r.flushBuffer(); err != nil {
		return err
	}

	// 关闭文件
	return r.logFile.Close()
}

// GetLSNManager 获取LSN管理器
func (r *RedoLogManager) GetLSNManager() *LSNManager {
	return r.lsnManager
}

// GetStats 获取Redo Log统计信息
func (r *RedoLogManager) GetStats() *RedoLogStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &RedoLogStats{
		CurrentLSN:     uint64(r.lsnManager.GetCurrentLSN()),
		LastCheckpoint: r.lastCheckpoint,
		BufferSize:     r.logBufferSize,
		BufferedLogs:   len(r.logBuffer),
		PendingCommits: len(r.pendingCommits),
	}
}

// RedoLogStats Redo日志统计信息
type RedoLogStats struct {
	CurrentLSN     uint64 `json:"current_lsn"`
	LastCheckpoint uint64 `json:"last_checkpoint"`
	BufferSize     int    `json:"buffer_size"`
	BufferedLogs   int    `json:"buffered_logs"`
	PendingCommits int    `json:"pending_commits"`
}

// CommitRequest 提交请求
type CommitRequest struct {
	LSN      uint64      // 需要提交到的LSN
	Callback func(error) // 完成回调
	Done     chan error  // 完成通知
}
