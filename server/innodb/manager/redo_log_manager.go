package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	logArchiver   *LogArchiver   // optional archival mirror of flushed redo

	// 检查点相关
	lastCheckpoint uint64    // 最后一次检查点LSN
	checkpointTime time.Time // 最后一次检查点时间

	// 组提交相关
	groupCommit       *GroupCommit        // 组提交管理器
	groupCommitWindow time.Duration       // 组提交窗口期
	pendingCommits    chan *CommitRequest // 待提交请求队列
	shutdown          chan struct{}       // 关闭信号
	closeOnce         sync.Once           // 幂等关闭标记
	workerWg          sync.WaitGroup      // 后台 flush/group-commit worker
	closed            atomic.Bool
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
	manager.workerWg.Add(2)
	go func() {
		defer manager.workerWg.Done()
		manager.backgroundFlush()
	}()

	// 启动组提交协程
	go func() {
		defer manager.workerWg.Done()
		manager.groupCommitWorker()
	}()

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

	return r.flushBuffer(untilLSN)
}

// FlushAsync 异步刷新日志（使用组提交）
func (r *RedoLogManager) FlushAsync(untilLSN uint64, callback func(error)) {
	if r.closed.Load() {
		if callback != nil {
			callback(fmt.Errorf("redo log manager is closed"))
		}
		return
	}
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

// ConfigureGroupCommit updates the runtime batching policy used by the
// asynchronous commit worker.  Both values are required to be positive so a
// misconfiguration cannot silently disable batching or create a busy loop.
func (r *RedoLogManager) ConfigureGroupCommit(window time.Duration, maxBatchSize int) error {
	if r == nil || r.groupCommit == nil {
		return fmt.Errorf("group commit is not initialized")
	}
	if window <= 0 {
		return fmt.Errorf("group commit window must be positive")
	}
	if maxBatchSize <= 0 {
		return fmt.Errorf("group commit batch size must be positive")
	}
	r.groupCommit.SetWindowDuration(window)
	r.groupCommit.SetMaxBatchSize(maxBatchSize)
	return nil
}

// GetGroupCommitStats returns a snapshot of asynchronous commit batching
// metrics.  The returned value is detached from the manager and safe for the
// caller to retain.
func (r *RedoLogManager) GetGroupCommitStats() *GroupCommitStats {
	if r == nil || r.groupCommit == nil {
		return nil
	}
	return r.groupCommit.GetStats()
}

// EnableLogArchival mirrors flushed redo entries to a rotating archival
// stream while retaining redo.log as the authoritative recovery source.
// Archival is opt-in so existing deployments keep their current recovery
// layout and can introduce retention independently.
func (r *RedoLogManager) EnableLogArchival(archiveDir string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.logArchiver != nil {
		return fmt.Errorf("redo log archival is already enabled")
	}
	archiver, err := NewLogArchiver(r.logDir, archiveDir)
	if err != nil {
		return fmt.Errorf("create redo log archiver: %w", err)
	}
	r.logArchiver = archiver
	archiver.Start()
	return nil
}

// flushBuffer 将缓冲区中的日志写入文件
func (r *RedoLogManager) flushBuffer(targetLSN ...uint64) error {
	if len(r.logBuffer) == 0 {
		return nil
	}
	limit := uint64(0)
	if len(targetLSN) > 0 {
		limit = targetLSN[0]
	}
	flushCount := len(r.logBuffer)
	if limit != 0 {
		flushCount = 0
		for flushCount < len(r.logBuffer) && r.logBuffer[flushCount].LSN <= limit {
			flushCount++
		}
	}
	if flushCount == 0 {
		return nil
	}
	entries := r.logBuffer[:flushCount]

	var encoded bytes.Buffer
	// 序列化日志条目
	for _, entry := range entries {
		// 写入LSN
		if err := binary.Write(&encoded, binary.BigEndian, entry.LSN); err != nil {
			return err
		}

		// 写入事务ID
		if err := binary.Write(&encoded, binary.BigEndian, entry.TrxID); err != nil {
			return err
		}

		// 写入页面信息
		if err := binary.Write(&encoded, binary.BigEndian, entry.PageID); err != nil {
			return err
		}

		// 写入操作类型
		if err := binary.Write(&encoded, binary.BigEndian, entry.Type); err != nil {
			return err
		}

		// 写入数据长度和数据
		dataLen := uint16(len(entry.Data))
		if err := binary.Write(&encoded, binary.BigEndian, dataLen); err != nil {
			return err
		}
		if _, err := encoded.Write(entry.Data); err != nil {
			return err
		}
	}

	data := encoded.Bytes()
	if r.logFile != nil {
		if _, err := r.logFile.Write(data); err != nil {
			return err
		}
		if err := r.logFile.Sync(); err != nil {
			return err
		}
	}
	if r.logArchiver != nil {
		if _, err := r.logArchiver.Write(data); err != nil {
			return fmt.Errorf("archive redo log: %w", err)
		}
	}

	// Remove only the entries covered by the requested target LSN.  Entries
	// appended after that target remain buffered for a later commit/flush.
	remaining := r.logBuffer[flushCount:]
	if len(remaining) == 0 {
		r.logBuffer = r.logBuffer[:0]
	} else {
		copy(r.logBuffer, remaining)
		r.logBuffer = r.logBuffer[:len(remaining)]
	}
	return nil
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
			timeout := time.After(r.groupCommit.GetWindowDuration())
			maxBatchSize := r.groupCommit.GetMaxBatchSize()
			if maxBatchSize <= 0 {
				maxBatchSize = 1
			}

			// 收集更多请求或超时
			collecting := true
			for collecting {
				select {
				case req := <-r.pendingCommits:
					batch = append(batch, req)
					if len(batch) >= maxBatchSize {
						collecting = false
					}
				case <-timeout:
					collecting = false
				}
			}

			// 执行组提交
			r.executeGroupCommit(batch)

		case <-r.shutdown:
			// Close must not strand callbacks already accepted into the queue.
			for {
				select {
				case req := <-r.pendingCommits:
					if req != nil {
						r.executeGroupCommit([]*CommitRequest{req})
					}
				default:
					return
				}
			}
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

	start := time.Now()
	// 一次性刷新到最大LSN
	err := r.Flush(maxLSN)
	if r.groupCommit != nil {
		r.groupCommit.RecordCommit(len(batch), time.Since(start))
	}

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
	// 幂等关闭，避免重复 close 导致 panic
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		close(r.shutdown)
	})
	// Stop the workers only after the group-commit worker has drained accepted
	// requests and delivered their callbacks.
	r.workerWg.Wait()

	r.mu.Lock()
	if r.logFile == nil && r.logArchiver == nil {
		r.mu.Unlock()
		return nil
	}

	// 刷新所有缓冲的日志
	if err := r.flushBuffer(); err != nil {
		r.mu.Unlock()
		return err
	}

	logFile := r.logFile
	r.logFile = nil
	archiver := r.logArchiver
	r.logArchiver = nil
	r.mu.Unlock()

	var closeErr error
	if logFile != nil {
		closeErr = logFile.Close()
	}
	if archiver != nil {
		archiver.Stop()
	}
	return closeErr
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

// GetFileSnapshot returns the live metadata for the redo log file owned by
// this manager. xmysql currently uses one append-only redo.log rather than
// MySQL's circular multi-file redo log; callers should therefore treat this
// as the single-file compatibility projection, not as a complete active-file
// inventory.
func (r *RedoLogManager) GetFileSnapshot() (*RedoLogFileSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("redo log manager is nil")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	fileName := filepath.Join(r.logDir, "redo.log")
	var fileInfo os.FileInfo
	var err error
	if r.logFile != nil {
		fileName = r.logFile.Name()
		fileInfo, err = r.logFile.Stat()
	} else {
		fileInfo, err = os.Stat(fileName)
	}
	if err != nil {
		return nil, err
	}

	return &RedoLogFileSnapshot{
		FileID:        0,
		FileName:      fileName,
		StartLSN:      r.lastCheckpoint,
		EndLSN:        uint64(r.lsnManager.GetCurrentLSN()),
		SizeInBytes:   fileInfo.Size(),
		IsFull:        false,
		ConsumerLevel: 0,
	}, nil
}

// RedoLogFileSnapshot is the source-backed subset used by
// performance_schema.innodb_redo_log_files.
type RedoLogFileSnapshot struct {
	FileID        uint64 `json:"file_id"`
	FileName      string `json:"file_name"`
	StartLSN      uint64 `json:"start_lsn"`
	EndLSN        uint64 `json:"end_lsn"`
	SizeInBytes   int64  `json:"size_in_bytes"`
	IsFull        bool   `json:"is_full"`
	ConsumerLevel int64  `json:"consumer_level"`
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
