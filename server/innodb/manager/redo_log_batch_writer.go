package manager

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// ============ LOG-003.3: Redo日志批量写入优化 ============

// BatchWriter Redo日志批量写入器
type BatchWriter struct {
	mu sync.Mutex

	// 文件操作
	file     *os.File // 日志文件
	filePath string   // 文件路径

	// 批量写入缓冲区
	buffer       []byte // 写入缓冲区
	bufferSize   int    // 缓冲区大小
	currentSize  int    // 当前已使用大小
	maxBatchSize int    // 最大批次大小

	// 批次管理
	pendingBatch  []*RedoLogEntry // 待写入的日志条目
	batchInterval time.Duration   // 批次刷新间隔

	// 异步写入
	writeChan chan *WriteRequest // 写入请求通道
	flushChan chan *FlushRequest // 刷新请求通道
	stopChan  chan struct{}      // 停止信号

	// LSN管理
	lsnManager *LSNManager // LSN管理器

	// 统计信息
	stats *BatchWriterStats

	// 压缩器
	compressor *RedoLogCompressor // 日志压缩器
}

// WriteRequest 写入请求
type WriteRequest struct {
	Entry    *RedoLogEntry // 日志条目
	Callback func(error)   // 完成回调
	Done     chan error    // 完成通知
}

// FlushRequest 刷新请求
type FlushRequest struct {
	Force    bool        // 是否强制刷新
	Callback func(error) // 完成回调
	Done     chan error  // 完成通知
}

// BatchWriterStats 批量写入器统计
type BatchWriterStats struct {
	TotalWrites       uint64        `json:"total_writes"`       // 总写入次数
	TotalFlushes      uint64        `json:"total_flushes"`      // 总刷新次数
	BytesWritten      uint64        `json:"bytes_written"`      // 已写入字节数
	AvgBatchSize      int           `json:"avg_batch_size"`     // 平均批次大小
	AvgWriteTime      time.Duration `json:"avg_write_time"`     // 平均写入时间
	AvgFlushTime      time.Duration `json:"avg_flush_time"`     // 平均刷新时间
	PendingWrites     int           `json:"pending_writes"`     // 待写入数量
	BufferUtilization float64       `json:"buffer_utilization"` // 缓冲区利用率
}

// NewBatchWriter 创建批量写入器
func NewBatchWriter(filePath string, bufferSize int, lsnManager *LSNManager) (*BatchWriter, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	bw := &BatchWriter{
		file:          file,
		filePath:      filePath,
		buffer:        make([]byte, bufferSize),
		bufferSize:    bufferSize,
		maxBatchSize:  1000,
		batchInterval: 10 * time.Millisecond,
		pendingBatch:  make([]*RedoLogEntry, 0, 100),
		writeChan:     make(chan *WriteRequest, 1000),
		flushChan:     make(chan *FlushRequest, 100),
		stopChan:      make(chan struct{}),
		lsnManager:    lsnManager,
		stats:         &BatchWriterStats{},
		compressor:    NewRedoLogCompressor(COMPRESS_GZIP, 6),
	}

	// 启动批量写入协程
	go bw.batchWriteWorker()

	// 启动定时刷新协程
	go bw.periodicFlushWorker()

	return bw, nil
}

// Write 异步写入日志条目
func (bw *BatchWriter) Write(entry *RedoLogEntry) error {
	req := &WriteRequest{
		Entry: entry,
		Done:  make(chan error, 1),
	}

	select {
	case bw.writeChan <- req:
		// 请求已加入队列，异步等待结果
		return <-req.Done
	default:
		// 队列满，同步写入
		return bw.writeSync(entry)
	}
}

// WriteAsync 异步写入（不等待结果）
func (bw *BatchWriter) WriteAsync(entry *RedoLogEntry, callback func(error)) {
	req := &WriteRequest{
		Entry:    entry,
		Callback: callback,
		Done:     make(chan error, 1),
	}

	select {
	case bw.writeChan <- req:
		// 成功加入队列
	default:
		// 队列满，同步写入并回调
		err := bw.writeSync(entry)
		if callback != nil {
			callback(err)
		}
	}
}

// writeSync 同步写入
func (bw *BatchWriter) writeSync(entry *RedoLogEntry) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// 分配LSN
	entry.LSN = uint64(bw.lsnManager.AllocateLSN())
	entry.Timestamp = time.Now()

	// 序列化日志条目
	data, err := entry.Serialize()
	if err != nil {
		return err
	}

	// 写入文件
	if _, err := bw.file.Write(data); err != nil {
		return err
	}

	// 更新统计
	bw.stats.TotalWrites++
	bw.stats.BytesWritten += uint64(len(data))

	return nil
}

// batchWriteWorker 批量写入工作协程
func (bw *BatchWriter) batchWriteWorker() {
	ticker := time.NewTicker(bw.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case req := <-bw.writeChan:
			// 添加到批次
			bw.addToBatch(req)

			// 如果批次已满，立即刷新
			if len(bw.pendingBatch) >= bw.maxBatchSize {
				bw.flushBatch()
			}

		case <-ticker.C:
			// 定时刷新批次
			if len(bw.pendingBatch) > 0 {
				bw.flushBatch()
			}

		case req := <-bw.flushChan:
			// 处理刷新请求
			err := bw.flushBatch()
			if req.Callback != nil {
				req.Callback(err)
			}
			select {
			case req.Done <- err:
			default:
			}

		case <-bw.stopChan:
			// 关闭前刷新所有待写入数据
			bw.flushBatch()
			return
		}
	}
}

// addToBatch 添加到批次
func (bw *BatchWriter) addToBatch(req *WriteRequest) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// 分配LSN
	req.Entry.LSN = uint64(bw.lsnManager.AllocateLSN())
	req.Entry.Timestamp = time.Now()

	bw.pendingBatch = append(bw.pendingBatch, req.Entry)

	// 如果有回调，保存起来
	if req.Callback != nil {
		// 批量刷新后统一调用回调
		go func() {
			// 等待下一次刷新
			time.Sleep(bw.batchInterval * 2)
			req.Callback(nil)
		}()
	}

	// 通知完成
	select {
	case req.Done <- nil:
	default:
	}
}

// flushBatch 刷新批次
func (bw *BatchWriter) flushBatch() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if len(bw.pendingBatch) == 0 {
		return nil
	}

	startTime := time.Now()

	// 序列化所有日志条目
	totalSize := 0
	for _, entry := range bw.pendingBatch {
		data, err := entry.Serialize()
		if err != nil {
			return err
		}

		// 检查缓冲区空间
		if bw.currentSize+len(data) > bw.bufferSize {
			// 缓冲区满，先刷新
			if err := bw.flushBuffer(); err != nil {
				return err
			}
		}

		// 复制到缓冲区
		copy(bw.buffer[bw.currentSize:], data)
		bw.currentSize += len(data)
		totalSize += len(data)
	}

	// 刷新缓冲区到磁盘
	if err := bw.flushBuffer(); err != nil {
		return err
	}

	// 更新统计
	bw.stats.TotalFlushes++
	bw.stats.BytesWritten += uint64(totalSize)
	bw.stats.AvgBatchSize = (bw.stats.AvgBatchSize*int(bw.stats.TotalFlushes-1) + len(bw.pendingBatch)) / int(bw.stats.TotalFlushes)
	bw.stats.AvgFlushTime = time.Since(startTime)

	// 清空批次
	bw.pendingBatch = bw.pendingBatch[:0]

	return nil
}

// flushBuffer 刷新缓冲区到磁盘
func (bw *BatchWriter) flushBuffer() error {
	if bw.currentSize == 0 {
		return nil
	}

	// 写入文件
	if _, err := bw.file.Write(bw.buffer[:bw.currentSize]); err != nil {
		return err
	}

	// 同步到磁盘
	if err := bw.file.Sync(); err != nil {
		return err
	}

	// 重置缓冲区
	bw.currentSize = 0

	return nil
}

// Flush 刷新所有待写入数据
func (bw *BatchWriter) Flush() error {
	req := &FlushRequest{
		Force: true,
		Done:  make(chan error, 1),
	}

	select {
	case bw.flushChan <- req:
		return <-req.Done
	case <-time.After(5 * time.Second):
		return fmt.Errorf("flush timeout")
	}
}

// periodicFlushWorker 定期刷新工作协程
func (bw *BatchWriter) periodicFlushWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定期刷新缓冲区
			bw.mu.Lock()
			if bw.currentSize > 0 {
				bw.flushBuffer()
			}
			bw.mu.Unlock()

		case <-bw.stopChan:
			return
		}
	}
}

// Close 关闭批量写入器
func (bw *BatchWriter) Close() error {
	// 发送停止信号
	close(bw.stopChan)

	// 等待协程结束
	time.Sleep(100 * time.Millisecond)

	bw.mu.Lock()
	defer bw.mu.Unlock()

	// 刷新所有待写入数据
	if len(bw.pendingBatch) > 0 {
		bw.flushBatch()
	}
	if bw.currentSize > 0 {
		bw.flushBuffer()
	}

	// 关闭文件
	return bw.file.Close()
}

// GetStats 获取统计信息
func (bw *BatchWriter) GetStats() *BatchWriterStats {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	stats := *bw.stats
	stats.PendingWrites = len(bw.pendingBatch)
	stats.BufferUtilization = float64(bw.currentSize) / float64(bw.bufferSize)

	return &stats
}

// SetBatchInterval 设置批次刷新间隔
func (bw *BatchWriter) SetBatchInterval(interval time.Duration) {
	bw.batchInterval = interval
}

// SetMaxBatchSize 设置最大批次大小
func (bw *BatchWriter) SetMaxBatchSize(size int) {
	bw.maxBatchSize = size
}
