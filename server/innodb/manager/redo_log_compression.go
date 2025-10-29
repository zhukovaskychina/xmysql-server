package manager

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
)

// ============ LOG-003.2: Redo日志压缩算法实现 ============

// CompressionAlgorithm 压缩算法类型
type CompressionAlgorithm int

const (
	COMPRESS_NONE CompressionAlgorithm = iota // 不压缩
	COMPRESS_GZIP                             // GZIP压缩
	COMPRESS_ZLIB                             // ZLIB压缩
	COMPRESS_LZ4                              // LZ4压缩（高性能）
)

// RedoLogCompressor Redo日志压缩器
type RedoLogCompressor struct {
	algorithm CompressionAlgorithm // 压缩算法
	level     int                  // 压缩级别 (1-9)
	minSize   int                  // 最小压缩大小（小于此值不压缩）

	// 统计信息
	stats *CompressionStats
}

// CompressionStats 压缩统计信息
type CompressionStats struct {
	TotalCompressed   uint64  `json:"total_compressed"`   // 总压缩次数
	TotalDecompressed uint64  `json:"total_decompressed"` // 总解压次数
	BytesBeforeComp   uint64  `json:"bytes_before_comp"`  // 压缩前字节数
	BytesAfterComp    uint64  `json:"bytes_after_comp"`   // 压缩后字节数
	CompressionRatio  float64 `json:"compression_ratio"`  // 压缩率
	AvgCompTime       int64   `json:"avg_comp_time_ns"`   // 平均压缩时间(纳秒)
	AvgDecompTime     int64   `json:"avg_decomp_time_ns"` // 平均解压时间(纳秒)
}

// NewRedoLogCompressor 创建Redo日志压缩器
func NewRedoLogCompressor(algorithm CompressionAlgorithm, level int) *RedoLogCompressor {
	if level < 1 || level > 9 {
		level = 6 // 默认压缩级别
	}

	return &RedoLogCompressor{
		algorithm: algorithm,
		level:     level,
		minSize:   128, // 默认128字节以下不压缩
		stats:     &CompressionStats{},
	}
}

// Compress 压缩数据
func (c *RedoLogCompressor) Compress(data []byte) ([]byte, error) {
	// 如果数据太小，不压缩
	if len(data) < c.minSize {
		return c.wrapUncompressed(data), nil
	}

	var compressed []byte
	var err error

	switch c.algorithm {
	case COMPRESS_NONE:
		compressed = data

	case COMPRESS_GZIP:
		compressed, err = c.compressGzip(data)

	case COMPRESS_ZLIB:
		compressed, err = c.compressZlib(data)

	case COMPRESS_LZ4:
		// TODO: 实现LZ4压缩
		compressed, err = c.compressGzip(data) // 临时使用GZIP

	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %d", c.algorithm)
	}

	if err != nil {
		return nil, err
	}

	// 如果压缩后反而更大，返回原始数据
	if len(compressed) >= len(data) {
		return c.wrapUncompressed(data), nil
	}

	// 更新统计
	c.stats.TotalCompressed++
	c.stats.BytesBeforeComp += uint64(len(data))
	c.stats.BytesAfterComp += uint64(len(compressed))
	c.updateCompressionRatio()

	return c.wrapCompressed(compressed, c.algorithm), nil
}

// Decompress 解压数据
func (c *RedoLogCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("data too small")
	}

	// 读取压缩类型标志
	algorithm := CompressionAlgorithm(data[0])
	compressedData := data[1:]

	var decompressed []byte
	var err error

	switch algorithm {
	case COMPRESS_NONE:
		decompressed = compressedData

	case COMPRESS_GZIP:
		decompressed, err = c.decompressGzip(compressedData)

	case COMPRESS_ZLIB:
		decompressed, err = c.decompressZlib(compressedData)

	case COMPRESS_LZ4:
		// TODO: 实现LZ4解压
		decompressed, err = c.decompressGzip(compressedData) // 临时使用GZIP

	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %d", algorithm)
	}

	if err != nil {
		return nil, err
	}

	// 更新统计
	c.stats.TotalDecompressed++

	return decompressed, nil
}

// compressGzip GZIP压缩
func (c *RedoLogCompressor) compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, c.level)
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompressGzip GZIP解压
func (c *RedoLogCompressor) decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// compressZlib ZLIB压缩
func (c *RedoLogCompressor) compressZlib(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := zlib.NewWriterLevel(&buf, c.level)
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompressZlib ZLIB解压
func (c *RedoLogCompressor) decompressZlib(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// wrapCompressed 包装压缩数据（添加压缩类型标志）
func (c *RedoLogCompressor) wrapCompressed(data []byte, algorithm CompressionAlgorithm) []byte {
	result := make([]byte, len(data)+1)
	result[0] = byte(algorithm)
	copy(result[1:], data)
	return result
}

// wrapUncompressed 包装未压缩数据
func (c *RedoLogCompressor) wrapUncompressed(data []byte) []byte {
	result := make([]byte, len(data)+1)
	result[0] = byte(COMPRESS_NONE)
	copy(result[1:], data)
	return result
}

// updateCompressionRatio 更新压缩率
func (c *RedoLogCompressor) updateCompressionRatio() {
	if c.stats.BytesBeforeComp > 0 {
		c.stats.CompressionRatio = float64(c.stats.BytesAfterComp) / float64(c.stats.BytesBeforeComp)
	}
}

// GetStats 获取压缩统计信息
func (c *RedoLogCompressor) GetStats() *CompressionStats {
	stats := *c.stats
	return &stats
}

// SetMinSize 设置最小压缩大小
func (c *RedoLogCompressor) SetMinSize(size int) {
	c.minSize = size
}

// ============ 批量压缩优化 ============

// BatchCompressor 批量压缩器
type BatchCompressor struct {
	compressor *RedoLogCompressor
	batchSize  int           // 批量大小
	buffer     [][]byte      // 数据缓冲区
	resultChan chan []byte   // 结果通道
	stopChan   chan struct{} // 停止信号
}

// NewBatchCompressor 创建批量压缩器
func NewBatchCompressor(algorithm CompressionAlgorithm, level, batchSize int) *BatchCompressor {
	bc := &BatchCompressor{
		compressor: NewRedoLogCompressor(algorithm, level),
		batchSize:  batchSize,
		buffer:     make([][]byte, 0, batchSize),
		resultChan: make(chan []byte, batchSize),
		stopChan:   make(chan struct{}),
	}

	// 启动批量压缩协程
	go bc.batchCompressWorker()

	return bc
}

// Add 添加数据到批量压缩队列
func (bc *BatchCompressor) Add(data []byte) {
	bc.buffer = append(bc.buffer, data)

	// 达到批量大小，触发压缩
	if len(bc.buffer) >= bc.batchSize {
		bc.flush()
	}
}

// flush 刷新缓冲区
func (bc *BatchCompressor) flush() {
	if len(bc.buffer) == 0 {
		return
	}

	// 合并数据
	var totalSize int
	for _, data := range bc.buffer {
		totalSize += len(data)
	}

	merged := make([]byte, 0, totalSize)
	for _, data := range bc.buffer {
		merged = append(merged, data...)
	}

	// 压缩
	compressed, err := bc.compressor.Compress(merged)
	if err == nil {
		bc.resultChan <- compressed
	}

	// 清空缓冲区
	bc.buffer = bc.buffer[:0]
}

// batchCompressWorker 批量压缩工作协程
func (bc *BatchCompressor) batchCompressWorker() {
	// 定期刷新缓冲区
	// ticker := time.NewTicker(100 * time.Millisecond)
	// defer ticker.Stop()

	for {
		select {
		case <-bc.stopChan:
			bc.flush()
			return
		}
	}
}

// GetResult 获取压缩结果
func (bc *BatchCompressor) GetResult() <-chan []byte {
	return bc.resultChan
}

// Close 关闭批量压缩器
func (bc *BatchCompressor) Close() {
	close(bc.stopChan)
}

// ============ 日志块压缩 ============

// LogBlock 日志块（用于批量压缩）
type LogBlock struct {
	BlockID    uint64   // 块ID
	StartLSN   uint64   // 起始LSN
	EndLSN     uint64   // 结束LSN
	LogEntries [][]byte // 日志条目列表
	Compressed bool     // 是否已压缩
	Size       int      // 总大小
}

// NewLogBlock 创建日志块
func NewLogBlock(blockID, startLSN uint64) *LogBlock {
	return &LogBlock{
		BlockID:    blockID,
		StartLSN:   startLSN,
		LogEntries: make([][]byte, 0, 100),
	}
}

// AddEntry 添加日志条目
func (lb *LogBlock) AddEntry(entry []byte) {
	lb.LogEntries = append(lb.LogEntries, entry)
	lb.Size += len(entry)
}

// Compress 压缩日志块
func (lb *LogBlock) Compress(compressor *RedoLogCompressor) ([]byte, error) {
	// 序列化块头
	buf := new(bytes.Buffer)

	// 写入块头
	header := LogBlockHeader{
		BlockID:    lb.BlockID,
		StartLSN:   lb.StartLSN,
		EndLSN:     lb.EndLSN,
		EntryCount: uint32(len(lb.LogEntries)),
		TotalSize:  uint32(lb.Size),
	}

	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		return nil, err
	}

	// 合并所有日志条目
	for _, entry := range lb.LogEntries {
		buf.Write(entry)
	}

	// 压缩
	compressed, err := compressor.Compress(buf.Bytes())
	if err != nil {
		return nil, err
	}

	lb.Compressed = true
	return compressed, nil
}

// LogBlockHeader 日志块头部
type LogBlockHeader struct {
	BlockID    uint64 // 块ID (8字节)
	StartLSN   uint64 // 起始LSN (8字节)
	EndLSN     uint64 // 结束LSN (8字节)
	EntryCount uint32 // 条目数量 (4字节)
	TotalSize  uint32 // 总大小 (4字节)
	Checksum   uint32 // 校验和 (4字节)
	Reserved   uint32 // 保留 (4字节)
}
