package protocol

import (
	"bytes"
	"compress/zlib"
	_ "encoding/binary"
	"fmt"
	"io"
)

// MySQL压缩协议实现
// 参考: https://dev.mysql.com/doc/internals/en/compressed-packet-header.html

const (
	// 压缩阈值：小于此大小的包不进行压缩
	CompressionThreshold = 50

	// 压缩包头大小：7字节
	CompressedHeaderSize = 7
)

// CompressionHandler 压缩处理器
type CompressionHandler struct {
	enabled bool
}

// NewCompressionHandler 创建压缩处理器
func NewCompressionHandler(enabled bool) *CompressionHandler {
	return &CompressionHandler{
		enabled: enabled,
	}
}

// IsEnabled 是否启用压缩
func (ch *CompressionHandler) IsEnabled() bool {
	return ch.enabled
}

// Enable 启用压缩
func (ch *CompressionHandler) Enable() {
	ch.enabled = true
}

// Disable 禁用压缩
func (ch *CompressionHandler) Disable() {
	ch.enabled = false
}

// CompressPacket 压缩MySQL包
// 压缩包格式：
// - 3字节：压缩后的长度
// - 1字节：序列号
// - 3字节：压缩前的长度（如果未压缩则为0）
// - N字节：压缩后的数据（或未压缩的原始数据）
func (ch *CompressionHandler) CompressPacket(payload []byte, sequenceId byte) ([]byte, error) {
	if !ch.enabled {
		return nil, fmt.Errorf("compression not enabled")
	}

	// 如果payload太小，不进行压缩
	if len(payload) < CompressionThreshold {
		return ch.createUncompressedPacket(payload, sequenceId), nil
	}

	// 使用zlib压缩
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)

	_, err := writer.Write(payload)
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("compression close failed: %w", err)
	}

	compressed := buf.Bytes()

	// 如果压缩后反而更大，使用未压缩的数据
	if len(compressed) >= len(payload) {
		return ch.createUncompressedPacket(payload, sequenceId), nil
	}

	// 创建压缩包
	return ch.createCompressedPacket(compressed, len(payload), sequenceId), nil
}

// DecompressPacket 解压MySQL包
func (ch *CompressionHandler) DecompressPacket(packet []byte) ([]byte, byte, error) {
	if !ch.enabled {
		return nil, 0, fmt.Errorf("compression not enabled")
	}

	if len(packet) < CompressedHeaderSize {
		return nil, 0, fmt.Errorf("compressed packet too short: %d bytes", len(packet))
	}

	// 解析压缩包头
	compressedLength := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
	sequenceId := packet[3]
	uncompressedLength := int(packet[4]) | int(packet[5])<<8 | int(packet[6])<<16

	// 验证长度
	if len(packet) != compressedLength+CompressedHeaderSize {
		return nil, 0, fmt.Errorf("packet length mismatch: expected %d, got %d",
			compressedLength+CompressedHeaderSize, len(packet))
	}

	payload := packet[CompressedHeaderSize:]

	// 如果uncompressedLength为0，说明数据未压缩
	if uncompressedLength == 0 {
		return payload, sequenceId, nil
	}

	// 解压数据
	reader, err := zlib.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, 0, fmt.Errorf("decompression init failed: %w", err)
	}
	defer reader.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	if err != nil {
		return nil, 0, fmt.Errorf("decompression failed: %w", err)
	}

	decompressed := buf.Bytes()

	// 验证解压后的长度
	if len(decompressed) != uncompressedLength {
		return nil, 0, fmt.Errorf("decompressed length mismatch: expected %d, got %d",
			uncompressedLength, len(decompressed))
	}

	return decompressed, sequenceId, nil
}

// createCompressedPacket 创建压缩包
func (ch *CompressionHandler) createCompressedPacket(compressed []byte, originalLength int, sequenceId byte) []byte {
	packet := make([]byte, CompressedHeaderSize+len(compressed))

	// 压缩后的长度（3字节）
	compressedLength := len(compressed)
	packet[0] = byte(compressedLength)
	packet[1] = byte(compressedLength >> 8)
	packet[2] = byte(compressedLength >> 16)

	// 序列号（1字节）
	packet[3] = sequenceId

	// 压缩前的长度（3字节）
	packet[4] = byte(originalLength)
	packet[5] = byte(originalLength >> 8)
	packet[6] = byte(originalLength >> 16)

	// 压缩后的数据
	copy(packet[CompressedHeaderSize:], compressed)

	return packet
}

// createUncompressedPacket 创建未压缩包（但使用压缩包格式）
func (ch *CompressionHandler) createUncompressedPacket(payload []byte, sequenceId byte) []byte {
	packet := make([]byte, CompressedHeaderSize+len(payload))

	// 数据长度（3字节）
	length := len(payload)
	packet[0] = byte(length)
	packet[1] = byte(length >> 8)
	packet[2] = byte(length >> 16)

	// 序列号（1字节）
	packet[3] = sequenceId

	// 压缩前的长度为0表示未压缩（3字节）
	packet[4] = 0
	packet[5] = 0
	packet[6] = 0

	// 原始数据
	copy(packet[CompressedHeaderSize:], payload)

	return packet
}

// CompressMultiplePackets 压缩多个MySQL包
// 可以将多个小包合并后一起压缩，提高压缩率
func (ch *CompressionHandler) CompressMultiplePackets(packets [][]byte, sequenceId byte) ([]byte, error) {
	if !ch.enabled {
		return nil, fmt.Errorf("compression not enabled")
	}

	if len(packets) == 0 {
		return nil, fmt.Errorf("no packets to compress")
	}

	// 合并所有包
	var combined bytes.Buffer
	for _, packet := range packets {
		combined.Write(packet)
	}

	// 压缩合并后的数据
	return ch.CompressPacket(combined.Bytes(), sequenceId)
}

// GetCompressionRatio 获取压缩率
func (ch *CompressionHandler) GetCompressionRatio(original, compressed []byte) float64 {
	if len(original) == 0 {
		return 0
	}
	return float64(len(compressed)) / float64(len(original))
}

// ShouldCompress 判断是否应该压缩
func (ch *CompressionHandler) ShouldCompress(payload []byte) bool {
	return ch.enabled && len(payload) >= CompressionThreshold
}

// ParseCompressedHeader 解析压缩包头
func ParseCompressedHeader(header []byte) (compressedLength, uncompressedLength int, sequenceId byte, err error) {
	if len(header) < CompressedHeaderSize {
		return 0, 0, 0, fmt.Errorf("header too short: %d bytes", len(header))
	}

	compressedLength = int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	sequenceId = header[3]
	uncompressedLength = int(header[4]) | int(header[5])<<8 | int(header[6])<<16

	return compressedLength, uncompressedLength, sequenceId, nil
}

// EncodeCompressedHeader 编码压缩包头
func EncodeCompressedHeader(compressedLength, uncompressedLength int, sequenceId byte) []byte {
	header := make([]byte, CompressedHeaderSize)

	header[0] = byte(compressedLength)
	header[1] = byte(compressedLength >> 8)
	header[2] = byte(compressedLength >> 16)
	header[3] = sequenceId
	header[4] = byte(uncompressedLength)
	header[5] = byte(uncompressedLength >> 8)
	header[6] = byte(uncompressedLength >> 16)

	return header
}

// CompressedPacketReader 压缩包读取器
type CompressedPacketReader struct {
	reader  io.Reader
	handler *CompressionHandler
}

// NewCompressedPacketReader 创建压缩包读取器
func NewCompressedPacketReader(reader io.Reader, handler *CompressionHandler) *CompressedPacketReader {
	return &CompressedPacketReader{
		reader:  reader,
		handler: handler,
	}
}

// ReadPacket 读取并解压包
func (cpr *CompressedPacketReader) ReadPacket() ([]byte, byte, error) {
	// 读取压缩包头
	header := make([]byte, CompressedHeaderSize)
	_, err := io.ReadFull(cpr.reader, header)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read header: %w", err)
	}

	compressedLength, uncompressedLength, sequenceId, err := ParseCompressedHeader(header)
	if err != nil {
		return nil, 0, err
	}

	// 读取payload
	payload := make([]byte, compressedLength)
	_, err = io.ReadFull(cpr.reader, payload)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read payload: %w", err)
	}

	// 如果未压缩，直接返回
	if uncompressedLength == 0 {
		return payload, sequenceId, nil
	}

	// 解压
	reader, err := zlib.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, 0, fmt.Errorf("decompression init failed: %w", err)
	}
	defer reader.Close()

	decompressed := make([]byte, uncompressedLength)
	_, err = io.ReadFull(reader, decompressed)
	if err != nil {
		return nil, 0, fmt.Errorf("decompression failed: %w", err)
	}

	return decompressed, sequenceId, nil
}

// CompressedPacketWriter 压缩包写入器
type CompressedPacketWriter struct {
	writer  io.Writer
	handler *CompressionHandler
}

// NewCompressedPacketWriter 创建压缩包写入器
func NewCompressedPacketWriter(writer io.Writer, handler *CompressionHandler) *CompressedPacketWriter {
	return &CompressedPacketWriter{
		writer:  writer,
		handler: handler,
	}
}

// WritePacket 压缩并写入包
func (cpw *CompressedPacketWriter) WritePacket(payload []byte, sequenceId byte) error {
	compressed, err := cpw.handler.CompressPacket(payload, sequenceId)
	if err != nil {
		return fmt.Errorf("compression failed: %w", err)
	}

	_, err = cpw.writer.Write(compressed)
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	return nil
}

// CompressionStats 压缩统计信息
type CompressionStats struct {
	TotalPackets      int64
	CompressedPackets int64
	TotalBytesIn      int64
	TotalBytesOut     int64
	CompressionRatio  float64
}

// UpdateStats 更新统计信息
func (cs *CompressionStats) UpdateStats(originalSize, compressedSize int, wasCompressed bool) {
	cs.TotalPackets++
	cs.TotalBytesIn += int64(originalSize)
	cs.TotalBytesOut += int64(compressedSize)

	if wasCompressed {
		cs.CompressedPackets++
	}

	if cs.TotalBytesIn > 0 {
		cs.CompressionRatio = float64(cs.TotalBytesOut) / float64(cs.TotalBytesIn)
	}
}

// GetCompressionPercentage 获取压缩包占比
func (cs *CompressionStats) GetCompressionPercentage() float64 {
	if cs.TotalPackets == 0 {
		return 0
	}
	return float64(cs.CompressedPackets) / float64(cs.TotalPackets) * 100
}

// GetBytesSaved 获取节省的字节数
func (cs *CompressionStats) GetBytesSaved() int64 {
	return cs.TotalBytesIn - cs.TotalBytesOut
}
