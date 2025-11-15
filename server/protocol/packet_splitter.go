package protocol

import (
	_ "encoding/binary"
	"fmt"
)

// MySQL协议规定单个包的最大长度为 0xFFFFFF (16MB - 1)
const (
	MaxPacketSize = 0xFFFFFF // 16777215 bytes
)

// PacketSplitter 大包分片处理器
type PacketSplitter struct{}

// NewPacketSplitter 创建包分片处理器
func NewPacketSplitter() *PacketSplitter {
	return &PacketSplitter{}
}

// SplitPacket 将大包分片为多个MySQL包
// 当payload大于16MB时，需要分成多个包发送
// 每个包最大16MB，最后一个包可能小于16MB
func (ps *PacketSplitter) SplitPacket(payload []byte, startSequenceId byte) [][]byte {
	if len(payload) <= MaxPacketSize {
		// 不需要分片，直接返回单个包
		return [][]byte{ps.addPacketHeader(payload, startSequenceId)}
	}

	// 需要分片
	var packets [][]byte
	sequenceId := startSequenceId
	offset := 0

	for offset < len(payload) {
		// 计算当前片段的大小
		chunkSize := MaxPacketSize
		if offset+chunkSize > len(payload) {
			chunkSize = len(payload) - offset
		}

		// 提取当前片段
		chunk := payload[offset : offset+chunkSize]

		// 添加包头
		packet := ps.addPacketHeader(chunk, sequenceId)
		packets = append(packets, packet)

		offset += chunkSize
		sequenceId++

		// 如果当前片段正好是MaxPacketSize，需要发送一个空包表示结束
		if chunkSize == MaxPacketSize && offset >= len(payload) {
			emptyPacket := ps.addPacketHeader([]byte{}, sequenceId)
			packets = append(packets, emptyPacket)
		}
	}

	return packets
}

// MergePackets 合并多个分片包
// 当接收到长度为MaxPacketSize的包时，需要继续接收下一个包
func (ps *PacketSplitter) MergePackets(packets [][]byte) ([]byte, error) {
	if len(packets) == 0 {
		return nil, fmt.Errorf("no packets to merge")
	}

	var result []byte

	for i, packet := range packets {
		if len(packet) < 4 {
			return nil, fmt.Errorf("packet %d too short: %d bytes", i, len(packet))
		}

		// 解析包头
		length := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
		// sequenceId := packet[3]

		// 验证包长度
		if len(packet) != length+4 {
			return nil, fmt.Errorf("packet %d length mismatch: header=%d, actual=%d", i, length, len(packet)-4)
		}

		// 提取payload
		payload := packet[4:]
		result = append(result, payload...)

		// 如果包长度小于MaxPacketSize，说明这是最后一个包
		if length < MaxPacketSize {
			break
		}
	}

	return result, nil
}

// addPacketHeader 添加MySQL包头
func (ps *PacketSplitter) addPacketHeader(payload []byte, sequenceId byte) []byte {
	length := len(payload)
	header := make([]byte, 4)

	// 包长度 (3字节，小端序)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)

	// 序列号
	header[3] = sequenceId

	return append(header, payload...)
}

// ReadPacketWithSplit 读取可能分片的包
// 这个函数会自动处理分片包的读取和合并
func (ps *PacketSplitter) ReadPacketWithSplit(reader PacketReader) ([]byte, byte, error) {
	var packets [][]byte
	var lastSequenceId byte

	for {
		// 读取一个包
		packet, err := reader.ReadPacket()
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read packet: %w", err)
		}

		if len(packet) < 4 {
			return nil, 0, fmt.Errorf("packet too short: %d bytes", len(packet))
		}

		// 解析包头
		length := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
		lastSequenceId = packet[3]

		packets = append(packets, packet)

		// 如果包长度小于MaxPacketSize，说明这是最后一个包
		if length < MaxPacketSize {
			break
		}
	}

	// 合并所有包
	payload, err := ps.MergePackets(packets)
	if err != nil {
		return nil, 0, err
	}

	return payload, lastSequenceId, nil
}

// PacketReader 包读取器接口
type PacketReader interface {
	ReadPacket() ([]byte, error)
}

// EncodePacketWithSplit 编码包并自动处理分片
func EncodePacketWithSplit(payload []byte, sequenceId byte) [][]byte {
	splitter := NewPacketSplitter()
	return splitter.SplitPacket(payload, sequenceId)
}

// WritePacketsWithSplit 写入可能需要分片的包
func WritePacketsWithSplit(writer PacketWriter, payload []byte, sequenceId byte) error {
	packets := EncodePacketWithSplit(payload, sequenceId)

	for _, packet := range packets {
		if err := writer.WritePacket(packet); err != nil {
			return fmt.Errorf("failed to write packet: %w", err)
		}
	}

	return nil
}

// PacketWriter 包写入器接口
type PacketWriter interface {
	WritePacket(packet []byte) error
}

// GetPacketLength 获取包的payload长度（不包括包头）
func GetPacketLength(packet []byte) (int, error) {
	if len(packet) < 4 {
		return 0, fmt.Errorf("packet too short")
	}

	length := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
	return length, nil
}

// GetPacketSequenceId 获取包的序列号
func GetPacketSequenceId(packet []byte) (byte, error) {
	if len(packet) < 4 {
		return 0, fmt.Errorf("packet too short")
	}

	return packet[3], nil
}

// ValidatePacket 验证包的完整性
func ValidatePacket(packet []byte) error {
	if len(packet) < 4 {
		return fmt.Errorf("packet too short: %d bytes", len(packet))
	}

	length := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16

	if len(packet) != length+4 {
		return fmt.Errorf("packet length mismatch: header=%d, actual=%d", length, len(packet)-4)
	}

	if length > MaxPacketSize {
		return fmt.Errorf("packet length exceeds maximum: %d > %d", length, MaxPacketSize)
	}

	return nil
}

// CalculatePacketCount 计算需要多少个包来传输指定大小的payload
func CalculatePacketCount(payloadSize int) int {
	if payloadSize <= MaxPacketSize {
		return 1
	}

	count := payloadSize / MaxPacketSize
	if payloadSize%MaxPacketSize != 0 {
		count++
	}

	// 如果最后一个包正好是MaxPacketSize，需要额外的空包
	if payloadSize%MaxPacketSize == 0 {
		count++
	}

	return count
}

// CreateLargePacketHeader 为大包创建包头信息
func CreateLargePacketHeader(totalSize int, startSequenceId byte) []PacketHeaderInfo {
	var headers []PacketHeaderInfo

	sequenceId := startSequenceId
	offset := 0

	for offset < totalSize {
		size := MaxPacketSize
		if offset+size > totalSize {
			size = totalSize - offset
		}

		headers = append(headers, PacketHeaderInfo{
			Length:     size,
			SequenceId: sequenceId,
			Offset:     offset,
		})

		offset += size
		sequenceId++

		// 如果正好是MaxPacketSize，需要空包
		if size == MaxPacketSize && offset >= totalSize {
			headers = append(headers, PacketHeaderInfo{
				Length:     0,
				SequenceId: sequenceId,
				Offset:     offset,
			})
		}
	}

	return headers
}

// PacketHeaderInfo 包头信息
type PacketHeaderInfo struct {
	Length     int  // payload长度
	SequenceId byte // 序列号
	Offset     int  // 在总payload中的偏移
}

// EncodePacketHeader 编码包头
func EncodePacketHeader(length int, sequenceId byte) []byte {
	header := make([]byte, 4)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = sequenceId
	return header
}

// DecodePacketHeader 解码包头
func DecodePacketHeader(header []byte) (length int, sequenceId byte, err error) {
	if len(header) < 4 {
		return 0, 0, fmt.Errorf("header too short: %d bytes", len(header))
	}

	length = int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	sequenceId = header[3]

	return length, sequenceId, nil
}
