package net

import (
	"errors"
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
	"github.com/zhukovaskychina/xmysql-server/util"
)

// MySQLPkgHandler 实现 Getty 的 ReadWriter，用于编解码 MySQL 协议数据包
// 包格式: 3 字节小端长度 + 1 字节序号 + payload
// Read 支持粘包/半包，一次只返回一个 *MySQLPackage
// Write 支持 *MySQLPackage 和 []byte（已经编码好的原始 MySQL 包）
type MySQLPkgHandler struct{}

const (
	mysqlCompressionEnabledKey  = "mysql_compression_enabled"
	mysqlCompressionWriteSeqKey = "mysql_compression_write_sequence"
	mysqlCompressionPendingKey  = "mysql_compression_pending_packets"
)

// NewMySQLPkgHandler 创建一个新的 MySQLPkgHandler 实例
func NewMySQLPkgHandler() ReadWriter {
	return &MySQLPkgHandler{}
}

// Read 从网络流中解析一个 MySQL 协议包
// 行为约定：
//   - 半包：返回 (nil, 0, nil)
//   - 粘包：一次只返回一个完整包，并返回消费的字节数
//   - 错误：返回 (nil, 0, error)
//
// data 为当前缓冲区中的所有可读字节，不保证只包含一个包
func (h *MySQLPkgHandler) Read(session Session, data []byte) (interface{}, int, error) {
	if mysqlCompressionEnabled(session) {
		return h.readCompressed(session, data)
	}
	return h.readPlain(data)
}

func (h *MySQLPkgHandler) readPlain(data []byte) (interface{}, int, error) {
	// 至少需要 4 字节头部: 3 字节长度 + 1 字节序号
	if len(data) < 4 {
		// 半包：头部都不完整
		return nil, 0, nil
	}

	// 使用 util.ReadUB3 解码 3 字节小端长度
	cursor, payloadLen := util.ReadUB3(data, 0)
	_ = cursor // 固定为 3，这里无需使用

	totalLen := int(payloadLen) + 4 // 包括 3 字节长度 + 1 字节序号
	if totalLen < 4 {
		// 协议非法，长度下溢
		logger.Errorf("[MySQLPkgHandler.Read] 非法的包长度: %d", payloadLen)
		return nil, 0, ErrIllegalMagic
	}

	if len(data) < totalLen {
		// 半包：头部已完整，但 payload 未收全
		return nil, 0, nil
	}

	// 构造 MySQLPackage，头部和 Body 原样交付给上层
	headerLenBytes := make([]byte, 3)
	copy(headerLenBytes, data[0:3])

	pkt := &MySQLPackage{
		Header: MySQLPkgHeader{
			PacketLength: headerLenBytes,
			PacketId:     data[3],
		},
		Body: make([]byte, payloadLen),
	}
	copy(pkt.Body, data[4:totalLen])

	logger.Debugf("[MySQLPkgHandler.Read] 解码 MySQL 包: payloadLen=%d, seq=%d", payloadLen, pkt.Header.PacketId)

	// 一次只返回一个完整包，告知 Getty 消费了 totalLen 字节
	return pkt, totalLen, nil
}

// ReadPending drains ordinary MySQL packets grouped in one compressed
// transport frame. The session loop consumes the compressed frame once and
// then calls this hook before reading the next TCP frame.
func (h *MySQLPkgHandler) ReadPending(session Session) (interface{}, bool, error) {
	if session == nil {
		return nil, false, nil
	}
	pending, _ := session.GetAttribute(mysqlCompressionPendingKey).([]*MySQLPackage)
	if len(pending) == 0 {
		return nil, false, nil
	}
	pkg := pending[0]
	if len(pending) == 1 {
		session.RemoveAttribute(mysqlCompressionPendingKey)
	} else {
		session.SetAttribute(mysqlCompressionPendingKey, pending[1:])
	}
	return pkg, true, nil
}

func (h *MySQLPkgHandler) readCompressed(session Session, data []byte) (interface{}, int, error) {
	if len(data) < protocol.CompressedHeaderSize {
		return nil, 0, nil
	}
	compressedLength, _, _, err := protocol.ParseCompressedHeader(data[:protocol.CompressedHeaderSize])
	if err != nil {
		return nil, 0, err
	}
	totalLength := protocol.CompressedHeaderSize + compressedLength
	if totalLength < protocol.CompressedHeaderSize {
		return nil, 0, fmt.Errorf("compressed packet length overflow")
	}
	if len(data) < totalLength {
		return nil, 0, nil
	}
	decompressed, _, err := protocol.NewCompressionHandler(true).DecompressPacket(data[:totalLength])
	if err != nil {
		return nil, 0, err
	}
	packets, err := decodePlainMySQLPackets(decompressed)
	if err != nil {
		return nil, 0, err
	}
	if len(packets) == 0 {
		return nil, 0, fmt.Errorf("compressed packet contains no MySQL packets")
	}
	if packets[0].Header.PacketId == 0 && len(packets[0].Body) > 0 {
		session.SetAttribute(mysqlCompressionWriteSeqKey, uint8(0))
	}
	if len(packets) > 1 {
		session.SetAttribute(mysqlCompressionPendingKey, packets[1:])
	}
	return packets[0], totalLength, nil
}

func decodePlainMySQLPackets(data []byte) ([]*MySQLPackage, error) {
	packets := make([]*MySQLPackage, 0, 1)
	for offset := 0; offset < len(data); {
		if len(data)-offset < 4 {
			return nil, fmt.Errorf("compressed payload contains incomplete MySQL header")
		}
		_, payloadLen := util.ReadUB3(data, offset)
		totalLength := int(payloadLen) + 4
		if totalLength < 4 || len(data)-offset < totalLength {
			return nil, fmt.Errorf("compressed payload contains incomplete MySQL packet")
		}
		header := []byte{data[offset], data[offset+1], data[offset+2]}
		body := make([]byte, int(payloadLen))
		copy(body, data[offset+4:offset+totalLength])
		packets = append(packets, &MySQLPackage{
			Header: MySQLPkgHeader{PacketLength: header, PacketId: data[offset+3]},
			Body:   body,
		})
		offset += totalLength
	}
	return packets, nil
}

func mysqlCompressionEnabled(session Session) bool {
	if session == nil {
		return false
	}
	enabled, _ := session.GetAttribute(mysqlCompressionEnabledKey).(bool)
	return enabled
}

func compressMySQLTransportPayload(session Session, payload []byte) ([]byte, error) {
	if !mysqlCompressionEnabled(session) {
		return payload, nil
	}
	sequence := uint8(0)
	if raw, ok := session.GetAttribute(mysqlCompressionWriteSeqKey).(uint8); ok {
		sequence = raw
	}
	compressed, err := protocol.NewCompressionHandler(true).CompressPacket(payload, sequence)
	if err != nil {
		return nil, err
	}
	session.SetAttribute(mysqlCompressionWriteSeqKey, sequence+1)
	return compressed, nil
}

// Write 将业务层的包编码为字节流
// 支持：
//   - *MySQLPackage: 根据 Header.PacketLength/PacketId 和 Body 进行编码
//   - []byte: 业务层已经编码好的原始 MySQL 包，直接透传
func (h *MySQLPkgHandler) Write(session Session, pkg interface{}) ([]byte, error) {
	switch v := pkg.(type) {
	case *MySQLPackage:
		buf, err := v.Marshal()
		if err != nil {
			logger.Errorf("[MySQLPkgHandler.Write] Marshal MySQLPackage 失败: %v", err)
			return nil, err
		}
		return buf.Bytes(), nil

	case []byte:
		// 已经编码好的普通 MySQL 包；传输压缩在 Session 发送层统一处理。
		return v, nil

	default:
		logger.Errorf("[MySQLPkgHandler.Write] 不支持的 pkg 类型: %T", pkg)
		return nil, errors.New("MySQLPkgHandler.Write: unsupported pkg type, expect *MySQLPackage or []byte")
	}
}
