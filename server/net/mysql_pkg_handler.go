package net

import (
	"errors"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/util"
)

// MySQLPkgHandler 实现 Getty 的 ReadWriter，用于编解码 MySQL 协议数据包
// 包格式: 3 字节小端长度 + 1 字节序号 + payload
// Read 支持粘包/半包，一次只返回一个 *MySQLPackage
// Write 支持 *MySQLPackage 和 []byte（已经编码好的原始 MySQL 包）
type MySQLPkgHandler struct{}

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
		// 已经编码好的 MySQL 包，直接下发
		return v, nil

	default:
		logger.Errorf("[MySQLPkgHandler.Write] 不支持的 pkg 类型: %T", pkg)
		return nil, errors.New("MySQLPkgHandler.Write: unsupported pkg type, expect *MySQLPackage or []byte")
	}
}
