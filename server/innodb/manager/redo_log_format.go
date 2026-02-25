package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// RedoLogHeader Redo日志头部
// 格式：LSN(8) + TxID(8) + Type(1) + PageID(8) + Offset(4) + DataLen(4) + Checksum(4) = 37字节
type RedoLogHeader struct {
	LSN      uint64 // 日志序列号
	TrxID    int64  // 事务ID
	Type     uint8  // 操作类型
	PageID   uint64 // 页面ID
	Offset   uint32 // 页内偏移
	DataLen  uint32 // 数据长度
	Checksum uint32 // 校验和
}

// RedoLogFormatter Redo日志格式化器
type RedoLogFormatter struct {
	headerSize int // 头部大小（固定37字节）
}

// NewRedoLogFormatter 创建Redo日志格式化器
func NewRedoLogFormatter() *RedoLogFormatter {
	return &RedoLogFormatter{
		headerSize: 37, // LSN(8) + TxID(8) + Type(1) + PageID(8) + Offset(4) + DataLen(4) + Checksum(4)
	}
}

// FormatInsertLog 格式化INSERT日志
// 包含：旧值为空，新值为完整记录
func (f *RedoLogFormatter) FormatInsertLog(
	lsn uint64,
	txID int64,
	pageID uint64,
	offset uint32,
	recordData []byte,
) ([]byte, error) {
	return f.formatLog(&RedoLogEntry{
		LSN:    lsn,
		TrxID:  txID,
		Type:   LOG_TYPE_INSERT,
		PageID: pageID,
		Data:   recordData,
	}, offset)
}

// FormatUpdateLog 格式化UPDATE日志
// 包含：旧值和新值
func (f *RedoLogFormatter) FormatUpdateLog(
	lsn uint64,
	txID int64,
	pageID uint64,
	offset uint32,
	oldData []byte,
	newData []byte,
) ([]byte, error) {
	// UPDATE日志格式：[旧值长度(4字节)][旧值][新值]
	buffer := new(bytes.Buffer)

	// 写入旧值长度
	oldLen := uint32(len(oldData))
	if err := binary.Write(buffer, binary.BigEndian, oldLen); err != nil {
		return nil, err
	}

	// 写入旧值
	if _, err := buffer.Write(oldData); err != nil {
		return nil, err
	}

	// 写入新值
	if _, err := buffer.Write(newData); err != nil {
		return nil, err
	}

	return f.formatLog(&RedoLogEntry{
		LSN:    lsn,
		TrxID:  txID,
		Type:   LOG_TYPE_UPDATE,
		PageID: pageID,
		Data:   buffer.Bytes(),
	}, offset)
}

// FormatDeleteLog 格式化DELETE日志
// 包含：被删除的完整记录（用于回滚）
func (f *RedoLogFormatter) FormatDeleteLog(
	lsn uint64,
	txID int64,
	pageID uint64,
	offset uint32,
	recordData []byte,
) ([]byte, error) {
	return f.formatLog(&RedoLogEntry{
		LSN:    lsn,
		TrxID:  txID,
		Type:   LOG_TYPE_DELETE,
		PageID: pageID,
		Data:   recordData,
	}, offset)
}

// FormatPageLog 格式化页面操作日志
func (f *RedoLogFormatter) FormatPageLog(
	lsn uint64,
	txID int64,
	logType uint8,
	pageID uint64,
	pageData []byte,
) ([]byte, error) {
	return f.formatLog(&RedoLogEntry{
		LSN:    lsn,
		TrxID:  txID,
		Type:   logType,
		PageID: pageID,
		Data:   pageData,
	}, 0)
}

// FormatTransactionLog 格式化事务操作日志
func (f *RedoLogFormatter) FormatTransactionLog(
	lsn uint64,
	txID int64,
	logType uint8,
) ([]byte, error) {
	return f.formatLog(&RedoLogEntry{
		LSN:    lsn,
		TrxID:  txID,
		Type:   logType,
		PageID: 0,
		Data:   nil,
	}, 0)
}

// formatLog 内部统一格式化方法
func (f *RedoLogFormatter) formatLog(entry *RedoLogEntry, offset uint32) ([]byte, error) {
	buffer := new(bytes.Buffer)

	// 计算数据长度
	dataLen := uint32(len(entry.Data))

	// 创建头部
	header := &RedoLogHeader{
		LSN:     entry.LSN,
		TrxID:   entry.TrxID,
		Type:    entry.Type,
		PageID:  entry.PageID,
		Offset:  offset,
		DataLen: dataLen,
	}

	// 计算校验和（不包括校验和字段本身）
	header.Checksum = f.calculateChecksum(header, entry.Data)

	// 写入头部
	if err := f.writeHeader(buffer, header); err != nil {
		return nil, err
	}

	// 写入数据
	if dataLen > 0 {
		if _, err := buffer.Write(entry.Data); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

// writeHeader 写入日志头部
func (f *RedoLogFormatter) writeHeader(buffer *bytes.Buffer, header *RedoLogHeader) error {
	if err := binary.Write(buffer, binary.BigEndian, header.LSN); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.TrxID); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Type); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.PageID); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Offset); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.DataLen); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Checksum); err != nil {
		return err
	}
	return nil
}

// ParseLog 解析日志记录
func (f *RedoLogFormatter) ParseLog(data []byte) (*RedoLogEntry, *RedoLogHeader, error) {
	if len(data) < f.headerSize {
		return nil, nil, fmt.Errorf("日志记录太短：需要至少%d字节，实际%d字节", f.headerSize, len(data))
	}

	reader := bytes.NewReader(data)
	header := &RedoLogHeader{}

	// 读取头部
	if err := binary.Read(reader, binary.BigEndian, &header.LSN); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.TrxID); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.Type); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.PageID); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.Offset); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.DataLen); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.Checksum); err != nil {
		return nil, nil, err
	}

	// 验证数据长度
	if len(data) < f.headerSize+int(header.DataLen) {
		return nil, nil, fmt.Errorf("数据不完整：期望%d字节，实际%d字节",
			f.headerSize+int(header.DataLen), len(data))
	}

	// 读取数据
	logData := data[f.headerSize : f.headerSize+int(header.DataLen)]

	// 验证校验和
	expectedChecksum := f.calculateChecksum(header, logData)
	if expectedChecksum != header.Checksum {
		return nil, nil, fmt.Errorf("校验和不匹配：期望%d，实际%d", expectedChecksum, header.Checksum)
	}

	// 创建日志条目
	entry := &RedoLogEntry{
		LSN:    header.LSN,
		TrxID:  header.TrxID,
		Type:   header.Type,
		PageID: header.PageID,
		Data:   logData,
	}

	return entry, header, nil
}

// ParseUpdateLog 解析UPDATE日志，分离旧值和新值
func (f *RedoLogFormatter) ParseUpdateLog(data []byte) (oldData, newData []byte, err error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("UPDATE日志数据太短")
	}

	reader := bytes.NewReader(data)

	// 读取旧值长度
	var oldLen uint32
	if err := binary.Read(reader, binary.BigEndian, &oldLen); err != nil {
		return nil, nil, err
	}

	// 验证长度
	if len(data) < 4+int(oldLen) {
		return nil, nil, fmt.Errorf("UPDATE日志数据不完整")
	}

	// 读取旧值
	oldData = data[4 : 4+oldLen]

	// 读取新值
	newData = data[4+oldLen:]

	return oldData, newData, nil
}

// calculateChecksum 计算校验和
func (f *RedoLogFormatter) calculateChecksum(header *RedoLogHeader, data []byte) uint32 {
	// 使用CRC32计算校验和
	buffer := new(bytes.Buffer)

	// 包含头部字段（不包括校验和本身）
	binary.Write(buffer, binary.BigEndian, header.LSN)
	binary.Write(buffer, binary.BigEndian, header.TrxID)
	binary.Write(buffer, binary.BigEndian, header.Type)
	binary.Write(buffer, binary.BigEndian, header.PageID)
	binary.Write(buffer, binary.BigEndian, header.Offset)
	binary.Write(buffer, binary.BigEndian, header.DataLen)

	// 包含数据
	if len(data) > 0 {
		buffer.Write(data)
	}

	return crc32.ChecksumIEEE(buffer.Bytes())
}

// GetHeaderSize 获取头部大小
func (f *RedoLogFormatter) GetHeaderSize() int {
	return f.headerSize
}

// ValidateLogType 验证日志类型是否有效
func (f *RedoLogFormatter) ValidateLogType(logType uint8) bool {
	switch logType {
	case LOG_TYPE_INSERT, LOG_TYPE_UPDATE, LOG_TYPE_DELETE, LOG_TYPE_COMPENSATE,
		LOG_TYPE_PAGE_CREATE, LOG_TYPE_PAGE_DELETE, LOG_TYPE_PAGE_MODIFY,
		LOG_TYPE_PAGE_SPLIT, LOG_TYPE_PAGE_MERGE,
		LOG_TYPE_INDEX_INSERT, LOG_TYPE_INDEX_DELETE, LOG_TYPE_INDEX_UPDATE,
		LOG_TYPE_TXN_BEGIN, LOG_TYPE_TXN_COMMIT, LOG_TYPE_TXN_ROLLBACK, LOG_TYPE_TXN_SAVEPOINT,
		LOG_TYPE_CHECKPOINT, LOG_TYPE_FILE_CREATE, LOG_TYPE_FILE_DELETE, LOG_TYPE_FILE_EXTEND:
		return true
	default:
		return false
	}
}

// GetLogTypeName 获取日志类型名称
func (f *RedoLogFormatter) GetLogTypeName(logType uint8) string {
	switch logType {
	case LOG_TYPE_INSERT:
		return "INSERT"
	case LOG_TYPE_UPDATE:
		return "UPDATE"
	case LOG_TYPE_DELETE:
		return "DELETE"
	case LOG_TYPE_COMPENSATE:
		return "COMPENSATE"
	case LOG_TYPE_PAGE_CREATE:
		return "PAGE_CREATE"
	case LOG_TYPE_PAGE_DELETE:
		return "PAGE_DELETE"
	case LOG_TYPE_PAGE_MODIFY:
		return "PAGE_MODIFY"
	case LOG_TYPE_PAGE_SPLIT:
		return "PAGE_SPLIT"
	case LOG_TYPE_PAGE_MERGE:
		return "PAGE_MERGE"
	case LOG_TYPE_INDEX_INSERT:
		return "INDEX_INSERT"
	case LOG_TYPE_INDEX_DELETE:
		return "INDEX_DELETE"
	case LOG_TYPE_INDEX_UPDATE:
		return "INDEX_UPDATE"
	case LOG_TYPE_TXN_BEGIN:
		return "TXN_BEGIN"
	case LOG_TYPE_TXN_COMMIT:
		return "TXN_COMMIT"
	case LOG_TYPE_TXN_ROLLBACK:
		return "TXN_ROLLBACK"
	case LOG_TYPE_TXN_SAVEPOINT:
		return "TXN_SAVEPOINT"
	case LOG_TYPE_CHECKPOINT:
		return "CHECKPOINT"
	case LOG_TYPE_FILE_CREATE:
		return "FILE_CREATE"
	case LOG_TYPE_FILE_DELETE:
		return "FILE_DELETE"
	case LOG_TYPE_FILE_EXTEND:
		return "FILE_EXTEND"
	default:
		return "UNKNOWN"
	}
}
