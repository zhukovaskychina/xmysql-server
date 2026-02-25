package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// UndoLogHeader Undo日志头部
// 格式：LSN(8) + TxID(8) + Type(1) + TableID(8) + RecordID(8) + DataLen(4) + Checksum(4) = 41字节
type UndoLogHeader struct {
	LSN      uint64 // 日志序列号
	TrxID    int64  // 事务ID
	Type     uint8  // 操作类型
	TableID  uint64 // 表ID
	RecordID uint64 // 记录ID
	DataLen  uint32 // 数据长度
	Checksum uint32 // 校验和
}

// UndoLogFormatter Undo日志格式化器
type UndoLogFormatter struct {
	headerSize int // 头部大小（固定41字节）
}

// NewUndoLogFormatter 创建Undo日志格式化器
func NewUndoLogFormatter() *UndoLogFormatter {
	return &UndoLogFormatter{
		headerSize: 41, // LSN(8) + TxID(8) + Type(1) + TableID(8) + RecordID(8) + DataLen(4) + Checksum(4)
	}
}

// FormatInsertUndo 格式化INSERT的Undo日志
// INSERT的Undo：记录主键信息，用于DELETE回滚
func (f *UndoLogFormatter) FormatInsertUndo(
	lsn uint64,
	txID int64,
	tableID uint64,
	recordID uint64,
	primaryKeyData []byte,
) ([]byte, error) {
	return f.formatLog(&UndoLogEntry{
		LSN:     lsn,
		TrxID:   txID,
		Type:    LOG_TYPE_INSERT,
		TableID: tableID,
		Data:    primaryKeyData,
	}, recordID)
}

// FormatUpdateUndo 格式化UPDATE的Undo日志
// UPDATE的Undo：记录旧值，用于恢复原值
func (f *UndoLogFormatter) FormatUpdateUndo(
	lsn uint64,
	txID int64,
	tableID uint64,
	recordID uint64,
	oldRecordData []byte,
	columnBitmap []byte, // 标记哪些列被更新
) ([]byte, error) {
	// UPDATE Undo格式：[bitmap长度(4字节)][bitmap][旧值]
	buffer := new(bytes.Buffer)

	// 写入bitmap长度
	bitmapLen := uint32(len(columnBitmap))
	if err := binary.Write(buffer, binary.BigEndian, bitmapLen); err != nil {
		return nil, err
	}

	// 写入bitmap
	if bitmapLen > 0 {
		if _, err := buffer.Write(columnBitmap); err != nil {
			return nil, err
		}
	}

	// 写入旧值
	if _, err := buffer.Write(oldRecordData); err != nil {
		return nil, err
	}

	return f.formatLog(&UndoLogEntry{
		LSN:     lsn,
		TrxID:   txID,
		Type:    LOG_TYPE_UPDATE,
		TableID: tableID,
		Data:    buffer.Bytes(),
	}, recordID)
}

// FormatDeleteUndo 格式化DELETE的Undo日志
// DELETE的Undo：记录完整记录，用于INSERT回滚
func (f *UndoLogFormatter) FormatDeleteUndo(
	lsn uint64,
	txID int64,
	tableID uint64,
	recordID uint64,
	fullRecordData []byte,
) ([]byte, error) {
	return f.formatLog(&UndoLogEntry{
		LSN:     lsn,
		TrxID:   txID,
		Type:    LOG_TYPE_DELETE,
		TableID: tableID,
		Data:    fullRecordData,
	}, recordID)
}

// formatLog 内部统一格式化方法
func (f *UndoLogFormatter) formatLog(entry *UndoLogEntry, recordID uint64) ([]byte, error) {
	buffer := new(bytes.Buffer)

	// 计算数据长度
	dataLen := uint32(len(entry.Data))

	// 创建头部
	header := &UndoLogHeader{
		LSN:      entry.LSN,
		TrxID:    entry.TrxID,
		Type:     entry.Type,
		TableID:  entry.TableID,
		RecordID: recordID,
		DataLen:  dataLen,
	}

	// 计算校验和
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
func (f *UndoLogFormatter) writeHeader(buffer *bytes.Buffer, header *UndoLogHeader) error {
	if err := binary.Write(buffer, binary.BigEndian, header.LSN); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.TrxID); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.Type); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.TableID); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, header.RecordID); err != nil {
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

// ParseLog 解析Undo日志记录
func (f *UndoLogFormatter) ParseLog(data []byte) (*UndoLogEntry, *UndoLogHeader, error) {
	if len(data) < f.headerSize {
		return nil, nil, fmt.Errorf("Undo日志记录太短：需要至少%d字节，实际%d字节", f.headerSize, len(data))
	}

	reader := bytes.NewReader(data)
	header := &UndoLogHeader{}

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
	if err := binary.Read(reader, binary.BigEndian, &header.TableID); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.RecordID); err != nil {
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
	entry := &UndoLogEntry{
		LSN:     header.LSN,
		TrxID:   header.TrxID,
		Type:    header.Type,
		TableID: header.TableID,
		Data:    logData,
	}

	return entry, header, nil
}

// ParseUpdateUndo 解析UPDATE Undo日志，分离bitmap和旧值
func (f *UndoLogFormatter) ParseUpdateUndo(data []byte) (columnBitmap, oldData []byte, err error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("UPDATE Undo日志数据太短")
	}

	reader := bytes.NewReader(data)

	// 读取bitmap长度
	var bitmapLen uint32
	if err := binary.Read(reader, binary.BigEndian, &bitmapLen); err != nil {
		return nil, nil, err
	}

	// 验证长度
	if len(data) < 4+int(bitmapLen) {
		return nil, nil, fmt.Errorf("UPDATE Undo日志数据不完整")
	}

	// 读取bitmap
	if bitmapLen > 0 {
		columnBitmap = data[4 : 4+bitmapLen]
	}

	// 读取旧值
	oldData = data[4+bitmapLen:]

	return columnBitmap, oldData, nil
}

// calculateChecksum 计算校验和
func (f *UndoLogFormatter) calculateChecksum(header *UndoLogHeader, data []byte) uint32 {
	buffer := new(bytes.Buffer)

	// 包含头部字段（不包括校验和本身）
	binary.Write(buffer, binary.BigEndian, header.LSN)
	binary.Write(buffer, binary.BigEndian, header.TrxID)
	binary.Write(buffer, binary.BigEndian, header.Type)
	binary.Write(buffer, binary.BigEndian, header.TableID)
	binary.Write(buffer, binary.BigEndian, header.RecordID)
	binary.Write(buffer, binary.BigEndian, header.DataLen)

	// 包含数据
	if len(data) > 0 {
		buffer.Write(data)
	}

	return crc32.ChecksumIEEE(buffer.Bytes())
}

// GetHeaderSize 获取头部大小
func (f *UndoLogFormatter) GetHeaderSize() int {
	return f.headerSize
}

// RollbackOperation 回滚操作接口
type RollbackOperation interface {
	// Execute 执行回滚操作
	Execute() error
	// GetType 获取回滚操作类型
	GetType() uint8
	// GetRecordID 获取记录ID
	GetRecordID() uint64
}

// InsertRollback INSERT的回滚操作（执行DELETE）
type InsertRollback struct {
	tableID        uint64
	recordID       uint64
	primaryKeyData []byte
	executor       RollbackExecutor
}

// NewInsertRollback 创建INSERT回滚操作
func NewInsertRollback(tableID, recordID uint64, pkData []byte, executor RollbackExecutor) *InsertRollback {
	return &InsertRollback{
		tableID:        tableID,
		recordID:       recordID,
		primaryKeyData: pkData,
		executor:       executor,
	}
}

// Execute 执行回滚（删除插入的记录）
func (r *InsertRollback) Execute() error {
	return r.executor.DeleteRecord(r.tableID, r.recordID, r.primaryKeyData)
}

// GetType 获取操作类型
func (r *InsertRollback) GetType() uint8 {
	return LOG_TYPE_INSERT
}

// GetRecordID 获取记录ID
func (r *InsertRollback) GetRecordID() uint64 {
	return r.recordID
}

// UpdateRollback UPDATE的回滚操作（恢复旧值）
type UpdateRollback struct {
	tableID      uint64
	recordID     uint64
	oldData      []byte
	columnBitmap []byte
	executor     RollbackExecutor
}

// NewUpdateRollback 创建UPDATE回滚操作
func NewUpdateRollback(tableID, recordID uint64, oldData, bitmap []byte, executor RollbackExecutor) *UpdateRollback {
	return &UpdateRollback{
		tableID:      tableID,
		recordID:     recordID,
		oldData:      oldData,
		columnBitmap: bitmap,
		executor:     executor,
	}
}

// Execute 执行回滚（恢复旧值）
func (r *UpdateRollback) Execute() error {
	return r.executor.UpdateRecord(r.tableID, r.recordID, r.oldData, r.columnBitmap)
}

// GetType 获取操作类型
func (r *UpdateRollback) GetType() uint8 {
	return LOG_TYPE_UPDATE
}

// GetRecordID 获取记录ID
func (r *UpdateRollback) GetRecordID() uint64 {
	return r.recordID
}

// DeleteRollback DELETE的回滚操作（重新插入）
type DeleteRollback struct {
	tableID        uint64
	recordID       uint64
	fullRecordData []byte
	executor       RollbackExecutor
}

// NewDeleteRollback 创建DELETE回滚操作
func NewDeleteRollback(tableID, recordID uint64, fullData []byte, executor RollbackExecutor) *DeleteRollback {
	return &DeleteRollback{
		tableID:        tableID,
		recordID:       recordID,
		fullRecordData: fullData,
		executor:       executor,
	}
}

// Execute 执行回滚（重新插入记录）
func (r *DeleteRollback) Execute() error {
	return r.executor.InsertRecord(r.tableID, r.recordID, r.fullRecordData)
}

// GetType 获取操作类型
func (r *DeleteRollback) GetType() uint8 {
	return LOG_TYPE_DELETE
}

// GetRecordID 获取记录ID
func (r *DeleteRollback) GetRecordID() uint64 {
	return r.recordID
}

// RollbackExecutor 回滚执行器接口
type RollbackExecutor interface {
	// InsertRecord 插入记录（用于DELETE回滚）
	InsertRecord(tableID, recordID uint64, data []byte) error
	// UpdateRecord 更新记录（用于UPDATE回滚）
	UpdateRecord(tableID, recordID uint64, data, columnBitmap []byte) error
	// DeleteRecord 删除记录（用于INSERT回滚）
	DeleteRecord(tableID, recordID uint64, primaryKeyData []byte) error
}
