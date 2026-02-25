package protocol

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// PreparedStatementManager 预编译语句管理器
type PreparedStatementManager struct {
	statements map[uint32]*PreparedStatement
	mu         sync.RWMutex
	nextID     uint32
}

// NewPreparedStatementManager 创建预编译语句管理器
func NewPreparedStatementManager() *PreparedStatementManager {
	return &PreparedStatementManager{
		statements: make(map[uint32]*PreparedStatement),
		nextID:     1,
	}
}

// PreparedStatement 预编译语句
type PreparedStatement struct {
	ID           uint32            // 语句ID
	SQL          string            // 原始SQL
	ParamCount   uint16            // 参数数量
	ColumnCount  uint16            // 列数量
	Params       []*ParamMetadata  // 参数元数据
	Columns      []*ColumnMetadata // 列元数据
	CreatedAt    time.Time         // 创建时间
	LastUsedAt   time.Time         // 最后使用时间
	ExecuteCount uint64            // 执行次数
}

// ParamMetadata 参数元数据
type ParamMetadata struct {
	Index    uint16 // 参数索引（从0开始）
	Type     byte   // MySQL类型
	Unsigned bool   // 是否无符号
	Name     string // 参数名（如果有）
}

// ColumnMetadata 列元数据
type ColumnMetadata struct {
	Catalog  string // 目录名（通常是"def"）
	Database string // 数据库名
	Table    string // 表名
	OrgTable string // 原始表名
	Name     string // 列名
	OrgName  string // 原始列名
	Charset  uint16 // 字符集
	Length   uint32 // 列长度
	Type     byte   // 列类型
	Flags    uint16 // 列标志
	Decimals byte   // 小数位数
}

// Prepare 准备一个SQL语句
func (m *PreparedStatementManager) Prepare(sql string) (*PreparedStatement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 生成唯一的语句ID
	stmtID := atomic.AddUint32(&m.nextID, 1)

	// 解析SQL，提取参数
	paramCount := uint16(strings.Count(sql, "?"))

	// 创建预编译语句
	stmt := &PreparedStatement{
		ID:          stmtID,
		SQL:         sql,
		ParamCount:  paramCount,
		ColumnCount: 0, // 需要执行后才知道列数
		Params:      make([]*ParamMetadata, paramCount),
		Columns:     nil, // 稍后填充
		CreatedAt:   time.Now(),
		LastUsedAt:  time.Now(),
	}

	// 初始化参数元数据
	for i := uint16(0); i < paramCount; i++ {
		stmt.Params[i] = &ParamMetadata{
			Index:    i,
			Type:     common.COLUMN_TYPE_VAR_STRING, // 默认类型
			Unsigned: false,
			Name:     fmt.Sprintf("?%d", i),
		}
	}

	// 缓存语句
	m.statements[stmtID] = stmt

	return stmt, nil
}

// Get 获取预编译语句
func (m *PreparedStatementManager) Get(stmtID uint32) (*PreparedStatement, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stmt, exists := m.statements[stmtID]
	if !exists {
		return nil, fmt.Errorf("prepared statement %d not found", stmtID)
	}

	// 更新最后使用时间
	stmt.LastUsedAt = time.Now()
	atomic.AddUint64(&stmt.ExecuteCount, 1)

	return stmt, nil
}

// Close 关闭预编译语句
func (m *PreparedStatementManager) Close(stmtID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.statements[stmtID]; !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}

	delete(m.statements, stmtID)
	return nil
}

// Count 返回当前缓存的语句数量
func (m *PreparedStatementManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.statements)
}

// EncodePrepareResponse 编码 COM_STMT_PREPARE 响应包
func EncodePrepareResponse(stmt *PreparedStatement, seqID byte) [][]byte {
	packets := make([][]byte, 0)

	// 1. 发送 OK 包（包含语句ID、列数、参数数）
	okPayload := make([]byte, 12)
	okPayload[0] = 0x00 // OK标识符
	binary.LittleEndian.PutUint32(okPayload[1:5], stmt.ID)
	binary.LittleEndian.PutUint16(okPayload[5:7], stmt.ColumnCount)
	binary.LittleEndian.PutUint16(okPayload[7:9], stmt.ParamCount)
	okPayload[9] = 0x00                                // 保留字节
	binary.LittleEndian.PutUint16(okPayload[10:12], 0) // warning_count

	okPacket := addPacketHeader(okPayload, seqID)
	packets = append(packets, okPacket)
	seqID++

	// 2. 如果有参数，发送参数元数据
	if stmt.ParamCount > 0 {
		for _, param := range stmt.Params {
			paramPacket := encodeParamDefinition(param, seqID)
			packets = append(packets, paramPacket)
			seqID++
		}

		// 发送 EOF 包（结束参数定义）
		eofPacket := EncodeEOFPacket(0, common.SERVER_STATUS_AUTOCOMMIT)
		eofPacket = addPacketHeader(eofPacket[4:], seqID) // 去掉原有的header，重新添加
		packets = append(packets, eofPacket)
		seqID++
	}

	// 3. 如果有列，发送列元数据
	if stmt.ColumnCount > 0 {
		for _, col := range stmt.Columns {
			colPacket := encodeColumnDefinition(col, seqID)
			packets = append(packets, colPacket)
			seqID++
		}

		// 发送 EOF 包（结束列定义）
		eofPacket := EncodeEOFPacket(0, common.SERVER_STATUS_AUTOCOMMIT)
		eofPacket = addPacketHeader(eofPacket[4:], seqID)
		packets = append(packets, eofPacket)
	}

	return packets
}

// encodeParamDefinition 编码参数定义包
func encodeParamDefinition(param *ParamMetadata, seqID byte) []byte {
	var data []byte

	// catalog (固定为"def")
	data = appendLengthEncodedString(data, "def")

	// schema (空)
	data = appendLengthEncodedString(data, "")

	// table (空)
	data = appendLengthEncodedString(data, "")

	// org_table (空)
	data = appendLengthEncodedString(data, "")

	// name (参数名)
	data = appendLengthEncodedString(data, param.Name)

	// org_name (参数名)
	data = appendLengthEncodedString(data, param.Name)

	// 固定长度字段
	data = append(data, 0x0c) // length of fixed-length fields

	// character_set (utf8_general_ci)
	data = append(data, 0x21, 0x00)

	// column_length
	data = append(data, 0xff, 0xff, 0xff, 0xff)

	// type
	data = append(data, param.Type)

	// flags
	flags := uint16(0)
	if param.Unsigned {
		flags |= 0x0020 // UNSIGNED_FLAG
	}
	data = append(data, byte(flags), byte(flags>>8))

	// decimals
	data = append(data, 0x00)

	// filler
	data = append(data, 0x00, 0x00)

	return addPacketHeader(data, seqID)
}

// encodeColumnDefinition 编码列定义包
func encodeColumnDefinition(col *ColumnMetadata, seqID byte) []byte {
	var data []byte

	// catalog
	data = appendLengthEncodedString(data, col.Catalog)

	// schema
	data = appendLengthEncodedString(data, col.Database)

	// table
	data = appendLengthEncodedString(data, col.Table)

	// org_table
	data = appendLengthEncodedString(data, col.OrgTable)

	// name
	data = appendLengthEncodedString(data, col.Name)

	// org_name
	data = appendLengthEncodedString(data, col.OrgName)

	// 固定长度字段
	data = append(data, 0x0c) // length of fixed-length fields

	// character_set
	data = append(data, byte(col.Charset), byte(col.Charset>>8))

	// column_length
	data = append(data, byte(col.Length), byte(col.Length>>8),
		byte(col.Length>>16), byte(col.Length>>24))

	// type
	data = append(data, col.Type)

	// flags
	data = append(data, byte(col.Flags), byte(col.Flags>>8))

	// decimals
	data = append(data, col.Decimals)

	// filler
	data = append(data, 0x00, 0x00)

	return addPacketHeader(data, seqID)
}

// 注意：appendLengthEncodedString 和 addPacketHeader 函数在 mysql_codec.go 中定义
