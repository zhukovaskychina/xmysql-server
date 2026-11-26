package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/observability/compatibility"
)

// PreparedStatementManager 预编译语句管理器
type PreparedStatementManager struct {
	statements   map[uint32]*PreparedStatement
	mu           sync.RWMutex
	nextID       uint32
	contentStore *content.SegmentStore
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
	ID                    uint32            // 语句ID
	SQL                   string            // 原始SQL
	ParamCount            uint16            // 参数数量
	ColumnCount           uint16            // 列数量
	Params                []*ParamMetadata  // 参数元数据
	Columns               []*ColumnMetadata // 列元数据
	LastParamTypes        []byte            // 最近一次 EXECUTE 的参数字节（每条 2 字节），供 new_params_bound_flag=0 复用
	LongData              map[uint16][]byte
	ContentLongData       map[uint16]*content.SegmentWriter
	LongDataError         error
	ContentParamMIMETypes map[uint16]string
	CreatedAt             time.Time // 创建时间
	LastUsedAt            time.Time // 最后使用时间
	ExecuteCount          uint64    // 执行次数
	ExecuteTimeTotal      time.Duration
	ExecuteTimeMin        time.Duration
	ExecuteTimeMax        time.Duration
	ErrorCount            uint64
	WarningCount          uint64
	RowsAffected          uint64
	RowsSent              uint64
	RowsExamined          uint64
	CPUTimeTotal          uint64
	CursorResult          *MessageQueryResult
	CursorOffset          int
}

// BindInlineContentParameters gives ordinary EXECUTE parameters the same
// execution-local typed representation as streamed CONTENT parameters. Media
// bytes must not be expanded into SQL literals and repeatedly lexed as SQL.
func (stmt *PreparedStatement) BindInlineContentParameters(params []interface{}, values map[uint16]content.Value) (map[uint16]content.Value, error) {
	if values == nil {
		values = make(map[uint16]content.Value)
	}
	for index, mimeType := range stmt.ContentParamMIMETypes {
		if int(index) >= len(params) {
			return nil, fmt.Errorf("CONTENT parameter %d is out of range", index)
		}
		if _, streamed := values[index]; streamed {
			continue
		}
		var source []byte
		switch value := params[index].(type) {
		case PreparedBinaryParam:
			source = []byte(value)
		case []byte:
			source = value
		case string:
			source = []byte(value)
		default:
			// Preserve NULL and existing non-string type validation semantics.
			continue
		}
		value, err := content.NewValue(mimeType, 1, source)
		if err != nil {
			return nil, fmt.Errorf("bind CONTENT parameter %d: %w", index, err)
		}
		values[index] = value
		params[index] = PreparedContentParam(index)
	}
	return values, nil
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
	paramCount := uint16(len(sqlPlaceholderPositions(sql)))

	// 创建预编译语句
	stmt := &PreparedStatement{
		ID:                    stmtID,
		SQL:                   sql,
		ParamCount:            paramCount,
		ColumnCount:           0, // 需要执行后才知道列数
		Params:                make([]*ParamMetadata, paramCount),
		LongData:              make(map[uint16][]byte),
		ContentLongData:       make(map[uint16]*content.SegmentWriter),
		ContentParamMIMETypes: preparedContentParamMIMETypes(sql),
		Columns:               nil, // 稍后填充
		CreatedAt:             time.Now(),
		LastUsedAt:            time.Now(),
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

// SetContentSegmentStore enables incremental COM_STMT_SEND_LONG_DATA writes
// for parameters explicitly wrapped as CONTENT_FROM_BYTES(?, '<fixed MIME>')
// in DML statements. Other parameter types retain the normal MySQL byte path.
func (m *PreparedStatementManager) SetContentSegmentStore(store *content.SegmentStore) {
	if m == nil || store == nil {
		return
	}
	m.mu.Lock()
	m.contentStore = store
	m.mu.Unlock()
}

// AppendLongData appends one COM_STMT_SEND_LONG_DATA chunk to a parameter.
func (m *PreparedStatementManager) AppendLongData(stmtID uint32, paramID uint16, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}
	if stmt.LongDataError != nil {
		return stmt.LongDataError
	}
	latchError := func(err error) error {
		if err != nil && stmt.LongDataError == nil {
			stmt.LongDataError = err
		}
		return err
	}
	if paramID >= stmt.ParamCount {
		return latchError(fmt.Errorf("parameter %d out of range for prepared statement %d", paramID, stmtID))
	}
	if stmt.LongData == nil {
		stmt.LongData = make(map[uint16][]byte)
	}
	if mimeType, isContentParam := stmt.ContentParamMIMETypes[paramID]; isContentParam && m.contentStore != nil {
		writer := stmt.ContentLongData[paramID]
		if writer == nil {
			var err error
			writer, err = m.contentStore.NewWriter(context.Background(), mimeType, 1)
			if err != nil {
				return latchError(fmt.Errorf("open CONTENT parameter writer: %w", err))
			}
			stmt.ContentLongData[paramID] = writer
		}
		if _, err := writer.Write(data); err != nil {
			_ = writer.Abort()
			delete(stmt.ContentLongData, paramID)
			return latchError(fmt.Errorf("write CONTENT parameter chunk: %w", err))
		}
		return nil
	}
	if _, isContentParam := stmt.ContentParamMIMETypes[paramID]; isContentParam {
		return latchError(fmt.Errorf("CONTENT parameter %d requires the database-managed segment store", paramID))
	}
	stmt.LongData[paramID] = append(stmt.LongData[paramID], data...)
	return nil
}

// LatchLongDataError retains a protocol-level transfer error for reporting on
// the matching COM_STMT_EXECUTE response.
func (m *PreparedStatementManager) LatchLongDataError(stmtID uint32, transferErr error) error {
	if transferErr == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}
	if stmt.LongDataError == nil {
		stmt.LongDataError = transferErr
	}
	return nil
}

// ConsumeLongDataForExecution transfers ordinary long-data buffers and
// publishes eligible CONTENT parameters as immutable segmented values.
// Returned object refs belong to this execution until a successful DML row
// records them or the caller removes them after a failed statement.
func (m *PreparedStatementManager) ConsumeLongDataForExecution(stmtID uint32) (map[uint16][]byte, map[uint16]content.Value, []content.ObjectRef, error) {
	m.mu.Lock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		m.mu.Unlock()
		return nil, nil, nil, fmt.Errorf("prepared statement %d not found", stmtID)
	}
	inline := stmt.LongData
	writers := stmt.ContentLongData
	store := m.contentStore
	longDataErr := stmt.LongDataError
	stmt.LongData = make(map[uint16][]byte)
	stmt.ContentLongData = make(map[uint16]*content.SegmentWriter)
	stmt.LongDataError = nil
	m.mu.Unlock()
	if longDataErr != nil {
		for _, writer := range writers {
			_ = writer.Abort()
		}
		return nil, nil, nil, fmt.Errorf("COM_STMT_SEND_LONG_DATA failed: %w", longDataErr)
	}

	values := make(map[uint16]content.Value, len(writers))
	refs := make([]content.ObjectRef, 0, len(writers))
	for paramID, writer := range writers {
		ref, err := writer.Commit()
		if err != nil {
			for _, remaining := range writers {
				_ = remaining.Abort()
			}
			if store != nil {
				for _, published := range refs {
					_ = store.Delete(published)
				}
			}
			return nil, nil, nil, fmt.Errorf("publish CONTENT parameter %d: %w", paramID, err)
		}
		value, err := content.NewTransientSegmentedValue(ref)
		if err != nil {
			if store != nil {
				_ = store.Delete(ref)
				for _, published := range refs {
					_ = store.Delete(published)
				}
			}
			for _, remaining := range writers {
				_ = remaining.Abort()
			}
			return nil, nil, nil, fmt.Errorf("bind CONTENT parameter %d: %w", paramID, err)
		}
		values[paramID] = value
		refs = append(refs, ref)
	}
	return inline, values, refs, nil
}

// ConsumeLongData returns and clears all buffered long-data parameters.
func (m *PreparedStatementManager) ConsumeLongData(stmtID uint32) (map[uint16][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return nil, fmt.Errorf("prepared statement %d not found", stmtID)
	}
	// Transfer ownership of the accumulated buffers to the execute path. The
	// statement receives a fresh map immediately, so a second full-size copy
	// would only double peak memory for large COM_STMT_SEND_LONG_DATA values.
	data := stmt.LongData
	writers := stmt.ContentLongData
	longDataErr := stmt.LongDataError
	stmt.LongData = make(map[uint16][]byte)
	stmt.ContentLongData = make(map[uint16]*content.SegmentWriter)
	stmt.LongDataError = nil
	for _, writer := range writers {
		_ = writer.Abort()
	}
	if longDataErr != nil {
		return nil, fmt.Errorf("COM_STMT_SEND_LONG_DATA failed: %w", longDataErr)
	}
	if len(writers) != 0 {
		return nil, fmt.Errorf("streamed CONTENT long data must be consumed by the database execution path")
	}
	return data, nil
}

// Reset clears execution-bound state for a prepared statement.
func (m *PreparedStatementManager) Reset(stmtID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}
	stmt.LastParamTypes = nil
	stmt.LongDataError = nil
	for _, writer := range stmt.ContentLongData {
		_ = writer.Abort()
	}
	stmt.LongData = make(map[uint16][]byte)
	stmt.ContentLongData = make(map[uint16]*content.SegmentWriter)
	stmt.CursorResult = nil
	stmt.CursorOffset = 0
	return nil
}

// SetCursorResult stores the result of an execute request that used the
// server-side cursor flag. The result is consumed incrementally by FETCH.
func (m *PreparedStatementManager) SetCursorResult(stmtID uint32, result *MessageQueryResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}
	if stmt.CursorResult != nil {
		return fmt.Errorf("prepared statement %d has an open cursor", stmtID)
	}
	stmt.CursorResult = cloneQueryResult(result)
	stmt.CursorOffset = 0
	return nil
}

// HasOpenCursor reports whether a server-side cursor is still associated with
// the prepared statement. MySQL requires the caller to fetch it to exhaustion
// or reset the statement before executing it again.
func (m *PreparedStatementManager) HasOpenCursor(stmtID uint32) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return false, fmt.Errorf("prepared statement %d not found", stmtID)
	}
	return stmt.CursorResult != nil, nil
}

// FetchCursor returns at most rowCount rows and advances the cursor.
func (m *PreparedStatementManager) FetchCursor(stmtID uint32, rowCount uint32) (*MessageQueryResult, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return nil, false, fmt.Errorf("prepared statement %d not found", stmtID)
	}
	if stmt.CursorResult == nil {
		return nil, false, fmt.Errorf("prepared statement %d has no open cursor", stmtID)
	}
	columns := append([]string(nil), stmt.CursorResult.Columns...)
	columnTypes := append([]string(nil), stmt.CursorResult.ColumnTypes...)
	if rowCount == 0 {
		empty := cloneQueryResult(stmt.CursorResult)
		empty.Rows = [][]interface{}{}
		if stmt.CursorOffset >= len(stmt.CursorResult.Rows) {
			stmt.CursorResult = nil
			stmt.CursorOffset = 0
			return empty, true, nil
		}
		return empty, false, nil
	}
	start := stmt.CursorOffset
	if start >= len(stmt.CursorResult.Rows) {
		stmt.CursorResult = nil
		stmt.CursorOffset = 0
		return &MessageQueryResult{Columns: columns, ColumnTypes: columnTypes, Type: "select"}, true, nil
	}
	end := start + int(rowCount)
	if end > len(stmt.CursorResult.Rows) {
		end = len(stmt.CursorResult.Rows)
	}
	result := cloneQueryResult(stmt.CursorResult)
	result.Rows = cloneRows(stmt.CursorResult.Rows[start:end])
	stmt.CursorOffset = end
	last := end >= len(stmt.CursorResult.Rows)
	if last {
		stmt.CursorResult = nil
		stmt.CursorOffset = 0
	}
	return result, last, nil
}

func cloneQueryResult(result *MessageQueryResult) *MessageQueryResult {
	if result == nil {
		return nil
	}
	copyResult := *result
	copyResult.Columns = append([]string(nil), result.Columns...)
	copyResult.ColumnTypes = append([]string(nil), result.ColumnTypes...)
	copyResult.ColumnMetadata = append([]ResultColumnMetadata(nil), result.ColumnMetadata...)
	copyResult.Rows = cloneRows(result.Rows)
	return &copyResult
}

func cloneRows(rows [][]interface{}) [][]interface{} {
	copyRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		copyRows[i] = append([]interface{}(nil), row...)
	}
	return copyRows
}

// Get 获取预编译语句
func (m *PreparedStatementManager) Get(stmtID uint32) (*PreparedStatement, error) {
	// Get updates LastUsedAt and ExecuteCount, so it must use the write lock.
	// Returning a statement pointer remains safe for the caller because the
	// statement itself is retained until an explicit COM_STMT_CLOSE/reset.
	m.mu.Lock()
	defer m.mu.Unlock()

	stmt, exists := m.statements[stmtID]
	if !exists {
		return nil, fmt.Errorf("prepared statement %d not found", stmtID)
	}

	// 更新最后使用时间
	stmt.LastUsedAt = time.Now()
	atomic.AddUint64(&stmt.ExecuteCount, 1)

	return stmt, nil
}

// RecordExecution adds the authoritative result accounting for one prepared
// statement execution. The manager lock makes the min/max and cumulative
// values consistent with the statement inventory snapshot.
func (m *PreparedStatementManager) RecordExecution(stmtID uint32, stats compatibility.PreparedStatementExecutionStats) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmt, exists := m.statements[stmtID]
	if !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}
	duration := stats.Duration
	if duration < 0 {
		duration = 0
	}
	stmt.ExecuteTimeTotal += duration
	if stmt.ExecuteCount == 1 || stmt.ExecuteTimeMin == 0 || duration < stmt.ExecuteTimeMin {
		stmt.ExecuteTimeMin = duration
	}
	if duration > stmt.ExecuteTimeMax {
		stmt.ExecuteTimeMax = duration
	}
	if stats.Failed {
		stmt.ErrorCount++
	}
	stmt.WarningCount += stats.Warnings
	stmt.RowsAffected += stats.RowsAffected
	stmt.RowsSent += stats.RowsSent
	stmt.RowsExamined += stats.RowsExamined
	if stats.CPUTimeCaptured {
		stmt.CPUTimeTotal += stats.CPUTime
	}
	return nil
}

// Peek 获取预编译语句但不计入执行次数（用于 COM_STMT_RESET 等）。
func (m *PreparedStatementManager) Peek(stmtID uint32) (*PreparedStatement, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stmt, exists := m.statements[stmtID]
	if !exists {
		return nil, fmt.Errorf("prepared statement %d not found", stmtID)
	}
	return stmt, nil
}

// Close 关闭预编译语句
func (m *PreparedStatementManager) Close(stmtID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.statements[stmtID]; !exists {
		return fmt.Errorf("prepared statement %d not found", stmtID)
	}

	stmt := m.statements[stmtID]
	for _, writer := range stmt.ContentLongData {
		_ = writer.Abort()
	}
	delete(m.statements, stmtID)
	return nil
}

// CloseAll releases all execution-bound resources owned by this connection.
// In particular, pending streamed CONTENT writers must be aborted when a
// connection is reset or closed so their staging directories do not linger.
func (m *PreparedStatementManager) CloseAll() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, stmt := range m.statements {
		for _, writer := range stmt.ContentLongData {
			_ = writer.Abort()
		}
	}
	m.statements = make(map[uint32]*PreparedStatement)
}

// Count 返回当前缓存的语句数量
func (m *PreparedStatementManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.statements)
}

// Snapshot returns a deterministic copy of all currently prepared
// statements. Mutable cursor and long-data state is intentionally omitted;
// those are protocol details rather than prepared-statements_instances
// metadata.
func (m *PreparedStatementManager) Snapshot() []compatibility.PreparedStatementSnapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshots := make([]compatibility.PreparedStatementSnapshot, 0, len(m.statements))
	for _, stmt := range m.statements {
		if stmt == nil {
			continue
		}
		snapshots = append(snapshots, compatibility.PreparedStatementSnapshot{
			ID:               stmt.ID,
			SQL:              stmt.SQL,
			ParamCount:       stmt.ParamCount,
			ColumnCount:      stmt.ColumnCount,
			CreatedAt:        stmt.CreatedAt,
			LastUsedAt:       stmt.LastUsedAt,
			ExecuteCount:     atomic.LoadUint64(&stmt.ExecuteCount),
			ExecuteTimeTotal: stmt.ExecuteTimeTotal,
			ExecuteTimeMin:   stmt.ExecuteTimeMin,
			ExecuteTimeMax:   stmt.ExecuteTimeMax,
			ErrorCount:       stmt.ErrorCount,
			WarningCount:     stmt.WarningCount,
			RowsAffected:     stmt.RowsAffected,
			RowsSent:         stmt.RowsSent,
			RowsExamined:     stmt.RowsExamined,
			CPUTimeTotal:     stmt.CPUTimeTotal,
		})
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].ID < snapshots[j].ID })
	return snapshots
}

func preparedContentParamMIMETypes(sql string) map[uint16]string {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil
	}
	switch typed := statement.(type) {
	case *sqlparser.Insert:
		if !strings.EqualFold(typed.Action, sqlparser.InsertStr) && !strings.EqualFold(typed.Action, sqlparser.ReplaceStr) {
			return nil
		}
	case *sqlparser.Update:
	default:
		return nil
	}

	parameterIndexes := make(map[*sqlparser.SQLVal]uint16)
	var next uint16
	if err := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if value, ok := node.(*sqlparser.SQLVal); ok && value.Type == sqlparser.ValArg {
			parameterIndexes[value] = next
			next++
		}
		return true, nil
	}, statement); err != nil {
		return nil
	}

	contentParams := make(map[uint16]string)
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		function, ok := node.(*sqlparser.FuncExpr)
		if !ok || !strings.EqualFold(strings.Trim(function.Name.String(), "` "), "CONTENT_FROM_BYTES") || len(function.Exprs) != 2 {
			return true, nil
		}
		bytesArgument, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			return true, nil
		}
		placeholder, ok := bytesArgument.Expr.(*sqlparser.SQLVal)
		if !ok || placeholder.Type != sqlparser.ValArg {
			return true, nil
		}
		mimeArgument, ok := function.Exprs[1].(*sqlparser.AliasedExpr)
		if !ok {
			return true, nil
		}
		mimeLiteral, ok := mimeArgument.Expr.(*sqlparser.SQLVal)
		if !ok || mimeLiteral.Type != sqlparser.StrVal {
			return true, nil
		}
		if index, exists := parameterIndexes[placeholder]; exists {
			contentParams[index] = string(mimeLiteral.Val)
		}
		return true, nil
	}, statement)
	if len(contentParams) == 0 {
		return nil
	}
	return contentParams
}

// EncodePrepareResponse 编码 COM_STMT_PREPARE 响应包
func EncodePrepareResponse(stmt *PreparedStatement, seqID byte) [][]byte {
	return EncodePrepareResponseWithCapabilities(stmt, seqID, 0)
}

func EncodePrepareResponseWithCapabilities(stmt *PreparedStatement, seqID byte, capabilities uint32) [][]byte {
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

		// CLIENT_DEPRECATE_EOF omits both metadata terminators in PREPARE.
		if capabilities&common.CLIENT_DEPRECATE_EOF == 0 {
			eofPacket := EncodeEOFPacket(0, common.SERVER_STATUS_AUTOCOMMIT)
			eofPacket = addPacketHeader(eofPacket[4:], seqID) // 去掉原有的header，重新添加
			packets = append(packets, eofPacket)
			seqID++
		}
	}

	// 3. 如果有列，发送列元数据
	if stmt.ColumnCount > 0 {
		for _, col := range stmt.Columns {
			colPacket := encodeColumnDefinition(col, seqID)
			packets = append(packets, colPacket)
			seqID++
		}

		if capabilities&common.CLIENT_DEPRECATE_EOF == 0 {
			eofPacket := EncodeEOFPacket(0, common.SERVER_STATUS_AUTOCOMMIT)
			eofPacket = addPacketHeader(eofPacket[4:], seqID)
			packets = append(packets, eofPacket)
		}
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
		flags |= uint16(common.UnsignedFlag)
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
