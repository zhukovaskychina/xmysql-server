package protocol

import (
	_ "encoding/binary"
	"fmt"
	"strings"
)

// MySQL会话状态跟踪（CLIENT_SESSION_TRACK）
// 参考: https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html

// SessionTrackType 会话跟踪类型
type SessionTrackType byte

const (
	// SESSION_TRACK_SYSTEM_VARIABLES 系统变量变更
	SessionTrackSystemVariables SessionTrackType = 0x00

	// SESSION_TRACK_SCHEMA 当前数据库变更
	SessionTrackSchema SessionTrackType = 0x01

	// SESSION_TRACK_STATE_CHANGE 会话状态变更
	SessionTrackStateChange SessionTrackType = 0x02

	// SESSION_TRACK_GTIDS GTID信息
	SessionTrackGTIDs SessionTrackType = 0x03

	// SESSION_TRACK_TRANSACTION_CHARACTERISTICS 事务特性
	SessionTrackTransactionCharacteristics SessionTrackType = 0x04

	// SESSION_TRACK_TRANSACTION_STATE 事务状态
	SessionTrackTransactionState SessionTrackType = 0x05
)

// SessionTracker 会话跟踪器
type SessionTracker struct {
	enabled bool
	changes []SessionTrackData
}

// SessionTrackData 会话跟踪数据
type SessionTrackData struct {
	Type SessionTrackType
	Data []byte
}

// NewSessionTracker 创建会话跟踪器
func NewSessionTracker(enabled bool) *SessionTracker {
	return &SessionTracker{
		enabled: enabled,
		changes: make([]SessionTrackData, 0),
	}
}

// IsEnabled 是否启用会话跟踪
func (st *SessionTracker) IsEnabled() bool {
	return st.enabled
}

// Enable 启用会话跟踪
func (st *SessionTracker) Enable() {
	st.enabled = true
}

// Disable 禁用会话跟踪
func (st *SessionTracker) Disable() {
	st.enabled = false
}

// TrackSystemVariable 跟踪系统变量变更
func (st *SessionTracker) TrackSystemVariable(name, value string) {
	if !st.enabled {
		return
	}

	data := encodeSystemVariableChange(name, value)
	st.changes = append(st.changes, SessionTrackData{
		Type: SessionTrackSystemVariables,
		Data: data,
	})
}

// TrackSchema 跟踪数据库变更
func (st *SessionTracker) TrackSchema(schema string) {
	if !st.enabled {
		return
	}

	data := encodeLengthEncodedString(schema)
	st.changes = append(st.changes, SessionTrackData{
		Type: SessionTrackSchema,
		Data: data,
	})
}

// TrackStateChange 跟踪会话状态变更
func (st *SessionTracker) TrackStateChange(changed bool) {
	if !st.enabled {
		return
	}

	var data []byte
	if changed {
		data = encodeLengthEncodedString("1")
	} else {
		data = encodeLengthEncodedString("0")
	}

	st.changes = append(st.changes, SessionTrackData{
		Type: SessionTrackStateChange,
		Data: data,
	})
}

// TrackGTIDs 跟踪GTID信息
func (st *SessionTracker) TrackGTIDs(gtids string) {
	if !st.enabled {
		return
	}

	// GTID格式：encoding + gtid_set
	// encoding: 0x00 = off, 0x01 = own_gtid, 0x02 = all_gtids
	data := []byte{0x01} // own_gtid
	data = append(data, encodeLengthEncodedString(gtids)...)

	st.changes = append(st.changes, SessionTrackData{
		Type: SessionTrackGTIDs,
		Data: data,
	})
}

// TrackTransactionCharacteristics 跟踪事务特性
func (st *SessionTracker) TrackTransactionCharacteristics(characteristics string) {
	if !st.enabled {
		return
	}

	data := encodeLengthEncodedString(characteristics)
	st.changes = append(st.changes, SessionTrackData{
		Type: SessionTrackTransactionCharacteristics,
		Data: data,
	})
}

// TrackTransactionState 跟踪事务状态
func (st *SessionTracker) TrackTransactionState(state string) {
	if !st.enabled {
		return
	}

	data := encodeLengthEncodedString(state)
	st.changes = append(st.changes, SessionTrackData{
		Type: SessionTrackTransactionState,
		Data: data,
	})
}

// GetChanges 获取所有变更
func (st *SessionTracker) GetChanges() []SessionTrackData {
	return st.changes
}

// HasChanges 是否有变更
func (st *SessionTracker) HasChanges() bool {
	return len(st.changes) > 0
}

// Clear 清空变更
func (st *SessionTracker) Clear() {
	st.changes = make([]SessionTrackData, 0)
}

// Encode 编码会话跟踪数据
func (st *SessionTracker) Encode() []byte {
	if !st.enabled || len(st.changes) == 0 {
		return nil
	}

	var result []byte

	for _, change := range st.changes {
		// 类型（1字节）
		result = append(result, byte(change.Type))

		// 数据长度（length-encoded integer）
		length := encodeLengthEncodedInteger(uint64(len(change.Data)))
		result = append(result, length...)

		// 数据
		result = append(result, change.Data...)
	}

	return result
}

// EncodeForOKPacket 为OK包编码会话跟踪数据
// 返回完整的session_track信息（包括总长度）
func (st *SessionTracker) EncodeForOKPacket() []byte {
	if !st.enabled || len(st.changes) == 0 {
		return nil
	}

	trackData := st.Encode()

	// 添加总长度
	totalLength := encodeLengthEncodedInteger(uint64(len(trackData)))
	return append(totalLength, trackData...)
}

// encodeSystemVariableChange 编码系统变量变更
func encodeSystemVariableChange(name, value string) []byte {
	var result []byte

	// 变量名（length-encoded string）
	result = append(result, encodeLengthEncodedString(name)...)

	// 变量值（length-encoded string）
	result = append(result, encodeLengthEncodedString(value)...)

	return result
}

// ParseSessionTrack 解析会话跟踪数据
func ParseSessionTrack(data []byte) ([]SessionTrackData, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var result []SessionTrackData
	offset := 0

	// 读取总长度
	totalLength, bytesRead := readLengthEncodedInteger(data[offset:])
	if bytesRead == 0 {
		return nil, fmt.Errorf("failed to read total length")
	}
	offset += bytesRead

	endOffset := offset + int(totalLength)
	if endOffset > len(data) {
		return nil, fmt.Errorf("insufficient data for session track")
	}

	// 解析每个变更
	for offset < endOffset {
		// 读取类型
		if offset >= len(data) {
			break
		}
		trackType := SessionTrackType(data[offset])
		offset++

		// 读取数据长度
		dataLength, bytesRead := readLengthEncodedInteger(data[offset:])
		if bytesRead == 0 {
			return nil, fmt.Errorf("failed to read data length")
		}
		offset += bytesRead

		// 读取数据
		if offset+int(dataLength) > len(data) {
			return nil, fmt.Errorf("insufficient data for track data")
		}
		trackData := data[offset : offset+int(dataLength)]
		offset += int(dataLength)

		result = append(result, SessionTrackData{
			Type: trackType,
			Data: trackData,
		})
	}

	return result, nil
}

// SessionTrackInfo 会话跟踪信息
type SessionTrackInfo struct {
	SystemVariables   map[string]string
	Schema            string
	StateChanged      bool
	GTIDs             string
	TxCharacteristics string
	TxState           string
}

// NewSessionTrackInfo 创建会话跟踪信息
func NewSessionTrackInfo() *SessionTrackInfo {
	return &SessionTrackInfo{
		SystemVariables: make(map[string]string),
	}
}

// ParseSessionTrackInfo 解析会话跟踪信息
func ParseSessionTrackInfo(data []SessionTrackData) (*SessionTrackInfo, error) {
	info := NewSessionTrackInfo()

	for _, track := range data {
		switch track.Type {
		case SessionTrackSystemVariables:
			// 解析系统变量
			name, value, err := parseSystemVariableChange(track.Data)
			if err != nil {
				return nil, fmt.Errorf("failed to parse system variable: %w", err)
			}
			info.SystemVariables[name] = value

		case SessionTrackSchema:
			// 解析数据库
			schema, _ := readLengthEncodedString(track.Data)
			info.Schema = schema

		case SessionTrackStateChange:
			// 解析状态变更
			state, _ := readLengthEncodedString(track.Data)
			info.StateChanged = (state == "1")

		case SessionTrackGTIDs:
			// 解析GTID
			if len(track.Data) > 1 {
				gtids, _ := readLengthEncodedString(track.Data[1:])
				info.GTIDs = gtids
			}

		case SessionTrackTransactionCharacteristics:
			// 解析事务特性
			chars, _ := readLengthEncodedString(track.Data)
			info.TxCharacteristics = chars

		case SessionTrackTransactionState:
			// 解析事务状态
			state, _ := readLengthEncodedString(track.Data)
			info.TxState = state
		}
	}

	return info, nil
}

// parseSystemVariableChange 解析系统变量变更
func parseSystemVariableChange(data []byte) (string, string, error) {
	offset := 0

	// 读取变量名
	name, bytesRead := readLengthEncodedString(data[offset:])
	if bytesRead == 0 {
		return "", "", fmt.Errorf("failed to read variable name")
	}
	offset += bytesRead

	// 读取变量值
	value, bytesRead := readLengthEncodedString(data[offset:])
	if bytesRead == 0 {
		return "", "", fmt.Errorf("failed to read variable value")
	}

	return name, value, nil
}

// String 返回会话跟踪信息的字符串表示
func (sti *SessionTrackInfo) String() string {
	var parts []string

	if len(sti.SystemVariables) > 0 {
		var vars []string
		for k, v := range sti.SystemVariables {
			vars = append(vars, fmt.Sprintf("%s=%s", k, v))
		}
		parts = append(parts, fmt.Sprintf("variables: %s", strings.Join(vars, ", ")))
	}

	if sti.Schema != "" {
		parts = append(parts, fmt.Sprintf("schema: %s", sti.Schema))
	}

	if sti.StateChanged {
		parts = append(parts, "state_changed: true")
	}

	if sti.GTIDs != "" {
		parts = append(parts, fmt.Sprintf("gtids: %s", sti.GTIDs))
	}

	if sti.TxCharacteristics != "" {
		parts = append(parts, fmt.Sprintf("tx_characteristics: %s", sti.TxCharacteristics))
	}

	if sti.TxState != "" {
		parts = append(parts, fmt.Sprintf("tx_state: %s", sti.TxState))
	}

	return strings.Join(parts, "; ")
}

// SessionStateManager 会话状态管理器
type SessionStateManager struct {
	tracker           *SessionTracker
	currentSchema     string
	systemVariables   map[string]string
	inTransaction     bool
	txCharacteristics string
}

// NewSessionStateManager 创建会话状态管理器
func NewSessionStateManager(trackingEnabled bool) *SessionStateManager {
	return &SessionStateManager{
		tracker:         NewSessionTracker(trackingEnabled),
		systemVariables: make(map[string]string),
	}
}

// SetSystemVariable 设置系统变量
func (ssm *SessionStateManager) SetSystemVariable(name, value string) {
	oldValue, exists := ssm.systemVariables[name]
	ssm.systemVariables[name] = value

	// 如果值发生变化，记录跟踪
	if !exists || oldValue != value {
		ssm.tracker.TrackSystemVariable(name, value)
		ssm.tracker.TrackStateChange(true)
	}
}

// GetSystemVariable 获取系统变量
func (ssm *SessionStateManager) GetSystemVariable(name string) (string, bool) {
	value, exists := ssm.systemVariables[name]
	return value, exists
}

// SetSchema 设置当前数据库
func (ssm *SessionStateManager) SetSchema(schema string) {
	if ssm.currentSchema != schema {
		ssm.currentSchema = schema
		ssm.tracker.TrackSchema(schema)
		ssm.tracker.TrackStateChange(true)
	}
}

// GetSchema 获取当前数据库
func (ssm *SessionStateManager) GetSchema() string {
	return ssm.currentSchema
}

// BeginTransaction 开始事务
func (ssm *SessionStateManager) BeginTransaction(characteristics string) {
	ssm.inTransaction = true
	ssm.txCharacteristics = characteristics

	if characteristics != "" {
		ssm.tracker.TrackTransactionCharacteristics(characteristics)
	}
	ssm.tracker.TrackTransactionState("T_______")
	ssm.tracker.TrackStateChange(true)
}

// CommitTransaction 提交事务
func (ssm *SessionStateManager) CommitTransaction() {
	ssm.inTransaction = false
	ssm.txCharacteristics = ""

	ssm.tracker.TrackTransactionState("________")
	ssm.tracker.TrackStateChange(true)
}

// RollbackTransaction 回滚事务
func (ssm *SessionStateManager) RollbackTransaction() {
	ssm.inTransaction = false
	ssm.txCharacteristics = ""

	ssm.tracker.TrackTransactionState("________")
	ssm.tracker.TrackStateChange(true)
}

// IsInTransaction 是否在事务中
func (ssm *SessionStateManager) IsInTransaction() bool {
	return ssm.inTransaction
}

// GetTracker 获取跟踪器
func (ssm *SessionStateManager) GetTracker() *SessionTracker {
	return ssm.tracker
}

// ClearTracking 清空跟踪信息
func (ssm *SessionStateManager) ClearTracking() {
	ssm.tracker.Clear()
}

// GetTrackingData 获取跟踪数据（用于OK包）
func (ssm *SessionStateManager) GetTrackingData() []byte {
	return ssm.tracker.EncodeForOKPacket()
}

// TransactionState 事务状态常量
const (
	TxStateIdle                   = "________" // 空闲
	TxStateActive                 = "T_______" // 活动事务
	TxStateReadOnly               = "T_____r_" // 只读事务
	TxStateReadWrite              = "T_____w_" // 读写事务
	TxStateWithConsistentSnapshot = "T___s___" // 一致性快照
)

// GetTransactionStateString 获取事务状态字符串
func GetTransactionStateString(inTransaction, readOnly, hasSnapshot bool) string {
	if !inTransaction {
		return TxStateIdle
	}

	state := []byte("T_______")

	if readOnly {
		state[6] = 'r'
	} else {
		state[6] = 'w'
	}

	if hasSnapshot {
		state[4] = 's'
	}

	return string(state)
}

// SessionTrackTypeString 获取跟踪类型的字符串表示
func SessionTrackTypeString(trackType SessionTrackType) string {
	switch trackType {
	case SessionTrackSystemVariables:
		return "SYSTEM_VARIABLES"
	case SessionTrackSchema:
		return "SCHEMA"
	case SessionTrackStateChange:
		return "STATE_CHANGE"
	case SessionTrackGTIDs:
		return "GTIDS"
	case SessionTrackTransactionCharacteristics:
		return "TRANSACTION_CHARACTERISTICS"
	case SessionTrackTransactionState:
		return "TRANSACTION_STATE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", trackType)
	}
}
