package mvcc

import (
	"fmt"
)

// LockMode 锁模式类型
type LockMode int

// 锁模式常量定义
const (
	// LockModeNone 无锁
	LockModeNone LockMode = iota

	// LockModeShared 共享锁(S锁)
	// 允许多个事务同时持有，但不允许修改
	LockModeShared

	// LockModeExclusive 排他锁(X锁)
	// 只允许一个事务持有，允许读写
	LockModeExclusive

	// LockModeIntentionShared 意向共享锁(IS锁)
	// 表示事务打算在更细粒度上获取S锁
	LockModeIntentionShared

	// LockModeIntentionExclusive 意向排他锁(IX锁)
	// 表示事务打算在更细粒度上获取X锁
	LockModeIntentionExclusive

	// LockModeSchemaShared 模式共享锁(Sch-S锁)
	// 用于DDL操作的共享锁
	LockModeSchemaShared

	// LockModeSchemaModification 模式修改锁(Sch-M锁)
	// 用于DDL操作的排他锁
	LockModeSchemaModification

	// LockModeUpdate 更新锁(U锁)
	// 用于防止死锁的特殊锁模式
	LockModeUpdate
)

// String 返回锁模式的字符串表示
func (lm LockMode) String() string {
	switch lm {
	case LockModeNone:
		return "NONE"
	case LockModeShared:
		return "SHARED"
	case LockModeExclusive:
		return "EXCLUSIVE"
	case LockModeIntentionShared:
		return "INTENTION_SHARED"
	case LockModeIntentionExclusive:
		return "INTENTION_EXCLUSIVE"
	case LockModeSchemaShared:
		return "SCHEMA_SHARED"
	case LockModeSchemaModification:
		return "SCHEMA_MODIFICATION"
	case LockModeUpdate:
		return "UPDATE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(lm))
	}
}

// IsCompatible 检查两个锁模式是否兼容
func (lm LockMode) IsCompatible(other LockMode) bool {
	// 锁兼容性矩阵
	// 格式：[当前锁][请求锁] = 是否兼容
	compatibilityMatrix := [8][8]bool{
		// NONE, S,   X,   IS,  IX,  Sch-S, Sch-M, U
		{true, true, true, true, true, true, true, true},        // NONE
		{true, true, false, true, false, true, false, false},    // SHARED
		{true, false, false, false, false, false, false, false}, // EXCLUSIVE
		{true, true, false, true, true, true, false, true},      // INTENTION_SHARED
		{true, false, false, true, true, false, false, false},   // INTENTION_EXCLUSIVE
		{true, true, false, true, false, true, false, true},     // SCHEMA_SHARED
		{true, false, false, false, false, false, false, false}, // SCHEMA_MODIFICATION
		{true, false, false, true, false, true, false, false},   // UPDATE
	}

	// 确保索引在有效范围内
	if int(lm) >= len(compatibilityMatrix) || int(other) >= len(compatibilityMatrix[0]) {
		return false
	}

	return compatibilityMatrix[lm][other]
}

// IsExclusive 检查是否为排他性锁
func (lm LockMode) IsExclusive() bool {
	return lm == LockModeExclusive || lm == LockModeSchemaModification
}

// IsShared 检查是否为共享性锁
func (lm LockMode) IsShared() bool {
	return lm == LockModeShared || lm == LockModeSchemaShared
}

// IsIntentionLock 检查是否为意向锁
func (lm LockMode) IsIntentionLock() bool {
	return lm == LockModeIntentionShared || lm == LockModeIntentionExclusive
}

// IsSchemaLock 检查是否为模式锁
func (lm LockMode) IsSchemaLock() bool {
	return lm == LockModeSchemaShared || lm == LockModeSchemaModification
}

// GetStrength 获取锁的强度等级(数值越大强度越高)
func (lm LockMode) GetStrength() int {
	switch lm {
	case LockModeNone:
		return 0
	case LockModeIntentionShared:
		return 1
	case LockModeShared:
		return 2
	case LockModeUpdate:
		return 3
	case LockModeIntentionExclusive:
		return 4
	case LockModeSchemaShared:
		return 5
	case LockModeExclusive:
		return 6
	case LockModeSchemaModification:
		return 7
	default:
		return -1
	}
}

// CanUpgradeTo 检查是否可以升级到指定锁模式
func (lm LockMode) CanUpgradeTo(target LockMode) bool {
	// 只能从低强度锁升级到高强度锁
	currentStrength := lm.GetStrength()
	targetStrength := target.GetStrength()

	if currentStrength < 0 || targetStrength < 0 {
		return false
	}

	return currentStrength <= targetStrength
}

// LockRequest 锁请求结构
type LockRequest struct {
	TxID     uint64   // 事务ID
	Mode     LockMode // 锁模式
	Resource string   // 锁定的资源标识
	Granted  bool     // 是否已授予
}

// NewLockRequest 创建新的锁请求
func NewLockRequest(txID uint64, mode LockMode, resource string) *LockRequest {
	return &LockRequest{
		TxID:     txID,
		Mode:     mode,
		Resource: resource,
		Granted:  false,
	}
}

// String 返回锁请求的字符串表示
func (lr *LockRequest) String() string {
	status := "WAITING"
	if lr.Granted {
		status = "GRANTED"
	}
	return fmt.Sprintf("LockRequest{TxID: %d, Mode: %s, Resource: %s, Status: %s}",
		lr.TxID, lr.Mode.String(), lr.Resource, status)
}
