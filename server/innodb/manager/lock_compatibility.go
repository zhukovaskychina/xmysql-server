package manager

// ============ TXN-012/013: 锁兼容性检查 ============

// LockCompatibilityMatrix 锁兼容性矩阵
// 用于检查不同类型和粒度的锁之间的兼容性

// IsGapLockCompatible 检查Gap锁兼容性
// Gap锁的特性:
// 1. Gap锁之间互相兼容 (S-Gap与S-Gap兼容, S-Gap与X-Gap兼容, X-Gap与X-Gap兼容)
// 2. Gap锁与插入意向锁冲突
// 3. Gap锁不关心记录锁
func IsGapLockCompatible(lock1Type, lock2Type LockType) bool {
	// Gap锁之间总是兼容的，无论是S还是X
	return true
}

// IsInsertIntentionLockCompatible 检查插入意向锁兼容性
// 插入意向锁的特性:
// 1. 插入意向锁之间互相兼容
// 2. 插入意向锁与Gap锁冲突
// 3. 插入意向锁与Next-Key锁冲突
func IsInsertIntentionLockCompatible(insertLock1, insertLock2 *InsertIntentionLockInfo) bool {
	// 插入意向锁之间总是兼容的
	return true
}

// IsGapLockCompatibleWithInsertIntention 检查Gap锁与插入意向锁的兼容性
// Gap锁会阻止插入意向锁
func IsGapLockCompatibleWithInsertIntention(gapLock *GapLockInfo, insertLock *InsertIntentionLockInfo) bool {
	// 检查插入键是否在Gap范围内
	if gapRangeContains(gapLock.GapRange, insertLock.InsertKey) {
		return false // 冲突
	}
	return true
}

// IsNextKeyLockCompatibleWithInsertIntention 检查Next-Key锁与插入意向锁的兼容性
// Next-Key锁会阻止插入意向锁
func IsNextKeyLockCompatibleWithInsertIntention(nextKeyLock *NextKeyLockInfo, insertLock *InsertIntentionLockInfo) bool {
	// 检查插入键是否在Gap范围内
	if gapRangeContains(nextKeyLock.GapRange, insertLock.InsertKey) {
		return false // 冲突
	}
	return true
}

// IsRecordLockCompatibleWithGap 检查Record Lock与Gap锁的兼容性
// Record Lock和Gap锁是独立的，它们之间兼容
func IsRecordLockCompatibleWithGap(recordLockType, gapLockType LockType) bool {
	// Record Lock和Gap锁是正交的，总是兼容
	return true
}

// IsNextKeyLockCompatibleWithRecordLock 检查Next-Key锁与Record Lock的兼容性
// Next-Key锁包含Record Lock部分，因此需要检查Record Lock兼容性
func IsNextKeyLockCompatibleWithRecordLock(nextKeyLockType, recordLockType LockType) bool {
	// 检查Record Lock部分的兼容性
	return isLockCompatible(nextKeyLockType, recordLockType)
}

// CheckLockConflict 综合检查锁冲突
// 返回冲突的事务ID列表
func (lm *LockManager) CheckLockConflict(
	txID uint64,
	granularity LockGranularity,
	lockType LockType,
	lockRange *LockRange,
) ([]uint64, error) {
	var conflictingTxIDs []uint64

	switch granularity {
	case LOCK_RECORD:
		// 检查Record Lock冲突
		resourceID := makeResourceID(lockRange.TableID, lockRange.PageID, lockRange.RowID)
		if info, exists := lm.lockTable[resourceID]; exists {
			for _, req := range info.Requests {
				if req.TxID != txID && req.Granted && !isLockCompatible(req.LockType, lockType) {
					conflictingTxIDs = append(conflictingTxIDs, req.TxID)
				}
			}
		}

	case LOCK_GAP:
		// 检查Gap锁冲突
		// Gap锁之间不冲突，只与插入意向锁冲突
		key := makeGapLockKey(lockRange.TableID, lockRange.GapRange.IndexID)
		if insertLocks, exists := lm.insertIntLocks[key]; exists {
			for _, ilock := range insertLocks {
				if ilock.TxID != txID && ilock.Granted {
					if gapRangeContains(lockRange.GapRange, ilock.InsertKey) {
						conflictingTxIDs = append(conflictingTxIDs, ilock.TxID)
					}
				}
			}
		}

	case LOCK_NEXT_KEY:
		// 检查Next-Key锁冲突
		// 1. 检查Record Lock部分
		resourceID := makeResourceID(lockRange.TableID, lockRange.PageID, lockRange.RowID)
		if info, exists := lm.lockTable[resourceID]; exists {
			for _, req := range info.Requests {
				if req.TxID != txID && req.Granted && !isLockCompatible(req.LockType, lockType) {
					conflictingTxIDs = append(conflictingTxIDs, req.TxID)
				}
			}
		}

		// 2. 检查Gap Lock部分
		key := makeNextKeyLockKey(lockRange.TableID, lockRange.GapRange.IndexID)
		if insertLocks, exists := lm.insertIntLocks[key]; exists {
			for _, ilock := range insertLocks {
				if ilock.TxID != txID && ilock.Granted {
					if gapRangeContains(lockRange.GapRange, ilock.InsertKey) {
						conflictingTxIDs = append(conflictingTxIDs, ilock.TxID)
					}
				}
			}
		}

	case LOCK_INSERT_INTENTION:
		// 检查插入意向锁冲突
		// 1. 检查与Gap锁的冲突
		gapKey := makeGapLockKey(lockRange.TableID, lockRange.GapRange.IndexID)
		if gapLocks, exists := lm.gapLocks[gapKey]; exists {
			for _, glock := range gapLocks {
				if glock.TxID != txID && glock.Granted {
					if gapRangeContains(glock.GapRange, lockRange.RecordKey) {
						conflictingTxIDs = append(conflictingTxIDs, glock.TxID)
					}
				}
			}
		}

		// 2. 检查与Next-Key锁的冲突
		nextKeyKey := makeNextKeyLockKey(lockRange.TableID, lockRange.GapRange.IndexID)
		if nextKeyLocks, exists := lm.nextKeyLocks[nextKeyKey]; exists {
			for _, nklock := range nextKeyLocks {
				if nklock.TxID != txID && nklock.Granted {
					if gapRangeContains(nklock.GapRange, lockRange.RecordKey) {
						conflictingTxIDs = append(conflictingTxIDs, nklock.TxID)
					}
				}
			}
		}
	}

	return conflictingTxIDs, nil
}

// GetLockCompatibilityMatrix 获取锁兼容性矩阵（用于调试和监控）
// 返回一个二维矩阵，表示不同锁类型之间的兼容性
func GetLockCompatibilityMatrix() map[string]map[string]bool {
	matrix := make(map[string]map[string]bool)

	// Record Lock兼容性矩阵
	matrix["RECORD_S"] = map[string]bool{
		"RECORD_S": true,  // S-S兼容
		"RECORD_X": false, // S-X不兼容
	}
	matrix["RECORD_X"] = map[string]bool{
		"RECORD_S": false, // X-S不兼容
		"RECORD_X": false, // X-X不兼容
	}

	// Gap Lock兼容性矩阵
	matrix["GAP_S"] = map[string]bool{
		"GAP_S":            true,  // S-Gap与S-Gap兼容
		"GAP_X":            true,  // S-Gap与X-Gap兼容
		"INSERT_INTENTION": false, // Gap与插入意向锁不兼容
	}
	matrix["GAP_X"] = map[string]bool{
		"GAP_S":            true,  // X-Gap与S-Gap兼容
		"GAP_X":            true,  // X-Gap与X-Gap兼容
		"INSERT_INTENTION": false, // Gap与插入意向锁不兼容
	}

	// Next-Key Lock兼容性矩阵
	matrix["NEXT_KEY_S"] = map[string]bool{
		"NEXT_KEY_S":       true,  // S-Next-Key与S-Next-Key兼容（相同记录）
		"NEXT_KEY_X":       false, // S-Next-Key与X-Next-Key不兼容
		"INSERT_INTENTION": false, // Next-Key与插入意向锁不兼容
	}
	matrix["NEXT_KEY_X"] = map[string]bool{
		"NEXT_KEY_S":       false, // X-Next-Key与S-Next-Key不兼容
		"NEXT_KEY_X":       false, // X-Next-Key与X-Next-Key不兼容
		"INSERT_INTENTION": false, // Next-Key与插入意向锁不兼容
	}

	// 插入意向锁兼容性矩阵
	matrix["INSERT_INTENTION"] = map[string]bool{
		"INSERT_INTENTION": true,  // 插入意向锁之间兼容
		"GAP_S":            false, // 插入意向锁与Gap不兼容
		"GAP_X":            false, // 插入意向锁与Gap不兼容
		"NEXT_KEY_S":       false, // 插入意向锁与Next-Key不兼容
		"NEXT_KEY_X":       false, // 插入意向锁与Next-Key不兼容
	}

	return matrix
}

// ExplainLockConflict 解释锁冲突原因（用于调试）
func ExplainLockConflict(gran1, gran2 LockGranularity, type1, type2 LockType) string {
	switch {
	case gran1 == LOCK_RECORD && gran2 == LOCK_RECORD:
		if type1 == LOCK_X || type2 == LOCK_X {
			return "Record Lock冲突: 排他锁与其他锁不兼容"
		}
		return "Record Lock兼容: 共享锁之间兼容"

	case gran1 == LOCK_GAP && gran2 == LOCK_GAP:
		return "Gap Lock兼容: Gap锁之间总是兼容"

	case gran1 == LOCK_GAP && gran2 == LOCK_INSERT_INTENTION:
		return "Gap Lock与插入意向锁冲突: Gap锁阻止插入"

	case gran1 == LOCK_INSERT_INTENTION && gran2 == LOCK_GAP:
		return "插入意向锁与Gap Lock冲突: Gap锁阻止插入"

	case gran1 == LOCK_NEXT_KEY && gran2 == LOCK_INSERT_INTENTION:
		return "Next-Key Lock与插入意向锁冲突: Next-Key锁阻止插入"

	case gran1 == LOCK_INSERT_INTENTION && gran2 == LOCK_NEXT_KEY:
		return "插入意向锁与Next-Key Lock冲突: Next-Key锁阻止插入"

	case gran1 == LOCK_INSERT_INTENTION && gran2 == LOCK_INSERT_INTENTION:
		return "插入意向锁兼容: 插入意向锁之间总是兼容"

	case gran1 == LOCK_NEXT_KEY && gran2 == LOCK_NEXT_KEY:
		if type1 == LOCK_X || type2 == LOCK_X {
			return "Next-Key Lock冲突: 排他锁与其他锁不兼容（Record部分）"
		}
		return "Next-Key Lock兼容: 共享锁之间兼容"

	default:
		return "其他锁组合: 需要具体分析"
	}
}
