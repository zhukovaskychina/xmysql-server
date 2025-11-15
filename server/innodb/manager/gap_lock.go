package manager

import (
	"fmt"
	"time"
)

// ============ TXN-012: Gap锁实现 ============

// makeGapLockKey 生成Gap锁的键
func makeGapLockKey(tableID, indexID uint32) string {
	return fmt.Sprintf("gap_%d_%d", tableID, indexID)
}

// makeNextKeyLockKey 生成Next-Key锁的键
func makeNextKeyLockKey(tableID, indexID uint32) string {
	return fmt.Sprintf("nextkey_%d_%d", tableID, indexID)
}

// AcquireGapLock 获取Gap锁
// Gap锁用于锁定索引记录之间的间隙，防止幻读
// 注意: Gap锁之间不会冲突(S-Gap和X-Gap可以共存)，但会与插入意向锁冲突
func (lm *LockManager) AcquireGapLock(txID uint64, gapRange *GapRange, lockType LockType) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if gapRange == nil {
		return fmt.Errorf("gap range cannot be nil")
	}

	key := makeGapLockKey(gapRange.TableID, gapRange.IndexID)

	// 检查是否已持有相同的Gap锁
	if locks, exists := lm.gapLocks[key]; exists {
		for _, lock := range locks {
			if lock.TxID == txID && lock.Granted {
				if gapRangesEqual(lock.GapRange, gapRange) {
					return nil // 已持有相同的Gap锁
				}
			}
		}
	}

	// 检查与插入意向锁的冲突
	// Gap锁会阻止插入意向锁
	var holdingTxIDs []uint64
	if insertLocks, exists := lm.insertIntLocks[key]; exists {
		for _, ilock := range insertLocks {
			if ilock.Granted && gapRangeContains(gapRange, ilock.InsertKey) {
				holdingTxIDs = append(holdingTxIDs, ilock.TxID)
			}
		}
	}

	// 创建Gap锁请求
	newLock := &GapLockInfo{
		TxID:       txID,
		LockType:   lockType,
		GapRange:   gapRange,
		Granted:    len(holdingTxIDs) == 0,
		WaitChan:   make(chan bool, 1),
		CreateTime: time.Now(),
	}

	// 添加到Gap锁表
	if lm.gapLocks[key] == nil {
		lm.gapLocks[key] = make([]*GapLockInfo, 0)
	}
	lm.gapLocks[key] = append(lm.gapLocks[key], newLock)

	// 如果需要等待，更新等待图
	if len(holdingTxIDs) > 0 {
		lm.updateWaitGraph(txID, holdingTxIDs)
		// 检查死锁
		visited := make(map[uint64]bool)
		if lm.checkDeadlock(txID, visited) {
			// 移除Gap锁请求
			lm.gapLocks[key] = lm.gapLocks[key][:len(lm.gapLocks[key])-1]
			return fmt.Errorf("deadlock detected")
		}

		// 等待锁被授予
		lm.mu.Unlock()
		select {
		case <-newLock.WaitChan:
			// 锁已授予
			lm.mu.Lock()
		case <-time.After(30 * time.Second):
			// 超时
			lm.mu.Lock()
			// 移除Gap锁请求
			locks := lm.gapLocks[key]
			var newLocks []*GapLockInfo
			for _, lock := range locks {
				if lock.TxID != txID || !gapRangesEqual(lock.GapRange, gapRange) {
					newLocks = append(newLocks, lock)
				}
			}
			lm.gapLocks[key] = newLocks
			lm.removeFromWaitGraph(txID)
			return fmt.Errorf("lock wait timeout")
		}
	}

	// 记录事务持有的Gap锁
	lm.txnGapLocks[txID] = append(lm.txnGapLocks[txID], key)

	return nil
}

// ReleaseGapLock 释放Gap锁
func (lm *LockManager) ReleaseGapLock(txID uint64, gapRange *GapRange) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if gapRange == nil {
		return fmt.Errorf("gap range cannot be nil")
	}

	key := makeGapLockKey(gapRange.TableID, gapRange.IndexID)
	locks, exists := lm.gapLocks[key]
	if !exists {
		return nil // 没有Gap锁
	}

	// 移除指定的Gap锁
	var newLocks []*GapLockInfo
	for _, lock := range locks {
		if lock.TxID != txID || !gapRangesEqual(lock.GapRange, gapRange) {
			newLocks = append(newLocks, lock)
		}
	}

	if len(newLocks) == 0 {
		delete(lm.gapLocks, key)
	} else {
		lm.gapLocks[key] = newLocks
	}

	// 尝试授予等待的插入意向锁（无论Gap锁是否完全释放）
	lm.grantWaitingInsertIntentionLocks(key)

	return nil
}

// ReleaseAllGapLocks 释放事务持有的所有Gap锁
func (lm *LockManager) ReleaseAllGapLocks(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 获取事务持有的所有Gap锁键
	keys := lm.txnGapLocks[txID]
	delete(lm.txnGapLocks, txID)

	// 释放每个键上的Gap锁
	for _, key := range keys {
		locks := lm.gapLocks[key]
		if locks == nil {
			continue
		}

		// 移除该事务的Gap锁
		var newLocks []*GapLockInfo
		for _, lock := range locks {
			if lock.TxID != txID {
				newLocks = append(newLocks, lock)
			}
		}

		if len(newLocks) == 0 {
			delete(lm.gapLocks, key)
		} else {
			lm.gapLocks[key] = newLocks
		}

		// 尝试授予等待的插入意向锁
		lm.grantWaitingInsertIntentionLocks(key)
	}
}

// ============ TXN-013: Next-Key锁实现 ============

// AcquireNextKeyLock 获取Next-Key锁
// Next-Key锁 = Record Lock + Gap Lock (记录锁+记录之前的间隙锁)
// 用于REPEATABLE READ隔离级别下防止幻读
func (lm *LockManager) AcquireNextKeyLock(txID uint64, recordKey interface{}, gapRange *GapRange, lockType LockType) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if gapRange == nil {
		return fmt.Errorf("gap range cannot be nil")
	}

	key := makeNextKeyLockKey(gapRange.TableID, gapRange.IndexID)

	// 检查是否已持有相同的Next-Key锁
	if locks, exists := lm.nextKeyLocks[key]; exists {
		for _, lock := range locks {
			if lock.TxID == txID && lock.Granted {
				if compareKeys(lock.RecordKey, recordKey) == 0 && gapRangesEqual(lock.GapRange, gapRange) {
					return nil // 已持有相同的Next-Key锁
				}
			}
		}
	}

	// 检查与其他锁的冲突
	var holdingTxIDs []uint64

	// 1. 检查与Record Lock的冲突
	// TODO: 这里应该调用现有的Record Lock检查逻辑

	// 2. 检查与插入意向锁的冲突
	if insertLocks, exists := lm.insertIntLocks[key]; exists {
		for _, ilock := range insertLocks {
			if ilock.Granted && gapRangeContains(gapRange, ilock.InsertKey) {
				holdingTxIDs = append(holdingTxIDs, ilock.TxID)
			}
		}
	}

	// 创建Next-Key锁请求
	newLock := &NextKeyLockInfo{
		TxID:       txID,
		LockType:   lockType,
		RecordKey:  recordKey,
		GapRange:   gapRange,
		Granted:    len(holdingTxIDs) == 0,
		WaitChan:   make(chan bool, 1),
		CreateTime: time.Now(),
	}

	// 添加到Next-Key锁表
	if lm.nextKeyLocks[key] == nil {
		lm.nextKeyLocks[key] = make([]*NextKeyLockInfo, 0)
	}
	lm.nextKeyLocks[key] = append(lm.nextKeyLocks[key], newLock)

	// 如果需要等待，更新等待图
	if len(holdingTxIDs) > 0 {
		lm.updateWaitGraph(txID, holdingTxIDs)
		// 检查死锁
		visited := make(map[uint64]bool)
		if lm.checkDeadlock(txID, visited) {
			// 移除Next-Key锁请求
			lm.nextKeyLocks[key] = lm.nextKeyLocks[key][:len(lm.nextKeyLocks[key])-1]
			return fmt.Errorf("deadlock detected")
		}

		// 等待锁被授予
		lm.mu.Unlock()
		select {
		case <-newLock.WaitChan:
			// 锁已授予
			lm.mu.Lock()
		case <-time.After(30 * time.Second):
			// 超时
			lm.mu.Lock()
			// 移除Next-Key锁请求
			locks := lm.nextKeyLocks[key]
			var newLocks []*NextKeyLockInfo
			for _, lock := range locks {
				if lock.TxID != txID || compareKeys(lock.RecordKey, recordKey) != 0 || !gapRangesEqual(lock.GapRange, gapRange) {
					newLocks = append(newLocks, lock)
				}
			}
			lm.nextKeyLocks[key] = newLocks
			lm.removeFromWaitGraph(txID)
			return fmt.Errorf("lock wait timeout")
		}
	}

	// 记录事务持有的Next-Key锁
	lm.txnNextKeyLocks[txID] = append(lm.txnNextKeyLocks[txID], key)

	return nil
}

// ReleaseNextKeyLock 释放Next-Key锁
func (lm *LockManager) ReleaseNextKeyLock(txID uint64, recordKey interface{}, gapRange *GapRange) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if gapRange == nil {
		return fmt.Errorf("gap range cannot be nil")
	}

	key := makeNextKeyLockKey(gapRange.TableID, gapRange.IndexID)
	locks, exists := lm.nextKeyLocks[key]
	if !exists {
		return nil // 没有Next-Key锁
	}

	// 移除指定的Next-Key锁
	var newLocks []*NextKeyLockInfo
	for _, lock := range locks {
		if lock.TxID != txID || compareKeys(lock.RecordKey, recordKey) != 0 || !gapRangesEqual(lock.GapRange, gapRange) {
			newLocks = append(newLocks, lock)
		}
	}

	if len(newLocks) == 0 {
		delete(lm.nextKeyLocks, key)
	} else {
		lm.nextKeyLocks[key] = newLocks
	}

	// 尝试授予等待的锁（无论Next-Key锁是否完全释放）
	lm.grantWaitingNextKeyLocks(key)

	// Next-Key锁包含Gap锁，释放后也需要尝试授予插入意向锁
	gapKey := makeGapLockKey(gapRange.TableID, gapRange.IndexID)
	lm.grantWaitingInsertIntentionLocks(gapKey)

	return nil
}

// ReleaseAllNextKeyLocks 释放事务持有的所有Next-Key锁
func (lm *LockManager) ReleaseAllNextKeyLocks(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 获取事务持有的所有Next-Key锁键
	keys := lm.txnNextKeyLocks[txID]
	delete(lm.txnNextKeyLocks, txID)

	// 释放每个键上的Next-Key锁
	for _, key := range keys {
		locks := lm.nextKeyLocks[key]
		if locks == nil {
			continue
		}

		// 保存第一个锁的GapRange用于后续授予插入意向锁
		var gapRange *GapRange
		if len(locks) > 0 && locks[0].GapRange != nil {
			gapRange = locks[0].GapRange
		}

		// 移除该事务的Next-Key锁
		var newLocks []*NextKeyLockInfo
		for _, lock := range locks {
			if lock.TxID != txID {
				newLocks = append(newLocks, lock)
			}
		}

		if len(newLocks) == 0 {
			delete(lm.nextKeyLocks, key)
		} else {
			lm.nextKeyLocks[key] = newLocks
		}

		// 尝试授予等待的锁
		lm.grantWaitingNextKeyLocks(key)

		// Next-Key锁包含Gap锁，释放后也需要尝试授予插入意向锁
		if gapRange != nil {
			gapKey := makeGapLockKey(gapRange.TableID, gapRange.IndexID)
			lm.grantWaitingInsertIntentionLocks(gapKey)
		}
	}
}

// ============ 插入意向锁实现 ============

// AcquireInsertIntentionLock 获取插入意向锁
// 插入意向锁是一种特殊的Gap锁，表示插入意图
// 插入意向锁之间不冲突，但会与Gap锁和Next-Key锁冲突
func (lm *LockManager) AcquireInsertIntentionLock(txID uint64, insertKey interface{}, gapRange *GapRange) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if gapRange == nil {
		return fmt.Errorf("gap range cannot be nil")
	}

	key := makeGapLockKey(gapRange.TableID, gapRange.IndexID)

	// 检查与Gap锁的冲突
	var holdingTxIDs []uint64
	if gapLocks, exists := lm.gapLocks[key]; exists {
		for _, glock := range gapLocks {
			// Gap锁与插入意向锁冲突，除非是同一个事务
			if glock.Granted && glock.TxID != txID && gapRangeContains(glock.GapRange, insertKey) {
				holdingTxIDs = append(holdingTxIDs, glock.TxID)
			}
		}
	}

	// 检查与Next-Key锁的冲突
	nextKeyKey := makeNextKeyLockKey(gapRange.TableID, gapRange.IndexID)
	if nextKeyLocks, exists := lm.nextKeyLocks[nextKeyKey]; exists {
		for _, nklock := range nextKeyLocks {
			// Next-Key锁与插入意向锁冲突，除非是同一个事务
			if nklock.Granted && nklock.TxID != txID && gapRangeContains(nklock.GapRange, insertKey) {
				holdingTxIDs = append(holdingTxIDs, nklock.TxID)
			}
		}
	}

	// 创建插入意向锁请求
	newLock := &InsertIntentionLockInfo{
		TxID:       txID,
		InsertKey:  insertKey,
		GapRange:   gapRange,
		Granted:    len(holdingTxIDs) == 0,
		WaitChan:   make(chan bool, 1),
		CreateTime: time.Now(),
	}

	// 添加到插入意向锁表
	if lm.insertIntLocks[key] == nil {
		lm.insertIntLocks[key] = make([]*InsertIntentionLockInfo, 0)
	}
	lm.insertIntLocks[key] = append(lm.insertIntLocks[key], newLock)

	// 如果需要等待，更新等待图
	if len(holdingTxIDs) > 0 {
		lm.updateWaitGraph(txID, holdingTxIDs)
		// 检查死锁
		visited := make(map[uint64]bool)
		if lm.checkDeadlock(txID, visited) {
			// 移除插入意向锁请求
			lm.insertIntLocks[key] = lm.insertIntLocks[key][:len(lm.insertIntLocks[key])-1]
			return fmt.Errorf("deadlock detected")
		}

		// 等待锁被授予
		lm.mu.Unlock()
		select {
		case <-newLock.WaitChan:
			// 锁已授予
			lm.mu.Lock()
			return nil
		case <-time.After(30 * time.Second):
			// 超时
			lm.mu.Lock()
			// 移除插入意向锁请求
			locks := lm.insertIntLocks[key]
			var newLocks []*InsertIntentionLockInfo
			for _, lock := range locks {
				if lock.TxID != txID || lock.InsertKey != insertKey {
					newLocks = append(newLocks, lock)
				}
			}
			lm.insertIntLocks[key] = newLocks
			lm.removeFromWaitGraph(txID)
			return fmt.Errorf("lock wait timeout")
		}
	}

	return nil
}

// ============ 辅助函数 ============

// grantWaitingInsertIntentionLocks 授予等待的插入意向锁
func (lm *LockManager) grantWaitingInsertIntentionLocks(key string) {
	insertLocks := lm.insertIntLocks[key]
	if insertLocks == nil {
		return
	}

	gapLocks := lm.gapLocks[key]

	for _, ilock := range insertLocks {
		if ilock.Granted {
			continue
		}

		// 检查是否还有冲突的Gap锁
		hasConflict := false
		if gapLocks != nil {
			for _, glock := range gapLocks {
				// Gap锁与插入意向锁冲突，除非是同一个事务
				if glock.Granted && glock.TxID != ilock.TxID && gapRangeContains(glock.GapRange, ilock.InsertKey) {
					hasConflict = true
					break
				}
			}
		}

		if !hasConflict {
			ilock.Granted = true
			// 通知等待的事务
			select {
			case ilock.WaitChan <- true:
			default:
			}
		}
	}
}

// grantWaitingNextKeyLocks 授予等待的Next-Key锁
func (lm *LockManager) grantWaitingNextKeyLocks(key string) {
	locks := lm.nextKeyLocks[key]
	if locks == nil {
		return
	}

	var grantedLocks []*NextKeyLockInfo
	var waitingLocks []*NextKeyLockInfo

	// 分离已授予和等待的锁
	for _, lock := range locks {
		if lock.Granted {
			grantedLocks = append(grantedLocks, lock)
		} else {
			waitingLocks = append(waitingLocks, lock)
		}
	}

	// 尝试授予等待的锁
	for _, waiting := range waitingLocks {
		canGrant := true
		// 检查与已授予的锁的兼容性
		for _, granted := range grantedLocks {
			if !isNextKeyLockCompatible(granted, waiting) {
				canGrant = false
				break
			}
		}

		if canGrant {
			waiting.Granted = true
			grantedLocks = append(grantedLocks, waiting)
			// 通知等待的事务
			select {
			case waiting.WaitChan <- true:
			default:
			}
		}
	}
}

// isNextKeyLockCompatible 检查Next-Key锁兼容性
func isNextKeyLockCompatible(lock1, lock2 *NextKeyLockInfo) bool {
	// 如果锁定不同的记录，则兼容
	if compareKeys(lock1.RecordKey, lock2.RecordKey) != 0 {
		return true
	}

	// 锁定相同记录时，检查锁类型兼容性
	if lock1.LockType == LOCK_X || lock2.LockType == LOCK_X {
		return false
	}

	return true // S-S兼容
}

// gapRangesEqual 检查两个Gap范围是否相等
func gapRangesEqual(r1, r2 *GapRange) bool {
	if r1 == nil || r2 == nil {
		return r1 == r2
	}

	return r1.TableID == r2.TableID &&
		r1.IndexID == r2.IndexID &&
		compareKeys(r1.LowerBound, r2.LowerBound) == 0 &&
		compareKeys(r1.UpperBound, r2.UpperBound) == 0
}

// gapRangeContains 检查Gap范围是否包含指定键值
func gapRangeContains(gapRange *GapRange, key interface{}) bool {
	if gapRange == nil {
		return false
	}

	// key应该在 (LowerBound, UpperBound) 范围内
	if gapRange.LowerBound != nil && compareKeys(key, gapRange.LowerBound) <= 0 {
		return false
	}

	if gapRange.UpperBound != nil && compareKeys(key, gapRange.UpperBound) >= 0 {
		return false
	}

	return true
}

// compareKeys 比较两个键值
// 返回: -1 (k1 < k2), 0 (k1 == k2), 1 (k1 > k2)
func compareKeys(k1, k2 interface{}) int {
	if k1 == nil && k2 == nil {
		return 0
	}
	if k1 == nil {
		return -1
	}
	if k2 == nil {
		return 1
	}

	// 简单实现：支持常见类型
	switch v1 := k1.(type) {
	case int:
		v2 := k2.(int)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	case int64:
		v2 := k2.(int64)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2 := k2.(string)
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	default:
		// 默认认为相等
		return 0
	}
}
