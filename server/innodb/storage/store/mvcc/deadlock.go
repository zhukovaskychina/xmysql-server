package mvcc

import (
	"sync"
)

// DeadlockDetector 死锁检测器
type DeadlockDetector struct {
	mu           sync.RWMutex
	waitForGraph map[uint64]map[uint64]bool // txnID -> set of txnIDs it's waiting for
}

// NewDeadlockDetector 创建死锁检测器
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		waitForGraph: make(map[uint64]map[uint64]bool),
	}
}

// AddWaitFor 添加等待关系
func (dd *DeadlockDetector) AddWaitFor(waiter, holder uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	if dd.waitForGraph[waiter] == nil {
		dd.waitForGraph[waiter] = make(map[uint64]bool)
	}
	dd.waitForGraph[waiter][holder] = true
}

// RemoveWaitFor 移除等待关系
func (dd *DeadlockDetector) RemoveWaitFor(waiter, holder uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	if waitSet, exists := dd.waitForGraph[waiter]; exists {
		delete(waitSet, holder)
		if len(waitSet) == 0 {
			delete(dd.waitForGraph, waiter)
		}
	}
}

// RemoveTransaction 移除事务的所有等待关系
func (dd *DeadlockDetector) RemoveTransaction(txnID uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	// 移除作为等待者的关系
	delete(dd.waitForGraph, txnID)

	// 移除作为被等待者的关系
	for waiter, waitSet := range dd.waitForGraph {
		delete(waitSet, txnID)
		if len(waitSet) == 0 {
			delete(dd.waitForGraph, waiter)
		}
	}
}

// WouldCauseCycle 检查添加新的等待关系是否会导致死锁
func (dd *DeadlockDetector) WouldCauseCycle(waiter uint64, resourceID string) bool {
	dd.mu.RLock()
	defer dd.mu.RUnlock()

	visited := make(map[uint64]bool)
	return dd.dfs(waiter, visited)
}

// dfs 深度优先搜索检测环
func (dd *DeadlockDetector) dfs(current uint64, visited map[uint64]bool) bool {
	if visited[current] {
		return true // 发现环
	}

	visited[current] = true
	for next := range dd.waitForGraph[current] {
		if dd.dfs(next, visited) {
			return true
		}
	}
	delete(visited, current)
	return false
}

// GetWaitForGraph 获取等待图的快照(用于调试)
func (dd *DeadlockDetector) GetWaitForGraph() map[uint64][]uint64 {
	dd.mu.RLock()
	defer dd.mu.RUnlock()

	result := make(map[uint64][]uint64)
	for waiter, waitSet := range dd.waitForGraph {
		holders := make([]uint64, 0, len(waitSet))
		for holder := range waitSet {
			holders = append(holders, holder)
		}
		result[waiter] = holders
	}
	return result
}

// GetDeadlockedTransactions 获取死锁的事务集合
func (dd *DeadlockDetector) GetDeadlockedTransactions() []uint64 {
	dd.mu.RLock()
	defer dd.mu.RUnlock()

	deadlocked := make(map[uint64]bool)
	visited := make(map[uint64]bool)
	stack := make(map[uint64]bool)

	// 对每个事务进行DFS
	for txnID := range dd.waitForGraph {
		if !visited[txnID] {
			dd.findDeadlocks(txnID, visited, stack, deadlocked)
		}
	}

	// 转换为切片
	result := make([]uint64, 0, len(deadlocked))
	for txnID := range deadlocked {
		result = append(result, txnID)
	}
	return result
}

// findDeadlocks 使用Tarjan算法查找强连通分量(死锁)
func (dd *DeadlockDetector) findDeadlocks(current uint64, visited, stack, deadlocked map[uint64]bool) {
	visited[current] = true
	stack[current] = true

	// 遍历当前事务等待的所有事务
	for next := range dd.waitForGraph[current] {
		if !visited[next] {
			dd.findDeadlocks(next, visited, stack, deadlocked)
		} else if stack[next] {
			// 发现环，将环上的所有事务标记为死锁
			deadlocked[current] = true
			deadlocked[next] = true
		}
	}

	delete(stack, current)
}
