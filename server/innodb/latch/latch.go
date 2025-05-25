package latch

import "sync"

// Latch 提供了一个简单的锁机制
type Latch struct {
	mu sync.RWMutex
}

// NewLatch 创建一个新的锁
func NewLatch() *Latch {
	return &Latch{}
}

// Lock 获取写锁
func (l *Latch) Lock() {
	l.mu.Lock()
}

// Unlock 释放写锁
func (l *Latch) Unlock() {
	l.mu.Unlock()
}

// RLock 获取读锁
func (l *Latch) RLock() {
	l.mu.RLock()
}

// RUnlock 释放读锁
func (l *Latch) RUnlock() {
	l.mu.RUnlock()
}

// TryLock 尝试获取写锁
func (l *Latch) TryLock() bool {
	return l.mu.TryLock()
}

// TryRLock 尝试获取读锁
func (l *Latch) TryRLock() bool {
	return l.mu.TryRLock()
}
