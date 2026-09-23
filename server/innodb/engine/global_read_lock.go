package engine

import (
	"context"
	"sync"
)

// globalReadLockGate is a context-aware reader/writer gate used for
// FLUSH TABLES WITH READ LOCK. A sync.RWMutex cannot be interrupted while a
// writer waits for existing readers, which would make KILL QUERY ineffective
// for a statement blocked by the global read lock.
type globalReadLockGate struct {
	init    sync.Once
	mu      sync.Mutex
	changed chan struct{}
	readers int
	writer  bool
}

func (g *globalReadLockGate) ensureInitialized() {
	g.init.Do(func() {
		g.changed = make(chan struct{})
	})
}

func (g *globalReadLockGate) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

func (g *globalReadLockGate) RLock(ctx context.Context) error {
	g.ensureInitialized()
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		g.mu.Lock()
		if !g.writer {
			g.readers++
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (g *globalReadLockGate) RUnlock() {
	g.ensureInitialized()
	g.mu.Lock()
	if g.readers > 0 {
		g.readers--
		if g.readers == 0 {
			g.signalLocked()
		}
	}
	g.mu.Unlock()
}

func (g *globalReadLockGate) Lock(ctx context.Context) error {
	g.ensureInitialized()
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		g.mu.Lock()
		if !g.writer && g.readers == 0 {
			g.writer = true
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (g *globalReadLockGate) Unlock() {
	g.ensureInitialized()
	g.mu.Lock()
	if g.writer {
		g.writer = false
		g.signalLocked()
	}
	g.mu.Unlock()
}
