package manager

import (
	"sync"
	"testing"
)

func TestEnhancedBTreeManagerCloseIsSafeForConcurrentCallers(t *testing.T) {
	manager, bufferPool := newTestEnhancedBTreeManagerForP0(t)
	t.Cleanup(func() {
		_ = bufferPool.Close()
	})

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- manager.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent close returned error: %v", err)
		}
	}
}
