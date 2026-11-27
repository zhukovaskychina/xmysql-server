package dispatcher

import (
	"sync"
	"testing"
)

func TestEnhancedMockMySQLServerSessionConcurrentParamAccess(t *testing.T) {
	session := NewEnhancedMockMySQLServerSession("concurrent-session", "app")

	const workers = 32
	const iterations = 1000
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				session.SetParamByName("row_count", int64(worker+iteration))
				_ = session.GetParamByName("row_count")
			}
		}()
	}
	waitGroup.Wait()
}
