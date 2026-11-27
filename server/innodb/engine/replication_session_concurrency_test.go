package engine

import (
	"sync"
	"testing"
)

func TestReplicationSessionParametersAreSafeForConcurrentAccess(t *testing.T) {
	session := newReplicationSession()
	const workers = 8
	const iterations = 1000

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				name := "replication_param_" + string(rune('a'+worker))
				session.SetParamByName(name, iteration)
				if got := session.GetParamByName(name); got == nil {
					t.Errorf("parameter %s disappeared during concurrent access", name)
				}
			}
		}(worker)
	}
	wg.Wait()
}
