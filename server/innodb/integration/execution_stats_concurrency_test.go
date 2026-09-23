package integration

import (
	"sync"
	"testing"
)

func TestExecutionIntegrationAccessMethodStatsAreConcurrentSafe(t *testing.T) {
	integrator := &ExecutionEngineIntegrator{executionStats: &ExecutionIntegrationStats{}}

	const callers = 64
	const scansPerCaller = 25
	var wg sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for scan := 0; scan < scansPerCaller; scan++ {
				integrator.recordAccessMethod(AccessMethodIndexScan)
				integrator.recordAccessMethod(AccessMethodTableScan)
			}
		}()
	}
	wg.Wait()

	stats := integrator.GetExecutionStats()
	want := uint64(callers * scansPerCaller)
	if stats.IndexScansUsed != want || stats.TableScansUsed != want {
		t.Fatalf("access method stats = index %d/table %d, want %d/%d", stats.IndexScansUsed, stats.TableScansUsed, want, want)
	}
}
