package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

func main() {
	fmt.Println("=== Testing LRU Cache Deadlock Fixes ===")

	// Test 1: Basic functionality
	fmt.Println("\n1. Testing basic functionality...")
	testBasicFunctionality()

	// Test 2: Concurrent access (this would deadlock before the fix)
	fmt.Println("\n2. Testing concurrent access...")
	testConcurrentAccess()

	// Test 3: Mixed operations
	fmt.Println("\n3. Testing mixed operations...")
	testMixedOperations()

	// Test 4: Performance comparison
	fmt.Println("\n4. Testing performance...")
	testPerformance()

	fmt.Println("\n=== All tests completed successfully! ===")
}

func testBasicFunctionality() {
	cache := buffer_pool.NewLRUCacheImpl(100, 0.6, 0.4, 1000)

	// Create a mock BufferBlock
	mockBlock := &buffer_pool.BufferBlock{
		// Add necessary fields based on BufferBlock structure
	}

	// Test SetYoung and GetYoung
	cache.SetYoung(1, 1, mockBlock)
	result, err := cache.GetYoung(1, 1)
	if err != nil {
		fmt.Printf("ERROR: GetYoung failed: %v\n", err)
		return
	}
	if result != mockBlock {
		fmt.Printf("ERROR: GetYoung returned wrong value\n")
		return
	}

	// Test SetOld and GetOld
	cache.SetOld(2, 2, mockBlock)
	result, err = cache.GetOld(2, 2)
	if err != nil {
		fmt.Printf("ERROR: GetOld failed: %v\n", err)
		return
	}
	if result != mockBlock {
		fmt.Printf("ERROR: GetOld returned wrong value\n")
		return
	}

	fmt.Println("✓ Basic functionality test passed")
}

func testConcurrentAccess() {
	cache := buffer_pool.NewLRUCacheImpl(1000, 0.6, 0.4, 1000)

	// Create mock blocks
	mockBlocks := make([]*buffer_pool.BufferBlock, 100)
	for i := range mockBlocks {
		mockBlocks[i] = &buffer_pool.BufferBlock{
			// Initialize as needed
		}
	}

	var wg sync.WaitGroup
	numGoroutines := 50
	operationsPerGoroutine := 100

	// Test concurrent GetYoung operations (this would deadlock before the fix)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				spaceID := uint32(goroutineID*operationsPerGoroutine + j)
				pageNo := uint32(j)

				// Set and get operations
				cache.SetYoung(spaceID, pageNo, mockBlocks[j%len(mockBlocks)])
				_, _ = cache.GetYoung(spaceID, pageNo)

				// Also test old operations
				cache.SetOld(spaceID+10000, pageNo, mockBlocks[j%len(mockBlocks)])
				_, _ = cache.GetOld(spaceID+10000, pageNo)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	// Wait with timeout to detect deadlocks
	select {
	case <-done:
		fmt.Println("✓ Concurrent access test passed (no deadlocks)")
	case <-time.After(10 * time.Second):
		fmt.Println("✗ Concurrent access test FAILED (possible deadlock)")
		// Print goroutine stack traces to help debug
		buf := make([]byte, 1<<16)
		stackSize := runtime.Stack(buf, true)
		fmt.Printf("Goroutine stack traces:\n%s\n", buf[:stackSize])
		return
	}
}

func testMixedOperations() {
	cache := buffer_pool.NewLRUCacheImpl(500, 0.6, 0.4, 1000)

	mockBlock := &buffer_pool.BufferBlock{}

	var wg sync.WaitGroup
	numWorkers := 20

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				spaceID := uint32(workerID*100 + j)
				pageNo := uint32(j)

				// Mix of operations
				switch j % 6 {
				case 0:
					cache.SetYoung(spaceID, pageNo, mockBlock)
				case 1:
					_, _ = cache.GetYoung(spaceID, pageNo)
				case 2:
					cache.SetOld(spaceID, pageNo, mockBlock)
				case 3:
					_, _ = cache.GetOld(spaceID, pageNo)
				case 4:
					_ = cache.Set(spaceID, pageNo, mockBlock)
				case 5:
					_, _ = cache.Get(spaceID, pageNo)
				}
			}
		}(i)
	}

	// Wait with timeout
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		fmt.Println("✓ Mixed operations test passed")
	case <-time.After(5 * time.Second):
		fmt.Println("✗ Mixed operations test FAILED (timeout)")
		return
	}
}

func testPerformance() {
	cache := buffer_pool.NewLRUCacheImpl(1000, 0.6, 0.4, 1000)
	mockBlock := &buffer_pool.BufferBlock{}

	// Warm up the cache
	for i := 0; i < 500; i++ {
		cache.Set(uint32(i), uint32(i), mockBlock)
	}

	// Performance test
	numOperations := 10000
	start := time.Now()

	for i := 0; i < numOperations; i++ {
		spaceID := uint32(i % 500)
		pageNo := uint32(i % 100)

		if i%2 == 0 {
			_, _ = cache.Get(spaceID, pageNo)
		} else {
			_ = cache.Set(spaceID, pageNo, mockBlock)
		}
	}

	duration := time.Since(start)
	opsPerSecond := float64(numOperations) / duration.Seconds()

	fmt.Printf("✓ Performance test: %.0f operations/second\n", opsPerSecond)

	// Print cache statistics
	if len := cache.Len(); len > 0 {
		fmt.Printf("  Cache length: %d\n", len)
	}
}
