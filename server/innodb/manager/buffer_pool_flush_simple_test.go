package manager

import (
	"testing"
	"time"
)

// TestCalculateFlushBatchSize 测试批量刷新大小计算
func TestCalculateFlushBatchSize(t *testing.T) {
	// 创建一个简单的 BufferPoolManager 用于测试
	bpm := &BufferPoolManager{
		config: &BufferPoolConfig{
			PoolSize: 1000,
		},
	}

	tests := []struct {
		name          string
		dirtyRatio    float64
		expectedBatch int
	}{
		{
			name:          "VeryLowDirtyRatio",
			dirtyRatio:    0.10, // 10%
			expectedBatch: 0,    // 不需要刷新
		},
		{
			name:          "LowDirtyRatio",
			dirtyRatio:    0.20, // 20%
			expectedBatch: 0,    // 不需要刷新
		},
		{
			name:          "LightFlush",
			dirtyRatio:    0.30,             // 30%
			expectedBatch: BATCH_FLUSH_SIZE, // 轻度刷新
		},
		{
			name:          "ModerateFlush",
			dirtyRatio:    0.60,                 // 60%
			expectedBatch: BATCH_FLUSH_SIZE * 2, // 中等刷新
		},
		{
			name:          "AggressiveFlush",
			dirtyRatio:    0.80,                 // 80%
			expectedBatch: BATCH_FLUSH_SIZE * 4, // 激进刷新
		},
		{
			name:          "VeryHighDirtyRatio",
			dirtyRatio:    0.95,                 // 95%
			expectedBatch: BATCH_FLUSH_SIZE * 4, // 激进刷新
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchSize := bpm.calculateFlushBatchSize(tt.dirtyRatio)
			if batchSize != tt.expectedBatch {
				t.Errorf("calculateFlushBatchSize(%.2f%%) = %d, expected %d",
					tt.dirtyRatio*100, batchSize, tt.expectedBatch)
			} else {
				t.Logf("✓ Dirty ratio %.2f%% -> batch size %d", tt.dirtyRatio*100, batchSize)
			}
		})
	}
}

// TestApplyRateLimit 测试速率限制
func TestApplyRateLimit(t *testing.T) {
	bpm := &BufferPoolManager{
		flushRateLimit: 100, // 100 pages/sec
		lastFlushTime:  time.Now().Add(-1 * time.Second),
	}

	tests := []struct {
		name           string
		requestedPages int
		elapsedTime    time.Duration
		maxAllowed     int
	}{
		{
			name:           "WithinLimit",
			requestedPages: 50,
			elapsedTime:    1 * time.Second,
			maxAllowed:     100,
		},
		{
			name:           "ExceedsLimit",
			requestedPages: 500,
			elapsedTime:    1 * time.Second,
			maxAllowed:     100,
		},
		{
			name:           "HalfSecond",
			requestedPages: 100,
			elapsedTime:    500 * time.Millisecond,
			maxAllowed:     50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpm.lastFlushTime = time.Now().Add(-tt.elapsedTime)
			allowedPages := bpm.applyRateLimit(tt.requestedPages)

			if allowedPages > tt.maxAllowed {
				t.Errorf("applyRateLimit(%d) = %d, should not exceed %d",
					tt.requestedPages, allowedPages, tt.maxAllowed)
			} else {
				t.Logf("✓ Requested %d pages, allowed %d pages (limit: %d pages/sec, elapsed: %v)",
					tt.requestedPages, allowedPages, bpm.flushRateLimit, tt.elapsedTime)
			}
		})
	}
}

// TestAdjustFlushInterval 测试自适应刷新间隔调整
func TestAdjustFlushInterval(t *testing.T) {
	tests := []struct {
		name            string
		dirtyRatio      float64
		initialInterval time.Duration
		expectDecrease  bool
		expectIncrease  bool
	}{
		{
			name:            "AggressiveFlush_DecreaseInterval",
			dirtyRatio:      0.80, // 80%
			initialInterval: 1 * time.Second,
			expectDecrease:  true,
			expectIncrease:  false,
		},
		{
			name:            "ModerateFlush_SlightDecrease",
			dirtyRatio:      0.60, // 60%
			initialInterval: 1 * time.Second,
			expectDecrease:  true,
			expectIncrease:  false,
		},
		{
			name:            "LowFlush_IncreaseInterval",
			dirtyRatio:      0.10, // 10%
			initialInterval: 1 * time.Second,
			expectDecrease:  false,
			expectIncrease:  true,
		},
		{
			name:            "NormalFlush_NoChange",
			dirtyRatio:      0.30, // 30%
			initialInterval: 1 * time.Second,
			expectDecrease:  false,
			expectIncrease:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpm := &BufferPoolManager{
				currentFlushInterval: tt.initialInterval,
			}

			bpm.adjustFlushInterval(tt.dirtyRatio)

			if tt.expectDecrease && bpm.currentFlushInterval >= tt.initialInterval {
				t.Errorf("Expected interval to decrease from %v, got %v",
					tt.initialInterval, bpm.currentFlushInterval)
			} else if tt.expectIncrease && bpm.currentFlushInterval <= tt.initialInterval {
				t.Errorf("Expected interval to increase from %v, got %v",
					tt.initialInterval, bpm.currentFlushInterval)
			} else if !tt.expectDecrease && !tt.expectIncrease && bpm.currentFlushInterval != tt.initialInterval {
				t.Errorf("Expected interval to remain %v, got %v",
					tt.initialInterval, bpm.currentFlushInterval)
			} else {
				t.Logf("✓ Dirty ratio %.2f%% -> interval %v -> %v",
					tt.dirtyRatio*100, tt.initialInterval, bpm.currentFlushInterval)
			}

			// 验证间隔在合理范围内
			if bpm.currentFlushInterval < MIN_FLUSH_INTERVAL {
				t.Errorf("Interval %v is below minimum %v", bpm.currentFlushInterval, MIN_FLUSH_INTERVAL)
			}
			if bpm.currentFlushInterval > MAX_FLUSH_INTERVAL {
				t.Errorf("Interval %v is above maximum %v", bpm.currentFlushInterval, MAX_FLUSH_INTERVAL)
			}
		})
	}
}

// TestFlushIntervalBounds 测试刷新间隔边界
func TestFlushIntervalBounds(t *testing.T) {
	bpm := &BufferPoolManager{
		currentFlushInterval: 1 * time.Second,
	}

	// 测试下限
	t.Run("MinimumBound", func(t *testing.T) {
		bpm.currentFlushInterval = MIN_FLUSH_INTERVAL / 2 // 设置为低于最小值
		bpm.adjustFlushInterval(0.90)                     // 极高脏页比例，会尝试进一步减少

		if bpm.currentFlushInterval < MIN_FLUSH_INTERVAL {
			t.Errorf("Interval %v is below minimum %v", bpm.currentFlushInterval, MIN_FLUSH_INTERVAL)
		} else {
			t.Logf("✓ Interval correctly bounded to minimum: %v", bpm.currentFlushInterval)
		}
	})

	// 测试上限
	t.Run("MaximumBound", func(t *testing.T) {
		bpm.currentFlushInterval = MAX_FLUSH_INTERVAL * 2 // 设置为高于最大值
		bpm.adjustFlushInterval(0.05)                     // 极低脏页比例，会尝试进一步增加

		if bpm.currentFlushInterval > MAX_FLUSH_INTERVAL {
			t.Errorf("Interval %v is above maximum %v", bpm.currentFlushInterval, MAX_FLUSH_INTERVAL)
		} else {
			t.Logf("✓ Interval correctly bounded to maximum: %v", bpm.currentFlushInterval)
		}
	})
}

// TestFlushStrategyIntegration 测试刷新策略集成
func TestFlushStrategyIntegration(t *testing.T) {
	t.Run("StrategyProgression", func(t *testing.T) {
		bpm := &BufferPoolManager{
			config: &BufferPoolConfig{
				PoolSize: 1000,
			},
			currentFlushInterval: 1 * time.Second,
			flushRateLimit:       MAX_FLUSH_PAGES_PER_SEC,
			lastFlushTime:        time.Now(),
		}

		// 模拟脏页比例逐渐增加的场景
		dirtyRatios := []float64{0.10, 0.30, 0.55, 0.80}
		expectedBatches := []int{0, BATCH_FLUSH_SIZE, BATCH_FLUSH_SIZE * 2, BATCH_FLUSH_SIZE * 4}

		for i, ratio := range dirtyRatios {
			batchSize := bpm.calculateFlushBatchSize(ratio)
			if batchSize != expectedBatches[i] {
				t.Errorf("At dirty ratio %.2f%%, expected batch %d, got %d",
					ratio*100, expectedBatches[i], batchSize)
			} else {
				t.Logf("✓ Dirty ratio %.2f%% -> batch size %d (expected %d)",
					ratio*100, batchSize, expectedBatches[i])
			}

			// 调整刷新间隔
			oldInterval := bpm.currentFlushInterval
			bpm.adjustFlushInterval(ratio)
			t.Logf("  Interval adjusted: %v -> %v", oldInterval, bpm.currentFlushInterval)
		}
	})
}

// TestConstants 测试常量定义
func TestConstants(t *testing.T) {
	t.Run("FlushIntervalConstants", func(t *testing.T) {
		if MIN_FLUSH_INTERVAL >= MAX_FLUSH_INTERVAL {
			t.Errorf("MIN_FLUSH_INTERVAL (%v) should be less than MAX_FLUSH_INTERVAL (%v)",
				MIN_FLUSH_INTERVAL, MAX_FLUSH_INTERVAL)
		}
		t.Logf("✓ Flush interval range: %v - %v", MIN_FLUSH_INTERVAL, MAX_FLUSH_INTERVAL)
	})

	t.Run("FlushRatioConstants", func(t *testing.T) {
		if LIGHT_FLUSH_RATIO >= MODERATE_FLUSH_RATIO || MODERATE_FLUSH_RATIO >= AGGRESSIVE_FLUSH_RATIO {
			t.Errorf("Flush ratios should be in ascending order: LIGHT (%.2f) < MODERATE (%.2f) < AGGRESSIVE (%.2f)",
				LIGHT_FLUSH_RATIO, MODERATE_FLUSH_RATIO, AGGRESSIVE_FLUSH_RATIO)
		}
		t.Logf("✓ Flush ratio thresholds: Light=%.2f%%, Moderate=%.2f%%, Aggressive=%.2f%%",
			LIGHT_FLUSH_RATIO*100, MODERATE_FLUSH_RATIO*100, AGGRESSIVE_FLUSH_RATIO*100)
	})

	t.Run("BatchSizeConstants", func(t *testing.T) {
		if BATCH_FLUSH_SIZE <= 0 {
			t.Errorf("BATCH_FLUSH_SIZE (%d) should be positive", BATCH_FLUSH_SIZE)
		}
		t.Logf("✓ Batch flush size: %d pages", BATCH_FLUSH_SIZE)
	})
}
