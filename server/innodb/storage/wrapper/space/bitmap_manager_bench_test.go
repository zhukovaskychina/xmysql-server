package space

import (
	"math/rand"
	"testing"
)

// ========================================
// 单线程基准测试
// ========================================

// BenchmarkBitmapManager_Set_Single 全局锁版本 - 单线程Set
func BenchmarkBitmapManager_Set_Single(b *testing.B) {
	bm := NewBitmapManager(100000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pos := uint32(i % 100000)
		_ = bm.Set(pos)
	}
}

// BenchmarkSegmentedBitmapManager_Set_Single 分段锁版本 - 单线程Set
func BenchmarkSegmentedBitmapManager_Set_Single(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pos := uint32(i % 100000)
		_ = sbm.Set(pos)
	}
}

// BenchmarkBitmapManager_IsSet_Single 全局锁版本 - 单线程IsSet
func BenchmarkBitmapManager_IsSet_Single(b *testing.B) {
	bm := NewBitmapManager(100000)
	// 预先设置一些位
	for i := 0; i < 50000; i++ {
		_ = bm.Set(uint32(i * 2))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pos := uint32(i % 100000)
		_, _ = bm.IsSet(pos)
	}
}

// BenchmarkSegmentedBitmapManager_IsSet_Single 分段锁版本 - 单线程IsSet
func BenchmarkSegmentedBitmapManager_IsSet_Single(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	// 预先设置一些位
	for i := 0; i < 50000; i++ {
		_ = sbm.Set(uint32(i * 2))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pos := uint32(i % 100000)
		_, _ = sbm.IsSet(pos)
	}
}

// ========================================
// 多线程基准测试 - Set操作
// ========================================

// BenchmarkBitmapManager_Set_Parallel4 全局锁版本 - 4线程Set
func BenchmarkBitmapManager_Set_Parallel4(b *testing.B) {
	bm := NewBitmapManager(100000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_ = bm.Set(pos)
			i++
		}
	})
}

// BenchmarkSegmentedBitmapManager_Set_Parallel4 分段锁版本 - 4线程Set
func BenchmarkSegmentedBitmapManager_Set_Parallel4(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_ = sbm.Set(pos)
			i++
		}
	})
}

// BenchmarkBitmapManager_Set_Parallel8 全局锁版本 - 8线程Set
func BenchmarkBitmapManager_Set_Parallel8(b *testing.B) {
	bm := NewBitmapManager(100000)
	b.SetParallelism(8)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_ = bm.Set(pos)
			i++
		}
	})
}

// BenchmarkSegmentedBitmapManager_Set_Parallel8 分段锁版本 - 8线程Set
func BenchmarkSegmentedBitmapManager_Set_Parallel8(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	b.SetParallelism(8)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_ = sbm.Set(pos)
			i++
		}
	})
}

// BenchmarkBitmapManager_Set_Parallel16 全局锁版本 - 16线程Set
func BenchmarkBitmapManager_Set_Parallel16(b *testing.B) {
	bm := NewBitmapManager(100000)
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_ = bm.Set(pos)
			i++
		}
	})
}

// BenchmarkSegmentedBitmapManager_Set_Parallel16 分段锁版本 - 16线程Set
func BenchmarkSegmentedBitmapManager_Set_Parallel16(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_ = sbm.Set(pos)
			i++
		}
	})
}

// ========================================
// 多线程基准测试 - IsSet操作
// ========================================

// BenchmarkBitmapManager_IsSet_Parallel4 全局锁版本 - 4线程IsSet
func BenchmarkBitmapManager_IsSet_Parallel4(b *testing.B) {
	bm := NewBitmapManager(100000)
	for i := 0; i < 50000; i++ {
		_ = bm.Set(uint32(i * 2))
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_, _ = bm.IsSet(pos)
			i++
		}
	})
}

// BenchmarkSegmentedBitmapManager_IsSet_Parallel4 分段锁版本 - 4线程IsSet
func BenchmarkSegmentedBitmapManager_IsSet_Parallel4(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	for i := 0; i < 50000; i++ {
		_ = sbm.Set(uint32(i * 2))
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_, _ = sbm.IsSet(pos)
			i++
		}
	})
}

// BenchmarkBitmapManager_IsSet_Parallel16 全局锁版本 - 16线程IsSet
func BenchmarkBitmapManager_IsSet_Parallel16(b *testing.B) {
	bm := NewBitmapManager(100000)
	for i := 0; i < 50000; i++ {
		_ = bm.Set(uint32(i * 2))
	}
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_, _ = bm.IsSet(pos)
			i++
		}
	})
}

// BenchmarkSegmentedBitmapManager_IsSet_Parallel16 分段锁版本 - 16线程IsSet
func BenchmarkSegmentedBitmapManager_IsSet_Parallel16(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	for i := 0; i < 50000; i++ {
		_ = sbm.Set(uint32(i * 2))
	}
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			_, _ = sbm.IsSet(pos)
			i++
		}
	})
}

// ========================================
// 混合负载基准测试
// ========================================

// BenchmarkBitmapManager_Mixed_Parallel16 全局锁版本 - 16线程混合负载
func BenchmarkBitmapManager_Mixed_Parallel16(b *testing.B) {
	bm := NewBitmapManager(100000)
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			if i%10 < 7 {
				// 70% 读操作
				_, _ = bm.IsSet(pos)
			} else {
				// 30% 写操作
				_ = bm.Set(pos)
			}
			i++
		}
	})
}

// BenchmarkSegmentedBitmapManager_Mixed_Parallel16 分段锁版本 - 16线程混合负载
func BenchmarkSegmentedBitmapManager_Mixed_Parallel16(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pos := uint32(i % 100000)
			if i%10 < 7 {
				// 70% 读操作
				_, _ = sbm.IsSet(pos)
			} else {
				// 30% 写操作
				_ = sbm.Set(pos)
			}
			i++
		}
	})
}

// ========================================
// 竞争场景基准测试
// ========================================

// BenchmarkBitmapManager_Contention 全局锁版本 - 高竞争场景
func BenchmarkBitmapManager_Contention(b *testing.B) {
	bm := NewBitmapManager(100000)
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// 所有线程访问相同的小范围（高竞争）
		for pb.Next() {
			pos := uint32(rand.Intn(100)) // 只访问前100个位
			_ = bm.Set(pos)
		}
	})
}

// BenchmarkSegmentedBitmapManager_Contention 分段锁版本 - 高竞争场景
func BenchmarkSegmentedBitmapManager_Contention(b *testing.B) {
	sbm := NewSegmentedBitmapManager(100000)
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// 所有线程访问相同的小范围（高竞争）
		for pb.Next() {
			pos := uint32(rand.Intn(100)) // 只访问前100个位
			_ = sbm.Set(pos)
		}
	})
}
