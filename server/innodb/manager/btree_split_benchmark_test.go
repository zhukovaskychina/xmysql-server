package manager

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkNodeSplitter_Original 原始分裂器性能基准测试
func BenchmarkNodeSplitter_Original(b *testing.B) {
	bpm := setupBenchmarkBufferPool(b)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	if err != nil {
		b.Fatalf("Failed to init B+Tree: %v", err)
	}

	splitter := NewNodeSplitter(btm, 3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 创建满载叶子节点
		fullNode := &BPlusTreeNode{
			PageNum:  uint32(1000 + i),
			IsLeaf:   true,
			Keys:     []interface{}{10, 20, 30, 40, 50, 60},
			Records:  []uint32{1, 2, 3, 4, 5, 6},
			NextLeaf: 0,
		}

		btm.nodeCache[fullNode.PageNum] = fullNode

		// 执行分裂
		_, _, err := splitter.SplitLeafNode(ctx, fullNode)
		if err != nil {
			b.Logf("Split error (expected): %v", err)
		}
	}
}

// BenchmarkNodeSplitter_Optimized 优化分裂器性能基准测试
func BenchmarkNodeSplitter_Optimized(b *testing.B) {
	bpm := setupBenchmarkBufferPool(b)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	if err != nil {
		b.Fatalf("Failed to init B+Tree: %v", err)
	}

	splitter := NewOptimizedNodeSplitter(btm, 3)
	splitter.SetDeferredFlush(true) // 启用延迟刷盘

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 创建满载叶子节点
		fullNode := &BPlusTreeNode{
			PageNum:  uint32(1000 + i),
			IsLeaf:   true,
			Keys:     []interface{}{10, 20, 30, 40, 50, 60},
			Records:  []uint32{1, 2, 3, 4, 5, 6},
			NextLeaf: 0,
		}

		btm.nodeCache[fullNode.PageNum] = fullNode

		// 执行分裂
		_, _, err := splitter.SplitLeafNode(ctx, fullNode)
		if err != nil {
			b.Logf("Split error (expected): %v", err)
		}
	}

	// 最后批量刷新
	_ = splitter.FlushDirtyPages(ctx)
}

// BenchmarkBatchInsert_Original 原始分裂器批量插入性能测试
func BenchmarkBatchInsert_Original(b *testing.B) {
	bpm := setupBenchmarkBufferPool(b)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	if err != nil {
		b.Fatalf("Failed to init B+Tree: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%06d", i)
		value := []byte(fmt.Sprintf("value_%06d", i))
		_ = btm.Insert(ctx, key, value)
	}
}

// BenchmarkBatchInsert_Optimized 优化分裂器批量插入性能测试
func BenchmarkBatchInsert_Optimized(b *testing.B) {
	bpm := setupBenchmarkBufferPool(b)

	// 使用优化配置
	config := &BPlusTreeConfig{
		MaxCacheSize:   1000,
		DirtyThreshold: 0.8,
		EvictionPolicy: "LRU",
	}

	btm := NewBPlusTreeManager(bpm, config)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	if err != nil {
		b.Fatalf("Failed to init B+Tree: %v", err)
	}

	// 替换为优化的分裂器
	optimizedSplitter := NewOptimizedNodeSplitter(btm, 3)
	optimizedSplitter.SetDeferredFlush(true)
	optimizedSplitter.SetSplitThreshold(1.1) // 允许110%满再分裂

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%06d", i)
		value := []byte(fmt.Sprintf("value_%06d", i))
		_ = btm.Insert(ctx, key, value)
	}

	// 最后批量刷新
	_ = optimizedSplitter.FlushDirtyPages(ctx)
}

// BenchmarkParentLookup_Original 原始父节点查找性能测试
func BenchmarkParentLookup_Original(b *testing.B) {
	bpm := setupBenchmarkBufferPool(b)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	if err != nil {
		b.Fatalf("Failed to init B+Tree: %v", err)
	}

	// 创建一个3层的B+树结构
	rootNode := &BPlusTreeNode{
		PageNum:  2001,
		IsLeaf:   false,
		Keys:     []interface{}{100, 200},
		Children: []uint32{3001, 3002, 3003},
	}

	child1 := &BPlusTreeNode{
		PageNum:  3001,
		IsLeaf:   false,
		Keys:     []interface{}{50},
		Children: []uint32{4001, 4002},
	}

	child2 := &BPlusTreeNode{
		PageNum:  3002,
		IsLeaf:   false,
		Keys:     []interface{}{150},
		Children: []uint32{4003, 4004},
	}

	child3 := &BPlusTreeNode{
		PageNum:  3003,
		IsLeaf:   false,
		Keys:     []interface{}{250},
		Children: []uint32{4005, 4006},
	}

	btm.nodeCache[rootNode.PageNum] = rootNode
	btm.nodeCache[child1.PageNum] = child1
	btm.nodeCache[child2.PageNum] = child2
	btm.nodeCache[child3.PageNum] = child3
	btm.rootPage = rootNode.PageNum

	splitter := NewNodeSplitter(btm, 3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 查找child1的父节点
		_, err := splitter.findParentNode(ctx, 3001)
		if err != nil {
			b.Fatalf("Failed to find parent: %v", err)
		}
	}
}

// BenchmarkParentLookup_Optimized 优化父节点查找性能测试
func BenchmarkParentLookup_Optimized(b *testing.B) {
	bpm := setupBenchmarkBufferPool(b)
	btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
	ctx := context.Background()

	err := btm.Init(ctx, 1, 1)
	if err != nil {
		b.Fatalf("Failed to init B+Tree: %v", err)
	}

	// 创建一个3层的B+树结构
	rootNode := &BPlusTreeNode{
		PageNum:  2001,
		IsLeaf:   false,
		Keys:     []interface{}{100, 200},
		Children: []uint32{3001, 3002, 3003},
	}

	child1 := &BPlusTreeNode{
		PageNum:  3001,
		IsLeaf:   false,
		Keys:     []interface{}{50},
		Children: []uint32{4001, 4002},
	}

	child2 := &BPlusTreeNode{
		PageNum:  3002,
		IsLeaf:   false,
		Keys:     []interface{}{150},
		Children: []uint32{4003, 4004},
	}

	child3 := &BPlusTreeNode{
		PageNum:  3003,
		IsLeaf:   false,
		Keys:     []interface{}{250},
		Children: []uint32{4005, 4006},
	}

	btm.nodeCache[rootNode.PageNum] = rootNode
	btm.nodeCache[child1.PageNum] = child1
	btm.nodeCache[child2.PageNum] = child2
	btm.nodeCache[child3.PageNum] = child3
	btm.rootPage = rootNode.PageNum

	splitter := NewOptimizedNodeSplitter(btm, 3)

	// 预热缓存
	_, _ = splitter.findParentNodeOptimized(ctx, 3001)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 查找child1的父节点（应该命中缓存）
		_, err := splitter.findParentNodeOptimized(ctx, 3001)
		if err != nil {
			b.Fatalf("Failed to find parent: %v", err)
		}
	}
}

// setupBenchmarkBufferPool 创建基准测试用的缓冲池
func setupBenchmarkBufferPool(b *testing.B) *OptimizedBufferPoolManager {
	config := &BufferPoolConfig{
		PoolSize:        256,
		PageSize:        16384,
		FlushInterval:   time.Second * 10,
		YoungListRatio:  0.75,
		OldListRatio:    0.25,
		OldBlockTime:    1000,
		PrefetchWorkers: 4,
		MaxQueueSize:    200,
		StorageProvider: &MockStorageProviderForBTree{},
	}

	bpm, err := NewOptimizedBufferPoolManager(config)
	if err != nil {
		b.Fatalf("Failed to create buffer pool: %v", err)
	}
	return bpm
}
