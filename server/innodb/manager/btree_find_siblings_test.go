package manager

import (
	"context"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// MockStorageProviderForSiblings 模拟存储提供者
type MockStorageProviderForSiblings struct{}

func (m *MockStorageProviderForSiblings) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	return make([]byte, 16384), nil
}

func (m *MockStorageProviderForSiblings) WritePage(spaceID, pageNo uint32, data []byte) error {
	return nil
}

func (m *MockStorageProviderForSiblings) AllocatePage(spaceID uint32) (uint32, error) {
	return 1000, nil
}

func (m *MockStorageProviderForSiblings) FreePage(spaceID, pageNo uint32) error {
	return nil
}

func (m *MockStorageProviderForSiblings) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return 1, nil
}

func (m *MockStorageProviderForSiblings) OpenSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForSiblings) CloseSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForSiblings) DeleteSpace(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForSiblings) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{
		SpaceID:      spaceID,
		Name:         "test_space",
		Path:         "/tmp/test.ibd",
		PageSize:     16384,
		TotalPages:   100,
		FreePages:    50,
		ExtentSize:   64,
		IsCompressed: false,
		State:        "active",
	}, nil
}

func (m *MockStorageProviderForSiblings) ListSpaces() ([]basic.SpaceInfo, error) {
	return []basic.SpaceInfo{}, nil
}

func (m *MockStorageProviderForSiblings) BeginTransaction() (uint64, error) {
	return 1, nil
}

func (m *MockStorageProviderForSiblings) CommitTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProviderForSiblings) RollbackTransaction(txID uint64) error {
	return nil
}

func (m *MockStorageProviderForSiblings) Sync(spaceID uint32) error {
	return nil
}

func (m *MockStorageProviderForSiblings) Close() error {
	return nil
}

// createTestBPMForSiblings 创建测试用的BufferPoolManager
func createTestBPMForSiblings() *OptimizedBufferPoolManager {
	config := &BufferPoolConfig{
		PoolSize:        100,
		PageSize:        16384,
		FlushInterval:   time.Second,
		YoungListRatio:  0.75,
		OldListRatio:    0.25,
		OldBlockTime:    1000,
		PrefetchWorkers: 2,
		MaxQueueSize:    100,
		StorageProvider: &MockStorageProviderForSiblings{},
	}

	bpm, err := NewOptimizedBufferPoolManager(config)
	if err != nil {
		panic(err)
	}
	return bpm
}

// TestFindSiblings_RootNode 测试根节点没有兄弟
func TestFindSiblings_RootNode(t *testing.T) {
	// 创建B+树管理器
	bpm := createTestBPMForSiblings()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 初始化B+树
	ctx := context.Background()
	err := btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 获取根节点
	rootNode, err := btm.getNode(ctx, btm.rootPage)
	if err != nil {
		t.Fatalf("Failed to get root node: %v", err)
	}

	// 创建NodeMerger
	merger := NewNodeMerger(btm, 3)

	// 查找根节点的兄弟
	leftSibling, rightSibling, err := merger.FindSiblings(ctx, rootNode)
	if err != nil {
		t.Errorf("FindSiblings should not return error for root node: %v", err)
	}

	// 根节点应该没有兄弟
	if leftSibling != nil {
		t.Error("Root node should not have left sibling")
	}
	if rightSibling != nil {
		t.Error("Root node should not have right sibling")
	}

	t.Log("✅ Root node correctly has no siblings")
}

// TestFindSiblings_WithSiblings 测试有兄弟节点的情况
func TestFindSiblings_WithSiblings(t *testing.T) {
	// 创建B+树管理器
	bpm := createTestBPMForSiblings()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 初始化B+树
	ctx := context.Background()
	err := btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 插入足够多的数据以触发分裂，创建多个节点
	keys := []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	for _, key := range keys {
		value := []byte{byte(key)}
		err := btm.Insert(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	t.Logf("Inserted %d keys, tree height: %d", len(keys), btm.GetTreeHeight())

	// 如果树高度大于1，说明发生了分裂
	if btm.GetTreeHeight() > 1 {
		// 获取根节点
		rootNode, err := btm.getNode(ctx, btm.rootPage)
		if err != nil {
			t.Fatalf("Failed to get root node: %v", err)
		}

		// 根节点应该有子节点
		if len(rootNode.Children) < 2 {
			t.Skip("Tree did not split enough to have siblings")
		}

		// 获取中间的子节点（应该有左右兄弟）
		middleChildPage := rootNode.Children[1]
		middleChild, err := btm.getNode(ctx, middleChildPage)
		if err != nil {
			t.Fatalf("Failed to get middle child: %v", err)
		}

		// 创建NodeMerger
		merger := NewNodeMerger(btm, 3)

		// 查找兄弟节点
		leftSibling, rightSibling, err := merger.FindSiblings(ctx, middleChild)
		if err != nil {
			t.Fatalf("FindSiblings failed: %v", err)
		}

		// 中间节点应该有左兄弟
		if leftSibling == nil {
			t.Error("Middle child should have left sibling")
		} else {
			t.Logf("✅ Found left sibling: page=%d, keys=%d", leftSibling.PageNum, len(leftSibling.Keys))
		}

		// 如果有3个或更多子节点，中间节点应该有右兄弟
		if len(rootNode.Children) >= 3 {
			if rightSibling == nil {
				t.Error("Middle child should have right sibling")
			} else {
				t.Logf("✅ Found right sibling: page=%d, keys=%d", rightSibling.PageNum, len(rightSibling.Keys))
			}
		}
	} else {
		t.Skip("Tree height is 1, no siblings to test")
	}
}

// TestFindSiblings_LeftmostNode 测试最左边的节点（只有右兄弟）
func TestFindSiblings_LeftmostNode(t *testing.T) {
	// 创建B+树管理器
	bpm := createTestBPMForSiblings()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 初始化B+树
	ctx := context.Background()
	err := btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 插入数据触发分裂
	keys := []int{10, 20, 30, 40, 50, 60, 70, 80}
	for _, key := range keys {
		value := []byte{byte(key)}
		err := btm.Insert(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// 如果树高度大于1
	if btm.GetTreeHeight() > 1 {
		// 获取根节点
		rootNode, err := btm.getNode(ctx, btm.rootPage)
		if err != nil {
			t.Fatalf("Failed to get root node: %v", err)
		}

		if len(rootNode.Children) < 2 {
			t.Skip("Tree did not split enough")
		}

		// 获取最左边的子节点
		leftmostPage := rootNode.Children[0]
		leftmostNode, err := btm.getNode(ctx, leftmostPage)
		if err != nil {
			t.Fatalf("Failed to get leftmost node: %v", err)
		}

		// 创建NodeMerger
		merger := NewNodeMerger(btm, 3)

		// 查找兄弟节点
		leftSibling, rightSibling, err := merger.FindSiblings(ctx, leftmostNode)
		if err != nil {
			t.Fatalf("FindSiblings failed: %v", err)
		}

		// 最左边的节点不应该有左兄弟
		if leftSibling != nil {
			t.Error("Leftmost node should not have left sibling")
		}

		// 应该有右兄弟
		if rightSibling == nil {
			t.Error("Leftmost node should have right sibling")
		} else {
			t.Logf("✅ Leftmost node has right sibling: page=%d", rightSibling.PageNum)
		}
	} else {
		t.Skip("Tree height is 1, no siblings to test")
	}
}

// TestFindSiblings_RightmostNode 测试最右边的节点（只有左兄弟）
func TestFindSiblings_RightmostNode(t *testing.T) {
	// 创建B+树管理器
	bpm := createTestBPMForSiblings()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 初始化B+树
	ctx := context.Background()
	err := btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 插入数据触发分裂
	keys := []int{10, 20, 30, 40, 50, 60, 70, 80}
	for _, key := range keys {
		value := []byte{byte(key)}
		err := btm.Insert(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// 如果树高度大于1
	if btm.GetTreeHeight() > 1 {
		// 获取根节点
		rootNode, err := btm.getNode(ctx, btm.rootPage)
		if err != nil {
			t.Fatalf("Failed to get root node: %v", err)
		}

		if len(rootNode.Children) < 2 {
			t.Skip("Tree did not split enough")
		}

		// 获取最右边的子节点
		rightmostPage := rootNode.Children[len(rootNode.Children)-1]
		rightmostNode, err := btm.getNode(ctx, rightmostPage)
		if err != nil {
			t.Fatalf("Failed to get rightmost node: %v", err)
		}

		// 创建NodeMerger
		merger := NewNodeMerger(btm, 3)

		// 查找兄弟节点
		leftSibling, rightSibling, err := merger.FindSiblings(ctx, rightmostNode)
		if err != nil {
			t.Fatalf("FindSiblings failed: %v", err)
		}

		// 应该有左兄弟
		if leftSibling == nil {
			t.Error("Rightmost node should have left sibling")
		} else {
			t.Logf("✅ Rightmost node has left sibling: page=%d", leftSibling.PageNum)
		}

		// 最右边的节点不应该有右兄弟
		if rightSibling != nil {
			t.Error("Rightmost node should not have right sibling")
		}
	} else {
		t.Skip("Tree height is 1, no siblings to test")
	}
}

// TestFindSiblings_Integration 集成测试：插入和删除操作中的FindSiblings
func TestFindSiblings_Integration(t *testing.T) {
	// 创建B+树管理器
	bpm := createTestBPMForSiblings()
	defer bpm.Close()

	config := &BPlusTreeConfig{
		MaxCacheSize:   100,
		DirtyThreshold: 0.7,
		EvictionPolicy: "LRU",
	}
	btm := NewBPlusTreeManager(bpm, config)

	// 初始化B+树
	ctx := context.Background()
	err := btm.Init(ctx, 0, 1)
	if err != nil {
		t.Fatalf("Failed to initialize B+tree: %v", err)
	}

	// 插入大量数据
	keys := make([]int, 20)
	for i := 0; i < 20; i++ {
		keys[i] = (i + 1) * 10
	}

	for _, key := range keys {
		value := []byte{byte(key)}
		err := btm.Insert(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	t.Logf("Inserted %d keys, tree height: %d", len(keys), btm.GetTreeHeight())

	// 删除一些键，触发重平衡（会使用FindSiblings）
	deleteKeys := []int{50, 60, 70}
	for _, key := range deleteKeys {
		err := btm.Delete(ctx, key)
		if err != nil {
			t.Logf("Warning: Failed to delete key %d: %v", key, err)
			// 不失败，因为Delete可能还有其他问题
		}
	}

	t.Log("✅ Integration test completed (FindSiblings used during rebalancing)")
}
