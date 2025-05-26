package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

func main() {
	fmt.Println("🚀 开始B+树管理器死锁修复测试...")

	// 创建模拟存储提供者
	storageProvider := &MockStorageProvider{}

	// 创建配置
	config := &manager.BufferPoolConfig{
		PoolSize:        100,
		PageSize:        16384,
		FlushInterval:   time.Second,
		YoungListRatio:  0.75,
		OldListRatio:    0.25,
		OldBlockTime:    1000,
		PrefetchWorkers: 2,
		MaxQueueSize:    100,
		StorageProvider: storageProvider,
	}

	// 创建缓冲池管理器
	bpm, err := manager.NewOptimizedBufferPoolManager(config)
	if err != nil {
		log.Fatalf("创建缓冲池管理器失败: %v", err)
	}
	defer bpm.Close()

	// 创建B+树管理器
	btm := manager.NewBPlusTreeManager(bpm, nil)

	// 初始化B+树
	ctx := context.Background()
	spaceId := uint32(1)
	rootPage := uint32(1)

	fmt.Println("📝 初始化B+树...")
	if err := btm.Init(ctx, spaceId, rootPage); err != nil {
		log.Printf("❌ Init failed: %v", err)
	} else {
		fmt.Println("✅ Init成功")
	}

	// 创建高并发测试场景
	fmt.Println("🔥 开始高并发死锁测试...")

	var wg sync.WaitGroup
	goroutineCount := 50
	operationsPerGoroutine := 100

	// 启动多个goroutine执行不同操作
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				switch j % 4 {
				case 0:
					// 搜索操作
					_, _, err := btm.Search(ctx, fmt.Sprintf("key_%d_%d", id, j))
					if err != nil {
						log.Printf("Goroutine %d search failed: %v", id, err)
					}

				case 1:
					// 获取第一个叶子页面
					_, err := btm.GetFirstLeafPage(ctx)
					if err != nil {
						log.Printf("Goroutine %d get first leaf failed: %v", id, err)
					}

				case 2:
					// 获取所有叶子页面
					_, err := btm.GetAllLeafPages(ctx)
					if err != nil {
						log.Printf("Goroutine %d get all leaves failed: %v", id, err)
					}

				case 3:
					// 范围搜索
					_, err := btm.RangeSearch(ctx, fmt.Sprintf("key_%d_0", id), fmt.Sprintf("key_%d_10", id))
					if err != nil {
						log.Printf("Goroutine %d range search failed: %v", id, err)
					}
				}

				// 添加一点随机延迟来模拟真实场景
				if j%20 == 0 {
					time.Sleep(time.Microsecond * 10)
				}
			}

			fmt.Printf("✅ Goroutine %d 完成 %d 次操作\n", id, operationsPerGoroutine)
		}(i)
	}

	// 启动额外的后台压力测试
	fmt.Println("🔧 启动后台压力测试...")
	stopPressure := make(chan bool)

	// 持续的Init调用
	go func() {
		for {
			select {
			case <-stopPressure:
				return
			default:
				if err := btm.Init(ctx, spaceId+1, rootPage+1); err != nil {
					log.Printf("Background init failed: %v", err)
				}
				time.Sleep(time.Millisecond * 50)
			}
		}
	}()

	// 监控goroutine数量和死锁检测
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stopPressure:
				return
			case <-ticker.C:
				numGoroutines := runtime.NumGoroutine()
				fmt.Printf("📊 当前goroutine数量: %d\n", numGoroutines)

				// 如果goroutine数量异常增长，可能存在死锁
				if numGoroutines > 200 {
					fmt.Printf("⚠️  警告: goroutine数量异常高: %d\n", numGoroutines)
				}
			}
		}
	}()

	// 等待所有测试goroutine完成
	fmt.Println("⏳ 等待所有操作完成...")
	wg.Wait()

	// 停止后台压力测试
	close(stopPressure)
	time.Sleep(time.Second * 2) // 等待后台goroutine退出

	fmt.Println("🎉 所有测试完成！")

	finalGoroutines := runtime.NumGoroutine()
	fmt.Printf("📈 最终goroutine数量: %d\n", finalGoroutines)

	if finalGoroutines < 20 { // 正常情况下应该很少
		fmt.Println("✅ 死锁修复测试通过 - 没有检测到死锁!")
	} else {
		fmt.Printf("⚠️  可能存在goroutine泄漏或死锁: %d\n", finalGoroutines)
	}

	fmt.Println("🔍 测试总结:")
	fmt.Printf("  - 总操作数: %d\n", goroutineCount*operationsPerGoroutine)
	fmt.Printf("  - 并发goroutine数: %d\n", goroutineCount)
	fmt.Printf("  - 最终goroutine数: %d\n", finalGoroutines)
	fmt.Println("✅ 测试完成")
}

// MockStorageProvider 模拟存储提供者
type MockStorageProvider struct{}

func (msp *MockStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	// 返回模拟页面数据
	data := make([]byte, 16384)
	// 填充一些测试数据
	for i := 0; i < len(data); i += 4 {
		data[i] = byte(spaceID)
		data[i+1] = byte(spaceID >> 8)
		data[i+2] = byte(pageNo)
		data[i+3] = byte(pageNo >> 8)
	}
	return data, nil
}

func (msp *MockStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	// 模拟写入操作，实际什么都不做
	return nil
}

func (msp *MockStorageProvider) AllocatePage(spaceID uint32) (uint32, error) {
	// 简单返回一个页面号
	return 1, nil
}

func (msp *MockStorageProvider) FreePage(spaceID, pageNo uint32) error {
	// 模拟释放页面
	return nil
}

func (msp *MockStorageProvider) CreateSpace(name string, pageSize uint32) (uint32, error) {
	// 简单返回一个空间ID
	return 1, nil
}

func (msp *MockStorageProvider) OpenSpace(spaceID uint32) error {
	// 模拟打开空间
	return nil
}

func (msp *MockStorageProvider) CloseSpace(spaceID uint32) error {
	// 模拟关闭空间
	return nil
}

func (msp *MockStorageProvider) DeleteSpace(spaceID uint32) error {
	// 模拟删除空间
	return nil
}

func (msp *MockStorageProvider) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	// 返回模拟空间信息
	return &basic.SpaceInfo{
		SpaceID:      spaceID,
		Name:         "test_space",
		Path:         "/tmp/test_space.ibd",
		PageSize:     16384,
		TotalPages:   1000,
		FreePages:    500,
		ExtentSize:   64,
		IsCompressed: false,
		State:        "active",
	}, nil
}

func (msp *MockStorageProvider) ListSpaces() ([]basic.SpaceInfo, error) {
	// 返回空间列表
	return []basic.SpaceInfo{}, nil
}

func (msp *MockStorageProvider) BeginTransaction() (uint64, error) {
	// 返回一个事务ID
	return 1, nil
}

func (msp *MockStorageProvider) CommitTransaction(txID uint64) error {
	// 模拟提交事务
	return nil
}

func (msp *MockStorageProvider) RollbackTransaction(txID uint64) error {
	// 模拟回滚事务
	return nil
}

func (msp *MockStorageProvider) Sync(spaceID uint32) error {
	// 模拟同步操作
	return nil
}

func (msp *MockStorageProvider) Close() error {
	// 模拟关闭操作
	return nil
}
