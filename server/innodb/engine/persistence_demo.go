package engine

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

// PersistenceDemo 持久化机制演示
type PersistenceDemo struct {
	persistenceManager *PersistenceManager
	bufferPoolManager  *manager.OptimizedBufferPoolManager
	storageManager     *manager.StorageManager
	dataDir            string
}

// NewPersistenceDemo 创建持久化演示
func NewPersistenceDemo(dataDir string) (*PersistenceDemo, error) {
	// 创建存储提供者
	storageProvider := &DemoStorageProvider{}

	// 创建缓冲池配置
	config := &manager.BufferPoolConfig{
		PoolSize:        1000,
		PageSize:        16384,
		FlushInterval:   time.Second * 2,
		YoungListRatio:  0.375,
		OldListRatio:    0.625,
		OldBlockTime:    1000,
		PrefetchWorkers: 2,
		MaxQueueSize:    1000,
		StorageProvider: storageProvider,
	}

	// 创建缓冲池管理器
	bufferPoolManager, err := manager.NewOptimizedBufferPoolManager(config)
	if err != nil {
		return nil, fmt.Errorf("创建缓冲池管理器失败: %v", err)
	}

	// 创建存储管理器
	storageManager := &manager.StorageManager{}

	// 创建持久化管理器
	persistenceManager := NewPersistenceManager(bufferPoolManager, storageManager, dataDir)

	return &PersistenceDemo{
		persistenceManager: persistenceManager,
		bufferPoolManager:  bufferPoolManager,
		storageManager:     storageManager,
		dataDir:            dataDir,
	}, nil
}

// RunDemo 运行持久化演示
func (demo *PersistenceDemo) RunDemo() error {
	ctx := context.Background()

	fmt.Println("🚀 启动XMySQL页面持久化机制演示")
	fmt.Println(strings.Repeat("=", 60))

	// 1. 启动持久化管理器
	fmt.Println(" 步骤1: 启动持久化管理器")
	if err := demo.persistenceManager.Start(ctx); err != nil {
		return fmt.Errorf("启动持久化管理器失败: %v", err)
	}
	fmt.Println(" 持久化管理器启动成功")
	fmt.Println()

	// 2. 模拟页面操作
	fmt.Println(" 步骤2: 模拟页面操作")
	demo.simulatePageOperations(ctx)
	fmt.Println()

	// 3. 创建检查点
	fmt.Println(" 步骤3: 创建检查点")
	if err := demo.persistenceManager.CreateCheckpoint(ctx); err != nil {
		return fmt.Errorf("创建检查点失败: %v", err)
	}
	fmt.Println(" 检查点创建成功")
	fmt.Println()

	// 4. 显示统计信息
	fmt.Println(" 步骤4: 显示统计信息")
	demo.showStatistics()
	fmt.Println()

	// 5. 模拟恢复过程
	fmt.Println(" 步骤5: 模拟恢复过程")
	if err := demo.simulateRecovery(ctx); err != nil {
		return fmt.Errorf("模拟恢复失败: %v", err)
	}
	fmt.Println()

	// 6. 停止持久化管理器
	fmt.Println(" 步骤6: 停止持久化管理器")
	if err := demo.persistenceManager.Stop(); err != nil {
		return fmt.Errorf("停止持久化管理器失败: %v", err)
	}
	fmt.Println(" 持久化管理器停止成功")

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("🎉 XMySQL页面持久化机制演示完成！")

	return nil
}

// simulatePageOperations 模拟页面操作
func (demo *PersistenceDemo) simulatePageOperations(ctx context.Context) {
	fmt.Println("   💾 模拟页面刷新操作...")

	// 模拟刷新多个页面
	for i := uint32(1); i <= 5; i++ {
		spaceID := uint32(1)
		pageNo := i

		logger.Debugf("   📄 刷新页面: SpaceID=%d, PageNo=%d\n", spaceID, pageNo)

		// 尝试刷新页面（可能会失败，因为没有真实数据）
		if err := demo.persistenceManager.FlushPage(ctx, spaceID, pageNo); err != nil {
			logger.Debugf("     页面刷新失败（预期的）: %v\n", err)
		} else {
			logger.Debugf("    页面刷新成功\n")
		}

		// 短暂延迟
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("   💾 尝试刷新所有脏页...")
	if err := demo.persistenceManager.FlushAllDirtyPages(ctx); err != nil {
		logger.Debugf("     刷新所有脏页失败（预期的）: %v\n", err)
	} else {
		fmt.Println("    所有脏页刷新成功")
	}
}

// showStatistics 显示统计信息
func (demo *PersistenceDemo) showStatistics() {
	// 持久化统计
	persistenceStats := demo.persistenceManager.GetStats()
	fmt.Println("    持久化统计信息:")
	logger.Debugf("      总刷新次数: %d\n", persistenceStats.TotalFlushes)
	logger.Debugf("      总WAL写入: %d\n", persistenceStats.TotalWALWrites)
	logger.Debugf("      总检查点: %d\n", persistenceStats.TotalCheckpoints)
	logger.Debugf("      脏页数量: %d\n", persistenceStats.DirtyPagesCount)
	logger.Debugf("      已刷新页面: %d\n", persistenceStats.FlushedPagesCount)
	logger.Debugf("      最后刷新时间: %v\n", persistenceStats.LastFlushTime)
	logger.Debugf("      最后检查点时间: %v\n", persistenceStats.LastCheckpointTime)

	// 缓冲池统计
	bufferPoolStats := demo.bufferPoolManager.GetStats()
	fmt.Println("    缓冲池统计信息:")
	logger.Debugf("      缓存命中: %v\n", bufferPoolStats["hits"])
	logger.Debugf("      缓存未命中: %v\n", bufferPoolStats["misses"])
	logger.Debugf("      命中率: %.2f%%\n", bufferPoolStats["hit_rate"].(float64)*100)
	logger.Debugf("      页面读取: %v\n", bufferPoolStats["page_reads"])
	logger.Debugf("      页面写入: %v\n", bufferPoolStats["page_writes"])
	logger.Debugf("      脏页数量: %v\n", bufferPoolStats["dirty_pages"])
	logger.Debugf("      缓存大小: %v\n", bufferPoolStats["cache_size"])
}

// simulateRecovery 模拟恢复过程
func (demo *PersistenceDemo) simulateRecovery(ctx context.Context) error {
	fmt.Println("   🔄 模拟从检查点恢复...")

	if err := demo.persistenceManager.RecoverFromCheckpoint(ctx); err != nil {
		logger.Debugf("     从检查点恢复失败（可能是正常的）: %v\n", err)
	} else {
		fmt.Println("    从检查点恢复成功")
	}

	return nil
}

// DemoStorageProvider 演示用的存储提供者
type DemoStorageProvider struct {
	pages map[uint64][]byte // 模拟页面存储
}

func (d *DemoStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	if d.pages == nil {
		d.pages = make(map[uint64][]byte)
	}

	key := (uint64(spaceID) << 32) | uint64(pageNo)
	if data, exists := d.pages[key]; exists {
		return data, nil
	}

	// 返回模拟的页面数据
	data := make([]byte, 16384)
	// 填充一些模拟数据
	copy(data[:20], fmt.Sprintf("Page_%d_%d", spaceID, pageNo))
	d.pages[key] = data
	return data, nil
}

func (d *DemoStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	if d.pages == nil {
		d.pages = make(map[uint64][]byte)
	}

	key := (uint64(spaceID) << 32) | uint64(pageNo)
	d.pages[key] = make([]byte, len(data))
	copy(d.pages[key], data)
	return nil
}

func (d *DemoStorageProvider) AllocatePage(spaceID uint32) (uint32, error) {
	return 1, nil
}

func (d *DemoStorageProvider) FreePage(spaceID uint32, pageNo uint32) error {
	return nil
}

func (d *DemoStorageProvider) CreateSpace(name string, pageSize uint32) (uint32, error) {
	return 1, nil
}

func (d *DemoStorageProvider) OpenSpace(spaceID uint32) error {
	return nil
}

func (d *DemoStorageProvider) CloseSpace(spaceID uint32) error {
	return nil
}

func (d *DemoStorageProvider) DeleteSpace(spaceID uint32) error {
	return nil
}

func (d *DemoStorageProvider) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	return &basic.SpaceInfo{
		SpaceID:      spaceID,
		Name:         "demo_space",
		Path:         "/tmp/demo.ibd",
		PageSize:     16384,
		TotalPages:   100,
		FreePages:    50,
		ExtentSize:   64,
		IsCompressed: false,
		State:        "active",
	}, nil
}

func (d *DemoStorageProvider) ListSpaces() ([]basic.SpaceInfo, error) {
	return []basic.SpaceInfo{}, nil
}

func (d *DemoStorageProvider) BeginTransaction() (uint64, error) {
	return 1, nil
}

func (d *DemoStorageProvider) CommitTransaction(txID uint64) error {
	return nil
}

func (d *DemoStorageProvider) RollbackTransaction(txID uint64) error {
	return nil
}

func (d *DemoStorageProvider) Sync(spaceID uint32) error {
	return nil
}

func (d *DemoStorageProvider) Close() error {
	return nil
}

// RunPersistenceDemo 运行持久化演示的主函数
func RunPersistenceDemo() error {
	// 创建演示
	demo, err := NewPersistenceDemo("./demo_data")
	if err != nil {
		logger.Errorf("创建持久化演示失败: %v", err)
		return err
	}

	// 运行演示
	if err := demo.RunDemo(); err != nil {
		logger.Errorf("运行持久化演示失败: %v", err)
		return err
	}
	return nil
}
