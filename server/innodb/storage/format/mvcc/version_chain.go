package mvcc

import (
	"fmt"
	"sync"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// VersionChain 版本链管理器
// 管理单个记录的所有版本
type VersionChain struct {
	mu      sync.RWMutex
	head    *RecordVersion // 链表头（最新版本）
	length  int            // 版本链长度
	minTxID uint64         // 最小事务ID（用于GC）
}

// NewVersionChain 创建新的版本链
func NewVersionChain() *VersionChain {
	return &VersionChain{
		head:   nil,
		length: 0,
	}
}

// InsertVersion 插入新版本（在链表头部插入）
func (vc *VersionChain) InsertVersion(version, txID, rollPtr uint64, key basic.Value, value basic.Row, deleted bool) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	newVersion := NewRecordVersion(version, txID, rollPtr, key, value)
	newVersion.Deleted = deleted
	newVersion.Next = vc.head

	vc.head = newVersion
	vc.length++
}

// FindVisibleVersion 查找对指定ReadView可见的版本
func (vc *VersionChain) FindVisibleVersion(readView *ReadView) *RecordVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	current := vc.head
	for current != nil {
		// 检查该版本是否对ReadView可见
		if current.IsVisible(readView) {
			return current
		}
		current = current.Next
	}

	return nil
}

// GetLatestVersion 获取最新版本（无论可见性）
func (vc *VersionChain) GetLatestVersion() *RecordVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return vc.head
}

// GetVersionByTxID 根据事务ID查找版本
func (vc *VersionChain) GetVersionByTxID(txID uint64) *RecordVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	current := vc.head
	for current != nil {
		if current.TxID == txID {
			return current
		}
		current = current.Next
	}

	return nil
}

// GetLength 获取版本链长度
func (vc *VersionChain) GetLength() int {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return vc.length
}

// PurgeOldVersions 清理旧版本
// 清理所有事务ID小于minTxID的版本（已不可能被任何活跃事务看到）
func (vc *VersionChain) PurgeOldVersions(minTxID uint64) int {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	if vc.head == nil {
		return 0
	}

	purged := 0

	// 保留第一个版本（最新版本）
	if vc.head.Next == nil {
		return 0
	}

	// 遍历版本链，清理旧版本
	prev := vc.head
	current := vc.head.Next

	for current != nil {
		// 如果当前版本的事务ID小于最小活跃事务ID，且不是链中唯一版本
		if current.TxID < minTxID && current.Next != nil {
			// 移除当前版本
			prev.Next = current.Next
			current = current.Next
			vc.length--
			purged++
		} else {
			prev = current
			current = current.Next
		}
	}

	return purged
}

// GetAllVersions 获取所有版本（用于调试）
func (vc *VersionChain) GetAllVersions() []*RecordVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	versions := make([]*RecordVersion, 0, vc.length)
	current := vc.head

	for current != nil {
		versions = append(versions, current)
		current = current.Next
	}

	return versions
}

// String 返回版本链的字符串表示（用于调试）
func (vc *VersionChain) String() string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	return fmt.Sprintf("VersionChain{Length:%d, Head:%v}", vc.length, vc.head)
}

// VersionChainManager 版本链管理器（管理所有记录的版本链）
type VersionChainManager struct {
	mu     sync.RWMutex
	chains map[string]*VersionChain // key -> VersionChain
	gcChan chan struct{}            // GC触发通道
}

// NewVersionChainManager 创建版本链管理器
func NewVersionChainManager() *VersionChainManager {
	return &VersionChainManager{
		chains: make(map[string]*VersionChain),
		gcChan: make(chan struct{}, 1),
	}
}

// GetOrCreateChain 获取或创建版本链
func (vcm *VersionChainManager) GetOrCreateChain(key string) *VersionChain {
	vcm.mu.Lock()
	defer vcm.mu.Unlock()

	if chain, exists := vcm.chains[key]; exists {
		return chain
	}

	chain := NewVersionChain()
	vcm.chains[key] = chain
	return chain
}

// GetChain 获取版本链
func (vcm *VersionChainManager) GetChain(key string) *VersionChain {
	vcm.mu.RLock()
	defer vcm.mu.RUnlock()

	return vcm.chains[key]
}

// DeleteChain 删除版本链
func (vcm *VersionChainManager) DeleteChain(key string) {
	vcm.mu.Lock()
	defer vcm.mu.Unlock()

	delete(vcm.chains, key)
}

// PurgeAllChains 清理所有版本链中的旧版本
func (vcm *VersionChainManager) PurgeAllChains(minTxID uint64) int {
	vcm.mu.RLock()
	chains := make([]*VersionChain, 0, len(vcm.chains))
	for _, chain := range vcm.chains {
		chains = append(chains, chain)
	}
	vcm.mu.RUnlock()

	totalPurged := 0
	for _, chain := range chains {
		purged := chain.PurgeOldVersions(minTxID)
		totalPurged += purged
	}

	return totalPurged
}

// GetChainCount 获取版本链数量
func (vcm *VersionChainManager) GetChainCount() int {
	vcm.mu.RLock()
	defer vcm.mu.RUnlock()

	return len(vcm.chains)
}

// GetTotalVersionCount 获取所有版本的总数
func (vcm *VersionChainManager) GetTotalVersionCount() int {
	vcm.mu.RLock()
	defer vcm.mu.RUnlock()

	total := 0
	for _, chain := range vcm.chains {
		total += chain.GetLength()
	}

	return total
}

// StartGC 启动后台垃圾回收
func (vcm *VersionChainManager) StartGC(interval time.Duration, minTxIDProvider func() uint64) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				minTxID := minTxIDProvider()
				vcm.PurgeAllChains(minTxID)
			case <-vcm.gcChan:
				return
			}
		}
	}()
}

// StopGC 停止后台垃圾回收
func (vcm *VersionChainManager) StopGC() {
	select {
	case vcm.gcChan <- struct{}{}:
	default:
	}
}
