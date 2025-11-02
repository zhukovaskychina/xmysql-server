package mvcc

// Deprecated: This file is deprecated and will be removed in a future version.
// VersionChain and RecordVersion functionality has been migrated to format/mvcc/version_chain.go
//
// Migration guide:
// - Old: import "github.com/.../storage/store/mvcc"
//        vc := mvcc.NewVersionChain()
//        vc.InsertVersion(trxID, rollPtr, data, deleteMark)
//
// - New: import formatmvcc "github.com/.../storage/format/mvcc"
//        vc := formatmvcc.NewVersionChainManager()
//        rv := formatmvcc.NewRecordVersion(txID, key, value)
//
// Key differences:
// 1. format/mvcc uses uint64 instead of TrxId (int64) for transaction IDs
// 2. format/mvcc.RecordVersion has more fields (Version, Key, Value, CreateTS)
// 3. format/mvcc provides VersionChainManager for managing multiple version chains
//
// This file will be removed after all references are updated.

import (
	"sync"
	"time"
)

// RecordVersion 记录版本
// Deprecated: 使用format/mvcc.RecordVersion代替
type RecordVersion struct {
	TrxID      TrxId          // 创建该版本的事务ID
	RollPtr    uint64         // 回滚指针，指向undo log中的记录
	Data       []byte         // 记录数据
	DeleteMark bool           // 删除标记
	CreateTime time.Time      // 创建时间
	Next       *RecordVersion // 下一个版本（旧版本）
}

// VersionChain 版本链管理器
// Deprecated: 使用format/mvcc.VersionChain和VersionChainManager代替
type VersionChain struct {
	mu       sync.RWMutex
	head     *RecordVersion // 链表头（最新版本）
	length   int            // 版本链长度
	minTrxID TrxId          // 最小事务ID（用于GC）
}

// NewVersionChain 创建新的版本链
func NewVersionChain() *VersionChain {
	return &VersionChain{
		head:   nil,
		length: 0,
	}
}

// InsertVersion 插入新版本（在链表头部插入）
func (vc *VersionChain) InsertVersion(trxID TrxId, rollPtr uint64, data []byte, deleteMark bool) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	newVersion := &RecordVersion{
		TrxID:      trxID,
		RollPtr:    rollPtr,
		Data:       make([]byte, len(data)),
		DeleteMark: deleteMark,
		CreateTime: time.Now(),
		Next:       vc.head,
	}
	copy(newVersion.Data, data)

	vc.head = newVersion
	vc.length++
}

// FindVisibleVersion 查找对指定ReadView可见的版本
func (vc *VersionChain) FindVisibleVersion(rv *ReadView) *RecordVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	current := vc.head
	for current != nil {
		// 检查该版本是否对ReadView可见
		if rv.IsVisible(int64(current.TrxID)) {
			// 如果是删除标记的版本，继续查找
			if current.DeleteMark {
				current = current.Next
				continue
			}
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

// GetVersionByTrxID 根据事务ID查找版本
func (vc *VersionChain) GetVersionByTrxID(trxID TrxId) *RecordVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	current := vc.head
	for current != nil {
		if current.TrxID == trxID {
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
// 清理所有事务ID小于minTrxID的版本（已不可能被任何活跃事务看到）
func (vc *VersionChain) PurgeOldVersions(minTrxID TrxId) int {
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
		if current.TrxID < minTrxID && current.Next != nil {
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
func (vcm *VersionChainManager) PurgeAllChains(minTrxID TrxId) int {
	vcm.mu.RLock()
	chains := make([]*VersionChain, 0, len(vcm.chains))
	for _, chain := range vcm.chains {
		chains = append(chains, chain)
	}
	vcm.mu.RUnlock()

	totalPurged := 0
	for _, chain := range chains {
		purged := chain.PurgeOldVersions(minTrxID)
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
func (vcm *VersionChainManager) StartGC(interval time.Duration, minTrxIDProvider func() TrxId) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				minTrxID := minTrxIDProvider()
				vcm.PurgeAllChains(minTrxID)
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
