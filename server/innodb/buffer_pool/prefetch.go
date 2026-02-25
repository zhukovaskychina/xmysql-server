package buffer_pool

import (
	"container/list"
	"sync"
	"time"
)

// AccessPattern 访问模式
type AccessPattern int

const (
	PatternSequential AccessPattern = iota // 顺序访问
	PatternRandom                          // 随机访问
	PatternHotSpot                         // 热点访问
	PatternUnknown                         // 未知模式
)

// PageAccess 页面访问记录
type PageAccess struct {
	SpaceID   uint32
	PageNo    uint32
	Timestamp time.Time
}

// PrefetchManager 管理预读
type PrefetchManager struct {
	bufferPool    *BufferPool
	prefetchQueue *list.List    // 预读请求队列
	prefetchSize  int           // 每次预读的页面数量
	maxQueueSize  int           // 最大队列长度
	workers       int           // 预读工作线程数
	workerPool    chan struct{} // 工作线程池
	mu            sync.Mutex

	// 智能预读相关
	accessHistory       []PageAccess
	maxHistorySize      int
	patternWindow       int     // 分析模式的窗口大小
	confidenceThreshold float64 // 置信度阈值
}

// PrefetchRequest 预读请求
type PrefetchRequest struct {
	SpaceID   uint32    // 表空间ID
	StartPage uint32    // 起始页号
	EndPage   uint32    // 结束页号
	Priority  int       // 优先级(1-10)
	Deadline  time.Time // 截止时间
}

// NewPrefetchManager 创建预读管理器
func NewPrefetchManager(bufferPool *BufferPool, prefetchSize int, maxQueueSize int, workers int) *PrefetchManager {
	pm := &PrefetchManager{
		bufferPool:          bufferPool,
		prefetchQueue:       list.New(),
		prefetchSize:        prefetchSize,
		maxQueueSize:        maxQueueSize,
		workers:             workers,
		workerPool:          make(chan struct{}, workers),
		accessHistory:       make([]PageAccess, 0, 1000),
		maxHistorySize:      1000,
		patternWindow:       10,
		confidenceThreshold: 0.7,
	}

	// 启动预读工作线程
	for i := 0; i < workers; i++ {
		go pm.prefetchWorker()
	}

	return pm
}

// TriggerPrefetch 触发预读
func (pm *PrefetchManager) TriggerPrefetch(spaceID uint32, startPage uint32) {
	// 计算预读范围
	endPage := startPage + uint32(pm.prefetchSize)

	request := &PrefetchRequest{
		SpaceID:   spaceID,
		StartPage: startPage,
		EndPage:   endPage,
		Priority:  5, // 默认优先级
		Deadline:  time.Now().Add(time.Second * 5),
	}

	pm.addPrefetchRequest(request)
}

// TriggerPrefetchWithPriority 带优先级的预读
func (pm *PrefetchManager) TriggerPrefetchWithPriority(spaceID uint32, startPage uint32, priority int, deadline time.Duration) {
	endPage := startPage + uint32(pm.prefetchSize)

	request := &PrefetchRequest{
		SpaceID:   spaceID,
		StartPage: startPage,
		EndPage:   endPage,
		Priority:  priority,
		Deadline:  time.Now().Add(deadline),
	}

	pm.addPrefetchRequest(request)
}

// addPrefetchRequest 添加预读请求到队列
func (pm *PrefetchManager) addPrefetchRequest(request *PrefetchRequest) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 如果队列已满,丢弃低优先级请求
	if pm.prefetchQueue.Len() >= pm.maxQueueSize {
		// 找到优先级最低的请求
		var lowestPriority *list.Element
		for e := pm.prefetchQueue.Front(); e != nil; e = e.Next() {
			req := e.Value.(*PrefetchRequest)
			if lowestPriority == nil || req.Priority < lowestPriority.Value.(*PrefetchRequest).Priority {
				lowestPriority = e
			}
		}

		// 如果新请求优先级更高,移除最低优先级请求
		if lowestPriority != nil && request.Priority > lowestPriority.Value.(*PrefetchRequest).Priority {
			pm.prefetchQueue.Remove(lowestPriority)
		} else {
			// 否则丢弃新请求
			return
		}
	}

	// 按优先级插入队列
	var insertAfter *list.Element
	for e := pm.prefetchQueue.Front(); e != nil; e = e.Next() {
		req := e.Value.(*PrefetchRequest)
		if request.Priority > req.Priority {
			insertAfter = e
			break
		}
	}

	if insertAfter != nil {
		pm.prefetchQueue.InsertAfter(request, insertAfter)
	} else {
		pm.prefetchQueue.PushBack(request)
	}
}

// prefetchWorker 预读工作线程
func (pm *PrefetchManager) prefetchWorker() {
	for {
		// 获取工作线程槽
		pm.workerPool <- struct{}{}

		// 获取预读请求
		request := pm.getNextRequest()
		if request == nil {
			<-pm.workerPool
			time.Sleep(time.Millisecond * 100)
			continue
		}

		// 执行预读
		go func(req *PrefetchRequest) {
			defer func() { <-pm.workerPool }()

			// 检查截止时间
			if time.Now().After(req.Deadline) {
				return
			}

			// 预读页面
			for pageNo := req.StartPage; pageNo < req.EndPage; pageNo++ {
				// 检查页面是否已在缓冲池中
				block, _ := pm.bufferPool.GetDirtyPageBlock(req.SpaceID, pageNo)
				if block != nil {
					continue
				}

				// 预读页面
				pm.bufferPool.GetPageBlock(req.SpaceID, pageNo)
			}
		}(request)
	}
}

// getNextRequest 获取下一个预读请求
func (pm *PrefetchManager) getNextRequest() *PrefetchRequest {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.prefetchQueue.Len() == 0 {
		return nil
	}

	// 获取并移除队首请求
	request := pm.prefetchQueue.Front()
	pm.prefetchQueue.Remove(request)
	return request.Value.(*PrefetchRequest)
}

// GetQueueLength 获取当前队列长度
func (pm *PrefetchManager) GetQueueLength() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.prefetchQueue.Len()
}

// ClearQueue 清空预读队列
func (pm *PrefetchManager) ClearQueue() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.prefetchQueue.Init()
}

// UpdateAccessHistory 更新访问历史
func (pm *PrefetchManager) UpdateAccessHistory(spaceID, pageNo uint32) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	access := PageAccess{
		SpaceID:   spaceID,
		PageNo:    pageNo,
		Timestamp: time.Now(),
	}

	pm.accessHistory = append(pm.accessHistory, access)

	// 保持历史记录在限制范围内
	if len(pm.accessHistory) > pm.maxHistorySize {
		pm.accessHistory = pm.accessHistory[len(pm.accessHistory)-pm.maxHistorySize:]
	}
}

// AnalyzeAccessPattern 分析访问模式
func (pm *PrefetchManager) AnalyzeAccessPattern() AccessPattern {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.accessHistory) < pm.patternWindow {
		return PatternUnknown
	}

	// 获取最近的访问记录
	recentAccesses := pm.accessHistory[len(pm.accessHistory)-pm.patternWindow:]

	sequentialCount := 0
	randomCount := 0
	hotSpotCount := 0

	// 分析连续访问
	for i := 1; i < len(recentAccesses); i++ {
		if recentAccesses[i].SpaceID == recentAccesses[i-1].SpaceID {
			pageDiff := int32(recentAccesses[i].PageNo) - int32(recentAccesses[i-1].PageNo)
			if pageDiff == 1 || pageDiff == -1 {
				sequentialCount++
			} else if pageDiff > 10 || pageDiff < -10 {
				randomCount++
			}
		}
	}

	// 分析热点访问
	pageFreq := make(map[uint64]int)
	for _, access := range recentAccesses {
		key := uint64(access.SpaceID)<<32 | uint64(access.PageNo)
		pageFreq[key]++
	}

	for _, freq := range pageFreq {
		if freq > len(recentAccesses)/4 { // 如果某个页面访问频率超过25%
			hotSpotCount++
		}
	}

	// 根据统计结果判断模式
	total := sequentialCount + randomCount + hotSpotCount
	if total == 0 {
		return PatternUnknown
	}

	if float64(sequentialCount)/float64(total) > 0.6 {
		return PatternSequential
	} else if float64(hotSpotCount)/float64(total) > 0.4 {
		return PatternHotSpot
	} else {
		return PatternRandom
	}
}

// TriggerSmartPrefetch 触发智能预读
func (pm *PrefetchManager) TriggerSmartPrefetch(spaceID, pageNo uint32) {
	pattern := pm.AnalyzeAccessPattern()

	// 根据访问模式调整预读策略
	switch pattern {
	case PatternSequential:
		// 顺序访问，预读更多页面
		pm.TriggerPrefetchWithPriority(spaceID, pageNo+1, 8, time.Second*3)
	case PatternRandom:
		// 随机访问，减少预读
		if pm.prefetchSize > 2 {
			endPage := pageNo + uint32(pm.prefetchSize/2)
			request := &PrefetchRequest{
				SpaceID:   spaceID,
				StartPage: pageNo + 1,
				EndPage:   endPage,
				Priority:  3,
				Deadline:  time.Now().Add(time.Second * 2),
			}
			pm.addPrefetchRequest(request)
		}
	case PatternHotSpot:
		// 热点访问，预读相邻页面
		pm.TriggerPrefetchWithPriority(spaceID, pageNo+1, 6, time.Second*4)
	default:
		// 未知模式，使用默认策略
		pm.TriggerPrefetch(spaceID, pageNo+1)
	}
}
