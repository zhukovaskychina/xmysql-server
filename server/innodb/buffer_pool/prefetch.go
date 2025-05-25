package buffer_pool

import (
	"container/list"
	"sync"
	"time"
)

// PrefetchManager 管理预读
type PrefetchManager struct {
	bufferPool    *BufferPool
	prefetchQueue *list.List    // 预读请求队列
	prefetchSize  int           // 每次预读的页面数量
	maxQueueSize  int           // 最大队列长度
	workers       int           // 预读工作线程数
	workerPool    chan struct{} // 工作线程池
	mu            sync.Mutex
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
		bufferPool:    bufferPool,
		prefetchQueue: list.New(),
		prefetchSize:  prefetchSize,
		maxQueueSize:  maxQueueSize,
		workers:       workers,
		workerPool:    make(chan struct{}, workers),
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
