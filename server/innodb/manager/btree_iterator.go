package manager

import (
	"context"
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
)

/*
BTreeIterator B+树迭代器实现

核心功能：
1. 流式遍历 - 支持叶子节点链表的顺序遍历
2. 预读优化 - 提前加载后续节点到缓存
3. 范围查询 - 高效的范围扫描支持

设计要点：
- 利用叶子节点链表结构
- 减少树遍历次数
- 提升缓存命中率
*/

// BTreeIterator B+树迭代器
type BTreeIterator struct {
	manager         *DefaultBPlusTreeManager
	ctx             context.Context
	startKey        interface{}
	endKey          interface{}
	currentNode     *BPlusTreeNode
	currentPos      int
	prefetchBuffer  []*BPlusTreeNode // 预读缓冲区
	prefetchSize    int              // 预读大小
	closed          bool
	totalScanned    uint64 // 已扫描记录数
	prefetchEnabled bool   // 是否启用预读
}

// NewBTreeIterator 创建B+树迭代器
func NewBTreeIterator(manager *DefaultBPlusTreeManager, ctx context.Context, startKey, endKey interface{}) (*BTreeIterator, error) {
	// 查找起始叶子节点
	startNode, err := manager.findLeafNode(ctx, startKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find start leaf node: %v", err)
	}

	// 定位到起始键的位置
	startPos := 0
	for i, key := range startNode.Keys {
		if manager.compareKeys(key, startKey) >= 0 {
			startPos = i
			break
		}
	}

	iterator := &BTreeIterator{
		manager:         manager,
		ctx:             ctx,
		startKey:        startKey,
		endKey:          endKey,
		currentNode:     startNode,
		currentPos:      startPos,
		prefetchBuffer:  make([]*BPlusTreeNode, 0, 5),
		prefetchSize:    3, // 默认预读3个节点
		closed:          false,
		totalScanned:    0,
		prefetchEnabled: true,
	}

	// 执行初始预读
	if iterator.prefetchEnabled {
		iterator.prefetch()
	}

	logger.Debugf("🔍 Iterator created: startKey=%v, endKey=%v, startNode=%d, startPos=%d",
		startKey, endKey, startNode.PageNum, startPos)

	return iterator, nil
}

// HasNext 检查是否有下一条记录
func (it *BTreeIterator) HasNext() bool {
	if it.closed {
		return false
	}

	// 检查当前节点是否还有记录
	if it.currentPos < len(it.currentNode.Keys) {
		// 检查是否超出范围
		currentKey := it.currentNode.Keys[it.currentPos]
		if it.manager.compareKeys(currentKey, it.endKey) <= 0 {
			return true
		}
		return false
	}

	// 当前节点遍历完，检查是否有下一个节点
	if it.currentNode.NextLeaf == 0 {
		return false
	}

	return true
}

// Next 获取下一条记录
func (it *BTreeIterator) Next() (key interface{}, value []byte, err error) {
	if it.closed {
		return nil, nil, fmt.Errorf("iterator is closed")
	}

	if !it.HasNext() {
		return nil, nil, fmt.Errorf("no more records")
	}

	// 如果当前节点遍历完，移动到下一个节点
	if it.currentPos >= len(it.currentNode.Keys) {
		if err := it.moveToNextNode(); err != nil {
			return nil, nil, err
		}
	}

	// 获取当前键
	key = it.currentNode.Keys[it.currentPos]

	// 检查是否超出范围
	if it.manager.compareKeys(key, it.endKey) > 0 {
		return nil, nil, fmt.Errorf("reached end of range")
	}

	// 获取记录
	recordPos := int(it.currentNode.Records[it.currentPos])
	record, err := it.manager.getRecord(it.ctx, it.currentNode.PageNum, recordPos)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get record: %v", err)
	}

	// 移动到下一个位置
	it.currentPos++
	it.totalScanned++

	return key, record.Data, nil
}

// moveToNextNode 移动到下一个叶子节点
func (it *BTreeIterator) moveToNextNode() error {
	if it.currentNode.NextLeaf == 0 {
		return fmt.Errorf("no next node")
	}

	// 首先尝试从预读缓冲区获取
	if len(it.prefetchBuffer) > 0 {
		it.currentNode = it.prefetchBuffer[0]
		it.prefetchBuffer = it.prefetchBuffer[1:]
		it.currentPos = 0

		logger.Debugf("📖 Moved to next node from prefetch buffer: page=%d", it.currentNode.PageNum)

		// 触发新的预读
		if it.prefetchEnabled && len(it.prefetchBuffer) < it.prefetchSize {
			go it.prefetch()
		}

		return nil
	}

	// 如果预读缓冲区为空，直接加载下一个节点
	nextNode, err := it.manager.getNode(it.ctx, it.currentNode.NextLeaf)
	if err != nil {
		return fmt.Errorf("failed to get next node: %v", err)
	}

	it.currentNode = nextNode
	it.currentPos = 0

	logger.Debugf("📖 Moved to next node: page=%d", it.currentNode.PageNum)

	// 触发预读
	if it.prefetchEnabled {
		go it.prefetch()
	}

	return nil
}

// prefetch 预读后续节点
func (it *BTreeIterator) prefetch() {
	if !it.prefetchEnabled || it.closed {
		return
	}

	// 从当前节点的下一个节点开始预读
	var nextPageNo uint32
	if len(it.prefetchBuffer) > 0 {
		// 如果缓冲区不为空，从最后一个预读节点继续
		lastNode := it.prefetchBuffer[len(it.prefetchBuffer)-1]
		nextPageNo = lastNode.NextLeaf
	} else {
		// 如果缓冲区为空，从当前节点的下一个节点开始
		nextPageNo = it.currentNode.NextLeaf
	}

	// 预读指定数量的节点
	for i := len(it.prefetchBuffer); i < it.prefetchSize && nextPageNo != 0; i++ {
		node, err := it.manager.getNode(it.ctx, nextPageNo)
		if err != nil {
			logger.Debugf("⚠️ Prefetch failed for page %d: %v", nextPageNo, err)
			break
		}

		it.prefetchBuffer = append(it.prefetchBuffer, node)
		nextPageNo = node.NextLeaf

		logger.Debugf("🔮 Prefetched node: page=%d (buffer size: %d)", node.PageNum, len(it.prefetchBuffer))
	}
}

// SetPrefetchSize 设置预读大小
func (it *BTreeIterator) SetPrefetchSize(size int) {
	if size > 0 && size <= 10 {
		it.prefetchSize = size
	}
}

// EnablePrefetch 启用预读
func (it *BTreeIterator) EnablePrefetch(enabled bool) {
	it.prefetchEnabled = enabled
}

// GetStats 获取迭代器统计信息
func (it *BTreeIterator) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_scanned":     it.totalScanned,
		"prefetch_size":     it.prefetchSize,
		"prefetch_buffered": len(it.prefetchBuffer),
		"current_node":      it.currentNode.PageNum,
		"current_pos":       it.currentPos,
		"prefetch_enabled":  it.prefetchEnabled,
	}
}

// Close 关闭迭代器
func (it *BTreeIterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true
	it.prefetchBuffer = nil

	logger.Debugf("🔒 Iterator closed: total_scanned=%d", it.totalScanned)
	return nil
}

// RangeSearchOptimized 优化的范围查询（使用迭代器）
func (m *DefaultBPlusTreeManager) RangeSearchOptimized(ctx context.Context, startKey, endKey interface{}) ([]interface{}, error) {
	// 创建迭代器
	iterator, err := NewBTreeIterator(m, ctx, startKey, endKey)
	if err != nil {
		return nil, err
	}
	defer iterator.Close()

	results := make([]interface{}, 0)

	// 使用迭代器遍历
	for iterator.HasNext() {
		key, value, err := iterator.Next()
		if err != nil {
			return nil, err
		}

		// 将key和value打包返回
		results = append(results, map[string]interface{}{
			"key":   key,
			"value": value,
		})
	}

	// 输出统计信息
	stats := iterator.GetStats()
	logger.Debugf("📊 Range search stats: %+v", stats)

	return results, nil
}
