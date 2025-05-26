package manager

import (
	"context"
	"fmt"
)

// IndexIteratorImpl 索引迭代器实现
type IndexIteratorImpl struct {
	index *EnhancedBTreeIndex // 索引实例
	ctx   context.Context     // 上下文

	// 当前位置
	currentPageNo uint32       // 当前页号
	currentSlot   uint16       // 当前槽位
	currentRecord *IndexRecord // 当前记录

	// 迭代状态
	isValid   bool // 是否有效
	isAtBegin bool // 是否在开始位置
	isAtEnd   bool // 是否在结束位置

	// 缓存
	currentPage *BTreePage // 当前页面
}

// NewIndexIterator 创建索引迭代器
func NewIndexIterator(index *EnhancedBTreeIndex, ctx context.Context) (*IndexIteratorImpl, error) {
	iterator := &IndexIteratorImpl{
		index:     index,
		ctx:       ctx,
		isValid:   false,
		isAtBegin: true,
		isAtEnd:   false,
	}

	return iterator, nil
}

// HasNext 检查是否有下一个记录
func (it *IndexIteratorImpl) HasNext() bool {
	if it.isAtEnd {
		return false
	}

	// 如果当前位置无效，尝试定位到第一个记录
	if !it.isValid {
		return it.trySeekFirst()
	}

	// 检查当前页面是否还有下一个记录
	if it.currentPage != nil && int(it.currentSlot+1) < len(it.currentPage.Records) {
		return true
	}

	// 检查是否有下一个页面
	if it.currentPage != nil && it.currentPage.NextPage != 0 {
		return true
	}

	return false
}

// Next 移动到下一个记录
func (it *IndexIteratorImpl) Next() (*IndexRecord, error) {
	if !it.HasNext() {
		it.isAtEnd = true
		return nil, fmt.Errorf("no more records")
	}

	// 如果当前位置无效，先定位到第一个记录
	if !it.isValid {
		return it.seekToFirst()
	}

	// 移动到下一个槽位
	it.currentSlot++

	// 检查是否需要移动到下一个页面
	if it.currentPage != nil && int(it.currentSlot) >= len(it.currentPage.Records) {
		if it.currentPage.NextPage == 0 {
			it.isAtEnd = true
			return nil, fmt.Errorf("reached end of index")
		}

		// 移动到下一个页面
		nextPage, err := it.index.GetPage(it.ctx, it.currentPage.NextPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page: %v", err)
		}

		it.currentPage = nextPage
		it.currentPageNo = nextPage.PageNo
		it.currentSlot = 0
	}

	// 获取当前记录
	if it.currentPage != nil && int(it.currentSlot) < len(it.currentPage.Records) {
		record := &it.currentPage.Records[it.currentSlot]
		it.currentRecord = record
		it.isAtBegin = false
		return record, nil
	}

	it.isAtEnd = true
	return nil, fmt.Errorf("no more records")
}

// SeekFirst 定位到第一个记录
func (it *IndexIteratorImpl) SeekFirst() error {
	_, err := it.seekToFirst()
	return err
}

// HasPrev 检查是否有前一个记录
func (it *IndexIteratorImpl) HasPrev() bool {
	if it.isAtBegin || !it.isValid {
		return false
	}

	// 检查当前页面是否还有前一个记录
	if it.currentSlot > 0 {
		return true
	}

	// 检查是否有前一个页面
	if it.currentPage != nil && it.currentPage.PrevPage != 0 {
		return true
	}

	return false
}

// Prev 移动到前一个记录
func (it *IndexIteratorImpl) Prev() (*IndexRecord, error) {
	if !it.HasPrev() {
		return nil, fmt.Errorf("no previous records")
	}

	// 如果当前槽位大于0，直接向前移动
	if it.currentSlot > 0 {
		it.currentSlot--
	} else {
		// 移动到前一个页面的最后一个记录
		if it.currentPage.PrevPage == 0 {
			it.isAtBegin = true
			return nil, fmt.Errorf("reached beginning of index")
		}

		prevPage, err := it.index.GetPage(it.ctx, it.currentPage.PrevPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get previous page: %v", err)
		}

		it.currentPage = prevPage
		it.currentPageNo = prevPage.PageNo
		if len(prevPage.Records) > 0 {
			it.currentSlot = uint16(len(prevPage.Records) - 1)
		} else {
			return nil, fmt.Errorf("empty previous page")
		}
	}

	// 获取当前记录
	if it.currentPage != nil && int(it.currentSlot) < len(it.currentPage.Records) {
		record := &it.currentPage.Records[it.currentSlot]
		it.currentRecord = record
		it.isAtEnd = false
		return record, nil
	}

	return nil, fmt.Errorf("invalid record position")
}

// SeekLast 定位到最后一个记录
func (it *IndexIteratorImpl) SeekLast() error {
	// 获取所有叶子页面
	leafPages, err := it.index.GetAllLeafPages(it.ctx)
	if err != nil {
		return fmt.Errorf("failed to get leaf pages: %v", err)
	}

	if len(leafPages) == 0 {
		it.isValid = false
		it.isAtEnd = true
		return fmt.Errorf("no leaf pages found")
	}

	// 获取最后一个叶子页面
	lastPageNo := leafPages[len(leafPages)-1]
	lastPage, err := it.index.GetPage(it.ctx, lastPageNo)
	if err != nil {
		return fmt.Errorf("failed to get last page: %v", err)
	}

	if len(lastPage.Records) == 0 {
		it.isValid = false
		it.isAtEnd = true
		return fmt.Errorf("last page is empty")
	}

	// 定位到最后一个记录
	it.currentPage = lastPage
	it.currentPageNo = lastPageNo
	it.currentSlot = uint16(len(lastPage.Records) - 1)
	it.currentRecord = &lastPage.Records[it.currentSlot]
	it.isValid = true
	it.isAtBegin = false
	it.isAtEnd = false

	return nil
}

// SeekTo 定位到指定键值
func (it *IndexIteratorImpl) SeekTo(key []byte) error {
	// 搜索指定键值
	record, err := it.index.Search(it.ctx, key)
	if err != nil {
		return fmt.Errorf("failed to search key: %v", err)
	}

	// 获取记录所在的页面
	page, err := it.index.GetPage(it.ctx, record.PageNo)
	if err != nil {
		return fmt.Errorf("failed to get page: %v", err)
	}

	// 在页面中查找记录位置
	for i, pageRecord := range page.Records {
		if it.index.compareKeys(pageRecord.Key, key) == 0 {
			it.currentPage = page
			it.currentPageNo = record.PageNo
			it.currentSlot = uint16(i)
			it.currentRecord = &pageRecord
			it.isValid = true
			it.isAtBegin = false
			it.isAtEnd = false
			return nil
		}
	}

	return fmt.Errorf("record not found in page")
}

// Current 获取当前记录
func (it *IndexIteratorImpl) Current() (*IndexRecord, error) {
	if !it.isValid || it.currentRecord == nil {
		return nil, fmt.Errorf("iterator is not positioned at valid record")
	}

	return it.currentRecord, nil
}

// GetPosition 获取当前位置
func (it *IndexIteratorImpl) GetPosition() (uint32, uint16) {
	return it.currentPageNo, it.currentSlot
}

// Close 关闭迭代器
func (it *IndexIteratorImpl) Close() error {
	it.isValid = false
	it.currentRecord = nil
	it.currentPage = nil
	return nil
}

// 内部辅助方法

// trySeekFirst 尝试定位到第一个记录
func (it *IndexIteratorImpl) trySeekFirst() bool {
	err := it.SeekFirst()
	return err == nil
}

// seekToFirst 定位到第一个记录
func (it *IndexIteratorImpl) seekToFirst() (*IndexRecord, error) {
	// 获取第一个叶子页面
	firstLeafPageNo, err := it.index.GetFirstLeafPage(it.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get first leaf page: %v", err)
	}

	// 获取第一个叶子页面
	firstPage, err := it.index.GetPage(it.ctx, firstLeafPageNo)
	if err != nil {
		return nil, fmt.Errorf("failed to get first page: %v", err)
	}

	if len(firstPage.Records) == 0 {
		it.isValid = false
		it.isAtEnd = true
		return nil, fmt.Errorf("first page is empty")
	}

	// 定位到第一个记录
	it.currentPage = firstPage
	it.currentPageNo = firstLeafPageNo
	it.currentSlot = 0
	it.currentRecord = &firstPage.Records[0]
	it.isValid = true
	it.isAtBegin = false
	it.isAtEnd = false

	return it.currentRecord, nil
}
