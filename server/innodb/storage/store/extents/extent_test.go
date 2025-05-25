package extents

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewExtentEntry(t *testing.T) {
	firstPageNo := uint32(64)
	entry := NewExtentEntry(firstPageNo)

	assert.Equal(t, uint64(0), entry.GetSegmentID())
	assert.Equal(t, uint8(EXTENT_FREE), entry.GetState())
	assert.Equal(t, uint8(0), entry.GetUsedPages())
	assert.Equal(t, firstPageNo, entry.FirstPageNo)
}

func TestExtentEntry_PageAllocation(t *testing.T) {
	entry := NewExtentEntry(0)

	// 测试页面分配
	err := entry.AllocatePage(0)
	assert.NoError(t, err)
	assert.Equal(t, uint8(EXTENT_PARTIAL), entry.GetState())
	assert.Equal(t, uint8(1), entry.GetUsedPages())
	assert.False(t, entry.IsPageFree(0))

	// 测试重复分配
	err = entry.AllocatePage(0)
	assert.Error(t, err)

	// 测试无效页号
	err = entry.AllocatePage(64)
	assert.Error(t, err)

	// 分配所有页面
	for i := uint8(1); i < PAGES_PER_EXTENT; i++ {
		err := entry.AllocatePage(i)
		assert.NoError(t, err)
	}

	assert.Equal(t, uint8(EXTENT_FULL), entry.GetState())
	assert.Equal(t, uint8(PAGES_PER_EXTENT), entry.GetUsedPages())
}

func TestExtentEntry_PageDeallocation(t *testing.T) {
	entry := NewExtentEntry(0)

	// 先分配一些页面
	err := entry.AllocatePage(0)
	assert.NoError(t, err)
	err = entry.AllocatePage(1)
	assert.NoError(t, err)

	// 测试页面释放
	err = entry.FreePage(0)
	assert.NoError(t, err)
	assert.True(t, entry.IsPageFree(0))
	assert.Equal(t, uint8(EXTENT_PARTIAL), entry.GetState())

	// 释放最后一个页面
	err = entry.FreePage(1)
	assert.NoError(t, err)
	assert.Equal(t, uint8(EXTENT_FREE), entry.GetState())
	assert.Equal(t, uint8(0), entry.GetUsedPages())

	// 测试重复释放
	err = entry.FreePage(1)
	assert.Error(t, err)

	// 测试无效页号
	err = entry.FreePage(64)
	assert.Error(t, err)
}

func TestExtentEntry_Serialization(t *testing.T) {
	entry := NewExtentEntry(128)
	entry.SetSegmentID(12345)
	entry.AllocatePage(0)
	entry.AllocatePage(1)

	// 序列化
	data := entry.Serialize()
	assert.Equal(t, 32, len(data))

	// 反序列化
	newEntry, err := DeserializeExtentEntry(data)
	assert.NoError(t, err)

	// 验证数据一致性
	assert.Equal(t, uint64(12345), newEntry.GetSegmentID())
	assert.Equal(t, uint8(EXTENT_PARTIAL), newEntry.GetState())
	assert.Equal(t, uint8(2), newEntry.GetUsedPages())
	assert.Equal(t, uint32(128), newEntry.FirstPageNo)
	assert.False(t, newEntry.IsPageFree(0))
	assert.False(t, newEntry.IsPageFree(1))
	assert.True(t, newEntry.IsPageFree(2))

	// 测试反序列化错误情况
	_, err = DeserializeExtentEntry(make([]byte, 16))
	assert.Error(t, err)
}
