package page

import (
	"errors"
	"xmysql-server/server/common"
	"xmysql-server/server/innodb/storage/store/pages"
)

var (
	ErrBlobPageFull    = errors.New("BLOB页面已满")
	ErrInvalidBlobData = errors.New("无效的BLOB数据")
)

// BlobPageWrapper BLOB页面包装器
type BlobPageWrapper struct {
	*BasePageWrapper

	// 底层的BLOB页面实现
	blobPage *pages.BlobPage
}

// NewBlobPageWrapper 创建新的BLOB页面包装器
func NewBlobPageWrapper(id, spaceID uint32, segmentID uint64) *BlobPageWrapper {
	base := NewBasePageWrapper(id, spaceID, common.FIL_PAGE_TYPE_BLOB)
	blobPage := pages.NewBlobPage(spaceID, id, segmentID)

	return &BlobPageWrapper{
		BasePageWrapper: base,
		blobPage:        blobPage,
	}
}

// 实现IPageWrapper接口

// ParseFromBytes 从字节数据解析BLOB页面
func (bpw *BlobPageWrapper) ParseFromBytes(data []byte) error {
	bpw.Lock()
	defer bpw.Unlock()

	if err := bpw.BasePageWrapper.ParseFromBytes(data); err != nil {
		return err
	}

	// 解析BLOB页面特有的数据
	if err := bpw.blobPage.Deserialize(data); err != nil {
		return err
	}

	return nil
}

// ToBytes 序列化BLOB页面为字节数组
func (bpw *BlobPageWrapper) ToBytes() ([]byte, error) {
	bpw.RLock()
	defer bpw.RUnlock()

	// 序列化BLOB页面
	data := bpw.blobPage.Serialize()

	// 更新基础包装器的内容
	if len(bpw.content) != len(data) {
		bpw.content = make([]byte, len(data))
	}
	copy(bpw.content, data)

	return data, nil
}

// BLOB页面特有的方法

// SetBlobData 设置BLOB数据
func (bpw *BlobPageWrapper) SetBlobData(data []byte, totalLength uint32, offset uint32, nextPage uint32) error {
	bpw.Lock()
	defer bpw.Unlock()

	if err := bpw.blobPage.SetBlobData(data, totalLength, offset, nextPage); err != nil {
		return err
	}

	bpw.MarkDirty()
	return nil
}

// GetBlobData 获取BLOB数据
func (bpw *BlobPageWrapper) GetBlobData() []byte {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.GetBlobData()
}

// GetNextPageNo 获取下一个页面号
func (bpw *BlobPageWrapper) GetNextPageNo() uint32 {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.GetNextPageNo()
}

// IsLastPage 判断是否为最后一个页面
func (bpw *BlobPageWrapper) IsLastPage() bool {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.IsLastPage()
}

// GetSegmentID 获取段ID
func (bpw *BlobPageWrapper) GetSegmentID() uint64 {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.GetSegmentID()
}

// GetTotalLength 获取BLOB总长度
func (bpw *BlobPageWrapper) GetTotalLength() uint32 {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.GetTotalLength()
}

// GetCurrentOffset 获取当前偏移量
func (bpw *BlobPageWrapper) GetCurrentOffset() uint32 {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.GetCurrentOffset()
}

// Validate 验证BLOB页面数据完整性
func (bpw *BlobPageWrapper) Validate() error {
	bpw.RLock()
	defer bpw.RUnlock()

	return bpw.blobPage.Validate()
}

// GetBlobPage 获取底层的BLOB页面实现
func (bpw *BlobPageWrapper) GetBlobPage() *pages.BlobPage {
	return bpw.blobPage
}
