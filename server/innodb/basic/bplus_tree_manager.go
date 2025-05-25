package basic

import (
	"context"
)

// BPlusTreeManager 管理B+树索引结构
type BPlusTreeManager interface {
	// 初始化，从rootPage开始建立树结构
	Init(ctx context.Context, spaceId uint32, rootPage uint32) error

	// 遍历所有叶子页号
	GetAllLeafPages(ctx context.Context) ([]uint32, error)

	// 查找一个主键，返回所在页号和记录槽位
	Search(ctx context.Context, key interface{}) (uint32, int, error)

	// 插入一个键值对
	Insert(ctx context.Context, key interface{}, value []byte) error

	// 范围查询
	RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]Row, error)

	// 获取第一个叶子节点
	GetFirstLeafPage(ctx context.Context) (uint32, error)
}
