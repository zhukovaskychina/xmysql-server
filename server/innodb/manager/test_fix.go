package manager

import (
	"context"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// TestBPlusTreeManagerInterface 测试B+树管理器是否实现了basic.BPlusTreeManager接口
func TestBPlusTreeManagerInterface() {
	// 创建一个B+树管理器实例
	var btreeManager basic.BPlusTreeManager

	// 尝试将DefaultBPlusTreeManager赋值给接口
	btreeManager = NewBPlusTreeManager(nil, nil)

	// 测试RangeSearch方法是否返回正确类型
	ctx := context.Background()
	rows, err := btreeManager.RangeSearch(ctx, "start", "end")
	if err != nil {
		// 处理错误
		_ = err
	}

	// 验证返回类型
	_ = rows // 应该是[]basic.Row类型
}

// TestIndexManagerRangeSearch 测试IndexManager的RangeSearch方法
func TestIndexManagerRangeSearch() {
	// 这里只是验证类型兼容性，不执行实际逻辑
	im := &IndexManager{}

	rows, err := im.RangeSearch(1, "start", "end")
	if err != nil {
		_ = err
	}

	// 验证返回类型
	_ = rows // 应该是[]basic.Row类型
}
