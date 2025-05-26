package manager

import (
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"testing"
)

// TestStorageManagerInterface 测试StorageManager是否正确实现了basic.StorageManager接口
func TestStorageManagerInterface(t *testing.T) {
	// 创建一个StorageManager实例
	var sm *StorageManager = &StorageManager{}

	// 测试接口实现
	var _ basic.StorageManager = sm

	t.Log("StorageManager correctly implements basic.StorageManager interface")
}

// TestStorageManagerMethods 测试StorageManager的主要方法是否可调用
func TestStorageManagerMethods(t *testing.T) {
	// 这个测试只验证方法签名正确性，不执行实际逻辑
	sm := &StorageManager{}

	// 测试页面管理方法签名
	var pageType basic.PageType = basic.PageTypeIndex
	_, err := sm.AllocPage(1, pageType)
	if err == nil {
		t.Log("AllocPage method signature is correct")
	}

	_, err = sm.GetPage(1, 1)
	if err == nil {
		t.Log("GetPage method signature is correct")
	}

	err = sm.FreePage(1, 1)
	if err == nil {
		t.Log("FreePage method signature is correct")
	}

	// 测试段管理方法签名
	_, err = sm.CreateSegment(1, basic.SegmentPurposeLeaf)
	if err == nil {
		t.Log("CreateSegment method signature is correct")
	}

	_, err = sm.GetSegment(1)
	if err == nil {
		t.Log("GetSegment method signature is correct")
	}

	err = sm.FreeSegment(1)
	if err == nil {
		t.Log("FreeSegment method signature is correct")
	}

	// 测试区管理方法签名
	_, err = sm.AllocateExtent(1, basic.ExtentPurposeData)
	if err == nil {
		t.Log("AllocateExtent method signature is correct")
	}

	err = sm.FreeExtent(1, 1)
	if err == nil {
		t.Log("FreeExtent method signature is correct")
	}

	// 测试事务方法签名
	tx, err := sm.Begin()
	if err == nil && tx != nil {
		t.Log("Begin method signature is correct")

		err = sm.Commit(tx)
		if err == nil {
			t.Log("Commit method signature is correct")
		}

		err = sm.Rollback(tx)
		if err == nil {
			t.Log("Rollback method signature is correct")
		}
	}

	// 测试维护方法签名
	err = sm.Flush()
	if err == nil {
		t.Log("Flush method signature is correct")
	}

	err = sm.Close()
	if err == nil {
		t.Log("Close method signature is correct")
	}
}
