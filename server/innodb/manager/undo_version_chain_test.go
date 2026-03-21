package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	formatmvcc "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format/mvcc"
)

// TestTraverseVersionChain 测试版本链遍历
func TestTraverseVersionChain(t *testing.T) {
	// 创建 Undo 日志管理器
	undoDir := t.TempDir()
	undoMgr, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)
	defer undoMgr.Close()

	// 场景：
	// - 事务1 (txID=100): 插入记录
	// - 事务2 (txID=101): 更新记录
	// - 事务3 (txID=102): 再次更新记录
	// - 事务4 (txID=103): 读取记录（创建 ReadView）

	recordID := uint64(1000)

	// 构建版本链
	chain := NewVersionChain(recordID)

	// 版本1: 事务100创建
	chain.AddVersion(100, 1000, 1000, []byte("version1_data"))

	// 版本2: 事务101更新
	chain.AddVersion(101, 1001, 1001, []byte("version2_data"))

	// 版本3: 事务102更新
	chain.AddVersion(102, 1002, 1002, []byte("version3_data"))

	// 保存版本链
	undoMgr.versionMu.Lock()
	undoMgr.versionChains[recordID] = chain
	undoMgr.versionMu.Unlock()

	t.Run("场景1: 事务103读取，事务100/101已提交，102活跃", func(t *testing.T) {
		// ReadView: 活跃事务=[102], 当前事务=103, 下一个事务=104
		readView := formatmvcc.NewReadView([]uint64{102}, 103, 104)

		// 应该看到版本2（事务101的版本）
		version, err := undoMgr.TraverseVersionChain(recordID, readView)
		assert.NoError(t, err)
		assert.NotNil(t, version)
		assert.Equal(t, int64(101), version.txID)
		assert.Equal(t, []byte("version2_data"), version.data)
	})

	t.Run("场景2: 事务103读取，所有事务都已提交", func(t *testing.T) {
		// ReadView: 活跃事务=[], 当前事务=103, 下一个事务=104
		readView := formatmvcc.NewReadView([]uint64{}, 103, 104)

		// 应该看到最新版本（版本3）
		version, err := undoMgr.TraverseVersionChain(recordID, readView)
		assert.NoError(t, err)
		assert.NotNil(t, version)
		assert.Equal(t, int64(102), version.txID)
		assert.Equal(t, []byte("version3_data"), version.data)
	})

	t.Run("场景3: 事务103读取，所有事务都活跃", func(t *testing.T) {
		// ReadView: 活跃事务=[100,101,102], 当前事务=103, 下一个事务=104
		readView := formatmvcc.NewReadView([]uint64{100, 101, 102}, 103, 104)

		// 应该看不到任何版本
		version, err := undoMgr.TraverseVersionChain(recordID, readView)
		assert.Error(t, err)
		assert.Nil(t, version)
		assert.Contains(t, err.Error(), "没有可见版本")
	})

	t.Run("场景4: 事务101读取自己的修改", func(t *testing.T) {
		// ReadView: 活跃事务=[100,102], 当前事务=101, 下一个事务=104
		readView := formatmvcc.NewReadView([]uint64{100, 102}, 101, 104)

		// 应该看到自己的版本（版本2）
		version, err := undoMgr.TraverseVersionChain(recordID, readView)
		assert.NoError(t, err)
		assert.NotNil(t, version)
		assert.Equal(t, int64(101), version.txID)
	})
}

// TestGetVisibleVersion 测试获取可见版本的便捷方法
func TestGetVisibleVersion(t *testing.T) {
	undoDir := t.TempDir()
	undoMgr, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)
	defer undoMgr.Close()

	recordID := uint64(2000)

	// 构建版本链
	chain := NewVersionChain(recordID)
	chain.AddVersion(200, 2000, 2000, []byte("test_data_v1"))
	chain.AddVersion(201, 2001, 2001, []byte("test_data_v2"))

	undoMgr.versionMu.Lock()
	undoMgr.versionChains[recordID] = chain
	undoMgr.versionMu.Unlock()

	// 创建 ReadView
	readView := formatmvcc.NewReadView([]uint64{201}, 202, 203)

	// 获取可见版本
	data, lsn, err := undoMgr.GetVisibleVersion(recordID, readView)
	assert.NoError(t, err)
	assert.Equal(t, []byte("test_data_v1"), data)
	assert.Equal(t, uint64(2000), lsn)
}

// TestGetVersionChainLength 测试获取版本链长度
func TestGetVersionChainLength(t *testing.T) {
	undoDir := t.TempDir()
	undoMgr, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)
	defer undoMgr.Close()

	recordID := uint64(3000)

	// 不存在的版本链
	length := undoMgr.GetVersionChainLength(recordID)
	assert.Equal(t, 0, length)

	// 创建版本链
	chain := NewVersionChain(recordID)
	chain.AddVersion(300, 3000, 3000, []byte("v1"))
	chain.AddVersion(301, 3001, 3001, []byte("v2"))
	chain.AddVersion(302, 3002, 3002, []byte("v3"))

	undoMgr.versionMu.Lock()
	undoMgr.versionChains[recordID] = chain
	undoMgr.versionMu.Unlock()

	// 获取长度
	length = undoMgr.GetVersionChainLength(recordID)
	assert.Equal(t, 3, length)
}

// TestGetVersionChainInfo 测试获取版本链详细信息
func TestGetVersionChainInfo(t *testing.T) {
	undoDir := t.TempDir()
	undoMgr, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)
	defer undoMgr.Close()

	recordID := uint64(4000)

	// 不存在的版本链
	info := undoMgr.GetVersionChainInfo(recordID)
	assert.False(t, info["exists"].(bool))

	// 创建版本链
	chain := NewVersionChain(recordID)
	chain.AddVersion(400, 4000, 4000, []byte("data1"))
	chain.AddVersion(401, 4001, 4001, []byte("data2"))

	undoMgr.versionMu.Lock()
	undoMgr.versionChains[recordID] = chain
	undoMgr.versionMu.Unlock()

	// 获取信息
	info = undoMgr.GetVersionChainInfo(recordID)
	assert.True(t, info["exists"].(bool))
	assert.Equal(t, recordID, info["recordID"].(uint64))
	assert.Equal(t, 2, info["versionCount"].(int))

	versions := info["versions"].([]map[string]interface{})
	assert.Len(t, versions, 2)

	// 验证第一个版本（最新）
	assert.Equal(t, int64(401), versions[0]["txID"].(int64))
	assert.Equal(t, uint64(4001), versions[0]["lsn"].(uint64))
}

// TestVersionChainConcurrency 测试版本链并发访问
func TestVersionChainConcurrency(t *testing.T) {
	undoDir := t.TempDir()
	undoMgr, err := NewUndoLogManager(undoDir)
	assert.NoError(t, err)
	defer undoMgr.Close()

	recordID := uint64(5000)

	// 创建版本链
	chain := NewVersionChain(recordID)
	for i := 0; i < 10; i++ {
		chain.AddVersion(int64(500+i), uint64(5000+i), uint64(5000+i), []byte("data"))
	}

	undoMgr.versionMu.Lock()
	undoMgr.versionChains[recordID] = chain
	undoMgr.versionMu.Unlock()

	// 并发读取
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(txID int) {
			readView := formatmvcc.NewReadView([]uint64{}, uint64(600+txID), 700)
			_, err := undoMgr.TraverseVersionChain(recordID, readView)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// 等待所有协程完成
	for i := 0; i < 10; i++ {
		<-done
	}
}
