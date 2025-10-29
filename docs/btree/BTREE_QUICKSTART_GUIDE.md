# B+树核心功能开发快速开始指南

## 快速导航

- [环境准备](#环境准备)
- [代码结构](#代码结构)
- [开发流程](#开发流程)
- [测试指南](#测试指南)
- [常见问题](#常见问题)

---

## 环境准备

### 1. 确认Go版本

```bash
go version
# 应显示: go version go1.16.2 darwin/amd64 (或类似)
```

⚠️ **重要**: 项目使用Go 1.16.2，不支持`atomic.Uint32`等新特性。

### 2. 克隆项目

```bash
cd /Users/zhukovasky/GolandProjects/xmysql-server
```

### 3. 安装依赖

```bash
go mod download
```

### 4. 运行基线测试

```bash
# 运行所有测试
go test ./server/innodb/manager/... -v

# 运行性能测试
go test ./server/innodb/manager/... -bench=. -benchmem
```

---

## 代码结构

### 核心文件

```
server/innodb/manager/
├── bplus_tree_manager.go       ← B+树管理器主体
├── btree_split.go              ← 节点分裂算法
├── btree_merge.go              ← 节点合并算法
├── btree_interface.go          ← 接口定义
├── enhanced_btree_manager.go   ← 增强版管理器
├── enhanced_btree_index.go     ← 增强版索引
├── index_manager.go            ← 索引管理器
└── index_metadata.go           ← 索引元数据
```

### 关键接口

```go
// B+树管理器接口
type BTreeManager interface {
    Insert(ctx, indexID, key, value) error
    Delete(ctx, indexID, key) error
    Search(ctx, indexID, key) (*IndexRecord, error)
    RangeSearch(ctx, indexID, startKey, endKey) ([]IndexRecord, error)
}

// B+树索引接口
type BTreeIndex interface {
    Insert(ctx, key, value) error
    Delete(ctx, key) error
    Search(ctx, key) (*IndexRecord, error)
    RangeSearch(ctx, startKey, endKey) ([]IndexRecord, error)
}
```

---

## 开发流程

### 任务1: 实现节点分裂 (示例)

#### 步骤1: 创建分支

```bash
git checkout -b feature/idx-001-node-split
```

#### 步骤2: 修改代码

**文件**: `btree_split.go`

```go
// 步骤2.1: 添加可配置分裂比例
func NewNodeSplitter(manager *DefaultBPlusTreeManager, degree int) *NodeSplitter {
    return &NodeSplitter{
        manager:    manager,
        minKeys:    degree - 1,
        maxKeys:    2*degree - 1,
        splitRatio: 0.5, // 默认50/50分裂
    }
}

// 步骤2.2: 允许动态设置分裂比例
func (s *NodeSplitter) SetSplitRatio(ratio float64) {
    if ratio >= 0.4 && ratio <= 0.6 {
        s.splitRatio = ratio
    }
}

// 步骤2.3: 完善递归向上分裂
func (s *NodeSplitter) InsertIntoParent(ctx context.Context, leftPage, rightPage uint32, middleKey interface{}) error {
    logger.Debugf("📌 Inserting middle_key=%v into parent (left=%d, right=%d)", middleKey, leftPage, rightPage)

    // 如果分裂的是根节点，创建新根
    if leftPage == s.manager.rootPage {
        return s.createNewRoot(ctx, leftPage, rightPage, middleKey)
    }

    // 找到父节点
    parentNode, err := s.findParentNode(ctx, leftPage)
    if err != nil {
        return fmt.Errorf("failed to find parent node: %v", err)
    }

    // 在父节点中插入新键和子节点指针
    insertPos := s.findInsertPosition(parentNode.Keys, middleKey)
    parentNode.Keys = append(parentNode.Keys[:insertPos], append([]interface{}{middleKey}, parentNode.Keys[insertPos:]...)...)
    parentNode.Children = append(parentNode.Children[:insertPos+1], append([]uint32{rightPage}, parentNode.Children[insertPos+1:]...)...)
    parentNode.isDirty = true

    // 检查父节点是否需要分裂（递归向上）
    if len(parentNode.Keys) > s.maxKeys {
        logger.Debugf("⚠️ Parent node %d is full (%d keys), needs splitting", parentNode.PageNum, len(parentNode.Keys))
        newParentPage, newMiddleKey, err := s.SplitNonLeafNode(ctx, parentNode)
        if err != nil {
            return fmt.Errorf("failed to split parent node: %v", err)
        }
        // 递归向上插入（关键改进）
        return s.InsertIntoParent(ctx, parentNode.PageNum, newParentPage, newMiddleKey)
    }

    logger.Debugf("✅ Inserted into parent node %d successfully", parentNode.PageNum)
    return nil
}
```

#### 步骤3: 编写单元测试

**文件**: `btree_split_test.go` (新建)

```go
package manager

import (
    "context"
    "testing"
    "github.com/stretchr/testify/assert"
)

func TestNodeSplitter_SplitLeafNode(t *testing.T) {
    // 准备测试环境
    bpm := setupTestBufferPool(t)
    btm := NewBPlusTreeManager(bpm, &DefaultBPlusTreeConfig)
    splitter := NewNodeSplitter(btm, 3) // degree=3

    // 创建一个满载的叶子节点
    fullNode := &BPlusTreeNode{
        PageNum: 1001,
        IsLeaf:  true,
        Keys:    []interface{}{10, 20, 30, 40, 50}, // 5个键(超过maxKeys=5)
        Records: []uint32{1, 2, 3, 4, 5},
        NextLeaf: 0,
    }

    // 执行分裂
    ctx := context.Background()
    newPageNo, middleKey, err := splitter.SplitLeafNode(ctx, fullNode)

    // 验证结果
    assert.NoError(t, err)
    assert.Greater(t, newPageNo, uint32(0))
    assert.Equal(t, 30, middleKey) // 中间键应该是第3个
    assert.Equal(t, 2, len(fullNode.Keys)) // 左节点保留2个键
    assert.Equal(t, newPageNo, fullNode.NextLeaf) // 链表指针更新
}

func TestNodeSplitter_RecursiveSplit(t *testing.T) {
    // 测试多层递归分裂
    // TODO: 实现测试逻辑
}

func TestNodeSplitter_RootSplit(t *testing.T) {
    // 测试根节点分裂和树高度增加
    // TODO: 实现测试逻辑
}
```

#### 步骤4: 运行测试

```bash
# 运行单个测试
go test ./server/innodb/manager -run TestNodeSplitter_SplitLeafNode -v

# 运行所有分裂测试
go test ./server/innodb/manager -run TestNodeSplitter -v

# 查看覆盖率
go test ./server/innodb/manager -cover
```

#### 步骤5: 代码审查

- [ ] 检查所有公开方法有文档注释
- [ ] 检查错误处理完整（无panic）
- [ ] 检查日志记录适当
- [ ] 检查单元测试覆盖关键路径
- [ ] 运行 `go vet` 和 `golint`

```bash
go vet ./server/innodb/manager/...
golint ./server/innodb/manager/...
```

#### 步骤6: 提交代码

```bash
git add btree_split.go btree_split_test.go
git commit -m "feat(idx-001): 完善节点分裂功能

- 实现可配置分裂比例
- 完善递归向上分裂逻辑
- 完善根节点分裂和树高度增加
- 添加10个单元测试用例
- 测试覆盖率达到90%

Closes #IDX-001"

git push origin feature/idx-001-node-split
```

---

## 测试指南

### 单元测试模板

```go
func TestFunctionName(t *testing.T) {
    // 准备阶段 (Arrange)
    ctx := context.Background()
    manager := setupTestManager(t)

    // 执行阶段 (Act)
    result, err := manager.FunctionName(ctx, testInput)

    // 断言阶段 (Assert)
    assert.NoError(t, err)
    assert.Equal(t, expectedValue, result)
}
```

### 集成测试模板

```go
func TestIntegration_BulkInsert(t *testing.T) {
    // 准备: 创建表和索引
    manager := setupIntegrationEnv(t)

    // 执行: 插入100万行
    for i := 0; i < 1000000; i++ {
        err := manager.Insert(ctx, uint64(i), []byte(fmt.Sprintf("value_%d", i)))
        assert.NoError(t, err)
    }

    // 验证: 检查索引完整性
    stats, err := manager.GetStatistics()
    assert.NoError(t, err)
    assert.Equal(t, uint64(1000000), stats.TotalRecords)
}
```

### 性能测试模板

```go
func BenchmarkInsert(b *testing.B) {
    manager := setupBenchmarkManager(b)
    ctx := context.Background()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = manager.Insert(ctx, uint64(i), []byte("test"))
    }
}
```

### 运行测试

```bash
# 单元测试
go test ./server/innodb/manager -v

# 集成测试
go test ./server/innodb/manager -tags=integration -v

# 性能测试
go test ./server/innodb/manager -bench=. -benchmem -benchtime=10s

# 覆盖率报告
go test ./server/innodb/manager -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html
```

---

## 常见问题

### Q1: 如何调试B+树结构？

**A**: 使用日志输出和可视化工具

```go
// 添加调试方法
func (m *DefaultBPlusTreeManager) PrintTree() {
    m.printNode(m.rootPage, 0)
}

func (m *DefaultBPlusTreeManager) printNode(pageNo uint32, level int) {
    node, _ := m.getNode(context.Background(), pageNo)
    indent := strings.Repeat("  ", level)
    fmt.Printf("%sNode %d (IsLeaf=%v, Keys=%v)\n", indent, pageNo, node.IsLeaf, node.Keys)
    
    if !node.IsLeaf {
        for _, child := range node.Children {
            m.printNode(child, level+1)
        }
    }
}

// 使用
btm.PrintTree()
// 输出:
// Node 1 (IsLeaf=false, Keys=[50, 100])
//   Node 2 (IsLeaf=true, Keys=[10, 20, 30, 40])
//   Node 3 (IsLeaf=true, Keys=[50, 60, 70, 80])
//   Node 4 (IsLeaf=true, Keys=[100, 110, 120])
```

### Q2: 如何处理并发问题？

**A**: 使用细粒度锁和无锁设计

```go
// 推荐: 节点级读写锁
type BPlusTreeNode struct {
    mu       sync.RWMutex
    PageNum  uint32
    // ...
}

// 使用示例
func (m *DefaultBPlusTreeManager) Search(ctx context.Context, key interface{}) (uint32, int, error) {
    node, err := m.getNode(ctx, m.rootPage)
    if err != nil {
        return 0, 0, err
    }

    for !node.IsLeaf {
        node.mu.RLock()
        childIndex := m.findChildIndex(node, key)
        childPage := node.Children[childIndex]
        node.mu.RUnlock()

        node, err = m.getNode(ctx, childPage)
        if err != nil {
            return 0, 0, err
        }
    }

    node.mu.RLock()
    defer node.mu.RUnlock()
    recordIndex := m.findRecordIndex(node, key)
    return node.PageNum, int(node.Records[recordIndex]), nil
}
```

### Q3: 如何优化性能？

**A**: 5个关键优化点

1. **缓存热点节点**
```go
// 根节点和高层节点常驻内存
func (m *DefaultBPlusTreeManager) shouldEvict(node *BPlusTreeNode) bool {
    if node.PageNum == m.rootPage {
        return false // 根节点不淘汰
    }
    // 其他逻辑...
}
```

2. **批量刷新脏页**
```go
// 累积脏页，定期批量刷新
func (m *DefaultBPlusTreeManager) flushDirtyNodesBatch() {
    dirtyPages := m.collectDirtyPages()
    for _, page := range dirtyPages {
        m.bufferPoolManager.FlushPage(m.spaceId, page.PageNum)
    }
}
```

3. **预读优化**
```go
// 范围扫描时预读后续页面
func (m *DefaultBPlusTreeManager) RangeSearch(ctx context.Context, startKey, endKey interface{}) {
    // 检测到顺序访问
    if m.detectSequentialAccess() {
        // 预读后续4页
        m.prefetchNextPages(currentNode.NextLeaf, 4)
    }
}
```

4. **压缩键**
```go
// 对于字符串键，使用前缀压缩
type CompressedKey struct {
    PrefixLen uint16
    Suffix    []byte
}
```

5. **异步刷新**
```go
// 后台异步刷新脏页
go func() {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        m.flushDirtyNodesBatch()
    }
}()
```

### Q4: 如何处理错误？

**A**: 统一错误处理策略

```go
// 定义错误类型
var (
    ErrKeyNotFound    = errors.New("key not found")
    ErrNodeFull       = errors.New("node is full")
    ErrNodeEmpty      = errors.New("node is empty")
    ErrInvalidNode    = errors.New("invalid node")
    ErrDuplicateKey   = errors.New("duplicate key")
)

// 使用示例
func (m *DefaultBPlusTreeManager) Insert(ctx context.Context, key interface{}, value []byte) error {
    if key == nil {
        return fmt.Errorf("invalid key: %w", ErrInvalidNode)
    }

    node, err := m.findLeafNode(ctx, key)
    if err != nil {
        return fmt.Errorf("failed to find leaf node: %w", err)
    }

    // 检查重复键
    if m.keyExists(node, key) {
        return fmt.Errorf("key %v already exists: %w", key, ErrDuplicateKey)
    }

    // 插入逻辑...
    return nil
}
```

### Q5: 如何监控运行状态？

**A**: 使用统计信息和日志

```go
// 定期输出统计信息
func (m *DefaultBPlusTreeManager) LogStats() {
    logger.Infof("📊 B+Tree Stats:")
    logger.Infof("  Cache Hits: %d", m.stats.cacheHits)
    logger.Infof("  Cache Misses: %d", m.stats.cacheMisses)
    logger.Infof("  Hit Rate: %.2f%%", float64(m.stats.cacheHits)/float64(m.stats.cacheHits+m.stats.cacheMisses)*100)
    logger.Infof("  Dirty Nodes: %d", m.stats.dirtyNodes)
    logger.Infof("  Cached Nodes: %d", len(m.nodeCache))
}

// 定期调用
go func() {
    ticker := time.NewTicker(1 * time.Minute)
    for range ticker.C {
        btm.LogStats()
    }
}()
```

---

## 开发检查清单

### 代码提交前

- [ ] 所有测试通过 (`go test ./...`)
- [ ] 代码覆盖率 ≥ 85% (`go test -cover`)
- [ ] 无编译警告 (`go build`)
- [ ] 无静态检查错误 (`go vet`, `golint`)
- [ ] 所有公开方法有文档注释
- [ ] 添加了必要的日志
- [ ] 更新了相关文档

### 功能完成前

- [ ] 单元测试覆盖所有分支
- [ ] 集成测试通过
- [ ] 性能测试达标
- [ ] 代码审查通过
- [ ] 文档完整

---

## 有用的命令

```bash
# 编译项目
go build ./...

# 运行服务器
./xmysql-server -configPath=./my.ini

# 查看测试覆盖率
go test -coverprofile=coverage.out ./server/innodb/manager/...
go tool cover -html=coverage.out

# 性能分析
go test -cpuprofile=cpu.prof -memprofile=mem.prof -bench=.
go tool pprof cpu.prof

# 代码格式化
go fmt ./...

# 代码检查
go vet ./...
golint ./...
staticcheck ./...

# 依赖管理
go mod tidy
go mod vendor
```

---

## 参考资源

### 内部文档
- [实施计划](BTREE_CORE_IMPLEMENTATION_PLAN.md)
- [架构概览](BTREE_ARCHITECTURE_OVERVIEW.md)
- [执行摘要](BTREE_IMPLEMENTATION_SUMMARY.md)

### 外部资源
- [Go语言圣经](https://golang.org/doc/)
- [Effective Go](https://golang.org/doc/effective_go)
- [B+树可视化工具](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)

---

## 获取帮助

遇到问题时：
1. 查看 [常见问题](#常见问题) 章节
2. 搜索项目Issue
3. 查看代码注释和文档
4. 咨询团队成员

---

**祝开发顺利！** 🚀
