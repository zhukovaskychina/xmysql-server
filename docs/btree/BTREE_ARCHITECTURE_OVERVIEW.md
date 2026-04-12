# B+树索引架构概览

> **文档导航（2026-04）**：总索引 [BTREE_DOCUMENTATION_INDEX.md](./BTREE_DOCUMENTATION_INDEX.md)；路线图 [../development/DEVELOPMENT_ROADMAP_TASKS.md](../development/DEVELOPMENT_ROADMAP_TASKS.md)。

## 设计理念

XMySQL Server的B+树索引模块采用分层架构设计，将索引管理、B+树操作、缓存策略和存储管理解耦，实现高性能、高可靠、高可维护的数据库索引系统。

---

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                        SQL执行层                              │
│  (Executor, QueryOptimizer, ExpressionEvaluator)           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     索引管理层                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │IndexManager  │  │IndexMetadata │  │IndexStatistics│      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    B+树操作层                                 │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │ BPlusTreeManager │  │EnhancedBTreeIndex│                │
│  └────────┬─────────┘  └────────┬─────────┘                │
│           │                      │                           │
│  ┌────────▼─────────┐  ┌────────▼─────────┐                │
│  │  NodeSplitter    │  │  NodeMerger      │                │
│  └──────────────────┘  └──────────────────┘                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     缓存管理层                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  NodeCache   │  │  LRU Eviction│  │ Prefetcher   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    存储管理层                                 │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │BufferPoolManager │  │  SegmentManager  │                │
│  └────────┬─────────┘  └────────┬─────────┘                │
│           │                      │                           │
│  ┌────────▼─────────┐  ┌────────▼─────────┐                │
│  │  PageAllocator   │  │  SpaceManager    │                │
│  └──────────────────┘  └──────────────────┘                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   持久化层                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ RedoLog      │  │  UndoLog     │  │  DataFiles   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

---

## 核心组件详解

### 1. 索引管理层

#### IndexManager
**职责**: 管理所有索引的生命周期

| 方法 | 功能 | 复杂度 |
|-----|------|--------|
| `CreateIndex()` | 创建新索引 | O(1) |
| `GetIndex()` | 获取索引实例 | O(1) |
| `DropIndex()` | 删除索引 | O(N) |
| `RebuildIndex()` | 重建索引 | O(N log N) |

**数据结构**
```go
type IndexManager struct {
    mu                sync.RWMutex
    indexes           map[uint64]*Index           // 索引ID -> 索引
    btreeManager      basic.BPlusTreeManager      // B+树管理器
    bufferPoolManager *OptimizedBufferPoolManager // 缓冲池
    segmentManager    *SegmentManager             // 段管理器
    stats             *IndexManagerStats          // 统计信息
}
```

#### IndexMetadata
**职责**: 存储索引元信息

| 字段 | 类型 | 说明 |
|-----|------|------|
| IndexID | uint64 | 全局唯一索引ID |
| TableID | uint64 | 所属表ID |
| SpaceID | uint32 | 表空间ID |
| RootPageNo | uint32 | 根页面号 |
| IndexType | uint8 | 索引类型(PRIMARY/UNIQUE/NORMAL) |
| Columns | []Column | 索引列定义 |

**持久化策略**
- 存储位置: 系统表 `INFORMATION_SCHEMA.INDEXES`
- 序列化格式: Protobuf或自定义二进制
- 加载时机: 数据库启动时全量加载

---

### 2. B+树操作层

#### BPlusTreeManager
**职责**: B+树的CRUD操作和节点管理

**核心方法**
| 方法 | 功能 | 时间复杂度 |
|-----|------|-----------|
| `Search(key)` | 单点查询 | O(log N) |
| `Insert(key, value)` | 插入记录 | O(log N) |
| `Delete(key)` | 删除记录 | O(log N) |
| `RangeSearch(start, end)` | 范围查询 | O(log N + K) |
| `GetAllLeafPages()` | 获取所有叶子页 | O(M) M=叶子页数 |

**节点缓存机制**
```go
type DefaultBPlusTreeManager struct {
    nodeCache  map[uint32]*BPlusTreeNode  // 页号 -> 节点
    lastAccess map[uint32]time.Time       // LRU访问时间
    config     BPlusTreeConfig            // 配置
}
```

**缓存淘汰策略**
1. **LRU基础策略**: 淘汰最久未访问的节点
2. **脏页优先刷新**: 淘汰前先刷新脏页
3. **根节点常驻**: 根节点和热点非叶子节点不淘汰

---

#### NodeSplitter (节点分裂器)

**设计模式**: Strategy Pattern

**核心算法**
```
算法: SplitLeafNode(node)
输入: 满载叶子节点(keys ≥ maxKeys)
输出: (newPageNo, middleKey, error)

步骤:
1. 计算分裂点 splitPoint = len(keys) * splitRatio
2. 分配新页面 newPageNo
3. 创建新节点 newNode
4. 复制右半部分 newNode.Keys = node.Keys[splitPoint:]
5. 截断原节点 node.Keys = node.Keys[:splitPoint]
6. 更新链表 node.NextLeaf = newPageNo
7. 提升中间键 middleKey = newNode.Keys[0]
8. 标记脏页 node.isDirty = true, newNode.isDirty = true
9. 返回 (newPageNo, middleKey, nil)
```

**递归向上分裂**
```
算法: InsertIntoParent(leftPage, rightPage, middleKey)
输入: 分裂产生的左右页号和中间键
输出: error

步骤:
1. IF leftPage == rootPage THEN
      创建新根节点(createNewRoot)
      返回
2. 查找父节点 parent = findParentNode(leftPage)
3. 在父节点插入middleKey和rightPage
4. IF len(parent.Keys) > maxKeys THEN
      分裂父节点 (newParentPage, newMiddleKey) = SplitNonLeafNode(parent)
      递归调用 InsertIntoParent(parent.PageNum, newParentPage, newMiddleKey)
5. 返回
```

**分裂配置参数**
| 参数 | 默认值 | 范围 | 影响 |
|-----|--------|------|------|
| splitRatio | 0.5 | 0.4-0.6 | 左节点占比，0.5表示50/50分裂 |
| minKeys | degree-1 | - | 最小键数，保证树平衡 |
| maxKeys | 2*degree-1 | - | 最大键数，触发分裂阈值 |

---

#### NodeMerger (节点合并器)

**设计模式**: Strategy Pattern + Template Method

**重平衡决策树**
```
决策流程:
1. 节点键数 < minKeys?
   ├─ NO: 无需重平衡
   └─ YES: 继续
2. 查找兄弟节点
   ├─ 左兄弟富余? (keys > minKeys)
   │  └─ YES: 从左兄弟借键 → 完成
   ├─ 右兄弟富余?
   │  └─ YES: 从右兄弟借键 → 完成
   ├─ 可与左兄弟合并? (totalKeys ≤ maxKeys)
   │  └─ YES: 合并到左兄弟 → 递归向上
   └─ 可与右兄弟合并?
      └─ YES: 合并到右兄弟 → 递归向上
```

**借键操作示例**

```
场景: 从左兄弟借键

原状态:
Left Sibling:  [10, 20, 30, 40]
Current Node:  [50]
Parent Key:    45

操作步骤:
1. 移动左兄弟最后一个键: 40
2. 插入到当前节点头部: [40, 50]
3. 更新父节点键: 45 → 50

新状态:
Left Sibling:  [10, 20, 30]
Current Node:  [40, 50]
Parent Key:    50
```

**合并操作示例**

```
场景: 叶子节点合并

原状态:
Left Node:   [10, 20]
Right Node:  [30]
Parent Key:  25

操作步骤:
1. 合并: Left.Keys = [10, 20, 30]
2. 更新链表: Left.NextLeaf = Right.NextLeaf
3. 从父节点删除分隔键25
4. 回收右节点页面

新状态:
Merged Node: [10, 20, 30]
Parent:      (删除了键25)
```

---

### 3. 缓存管理层

#### NodeCache (节点缓存)

**缓存策略**: 自适应LRU + 预读

**数据流**
```
查询请求 → 检查NodeCache
   ├─ 缓存命中 → 更新访问时间 → 返回节点
   └─ 缓存未命中
       ├─ 从BufferPool加载页面
       ├─ 解析为BPlusTreeNode
       ├─ 加入NodeCache
       ├─ 检查缓存大小
       │  └─ 超限 → 触发LRU淘汰
       └─ 返回节点
```

**淘汰算法优化**
1. **分层淘汰**: 优先淘汰叶子节点，保留内部节点
2. **热点保护**: 最近5秒访问过的节点不淘汰
3. **脏页优先处理**: 淘汰前先异步刷新脏页

**预读策略**
| 触发条件 | 预读范围 | 预期收益 |
|---------|---------|---------|
| 顺序访问3个连续页 | 预读后续8页 | 减少50% I/O |
| 根节点访问 | 预读一级子节点 | 加速查询启动 |
| 范围扫描 | 预读下一叶子页 | 流畅遍历 |

---

### 4. 存储管理层

#### BufferPoolManager

**职责**: 页面缓存和I/O调度

**核心接口**
| 方法 | 功能 | 调用方 |
|-----|------|--------|
| `GetPage(spaceID, pageNo)` | 获取页面 | BPlusTreeManager |
| `AllocatePage(spaceID)` | 分配新页面 | NodeSplitter |
| `FlushPage(pageNo)` | 刷新脏页 | NodeCache |
| `FlushDirtyPages()` | 批量刷新 | 后台任务 |

**缓冲池架构**
```
┌────────────────────────────────────┐
│        BufferPool (128MB)          │
│  ┌──────┐ ┌──────┐     ┌──────┐   │
│  │Page 1│ │Page 2│ ... │Page N│   │
│  └──────┘ └──────┘     └──────┘   │
│      │         │            │      │
│      ▼         ▼            ▼      │
│  ┌────────────────────────────┐   │
│  │    Free List / LRU List    │   │
│  └────────────────────────────┘   │
└────────────────────────────────────┘
         │                 │
         ▼                 ▼
   Disk Read         Disk Write
```

---

## 关键流程详解

### 流程1: 插入操作(含分裂)

```
用户请求: INSERT INTO users VALUES (100, 'Alice', 25)

步骤1: 定位叶子节点
   BPlusTreeManager.Insert(key=100, value=...)
   ├─ 加载根节点 root = getNode(rootPage)
   ├─ 二分查找子节点索引
   ├─ 递归向下 node = getNode(child)
   └─ 到达叶子节点 leafNode

步骤2: 检查是否需要分裂
   IF len(leafNode.Keys) >= maxKeys THEN
      触发分裂
   ELSE
      直接插入

步骤3: 执行分裂(假设需要)
   NodeSplitter.SplitLeafNode(leafNode)
   ├─ 分配新页面 newPageNo = 2001
   ├─ 分裂数据 [原节点: 1-50, 新节点: 51-100]
   ├─ 更新链表指针
   └─ 返回middleKey = 51

步骤4: 向上插入
   NodeSplitter.InsertIntoParent(leafNode.PageNum, newPageNo, middleKey=51)
   ├─ 查找父节点 parent = findParentNode(leafNode.PageNum)
   ├─ 在父节点插入键51和子节点2001
   ├─ 检查父节点是否满
   └─ IF 父节点满 THEN 递归分裂父节点

步骤5: 持久化
   ├─ 标记leafNode为脏页
   ├─ 标记newNode为脏页
   ├─ 标记parent为脏页
   └─ 后台任务异步刷新脏页
```

**性能指标**
- 无分裂插入: 1ms (1次磁盘I/O)
- 含分裂插入: 10ms (3-5次磁盘I/O)

---

### 流程2: 删除操作(含合并)

```
用户请求: DELETE FROM users WHERE id = 10

步骤1: 定位叶子节点
   BPlusTreeManager.Delete(key=10)
   └─ 同插入流程，定位到叶子节点 leafNode

步骤2: 删除键值对
   ├─ 查找键索引 index = findKeyIndex(leafNode, key=10)
   ├─ 删除键 leafNode.Keys = Keys[:index] + Keys[index+1:]
   ├─ 删除记录 leafNode.Records = Records[:index] + Records[index+1:]
   └─ 标记脏页 leafNode.isDirty = true

步骤3: 检查是否需要重平衡
   IF len(leafNode.Keys) < minKeys THEN
      触发重平衡
   ELSE
      删除完成

步骤4: 重平衡决策(假设需要)
   NodeMerger.Rebalance(leafNode)
   ├─ 查找兄弟节点 (left, right) = FindSiblings(leafNode)
   ├─ IF 左兄弟富余 THEN
   │     BorrowFromLeftSibling(leafNode, left, parentKey)
   ├─ ELSE IF 右兄弟富余 THEN
   │     BorrowFromRightSibling(leafNode, right, parentKey)
   ├─ ELSE IF 可与左兄弟合并 THEN
   │     MergeLeafNodes(left, leafNode)
   │     DeleteFromParent(parent, leafNode.PageNum)
   │     递归检查父节点
   └─ ELSE
         合并失败，保持当前状态

步骤5: 树高度降低(特殊情况)
   IF 根节点只有1个子节点 THEN
      ├─ 提升子节点为新根
      ├─ 更新 rootPage = child
      └─ 树高度 - 1
```

**性能指标**
- 简单删除: 1ms
- 含借键删除: 3ms
- 含合并删除: 10ms

---

### 流程3: 范围扫描

```
用户请求: SELECT * FROM users WHERE id BETWEEN 10 AND 100

步骤1: 定位起始节点
   RangeSearch(startKey=10, endKey=100)
   ├─ 查找包含key=10的叶子节点
   └─ startNode = findLeafNode(key=10)

步骤2: 顺序遍历叶子链表
   results = []
   currentNode = startNode
   WHILE currentNode != null DO
      FOR EACH key IN currentNode.Keys DO
         IF key > endKey THEN
            RETURN results  // 提前终止
         IF key >= startKey THEN
            record = getRecord(currentNode, key)
            results.append(toRow(record))
      END FOR
      currentNode = getNode(currentNode.NextLeaf)
   END WHILE
   RETURN results

步骤3: 预读优化
   ├─ 检测到顺序访问
   ├─ 触发预读: Prefetch(currentNode.NextLeaf, 4页)
   └─ 后续访问命中缓存
```

**性能优化点**
1. **提前终止**: 遇到第一个>endKey的键立即返回
2. **批量获取**: 每次获取一批记录而非逐条
3. **预读**: 预测性加载后续页面
4. **迭代器**: 流式返回避免大结果集内存压力

**性能指标**
- 100行范围扫描: 5ms
- 1000行范围扫描: 30ms
- 10000行范围扫描: 200ms

---

## 数据结构定义

### BPlusTreeNode

```go
type BPlusTreeNode struct {
    PageNum  uint32        // 页号
    IsLeaf   bool          // 是否叶子节点
    Keys     []interface{} // 键数组
    Children []uint32      // 子节点页号(非叶子节点)
    Records  []uint32      // 记录位置(叶子节点)
    NextLeaf uint32        // 下一叶子节点(叶子链表)
    isDirty  bool          // 脏页标记
}
```

**内存布局**
```
叶子节点内存结构:
┌─────────────────────────────────────┐
│ PageNum: 1001                       │
│ IsLeaf: true                        │
│ Keys: [10, 20, 30, 40, 50]         │
│ Records: [0x1000, 0x1020, ...]     │
│ NextLeaf: 1002                      │
│ isDirty: true                       │
└─────────────────────────────────────┘

非叶子节点内存结构:
┌─────────────────────────────────────┐
│ PageNum: 2001                       │
│ IsLeaf: false                       │
│ Keys: [50, 100, 150]               │
│ Children: [1001, 1002, 1003, 1004] │
│ NextLeaf: 0                         │
│ isDirty: false                      │
└─────────────────────────────────────┘
```

---

### IndexMetadata

```go
type IndexMetadata struct {
    IndexID    uint64    // 索引ID
    TableID    uint64    // 表ID
    SpaceID    uint32    // 表空间ID
    IndexName  string    // 索引名称
    IndexType  IndexType // PRIMARY/UNIQUE/NORMAL
    Columns    []IndexColumn
    RootPageNo uint32    // 根页号
    CreateTime time.Time
    UpdateTime time.Time
    IndexState EnhancedIndexState
}

type IndexColumn struct {
    ColumnName string
    ColumnID   uint64
    Position   uint8  // 在索引中的位置
    Ascending  bool   // 升序/降序
}
```

---

## 并发控制

### 锁策略

| 操作 | 锁类型 | 锁粒度 | 持锁时间 |
|-----|--------|--------|---------|
| **查询** | 读锁 | 节点级 | 极短(μs级) |
| **插入(无分裂)** | 写锁 | 叶子节点 | 短(ms级) |
| **插入(含分裂)** | 写锁 | 叶子节点→父节点 | 中等(10ms) |
| **删除(含合并)** | 写锁 | 叶子节点→父节点 | 中等(10ms) |
| **范围扫描** | 读锁 | 多个叶子节点(顺序) | 长(100ms+) |

### 死锁避免

**核心原则**: 自底向上加锁

```
加锁顺序规则:
1. 叶子节点 → 父节点 → 祖父节点 (永远向上)
2. 同一层级: 左节点 → 右节点 (永远向右)
3. 持锁时间最小化: 获取锁 → 操作 → 立即释放
```

**示例: 分裂时的加锁顺序**
```
步骤1: 锁叶子节点 Lock(leafNode)
步骤2: 分裂叶子节点 (持有leafNode锁)
步骤3: 锁父节点 Lock(parent)
步骤4: 释放叶子节点锁 Unlock(leafNode)
步骤5: 更新父节点 (持有parent锁)
步骤6: 释放父节点锁 Unlock(parent)
```

---

## 性能优化策略

### 1. 缓存优化

| 优化项 | 实现 | 收益 |
|--------|------|------|
| 热点节点常驻 | 根节点和高层节点不淘汰 | 减少30% I/O |
| 分层LRU | 内部节点和叶子节点分开管理 | 提升20%命中率 |
| 预读 | 顺序访问触发预读 | 减少50% I/O |
| 批量刷新 | 累积脏页批量写入 | 减少70% fsync |

### 2. 内存优化

| 优化项 | 实现 | 节省内存 |
|--------|------|---------|
| 页面复用 | 删除的页面加入空闲列表 | 30% |
| 压缩键 | 公共前缀压缩 | 40% |
| 延迟加载 | 按需加载子节点 | 50% |

### 3. 磁盘I/O优化

| 优化项 | 实现 | 减少I/O |
|--------|------|---------|
| 组提交 | 多个事务合并刷新 | 80% |
| 异步刷新 | 后台线程刷新脏页 | 无阻塞 |
| 双写缓冲 | 避免部分写 | 提升可靠性 |

---

## 容错与恢复

### 崩溃恢复流程

```
数据库重启:
1. 读取Redo日志
2. 重放未提交事务的操作
3. 回滚未完成事务
4. 重建索引元数据
5. 加载热点索引到内存
6. 服务可用
```

### 数据一致性保证

| 机制 | 作用 | 实现 |
|-----|------|------|
| WAL | 写前日志 | RedoLogManager |
| MVCC | 多版本并发控制 | VersionChainManager |
| 双写缓冲 | 防止部分写 | DoubleWriteBuffer |
| 校验和 | 检测损坏 | CRC32 |

---

## 监控与诊断

### 关键指标

| 指标 | 类型 | 阈值 | 告警 |
|-----|------|------|------|
| 缓存命中率 | 性能 | > 90% | 低于80%告警 |
| 平均查询延迟 | 性能 | < 1ms | 超过5ms告警 |
| 分裂频率 | 负载 | < 100次/秒 | 超过500次/秒告警 |
| 脏页比例 | 资源 | < 70% | 超过90%告警 |
| 树高度 | 结构 | < 5 | 超过7告警 |

### 诊断工具

```sql
-- 查看索引统计
SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES 
WHERE table_id = 100;

-- 查看索引页面使用
SHOW INDEX STATS FOR TABLE users;

-- 检查索引一致性
CHECK INDEX idx_users_name;

-- 重建索引
ALTER INDEX idx_users_name REBUILD;
```

---

## 总结

本架构设计通过分层解耦、缓存优化、并发控制和容错机制，构建了一个高性能、高可靠的B+树索引系统。核心设计原则包括：

1. **分层架构**: 清晰的层次划分，便于维护和扩展
2. **缓存优先**: 多级缓存减少磁盘I/O
3. **并发友好**: 细粒度锁和无锁设计提升并发性能
4. **容错设计**: WAL日志和MVCC保证数据一致性
5. **可监控性**: 丰富的指标和诊断工具

**下一步行动**: 参考《B+树核心功能实施计划》开始编码实现。

