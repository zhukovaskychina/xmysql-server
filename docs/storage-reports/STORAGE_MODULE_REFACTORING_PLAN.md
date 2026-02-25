# Storage 模块重构计划

**创建日期**: 2025-10-29  
**目标**: 解决 Storage 模块的架构和代码质量问题  
**预计工作量**: 4-7 周  

---

## 📋 重构目标

### 主要目标

1. **统一接口定义**: 解决 3 个不同的 `IPageWrapper` 接口问题
2. **消除代码重复**: 统一 Extent 和 Page 实现
3. **清理代码库**: 删除所有备份文件
4. **优化分层**: 明确 store 和 wrapper 层的职责边界
5. **提升质量**: 提高代码可维护性和测试覆盖率

### 成功标准

- ✅ 只有 1 个 `IPageWrapper` 接口定义
- ✅ Extent 实现减少到 2 个（store 层 1 个，wrapper 层 1 个）
- ✅ 删除所有 .bak 文件
- ✅ wrapper 层代码量减少 30%
- ✅ 测试覆盖率提升到 70%+

---

## 🗺️ 重构路线图

### 阶段 1: 接口统一 (Week 1-2)

**目标**: 统一所有接口定义

#### 任务 1.1: 创建统一的 IPageWrapper 接口

**文件**: `server/innodb/storage/types/page_interface.go` (新建)

```go
package types

import (
    "github.com/zhukovaskychina/xmysql-server/server/common"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// IPageWrapper 统一的页面包装器接口
// 所有页面实现都应该实现此接口
type IPageWrapper interface {
    // ========================================
    // 基本信息
    // ========================================
    
    // GetPageID 获取页面ID
    GetPageID() uint32
    
    // GetSpaceID 获取表空间ID
    GetSpaceID() uint32
    
    // GetPageNo 获取页面号（与 GetPageID 相同）
    GetPageNo() uint32
    
    // GetPageType 获取页面类型
    GetPageType() common.PageType
    
    // ========================================
    // LSN (Log Sequence Number) 管理
    // ========================================
    
    // GetLSN 获取页面的 LSN
    GetLSN() uint64
    
    // SetLSN 设置页面的 LSN
    SetLSN(lsn uint64)
    
    // ========================================
    // 状态管理
    // ========================================
    
    // GetState 获取页面状态
    GetState() basic.PageState
    
    // SetState 设置页面状态
    SetState(state basic.PageState)
    
    // IsDirty 检查页面是否为脏页
    IsDirty() bool
    
    // MarkDirty 标记页面为脏页
    MarkDirty()
    
    // ========================================
    // 缓冲池管理
    // ========================================
    
    // Pin 固定页面（增加引用计数）
    Pin()
    
    // Unpin 取消固定页面（减少引用计数）
    Unpin()
    
    // GetPinCount 获取引用计数
    GetPinCount() int32
    
    // GetStats 获取页面统计信息
    GetStats() *basic.PageStats
    
    // ========================================
    // 序列化
    // ========================================
    
    // GetFileHeader 获取文件头（38 字节）
    GetFileHeader() []byte
    
    // GetFileTrailer 获取文件尾（8 字节）
    GetFileTrailer() []byte
    
    // ToBytes 序列化页面为字节数组
    ToBytes() ([]byte, error)
    
    // ParseFromBytes 从字节数组反序列化页面
    ParseFromBytes(data []byte) error
    
    // ========================================
    // I/O 操作
    // ========================================
    
    // Read 从磁盘或缓冲池读取页面
    Read() error
    
    // Write 将页面写入缓冲池和磁盘
    Write() error
    
    // Flush 强制刷新页面到磁盘
    Flush() error
}

// IPageFactory 页面工厂接口
type IPageFactory interface {
    // CreatePage 创建指定类型的页面
    CreatePage(pageType common.PageType, spaceID, pageNo uint32) (IPageWrapper, error)
    
    // LoadPage 从字节数组加载页面
    LoadPage(data []byte) (IPageWrapper, error)
}
```

**工作量**: 1 天

---

#### 任务 1.2: 迁移现有实现

**步骤**:

1. **更新 wrapper/wrapper.go**
   ```go
   // 删除旧接口定义
   // type IPageWrapper interface { ... }
   
   // 导入新接口
   import "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/types"
   
   // 使用新接口
   type PageManager struct {
       pages map[uint32]types.IPageWrapper
   }
   ```

2. **更新 wrapper/page/page_factory.go**
   ```go
   // 删除旧接口定义
   // type IPageWrapper interface { ... }
   
   // 导入新接口
   import "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/types"
   
   // 实现新接口
   type PageFactory struct{}
   
   func (f *PageFactory) CreatePage(pageType common.PageType, spaceID, pageNo uint32) (types.IPageWrapper, error) {
       // ...
   }
   ```

3. **更新 wrapper/types/page_wrapper.go**
   ```go
   // 删除旧接口定义和 BasePageWrapper
   // 移动到 types/page_interface.go
   ```

**工作量**: 2-3 天

---

#### 任务 1.3: 更新所有引用

**影响的文件** (预估 30+ 个):
- `server/innodb/storage/wrapper/page/*.go`
- `server/innodb/storage/wrapper/system/*.go`
- `server/innodb/manager/*.go`
- `server/innodb/engine/*.go`

**工具辅助**:
```bash
# 查找所有引用
grep -r "IPageWrapper" server/innodb/ | wc -l

# 批量替换
find server/innodb -name "*.go" -exec sed -i '' 's/wrapper\.IPageWrapper/types.IPageWrapper/g' {} \;
```

**工作量**: 2-3 天

---

### 阶段 2: Extent 重构 (Week 3)

**目标**: 统一 Extent 实现

#### 任务 2.1: 保留 store 层实现

**文件**: `server/innodb/storage/store/extents/extent.go` (保留)

**职责**:
- 底层数据结构（位图）
- 序列化/反序列化
- 页面分配/释放的位操作

**不变**:
```go
type ExtentEntry struct {
    SegmentID   uint64
    State       uint8
    PageBitmap  [16]byte
    PageCount   uint8
    FirstPageNo uint32
}

func (e *ExtentEntry) AllocatePage(pageOffset uint8) error
func (e *ExtentEntry) FreePage(pageOffset uint8) error
func (e *ExtentEntry) IsPageFree(pageOffset uint8) bool
```

---

#### 任务 2.2: 重构 wrapper 层实现

**文件**: `server/innodb/storage/wrapper/extent/extent.go` (重构)

**新设计**:

```go
package extent

import (
    "sync"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/extents"
)

// Extent wrapper 层的 Extent 实现
// 依赖 store 层的 ExtentEntry，只负责并发控制和统计
type Extent struct {
    mu sync.RWMutex
    
    // 底层数据结构（来自 store 层）
    entry *extents.ExtentEntry
    
    // wrapper 层的职责
    stats    basic.ExtentStats
    purpose  basic.ExtentPurpose
    spaceID  uint32
}

// NewExtent 创建新的 Extent
func NewExtent(extentID, spaceID, startPage uint32, purpose basic.ExtentPurpose) *Extent {
    return &Extent{
        entry:   extents.NewExtentEntry(startPage),
        purpose: purpose,
        spaceID: spaceID,
        stats:   basic.ExtentStats{},
    }
}

// AllocatePage 分配页面（wrapper 层方法）
func (e *Extent) AllocatePage() (uint32, error) {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    // 1. 查找空闲页面
    var pageOffset uint8
    found := false
    for i := uint8(0); i < 64; i++ {
        if e.entry.IsPageFree(i) {
            pageOffset = i
            found = true
            break
        }
    }
    
    if !found {
        return 0, ErrExtentFull
    }
    
    // 2. 委托给 store 层分配
    if err := e.entry.AllocatePage(pageOffset); err != nil {
        return 0, err
    }
    
    // 3. wrapper 层只负责统计
    e.stats.TotalPages++
    e.stats.FreePages--
    e.stats.LastAllocated = time.Now().UnixNano()
    
    return e.entry.FirstPageNo + uint32(pageOffset), nil
}

// FreePage 释放页面
func (e *Extent) FreePage(pageNo uint32) error {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    // 计算页面偏移
    pageOffset := uint8(pageNo - e.entry.FirstPageNo)
    
    // 委托给 store 层释放
    if err := e.entry.FreePage(pageOffset); err != nil {
        return err
    }
    
    // 更新统计
    e.stats.TotalPages--
    e.stats.FreePages++
    
    return nil
}

// 实现 basic.Extent 接口
func (e *Extent) GetID() uint32 {
    return e.entry.FirstPageNo / 64
}

func (e *Extent) GetState() basic.ExtentState {
    e.mu.RLock()
    defer e.mu.RUnlock()
    
    switch e.entry.State {
    case extents.EXTENT_FREE:
        return basic.ExtentStateFree
    case extents.EXTENT_PARTIAL:
        return basic.ExtentStatePartial
    case extents.EXTENT_FULL:
        return basic.ExtentStateFull
    default:
        return basic.ExtentStateFree
    }
}

func (e *Extent) GetStats() basic.ExtentStats {
    e.mu.RLock()
    defer e.mu.RUnlock()
    return e.stats
}

// ... 其他方法
```

**工作量**: 2-3 天

---

#### 任务 2.3: 删除重复实现

**删除文件**:
- `server/innodb/storage/wrapper/space/extent.go` (ExtentImpl)

**迁移逻辑**:
- 将 `ExtentImpl` 的功能合并到 `wrapper/extent/extent.go`
- 更新 `IBDSpace` 使用新的 `Extent`

**工作量**: 1-2 天

---

### 阶段 3: 清理备份文件 (Week 3)

**目标**: 删除所有 .bak 文件

#### 任务 3.1: 检查备份文件

```bash
# 列出所有备份文件
find server/innodb/storage/wrapper/system -name "*.bak*"

# 对比差异
for file in server/innodb/storage/wrapper/system/*.bak*; do
    original="${file%.bak*}"
    if [ -f "$original" ]; then
        echo "Comparing $original with $file"
        diff "$original" "$file"
    fi
done
```

#### 任务 3.2: 删除备份文件

```bash
# 确认无差异后删除
rm server/innodb/storage/wrapper/system/*.bak*

# 提交到 Git
git add -A
git commit -m "chore: remove backup files in storage/wrapper/system"
```

**工作量**: 0.5 天

---

### 阶段 4: Page 实现简化 (Week 4-5)

**目标**: 减少 Page 实现重复

#### 任务 4.1: 定义 Page 分层原则

**原则**:

1. **store 层** (`server/innodb/storage/store/pages/`)
   - 只负责数据格式定义
   - 只负责序列化/反序列化
   - 不包含业务逻辑
   - 不包含并发控制

2. **wrapper 层** (`server/innodb/storage/wrapper/page/`)
   - 依赖 store 层的数据结构
   - 负责并发控制
   - 负责缓存管理
   - 负责统计信息
   - 负责业务逻辑

---

#### 任务 4.2: 重构 IndexPage

**store 层** (`store/pages/index_page.go`):

```go
package pages

// IndexPage store 层的索引页面
// 只负责数据格式和序列化
type IndexPage struct {
    AbstractPage
    
    // 页面头部
    PageHeader      PageHeader
    IndexPageHeader IndexPageHeader
    
    // 页面内容
    InfimumSupremum []byte
    UserRecords     []byte
    PageDirectory   []byte
    FreeSpace       []byte
}

// GetSerializeBytes 序列化
func (ip *IndexPage) GetSerializeBytes() []byte {
    // 只负责序列化
    buf := make([]byte, 16384)
    // ... 序列化逻辑
    return buf
}

// ParseFromBytes 反序列化
func (ip *IndexPage) ParseFromBytes(data []byte) error {
    // 只负责反序列化
    // ... 反序列化逻辑
    return nil
}
```

**wrapper 层** (`wrapper/page/index_page_wrapper.go`):

```go
package page

import (
    "sync"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/pages"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/types"
)

// IndexPageWrapper wrapper 层的索引页面
// 依赖 store 层，负责并发控制和业务逻辑
type IndexPageWrapper struct {
    sync.RWMutex
    
    // 底层数据结构（来自 store 层）
    storePage *pages.IndexPage
    
    // wrapper 层的职责
    stats    *basic.PageStats
    pinCount int32
    dirty    bool
    lsn      uint64
    state    basic.PageState
    
    // 缓存
    keySpace map[string]uint16
    entries  []*types.IndexEntry
}

// NewIndexPageWrapper 创建索引页面 wrapper
func NewIndexPageWrapper(spaceID, pageNo uint32) *IndexPageWrapper {
    return &IndexPageWrapper{
        storePage: pages.NewIndexPage(spaceID, pageNo),
        stats:     &basic.PageStats{},
        keySpace:  make(map[string]uint16),
    }
}

// InsertKey 插入键（业务逻辑）
func (ipw *IndexPageWrapper) InsertKey(key types.Key, pageNo uint32, rowID uint64) error {
    ipw.Lock()
    defer ipw.Unlock()
    
    // 业务逻辑
    // ...
    
    // 标记为脏页
    ipw.dirty = true
    ipw.stats.Modifications++
    
    return nil
}

// 实现 types.IPageWrapper 接口
func (ipw *IndexPageWrapper) GetPageID() uint32 {
    return ipw.storePage.FileHeader.GetCurrentPageOffset()
}

func (ipw *IndexPageWrapper) ToBytes() ([]byte, error) {
    ipw.RLock()
    defer ipw.RUnlock()
    
    // 委托给 store 层序列化
    return ipw.storePage.GetSerializeBytes(), nil
}

// ... 其他方法
```

**工作量**: 3-4 天

---

#### 任务 4.3: 应用到其他页面类型

**需要重构的页面类型**:
1. FSPPage (表空间头页面)
2. INodePage (INode 页面)
3. BlobPage (BLOB 页面)
4. CompressedPage (压缩页面)
5. EncryptedPage (加密页面)
6. ... (其他页面类型)

**工作量**: 5-7 天

---

### 阶段 5: 调整 MVCC 位置 (Week 6)

**目标**: 将 MVCC 从 storage 层移到 transaction 层

#### 任务 5.1: 创建 transaction/mvcc 包

**新建目录**: `server/innodb/transaction/mvcc/`

**迁移文件**:
```bash
# 从 storage/store/mvcc 迁移
mv server/innodb/storage/store/mvcc/mvcc.go server/innodb/transaction/mvcc/
mv server/innodb/storage/store/mvcc/read_view.go server/innodb/transaction/mvcc/
mv server/innodb/storage/store/mvcc/trx.go server/innodb/transaction/mvcc/
mv server/innodb/storage/store/mvcc/version_chain.go server/innodb/transaction/mvcc/
```

**保留在 storage 层**:
- `storage/wrapper/mvcc/mvcc_page.go` (页面级 MVCC)

**工作量**: 2-3 天

---

### 阶段 6: 测试和文档 (Week 7)

**目标**: 完善测试和文档

#### 任务 6.1: 编写单元测试

**测试覆盖**:
- `types/page_interface_test.go` - 接口测试
- `extent/extent_test.go` - Extent 测试
- `page/*_test.go` - 各种页面类型测试

**目标覆盖率**: 70%+

**工作量**: 3-4 天

---

#### 任务 6.2: 更新文档

**文档列表**:
1. `docs/STORAGE_ARCHITECTURE.md` - 架构文档
2. `docs/STORAGE_API.md` - API 文档
3. `docs/STORAGE_MIGRATION_GUIDE.md` - 迁移指南

**工作量**: 2-3 天

---

## 📊 工作量估算

| 阶段 | 任务 | 工作量 | 依赖 |
|------|------|--------|------|
| **阶段 1** | 接口统一 | 5-7 天 | 无 |
| **阶段 2** | Extent 重构 | 4-6 天 | 阶段 1 |
| **阶段 3** | 清理备份文件 | 0.5 天 | 无 |
| **阶段 4** | Page 实现简化 | 8-11 天 | 阶段 1 |
| **阶段 5** | 调整 MVCC 位置 | 2-3 天 | 无 |
| **阶段 6** | 测试和文档 | 5-7 天 | 阶段 1-5 |
| **总计** | | **24.5-34.5 天** | |

**预计工作量**: **5-7 周**

---

## ✅ 验收标准

### 代码质量

- [ ] 只有 1 个 `IPageWrapper` 接口定义
- [ ] Extent 实现减少到 2 个
- [ ] 删除所有 .bak 文件
- [ ] wrapper 层代码量减少 30%
- [ ] 代码重复率 < 10%

### 测试覆盖

- [ ] 单元测试覆盖率 > 70%
- [ ] 所有核心功能有测试
- [ ] 所有测试通过

### 文档完善

- [ ] 架构文档更新
- [ ] API 文档完整
- [ ] 迁移指南清晰

### 性能

- [ ] 重构后性能不下降
- [ ] 内存使用不增加

---

## 🚀 执行建议

### 优先级

1. **P0**: 阶段 1 (接口统一) + 阶段 3 (清理备份文件)
2. **P1**: 阶段 2 (Extent 重构) + 阶段 4 (Page 简化)
3. **P2**: 阶段 5 (MVCC 调整) + 阶段 6 (测试文档)

### 风险控制

1. **分支管理**: 在独立分支进行重构
2. **增量提交**: 每完成一个任务就提交
3. **回归测试**: 每个阶段完成后运行全量测试
4. **代码审查**: 每个 PR 都需要审查

---

**创建者**: Augment Agent  
**创建日期**: 2025-10-29  
**状态**: 待执行

