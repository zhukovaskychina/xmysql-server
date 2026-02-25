# Storage包优化任务清单

## 📋 任务概述

基于对`server/innodb/storage`包的分析，制定以下优化任务清单。

**总预计时间**: 4-6周  
**优先级**: P0 > P1 > P2 > P3

---

## 🔥 P0任务：紧急修复（预计1周）

### P0.1 实现Extent碎片整理

**文件**: `server/innodb/storage/wrapper/extent/extent.go`  
**问题**: Defragment方法只是TODO占位符  
**预计时间**: 0.5天

**任务**:
- [ ] 实现BaseExtent.Defragment方法
- [ ] 实现UnifiedExtent.Defragment方法
- [ ] 添加单元测试
- [ ] 与manager层的DefragmentSpace集成测试

**实现要点**:
```go
func (be *BaseExtent) Defragment() error {
    be.mu.Lock()
    defer be.mu.Unlock()
    
    // 1. 重建页面列表（按页号排序）
    // 2. 重新评估extent状态
    // 3. 更新统计信息
    // 4. 返回成功
}
```

---

### P0.2 统一IPageWrapper接口定义

**文件**: 
- `server/innodb/storage/wrapper/types/page_wrapper.go`
- `server/innodb/storage/wrapper/page/page_wrapper.go`
- `server/innodb/storage/wrapper/wrapper.go`

**问题**: 存在3个不兼容的接口定义  
**预计时间**: 1天

**任务**:
- [ ] 设计统一的IPageWrapper接口
- [ ] 在types包中定义标准接口
- [ ] 更新所有Page实现以符合新接口
- [ ] 废弃旧接口定义
- [ ] 更新所有引用代码
- [ ] 运行所有测试确保兼容性

**统一接口设计**:
```go
// server/innodb/storage/wrapper/types/page_wrapper.go
type IPageWrapper interface {
    // 基本信息
    GetID() uint32
    GetSpaceID() uint32
    GetPageNo() uint32
    GetPageType() common.PageType
    GetSize() uint32
    
    // 内容访问
    GetContent() []byte
    SetContent([]byte) error
    
    // 文件头尾
    GetFileHeader() *pages.FileHeader
    GetFileTrailer() *pages.FileTrailer
    
    // 序列化
    ToBytes() []byte
    ParseFromBytes([]byte) error
    
    // Buffer Pool支持
    GetBufferPage() *buffer_pool.BufferPage
    SetBufferPage(*buffer_pool.BufferPage)
    
    // 持久化
    Read() error
    Write() error
    Flush() error
    
    // 状态管理
    IsDirty() bool
    SetDirty(bool)
    GetState() basic.PageState
    
    // 生命周期
    Init() error
    Release() error
    
    // 并发控制
    Lock()
    Unlock()
    RLock()
    RUnlock()
}
```

---

### P0.3 修复并发安全问题

**文件**: 多个文件  
**问题**: 不可变字段加锁、锁粒度过粗  
**预计时间**: 1.5天

**任务**:
- [ ] 识别所有不可变字段（id, spaceID, type等）
- [ ] 移除不可变字段的锁保护
- [ ] 使用atomic操作替代简单计数器
- [ ] 审查所有锁的获取顺序
- [ ] 添加死锁检测测试

**优化示例**:
```go
// 优化前
func (be *BaseExtent) GetID() uint32 {
    be.mu.RLock()
    defer be.mu.RUnlock()
    return be.header.ExtentID  // 不可变字段
}

// 优化后
func (be *BaseExtent) GetID() uint32 {
    return be.id  // 直接返回，无需锁
}
```

---

## 🔴 P1任务：重要重构（预计2-3周）

### P1.1 废弃BaseExtent，统一到UnifiedExtent

**文件**: 
- `server/innodb/storage/wrapper/extent/extent.go`
- `server/innodb/storage/wrapper/extent/unified_extent.go`

**问题**: 两个Extent实现，代码重复  
**预计时间**: 3天

**任务**:
- [ ] 完善UnifiedExtent实现所有方法
- [ ] 创建BaseExtent到UnifiedExtent的迁移指南
- [ ] 更新manager层使用UnifiedExtent
- [ ] 更新所有测试
- [ ] 标记BaseExtent为Deprecated（已完成）
- [ ] 在下一个版本中删除BaseExtent

**迁移示例**:
```go
// 旧代码
ext := extent.NewBaseExtent(spaceID, extentID, extType)

// 新代码
ext := extent.NewUnifiedExtent(extentID, spaceID, startPage, extType, purpose)
```

---

### P1.2 统一Page实现到UnifiedPage

**文件**: 
- `server/innodb/storage/wrapper/types/unified_page.go`
- `server/innodb/storage/wrapper/page/*.go`
- `server/innodb/storage/store/pages/page.go`

**问题**: 5个不同的Page基类实现  
**预计时间**: 5天

**任务**:
- [ ] 完善UnifiedPage实现
- [ ] 实现所有必需的接口方法
- [ ] 创建适配器层兼容旧代码
- [ ] 迁移IndexPage到UnifiedPage
- [ ] 迁移FSPPage到UnifiedPage
- [ ] 迁移其他Page类型
- [ ] 废弃旧Page实现
- [ ] 更新所有测试

**UnifiedPage完善清单**:
```go
type UnifiedPage struct {
    // 已有字段
    mu       sync.RWMutex
    header   FileHeader
    body     []byte
    trailer  FileTrailer
    spaceID  uint32
    pageNo   uint32
    pageType common.PageType
    
    // 需要添加
    bufferPage *buffer_pool.BufferPage
    stats      basic.PageStats
    dirty      bool
    state      basic.PageState
}

// 需要实现的方法
func (up *UnifiedPage) GetBufferPage() *buffer_pool.BufferPage
func (up *UnifiedPage) SetBufferPage(bp *buffer_pool.BufferPage)
func (up *UnifiedPage) Read() error
func (up *UnifiedPage) Write() error
func (up *UnifiedPage) Flush() error
func (up *UnifiedPage) Init() error
func (up *UnifiedPage) Release() error
```

---

### P1.3 优化FileSpaceHeader内存布局

**文件**: `server/innodb/storage/store/pages/fsp_hrd_page.go`  
**问题**: 多个小字节数组分配，内存碎片化  
**预计时间**: 2天

**任务**:
- [ ] 重新设计FileSpaceHeader使用固定数组
- [ ] 实现getter/setter方法
- [ ] 优化GetSerializeBytes方法
- [ ] 更新所有使用代码
- [ ] 性能测试对比

**优化设计**:
```go
type FileSpaceHeader struct {
    data [112]byte  // 固定大小，栈分配
}

// 字段偏移量常量
const (
    FSH_SPACE_ID_OFFSET = 0
    FSH_NOT_USED_OFFSET = 4
    FSH_SIZE_OFFSET = 8
    FSH_FREE_LIMIT_OFFSET = 12
    // ...
)

func (fsh *FileSpaceHeader) GetSpaceID() uint32 {
    return binary.BigEndian.Uint32(fsh.data[FSH_SPACE_ID_OFFSET:])
}

func (fsh *FileSpaceHeader) SetSpaceID(id uint32) {
    binary.BigEndian.PutUint32(fsh.data[FSH_SPACE_ID_OFFSET:], id)
}

func (fsh *FileSpaceHeader) GetSerializeBytes() []byte {
    return fsh.data[:]  // 直接返回，无需分配
}
```

---

## 🟡 P2任务：性能优化（预计1-2周）

### P2.1 实现Extent位图优化

**文件**: `server/innodb/storage/wrapper/extent/extent.go`  
**问题**: 使用map跟踪页面，内存开销大  
**预计时间**: 2天

**任务**:
- [ ] 使用位图替代map[uint32]bool
- [ ] 实现位图操作方法
- [ ] 性能基准测试
- [ ] 内存使用对比测试

**优化实现**:
```go
type BaseExtent struct {
    bitmap [8]uint64  // 64位 × 8 = 512位，支持64页
}

func (be *BaseExtent) IsPageAllocated(pageNo uint32) bool {
    offset := pageNo - be.header.FirstPage
    wordIdx := offset / 64
    bitIdx := offset % 64
    return (be.bitmap[wordIdx] & (1 << bitIdx)) != 0
}

func (be *BaseExtent) SetPageAllocated(pageNo uint32) {
    offset := pageNo - be.header.FirstPage
    wordIdx := offset / 64
    bitIdx := offset % 64
    be.bitmap[wordIdx] |= (1 << bitIdx)
}
```

---

### P2.2 实现BitmapManager分段锁

**文件**: `server/innodb/storage/wrapper/space/bitmap_manager.go`  
**问题**: 全局锁，并发性能差  
**预计时间**: 2天

**任务**:
- [ ] 设计分段锁方案（16个segment）
- [ ] 实现分段锁版本
- [ ] 并发性能测试
- [ ] 与旧版本对比

**分段锁设计**:
```go
const SegmentCount = 16

type BitmapManager struct {
    segments [SegmentCount]struct {
        mu     sync.RWMutex
        bitmap []uint64
    }
    size uint32
}

func (bm *BitmapManager) Set(pos uint32) error {
    segIdx := (pos / BitsPerWord) % SegmentCount
    seg := &bm.segments[segIdx]
    
    seg.mu.Lock()
    defer seg.mu.Unlock()
    
    // 只锁一个segment
    wordIdx := (pos / BitsPerWord) / SegmentCount
    bitIdx := pos % BitsPerWord
    seg.bitmap[wordIdx] |= (1 << bitIdx)
    
    return nil
}
```

---

### P2.3 优化Page内容访问

**文件**: `server/innodb/storage/wrapper/page/base.go`  
**问题**: GetContent返回内部切片，可能被修改  
**预计时间**: 1天

**任务**:
- [ ] 实现安全的GetContent（返回副本）
- [ ] 添加ReadContent方法（回调模式）
- [ ] 更新所有调用代码
- [ ] 性能测试

**优化实现**:
```go
// 方案1：返回副本（安全但有开销）
func (bp *BasePage) GetContent() []byte {
    bp.RLock()
    defer bp.RUnlock()
    result := make([]byte, len(bp.content))
    copy(result, bp.content)
    return result
}

// 方案2：回调模式（高性能）
func (bp *BasePage) ReadContent(fn func([]byte)) {
    bp.RLock()
    defer bp.RUnlock()
    fn(bp.content)  // 在锁保护下访问
}

// 使用示例
page.ReadContent(func(content []byte) {
    // 直接访问content，无需复制
    checksum := calculateChecksum(content)
})
```

---

## 🟢 P3任务：架构改进（预计1-2月）

### P3.1 重新设计wrapper/store分层

**预计时间**: 1周

**任务**:
- [ ] 明确wrapper和store的职责边界
- [ ] store层只负责序列化/反序列化
- [ ] wrapper层负责业务逻辑
- [ ] 减少层次嵌套
- [ ] 编写架构文档

---

### P3.2 统一命名和编码规范

**预计时间**: 1周

**任务**:
- [ ] 制定命名规范文档
- [ ] 统一接口命名（IXxx vs Xxx）
- [ ] 统一方法命名（GetXxx vs Xxx）
- [ ] 统一错误处理模式
- [ ] 代码审查和重构

---

### P3.3 完善文档和测试

**预计时间**: 2周

**任务**:
- [ ] 为所有公共接口添加文档注释
- [ ] 编写架构设计文档
- [ ] 编写使用指南
- [ ] 提高测试覆盖率到80%+
- [ ] 添加性能基准测试
- [ ] 添加集成测试

---

## 📊 任务优先级矩阵

| 任务 | 优先级 | 影响 | 难度 | 预计时间 |
|------|--------|------|------|---------|
| P0.1 Extent碎片整理 | P0 | 高 | 低 | 0.5天 |
| P0.2 统一接口定义 | P0 | 高 | 中 | 1天 |
| P0.3 并发安全修复 | P0 | 高 | 中 | 1.5天 |
| P1.1 废弃BaseExtent | P1 | 高 | 中 | 3天 |
| P1.2 统一Page实现 | P1 | 高 | 高 | 5天 |
| P1.3 优化FSPHeader | P1 | 中 | 低 | 2天 |
| P2.1 Extent位图优化 | P2 | 中 | 中 | 2天 |
| P2.2 分段锁优化 | P2 | 中 | 中 | 2天 |
| P2.3 Page访问优化 | P2 | 低 | 低 | 1天 |
| P3.1 架构重新设计 | P3 | 高 | 高 | 1周 |
| P3.2 规范统一 | P3 | 中 | 中 | 1周 |
| P3.3 文档和测试 | P3 | 中 | 中 | 2周 |

---

## 🎯 实施建议

### 第1周：P0任务
- Day 1-2: P0.2 统一接口定义
- Day 3-4: P0.3 并发安全修复
- Day 5: P0.1 Extent碎片整理

### 第2-3周：P1任务
- Week 2: P1.1 废弃BaseExtent + P1.3 优化FSPHeader
- Week 3: P1.2 统一Page实现

### 第4周：P2任务
- Day 1-2: P2.1 Extent位图优化
- Day 3-4: P2.2 分段锁优化
- Day 5: P2.3 Page访问优化

### 第5-8周：P3任务（可选）
- Week 5: P3.1 架构重新设计
- Week 6: P3.2 规范统一
- Week 7-8: P3.3 文档和测试

---

## 📈 预期收益

### 代码质量
- 减少代码重复：~1500行
- 提高可维护性：统一接口和实现
- 降低bug风险：减少不一致

### 性能提升
- 内存使用：减少20-30%
- 并发性能：提升30-50%
- GC压力：减少40%

### 开发效率
- 新功能开发：提速30%
- Bug修复：提速50%
- 代码审查：提速40%

---

## ✅ 完成标准

每个任务完成需要满足：
- [ ] 代码实现完成
- [ ] 单元测试通过
- [ ] 集成测试通过
- [ ] 代码审查通过
- [ ] 文档更新完成
- [ ] 性能测试通过（如适用）

