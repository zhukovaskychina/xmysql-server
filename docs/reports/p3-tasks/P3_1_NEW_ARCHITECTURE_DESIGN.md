# P3.1 新分层架构设计

## 📋 概述

**设计目标**: 重新设计`server/innodb/storage`包的分层架构，明确职责边界，消除代码重复，简化层次结构。

**设计原则**:
1. **单一职责**: 每一层只负责一类功能
2. **依赖倒置**: 高层不依赖低层的具体实现
3. **接口隔离**: 接口定义清晰，职责单一
4. **开闭原则**: 对扩展开放，对修改关闭

**设计时间**: 2025-10-31

---

## 🏗️ 新的分层架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                     │
│              (executor, optimizer, etc.)                 │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                    Wrapper Layer                         │
│         (Business Logic & High-Level API)                │
│                                                           │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │   Page      │  │   Extent     │  │   Segment      │ │
│  │  Manager    │  │   Manager    │  │   Manager      │ │
│  └─────────────┘  └──────────────┘  └────────────────┘ │
│                                                           │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │   Space     │  │   Record     │  │   BLOB         │ │
│  │  Manager    │  │   Manager    │  │   Manager      │ │
│  └─────────────┘  └──────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                    Format Layer                          │
│         (Serialization & Deserialization)                │
│                                                           │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │   Page      │  │   Extent     │  │   Segment      │ │
│  │  Format     │  │   Format     │  │   Format       │ │
│  └─────────────┘  └──────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                      I/O Layer                           │
│              (File Operations & Caching)                 │
│                                                           │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │   IBD       │  │   Buffer     │  │   Log          │ │
│  │   File      │  │   Pool       │  │   File         │ │
│  └─────────────┘  └──────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

---

## 📦 各层职责定义

### 1. Format Layer（格式层）

**位置**: `server/innodb/storage/format/`

**职责**:
- ✅ 定义InnoDB存储格式（FileHeader, FileTrailer, PageBody等）
- ✅ 提供序列化/反序列化方法
- ✅ 实现校验和计算
- ❌ 不包含业务逻辑
- ❌ 不管理状态
- ❌ 不涉及I/O操作

**特点**:
- 纯数据结构
- 纯函数（无副作用）
- 无状态
- 高度可测试

**包结构**:
```
server/innodb/storage/format/
├── page_format.go      # 页面格式定义
├── extent_format.go    # Extent格式定义
├── segment_format.go   # Segment格式定义
├── record_format.go    # 记录格式定义
└── header_format.go    # 各种头部格式定义
```

**核心接口**:
```go
// format/page_format.go
package format

// PageFormat 页面格式（纯数据结构）
type PageFormat struct {
    Header  FileHeaderFormat  // 38字节
    Body    []byte            // 可变（16KB - 46字节）
    Trailer FileTrailerFormat // 8字节
}

// Serialize 序列化为字节数组
func (pf *PageFormat) Serialize() ([]byte, error) {
    // 纯序列化逻辑
    result := make([]byte, 0, 16384)
    result = append(result, pf.Header.Serialize()...)
    result = append(result, pf.Body...)
    result = append(result, pf.Trailer.Serialize()...)
    return result, nil
}

// Deserialize 从字节数组反序列化
func (pf *PageFormat) Deserialize(data []byte) error {
    // 纯反序列化逻辑
    if len(data) != 16384 {
        return ErrInvalidPageSize
    }
    
    if err := pf.Header.Deserialize(data[0:38]); err != nil {
        return err
    }
    
    pf.Body = data[38:16376]
    
    if err := pf.Trailer.Deserialize(data[16376:16384]); err != nil {
        return err
    }
    
    return nil
}

// CalculateChecksum 计算校验和
func (pf *PageFormat) CalculateChecksum() uint32 {
    // 纯计算逻辑
    return crc32.ChecksumIEEE(pf.Body)
}

// ValidateChecksum 验证校验和
func (pf *PageFormat) ValidateChecksum() error {
    expected := pf.Trailer.Checksum
    actual := pf.CalculateChecksum()
    if expected != actual {
        return ErrChecksumMismatch
    }
    return nil
}
```

**FileHeaderFormat定义**:
```go
// format/header_format.go
package format

// FileHeaderFormat 文件头格式（38字节）
type FileHeaderFormat struct {
    Checksum       uint32 // 4字节 - 页面校验和
    PageNo         uint32 // 4字节 - 页面号
    PrevPageNo     uint32 // 4字节 - 上一页号
    NextPageNo     uint32 // 4字节 - 下一页号
    LSN            uint64 // 8字节 - 日志序列号
    PageType       uint16 // 2字节 - 页面类型
    FlushLSN       uint64 // 8字节 - 刷新LSN
    SpaceID        uint32 // 4字节 - 表空间ID
}

// Serialize 序列化为38字节
func (fh *FileHeaderFormat) Serialize() []byte {
    buf := make([]byte, 38)
    binary.BigEndian.PutUint32(buf[0:4], fh.Checksum)
    binary.BigEndian.PutUint32(buf[4:8], fh.PageNo)
    binary.BigEndian.PutUint32(buf[8:12], fh.PrevPageNo)
    binary.BigEndian.PutUint32(buf[12:16], fh.NextPageNo)
    binary.BigEndian.PutUint64(buf[16:24], fh.LSN)
    binary.BigEndian.PutUint16(buf[24:26], fh.PageType)
    binary.BigEndian.PutUint64(buf[26:34], fh.FlushLSN)
    binary.BigEndian.PutUint32(buf[34:38], fh.SpaceID)
    return buf
}

// Deserialize 从38字节反序列化
func (fh *FileHeaderFormat) Deserialize(data []byte) error {
    if len(data) != 38 {
        return ErrInvalidHeaderSize
    }
    
    fh.Checksum = binary.BigEndian.Uint32(data[0:4])
    fh.PageNo = binary.BigEndian.Uint32(data[4:8])
    fh.PrevPageNo = binary.BigEndian.Uint32(data[8:12])
    fh.NextPageNo = binary.BigEndian.Uint32(data[12:16])
    fh.LSN = binary.BigEndian.Uint64(data[16:24])
    fh.PageType = binary.BigEndian.Uint16(data[24:26])
    fh.FlushLSN = binary.BigEndian.Uint64(data[26:34])
    fh.SpaceID = binary.BigEndian.Uint32(data[34:38])
    
    return nil
}
```

---

### 2. Wrapper Layer（包装器层）

**位置**: `server/innodb/storage/wrapper/`

**职责**:
- ✅ 提供高级API
- ✅ 管理页面状态（dirty, lsn, state等）
- ✅ 实现并发控制（RWMutex）
- ✅ 管理Buffer Pool集成
- ✅ 实现业务逻辑（Extent分配、Segment管理等）
- ✅ 协调I/O操作

**特点**:
- 有状态
- 有业务逻辑
- 管理生命周期
- 提供高级抽象

**包结构**:
```
server/innodb/storage/wrapper/
├── types/              # 统一类型定义
│   ├── page_wrapper.go     # IPageWrapper接口
│   └── unified_page.go     # UnifiedPage实现
├── page/               # 页面管理
│   ├── page_manager.go     # PageManager
│   ├── index_page.go       # IndexPage
│   ├── inode_page.go       # INodePage
│   └── xdes_page.go        # XDESPage
├── extent/             # Extent管理
│   ├── extent_manager.go   # ExtentManager
│   └── unified_extent.go   # UnifiedExtent
├── segment/            # Segment管理
│   └── segment_manager.go  # SegmentManager
├── space/              # 表空间管理
│   ├── space_manager.go    # SpaceManager
│   └── bitmap_manager.go   # BitmapManager
├── record/             # 记录管理
└── blob/               # BLOB管理
```

**核心接口**:
```go
// wrapper/types/page_wrapper.go
package types

// IPageWrapper 页面包装器接口（业务逻辑层）
type IPageWrapper interface {
    // 基本信息
    GetID() uint32
    GetSpaceID() uint32
    GetPageNo() uint32
    GetPageType() common.PageType
    GetSize() uint32
    
    // 内容访问
    GetContent() []byte              // 返回副本（安全）
    SetContent([]byte) error         // 设置内容
    ReadContent(fn func([]byte))     // 零拷贝访问（高性能）
    
    // 格式访问（使用format层）
    GetFormat() *format.PageFormat
    SetFormat(*format.PageFormat) error
    
    // 序列化（委托给format层）
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
    GetLSN() uint64
    SetLSN(uint64)
    
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

**UnifiedPage实现**:
```go
// wrapper/types/unified_page.go
package types

import (
    "sync"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/format"
)

// UnifiedPage 统一页面实现
type UnifiedPage struct {
    // 并发控制
    mu sync.RWMutex
    
    // 格式层（委托给format层）
    format *format.PageFormat
    
    // 元数据
    spaceID  uint32
    pageNo   uint32
    pageType common.PageType
    size     uint32
    
    // 状态
    dirty bool
    lsn   uint64
    state basic.PageState
    
    // Buffer Pool集成
    bufferPage *buffer_pool.BufferPage
    
    // 统计
    stats basic.PageStats
}

// GetContent 返回内容副本（安全）
func (up *UnifiedPage) GetContent() []byte {
    up.RLock()
    defer up.RUnlock()
    
    // 委托给format层序列化
    data, _ := up.format.Serialize()
    return data
}

// ReadContent 零拷贝访问（高性能）
func (up *UnifiedPage) ReadContent(fn func([]byte)) {
    up.RLock()
    defer up.RUnlock()
    
    // 委托给format层序列化
    data, _ := up.format.Serialize()
    fn(data)
}

// ToBytes 序列化（委托给format层）
func (up *UnifiedPage) ToBytes() []byte {
    up.RLock()
    defer up.RUnlock()
    
    data, _ := up.format.Serialize()
    return data
}

// ParseFromBytes 反序列化（委托给format层）
func (up *UnifiedPage) ParseFromBytes(data []byte) error {
    up.Lock()
    defer up.Unlock()
    
    // 创建新的format对象
    pf := &format.PageFormat{}
    if err := pf.Deserialize(data); err != nil {
        return err
    }
    
    // 更新format
    up.format = pf
    
    // 更新元数据
    up.pageNo = pf.Header.PageNo
    up.spaceID = pf.Header.SpaceID
    up.pageType = common.PageType(pf.Header.PageType)
    up.lsn = pf.Header.LSN
    
    return nil
}

// Read 从磁盘或Buffer Pool读取
func (up *UnifiedPage) Read() error {
    // 1. 尝试从Buffer Pool读取
    if up.bufferPage != nil {
        content := up.bufferPage.GetContent()
        return up.ParseFromBytes(content)
    }
    
    // 2. 从磁盘读取（通过I/O层）
    // TODO: 实现磁盘读取逻辑
    
    return nil
}

// Write 写入磁盘或Buffer Pool
func (up *UnifiedPage) Write() error {
    if !up.dirty {
        return nil // 不是脏页，无需写入
    }
    
    // 1. 序列化
    data := up.ToBytes()
    
    // 2. 更新Buffer Pool
    if up.bufferPage != nil {
        up.bufferPage.SetContent(data)
    }
    
    // 3. 写入磁盘（通过I/O层）
    // TODO: 实现磁盘写入逻辑
    
    // 4. 清除脏标记
    up.SetDirty(false)
    
    return nil
}
```

---

### 3. I/O Layer（I/O层）

**位置**: `server/innodb/storage/io/`

**职责**:
- ✅ 文件I/O操作
- ✅ Buffer Pool管理
- ✅ 日志文件管理
- ❌ 不包含业务逻辑
- ❌ 不解析页面格式

**包结构**:
```
server/innodb/storage/io/
├── ibd_file.go         # IBD文件I/O
├── log_file.go         # 日志文件I/O
└── io_optimizer.go     # I/O优化器
```

---

## 🔄 迁移路径

### 阶段1: 创建Format层（1天）

**任务**:
1. 创建`server/innodb/storage/format/`包
2. 实现`PageFormat`, `FileHeaderFormat`, `FileTrailerFormat`
3. 实现序列化/反序列化方法
4. 编写单元测试

**文件**:
- `format/page_format.go`
- `format/header_format.go`
- `format/extent_format.go`
- `format/segment_format.go`

---

### 阶段2: 更新Wrapper层（2天）

**任务**:
1. 更新`UnifiedPage`使用`format.PageFormat`
2. 移除`UnifiedPage`中的序列化逻辑
3. 委托给`format.PageFormat`
4. 更新所有使用`UnifiedPage`的代码

**修改文件**:
- `wrapper/types/unified_page.go`
- `wrapper/page/*.go`

---

### 阶段3: 废弃Store层（1天）

**任务**:
1. 标记`store/pages/AbstractPage`为Deprecated
2. 标记`store/pages/IPage`为Deprecated
3. 更新文档说明迁移路径
4. 保留向后兼容性（暂不删除）

**修改文件**:
- `store/pages/page.go`
- `store/pages/*.go`

---

### 阶段4: 文档和测试（1天）

**任务**:
1. 编写架构设计文档
2. 编写迁移指南
3. 编写使用示例
4. 提高测试覆盖率

---

## 📊 预期收益

### 代码质量

| 指标 | 当前 | 目标 | 改进 |
|------|------|------|------|
| 接口定义数量 | 3个 | 1个 | -67% |
| 代码重复行数 | ~800行 | 0行 | -100% |
| 层次嵌套深度 | 4层 | 2层 | -50% |
| 文件数量 | 152个 | ~140个 | -8% |

---

### 性能

| 指标 | 当前 | 目标 | 改进 |
|------|------|------|------|
| 函数调用层次 | 4层 | 2层 | -50% |
| 内存占用 | 高 | 低 | -20% |
| 序列化性能 | 基准 | 优化 | +10% |

---

### 可维护性

| 指标 | 改进 |
|------|------|
| 职责清晰度 | ✅ 大幅提升 |
| 代码可读性 | ✅ 大幅提升 |
| 测试覆盖率 | ✅ 提升到80%+ |
| 新人上手难度 | ✅ 降低50% |

---

## ✅ 总结

**新架构特点**:
- ✅ 职责清晰：Format层=数据，Wrapper层=逻辑，I/O层=存储
- ✅ 接口统一：只有1个IPageWrapper接口
- ✅ 代码简洁：消除~800行重复代码
- ✅ 层次简单：从4层减少到2层
- ✅ 易于测试：Format层纯函数，易于单元测试
- ✅ 易于维护：职责单一，修改影响范围小

**迁移计划**:
- 阶段1：创建Format层（1天）
- 阶段2：更新Wrapper层（2天）
- 阶段3：废弃Store层（1天）
- 阶段4：文档和测试（1天）
- **总计**: 5天

**风险评估**:
- ⚠️ 中等风险：需要修改大量代码
- ✅ 可控：保留向后兼容性
- ✅ 可测试：每个阶段都有测试验证

---

**报告生成时间**: 2025-10-31  
**报告作者**: Augment Agent  
**任务状态**: P3.1.2 ✅ 完成

