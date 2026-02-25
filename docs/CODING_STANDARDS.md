# XMySQL Server 编码规范

## 📋 概述

**版本**: 1.0  
**生效日期**: 2025-10-31  
**适用范围**: XMySQL Server项目所有Go代码  
**维护者**: Augment Agent

本文档定义了XMySQL Server项目的编码规范，包括命名规范、编码风格、错误处理、注释规范等。

---

## 🎯 设计原则

### 核心原则

1. **一致性优先**: 保持代码风格一致性
2. **可读性优先**: 代码应该易于阅读和理解
3. **简洁性优先**: 避免过度设计和冗余代码
4. **可维护性优先**: 代码应该易于维护和扩展

### Go语言惯例

遵循Go语言官方编码规范：
- [Effective Go](https://golang.org/doc/effective_go.html)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)

---

## 📝 命名规范

### 1. 接口命名

**规则**: 所有接口使用`I`前缀 + 驼峰命名

**✅ 正确示例**:
```go
// 页面包装器接口
type IPageWrapper interface {
    GetPageID() uint32
    GetSpaceID() uint32
}

// 页面工厂接口
type IPageFactory interface {
    CreatePage(pageType common.PageType) (IPageWrapper, error)
}

// 记录接口
type IRecord interface {
    GetID() uint64
    GetData() []byte
}
```

**❌ 错误示例**:
```go
// ❌ 缺少I前缀
type PageWrapper interface { ... }

// ❌ 缺少I前缀
type PageSerializer interface { ... }

// ❌ 缺少I前缀
type TableManager interface { ... }
```

**例外情况**:
- 标准库接口（如`io.Reader`, `io.Writer`）
- 单方法接口可以使用`-er`后缀（如`Stringer`, `Closer`）

---

### 2. 结构体命名

**规则**: 使用驼峰命名，避免使用`Impl`后缀

**✅ 正确示例**:
```go
// 接口
type ISpaceManager interface { ... }

// 实现（不使用Impl后缀）
type SpaceManager struct {
    mu      sync.RWMutex
    spaces  map[uint32]*Space
}

// 统计结构（使用Stats后缀）
type SpaceManagerStats struct {
    TotalSpaces   uint32
    ActiveSpaces  uint32
}
```

**❌ 错误示例**:
```go
// ❌ 使用Impl后缀
type SpaceManagerImpl struct { ... }

// ❌ 接口缺少I前缀
type SpaceManager interface { ... }
```

---

### 3. 方法命名

#### 3.1 Getter方法

**规则**: 使用`GetXxx()`前缀

**✅ 正确示例**:
```go
func (p *Page) GetPageID() uint32 { ... }
func (p *Page) GetSpaceID() uint32 { ... }
func (p *Page) GetPageType() common.PageType { ... }
func (p *Page) GetLSN() uint64 { ... }
func (p *Page) GetContent() []byte { ... }
```

**❌ 错误示例**:
```go
// ❌ 缺少Get前缀
func (p *Page) PageID() uint32 { ... }

// ❌ 缺少Get前缀
func (p *Page) Content() []byte { ... }

// ❌ 缺少Get前缀
func (ph *PageHeader) Bytes() []byte { ... }
```

#### 3.2 Setter方法

**规则**: 使用`SetXxx()`前缀

**✅ 正确示例**:
```go
func (p *Page) SetPageID(id uint32) { ... }
func (p *Page) SetDirty(dirty bool) { ... }
func (p *Page) SetLSN(lsn uint64) { ... }
```

#### 3.3 布尔方法

**规则**: 使用`IsXxx()`, `HasXxx()`, `CanXxx()`前缀

**✅ 正确示例**:
```go
func (p *Page) IsDirty() bool { ... }
func (p *Page) IsLeaf() bool { ... }
func (p *Page) IsRoot() bool { ... }
func (p *Page) HasChildren() bool { ... }
func (p *Page) CanSplit() bool { ... }
```

**❌ 错误示例**:
```go
// ❌ 使用Get前缀
func (p *Page) GetIsDirty() bool { ... }

// ❌ 使用Get前缀
func (p *Page) GetDirty() bool { ... }
```

#### 3.4 构造函数

**规则**: 使用`NewXxx()`前缀

**✅ 正确示例**:
```go
// NewPageHeader creates a new PageHeader instance with the given size.
func NewPageHeader(size uint32) *PageHeader { ... }

// NewIndexPage creates a new index page.
func NewIndexPage(spaceID, pageNo uint32) *IndexPage { ... }
```

---

### 4. 常量命名

**规则**: 使用驼峰命名或全大写+下划线

**✅ 正确示例**:
```go
// 驼峰命名（推荐）
const (
    DefaultPageSize = 16384
    MaxPageSize     = 65536
    MinPageSize     = 4096
)

// 全大写+下划线（用于导出常量）
const (
    FIL_PAGE_INDEX          = 17855
    FIL_PAGE_TYPE_ALLOCATED = 0
    FIL_PAGE_INODE          = 3
)
```

---

### 5. 变量命名

**规则**: 使用驼峰命名，简短但有意义

**✅ 正确示例**:
```go
// 局部变量
pageNo := uint32(0)
spaceID := uint32(1)
isLeaf := true

// 包级变量
var (
    defaultBufferPoolSize = 128 * 1024 * 1024
    maxConnections        = 1000
)
```

**❌ 错误示例**:
```go
// ❌ 过于简短
p := uint32(0)
s := uint32(1)

// ❌ 使用下划线
page_no := uint32(0)
space_id := uint32(1)
```

---

## 🚨 错误处理规范

### 1. 错误定义

**规则**: 使用预定义错误变量，每个包一个`errors.go`文件

**✅ 正确示例**:
```go
// server/innodb/storage/wrapper/types/errors.go
package types

import "errors"

var (
    // ErrPageNotFound is returned when the requested page does not exist.
    ErrPageNotFound = errors.New("page not found")
    
    // ErrInvalidPageType is returned when the page type is invalid.
    ErrInvalidPageType = errors.New("invalid page type")
    
    // ErrPageCorrupted is returned when the page data is corrupted.
    ErrPageCorrupted = errors.New("page corrupted")
)
```

**错误命名规范**:
- ✅ 使用`Err`前缀
- ✅ 使用驼峰命名
- ✅ 描述性强
- ✅ 添加文档注释

**错误分组**:
```go
// 页面相关错误
var (
    ErrPageNotFound    = errors.New("page not found")
    ErrPageCorrupted   = errors.New("page corrupted")
    ErrInvalidPageType = errors.New("invalid page type")
)

// 事务相关错误
var (
    ErrTxNotFound      = errors.New("transaction not found")
    ErrTxAlreadyExists = errors.New("transaction already exists")
    ErrTxTimeout       = errors.New("transaction timeout")
)
```

---

### 2. 错误返回

**规则**: 
1. 常见错误使用预定义错误
2. 需要上下文信息使用`fmt.Errorf`
3. 使用`%w`包装错误以支持`errors.Is`和`errors.As`

**✅ 正确示例**:
```go
// 1. 使用预定义错误
func (m *PageManager) GetPage(pageNo uint32) (*Page, error) {
    page, ok := m.pages[pageNo]
    if !ok {
        return nil, ErrPageNotFound
    }
    return page, nil
}

// 2. 使用fmt.Errorf添加上下文
func (m *PageManager) ReadPage(pageNo uint32) (*Page, error) {
    data, err := m.io.Read(pageNo)
    if err != nil {
        return nil, fmt.Errorf("failed to read page %d: %w", pageNo, err)
    }
    return m.parsePage(data)
}

// 3. 包装错误
func (m *PageManager) LoadPage(pageNo uint32) (*Page, error) {
    page, err := m.ReadPage(pageNo)
    if err != nil {
        return nil, fmt.Errorf("failed to load page: %w", err)
    }
    return page, nil
}
```

**❌ 错误示例**:
```go
// ❌ 使用errors.New（应该使用预定义错误）
func (m *PageManager) GetPage(pageNo uint32) (*Page, error) {
    page, ok := m.pages[pageNo]
    if !ok {
        return nil, errors.New("page not found")
    }
    return page, nil
}

// ❌ 使用%v而不是%w（无法unwrap）
func (m *PageManager) ReadPage(pageNo uint32) (*Page, error) {
    data, err := m.io.Read(pageNo)
    if err != nil {
        return nil, fmt.Errorf("failed to read page %d: %v", pageNo, err)
    }
    return m.parsePage(data)
}
```

---

### 3. 错误检查

**规则**: 使用`errors.Is`和`errors.As`检查错误

**✅ 正确示例**:
```go
// 检查特定错误
page, err := manager.GetPage(pageNo)
if errors.Is(err, ErrPageNotFound) {
    // 处理页面不存在的情况
    return createNewPage(pageNo)
}

// 提取错误类型
var pathErr *os.PathError
if errors.As(err, &pathErr) {
    // 处理路径错误
    log.Printf("Path error: %s", pathErr.Path)
}
```

**❌ 错误示例**:
```go
// ❌ 使用==比较错误（无法处理包装的错误）
if err == ErrPageNotFound {
    ...
}

// ❌ 使用字符串比较
if err.Error() == "page not found" {
    ...
}
```

---

## 📖 注释规范

### 1. 包注释

**规则**: 每个包必须有包注释

**✅ 正确示例**:
```go
// Package types defines the core types and interfaces for the storage layer.
// It provides unified abstractions for pages, records, indexes, and other
// storage-related concepts.
package types
```

---

### 2. 类型注释

**规则**: 所有导出类型必须有注释，注释以类型名开头

**✅ 正确示例**:
```go
// IPageWrapper is the unified page wrapper interface.
// All page implementations should implement this interface.
// This is the core interface of the Storage module for abstracting
// different types of pages.
type IPageWrapper interface {
    ...
}

// PageManager manages all pages in the buffer pool.
// It provides methods for creating, reading, updating, and deleting pages.
type PageManager struct {
    mu    sync.RWMutex
    pages map[uint32]*Page
}
```

---

### 3. 函数/方法注释

**规则**: 所有导出函数/方法必须有注释，注释以函数名开头

**✅ 正确示例**:
```go
// NewPageHeader creates a new PageHeader instance with the given size.
// The size parameter specifies the total size of the page header in bytes.
// Returns a pointer to the newly created PageHeader.
func NewPageHeader(size uint32) *PageHeader {
    ...
}

// GetPageID returns the page ID (same as GetPageNo).
// The page ID uniquely identifies a page within a tablespace.
func (p *Page) GetPageID() uint32 {
    ...
}
```

---

### 4. 复杂逻辑注释

**规则**: 复杂逻辑添加行内注释

**✅ 正确示例**:
```go
func (m *PageManager) AllocatePage() (uint32, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // 1. 查找空闲页面
    // 优先使用已释放的页面，避免文件增长
    if pageNo, ok := m.findFreePage(); ok {
        return pageNo, nil
    }
    
    // 2. 扩展文件
    // 如果没有空闲页面，扩展文件并分配新页面
    pageNo, err := m.extendFile()
    if err != nil {
        return 0, fmt.Errorf("failed to extend file: %w", err)
    }
    
    return pageNo, nil
}
```

---

## 🏗️ 代码组织规范

### 1. 文件组织

**规则**: 
- 每个文件专注于一个主要类型或功能
- 相关类型可以放在同一个文件中
- 测试文件使用`_test.go`后缀

**示例**:
```
wrapper/types/
├── page_wrapper.go      # IPageWrapper接口定义
├── unified_page.go      # UnifiedPage实现
├── unified_page_test.go # UnifiedPage测试
├── index_page.go        # IndexPage实现
├── record_types.go      # 记录相关类型
└── errors.go            # 错误定义
```

---

### 2. 导入顺序

**规则**: 标准库 → 第三方库 → 项目内部包

**✅ 正确示例**:
```go
import (
    // 标准库
    "errors"
    "fmt"
    "sync"
    
    // 第三方库
    "github.com/stretchr/testify/assert"
    
    // 项目内部包
    "github.com/zhukovaskychina/xmysql-server/server/common"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)
```

---

## ✅ 代码审查清单

### 命名检查

- [ ] 接口使用`I`前缀
- [ ] Getter方法使用`GetXxx()`
- [ ] Setter方法使用`SetXxx()`
- [ ] 布尔方法使用`IsXxx()`, `HasXxx()`, `CanXxx()`
- [ ] 构造函数使用`NewXxx()`
- [ ] 错误变量使用`Err`前缀

### 错误处理检查

- [ ] 使用预定义错误而不是`errors.New`
- [ ] 使用`%w`包装错误
- [ ] 使用`errors.Is`和`errors.As`检查错误
- [ ] 每个包有`errors.go`文件

### 注释检查

- [ ] 所有导出类型有注释
- [ ] 所有导出函数/方法有注释
- [ ] 注释以类型/函数名开头
- [ ] 复杂逻辑有行内注释

### 代码组织检查

- [ ] 导入顺序正确
- [ ] 文件组织合理
- [ ] 测试文件使用`_test.go`后缀

---

**文档版本**: 1.0  
**更新时间**: 2025-10-31  
**维护者**: Augment Agent

