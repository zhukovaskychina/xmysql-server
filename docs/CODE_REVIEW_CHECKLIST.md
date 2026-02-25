# XMySQL Server 代码审查清单

## 📋 概述

**版本**: 1.0  
**生效日期**: 2025-10-31  
**适用范围**: 所有Pull Request和代码审查  
**参考文档**: [CODING_STANDARDS.md](./CODING_STANDARDS.md)

本清单用于代码审查，确保代码符合项目编码规范。

---

## ✅ 审查清单

### 1. 命名规范 (NAMING)

#### 1.1 接口命名

- [ ] **NAMING-001**: 所有接口使用`I`前缀
  - ✅ 正确: `IPageWrapper`, `IPageFactory`, `IRecord`
  - ❌ 错误: `PageWrapper`, `PageSerializer`, `TableManager`

- [ ] **NAMING-002**: 接口名称描述性强，易于理解
  - ✅ 正确: `IPageWrapper`, `IRecordPage`
  - ❌ 错误: `IData`, `IObj`

#### 1.2 结构体命名

- [ ] **NAMING-003**: 结构体使用驼峰命名，不使用`Impl`后缀
  - ✅ 正确: `PageManager`, `SpaceManager`
  - ❌ 错误: `PageManagerImpl`, `SpaceManagerImpl`

- [ ] **NAMING-004**: 统计结构使用`Stats`后缀
  - ✅ 正确: `PageManagerStats`, `SpaceManagerStats`
  - ❌ 错误: `PageManagerStatistics`, `PageManagerInfo`

#### 1.3 方法命名

- [ ] **NAMING-005**: Getter方法使用`GetXxx()`前缀
  - ✅ 正确: `GetPageID()`, `GetSpaceID()`, `GetContent()`
  - ❌ 错误: `PageID()`, `Content()`, `Bytes()`

- [ ] **NAMING-006**: Setter方法使用`SetXxx()`前缀
  - ✅ 正确: `SetPageID()`, `SetDirty()`, `SetLSN()`
  - ❌ 错误: `PageID(id)`, `Dirty(bool)`, `LSN(lsn)`

- [ ] **NAMING-007**: 布尔方法使用`IsXxx()`, `HasXxx()`, `CanXxx()`前缀
  - ✅ 正确: `IsDirty()`, `IsLeaf()`, `HasChildren()`, `CanSplit()`
  - ❌ 错误: `GetIsDirty()`, `GetDirty()`, `Dirty()`

- [ ] **NAMING-008**: 构造函数使用`NewXxx()`前缀
  - ✅ 正确: `NewPageHeader()`, `NewIndexPage()`
  - ❌ 错误: `CreatePageHeader()`, `MakeIndexPage()`

#### 1.4 常量命名

- [ ] **NAMING-009**: 常量使用驼峰命名或全大写+下划线
  - ✅ 正确: `DefaultPageSize`, `FIL_PAGE_INDEX`
  - ❌ 错误: `default_page_size`, `filPageIndex`

#### 1.5 变量命名

- [ ] **NAMING-010**: 变量使用驼峰命名，简短但有意义
  - ✅ 正确: `pageNo`, `spaceID`, `isLeaf`
  - ❌ 错误: `p`, `s`, `page_no`, `space_id`

---

### 2. 错误处理 (ERROR)

#### 2.1 错误定义

- [ ] **ERROR-001**: 使用预定义错误变量，不使用`errors.New`
  - ✅ 正确: `return ErrPageNotFound`
  - ❌ 错误: `return errors.New("page not found")`

- [ ] **ERROR-002**: 错误变量使用`Err`前缀
  - ✅ 正确: `ErrPageNotFound`, `ErrInvalidPageType`
  - ❌ 错误: `PageNotFoundError`, `InvalidPageType`

- [ ] **ERROR-003**: 每个包有`errors.go`文件
  - ✅ 正确: `wrapper/types/errors.go`
  - ❌ 错误: 错误定义分散在多个文件中

- [ ] **ERROR-004**: 错误按功能分组
  - ✅ 正确: 页面错误、事务错误、锁错误分组
  - ❌ 错误: 所有错误混在一起

- [ ] **ERROR-005**: 错误有文档注释
  - ✅ 正确: `// ErrPageNotFound is returned when...`
  - ❌ 错误: 无注释

#### 2.2 错误返回

- [ ] **ERROR-006**: 需要上下文信息时使用`fmt.Errorf`
  - ✅ 正确: `fmt.Errorf("failed to read page %d: %w", pageNo, err)`
  - ❌ 错误: `errors.New("failed to read page")`

- [ ] **ERROR-007**: 使用`%w`包装错误，不使用`%v`
  - ✅ 正确: `fmt.Errorf("failed: %w", err)`
  - ❌ 错误: `fmt.Errorf("failed: %v", err)`

#### 2.3 错误检查

- [ ] **ERROR-008**: 使用`errors.Is`检查错误，不使用`==`
  - ✅ 正确: `if errors.Is(err, ErrPageNotFound) { ... }`
  - ❌ 错误: `if err == ErrPageNotFound { ... }`

- [ ] **ERROR-009**: 使用`errors.As`提取错误类型
  - ✅ 正确: `var pathErr *os.PathError; if errors.As(err, &pathErr) { ... }`
  - ❌ 错误: `if pathErr, ok := err.(*os.PathError); ok { ... }`

---

### 3. 注释规范 (DOC)

#### 3.1 包注释

- [ ] **DOC-001**: 每个包有包注释
  - ✅ 正确: `// Package types defines...`
  - ❌ 错误: 无包注释

#### 3.2 类型注释

- [ ] **DOC-002**: 所有导出类型有注释
  - ✅ 正确: `// IPageWrapper is the unified...`
  - ❌ 错误: 无注释

- [ ] **DOC-003**: 注释以类型名开头
  - ✅ 正确: `// PageManager manages all pages...`
  - ❌ 错误: `// Manages all pages...`

#### 3.3 函数/方法注释

- [ ] **DOC-004**: 所有导出函数/方法有注释
  - ✅ 正确: `// NewPageHeader creates...`
  - ❌ 错误: 无注释

- [ ] **DOC-005**: 注释以函数名开头
  - ✅ 正确: `// GetPageID returns the page ID...`
  - ❌ 错误: `// Returns the page ID...`

- [ ] **DOC-006**: 复杂函数说明参数和返回值
  - ✅ 正确: 包含参数说明和返回值说明
  - ❌ 错误: 只有一行简单说明

#### 3.4 复杂逻辑注释

- [ ] **DOC-007**: 复杂逻辑有行内注释
  - ✅ 正确: 关键步骤有注释说明
  - ❌ 错误: 复杂逻辑无注释

---

### 4. 代码组织 (ORG)

#### 4.1 文件组织

- [ ] **ORG-001**: 每个文件专注于一个主要类型或功能
  - ✅ 正确: `page_wrapper.go`只包含IPageWrapper相关代码
  - ❌ 错误: 一个文件包含多个不相关的类型

- [ ] **ORG-002**: 测试文件使用`_test.go`后缀
  - ✅ 正确: `unified_page_test.go`
  - ❌ 错误: `unified_page.test.go`, `test_unified_page.go`

#### 4.2 导入顺序

- [ ] **ORG-003**: 导入按标准库、第三方库、项目内部包顺序
  - ✅ 正确: 标准库 → 第三方库 → 项目内部包
  - ❌ 错误: 顺序混乱

- [ ] **ORG-004**: 导入分组之间有空行
  - ✅ 正确: 各组之间有空行分隔
  - ❌ 错误: 所有导入挤在一起

#### 4.3 代码结构

- [ ] **ORG-005**: 结构体字段按逻辑分组
  - ✅ 正确: 并发控制 → 核心数据 → 元数据 → 状态
  - ❌ 错误: 字段顺序混乱

- [ ] **ORG-006**: 方法按功能分组
  - ✅ 正确: 构造函数 → Getter → Setter → 业务方法
  - ❌ 错误: 方法顺序混乱

---

### 5. 并发安全 (CONCURRENCY)

- [ ] **CONCURRENCY-001**: 共享数据有适当的锁保护
  - ✅ 正确: 使用`sync.RWMutex`保护共享数据
  - ❌ 错误: 无锁保护

- [ ] **CONCURRENCY-002**: 使用`defer`确保锁释放
  - ✅ 正确: `defer mu.Unlock()`
  - ❌ 错误: 手动释放锁，可能遗漏

- [ ] **CONCURRENCY-003**: 读操作使用读锁，写操作使用写锁
  - ✅ 正确: `RLock()`用于读，`Lock()`用于写
  - ❌ 错误: 所有操作都使用写锁

---

### 6. 性能优化 (PERFORMANCE)

- [ ] **PERFORMANCE-001**: 避免不必要的内存分配
  - ✅ 正确: 复用buffer，使用对象池
  - ❌ 错误: 频繁创建临时对象

- [ ] **PERFORMANCE-002**: 大对象使用指针传递
  - ✅ 正确: `func Process(page *Page)`
  - ❌ 错误: `func Process(page Page)`

- [ ] **PERFORMANCE-003**: 避免在循环中进行昂贵操作
  - ✅ 正确: 循环外计算，循环内复用
  - ❌ 错误: 循环内重复计算

---

### 7. 测试覆盖 (TEST)

- [ ] **TEST-001**: 新功能有单元测试
  - ✅ 正确: 每个新函数都有测试
  - ❌ 错误: 无测试

- [ ] **TEST-002**: 测试覆盖正常和异常情况
  - ✅ 正确: 测试成功和失败路径
  - ❌ 错误: 只测试成功路径

- [ ] **TEST-003**: 测试函数命名清晰
  - ✅ 正确: `TestPageManager_GetPage_Success`, `TestPageManager_GetPage_NotFound`
  - ❌ 错误: `TestGetPage`, `Test1`

---

## 📊 审查评分

### 评分标准

| 分数 | 等级 | 说明 |
|------|------|------|
| 90-100 | 优秀 | 完全符合规范，可以合并 |
| 80-89 | 良好 | 基本符合规范，有少量问题需要修复 |
| 70-79 | 及格 | 有一些问题需要修复 |
| <70 | 不及格 | 有严重问题，需要大幅修改 |

### 评分计算

- 每个检查项1分
- 总分 = (通过项数 / 总项数) × 100

---

## 🔧 常见问题修复

### 问题1: 接口缺少I前缀

**修复前**:
```go
type PageWrapper interface {
    GetPageID() uint32
}
```

**修复后**:
```go
type IPageWrapper interface {
    GetPageID() uint32
}
```

---

### 问题2: 方法缺少Get前缀

**修复前**:
```go
func (p *Page) PageID() uint32 {
    return p.pageID
}
```

**修复后**:
```go
func (p *Page) GetPageID() uint32 {
    return p.pageID
}
```

---

### 问题3: 使用errors.New而不是预定义错误

**修复前**:
```go
func (m *PageManager) GetPage(pageNo uint32) (*Page, error) {
    if page, ok := m.pages[pageNo]; ok {
        return page, nil
    }
    return nil, errors.New("page not found")
}
```

**修复后**:
```go
// 1. 在errors.go中定义错误
var ErrPageNotFound = errors.New("page not found")

// 2. 使用预定义错误
func (m *PageManager) GetPage(pageNo uint32) (*Page, error) {
    if page, ok := m.pages[pageNo]; ok {
        return page, nil
    }
    return nil, ErrPageNotFound
}
```

---

### 问题4: 使用%v而不是%w包装错误

**修复前**:
```go
func (m *PageManager) ReadPage(pageNo uint32) (*Page, error) {
    data, err := m.io.Read(pageNo)
    if err != nil {
        return nil, fmt.Errorf("failed to read page %d: %v", pageNo, err)
    }
    return m.parsePage(data)
}
```

**修复后**:
```go
func (m *PageManager) ReadPage(pageNo uint32) (*Page, error) {
    data, err := m.io.Read(pageNo)
    if err != nil {
        return nil, fmt.Errorf("failed to read page %d: %w", pageNo, err)
    }
    return m.parsePage(data)
}
```

---

### 问题5: 缺少文档注释

**修复前**:
```go
type PageManager struct {
    mu    sync.RWMutex
    pages map[uint32]*Page
}

func NewPageManager() *PageManager {
    return &PageManager{
        pages: make(map[uint32]*Page),
    }
}
```

**修复后**:
```go
// PageManager manages all pages in the buffer pool.
// It provides methods for creating, reading, updating, and deleting pages.
type PageManager struct {
    mu    sync.RWMutex
    pages map[uint32]*Page
}

// NewPageManager creates a new PageManager instance.
// Returns a pointer to the newly created PageManager.
func NewPageManager() *PageManager {
    return &PageManager{
        pages: make(map[uint32]*Page),
    }
}
```

---

## 📋 审查模板

```markdown
## 代码审查报告

**PR编号**: #XXX  
**审查者**: XXX  
**审查时间**: 2025-XX-XX

### 审查结果

- [ ] 命名规范 (X/10)
- [ ] 错误处理 (X/9)
- [ ] 注释规范 (X/7)
- [ ] 代码组织 (X/6)
- [ ] 并发安全 (X/3)
- [ ] 性能优化 (X/3)
- [ ] 测试覆盖 (X/3)

**总分**: X/41 (XX%)  
**等级**: 优秀/良好/及格/不及格

### 主要问题

1. [NAMING-001] 接口XXX缺少I前缀
2. [ERROR-001] 使用errors.New而不是预定义错误
3. [DOC-004] 函数XXX缺少文档注释

### 建议

1. 统一接口命名，添加I前缀
2. 定义预定义错误变量
3. 添加文档注释

### 审查意见

- ✅ 批准合并
- ⚠️ 需要修改后合并
- ❌ 拒绝合并
```

---

**文档版本**: 1.0  
**更新时间**: 2025-10-31  
**维护者**: Augment Agent

