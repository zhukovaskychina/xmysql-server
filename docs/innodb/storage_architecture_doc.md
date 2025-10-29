# InnoDB 存储引擎架构设计文档

## 概述

XMySQL 的 InnoDB 存储引擎采用分层架构设计，主要包括三个核心管理器：`SpaceManager`、`SystemSpaceManager` 和 `StorageManager`。

## 架构层次关系

```
StorageManager (顶层统一管理)
├── SpaceManager (表空间管理)
│   ├── 用户表空间 (space_id > 0)
│   └── 系统表空间 (space_id = 0, 作为普通表空间)
├── SystemSpaceManager (系统表空间特殊管理)
│   ├── 系统页面管理 (页面 0-7)
│   ├── 数据字典根页面 (页面 5)
│   └── DictionaryManager (数据字典管理)
├── SegmentManager (段管理)
├── BufferPoolManager (缓冲池管理)
└── PageManager (页面管理)
```

## 组件详细说明

### 1. SpaceManager (基础表空间管理器)

**接口定义**: `basic.SpaceManager`
**实现类**: `SpaceManagerImpl`

**核心职责**:
- 管理所有表空间的创建、打开、关闭和删除
- 管理 IBD 文件与表空间的映射关系
- 处理区(Extent)和页面的分配与回收
- 提供统一的表空间访问接口

**关键数据结构**:
```go
type SpaceManagerImpl struct {
    spaces   map[uint32]*space.IBDSpace  // 表空间ID -> IBDSpace实例
    ibdFiles map[uint32]*ibd.IBD_File    // 表空间ID -> IBD文件
    nameToID map[string]uint32           // 表空间名 -> 表空间ID
    nextID   uint32                      // 下一个可用的表空间ID
    dataDir  string                      // 数据目录
}
```

**管理的表空间类型**:
1. **系统表空间** (space_id = 0): `ibdata1` 文件
2. **用户表空间** (space_id > 0): 用户表的 `.ibd` 文件

### 2. SystemSpaceManager (系统表空间专用管理器)

**独立设计**: 专门处理系统表空间的特殊需求
**核心职责**:
- 管理系统表空间的固定页面布局
- 维护数据字典根页面(第5页)的结构
- 集成 DictionaryManager 进行数据字典管理
- 处理系统页面的特殊读写需求

**系统页面布局**:
```go
const (
    SYS_FSP_HDR_PAGE     = 0 // FSP头页面
    SYS_IBUF_BITMAP_PAGE = 1 // Insert Buffer位图页面  
    SYS_INODE_PAGE       = 2 // INode页面
    SYS_SYS_PAGE         = 3 // 系统页面
    SYS_INDEX_PAGE       = 4 // 索引页面
    SYS_DICT_ROOT_PAGE   = 5 // 数据字典根页面 ⭐
    SYS_TRX_SYS_PAGE     = 6 // 事务系统页面
    SYS_FIRST_RSEG_PAGE  = 7 // 第一个回滚段页面
)
```

**数据字典根页面结构**:
```go
type DictRootPageData struct {
    // 页面头部
    PageType   uint8
    EntryCount uint16
    FreeSpace  uint16
    
    // 数据字典头部信息
    MaxTableID uint64 // 最大表ID
    MaxIndexID uint64 // 最大索引ID
    MaxSpaceID uint32 // 最大表空间ID
    MaxRowID   uint64 // 最大行ID
    
    // 系统表根页面指针
    SysTablesRootPage  uint32 // SYS_TABLES表根页面
    SysColumnsRootPage uint32 // SYS_COLUMNS表根页面
    SysIndexesRootPage uint32 // SYS_INDEXES表根页面
    SysFieldsRootPage  uint32 // SYS_FIELDS表根页面
    
    // 段信息
    TablesSegmentID  uint32 // 表段ID
    IndexesSegmentID uint32 // 索引段ID
    ColumnsSegmentID uint32 // 列段ID
}
```

### 3. StorageManager (顶层存储管理器)

**接口定义**: `basic.StorageManager`
**实现类**: `StorageManager`

**核心职责**:
- 作为存储引擎的统一入口点
- 协调各个子管理器的工作
- 提供事务管理支持
- 处理跨组件的操作

**组合关系**:
```go
type StorageManager struct {
    spaceMgr    basic.SpaceManager      // SpaceManager实例
    segmentMgr  *SegmentManager         // 段管理器
    bufferPool  *buffer_pool.BufferPool // 缓冲池
    pageMgr     *DefaultPageManager     // 页面管理器
    tablespaces map[string]*TablespaceHandle // 表空间句柄缓存
}
```

## 工作流程和交互

### 1. 系统初始化流程

```
1. StorageManager.NewStorageManager(cfg)
   ├── 创建 SpaceManager (管理所有表空间)
   ├── 创建 BufferPool (页面缓存)
   ├── 创建 PageManager (页面管理)
   ├── 创建 SegmentManager (段管理)
   └── 调用 initializeSystemTablespaces()
       ├── 创建系统表空间 ibdata1 (space_id=0)
       ├── 创建 SystemSpaceManager
       ├── 初始化数据字典根页面 (页面5)
       └── 创建 DictionaryManager

2. SystemSpaceManager 初始化
   ├── 加载/创建数据字典根页面
   ├── 创建系统表段 (SYS_TABLES, SYS_COLUMNS, SYS_INDEXES)
   └── 初始化 B+树索引结构
```

### 2. 表空间操作流程

**创建用户表空间**:
```
1. StorageManager.CreateTablespace(name)
   └── SpaceManager.CreateTableSpace(name)
       ├── 分配新的 space_id (> 0)
       ├── 创建 .ibd 文件
       ├── 创建 IBDSpace 实例
       └── 注册到 spaces 映射

2. 更新数据字典
   └── DictionaryManager.CreateTable()
       ├── 在系统表空间中记录表定义
       └── 更新数据字典根页面
```

**系统表空间特殊处理**:
```
1. 系统页面访问 (页面 0-7)
   └── SystemSpaceManager.LoadDictRootPage()
       ├── 通过 BufferPoolManager 获取页面
       ├── 解析数据字典根页面结构
       └── 更新系统页面缓存

2. 数据字典操作
   └── DictionaryManager (集成在 SystemSpaceManager 中)
       ├── 管理 SYS_TABLES 等系统表
       └── 维护表空间和表的映射关系
```

## 设计优势

### 1. 职责分离
- **SpaceManager**: 专注于表空间和IBD文件管理
- **SystemSpaceManager**: 专门处理系统表空间的复杂需求  
- **StorageManager**: 提供统一的协调和事务管理

### 2. 扩展性
- 各组件相对独立，便于单独优化和测试
- 新的存储特性可以在相应层次添加

### 3. 兼容性
- 系统表空间的设计兼容 MySQL InnoDB 
- 数据字典根页面存储在标准的第5页

## 使用示例

```go
// 1. 初始化存储管理器
cfg := &conf.Cfg{
    DataDir: "/data/mysql",
    InnodbBufferPoolSize: 134217728, // 128MB
}
sm := manager.NewStorageManager(cfg)

// 2. 创建用户表空间
spaceID, err := sm.CreateTablespace("test_db/user_table")

// 3. 访问系统表空间
systemSpaceMgr := sm.GetSystemSpaceManager()
dictRootPage, err := systemSpaceMgr.LoadDictRootPage()

// 4. 数据字典操作
dictMgr := systemSpaceMgr.GetDictManager()
table, err := dictMgr.CreateTable("user_table", spaceID, columns)
```

## 总结

这种分层设计清晰地分离了不同层次的职责：

1. **SpaceManager** 处理底层的表空间和文件管理
2. **SystemSpaceManager** 专门处理系统表空间的特殊需求
3. **StorageManager** 提供统一的上层接口和协调功能

这样的设计既保持了代码的模块化，又满足了InnoDB存储引擎的复杂需求。 