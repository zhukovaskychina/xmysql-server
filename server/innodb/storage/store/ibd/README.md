# InnoDB IBD文件实现

## 一、概述

IBD（InnoDB Tablespace）文件是InnoDB存储引擎中的独立表空间文件，用于持久化存储表的数据和索引。本实现遵循MySQL InnoDB的设计原则，提供了完整的表空间管理功能。

## 二、文件结构

### 1. 页面类型

| 页号 | 类型 | 描述 |
|------|------|------|
| 0 | FSP_HDR | 表空间头页，存储元信息和空间分配信息 |
| 1 | XDES | 区描述符页，管理extent的分配状态 |
| 2+ | INODE | 段管理页，存储segment的inode信息 |
| n | INDEX | B+树数据页，存储实际的表数据和索引 |
| n | UNDO | 事务回滚页，支持MVCC |
| n | BLOB | 大字段存储页 |

### 2. 层级结构

```
IBD文件
 └── Segment（段，逻辑管理单元）
      └── Extent（区，1MB，64页）
           └── Page（页，16KB）
```

## 三、核心管理器

### 1. 表空间管理器 (IBD_File)
```go
type IBD_File struct {
    // 文件元信息
    tableName    string
    spaceId      uint32
    dataBaseName string
    
    // 页面管理
    fspHdrPage   *FspHdrPage
    xdesPage     *XDESPage
    
    // 段管理
    clusterIndex  *Segment    // 聚簇索引段
    secondIndexes []*Segment  // 二级索引段
}
```

### 2. 段管理器 (Segment)
```go
type Segment struct {
    ID         uint64
    Type       uint8
    SpaceID    uint32
    FreeExtents []*ExtentEntry
    FragExtents []*ExtentEntry
    FullExtents []*ExtentEntry
}
```

### 3. 区管理器 (Extent)
```go
type ExtentEntry struct {
    FirstPageNo uint32
    State       uint8
    PageBitmap  [64]byte
}
```

## 四、关键流程

### 1. 表空间创建
1. 创建IBD文件
2. 初始化FSP头页和XDES页
3. 分配第一个Extent
4. 创建聚簇索引段

### 2. 页面分配
1. 检查段中的可用页面
2. 如果没有可用页面，分配新的Extent
3. 在Extent中分配页面
4. 更新位图和统计信息

### 3. 文件扩展
1. 检测空间不足
2. 按照配置增长大小扩展文件
3. 初始化新的Extent

## 五、管理器职责

### 1. IBD文件管理器
- 文件的创建、打开、关闭
- 页面的读写操作
- 空间分配和回收
- 文件同步和持久化

### 2. 段管理器
- Extent的分配和释放
- 页面的分配策略
- 空间使用统计
- Extent状态转换

### 3. 区管理器
- 页面位图管理
- 空闲页面追踪
- 页面分配状态维护

## 六、使用示例

```go
// 创建表空间
ibdFile := NewIBDFile("users", "mydb", 1, "/data", "users.ibd", 0)
ibdFile.CreateTable()

// 分配页面
pageNo, err := ibdFile.AllocatePage()
if err != nil {
    // 处理错误
}

// 写入数据
data := []byte{...}
ibdFile.WritePage(pageNo, data)

// 同步到磁盘
ibdFile.Sync()
```

## 七、注意事项

1. 文件操作
- 所有文件操作都需要加锁保护
- 写操作必须确保原子性
- 定期同步文件到磁盘

2. 空间管理
- 优先使用已分配Extent中的空闲页面
- 及时回收未使用的空间
- 避免空间碎片化

3. 性能优化
- 使用缓冲池减少IO
- 批量分配提高效率
- 预分配空间减少扩展次数

## 八、与MySQL交互

### 1. 引擎层调用
```go
func (srv *XMySQLEngine) ExecuteQuery(session server.MySQLServerSession, query string, databaseName string) {
    // 解析SQL
    // 执行计划生成
    // 调用存储层
    // 返回结果
}
```

### 2. 存储层接口
```go
// 表空间操作
CreateTable()
DropTable()
AllocatePage()
FreePage()

// 事务支持
BeginTransaction()
Commit()
Rollback()
```
