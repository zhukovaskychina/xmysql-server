# INode 页面包装器实现

## 概述

这个文件实现了基于 `store/pages` 中 INode 页面的包装器。INode页面在InnoDB中用于管理段(segment)的元数据。

## 架构

```
page_constructors.go
       ↓
    NewINode()
       ↓
types/inode.go (包装器)
       ↓  
store/pages/inode_page.go (底层实现)
```

## 主要组件

### INode 结构体
- `SpaceID`: 表空间ID
- `PageNo`: 页面号
- `Offset`: 偏移量
- `FSPHeader`: 文件空间头部信息
- `inodePage`: 底层的 `pages.INodePage` 实例

### NewINode 函数
根据给定的 `spaceID` 和 `pageNo` 创建新的 INode 包装器实例。内部会创建一个 `pages.NewINodePage` 作为底层实现。

### 主要方法
- `GetInodePage()`: 获取底层的 INode 页面
- `ToBytes()`: 序列化为字节数组
- `ParseFromBytes()`: 从字节数组反序列化

## 使用方式

```go
// 创建 INode 页面包装器
inode := NewInodePageWrapper(pageID, spaceID)

// 获取底层页面
inodePage := inode.GetInodePage()

// 序列化
data := inode.ToBytes()

// 反序列化
newInode := &INode{}
err := newInode.ParseFromBytes(data)
```

## 依赖关系

该实现依赖于:
- `xmysql-server/server/innodb/storage/store/pages` 包中的 `INodePage` 类型
- 底层的 pages 包提供了实际的 INode 页面管理功能

## 设计原则

1. **封装**: 将底层的 `pages.INodePage` 封装在更高级的接口中
2. **简化**: 提供简化的API来创建和管理 INode 页面
3. **兼容性**: 与现有的页面构造器模式保持一致 