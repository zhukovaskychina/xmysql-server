# InnoDB 页面实现说明

## 页面类型概述

InnoDB存储引擎中的页面是数据存储的基本单位，不同类型的页面用于存储不同类型的数据和管理信息。本项目实现了以下几种页面类型：

### 1. 数据页 (FIL_PAGE_INDEX, 0x0000)
- **用途**：存储B+树的索引和数据，包括主键索引页和辅助索引页
- **实现文件**：`cluster_index_page.go`
- **主要功能**：
  - 实现B+树节点的数据存储
  - 支持记录的插入、删除和更新
  - 维护页面内记录的有序性
  - 管理页面空间的分配和回收

### 2. Undo日志页 (FIL_PAGE_UNDO_LOG, 0x0002)
- **用途**：存储事务的Undo日志，支持事务回滚和MVCC
- **实现文件**：`undo_log_page.go`
- **主要功能**：
  - 记录事务修改前的数据
  - 支持事务回滚操作
  - 实现MVCC多版本并发控制

### 3. INode页 (FIL_PAGE_INODE, 0x0003)
- **用途**：管理段（Extent）的页面
- **实现文件**：`inode_page.go`
- **主要功能**：
  - 管理表空间中的区段（Extent）
  - 维护段的元数据信息
  - 跟踪段内页面的使用状态

### 4. 插入缓冲空闲列表页 (FIL_PAGE_IBUF_FREE_LIST, 0x0004)
- **用途**：管理插入缓冲的空闲页面列表（已废弃，保留兼容性）
- **实现文件**：`ibuf_free_list_page.go`
- **主要功能**：
  - 管理Insert Buffer的空闲页面
  - 优化辅助索引的插入操作
  - 维护插入缓冲页面的分配状态

### 5. 插入缓冲位图页 (FIL_PAGE_IBUF_BITMAP, 0x0005)
- **用途**：记录插入缓冲的状态信息
- **实现文件**：`ibuf_bitmap_page.go`
- **主要功能**：
  - 跟踪插入缓冲的使用状态
  - 管理插入缓冲的空间分配

### 6. 系统页 (FIL_PAGE_TYPE_SYS, 0x0006)
- **用途**：存储系统级别的信息
- **实现文件**：`file_page_type_sys_page.go`
- **主要功能**：
  - 存储表空间的系统信息
  - 维护系统表的元数据
  - 管理系统级配置参数

### 7. 事务系统页 (FIL_PAGE_TYPE_TRX_SYS, 0x0007)
- **用途**：管理事务系统状态
- **实现文件**：`sys_trx_sys_page.go`
- **主要功能**：
  - 记录活跃事务信息
  - 维护事务ID生成
  - 管理回滚段信息

### 8. 表空间头页 (FIL_PAGE_TYPE_FSP_HDR, 0x0008)
- **用途**：管理表空间的元信息
- **实现文件**：`fsp_hrd_page.go`
- **主要功能**：
  - 存储表空间的基本信息
  - 管理空间分配状态
  - 维护区段描述符

### 9. 区段描述符页 (FIL_PAGE_TYPE_XDES, 0x0009)
- **用途**：管理表空间中的区段
- **实现文件**：`xdes_page.go`
- **主要功能**：
  - 描述区段的使用状态
  - 管理区段的分配信息
  - 维护空闲空间信息

### 10. BLOB页 (FIL_PAGE_TYPE_BLOB, 0x000A)
- **用途**：存储BLOB、TEXT等大字段的实际内容
- **实现文件**：`blob_page.go`
- **主要功能**：
  - 存储超过页面内联限制的大字段数据
  - 支持链式存储，跨越多个BLOB页面
  - 管理BLOB数据的分片和重组

### 11. 压缩页 (FIL_PAGE_TYPE_COMPRESSED, 0x000B)
- **用途**：存储经过压缩的页面数据
- **实现文件**：`compressed_page.go`
- **主要功能**：
  - 减少磁盘I/O和存储空间消耗
  - 支持ZLIB、LZ4、Snappy等压缩算法
  - 在内存中自动解压缩，透明访问

### 12. 加密页 (FIL_PAGE_TYPE_ENCRYPTED, 0x000C)
- **用途**：存储经过加密的页面数据
- **实现文件**：`encrypted_page.go`
- **主要功能**：
  - 透明数据加密(TDE)功能
  - 支持AES-128/256 CBC/CTR模式
  - 保护敏感数据，防止磁盘文件被非法访问

### 13. 已分配页 (FIL_PAGE_TYPE_ALLOCATED, 0x000F)
- **用途**：标记已分配但未初始化的页面
- **实现文件**：`allocated_page.go`
- **主要功能**：
  - 页面生命周期管理
  - 从空闲状态到具体用途的过渡状态
  - 支持页面类型转换

### 14. 数据字典头页
- **用途**：存储数据字典的头部信息
- **实现文件**：`data_dictionary_hdr_page.go`
- **主要功能**：
  - 管理数据字典的元信息
  - 维护表、索引等对象的定义

### 15. 回滚页
- **用途**：管理回滚段和回滚记录
- **实现文件**：`rollback_page.go`
- **主要功能**：
  - 存储回滚段信息
  - 管理事务回滚数据

## 页面通用结构

所有类型的页面都包含以下基本组成部分：

1. **文件头（File Header, 38字节）**
   - 包含页面的基本信息
   - 实现在 `page.go` 中的 `FileHeader` 结构

2. **页面主体**
   - 根据页面类型不同，存储不同的数据
   - 每种类型的页面有其特定的结构

3. **文件尾（File Trailer, 8字节）**
   - 用于页面完整性检查
   - 实现在 `page.go` 中的 `FileTrailer` 结构

## 主要接口

```go
type IPage interface {
    GetFileHeader() FileHeader
    GetFileTrailer() FileTrailer
    GetSerializeBytes() []byte
    LoadFileHeader(content []byte)
    LoadFileTrailer(content []byte)
}
```

## 实现特点

### 1. 内存优化
- 使用固定大小数组代替切片
- 预分配缓冲区
- 优化序列化操作

### 2. 错误处理
- 输入验证
- 完整性检查
- 异常状态处理

### 3. 并发控制
- 事务隔离
- MVCC支持
- 锁机制实现

### 4. 数据安全
- 压缩支持（ZLIB算法）
- 加密支持（AES-128/256）
- 校验和验证

## 使用说明

### 1. 创建页面：
```go
// 创建索引页面
indexPage := NewIndexPage(pageNo, spaceId)

// 创建Undo日志页
undoPage := NewUndoLogPage()

// 创建BLOB页面
blobPage := NewBlobPage(spaceID, pageNo, segmentID)

// 创建压缩页面
compressedPage := NewCompressedPage(spaceID, pageNo, CompressionZLIB)

// 创建加密页面
encryptedPage := NewEncryptedPage(spaceID, pageNo, EncryptionAES256CBC, keyID, keyVersion)
```

### 2. 序列化/反序列化：
```go
// 序列化页面
bytes := page.GetSerializeBytes()

// 加载页面内容
page.LoadFileHeader(content)

// 反序列化页面
err := page.Deserialize(data)
```

### 3. 页面操作：
```go
// 获取页面类型
pageType := header.GetPageType()

// 获取页面LSN
lsn := header.GetPageLSN()

// BLOB操作
blobData := blobPage.GetBlobData()
err := blobPage.SetBlobData(data, totalLength, offset, nextPage)

// 压缩操作
err := compressedPage.CompressData(originalData)
decompressedData, err := compressedPage.DecompressData()

// 加密操作
err := encryptedPage.EncryptData(originalData, key)
decryptedData, err := encryptedPage.DecryptData(key)
```

## 页面类型映射表

| 页面类型 | 编码 | 实现文件 | 状态 |
|---------|------|----------|------|
| FIL_PAGE_INDEX | 0x0000 | cluster_index_page.go |  完整 |
| FIL_PAGE_UNDO_LOG | 0x0002 | undo_log_page.go |  完整 |
| FIL_PAGE_INODE | 0x0003 | inode_page.go |  完整 |
| FIL_PAGE_IBUF_FREE_LIST | 0x0004 | ibuf_free_list_page.go |  新增 |
| FIL_PAGE_IBUF_BITMAP | 0x0005 | ibuf_bitmap_page.go |  完整 |
| FIL_PAGE_TYPE_SYS | 0x0006 | file_page_type_sys_page.go |  完善 |
| FIL_PAGE_TYPE_TRX_SYS | 0x0007 | sys_trx_sys_page.go |  完整 |
| FIL_PAGE_TYPE_FSP_HDR | 0x0008 | fsp_hrd_page.go |  完整 |
| FIL_PAGE_TYPE_XDES | 0x0009 | xdes_page.go |  完整 |
| FIL_PAGE_TYPE_BLOB | 0x000A | blob_page.go |  新增 |
| FIL_PAGE_TYPE_COMPRESSED | 0x000B | compressed_page.go |  新增 |
| FIL_PAGE_TYPE_ENCRYPTED | 0x000C | encrypted_page.go |  新增 |
| FIL_PAGE_TYPE_ALLOCATED | 0x000F | allocated_page.go |  完整 |

## 注意事项

1. **页面大小固定**，通常为16KB
2. **页面类型的正确性**至关重要
3. 需要保证**页面数据的完整性**
4. 注意处理**并发访问**的情况
5. **压缩和加密**功能需要额外的性能考虑
6. **BLOB页面**支持链式存储，需要正确处理页面链
7. **插入缓冲相关页面**在新版本中已废弃，但保留兼容性

## 更新日志

### 最新更新
-  新增 BLOB页面实现 (`blob_page.go`)
-  新增 压缩页面实现 (`compressed_page.go`)
-  新增 加密页面实现 (`encrypted_page.go`)
-  新增 插入缓冲空闲列表页面实现 (`ibuf_free_list_page.go`)
-  完善 系统页面实现 (`file_page_type_sys_page.go`)
- 🗑️ 删除重复页面：`ibuf_buffer_sys_page.go`, `ibuf_root_page.go`, `first_roll_seg_page.go`
-  更新文档，包含所有页面类型的详细说明

### 技术特性
- 支持多种压缩算法（ZLIB、LZ4、Snappy）
- 支持多种加密算法（AES-128/256 CBC/CTR）
- 完整的序列化/反序列化支持
- 数据完整性验证
- 错误处理和异常恢复
