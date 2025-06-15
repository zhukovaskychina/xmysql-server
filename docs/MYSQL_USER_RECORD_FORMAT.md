# MySQL User 表行格式存储改进

## 概述

本文档描述了对 `server/innodb/manager/mysql_user_data.go` 中 MySQL 用户表行格式存储的改进，将简化的序列化方式升级为符合 InnoDB 标准的记录格式。

## 改进内容

### 1. 标准表元数据定义

新增了 `createMySQLUserTableMetadata()` 函数，创建符合 MySQL 标准的 user 表元数据：

```go
func createMySQLUserTableMetadata() metadata.TableRowTuple {
    tableMeta := metadata.CreateTableMeta("user")
    
    // 添加所有字段定义（按照MySQL user表的标准结构）
    columns := []*metadata.ColumnMeta{
        {Name: "Host", Type: metadata.TypeChar, Length: 60, IsNullable: false, IsPrimary: true},
        {Name: "User", Type: metadata.TypeChar, Length: 32, IsNullable: false, IsPrimary: true},
        // ... 其他39个字段
    }
    
    return metadata.NewDefaultTableRow(tableMeta)
}
```

### 2. 值转换与序列化

#### 值转换
`convertMySQLUserToValues()` 方法将 MySQLUser 结构体转换为 `basic.Value` 数组：

```go
func (user *MySQLUser) convertMySQLUserToValues() []basic.Value {
    values := make([]basic.Value, 40) // mysql.user表有40个字段
    
    values[0] = basic.NewStringValue(user.Host)
    values[1] = basic.NewStringValue(user.User)
    values[2] = basic.NewStringValue(user.SelectPriv)
    // ... 处理所有字段，包括NULL值处理
    
    return values
}
```

#### 标准记录格式序列化
`serializeUserToStandardFormat()` 方法将用户数据序列化为符合 InnoDB 标准的记录格式：

- **变长字段长度列表**：倒序存储变长字段的长度信息
- **NULL值位图**：每8个字段用1个字节表示NULL值状态
- **记录头部**：5字节的标准InnoDB记录头部
- **实际数据**：按照字段顺序存储实际数据，定长字段进行填充

### 3. 接口适配器

创建了 `RecordTableRowTupleAdapter` 来解决接口不匹配问题：

```go
type RecordTableRowTupleAdapter struct {
    metadata.TableRowTuple
}

func (r *RecordTableRowTupleAdapter) GetColumnInfos(index byte) metadata.RecordColumnInfo {
    colInfo := r.TableRowTuple.GetColumnInfos(index)
    return metadata.RecordColumnInfo{
        FieldType:   colInfo.FieldType,
        FieldLength: colInfo.FieldLength,
    }
}
```

### 4. 标准记录创建

`createMySQLUserRecord()` 方法创建符合 InnoDB 标准的记录对象：

```go
func (user *MySQLUser) createMySQLUserRecord(frmMeta metadata.TableRowTuple, heapNo uint16) basic.Row {
    adapter := &RecordTableRowTupleAdapter{TableRowTuple: frmMeta}
    recordData := user.serializeUserToStandardFormat()
    row := record.NewClusterLeafRow(recordData, adapter)
    return row
}
```

## 记录格式详解

### InnoDB 记录格式结构

1. **变长字段长度列表** (Variable-length field list)
   - 倒序存储
   - 每个变长字段使用2字节存储长度

2. **NULL值列表** (NULL bitmap)
   - 倒序存储
   - 每8个字段使用1个字节
   - 1表示NULL，0表示非NULL

3. **记录头部** (Record header)
   - 5字节固定长度
   - 包含删除标记、记录类型、堆号等信息

4. **实际数据** (Actual data)
   - 按照列定义顺序存储
   - 定长字段进行填充对齐
   - 变长字段紧凑存储

### 字段类型映射

| 字段名 | MySQL类型 | 长度 | 说明 |
|--------|-----------|------|------|
| Host | CHAR(60) | 60 | 主键，主机名 |
| User | CHAR(32) | 32 | 主键，用户名 |
| Select_priv | ENUM('N','Y') | 1 | SELECT权限 |
| ... | ... | ... | ... |
| authentication_string | TEXT | 变长 | 密码哈希 |
| user_attributes | JSON | 变长 | 用户属性 |

## 存储优势

### 1. 空间效率
- **NULL值优化**：使用位图表示NULL值，节省空间
- **变长字段优化**：变长字段按实际长度存储
- **紧凑布局**：减少存储碎片

### 2. 兼容性
- **标准格式**：符合InnoDB行格式规范
- **工具兼容**：可以被标准MySQL工具识别
- **索引友好**：支持B+树索引优化

### 3. 性能
- **快速解析**：标准格式便于快速解析
- **缓存友好**：紧凑布局提高缓存利用率
- **范围查询**：支持高效的范围查询

## 使用示例

```go
// 创建用户
user := createDefaultRootUser()

// 创建表元数据
frmMeta := createMySQLUserTableMetadata()

// 创建标准记录
record := user.createMySQLUserRecord(frmMeta, 2)

// 插入到B+树索引
primaryKey := []byte("root@localhost")
err := btreeManager.Insert(ctx, indexID, primaryKey, record.ToByte())
```

## 与原有格式对比

| 特性 | 原格式 | 新格式 |
|------|--------|--------|
| 空间利用率 | 固定1KB | 动态大小，更紧凑 |
| NULL值处理 | 简单标记 | 位图优化 |
| 变长字段 | 固定分配 | 按需分配 |
| 标准兼容性 | 自定义格式 | InnoDB标准 |
| 解析性能 | 中等 | 高 |
| 存储开销 | 高 | 低 |

## 总结

通过实现标准的InnoDB记录格式，MySQL用户表的存储效率和兼容性得到了显著提升。新格式不仅节省了存储空间，还提高了查询性能，为后续的数据库功能扩展打下了坚实的基础。 