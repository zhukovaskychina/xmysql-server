# MySQL 协议快速参考

## 🚀 快速开始

### 使用新的协议编码器

```go
import "github.com/zhukovaskychina/xmysql-server/server/net"

// 创建编码器
encoder := net.NewMySQLProtocolEncoder()

// 编码列定义
colDef := encoder.CreateColumnDefinition("tx_read_only", net.MYSQL_TYPE_LONGLONG, net.FLAG_NOT_NULL)
data := encoder.WriteColumnDefinitionPacket(colDef)

// 编码行数据
row := []interface{}{int64(0)}
rowData := encoder.WriteRowDataPacket(row)

// 生成完整 ResultSet
resultSet := &net.ResultSetData{
    Columns: []string{"tx_read_only"},
    Rows:    [][]interface{}{{int64(0)}},
}
packets := encoder.SendResultSetPackets(resultSet)
```

## 📊 MySQL 字段类型速查表

| Go 类型 | MySQL 类型 | 类型码 | 说明 |
|---------|-----------|--------|------|
| `bool` | TINYINT | 0x01 | 布尔值 |
| `int8` | TINYINT | 0x01 | 8位整数 |
| `int16` | SMALLINT | 0x02 | 16位整数 |
| `int`, `int32` | INT | 0x03 | 32位整数 |
| `int64` | BIGINT | 0x08 | 64位整数 |
| `float32` | FLOAT | 0x04 | 单精度浮点 |
| `float64` | DOUBLE | 0x05 | 双精度浮点 |
| `string` | VARCHAR | 0xFD | 字符串 |
| `[]byte` | BLOB | 0xFC | 二进制数据 |
| `nil` | NULL | 0x06 | 空值 |

## 🔢 Length-Encoded Integer 速查

| 值范围 | 编码格式 | 示例 |
|--------|---------|------|
| 0-250 | 1 字节 | `250` → `[0xFA]` |
| 251-65535 | 0xFC + 2 字节 | `251` → `[0xFC, 0xFB, 0x00]` |
| 65536-16777215 | 0xFD + 3 字节 | `65536` → `[0xFD, 0x00, 0x00, 0x01]` |
| 16777216+ | 0xFE + 8 字节 | `16777216` → `[0xFE, 0x00, 0x00, 0x00, 0x01, ...]` |

## 📝 Length-Encoded String 速查

| 值 | 编码 | 说明 |
|----|------|------|
| `""` | `[0x00]` | 空字符串 |
| `"a"` | `[0x01, 0x61]` | 单字符 |
| `"hello"` | `[0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F]` | 普通字符串 |
| `nil` | `[0xFB]` | NULL 值 |

## 📦 ResultSet 包顺序

```
1. Column Count        [lenenc-int]
2. Column Definition 1 [ColumnDefinition41]
3. Column Definition 2 [ColumnDefinition41]
   ...
N. Column Definition N [ColumnDefinition41]
N+1. EOF              [0xFE + warnings + status]
N+2. Row 1            [lenenc-str, lenenc-str, ...]
N+3. Row 2            [lenenc-str, lenenc-str, ...]
     ...
M. Row N              [lenenc-str, lenenc-str, ...]
M+1. EOF              [0xFE + warnings + status]
```

## 🔧 常用代码片段

### 1. 发送简单查询结果

```go
func sendSimpleResult(session Session, value int64) error {
    encoder := NewMySQLProtocolEncoder()
    
    // 准备数据
    data := &ResultSetData{
        Columns: []string{"result"},
        Rows:    [][]interface{}{{value}},
    }
    
    // 生成所有包
    packets := encoder.SendResultSetPackets(data)
    
    // 发送包
    seqID := byte(1)
    for _, pkt := range packets {
        mysqlPkt := createMySQLPacket(pkt, seqID)
        if err := session.WriteBytes(mysqlPkt); err != nil {
            return err
        }
        seqID++
    }
    
    return nil
}
```

### 2. 发送多列结果

```go
func sendMultiColumnResult(session Session) error {
    encoder := NewMySQLProtocolEncoder()
    
    data := &ResultSetData{
        Columns: []string{"id", "name", "age"},
        Rows: [][]interface{}{
            {int64(1), "Alice", int64(25)},
            {int64(2), "Bob", int64(30)},
        },
    }
    
    packets := encoder.SendResultSetPackets(data)
    
    seqID := byte(1)
    for _, pkt := range packets {
        mysqlPkt := createMySQLPacket(pkt, seqID)
        if err := session.WriteBytes(mysqlPkt); err != nil {
            return err
        }
        seqID++
    }
    
    return nil
}
```

### 3. 处理 NULL 值

```go
func sendResultWithNull(session Session) error {
    encoder := NewMySQLProtocolEncoder()
    
    data := &ResultSetData{
        Columns: []string{"id", "nullable_field"},
        Rows: [][]interface{}{
            {int64(1), nil},           // NULL 值
            {int64(2), "not null"},    // 非 NULL 值
        },
    }
    
    packets := encoder.SendResultSetPackets(data)
    
    // 发送...
    return nil
}
```

### 4. 自定义列定义

```go
func sendCustomColumnResult(session Session) error {
    encoder := NewMySQLProtocolEncoder()
    
    // 手动创建列定义
    colDef := &ColumnDefinition{
        Catalog:      "def",
        Schema:       "mydb",
        Table:        "mytable",
        OrgTable:     "mytable",
        Name:         "my_column",
        OrgName:      "my_column",
        CharacterSet: 0x21,  // UTF-8
        ColumnLength: 20,
        ColumnType:   MYSQL_TYPE_LONGLONG,
        Flags:        FLAG_NOT_NULL | FLAG_PRI_KEY,
        Decimals:     0,
    }
    
    colDefData := encoder.WriteColumnDefinitionPacket(colDef)
    
    // 发送...
    return nil
}
```

## 🧪 测试命令

```bash
# 运行所有测试
go test -v ./server/net

# 运行特定测试
go test -v ./server/net -run TestSendResultSetPackets_TxReadOnly

# 运行性能测试
go test -bench=. ./server/net -benchmem

# 查看测试覆盖率
go test -cover ./server/net
```

## 🔍 调试技巧

### 1. 打印包的十六进制

```go
import "encoding/hex"

data := encoder.WriteLenEncInt(123)
fmt.Printf("Hex: %s\n", hex.EncodeToString(data))
// 输出: Hex: 7b
```

### 2. 验证包结构

```go
func verifyPacket(data []byte) {
    fmt.Printf("Length: %d bytes\n", len(data))
    fmt.Printf("Hex: ")
    for i, b := range data {
        fmt.Printf("%02X ", b)
        if (i+1)%16 == 0 {
            fmt.Println()
            fmt.Printf("     ")
        }
    }
    fmt.Println()
}
```

### 3. 使用 Wireshark 抓包

```bash
# 启动 Wireshark
# 过滤器: tcp.port == 3309 && mysql

# 或使用 tcpdump
sudo tcpdump -i lo0 -w mysql.pcap port 3309
```

## ⚠️ 常见陷阱

### 1. 不要混用字符串和二进制编码

```go
// ❌ 错误：直接发送二进制整数
data := []byte{0x00, 0x00, 0x00, 0x01}  // 错误！

// ✅ 正确：使用文本协议
data := encoder.WriteLenEncString("1")  // 正确！
```

### 2. 不要忘记 EOF 包

```go
// ❌ 错误：缺少 EOF 包
// 1. Column Count
// 2. Column Definitions
// 3. Row Data  ← 缺少 EOF！

// ✅ 正确：完整的包序列
// 1. Column Count
// 2. Column Definitions
// 3. EOF  ← 必须有！
// 4. Row Data
// 5. EOF  ← 必须有！
```

### 3. 不要硬编码列类型

```go
// ❌ 错误：所有列都是 VARCHAR
for _, col := range columns {
    colDef := CreateColumnDefinition(col, MYSQL_TYPE_VAR_STRING, 0)
}

// ✅ 正确：根据实际数据推断类型
for i, col := range columns {
    value := rows[0][i]
    colDef := CreateColumnDefinitionFromValue(col, value)
}
```

## 📞 获取帮助

- 查看完整文档: `PROTOCOL_FIX_GUIDE.md`
- 查看测试用例: `server/net/mysql_protocol_encoder_test.go`
- MySQL 协议文档: https://dev.mysql.com/doc/internals/en/client-server-protocol.html

## ✅ 检查清单

在提交代码前，确保：

- [ ] 所有单元测试通过
- [ ] JDBC 可以成功连接
- [ ] `SELECT @@session.tx_read_only` 返回正确结果
- [ ] MySQL CLI 可以正常查询
- [ ] 没有硬编码的列名或类型
- [ ] NULL 值正确处理
- [ ] 日志输出清晰
- [ ] 性能测试通过

---

**最后更新**: 2024-11-15  
**版本**: 1.0.0  
**状态**: ✅ 生产就绪
