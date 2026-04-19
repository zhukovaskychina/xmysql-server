# MySQL协议高级特性实现文档

> **文档状态（2026-04）**：特性清单与实现说明；总索引见 [protocol/PROTOCOL_DOCUMENTATION_INDEX.md](./protocol/PROTOCOL_DOCUMENTATION_INDEX.md)。  
> **JDBC 联调**：全量 `jdbc_client` 失败项的分阶段修复与验收见 [planning/JDBC_INTEGRATION_TEST_FIX_PLAN.md](./planning/JDBC_INTEGRATION_TEST_FIX_PLAN.md)（日常连接/变量回归可用 Maven profile `jdbc-connectivity`）。

## 概述

本文档描述了xmysql-server中实现的MySQL协议高级特性，包括大包分片、压缩协议、字符集支持、连接属性和会话状态跟踪。

## 1. 大包分片处理（16MB限制）

### 功能描述

MySQL协议规定单个包的最大长度为 `0xFFFFFF` (16,777,215 bytes ≈ 16MB)。当数据超过此限制时，需要分片传输。

### 实现文件

- `server/protocol/packet_splitter.go`

### 核心功能

#### PacketSplitter

```go
splitter := NewPacketSplitter()

// 分片大包
packets := splitter.SplitPacket(largePayload, startSequenceId)

// 合并分片
merged, err := splitter.MergePackets(packets)
```

#### 主要方法

- `SplitPacket()` - 将大payload分片为多个MySQL包
- `MergePackets()` - 合并多个分片包
- `ReadPacketWithSplit()` - 自动处理分片包的读取
- `WritePacketsWithSplit()` - 自动处理分片包的写入
- `ValidatePacket()` - 验证包的完整性
- `CalculatePacketCount()` - 计算需要的包数量

#### 分片规则

1. 每个包最大 16MB
2. 如果最后一个包正好是 16MB，需要发送一个空包表示结束
3. 序列号连续递增
4. 接收方根据包长度判断是否还有后续包

### 使用示例

```go
// 发送大数据
largeData := make([]byte, 20*1024*1024) // 20MB
packets := splitter.SplitPacket(largeData, 0)
for _, packet := range packets {
    conn.Write(packet)
}

// 接收大数据
payload, sequenceId, err := splitter.ReadPacketWithSplit(reader)
```

---

## 2. 压缩协议支持

### 功能描述

MySQL压缩协议使用zlib算法压缩数据包，减少网络传输量。适用于慢速网络或大数据传输场景。

### 实现文件

- `server/protocol/compression.go`

### 核心功能

#### CompressionHandler

```go
handler := NewCompressionHandler(true) // 启用压缩

// 压缩包
compressed, err := handler.CompressPacket(payload, sequenceId)

// 解压包
decompressed, sequenceId, err := handler.DecompressPacket(compressed)
```

#### 压缩包格式

```
+-------------------+
| 压缩后长度 (3字节) |
+-------------------+
| 序列号 (1字节)     |
+-------------------+
| 压缩前长度 (3字节) |  // 0表示未压缩
+-------------------+
| 压缩数据 (N字节)   |
+-------------------+
```

#### 主要特性

- **压缩阈值**: 小于50字节的包不压缩
- **智能压缩**: 如果压缩后反而更大，使用原始数据
- **统计信息**: 跟踪压缩率和节省的字节数
- **批量压缩**: 支持合并多个小包后一起压缩

#### 压缩统计

```go
stats := &CompressionStats{}
stats.UpdateStats(originalSize, compressedSize, wasCompressed)

ratio := stats.CompressionRatio
percentage := stats.GetCompressionPercentage()
saved := stats.GetBytesSaved()
```

### 使用示例

```go
// 启用压缩
handler := NewCompressionHandler(true)

// 发送压缩数据
compressed, _ := handler.CompressPacket(data, 1)
conn.Write(compressed)

// 接收压缩数据
reader := NewCompressedPacketReader(conn, handler)
data, sequenceId, _ := reader.ReadPacket()
```

---

## 3. 完整的字符集支持

### 功能描述

支持MySQL的所有标准字符集和校对规则，包括UTF-8、UTF-8MB4、GBK、Latin1等。

### 实现文件

- `server/protocol/charset_manager.go`

### 核心功能

#### CharsetManager

```go
manager := GetGlobalCharsetManager()

// 根据名称获取字符集
charset, err := manager.GetCharsetByName("utf8mb4")

// 根据ID获取字符集
charset, err := manager.GetCharsetByID(255)

// 获取默认校对规则
collation, err := manager.GetDefaultCollation("utf8mb4")
```

#### 支持的字符集（部分）


| ID  | 名称      | 默认校对规则             | 最大字节长度 |
| --- | ------- | ------------------ | ------ |
| 8   | latin1  | latin1_swedish_ci  | 1      |
| 28  | gbk     | gbk_chinese_ci     | 2      |
| 33  | utf8    | utf8_general_ci    | 3      |
| 45  | utf8mb4 | utf8mb4_general_ci | 4      |
| 63  | binary  | binary             | 1      |
| 255 | utf8mb4 | utf8mb4_0900_ai_ci | 4      |


#### 字符集信息

```go
type CharsetInfo struct {
    ID          uint8   // 字符集ID
    Name        string  // 字符集名称
    Collation   string  // 默认校对规则
    MaxLen      uint8   // 最大字节长度
    Description string  // 描述
}
```

#### 主要方法

- `GetCharsetByID()` - 根据ID获取字符集
- `GetCharsetByName()` - 根据名称获取字符集
- `GetCollationByID()` - 根据ID获取校对规则
- `GetDefaultCollation()` - 获取默认校对规则
- `IsMultiByteCharset()` - 判断是否为多字节字符集
- `GetAllCharsets()` - 获取所有字符集
- `GetCollationsByCharset()` - 获取指定字符集的所有校对规则

### 使用示例

```go
manager := GetGlobalCharsetManager()

// 获取UTF-8MB4字符集
utf8mb4, _ := manager.GetCharsetByName("utf8mb4")
fmt.Printf("字符集: %s, 最大长度: %d\n", utf8mb4.Name, utf8mb4.MaxLen)

// 获取所有UTF-8MB4的校对规则
collations := manager.GetCollationsByCharset("utf8mb4")
for _, col := range collations {
    fmt.Printf("校对规则: %s (ID: %d)\n", col.Name, col.ID)
}

// 验证字符集
if manager.IsValidCharset(255) {
    fmt.Println("字符集255有效")
}
```

---

## 4. 连接属性（CLIENT_CONNECT_ATTRS）

### 功能描述

连接属性允许客户端在握手阶段发送元数据信息，如客户端名称、版本、操作系统等。

### 实现文件

- `server/protocol/connection_attributes.go`

### 核心功能

#### ConnectionAttributes

```go
attrs := NewConnectionAttributes()

// 设置属性
attrs.Set("_client_name", "mysql-connector-go")
attrs.Set("_client_version", "8.0.33")
attrs.Set("_os", "Linux")

// 获取属性
value, exists := attrs.Get("_client_name")

// 获取客户端信息
clientInfo := attrs.GetClientInfo()
```

#### 标准属性键名

```go
const (
    AttrClientName        = "_client_name"        // 客户端名称
    AttrClientVersion     = "_client_version"     // 客户端版本
    AttrOS                = "_os"                 // 操作系统
    AttrPlatform          = "_platform"           // 平台
    AttrPID               = "_pid"                // 进程ID
    AttrConnectionID      = "_connection_id"      // 连接ID
    AttrThreadID          = "_thread"             // 线程ID
    AttrClientLicense     = "_client_license"     // 客户端许可证
    AttrProgramName       = "program_name"        // 程序名称
)
```

#### 编码格式

```
+-------------------------+
| 总长度 (length-encoded) |
+-------------------------+
| key1 (length-encoded)   |
+-------------------------+
| value1 (length-encoded) |
+-------------------------+
| key2 (length-encoded)   |
+-------------------------+
| value2 (length-encoded) |
+-------------------------+
| ...                     |
+-------------------------+
```

#### 主要方法

- `ParseConnectionAttributes()` - 解析连接属性
- `EncodeConnectionAttributes()` - 编码连接属性
- `ValidateAttributes()` - 验证属性（大小限制）
- `FilterAttributes()` - 过滤属性
- `MergeAttributes()` - 合并属性

### 使用示例

```go
// 创建连接属性
attrs := NewConnectionAttributes()
attrs.Set(AttrClientName, "myapp")
attrs.Set(AttrClientVersion, "1.0.0")
attrs.Set(AttrOS, "Linux")
attrs.Set("custom_key", "custom_value")

// 编码
encoded := EncodeConnectionAttributes(attrs)

// 解析
parsed, bytesRead, err := ParseConnectionAttributes(encoded)

// 获取客户端信息
clientInfo := parsed.GetClientInfo()
fmt.Printf("客户端: %s %s on %s\n", 
    clientInfo.Name, clientInfo.Version, clientInfo.OS)

// 验证
parser := NewConnectionAttributesParser()
if err := parser.ValidateAttributes(attrs); err != nil {
    fmt.Printf("验证失败: %v\n", err)
}
```

---

## 5. 会话状态跟踪（CLIENT_SESSION_TRACK）

### 功能描述

会话状态跟踪允许服务器在OK包中返回会话状态变更信息，如系统变量变更、数据库切换、事务状态等。

### 实现文件

- `server/protocol/session_track.go`

### 核心功能

#### SessionTracker

```go
tracker := NewSessionTracker(true) // 启用跟踪

// 跟踪系统变量
tracker.TrackSystemVariable("autocommit", "ON")

// 跟踪数据库变更
tracker.TrackSchema("testdb")

// 跟踪事务状态
tracker.TrackTransactionState(TxStateActive)

// 获取编码数据
trackData := tracker.EncodeForOKPacket()
```

#### 跟踪类型

```go
const (
    SessionTrackSystemVariables            = 0x00  // 系统变量
    SessionTrackSchema                     = 0x01  // 数据库
    SessionTrackStateChange                = 0x02  // 状态变更
    SessionTrackGTIDs                      = 0x03  // GTID
    SessionTrackTransactionCharacteristics = 0x04  // 事务特性
    SessionTrackTransactionState           = 0x05  // 事务状态
)
```

#### SessionStateManager

```go
manager := NewSessionStateManager(true)

// 设置系统变量
manager.SetSystemVariable("autocommit", "ON")

// 切换数据库
manager.SetSchema("mydb")

// 开始事务
manager.BeginTransaction("READ WRITE")

// 提交事务
manager.CommitTransaction()

// 获取跟踪数据（用于OK包）
trackData := manager.GetTrackingData()
```

#### 事务状态字符串

```go
const (
    TxStateIdle                       = "________"  // 空闲
    TxStateActive                     = "T_______"  // 活动事务
    TxStateReadOnly                   = "T_____r_"  // 只读事务
    TxStateReadWrite                  = "T_____w_"  // 读写事务
    TxStateWithConsistentSnapshot     = "T___s___"  // 一致性快照
)
```

#### OK包中的会话跟踪格式

```
OK包:
+------------------+
| 0x00 (OK标识)    |
+------------------+
| affected_rows    |
+------------------+
| last_insert_id   |
+------------------+
| status_flags     |
+------------------+
| warnings         |
+------------------+
| info (可选)      |
+------------------+
| session_track    |  <-- 会话跟踪数据
+------------------+

session_track格式:
+---------------------------+
| 总长度 (length-encoded)   |
+---------------------------+
| 类型1 (1字节)             |
+---------------------------+
| 数据1长度 (length-encoded)|
+---------------------------+
| 数据1                     |
+---------------------------+
| 类型2 (1字节)             |
+---------------------------+
| ...                       |
+---------------------------+
```

### 使用示例

```go
// 创建会话状态管理器
manager := NewSessionStateManager(true)

// 执行SET命令
manager.SetSystemVariable("sql_mode", "STRICT_TRANS_TABLES")
manager.SetSystemVariable("autocommit", "OFF")

// 执行USE命令
manager.SetSchema("testdb")

// 执行BEGIN
manager.BeginTransaction("READ WRITE")

// ... 执行SQL ...

// 执行COMMIT
manager.CommitTransaction()

// 在OK包中包含跟踪数据
trackData := manager.GetTrackingData()
if trackData != nil {
    // 将trackData添加到OK包的末尾
    okPacket = append(okPacket, trackData...)
}

// 清空跟踪（为下一个命令准备）
manager.ClearTracking()

// 解析会话跟踪数据
changes, _ := ParseSessionTrack(trackData)
info, _ := ParseSessionTrackInfo(changes)
fmt.Printf("会话变更: %s\n", info.String())
```

---

## 集成使用

### AdvancedProtocolHandler

提供了一个统一的高级协议处理器，集成所有功能：

```go
// 创建配置
config := NewDefaultAdvancedProtocolConfig()
config.EnableCompression = true
config.EnableSessionTracking = true
config.DefaultCharset = "utf8mb4"

// 创建处理器
handler := NewAdvancedProtocolHandler(config)

// 使用各个组件
charsetMgr := handler.GetCharsetManager()
compressor := handler.GetCompressionHandler()
sessionMgr := handler.GetSessionManager()
splitter := handler.GetPacketSplitter()

// 处理包（自动压缩和分片）
packets, err := handler.ProcessPacket(largePayload, sequenceId)
```

### 完整示例

```go
// 1. 初始化
handler := NewAdvancedProtocolHandler(nil) // 使用默认配置

// 2. 设置字符集
charsetMgr := handler.GetCharsetManager()
utf8mb4, _ := charsetMgr.GetCharsetByName("utf8mb4")

// 3. 解析连接属性
attrs, _, _ := ParseConnectionAttributes(authPacketData)
clientInfo := attrs.GetClientInfo()
fmt.Printf("客户端: %s\n", clientInfo.String())

// 4. 启用压缩（如果客户端支持）
if clientSupportsCompression {
    handler.EnableCompression()
}

// 5. 跟踪会话状态
sessionMgr := handler.GetSessionManager()
sessionMgr.SetSchema("mydb")
sessionMgr.SetSystemVariable("autocommit", "ON")

// 6. 处理大数据包
largeResult := generateLargeResultSet()
packets, _ := handler.ProcessPacket(largeResult, 1)
for _, packet := range packets {
    conn.Write(packet)
}

// 7. 在OK包中包含会话跟踪
trackData := sessionMgr.GetTrackingData()
okPacket := createOKPacket(affectedRows, lastInsertId)
if trackData != nil {
    okPacket = append(okPacket, trackData...)
}
conn.Write(okPacket)
```

---

## 性能考虑

### 压缩协议

- **优点**: 减少网络传输量，适合慢速网络
- **缺点**: 增加CPU开销
- **建议**: 
  - 局域网环境可以不启用
  - 广域网或慢速网络建议启用
  - 根据实际测试调整压缩阈值

### 大包分片

- **开销**: 每个包4字节包头
- **建议**: 
  - 尽量避免发送超大数据包
  - 考虑使用流式处理
  - 对于大结果集，使用游标或分页

### 会话状态跟踪

- **开销**: 每个OK包增加少量字节
- **建议**:
  - 只跟踪真正变化的状态
  - 及时清空跟踪缓存
  - 对于不需要跟踪的客户端可以禁用

---

## 兼容性

### MySQL版本兼容性

- **压缩协议**: MySQL 3.23+
- **字符集支持**: MySQL 4.1+
- **连接属性**: MySQL 5.6+
- **会话状态跟踪**: MySQL 5.7+

### 客户端兼容性

所有实现都向后兼容，不支持的客户端会自动忽略这些特性。

---

## 测试建议

### 单元测试

```go
// 测试大包分片
func TestPacketSplitter(t *testing.T) {
    splitter := NewPacketSplitter()
    largeData := make([]byte, 20*1024*1024)
    packets := splitter.SplitPacket(largeData, 0)
    merged, _ := splitter.MergePackets(packets)
    assert.Equal(t, len(largeData), len(merged))
}

// 测试压缩
func TestCompression(t *testing.T) {
    handler := NewCompressionHandler(true)
    data := []byte("test data...")
    compressed, _ := handler.CompressPacket(data, 0)
    decompressed, _, _ := handler.DecompressPacket(compressed)
    assert.Equal(t, data, decompressed)
}
```

### 集成测试

使用真实的MySQL客户端连接测试：

```bash
# 测试压缩协议
mysql -h localhost -P 3306 -u root --compress

# 测试字符集
mysql -h localhost -P 3306 -u root --default-character-set=utf8mb4

# 测试连接属性
mysql -h localhost -P 3306 -u root --connect-attrs="app=myapp,version=1.0"
```

---

## 故障排查

### 常见问题

1. **压缩失败**
  - 检查zlib库是否正确安装
  - 验证压缩阈值设置
  - 查看压缩统计信息
2. **分片错误**
  - 验证包序号是否连续
  - 检查包长度是否正确
  - 确认最后是否有空包
3. **字符集问题**
  - 确认客户端和服务器字符集一致
  - 检查字符集ID是否有效
  - 验证多字节字符处理
4. **会话跟踪不生效**
  - 确认客户端支持SESSION_TRACK
  - 检查是否正确编码到OK包
  - 验证跟踪是否已启用

---

## 参考资料

- [MySQL Internals Manual](https://dev.mysql.com/doc/internals/en/)
- [MySQL Protocol Documentation](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)
- [MySQL Compressed Protocol](https://dev.mysql.com/doc/internals/en/compressed-packet-header.html)
- [MySQL Character Sets](https://dev.mysql.com/doc/refman/8.0/en/charset.html)
- [MySQL Session State Tracking](https://dev.mysql.com/doc/refman/8.0/en/session-state-tracking.html)

---

## 更新日志

### v1.0.0 (2025-11-14)

- ✅ 实现大包分片处理
- ✅ 实现压缩协议支持
- ✅ 实现完整的字符集支持
- ✅ 实现连接属性解析
- ✅ 实现会话状态跟踪
- ✅ 提供集成使用示例

---

## 贡献

欢迎提交问题和改进建议！