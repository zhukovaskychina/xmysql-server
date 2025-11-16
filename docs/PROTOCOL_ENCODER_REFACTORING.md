# MySQL 协议编码器重构文档

## 📋 重构目标

将 `MySQLProtocolEncoder` 从 `net` 包移动到 `protocol` 包，并集成到 `MySQLProtocolHandler` 中，实现更好的代码组织和复用。

## 🔄 重构内容

### 1. **文件变更**

#### 新增文件
- **`server/protocol/resultset_encoder.go`** - MySQL ResultSet 协议编码器
  - 专门用于编码查询结果集相关的协议包
  - 包含所有 ResultSet 编码逻辑

#### 保留文件
- **`server/protocol/encoder.go`** - 消息编码器注册表（已存在）
  - 用于注册和管理各种消息类型的编码器
  - 与 ResultSet 编码器功能不同，互不冲突

#### 删除文件
- **`server/net/mysql_protocol_encoder.go`** - 已移除
  - 功能已迁移到 `protocol/resultset_encoder.go`

### 2. **类型重命名**

为了避免与 `protocol/encoder.go` 中的 `MySQLProtocolEncoder` 冲突，将 ResultSet 编码器重命名：

**旧名称**: `MySQLProtocolEncoder` (in net package)
**新名称**: `MySQLResultSetEncoder` (in protocol package)

### 3. **核心类型和常量**

#### MySQLResultSetEncoder 结构体
```go
// MySQLResultSetEncoder MySQL ResultSet 协议编码器
// 专门用于编码查询结果集（ResultSet）相关的协议包
// 严格按照 MySQL 5.7/8.0 协议规范实现
type MySQLResultSetEncoder struct{}
```

#### 导出的类型
```go
// ColumnDefinition 列定义结构
type ColumnDefinition struct {
    Catalog      string
    Schema       string
    Table        string
    OrgTable     string
    Name         string
    OrgName      string
    CharacterSet uint16
    ColumnLength uint32
    ColumnType   byte
    Flags        uint16
    Decimals     byte
}

// ResultSetData 结果集数据
type ResultSetData struct {
    Columns []string
    Rows    [][]interface{}
}
```

#### MySQL 字段类型常量
```go
const (
    MYSQL_TYPE_DECIMAL     byte = 0x00
    MYSQL_TYPE_TINY        byte = 0x01  // TINYINT
    MYSQL_TYPE_SHORT       byte = 0x02  // SMALLINT
    MYSQL_TYPE_LONG        byte = 0x03  // INT
    MYSQL_TYPE_FLOAT       byte = 0x04  // FLOAT
    MYSQL_TYPE_DOUBLE      byte = 0x05  // DOUBLE
    MYSQL_TYPE_NULL        byte = 0x06
    MYSQL_TYPE_TIMESTAMP   byte = 0x07
    MYSQL_TYPE_LONGLONG    byte = 0x08  // BIGINT
    MYSQL_TYPE_INT24       byte = 0x09  // MEDIUMINT
    MYSQL_TYPE_DATE        byte = 0x0A
    MYSQL_TYPE_TIME        byte = 0x0B
    MYSQL_TYPE_DATETIME    byte = 0x0C
    MYSQL_TYPE_YEAR        byte = 0x0D
    MYSQL_TYPE_VARCHAR     byte = 0x0F
    MYSQL_TYPE_BIT         byte = 0x10
    MYSQL_TYPE_NEWDECIMAL  byte = 0xF6
    MYSQL_TYPE_ENUM        byte = 0xF7
    MYSQL_TYPE_SET         byte = 0xF8
    MYSQL_TYPE_BLOB        byte = 0xFC
    MYSQL_TYPE_VAR_STRING  byte = 0xFD  // VARCHAR/TEXT
    MYSQL_TYPE_STRING      byte = 0xFE  // CHAR
    MYSQL_TYPE_GEOMETRY    byte = 0xFF
)
```

#### 列标志常量
```go
const (
    FLAG_NOT_NULL       uint16 = 0x0001
    FLAG_PRI_KEY        uint16 = 0x0002
    FLAG_UNIQUE_KEY     uint16 = 0x0004
    FLAG_MULTIPLE_KEY   uint16 = 0x0008
    FLAG_BLOB           uint16 = 0x0010
    FLAG_UNSIGNED       uint16 = 0x0020
    FLAG_ZEROFILL       uint16 = 0x0040
    FLAG_BINARY         uint16 = 0x0080
    FLAG_ENUM           uint16 = 0x0100
    FLAG_AUTO_INCREMENT uint16 = 0x0200
    FLAG_TIMESTAMP      uint16 = 0x0400
    FLAG_SET            uint16 = 0x0800
)
```

#### 服务器状态标志
```go
const (
    SERVER_STATUS_IN_TRANS             uint16 = 0x0001
    SERVER_STATUS_AUTOCOMMIT           uint16 = 0x0002
    SERVER_MORE_RESULTS_EXISTS         uint16 = 0x0008
    SERVER_STATUS_NO_GOOD_INDEX_USED   uint16 = 0x0010
    SERVER_STATUS_NO_INDEX_USED        uint16 = 0x0020
    SERVER_STATUS_CURSOR_EXISTS        uint16 = 0x0040
    SERVER_STATUS_LAST_ROW_SENT        uint16 = 0x0080
    SERVER_STATUS_DB_DROPPED           uint16 = 0x0100
    SERVER_STATUS_NO_BACKSLASH_ESCAPES uint16 = 0x0200
    SERVER_STATUS_METADATA_CHANGED     uint16 = 0x0400
    SERVER_QUERY_WAS_SLOW              uint16 = 0x0800
    SERVER_PS_OUT_PARAMS               uint16 = 0x1000
)
```

### 4. **核心方法**

#### 编码方法
```go
// Length-Encoded Integer/String
func (e *MySQLResultSetEncoder) WriteLenEncInt(value uint64) []byte
func (e *MySQLResultSetEncoder) WriteLenEncString(str string) []byte
func (e *MySQLResultSetEncoder) WriteLenEncNullString(str *string) []byte

// 协议包编码
func (e *MySQLResultSetEncoder) WriteColumnDefinitionPacket(col *ColumnDefinition) []byte
func (e *MySQLResultSetEncoder) WriteEOFPacket(warnings uint16, statusFlags uint16) []byte
func (e *MySQLResultSetEncoder) WriteOKPacket(affectedRows, lastInsertID uint64, statusFlags, warnings uint16) []byte
func (e *MySQLResultSetEncoder) WriteRowDataPacket(values []interface{}) []byte

// 完整 ResultSet 编码
func (e *MySQLResultSetEncoder) SendResultSetPackets(data *ResultSetData) [][]byte
```

#### 辅助方法
```go
// 类型推断
func (e *MySQLResultSetEncoder) InferMySQLType(value interface{}) byte
func (e *MySQLResultSetEncoder) InferColumnLength(fieldType byte) uint32

// 列定义创建
func (e *MySQLResultSetEncoder) CreateColumnDefinition(name string, fieldType byte, flags uint16) *ColumnDefinition
func (e *MySQLResultSetEncoder) CreateColumnDefinitionFromValue(name string, value interface{}) *ColumnDefinition

// 工具方法
func (e *MySQLResultSetEncoder) LenEncIntSize(value uint64) int
func (e *MySQLResultSetEncoder) LenEncStringSize(str string) int
func (e *MySQLResultSetEncoder) IsNaN(value interface{}) bool
func (e *MySQLResultSetEncoder) IsInf(value interface{}) bool
```

### 5. **集成到 MySQLProtocolHandler**

#### 修改前
```go
type MySQLProtocolHandler struct {
    sessionManager  session.SessionManager
    queryDispatcher QueryDispatcher
    preparedStmtMgr *PreparedStatementManager
}
```

#### 修改后
```go
type MySQLProtocolHandler struct {
    sessionManager  session.SessionManager
    queryDispatcher QueryDispatcher
    preparedStmtMgr *PreparedStatementManager
    encoder         *MySQLResultSetEncoder // ResultSet 编码器（复用实例）
}

func NewMySQLProtocolHandler(sessionMgr session.SessionManager, dispatcher QueryDispatcher) *MySQLProtocolHandler {
    return &MySQLProtocolHandler{
        sessionManager:  sessionMgr,
        queryDispatcher: dispatcher,
        preparedStmtMgr: NewPreparedStatementManager(),
        encoder:         NewMySQLResultSetEncoder(), // 初始化编码器
    }
}
```

#### 使用示例
```go
// sendQueryResult 发送查询结果
func (h *MySQLProtocolHandler) sendQueryResult(conn net.Conn, result *QueryResult) {
    // 使用编码器生成 ResultSet 包
    resultSetData := &ResultSetData{
        Columns: result.Columns,
        Rows:    result.Rows,
    }

    // 生成所有包（不包括 MySQL packet header）
    packets := h.encoder.SendResultSetPackets(resultSetData)

    // 添加 MySQL packet header 并发送
    seqID := byte(1)
    for _, payload := range packets {
        packet := h.addPacketHeader(payload, seqID)
        conn.Write(packet)
        seqID++
    }
}
```

### 6. **DecoupledMySQLMessageHandler 更新**

#### 修改前
```go
type DecoupledMySQLMessageHandler struct {
    // ... 其他字段
    resultSetEncoder *MySQLProtocolEncoder // 旧类型
}
```

#### 修改后
```go
type DecoupledMySQLMessageHandler struct {
    // ... 其他字段
    resultSetEncoder *protocol.MySQLResultSetEncoder // 新类型
}

func NewDecoupledMySQLMessageHandlerWithEngine(cfg *conf.Cfg, xmysqlEngine *engine.XMySQLEngine) *DecoupledMySQLMessageHandler {
    handler := &DecoupledMySQLMessageHandler{
        // ... 其他初始化
        resultSetEncoder: protocol.NewMySQLResultSetEncoder(), // 使用新构造函数
    }
    return handler
}
```

#### 使用更新
```go
// sendQueryResultSet 方法中的使用
encoder := h.resultSetEncoder

// 使用 protocol 包的常量
colDef := encoder.CreateColumnDefinition(colName, protocol.MYSQL_TYPE_VAR_STRING, 0)
eofData := encoder.WriteEOFPacket(0, protocol.SERVER_STATUS_AUTOCOMMIT)
```

## 📊 代码组织结构

### 重构前
```
server/
├── net/
│   ├── mysql_protocol_encoder.go      ← 编码器在 net 包
│   ├── mysql_protocol_encoder_test.go
│   ├── encoder_optimization_test.go
│   └── decoupled_handler.go
└── protocol/
    ├── encoder.go                      ← 消息编码器注册表
    └── mysql_protocol.go
```

### 重构后
```
server/
├── net/
│   ├── decoupled_handler.go            ← 使用 protocol.MySQLResultSetEncoder
│   └── ... (测试文件需要更新)
└── protocol/
    ├── encoder.go                      ← 消息编码器注册表（保持不变）
    ├── resultset_encoder.go            ← ✨ 新增：ResultSet 编码器
    └── mysql_protocol.go               ← 集成 ResultSet 编码器
```

## 🎯 设计优势

### 1. **职责分离**
- **`protocol/encoder.go`**: 消息类型注册和分发
- **`protocol/resultset_encoder.go`**: ResultSet 协议包编码
- **`protocol/mysql_protocol.go`**: 协议处理和编码器集成

### 2. **复用性提升**
- `MySQLProtocolHandler` 和 `DecoupledMySQLMessageHandler` 都可以使用同一个编码器
- 编码器实例在处理器创建时初始化，避免重复创建

### 3. **命名清晰**
- `MySQLProtocolEncoder` - 消息编码器注册表
- `MySQLResultSetEncoder` - ResultSet 专用编码器
- 两者功能明确，不会混淆

### 4. **包结构合理**
- 协议相关的所有代码都在 `protocol` 包中
- `net` 包专注于网络层处理
- 符合单一职责原则

## 🧪 测试更新需求

以下测试文件需要更新导入和类型引用：

### 1. `server/net/mysql_protocol_encoder_test.go`
```go
// 需要更新为
import "github.com/zhukovaskychina/xmysql-server/server/protocol"

func TestWriteLenEncInt(t *testing.T) {
    encoder := protocol.NewMySQLResultSetEncoder() // 更新构造函数
    // ... 测试代码
}
```

### 2. `server/net/encoder_optimization_test.go`
```go
// 需要更新为
import "github.com/zhukovaskychina/xmysql-server/server/protocol"

func BenchmarkEncoderCreation(b *testing.B) {
    for i := 0; i < b.N; i++ {
        encoder := protocol.NewMySQLResultSetEncoder() // 更新
        _ = encoder.SendResultSetPackets(data)
    }
}
```

## ✅ 验收标准

- [x] `protocol/resultset_encoder.go` 创建成功
- [x] `MySQLProtocolHandler` 集成编码器
- [x] `DecoupledMySQLMessageHandler` 更新引用
- [x] 所有常量使用 `protocol.` 前缀
- [x] 编译通过（`go build .`）
- [ ] 测试文件更新（需要手动更新）
- [ ] 运行时验证（需要实际测试）

## 📝 使用示例

### 创建编码器
```go
encoder := protocol.NewMySQLResultSetEncoder()
```

### 编码 ResultSet
```go
data := &protocol.ResultSetData{
    Columns: []string{"id", "name", "age"},
    Rows: [][]interface{}{
        {1, "Alice", 25},
        {2, "Bob", 30},
    },
}

packets := encoder.SendResultSetPackets(data)
// packets 包含：
// [0] Column Count Packet
// [1] Column Definition for "id"
// [2] Column Definition for "name"
// [3] Column Definition for "age"
// [4] EOF Packet (after columns)
// [5] Row Data for row 1
// [6] Row Data for row 2
// [7] EOF Packet (after rows)
```

### 使用常量
```go
// 字段类型
colDef := encoder.CreateColumnDefinition("age", protocol.MYSQL_TYPE_LONG, protocol.FLAG_NOT_NULL)

// 服务器状态
eofPacket := encoder.WriteEOFPacket(0, protocol.SERVER_STATUS_AUTOCOMMIT)

// OK 包
okPacket := encoder.WriteOKPacket(1, 0, protocol.SERVER_STATUS_AUTOCOMMIT, 0)
```

## 🔧 后续优化建议

### 1. 性能优化
- 考虑使用 `sync.Pool` 复用 buffer
- 预分配 slice 容量以减少内存分配

### 2. 功能扩展
- 支持二进制协议（Binary Protocol）
- 支持压缩协议（Compressed Protocol）
- 支持 SSL/TLS 加密

### 3. 测试完善
- 添加更多边界条件测试
- 添加性能基准测试
- 添加协议兼容性测试

## 📚 相关文档

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)
- [MySQL Text Protocol](https://dev.mysql.com/doc/internals/en/text-protocol.html)
- [MySQL Binary Protocol](https://dev.mysql.com/doc/internals/en/binary-protocol.html)

---

**重构完成时间**: 2024-11-16  
**版本**: 1.0.0  
**状态**: ✅ 编译通过，待测试验证
