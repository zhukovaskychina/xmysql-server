# MySQL 协议与 JDBC 文档索引

> **2026-04**：协议相关文档分散在 `docs/protocol/`（连接/字符集修复）与 `docs/protocol-reports/`（PROTO-*、JDBC 深度分析）。本页为 **推荐入口**；预处理语句的**维护说明**以 `development/NET-001` 为准。

---

## `docs/protocol/`（连接与行为修复）


| 文档                                                                                                                                    | 说明                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [JDBC_CONNECTION_FIX_SUMMARY.md](./JDBC_CONNECTION_FIX_SUMMARY.md)                                                                    | **第一部分**：系统变量结果集 / Schema（`ResultSet is from UPDATE` 等）          |
| [JDBC_CONNECTION_COMMUNICATION_FIX.md](./JDBC_CONNECTION_COMMUNICATION_FIX.md)                                                        | **第二部分**：异步写包、包序列号（`Communications link failure`）— **请按顺序与上文联读** |
| [SET_NAMES_PROTOCOL_ANALYSIS.md](./SET_NAMES_PROTOCOL_ANALYSIS.md) / [SET_NAMES_UTF8_FIX_SUMMARY.md](./SET_NAMES_UTF8_FIX_SUMMARY.md) | `SET NAMES` 与 UTF8                                               |
| [TX_READ_ONLY_FIX_SUMMARY.md](./TX_READ_ONLY_FIX_SUMMARY.md)                                                                          | 事务只读相关修复                                                         |


---

## `docs/protocol-reports/`（专项与深度分析）


| 文档                                                                                                               | 说明                                                                                                       |
| ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| [JDBC_PROTOCOL_ANALYSIS.md](../protocol-reports/JDBC_PROTOCOL_ANALYSIS.md)                                       | JDBC 协议阶段、兼容性与问题清单（**长文**）                                                                               |
| [PROTO-003_PASSWORD_VERIFICATION_FIX.md](../protocol-reports/PROTO-003_PASSWORD_VERIFICATION_FIX.md)             | 密码验证                                                                                                     |
| [PROTO-004_COLUMN_TYPE_MAPPING_FIX.md](../protocol-reports/PROTO-004_COLUMN_TYPE_MAPPING_FIX.md)                 | 列类型映射                                                                                                    |
| [PROTO-005_ERROR_CODE_MAPPING_REPORT.md](../protocol-reports/PROTO-005_ERROR_CODE_MAPPING_REPORT.md)             | 错误码映射                                                                                                    |
| [PREPARED_STATEMENT_IMPLEMENTATION_SUMMARY.md](../protocol-reports/PREPARED_STATEMENT_IMPLEMENTATION_SUMMARY.md) | **→ 跳转** 至 [NET-001-PREPARED-STATEMENT-SUMMARY.md](../development/NET-001-PREPARED-STATEMENT-SUMMARY.md) |


---

## `docs/` 根目录：会话与协议补充

与 **current DB**、`COM_INIT_DB` / `COM_QUERY`+`USE`、包处理相关的说明（不在 `protocol/` 子目录）：

| 文档 | 说明 |
|------|------|
| [SESSION_STATE_AND_PROTOCOL.md](../SESSION_STATE_AND_PROTOCOL.md) | 会话状态与 `handlePacket`（INIT_DB vs QUERY+USE） |
| [SESSION_CONTEXT_DESIGN.md](../SESSION_CONTEXT_DESIGN.md) | 会话上下文设计 |
| [ADVANCED_PROTOCOL_FEATURES.md](../ADVANCED_PROTOCOL_FEATURES.md) | 高级协议特性备忘 |
| [HANDSHAKE_FILES_MERGE.md](../HANDSHAKE_FILES_MERGE.md) | 握手相关合并说明 |
| [SHOW_DROP_IMPLEMENTATION.md](../SHOW_DROP_IMPLEMENTATION.md) | SHOW / DROP DATABASE 与 `enginx.go` 实现说明 |

---

## 代码入口

- `server/net/decoupled_handler.go` — `COM_STMT_*` 主路径  
- `server/protocol/` — 握手、包编解码、`prepared_statement_manager.go`  
- `server/net/` — `handlePacket`、会话与选库逻辑（与上表文档对应）

