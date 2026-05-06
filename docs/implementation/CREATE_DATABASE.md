# CREATE DATABASE 设计与实现

> **文档状态（2026-04）**：与当前 `server/` 实现一致；缺口与优先级见 [未实现功能梳理.md](../未实现功能梳理.md)；任务 ID 见 [development/DEVELOPMENT_ROADMAP_TASKS.md](../development/DEVELOPMENT_ROADMAP_TASKS.md)。

---

## 1. 功能范围

- MySQL 风格 `**CREATE DATABASE` / `CREATE SCHEMA`**，支持 `**IF NOT EXISTS**`。
- 默认 **charset / collation**（如 `utf8mb4` / `utf8mb4_general_ci`），写入 `**db.opt`**，并与 **数据字典 / 内存 schema 缓存** 同步（细节以代码为准）。

---

## 2. 代码入口（权威）


| 职责                                        | 位置                                                                                         |
| ----------------------------------------- | ------------------------------------------------------------------------------------------ |
| Schema 管理器侧创建（目录、`db.opt`、字典、`schemaMap`） | `server/innodb/manager/schema_manager.go` — `CreateDatabase`                               |
| 执行器侧 DDL 分派与落盘（目录、`db.opt` 等）             | `server/innodb/engine/executor.go` — `executeCreateDatabaseStatement`、`createDatabaseImpl` |


排障时请用调用栈确认实际走的是 **SchemaManager** 路径还是 **executor** 内联路径（两者可能并存，以你分支为准）。

---

## 3. 相关文档

- [DML_IMPLEMENTATION.md](./DML_IMPLEMENTATION.md) — 其它 DML/DDL 说明  
- [IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md) — 实现总览  
- 协议与会话（`USE` / `COM_INIT_DB` 与 current DB）：[SESSION_STATE_AND_PROTOCOL.md](../SESSION_STATE_AND_PROTOCOL.md)、[protocol/PROTOCOL_DOCUMENTATION_INDEX.md](../protocol/PROTOCOL_DOCUMENTATION_INDEX.md)

---

## 4. 验证

在仓库根目录对 `server/` 检索测试与调用点，例如：

`rg -n "CreateDatabase|CREATE DATABASE|executeCreateDatabaseStatement" server/`